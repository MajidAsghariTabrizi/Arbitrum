// SPDX-License-Identifier: MIT
pragma solidity ^0.8.10;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

// =====================================================================
// AAVE V2 / RADIANT INTERFACES
// =====================================================================
interface ILendingPoolAddressesProvider {
    function getLendingPool() external view returns (address);
}

interface ILendingPool {
    function flashLoan(
        address receiverAddress,
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata modes,
        address onBehalfOf,
        bytes calldata params,
        uint16 referralCode
    ) external;

    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external;

    function getUserAccountData(address user) external view returns (
        uint256 totalCollateralETH,
        uint256 totalDebtETH,
        uint256 availableBorrowsETH,
        uint256 currentLiquidationThreshold,
        uint256 ltv,
        uint256 healthFactor
    );
}

interface IFlashLoanReceiver {
    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external returns (bool);
}

// =====================================================================
// UNISWAP V3 INTERFACE
// =====================================================================
interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

// =====================================================================
// CONTRACT
// =====================================================================
contract RadiantLiquidator is IFlashLoanReceiver, Ownable {
    
    ILendingPoolAddressesProvider public immutable addressesProvider;
    ISwapRouter public immutable swapRouter;

    constructor(
        address _addressesProvider, 
        address _swapRouter
    ) {
        addressesProvider = ILendingPoolAddressesProvider(_addressesProvider);
        swapRouter = ISwapRouter(_swapRouter);
    }

    // Custom Errors
    error UserNotLiquidatable(uint256 healthFactor);
    error NoCollateralSeized();
    error NotProfitable(uint256 received, uint256 required);

    struct LiquidationParams {
        address userToLiquidate;
        address collateralAsset;
        uint24 fee;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    /**
     * @dev Initiates a flash loan to perform a liquidation.
     */
    function requestFlashLoan(
        address _userToLiquidate,
        address _debtAsset,
        address _collateralAsset,
        uint256 _debtAmount,
        uint24 _fee,
        uint256 _amountOutMinimum,
        uint160 _sqrtPriceLimitX96
    ) external onlyOwner {

        ILendingPool lendingPool = ILendingPool(addressesProvider.getLendingPool());

        // 1. Pre-flight Check
        (,,,,, uint256 healthFactor) = lendingPool.getUserAccountData(_userToLiquidate);
        if (healthFactor >= 1e18) {
            revert UserNotLiquidatable(healthFactor);
        }

        // 2. Prepare Data
        address[] memory assets = new address[](1);
        assets[0] = _debtAsset;

        uint256[] memory amounts = new uint256[](1);
        amounts[0] = _debtAmount;

        // 0 = no debt (flash loan), 1 = stable, 2 = variable
        uint256[] memory modes = new uint256[](1);
        modes[0] = 0; 

        bytes memory params = abi.encode(LiquidationParams({
            userToLiquidate: _userToLiquidate,
            collateralAsset: _collateralAsset,
            fee: _fee,
            amountOutMinimum: _amountOutMinimum,
            sqrtPriceLimitX96: _sqrtPriceLimitX96
        }));

        // 3. Execute Flash Loan
        lendingPool.flashLoan(
            address(this),
            assets,
            amounts,
            modes,
            address(this),
            params,
            0 // referralCode
        );
    }

    /**
     * @dev Callback function called by Radiant LendingPool after sending funds.
     */
    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        
        // Ensure caller is LendingPool
        ILendingPool lendingPool = ILendingPool(addressesProvider.getLendingPool());
        require(msg.sender == address(lendingPool), "Caller must be LendingPool");
        require(initiator == address(this), "Initiator must be this contract");

        // 1. Decode params
        LiquidationParams memory liqParams = abi.decode(params, (LiquidationParams));
        address debtAsset = assets[0];
        uint256 debtAmount = amounts[0];
        uint256 premium = premiums[0];

        // 2. Approve LendingPool to spend the debt asset (for liquidation)
        IERC20(debtAsset).approve(address(lendingPool), debtAmount);

        // 3. Liquidate
        lendingPool.liquidationCall(
            liqParams.collateralAsset,
            debtAsset,
            liqParams.userToLiquidate,
            debtAmount,
            false // receive underlying collateral
        );

        // 4. Swap Collateral -> Debt Asset
        uint256 collateralBalance = IERC20(liqParams.collateralAsset).balanceOf(address(this));
        if (collateralBalance == 0) {
            revert NoCollateralSeized();
        }

        IERC20(liqParams.collateralAsset).approve(address(swapRouter), collateralBalance);

        ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter.ExactInputSingleParams({
            tokenIn: liqParams.collateralAsset,
            tokenOut: debtAsset,
            fee: liqParams.fee,
            recipient: address(this),
            deadline: type(uint256).max,
            amountIn: collateralBalance,
            amountOutMinimum: liqParams.amountOutMinimum,
            sqrtPriceLimitX96: liqParams.sqrtPriceLimitX96
        });

        uint256 amountReceived = swapRouter.exactInputSingle(swapParams);

        // 5. Repay Flash Loan
        uint256 totalDebt = debtAmount + premium;
        if (amountReceived < totalDebt) {
            revert NotProfitable(amountReceived, totalDebt);
        }

        IERC20(debtAsset).approve(address(lendingPool), totalDebt);

        // 6. Withdraw Profit
        uint256 profit = amountReceived - totalDebt;
        if (profit > 0) {
            IERC20(debtAsset).transfer(owner(), profit);
        }

        return true;
    }

    function withdraw(address _token) external onlyOwner {
        uint256 balance = IERC20(_token).balanceOf(address(this));
        if (balance > 0) {
            IERC20(_token).transfer(owner(), balance);
        }
    }
}
