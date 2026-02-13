// SPDX-License-Identifier: MIT
pragma solidity ^0.8.10;

import "@aave/core-v3/contracts/flashloan/base/FlashLoanSimpleReceiverBase.sol";
import "@aave/core-v3/contracts/interfaces/IPoolAddressesProvider.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

// Interfaces for Uniswap V3
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

contract FlashLoanLiquidator is FlashLoanSimpleReceiverBase, Ownable {
    
    ISwapRouter public immutable swapRouter;

    // Arbitrum One Addresses
    address private constant USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address private constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;

    constructor(
        address _addressProvider, 
        address _swapRouter
    ) FlashLoanSimpleReceiverBase(IPoolAddressesProvider(_addressProvider)) {
        swapRouter = ISwapRouter(_swapRouter);
    }

    // Struct to pass custom data to executeOperation
    struct LiquidationParams {
        address userToLiquidate;
        address collateralAsset;
        uint24 fee;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function requestFlashLoan(
        address _userToLiquidate,
        address _debtAsset, // Underlying asset to borrow (e.g. USDC)
        address _collateralAsset, // Collateral to seize (e.g. WETH)
        uint256 _debtAmount,
        uint24 _fee,
        uint256 _amountOutMinimum,
        uint160 _sqrtPriceLimitX96
    ) external onlyOwner {
        
        // Encode params to pass to callback
        bytes memory params = abi.encode(LiquidationParams({
            userToLiquidate: _userToLiquidate,
            collateralAsset: _collateralAsset,
            fee: _fee,
            amountOutMinimum: _amountOutMinimum,
            sqrtPriceLimitX96: _sqrtPriceLimitX96
        }));

        // Request Flash Loan
        // mode = 0 (no debt opened)
        POOL.flashLoanSimple(
            address(this),
            _debtAsset,
            _debtAmount,
            params,
            0 
        );
    }

    /**
     * @dev This function is called after your contract has received the flash loaned amount
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        
        // 1. Decode params
        LiquidationParams memory liqParams = abi.decode(params, (LiquidationParams));
        
        // 2. Liquidate
        // Approve POOL to spend the borrowed debt asset
        IERC20(asset).approve(address(POOL), amount);

        // Perform liquidation
        // debtToCover = amount (we borrowed exactly what we want to repay)
        // receiveAToken = false (we want the underlying collateral)
        POOL.liquidationCall(
            liqParams.collateralAsset,
            asset,
            liqParams.userToLiquidate,
            amount,
            false // receive underlying
        );

        // 3. Swap Collateral for Debt Asset to repay Flash Loan
        uint256 collateralBalance = IERC20(liqParams.collateralAsset).balanceOf(address(this));
        
        require(collateralBalance > 0, "No collateral seized");

        // Approve SwapRouter to spend collateral
        IERC20(liqParams.collateralAsset).approve(address(swapRouter), collateralBalance);

        // Uniswap V3 Swap
        ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter.ExactInputSingleParams({
            tokenIn: liqParams.collateralAsset,
            tokenOut: asset,
            fee: liqParams.fee, 
            recipient: address(this),
            deadline: block.timestamp,
            amountIn: collateralBalance,
            amountOutMinimum: liqParams.amountOutMinimum, 
            sqrtPriceLimitX96: liqParams.sqrtPriceLimitX96
        });

        uint256 amountReceived = swapRouter.exactInputSingle(swapParams);

        // 4. Repay Flash Loan
        uint256 totalDebt = amount + premium;
        require(amountReceived >= totalDebt, "Not profitable");

        IERC20(asset).approve(address(POOL), totalDebt);

        // 5. Transfer Profit to Owner
        uint256 profit = amountReceived - totalDebt;
        if (profit > 0) {
            IERC20(asset).transfer(owner(), profit);
        }

        return true;
    }

    // Helper to withdraw any stuck funds
    function withdraw(address _token) external onlyOwner {
        uint256 balance = IERC20(_token).balanceOf(address(this));
        IERC20(_token).transfer(owner(), balance);
    }
}
