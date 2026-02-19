// SPDX-License-Identifier: MIT
pragma solidity ^0.8.10;

import "@aave/core-v3/contracts/flashloan/base/FlashLoanSimpleReceiverBase.sol";
import "@aave/core-v3/contracts/interfaces/IPoolAddressesProvider.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

/**
 * @title TriArbitrageur
 * @notice Multi-hop arbitrage contract using Aave V3 Flashloans
 */
contract TriArbitrageur is FlashLoanSimpleReceiverBase, Ownable {

    // Custom Error for profitability check
    error NotProfitable(uint256 balance, uint256 required);

    // Route struct for multi-hop execution
    struct Route {
        address router;
        address tokenIn;
        bytes payload; // Encoded function call (e.g., swap, exactInput, etc.)
    }

    constructor(address _addressProvider) 
        FlashLoanSimpleReceiverBase(IPoolAddressesProvider(_addressProvider)) 
    {}

    /**
     * @notice Initiates a flashloan to perform multi-hop arbitrage
     * @param _token The asset to flashloan (e.g. USDC)
     * @param _amount The amount to flashloan
     * @param _routes Array of Route structs defining the swap path
     */
    function requestFlashLoan(
        address _token,
        uint256 _amount,
        Route[] calldata _routes
    ) external onlyOwner {
        // Encode the routes array to pass it to executeOperation
        bytes memory params = abi.encode(_routes);

        // flashLoanSimple(receiver, asset, amount, params, referralCode)
        POOL.flashLoanSimple(
            address(this),
            _token,
            _amount,
            params,
            0 
        );
    }

    /**
     * @notice Callback function executed by Aave V3 after receiving the flashloan
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        
        // 1. Decode the routes
        Route[] memory routes = abi.decode(params, (Route[]));

        // 2. Iterate through each route and execute the swap
        for (uint256 i = 0; i < routes.length; i++) {
            Route memory route = routes[i];

            // Dynamic check of the input token balance
            uint256 balanceIn = IERC20(route.tokenIn).balanceOf(address(this));
            
            // Revert if zero balance (sanity check)
            require(balanceIn > 0, "Zero balance for route step");

            // Approve the router to spend the tokens
            // Reset approval to 0 first to handle tokens like USDT that require it (optional but safer)
            // For standard ERC20, strict approve is fine. 
            // We use simple approve here as per requirements "Approve route.router to spend the exact balance"
            IERC20(route.tokenIn).approve(route.router, balanceIn);

            // Execute the swap via low-level call
            (bool success, ) = route.router.call(route.payload);
            require(success, "Swap failed");
        }

        // 3. Verify Repayment
        uint256 amountToRepay = amount + premium;
        uint256 finalBalance = IERC20(asset).balanceOf(address(this));

        if (finalBalance < amountToRepay) {
            revert NotProfitable(finalBalance, amountToRepay);
        }

        // 4. Approve Aave to pull the repayment amount
        IERC20(asset).approve(address(POOL), amountToRepay);

        return true;
    }

    /**
     * @notice Allows the owner to withdraw any token from the contract (profits)
     * @param _token The token address to withdraw
     */
    function withdraw(address _token) external onlyOwner {
        uint256 balance = IERC20(_token).balanceOf(address(this));
        if (balance > 0) {
            IERC20(_token).transfer(owner(), balance);
        }
    }
}
