// SPDX-License-Identifier: MIT
pragma solidity 0.8.10;

import "@aave/core-v3/contracts/flashloan/base/FlashLoanSimpleReceiverBase.sol";
import "@aave/core-v3/contracts/interfaces/IPoolAddressesProvider.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

/**
 * @title DexArbitrageur
 * @author Anti-Gravity MEV Platform
 * @notice Production-grade Atomic DEX Arbitrage using Aave V3 Flashloans on Arbitrum
 * @dev Gas-optimized with custom errors, block scoping, and strict profitability enforcement.
 *
 * Flow:
 *   1. Owner calls requestFlashLoan(tokenX, amount, params)
 *   2. Aave grants flashloan → executeOperation callback fires
 *   3. Swap A: tokenX → tokenY via Router A
 *   4. Swap B: tokenY → tokenX via Router B
 *   5. Verify finalBalance(tokenX) > amount + premium (strictly profitable)
 *   6. Repay Aave; profit stays in contract for owner withdrawal
 */
contract DexArbitrageur is FlashLoanSimpleReceiverBase, Ownable {
    using SafeERC20 for IERC20;

    // ═══════════════════════════════════════════════════════════════════
    // CUSTOM ERRORS — Gas-efficient revert reasons (saves ~200 gas each)
    // ═══════════════════════════════════════════════════════════════════
    error NotProfitable(uint256 finalBalance, uint256 requiredRepayment);
    error SwapFailed(address router);
    error Unauthorized();
    error ZeroAmount();
    error ZeroAddress();

    // ═══════════════════════════════════════════════════════════════════
    // EVENTS
    // ═══════════════════════════════════════════════════════════════════
    event ArbitrageExecuted(
        address indexed asset,
        uint256 amount,
        uint256 profit,
        address routerA,
        address routerB
    );
    event EmergencyWithdrawal(address indexed token, uint256 amount);

    // ═══════════════════════════════════════════════════════════════════
    // DATA STRUCTURES
    // ═══════════════════════════════════════════════════════════════════

    /**
     * @notice Parameters passed through flashloan for atomic execution
     * @param routerA   DEX router for the first swap  (tokenX → tokenY)
     * @param dataA     Encoded swap calldata for Router A
     * @param routerB   DEX router for the reverse swap (tokenY → tokenX)
     * @param dataB     Encoded swap calldata for Router B
     * @param tokenIntermediate  The intermediate token (tokenY) address
     */
    struct ArbParams {
        address routerA;
        bytes   dataA;
        address routerB;
        bytes   dataB;
        address tokenIntermediate;
    }

    // ═══════════════════════════════════════════════════════════════════
    // CONSTRUCTOR
    // ═══════════════════════════════════════════════════════════════════

    constructor(address _addressProvider)
        FlashLoanSimpleReceiverBase(IPoolAddressesProvider(_addressProvider))
    {}

    // ═══════════════════════════════════════════════════════════════════
    // EXTERNAL — Owner Entry Point
    // ═══════════════════════════════════════════════════════════════════

    /**
     * @notice Initiates a flashloan to exploit an arbitrage opportunity
     * @param _token   The asset to borrow (e.g., USDC or WETH)
     * @param _amount  Amount to borrow in base units
     * @param _params  ABI-encoded ArbParams struct
     */
    function requestFlashLoan(
        address _token,
        uint256 _amount,
        bytes calldata _params
    ) external onlyOwner {
        if (_amount == 0) revert ZeroAmount();
        if (_token == address(0)) revert ZeroAddress();

        POOL.flashLoanSimple(
            address(this),
            _token,
            _amount,
            _params,
            0 // referralCode
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    // INTERNAL — Aave V3 Callback
    // ═══════════════════════════════════════════════════════════════════

    /**
     * @notice Callback executed by Aave V3 Pool after flashloan is granted
     * @dev Uses Block Scoping { } to prevent "Stack too deep" errors
     *      Uses type(uint256).max for deadline params to save gas
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        // ── Security Gate ──
        if (msg.sender != address(POOL)) revert Unauthorized();
        if (initiator != address(this)) revert Unauthorized();

        // ── Block Scope: Decode & Execute Swaps ──
        // Scoped to free stack slots after swaps complete
        {
            ArbParams memory arb = abi.decode(params, (ArbParams));

            // ── SWAP A: Borrowed Asset → Intermediate Token ──
            {
                // Approve Router A to spend the borrowed amount
                IERC20(asset).safeApprove(arb.routerA, 0);
                IERC20(asset).safeApprove(arb.routerA, amount);

                (bool successA, ) = arb.routerA.call(arb.dataA);
                if (!successA) revert SwapFailed(arb.routerA);
            }

            // ── SWAP B: Intermediate Token → Borrowed Asset ──
            {
                uint256 intermediateBalance = IERC20(arb.tokenIntermediate)
                    .balanceOf(address(this));

                // Approve Router B to spend intermediate tokens
                IERC20(arb.tokenIntermediate).safeApprove(arb.routerB, 0);
                IERC20(arb.tokenIntermediate).safeApprove(
                    arb.routerB,
                    intermediateBalance
                );

                (bool successB, ) = arb.routerB.call(arb.dataB);
                if (!successB) revert SwapFailed(arb.routerB);
            }

            // ── Emit event with routers (still in scope of arb) ──
            uint256 _finalBal = IERC20(asset).balanceOf(address(this));
            uint256 _repay = amount + premium;
            if (_finalBal > _repay) {
                emit ArbitrageExecuted(
                    asset,
                    amount,
                    _finalBal - _repay,
                    arb.routerA,
                    arb.routerB
                );
            }
        }

        // ── Profitability Gate (outside swap scope) ──
        {
            uint256 amountToRepay = amount + premium;
            uint256 finalBalance = IERC20(asset).balanceOf(address(this));

            // STRICT: must be strictly greater — no break-even trades
            if (finalBalance <= amountToRepay) {
                revert NotProfitable(finalBalance, amountToRepay);
            }

            // Approve Aave Pool to pull the repayment
            IERC20(asset).safeApprove(address(POOL), 0);
            IERC20(asset).safeApprove(address(POOL), amountToRepay);
        }

        // Profit remains in contract — extract via emergencyWithdraw()
        return true;
    }

    // ═══════════════════════════════════════════════════════════════════
    // EMERGENCY — Owner-only rescue functions
    // ═══════════════════════════════════════════════════════════════════

    /**
     * @notice Rescue ERC20 tokens (profits or stuck tokens)
     * @param _token Address of the ERC20 token to withdraw
     */
    function emergencyWithdraw(address _token) external onlyOwner {
        if (_token == address(0)) revert ZeroAddress();
        uint256 balance = IERC20(_token).balanceOf(address(this));
        if (balance > 0) {
            IERC20(_token).safeTransfer(msg.sender, balance);
            emit EmergencyWithdrawal(_token, balance);
        }
    }

    /**
     * @notice Rescue native ETH from the contract
     */
    function emergencyWithdrawETH() external onlyOwner {
        uint256 balance = address(this).balance;
        if (balance > 0) {
            (bool sent, ) = payable(msg.sender).call{value: balance}("");
            if (!sent) revert SwapFailed(address(0));
            emit EmergencyWithdrawal(address(0), balance);
        }
    }

    // Allow contract to receive ETH
    receive() external payable {}
}