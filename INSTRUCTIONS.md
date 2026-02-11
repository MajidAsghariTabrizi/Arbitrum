# Aave V3 Liquidation Bot (Arbitrum)

This project contains a hybrid liquidation bot for Aave V3 on Arbitrum One. It consists of a Solidity smart contract for executing flash loan liquidations and a Python script for monitoring user health factors.

## Prerequisites

- Node.js (v16+)
- Python 3.8+
- [Alchemy](https://www.alchemy.com/) API Key (for Arbitrum Mainnet Fork)

## Setup

1.  **Install Node.js Dependencies:**
    ```bash
    npm install
    ```

2.  **Install Python Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment:**
    - Open `.env` file.
    - Set `ALCHEMY_API_KEY` to your Alchemy Arbitrum Mainnet API Key.
    - Set `PRIVATE_KEY` to your wallet private key (for deployment/mainnet usage).

## Running Tests (Simulation)

To verify the liquidation logic on a local mainnet fork:

```bash
npx hardhat test scripts/test_liquidation.js
```
*Note: This requires a valid `ALCHEMY_API_KEY` in `.env`.*

## Running the Bot locally (Integration)

Follow these steps to run the bot against a local Hardhat node with the contract deployed:

1.  **Start Local Node:**
    Open a terminal and run:
    ```bash
    npx hardhat node
    ```
    *Keep this terminal open.*

2.  **Deploy Contract:**
    Open a **second terminal** and run:
    ```bash
    npx hardhat run scripts/deploy.js --network localhost
    ```
    Copy the **deployed contract address** from the output (e.g., `FlashLoanLiquidator deployed to: 0x...`).

3.  **Update Bot Configuration:**
    - Open `bot.py`.
    - Find `LIQUIDATOR_ADDRESS` (around line 48).
    - Replace the placeholder with the address you just copied.
    
    *Alternatively, you can set it via environment variable:*
    ```bash
    $env:LIQUIDATOR_ADDRESS="0xYourContractAddress"
    ```

4.  **Run the Bot:**
    In the second terminal, run:
    ```bash
    python bot.py
    ```
    The bot will connect to `http://127.0.0.1:8545` and start monitoring.

5.  **Simulate Activity (Optional):**
    To see the bot in action, you can use Hardhat console to manipulate user health factors on the running node:
    ```bash
    npx hardhat console --network localhost
    ```

## Project Structure

- `contracts/FlashLoanLiquidator.sol`: Main liquidation logic.
- `contracts/MockOracle.sol`: Mock oracle for testing price manipulation.
- `scripts/test_liquidation.js`: Simulation script.
- `scripts/deploy.js`: Deployment script.
- `bot.py`: Python monitoring bot.
- `hardhat.config.js`: Hardhat configuration.
