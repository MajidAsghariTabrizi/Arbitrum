import os
import time
import json
from web3 import Web3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
RPC_URL = "http://127.0.0.1:8545" # Local Hardhat Node
web3 = Web3(Web3.HTTPProvider(RPC_URL))

if not web3.is_connected():
    raise Exception("Failed to connect to Ethereum node")

print(f"Connected to node: {RPC_URL}")

# Addresses (Arbitrum One)
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# Load Contract ABIs
# Ideally, load these from artifacts/contracts/... if available. 
# For simplicity, we define minimal ABIs here.

POOL_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase", "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase", "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase", "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv", "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# This will be loaded from the deployed contract address later
# For now, put a placeholder or load from a file if we had the deployment address.
# We will assume the user provides the deployed liquidator address in an env var or we hardcode it after deployment.
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS", "0xF70A22d7e521e25BA2f37cb8404b659C101b3f69")

LIQUIDATOR_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "_userToLiquidate", "type": "address"},
            {"internalType": "address", "name": "_debtAsset", "type": "address"},
            {"internalType": "address", "name": "_collateralAsset", "type": "address"},
            {"internalType": "uint256", "name": "_debtAmount", "type": "uint256"}
        ],
        "name": "requestFlashLoan",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

# Initialize Contracts
pool_contract = web3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
liquidator_contract = web3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)

# Target Users to Monitor
TARGET_USERS = [
    "0x0000000000000000000000000000000000000000", # Replace with actual target
]

def check_health_factor(user_address):
    try:
        data = pool_contract.functions.getUserAccountData(user_address).call()
        health_factor = data[5]
        # Health Factor is with 18 decimals. < 1.0 means < 1e18
        hf_human = health_factor / 1e18
        print(f"User {user_address} Health Factor: {hf_human}")
        return health_factor
    except Exception as e:
        print(f"Error checking user {user_address}: {e}")
        return None

def trigger_liquidation(user_address, debt_asset, collateral_asset, debt_amount):
    print(f"Attempting to liquidate {user_address}...")
    
    # We need a signer account (e.g. the first account from Hardhat node)
    # This requires running 'hardhat node' or fork with accounts unlocked
    account = web3.eth.accounts[0] 
    
    # Execute Transaction
    tx_hash = liquidator_contract.functions.requestFlashLoan(
        user_address,
        debt_asset,
        collateral_asset,
        debt_amount
    ).transact({'from': account})
    
    print(f"Liquidation Tx sent: {tx_hash.hex()}")
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    print(f"Tx Mined. Status: {receipt.status}")

def run_bot():
    print("Starting Bot...")
    while True:
        for user in TARGET_USERS:
            hf = check_health_factor(user)
            if hf is not None and hf < 1e18:
                print(f"HEALTH FACTOR < 1.0! TRIGGERING LIQUIDATION FOR {user}")
                # Determine debt amount: typically 50% of the debt or max allowed
                # For this demo, we'll assume we liquidate a fixed amount or fetch it properly.
                amount_to_liquidate = 1000 * 10**6 
                
                try:
                    trigger_liquidation(user, USDC_ADDRESS, WETH_ADDRESS, amount_to_liquidate)
                except Exception as e:
                    print(f"Liquidation failed: {e}")
                    
        time.sleep(5) # Poll every 5 seconds

if __name__ == "__main__":
    run_bot()
