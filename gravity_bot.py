import asyncio
import logging
import os
import json
import time
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from dotenv import load_dotenv
import db_manager

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ExecutionEngine")

# Load Environment
load_dotenv()

# Configuration
RPC_URL = os.getenv("RPC_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")

# Arbitrum Mainnet Addresses (Aave V3)
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# Hardcoded Fallbacks (Whales)
FALLBACK_TARGETS = [
    "0x99525208453488C9518001712C7F72428514197F",
    "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb",
    "0xF977814e90dA44bFA03b6295A0616a897441aceC",
    "0x4a923335FDD029841103F647065094247290A7a2"
]

# ABIs
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

class ExecutionEngine:
    def __init__(self):
        self.w3 = None
        self.pool = None
        self.liquidator = None
        self.account = None
        self.check_counter = 0

    async def connect(self):
        if not RPC_URL or not PRIVATE_KEY or not LIQUIDATOR_ADDRESS:
            logger.error("❌ Missing .env configuration (RPC_URL, PRIVATE_KEY, or LIQUIDATOR_ADDRESS)")
            db_manager.log_event("ERROR", "Missing .env configuration")
            return False
        
        try:
            self.w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL))
            if not await self.w3.is_connected():
                logger.error("❌ Failed to connect to Arbitrum RPC")
                db_manager.log_event("ERROR", "Failed to connect to Arbitrum RPC")
                return False
            
            self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
            self.pool = self.w3.eth.contract(address=self.w3.to_checksum_address(POOL_ADDRESS), abi=POOL_ABI)
            self.liquidator = self.w3.eth.contract(address=self.w3.to_checksum_address(LIQUIDATOR_ADDRESS), abi=LIQUIDATOR_ABI)
            
            logger.info(f"✅ Connection Established | Account: {self.account.address}")
            db_manager.log_event("INFO", f"Connection Established | Account: {self.account.address}")
            logger.info(f"📜 Liquidator: {LIQUIDATOR_ADDRESS}")
            return True
        except Exception as e:
            logger.error(f"❌ Initialization Error: {e}")
            return False

    def hot_reload_targets(self):
        """Read targets.json with graceful JSON error handling"""
        try:
            if os.path.exists('targets.json'):
                with open('targets.json', 'r') as f:
                    targets = json.load(f)
                    if targets and isinstance(targets, list):
                        return list(set([self.w3.to_checksum_address(t) for t in targets if t]))
            
            logger.warning("⚠️ targets.json empty. Using Fallback Whales.")
            return [self.w3.to_checksum_address(t) for t in FALLBACK_TARGETS]
        except (json.JSONDecodeError, PermissionError):
            # Scanner might be writing; skip this read and return safe list
            return [self.w3.to_checksum_address(t) for t in FALLBACK_TARGETS]
        except Exception as e:
            logger.error(f"⚠️ Load Error: {e}")
            return [self.w3.to_checksum_address(t) for t in FALLBACK_TARGETS]

    async def check_health(self, user):
        """Returns health factor and total debt from Aave"""
        try:
            data = await self.pool.functions.getUserAccountData(user).call()
            hf = data[5] / 1e18
            debt = data[1] # USD value with 8 decimals
            return hf, debt
        except Exception:
            return 99.0, 0

    async def execute_liquidation(self, user, debt_base):
        """Trigger Flash Loan on the smart contract"""
        try:
            # We liquidate 50% of the debt
            # debt_base is in USD (8 decimals). USDC on Arbitrum is 6 decimals.
            # Approx Conversion: (Debt / 10^8) * 0.5 * 10^6 
            amount_to_liquidate = int((debt_base / 10**8) * 0.5 * 10**6)
            
            if amount_to_liquidate < 1:
                return False

            logger.warning(f"💥 LIQUIDATING USER: {user} | Dept Amount: {amount_to_liquidate/1e6:.2f} USDC")
            db_manager.log_event("WARNING", f"LIQUIDATING USER: {user} | Dept: {amount_to_liquidate/1e6:.2f} USDC")
            
            nonce = await self.w3.eth.get_transaction_count(self.account.address)
            gas_price = await self.w3.eth.gas_price
            
            # 20% premium on gas for high-speed execution
            fast_gas = int(gas_price * 1.2)
            
            tx = await self.liquidator.functions.requestFlashLoan(
                user,
                self.w3.to_checksum_address(USDC_ADDRESS),
                self.w3.to_checksum_address(WETH_ADDRESS),
                amount_to_liquidate
            ).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': 2000000,
                'gasPrice': fast_gas
            })
            
            signed_tx = self.account.sign_transaction(tx)
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            logger.info(f" Transaction Broadcasted: {tx_hash.hex()}")
            db_manager.log_event("INFO", f"Transaction Broadcasted: {tx_hash.hex()}")
            
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt.status == 1:
                logger.info(f"✅ LIQUIDATION CONFIRMED | Block: {receipt.blockNumber}")
                db_manager.log_event("INFO", f"LIQUIDATION CONFIRMED | Block: {receipt.blockNumber}")
                
                # Record Execution
                db_manager.record_execution(
                    tx_hash.hex(),
                    user,
                    USDC_ADDRESS, # Debt
                    WETH_ADDRESS, # Collateral (Assuming WETH for now as per logic)
                    0.0, # Eth Profit (Needs calculation or logic update to fetch actuals)
                    ((amount_to_liquidate * 0.05) / 1e6) # Est Profit (5% bonus approx)
                )
                return True
            else:
                logger.error("❌ LIQUIDATION REVERTED")
                db_manager.log_event("ERROR", "LIQUIDATION REVERTED")
                return False
                
        except Exception as e:
            logger.error(f"❌ Execution Failure: {e}")
            db_manager.log_event("ERROR", f"Execution Failure: {e}")
            return False

    async def monitor(self):
        logger.info("⚡ Symphony Engine Started. Monitoring Live...")
        db_manager.log_event("INFO", "Symphony Engine Started")
        
        while True:
            try:
                # 1. DYNAMIC HOT RELOAD
                targets = self.hot_reload_targets()
                logger.info(f"🎯 Loaded {len(targets)} targets from scanner...")
                db_manager.log_event("INFO", f"Loaded {len(targets)} targets")
                
                # 2. PRECISION HEALTH CHECK
                for user in targets:
                    self.check_counter += 1
                    hf, debt = await self.check_health(user)
                    
                    # Log every 50 users or if danger
                    if self.check_counter % 50 == 0 or hf < 1.05:
                        status = "Safe" if hf >= 1.0 else "DANGER"
                        logger.info(f"Checking User: {user[:10]}... | HF: {hf:.2f} ({status})")
                    
                    # 3. THE TRIGGER
                    if 0 < hf < 1.0:
                        await self.execute_liquidation(user, debt)
                        # Avoid double-hitting same user in rapid bursts
                        await asyncio.sleep(2)
                
                # Short rest after complete list scan
                await asyncio.sleep(5)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"🔄 Loop Error: {e}")
                await asyncio.sleep(5)

async def main():
    engine = ExecutionEngine()
    if await engine.connect():
        await engine.monitor()

if __name__ == "__main__":
    asyncio.run(main())
