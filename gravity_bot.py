
import os
import json
import asyncio
import logging
import re
import datetime
from decimal import Decimal

import aiofiles
import requests
from web3 import AsyncWeb3
from web3.exceptions import BlockNotFound, ContractLogicError
from dotenv import load_dotenv

# --- 1. CONFIGURATION & SETUP ---

# Load Environment Variables
ENV_PATH = "/root/Arbitrum/.env"
# Fallback for local testing if /root path doesn't exist
if not os.path.exists(ENV_PATH):
    ENV_PATH = ".env"
load_dotenv(ENV_PATH)

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("GravityBot")

# Database & Notification Setup
try:
    import db_manager
    DB_ENABLED = True
except ImportError:
    DB_ENABLED = False
    logger.warning("⚠️ db_manager.py not found. Dashboard logging disabled.")

# Configuration Constants
RPC_URL = os.getenv("RPC_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

if not RPC_URL or not PRIVATE_KEY:
    logger.error("❌ Critical Error: Missing RPC_URL or PRIVATE_KEY")
    exit(1)

# Aave V3 Pool Address (Arbitrum)
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# Concurrency & Rate Limiting
MAX_CONCURRENT_REQUESTS = 15  # Matches your RPC plan
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ABI Definition
POOL_ABI = [{
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
}]

# --- 2. ASYNC BOT CLASS ---

class AdaptiveSniperBot:
    def __init__(self):
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
        self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        self.pool_contract = self.w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
        self.targets = []
        self.running = True
        
        # Regular expression to extract wait time from RPC errors
        # Example: "try_again_in": "518.715ms"
        self.retry_regex = re.compile(r"try_again_in['\"]?:\s*['\"]?([\d\.]+)ms")

    async def log_system(self, msg, level="info"):
        """Async logger that pushes to Console, DB, and Discord."""
        # 1. Console
        if level == "error":
            logger.error(msg)
        elif level == "warning":
            logger.warning(msg)
        else:
            logger.info(msg)

        # 2. Database (Non-blocking via thread executor if DB is synchronous)
        if DB_ENABLED:
            # db_manager is likely synchronous sqlite3, so run in executor
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, db_manager.log_event, level, msg)

        # 3. Discord (Critical only)
        if DISCORD_WEBHOOK and (level == "success" or level == "error"):
            await self.send_discord_alert(msg, level)

    async def send_discord_alert(self, msg, level):
        """Sends Discord webhook asynchronously."""
        try:
            color = 0x00ff00 if level == "success" else 0xff0000
            payload = {
                "embeds": [{"title": "🦅 Adaptive Sniper Bot", "description": msg, "color": color}]
            }
            # Use aiohttp or just run requests in executor. 
            # For simplicity and to avoid adding dependencies, we'll use requests in executor.
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(DISCORD_WEBHOOK, json=payload))
        except Exception as e:
            logger.error(f"Discord Error: {e}")

    async def load_targets_async(self):
        """Reads targets.json asynchronously."""
        try:
            async with aiofiles.open("targets.json", "r") as f:
                content = await f.read()
                self.targets = json.loads(content)
        except FileNotFoundError:
            self.targets = []
        except Exception as e:
            await self.log_system(f"Error loading targets: {e}", "error")

    async def get_recommended_gas(self):
        """Calculates aggressive gas price (Base + Priority)."""
        try:
            block = await self.w3.eth.get_block('latest')
            base_fee = block['baseFeePerGas']
            
            # Aggressive Strategy: 
            # Priority Fee = 1.5x - 2.0x of network average (Using hardcoded high value for now)
            # Arbitrum standard is usually 0.1 gwei, we can go higher for sniping
            max_priority_fee = self.w3.to_wei(0.1, 'gwei') 
            
            # If network is congested (base_fee high), increase priority
            if base_fee > self.w3.to_wei(0.1, 'gwei'):
                 max_priority_fee = self.w3.to_wei(0.5, 'gwei')

            max_fee_per_gas = base_fee + max_priority_fee
            return max_fee_per_gas, max_priority_fee
        except Exception:
            # Fallback
            return None, None

    async def safe_rpc_call(self, coro):
        """
        Adaptive Backoff Wrapper.
        Retries on rate limit errors after parsing exact wait time.
        """
        while True:
            try:
                return await coro
            except Exception as e:
                error_str = str(e)
                if "-32005" in error_str: # Chainstack Rate Limit Code
                    match = self.retry_regex.search(error_str)
                    if match:
                        wait_ms = float(match.group(1))
                        wait_sec = (wait_ms / 1000.0) + 0.05 # Add buffer
                        # logger.warning(f"Rate Limited! Sleeping {wait_sec:.4f}s...")
                        await asyncio.sleep(wait_sec)
                        continue # Retry immediately after sleep
                    else:
                        # Could not parse time, default sleep
                        await asyncio.sleep(0.5)
                        continue
                else:
                    # Not a rate limit error, re-raise or return None
                    return None

    async def check_user_health(self, user):
        """Checks HF for a single user with concurrency control."""
        async with SEMAPHORE:
            hf_raw = await self.safe_rpc_call(
                self.pool_contract.functions.getUserAccountData(user).call()
            )
            
            if hf_raw:
                # HF is index 5
                hf = hf_raw[5] / 10**18
                return user, hf
            return user, None

    async def execute_flash_loan(self, user, debt_asset, collateral_asset, debt_amount):
        """
        Executes the flash loan with:
        1. Dynamic Gas
        2. Off-chain Swap Params
        """
        try:
            await self.log_system(f"🚀 ATTEMPTING LIQUIDATION: {user}", "warning")
            
            # 1. Gas Prep
            max_fee, priority_fee = await self.get_recommended_gas()
            
            # 2. Swap Params (Off-chain calculation placeholder)
            # In a real scenario, use Uniswap Quoter to find best pool fee and amounts
            # specific to this debt/collateral pair.
            fee = 3000 # 0.3%
            sqrt_price_limit_x96 = 0
            amount_out_min = 0 # TODO: Calculate slippage protection
            
            # 3. Build Transaction
            # Note: We need the Liquidator Contract instance here. 
            # Assuming ABI is loaded or known. For now, we'll use a placeholder structure
            # since the user didn't provide the Python side ABI for FlashLoanLiquidator.
            
            # We need the contract object for the Liquidator
            # liquidator_contract = self.w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)
            # tx = await liquidator_contract.functions.requestFlashLoan(...).build_transaction(...)
            
            msg = f"Simulating Flash Loan for {user}. Gas: {max_fee}"
            await self.log_system(msg, "success")
            
            # TODO: ACTUAL TRANSACTION SENDING LOGIC
            # When ready, uncomment and ensure ABI is available
            
        except Exception as e:
            await self.log_system(f"Execution Failed: {e}", "error")

    async def worker_loop(self):
        """Main monitoring loop."""
        await self.log_system(f"🦅 Adaptive Sniper Bot Started. Wallet: {self.account.address}", "info")
        
        last_target_refresh = 0
        
        while self.running:
            start_time = time.time()
            
            # 1. Refresh Targets every 5 seconds
            if start_time - last_target_refresh > 5:
                await self.load_targets_async()
                last_target_refresh = start_time
                print(f"🎯 Tracking {len(self.targets)} targets...", end="\r")

            if not self.targets:
                await asyncio.sleep(1)
                continue

            # 2. Create tasks for all targets
            tasks = [self.check_user_health(user) for user in self.targets]
            results = await asyncio.gather(*tasks)

            # 3. Process Results
            for user, hf in results:
                if hf is None: 
                    continue
                
                if hf < 1.0:
                    msg = f"💥 LIQUIDATING USER: {user} | HF: {hf:.4f}"
                    await self.log_system(msg, "success")
                    
                    # Fire Flash Loan!
                    # Example assets (USDC debt, WETH collateral) - logic to determine best pair needed
                    # await self.execute_flash_loan(user, USDC_ADDRESS, WETH_ADDRESS, debt_amount)
                    
                elif hf < 1.05:
                    # Log silently or to console only to save DB space
                    # await self.log_system(f"⚠️ Risky User: {user} | HF: {hf:.4f}", "warning")
                    pass

            # 4. Loop pacing (optional, since semaphore limits rate)
            # If we processed too fast, maybe sleep a tiny bit?
            # With 15 concurrent reqs, we might not need extra sleep if list is long.
            elapsed = time.time() - start_time
            if elapsed < 0.1:
                await asyncio.sleep(0.1)

    def run(self):
        try:
            asyncio.run(self.worker_loop())
        except KeyboardInterrupt:
            print("\n🛑 Bot Stopped.")

if __name__ == "__main__":
    bot = AdaptiveSniperBot()
    bot.run()