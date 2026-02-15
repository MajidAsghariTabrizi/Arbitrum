import os
import json
import asyncio
import logging
import re
import time
import warnings
from decimal import Decimal
import aiofiles
import requests
from web3 import AsyncWeb3
from dotenv import load_dotenv

# Suppress ResourceWarning for cleaner logs
warnings.filterwarnings("ignore", category=ResourceWarning)

# --- 1. CONFIGURATION & SETUP ---

# Load Environment Variables
ENV_PATH = "/root/Arbitrum/.env"
if not os.path.exists(ENV_PATH):
    ENV_PATH = ".env"
load_dotenv(ENV_PATH)

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("WSS_Sniper")

# Database & Notification Setup
try:
    import db_manager
    DB_ENABLED = True
except ImportError:
    DB_ENABLED = False
    logger.warning("⚠️ db_manager.py not found. Dashboard logging disabled.")

# Configuration Constants
WSS_URL = os.getenv("WSS_URL")
if not WSS_URL:
    # Fallback to HTTP if WSS not set, but warn heavily
    logger.warning("⚠️ WSS_URL not found! Using PRIMARY_RPC (HTTP polling mode). Latency will be high.")
    WSS_URL = os.getenv("PRIMARY_RPC")

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not PRIVATE_KEY or not LIQUIDATOR_ADDRESS:
    logger.error("❌ Critical Error: Missing PRIVATE_KEY or LIQUIDATOR_ADDRESS in .env")
    exit(1)

# Arbitrum One Addresses (EIP-55 Checksummed)
POOL_ADDRESS = AsyncWeb3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
POOL_ADDRESSES_PROVIDER = AsyncWeb3.to_checksum_address("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb")
DATA_PROVIDER_ADDRESS = AsyncWeb3.to_checksum_address("0x69fa688f1dc47d4b5d8029d5a35fb7a548310654")
QUOTER_V2_ADDRESS = AsyncWeb3.to_checksum_address("0x61fFE014bA17989E743c5F6cB21bF9697530B21e")

# ABIs (Condensed for brevity but functional)
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
}, {
    "inputs": [],
    "name": "getReservesList",
    "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
    "stateMutability": "view",
    "type": "function"
}]

DATA_PROVIDER_ABI = [{
    "inputs": [
        {"internalType": "address", "name": "asset", "type": "address"},
        {"internalType": "address", "name": "user", "type": "address"}
    ],
    "name": "getUserReserveData",
    "outputs": [
        {"internalType": "uint256", "name": "currentATokenBalance", "type": "uint256"},
        {"internalType": "uint256", "name": "currentStableDebt", "type": "uint256"},
        {"internalType": "uint256", "name": "currentVariableDebt", "type": "uint256"},
        {"internalType": "uint256", "name": "principalStableDebt", "type": "uint256"},
        {"internalType": "uint256", "name": "scaledVariableDebt", "type": "uint256"},
        {"internalType": "uint256", "name": "stableBorrowRate", "type": "uint256"},
        {"internalType": "uint256", "name": "liquidityRate", "type": "uint256"},
        {"internalType": "uint40", "name": "stableRateLastUpdated", "type": "uint40"},
        {"internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool"}
    ],
    "stateMutability": "view",
    "type": "function"
}]

ORACLE_ABI = [{
    "inputs": [{"internalType": "address[]", "name": "assets", "type": "address[]"}],
    "name": "getAssetsPrices",
    "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
    "stateMutability": "view",
    "type": "function"
}]

ADDRESSES_PROVIDER_ABI = [{
    "inputs": [],
    "name": "getPriceOracle",
    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
    "stateMutability": "view",
    "type": "function"
}]

LIQUIDATOR_ABI = [{
    "inputs": [
        {"internalType": "address", "name": "_userToLiquidate", "type": "address"},
        {"internalType": "address", "name": "_debtAsset", "type": "address"},
        {"internalType": "address", "name": "_collateralAsset", "type": "address"},
        {"internalType": "uint256", "name": "_debtAmount", "type": "uint256"},
        {"internalType": "uint24", "name": "_fee", "type": "uint24"},
        {"internalType": "uint256", "name": "_amountOutMinimum", "type": "uint256"},
        {"internalType": "uint160", "name": "_sqrtPriceLimitX96", "type": "uint160"}
    ],
    "name": "requestFlashLoan",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
}]

ERC20_ABI = [{
    "inputs": [],
    "name": "decimals",
    "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
    "stateMutability": "view",
    "type": "function"
}]


# --- 2. ASYNC BOT CLASS ---

class GravitySniperWSS:
    def __init__(self):
        # WSS Connection initialized in run() until connected
        self.w3 = None
        self.account = None
        
        # Contracts
        self.pool = None
        self.data_provider = None
        self.addresses_provider = None
        self.liquidator_contract = None
        self.oracle_contract = None
        
        # Data
        self.targets = []
        self.reserves_list = []
        self.asset_decimals = {} 
        self.prices = {}
        
        # Concurrency & State
        self.running = True
        self.semaphore = asyncio.Semaphore(5) # Max 5 concurrent checks per block
        self.nonce_lock = asyncio.Lock() # Nonce safety
        self._last_errors = {}
        
    async def init_connection(self):
        """Initializes AsyncWeb3 with WSS Provider."""
        logger.info(f"🔌 Connecting to WSS: {WSS_URL[:25]}...")
        
        if "http" in WSS_URL:
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(WSS_URL))
        else:
            # Standard AsyncWebsocketProvider
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncWebsocketProvider(WSS_URL))
            
        if not await self.w3.is_connected():
            raise ConnectionError("❌ Failed to connect to WSS/RPC")
            
        self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        logger.info(f"🔑 Loaded Liquidator: {self.account.address}")
        
        # Init Contracts
        self.pool = self.w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
        self.data_provider = self.w3.eth.contract(address=DATA_PROVIDER_ADDRESS, abi=DATA_PROVIDER_ABI)
        self.addresses_provider = self.w3.eth.contract(address=POOL_ADDRESSES_PROVIDER, abi=ADDRESSES_PROVIDER_ABI)
        self.liquidator_contract = self.w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)
        
        # Init Oracle
        oracle_addr = await self.addresses_provider.functions.getPriceOracle().call()
        self.oracle_contract = self.w3.eth.contract(address=oracle_addr, abi=ORACLE_ABI)
        
        # Load Reserves
        self.reserves_list = await self.pool.functions.getReservesList().call()
        logger.info(f"📚 Loaded {len(self.reserves_list)} market assets.")

    async def log_system(self, msg, level="info"):
        if level == "error": logger.error(msg)
        elif level == "warning": logger.warning(msg)
        else: logger.info(msg)
        
        if DB_ENABLED:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, db_manager.log_event, level, msg)
            
        if DISCORD_WEBHOOK and (level == "success" or level == "error"):
            await self.send_discord_alert(msg, level)

    async def send_discord_alert(self, msg, level):
        try:
            color = 0x00ff00 if level == "success" else 0xff0000
            payload = {"embeds": [{"title": "🦅 Gravity WSS Sniper", "description": msg, "color": color}]}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(DISCORD_WEBHOOK, json=payload))
        except Exception: pass

    async def send_telegram_alert(self, msg, is_error=False):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
        
        # Anti-spam
        if is_error:
            error_key = msg[:100]
            now = time.time()
            if error_key in self._last_errors and (now - self._last_errors[error_key]) < 300:
                return
            self._last_errors[error_key] = now

        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
        except Exception: pass

    async def update_prices(self):
        """Updates asset prices in bulk."""
        try:
            prices = await self.oracle_contract.functions.getAssetsPrices(self.reserves_list).call()
            for i, asset in enumerate(self.reserves_list):
                self.prices[asset] = prices[i]
        except Exception as e:
            logger.warning(f"Price update failed: {e}")

    async def get_decimals(self, token):
        if token in self.asset_decimals: return self.asset_decimals[token]
        try:
            checksum_token = self.w3.to_checksum_address(token)
            erc20 = self.w3.eth.contract(address=checksum_token, abi=ERC20_ABI)
            decimals = await erc20.functions.decimals().call()
            self.asset_decimals[token] = decimals
            return decimals
        except: return 18

    async def load_targets_async(self):
        """Reloads targets.json."""
        try:
            path = "/root/Arbitrum/targets.json" if os.path.exists("/root/Arbitrum") else "targets.json"
            async with aiofiles.open(path, mode='r') as f:
                content = await f.read()
                if content: self.targets = json.loads(content)
        except: self.targets = []

    async def analyze_user_assets(self, user):
        """Finds best debt and collateral for liquidatable user."""
        if not self.prices: await self.update_prices()

        best_debt = None
        best_collateral = None
        max_debt_value = Decimal(0)
        max_collateral_value = Decimal(0)
        debt_amount_raw = 0

        # Create tasks
        tasks = [self.data_provider.functions.getUserReserveData(asset, user).call() for asset in self.reserves_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            if isinstance(res, Exception): continue
            
            asset = self.reserves_list[i]
            price = self.prices.get(asset, 0)
            if price == 0: continue

            # Collateral (Index 0)
            collateral_bal = res[0]
            if collateral_bal > 0:
                decimals = await self.get_decimals(asset)
                value_usd = (Decimal(collateral_bal) / Decimal(10**decimals)) * (Decimal(price) / Decimal(10**8))
                if value_usd > max_collateral_value:
                    max_collateral_value = value_usd
                    best_collateral = asset

            # Debt (Variable Index 2)
            variable_debt = res[2]
            if variable_debt > 0:
                decimals = await self.get_decimals(asset)
                value_usd = (Decimal(variable_debt) / Decimal(10**decimals)) * (Decimal(price) / Decimal(10**8))
                if value_usd > max_debt_value:
                    max_debt_value = value_usd
                    best_debt = asset
                    debt_amount_raw = variable_debt

        return best_debt, best_collateral, debt_amount_raw, max_debt_value

    async def execute_liquidation(self, user):
        """Builds and sends Flash Loan execution."""
        debt_asset, collateral_asset, debt_amount, debt_val = await self.analyze_user_assets(user)
        
        if not debt_asset or not collateral_asset or debt_val < 50:
            return

        # V1 HIGH-LIQUIDITY SAFETY: Only target USDC/WETH pairs if possible for stability
        # But for now, trust analysis logic.
        logger.info(f"⚔️ SNIPING: {user} | Debt: ${debt_val:.2f}")

        # Params
        fee = 3000
        amount_out_min = 0 # TODO: Slippage calc
        sqrt_price_limit = 0
        
        # Build TX Function
        tx_func = self.liquidator_contract.functions.requestFlashLoan(
            user, debt_asset, collateral_asset, int(debt_amount), fee, int(amount_out_min), int(sqrt_price_limit)
        )
        
        try:
            # NONCE MANAGEMENT with LOCK
            # Prevents multiple transactions in same block from re-using nonce
            async with self.nonce_lock:
                nonce = await self.w3.eth.get_transaction_count(self.account.address)
                
                # Gas Estimation
                gas_est = await tx_func.estimate_gas({'from': self.account.address})
                gas_limit = int(gas_est * 1.2)
                
                # EIP-1559 Fees
                block = await self.w3.eth.get_block('latest')
                base_fee = block['baseFeePerGas']
                priority = self.w3.to_wei(0.5, 'gwei') # Moderate priority for WSS
                max_fee = base_fee + priority
                
                tx = await tx_func.build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'maxFeePerGas': max_fee,
                    'maxPriorityFeePerGas': priority,
                    'gas': gas_limit,
                    'chainId': 42161
                })
                
                signed = self.account.sign_transaction(tx)
                tx_hash = await self.w3.eth.send_raw_transaction(signed.rawTransaction)
            
            await self.log_system(f"🔥 SENT: {tx_hash.hex()}", "success")
            await self.send_telegram_alert(f"🚀 <b>Liquidation Sent:</b> {user}\nTX: {tx_hash.hex()}")
            
        except Exception as e:
            await self.log_system(f"Execution Failed: {e}", "error")

    async def check_user_health(self, user):
        """Concurrent health check protected by Semaphore."""
        async with self.semaphore:
            try:
                data = await self.pool.functions.getUserAccountData(user).call()
                # hf is index 5, 1e18 scale
                hf = Decimal(data[5]) / Decimal(10**18)
                
                if hf < 1.05 and hf > 0: # Warning zone
                    if hf < 1.0:
                        logger.info(f"💀 LIQUIDATABLE: {user} (HF: {hf})")
                        await self.execute_liquidation(user)
                    else:
                        # Just log low HF for debugging/monitoring
                        pass 
            except Exception:
                pass

    async def on_new_block(self, block_header):
        """Event Handler: Triggered on every new WSS block header."""
        start_time = time.time()
        
        # 1. Update Prices first (critical for accurate liquidation calc)
        await self.update_prices()
        
        # 2. Reload targets (async fast read)
        await self.load_targets_async()
        if not self.targets: return

        # 3. Concurrent Health Checks
        # We spawn tasks for all targets, but semaphore limits active WSS requests
        tasks = [self.check_user_health(user) for user in self.targets]
        await asyncio.gather(*tasks)
        
        elapsed = (time.time() - start_time) * 1000
        logger.info(f"🧱 Block {block_header['number']} processed. {len(self.targets)} targets in {elapsed:.0f}ms")

    async def run_forever(self):
        """Main WSS Loop with Reconnection (Manual implementation)."""
        while True:
            try:
                await self.init_connection()
                await self.send_telegram_alert("🟢 <b>WSS Sniper Started</b>")
                
                # Subscribe to newHeads
                subscription_id = await self.w3.eth.subscribe('newHeads')
                logger.info(f"✅ Subscribed to newHeads (ID: {subscription_id})")

                # Listen for events - manual iterator for older web3 compatibility
                # For newer web3 (6.x+), use process_subscriptions or manual recv
                async for response in self.w3.socket.process_subscriptions():
                    try:
                        header = response.get('result', {})
                        # Some providers wrap differently; handle robustly
                        if not header and 'params' in response:
                            header = response['params'].get('result', {})

                        if 'number' in header:
                            # Parse hex block number if needed
                            if isinstance(header['number'], str):
                                header['number'] = int(header['number'], 16)
                            await self.on_new_block(header)
                    except Exception as loop_err:
                        logger.error(f"Event Loop Error: {loop_err}")

            except Exception as e:
                logger.error(f"🔌 WSS Connection Lost: {e}")
                await self.send_telegram_alert(f"⚠️ <b>WSS Disconnected:</b> {e}", is_error=True)
                await asyncio.sleep(5) # Reconnect delay

if __name__ == "__main__":
    bot = GravitySniperWSS()
    try:
        asyncio.run(bot.run_forever())
    except KeyboardInterrupt:
        print("\n🛑 Bot Stopped.")