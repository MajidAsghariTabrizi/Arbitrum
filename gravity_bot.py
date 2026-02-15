import os
import json
import asyncio
import logging
import re
import time
from decimal import Decimal
import aiofiles
import requests
from web3 import AsyncWeb3
from dotenv import load_dotenv

# --- 1. CONFIGURATION & SETUP ---

# Load Environment Variables
ENV_PATH = "/root/Arbitrum/.env"
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
PRIMARY_RPC = os.getenv("PRIMARY_RPC")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not PRIMARY_RPC or not PRIVATE_KEY:
    logger.error("❌ Critical Error: Missing PRIMARY_RPC or PRIVATE_KEY")
    exit(1)


# --- 2. ASYNC RPC MANAGER ---
class AsyncRPCManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.primary_rpc = os.getenv("PRIMARY_RPC")
        self.fallback_rpcs = os.getenv("FALLBACK_RPCS", "").split(",")
        self.fallback_rpcs = [url.strip() for url in self.fallback_rpcs if url.strip()]
        
        # Validation
        if not self.primary_rpc:
            logger.error("❌ PRIMARY_RPC not found in .env")
            exit(1)

        self.active_rpc_index = -1 # -1 = Primary
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.primary_rpc, request_kwargs={'timeout': 60}))
        self.prober_running = False
        
        # Adaptive Rate Limiting
        self.rpc_delay = 0.1
        self.consecutive_errors = 0

    async def start_background_prober(self):
        """Background task: Pings Primary RPC every 30s if we are on Fallback."""
        if self.prober_running: return
        self.prober_running = True
        logger.info("🕵️ RPC Prober Task Started")
        
        while True:
            await asyncio.sleep(30)
            if self.active_rpc_index != -1: # Only probe if on fallback
                try:
                    # Probe Primary
                    temp_w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.primary_rpc, request_kwargs={'timeout': 10}))
                    await temp_w3.eth.block_number
                    
                    # Success! Revert
                    logger.info("🟢 Primary RPC Alive! Switching back...")
                    self.active_rpc_index = -1
                    self.w3 = temp_w3
                    self.rpc_delay = 0.1
                    self.consecutive_errors = 0
                    
                    # Re-init contracts linked to new w3
                    await self.bot.reinit_contracts()
                    
                    await self.bot.send_telegram_alert("🟢 <b>Primary RPC Restored.</b> Bot switched back to main node.")
                except Exception:
                    pass # Still down

    async def handle_failure(self):
        """Switches to next fallback on failure."""
        # Try next fallback
        next_idx = self.active_rpc_index + 1
        if next_idx < len(self.fallback_rpcs):
            new_url = self.fallback_rpcs[next_idx]
            self.active_rpc_index = next_idx
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(new_url, request_kwargs={'timeout': 60}))
            
            # Strict Fallback Throttling
            self.rpc_delay = 0.5
            self.consecutive_errors = 0
            
            logger.warning(f"⚠️ RPC Failure. Switching to Fallback #{next_idx + 1}: {new_url}")
            await self.bot.reinit_contracts()
            
            await self.bot.send_telegram_alert(
                f"⚠️ <b>Primary RPC Failed.</b> Switching to Fallback #{next_idx + 1}.",
                is_error=True
            )
            return True
        else:
            # Exhausted all? Reset to Primary to retry
            logger.error("❌ All RPCs exhausted. Sleeping 30s then Resetting to Primary.")
            await asyncio.sleep(30)
            
            self.active_rpc_index = -1
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.primary_rpc, request_kwargs={'timeout': 60}))
            self.rpc_delay = 0.1
            self.consecutive_errors = 0
            
            await self.bot.reinit_contracts()
            return False
            
    async def call(self, coro_func, *args, **kwargs):
        """Async Wrapper with Adaptive Rate Limiting & 3-Strike Rule."""
        # Enforce Delay
        delay = 0.5 if self.active_rpc_index >= 0 else self.rpc_delay
        await asyncio.sleep(delay)
        
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            
            # Adaptive Backoff (CRITICAL HOTFIX: 30s Wait)
            if "429" in error_str or "403" in error_str or "too many requests" in error_str:
                self.consecutive_errors += 1
                logger.warning(f"⚠️ Rate Limit Hit (Strike {self.consecutive_errors}/3). CAUTION: Cooling down for 30s...")
                await asyncio.sleep(30) # <--- CRITICAL UPDATE: 30s wait
                
                if self.active_rpc_index == -1:
                    self.rpc_delay += 0.1
                    logger.info(f"🐌 Increased Primary Delay to {self.rpc_delay:.2f}s")
                
                # 3-Strike Rule
                if self.consecutive_errors >= 3:
                    if await self.handle_failure():
                        # Retry on new node
                        return await self.call(coro_func, *args, **kwargs)
                    else:
                        raise e
                else:
                    return await self.call(coro_func, *args, **kwargs)
            else:
                 # Other errors
                self.consecutive_errors += 1
                if self.consecutive_errors >= 3:
                     if await self.handle_failure():
                        return await self.call(coro_func, *args, **kwargs)
                     else:
                        raise e
                raise e


# Arbitrum One Addresses (EIP-55 Checksummed)
POOL_ADDRESS = AsyncWeb3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
POOL_ADDRESSES_PROVIDER = AsyncWeb3.to_checksum_address("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb")
DATA_PROVIDER_ADDRESS = AsyncWeb3.to_checksum_address("0x69fa688f1dc47d4b5d8029d5a35fb7a548310654")
QUOTER_V2_ADDRESS = AsyncWeb3.to_checksum_address("0x61fFE014bA17989E743c5F6cB21bF9697530B21e") # Uniswap V3 Quoter V2

# Concurrency
MAX_CONCURRENT_REQUESTS = 15
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ABIs
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

QUOTER_ABI = [{
    "inputs": [{
        "components": [
            {"internalType": "address", "name": "tokenIn", "type": "address"},
            {"internalType": "address", "name": "tokenOut", "type": "address"},
            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
            {"internalType": "uint24", "name": "fee", "type": "uint24"},
            {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
        ],
        "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
        "name": "params",
        "type": "tuple"
    }],
    "name": "quoteExactInputSingle",
    "outputs": [
        {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
        {"internalType": "uint160", "name": "sqrtPriceX96After", "type": "uint160"},
        {"internalType": "uint32", "name": "initializedTicksCrossed", "type": "uint32"},
        {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"}
    ],
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

class AdaptiveSniperBot:
    def __init__(self):
        # Init RPC Manager
        self.rpc_manager = AsyncRPCManager(self)
        
        # Account
        self.account = self.rpc_manager.w3.eth.account.from_key(PRIVATE_KEY)
        logger.info(f"🔑 Loaded Liquidator: {self.account.address}")
        
        # Contracts (Initialized in init_infrastructure via reinit_contracts)
        self.pool = None
        self.data_provider = None
        self.addresses_provider = None
        self.quoter = None
        self.liquidator_contract = None
        self.oracle_contract = None
        
        self.targets = []
        self.reserves_list = []
        self.asset_decimals = {} # Cache for decimals
        self.prices = {} # Cache for prices
        self.running = True
        self._last_errors = {}
        self.retry_regex = re.compile(r"try_again_in['\"]?:\s*['\"]?([\d\.]+)ms")

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
            payload = {"embeds": [{"title": "🦅 Gravity Bot", "description": msg, "color": color}]}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(DISCORD_WEBHOOK, json=payload))
        except Exception: pass

    async def send_telegram_alert(self, msg, is_error=False):
        """Sends an HTML-formatted Telegram alert with anti-spam cooldown for errors."""
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return

        # Anti-spam: skip duplicate error alerts within cooldown period
        if is_error:
            error_key = msg[:100]
            now = time.time()
            if error_key in self._last_errors and (now - self._last_errors[error_key]) < 300:
                return  # Suppress duplicate
            self._last_errors[error_key] = now

        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
        except Exception as e:
            logger.warning(f"Telegram alert failed: {e}")

    async def reinit_contracts(self):
        """Called by RPC Manager when w3 instance changes."""
        w3 = self.rpc_manager.w3
        self.pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
        self.data_provider = w3.eth.contract(address=DATA_PROVIDER_ADDRESS, abi=DATA_PROVIDER_ABI)
        self.addresses_provider = w3.eth.contract(address=POOL_ADDRESSES_PROVIDER, abi=ADDRESSES_PROVIDER_ABI)
        self.quoter = w3.eth.contract(address=QUOTER_V2_ADDRESS, abi=QUOTER_ABI)
        
        if LIQUIDATOR_ADDRESS:
             self.liquidator_contract = w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)
        
        if self.oracle_contract:
            self.oracle_contract = w3.eth.contract(address=self.oracle_contract.address, abi=ORACLE_ABI)

    async def init_infrastructure(self):
        """Initializes Oracle and Reserve caches."""
        try:
            # Ensure contracts are loaded
            await self.reinit_contracts()

            # 1. Get Oracle Address
            oracle_address = await self.addresses_provider.functions.getPriceOracle().call()
            self.oracle_contract = self.rpc_manager.w3.eth.contract(address=oracle_address, abi=ORACLE_ABI)
            
            # 2. Get Reserves List
            self.reserves_list = await self.pool.functions.getReservesList().call()
            await self.log_system(f"Loaded {len(self.reserves_list)} market assets.", "info")
            
        except Exception as e:
            await self.log_system(f"Init Failed: {e}", "error")

    async def update_prices(self):
        """Updates asset prices in bulk."""
        try:
            # Wrapped call? Yes, use rpc_manager.call for critical calls
            prices = await self.rpc_manager.call(self.oracle_contract.functions.getAssetsPrices(self.reserves_list).call)
            
            for i, asset in enumerate(self.reserves_list):
                self.prices[asset] = prices[i]
                
        except Exception as e:
            logger.warning(f"Price update failed: {e}")

    async def get_decimals(self, token):
        if token in self.asset_decimals:
            return self.asset_decimals[token]
        try:
            checksum_token = self.rpc_manager.w3.to_checksum_address(token)
            erc20 = self.rpc_manager.w3.eth.contract(address=checksum_token, abi=ERC20_ABI)
            decimals = await self.rpc_manager.call(erc20.functions.decimals().call)
            self.asset_decimals[token] = decimals
            return decimals
        except Exception:
            return 18

    async def load_targets_async(self):
        """Reads targets.json asynchronously."""
        try:
            path = "/root/Arbitrum/targets.json" if os.path.exists("/root/Arbitrum") else "targets.json"
            async with aiofiles.open(path, mode='r') as f:
                content = await f.read()
                if content:
                    self.targets = json.loads(content)
                else:
                    self.targets = []
        except Exception as e:
            self.targets = []

    async def get_recommended_gas(self):
        """EIP-1559 Gas Sniper Strategy."""
        try:
            block = await self.rpc_manager.call(self.rpc_manager.w3.eth.get_block, 'latest')
            base_fee = block['baseFeePerGas']
            
            # 🚀 SNIPER MODE: 2x - 3x Priority Fee
            priority_fee = self.rpc_manager.w3.to_wei(0.5, 'gwei') 
            if base_fee > self.rpc_manager.w3.to_wei(0.1, 'gwei'):
                priority_fee = self.rpc_manager.w3.to_wei(1.5, 'gwei') # Very aggressive
                
            max_fee = base_fee + priority_fee
            return max_fee, priority_fee
        except:
            return None, None

    async def analyze_user_assets(self, user):
        """
        Dynamically identifies best assets.
        Note: We don't wrap every individual call in `gather` with `call()` because it complicates concurrency.
        We rely on SEMAPHORE. But if we need rate limiting, we should.
        Standard `call()` uses delay.
        """
        if not self.prices:
            await self.update_prices()

        best_debt = None
        best_collateral = None
        max_debt_value = Decimal(0)
        max_collateral_value = Decimal(0)
        debt_amount_raw = 0

        # Create tasks for all assets
        tasks = []
        for asset in self.reserves_list:
             # We invoke call() wrapper here to ensure rate limiting per request
             coro = self.rpc_manager.call(self.data_provider.functions.getUserReserveData(asset, user).call)
             tasks.append(coro)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            if isinstance(res, Exception):
                await self.log_system(f"⚠️ DataProvider error for asset {self.reserves_list[i]}: {res}", "warning")
                continue
            
            asset = self.reserves_list[i]
            price = self.prices.get(asset, 0)
            if price == 0: continue

            # DataProvider returns: 
            # 0: currentATokenBalance (Collateral)
            # 1: currentStableDebt
            # 2: currentVariableDebt
            
            # Collateral
            collateral_bal = res[0]
            if collateral_bal > 0:
                decimals = await self.get_decimals(asset)
                value_usd = (Decimal(collateral_bal) / Decimal(10**decimals)) * (Decimal(price) / Decimal(10**8))
                if value_usd > max_collateral_value:
                    max_collateral_value = value_usd
                    best_collateral = asset

            # Debt (Variable only for now)
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
        try:
            await self._execute_liquidation_inner(user)
        except Exception as e:
            await self.log_system(f"Liquidation Task Error for {user}: {e}", "error")
            await self.rpc_manager.handle_failure() # Attempt failover on error
            await self.send_telegram_alert(
                f"⚠️ <b>Liquidation Task Error</b> for <code>{user}</code>:\n<code>{e}</code>",
                is_error=True
            )

    async def _execute_liquidation_inner(self, user):
        debt_asset, collateral_asset, debt_amount, debt_val = await self.analyze_user_assets(user)
        
        if not debt_asset or not collateral_asset:
            return

        if debt_val < 50: # Ignore dust < $50
            return

        # Prepare Flash Loan Params
        fee = 3000 # 0.3% Uniswap fee tier
        amount_out_min = 0 # Slippage protection (TODO: calculate off-chain)
        sqrt_price_limit = 0 
        
        logger.info(f"⚔️ ATTEMPTING LIQUIDATION: {user} | Debt: ${debt_val} | Asset: {debt_asset}")
        
        # Build TX
        tx_func = self.liquidator_contract.functions.requestFlashLoan(
            user, debt_asset, collateral_asset, int(debt_amount), fee, int(amount_out_min), int(sqrt_price_limit)
        )
        
        start_time = time.time()
        
        # Gas War Strategy
        max_fee, priority_fee = await self.get_recommended_gas()
        if not max_fee:
             logger.error("Failed to estimate gas. Aborting.")
             return
             
        gas_est = 2500000 # Hardcoded safe limit for strict timing

        tx = await tx_func.build_transaction({
            'from': self.account.address,
            'nonce': await self.rpc_manager.call(self.rpc_manager.w3.eth.get_transaction_count, self.account.address),
            'maxFeePerGas': max_fee,
            'maxPriorityFeePerGas': priority_fee,
            'gas': int(gas_est * 1.2), # Buffer
            'chainId': 42161 # Arbitrum One
        })
        
        # Sign
        signed_tx = self.rpc_manager.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        
        # SEND !
        tx_hash = await self.rpc_manager.call(self.rpc_manager.w3.eth.send_raw_transaction, signed_tx.rawTransaction)
        await self.log_system(f"🔥 TX SENT: {tx_hash.hex()}", "success")
        
        # Monitor TX receipt
        try:
            receipt = await self.rpc_manager.call(self.rpc_manager.w3.eth.wait_for_transaction_receipt, tx_hash, timeout=30)
            gas_used = receipt['gasUsed']
            effective_gas_price = receipt['effectiveGasPrice']
            gas_cost_eth = Decimal(gas_used * effective_gas_price) / Decimal(10**18)
            arbiscan_link = f"https://arbiscan.io/tx/{tx_hash.hex()}"
            
            if receipt['status'] == 1:
                # SUCCESS
                alert_msg = (
                    f"🟢 <b>Liquidation SUCCESS</b>\n"
                    f"🎯 Target: <code>{user}</code>\n"
                    f"💰 Est. Debt Value: ~${float(debt_val / Decimal(10**8)):.2f}\n"
                    f"⛽ Gas Cost: {gas_cost_eth:.6f} ETH\n"
                    f"🔗 <a href='{arbiscan_link}'>View on Arbiscan</a>"
                )
                await self.send_telegram_alert(alert_msg)
                await self.log_system(f"✅ TX CONFIRMED: {tx_hash.hex()} | Gas: {gas_cost_eth:.6f} ETH", "success")
            else:
                # REVERTED
                alert_msg = (
                    f"🟡 <b>TX REVERTED</b>\n"
                    f"🎯 Target: <code>{user}</code>\n"
                    f"💸 Gas Wasted: {gas_cost_eth:.6f} ETH\n"
                    f"🔗 <a href='{arbiscan_link}'>View on Arbiscan</a>"
                )
                await self.send_telegram_alert(alert_msg)
                await self.log_system(f"❌ TX REVERTED: {tx_hash.hex()}", "error")

        except Exception as e:
            await self.log_system(f"Transaction Monitor Error: {e}", "error")
        
    async def check_user_health(self, user):
        """Standard Health Check."""
        async with SEMAPHORE:
            try:
                # Use call() wrapper
                data = await self.rpc_manager.call(self.pool.functions.getUserAccountData(user).call)
                hf = Decimal(data[5]) / Decimal(10**18)
                
                if hf < 1.0:
                    logger.info(f"💀 LIQUIDATABLE: {user} (HF: {hf})")
                    await self.execute_liquidation(user)
                    return True
                return False
            except Exception as e:
                return False

    async def worker_loop(self):
        """Main Loop: Fetch targets -> Check Health -> Refresh."""
        await self.init_infrastructure()
        
        while self.running:
            try:
                start_time = time.time()
                
                # Reload targets
                await self.load_targets_async()
                if not self.targets:
                    print("💤 No targets. Sleeping...", end="\r")
                    await asyncio.sleep(10) # <--- CRITICAL UPDATE/HOTFIX: 10s sleep
                    continue

                print(f"🎯 Tracking {len(self.targets)} targets...", end="\r")
                
                # Concurrent Checks
                tasks = [self.check_user_health(user) for user in self.targets]
                await asyncio.gather(*tasks)
                
                elapsed = time.time() - start_time
                # CRITICAL HOTFIX: Forced 1.0s delay ALWAYS after loop
                await asyncio.sleep(1.0)

            except Exception as e:
                await self.log_system(f"💥 Worker loop error: {e}", "error")
                await self.rpc_manager.handle_failure() # Trigger Failover
                await self.send_telegram_alert(
                    f"⚠️ <b>Worker Loop Error:</b>\n<code>{e}</code>",
                    is_error=True
                )
                await asyncio.sleep(5)

    async def _run_with_alerts(self):
        """Wraps worker_loop with Telegram startup & crash alerts."""
        # Start Background Prober
        asyncio.create_task(self.rpc_manager.start_background_prober())
        
        await self.send_telegram_alert("🟢 <b>Bot Started:</b> Scanning the market.")
        try:
            await self.worker_loop()
        except Exception as e:
            # Global crash handler
            await self.rpc_manager.handle_failure()
            
            crash_msg = f"🆘 <b>CRASH ALERT:</b> <code>{e}</code>"
            await self.send_telegram_alert(crash_msg)
            await self.log_system(f"💥 FATAL CRASH: {e}", "error")
            raise

    def run(self):
        try:
            asyncio.run(self._run_with_alerts())
        except KeyboardInterrupt:
            print("\n🛑 Bot Stopped.")

if __name__ == "__main__":
    bot = AdaptiveSniperBot()
    bot.run()