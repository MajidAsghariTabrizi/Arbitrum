import os
import json
import asyncio
import logging
import time
import warnings
import aiohttp
import websockets

warnings.filterwarnings("ignore", category=ResourceWarning, module="aiohttp")
from decimal import Decimal
import aiofiles
import requests
from web3 import AsyncWeb3
from dotenv import load_dotenv
from eth_abi import decode

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
logger = logging.getLogger("AntiGravity")

# Database & Notification Setup
try:
    import db_manager
    DB_ENABLED = True
except ImportError:
    DB_ENABLED = False
    logger.warning("⚠️ db_manager.py not found. Dashboard logging disabled.")

# Configuration Constants
PRIMARY_WSS = os.getenv("PRIMARY_WSS")
PRIMARY_RPC = os.getenv("PRIMARY_RPC")
FALLBACK_RPCS = [r.strip() for r in os.getenv("FALLBACK_RPCS", "").split(",") if r.strip()]
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not PRIMARY_RPC:
    logger.error("❌ Critical Error: Missing PRIMARY_RPC in .env")
    exit(1)
if not PRIVATE_KEY or not LIQUIDATOR_ADDRESS:
    logger.error("❌ Critical Error: Missing PRIVATE_KEY or LIQUIDATOR_ADDRESS in .env")
    exit(1)

# Polling Config
POLL_INTERVAL = 0.1          # 100ms — check for new blocks rapidly
SCOUT_INTERVAL = 10          # Scout (Tier 2) runs every N blocks

# Tier Thresholds (must match scanner.py)
TIER_1_MAX_HF = Decimal('1.050')
TIER_2_MAX_HF = Decimal('1.200')

# Arbitrum One Addresses (EIP-55 Checksummed)
POOL_ADDRESS = AsyncWeb3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
POOL_ADDRESSES_PROVIDER = AsyncWeb3.to_checksum_address("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb")
DATA_PROVIDER_ADDRESS = AsyncWeb3.to_checksum_address("0x69fa688f1dc47d4b5d8029d5a35fb7a548310654")
QUOTER_V2_ADDRESS = AsyncWeb3.to_checksum_address("0x61fFE014bA17989E743c5F6cB21bF9697530B21e")

# Multicall3 — Arbitrum One (EIP-55 Checksummed)
MULTICALL3_ADDRESS = AsyncWeb3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
MULTICALL3_ABI = [{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call[]","name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"},{"internalType":"bytes[]","name":"returnData","type":"bytes[]"}],"stateMutability":"view","type":"function"}]

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


# --- 2. ASYNC RPC MANAGER (Enhanced: 403 support) ---

class AsyncRPCManager:
    """Manages RPC endpoints with automatic failover for 429 AND 403 errors."""
    def __init__(self):
        self.endpoints = []
        if PRIMARY_WSS:
            self.endpoints.append(PRIMARY_WSS)
        if PRIMARY_RPC:
            self.endpoints.append(PRIMARY_RPC)
        self.endpoints.extend(FALLBACK_RPCS)
        self.current_index = 0
        self.strike_count = 0
        self.last_rate_limit = 0
        self.w3 = None

    async def connect(self):
        """Connect to the current RPC endpoint. Closes any existing session first."""
        # Gracefully close the previous aiohttp session to prevent
        # "Unclosed client session" warnings and memory leaks
        if self.w3 and hasattr(self.w3.provider, '_request_kwargs'):
            try:
                session = await self.w3.provider.cache_async_session(None)
                if session and not session.closed:
                    await session.close()
                    logger.info("🔒 Previous aiohttp session closed cleanly.")
            except Exception:
                pass  # Best-effort cleanup — don't block reconnection

        url = self.endpoints[self.current_index]
        logger.info(f"🔌 Connecting to RPC: {url[:40]}...")
        
        if url.startswith("wss://"):
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncWebsocketProvider(url))
        else:
            self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
            
        try:
            connected = await self.w3.is_connected()
            if not connected:
                logger.warning(f"⚠️ RPC {url[:40]} might be down, but continuing...")
        except Exception as e:
            logger.warning(f"⚠️ Connection test failed (likely rate limit), bypassing: {e}")
            
        logger.info(f"🟢 Connected to RPC [{self.current_index + 1}/{len(self.endpoints)}]")

    async def handle_rate_limit(self):
        """Handle 429/403 errors with adaptive backoff and failover."""
        self.strike_count += 1
        self.last_rate_limit = time.time()

        if self.strike_count >= 3:
            self.strike_count = 0
            self.current_index = (self.current_index + 1) % len(self.endpoints)
            logger.warning(f"🔄 3 strikes! Switching to RPC [{self.current_index + 1}/{len(self.endpoints)}]")
            await self.connect()
        else:
            cooldown = 30
            logger.warning(f"⏳ Rate limited (Strike {self.strike_count}/3). Cooling down {cooldown}s...")
            await asyncio.sleep(cooldown)

    def is_rate_limit_error(self, error):
        """Check if an error is a rate limit / forbidden error."""
        err_str = str(error).lower()
        return any(k in err_str for k in ["429", "403", "rate", "forbidden", "quota", "too many requests", "-32001"])

    async def block_stream(self):
        """Yields new block numbers using WSS subscriptions or HTTP polling fallback."""
        url = self.endpoints[self.current_index]
        
        if url.startswith("wss://"):
            logger.info(f"🎧 Starting WSS Block Stream on {url[:40]}...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                sub_msg = {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}
                await ws.send(json.dumps(sub_msg))
                response = await ws.recv()
                logger.info(f"✅ WSS Subscribed: {response}")
                
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "method" in data and data["method"] == "eth_subscription":
                        block_hex = data["params"]["result"]["number"]
                        block_number = int(block_hex, 16)
                        yield block_number
        else:
            logger.info(f"📡 Starting HTTP Block Polling on {url[:40]}...")
            last_block = 0
            if self.w3:
                last_block = await self.w3.eth.block_number
                yield last_block

            while True:
                current_block = await self.w3.eth.block_number
                if current_block > last_block:
                    last_block = current_block
                    yield current_block
                else:
                    await asyncio.sleep(POLL_INTERVAL)


# --- 3. ANTI-GRAVITY BOT CLASS ---

class AntiGravityBot:
    """
    Anti-Gravity MEV Sniper — Dual-task architecture:
      Task A (Sniper): Scans Tier 1 (Danger) targets every block via Multicall3.
      Task B (Scout):  Scans Tier 2 (Watchlist) every 10 blocks, promotes to Tier 1.
    """
    def __init__(self):
        # RPC Manager
        self.rpc = AsyncRPCManager()
        self.account = None

        # Contracts
        self.pool = None
        self.data_provider = None
        self.addresses_provider = None
        self.liquidator_contract = None
        self.oracle_contract = None

        # ================================================================
        # RAM PRIORITY QUEUE — Tiered target lists loaded from targets.json
        # ================================================================
        self.tier_1_danger = []      # HF 1.000 – 1.050 (scanned every block)
        self.tier_2_watchlist = []    # HF 1.051 – 1.200 (scanned every 10 blocks)

        # Data
        self.reserves_list = []
        self.asset_decimals = {}
        self.prices = {}

        # Block Tracking
        self.last_processed_block = 0
        self.blocks_since_scout = 0  # Counter for scout interval

        # Concurrency
        self.nonce_lock = asyncio.Lock()
        self._last_errors = {}

    @property
    def w3(self):
        return self.rpc.w3

    async def init_contracts(self):
        """Initialize all contracts after RPC is connected."""
        self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        logger.info(f"🔑 Loaded Wallet: {self.account.address}")

        self.pool = self.w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
        self.data_provider = self.w3.eth.contract(address=DATA_PROVIDER_ADDRESS, abi=DATA_PROVIDER_ABI)
        self.addresses_provider = self.w3.eth.contract(address=POOL_ADDRESSES_PROVIDER, abi=ADDRESSES_PROVIDER_ABI)
        self.liquidator_contract = self.w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)

        # Fetch Oracle address dynamically
        oracle_addr = await self.addresses_provider.functions.getPriceOracle().call()
        self.oracle_contract = self.w3.eth.contract(address=oracle_addr, abi=ORACLE_ABI)

        # Multicall3 — used for batched health-factor checks
        self.multicall = self.w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)

        # Cache reserves list
        self.reserves_list = await self.pool.functions.getReservesList().call()
        logger.info(f"📚 Loaded {len(self.reserves_list)} market assets.")

    async def log_system(self, msg, level="info"):
        if level == "error":
            logger.error(msg)
        elif level == "warning":
            logger.warning(msg)
        else:
            logger.info(msg)

        if DB_ENABLED:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, db_manager.log_event, level, msg)

        if DISCORD_WEBHOOK and (level == "success" or level == "error"):
            await self.send_discord_alert(msg, level)

    async def send_discord_alert(self, msg, level):
        try:
            color = 0x00ff00 if level == "success" else 0xff0000
            payload = {"embeds": [{"title": "🦅 Anti-Gravity Bot", "description": msg, "color": color}]}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(DISCORD_WEBHOOK, json=payload))
        except Exception:
            pass

    async def send_telegram_alert(self, msg, is_error=False):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return

        # Anti-spam: skip duplicate error alerts within 5-minute cooldown
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
        except Exception:
            pass

    async def update_prices(self):
        """Updates asset prices in bulk from Aave Oracle."""
        try:
            prices = await self.oracle_contract.functions.getAssetsPrices(self.reserves_list).call()
            for i, asset in enumerate(self.reserves_list):
                self.prices[asset] = prices[i]
        except Exception as e:
            logger.warning(f"Price update failed: {e}")

    async def get_decimals(self, token):
        if token in self.asset_decimals:
            return self.asset_decimals[token]
        try:
            checksum_token = self.w3.to_checksum_address(token)
            erc20 = self.w3.eth.contract(address=checksum_token, abi=ERC20_ABI)
            decimals = await erc20.functions.decimals().call()
            self.asset_decimals[token] = decimals
            return decimals
        except Exception:
            return 18

    # ================================================================
    # DYNAMIC RAM LOADING — Read tiered targets.json without blocking
    # ================================================================

    async def load_targets_async(self):
        """Reloads targets.json asynchronously into tiered RAM queues.
        Supports both structured (tiered) and flat (legacy) formats."""
        try:
            path = "/root/Arbitrum/targets.json" if os.path.exists("/root/Arbitrum") else "targets.json"
            async with aiofiles.open(path, mode='r') as f:
                content = await f.read()
                if not content:
                    return

                data = json.loads(content)

                if isinstance(data, dict):
                    # Structured tiered format from new scanner
                    self.tier_1_danger = data.get("tier_1_danger", [])
                    self.tier_2_watchlist = data.get("tier_2_watchlist", [])
                elif isinstance(data, list):
                    # Legacy flat array — treat all as Tier 1
                    self.tier_1_danger = data
                    self.tier_2_watchlist = []
                else:
                    self.tier_1_danger = []
                    self.tier_2_watchlist = []

        except Exception as e:
            logger.warning(f"⚠️ Failed to load targets: {e}")

    async def analyze_user_assets(self, user):
        """Finds best debt and collateral for a liquidatable user."""
        if not self.prices:
            await self.update_prices()

        best_debt = None
        best_collateral = None
        max_debt_value = Decimal(0)
        max_collateral_value = Decimal(0)
        debt_amount_raw = 0

        tasks = [
            self.data_provider.functions.getUserReserveData(asset, user).call()
            for asset in self.reserves_list
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res in enumerate(results):
            if isinstance(res, Exception):
                continue

            asset = self.reserves_list[i]
            price = self.prices.get(asset, 0)
            if price == 0:
                continue

            # Collateral (Index 0)
            collateral_bal = res[0]
            if collateral_bal > 0:
                decimals = await self.get_decimals(asset)
                value_usd = (Decimal(collateral_bal) / Decimal(10**decimals)) * (Decimal(price) / Decimal(10**8))
                if value_usd > max_collateral_value:
                    max_collateral_value = value_usd
                    best_collateral = asset

            # Debt — Variable (Index 2)
            variable_debt = res[2]
            if variable_debt > 0:
                decimals = await self.get_decimals(asset)
                value_usd = (Decimal(variable_debt) / Decimal(10**decimals)) * (Decimal(price) / Decimal(10**8))
                if value_usd > max_debt_value:
                    max_debt_value = value_usd
                    best_debt = asset
                    debt_amount_raw = variable_debt

        return best_debt, best_collateral, debt_amount_raw, max_debt_value

    # ================================================================
    # PRE-FLIGHT SIMULATION — Simulate TX before broadcasting
    # ================================================================

    async def simulate_liquidation(self, tx_func, user):
        """Simulate the transaction via eth_call. Returns True if it would succeed."""
        try:
            await tx_func.call({'from': self.account.address})
            logger.info(f"✅ Pre-flight PASS for {user[:10]}... — TX will succeed")
            return True
        except Exception as e:
            err_str = str(e)
            logger.warning(f"🚫 Pre-flight FAIL for {user[:10]}...: {err_str}")
            await self.log_system(
                f"🚫 Simulation reverted for {user}: {err_str}", "warning"
            )
            return False

    async def execute_liquidation(self, user):
        """Builds, simulates, signs, and sends Flash Loan liquidation transaction."""
        debt_asset, collateral_asset, debt_amount, debt_val = await self.analyze_user_assets(user)

        if not debt_asset or not collateral_asset or debt_val < 50:
            return

        logger.info(f"⚔️ SNIPING: {user} | Debt: ${debt_val:.2f}")

        # ==============================================================
        # SMART SLIPPAGE — Calculate dynamic amount_out_min
        # The flash loan borrows `debt_amount` of `debt_asset`, liquidates
        # the user, seizes collateral, and swaps collateral → debt_asset
        # to repay. `amount_out_min` protects the swap output.
        #
        # Formula: min_output = debt_amount * 0.98 (2% slippage tolerance)
        # This ensures we receive at least 98% of what we need to repay.
        # ==============================================================
        SLIPPAGE_TOLERANCE = Decimal('0.98')  # 2% max slippage
        amount_out_min = int(Decimal(debt_amount) * SLIPPAGE_TOLERANCE)

        # Params
        fee = 3000          # 0.3% Uniswap fee tier
        sqrt_price_limit = 0

        logger.info(f"📊 Slippage Guard: amount_out_min={amount_out_min} (2% tolerance)")

        # Build TX Function
        tx_func = self.liquidator_contract.functions.requestFlashLoan(
            user,
            debt_asset,
            collateral_asset,
            int(debt_amount),
            fee,
            int(amount_out_min),
            int(sqrt_price_limit)
        )

        # ==============================================================
        # PRE-FLIGHT SIMULATION — simulate via eth_call before broadcasting
        # If the TX would revert, skip it to avoid wasting gas.
        # ==============================================================
        sim_pass = await self.simulate_liquidation(tx_func, user)
        if not sim_pass:
            logger.warning(f"🚫 DROPPING target {user} — simulation reverted. Will not broadcast.")
            return

        try:
            # ============================================================
            # NONCE LOCK — prevents "nonce too low" when multiple targets
            # are liquidatable in the same block.
            # Uses 'pending' to account for in-flight transactions.
            # try/finally ensures clean error handling inside the lock.
            # ============================================================
            async with self.nonce_lock:
                try:
                    nonce = await self.w3.eth.get_transaction_count(
                        self.account.address, 'pending'
                    )

                    # Gas Estimation (with safe fallback)
                    try:
                        gas_est = await tx_func.estimate_gas({'from': self.account.address})
                        gas_limit = int(gas_est * 1.2)
                    except Exception as gas_err:
                        logger.warning(f"⚠️ Gas estimation failed, using fallback: {gas_err}")
                        gas_limit = 2500000  # Safe fallback for flash loans

                    # EIP-1559 Fees
                    block = await self.w3.eth.get_block('latest')
                    base_fee = block['baseFeePerGas']
                    priority = self.w3.to_wei(0.5, 'gwei')
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
                    tx_hash = await self.w3.eth.send_raw_transaction(signed.raw_transaction)

                except Exception as build_err:
                    # Log clearly if build/sign/send fails inside the lock
                    await self.log_system(
                        f"TX Build/Send Failed for {user}: {build_err}", "error"
                    )
                    await self.send_telegram_alert(
                        f"⚠️ <b>TX Build Failed</b> for <code>{user}</code>:\n"
                        f"<code>{build_err}</code>",
                        is_error=True
                    )
                    return  # Exit cleanly — lock is released by `async with`

            # --- Post-send logging (outside lock to release nonce ASAP) ---
            tx_hex = tx_hash.hex()
            arbiscan_link = f"https://arbiscan.io/tx/{tx_hex}"

            await self.log_system(f"🔥 TX SENT: {tx_hex}", "success")
            await self.send_telegram_alert(
                f"🚀 <b>Liquidation Sent</b>\n"
                f"🎯 Target: <code>{user}</code>\n"
                f"💰 Debt: ~${float(debt_val):.2f}\n"
                f"🔗 <a href='{arbiscan_link}'>View on Arbiscan</a>"
            )

            # Monitor receipt (non-blocking, best-effort)
            try:
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                gas_used = receipt['gasUsed']
                effective_gas_price = receipt['effectiveGasPrice']
                gas_cost_eth = Decimal(gas_used * effective_gas_price) / Decimal(10**18)

                if receipt['status'] == 1:
                    await self.log_system(f"✅ TX CONFIRMED: {tx_hex} | Gas: {gas_cost_eth:.6f} ETH", "success")
                    await self.send_telegram_alert(
                        f"🟢 <b>Liquidation SUCCESS</b>\n"
                        f"⛽ Gas: {gas_cost_eth:.6f} ETH\n"
                        f"🔗 <a href='{arbiscan_link}'>Arbiscan</a>"
                    )
                else:
                    await self.log_system(f"❌ TX REVERTED: {tx_hex}", "error")
                    await self.send_telegram_alert(
                        f"🟡 <b>TX REVERTED</b>\n"
                        f"💸 Gas Wasted: {gas_cost_eth:.6f} ETH\n"
                        f"🔗 <a href='{arbiscan_link}'>Arbiscan</a>"
                    )
            except Exception as receipt_err:
                await self.log_system(f"Receipt Monitor Error: {receipt_err}", "warning")

        except Exception as e:
            await self.log_system(f"Execution Failed for {user}: {e}", "error")
            await self.send_telegram_alert(
                f"⚠️ <b>Execution Failed</b> for <code>{user}</code>:\n<code>{e}</code>",
                is_error=True
            )

    # ================================================================
    # MULTICALL3 BATCH SCAN — Shared logic for Sniper & Scout
    # ================================================================

    async def multicall_scan(self, targets, task_name, block_number):
        """
        Batch-check a list of targets via Multicall3.
        Returns list of (user, hf_decimal, collateral_usd, debt_usd) tuples.
        """
        if not targets:
            return []

        # Build calldata
        calls = []
        for user in targets:
            call_data = self.pool.functions.getUserAccountData(user)._encode_transaction_data()
            calls.append((POOL_ADDRESS, call_data))

        try:
            # Single batched RPC call replaces N individual calls
            _, return_data = await self.multicall.functions.aggregate(calls).call()
        except Exception as e:
            if self.rpc.is_rate_limit_error(e):
                await self.rpc.handle_rate_limit()
            else:
                logger.error(f"❌ [{task_name}] Multicall failed on block {block_number}: {e}")
            return []

        # Decode results
        results = []
        for i, raw_bytes in enumerate(return_data):
            user = targets[i]
            try:
                decoded_data = decode(
                    ['uint256', 'uint256', 'uint256', 'uint256', 'uint256', 'uint256'],
                    raw_bytes
                )

                # Aave V3 getUserAccountData returns:
                #   [0] totalCollateralBase  (USD, 8 decimals)
                #   [1] totalDebtBase        (USD, 8 decimals)
                #   [5] healthFactor         (18 decimals)
                total_collateral_usd = float(Decimal(decoded_data[0]) / Decimal(10**8))
                total_debt_usd = float(Decimal(decoded_data[1]) / Decimal(10**8))
                hf = Decimal(decoded_data[5]) / Decimal(10**18)

                results.append((user, hf, total_collateral_usd, total_debt_usd))

            except Exception as e:
                logger.warning(f"⚠️ [{task_name}] Failed to decode data for {user}: {e}")
                continue

        return results

    # ================================================================
    # TASK A: THE SNIPER — Tier 1 every block
    # ================================================================

    async def sniper_scan(self, block_number):
        """Process Tier 1 (Danger) targets: check HF and liquidate if < 1.0."""
        start_time = time.time()

        results = await self.multicall_scan(self.tier_1_danger, "SNIPER", block_number)

        # Build live data for dashboard
        live_targets_data = []

        for user, hf, coll_usd, debt_usd in results:
            hf_float = float(hf)
            live_targets_data.append((user, hf_float, debt_usd, coll_usd))

            if 0 < hf < Decimal('1.0'):
                logger.info(f"💀 LIQUIDATABLE: {user} (HF: {hf})")
                await self.execute_liquidation(user)

        elapsed = (time.time() - start_time) * 1000
        t1_count = len(self.tier_1_danger)

        if t1_count > 0:
            logger.info(f"🎯 [SNIPER] Block {block_number} | {t1_count} Tier-1 targets in {elapsed:.0f}ms")

        return live_targets_data, elapsed

    # ================================================================
    # TASK B: THE SCOUT — Tier 2 every 10 blocks, promote to Tier 1
    # ================================================================

    async def scout_scan(self, block_number):
        """Process Tier 2 (Watchlist) targets: promote to Tier 1 if HF drops below threshold."""
        start_time = time.time()

        results = await self.multicall_scan(self.tier_2_watchlist, "SCOUT", block_number)

        promoted = []
        remaining_t2 = []
        live_targets_data = []

        for user, hf, coll_usd, debt_usd in results:
            hf_float = float(hf)
            live_targets_data.append((user, hf_float, debt_usd, coll_usd))

            if 0 < hf < TIER_1_MAX_HF:
                # Promote to Tier 1!
                promoted.append(user)
                logger.info(f"⬆️ PROMOTED to Tier 1: {user} (HF: {hf:.4f})")
            elif hf > TIER_2_MAX_HF or hf == 0:
                # HF recovered above 1.200 or no debt — drop from watchlist
                pass
            else:
                remaining_t2.append(user)

        # Apply promotions to RAM
        if promoted:
            self.tier_1_danger.extend(promoted)
            self.tier_2_watchlist = remaining_t2
            logger.info(f"📊 Promoted {len(promoted)} targets. Tier 1: {len(self.tier_1_danger)}, Tier 2: {len(self.tier_2_watchlist)}")
            await self.send_telegram_alert(
                f"⬆️ <b>{len(promoted)} targets promoted to Tier 1</b>\n"
                f"🔴 Tier 1: {len(self.tier_1_danger)} | 🟠 Tier 2: {len(self.tier_2_watchlist)}"
            )
        else:
            self.tier_2_watchlist = remaining_t2

        elapsed = (time.time() - start_time) * 1000
        logger.info(f"🔭 [SCOUT] Block {block_number} | {len(results)} Tier-2 targets in {elapsed:.0f}ms")

        return live_targets_data, elapsed

    # ================================================================
    # MAIN BLOCK PROCESSOR — Orchestrates Sniper + Scout
    # ================================================================

    async def process_block(self, block_number):
        """Process a single new block: run Sniper, optionally run Scout."""
        start_time = time.time()

        # 1. Update Prices
        await self.update_prices()

        # 2. Reload targets from disk (non-blocking)
        await self.load_targets_async()

        total_targets = len(self.tier_1_danger) + len(self.tier_2_watchlist)
        if total_targets == 0:
            return

        # 3. TASK A: Sniper — Tier 1 every block
        sniper_data, sniper_ms = await self.sniper_scan(block_number)

        # 4. TASK B: Scout — Tier 2 every SCOUT_INTERVAL blocks
        scout_data = []
        self.blocks_since_scout += 1
        if self.blocks_since_scout >= SCOUT_INTERVAL:
            self.blocks_since_scout = 0
            scout_data_result, scout_ms = await self.scout_scan(block_number)
            scout_data = scout_data_result

        elapsed = (time.time() - start_time) * 1000
        logger.info(
            f"🧱 Block {block_number} | "
            f"T1: {len(self.tier_1_danger)} | T2: {len(self.tier_2_watchlist)} | "
            f"{elapsed:.0f}ms"
        )

        # 5. Async DB push — fire-and-forget (zero main-loop impact)
        if DB_ENABLED:
            try:
                all_live_data = sniper_data + scout_data
                asyncio.ensure_future(
                    asyncio.to_thread(db_manager.update_live_targets, all_live_data)
                )
                t1_count = len(self.tier_1_danger)
                t2_count = len(self.tier_2_watchlist)
                asyncio.ensure_future(
                    asyncio.to_thread(
                        db_manager.log_system_metric,
                        block_number, total_targets, elapsed,
                        tier_1_count=t1_count, tier_2_count=t2_count
                    )
                )
            except Exception as e:
                logger.warning(f"⚠️ Dashboard DB push failed (non-blocking): {e}")

    async def run_forever(self):
        """Main Smart HTTP Polling Loop — tracks new blocks and processes them."""
        # Connect and initialize
        await self.rpc.connect()
        await self.init_contracts()

        await self.send_telegram_alert("🟢 <b>Anti-Gravity Bot Started (HTTP Polling)</b>")
        logger.info("🚀 Anti-Gravity Engine started. Sniper + Scout architecture active.")

        # Seed the block tracker
        self.last_processed_block = await self.w3.eth.block_number
        logger.info(f"📍 Starting from block: {self.last_processed_block}")

        # Load initial targets
        await self.load_targets_async()
        logger.info(f"📊 Initial targets: Tier 1: {len(self.tier_1_danger)} | Tier 2: {len(self.tier_2_watchlist)}")

        # ============================================================
        # BLOCK STREAMING LOOP
        # - Seamlessly consumes blocks from WSS or HTTP fallback
        # - Handles RPC failures with reconnect logic (429 + 403)
        # ============================================================
        while True:
            try:
                async for current_block in self.rpc.block_stream():
                    if current_block > self.last_processed_block:
                        # New block(s) detected — process the latest one
                        self.last_processed_block = current_block
                        await self.process_block(current_block)

            except Exception as e:
                if self.rpc.is_rate_limit_error(e):
                    await self.rpc.handle_rate_limit()
                    # Re-init contracts after RPC switch
                    await self.init_contracts()
                else:
                    logger.error(f"⚠️ Polling Error: {e}")
                    await self.send_telegram_alert(
                        f"⚠️ <b>Polling Error:</b> <code>{e}</code>",
                        is_error=True
                    )
                    await asyncio.sleep(5)


if __name__ == "__main__":
    bot = AntiGravityBot()
    try:
        asyncio.run(bot.run_forever())
    except KeyboardInterrupt:
        print("\n🛑 Anti-Gravity Bot Stopped.")