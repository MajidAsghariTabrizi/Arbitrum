import os
import json
import asyncio
import random
import logging
import time
import warnings
import aiohttp


warnings.filterwarnings("ignore", category=ResourceWarning, module="aiohttp")
from decimal import Decimal
import aiofiles
import requests
from web3 import AsyncWeb3
from market_sentinel import MarketSentinel
import zmq
import zmq.asyncio
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
logger = logging.getLogger("LodestarBot")

# Database & Notification Setup
try:
    import db_manager
    DB_ENABLED = True
except ImportError:
    DB_ENABLED = False
    logger.warning("⚠️ db_manager.py not found. Dashboard logging disabled.")

# Configuration Constants
# Configuration Constants — Strict QoS Lane: Tier 2 (Snipers) → SNIPER_RPC
SNIPER_RPC = os.getenv("SNIPER_RPC")
FALLBACK_RPCS_RAW = os.getenv("FALLBACK_RPCS", "").replace('"', '').replace("'", "")
FALLBACK_RPCS = [r.strip() for r in FALLBACK_RPCS_RAW.split(",") if r.strip()]
PRIVATE_KEY = os.getenv("PRIVATE_KEY")

# CRITICAL: Use RADIANT_LIQUIDATOR_ADDRESS (Keeping the env var name as requested, or using LODESTAR if updated)
LIQUIDATOR_ADDRESS = os.getenv("LODESTAR_LIQUIDATOR_ADDRESS", os.getenv("RADIANT_LIQUIDATOR_ADDRESS"))

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not SNIPER_RPC:
    logger.error("❌ Critical Error: Missing SNIPER_RPC in .env")
    exit(1)
if not PRIVATE_KEY or not LIQUIDATOR_ADDRESS:
    logger.error("❌ Critical Error: Missing PRIVATE_KEY or LIQUIDATOR_ADDRESS in .env")
    exit(1)

# Polling Config
POLL_INTERVAL = 2.0          # 2.0s — relaxed polling
SCOUT_INTERVAL = 10          # Scout (Tier 2) runs every N blocks

# Tier Thresholds (must match scanner.py)
TIER_1_MAX_HF = Decimal('1.050')
TIER_2_MAX_HF = Decimal('1.200')

# Lodestar Finance Addresses (Compound V2)
COMPTROLLER_ADDRESS = AsyncWeb3.to_checksum_address("0x264906F21b6DDFc07f43372fC24422B9c0587a8b")

# Multicall3 — Arbitrum One
MULTICALL3_ADDRESS = AsyncWeb3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
MULTICALL3_ABI = [{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call[]","name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"},{"internalType":"bytes[]","name":"returnData","type":"bytes[]"}],"stateMutability":"view","type":"function"}]

# ABIs
COMPTROLLER_ABI = [{
    "constant": True,
    "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
    "name": "getAccountLiquidity",
    "outputs": [
        {"internalType": "uint256", "name": "", "type": "uint256"},
        {"internalType": "uint256", "name": "", "type": "uint256"},
        {"internalType": "uint256", "name": "", "type": "uint256"}
    ],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}, {
    "constant": True,
    "inputs": [],
    "name": "oracle",
    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}, {
    "constant": True,
    "inputs": [],
    "name": "getAllMarkets",
    "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}]

CTOKEN_ABI = [{
    "constant": True,
    "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
    "name": "getAccountSnapshot",
    "outputs": [
        {"internalType": "uint256", "name": "", "type": "uint256"},
        {"internalType": "uint256", "name": "", "type": "uint256"},
        {"internalType": "uint256", "name": "", "type": "uint256"},
        {"internalType": "uint256", "name": "", "type": "uint256"}
    ],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}, {
    "constant": True,
    "inputs": [],
    "name": "underlying",
    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}]

ORACLE_ABI = [{
    "inputs": [{"internalType": "address", "name": "cToken", "type": "address"}],
    "name": "getUnderlyingPrice",
    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function"
}]

# Liquidator (Clone of FlashLoanLiquidator interface)
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

class SmartAsyncRPCManager:
    """
    Round-Robin Async RPC Manager (Tier 2: SNIPER_RPC).
    - Rotates through all available RPC nodes on rate limit / quota errors.
    """
    HARD_ERROR_KEYWORDS = ["serverdisconnected", "connectionerror", "connection refused",
                           "cannot connect", "server disconnected", "connectionreseterror",
                           "clientconnectorerror", "oserror", "gaierror"]

    def __init__(self):
        self.primary_url = SNIPER_RPC
        self.rpc_urls = [self.primary_url] + FALLBACK_RPCS.copy()
        self.current_index = 0
        self.active_url = self.rpc_urls[self.current_index]
        self.w3 = None
        self.strike_count = 0

    async def connect(self):
        """Connect to the current SNIPER_RPC node."""
        if self.w3 and hasattr(self.w3.provider, '_request_kwargs'):
            try:
                session = await self.w3.provider.cache_async_session(None)
                if session and not session.closed:
                    await session.close()
            except Exception:
                pass

        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(
            self.active_url, request_kwargs={'timeout': 60}
        ))
        logger.info(f"🔌 Smart RPC (Tier 2 Sniper): {self.active_url[:50]}...")

    async def handle_rate_limit(self):
        """Immediately rotate to the next node and sleep briefly."""
        self.current_index = (self.current_index + 1) % len(self.rpc_urls)
        self.active_url = self.rpc_urls[self.current_index]
        await self.connect()
        
        cooldown = random.uniform(1.0, 2.0)
        logger.warning(f"⏳ Rate limited or Quota exceeded. Rotating to {self.active_url[:50]}... (Sleep {cooldown:.1f}s)")
        await asyncio.sleep(cooldown)

    async def handle_hard_error(self, error):
        """Hard connection error: Rotate to next node."""
        logger.error(f"💥 Hard RPC error: {error}. Rotating...")
        await self.handle_rate_limit()

    def is_rate_limit_error(self, error):
        """Check if an error is a rate limit / forbidden error."""
        err_str = str(error).lower()
        return any(k in err_str for k in ["429", "403", "rate", "forbidden", "quota", "too many requests", "-32001"])

    def is_hard_error(self, error):
        err_str = str(error).lower()
        return any(k in err_str for k in self.HARD_ERROR_KEYWORDS)

# --- 3. LODESTAR BOT CLASS ---

class LodestarBot:
    """
    Lodestar MEV Sniper 
    """
    def __init__(self):
        # RPC Manager
        self.rpc = SmartAsyncRPCManager()
        self.account = None

        # Contracts
        self.comptroller = None
        self.liquidator_contract = None
        self.multicall = None
        self.oracle_contract = None

        # ================================================================
        # RAM PRIORITY QUEUE — Tiered target lists loaded from lodestar_targets.json
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
        
        self.comptroller = self.w3.eth.contract(address=COMPTROLLER_ADDRESS, abi=COMPTROLLER_ABI)
        self.liquidator_contract = self.w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)
        self.multicall = self.w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)

        # Try to fetch Oracle address dynamically from Comptroller
        try:
            oracle_addr = await self.comptroller.functions.oracle().call()
            self.oracle_contract = self.w3.eth.contract(address=oracle_addr, abi=ORACLE_ABI)
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch Oracle from Comptroller: {e}")
            self.oracle_contract = None

        # Try to cache markets list, but don't crash
        try:
            self.reserves_list = await self.comptroller.functions.getAllMarkets().call()
            logger.info(f"📚 Loaded {len(self.reserves_list)} generic markets.")
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch Markets List: {e}")
            self.reserves_list = []

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
            payload = {"embeds": [{"title": "🦅 Lodestar Bot", "description": msg, "color": color}]}
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
        """Updates asset prices in bulk from Oracle."""
        if not self.oracle_contract:
            return

        for ctoken in self.reserves_list:
            try:
                price = await self.oracle_contract.functions.getUnderlyingPrice(ctoken).call()
                self.prices[ctoken] = price
            except Exception as e:
                pass
            await asyncio.sleep(0.05)

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
    # DYNAMIC RAM LOADING — Read lodestar_targets.json
    # ================================================================

    async def load_targets_async(self):
        """Reloads lodestar_targets.json asynchronously into tiered RAM queues.
        Falls back to hardcoded borrower addresses if file is missing or empty."""
        # Hardcoded fallback empty list for Lodestar
        HARDCODED_FALLBACK_TARGETS = []
        try:
            path = "/root/Arbitrum/lodestar_targets.json" if os.path.exists("/root/Arbitrum") else "lodestar_targets.json"
            
            if not os.path.exists(path):
                raise FileNotFoundError(f"targets file not found: {path}")

            async with aiofiles.open(path, mode='r') as f:
                content = await f.read()
                if not content:
                    raise ValueError("Empty file")

                data = json.loads(content)

                if isinstance(data, dict):
                    self.tier_1_danger = data.get("tier_1_danger", [])
                    self.tier_2_watchlist = data.get("tier_2_watchlist", [])
                elif isinstance(data, list):
                    self.tier_1_danger = data
                    self.tier_2_watchlist = []
                else:
                    self.tier_1_danger = []
                    self.tier_2_watchlist = []

        except Exception as e:
            logger.warning(f"⚠️ Failed to load targets: {e}")
            self.tier_1_danger = []
            self.tier_2_watchlist = []

        # Fallback seeding: ensure bot always has something to monitor
        if not self.tier_1_danger and not self.tier_2_watchlist:
            if HARDCODED_FALLBACK_TARGETS:
                logger.warning("⚠️ No targets loaded — seeding Tier 2 with hardcoded whale fallbacks.")
                self.tier_2_watchlist = [self.w3.to_checksum_address(addr) for addr in HARDCODED_FALLBACK_TARGETS]

    async def analyze_user_assets(self, user):
        """Finds best debt and collateral for a liquidatable user."""
        if not self.prices:
            await self.update_prices()

        best_debt = None
        best_collateral = None
        max_debt_value = Decimal(0)
        max_collateral_value = Decimal(0)
        debt_amount_raw = 0

        # Soft-Start Initialization
        results = []
        for ctoken_addr in self.reserves_list:
            try:
                ctoken = self.w3.eth.contract(address=ctoken_addr, abi=CTOKEN_ABI)
                task = ctoken.functions.getAccountSnapshot(user).call()
                res = await task
                results.append((ctoken_addr, res))
            except Exception as e:
                pass
            await asyncio.sleep(0.1) 

        for ctoken_addr, res in results:
            error_code, ctoken_bal, borrow_bal, exchange_rate = res
            if error_code != 0:
                continue

            # In Compound V2, getUnderlyingPrice(cToken) returns pre-scaled price 1e(36 - decimals)
            # which we use for generic comparisons.
            price = self.prices.get(ctoken_addr, 10**18)
            try:
                ctoken = self.w3.eth.contract(address=ctoken_addr, abi=CTOKEN_ABI)
                underlying = await ctoken.functions.underlying().call()
            except:
                underlying = ctoken_addr

            # Collateral Value
            if ctoken_bal > 0:
                underlying_bal = Decimal(ctoken_bal) * Decimal(exchange_rate) / Decimal(10**18)
                value = underlying_bal * Decimal(price)
                if value > max_collateral_value:
                    max_collateral_value = value
                    best_collateral = underlying

            # Debt Value
            if borrow_bal > 0:
                value = Decimal(borrow_bal) * Decimal(price)
                if value > max_debt_value:
                    max_debt_value = value
                    best_debt = underlying
                    debt_amount_raw = borrow_bal

        return best_debt, best_collateral, debt_amount_raw, float(max_debt_value / Decimal(10**36))

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
            return False

    async def execute_liquidation(self, user):
        """Builds, simulates, signs, and sends Flash Loan liquidation."""
        debt_asset, collateral_asset, debt_amount, debt_val = await self.analyze_user_assets(user)

        # Heuristic minimum value check (ignoring unit difference for now, keeping logic same)
        if not debt_asset or not collateral_asset or debt_val < 1:
            return

        logger.info(f"⚔️ SNIPING: {user} | Debt Value: {debt_val:.2f}")

        # SLIPPAGE TOLERANCE
        SLIPPAGE_TOLERANCE = Decimal('0.98')  # 2% max slippage
        amount_out_min = int(Decimal(debt_amount) * SLIPPAGE_TOLERANCE)

        # Params
        fee = 3000          # 0.3% Uniswap fee tier
        sqrt_price_limit = 0

        # Build TX Function
        # Note: This calls requestFlashLoan
        # The arguments passed here match the Solidity requestFlashLoan signature for both V2/V3 contracts.
        tx_func = self.liquidator_contract.functions.requestFlashLoan(
            user,
            debt_asset,
            collateral_asset,
            int(debt_amount),
            fee,
            int(amount_out_min),
            int(sqrt_price_limit)
        )

        sim_pass = await self.simulate_liquidation(tx_func, user)
        if not sim_pass:
            logger.warning(f"🚫 DROPPING target {user} — simulation reverted. Will not broadcast.")
            return

        try:
            async with self.nonce_lock:
                try:
                    nonce = await self.w3.eth.get_transaction_count(
                        self.account.address, 'pending'
                    )

                    try:
                        gas_est = await tx_func.estimate_gas({'from': self.account.address})
                        gas_limit = int(gas_est * 1.2)
                    except Exception as gas_err:
                        logger.warning(f"⚠️ Gas estimation failed, using fallback: {gas_err}")
                        gas_limit = 2500000

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
                    await self.log_system(
                        f"TX Build/Send Failed for {user}: {build_err}", "error"
                    )
                    await self.send_telegram_alert(
                        f"⚠️ <b>TX Build Failed</b> for <code>{user}</code>:\n"
                        f"<code>{build_err}</code>",
                        is_error=True
                    )
                    return

            tx_hex = tx_hash.hex()
            arbiscan_link = f"https://arbiscan.io/tx/{tx_hex}"

            await self.log_system(f"🔥 TX SENT: {tx_hex}", "success")
            await self.send_telegram_alert(
                f"🚀 <b>Lodestar Liquidation Sent</b>\n"
                f"🎯 Target: <code>{user}</code>\n"
                f"💰 Val: ~{float(debt_val):.2f}\n"
                f"🔗 <a href='{arbiscan_link}'>View on Arbiscan</a>"
            )

            try:
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                gas_used = receipt['gasUsed']
                effective_gas_price = receipt['effectiveGasPrice']
                gas_cost_eth = Decimal(gas_used * effective_gas_price) / Decimal(10**18)

                if receipt['status'] == 1:
                    await self.log_system(f"✅ TX CONFIRMED: {tx_hex} | Gas: {gas_cost_eth:.6f} ETH", "success")
                    await self.send_telegram_alert(
                        f"🟢 <b>Lodestar Liquidation SUCCESS</b>\n"
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
    # MULTICALL3 BATCH SCAN
    # ================================================================

    async def multicall_scan(self, targets, task_name, block_number):
        """
        Batch-check a list of targets via Multicall3 using Comptroller given Compound V2 logic.
        """
        if not targets:
            return []

        calls = []
        for user in targets:
            call_data = self.comptroller.functions.getAccountLiquidity(user)._encode_transaction_data()
            calls.append((self.comptroller.address, call_data))

        try:
            _, return_data = await self.multicall.functions.aggregate(calls).call()
        except Exception as e:
            if self.rpc.is_rate_limit_error(e):
                await self.rpc.handle_rate_limit()
            else:
                logger.error(f"❌ [{task_name}] Multicall failed on block {block_number}: {e}")
            return []

        results = []
        for i, raw_bytes in enumerate(return_data):
            user = targets[i]
            try:
                decoded_data = decode(['uint256', 'uint256', 'uint256'], raw_bytes)
                error_code = decoded_data[0]
                liquidity = decoded_data[1]
                shortfall = decoded_data[2]

                if error_code != 0:
                    continue

                if shortfall > 0:
                    hf = Decimal('0.5') # Translates to Tier 1
                elif liquidity < 500 * 10**18:
                    hf = Decimal('1.1') # Translates to Tier 2
                else:
                    hf = Decimal('2.0') # Immune

                results.append((user, hf, 0.0, 0.0))

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

        live_targets_data = []

        for user, hf, coll_val, debt_val in results:
            hf_float = float(hf)
            live_targets_data.append((user, hf_float, debt_val, coll_val))

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

        for user, hf, coll_val, debt_val in results:
            hf_float = float(hf)
            live_targets_data.append((user, hf_float, debt_val, coll_val))

            if 0 < hf < TIER_1_MAX_HF:
                promoted.append(user)
                logger.info(f"⬆️ PROMOTED to Tier 1: {user} (HF: {hf:.4f})")
            elif hf > TIER_2_MAX_HF or hf == 0:
                pass
            else:
                remaining_t2.append(user)

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
    # MAIN BLOCK PROCESSOR
    # ================================================================

    async def process_block(self, block_number):
        """Process a single new block: run Sniper, optionally run Scout."""
        start_time = time.time()

        await self.update_prices()
        await self.load_targets_async()

        total_targets = len(self.tier_1_danger) + len(self.tier_2_watchlist)
        if total_targets == 0:
            return

        sniper_data, sniper_ms = await self.sniper_scan(block_number)

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
        # --- STARTUP PROTECTION ---
        # Jitter to prevent all PM2 instances from hitting RPC simultaneously on boot
        await asyncio.sleep(random.uniform(1.0, 10.0))
        while True:
            try:
                await self.rpc.connect()
                await self.init_contracts()

                await self.send_telegram_alert("🟢 <b>Lodestar Bot Started (HTTP Polling)</b>")
                logger.info("🚀 Lodestar Bot Engine started. Sniper + Scout architecture active.")
                
                await asyncio.sleep(random.uniform(0.5, 2.0))
                self.last_processed_block = await self.w3.eth.block_number
                logger.info(f"📍 Starting from block: {self.last_processed_block}")

                await self.load_targets_async()
                break # Success, exit startup loop

            except Exception as e:
                if self.rpc.is_rate_limit_error(e):
                    logger.warning("🐌 Rate limit on STARTUP. Yielding to backoff...")
                    await self.rpc.handle_rate_limit()
                else:
                    logger.error(f"💥 Fatal Startup Error: {e}")
                    await asyncio.sleep(60)
        logger.info(f"📊 Initial targets: Tier 1: {len(self.tier_1_danger)} | Tier 2: {len(self.tier_2_watchlist)}")

        sentinel = MarketSentinel()

        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.SUB)
        socket.connect("tcp://127.0.0.1:5555")
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
        logger.info("🎧 Subscribed to ZeroMQ Block Emitter.")

        while True:
            try:
                block_msg = await socket.recv_string()
                current_block = int(block_msg)

                if current_block <= self.last_processed_block:
                    continue

                if not await sentinel.should_scan():
                    continue

                await asyncio.sleep(random.uniform(2.0, 8.0))

                self.last_processed_block = current_block
                await self.process_block(current_block)
                sentinel.update_last_price()

            except Exception as e:
                if self.rpc.is_rate_limit_error(e):
                    await self.rpc.handle_rate_limit()
                    await self.init_contracts()
                else:
                    logger.error(f"⚠️ Polling Error: {e}")
                    await self.send_telegram_alert(
                        f"⚠️ <b>Polling Error:</b> <code>{e}</code>",
                        is_error=True
                    )
                    await asyncio.sleep(1)

if __name__ == "__main__":
    bot = LodestarBot()
    try:
        asyncio.run(bot.run_forever())
    except KeyboardInterrupt:
        print("🛑 Lodestar Bot Stopped.")
    except Exception as e:
        print(f"💥 Fatal Error: {e}")
