import os
import json
import asyncio
import logging
import time
import warnings
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
POLL_INTERVAL = 0.1         # 100ms — check for new blocks rapidly

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


# --- 2. ASYNC RPC MANAGER ---

class AsyncRPCManager:
    """Manages RPC endpoints with automatic failover."""
    def __init__(self):
        self.endpoints = [PRIMARY_RPC] + FALLBACK_RPCS
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
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
        if not await self.w3.is_connected():
            raise ConnectionError(f"❌ Failed to connect to {url}")
        logger.info(f"🟢 Connected to RPC [{self.current_index + 1}/{len(self.endpoints)}]")

    async def handle_rate_limit(self):
        """Handle 429 errors with adaptive backoff and failover."""
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


# --- 3. ASYNC BOT CLASS ---

class GravityBot:
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

        # Data
        self.targets = []
        self.reserves_list = []
        self.asset_decimals = {}
        self.prices = {}

        # Block Tracking
        self.last_processed_block = 0

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
            payload = {"embeds": [{"title": "🦅 Gravity Bot", "description": msg, "color": color}]}
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

    async def load_targets_async(self):
        """Reloads targets.json asynchronously."""
        try:
            path = "/root/Arbitrum/targets.json" if os.path.exists("/root/Arbitrum") else "targets.json"
            async with aiofiles.open(path, mode='r') as f:
                content = await f.read()
                if content:
                    self.targets = json.loads(content)
                else:
                    self.targets = []
        except Exception:
            self.targets = []

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

    async def execute_liquidation(self, user):
        """Builds, signs, and sends Flash Loan liquidation transaction."""
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

    async def process_block(self, block_number):
        """Process a single new block: batch-check all targets via Multicall3."""
        start_time = time.time()

        # 1. Update Prices
        await self.update_prices()

        # 2. Reload targets
        await self.load_targets_async()
        if not self.targets:
            return

        # 3. Batch Health Checks via Multicall3
        # Build a list of (target, callData) tuples for aggregate()
        calls = []
        for user in self.targets:
            call_data = self.pool.encodeABI(fn_name="getUserAccountData", args=[user])
            calls.append((POOL_ADDRESS, call_data))

        try:
            # Single batched RPC call replaces N individual calls
            _, return_data = await self.multicall.functions.aggregate(calls).call()
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "rate" in err_str:
                await self.rpc.handle_rate_limit()
            else:
                logger.error(f"❌ Multicall failed on block {block_number}: {e}")
            return

        # 4. Decode results and check for liquidatable positions
        for i, raw_bytes in enumerate(return_data):
            user = self.targets[i]
            try:
                decoded_data = decode(
                    ['uint256', 'uint256', 'uint256', 'uint256', 'uint256', 'uint256'],
                    raw_bytes
                )
                hf = Decimal(decoded_data[5]) / Decimal(10**18)

                if 0 < hf < Decimal('1.0'):
                    logger.info(f"💀 LIQUIDATABLE: {user} (HF: {hf})")
                    await self.execute_liquidation(user)
            except Exception as e:
                logger.warning(f"⚠️ Failed to decode data for {user}: {e}")
                continue

        elapsed = (time.time() - start_time) * 1000
        logger.info(f"🧱 Block {block_number} scanned. {len(self.targets)} targets in {elapsed:.0f}ms")

    async def run_forever(self):
        """Main Smart HTTP Polling Loop — tracks new blocks and processes them."""
        # Connect and initialize
        await self.rpc.connect()
        await self.init_contracts()

        await self.send_telegram_alert("🟢 <b>Gravity Bot Started (HTTP Polling)</b>")
        logger.info("🚀 Smart HTTP Polling Engine started.")

        # Seed the block tracker
        self.last_processed_block = await self.w3.eth.block_number
        logger.info(f"📍 Starting from block: {self.last_processed_block}")

        # ============================================================
        # SMART POLLING LOOP
        # - Checks for new blocks every POLL_INTERVAL (100ms)
        # - Only processes when a genuinely new block is detected
        # - Handles RPC failures with reconnect logic
        # ============================================================
        while True:
            try:
                current_block = await self.w3.eth.block_number

                if current_block > self.last_processed_block:
                    # New block(s) detected — process the latest one
                    self.last_processed_block = current_block
                    await self.process_block(current_block)
                else:
                    # No new block yet — wait briefly and re-check
                    await asyncio.sleep(POLL_INTERVAL)

            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "rate" in err_str:
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
    bot = GravityBot()
    try:
        asyncio.run(bot.run_forever())
    except KeyboardInterrupt:
        print("\n🛑 Bot Stopped.")