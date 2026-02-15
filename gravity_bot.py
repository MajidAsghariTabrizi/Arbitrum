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
    logger.error("❌ WSS_URL not found in .env! Required for WebSocket mode.")
    exit(1)

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


# --- 2. ASYNC BOT CLASS ---

class GravitySniperWSS:
    def __init__(self):
        # WSS Connection — set inside persistent_websocket context
        self.w3 = None
        self.account = None

        # Contracts — initialized after w3 is ready
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
        self.semaphore = asyncio.Semaphore(5)
        self.nonce_lock = asyncio.Lock()
        self._last_errors = {}

    async def init_contracts(self):
        """Initialize all contracts after w3 is ready inside the WSS context."""
        self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        logger.info(f"🔑 Loaded Liquidator Wallet: {self.account.address}")

        self.pool = self.w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
        self.data_provider = self.w3.eth.contract(address=DATA_PROVIDER_ADDRESS, abi=DATA_PROVIDER_ABI)
        self.addresses_provider = self.w3.eth.contract(address=POOL_ADDRESSES_PROVIDER, abi=ADDRESSES_PROVIDER_ABI)
        self.liquidator_contract = self.w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)

        # Fetch Oracle address dynamically
        oracle_addr = await self.addresses_provider.functions.getPriceOracle().call()
        self.oracle_contract = self.w3.eth.contract(address=oracle_addr, abi=ORACLE_ABI)

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
            payload = {"embeds": [{"title": "🦅 Gravity WSS Sniper", "description": msg, "color": color}]}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(DISCORD_WEBHOOK, json=payload))
        except Exception:
            pass

    async def send_telegram_alert(self, msg, is_error=False):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return

        # Anti-spam: skip duplicate error alerts within cooldown period
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

        # Params
        fee = 3000          # 0.3% Uniswap fee tier
        amount_out_min = 0  # TODO: Slippage calc
        sqrt_price_limit = 0

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
            # are liquidatable in the same block
            # ============================================================
            async with self.nonce_lock:
                nonce = await self.w3.eth.get_transaction_count(self.account.address)

                # Gas Estimation (with safe fallback)
                try:
                    gas_est = await tx_func.estimate_gas({'from': self.account.address})
                    gas_limit = int(gas_est * 1.2)
                except Exception:
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

    async def check_user_health(self, user):
        """Concurrent health check protected by Semaphore."""
        async with self.semaphore:
            try:
                data = await self.pool.functions.getUserAccountData(user).call()
                hf = Decimal(data[5]) / Decimal(10**18)

                if hf < 1.0 and hf > 0:
                    logger.info(f"💀 LIQUIDATABLE: {user} (HF: {hf})")
                    await self.execute_liquidation(user)
            except Exception:
                pass

    async def on_new_block(self, block_number):
        """Event Handler: Triggered on every new WSS block header."""
        start_time = time.time()

        # 1. Update Prices
        await self.update_prices()

        # 2. Reload targets
        await self.load_targets_async()
        if not self.targets:
            return

        # 3. Concurrent Health Checks (semaphore limits to 5 active)
        tasks = [self.check_user_health(user) for user in self.targets]
        await asyncio.gather(*tasks)

        elapsed = (time.time() - start_time) * 1000
        logger.info(f"🧱 Block {block_number} scanned. {len(self.targets)} targets in {elapsed:.0f}ms")

    async def run_forever(self):
        """Main WSS Loop using Web3.py v6 persistent_websocket context manager."""
        while True:
            try:
                logger.info(f"🔌 Connecting to WSS: {WSS_URL[:40]}...")

                # ============================================================
                # Web3.py v6+ PERSISTENT WEBSOCKET CONTEXT MANAGER
                # This is the ONLY supported way to use async WSS in v6.
                # The context manager handles connection lifecycle, keepalive,
                # and clean teardown automatically.
                # ============================================================
                async with AsyncWeb3.persistent_websocket(WSS_URL) as w3:
                    self.w3 = w3
                    logger.info("🟢 WSS Connected Successfully!")

                    # Initialize all contracts now that w3 is live
                    await self.init_contracts()

                    await self.send_telegram_alert("🟢 <b>WSS Sniper Started</b>")

                    # Subscribe to newHeads
                    subscription_id = await self.w3.eth.subscribe('newHeads')
                    logger.info(f"✅ Subscribed to newHeads (ID: {subscription_id})")

                    # ============================================================
                    # EVENT LOOP — process_subscriptions yields each new block
                    # header as it arrives over the persistent WSS connection.
                    # ============================================================
                    async for response in self.w3.ws.process_subscriptions():
                        try:
                            # Extract block header from response
                            header = response.get('result', {})
                            if not header and 'params' in response:
                                header = response['params'].get('result', {})

                            if 'number' in header:
                                block_num = header['number']
                                # Parse hex block number if needed
                                if isinstance(block_num, str):
                                    block_num = int(block_num, 16)
                                await self.on_new_block(block_num)
                        except Exception as loop_err:
                            logger.error(f"Block Processing Error: {loop_err}")

            except Exception as e:
                logger.error(f"🔌 WSS Connection Lost: {e}")
                await self.send_telegram_alert(
                    f"⚠️ <b>WSS Disconnected:</b> <code>{e}</code>",
                    is_error=True
                )
                # Reconnect delay — prevents rapid reconnect spam
                await asyncio.sleep(5)


if __name__ == "__main__":
    bot = GravitySniperWSS()
    try:
        asyncio.run(bot.run_forever())
    except KeyboardInterrupt:
        print("\n🛑 Bot Stopped.")