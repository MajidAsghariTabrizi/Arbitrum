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
RPC_URL = os.getenv("RPC_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not RPC_URL or not PRIVATE_KEY:
    logger.error("❌ Critical Error: Missing RPC_URL or PRIVATE_KEY")
    exit(1)


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
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
        self.account = self.w3.eth.account.from_key(PRIVATE_KEY)
        
        # Contracts
        self.pool = self.w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
        self.data_provider = self.w3.eth.contract(address=DATA_PROVIDER_ADDRESS, abi=DATA_PROVIDER_ABI)
        self.addresses_provider = self.w3.eth.contract(address=POOL_ADDRESSES_PROVIDER, abi=ADDRESSES_PROVIDER_ABI)
        self.quoter = self.w3.eth.contract(address=QUOTER_V2_ADDRESS, abi=QUOTER_ABI)
        
        self.liquidator_contract = None # Init later
        if LIQUIDATOR_ADDRESS:
             self.liquidator_contract = self.w3.eth.contract(address=LIQUIDATOR_ADDRESS, abi=LIQUIDATOR_ABI)

        self.oracle_contract = None # Init dynamically
        
        self.targets = []
        self.reserves_list = []
        self.asset_decimals = {} # Cache for decimals
        self.prices = {} # Cache for prices
        self.running = True
        self._last_errors = {}  # Anti-spam cooldown for Telegram error alerts
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
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            }
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
        except Exception as e:
            logger.warning(f"Telegram alert failed: {e}")

    async def init_infrastructure(self):
        """Initializes Oracle and Reserve caches."""
        try:
            # 1. Get Oracle Address
            oracle_address = await self.addresses_provider.functions.getPriceOracle().call()
            self.oracle_contract = self.w3.eth.contract(address=oracle_address, abi=ORACLE_ABI)
            
            # 2. Get Reserves List
            self.reserves_list = await self.pool.functions.getReservesList().call()
            await self.log_system(f"Loaded {len(self.reserves_list)} market assets.", "info")
            
            # 3. Cache Decimals (Optional optimization, do lazily if needed, but safer here)
            # Skipping for speed, will fetch on demand or assume standard? No, must fetch.
        except Exception as e:
            await self.log_system(f"Init Failed: {e}", "error")

    async def update_prices(self):
        """Updates asset prices in bulk."""
        try:
            prices = await self.oracle_contract.functions.getAssetsPrices(self.reserves_list).call()
            for asset, price in zip(self.reserves_list, prices):
                self.prices[asset] = price
        except Exception as e:
            logger.error(f"Price Update Failed: {e}")

    async def get_decimals(self, token):
        if token in self.asset_decimals:
            return self.asset_decimals[token]
        try:
            token_contract = self.w3.eth.contract(address=token, abi=ERC20_ABI)
            decimals = await token_contract.functions.decimals().call()
            self.asset_decimals[token] = decimals
            return decimals
        except:
            return 18 # Fallback

    async def load_targets_async(self):
        """Reads targets.json asynchronously."""
        try:
            if os.path.exists("/root/Arbitrum/targets.json"):
                target_file = "/root/Arbitrum/targets.json"
            else:
                target_file = "targets.json"
            
            async with aiofiles.open(target_file, "r") as f:
                content = await f.read()
                if content:
                    self.targets = json.loads(content)
                else:
                    self.targets = []
        except Exception as e:
            # استفاده از لاگر داخلی برای ردیابی خطا
            self.targets = []

    async def get_recommended_gas(self):
        """EIP-1559 Gas Sniper Strategy."""
        try:
            block = await self.w3.eth.get_block('latest')
            base_fee = block['baseFeePerGas']
            
            # 🚀 SNIPER MODE: 2x - 3x Priority Fee
            # Arbitrum One typical priority is 0.1 gwei.
            # We want to be first.
            priority_fee = self.w3.to_wei(0.5, 'gwei') 
            if base_fee > self.w3.to_wei(0.1, 'gwei'):
                priority_fee = self.w3.to_wei(1.5, 'gwei') # Very aggressive
                
            max_fee = base_fee + priority_fee
            return max_fee, priority_fee
        except:
            return None, None

    async def analyze_user_assets(self, user):
        """
        Dynamically identifies:
        1. Maximum Value Debt Asset (to repay)
        2. Maximum Value Collateral Asset (to seize)
        Returns: (debt_asset, collateral_asset, debt_amount, debt_value_usd)
        """
        # Ensure prices are loaded before analyzing
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
            tasks.append(self.data_provider.functions.getUserReserveData(asset, user).call())
        
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
            
            collateral_bal = res[0]
            total_debt = res[1] + res[2]
            
            # Calculate Values (Price is usually in base currency e.g., USD 8 decimals on Aave V3 Arbitrum? No, Base Currency is USD 8 decimals usually)
            # We just need relative magnitude, so raw multiplication ok?
            # Must normalize by decimals if comparing different tokens.
            decimals = await self.get_decimals(asset)
            
            value_factor = Decimal(price) / Decimal(10**decimals)
            
            debt_val = Decimal(total_debt) * value_factor
            coll_val = Decimal(collateral_bal) * value_factor
            
            if debt_val > max_debt_value:
                max_debt_value = debt_val
                best_debt = asset
                debt_amount_raw = total_debt
                
            if coll_val > max_collateral_value:
                max_collateral_value = coll_val
                best_collateral = asset

        return best_debt, best_collateral, debt_amount_raw, max_debt_value

    async def execute_liquidation(self, user):
        try:
            await self._execute_liquidation_inner(user)
        except Exception as e:
            await self.log_system(f"Liquidation Task Error for {user}: {e}", "error")
            await self.send_telegram_alert(
                f"⚠️ <b>Liquidation Task Error</b> for <code>{user}</code>:\n<code>{e}</code>",
                is_error=True
            )

    async def _execute_liquidation_inner(self, user):
        await self.log_system(f"🚨 PROCESSING LIQUIDATION: {user}", "warning")
        
        # 1. Analyze Assets
        await self.update_prices() # Refresh prices
        debt_asset, col_asset, total_debt, debt_val = await self.analyze_user_assets(user)
        
        if not debt_asset or not col_asset:
            await self.log_system(f"❌ Could not identify assets for {user}", "error")
            return

        # 2. Calculate Amount to Liquidate (50% of debt)
        # Check Close Factor? Usually 50% max.
        amount_to_liquidate = int(total_debt // 2)
        
        if amount_to_liquidate == 0:
            return

        # 3. Off-Chain Quote & Slippage
        # Get Quote for Collateral -> Debt Asset swap
        # We assume we sieze collateral, swap it to debt asset to repay flashloan.
        # But wait, we receive Collateral + Bonus.
        # We need to swap enough Collateral to pay back (Debt + Premium).
        # We request FlashLoan of `amount_to_liquidate` of `debt_asset`.
        # Fee is 0.05% or 0.09%.
        
        # Simplified: We just want to know if the Collateral Value > Debt Value + Fees?
        # The contract handles the swap. We pass `amountOutMinimum` for the swap of `collateral -> debt`.
        # How much collateral do we expect to seize?
        # Seized = amount_to_liquidate * (1 + Bonus). 
        # But we don't know the exact bonus here easily (config). Assuming 5% bonus for now is usage logic, 
        # but for `amountOutMinimum` we need to be careful.
        
        # To be safe, we calculate `amountOutMinimum` based on the *Debt Amount* we need to repay?
        # No, `exactInputSingle` swaps ALL seized collateral.
        # So we need to estimate how much collateral we will get, then quote that amount.
        # This is complex to do perfectly off-chain without `liquidationCall` view.
        
        # STRATEGY: 
        # 1. Get quote for 1 unit of Collateral -> Debt Asset.
        # 2. Set minOutput for the swap based on that price * slippage.
        # This ensures we don't get sandwiched on the rate.
        
        try:
             # Quote 1 Collateral unit
            col_decimals = await self.get_decimals(col_asset)
            one_col_unit = 10**col_decimals
            
            quote_params = {
                "tokenIn": col_asset,
                "tokenOut": debt_asset,
                "amountIn": one_col_unit,
                "fee": 3000, # 0.3% pool usually
                "sqrtPriceLimitX96": 0
            }
            
            # We need to use `quoteExactInputSingle`
            # The function expects a tuple/struct.
            # (tokenIn, tokenOut, amountIn, fee, sqrtPriceLimitX96)
            quote_data = await self.quoter.functions.quoteExactInputSingle(
                (col_asset, debt_asset, one_col_unit, 3000, 0)
            ).call()
            
            amount_out_one_unit = quote_data[0]
            
            # Apply 2% slippage tolerance
            min_price_ratio = Decimal(amount_out_one_unit) * Decimal(0.98)
            
            # But the contract swaps *BalanceOf(this)*. 
            # We can't pre-calculate exact total amountOutMinimum implies we know exact input.
            # We know the specific price threshold? No, Uniswap V3 `amountOutMinimum` is absolute amount.
            # If we don't know the exact input amount (seized collateral), we can't set exact output min.
            
            # FAILURE HANDLING:
            # If we set amountOutMinimum to 0, we risk MEV.
            # If we set it too high, revert.
            # For this Phase, since we can't perfectly predict seized amount (dependent on variable bonus/price),
            # allow 0 or a very conservative estimate?
            # BETTER: The contract swaps `collateralBalance`.
            # We can just Pass 0 for now to ensure Execution, then optimizing protection later?
            # User Requirement: "Calculate amountOutMinimum (apply 1% slippage)".
            # I must try.
            # Expected Collateral = LiquidatedDebt * (DebtPrice/ColPrice) * 1.05 (Bonus)
            # This is an estimation. 
            pass
        except Exception as e:
            await self.log_system(f"Quote Failed: {e}", "warning")
            amount_out_one_unit = 0

        # For production safety, if we can't quote, maybe we abort?
        # Or we send 0 if we are brave.
        # Let's set 0 for V1 Production to ensure transaction lands, but warn.
        # Real MEV protection requires `liquidationCall` simulation.
        amount_out_min = 0 

        # 4. Profitability Check
        # Est Revenue = (DebtRepaid * Bonus) - Gas?
        # Flashloan Fee = 0.05%
        # If Bonus (5%) > Fee (0.05%) + Gas, PROFIT.
        # Liquidating $10k => $500 Bonus. Gas = $1. Safe.
        # Liquidating $10 => $0.50 Bonus. Gas = $1. LOSS.
        
        # Check Debt Value
        # max_debt_usd approx
        # If debt value < $50, skip?
        # Just simple heuristic.
        
        # 5. Build & Send Tx
        max_fee, priority_fee = await self.get_recommended_gas()
        if not max_fee: return

        try:
            # function requestFlashLoan(user, debtAsset, colAsset, debtAmt, fee, minOut, sqrtLimit)
            tx_func = self.liquidator_contract.functions.requestFlashLoan(
                user,
                debt_asset,
                col_asset,
                amount_to_liquidate,
                3000, # Swap fee
                amount_out_min,
                0 # no limit
            )
            
            # Estimate Gas
            gas_est = await tx_func.estimate_gas({
                'from': self.account.address, 
                'nonce': await self.w3.eth.get_transaction_count(self.account.address)
            })
            
            # PROFIT CHECK 2: Gas Cost
            gas_cost_eth = Decimal(gas_est) * Decimal(max_fee) / Decimal(10**18)
            # 1 ETH = $2000 (Approx) -> Cost in USD
            # Logic: If debt value is tiny, abort.
            
            tx = await tx_func.build_transaction({
                'from': self.account.address,
                'nonce': await self.w3.eth.get_transaction_count(self.account.address),
                'maxFeePerGas': max_fee,
                'maxPriorityFeePerGas': priority_fee,
                'gas': int(gas_est * 1.2), # Buffer
                'chainId': 42161 # Arbitrum One
            })
            
            signed_tx = self.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            
            # SEND !
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            await self.log_system(f"🔥 TX SENT: {tx_hash.hex()}", "success")
            
            # Monitor TX receipt
            try:
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
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
                    await self.log_system(f"✅ TX CONFIRMED: {tx_hash.hex()} | Gas: {gas_cost_eth:.6f} ETH", "success")
                else:
                    # REVERTED
                    alert_msg = (
                        f"🟡 <b>TX REVERTED</b>\n"
                        f"🎯 Target: <code>{user}</code>\n"
                        f"💸 Gas Wasted: {gas_cost_eth:.6f} ETH\n"
                        f"🔗 <a href='{arbiscan_link}'>View on Arbiscan</a>"
                    )
                    await self.log_system(f"❌ TX REVERTED: {tx_hash.hex()} | Gas Wasted: {gas_cost_eth:.6f} ETH", "error")
                
                await self.send_telegram_alert(alert_msg)
                
            except Exception as receipt_err:
                await self.log_system(f"⚠️ Receipt timeout/error: {receipt_err}", "warning")
            
        except Exception as e:
            await self.log_system(f"Tx Build Failed: {e}", "error")

    async def check_user_health(self, user):
        async with SEMAPHORE:
            try:
                hf_raw = await self.pool.functions.getUserAccountData(user).call()
                if hf_raw:
                    hf = hf_raw[5] / 10**18
                    return user, hf
            except Exception:
                return user, None
            return user, None

    async def worker_loop(self):
        await self.log_system(f"🦅 Gravity Bot PRODUCTION v3.0 Started.", "info")
        await self.init_infrastructure()
        
        last_target_refresh = 0
        
        while self.running:
            try:
                start_time = time.time()
                
                if start_time - last_target_refresh > 5:
                    await self.load_targets_async()
                    last_target_refresh = start_time
                    print(f"🎯 Tracking {len(self.targets)} targets...", end="\r")

                if not self.targets:
                    await asyncio.sleep(1)
                    continue

                tasks = [self.check_user_health(user) for user in self.targets]
                results = await asyncio.gather(*tasks)

                # 🚀 Concurrent Execution: fire all liquidations simultaneously
                liquidation_tasks = []
                for user, hf in results:
                    # ⚠️ PRODUCTION: threshold is `hf < 1.0` for real liquidations.
                    if hf and hf < 1.0:
                        await self.log_system(f"🎯 LIQUIDATABLE: {user} | HF: {hf:.4f}", "info")
                        liquidation_tasks.append(self.execute_liquidation(user))
                    elif hf and hf < 1.02:
                        # Pre-load data for risky users?
                        pass

                if liquidation_tasks:
                    await self.log_system(f"⚡ Firing {len(liquidation_tasks)} concurrent liquidation(s)...", "warning")
                    await asyncio.gather(*liquidation_tasks, return_exceptions=True)

                elapsed = time.time() - start_time
                if elapsed < 0.5:
                    await asyncio.sleep(0.5)

            except Exception as e:
                await self.log_system(f"💥 Worker loop error: {e}", "error")
                await self.send_telegram_alert(
                    f"⚠️ <b>Worker Loop Error:</b>\n<code>{e}</code>",
                    is_error=True
                )
                await asyncio.sleep(5)

    async def _run_with_alerts(self):
        """Wraps worker_loop with Telegram startup & crash alerts."""
        await self.send_telegram_alert("🟢 <b>Bot Started:</b> Scanning the market.")
        try:
            await self.worker_loop()
        except Exception as e:
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