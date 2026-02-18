"""
═══════════════════════════════════════════════════════════════════════════════
🛸 ANTI-GRAVITY — DEX Arbitrage Engine (Arbitrum One)
═══════════════════════════════════════════════════════════════════════════════
Production-grade atomic arbitrage scanner & executor.
Scans Uniswap V3, SushiSwap V3, and Camelot DEX for cross-DEX price spreads.
Uses Aave V3 flashloans for zero-capital execution.

Architecture:
  - AsyncWeb3 + asyncio event loop
  - Polls every new block on Arbitrum (~250ms blocks)
  - Fetches quotes from 3 DEX quoters per token pair
  - Calculates net profit after flashloan fees, swap fees, and gas
  - Simulates via eth_call before broadcasting
  - Logs all spreads and executions to SQLite via db_manager.py
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import os
import json
import logging
import time
import traceback
from decimal import Decimal
from itertools import permutations
from typing import Dict, List, Optional, Tuple

import requests as req_sync
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError
from eth_abi import encode, decode
from hexbytes import HexBytes

import db_manager

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("arb_engine.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("ArbEngine")

# ═══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT
# ═══════════════════════════════════════════════════════════════════════════════
load_dotenv()

# RPC — unified convention: PRIMARY_RPC is the main HTTP endpoint (matches gravity_bot.py)
PRIMARY_RPC = os.getenv("PRIMARY_RPC")
if not PRIMARY_RPC:
    PRIMARY_RPC = os.getenv("RPC_URL", "")

FALLBACK_RPCS = [r.strip() for r in os.getenv("FALLBACK_RPCS", "").split(",") if r.strip()]
# ALL_RPCS = [PRIMARY_RPC] + FALLBACK_RPCS  <-- managed by AsyncRPCManager now

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
DEX_ARBITRAGEUR_ADDRESS = os.getenv("DEX_ARBITRAGEUR_ADDRESS", "")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

if not PRIMARY_RPC:
    logger.critical("❌ PRIMARY_RPC not found in .env — exiting")
    exit(1)

if not PRIVATE_KEY:
    logger.warning("⚠️  PRIVATE_KEY not set — execution will be disabled (scan-only mode)")

if not DEX_ARBITRAGEUR_ADDRESS or DEX_ARBITRAGEUR_ADDRESS == "0x0000000000000000000000000000000000000000":
    logger.warning("⚠️  DEX_ARBITRAGEUR_ADDRESS not set — deploy contract first, then add to .env")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logger.warning("⚠️  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — Telegram alerts disabled")


def send_telegram_alert(msg: str):
    """Send an HTML-formatted Telegram notification (fire-and-forget)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        req_sync.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════════
# MULTICALL3 CONFIGURATION (Arbitrum One)
# ═══════════════════════════════════════════════════════════════════════════════
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_ABI = [
    {
        "inputs": [
            {"name": "requireSuccess", "type": "bool"},
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "callData", "type": "bytes"}
                ],
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "tryAggregate",
        "outputs": [
            {
                "components": [
                    {"name": "success", "type": "bool"},
                    {"name": "returnData", "type": "bytes"}
                ],
                "name": "returnData",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# ═══════════════════════════════════════════════════════════════════════════════
# TOKEN CONFIGURATION — Arbitrum Mainnet (Real Addresses)
# ═══════════════════════════════════════════════════════════════════════════════
TOKENS: Dict[str, dict] = {
    "WETH":   {"address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "decimals": 18},
    "ARB":    {"address": "0x912CE59144191C1204E64559FE8253a0e49E6548", "decimals": 18},
    "MAGIC":  {"address": "0x539bdE0d7Dbd33f84E8aaf9084C942D9800Ef002", "decimals": 18},
    "GRAIL":  {"address": "0x3d9907F9a368ad0a51Be60f7Da3b97cf940982D8", "decimals": 18},
    "PENDLE": {"address": "0x0c880f6761F1af8d9Aa9C466984785263cf79560", "decimals": 18},
    "GMX":    {"address": "0xfc5A1A6EB076a2C7AD06EDb220f4daaC9AF172af", "decimals": 18},
    "RDNT":   {"address": "0x3082CC23568eA640225c2467653dB90e9250AaA0", "decimals": 18},
}

# Base quote token
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC (Arbitrum)
USDC_DECIMALS = 6

# Fixed flashloan size in USDC (6 decimals). $1000 USDC = 1_000_000_000
FLASHLOAN_USDC_AMOUNT = 1000 * 10**USDC_DECIMALS  # $1,000

# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC RPC MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class AsyncRPCManager:
    """Manages RPC endpoints with automatic failover for 429 AND 403 errors."""
    def __init__(self):
        self.endpoints = [PRIMARY_RPC] + FALLBACK_RPCS
        self.current_index = 0
        self.strike_count = 0
        self.w3 = None

    async def connect(self):
        """Connect to the current RPC endpoint. Closes any existing session first."""
        # Gracefully close the previous aiohttp session
        if self.w3 and hasattr(self.w3.provider, '_request_kwargs'):
            try:
                session = await self.w3.provider.cache_async_session(None)
                if session and not session.closed:
                    await session.close()
                    logger.info("🔒 Previous aiohttp session closed cleanly.")
            except Exception:
                pass

        url = self.endpoints[self.current_index]
        logger.info(f"🔌 Connecting to RPC: {url[:40]}...")
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
        if not await self.w3.is_connected():
            raise ConnectionError(f"❌ Failed to connect to {url}")
        logger.info(f"🟢 Connected to RPC [{self.current_index + 1}/{len(self.endpoints)}]")

    async def handle_rate_limit(self):
        """Handle 429/403 errors with adaptive backoff and failover."""
        self.strike_count += 1

        if self.strike_count >= 3:
            self.strike_count = 0
            self.current_index = (self.current_index + 1) % len(self.endpoints)
            logger.warning(f"🔄 3 strikes! Switching to RPC [{self.current_index + 1}/{len(self.endpoints)}]")
            await self.connect()
        else:
            cooldown = 2
            logger.warning(f"⏳ Rate limited (Strike {self.strike_count}/3). Cooling down {cooldown}s...")
            await asyncio.sleep(cooldown)

    async def get_w3(self) -> AsyncWeb3:
        if self.w3 is None:
            await self.connect()
        return self.w3

# ═══════════════════════════════════════════════════════════════════════════════
# DEX CONFIGURATION — Arbitrum Mainnet (Real Addresses)
# ═══════════════════════════════════════════════════════════════════════════════

# Uniswap V3 (Arbitrum)
UNI_V3_QUOTER   = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"  # QuoterV2
UNI_V3_ROUTER   = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"  # SwapRouter02

# SushiSwap V3 (Arbitrum) — Uses Uni V3 fork architecture
SUSHI_V3_QUOTER  = "0x0524E833cCD057e4d7A296e3aaAb9f7675964Ce1"  # SushiSwap V3 QuoterV2
SUSHI_V3_ROUTER  = "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c"  # SushiSwap V3 SwapRouter

# Camelot DEX (Arbitrum-native, Algebra V1.9-fork — uses algebraPool not tickSpacing)
CAMELOT_ROUTER   = "0xc873fEcbd354f5A56E00E710B9048C68fD3EA22B"  # Camelot V2 Router
CAMELOT_QUOTER   = "0x4a6eDa4451BcF25E07F1f55B77267e5B89975f68"  # Camelot Algebra Quoter

# DEX Registry
DEXES = {
    "Uniswap_V3": {
        "quoter": UNI_V3_QUOTER,
        "router": UNI_V3_ROUTER,
        "type": "v3",          # Standard UniV3 QuoterV2 interface
        "fee_tiers": [100, 500, 3000, 10000],  # 0.01%, 0.05%, 0.3%, 1%
    },
    "SushiSwap_V3": {
        "quoter": SUSHI_V3_QUOTER,
        "router": SUSHI_V3_ROUTER,
        "type": "v3",
        "fee_tiers": [100, 500, 3000, 10000],
    },
    "Camelot": {
        "quoter": CAMELOT_QUOTER,
        "router": CAMELOT_ROUTER,
        "type": "algebra",     # Algebra-style quoter (no fee param, dynamic fees)
        "fee_tiers": [0],      # Camelot uses dynamic fees internally
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# ABIs (Minimal — only what we call)
# ═══════════════════════════════════════════════════════════════════════════════

# UniV3 / SushiV3 QuoterV2.quoteExactInputSingle
QUOTER_V2_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn",            "type": "address"},
                    {"name": "tokenOut",           "type": "address"},
                    {"name": "amountIn",           "type": "uint256"},
                    {"name": "fee",                "type": "uint24"},
                    {"name": "sqrtPriceLimitX96",  "type": "uint160"},
                ],
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"name": "amountOut",              "type": "uint256"},
            {"name": "sqrtPriceX96After",      "type": "uint160"},
            {"name": "initializedTicksCrossed","type": "uint32"},
            {"name": "gasEstimate",            "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Camelot (Algebra) Quoter — quoteExactInputSingle without fee param
ALGEBRA_QUOTER_ABI = [
    {
        "inputs": [
            {"name": "tokenIn",           "type": "address"},
            {"name": "tokenOut",          "type": "address"},
            {"name": "amountIn",          "type": "uint256"},
            {"name": "limitSqrtPrice",    "type": "uint160"},
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"name": "amountOut",         "type": "uint256"},
            {"name": "fee",              "type": "uint16"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# UniV3 SwapRouter02 exactInputSingle
SWAP_ROUTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn",       "type": "address"},
                    {"name": "tokenOut",      "type": "address"},
                    {"name": "fee",           "type": "uint24"},
                    {"name": "recipient",     "type": "address"},
                    {"name": "amountIn",      "type": "uint256"},
                    {"name": "amountOutMinimum", "type": "uint256"},
                    {"name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "exactInputSingle",
        "outputs": [{"name": "amountOut", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    }
]

# DexArbitrageur.requestFlashLoan
ARB_CONTRACT_ABI = [
    {
        "inputs": [
            {"name": "_token",  "type": "address"},
            {"name": "_amount", "type": "uint256"},
            {"name": "_params", "type": "bytes"},
        ],
        "name": "requestFlashLoan",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
AAVE_FLASHLOAN_FEE_BPS = 5        # 0.05% = 5 basis points
MIN_PROFIT_USD = 5.00              # $5 minimum — accounts for gas spikes + slippage drift
MAX_GAS_PRICE_GWEI = 1.0          # Arbitrum gas is cheap, but cap it
SCAN_COOLDOWN_SECONDS = 0.5       # Minimum time between scans
MAX_SLIPPAGE_BPS = 50             # 0.5% max slippage for trade sizing
SAFETY_MARGIN_MULTIPLIER = 1.5    # Extra margin on cost estimates to avoid NotProfitable
LEG_A_SLIPPAGE_BPS = 50           # 0.5% slippage tolerance on Leg A output
MULTICALL_CHUNK_SIZE = 15         # Max calls per tryAggregate to avoid gas limits

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTE CONFIDENCE — Tracks simulation failures per route
# Routes with repeated failures get temporarily blacklisted.
# ═══════════════════════════════════════════════════════════════════════════════
MAX_ROUTE_FAILURES = 3            # After 3 sim failures, skip route
ROUTE_COOLDOWN_SECONDS = 600      # 10-minute cooldown after blacklist
route_failures: Dict[str, int] = {}     # "TOKEN/dex_a/dex_b" -> failure count
route_blacklist: Dict[str, float] = {}  # "TOKEN/dex_a/dex_b" -> blacklist timestamp

# ═══════════════════════════════════════════════════════════════════════════════
# QUOTE FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

async def get_v3_quote_robust(
    rpc_manager: AsyncRPCManager,
    semaphore: asyncio.Semaphore,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
) -> Optional[int]:
    """Robust wrapper for get_v3_quote with retries."""
    retries = 0
    while retries < 3:
        try:
            w3 = await rpc_manager.get_w3()
            async with semaphore:
                quoter = w3.eth.contract(
                    address=w3.to_checksum_address(quoter_address),
                    abi=QUOTER_V2_ABI,
                )
                result = await quoter.functions.quoteExactInputSingle(
                    (
                        w3.to_checksum_address(token_in),
                        w3.to_checksum_address(token_out),
                        amount_in,
                        fee,
                        0,
                    )
                ).call()
                return result[0]
        except ContractLogicError:
            # Pool doesn't exist or revert — legitimate failure
            return None
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "403" in err_str or "connection" in err_str:
                await rpc_manager.handle_rate_limit()
                retries += 1
            else:
                return None
    return None


async def get_algebra_quote_robust(
    rpc_manager: AsyncRPCManager,
    semaphore: asyncio.Semaphore,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
) -> Optional[int]:
    """Robust wrapper for get_algebra_quote with retries."""
    retries = 0
    while retries < 3:
        try:
            w3 = await rpc_manager.get_w3()
            async with semaphore:
                quoter = w3.eth.contract(
                    address=w3.to_checksum_address(quoter_address),
                    abi=ALGEBRA_QUOTER_ABI,
                )
                result = await quoter.functions.quoteExactInputSingle(
                    w3.to_checksum_address(token_in),
                    w3.to_checksum_address(token_out),
                    amount_in,
                    0,
                ).call()
                return result[0]
        except ContractLogicError:
            return None
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "403" in err_str or "connection" in err_str:
                await rpc_manager.handle_rate_limit()
                retries += 1
            else:
                return None
    return None




# ═══════════════════════════════════════════════════════════════════════════════
# MULTICALL HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _encode_quoter_call(
    w3: AsyncWeb3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
    dex_type: str,
) -> Tuple[str, bytes]:
    """
    Generate (target, calldata) for a quote call.
    Does NOT make an RPC call.
    """
    quoter_contract = w3.eth.contract(
        address=w3.to_checksum_address(quoter_address),
        abi=QUOTER_V2_ABI if dex_type == "v3" else ALGEBRA_QUOTER_ABI,
    )
    
    if dex_type == "v3":
        # Checksum addresses strictly for encoding
        t_in = w3.to_checksum_address(token_in)
        t_out = w3.to_checksum_address(token_out)
        
        # quoteExactInputSingle((tokenIn, tokenOut, amountIn, fee, sqrtPriceLimitX96))
        call_fn = quoter_contract.functions.quoteExactInputSingle((
            t_in, t_out, amount_in, fee, 0
        ))
    else:  # algebra
        t_in = w3.to_checksum_address(token_in)
        t_out = w3.to_checksum_address(token_out)
        
        # quoteExactInputSingle(tokenIn, tokenOut, amountIn, limitSqrtPrice)
        call_fn = quoter_contract.functions.quoteExactInputSingle(
            t_in, t_out, amount_in, 0
        )
        
    # Extract raw calldata
    # We use _encode_transaction_data() which returns a hex string "0x..."
    hex_data = call_fn._encode_transaction_data()
    raw_data = bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)
    
    return (quoter_contract.address, raw_data)


def _decode_quoter_result(raw_bytes: bytes, dex_type: str) -> int:
    """
    Decode the return bytes from Multicall3.tryAggregate.
    Returns amountOut (int) or 0 on failure.
    """
    if not raw_bytes:
        return 0
    try:
        if dex_type == "v3":
            # Returns (amountOut, sqrtPriceX96After, initializedTicksCrossed, gasEstimate)
            # Tuple types: (uint256, uint160, uint32, uint256)
            decoded = decode(['uint256', 'uint160', 'uint32', 'uint256'], raw_bytes)
            return decoded[0]
        else:  # algebra
            # Returns (amountOut, fee)
            # Tuple types: (uint256, uint16)
            decoded = decode(['uint256', 'uint16'], raw_bytes)
            return decoded[0]
    except Exception:
        return 0


def estimate_net_profit_usd(
    gross_profit_usd: float,
    gas_cost_wei: int,
    eth_price_usd: float,
) -> float:
    """
    Net profit after gas costs with safety margin.
    Flashloan fee is already deducted in gross_profit_usd.
    """
    gas_cost_eth = gas_cost_wei / (10 ** 18)
    gas_cost_usd = gas_cost_eth * eth_price_usd * SAFETY_MARGIN_MULTIPLIER
    return gross_profit_usd - gas_cost_usd


# ═══════════════════════════════════════════════════════════════════════════════
# SWAP CALLDATA BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def build_v3_swap_calldata(
    w3: AsyncWeb3,
    token_in: str,
    token_out: str,
    fee: int,
    recipient: str,
    amount_in: int,
    amount_out_min: int,
) -> bytes:
    """Build calldata for UniV3/SushiV3 SwapRouter02.exactInputSingle"""
    router = w3.eth.contract(
        address=w3.to_checksum_address(recipient),  # placeholder, not used for encoding
        abi=SWAP_ROUTER_ABI,
    )
    # type(uint256).max deadline for gas savings
    # The SwapRouter02 doesn't have a deadline field in exactInputSingle params
    # (it's set via multicall with deadline wrapper, or is implicit)
    fn = router.functions.exactInputSingle(
        (
            w3.to_checksum_address(token_in),
            w3.to_checksum_address(token_out),
            fee,
            w3.to_checksum_address(recipient),
            amount_in,
            amount_out_min,
            0,  # sqrtPriceLimitX96 = 0
        )
    )
    # _encode_transaction_data() returns a hex string ("0x...").
    # Convert to raw bytes so eth_abi.encode() can handle it as `bytes`.
    hex_data = fn._encode_transaction_data()
    return bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_arbitrage(
    w3: AsyncWeb3,
    token_symbol: str,
    token_address: str,
    dex_a: str,
    dex_b: str,
    flashloan_usdc: int,
    leg_a_token_out: int,
    leg_b_usdc_out: int,
    net_profit_usd: float,
) -> Optional[str]:
    """
    Build, simulate, and broadcast an arbitrage transaction.

    Uses EXACT amounts from sequential quoting:
      - Leg A: USDC → TOKEN on dex_a, expects `leg_a_token_out` tokens
      - Leg B: TOKEN → USDC on dex_b, swaps exactly `leg_a_token_out` tokens

    Returns tx_hash on success, None on failure.
    """
    if not DEX_ARBITRAGEUR_ADDRESS or not PRIVATE_KEY:
        logger.warning("⚠️  Cannot execute — contract address or private key missing")
        return None

    try:
        account = w3.eth.account.from_key(PRIVATE_KEY)
        contract = w3.eth.contract(
            address=w3.to_checksum_address(DEX_ARBITRAGEUR_ADDRESS),
            abi=ARB_CONTRACT_ABI,
        )

        buy_config = DEXES[dex_a]
        sell_config = DEXES[dex_b]

        # ── Leg A calldata: USDC → Token (buy on dex_a) ──
        # amount_in  = flashloan_usdc (exact USDC from flashloan)
        # amount_out_min = leg_a_token_out minus 0.5% slippage
        leg_a_min_out = leg_a_token_out * (10000 - LEG_A_SLIPPAGE_BPS) // 10000

        data_a = build_v3_swap_calldata(
            w3,
            USDC_ADDRESS,
            token_address,
            500,  # 0.05% fee tier (most liquid)
            DEX_ARBITRAGEUR_ADDRESS,
            flashloan_usdc,     # Exact USDC input
            leg_a_min_out,      # Minimum tokens expected (with slippage)
        )

        # ── Leg B calldata: Token → USDC (sell on dex_b) ──
        # amount_in = leg_a_token_out (exact tokens from Leg A quote)
        # amount_out_min = flashloan + fee (must at least repay the loan)
        flashloan_fee = (flashloan_usdc * AAVE_FLASHLOAN_FEE_BPS) // 10000
        min_usdc_repay = flashloan_usdc + flashloan_fee

        data_b = build_v3_swap_calldata(
            w3,
            token_address,
            USDC_ADDRESS,
            500,
            DEX_ARBITRAGEUR_ADDRESS,
            leg_a_token_out,    # ← EXACT token amount, NEVER zero
            min_usdc_repay,     # Must repay flashloan + premium
        )

        # ── Encode ArbParams struct ──
        arb_params = HexBytes(encode(
            ["(address,bytes,address,bytes,address)"],
            [(
                w3.to_checksum_address(buy_config["router"]),
                bytes(data_a),
                w3.to_checksum_address(sell_config["router"]),
                bytes(data_b),
                w3.to_checksum_address(token_address),
            )]
        ))

        # ── Build flashloan transaction ──
        nonce = await w3.eth.get_transaction_count(account.address)
        gas_price = await w3.eth.gas_price

        tx = await contract.functions.requestFlashLoan(
            w3.to_checksum_address(USDC_ADDRESS),
            flashloan_usdc,
            arb_params,  # HexBytes — correct type for ByteStringEncoder
        ).build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": 800_000,
            "maxFeePerGas": gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei(0.01, "gwei"),
        })

        # ── Simulation Shield: eth_call before broadcast ──
        logger.info(
            f"🧪 Simulating arb: {token_symbol}/USDC | {dex_a}→{dex_b} | "
            f"Leg A out: {leg_a_token_out} tokens | Leg B out: {leg_b_usdc_out} USDC raw"
        )
        try:
            await w3.eth.call(tx)
            logger.info(f"✅ Simulation passed for {token_symbol}")
        except Exception as sim_err:
            logger.warning(f"❌ Simulation reverted for {token_symbol}: {sim_err}")
            return None

        # ── Broadcast ──
        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = tx_hash.hex()

        logger.info(f"🚀 TX SENT: {tx_hash_hex} | {token_symbol} | +${net_profit_usd:.2f}")

        # Log to database
        db_manager.record_arb_execution(
            tx_hash=tx_hash_hex,
            token_pair=f"{token_symbol}/USDC",
            dex_a=dex_a,
            dex_b=dex_b,
            profit_usd=net_profit_usd,
        )

        # Telegram notification
        send_telegram_alert(
            f"🔄 <b>Arb Executed</b>\n"
            f"📊 Pair: <code>{token_symbol}/USDC</code>\n"
            f"🔀 Route: {dex_a} → {dex_b}\n"
            f"💰 Profit: +${net_profit_usd:.2f}\n"
            f"🔗 <a href='https://arbiscan.io/tx/{tx_hash_hex}'>Arbiscan</a>"
        )

        return tx_hash_hex

    except Exception as e:
        logger.error(f"❌ Execution failed for {token_symbol}: {e}")
        send_telegram_alert(
            f"⚠️ <b>Arb Execution Failed</b>\n"
            f"📊 <code>{token_symbol}/USDC</code>\n"
            f"<code>{e}</code>"
        )
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SCANNING LOOP
# ═══════════════════════════════════════════════════════════════════════════════

async def get_eth_price(rpc_manager: AsyncRPCManager) -> float:
    """Get ETH price in USD via Multicall (1 call)."""
    try:
        w3 = await rpc_manager.get_w3()
        multicall = w3.eth.contract(address=w3.to_checksum_address(MULTICALL3_ADDRESS), abi=MULTICALL3_ABI)
        
        # Quote 1 WETH → USDC on UniV3 0.05%
        target, data = _encode_quoter_call(
            w3, UNI_V3_QUOTER, 
            TOKENS["WETH"]["address"], USDC_ADDRESS, 
            10**18, 500, "v3"
        )
        
        result = await multicall.functions.tryAggregate(False, [(target, data)]).call()
        success, ret_bytes = result[0]
        
        if success:
            quote = _decode_quoter_result(ret_bytes, "v3")
            return quote / (10 ** USDC_DECIMALS)
            
    except Exception:
        pass
    return 2500.0  # Fallback estimate


async def scan_and_execute(rpc_manager: AsyncRPCManager, block_number: int, eth_price_usd: float):
    """
    2-Step Multicall3 Scan Loop (0 Rate Limits).
    
    Leg A Batch: USDC → Token per (DEX, Fee)  [1 RPC Call]
    Leg B Batch: Token → USDC per (DEX, Fee) using Leg A output  [1 RPC Call]
    Execute: If Leg B output > flashloan + fees
    """
    spreads_found = 0
    now = time.time()
    w3 = await rpc_manager.get_w3()
    multicall = w3.eth.contract(address=w3.to_checksum_address(MULTICALL3_ADDRESS), abi=MULTICALL3_ABI)
    
    # ════════════════════════════════════════════════════════════════════════════
    # STEP 1: BATCH LEG A (USDC → TOKEN)
    # ════════════════════════════════════════════════════════════════════════════
    leg_a_calls = []
    leg_a_map = []  # Tuple (symbol, dex_name, fee_tier) to map results back

    for symbol, token_info in TOKENS.items():
        token_addr = token_info["address"]
        
        for dex_name, dex_config in DEXES.items():
            fees = dex_config["fee_tiers"]
            for fee in fees:
                target, calldata = _encode_quoter_call(
                    w3, 
                    dex_config["quoter"], 
                    USDC_ADDRESS, 
                    token_addr, 
                    FLASHLOAN_USDC_AMOUNT, 
                    fee, 
                    dex_config["type"]
                )
                leg_a_calls.append((target, calldata))
                leg_a_map.append((symbol, dex_name, fee, dex_config["type"]))

    # Execute Leg A Batch (Chunked to prevent Out of Gas)
    if not leg_a_calls:
        return 0

    try:
        # Split into chunks of 15
        chunks = [leg_a_calls[i : i + MULTICALL_CHUNK_SIZE] for i in range(0, len(leg_a_calls), MULTICALL_CHUNK_SIZE)]
        
        # Create concurrent tasks for each chunk
        tasks = []
        for chunk in chunks:
            tasks.append(
                multicall.functions.tryAggregate(False, chunk).call({'gas': 50_000_000})
            )
            
        # Fire all chunks
        chunk_results = await asyncio.gather(*tasks)
        
        # Flatten results
        leg_a_results = [item for sublist in chunk_results for item in sublist]

    except Exception as e:
        logger.error(f"❌ Leg A Multicall failed: {e}")
        await rpc_manager.handle_rate_limit()
        return 0

    # Decode Leg A results
    # best_leg_a: {symbol: {dex_name: max_amount_out_observed}}
    best_leg_a = {s: {} for s in TOKENS}

    for idx, (success, ret_bytes) in enumerate(leg_a_results):
        if not success or not ret_bytes:
            continue
            
        symbol, dex_name, fee, dex_type = leg_a_map[idx]
        amount_out = _decode_quoter_result(ret_bytes, dex_type)
        
        if amount_out > 0:
            current_max = best_leg_a[symbol].get(dex_name, 0)
            if amount_out > current_max:
                best_leg_a[symbol][dex_name] = amount_out

    # ════════════════════════════════════════════════════════════════════════════
    # STEP 2: BATCH LEG B (TOKEN → USDC)
    # ════════════════════════════════════════════════════════════════════════════
    leg_b_calls = []
    leg_b_map = []  # Tuple (symbol, buy_dex, sell_dex, sell_fee, sell_dex_type, amount_in_token)

    for symbol in TOKENS:
        token_addr = TOKENS[symbol]["address"]
        
        # Iterating per DEX permutations
        dex_names = list(DEXES.keys())
        for buy_dex, sell_dex in permutations(dex_names, 2):
            
            # Check if we have a valid Leg A output for this buy_dex
            amount_in_token = best_leg_a[symbol].get(buy_dex, 0)
            if amount_in_token == 0:
                continue

            # Route Confidence Check
            route_key = f"{symbol}/{buy_dex}/{sell_dex}"
            if route_key in route_blacklist:
                if now - route_blacklist[route_key] < ROUTE_COOLDOWN_SECONDS:
                    continue
                else:
                    del route_blacklist[route_key]
                    route_failures.pop(route_key, None)
                    logger.info(f"🔓 Route {route_key} cooldown expired")

            # Build Leg B calls for all fee tiers of sell_dex
            sell_config = DEXES[sell_dex]
            fees = sell_config["fee_tiers"]
            
            for fee in fees:
                target, calldata = _encode_quoter_call(
                    w3, 
                    sell_config["quoter"], 
                    token_addr, 
                    USDC_ADDRESS, 
                    amount_in_token, # Exact output from Leg A
                    fee, 
                    sell_config["type"]
                )
                leg_b_calls.append((target, calldata))
                leg_b_map.append((symbol, buy_dex, sell_dex, fee, sell_config["type"], amount_in_token))

    if not leg_b_calls:
        return 0

    # Execute Leg B Batch (Chunked)
    try:
        # Split into chunks
        chunks_b = [leg_b_calls[i : i + MULTICALL_CHUNK_SIZE] for i in range(0, len(leg_b_calls), MULTICALL_CHUNK_SIZE)]
        
        # Concurrent execution
        tasks_b = []
        for chunk in chunks_b:
            tasks_b.append(
                multicall.functions.tryAggregate(False, chunk).call({'gas': 50_000_000})
            )
            
        chunk_results_b = await asyncio.gather(*tasks_b)
        
        # Flatten
        leg_b_results = [item for sublist in chunk_results_b for item in sublist]

    except Exception as e:
        logger.warning(f"⚠️ Leg B Multicall failed: {e}")
        return 0

    # Decode Leg B results & Find Profit
    for idx, (success, ret_bytes) in enumerate(leg_b_results):
        if not success or not ret_bytes:
            continue
            
        symbol, buy_dex, sell_dex, sell_fee, sell_dex_type, amount_in_token = leg_b_map[idx]
        amount_out_usdc = _decode_quoter_result(ret_bytes, sell_dex_type)
        
        if amount_out_usdc == 0:
            continue

        # ── Profit Calculation ──
        flashloan_fee = (FLASHLOAN_USDC_AMOUNT * AAVE_FLASHLOAN_FEE_BPS) // 10000
        total_repay = FLASHLOAN_USDC_AMOUNT + flashloan_fee
        gross_profit_raw = amount_out_usdc - total_repay
        gross_profit_usd = gross_profit_raw / (10 ** USDC_DECIMALS)
        
        spread_pct = (gross_profit_raw / FLASHLOAN_USDC_AMOUNT) * 100.0

        # Log meaningful spreads
        if spread_pct > 0.05:
            logger.info(
                f"📊 {symbol}/USDC | {buy_dex}→{sell_dex} | "
                f"Spread: {spread_pct:.3f}% | Gross: ${gross_profit_usd:.2f} | "
                f"Net: ${gross_profit_usd:.2f}"
            )
            
            # DB logging
            try:
                db_manager.log_arb_spread(
                    token_pair=f"{symbol}/USDC",
                    dex_a=buy_dex,
                    dex_b=sell_dex,
                    spread_percent=round(spread_pct, 4),
                )
            except Exception:
                pass
            spreads_found += 1

        if gross_profit_usd <= 0:
            continue

        # Estimate Net Profit (minus gas)
        gas_price = await w3.eth.gas_price
        gas_cost_wei = 500_000 * gas_price
        
        net_profit = estimate_net_profit_usd(
            gross_profit_usd,
            gas_cost_wei,
            eth_price_usd
        )

        if net_profit >= MIN_PROFIT_USD:
            logger.info(
                f"💰 PROFITABLE: {symbol}/USDC | Net: +${net_profit:.2f} | "
                f"Route: {buy_dex} → {sell_dex}"
            )

            # Fire execution
            tx_hash = await execute_arbitrage(
                w3=w3,
                token_symbol=symbol,
                token_address=TOKENS[symbol]["address"],
                dex_a=buy_dex,
                dex_b=sell_dex,
                flashloan_usdc=FLASHLOAN_USDC_AMOUNT,
                leg_a_token_out=amount_in_token,
                leg_b_usdc_out=amount_out_usdc,
                net_profit_usd=net_profit,
            )
            
            route_key = f"{symbol}/{buy_dex}/{sell_dex}"
            if not tx_hash:
                # Blacklist on failure
                route_failures[route_key] = route_failures.get(route_key, 0) + 1
                if route_failures[route_key] >= MAX_ROUTE_FAILURES:
                    route_blacklist[route_key] = now
                    logger.warning(f"🚫 Route {route_key} blacklisted")
            else:
                # Success - clear failures
                route_failures.pop(route_key, None)

    return spreads_found


async def main():
    """Main entry point — continuous block-by-block scanning with RPC failover."""
    logger.info("═══════════════════════════════════════════════════════════")
    logger.info("🛸 ANTI-GRAVITY — DEX Arbitrage Engine v2.1")
    logger.info("═══════════════════════════════════════════════════════════")
    
    # Initialize RPC Manager
    rpc_manager = AsyncRPCManager()
    await rpc_manager.connect()
    
    logger.info(f"📊 Scanning {len(TOKENS)} tokens across {len(DEXES)} DEXs")
    logger.info(f"🎯 Tokens: {', '.join(TOKENS.keys())}")
    logger.info(f"🏪 DEXs: {', '.join(DEXES.keys())}")
    logger.info(f"💰 Min Profit: ${MIN_PROFIT_USD}")
    logger.info("═══════════════════════════════════════════════════════════")

    w3 = await rpc_manager.get_w3()
    chain_id = await w3.eth.chain_id
    logger.info(f"✅ Connected to chain {chain_id}")

    if chain_id != 42161:
        logger.warning(f"⚠️  Expected Arbitrum One (42161), got chain {chain_id}")

    # Initialize DB
    db_manager.init_db()
    logger.info("✅ Database initialized")

    # Telegram startup notification
    send_telegram_alert(
        f"🔄 <b>DEX Arb Engine Started (Multicall3)</b>\n"
        f"📊 {len(TOKENS)} tokens × {len(DEXES)} DEXs\n"
        f"🔗 RPC: <code>{rpc_manager.endpoints[0][:40]}...</code>"
    )

    last_block = 0
    # Use dummy semaphore for initial price fetch (Multicall inside get_eth_price doesn't use it, but valid for sig)
    eth_price_usd = await get_eth_price(rpc_manager)
    eth_price_refresh = time.time()
    
    logger.info(f"📈 ETH Price: ${eth_price_usd:,.0f}")

    # ── Infinite Scan Loop ──
    while True:
        try:
            scan_start = time.time()
            w3 = await rpc_manager.get_w3()
            
            try:
                current_block = await w3.eth.block_number
            except Exception:
                await rpc_manager.handle_rate_limit()
                continue

            # Skip if same block
            if current_block <= last_block:
                await asyncio.sleep(SCAN_COOLDOWN_SECONDS)
                continue

            # Refresh ETH price every 5 minutes
            if time.time() - eth_price_refresh > 300:
                eth_price_usd = await get_eth_price(rpc_manager)
                eth_price_refresh = time.time()
                logger.info(f"📈 ETH Price refreshed: ${eth_price_usd:,.0f}")

            # ── Run scan ──
            spreads = await scan_and_execute(rpc_manager, current_block, eth_price_usd)

            scan_duration = time.time() - scan_start
            blocks_jumped = current_block - last_block if last_block > 0 else 1
            last_block = current_block

            logger.info(
                f"🧱 Block {current_block} | "
                f"Spreads: {spreads} | "
                f"{scan_duration*1000:.0f}ms | "
                f"Δ{blocks_jumped} blocks"
            )

            # Dynamic sleep — Arbitrum has ~250ms blocks
            sleep_time = max(SCAN_COOLDOWN_SECONDS, 1.0 - scan_duration)
            await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"❌ Loop error: {e}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(1)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛸 Anti-Gravity DEX Arb Engine stopped.")
