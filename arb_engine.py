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
import random
import zmq
import zmq.asyncio
import traceback
from decimal import Decimal
from market_sentinel import MarketSentinel
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

# RPC — unified convention: PRIMARY_WSS and PRIMARY_RPC
PRIMARY_WSS = os.getenv("PRIMARY_WSS")
PRIMARY_RPC = os.getenv("PRIMARY_RPC")
if not PRIMARY_RPC:
    PRIMARY_RPC = os.getenv("RPC_URL", "")

FALLBACK_RPCS_RAW = os.getenv("FALLBACK_RPCS", "").replace('"', '').replace("'", "")
FALLBACK_RPCS = [r.strip() for r in FALLBACK_RPCS_RAW.split(",") if r.strip()]
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
    "USDC":   {"address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "decimals": 6},
    "USDT":   {"address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "decimals": 6},
    "DAI":    {"address": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "decimals": 18},
    "WETH":   {"address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "decimals": 18},
    "ARB":    {"address": "0x912CE59144191C1204E64559FE8253a0e49E6548", "decimals": 18},
    "MAGIC":  {"address": "0x539bdE0d7Dbd33f84E8aaf9084C942D9800Ef002", "decimals": 18},
    "GRAIL":  {"address": "0x3d9907F9a368ad0a51Be60f7Da3b97cf940982D8", "decimals": 18},
    "PENDLE": {"address": "0x0c880f6761F1af8d9Aa9C466984785263cf79560", "decimals": 18},
    "GMX":    {"address": "0xfc5A1A6EB076a2C7AD06EDb220f4daaC9AF172af", "decimals": 18},
    "RDNT":   {"address": "0x3082CC23568eA640225c2467653dB90e9250AaA0", "decimals": 18},
    "AIDOGE": {"address": "0x09E18590E8f76b6Cf471b3cd75fE1A1a9D2B2c2b", "decimals": 18},
    "XAI":    {"address": "0x4cb9a7ae498cedcbb5eae9f25736ae7d428c9d66", "decimals": 18},
    "JOE":    {"address": "0x371c7ec6D8039ff7933a2AA28EB827Ffe1F52f07", "decimals": 18},
    "GNS":    {"address": "0x18c11FD286C5EC11c3b683Caa813B77f5163A122", "decimals": 18},
    "VRTX":   {"address": "0x95146881b86B3ee99e63705eC87AfE29Fcc0Baa4", "decimals": 18},
}

# Base quote token
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC (Arbitrum)
USDC_DECIMALS = 6

# Fixed flashloan size in USDC (6 decimals). $1000 USDC = 1_000_000_000
FLASHLOAN_USDC_AMOUNT = 1000 * 10**USDC_DECIMALS  # $1,000

# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC RPC MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# SMART ASYNC RPC MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class SmartAsyncRPCManager:
    """
    Tiered RPC Router:
    - Tier 1 (Premium): PRIMARY_RPC (QuickNode, etc.) - Used for execution & high volatility.
    - Tier 2 (Free): FALLBACK_RPCS - Used for polling and routine tasks.
    Ranks Tier 2 nodes every 60s based on latency.
    """
    def __init__(self):
        self.premium_url = PRIMARY_RPC
        self.free_urls = FALLBACK_RPCS.copy()
        
        # Connection Pools
        self.premium_w3 = None
        self.free_w3s: Dict[str, AsyncWeb3] = {}
        
        # Tier 2 Rankings: [{"url": str, "latency": float, "is_blacklisted": bool, "blacklist_until": float}]
        self.free_nodes_rank = [{"url": url, "latency": 999.0, "is_blacklisted": False, "blacklist_until": 0} for url in self.free_urls]
        self.strike_counts: Dict[str, int] = {url: 0 for url in self.free_urls}
        
    async def connect_all(self):
        """Initializes Web3 instances for all nodes."""
        logger.info(f"🔌 Connecting to Premium RPC (Tier 1): {self.premium_url[:40]}...")
        self.premium_w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.premium_url))
        
        for url in self.free_urls:
            self.free_w3s[url] = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
            
        # Start background ranker
        asyncio.create_task(self._rank_nodes_loop())
        logger.info(f"🟢 Smart RPC Manager Initialized ({len(self.free_urls)} Free Nodes).")

    async def _rank_nodes_loop(self):
        """Background task that pings Tier 2 nodes every 60s."""
        while True:
            await self.rank_free_nodes()
            await asyncio.sleep(60)

    async def rank_free_nodes(self):
        """Pings all free nodes and updates their latency."""
        now = time.time()
        
        for node in self.free_nodes_rank:
            # Un-blacklist if time has passed
            if node["is_blacklisted"] and now > node["blacklist_until"]:
                node["is_blacklisted"] = False
                node["latency"] = 999.0 # Reset
                self.strike_counts[node["url"]] = 0
                logger.info(f"🟢 Node un-blacklisted: {node['url'][:40]}...")

            if node["is_blacklisted"]:
                continue

            # Ping test
            w3 = self.free_w3s[node["url"]]
            start = time.time()
            try:
                # 3-second strict timeout for ping
                await asyncio.wait_for(w3.eth.block_number, timeout=3.0)
                node["latency"] = time.time() - start
            except Exception:
                # Penalty for failing ping, but not immediate blacklisting
                node["latency"] = 999.0
        
        # Sort by latency (lowest first, blacklisted at the bottom)
        self.free_nodes_rank.sort(key=lambda x: (x["is_blacklisted"], x["latency"]))

    async def get_optimal_w3(self, is_critical=False, sentinel: MarketSentinel = None) -> AsyncWeb3:
        """
        Routes the request.
        is_critical=True OR sentinel.is_high_volatility == True -> Premium Node
        is_critical=False -> Best available Free Node
        """
        if is_critical or (sentinel and sentinel.is_high_volatility):
            return self.premium_w3

        # Find best free node
        for node in self.free_nodes_rank:
            if not node["is_blacklisted"]:
                return self.free_w3s[node["url"]]
                
        # Failsafe: if ALL free nodes are blacklisted, fallback to premium temporarily
        logger.warning("⚠️ All Free Nodes blacklisted! Falling back to Premium Node temporarily.")
        return self.premium_w3

    async def handle_rate_limit(self, w3_instance: AsyncWeb3):
        """
        If a Free node hits 429, blacklist it for 5 minutes.
        If the Premium node hits 429, apply exponential backoff.
        """
        url_failed = w3_instance.provider.endpoint_uri
        
        if url_failed == self.premium_url:
            self.strike_counts["premium"] = self.strike_counts.get("premium", 0) + 1
            cooldown = min(30, (2 ** self.strike_counts["premium"])) + random.uniform(0.1, 1.0)
            logger.warning(f"💎 PREMIUM Rate limited (Strike {self.strike_counts['premium']}). Cooling down {cooldown:.2f}s...")
            await asyncio.sleep(cooldown)
        else:
            for node in self.free_nodes_rank:
                if node["url"] == url_failed:
                    self.strike_counts[url_failed] += 1
                    if self.strike_counts[url_failed] >= 3:
                        node["is_blacklisted"] = True
                        node["blacklist_until"] = time.time() + 300 # 5 minutes
                        logger.warning(f"🚫 Free Node Blacklisted (5m): {url_failed[:40]}...")
                    else:
                        logger.warning(f"🐌 Free Node Strike {self.strike_counts[url_failed]}/3: {url_failed[:40]}...")
                    break
            
            # Re-sort to push blacklisted down
            self.free_nodes_rank.sort(key=lambda x: (x["is_blacklisted"], x["latency"]))

    def is_rate_limit_error(self, error):
        err_str = str(error).lower()
        return any(k in err_str for k in ["429", "403", "rate", "forbidden", "quota", "too many requests", "-32001", "timeout"])

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

# Curve Finance 3Pool (Arbitrum)
CURVE_3POOL_ADDRESS = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"

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
    "Curve_3Pool": {
        "quoter": CURVE_3POOL_ADDRESS,
        "router": CURVE_3POOL_ADDRESS,
        "type": "curve",
        "fee_tiers": [0],      # No fee tiers for Curve
        "curve_indices": {"USDC": 1, "USDT": 2, "DAI": 0},
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

# Curve 3Pool (Mainnet/Arbitrum)
CURVE_3POOL_ABI = [
    {
        "name": "get_dy",
        "inputs": [
            {"name": "i", "type": "int128"},
            {"name": "j", "type": "int128"},
            {"name": "dx", "type": "uint256"}
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "name": "exchange",
        "inputs": [
            {"name": "i", "type": "int128"},
            {"name": "j", "type": "int128"},
            {"name": "dx", "type": "uint256"},
            {"name": "min_dy", "type": "uint256"}
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function"
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
MIN_PROFIT_USD = 1.00              # $1 minimum — accounts for gas spikes + slippage drift
MAX_GAS_PRICE_GWEI = 1.0          # Arbitrum gas is cheap, but cap it
SCAN_COOLDOWN_SECONDS = 2.0       # Strict 2.0s rate-limit delay
MAX_SLIPPAGE_BPS = 50             # 0.5% max slippage for trade sizing
SAFETY_MARGIN_MULTIPLIER = 1.5    # Extra margin on cost estimates to avoid NotProfitable
LEG_A_SLIPPAGE_BPS = 50           # 0.5% slippage tolerance on Leg A output
MULTICALL_CHUNK_SIZE = 3         # Max calls per tryAggregate to avoid gas limits

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
    rpc_manager: SmartAsyncRPCManager,
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
    rpc_manager: SmartAsyncRPCManager,
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
    quoter_address: str,  # Kept for compatibility, but we prefer dex_config
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
    dex_config: Dict,
) -> Tuple[str, bytes]:
    """
    Generate (target, calldata) for a quote call.
    Supports V3, Algebra, and Curve.
    """
    dex_type = dex_config["type"]
    
    # ─── CURVE 3POOL ───
    if dex_type == "curve":
        quoter_contract = w3.eth.contract(address=w3.to_checksum_address(quoter_address), abi=CURVE_3POOL_ABI)
        
        # Resolve indices
        # We need to find the index for token_in and token_out
        # Iterate TOKENS and/or USDC to find symbol matching address?
        # Better: use the 'curve_indices' map and look up by address-to-symbol.
        # Since we don't have a reliable address->symbol map passed in, we can try to infer it
        # or just check against known addresses.
        
        indices = dex_config.get("curve_indices", {})
        
        # Reverse lookup helper (local to this block)
        def get_curve_index(addr):
            # Check against TOKENS
            for sym, data in TOKENS.items():
                if data["address"].lower() == addr.lower():
                    return indices.get(sym)
            return None

        i = get_curve_index(token_in)
        j = get_curve_index(token_out)

        if i is None or j is None:
             # Invalid pair for this pool (e.g. WETH on 3Pool)
             # Return dummy data that will fail decoding or execution gracefully
             # Here we return a call to 'get_dy(0,0,0)' which is valid but useless,
             # OR we raise an error. Raising error is safer but needs handling.
             # Better: Return a dummy target that is effectively a no-op or revert?
             # For now, let's assume the caller filters invalid pairs.
             pass

        # get_dy(int128 i, int128 j, uint256 dx)
        call_fn = quoter_contract.functions.get_dy(i, j, amount_in)
        
        hex_data = call_fn._encode_transaction_data()
        raw_data = bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)
        return (quoter_contract.address, raw_data)

    # ─── UNISWAP V3 / ALGEBRA ───
    quoter_contract = w3.eth.contract(
        address=w3.to_checksum_address(quoter_address),
        abi=QUOTER_V2_ABI if dex_type == "v3" else ALGEBRA_QUOTER_ABI,
    )
    
    if dex_type == "v3":
        t_in = w3.to_checksum_address(token_in)
        t_out = w3.to_checksum_address(token_out)
        call_fn = quoter_contract.functions.quoteExactInputSingle((
            t_in, t_out, amount_in, fee, 0
        ))
    else:  # algebra
        t_in = w3.to_checksum_address(token_in)
        t_out = w3.to_checksum_address(token_out)
        call_fn = quoter_contract.functions.quoteExactInputSingle(
            t_in, t_out, amount_in, 0
        )
        
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
            decoded = decode(['uint256', 'uint160', 'uint32', 'uint256'], raw_bytes)
            return decoded[0]
        elif dex_type == "curve":
            # Returns (uint256)
            decoded = decode(['uint256'], raw_bytes)
            return decoded[0]
        else:  # algebra
            # Returns (amountOut, fee)
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


def build_curve_swap_calldata(
    w3: AsyncWeb3,
    pool_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
    amount_out_min: int,
    dex_config: Dict,
) -> bytes:
    """Build calldata for Curve 3Pool exchange."""
    contract = w3.eth.contract(
        address=w3.to_checksum_address(pool_address),
        abi=CURVE_3POOL_ABI,
    )
    
    indices = dex_config.get("curve_indices", {})
    t_in_lower = token_in.lower()
    t_out_lower = token_out.lower()
    
    i = None
    j = None
    
    # Resolve indices by checking against TOKENS addresses
    # (Since we don't pass symbols here, avoiding global lookup if possible, but we have TOKENS global)
    for sym, data in TOKENS.items():
        addr = data["address"].lower()
        if addr == t_in_lower:
            i = indices.get(sym)
        if addr == t_out_lower:
            j = indices.get(sym)
            
    if i is None or j is None:
        raise ValueError(f"Invalid tokens for Curve 3Pool: {token_in} -> {token_out}")
        
    fn = contract.functions.exchange(i, j, amount_in, amount_out_min)
    hex_data = fn._encode_transaction_data()
    return bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_arbitrage(
    rpc_manager: SmartAsyncRPCManager,
    route_details: dict,
    gross_profit_usd: float,
    eth_price_usd: float,
    current_block: int
):
    """Executes the flash loan and multi-DEX swap via the smart contract using Tier 1 RPC."""
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

        if buy_config["type"] == "curve":
             data_a = build_curve_swap_calldata(
                w3,
                buy_config["router"],
                USDC_ADDRESS,
                token_address,
                flashloan_usdc,
                leg_a_min_out,
                buy_config
            )
        else:
            data_a = build_v3_swap_calldata(
                w3,
                USDC_ADDRESS,
                token_address,
                500,  # 0.05% fee tier (most liquid for V3/Camelot usually)
                DEX_ARBITRAGEUR_ADDRESS,
                flashloan_usdc,     # Exact USDC input
                leg_a_min_out,      # Minimum tokens expected
            )

        # ── Leg B calldata: Token → USDC (sell on dex_b) ──
        # amount_in = leg_a_token_out (exact tokens from Leg A quote)
        # amount_out_min = flashloan + fee (must at least repay the loan)
        flashloan_fee = (flashloan_usdc * AAVE_FLASHLOAN_FEE_BPS) // 10000
        min_usdc_repay = flashloan_usdc + flashloan_fee

        if sell_config["type"] == "curve":
            data_b = build_curve_swap_calldata(
                w3,
                sell_config["router"],
                token_address,
                USDC_ADDRESS,
                leg_a_token_out,
                min_usdc_repay,
                sell_config
            )
        else:
            data_b = build_v3_swap_calldata(
                w3,
                token_address,
                USDC_ADDRESS,
                500,
                DEX_ARBITRAGEUR_ADDRESS,
                leg_a_token_out,    # ← EXACT token amount
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
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        tx_hash_hex = tx_hash.hex()

        logger.info(f"🚀 TX SENT: {tx_hash_hex} | {token_symbol} | +${gross_profit_usd:.2f}")

        # Log to database
        db_manager.record_arb_execution(
            tx_hash=tx_hash_hex,
            token_pair=f"{token_symbol}/USDC",
            dex_a=dex_a,
            dex_b=dex_b,
            profit_usd=gross_profit_usd,
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

async def get_eth_price(rpc_manager: SmartAsyncRPCManager) -> float:
    """Fetch ETH price efficiently using Multicall3 on a Free Tier Node."""
    while True:
        try:
            w3 = await rpc_manager.get_optimal_w3(is_critical=False)
            multicall = w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)
        
            # Quote 1 WETH → USDC on UniV3 0.05%
            target, data = _encode_quoter_call(
                w3, UNI_V3_QUOTER, 
                TOKENS["WETH"]["address"], USDC_ADDRESS, 
                10**18, 500, DEXES["Uniswap_V3"] # Pass the full DEX config
            )
            
            result = await multicall.functions.tryAggregate(False, [(target, data)]).call()
            success, ret_bytes = result[0]
            
            if success:
                quote = _decode_quoter_result(ret_bytes, "v3")
                return quote / (10 ** USDC_DECIMALS)
                
        except Exception as e:
            if rpc_manager.is_rate_limit_error(e):
                logger.warning("🐌 Rate limit on ETH price fetch. Yielding to backoff...")
                await rpc_manager.handle_rate_limit(w3)
            else:
                logger.error(f"⚠️ Error fetching ETH price: {e}")
                logger.debug(traceback.format_exc())
            await asyncio.sleep(5) # Wait before retrying
        return 2500.0  # Fallback estimate


async def scan_and_execute(rpc_manager: SmartAsyncRPCManager, current_block: int, eth_price_usd: float, sentinel: MarketSentinel = None):
    """
    2-Step Multicall3 Scan Loop (0 Rate Limits).
    
    Leg A Batch: USDC → Token per (DEX, Fee)  [1 RPC Call]
    Leg B Batch: Token → USDC per (DEX, Fee) using Leg A output  [1 RPC Call]
    Execute: If Leg B output > flashloan + fees
    """
    valid_routes = []
    
    # Check allowances asynchronously but only periodically (or implement properly)
    # asyncio.create_task(check_contract_allowance(rpc_manager))
    spreads_found = 0
    now = time.time()
    w3 = await rpc_manager.get_optimal_w3(is_critical=True)
    multicall = w3.eth.contract(address=w3.to_checksum_address(MULTICALL3_ADDRESS), abi=MULTICALL3_ABI)
    
    # ════════════════════════════════════════════════════════════════════════════
    # STEP 1: BATCH LEG A (USDC → TOKEN)
    # ════════════════════════════════════════════════════════════════════════════
    leg_a_calls = []
    leg_a_map = []  # Tuple (symbol, dex_name, fee_tier) to map results back

    for symbol, token_info in TOKENS.items():
        token_addr = token_info["address"]
        
        for dex_name, dex_config in DEXES.items():
            # Skip Curve if token not in 3Pool (USDC, USDT, DAI)
            if dex_config["type"] == "curve":
                if symbol not in ["USDC", "USDT", "DAI"]:
                    continue
            
            fees = dex_config["fee_tiers"]
            for fee in fees:
                target, calldata = _encode_quoter_call(
                    w3, 
                    dex_config["quoter"], 
                    USDC_ADDRESS, 
                    token_addr, 
                    FLASHLOAN_USDC_AMOUNT, 
                    fee, 
                    dex_config
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
        await rpc_manager.handle_rate_limit(w3)
        await asyncio.sleep(5)
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

            # Skip Curve if token not in 3Pool (USDC, USDT, DAI)
            if sell_config["type"] == "curve":
                 if symbol not in ["USDC", "USDT", "DAI"]:
                     continue

            fees = sell_config["fee_tiers"]
            
            for fee in fees:
                target, calldata = _encode_quoter_call(
                    w3, 
                    sell_config["quoter"], 
                    token_addr, 
                    USDC_ADDRESS, 
                    amount_in_token, # Exact output from Leg A
                    fee, 
                    sell_config
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
                rpc_manager=rpc_manager,
                route_details={
                    "token_symbol": symbol,
                    "token_address": TOKENS[symbol]["address"],
                    "dex_a": buy_dex,
                    "dex_b": sell_dex,
                    "flashloan_usdc": FLASHLOAN_USDC_AMOUNT,
                    "leg_a_token_out": amount_in_token,
                    "leg_b_usdc_out": amount_out_usdc
                },
                gross_profit_usd=net_profit,
                eth_price_usd=eth_price_usd,
                current_block=current_block
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
    rpc_manager = SmartAsyncRPCManager()
    await rpc_manager.connect_all()
    
    # Start background task for RPC ranking
    asyncio.create_task(rpc_manager._rank_nodes_loop())

    logger.info(f"📊 Scanning {len(TOKENS)} tokens across {len(DEXES)} DEXs")
    logger.info(f"🎯 Tokens: {', '.join(TOKENS.keys())}")
    logger.info(f"🏪 DEXs: {', '.join(DEXES.keys())}")
    logger.info(f"💰 Min Profit: ${MIN_PROFIT_USD}")
    logger.info("═══════════════════════════════════════════════════════════")

    # --- STARTUP PROTECTION ---
    # Jitter to prevent all PM2 instances from hitting RPC simultaneously on boot
    await asyncio.sleep(random.uniform(1.0, 10.0))
    while True:
        try:
            w3 = await rpc_manager.get_optimal_w3(is_critical=False)
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
                f"🔗 RPC: <code>{rpc_manager.premium_url[:40]}...</code>"
            )

            last_block = 0
            # Use dummy semaphore for initial price fetch
            eth_price_usd = await get_eth_price(rpc_manager)
            eth_price_refresh = time.time()
            
            logger.info(f"📈 ETH Price: ${eth_price_usd:,.0f}")
            break # Success, exit startup loop

        except Exception as e:
            if rpc_manager.is_rate_limit_error(e):
                logger.warning("🐌 Rate limit on STARTUP. Yielding to backoff...")
                await rpc_manager.handle_rate_limit(w3)
            else:
                logger.error(f"💥 Fatal Startup Error: {e}")
                await asyncio.sleep(60)

    # ── Scanning Loop (ZMQ SUB) ──
    sentinel = MarketSentinel()

    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.SUB)
    socket.connect("tcp://127.0.0.1:5555")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    logger.info("🎧 Subscribed to ZeroMQ Block Emitter.")

    while True:
        try:
            # Wait for Emitter to broadcast a new block
            block_msg = await socket.recv_string()
            current_block = int(block_msg)
            
            if current_block <= last_block:
                continue

            if not await sentinel.should_scan():
                continue

            # Jitter to avoid thundering herd across all bots
            await asyncio.sleep(random.uniform(2.0, 8.0))

            scan_start = time.time()
            
            # Refresh ETH price every 5 minutes
            if time.time() - eth_price_refresh > 300:
                eth_price_usd = await get_eth_price(rpc_manager)
                eth_price_refresh = time.time()
                logger.info(f"📈 ETH Price refreshed: ${eth_price_usd:,.0f}")

            # ── Run scan ──
            spreads = await scan_and_execute(rpc_manager, current_block, eth_price_usd, sentinel=sentinel)

            scan_duration = time.time() - scan_start
            blocks_jumped = current_block - last_block if last_block > 0 else 1
            last_block = current_block

            logger.info(
                f"🧱 Block {current_block} | "
                f"Spreads: {spreads} | "
                f"{scan_duration*1000:.0f}ms | "
                f"Δ{blocks_jumped} blocks"
            )

            sentinel.update_last_price()

            # Strict throttle for 15 RPS limit
            await asyncio.sleep(SCAN_COOLDOWN_SECONDS)

        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down gracefully...")
            break
        except Exception as e:
            if rpc_manager.is_rate_limit_error(e):
                await rpc_manager.handle_rate_limit(w3)
            else:
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
