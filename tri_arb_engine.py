"""
═══════════════════════════════════════════════════════════════════════════════
🛸 ANTI-GRAVITY — Triangular Arbitrage Engine (Arbitrum One)
═══════════════════════════════════════════════════════════════════════════════
Production-grade multi-hop (3-hop) atomic arbitrage scanner & executor.
Scans Uniswap V3, SushiSwap V3, and Camelot DEX for 3-hop price spreads.
Uses Aave V3 flashloans via TriArbitrageur.sol for zero-capital execution.

Architecture:
  - Base Token: USDC
  - Hub Tokens: WETH, ARB
  - Target Tokens: All other supported TOKENS
  - Scans Route 1: USDC -> Hub -> Target -> USDC
  - Scans Route 2: USDC -> Target -> Hub -> USDC
  - 3-Stage Multicall3 Batching for 0 Rate Limits
  - Simulates via eth_call before broadcasting
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
        logging.FileHandler("tri_arb_engine.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("TriArbEngine")

# ═══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT
# ═══════════════════════════════════════════════════════════════════════════════
load_dotenv()

PRIMARY_WSS = os.getenv("PRIMARY_WSS")
PRIMARY_RPC = os.getenv("PRIMARY_RPC")
if not PRIMARY_RPC:
    PRIMARY_RPC = os.getenv("RPC_URL", "")

FALLBACK_RPCS_RAW = os.getenv("FALLBACK_RPCS", "").replace('"', '').replace("'", "")
FALLBACK_RPCS = [r.strip() for r in FALLBACK_RPCS_RAW.split(",") if r.strip()]

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
TRI_ARBITRAGEUR_ADDRESS = os.getenv("TRI_ARBITRAGEUR_ADDRESS", "")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

if not PRIMARY_RPC:
    logger.critical("❌ PRIMARY_RPC not found in .env — exiting")
    exit(1)

if not PRIVATE_KEY:
    logger.warning("⚠️  PRIVATE_KEY not set — execution will be disabled (scan-only mode)")

if not TRI_ARBITRAGEUR_ADDRESS or TRI_ARBITRAGEUR_ADDRESS == "0x0000000000000000000000000000000000000000":
    logger.warning("⚠️  TRI_ARBITRAGEUR_ADDRESS not set — deploy contract first, then add to .env")

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
# TOKEN CONFIGURATION — Arbitrum Mainnet
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

HUBS = ["WETH", "ARB"]
TARGETS = [s for s in TOKENS if s not in HUBS and s != "USDC"]

# Base quote token
USDC_ADDRESS = TOKENS["USDC"]["address"]
USDC_DECIMALS = TOKENS["USDC"]["decimals"]

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
# DEX CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
UNI_V3_QUOTER   = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
UNI_V3_ROUTER   = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"
SUSHI_V3_QUOTER = "0x0524E833cCD057e4d7A296e3aaAb9f7675964Ce1"
SUSHI_V3_ROUTER = "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c"
CAMELOT_ROUTER  = "0xc873fEcbd354f5A56E00E710B9048C68fD3EA22B"
CAMELOT_QUOTER  = "0x4a6eDa4451BcF25E07F1f55B77267e5B89975f68"
CURVE_3POOL_ADDRESS = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"

DEXES = {
    "Uniswap_V3": {
        "quoter": UNI_V3_QUOTER, "router": UNI_V3_ROUTER,
        "type": "v3", "fee_tiers": [100, 500, 3000, 10000],
    },
    "SushiSwap_V3": {
        "quoter": SUSHI_V3_QUOTER, "router": SUSHI_V3_ROUTER,
        "type": "v3", "fee_tiers": [100, 500, 3000, 10000],
    },
    "Camelot": {
        "quoter": CAMELOT_QUOTER, "router": CAMELOT_ROUTER,
        "type": "algebra", "fee_tiers": [0],
    },
    "Curve_3Pool": {
        "quoter": CURVE_3POOL_ADDRESS, "router": CURVE_3POOL_ADDRESS,
        "type": "curve", "fee_tiers": [0], "curve_indices": {"USDC": 1, "USDT": 2, "DAI": 0},
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# ABIs
# ═══════════════════════════════════════════════════════════════════════════════

QUOTER_V2_ABI = [{
    "inputs": [{
        "components": [
            {"name": "tokenIn",            "type": "address"},
            {"name": "tokenOut",           "type": "address"},
            {"name": "amountIn",           "type": "uint256"},
            {"name": "fee",                "type": "uint24"},
            {"name": "sqrtPriceLimitX96",  "type": "uint160"},
        ],
        "name": "params", "type": "tuple",
    }],
    "name": "quoteExactInputSingle",
    "outputs": [
        {"name": "amountOut",              "type": "uint256"},
        {"name": "sqrtPriceX96After",      "type": "uint160"},
        {"name": "initializedTicksCrossed","type": "uint32"},
        {"name": "gasEstimate",            "type": "uint256"},
    ],
    "stateMutability": "nonpayable", "type": "function",
}]

ALGEBRA_QUOTER_ABI = [{
    "inputs": [
        {"name": "tokenIn",           "type": "address"},
        {"name": "tokenOut",          "type": "address"},
        {"name": "amountIn",          "type": "uint256"},
        {"name": "limitSqrtPrice",    "type": "uint160"},
    ],
    "name": "quoteExactInputSingle",
    "outputs": [
        {"name": "amountOut",         "type": "uint256"},
        {"name": "fee",               "type": "uint16"},
    ],
    "stateMutability": "nonpayable", "type": "function",
}]

SWAP_ROUTER_ABI = [{
    "inputs": [{
        "components": [
            {"name": "tokenIn",       "type": "address"},
            {"name": "tokenOut",      "type": "address"},
            {"name": "fee",           "type": "uint24"},
            {"name": "recipient",     "type": "address"},
            {"name": "amountIn",      "type": "uint256"},
            {"name": "amountOutMinimum", "type": "uint256"},
            {"name": "sqrtPriceLimitX96", "type": "uint160"},
        ],
        "name": "params", "type": "tuple",
    }],
    "name": "exactInputSingle",
    "outputs": [{"name": "amountOut", "type": "uint256"}],
    "stateMutability": "payable", "type": "function",
}]

CURVE_3POOL_ABI = [
    {
        "name": "get_dy",
        "inputs": [
            {"name": "i", "type": "int128"},
            {"name": "j", "type": "int128"},
            {"name": "dx", "type": "uint256"}
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view", "type": "function"
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
        "stateMutability": "payable", "type": "function"
    }
]

TRI_ARB_ABI = [{
    "inputs": [
        {"internalType": "address", "name": "_token", "type": "address"},
        {"internalType": "uint256", "name": "_amount", "type": "uint256"},
        {
            "components": [
                {"internalType": "address", "name": "router", "type": "address"},
                {"internalType": "address", "name": "tokenIn", "type": "address"},
                {"internalType": "bytes", "name": "payload", "type": "bytes"}
            ],
            "internalType": "struct TriArbitrageur.Route[]",
            "name": "_routes",
            "type": "tuple[]"
        }
    ],
    "name": "requestFlashLoan",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
}]

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
AAVE_FLASHLOAN_FEE_BPS = 5
MIN_PROFIT_USD = 1.00
SCAN_COOLDOWN_SECONDS = 2.0       # Strict 2.0s rate-limit delay
LEG_A_SLIPPAGE_BPS = 50           # 0.5% max slippage allowed for tri-arb routes
SAFETY_MARGIN_MULTIPLIER = 1.5
MULTICALL_CHUNK_SIZE = 5

# Route Failure Handling
MAX_ROUTE_FAILURES = 3
ROUTE_COOLDOWN_SECONDS = 600
route_failures: Dict[str, int] = {}
route_blacklist: Dict[str, float] = {}

# ═══════════════════════════════════════════════════════════════════════════════
# QUOTE FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

def _encode_quoter_call(
    w3: AsyncWeb3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
    dex_config: Dict,
) -> Tuple[str, bytes]:
    dex_type = dex_config["type"]
    
    if dex_type == "curve":
        quoter_contract = w3.eth.contract(address=w3.to_checksum_address(quoter_address), abi=CURVE_3POOL_ABI)
        indices = dex_config.get("curve_indices", {})
        
        def get_curve_index(addr):
            for sym, data in TOKENS.items():
                if data["address"].lower() == addr.lower():
                    return indices.get(sym)
            return None

        i = get_curve_index(token_in)
        j = get_curve_index(token_out)

        if i is None or j is None:
            return (w3.to_checksum_address("0x0000000000000000000000000000000000000000"), b"")

        call_fn = quoter_contract.functions.get_dy(i, j, amount_in)
        hex_data = call_fn._encode_transaction_data()
        raw_data = bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)
        return (quoter_contract.address, raw_data)

    quoter_contract = w3.eth.contract(
        address=w3.to_checksum_address(quoter_address),
        abi=QUOTER_V2_ABI if dex_type == "v3" else ALGEBRA_QUOTER_ABI,
    )
    
    t_in = w3.to_checksum_address(token_in)
    t_out = w3.to_checksum_address(token_out)
    
    if dex_type == "v3":
        call_fn = quoter_contract.functions.quoteExactInputSingle((
            t_in, t_out, amount_in, fee, 0
        ))
    else:
        call_fn = quoter_contract.functions.quoteExactInputSingle(
            t_in, t_out, amount_in, 0
        )
        
    hex_data = call_fn._encode_transaction_data()
    raw_data = bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)
    
    return (quoter_contract.address, raw_data)

def _decode_quoter_result(raw_bytes: bytes, dex_type: str) -> int:
    if not raw_bytes:
        return 0
    try:
        if dex_type == "v3":
            decoded = decode(['uint256', 'uint160', 'uint32', 'uint256'], raw_bytes)
            return decoded[0]
        elif dex_type == "curve":
            decoded = decode(['uint256'], raw_bytes)
            return decoded[0]
        else:
            decoded = decode(['uint256', 'uint16'], raw_bytes)
            return decoded[0]
    except Exception:
        return 0

def estimate_net_profit_usd(gross_profit_usd: float, gas_cost_wei: int, eth_price_usd: float) -> float:
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
    router = w3.eth.contract(
        address=w3.to_checksum_address(recipient), 
        abi=SWAP_ROUTER_ABI,
    )
    fn = router.functions.exactInputSingle(
        (
            w3.to_checksum_address(token_in),
            w3.to_checksum_address(token_out),
            fee,
            w3.to_checksum_address(recipient),
            amount_in,
            amount_out_min,
            0,
        )
    )
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
    contract = w3.eth.contract(
        address=w3.to_checksum_address(pool_address),
        abi=CURVE_3POOL_ABI,
    )
    indices = dex_config.get("curve_indices", {})
    t_in_lower = token_in.lower()
    t_out_lower = token_out.lower()
    
    i = None
    j = None
    for sym, data in TOKENS.items():
        addr = data["address"].lower()
        if addr == t_in_lower: i = indices.get(sym)
        if addr == t_out_lower: j = indices.get(sym)
            
    if i is None or j is None:
        raise ValueError(f"Invalid tokens for Curve 3Pool: {token_in} -> {token_out}")
        
    fn = contract.functions.exchange(i, j, amount_in, amount_out_min)
    hex_data = fn._encode_transaction_data()
    return bytes.fromhex(hex_data[2:]) if isinstance(hex_data, str) else bytes(hex_data)

# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_tri_arbitrage(
    w3: AsyncWeb3,
    route_type: int,
    sym1: str,
    sym2: str,
    dex1: str, fee1: int,
    dex2: str, fee2: int,
    dex3: str, fee3: int,
    flashloan_usdc: int,
    out1: int, out2: int, out3: int,
    net_profit_usd: float,
) -> Optional[str]:
    """Execute the multi-hop triangular arbitrage via TriArbitrageur."""
    if not TRI_ARBITRAGEUR_ADDRESS or not PRIVATE_KEY:
        logger.warning("⚠️  Cannot execute — contract address or private key missing")
        return None

    try:
        account = w3.eth.account.from_key(PRIVATE_KEY)
        contract = w3.eth.contract(
            address=w3.to_checksum_address(TRI_ARBITRAGEUR_ADDRESS),
            abi=TRI_ARB_ABI,
        )

        token1_in = USDC_ADDRESS
        token1_out = TOKENS[sym1]["address"]
        token2_in = TOKENS[sym1]["address"]
        token2_out = TOKENS[sym2]["address"]
        token3_in = TOKENS[sym2]["address"]
        token3_out = USDC_ADDRESS

        # ── Payload 1 ──
        dex1_config = DEXES[dex1]
        min_out1 = out1 * (10000 - LEG_A_SLIPPAGE_BPS) // 10000
        if dex1_config["type"] == "curve":
            payload1 = build_curve_swap_calldata(w3, dex1_config["router"], token1_in, token1_out, flashloan_usdc, min_out1, dex1_config)
        else:
            payload1 = build_v3_swap_calldata(w3, token1_in, token1_out, fee1, TRI_ARBITRAGEUR_ADDRESS, flashloan_usdc, min_out1)

        # ── Payload 2 ──
        dex2_config = DEXES[dex2]
        min_out2 = out2 * (10000 - LEG_A_SLIPPAGE_BPS) // 10000
        if dex2_config["type"] == "curve":
            payload2 = build_curve_swap_calldata(w3, dex2_config["router"], token2_in, token2_out, out1, min_out2, dex2_config)
        else:
            payload2 = build_v3_swap_calldata(w3, token2_in, token2_out, fee2, TRI_ARBITRAGEUR_ADDRESS, out1, min_out2)

        # ── Payload 3 ──
        dex3_config = DEXES[dex3]
        flashloan_fee = (flashloan_usdc * AAVE_FLASHLOAN_FEE_BPS) // 10000
        min_out3 = flashloan_usdc + flashloan_fee
        if dex3_config["type"] == "curve":
            payload3 = build_curve_swap_calldata(w3, dex3_config["router"], token3_in, token3_out, out2, min_out3, dex3_config)
        else:
            payload3 = build_v3_swap_calldata(w3, token3_in, token3_out, fee3, TRI_ARBITRAGEUR_ADDRESS, out2, min_out3)

        # ── Grouping Route Tuple ──
        # [(router, tokenIn, payload), ...]
        route_structs = [
            (w3.to_checksum_address(dex1_config["router"]), w3.to_checksum_address(token1_in), payload1),
            (w3.to_checksum_address(dex2_config["router"]), w3.to_checksum_address(token2_in), payload2),
            (w3.to_checksum_address(dex3_config["router"]), w3.to_checksum_address(token3_in), payload3),
        ]

        # ── Transaction ──
        nonce = await w3.eth.get_transaction_count(account.address)
        gas_price = await w3.eth.gas_price

        tx = await contract.functions.requestFlashLoan(
            w3.to_checksum_address(USDC_ADDRESS),
            flashloan_usdc,
            route_structs,
        ).build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": 1_200_000, # More gas due to 3 hops
            "maxFeePerGas": gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei(0.01, "gwei"),
        })

        # ── Simulate ──
        logger.info(
            f"🧪 Simulating Tri-Arb: Route {route_type}: USDC → {sym1} → {sym2} → USDC\n"
            f"    DEXes: {dex1} → {dex2} → {dex3}\n"
            f"    Outputs Expected: {out1} → {out2} → {out3}"
        )
        try:
            await w3.eth.call(tx)
            logger.info(f"✅ Simulation passed for Route {sym1}→{sym2}")
        except Exception as sim_err:
            logger.warning(f"❌ Simulation reverted for Route {sym1}→{sym2}: {sim_err}")
            return None

        # ── Broadcast ──
        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = tx_hash.hex()

        logger.info(f"🚀 TRI-ARB TX SENT: {tx_hash_hex} | +${net_profit_usd:.2f}")

        # Log to DB
        db_manager.record_arb_execution(
            tx_hash=tx_hash_hex,
            token_pair=f"TRI: {sym1}->{sym2}",
            dex_a=dex1,
            dex_b=f"{dex2}->{dex3}",
            profit_usd=net_profit_usd,
        )

        send_telegram_alert(
            f"🔄 <b>Tri-Arb Executed</b>\n"
            f"📊 Path: USDC → {sym1} → {sym2} → USDC\n"
            f"🔀 Route: {dex1} → {dex2} → {dex3}\n"
            f"💰 Profit: +${net_profit_usd:.2f}\n"
            f"🔗 <a href='https://arbiscan.io/tx/{tx_hash_hex}'>Arbiscan</a>"
        )

        return tx_hash_hex

    except Exception as e:
        logger.error(f"❌ Execution failed for Route {sym1}→{sym2}: {e}")
        send_telegram_alert(
            f"⚠️ <b>Tri-Arb Execution Failed</b>\n"
            f"📊 <code>USDC-{sym1}-{sym2}</code>\n"
            f"<code>{e}</code>"
        )
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SCANNING LOOP
# ═══════════════════════════════════════════════════════════════════════════════

async def get_eth_price(rpc_manager: SmartAsyncRPCManager) -> float:
    """Get ETH price in USD via Multicall on Free node."""
    while True:
        try:
            w3 = await rpc_manager.get_optimal_w3(is_critical=False)
            multicall = w3.eth.contract(address=w3.to_checksum_address(MULTICALL3_ADDRESS), abi=MULTICALL3_ABI)
            target, data = _encode_quoter_call(w3, UNI_V3_QUOTER, TOKENS["WETH"]["address"], USDC_ADDRESS, 10**18, 500, DEXES["Uniswap_V3"])
            result = await multicall.functions.tryAggregate(False, [(target, data)]).call({'gas': 300_000_000})
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
            await asyncio.sleep(5) # Wait before retrying
        return 3000.0  # Fallback estimate

async def perform_multicall(multicall_contract, calls_list: List[Tuple[str, bytes]]) -> List[Tuple[bool, bytes]]:
    """Helper to dispatch multicall in chunks and flatten output."""
    if not calls_list:
        return []
        
    chunks = [calls_list[i : i + MULTICALL_CHUNK_SIZE] for i in range(0, len(calls_list), MULTICALL_CHUNK_SIZE)]
    tasks = [multicall_contract.functions.tryAggregate(False, chunk).call({'gas': 300_000_000}) for chunk in chunks]
    chunk_results = await asyncio.gather(*tasks)
    return [item for sublist in chunk_results for item in sublist]


async def scan_triangular_spreads(rpc_manager: SmartAsyncRPCManager, block_number: int, eth_price_usd: float):
    now = time.time()
    w3 = await rpc_manager.get_optimal_w3(is_critical=False)
    multicall = w3.eth.contract(address=w3.to_checksum_address(MULTICALL3_ADDRESS), abi=MULTICALL3_ABI)
    
    # ════════════════════════════════════════════════════════════════════════════
    # STAGE 1: BATCH LEG 1 (USDC → ALL TOKENS)
    # ════════════════════════════════════════════════════════════════════════════
    leg1_calls = []
    leg1_map = []
    
    for sym, token_info in TOKENS.items():
        if sym == "USDC": continue
        for dex_name, config in DEXES.items():
            if config["type"] == "curve" and sym not in ["USDT", "DAI"]: continue
            for fee in config["fee_tiers"]:
                t, c = _encode_quoter_call(w3, config["quoter"], USDC_ADDRESS, token_info["address"], FLASHLOAN_USDC_AMOUNT, fee, config)
                leg1_calls.append((t, c))
                leg1_map.append((sym, dex_name, fee, config["type"]))
                
    try:
        leg1_results = await perform_multicall(multicall, leg1_calls)
    except Exception as e:
        logger.error(f"❌ Leg 1 Multicall failed: {e}")
        await rpc_manager.handle_rate_limit()
        return 0
        
    best_leg1 = {sym: {} for sym in TOKENS}
    for idx, (success, ret) in enumerate(leg1_results):
        if not success or not ret: continue
        sym, dex, fee, dex_type = leg1_map[idx]
        out = _decode_quoter_result(ret, dex_type)
        if out > 0:
            if dex not in best_leg1[sym] or out > best_leg1[sym][dex].get("amount_out", 0):
                best_leg1[sym][dex] = {"amount_out": out, "fee": fee}

    # ════════════════════════════════════════════════════════════════════════════
    # STAGE 2: BATCH LEG 2
    # ════════════════════════════════════════════════════════════════════════════
    leg2_calls = []
    leg2_map = []
    
    # Route 1: Hub -> Target
    for hub in HUBS:
        for dex1, data1 in best_leg1[hub].items():
            in_amt = data1["amount_out"]
            if in_amt <= 0: continue
            fee1 = data1["fee"]
            for tgt in TARGETS:
                for dex2, config in DEXES.items():
                    if config["type"] == "curve" and tgt not in ["USDT", "DAI"]: continue
                    for fee2 in config["fee_tiers"]:
                        t, c = _encode_quoter_call(w3, config["quoter"], TOKENS[hub]["address"], TOKENS[tgt]["address"], in_amt, fee2, config)
                        leg2_calls.append((t, c))
                        leg2_map.append((1, hub, tgt, dex1, fee1, dex2, fee2, config["type"], in_amt))

    # Route 2: Target -> Hub
    for tgt in TARGETS:
        for dex1, data1 in best_leg1[tgt].items():
            in_amt = data1["amount_out"]
            if in_amt <= 0: continue
            fee1 = data1["fee"]
            for hub in HUBS:
                for dex2, config in DEXES.items():
                    if config["type"] == "curve" and hub not in ["USDT", "DAI"]: continue
                    for fee2 in config["fee_tiers"]:
                        t, c = _encode_quoter_call(w3, config["quoter"], TOKENS[tgt]["address"], TOKENS[hub]["address"], in_amt, fee2, config)
                        leg2_calls.append((t, c))
                        leg2_map.append((2, tgt, hub, dex1, fee1, dex2, fee2, config["type"], in_amt))

    if not leg2_calls: return 0
    
    try:
        leg2_results = await perform_multicall(multicall, leg2_calls)
    except Exception as e:
        logger.warning(f"⚠️ Leg 2 Multicall failed: {e}")
        return 0

    best_leg2_r1 = {}
    best_leg2_r2 = {}
    for idx, (success, ret) in enumerate(leg2_results):
        if not success or not ret: continue
        r_type, sym1, sym2, dex1, fee1, dex2, fee2, dex_type2, leg2_in = leg2_map[idx]
        out = _decode_quoter_result(ret, dex_type2)
        if out > 0:
            key = (sym1, sym2, dex1, dex2)
            tgt_dict = best_leg2_r1 if r_type == 1 else best_leg2_r2
            if key not in tgt_dict or out > tgt_dict[key].get("amount_out", 0):
                tgt_dict[key] = {"amount_out": out, "fee1": fee1, "fee2": fee2, "in_amount": leg2_in}

    # ════════════════════════════════════════════════════════════════════════════
    # STAGE 3: BATCH LEG 3
    # ════════════════════════════════════════════════════════════════════════════
    leg3_calls = []
    leg3_map = []

    # Route 1: Target -> USDC
    for (hub, tgt, dex1, dex2), data in best_leg2_r1.items():
        in_amt = data["amount_out"]
        if in_amt <= 0: continue
        fee1, fee2, leg2_in = data["fee1"], data["fee2"], data["in_amount"]
        
        # Route logic check
        route_key = f"{hub}-{tgt}/{dex1}-{dex2}"
        if route_key in route_blacklist:
            if now - route_blacklist[route_key] < ROUTE_COOLDOWN_SECONDS: continue
            else:
                del route_blacklist[route_key]
                route_failures.pop(route_key, None)

        for dex3, config in DEXES.items():
            if config["type"] == "curve" and tgt not in ["USDT", "DAI"]: continue
            for fee3 in config["fee_tiers"]:
                t, c = _encode_quoter_call(w3, config["quoter"], TOKENS[tgt]["address"], USDC_ADDRESS, in_amt, fee3, config)
                leg3_calls.append((t, c))
                leg3_map.append((1, hub, tgt, dex1, fee1, dex2, fee2, dex3, fee3, config["type"], leg2_in, in_amt))

    # Route 2: Hub -> USDC
    for (tgt, hub, dex1, dex2), data in best_leg2_r2.items():
        in_amt = data["amount_out"]
        if in_amt <= 0: continue
        fee1, fee2, leg2_in = data["fee1"], data["fee2"], data["in_amount"]
        
        route_key = f"{tgt}-{hub}/{dex1}-{dex2}"
        if route_key in route_blacklist:
            if now - route_blacklist[route_key] < ROUTE_COOLDOWN_SECONDS: continue
            else:
                del route_blacklist[route_key]
                route_failures.pop(route_key, None)

        for dex3, config in DEXES.items():
            if config["type"] == "curve": continue # Hubs WETH/ARB are never curve supported
            for fee3 in config["fee_tiers"]:
                t, c = _encode_quoter_call(w3, config["quoter"], TOKENS[hub]["address"], USDC_ADDRESS, in_amt, fee3, config)
                leg3_calls.append((t, c))
                leg3_map.append((2, tgt, hub, dex1, fee1, dex2, fee2, dex3, fee3, config["type"], leg2_in, in_amt))

    if not leg3_calls: return 0
    try:
        leg3_results = await perform_multicall(multicall, leg3_calls)
    except Exception as e:
        logger.warning(f"⚠️ Leg 3 Multicall failed: {e}")
        return 0

    best_profitable = None
    max_net_profit = 0
    spreads_found = 0

    # Parse and finding best profit
    for idx, (success, ret) in enumerate(leg3_results):
        if not success or not ret: continue
        r_type, sym1, sym2, dex1, fee1, dex2, fee2, dex3, fee3, dex_type3, leg2_in, leg3_in = leg3_map[idx]
        
        out_usdc = _decode_quoter_result(ret, dex_type3)
        if out_usdc == 0: continue
        
        flashloan_fee = (FLASHLOAN_USDC_AMOUNT * AAVE_FLASHLOAN_FEE_BPS) // 10000
        total_repay = FLASHLOAN_USDC_AMOUNT + flashloan_fee
        gross_profit_raw = out_usdc - total_repay
        gross_profit_usd = gross_profit_raw / (10 ** USDC_DECIMALS)
        spread_pct = (gross_profit_raw / FLASHLOAN_USDC_AMOUNT) * 100.0

        if spread_pct > 0.05:
            spreads_found += 1
            if spread_pct > 0.1: # Tame logs
                logger.info(
                    f"📊 TRI-ARB: USDC→{sym1}→{sym2}→USDC | {dex1}→{dex2}→{dex3} | "
                    f"Spread: {spread_pct:.3f}% | Gross: ${gross_profit_usd:.2f}"
                )

        if gross_profit_usd > 0:
            gas_price = await w3.eth.gas_price
            gas_cost_wei = 1_000_000 * gas_price # Higher estimate for tri-arb calculation padding
            net_profit = estimate_net_profit_usd(gross_profit_usd, gas_cost_wei, eth_price_usd)
            
            if net_profit >= MIN_PROFIT_USD and net_profit > max_net_profit:
                max_net_profit = net_profit
                best_profitable = {
                    "r_type": r_type, "sym1": sym1, "sym2": sym2,
                    "dex1": dex1, "fee1": fee1, 
                    "dex2": dex2, "fee2": fee2, 
                    "dex3": dex3, "fee3": fee3,
                    "out1": leg2_in, "out2": leg3_in, "out3": out_usdc,
                    "net_profit": net_profit
                }

    if best_profitable:
        bp = best_profitable
        logger.info(
            f"💰 BEST TRI-ARB: {bp['sym1']}-{bp['sym2']} | Net: +${bp['net_profit']:.2f} | "
            f"Path: {bp['dex1']} → {bp['dex2']} → {bp['dex3']}"
        )
        tx_hash = await execute_tri_arbitrage(
            w3, bp["r_type"], bp["sym1"], bp["sym2"],
            bp["dex1"], bp["fee1"], bp["dex2"], bp["fee2"], bp["dex3"], bp["fee3"],
            FLASHLOAN_USDC_AMOUNT, bp["out1"], bp["out2"], bp["out3"], bp["net_profit"]
        )
        
        route_key = f"{bp['sym1']}-{bp['sym2']}/{bp['dex1']}-{bp['dex2']}"
        if not tx_hash:
            route_failures[route_key] = route_failures.get(route_key, 0) + 1
            if route_failures[route_key] >= MAX_ROUTE_FAILURES:
                route_blacklist[route_key] = now
                logger.warning(f"🚫 Route {route_key} blacklisted")
        else:
            route_failures.pop(route_key, None)

    return spreads_found


async def main():
    logger.info("═══════════════════════════════════════════════════════════")
    logger.info("🛸 ANTI-GRAVITY — Triangular Arbitrage Engine")
    logger.info("═══════════════════════════════════════════════════════════")
    
    # Initialize RPC Manager
    rpc_manager = SmartAsyncRPCManager()
    await rpc_manager.connect_all()
    
    # Start background task for RPC ranking
    asyncio.create_task(rpc_manager._rank_nodes_loop())
    
    logger.info(f"📊 Scanning {len(TOKENS)} tokens via {len(HUBS)} Hubs across {len(DEXES)} DEXs")
    logger.info("═══════════════════════════════════════════════════════════")

    # --- STARTUP PROTECTION ---
    # Jitter to prevent all PM2 instances from hitting RPC simultaneously on boot
    await asyncio.sleep(random.uniform(1.0, 10.0))
    while True:
        try:
            w3 = await rpc_manager.get_optimal_w3(is_critical=False)
            chain_id = await w3.eth.chain_id
            
            if chain_id != 42161:
                logger.warning(f"⚠️  Expected Arbitrum One (42161), got chain {chain_id}")

            send_telegram_alert(
                f"🔄 <b>Tri-Arb Engine Started</b>\n"
                f"🔗 RPC: <code>{rpc_manager.premium_url[:40]}...</code>"
            )

            last_block = 0
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
                await asyncio.sleep(20)

    db_manager.init_db()

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
            
            if current_block <= last_block:
                continue

            if not await sentinel.should_scan():
                continue

            await asyncio.sleep(random.uniform(1.0, 7.0))

            scan_start = time.time()

            scan_start = time.time()
            
            if time.time() - eth_price_refresh > 300:
                eth_price_usd = await get_eth_price(rpc_manager)
                eth_price_refresh = time.time()
                logger.info(f"📈 ETH Price refreshed: ${eth_price_usd:,.0f}")

            spreads = await scan_triangular_spreads(rpc_manager, current_block, eth_price_usd)

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

        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down gracefully...")
            break
        except Exception as e:
            if rpc_manager.is_rate_limit_error(e):
                await rpc_manager.handle_rate_limit(w3)
                w3 = await rpc_manager.get_optimal_w3(is_critical=False)
                multicall = w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)
            else:
                logger.error(f"❌ Loop error: {e}")
                logger.debug(traceback.format_exc())
            await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛸 Anti-Gravity Tri-Arb Engine stopped.")
