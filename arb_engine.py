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
from itertools import combinations
from typing import Dict, List, Optional, Tuple

import requests as req_sync
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError
from eth_abi import encode

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
ALL_RPCS = [PRIMARY_RPC] + FALLBACK_RPCS
current_rpc_idx = 0  # Index into ALL_RPCS

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
# TOKEN CONFIGURATION — Arbitrum Mainnet (Real Addresses)
# ═══════════════════════════════════════════════════════════════════════════════
TOKENS: Dict[str, dict] = {
    "WETH": {
        "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "decimals": 18,
        "quote_amount": 1 * 10**18,          # 1 WETH
    },
    "ARB": {
        "address": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "decimals": 18,
        "quote_amount": 5000 * 10**18,        # 5000 ARB
    },
    "MAGIC": {
        "address": "0x539bdE0d7Dbd33f84E8aaf9084C942D9800Ef002",
        "decimals": 18,
        "quote_amount": 10000 * 10**18,       # 10000 MAGIC
    },
    "GRAIL": {
        "address": "0x3d9907F9a368ad0a51Be60f7Da3b97cf940982D8",
        "decimals": 18,
        "quote_amount": 10 * 10**18,          # 10 GRAIL
    },
    "PENDLE": {
        "address": "0x0c880f6761F1af8d9Aa9C466984785263cf79560",
        "decimals": 18,
        "quote_amount": 2000 * 10**18,        # 2000 PENDLE
    },
    "GMX": {
        "address": "0xfc5A1A6EB076a2C7AD06EDb220f4daaC9AF172af",
        "decimals": 18,
        "quote_amount": 50 * 10**18,          # 50 GMX
    },
    "RDNT": {
        "address": "0x3082CC23568eA640225c2467653dB90e9250AaA0",
        "decimals": 18,
        "quote_amount": 50000 * 10**18,       # 50000 RDNT
    },
}

# Base quote token
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC (Arbitrum)
USDC_DECIMALS = 6

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
MIN_PROFIT_USD = 0.50              # Minimum net profit after all costs
MAX_GAS_PRICE_GWEI = 1.0          # Arbitrum gas is cheap, but cap it
SCAN_COOLDOWN_SECONDS = 0.5       # Minimum time between scans
MAX_SLIPPAGE_BPS = 50             # 0.5% max slippage for trade sizing

# ═══════════════════════════════════════════════════════════════════════════════
# QUOTE FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

async def get_v3_quote(
    w3: AsyncWeb3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
) -> Optional[int]:
    """Fetch a quote from a Uniswap V3 / SushiSwap V3 QuoterV2."""
    try:
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
                0,  # sqrtPriceLimitX96 = 0 (no limit)
            )
        ).call()
        return result[0]  # amountOut
    except (ContractLogicError, Exception) as e:
        # Pool may not exist for this fee tier — silently skip
        return None


async def get_algebra_quote(
    w3: AsyncWeb3,
    quoter_address: str,
    token_in: str,
    token_out: str,
    amount_in: int,
) -> Optional[int]:
    """Fetch a quote from Camelot (Algebra-style) Quoter."""
    try:
        quoter = w3.eth.contract(
            address=w3.to_checksum_address(quoter_address),
            abi=ALGEBRA_QUOTER_ABI,
        )
        result = await quoter.functions.quoteExactInputSingle(
            w3.to_checksum_address(token_in),
            w3.to_checksum_address(token_out),
            amount_in,
            0,  # limitSqrtPrice = 0
        ).call()
        return result[0]  # amountOut
    except (ContractLogicError, Exception):
        return None


async def get_best_quote_for_dex(
    w3: AsyncWeb3,
    dex_name: str,
    dex_config: dict,
    token_in: str,
    token_out: str,
    amount_in: int,
) -> Optional[int]:
    """
    Get the best quote across all fee tiers for a given DEX.
    Returns the highest amountOut, or None if all fee tiers fail.
    """
    best_quote = None

    if dex_config["type"] == "v3":
        # Try all fee tiers, keep the best
        for fee in dex_config["fee_tiers"]:
            quote = await get_v3_quote(
                w3, dex_config["quoter"], token_in, token_out, amount_in, fee
            )
            if quote is not None and (best_quote is None or quote > best_quote):
                best_quote = quote

    elif dex_config["type"] == "algebra":
        # Algebra quoters don't use fee tiers — single call
        best_quote = await get_algebra_quote(
            w3, dex_config["quoter"], token_in, token_out, amount_in
        )

    return best_quote


# ═══════════════════════════════════════════════════════════════════════════════
# SPREAD CALCULATION
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_spread(
    quotes: Dict[str, int],
    amount_in: int,
    token_decimals: int,
) -> Optional[dict]:
    """
    Given quotes from multiple DEXs for the same pair & direction,
    find the max spread between any two DEXs.
    Returns: {dex_high, dex_low, spread_pct, buy_price, sell_price} or None
    """
    valid = {k: v for k, v in quotes.items() if v is not None and v > 0}
    if len(valid) < 2:
        return None

    dex_high = max(valid, key=valid.get)
    dex_low = min(valid, key=valid.get)

    high_val = valid[dex_high]
    low_val = valid[dex_low]

    if low_val == 0:
        return None

    spread_pct = ((high_val - low_val) / low_val) * 100.0

    return {
        "dex_high": dex_high,       # Buy here (cheaper — gives more output)
        "dex_low": dex_low,         # Sell here — wait, reverse:
        # Actually: high_val DEX gives MORE output → it's the SELL side
        # low_val DEX gives LESS output → it's the BUY side (token is more expensive)
        # For arb: Buy on dex_low (where token is cheap relative to USDC),
        #          Sell on dex_high (where token is expensive relative to USDC)
        # But since we're quoting TOKEN→USDC, high output = better sell price
        "sell_dex": dex_high,
        "buy_dex": dex_low,
        "spread_pct": spread_pct,
        "amount_high": high_val,
        "amount_low": low_val,
    }


def calculate_optimal_trade_size(
    base_amount: int,
    spread_pct: float,
    token_decimals: int,
) -> int:
    """
    Basic optimal trade sizing to avoid excessive price impact.
    If spread is thin, reduce trade size proportionally.
    If spread is fat, use full size.
    """
    if spread_pct <= 0.1:
        # Very thin spread — use 10% of base amount
        return base_amount // 10
    elif spread_pct <= 0.3:
        # Moderate spread — use 30%
        return (base_amount * 30) // 100
    elif spread_pct <= 0.5:
        # Decent spread — use 50%
        return base_amount // 2
    elif spread_pct <= 1.0:
        # Good spread — use 75%
        return (base_amount * 75) // 100
    else:
        # Fat spread — use full amount
        return base_amount


def estimate_net_profit_usd(
    amount_high: int,
    amount_low: int,
    flashloan_amount_usdc: int,
    gas_cost_wei: int,
    eth_price_usd: float,
) -> float:
    """
    Calculate net profit in USD after deducting:
      1. Aave flashloan fee (0.05%)
      2. Gas cost
    The spread itself already accounts for DEX swap fees (they're embedded in quotes).
    """
    # Gross spread in USDC terms (6 decimals)
    gross_spread_usdc = (amount_high - amount_low) / (10**USDC_DECIMALS)

    # Flashloan fee
    flashloan_fee_usdc = (flashloan_amount_usdc / (10**USDC_DECIMALS)) * (AAVE_FLASHLOAN_FEE_BPS / 10000)

    # Gas cost in USD
    gas_cost_eth = gas_cost_wei / (10**18)
    gas_cost_usd = gas_cost_eth * eth_price_usd

    net_profit = gross_spread_usdc - flashloan_fee_usdc - gas_cost_usd
    return net_profit


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
    return fn._encode_transaction_data()


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_arbitrage(
    w3: AsyncWeb3,
    token_symbol: str,
    token_address: str,
    buy_dex: str,
    sell_dex: str,
    trade_amount: int,
    amount_out_min: int,
    net_profit_usd: float,
) -> Optional[str]:
    """
    Build, simulate, and broadcast an arbitrage transaction.
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

        buy_config = DEXES[buy_dex]
        sell_config = DEXES[sell_dex]

        # Build swap calldata for both legs
        # Leg A: USDC → Token (buy on cheaper DEX)
        data_a = build_v3_swap_calldata(
            w3,
            USDC_ADDRESS,
            token_address,
            500,  # 0.05% fee tier (most liquid for majors)
            DEX_ARBITRAGEUR_ADDRESS,
            trade_amount,
            0,    # amountOutMin handled by profitability check in contract
        )

        # Leg B: Token → USDC (sell on more expensive DEX)
        data_b = build_v3_swap_calldata(
            w3,
            token_address,
            USDC_ADDRESS,
            500,
            DEX_ARBITRAGEUR_ADDRESS,
            0,    # Will use full intermediate balance in contract
            amount_out_min,
        )

        # Encode ArbParams struct
        arb_params = encode(
            ["(address,bytes,address,bytes,address)"],
            [(
                w3.to_checksum_address(buy_config["router"]),
                data_a,
                w3.to_checksum_address(sell_config["router"]),
                data_b,
                w3.to_checksum_address(token_address),
            )]
        )

        # Build the flashloan transaction
        nonce = await w3.eth.get_transaction_count(account.address)
        gas_price = await w3.eth.gas_price

        tx = await contract.functions.requestFlashLoan(
            w3.to_checksum_address(USDC_ADDRESS),
            trade_amount,
            arb_params,
        ).build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": 800_000,  # Conservative gas limit for flashloan + 2 swaps
            "maxFeePerGas": gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei(0.01, "gwei"),
        })

        # ── Simulation Shield: eth_call before broadcast ──
        logger.info(f"🧪 Simulating arb: {token_symbol}/USDC | {buy_dex}→{sell_dex}")
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
            dex_a=buy_dex,
            dex_b=sell_dex,
            profit_usd=net_profit_usd,
        )

        # Telegram notification
        send_telegram_alert(
            f"🔄 <b>Arb Executed</b>\n"
            f"📊 Pair: <code>{token_symbol}/USDC</code>\n"
            f"🔀 Route: {buy_dex} → {sell_dex}\n"
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

async def get_eth_price(w3: AsyncWeb3) -> float:
    """Get ETH price in USD by quoting 1 WETH → USDC on Uniswap V3."""
    try:
        quote = await get_v3_quote(
            w3,
            UNI_V3_QUOTER,
            TOKENS["WETH"]["address"],
            USDC_ADDRESS,
            10**18,  # 1 WETH
            500,     # 0.05% pool
        )
        if quote:
            return quote / (10**USDC_DECIMALS)
    except Exception:
        pass
    return 2500.0  # Fallback estimate


async def scan_and_execute(w3: AsyncWeb3, block_number: int, eth_price_usd: float):
    """
    Core scan loop for a single block.
    Fetches quotes for all tokens across all DEXs, calculates spreads,
    and executes if profitable.
    """
    spreads_found = 0

    for symbol, token_info in TOKENS.items():
        if symbol == "WETH":
            # Quote WETH→USDC directly
            token_in = token_info["address"]
            token_out = USDC_ADDRESS
        else:
            # Quote TOKEN→USDC
            token_in = token_info["address"]
            token_out = USDC_ADDRESS

        amount_in = token_info["quote_amount"]

        # ── Fetch quotes from all DEXs concurrently ──
        quote_tasks = {}
        for dex_name, dex_config in DEXES.items():
            quote_tasks[dex_name] = get_best_quote_for_dex(
                w3, dex_name, dex_config, token_in, token_out, amount_in
            )

        # Await all quotes in parallel
        results = {}
        for dex_name, task in quote_tasks.items():
            results[dex_name] = await task

        # ── Calculate spread ──
        spread = calculate_spread(results, amount_in, token_info["decimals"])

        if spread is None:
            continue

        spread_pct = spread["spread_pct"]

        # Log all spreads > 0.01% to database for charting
        if spread_pct > 0.01:
            try:
                db_manager.log_arb_spread(
                    token_pair=f"{symbol}/USDC",
                    dex_a=spread["sell_dex"],
                    dex_b=spread["buy_dex"],
                    spread_percent=round(spread_pct, 4),
                )
            except Exception:
                pass
            spreads_found += 1

        # ── Log significant spreads ──
        if spread_pct > 0.05:
            logger.info(
                f"📊 {symbol}/USDC | Spread: {spread_pct:.3f}% | "
                f"High: {spread['sell_dex']} ({spread['amount_high']}) | "
                f"Low: {spread['buy_dex']} ({spread['amount_low']})"
            )

        # ── Profitability Check ──
        if spread_pct > 0.08:  # Minimum spread threshold worth evaluating
            # Calculate optimal trade size
            trade_amount = calculate_optimal_trade_size(
                amount_in, spread_pct, token_info["decimals"]
            )

            # Estimate gas cost (typical arb tx on Arbitrum ≈ 500k gas)
            gas_price = await w3.eth.gas_price
            gas_cost_wei = 500_000 * gas_price

            # For token→USDC arb, flashloan amount is in USDC
            # We need to estimate the USDC equivalent of our trade
            # Use the lower quote as a conservative estimate
            flashloan_usdc = spread["amount_low"]

            net_profit = estimate_net_profit_usd(
                spread["amount_high"],
                spread["amount_low"],
                flashloan_usdc,
                gas_cost_wei,
                eth_price_usd,
            )

            if net_profit >= MIN_PROFIT_USD:
                logger.info(
                    f"💰 PROFITABLE: {symbol}/USDC | Net: +${net_profit:.2f} | "
                    f"Route: {spread['buy_dex']} → {spread['sell_dex']}"
                )

                # Execute the arbitrage
                tx_hash = await execute_arbitrage(
                    w3=w3,
                    token_symbol=symbol,
                    token_address=token_info["address"],
                    buy_dex=spread["buy_dex"],
                    sell_dex=spread["sell_dex"],
                    trade_amount=trade_amount,
                    amount_out_min=flashloan_usdc,  # Minimum to repay flashloan
                    net_profit_usd=net_profit,
                )

                if tx_hash:
                    logger.info(f"✅ Arb executed: {tx_hash}")

    return spreads_found


async def main():
    """Main entry point — continuous block-by-block scanning with RPC failover."""
    global current_rpc_idx

    logger.info("═══════════════════════════════════════════════════════════")
    logger.info("🛸 ANTI-GRAVITY — DEX Arbitrage Engine v2.0")
    logger.info("═══════════════════════════════════════════════════════════")
    logger.info(f"🔗 Primary RPC: {PRIMARY_RPC[:50]}...")
    logger.info(f"🔗 Fallback RPCs: {len(FALLBACK_RPCS)} configured")
    logger.info(f"📊 Scanning {len(TOKENS)} tokens across {len(DEXES)} DEXs")
    logger.info(f"🎯 Tokens: {', '.join(TOKENS.keys())}")
    logger.info(f"🏪 DEXs: {', '.join(DEXES.keys())}")
    logger.info(f"💰 Min Profit: ${MIN_PROFIT_USD}")
    logger.info("═══════════════════════════════════════════════════════════")

    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ALL_RPCS[current_rpc_idx]))

    if not await w3.is_connected():
        logger.critical("❌ Failed to connect to RPC — exiting")
        return

    chain_id = await w3.eth.chain_id
    logger.info(f"✅ Connected to chain {chain_id}")

    if chain_id != 42161:
        logger.warning(f"⚠️  Expected Arbitrum One (42161), got chain {chain_id}")

    # Initialize DB
    db_manager.init_db()
    logger.info("✅ Database initialized")

    # Telegram startup notification
    send_telegram_alert(
        f"🔄 <b>DEX Arb Engine Started</b>\n"
        f"📊 {len(TOKENS)} tokens × {len(DEXES)} DEXs\n"
        f"🔗 RPC: <code>{ALL_RPCS[current_rpc_idx][:40]}...</code>"
    )

    last_block = 0
    eth_price_usd = await get_eth_price(w3)
    eth_price_refresh = time.time()
    consecutive_errors = 0
    logger.info(f"📈 ETH Price: ${eth_price_usd:,.0f}")

    # ── Infinite Scan Loop ──
    while True:
        try:
            scan_start = time.time()
            current_block = await w3.eth.block_number

            # Reset error counter on success
            consecutive_errors = 0

            # Skip if same block
            if current_block <= last_block:
                await asyncio.sleep(SCAN_COOLDOWN_SECONDS)
                continue

            # Refresh ETH price every 5 minutes
            if time.time() - eth_price_refresh > 300:
                eth_price_usd = await get_eth_price(w3)
                eth_price_refresh = time.time()
                logger.info(f"📈 ETH Price refreshed: ${eth_price_usd:,.0f}")

            # ── Run scan ──
            spreads = await scan_and_execute(w3, current_block, eth_price_usd)

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
            consecutive_errors += 1
            err_str = str(e).lower()
            is_rpc_error = any(x in err_str for x in ["429", "403", "rate", "forbidden", "timeout", "connection"])

            logger.error(f"❌ Loop error (#{consecutive_errors}): {e}")
            logger.debug(traceback.format_exc())

            # ── RPC Failover on 3 consecutive errors ──
            if is_rpc_error and consecutive_errors >= 3 and len(ALL_RPCS) > 1:
                old_idx = current_rpc_idx
                current_rpc_idx = (current_rpc_idx + 1) % len(ALL_RPCS)
                new_rpc = ALL_RPCS[current_rpc_idx]
                logger.warning(f"🔄 3 strikes! Switching RPC [{old_idx+1}→{current_rpc_idx+1}/{len(ALL_RPCS)}]: {new_rpc[:40]}...")
                w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(new_rpc))
                consecutive_errors = 0
                send_telegram_alert(
                    f"⚠️ <b>Arb Engine RPC Failover</b>\n"
                    f"🔄 Switched to: <code>{new_rpc[:40]}...</code>"
                )
            elif consecutive_errors >= 10:
                send_telegram_alert(
                    f"🆘 <b>Arb Engine: {consecutive_errors} consecutive errors</b>\n"
                    f"<code>{e}</code>"
                )

            await asyncio.sleep(3)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛸 Anti-Gravity DEX Arb Engine stopped.")
