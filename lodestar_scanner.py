import os
import json
import time
import requests
import traceback
import random
import threading
import asyncio
import aiohttp
from web3 import Web3
from dotenv import load_dotenv
from eth_abi import decode

# Load Env
load_dotenv()

def ensure_json_exists(filepath: str):
    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            json.dump({"tier_1_danger": [], "tier_2_watchlist": []}, f)

# Ensure local targets file exists for sniper bots
ensure_json_exists("lodestar_targets.json")

# --- RPC MANAGER (Strict QoS Lane: Tier 3 → SCANNER_RPC) ---
class SmartSyncRPCManager:
    """
    Round-Robin Sync RPC Manager:
    - Rotates through all available RPC nodes on rate limit / quota errors.
    """
    HARD_ERROR_KEYWORDS = ["serverdisconnected", "connectionerror", "connection refused",
                           "cannot connect", "server disconnected", "connectionreseterror",
                           "oserror", "gaierror", "remotedisconnected",
                           "413", "too large", "entity too large"]

    def __init__(self):
        # Force load latest .env to capture manual PM2 changes
        load_dotenv(override=True)
        self.primary_url = os.getenv("SCANNER_RPC")
        fallback_rpcs_raw = os.getenv("FALLBACK_RPCS", "").replace('"', '').replace("'", "").split(",")
        self.fallback_urls = [url.strip() for url in fallback_rpcs_raw if url.strip()]

        if not self.primary_url:
            print("❌ SCANNER_RPC not found in .env. Exiting.")
            exit(1)

        self.rpc_urls = [self.primary_url] + self.fallback_urls
        self.current_index = 0
        self.active_url = self.rpc_urls[self.current_index]

        self.premium_w3 = Web3(Web3.HTTPProvider(self.active_url, request_kwargs={'timeout': 60}))
        
        # Basic connectivity check
        if not self.premium_w3.is_connected():
            print(f"⚠️ Primary RPC {self.active_url[:30]} failed instantly. Rotating...")
            self.handle_rate_limit(self.active_url)
            
        self.on_fallback = False
        self.strike_count = 0

        print(f"🟢 Smart Sync RPC Manager: Starting with {self.active_url[:50]}...")

    def get_optimal_w3(self, is_critical=False) -> Web3:
        """Returns the current Web3 instance."""
        return self.premium_w3

    def handle_rate_limit(self, url_failed: str):
        """Immediately rotate to the next node and sleep briefly."""
        self.current_index = (self.current_index + 1) % len(self.rpc_urls)
        self.active_url = self.rpc_urls[self.current_index]
        self.premium_w3 = Web3(Web3.HTTPProvider(self.active_url, request_kwargs={'timeout': 60}))
        
        cooldown = random.uniform(1.0, 2.0)
        print(f"⏳ Rate limited or Quota exceeded. Rotating to {self.active_url[:50]}... (Sleep {cooldown:.1f}s)")
        time.sleep(cooldown)

    def handle_hard_error(self, error):
        """Hard connection error: Rotate to next node."""
        print(f"💥 Hard RPC error: {error}. Rotating...")
        self.handle_rate_limit(self.active_url)

    def is_rate_limit_error(self, error_str):
        return any(k in error_str for k in ["429", "403", "too many requests", "forbidden", "timeout", "quota", "-32001"])

    def is_hard_error(self, error_str):
        return any(k in error_str for k in self.HARD_ERROR_KEYWORDS)

    def call(self, func, is_critical=False, *args, **kwargs):
        """Wrapper for Web3 calls with Smart Routing & Rate Limiting."""
        try:
            w3_instance = self.get_optimal_w3(is_critical)
            func_name = func.__name__
            if hasattr(w3_instance.eth, func_name):
                optimal_func = getattr(w3_instance.eth, func_name)
            else:
                optimal_func = func
            return optimal_func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()

            if self.is_rate_limit_error(error_str):
                self.handle_rate_limit(self.active_url)
                return self.call(func, is_critical, *args, **kwargs)
            elif self.is_hard_error(error_str):
                self.handle_hard_error(e)
                return self.call(func, is_critical, *args, **kwargs)
            else:
                raise e


rpc_manager = SmartSyncRPCManager()
# w3 globally needed for utility formatting
w3 = rpc_manager.premium_w3

# --- CONFIGURATION (LODESTAR SPECIFIC) ---
COMPTROLLER_ADDRESS = Web3.to_checksum_address("0x264906F21b6DDFc07f43372fC24422B9c0587a8b")

MULTICALL3_ADDRESS = Web3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
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

COMPTROLLER_ABI = [{
    "constant": True, "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
    "name": "getAccountLiquidity",
    "outputs": [{"internalType": "uint256", "name": "error", "type": "uint256"}, {"internalType": "uint256", "name": "liquidity", "type": "uint256"}, {"internalType": "uint256", "name": "shortfall", "type": "uint256"}],
    "payable": False, "stateMutability": "view", "type": "function"
}]

UNDERLYING_ASSETS = {
    "USDC":   "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "USDC_e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
    "WETH":   "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    "WBTC":   "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    "USDT":   "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    "ARB":    "0x912CE59144191C1204E64559FE8253a0e49E6548",
    "LINK":   "0xf97f4df75117a78c1A5a0DBb814Af92455853904",
}

# Borrow event signature in Compound V2
BORROW_TOPIC = "0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80"

TOTAL_BLOCKS_TO_SCAN = 10000 # Polling Config
CHUNK_SIZE = 200
SCAN_INTERVAL = 43200
MULTICALL_BATCH_SIZE = 150
TIER_1_MAX_HF = 1.050
TIER_2_MAX_HF = 1.200
LAST_ERRORS = {}
ALERT_COOLDOWN = 300
RPC_WAS_DOWN = False

def send_telegram_alert(msg, is_error=False):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return
    if is_error:
        error_key = msg[:100]
        now = time.time()
        if error_key in LAST_ERRORS and (now - LAST_ERRORS[error_key]) < ALERT_COOLDOWN:
            return
        LAST_ERRORS[error_key] = now
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception:
        pass

def build_token_map():
    # Lodestar Finance Variable Debt Tokens
    return {
        "USDC": "0x58c0c45152F942468352eE6236b2E57049D8E6e4",
        "USDC_e": "0x58c0c45152F942468352eE6236b2E57049D8E6e4", # Lodestar primarily uses USDC.e
        "WETH": "0x00E6A2DF2b947f63ebC4eb8e3f43dCA2bECAcdC0",
        "WBTC": "0x2F113C4713c721f4EDfE118544dEFE937A5f91A3",
        "USDT": "0x773dF153F57858c4F8171120a16B1982c7d95955",
        "ARB": "0x0e5d0831D4B85C4F519b7a42F807f3001859D3D0",
        "MAGIC": "0x16b6Fffc23BE504cbF70E75DFB2aD0cFe6D86BB3"
    }

def get_target_path():
    if os.path.exists("/root/Arbitrum"):
        return "/root/Arbitrum/lodestar_targets.json"
    return "lodestar_targets.json"

def save_targets_atomic(targets_data):
    target_path = get_target_path()
    temp_path = target_path + ".tmp"
    with open(temp_path, "w") as f:
        json.dump(targets_data, f, indent=2)
    os.replace(temp_path, target_path)

def classify_targets_multicall(all_users_list):
    w3 = rpc_manager.premium_w3
    
    # Instantiate Comptroller
    comptroller = w3.eth.contract(address=COMPTROLLER_ADDRESS, abi=COMPTROLLER_ABI)
    multicall_contract = w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)
    
    tier_1 = []
    tier_2 = []
    discarded = 0

    for batch_start in range(0, len(all_users_list), MULTICALL_BATCH_SIZE):
        batch = all_users_list[batch_start:batch_start + MULTICALL_BATCH_SIZE]
        calls = []
        for user in batch:
            try:
                call_data = comptroller.functions.getAccountLiquidity(Web3.to_checksum_address(user))._encode_transaction_data()
                calls.append((COMPTROLLER_ADDRESS, call_data))
            except Exception:
                continue
            
        if not calls:
            continue
        
        try:
            return_data = rpc_manager.call(
                multicall_contract.functions.tryAggregate(False, calls).call, False, {'to': MULTICALL3_ADDRESS}
            )
        except Exception as e:
            print(f"  ⚠️ Multicall batch failed: {e}")
            continue
            
        for i, (success, raw_bytes) in enumerate(return_data):
            if not success or not raw_bytes:
                discarded += 1
                continue
            
            user = batch[i]
            try:
                # Returns (error, liquidity, shortfall)
                decoded = decode(['uint256', 'uint256', 'uint256'], raw_bytes)
                error_code = decoded[0]
                liquidity = decoded[1]
                shortfall = decoded[2]
                
                if error_code != 0:
                    discarded += 1
                    continue
                
                # Compound V2 Logic
                if shortfall > 0:
                    tier_1.append(user)
                elif shortfall == 0 and liquidity < 500 * (10**18):
                    tier_2.append(user)
                else:
                    discarded += 1
            except Exception:
                discarded += 1
                continue
            
        time.sleep(0.5)
        print(f"  📊 Classified {min(batch_start + MULTICALL_BATCH_SIZE, len(all_users_list))}/{len(all_users_list)} | T1: {len(tier_1)} | T2: {len(tier_2)}", end="\r")
    return {"tier_1_danger": tier_1, "tier_2_watchlist": tier_2}

async def fetch_logs_for_chunk(session, address, start_block, end_block, semaphore, all_users, rpc_manager):
    """Fetch logs for a specific block chunk using raw JSON-RPC to avoid Web3.py overhead."""
    async with semaphore:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "address": address,
                "fromBlock": hex(start_block),
                "toBlock": hex(end_block),
                "topics": [BORROW_TOPIC]
            }],
            "id": 1
        }
        
        for _ in range(3):  # 3 Retries
            url = rpc_manager.active_url
            try:
                async with session.post(url, json=payload, timeout=10) as response:
                    if response.status in [200, 201]:
                        data = await response.json()
                        if 'error' in data:
                            err_str = str(data['error']).lower()
                            if rpc_manager.is_rate_limit_error(err_str):
                                rpc_manager.handle_rate_limit(url)
                                continue
                            break
                        
                        logs = data.get('result', [])
                        for log in logs:
                            # In Compound V2 Borrow(address borrower, uint256 borrowAmount, uint256 accountBorrows, uint256 totalBorrows)
                            # The borrower is usually the first 32 bytes of the non-indexed data
                            log_data = log.get('data', '')
                            if log_data and len(log_data) >= 66:
                                # Extract borrower: 0x + first 32 bytes (64 hex characters)
                                # The address is the last 40 characters inside those 64 characters
                                borrower_hex = "0x" + log_data[2:66][-40:]
                                user = Web3.to_checksum_address(borrower_hex)
                                if user != "0x0000000000000000000000000000000000000000":
                                    all_users.add(user)
                        return # Success
                    elif response.status in [429, 403, 413]:
                        rpc_manager.handle_rate_limit(url)
                        continue
                    else:
                        break
            except Exception as e:
                err_str = str(e).lower()
                if rpc_manager.is_rate_limit_error(err_str) or rpc_manager.is_hard_error(err_str):
                    rpc_manager.handle_rate_limit(url)
                await asyncio.sleep(1)

async def scan_debt_tokens():
    global RPC_WAS_DOWN
    # 1. Proactive Health Check (Auto-Recovery)
    # rpc_manager.check_primary_health()
    w3 = rpc_manager.premium_w3  # Get current active instance

    print("═══════════════════════════════════════════════════════════")
    
    # Pre-ping logic
    
    try:
        current_block = rpc_manager.call(w3.eth.get_block_number, is_critical=False)
        print(f"📍 Baseline Block: {current_block}")
    except Exception as e:
        error_msg = str(e)
        if not RPC_WAS_DOWN:
            send_telegram_alert(f"⚠️ Lodestar Scanner RPC Down: {error_msg}", is_error=True)
            RPC_WAS_DOWN = True
        return {"tier_1_danger": [], "tier_2_watchlist": []}

    if RPC_WAS_DOWN:
        send_telegram_alert(f"🟢 Lodestar Scanner RPC Restored (Block {current_block})")
        RPC_WAS_DOWN = False

    print("📡 Fetching Debt Token addresses...")
    token_map = build_token_map()
    if not token_map:
        return {"tier_1_danger": [], "tier_2_watchlist": []}

    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    all_users = set()
    print(f"Scanning from {start_block} to {current_block}...")

    semaphore = asyncio.Semaphore(5)
    
    async with aiohttp.ClientSession() as session:
        for name, address in token_map.items():
            print(f"\n🔍 Scanning {name} [{address}] concurrently...")
            
            chunks = []
            chunk_start = start_block
            while chunk_start < current_block:
                chunk_end = min(chunk_start + CHUNK_SIZE - 1, current_block)
                chunks.append((chunk_start, chunk_end))
                chunk_start = chunk_end + 1
                
            tasks = []
            for start, end in chunks:
                tasks.append(fetch_logs_for_chunk(session, address, start, end, semaphore, all_users, rpc_manager))
            
            await asyncio.gather(*tasks)

    if len(all_users) == 0:
         pass # Clean start for Lodestar

    all_users_list = list(all_users)
    print(f"✅ Found {len(all_users_list)} users.")
    return classify_targets_multicall(all_users_list)

async def main():
    send_telegram_alert("🟢 <b>Lodestar Scanner Started</b>")
    try:
        while True:
            try:
                print("\n🔍 Starting scan...")
                start = time.time()
                tiered_targets = await scan_debt_tokens()
                total = len(tiered_targets['tier_1_danger']) + len(tiered_targets['tier_2_watchlist'])
                elapsed = time.time() - start
                if total > 0:
                    save_targets_atomic(tiered_targets)
                    print(f"💾 Saved {total} targets ({elapsed:.0f}s)")
                    send_telegram_alert(f"📡 Lodestar Scan: T1: {len(tiered_targets['tier_1_danger'])} | T2: {len(tiered_targets['tier_2_watchlist'])}")
                
                print(f"💤 Sleeping for {SCAN_INTERVAL}s...")
                await asyncio.sleep(SCAN_INTERVAL)
            except Exception as e:
                print(f"Error in main loop: {e}")
                traceback.print_exc()
                await asyncio.sleep(60)
    except Exception as e:
        print(f"Fatal: {e}")

if __name__ == "__main__":
    asyncio.run(main())
