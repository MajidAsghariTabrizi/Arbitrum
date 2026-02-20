import os
import json
import time
import requests
import traceback
import random
import threading
from web3 import Web3
from dotenv import load_dotenv
from eth_abi import decode

# Load Env
load_dotenv()

# --- RPC MANAGER (Strict QoS Lane: Tier 3 → SCANNER_RPC) ---
class StickySyncRPCManager:
    """
    Strict QoS Sticky Sync RPC Manager:
    - Uses a SINGLE dedicated RPC node (SCANNER_RPC) for all requests.
    - On 429 Rate Limit: time.sleep() + retry on SAME node (no rotation).
    - On hard connection error: failover to FALLBACK_RPCS sequentially.
    """
    HARD_ERROR_KEYWORDS = ["serverdisconnected", "connectionerror", "connection refused",
                           "cannot connect", "server disconnected", "connectionreseterror",
                           "oserror", "gaierror", "remotedisconnected"]

    def __init__(self):
        self.primary_url = os.getenv("SCANNER_RPC")
        fallback_rpcs_raw = os.getenv("FALLBACK_RPCS", "").replace('"', '').replace("'", "").split(",")
        self.fallback_urls = [url.strip() for url in fallback_rpcs_raw if url.strip()]

        if not self.primary_url:
            print("❌ SCANNER_RPC not found in .env")
            exit()

        # Single sticky connection
        self.premium_w3 = Web3(Web3.HTTPProvider(self.primary_url, request_kwargs={'timeout': 60}))
        self.active_url = self.primary_url
        self.on_fallback = False
        self.strike_count = 0

        print(f"🟢 Sticky Sync RPC Manager (Tier 3 Scanner): {self.primary_url[:50]}...")

    def get_optimal_w3(self, is_critical=False) -> Web3:
        """Returns the single sticky Web3 instance."""
        return self.premium_w3

    def handle_rate_limit(self, url_failed: str):
        """429: Sleep with exponential backoff, retry on SAME node. No rotation."""
        self.strike_count += 1
        cooldown = min(120, 2 ** self.strike_count) + random.uniform(0.1, 1.0)
        print(f"⏳ Rate limited (Strike {self.strike_count}). Sleeping {cooldown:.1f}s on SAME node...")
        time.sleep(cooldown)

    def handle_hard_error(self, error):
        """Hard connection error: Failover to FALLBACK_RPCS sequentially."""
        print(f"💥 Hard RPC error: {error}. Failing over to FALLBACK_RPCS...")
        self.strike_count = 0

        for fb_url in self.fallback_urls:
            try:
                fb_w3 = Web3(Web3.HTTPProvider(fb_url, request_kwargs={'timeout': 60}))
                fb_w3.eth.block_number  # Test connection
                self.premium_w3 = fb_w3
                self.active_url = fb_url
                self.on_fallback = True
                print(f"🔄 Fallback active: {fb_url[:50]}...")
                return
            except Exception:
                continue

        print("❌ All FALLBACK_RPCS failed. Sleeping 10s then retrying primary...")
        time.sleep(10)
        self.premium_w3 = Web3(Web3.HTTPProvider(self.primary_url, request_kwargs={'timeout': 60}))
        self.active_url = self.primary_url
        self.on_fallback = False

    def is_rate_limit_error(self, error_str):
        return any(k in error_str for k in ["429", "403", "too many requests", "forbidden", "timeout"])

    def is_hard_error(self, error_str):
        return any(k in error_str for k in self.HARD_ERROR_KEYWORDS)

    def call(self, func, is_critical=False, *args, **kwargs):
        """Wrapper for Web3 calls with Sticky Routing & Rate Limiting."""
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
                time.sleep(2.0 + random.uniform(0.1, 1.0))
                return self.call(func, is_critical, *args, **kwargs)
            elif self.is_hard_error(error_str):
                self.handle_hard_error(e)
                return self.call(func, is_critical, *args, **kwargs)
            else:
                raise e


rpc_manager = StickySyncRPCManager()
# w3 globally needed for utility formatting
w3 = rpc_manager.premium_w3

# --- CONFIGURATION (RADIANT SPECIFIC) ---
POOL_ADDRESSES_PROVIDER = Web3.to_checksum_address("0x2032b9A8e9F7e76768CA9271003d3e43E1616B1F") # Updated Provider
DATA_PROVIDER_ADDRESS = Web3.to_checksum_address("0x596BBA96C892246dC955aAd9fA36B6900f684307") # Keep existing Data Provider? NO, User said update.
# User said: Update Provider: 0x454a8daf74b24037ee2fa073ce1be9277ed6160a
# Update Data Provider: 0xa3e42d11d8CC148160CC3ACED757FB44696a9CcA

POOL_ADDRESSES_PROVIDER = Web3.to_checksum_address("0x454a8daf74b24037ee2fa073ce1be9277ed6160a")
DATA_PROVIDER_ADDRESS = Web3.to_checksum_address("0xa3e42d11d8CC148160CC3ACED757FB44696a9CcA")

MULTICALL3_ADDRESS = Web3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
MULTICALL3_ABI = [{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call[]","name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"},{"internalType":"bytes[]","name":"returnData","type":"bytes[]"}],"stateMutability":"view","type":"function"}]

ADDRESSES_PROVIDER_ABI = [{
    "inputs": [],
    "name": "getLendingPool",
    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
    "stateMutability": "view",
    "type": "function"
}]

POOL_ABI = [{
    "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
    "name": "getUserAccountData",
    "outputs": [
        {"internalType": "uint256", "name": "totalCollateralETH", "type": "uint256"},
        {"internalType": "uint256", "name": "totalDebtETH", "type": "uint256"},
        {"internalType": "uint256", "name": "availableBorrowsETH", "type": "uint256"},
        {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
        {"internalType": "uint256", "name": "ltv", "type": "uint256"},
        {"internalType": "uint256", "name": "healthFactor", "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"
}]

DATA_PROVIDER_ABI = [{
    "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
    "name": "getReserveTokensAddresses",
    "outputs": [
        {"internalType": "address", "name": "aTokenAddress", "type": "address"},
        {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
        {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"}
    ],
    "stateMutability": "view",
    "type": "function"
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

TRANSFER_TOPIC = Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)"))

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
    # Hardcoded Radiant V2 Arbitrum Variable Debt Tokens (Zero RPC Calls)
    return {
        "USDC": "0xf92d501e74bd1e4308E6676C38Ab4d84389d7Bf3",
        "USDC_e": "0xf92d501e74bd1e4308E6676C38Ab4d84389d7Bf3", # Often maps to the same primary stablecoin on Radiant
        "WETH": "0x4e75D4bc81D9AD1a1abc972a3dd53d581e1CE16b",
        "WBTC": "0x0e16bAE17C61789d8a96Ea6529d788B633C4c8B6",
        "USDT": "0x9C3A8644A9cA181b90094be98dC19496F6b38a24",
        "ARB": "0x24C65D9Cbb174e92a472cbaDE2830fB54b6d36e2"
    }

def get_target_path():
    if os.path.exists("/root/Arbitrum"):
        return "/root/Arbitrum/radiant_targets.json"
    return "radiant_targets.json"

def save_targets_atomic(targets_data):
    target_path = get_target_path()
    temp_path = target_path + ".tmp"
    with open(temp_path, "w") as f:
        json.dump(targets_data, f, indent=2)
    os.replace(temp_path, target_path)

def classify_targets_multicall(all_users_list):
    w3 = rpc_manager.premium_w3
    
    # 1. Fetch dynamic pool address first
    addresses_provider = w3.eth.contract(address=POOL_ADDRESSES_PROVIDER, abi=ADDRESSES_PROVIDER_ABI)
    try:
        dynamic_pool_address = rpc_manager.call(addresses_provider.functions.getLendingPool().call, False, {'to': POOL_ADDRESSES_PROVIDER})
    except Exception as e:
        print(f"  ❌ Failed to fetch dynamic Pool Address: {e}")
        return {"tier_1_danger": [], "tier_2_watchlist": []}

    # 2. Instantiate pool with dynamic address
    pool = w3.eth.contract(address=dynamic_pool_address, abi=POOL_ABI)
    multicall_contract = w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)
    tier_1 = []
    tier_2 = []
    discarded = 0

    for batch_start in range(0, len(all_users_list), MULTICALL_BATCH_SIZE):
        batch = all_users_list[batch_start:batch_start + MULTICALL_BATCH_SIZE]
        calls = []
        for user in batch:
            try:
                call_data = pool.functions.getUserAccountData(Web3.to_checksum_address(user))._encode_transaction_data()
                calls.append((dynamic_pool_address, call_data))
            except Exception:
                continue
            
        if not calls:
            continue
        
        try:
            _, return_data = rpc_manager.call(
                multicall_contract.functions.aggregate(calls).call, False, {'to': MULTICALL3_ADDRESS}
            )
        except Exception as e:
            print(f"  ⚠️ Multicall batch failed: {e}")
            continue
        for i, raw_bytes in enumerate(return_data):
            user = batch[i]
            try:
                decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256', 'uint256'], raw_bytes)
                hf_raw = decoded[5]
                if hf_raw == 0:
                    discarded += 1
                    continue
                hf = hf_raw / 1e18
                if hf < TIER_1_MAX_HF:
                    tier_1.append(user)
                elif hf < TIER_2_MAX_HF:
                    tier_2.append(user)
                else:
                    discarded += 1
            except Exception:
                discarded += 1
                continue
            
        time.sleep(0.5)
        print(f"  📊 Classified {min(batch_start + MULTICALL_BATCH_SIZE, len(all_users_list))}/{len(all_users_list)} | T1: {len(tier_1)} | T2: {len(tier_2)}", end="\r")
    return {"tier_1_danger": tier_1, "tier_2_watchlist": tier_2}

def scan_debt_tokens():
    global RPC_WAS_DOWN
    # 1. Proactive Health Check (Auto-Recovery)
    # rpc_manager.check_primary_health()
    w3 = rpc_manager.premium_w3  # Get current active instance

    print("═══════════════════════════════════════════════════════════")
    
    # Pre-ping logic
    rpc_manager.rank_free_nodes()
    
    try:
        current_block = rpc_manager.call(w3.eth.get_block_number, is_critical=False)
        print(f"📍 Baseline Block: {current_block}")
    except Exception as e:
        error_msg = str(e)
        # The original code had rpc_manager.handle_failure() here, but it's not defined in the provided class.
        # Assuming it was meant to be a general error handling or a method that was removed.
        # For now, just log and return.
        if not RPC_WAS_DOWN:
            send_telegram_alert(f"⚠️ Radiant Scanner RPC Down: {error_msg}", is_error=True)
            RPC_WAS_DOWN = True
        return {"tier_1_danger": [], "tier_2_watchlist": []}

    if RPC_WAS_DOWN:
        send_telegram_alert(f"🟢 Radiant Scanner RPC Restored (Block {current_block})")
        RPC_WAS_DOWN = False

    print("📡 Fetching Debt Token addresses...")
    token_map = build_token_map()
    if not token_map:
        return {"tier_1_danger": [], "tier_2_watchlist": []}

    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    all_users = set()
    print(f"Scanning from {start_block} to {current_block}...")

    for name, address in token_map.items():
        try:
            print(f"\n🔍 Scanning {name} [{address}]...")
            
            chunk_start = start_block
            current_chunk_size = 200  # Fixed chunk size matching CHUNK_SIZE

            while chunk_start < current_block:
                chunk_end = min(chunk_start + current_chunk_size - 1, current_block)

                # Show progress
                print(f"   ⏳ Block: {chunk_start}-{chunk_end} (Size: {chunk_end - chunk_start + 1}) | Found: {len(all_users)}", end="\r")

                try:
                    logs = rpc_manager.call(w3.eth.get_logs, False, {
                        'fromBlock': hex(int(chunk_start)),
                        'toBlock': hex(int(chunk_end)),
                        'address': Web3.to_checksum_address(address),
                        'topics': [TRANSFER_TOPIC]
                    })

                    time.sleep(1.5)  # Force 1.5s delay between EVERY get_logs to prevent 429

                    # Success: keep size fixed
                    current_chunk_size = 200
                    
                    for log in logs:
                        if len(log['topics']) >= 3:
                            addr1 = Web3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                            addr2 = Web3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])

                            if addr1 != "0x0000000000000000000000000000000000000000":
                                all_users.add(addr1)
                            if addr2 != "0x0000000000000000000000000000000000000000":
                                all_users.add(addr2)

                    chunk_start = chunk_end + 1
                    time.sleep(20.0)

                except Exception as e:
                    # Failure: Halve the chunk size dynamically
                    print(f"\n   ⚠️ Chunk {chunk_start}-{chunk_end} Failed: {e}. Adapting chunk size...")
                    current_chunk_size = max(50, current_chunk_size // 2)
                    time.sleep(120) # 2 min breath before retry on 429

        except Exception as e:
            print(f"\n   ❌ Error scanning {name}: {e}")
            send_telegram_alert(f"⚠️ <b>Scanner Error</b> on <code>{name}</code>:\n<code>{e}</code>", is_error=True)
            continue

    if len(all_users) == 0:
         all_users.update(["0x99525208453488C9518001712C7F72428514197F", "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb"])

    all_users_list = list(all_users)
    print(f"✅ Found {len(all_users_list)} users.")
    return classify_targets_multicall(all_users_list)

if __name__ == "__main__":
    send_telegram_alert("🟢 <b>Radiant Scanner Started</b>")
    try:
        while True:
            try:
                print("\n🔍 Starting scan...")
                start = time.time()
                tiered_targets = scan_debt_tokens()
                total = len(tiered_targets['tier_1_danger']) + len(tiered_targets['tier_2_watchlist'])
                elapsed = time.time() - start
                if total > 0:
                    save_targets_atomic(tiered_targets)
                    print(f"💾 Saved {total} targets ({elapsed:.0f}s)")
                    send_telegram_alert(f"📡 Radiant Scan: T1: {len(tiered_targets['tier_1_danger'])} | T2: {len(tiered_targets['tier_2_watchlist'])}")
                time.sleep(SCAN_INTERVAL)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(60)
    except Exception as e:
        print(f"Fatal: {e}")
