import os
import json
import time
import requests
from web3 import Web3
from dotenv import load_dotenv
from eth_abi import decode

# Load Env
load_dotenv()

# --- RPC MANAGER ---
class SyncRPCManager:
    def __init__(self):
        self.primary_rpc = os.getenv("PRIMARY_RPC")
        self.fallback_rpcs = os.getenv("FALLBACK_RPCS", "").split(",")
        self.fallback_rpcs = [url.strip() for url in self.fallback_rpcs if url.strip()]

        self.active_rpc_index = -1
        self.w3 = Web3(Web3.HTTPProvider(self.primary_rpc, request_kwargs={'timeout': 60}))

        self.rpc_delay = 0.1
        self.consecutive_errors = 0

        if not self.primary_rpc:
            print("❌ PRIMARY_RPC not found in .env")
            exit()

    def check_primary_health(self):
        if self.active_rpc_index == -1:
            return

        try:
            temp_w3 = Web3(Web3.HTTPProvider(self.primary_rpc, request_kwargs={'timeout': 10}))
            temp_w3.eth.block_number
            print("\n🟢 Primary RPC checked OK. Switching back!")
            self.w3 = temp_w3
            self.active_rpc_index = -1
            self.rpc_delay = 0.1
            self.consecutive_errors = 0
            send_telegram_alert("🟢 <b>Primary RPC Restored.</b> Switched back to main node.")
        except Exception:
            pass

    def handle_failure(self):
        print(f"⚠️ RPC Failure ({self.consecutive_errors} strikes). Switching...")
        next_idx = self.active_rpc_index + 1
        if next_idx < len(self.fallback_rpcs):
            new_url = self.fallback_rpcs[next_idx]
            self.active_rpc_index = next_idx
            self.w3 = Web3(Web3.HTTPProvider(new_url, request_kwargs={'timeout': 60}))
            self.rpc_delay = 0.5
            self.consecutive_errors = 0
            msg = f"⚠️ <b>Primary RPC Failed.</b> Switching to Fallback #{next_idx + 1}."
            print(f"🔄 Switched to Fallback: {new_url} (Delay: {self.rpc_delay}s)")
            send_telegram_alert(msg, is_error=True)
            return True
        else:
            print("❌ All Fallbacks exhausted. Sleeping 30s then Resetting to Primary.")
            time.sleep(30)
            self.active_rpc_index = -1
            self.w3 = Web3(Web3.HTTPProvider(self.primary_rpc, request_kwargs={'timeout': 60}))
            self.rpc_delay = 0.1
            self.consecutive_errors = 0
            return False

    def call(self, func, *args, **kwargs):
        delay = 0.5 if self.active_rpc_index >= 0 else self.rpc_delay
        time.sleep(delay)

        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "403" in error_str or "too many requests" in error_str or "forbidden" in error_str:
                self.consecutive_errors += 1
                print(f"⚠️ Rate Limit Hit (Strike {self.consecutive_errors}/3). CAUTION: Cooling down for 30s...")
                time.sleep(30)
                if self.active_rpc_index == -1:
                    self.rpc_delay += 0.1
                    print(f"🐌 Increased Primary Delay to {self.rpc_delay:.2f}s")
                if self.consecutive_errors >= 3:
                    if self.handle_failure():
                        return self.call(func, *args, **kwargs)
                    else:
                        raise e
                else:
                    return self.call(func, *args, **kwargs)
            else:
                self.consecutive_errors += 1
                if self.consecutive_errors >= 3:
                    if self.handle_failure():
                        return self.call(func, *args, **kwargs)
                    else:
                        raise e
                raise e


rpc_manager = SyncRPCManager()

# --- CONFIGURATION (RADIANT SPECIFIC) ---
POOL_ADDRESSES_PROVIDER = Web3.to_checksum_address("0x091d52Cce1d49c8CE620B250284d126422CE04f0")
POOL_ADDRESS = Web3.to_checksum_address("0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1")
DATA_PROVIDER_ADDRESS = Web3.to_checksum_address("0x596BBA96C892246dC955aAd9fA36B6900f684307") # Radiant Protocol Data Provider
MULTICALL3_ADDRESS = Web3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")

MULTICALL3_ABI = [{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call[]","name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"},{"internalType":"bytes[]","name":"returnData","type":"bytes[]"}],"stateMutability":"view","type":"function"}]

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

TOTAL_BLOCKS_TO_SCAN = 50000
CHUNK_SIZE = 2000
SCAN_INTERVAL = 600
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
    data_provider = rpc_manager.w3.eth.contract(address=DATA_PROVIDER_ADDRESS, abi=DATA_PROVIDER_ABI)
    token_map = {}
    for name, underlying in UNDERLYING_ASSETS.items():
        try:
            underlying_cs = Web3.to_checksum_address(underlying)
            result = rpc_manager.call(data_provider.functions.getReserveTokensAddresses(underlying_cs).call)
            var_debt_token = result[2]
            if var_debt_token == "0x0000000000000000000000000000000000000000":
                print(f"  ⚠️ {name}: Not active on Radiant, skipping.")
                continue
            token_map[f"{name}_Debt"] = var_debt_token
            print(f"  ✅ {name}_Debt -> {var_debt_token}")
        except Exception as e:
            print(f"  ❌ Failed to fetch {name} debt token: {e}")
    return token_map

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
    w3 = rpc_manager.w3
    pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
    multicall = w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)
    tier_1 = []
    tier_2 = []
    discarded = 0

    for batch_start in range(0, len(all_users_list), MULTICALL_BATCH_SIZE):
        batch = all_users_list[batch_start:batch_start + MULTICALL_BATCH_SIZE]
        calls = []
        for user in batch:
            try:
                call_data = pool.functions.getUserAccountData(Web3.to_checksum_address(user))._encode_transaction_data()
                calls.append((POOL_ADDRESS, call_data))
            except Exception:
                continue
        if not calls:
            continue
        try:
            _, return_data = rpc_manager.call(multicall.functions.aggregate(calls).call)
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
    rpc_manager.check_primary_health()
    w3 = rpc_manager.w3
    try:
        current_block = rpc_manager.call(lambda: w3.eth.block_number)
    except Exception as e:
        error_msg = str(e)
        rpc_manager.handle_failure()
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
            print(f"🔍 Scanning {name} [{address}]...")
            for chunk_start in range(start_block, current_block, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE - 1, current_block)
                try:
                    logs = rpc_manager.call(w3.eth.get_logs, {
                        'fromBlock': hex(int(chunk_start)),
                        'toBlock': hex(int(chunk_end)),
                        'address': Web3.to_checksum_address(address),
                        'topics': [TRANSFER_TOPIC]
                    })
                except Exception:
                    logs = []
                for log in logs:
                    if len(log['topics']) >= 3:
                        addr1 = Web3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                        addr2 = Web3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])
                        if addr1 != "0x0000000000000000000000000000000000000000": all_users.add(addr1)
                        if addr2 != "0x0000000000000000000000000000000000000000": all_users.add(addr2)
                time.sleep(1.0)
        except Exception as e:
            print(f"Error scanning {name}: {e}")
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
