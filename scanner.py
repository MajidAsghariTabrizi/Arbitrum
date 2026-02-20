import os
import json
import traceback
import threading
import requests
from web3 import Web3
from dotenv import load_dotenv
from eth_abi import decode

# Load Env
load_dotenv()

# --- RPC MANAGER ---
class SmartSyncRPCManager:
    """
    Tiered Sync RPC Router:
    - Tier 1 (Premium): PRIMARY_RPC (QuickNode, etc.)
    - Tier 2 (Free): FALLBACK_RPCS
    """
    def __init__(self):
        self.premium_url = os.getenv("PRIMARY_RPC")
        fallback_rpcs_raw = os.getenv("FALLBACK_RPCS", "").split(",")
        self.free_urls = [url.strip() for url in fallback_rpcs_raw if url.strip()]

        if not self.premium_url:
            print("❌ PRIMARY_RPC not found in .env")
            exit()

        # Connection Pools
        self.premium_w3 = Web3(Web3.HTTPProvider(self.premium_url, request_kwargs={'timeout': 60}))
        self.free_w3s = {url: Web3(Web3.HTTPProvider(url, request_kwargs={'timeout': 60})) for url in self.free_urls}
        
        # Tier 2 Rankings: [{"url": str, "latency": float, "is_blacklisted": bool, "blacklist_until": float}]
        self.free_nodes_rank = [{"url": url, "latency": 999.0, "is_blacklisted": False, "blacklist_until": 0} for url in self.free_urls]
        self.strike_counts = {url: 0 for url in self.free_urls}
        self.strike_counts["premium"] = 0

        # Start background ranker thread
        self.ranker_thread = threading.Thread(target=self._rank_nodes_loop, daemon=True)
        self.ranker_thread.start()
        print(f"🟢 Smart Sync RPC Manager Initialized ({len(self.free_urls)} Free Nodes).")

    def _rank_nodes_loop(self):
        """Background thread that pings Tier 2 nodes every 60s."""
        while True:
            self.rank_free_nodes()
            time.sleep(60)

    def rank_free_nodes(self):
        """Pings all free nodes and updates their latency."""
        now = time.time()
        for node in self.free_nodes_rank:
            # Un-blacklist if time has passed
            if node["is_blacklisted"] and now > node["blacklist_until"]:
                node["is_blacklisted"] = False
                node["latency"] = 999.0 # Reset
                self.strike_counts[node["url"]] = 0
                print(f"🟢 Node un-blacklisted: {node['url'][:40]}...")

            if node["is_blacklisted"]:
                continue

            # Ping test with strict 3s timeout
            url = node["url"]
            start_time = time.time()
            try:
                # We create a temporary w3 with a short timeout just for pinging
                temp_w3 = Web3(Web3.HTTPProvider(url, request_kwargs={'timeout': 3}))
                temp_w3.eth.block_number
                node["latency"] = time.time() - start_time
            except Exception:
                node["latency"] = 999.0
        
        # Sort by latency
        self.free_nodes_rank.sort(key=lambda x: (x["is_blacklisted"], x["latency"]))

    def get_optimal_w3(self, is_critical=False) -> Web3:
        """
        Routes the request.
        is_critical=True -> Premium Node
        is_critical=False -> Best available Free Node
        """
        if is_critical:
            return self.premium_w3

        # Find best free node
        for node in self.free_nodes_rank:
            if not node["is_blacklisted"]:
                return self.free_w3s[node["url"]]
                
        # Failsafe
        if not is_critical:
            raise Exception("NO_FREE_NODES")
            
        print("⚠️ All Free Nodes blacklisted! Falling back to Premium Node temporarily.")
        return self.premium_w3

    def handle_rate_limit(self, url_failed: str):
        """Handles backoffs and blacklists per node."""
        if url_failed == self.premium_url:
            self.strike_counts["premium"] = self.strike_counts.get("premium", 0) + 1
            cooldown = min(120, 2 ** self.strike_counts["premium"])
            print(f"💎 PREMIUM Rate limited (Strike {self.strike_counts['premium']}). Cooling down {cooldown}s...")
            time.sleep(cooldown)
        else:
            for node in self.free_nodes_rank:
                if node["url"] == url_failed:
                    self.strike_counts[url_failed] += 1
                    if self.strike_counts[url_failed] >= 3:
                        node["is_blacklisted"] = True
                        node["blacklist_until"] = time.time() + 300 # 5 minutes
                        print(f"🚫 Free Node Blacklisted (5m): {url_failed[:40]}...")
                    else:
                        print(f"🐌 Free Node Strike {self.strike_counts[url_failed]}/3: {url_failed[:40]}...")
                    break
            
            # Re-sort to push blacklisted down
            self.free_nodes_rank.sort(key=lambda x: (x["is_blacklisted"], x["latency"]))

    def call(self, func, is_critical=False, *args, **kwargs):
        """Wrapper for Web3 calls with Smart Routing & Rate Limiting."""
        try:
            w3_instance = self.get_optimal_w3(is_critical)
            
            # We need to extract the actual function from the correctly routed w3 instance
            # Extract func name, e.g., 'get_logs' from bounds method
            func_name = func.__name__
            
            # It's safer to re-bind the function to the optimal w3 instance
            if hasattr(w3_instance.eth, func_name):
                optimal_func = getattr(w3_instance.eth, func_name)
            else:
                 optimal_func = func # Fallback if not an eth method

            return optimal_func(*args, **kwargs)

        except Exception as e:
            error_str = str(e).lower()
            
            if "no_free_nodes" in error_str:
                print("⏳ No Free Nodes available. Sleeping for 10s before retry...")
                time.sleep(10)
                return self.call(func, is_critical, *args, **kwargs)
                
            w3_instance = self.get_optimal_w3(is_critical)
            url_failed = w3_instance.provider.endpoint_uri

            if "429" in error_str or "403" in error_str or "too many requests" in error_str or "forbidden" in error_str or "timeout" in error_str:
                self.handle_rate_limit(url_failed)
                # Retry recursively with the next best node
                return self.call(func, is_critical, *args, **kwargs)
            else:
                self.strike_counts[url_failed] += 1
                if self.strike_counts[url_failed] >= 3:
                     self.handle_rate_limit(url_failed)
                     return self.call(func, is_critical, *args, **kwargs)
                raise e


rpc_manager = SmartSyncRPCManager()
# w3 globally needed for utility formatting in scanner
w3 = rpc_manager.premium_w3

# --- CONFIGURATION ---

# Aave V3 Arbitrum Addresses (Checksummed)
POOL_ADDRESS = Web3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
DATA_PROVIDER_ADDRESS = Web3.to_checksum_address("0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654")

# Multicall3 — Arbitrum One
MULTICALL3_ADDRESS = Web3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
MULTICALL3_ABI = [{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call[]","name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"},{"internalType":"bytes[]","name":"returnData","type":"bytes[]"}],"stateMutability":"view","type":"function"}]

# Pool ABI — getUserAccountData for HF classification
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
}]

# ABI: getReserveTokensAddresses(address asset) -> (aToken, stableDebtToken, variableDebtToken)
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

# Blue-chip underlying assets on Arbitrum One
UNDERLYING_ASSETS = {
    "USDC":   "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    "USDC_e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
    "WETH":   "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    "WBTC":   "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    "USDT":   "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    "ARB":    "0x912CE59144191C1204E64559FE8253a0e49E6548",
    "LINK":   "0xf97f4df75117a78c1A5a0DBb814Af92455853904",
}

# Transfer Event: Transfer(from, to, value)
# CRITICAL FIX: Use Web3.to_hex to ensure 0x prefix for strict RPC nodes
TRANSFER_TOPIC = Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)"))

# SETTINGS
TOTAL_BLOCKS_TO_SCAN = 50000   # Check last ~4 hours
CHUNK_SIZE = 2000              # 2000 blocks per chunk
SCAN_INTERVAL = 600            # 10 minutes between scans
MULTICALL_BATCH_SIZE = 150     # Max addresses per Multicall3 batch

# Tier Thresholds
TIER_1_MAX_HF = 1.050   # Danger: 1.000 – 1.050
TIER_2_MAX_HF = 1.200   # Watchlist: 1.051 – 1.200
                         # Discard: > 1.200

# Anti-spam cooldown for error alerts
LAST_ERRORS = {}
ALERT_COOLDOWN = 300  # 5 minutes

RPC_WAS_DOWN = False  # Global State


def send_telegram_alert(msg, is_error=False):
    """Sends an HTML-formatted Telegram alert with anti-spam cooldown for errors."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return

    # Anti-spam: skip duplicate error alerts within cooldown period
    if is_error:
        error_key = msg[:100]  # Use truncated message as key
        now = time.time()
        if error_key in LAST_ERRORS and (now - LAST_ERRORS[error_key]) < ALERT_COOLDOWN:
            return  # Suppress duplicate
        LAST_ERRORS[error_key] = now

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, json={
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception:
        pass


def build_token_map():
    """Dynamically fetches Variable Debt Token addresses from Aave V3 PoolDataProvider."""
    data_provider = rpc_manager.w3.eth.contract(
        address=DATA_PROVIDER_ADDRESS,
        abi=DATA_PROVIDER_ABI
    )
    token_map = {}
    for name, underlying in UNDERLYING_ASSETS.items():
        try:
            underlying_cs = Web3.to_checksum_address(underlying)
            result = rpc_manager.call(data_provider.functions.getReserveTokensAddresses(underlying_cs).call)

            var_debt_token = result[2]
            # Skip if returned zero address (asset not active on Aave)
            if var_debt_token == "0x0000000000000000000000000000000000000000":
                print(f"  ⚠️ {name}: Not active on Aave, skipping.")
                continue
            token_map[f"{name}_Debt"] = var_debt_token
            print(f"  ✅ {name}_Debt -> {var_debt_token}")
        except Exception as e:
            print(f"  ❌ Failed to fetch {name} debt token: {e}")
    return token_map


def get_target_path():
    """Returns the correct targets.json path (server vs local)."""
    if os.path.exists("/root/Arbitrum"):
        return "/root/Arbitrum/targets.json"
    return "targets.json"


def save_targets_atomic(targets_data):
    """Atomically writes targets to JSON using temp file + os.replace().
    Prevents JSON decode errors if the bot reads during a write.
    
    Args:
        targets_data: dict with 'tier_1_danger' and 'tier_2_watchlist' keys,
                      or a flat list (backward compat during progressive scan)
    """
    target_path = get_target_path()
    temp_path = target_path + ".tmp"
    with open(temp_path, "w") as f:
        json.dump(targets_data, f, indent=2)
    os.replace(temp_path, target_path)


def classify_targets_multicall(all_users_list):
    """
    Batch-classify all discovered users into Tier 1 / Tier 2 using Multicall3.
    
    Uses getUserAccountData to read Health Factor for every user in one batched call.
    - Tier_1_Danger:    HF between 1.000 and 1.050
    - Tier_2_Watchlist: HF between 1.051 and 1.200
    - Discarded:        HF > 1.200 or HF == 0 (no debt)
    
    Returns:
        dict: {"tier_1_danger": [...], "tier_2_watchlist": [...]}
    """
    w3 = rpc_manager.w3
    pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
    multicall_contract = w3.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)

    tier_1 = []
    tier_2 = []
    discarded = 0

    # Process in batches to avoid gas limit on Multicall3
    for batch_start in range(0, len(all_users_list), MULTICALL_BATCH_SIZE):
        batch = all_users_list[batch_start:batch_start + MULTICALL_BATCH_SIZE]

        # Build calldata for this batch
        calls = []
        for user in batch:
            try:
                call_data = pool.functions.getUserAccountData(
                    Web3.to_checksum_address(user)
                )._encode_transaction_data()
                calls.append((POOL_ADDRESS, call_data))
            except Exception:
                continue

        if not calls:
            continue

        try:
            _, return_data = rpc_manager.call(
                multicall_contract.functions.aggregate(calls).call, is_critical=False
            )
        except Exception as e:
            print(f"  ⚠️ Multicall batch failed (offset {batch_start}): {e}")
            continue

        # Decode each result
        for i, raw_bytes in enumerate(return_data):
            user = batch[i]
            try:
                decoded = decode(
                    ['uint256', 'uint256', 'uint256', 'uint256', 'uint256', 'uint256'],
                    raw_bytes
                )
                # healthFactor is index 5, 18 decimals
                hf_raw = decoded[5]

                # HF == 0 means no debt — skip
                if hf_raw == 0:
                    discarded += 1
                    continue

                hf = hf_raw / 1e18

                if hf < TIER_1_MAX_HF:
                    # Tier 1: Danger (HF 1.000 – 1.050)
                    tier_1.append(user)
                elif hf < TIER_2_MAX_HF:
                    # Tier 2: Watchlist (HF 1.051 – 1.200)
                    tier_2.append(user)
                else:
                    # HF > 1.200 — discard to keep JSON small
                    discarded += 1

            except Exception:
                discarded += 1
                continue

        # Throttle between batches
        time.sleep(0.5)

        progress = min(batch_start + MULTICALL_BATCH_SIZE, len(all_users_list))
        print(f"  📊 Classified {progress}/{len(all_users_list)} | "
              f"T1: {len(tier_1)} | T2: {len(tier_2)} | Discarded: {discarded}")

    return {"tier_1_danger": tier_1, "tier_2_watchlist": tier_2}


def scan_debt_tokens():
    """Phase 1: Discover borrower addresses from Transfer events on debt tokens."""
    global RPC_WAS_DOWN

    # 1. Proactive Health Check (Auto-Recovery)
    # rpc_manager.check_primary_health() # This method doesn't exist in SmartSyncRPCManager
    w3 = rpc_manager.w3  # Get current active instance

    # Active RPC test: fetch block_number to get the EXACT error on failure
    print("═══════════════════════════════════════════════════════════")
    
    # Pre-ping logic
    rpc_manager.rank_free_nodes()
    
    try:
        current_block = rpc_manager.call(w3.eth.get_block_number, is_critical=False)
        print(f"📍 Baseline Block: {current_block}")
    except Exception as e:
        error_msg = str(e)
        print(f"💥 RPC Connection Failed: {error_msg}")

        # Trigger Failover
        # rpc_manager.handle_failure() # This method doesn't exist in SmartSyncRPCManager

        if not RPC_WAS_DOWN:
            send_telegram_alert(f"⚠️ <b>Scanner RPC Down:</b>\n<code>{error_msg}</code>", is_error=True)
            RPC_WAS_DOWN = True
        return {"tier_1_danger": [], "tier_2_watchlist": []}

    # RPC recovered — send recovery alert if it was previously down
    if RPC_WAS_DOWN:
        send_telegram_alert(f"🟢 <b>Scanner RPC Restored:</b> Connected (Block {current_block}).")
        RPC_WAS_DOWN = False

    # Dynamically build the debt token map from on-chain data
    print("📡 Fetching Variable Debt Token addresses from PoolDataProvider...")
    token_map = build_token_map()
    if not token_map:
        print("❌ Could not load any debt tokens. Aborting scan.")
        return {"tier_1_danger": [], "tier_2_watchlist": []}
    print(f"🎯 Loaded {len(token_map)} debt tokens.\n")

    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    all_users = set()

    asset_names = ", ".join(UNDERLYING_ASSETS.keys())
    print(f"📡 Connected! Scanning Debt Tokens ({asset_names})")
    print(f"⏱️  Range: {start_block} to {current_block} (~4 Hours history)")
    print(f"📦 Chunk Size: {CHUNK_SIZE} blocks | Total Chunks: ~{(current_block - start_block) // CHUNK_SIZE}")

    # Scan each token (Progressive Feeding: save after each CHUNK)
    for name, address in token_map.items():
        try:
            print(f"\n🔍 Scanning {name} [{address}]...")

            chunk_start = start_block
            current_chunk_size = 500  # Start moderately

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
                    
                    # Success: Slightly increase chunk size for speed and move forward
                    current_chunk_size = min(2000, current_chunk_size + 100)
                    
                    for log in logs:
                        if len(log['topics']) >= 3:
                            addr1 = Web3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                            addr2 = Web3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])

                            if addr1 != "0x0000000000000000000000000000000000000000":
                                all_users.add(addr1)
                            if addr2 != "0x0000000000000000000000000000000000000000":
                                all_users.add(addr2)

                    chunk_start = chunk_end + 1
                    time.sleep(0.5)

                except Exception as e:
                    # Failure: Halve the chunk size dynamically
                    print(f"\n   ⚠️ Chunk {chunk_start}-{chunk_end} Failed: {e}. Adapting chunk size...")
                    current_chunk_size = max(50, current_chunk_size // 2)
                    time.sleep(2) # Breath before retry

        except Exception as e:
            print(f"\n   ❌ Error scanning {name}: {e}")
            send_telegram_alert(f"⚠️ <b>Scanner Error</b> on <code>{name}</code>:\n<code>{e}</code>", is_error=True)
            continue

    # --- FALLBACK MECHANISM ---
    # If network is super quiet, add some known active whales so bot is not empty
    if len(all_users) == 0:
        print("\n⚠️ Network quiet. Adding fallback targets (Active Whales) to ensure bot runs.")
        fallback_targets = [
            "0x99525208453488C9518001712C7F72428514197F",
            "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb",
            "0xF977814e90dA44bFA03b6295A0616a897441aceC"
        ]
        all_users.update(fallback_targets)

    all_users_list = list(all_users)
    print(f"\n\n✅ Phase 1 Complete. {len(all_users_list)} unique borrowers discovered.")

    # ================================================================
    # PHASE 2: Classify all discovered users via Multicall3 batch HF check
    # ================================================================
    print(f"\n🔬 Phase 2: Classifying {len(all_users_list)} users into Tiers via Multicall3...")
    tiered_result = classify_targets_multicall(all_users_list)

    print(f"\n📊 Classification Complete:")
    print(f"  🔴 Tier 1 (Danger):    {len(tiered_result['tier_1_danger'])} targets")
    print(f"  🟠 Tier 2 (Watchlist): {len(tiered_result['tier_2_watchlist'])} targets")

    return tiered_result


if __name__ == "__main__":
    send_telegram_alert("🟢 <b>Radar Scanner Started:</b> Hunting for whale debts (10-min intervals).")
    try:
        while True:
            try:
                print("\n🔍 Starting new radar scan...")
                start = time.time()
                tiered_targets = scan_debt_tokens()

                total = len(tiered_targets['tier_1_danger']) + len(tiered_targets['tier_2_watchlist'])
                elapsed = time.time() - start

                # Atomic save (only if we have targets)
                if total > 0:
                    save_targets_atomic(tiered_targets)
                    print(f"💾 Saved: {total} targets to '{get_target_path()}' ({elapsed:.0f}s)")

                    # Telegram summary
                    send_telegram_alert(
                        f"📡 <b>Radar Scan Complete</b>\n"
                        f"🔴 Tier 1 (Danger): {len(tiered_targets['tier_1_danger'])}\n"
                        f"🟠 Tier 2 (Watchlist): {len(tiered_targets['tier_2_watchlist'])}\n"
                        f"⏱️ Duration: {elapsed:.0f}s"
                    )
                else:
                    print("⚠️ Scan returned 0 targets. Keeping previous targets in cache.")

                print(f"⏳ Sleeping for {SCAN_INTERVAL} seconds ({SCAN_INTERVAL // 60} mins)...")
                time.sleep(SCAN_INTERVAL)

            except Exception as e:
                print(f"❌ Radar Error: {e}")
                send_telegram_alert(f"🆘 <b>Radar Crash Alert:</b> <code>{e}</code>", is_error=True)
                time.sleep(60)
    except Exception as e:
        send_telegram_alert(f"🆘 <b>Fatal Scanner Crash:</b> <code>{e}</code>")
        print(f"💥 FATAL: {e}")
        time.sleep(60)