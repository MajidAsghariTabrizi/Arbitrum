import os
import json
import time
import requests
from web3 import Web3
from dotenv import load_dotenv

# Load Env
load_dotenv()

# --- RPC MANAGER ---
class SyncRPCManager:
    def __init__(self):
        self.primary_rpc = os.getenv("PRIMARY_RPC")
        self.fallback_rpcs = os.getenv("FALLBACK_RPCS", "").split(",")
        self.fallback_rpcs = [url.strip() for url in self.fallback_rpcs if url.strip()]
        
        self.active_rpc_index = -1 # -1 = Primary, 0+ = Fallback index
        self.w3 = Web3(Web3.HTTPProvider(self.primary_rpc, request_kwargs={'timeout': 60}))
        
        # Validation
        if not self.primary_rpc:
            print("❌ PRIMARY_RPC not found in .env")
            exit()

    def check_primary_health(self):
        """Probes Primary RPC if we are currently on a Fallback."""
        if self.active_rpc_index == -1:
            return # Already on primary

        try:
            # Probe Primary
            temp_w3 = Web3(Web3.HTTPProvider(self.primary_rpc, request_kwargs={'timeout': 10}))
            temp_w3.eth.block_number
            
            # If success, switch back
            print("\n🟢 Primary RPC checked OK. Switching back!")
            self.w3 = temp_w3
            self.active_rpc_index = -1
            send_telegram_alert("🟢 <b>Primary RPC Restored.</b> Switched back to main node.")
        except Exception:
            pass # Primary still down, stay on fallback

    def handle_failure(self):
        """Switches to next fallback RPC on failure."""
        curr_mode = "Primary" if self.active_rpc_index == -1 else f"Fallback #{self.active_rpc_index + 1}"
        print(f"⚠️ RPC Failure on {curr_mode}. Switching...")
        
        # Try next fallback
        next_idx = self.active_rpc_index + 1
        if next_idx < len(self.fallback_rpcs):
            new_url = self.fallback_rpcs[next_idx]
            self.active_rpc_index = next_idx
            self.w3 = Web3(Web3.HTTPProvider(new_url, request_kwargs={'timeout': 60}))
            msg = f"⚠️ <b>Primary RPC Failed.</b> Switching to Fallback #{next_idx + 1}."
            print(f"🔄 Switched to Fallback: {new_url}")
            send_telegram_alert(msg, is_error=True)
            return True
        else:
            print("❌ All Fallbacks exhausted. Resetting to Primary to retry.")
            self.active_rpc_index = -1
            self.w3 = Web3(Web3.HTTPProvider(self.primary_rpc, request_kwargs={'timeout': 60}))
            return False

rpc_manager = SyncRPCManager()

# --- CONFIGURATION ---

# Aave V3 Arbitrum PoolDataProvider (Checksummed)
DATA_PROVIDER_ADDRESS = Web3.to_checksum_address("0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654")

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
# In debt tokens, this means debt moved (minted/burned)
# Transfer Event: Transfer(from, to, value)
# In debt tokens, this means debt moved (minted/burned)
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

# SETTINGS
TOTAL_BLOCKS_TO_SCAN = 50000   # Check last ~4 hours
CHUNK_SIZE = 50                # Keep 50 to satisfy Chainstack limits

# Anti-spam cooldown for error alerts
LAST_ERRORS = {}
ALERT_COOLDOWN = 300  # 5 minutes


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
            # Returns: (aToken, stableDebtToken, variableDebtToken)
            result = data_provider.functions.getReserveTokensAddresses(underlying_cs).call()
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


def save_targets_atomic(targets_list):
    """Atomically writes targets to JSON using temp file + os.replace().
    Prevents JSON decode errors if the bot reads during a write."""
    target_path = get_target_path()
    temp_path = target_path + ".tmp"
    with open(temp_path, "w") as f:
        json.dump(targets_list, f)
    os.replace(temp_path, target_path)


def scan_debt_tokens():
    global RPC_WAS_DOWN

    # 1. Proactive Health Check (Auto-Recovery)
    rpc_manager.check_primary_health()
    w3 = rpc_manager.w3 # Get current active instance

    # Active RPC test: fetch block_number to get the EXACT error on failure
    try:
        current_block = w3.eth.block_number
    except Exception as e:
        error_msg = str(e)
        print(f"💥 RPC Connection Failed: {error_msg}")
        
        # 2. Trigger Failover
        rpc_manager.handle_failure()
        
        if not RPC_WAS_DOWN:
            send_telegram_alert(f"⚠️ <b>Scanner RPC Down:</b>\n<code>{error_msg}</code>", is_error=True)
            RPC_WAS_DOWN = True
        return []

    # RPC recovered — send recovery alert if it was previously down
    if RPC_WAS_DOWN:
        send_telegram_alert(f"🟢 <b>Scanner RPC Restored:</b> Connected (Block {current_block}).")
        RPC_WAS_DOWN = False

    # Dynamically build the debt token map from on-chain data
    print("📡 Fetching Variable Debt Token addresses from PoolDataProvider...")
    token_map = build_token_map()
    if not token_map:
        print("❌ Could not load any debt tokens. Aborting scan.")
        return []
    print(f"🎯 Loaded {len(token_map)} debt tokens.\n")

    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    all_users = set()

    asset_names = ", ".join(UNDERLYING_ASSETS.keys())
    print(f"📡 Connected! Scanning Debt Tokens ({asset_names})")
    print(f"⏱️  Range: {start_block} to {current_block} (~4 Hours history)")

    # Scan each token (Progressive Feeding: save after each token)
    for name, address in token_map.items():
        try:
            print(f"\n🔍 Scanning {name} [{address}]...")
            
            for chunk_start in range(start_block, current_block, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE, current_block)
                
                # Show progress on same line
                print(f"   ⏳ Block: {chunk_start} | Found: {len(all_users)}", end="\r")
                
                # Retry Logic for Stability
                logs = []
                for attempt in range(3):
                    try:
                        logs = w3.eth.get_logs({
                            'fromBlock': chunk_start,
                            'toBlock': chunk_end,
                            'address': address,
                            'topics': [TRANSFER_TOPIC]
                        })
                        break # Success
                    except Exception as e:
                        # Failover check inside the chunk loop
                        rpc_manager.handle_failure()
                        w3 = rpc_manager.w3 # Update reference
                        if attempt < 2:
                            time.sleep(2)
                        else:
                            logs = [] # Give up on this chunk
                
                for log in logs:
                    if len(log['topics']) >= 3:
                        # Topic 1 is 'from', Topic 2 is 'to'
                        # In Debt tokens: 
                        # - mint (borrow): from=0x0, to=User
                        # - burn (repay): from=User, to=0x0
                        addr1 = Web3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                        addr2 = Web3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])
                        
                        # Filter out the zero address (mint/burn origin)
                        if addr1 != "0x0000000000000000000000000000000000000000":
                            all_users.add(addr1)
                        if addr2 != "0x0000000000000000000000000000000000000000":
                            all_users.add(addr2)
                
                time.sleep(0.05) # Rate limit protection

            # 🚀 Progressive Feeding: flush targets to disk after each token scan
            # Cache Retention: only write if we have targets
            if len(all_users) > 0:
                save_targets_atomic(list(all_users))
                print(f"\n   💾 Progressive save: {len(all_users)} targets flushed to disk.")

        except Exception as e:
            print(f"\n   ❌ Error scanning {name}: {e}")
            send_telegram_alert(f"⚠️ <b>Scanner Error</b> on <code>{name}</code>:\n<code>{e}</code>", is_error=True)
            continue

    final_list = list(all_users)
    
    # --- FALLBACK MECHANISM ---
    # If network is super quiet, add some known active whales so bot is not empty
    if len(final_list) == 0:
        print("\n⚠️ Network quiet. Adding fallback targets (Active Whales) to ensure bot runs.")
        fallback_targets = [
            "0x99525208453488C9518001712C7F72428514197F",
            "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb",
            "0xF977814e90dA44bFA03b6295A0616a897441aceC"
        ]
        final_list.extend(fallback_targets)

    print(f"\n\n✅ Scan Complete. Total Targets: {len(final_list)}")
    return final_list

if __name__ == "__main__":
    send_telegram_alert("🟢 <b>Radar Scanner Started:</b> Hunting for whale debts.")
    try:
        while True:
            try:
                print("\n🔍 Starting new radar scan...")
                targets = scan_debt_tokens()
                
                # Final atomic save (Cache Retention: only if we have targets)
                if len(targets) > 0:
                    save_targets_atomic(targets)
                    print(f"💾 Final save: {len(targets)} targets to '{get_target_path()}'")
                else:
                    print("⚠️ Scan returned 0 targets. Keeping previous targets in cache.")
                
                print("⏳ Sleeping for 60 seconds...")
                time.sleep(60)
                
            except Exception as e:
                print(f"❌ Radar Error: {e}")
                send_telegram_alert(f"🆘 <b>Radar Crash Alert:</b> <code>{e}</code>", is_error=True)
                time.sleep(10)
    except Exception as e:
        send_telegram_alert(f"🆘 <b>Fatal Scanner Crash:</b> <code>{e}</code>")
        print(f"💥 FATAL: {e}")
        time.sleep(60)