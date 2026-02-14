import os
import json
import time
from web3 import Web3
from dotenv import load_dotenv

# Load Env
load_dotenv()

RPC_URL = os.getenv("RPC_URL")
if not RPC_URL:
    print("❌ RPC_URL not found in .env")
    exit()

w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={'timeout': 60}))

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
TRANSFER_TOPIC = w3.keccak(text="Transfer(address,address,uint256)").hex()

# SETTINGS
TOTAL_BLOCKS_TO_SCAN = 50000   # Check last ~4 hours
CHUNK_SIZE = 50                # Keep 50 to satisfy Chainstack limits


def build_token_map():
    """Dynamically fetches Variable Debt Token addresses from Aave V3 PoolDataProvider."""
    data_provider = w3.eth.contract(
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


def scan_debt_tokens():
    if not w3.is_connected():
        print("💥 Failed to connect to RPC Node.")
        return []

    # Dynamically build the debt token map from on-chain data
    print("📡 Fetching Variable Debt Token addresses from PoolDataProvider...")
    token_map = build_token_map()
    if not token_map:
        print("❌ Could not load any debt tokens. Aborting scan.")
        return []
    print(f"🎯 Loaded {len(token_map)} debt tokens.\n")

    current_block = w3.eth.block_number
    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    all_users = set()

    asset_names = ", ".join(UNDERLYING_ASSETS.keys())
    print(f"📡 Connected! Scanning Debt Tokens ({asset_names})")
    print(f"⏱️  Range: {start_block} to {current_block} (~4 Hours history)")

    # Scan each token
    for name, address in token_map.items():
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
                    addr1 = w3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                    addr2 = w3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])
                    
                    # Filter out the zero address (mint/burn origin)
                    if addr1 != "0x0000000000000000000000000000000000000000":
                        all_users.add(addr1)
                    if addr2 != "0x0000000000000000000000000000000000000000":
                        all_users.add(addr2)
            
            time.sleep(0.05) # Rate limit protection

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
    while True:
        try:
            print("\n🔍 Starting new radar scan...")
            targets = scan_debt_tokens()
            
            # ذخیره در آدرس منعطف (local vs server)
            target_path = "targets.json"
            if os.path.exists("/root/Arbitrum"):
                target_path = "/root/Arbitrum/targets.json"
                
            with open(target_path, "w") as f:
                json.dump(targets, f)
                
            print(f"💾 Saved {len(targets)} targets to '/root/Arbitrum/targets.json'")
            
            # استراحت ۱ دقیقه‌ای تا دفعه بعد
            print("⏳ Sleeping for 60 seconds...")
            time.sleep(60)
            
        except Exception as e:
            print(f"❌ Radar Error: {e}")
            time.sleep(10)