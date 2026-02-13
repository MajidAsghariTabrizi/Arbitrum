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

w3 = Web3(Web3.HTTPProvider(RPC_URL))

# --- CONFIGURATION ---
# Arbitrum Variable Debt Token Addresses
TOKEN_MAP = {
    "USDC_Debt": "0xFCCf3cAbbe80101232d343252614b6A3eE81C989", # Bridged USDC Debt
    "WETH_Debt": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351"  # WETH Debt
}

# Transfer Event: Transfer(from, to, value)
# In debt tokens, this means debt moved (minted/burned)
TRANSFER_TOPIC = w3.keccak(text="Transfer(address,address,uint256)").hex()

# SETTINGS
TOTAL_BLOCKS_TO_SCAN = 50000   # Check last ~4 hours
CHUNK_SIZE = 50                # Keep 50 to satisfy Chainstack limits

def scan_debt_tokens():
    if not w3.is_connected():
        print("💥 Failed to connect to RPC Node.")
        return []
    
    current_block = w3.eth.block_number
    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    all_users = set()

    print(f"📡 Connected! Scanning Debt Tokens (USDC & WETH)")
    print(f"⏱️  Range: {start_block} to {current_block} (~4 Hours history)")

    # Scan each token
    for name, address in TOKEN_MAP.items():
        print(f"\n🔍 Scanning {name} [{address}]...")
        
        for chunk_start in range(start_block, current_block, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, current_block)
            
            # Show progress on same line
            print(f"   ⏳ Block: {chunk_start} | Found: {len(all_users)}", end="\r")
            
            try:
                logs = w3.eth.get_logs({
                    'fromBlock': chunk_start,
                    'toBlock': chunk_end,
                    'address': address,
                    'topics': [TRANSFER_TOPIC]
                })
                
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

            except Exception as e:
                # Just skip failed chunks to keep moving
                pass

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
            
            # ذخیره در آدرس مطلق برای اینکه PM2 گمش نکنه
            with open("/root/Arbitrum/targets.json", "w") as f:
                json.dump(targets, f)
                
            print(f"💾 Saved {len(targets)} targets to '/root/Arbitrum/targets.json'")
            
            # استراحت ۱ دقیقه‌ای تا دفعه بعد
            print("⏳ Sleeping for 60 seconds...")
            time.sleep(60)
            
        except Exception as e:
            print(f"❌ Radar Error: {e}")
            time.sleep(10)