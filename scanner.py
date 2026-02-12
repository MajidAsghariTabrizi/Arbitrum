import os
import json
import time
from web3 import Web3
from dotenv import load_dotenv

# Load Env
load_dotenv()

# Config
RPC_URL = os.getenv("RPC_URL")
if not RPC_URL:
    print("❌ RPC_URL not found in .env")
    exit()

w3 = Web3(Web3.HTTPProvider(RPC_URL))

# Aave V3 Pool Address (Arbitrum)
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# Borrow Event Signature
BORROW_TOPIC = w3.keccak(text="Borrow(address,address,address,uint256,uint256,uint256,uint16)").hex()

# SETTINGS - ULTRA SAFE MODE
TOTAL_BLOCKS_TO_SCAN = 50000   # Reduced history to speed up (last ~3 hours)
CHUNK_SIZE = 500               # Very small chunks to prevent "Limit Exceeded" error

def scan_recent_borrowers():
    if not w3.is_connected():
        print("💥 Failed to connect to RPC Node.")
        return []
    
    current_block = w3.eth.block_number
    start_block = current_block - TOTAL_BLOCKS_TO_SCAN
    
    print(f"📡 Connected! Scanning blocks {start_block} to {current_block}")
    print(f"🔄 Strategy: Ultra-Safe Chunking ({CHUNK_SIZE} blocks/req)...")

    all_users = set()
    
    # Loop backwards or forwards in chunks
    for chunk_start in range(start_block, current_block, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, current_block)
        
        # Print progress clearly
        print(f"   ⏳ Scanning: {chunk_start} -> {chunk_end} | Found so far: {len(all_users)}", end="\r")
        
        try:
            logs = w3.eth.get_logs({
                'fromBlock': chunk_start,
                'toBlock': chunk_end,
                'address': POOL_ADDRESS,
                'topics': [BORROW_TOPIC]
            })
            
            for log in logs:
                if len(log['topics']) >= 4:
                    user_address = "0x" + log['topics'][3].hex()[-40:]
                    all_users.add(w3.to_checksum_address(user_address))
            
            # Small delay to be kind to the API
            time.sleep(0.1) 
                    
        except Exception as e:
            print(f"\n   ⚠️ Error in chunk {chunk_start}-{chunk_end}: {e}")
            time.sleep(1)
            
    print(f"\n✅ Scan Complete. Found {len(all_users)} unique active borrowers.")
    return list(all_users)

if __name__ == "__main__":
    targets = scan_recent_borrowers()
    
    if len(targets) > 0:
        with open("targets.json", "w") as f:
            json.dump(targets, f)
        print(f"💾 Saved {len(targets)} targets to 'targets.json'")
    else:
        print("⚠️ No borrowers found. Try increasing TOTAL_BLOCKS_TO_SCAN.")