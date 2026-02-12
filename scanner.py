import os
import json
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
# Borrow(address,address,address,uint256,uint256,uint256,uint16)
BORROW_TOPIC = w3.keccak(text="Borrow(address,address,address,uint256,uint256,uint256,uint16)").hex()

def scan_recent_borrowers():
    if not w3.is_connected():
        print("💥 Failed to connect to RPC Node.")
        return []
    
    current_block = w3.eth.block_number
    print(f"📡 Connected to Chainstack! Current Block: {current_block}")
    
    # Scan last 200,000 blocks (Approx ~14 hours of activity)
    # This finds ACTIVE users who recently interacted
    start_block = current_block - 200000
    print(f"🔍 Scanning blocks {start_block} to {current_block} for borrowers...")

    try:
        logs = w3.eth.get_logs({
            'fromBlock': start_block,
            'toBlock': current_block,
            'address': POOL_ADDRESS,
            'topics': [BORROW_TOPIC]
        })
        
        users = set()
        for log in logs:
            # Topic 0 is event hash
            # Topic 1 is reserve (collateral)
            # Topic 2 is onBehalfOf (THE USER WE WANT) - indexed
            # Topic 3 is referralCode - indexed (wait, check abi)
            
            # According to ABI: Borrow(address indexed reserve, address user, address indexed onBehalfOf, ...)
            # So topic[1] = reserve, topic[2] = user (initiator), topic[3] = onBehalfOf (target)
            
            if len(log['topics']) >= 4:
                # Extract address from topic (last 40 chars)
                user_address = "0x" + log['topics'][3].hex()[-40:]
                users.add(w3.to_checksum_address(user_address))
        
        print(f"✅ Found {len(users)} active borrowers from blockchain logs.")
        return list(users)

    except Exception as e:
        print(f"❌ Error scanning logs: {e}")
        return []

if __name__ == "__main__":
    targets = scan_recent_borrowers()
    
    if len(targets) > 0:
        with open("targets.json", "w") as f:
            json.dump(targets, f)
        print(f"💾 Saved {len(targets)} targets to 'targets.json'")
    else:
        print("⚠️ No borrowers found in recent blocks. Try increasing block range.")