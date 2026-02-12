import requests
import json
import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("Scanner")

SUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum"

def fetch_targets():
    query = """
    {
      users(first: 1000, where: {borrowedReservesCount_gt: 0}) {
        id
      }
    }
    """
    
    logger.info("📡 Querying Aave V3 Arbitrum Subgraph...")
    
    try:
        response = requests.post(SUBGRAPH_URL, json={'query': query})
        response.raise_for_status()
        
        data = response.json()
        users = data.get('data', {}).get('users', [])
        
        target_addresses = [user['id'] for user in users]
        
        # Save to targets.json
        with open('targets.json', 'w') as f:
            json.dump(target_addresses, f, indent=4)
            
        logger.info(f"✅ Successfully found and saved {len(target_addresses)} targets to targets.json")
        return target_addresses
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch targets: {e}")
        return []

if __name__ == "__main__":
    fetch_targets()
