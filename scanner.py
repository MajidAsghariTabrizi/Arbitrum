import requests
import json
import time

# Aave V3 Arbitrum Subgraph
GRAPH_URL = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum"

def get_risky_users():
    print("📡 Querying Aave V3 Arbitrum Subgraph (Deep Scan)...")
    
    # کوئری جدید: جستجو در دفترچه بدهی‌ها (دقیق‌تر)
    query = """
    {
      userReserves(first: 1000, where: {currentTotalDebt_gt: "0"}, orderBy: currentTotalDebt, orderDirection: desc) {
        user {
          id
        }
      }
    }
    """
    
    try:
        response = requests.post(GRAPH_URL, json={'query': query})
        data = response.json()
        
        if 'errors' in data:
            print("❌ Graph Error:", data['errors'])
            return []
            
        # استخراج آدرس‌های منحصر به فرد
        users = list(set([item['user']['id'] for item in data['data']['userReserves']]))
        
        print(f"✅ Successfully found {len(users)} active borrowers.")
        return users
        
    except Exception as e:
        print(f"💥 Connection failed: {e}")
        return []

if __name__ == "__main__":
    targets = get_risky_users()
    
    if len(targets) > 0:
        with open("targets.json", "w") as f:
            json.dump(targets, f)
        print(f"💾 Saved {len(targets)} unique targets to 'targets.json'")
    else:
        print("⚠️ No targets found. Something is wrong with the Graph API.")