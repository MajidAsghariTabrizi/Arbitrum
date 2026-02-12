import os
import json
import time
import logging
import requests
from web3 import Web3
from dotenv import load_dotenv

# --- 1. CONFIGURATION & SETUP ---

# آدرس دقیق فایل env برای اینکه PM2 گم نکند
ENV_PATH = "/root/Arbitrum/.env"
load_dotenv(ENV_PATH)

# تنظیمات لاگ (نمایش در ترمینال)
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

# تلاش برای اتصال به دیتابیس داشبورد (اگر موجود باشد)
try:
    import db_manager
    DB_ENABLED = True
except ImportError:
    DB_ENABLED = False
    logging.warning("⚠️ db_manager.py not found. Dashboard logging disabled.")

# دریافت متغیرها
RPC_URL = os.getenv("RPC_URL")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")  # اختیاری

# بررسی حیاتی متغیرها
if not RPC_URL or not PRIVATE_KEY:
    logging.error(f"❌ Critical Error: Missing configuration in {ENV_PATH}")
    logging.error(f"Make sure RPC_URL and PRIVATE_KEY are set.")
    exit()

# Setup Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))
account = w3.eth.account.from_key(PRIVATE_KEY)

# Aave V3 Pool Address (Arbitrum)
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# ABI خلاصه شده برای گرفتن Health Factor
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

pool_contract = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)

# --- 2. HELPER FUNCTIONS ---

def log_system(msg, level="info"):
    """Logs to Console, Database (Dashboard), and Discord (if critical)."""
    # 1. Console Log
    if level == "error":
        logging.error(msg)
    else:
        logging.info(msg)
    
    # 2. Database Log (For Streamlit Dashboard)
    if DB_ENABLED:
        try:
            db_manager.log_event(level, msg)
        except:
            pass

    # 3. Discord Log (Only for Liquidations or Errors)
    if DISCORD_WEBHOOK and (level == "success" or level == "error"):
        try:
            color = 0x00ff00 if level == "success" else 0xff0000
            requests.post(DISCORD_WEBHOOK, json={
                "embeds": [{"title": "🦅 Gravity Bot", "description": msg, "color": color}]
            })
        except:
            pass

def load_targets():
    """Reads targets.json dynamically."""
    try:
        with open("targets.json", "r") as f:
            return json.load(f)
    except:
        return []

def check_health(user):
    """Returns Health Factor (HF)."""
    try:
        data = pool_contract.functions.getUserAccountData(user).call()
        # Health Factor is at index 5, with 18 decimals
        hf = data[5] / 10**18
        return hf
    except Exception as e:
        return 100.0  # Assume safe on error

# --- 3. MAIN LOOP ---

def run_bot():
    log_system(f"🦅 Gravity Bot Started. Wallet: {account.address}", "info")
    
    while True:
        # 1. Load Targets (Hot Reload)
        targets = load_targets()
        
        if len(targets) > 0:
            print(f"🎯 Syncing... {len(targets)} active targets loaded.", end="\r")
        
        # 2. Scan Targets
        for user in targets:
            hf = check_health(user)
            
            # 🚨 LIQUIDATION TRIGGER (HF < 1.0)
            if 0 < hf < 1.0:
                msg = f"💥 LIQUIDATING USER: {user} | HF: {hf:.4f}"
                log_system(msg, "success") # 'success' triggers Discord & DB green log
                
                # TODO: Uncomment below line to enable Real Money execution
                # execute_flash_loan(user)
                
            # Log risky users just for info (HF < 1.05)
            elif hf < 1.05:
                log_system(f"⚠️ Risky User: {user} | HF: {hf:.4f}", "warning")
                
            time.sleep(0.05) # Anti-Rate Limit

        time.sleep(2) # Wait before re-reading file

if __name__ == "__main__":
    run_bot()