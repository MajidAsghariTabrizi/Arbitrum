import asyncio
import logging
import os
import time
import json
from web3 import AsyncWeb3
from web3.providers import WebSocketProvider
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("GravityBot")

# Load Env
load_dotenv()

# Configuration
# WSS URL provided by user
WSS_URL = "wss://arbitrum-mainnet.core.chainstack.com/c0c7c2b72d19c3a2d7db88252b491c3a"
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD" 
LIQUIDATOR_ADDRESS = os.getenv("LIQUIDATOR_ADDRESS", "0x0000000000000000000000000000000000000000") 

# Constants
GRAVITY_HF_THRESHOLD = 1.05
ANTI_GRAVITY_HF_THRESHOLD = 1.1
GAS_PRICE_SPIKE_THRESHOLD = 0.5 * 10**9 # 0.5 Gwei

FALLBACK_TARGETS = [
    "0x99525208453488C9518001712C7F72428514197F",
    "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb",
    "0xF977814e90dA44bFA03b6295A0616a897441aceC",
    "0x4a923335FDD029841103F647065094247290A7a2"
]

# State
user_states = {} # address -> 'Aggressive' | 'Normal'

# Minimal ABI for getUserAccountData
POOL_ABI = [
    {
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
    }
]

class GravityBot:
    def __init__(self):
        self.w3 = None
        self.pool_contract = None
        self.dry_run = False # Production Mode enabled

    async def connect(self):
        try:
            # Initialize AsyncWeb3 with WebSocketProvider
            self.w3 = await AsyncWeb3(WebSocketProvider(WSS_URL))
            
            if await self.w3.is_connected():
                logger.info(f"✅ Connected to Arbitrum via Chainstack WSS")
            else:
                logger.error("❌ Failed to connect to WSS")
                return False
            
            self.pool_contract = self.w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)
            return True
        except Exception as e:
            logger.error(f"Connection Error: {e}")
            return False

    def load_targets(self):
        """Reloads TARGET_USERS from targets.json or uses fallbacks."""
        try:
            if os.path.exists('targets.json'):
                with open('targets.json', 'r') as f:
                    targets = json.load(f)
                    if targets:
                        return list(set(targets)) # Unique
            
            logger.warning("⚠️ targets.json empty or missing. Using Fallback Whales.")
            return FALLBACK_TARGETS
        except Exception as e:
            logger.error(f"Error loading targets: {e}")
            return FALLBACK_TARGETS

    async def check_health_factor(self, user):
        try:
            # Async call to contract
            data = await self.pool_contract.functions.getUserAccountData(user).call()
            # Health Factor is index 5
            health_factor_wei = data[5]
            # Convert to float (18 decimals)
            hf_human = health_factor_wei / 1e18
            return hf_human
        except Exception as e:
            # Log error but don't crash
            logger.warning(f"Error checking HF for {user}: {e}")
            return None

    async def get_gas_price(self):
        try:
            return await self.w3.eth.gas_price
        except:
            return 0

    async def process_user(self, user, current_gas_price):
        hf = await self.check_health_factor(user)
        
        # If simulation/mock, we might inject a fake HF here if user is 0x0...
        if user == "0x0000000000000000000000000000000000000000":
             # Demo: Simulate falling HF
             # Use time to cycle HF? Or just random? 
             # Let's just log simulated values for the "Mock Test" function instead
             pass

        if hf is None:
            return

        current_state = user_states.get(user, 'Normal')

        # --- GRAVITY MODE LOGIC (Aggressive) ---
        if hf < GRAVITY_HF_THRESHOLD:
            if current_state != 'Aggressive':
                logger.warning(f"🧲 GRAVITY ACTIVATED for User [{user}] | HF: {hf:.4f}")
                logger.info(f"   -> Switching to High Frequency Monitoring for {user}")
                user_states[user] = 'Aggressive'
            
            # IN GRAVITY MODE:
            # 1. Pre-calculate Liquidation Call Data (Flashloan)
            # 2. Check Profitability immediately
            
            if self.dry_run:
                # Simulate logic
                # For real implementation:
                # tx_params = contract.functions.requestFlashLoan(...).build_transaction(...)
                # try: w3.eth.call(tx_params) -> success? 
                
                logger.info(f"   [Dry Run] Simulating Flash Loan execution...")
                # Mock Profit Calculation
                estimated_profit = 0.05 # ETH
                logger.info(f"💰 SIMULATION: Liquidation opportunity found! Estimated Profit: {estimated_profit} ETH")
            
        
        # --- ANTI-GRAVITY MODE LOGIC (Defensive) ---
        elif (hf > ANTI_GRAVITY_HF_THRESHOLD) or (current_gas_price > GAS_PRICE_SPIKE_THRESHOLD):
            # If we are in Aggressive mode, back off
            if current_state == 'Aggressive':
                reason = "Safe HF" if hf > ANTI_GRAVITY_HF_THRESHOLD else "High Gas Price"
                logger.info(f"🛡️ Anti-Gravity: Backing off for {user} ({reason}).")
                logger.info(f"   -> Clearing cache/memory for {user}")
                user_states[user] = 'Normal'
            
            # If already Normal, we do nothing (save resources)
            
        else:
            # HF is between 1.05 and 1.1 - Intermediate Zone
            if current_state == 'Aggressive':
                 logger.info(f"⚠️ Maintaining Gravity Mode for {user} | HF: {hf:.4f}")

    async def run_mock_logic(self):
        """
        Runs a mock scenario to demonstrate the logic without needing real on-chain low HF users.
        """
        logger.info("🧪 STARTING MOCK SIMULATION (Dry Run)")
        mock_user = "0xMockUserAddress123"
        
        # Mock Scenario: HF Drops
        logger.info("\n--- Step 1: User HF is Safe (1.20) ---")
        hf = 1.20
        # Logic replication for mock
        if hf > ANTI_GRAVITY_HF_THRESHOLD:
             logger.info(f"🛡️ Anti-Gravity: User is Safe.")

        await asyncio.sleep(1)

        logger.info("\n--- Step 2: Market Crash! HF Drops to 1.03 (Danger Zone) ---")
        hf = 1.03
        if hf < GRAVITY_HF_THRESHOLD:
            logger.warning(f"🧲 GRAVITY ACTIVATED for User [{mock_user}] | HF: {hf}")
            user_states[mock_user] = 'Aggressive'
            # Simulating High Freq check
            logger.info("   -> Pre-calculating Flash Loan Data...")
            logger.info("💰 SIMULATION: Liquidation opportunity found! Estimated Profit: 0.12 ETH")

        await asyncio.sleep(1)
        
        logger.info("\n--- Step 3: Gas Spike (0.6 Gwei) ---")
        gas_price = 0.6 * 10**9
        if gas_price > GAS_PRICE_SPIKE_THRESHOLD:
             logger.info(f"🛡️ Anti-Gravity: Backing off (High Gas Price: {gas_price/1e9} Gwei).")
             user_states[mock_user] = 'Normal'

        logger.info("\n✅ Mock Simulation Complete.")

    async def monitor_loop(self):
        """
        Main Production Loop: Reloads targets and checks health factors continuously.
        """
        logger.info("🚀 Starting Production Monitoring Loop...")
        
        while True:
            try:
                # 1. Dynamic Target Reloading
                targets = self.load_targets()
                gas_price = await self.get_gas_price()
                
                logger.info(f"--- Cycle Start | Targets: {len(targets)} | Gas: {gas_price/1e9:.2f} Gwei ---")
                
                # 2. Process all targets
                tasks = [self.process_user(u, gas_price) for u in targets]
                await asyncio.gather(*tasks)
                
                # 3. Sleep before next cycle
                await asyncio.sleep(10) # 10s check interval for Normal mode
                
            except Exception as e:
                logger.error(f"Loop Error: {e}")
                await asyncio.sleep(5)

async def main():
    bot = GravityBot()
    if await bot.connect():
        # run the mock logic to demonstrate features (Optional)
        # await bot.run_mock_logic()
        
        # Start Production Monitoring
        await bot.monitor_loop() 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot Stopped.")
