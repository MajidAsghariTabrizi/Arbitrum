import subprocess
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | NIGHT_MANAGER | %(message)s')

def run_night_shift():
    logging.info("🌙 Starting Night Shift: Halting main bots to free up RPCs...")
    subprocess.run(["pm2", "stop", "arb-engine", "tri-arb-engine", "gravity-bot", "radiant-bot"])
    time.sleep(10) # Let RPCs cool down

    logging.info("🔍 Starting Scanners...")
    subprocess.run(["pm2", "start", "scanner.py", "--interpreter", "python3", "--name", "temp-scanner"])
    subprocess.run(["pm2", "start", "radiant_scanner.py", "--interpreter", "python3", "--name", "temp-radiant-scanner"])
    
    # Wait for scanners to finish their job (Give them 20 minutes)
    logging.info("⏳ Waiting 20 minutes for scanners to rebuild targets.json...")
    time.sleep(1200)

    logging.info("🛑 Stopping Scanners...")
    subprocess.run(["pm2", "delete", "temp-scanner"])
    subprocess.run(["pm2", "delete", "temp-radiant-scanner"])

    logging.info("☀️ Morning! Restarting main combat bots...")
    subprocess.run(["pm2", "start", "arb-engine", "tri-arb-engine", "gravity-bot", "radiant-bot"])
    logging.info("✅ Night shift complete. System is back to hunting.")

if __name__ == "__main__":
    run_night_shift()
