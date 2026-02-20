import os
import time
import asyncio
import logging
import zmq
import zmq.asyncio
from web3 import AsyncWeb3
from dotenv import load_dotenv

# --- CONFIG START ---
load_dotenv()
PRIMARY_RPC = os.getenv("PRIMARY_RPC")
if not PRIMARY_RPC:
    raise ValueError("❌ Missing PRIMARY_RPC in .env for Block Emitter.")

FALLBACK_RPCS_RAW = os.getenv("FALLBACK_RPCS", "")
FALLBACK_RPCS = [url.strip().strip("'").strip('"') for url in FALLBACK_RPCS_RAW.split(",") if url.strip()]

ZMQ_ADDR = "tcp://127.0.0.1:5555"
STATE_FILE = ".system_state"

# Dynamic poll intervals based on system state
POLL_INTERVAL_PEACE = 5.0   # Conserve RPC bandwidth
POLL_INTERVAL_WAR   = 0.5   # Maximum speed for volatility

logging.basicConfig(level=logging.INFO, format='%(asctime)s | EMITTER | %(message)s')
logger = logging.getLogger("BlockEmitter")
# --- CONFIG END ---


def read_system_state() -> str:
    """Read the current system state from .system_state file."""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = f.read().strip().upper()
                if state in ("WAR", "PEACE"):
                    return state
    except Exception:
        pass
    return "PEACE"  # Default to PEACE if file missing or unreadable


async def main():
    logger.info("📡 Starting Centralized Block Emitter...")
    
    # 1. ZeroMQ Setup (PUB/SUB)
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.PUB)
    socket.bind(ZMQ_ADDR)
    logger.info(f"🔗 Bound ZeroMQ PUB on {ZMQ_ADDR}")

    # 2. Sticky Web3 Setup (PRIMARY_RPC, fallback on hard error only)
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(PRIMARY_RPC, request_kwargs={"timeout": 60}))
    fallback_index = 0
    
    while True:
        try:
            connected = await w3.is_connected()
            if not connected:
                raise ConnectionError("Not connected")
            
            last_block = await w3.eth.block_number
            logger.info(f"🟢 Connected to PRIMARY_RPC: {PRIMARY_RPC[:50]}...")
            logger.info(f"🧱 Starting from block: {last_block}")
            break
        except Exception as e:
            logger.warning(f"RPC busy on startup ({e}), trying fallback...")
            if FALLBACK_RPCS:
                fb_url = FALLBACK_RPCS[fallback_index % len(FALLBACK_RPCS)]
                w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(fb_url, request_kwargs={"timeout": 60}))
                fallback_index += 1
            await asyncio.sleep(5)

    # 3. Main Polling Loop (Dynamic Throttle)
    last_state = None
    while True:
        try:
            current_block = await w3.eth.block_number
            if current_block > last_block:
                logger.info(f"🚀 New Block: {current_block}")
                await socket.send_string(str(current_block))
                last_block = current_block
            
            # Dynamic throttle based on .system_state
            state = read_system_state()
            if state == "WAR":
                poll_interval = POLL_INTERVAL_WAR
            else:
                poll_interval = POLL_INTERVAL_PEACE

            # Log state transitions
            if state != last_state:
                logger.info(f"🔄 System State: {state} → POLL_INTERVAL={poll_interval}s")
                last_state = state
            
            await asyncio.sleep(poll_interval)
            
        except Exception as e:
            logger.error(f"⚠️ Polling Error: {e}. Reconnecting to PRIMARY_RPC...")
            w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(PRIMARY_RPC, request_kwargs={"timeout": 60}))
            await asyncio.sleep(2)
            continue

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Block Emitter Stopped.")

