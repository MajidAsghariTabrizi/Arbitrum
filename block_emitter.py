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

ZMQ_ADDR = "tcp://127.0.0.1:5555"
POLL_INTERVAL = 0.5  # HTTP Polling on a single script can be rapid.

logging.basicConfig(level=logging.INFO, format='%(asctime)s | EMITTER | %(message)s')
logger = logging.getLogger("BlockEmitter")
# --- CONFIG END ---

async def main():
    logger.info("📡 Starting Centralized Block Emitter...")
    
    # 1. ZeroMQ Setup (PUB/SUB)
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.PUB)
    socket.bind(ZMQ_ADDR)
    logger.info(f"🔗 Bound ZeroMQ PUB on {ZMQ_ADDR}")

    # 2. Web3 Setup
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(PRIMARY_RPC))
    connected = await w3.is_connected()
    if not connected:
        logger.error(f"❌ Failed to connect to RPC: {PRIMARY_RPC}")
        return
    logger.info(f"🟢 Connected to RPC: {PRIMARY_RPC}")

    # 3. State Tracking
    last_block = await w3.eth.block_number
    logger.info(f"🧱 Starting from block: {last_block}")

    # 4. Main Polling Loop
    while True:
        try:
            current_block = await w3.eth.block_number
            if current_block > last_block:
                logger.info(f"🚀 New Block: {current_block}")
                # Broadcast the block number as a string
                await socket.send_string(str(current_block))
                last_block = current_block
            
            # Smart delay: if no new block, rest slightly
            await asyncio.sleep(POLL_INTERVAL)
            
        except Exception as e:
            logger.error(f"⚠️ Polling Error: {e}")
            await asyncio.sleep(2)  # Cooldown on network drop

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Block Emitter Stopped.")
