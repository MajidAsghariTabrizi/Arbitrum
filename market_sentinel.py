import aiohttp
import asyncio
import time
import logging

logger = logging.getLogger("MarketSentinel")

class MarketSentinel:
    """
    Monitors a given market on Binance to trigger scanning only when
    volatility occurs or a heartbeat expires. This prevents RPC spam on
    quiet blocks, saving expensive node request quotas.
    """
    def __init__(self, symbol="ETHUSDT", threshold_pct=0.08, heartbeat_sec=60):
        self.symbol = symbol
        self.threshold_pct = threshold_pct
        self.heartbeat_sec = heartbeat_sec
        self.last_price = 0.0
        self.last_scan_time = 0
        self.current_price = 0.0
        self.last_fail_time = 0
        
        # Smart RPC Router State
        self.is_high_volatility = False
        self.volatility_timestamp = 0
        self.volatility_cooldown_sec = 60

    async def fetch_price(self) -> float:
        """Fetches the current price of the asset from Binance API, using a cross-process cache."""
        import os
        cache_file = f".{self.symbol.lower()}_price_cache.txt"
        current_time = time.time()
        
        # Check cache first
        try:
            if os.path.exists(cache_file):
                with open(cache_file, "r") as f:
                    ts, cached_price = f.read().split(",")
                    if current_time - float(ts) < 5.0:
                        return float(cached_price)
        except Exception:
            pass

        url = f"https://api.binance.com/api/v3/ticker/price?symbol={self.symbol}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    data = await response.json()
                    price = float(data.get("price", 0.0))
                    
                    # Store in cache
                    try:
                        with open(cache_file, "w") as f:
                            f.write(f"{current_time},{price}")
                    except Exception:
                        pass
                        
                    return price
        except Exception as e:
            logger.warning(f"⚠️ Sentinel failed to fetch price: {e}")
            return 0.0

    async def should_scan(self) -> bool:
        """
        Determines if a block scan should proceed based on volatility
        or if the heartbeat timeout has elapsed.
        """
        current_time = time.time()
        
        # 1. Heartbeat Trigger (Failsafe for quiet markets)
        if current_time - self.last_scan_time >= self.heartbeat_sec:
            return True
            
        # Fetch the latest price
        self.current_price = await self.fetch_price()

        # 2. Connection Failure (Failsafe to scan)
        if self.current_price == 0.0:
            if current_time - self.last_fail_time >= 10:
                self.last_fail_time = current_time
                return True
            return False

        # 3. First execution
        if self.last_price == 0.0:
            return True

        # 4. Volatility Spike
        price_diff_pct = abs(self.current_price - self.last_price) / self.last_price * 100.0
        if price_diff_pct > self.threshold_pct:
            logger.info(f"🚨 Volatility Spike! {self.symbol} moved {price_diff_pct:.3f}% (Price: ${self.current_price:.2f})")
            self.is_high_volatility = True
            self.volatility_timestamp = current_time
            return True

        # Check Volatility Cooldown
        if self.is_high_volatility:
            if current_time - self.volatility_timestamp >= self.volatility_cooldown_sec:
                self.is_high_volatility = False
                logger.info("📉 Market relaxed. Volatility premium mode deactivated.")

        return False

    def update_last_price(self):
        """
        Updates the baseline state marking the scan as successful.
        Called strictly after a full successful scan event is completed.
        """
        self.last_scan_time = time.time()
        if self.current_price > 0.0:
            self.last_price = self.current_price
