import sqlite3
import datetime
import os
import threading

# Configuration
DB_FILE = "mission_control.db"

# Thread-safe lock for database access
db_lock = threading.Lock()

def get_connection():
    """Returns a connection to the SQLite database with WAL mode for high-frequency writes."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode: allows concurrent reads while writing — prevents DB locking
    conn.execute("PRAGMA journal_mode=WAL;")
    # NORMAL sync: balanced durability/speed — safe for non-financial writes
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    """Initializes the database with all necessary tables."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Table: Executions (Liquidation Events) — EXISTING
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT,
                user_address TEXT,
                debt_asset TEXT,
                collateral_asset TEXT,
                profit_eth REAL,
                profit_usdc REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table: Logs (System Events) — EXISTING
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table: Live Targets — NEW (real-time on-chain data from Multicall3)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS live_targets (
                address TEXT PRIMARY KEY,
                health_factor REAL,
                total_debt_usd REAL,
                total_collateral_usd REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table: System Metrics — NEW (scan performance tracking)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_number INTEGER,
                target_count INTEGER,
                scan_time_ms REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

def log_event(level, message):
    """Logs a system event to the database."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO logs (level, message) VALUES (?, ?)", (level, message))
            conn.commit()
            conn.close()
            # Also print to console for debugging
            print(f"[{level}] {message}")
    except Exception as e:
        print(f"❌ DB Log Error: {e}")

def record_execution(tx_hash, user_address, debt_asset, collateral_asset, profit_eth, profit_usdc):
    """Records a successful liquidation event."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO executions (tx_hash, user_address, debt_asset, collateral_asset, profit_eth, profit_usdc)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tx_hash, user_address, debt_asset, collateral_asset, profit_eth, profit_usdc))
            conn.commit()
            conn.close()
    except Exception as e:
        log_event("ERROR", f"Failed to record execution: {e}")

def get_recent_logs(limit=50):
    """Fetches the most recent system logs."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []

def get_executions(limit=50):
    """Fetches the most recent executions."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM executions ORDER BY id DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []

def get_total_profit():
    """Calculates total profit in ETH and USDC."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(profit_eth), SUM(profit_usdc) FROM executions")
            result = cursor.fetchone()
            conn.close()
            return {
                "eth": result[0] if result[0] else 0.0,
                "usdc": result[1] if result[1] else 0.0
            }
    except Exception:
        return {"eth": 0.0, "usdc": 0.0}


# =====================================================================
# NEW — High-Performance Functions for Real-Time Monitoring
# =====================================================================

def update_live_targets(targets_data):
    """
    Batch UPSERT all live target data in a single lightning-fast transaction.
    
    Args:
        targets_data: list of tuples (address, health_factor, total_debt_usd, total_collateral_usd)
    """
    if not targets_data:
        return
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            now = datetime.datetime.utcnow().isoformat()
            # Prepare rows with timestamp
            rows = [(addr, hf, debt, coll, now) for addr, hf, debt, coll in targets_data]
            cursor.executemany('''
                INSERT INTO live_targets (address, health_factor, total_debt_usd, total_collateral_usd, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(address) DO UPDATE SET
                    health_factor = excluded.health_factor,
                    total_debt_usd = excluded.total_debt_usd,
                    total_collateral_usd = excluded.total_collateral_usd,
                    updated_at = excluded.updated_at
            ''', rows)
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"❌ update_live_targets Error: {e}")

def log_system_metric(block_number, target_count, scan_time_ms):
    """
    Logs a single scan performance metric.
    
    Args:
        block_number: the block just scanned
        target_count: number of targets checked
        scan_time_ms: elapsed time in milliseconds
    """
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO system_metrics (block_number, target_count, scan_time_ms)
                VALUES (?, ?, ?)
            ''', (block_number, target_count, scan_time_ms))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"❌ log_system_metric Error: {e}")

def get_live_targets():
    """Fetches all live targets sorted by health_factor ascending (closest to liquidation first)."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT address, health_factor, total_debt_usd, total_collateral_usd, updated_at
                FROM live_targets
                ORDER BY health_factor ASC
            ''')
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []

def get_live_targets_summary():
    """Returns aggregated KPI data from the live_targets table."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    COALESCE(SUM(total_debt_usd), 0) as total_debt,
                    COALESCE(SUM(total_collateral_usd), 0) as total_collateral,
                    COUNT(CASE WHEN health_factor < 1.05 AND health_factor > 0 THEN 1 END) as danger_count,
                    COUNT(*) as total_count
                FROM live_targets
            ''')
            result = cursor.fetchone()
            conn.close()
            return {
                "total_debt": result[0],
                "total_collateral": result[1],
                "danger_count": result[2],
                "total_count": result[3]
            }
    except Exception:
        return {"total_debt": 0, "total_collateral": 0, "danger_count": 0, "total_count": 0}

def get_recent_metrics(limit=100):
    """Fetches the most recent system metrics for scan performance charts."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT block_number, target_count, scan_time_ms, timestamp
                FROM system_metrics
                ORDER BY id DESC
                LIMIT ?
            ''', (limit,))
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []

def get_avg_scan_time(limit=100):
    """Returns average scan time over the last N entries."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COALESCE(AVG(scan_time_ms), 0)
                FROM (SELECT scan_time_ms FROM system_metrics ORDER BY id DESC LIMIT ?)
            ''', (limit,))
            result = cursor.fetchone()
            conn.close()
            return result[0]
    except Exception:
        return 0.0


# Initialize DB on module load
init_db()
