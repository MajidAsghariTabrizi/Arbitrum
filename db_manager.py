"""
═══════════════════════════════════════════════════════════════════════════════
🛸 ANTI-GRAVITY — Database Manager (SQLite)
═══════════════════════════════════════════════════════════════════════════════
Central database handler for all Anti-Gravity business lines:
  • Liquidation Bot (gravity_bot.py) — executions, logs, targets, metrics
  • DEX Arbitrage Engine (arb_engine.py) — arb_executions, arb_spreads

Uses WAL mode for high-frequency concurrent reads/writes.
All tables use CREATE TABLE IF NOT EXISTS for safe auto-migration.
═══════════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import datetime
import os
import threading
import json

# Configuration
DB_FILE = "mission_control.db"

# Thread-safe lock for database access
db_lock = threading.Lock()


def get_connection():
    """Returns a connection to the SQLite database with WAL mode for high-frequency writes."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    """Initializes the database with all necessary tables.
    Includes robust auto-migration for existing databases."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()

        # ═════════════════════════════════════════════════════════════
        # LIQUIDATION BOT TABLES
        # ═════════════════════════════════════════════════════════════

        # Table: Executions (Liquidation Events)
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

        # Table: Logs (System Events)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table: Live Targets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS live_targets (
                address TEXT PRIMARY KEY,
                health_factor REAL,
                total_debt_usd REAL,
                total_collateral_usd REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table: System Metrics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_number INTEGER,
                target_count INTEGER,
                scan_time_ms REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ═════════════════════════════════════════════════════════════
        # DEX ARBITRAGE TABLES
        # ═════════════════════════════════════════════════════════════

        # Table: Arb Executions (Successful Arbitrage Trades)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS arb_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT,
                token_pair TEXT,
                dex_a TEXT,
                dex_b TEXT,
                profit_usd REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Table: Arb Spreads (Live Spread Monitoring for Charts)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS arb_spreads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_pair TEXT,
                dex_a TEXT,
                dex_b TEXT,
                spread_percent REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ═════════════════════════════════════════════════════════════
        # SCHEMA MIGRATION: Auto-add columns if missing
        # ═════════════════════════════════════════════════════════════
        try:
            cursor.execute("ALTER TABLE system_metrics ADD COLUMN tier_1_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute("ALTER TABLE system_metrics ADD COLUMN tier_2_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

        conn.commit()
        conn.close()


# =====================================================================
# CORE FUNCTIONS
# =====================================================================

def log_event(level, message):
    """Logs a system event to the database."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO logs (level, message) VALUES (?, ?)", (level, message))
            conn.commit()
            conn.close()
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


def get_critical_logs(limit=10):
    """Fetches recent ERROR and WARNING logs for the Error Board."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM logs WHERE level IN ('error', 'warning', 'ERROR', 'WARNING') ORDER BY id DESC LIMIT ?", 
                (limit,)
            )
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []


def get_executions(limit=50):
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
# HIGH-PERFORMANCE FUNCTIONS
# =====================================================================

def update_live_targets(targets_data):
    """Batch UPSERT live target data."""
    if not targets_data:
        return
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            now = datetime.datetime.utcnow().isoformat()
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


def log_system_metric(block_number, target_count, scan_time_ms, tier_1_count=0, tier_2_count=0):
    """Logs scan metrics with tiered breakdown."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO system_metrics (block_number, target_count, scan_time_ms, tier_1_count, tier_2_count)
                VALUES (?, ?, ?, ?, ?)
            ''', (block_number, target_count, scan_time_ms, tier_1_count, tier_2_count))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"❌ log_system_metric Error: {e}")


# =====================================================================
# DASHBOARD QUERIES
# =====================================================================

def get_live_targets():
    """Fetches all live targets sorted by HF ascending."""
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
    """Returns aggregated KPI data."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    COALESCE(SUM(total_debt_usd), 0),
                    COALESCE(SUM(total_collateral_usd), 0),
                    COUNT(CASE WHEN health_factor < 1.05 AND health_factor > 0 THEN 1 END),
                    COUNT(CASE WHEN health_factor >= 1.05 AND health_factor < 1.20 THEN 1 END),
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN health_factor < 1.05 AND health_factor > 0 THEN total_debt_usd ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN health_factor >= 1.05 AND health_factor < 1.20 THEN total_debt_usd ELSE 0 END), 0)
                FROM live_targets
            ''')
            r = cursor.fetchone()
            conn.close()
            return {
                "total_debt": r[0], "total_collateral": r[1],
                "danger_count": r[2], "watchlist_count": r[3],
                "total_count": r[4], "danger_debt": r[5], "watchlist_debt": r[6]
            }
    except Exception:
        return {
            "total_debt": 0, "total_collateral": 0, "danger_count": 0,
            "watchlist_count": 0, "total_count": 0, "danger_debt": 0, "watchlist_debt": 0
        }


def get_recent_metrics(limit=100):
    """
    Fetches recent system metrics.
    Robustly handles cases where tier columns might be missing in query selection.
    """
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT block_number, target_count, tier_1_count, tier_2_count, scan_time_ms, timestamp
                FROM system_metrics
                ORDER BY id DESC
                LIMIT ?
            ''', (limit,))
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception as e:
        print(f"Metric Fetch Error: {e}")
        return []


def get_avg_scan_time(limit=100):
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


# =====================================================================
# ARBITRAGE FUNCTIONS
# =====================================================================

def record_arb_execution(tx_hash, token_pair, dex_a, dex_b, profit_usd):
    """Records a successful arbitrage execution."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO arb_executions (tx_hash, token_pair, dex_a, dex_b, profit_usd)
                VALUES (?, ?, ?, ?, ?)
            ''', (tx_hash, token_pair, dex_a, dex_b, profit_usd))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"❌ record_arb_execution Error: {e}")


def log_arb_spread(token_pair, dex_a, dex_b, spread_percent):
    """Logs a spread opportunity (for live chart)."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO arb_spreads (token_pair, dex_a, dex_b, spread_percent)
                VALUES (?, ?, ?, ?)
            ''', (token_pair, dex_a, dex_b, spread_percent))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"❌ log_arb_spread Error: {e}")


def get_recent_arb_executions(limit=50):
    """Fetches recent successful arbitrage executions."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, tx_hash, token_pair, dex_a, dex_b, profit_usd, timestamp "
                "FROM arb_executions ORDER BY id DESC LIMIT ?", (limit,)
            )
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []


def get_recent_spreads(limit=200):
    """Fetches recent spreads for the live chart."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, token_pair, dex_a, dex_b, spread_percent, timestamp "
                "FROM arb_spreads ORDER BY id DESC LIMIT ?", (limit,)
            )
            rows = cursor.fetchall()
            conn.close()
            return rows
    except Exception:
        return []


def get_total_arb_profit():
    """Returns total arbitrage profit in USD."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(SUM(profit_usd), 0) FROM arb_executions")
            result = cursor.fetchone()
            conn.close()
            return result[0] if result[0] else 0.0
    except Exception:
        return 0.0


def get_arb_execution_count():
    """Returns total number of successful arb executions."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM arb_executions")
            result = cursor.fetchone()
            conn.close()
            return result[0] if result[0] else 0
    except Exception:
        return 0


def get_active_spreads_count(minutes=60):
    """Returns count of unique spreads found in the last N minutes."""
    try:
        with db_lock:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*)
                FROM arb_spreads
                WHERE timestamp >= datetime('now', ? || ' minutes')
            ''', (f"-{minutes}",))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result[0] else 0
    except Exception:
        return 0


# Initialize DB on module load
init_db()
