import sqlite3
import datetime
import os
import threading

# Configuration
DB_FILE = "mission_control.db"

# Thread-safe lock for database access
db_lock = threading.Lock()

def get_connection():
    """Returns a connection to the SQLite database."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database with necessary tables."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        
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

# Initialize DB on module load
init_db()
