import streamlit as st
import pandas as pd
import sqlite3
import json
import time
import os
from web3 import Web3
from datetime import datetime

# --- Configuration ---
st.set_page_config(
    page_title="Gravity Bot - Mission Control",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# DB Path
DB_FILE = "mission_control.db"

# Custom CSS for "Hacker/Terminal" feel
st.markdown("""
    <style>
        .block-container { padding-top: 1rem; padding-bottom: 1rem; }
        .stDataFrame { border: 1px solid #333; }
        div[data-testid="stMetricValue"] { font-family: 'Courier New', monospace; }
        .terminal-log {
            background-color: #0e0e0e;
            color: #00ff00;
            font-family: 'Courier New', monospace;
            padding: 10px;
            border-radius: 5px;
            height: 400px;
            overflow-y: scroll;
            font-size: 12px;
            border: 1px solid #333;
        }
    </style>
""", unsafe_allow_html=True)

# --- Helper Functions ---

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        st.error(f"DB Connection Error: {e}")
        return None

def load_targets():
    """Reads targets.json and returns a DataFrame"""
    if not os.path.exists('targets.json'):
        return pd.DataFrame(), 0
        
    try:
        with open('targets.json', 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                # Convert to DF
                df = pd.DataFrame(data, columns=["User Address"])
                # Add mock/placeholder columns if real data isn't in JSON
                # In a real scenario, we'd fetch this or scanner would write it
                if "Health Factor" not in df.columns:
                    df["Health Factor"] = [1.2 for _ in range(len(df))] # Mock default
                if "Debt Amount" not in df.columns:
                    df["Debt Amount"] = [0.0 for _ in range(len(df))]
                return df, len(data)
    except Exception:
        pass
    return pd.DataFrame(), 0

def get_logs():
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query("SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT 50", conn)
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()

def get_executions():
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query("SELECT timestamp, rx_hash, user_address, profit_usdc, profit_eth FROM executions ORDER BY id DESC LIMIT 50", conn)
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()

def get_total_profits():
    conn = get_db_connection()
    eth, usdc = 0.0, 0.0
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT SUM(profit_eth), SUM(profit_usdc) FROM executions")
            res = cur.fetchone()
            if res:
                eth = res[0] if res[0] else 0.0
                usdc = res[1] if res[1] else 0.0
            conn.close()
        except Exception:
            conn.close()
    return eth, usdc

# --- Sidebar ---
with st.sidebar:
    st.title("🛸 Gravity Bot")
    st.caption("Arbitrum Liquidator v1.0")
    
    st.divider()
    
    # Status (Mock check of process could go here)
    st.metric("System Status", "ONLINE", delta="Active", delta_color="normal")
    
    # Profits
    total_eth, total_usdc = get_total_profits()
    st.metric("Total Profit (USDC)", f"${total_usdc:,.2f}")
    st.metric("Total Profit (ETH)", f"Ξ {total_eth:.4f}")
    
    st.divider()
    
    if st.button("Refresh Data"):
        st.rerun()
        
    # Auto-refresh logic (simple loop-based)
    time_stamp = datetime.now().strftime("%H:%M:%S")
    st.caption(f"Last Update: {time_stamp}")

# --- Main Content ---

tab1, tab2, tab3 = st.tabs(["🎯 Live Targets", "⚔️ Battle Log", "📜 System Terminal"])

with tab1:
    st.subheader("Radar Scan")
    df_targets, count = load_targets()
    
    col1, col2 = st.columns(2)
    col1.metric("Active Targets", count)
    
    if not df_targets.empty:
        # Highlight low HF
        def highlight_danger(val):
            color = 'red' if val < 1.1 else 'green'
            return f'color: {color}'

        # Apply styling if HF column exists and is numeric
        # For this demo, assuming we might simulate HF if scanner doesn't write it
        st.dataframe(
            df_targets,
            width='stretch',
            hide_index=True,
            height=400
        )
    else:
        st.info("No targets found in targets.json")

with tab2:
    st.subheader("Liquidation History")
    df_exec = get_executions()
    if not df_exec.empty:
        st.dataframe(df_exec, width='stretch')
    else:
        st.write("No liquidations recorded yet.")

with tab3:
    st.subheader("Live Logs")
    df_logs = get_logs()
    
    if not df_logs.empty:
        # Terminal view
        log_text = ""
        for index, row in df_logs.iterrows():
            # Color code based on level
            msg = f"[{row['timestamp']}] [{row['level']}] {row['message']}"
            log_text += msg + "\n"
            
        st.markdown(f'<div class="terminal-log"><pre>{log_text}</pre></div>', unsafe_allow_html=True)
    else:
        st.write("No logs available.")

# Auto-Refresh (Simple Loop)
time.sleep(5)
st.rerun()
