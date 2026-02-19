"""
═══════════════════════════════════════════════════════════════════════════════
🛸 ANTI-GRAVITY — Executive Command Center (Streamlit)
═══════════════════════════════════════════════════════════════════════════════
Ultimate unified dashboard for all MEV operations.

Features:
  Tab 1: 🌍 Main Command  — PM2 Fleet Health, Error Board, Global KPIs
  Tab 2: ⚔️ Liquidations — Radar, Danger Zone, Watchlist, Executions
  Tab 3: 🔄 Arbitrage    — Spread monitoring, Live charts, Arb history
  Tab 4: 📜 Full Terminal — Complete system log viewer

Uses subprocess.check_output to ping PM2 for live backend health.
═══════════════════════════════════════════════════════════════════════════════
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import json
import time
import os
import re
import subprocess
from datetime import datetime

# Try to import st_autorefresh for clean auto-refresh
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

# --- Configuration ---
st.set_page_config(
    page_title="⚡ Anti-Gravity — Command Center",
    page_icon="🛸",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# DB Path
DB_FILE = "mission_control.db"

# =====================================================================
# CUSTOM CSS — Professional Dark-Mode Quant Trading UI
# =====================================================================
st.markdown("""
    <style>
        /* Global spacing & Theme */
        .block-container { padding-top: 2rem; padding-bottom: 2rem; max-width: 95%; }
        body { background-color: #050508; color: #f1f2f6; }

        /* Metric Cards */
        div[data-testid="stMetricValue"] {
            font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', monospace;
            font-size: 2rem;
            font-weight: 800;
            text-shadow: 0 0 10px rgba(255,255,255,0.1);
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            font-weight: 600;
            opacity: 0.8;
        }

        /* KPI Card Highlights */
        .kpi-profit div[data-testid="stMetricValue"] { color: #00e676 !important; text-shadow: 0 0 15px rgba(0,230,118,0.3); }
        .kpi-hunts div[data-testid="stMetricValue"]  { color: #00b0ff !important; text-shadow: 0 0 15px rgba(0,176,255,0.3); }
        .kpi-danger div[data-testid="stMetricValue"] { color: #ff1744 !important; text-shadow: 0 0 15px rgba(255,23,68,0.3); }
        .kpi-arb div[data-testid="stMetricValue"]    { color: #d500f9 !important; text-shadow: 0 0 15px rgba(213,0,249,0.3); }

        /* Header Bar */
        .header-bar {
            background: linear-gradient(90deg, #11111a 0%, #1a1a2e 50%, #11111a 100%);
            padding: 20px 30px;
            border-radius: 12px;
            margin-bottom: 2rem;
            color: #ffffff;
            font-size: 1.5rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            border-left: 4px solid #00b0ff;
            border-right: 4px solid #00b0ff;
            box-shadow: 0 8px 32px 0 rgba(0,0,0,0.5);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        /* PM2 Fleet Status Table */
        .fleet-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0 8px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.1rem;
        }
        .fleet-table th {
            text-align: left;
            padding: 12px 20px;
            color: #8c9eff;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 700;
            font-size: 0.85rem;
            border-bottom: 1px solid rgba(140, 158, 255, 0.2);
        }
        .fleet-table td {
            background-color: #0c0c14;
            padding: 16px 20px;
            border-top: 1px solid rgba(255,255,255,0.05);
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .fleet-table tr td:first-child { border-left: 1px solid rgba(255,255,255,0.05); border-top-left-radius: 8px; border-bottom-left-radius: 8px; font-weight: bold;}
        .fleet-table tr td:last-child { border-right: 1px solid rgba(255,255,255,0.05); border-top-right-radius: 8px; border-bottom-right-radius: 8px; }
        
        .status-online { color: #00e676; font-weight: bold; text-shadow: 0 0 8px rgba(0,230,118,0.5); }
        .status-offline { color: #ff1744; font-weight: bold; padding: 4px 8px; background: rgba(255,23,68,0.1); border-radius: 4px; border: 1px solid rgba(255,23,68,0.3); animation: pulse 2s infinite; }

        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(255,23,68,0.4); }
            70% { box-shadow: 0 0 0 10px rgba(255,23,68,0); }
            100% { box-shadow: 0 0 0 0 rgba(255,23,68,0); }
        }

        /* Error Board Container */
        .error-board {
            background-color: #1a0b0f;
            color: #ff8a80;
            font-family: 'JetBrains Mono', monospace;
            padding: 20px;
            border-radius: 12px;
            height: 350px;
            overflow-y: auto;
            font-size: 13px;
            border: 1px solid #ff1744;
            line-height: 1.8;
            box-shadow: inset 0 0 20px rgba(255,23,68,0.15);
        }
        .error-board-title {
            color: #ff1744;
            font-size: 1.2rem;
            font-weight: 800;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        /* Standard Terminal Container */
        .terminal-log {
            background-color: #080810;
            color: #c8d6e5;
            font-family: 'JetBrains Mono', monospace;
            padding: 16px;
            border-radius: 10px;
            height: 600px;
            overflow-y: auto;
            font-size: 12px;
            border: 1px solid #1a1a2e;
            line-height: 1.7;
        }

        /* Terminal Keyword Highlights */
        .log-time     { opacity: 0.5; font-size: 10px; margin-right: 8px; }
        .log-error    { color: #ff1744; font-weight: bold; }
        .log-warning  { color: #ff9100; font-weight: bold; }
        .log-success  { color: #00e676; font-weight: bold; }
        .log-info     { color: #8c9eff; }
        .log-sniper   { color: #00e5ff; font-weight: 700; }
        .log-scout    { color: #b388ff; font-weight: 700; }
        .log-tx       { color: #ffff00; font-weight: 700; }

        /* Streamlit Tab Styling override */
        .stTabs [data-baseweb="tab-list"] {
            gap: 24px;
            background-color: transparent;
        }
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            white-space: pre-wrap;
            background-color: transparent;
            border-radius: 8px 8px 0px 0px;
            gap: 1px;
            padding-top: 10px;
            padding-bottom: 10px;
            padding-left: 20px;
            padding-right: 20px;
            font-size: 1.1rem;
            font-weight: 600;
            color: #8c9eff;
        }
        .stTabs [data-baseweb="tab"]:hover {
            color: #ffffff;
            background-color: rgba(140, 158, 255, 0.1);
        }
        .stTabs [aria-selected="true"] {
            background-color: rgba(0, 176, 255, 0.15) !important;
            color: #00b0ff !important;
            border-bottom: 2px solid #00b0ff;
        }

        /* Section Headers */
        h2, h3 { color: #f1f2f6 !important; font-weight: 700 !important; letter-spacing: 0.02em; }
        
        hr { border-color: rgba(255,255,255,0.1); margin: 2rem 0; }
    </style>
""", unsafe_allow_html=True)


# =====================================================================
# DATA NORMALIZATION & LOADING
# =====================================================================

def normalize_dataframe(df):
    if df.empty:
        return pd.DataFrame(columns=['Address', 'Health Factor', 'Debt (USD)', 'Collateral (USD)', 'Updated'])

    rename_map = {
        'address': 'Address',
        'health_factor': 'Health Factor',
        'total_debt_usd': 'Debt (USD)',
        'total_collateral_usd': 'Collateral (USD)',
        'updated_at': 'Updated'
    }
    df = df.rename(columns=rename_map)

    if 'Health Factor' in df.columns: df['Health Factor'] = pd.to_numeric(df['Health Factor'], errors='coerce')
    if 'Debt (USD)' in df.columns: df['Debt (USD)'] = pd.to_numeric(df['Debt (USD)'], errors='coerce')
    if 'Collateral (USD)' in df.columns: df['Collateral (USD)'] = pd.to_numeric(df['Collateral (USD)'], errors='coerce')

    return df


def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except Exception:
        return None


def safe_query(query, params=None):
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception:
        conn.close()
        return pd.DataFrame()


def load_live_targets():
    df = safe_query("SELECT address, health_factor, total_debt_usd, total_collateral_usd, updated_at FROM live_targets ORDER BY health_factor ASC")
    return normalize_dataframe(df)


def load_summary():
    conn = get_db_connection()
    if not conn: return {}
    try:
        cur = conn.cursor()
        cur.execute('''
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
        r = cur.fetchone()
        conn.close()
        return {
            "total_debt": r[0], "total_collateral": r[1],
            "danger_count": r[2], "watchlist_count": r[3],
            "total_count": r[4], "danger_debt": r[5], "watchlist_debt": r[6]
        }
    except Exception:
        return {}


def load_logs(limit=300):
    return safe_query("SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT ?", (limit,))


def get_critical_logs_sync(limit=20):
    """Direct DB call matching db_manager logic to grab recent warnings/errors."""
    conn = get_db_connection()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT timestamp, level, message FROM logs WHERE upper(level) IN ('ERROR', 'WARNING') ORDER BY id DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def load_executions(limit=100):
    return safe_query("SELECT timestamp, tx_hash, user_address, profit_usdc, profit_eth FROM executions ORDER BY id DESC LIMIT ?", (limit,))

def load_total_profits():
    conn = get_db_connection()
    if not conn: return 0.0, 0.0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(profit_eth), 0), COALESCE(SUM(profit_usdc), 0) FROM executions")
        r = cur.fetchone()
        conn.close()
        return float(r[0]), float(r[1])
    except Exception:
        return 0.0, 0.0

def load_exec_count():
    conn = get_db_connection()
    if not conn: return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM executions")
        r = cur.fetchone()
        conn.close()
        return int(r[0])
    except Exception:
        return 0

# --- Arb Loaders ---

def load_arb_executions(limit=100):
    return safe_query("SELECT timestamp, tx_hash, token_pair, dex_a, dex_b, profit_usd FROM arb_executions ORDER BY id DESC LIMIT ?", (limit,))

def load_arb_spreads(limit=500):
    return safe_query("SELECT token_pair, dex_a, dex_b, spread_percent, timestamp FROM arb_spreads ORDER BY id DESC LIMIT ?", (limit,))

def load_arb_total_profit():
    conn = get_db_connection()
    if not conn: return 0.0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(profit_usd), 0) FROM arb_executions")
        r = cur.fetchone()
        conn.close()
        return float(r[0]) if r[0] else 0.0
    except Exception:
        return 0.0

def load_arb_execution_count():
    conn = get_db_connection()
    if not conn: return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM arb_executions")
        r = cur.fetchone()
        conn.close()
        return int(r[0]) if r[0] else 0
    except Exception:
        return 0

def load_active_spreads_count(minutes=1440): # Default to 24H
    conn = get_db_connection()
    if not conn: return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM arb_spreads WHERE timestamp >= datetime('now', ? || ' minutes')", (f"-{minutes}",))
        r = cur.fetchone()
        conn.close()
        return int(r[0]) if r[0] else 0
    except Exception:
        return 0

# =====================================================================
# PM2 FLEET MONITOR
# =====================================================================

def get_pm2_status():
    """Parses `pm2 jlist` to get the live status of the fleet."""
    try:
        # Require pm2 in PATH.
        output = subprocess.check_output(['pm2', 'jlist'], stderr=subprocess.DEVNULL)
        processes = json.loads(output)
        
        fleet_data = []
        for p in processes:
            name = p.get('name', 'Unknown')
            pm2_env = p.get('pm2_env', {})
            status = pm2_env.get('status', 'offline')
            restart_count = pm2_env.get('restart_time', 0)
            
            # Memory
            monit = p.get('monit', {})
            mem_mb = monit.get('memory', 0) / (1024 * 1024)
            
            # Uptime calc
            uptime_ms = int(time.time() * 1000) - pm2_env.get('pm_uptime', 0)
            if status != 'online' or pm2_env.get('pm_uptime', 0) == 0:
                uptime_str = "-"
            else:
                m, s = divmod(uptime_ms // 1000, 60)
                h, m = divmod(m, 60)
                d, h = divmod(h, 24)
                if d > 0: uptime_str = f"{d}d {h}h {m}m"
                elif h > 0: uptime_str = f"{h}h {m}m {s}s"
                else: uptime_str = f"{m}m {s}s"

            fleet_data.append({
                "Name": name,
                "Status": status,
                "Memory": f"{mem_mb:.1f} MB",
                "Restarts": restart_count,
                "Uptime": uptime_str
            })
        return fleet_data
    except Exception as e:
        return [{"Name": "PM2 Error", "Status": "offline", "Memory": "-", "Restarts": "-", "Uptime": str(e)}]

# =====================================================================
# UI LAYOUT
# =====================================================================

if HAS_AUTOREFRESH:
    st_autorefresh(interval=3000, limit=None, key="dashboard_refresh")

st.markdown(
    '<div class="header-bar">'
    '<span><i class="fas fa-satellite-dish"></i> 🛸 ANTI-GRAVITY — Executive Command Center</span>'
    '<span style="font-size: 0.9rem; color: #00e5ff;">SYSTEM: <span style="color:#00e676">ONLINE</span> &nbsp; | &nbsp; ENV: ARBITRUM</span>'
    '</div>',
    unsafe_allow_html=True
)


# Fetch Global Data
summary = load_summary()
_, liq_usdc = load_total_profits()
arb_usd = load_arb_total_profit()
total_net_profit = liq_usdc + arb_usd

liq_execs = load_exec_count()
arb_execs = load_arb_execution_count()
total_hunts = liq_execs + arb_execs

value_at_risk = summary.get('danger_debt', 0) + summary.get('watchlist_debt', 0)
active_spreads_24h = load_active_spreads_count(1440)


# ── TABS ──
tab_main, tab_liq, tab_arb, tab_term = st.tabs([
    "🌍 Main Command", "⚔️ Liquidations", "🔄 Arbitrage", "📜 Full Terminal"
])

# ─── 1. MAIN COMMAND (The Exec View) ────────────────────────────────
with tab_main:
    
    # ROW 1: THE MONEY BOARD
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown('<div class="kpi-profit">', unsafe_allow_html=True)
        st.metric("💸 Total Net Profit", f"${total_net_profit:,.2f}")
        st.markdown('</div>', unsafe_allow_html=True)
    with k2:
        st.markdown('<div class="kpi-hunts">', unsafe_allow_html=True)
        st.metric("🎯 Successful Hunts", f"{total_hunts}")
        st.markdown('</div>', unsafe_allow_html=True)
    with k3:
        st.markdown('<div class="kpi-danger">', unsafe_allow_html=True)
        st.metric("💣 Value at Risk", f"${value_at_risk:,.0f}")
        st.markdown('</div>', unsafe_allow_html=True)
    with k4:
        st.markdown('<div class="kpi-arb">', unsafe_allow_html=True)
        st.metric("📈 24H Active Spreads", f"{active_spreads_24h}")
        st.markdown('</div>', unsafe_allow_html=True)
        
    st.markdown("<hr style='border: 1px solid rgba(0,176,255,0.2); margin: 30px 0;'>", unsafe_allow_html=True)

    # ROW 2 & 3: FLEET HEALTH & ERROR BOARD
    col_fleet, col_errors = st.columns([1.5, 1])
    
    with col_fleet:
        st.markdown("<h3>🚢 Live PM2 Fleet Health</h3>", unsafe_allow_html=True)
        fleet_data = get_pm2_status()
        
        # Build HTML Table
        table_html = "<table class='fleet-table'><tr><th>Bot Engine</th><th>Status</th><th>RAM</th><th>Restarts</th><th>Uptime</th></tr>"
        for row in fleet_data:
            c_status = "status-online" if row['Status'] == 'online' else "status-offline"
            i_status = "🟢 ONLINE" if row['Status'] == 'online' else "🔴 OFFLINE"
            
            table_html += f"<tr>"
            table_html += f"<td>{row['Name']}</td>"
            table_html += f"<td><span class='{c_status}'>{i_status}</span></td>"
            table_html += f"<td>{row['Memory']}</td>"
            table_html += f"<td>{row['Restarts']}</td>"
            table_html += f"<td>{row['Uptime']}</td>"
            table_html += f"</tr>"
        table_html += "</table>"
        
        st.markdown(table_html, unsafe_allow_html=True)

    with col_errors:
        st.markdown(
            '<div class="error-board-title">'
            '⚠️ CRITICAL ALERT MONITOR'
            '</div>', 
        unsafe_allow_html=True)
        
        c_logs = get_critical_logs_sync(15)
        if c_logs:
            err_html = ""
            for r in c_logs:
                # Colorize based on severity
                color = "#ff1744" if r['level'].upper() == "ERROR" else "#ff9100"
                err_html += f"<span style='color: rgba(255,255,255,0.4); font-size: 11px;'>[{r['timestamp'][11:19]}]</span> "
                err_html += f"<strong style='color:{color}'>[{r['level'].upper()}]</strong> "
                
                # HTML escape message
                msg = str(r['message']).replace("<", "&lt;").replace(">", "&gt;")
                err_html += f"{msg}<br>"
                
            st.markdown(f'<div class="error-board">{err_html}</div>', unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="error-board" style="display:flex; justify-content:center; align-items:center;">'
                '<span style="color:#00e676; font-size:1.5rem; font-weight:bold;">✅ ALL SYSTEMS NOMINAL</span>'
                '</div>', unsafe_allow_html=True)


# ─── 2. LIQUIDATIONS (Combined View) ────────────────────────────────
with tab_liq:
    df_all = load_live_targets()
    
    liq_c1, liq_c2 = st.columns([1, 1])
    
    with liq_c1:
        st.markdown("<h3>🎯 Target Radar</h3>", unsafe_allow_html=True)
        if not df_all.empty and 'Health Factor' in df_all.columns:
            df_radar = df_all[(df_all['Health Factor'] > 0) & (df_all['Health Factor'] < 1.25)].copy()
            if not df_radar.empty:
                df_radar['Tier'] = df_radar['Health Factor'].apply(lambda hf: '🔴 Tier 1 (Danger)' if hf < 1.05 else '🟠 Tier 2 (Watchlist)')
                df_radar['Short Address'] = df_radar['Address'].apply(lambda a: f"{str(a)[:6]}...{str(a)[-4:]}")

                fig = px.scatter(
                    df_radar, x='Health Factor', y='Debt (USD)', color='Tier',
                    color_discrete_map={'🔴 Tier 1 (Danger)': '#ff1744', '🟠 Tier 2 (Watchlist)': '#ff9100'},
                    size='Debt (USD)', size_max=35, hover_data=['Short Address', 'Collateral (USD)', 'Health Factor']
                )
                fig.add_vline(x=1.0, line_dash="dash", line_color="#ff1744", annotation_text="LIQUIDATION")
                fig.update_layout(template="plotly_dark", height=400, margin=dict(l=0,r=0,t=0,b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("📡 No targets in visual range (HF < 1.25).")
        else:
            st.info("🔍 No live target data available.")

        st.markdown("<h3>💣 Danger Zone (Tier 1)</h3>", unsafe_allow_html=True)
        if not df_all.empty:
            df_t1 = df_all[(df_all['Health Factor'] > 0) & (df_all['Health Factor'] < 1.05)].copy()
            if not df_t1.empty:
                st.dataframe(df_t1, hide_index=True, height=250, use_container_width=True,
                    column_config={
                        "Debt (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Collateral (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Health Factor": st.column_config.NumberColumn(format="%.4f")
                    })
            else:
                st.success("✅ Clear.")

    with liq_c2:
        st.markdown("<h3>⚔️ Liquidation History</h3>", unsafe_allow_html=True)
        df_exec = load_executions(100)
        if not df_exec.empty:
            st.dataframe(df_exec, hide_index=True, height=400, use_container_width=True,
                column_config={
                    "profit_usdc": st.column_config.NumberColumn("Profit (USD)", format="$%.2f"),
                    "profit_eth": st.column_config.NumberColumn("Profit (ETH)", format="%.4f"),
                })
        else:
            st.info("🏹 No liquidations yet.")
            
        st.markdown("<h3>🟠 Watchlist (Tier 2)</h3>", unsafe_allow_html=True)
        if not df_all.empty:
            df_t2 = df_all[(df_all['Health Factor'] >= 1.05) & (df_all['Health Factor'] < 1.20)].copy()
            if not df_t2.empty:
                st.dataframe(df_t2, hide_index=True, height=250, use_container_width=True,
                    column_config={
                        "Debt (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Collateral (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Health Factor": st.column_config.NumberColumn(format="%.4f")
                    })
            else:
                st.info("📋 Empty.")


# ─── 3. ARBITRAGE ────────────────────────────────
with tab_arb:
    st.markdown("<h3>📈 Live DEX Spreads (Last 300)</h3>", unsafe_allow_html=True)
    df_spreads = load_arb_spreads(300)

    if not df_spreads.empty:
        df_spreads['timestamp'] = pd.to_datetime(df_spreads['timestamp'], errors='coerce')
        df_spreads = df_spreads.dropna(subset=['timestamp']).sort_values('timestamp', ascending=True)

        fig_spread = px.line(
            df_spreads, x='timestamp', y='spread_percent', color='token_pair',
            color_discrete_sequence=['#00b0ff', '#ff1744', '#00e676', '#ff9100', '#d500f9', '#ffea00'],
        )
        fig_spread.add_hline(y=0.08, line_dash="dot", line_color="rgba(0, 230, 118, 0.6)", annotation_text="Profit Threshold")
        fig_spread.update_layout(
            template="plotly_dark", height=450, margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0.2)"
        )
        st.plotly_chart(fig_spread, use_container_width=True)
    else:
        st.info("📊 No spread data yet.")

    st.markdown("<h3>📋 Arbitrage Executions</h3>", unsafe_allow_html=True)
    df_arb_exec = load_arb_executions(100)
    if not df_arb_exec.empty:
        st.dataframe(
            df_arb_exec, hide_index=True, use_container_width=True, height=400,
            column_config={
                "profit_usd": st.column_config.NumberColumn("Profit (USD)", format="$%.2f"),
                "timestamp": st.column_config.TextColumn("Time")
            }
        )
    else:
        st.info("🔄 No arbitrage executions recorded yet.")


# ─── 4. FULL TERMINAL ────────────────────────────────
with tab_term:
    st.caption("Complete combined system logs | Auto-refreshes every 3s")
    df_logs = load_logs(400)

    if not df_logs.empty:
        log_html = ""
        for _, row in df_logs.iterrows():
            msg = str(row['message']).replace("<", "&lt;").replace(">", "&gt;")
            
            # Syntax Highlighting
            if "[SNIPER]" in msg: msg = msg.replace("[SNIPER]", '<span class="log-sniper">[SNIPER]</span>')
            elif "[SCOUT]" in msg: msg = msg.replace("[SCOUT]", '<span class="log-scout">[SCOUT]</span>')
            if "PROMOTED" in msg.upper(): msg = re.sub(r'(PROMOTED)', r'<span style="color:#ffea00;font-weight:bold;">\1</span>', msg, flags=re.IGNORECASE)
            if "TX SENT" in msg.upper() or "TX CONFIRMED" in msg:
                msg = re.sub(r'(TX SENT|TX CONFIRMED)', r'<span class="log-tx">\1</span>', msg, flags=re.IGNORECASE)

            # Level Coloring
            lev = str(row['level']).lower()
            lvl_class = "log-info"
            if lev == "error": lvl_class = "log-error"
            elif lev == "warning": lvl_class = "log-warning"
            elif lev == "success": lvl_class = "log-success"

            ts = row["timestamp"][11:19] if len(row["timestamp"]) > 18 else str(row["timestamp"])

            log_html += f'<span class="log-time">[{ts}]</span> <span class="{lvl_class}">{msg}</span><br>'

        st.markdown(f'<div class="terminal-log">{log_html}</div>', unsafe_allow_html=True)
    else:
        st.info("📜 No logs available.")
