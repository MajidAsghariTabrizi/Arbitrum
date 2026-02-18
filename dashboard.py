"""
═══════════════════════════════════════════════════════════════════════════════
🛸 ANTI-GRAVITY — Mission Control Dashboard (Streamlit)
═══════════════════════════════════════════════════════════════════════════════
Unified dashboard for all Anti-Gravity business lines:
  Tab 1: 📡 Radar          — Health Factor vs Debt scatter plot
  Tab 2: 🔴 Danger Zone    — Tier 1 targets (HF < 1.05)
  Tab 3: 🟠 Watchlist      — Tier 2 targets (1.05 ≤ HF < 1.20)
  Tab 4: ⚔️ Executions     — Liquidation history
  Tab 5: 🔄 DEX Arbitrage  — Spread monitoring, live charts, arb executions
  Tab 6: 📜 Live Terminal  — System log viewer

Uses width="stretch" for Streamlit dataframes (deprecation-safe).
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
from datetime import datetime

# Try to import st_autorefresh for clean auto-refresh
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

# --- Configuration ---
st.set_page_config(
    page_title="⚡ Anti-Gravity — Mission Control",
    page_icon="🛸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# DB Path
DB_FILE = "mission_control.db"

# =====================================================================
# CUSTOM CSS — Professional Dark-Mode Quant Trading UI
# =====================================================================
st.markdown("""
    <style>
        /* Global spacing */
        .block-container { padding-top: 0.8rem; padding-bottom: 1rem; }

        /* Metric Cards */
        div[data-testid="stMetricValue"] {
            font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', monospace;
            font-size: 1.5rem;
            font-weight: 700;
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            opacity: 0.75;
        }

        /* KPI Card Highlights */
        .kpi-danger div[data-testid="stMetricValue"] { color: #ff4757 !important; }
        .kpi-warning div[data-testid="stMetricValue"] { color: #ffa502 !important; }
        .kpi-safe div[data-testid="stMetricValue"] { color: #2ed573 !important; }
        .kpi-info div[data-testid="stMetricValue"] { color: #70a1ff !important; }
        .kpi-profit div[data-testid="stMetricValue"] { color: #7bed9f !important; }
        .kpi-arb div[data-testid="stMetricValue"] { color: #1e90ff !important; }

        /* Header Bar */
        .header-bar {
            background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
            padding: 14px 24px;
            border-radius: 10px;
            margin-bottom: 1rem;
            color: #f1f2f6;
            font-size: 1.15rem;
            font-weight: 700;
            letter-spacing: 0.03em;
            border: 1px solid rgba(102, 126, 234, 0.25);
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }

        /* Terminal Log Container */
        .terminal-log {
            background-color: #080810;
            color: #c8d6e5;
            font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', monospace;
            padding: 16px;
            border-radius: 10px;
            height: 500px;
            overflow-y: auto;
            font-size: 11px;
            border: 1px solid #1a1a2e;
            line-height: 1.7;
            box-shadow: inset 0 2px 8px rgba(0,0,0,0.5);
        }

        /* Terminal Keyword Highlights */
        .log-error    { color: #ff4757; font-weight: 600; }
        .log-warning  { color: #ffa502; }
        .log-success  { color: #2ed573; font-weight: 600; }
        .log-info     { color: #70a1ff; }
        .log-sniper   { color: #00d2d3; font-weight: 700; }
        .log-scout    { color: #54a0ff; font-weight: 700; }
        .log-promoted { color: #feca57; font-weight: 700; text-decoration: underline; }
        .log-preflight{ color: #c56cf0; font-weight: 600; }
        .log-tx       { color: #ff6348; font-weight: 700; }

        /* Tier Badges */
        .tier-1-badge {
            display: inline-block;
            background: linear-gradient(135deg, #ff4757, #c0392b);
            color: white;
            padding: 2px 10px;
            border-radius: 12px;
            font-weight: 700;
            font-size: 0.75rem;
            letter-spacing: 0.04em;
        }
        .tier-2-badge {
            display: inline-block;
            background: linear-gradient(135deg, #ffa502, #e67e22);
            color: white;
            padding: 2px 10px;
            border-radius: 12px;
            font-weight: 700;
            font-size: 0.75rem;
            letter-spacing: 0.04em;
        }
        .arb-badge {
            display: inline-block;
            background: linear-gradient(135deg, #1e90ff, #0052cc);
            color: white;
            padding: 2px 10px;
            border-radius: 12px;
            font-weight: 700;
            font-size: 0.75rem;
            letter-spacing: 0.04em;
        }
    </style>
""", unsafe_allow_html=True)


# =====================================================================
# DATA NORMALIZATION & LOADING
# =====================================================================

def normalize_dataframe(df):
    """
    Intelligently renames and standardizes dataframe columns.
    Ensures safe types for Plotly and Streamlit.
    """
    if df.empty:
        return pd.DataFrame(columns=['Address', 'Health Factor', 'Debt (USD)', 'Collateral (USD)', 'Updated'])

    rename_map = {
        'address': 'Address',
        'health_factor': 'Health Factor',
        'healthFactor': 'Health Factor',
        'hf': 'Health Factor',
        'total_debt_usd': 'Debt (USD)',
        'totalDebtBase': 'Debt (USD)',
        'debtToCover': 'Debt (USD)',
        'total_collateral_usd': 'Collateral (USD)',
        'totalCollateralBase': 'Collateral (USD)',
        'updated_at': 'Updated'
    }
    df = df.rename(columns=rename_map)

    if 'Health Factor' in df.columns:
        df['Health Factor'] = pd.to_numeric(df['Health Factor'], errors='coerce')
    if 'Debt (USD)' in df.columns:
        df['Debt (USD)'] = pd.to_numeric(df['Debt (USD)'], errors='coerce')
    if 'Collateral (USD)' in df.columns:
        df['Collateral (USD)'] = pd.to_numeric(df['Collateral (USD)'], errors='coerce')

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
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception:
        conn.close()
        return pd.DataFrame()


def load_live_targets():
    df = safe_query(
        "SELECT address, health_factor, total_debt_usd, total_collateral_usd, updated_at "
        "FROM live_targets ORDER BY health_factor ASC"
    )
    return normalize_dataframe(df)


def load_summary():
    conn = get_db_connection()
    if not conn:
        return {}
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


def load_metrics(limit=100):
    try:
        return safe_query(
            "SELECT block_number, target_count, tier_1_count, tier_2_count, scan_time_ms, timestamp "
            "FROM system_metrics ORDER BY id DESC LIMIT ?", (limit,)
        )
    except Exception:
        return safe_query(
            "SELECT block_number, target_count, 0 as tier_1_count, 0 as tier_2_count, scan_time_ms, timestamp "
            "FROM system_metrics ORDER BY id DESC LIMIT ?", (limit,)
        )


def load_logs(limit=200):
    return safe_query("SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT ?", (limit,))


def load_executions(limit=50):
    return safe_query(
        "SELECT timestamp, tx_hash, user_address, profit_usdc, profit_eth "
        "FROM executions ORDER BY id DESC LIMIT ?", (limit,)
    )


def load_total_profits():
    conn = get_db_connection()
    if not conn:
        return 0.0, 0.0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(profit_eth), 0), COALESCE(SUM(profit_usdc), 0) FROM executions")
        r = cur.fetchone()
        conn.close()
        return r[0], r[1]
    except Exception:
        return 0.0, 0.0


def load_targets_json():
    paths = ["/root/Arbitrum/targets.json", "targets.json"]
    for path in paths:
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    content = f.read().strip()
                    if not content:
                        return [], []
                    data = json.loads(content)
                    if isinstance(data, dict):
                        return data.get("tier_1_danger", []), data.get("tier_2_watchlist", [])
                    elif isinstance(data, list):
                        return data, []
            except Exception:
                pass
    return [], []


def load_logs_fallback(limit=100):
    """Reads actual log files if DB logs are empty."""
    log_paths = [
        os.path.expanduser("~/.pm2/logs/ArbitrumBot-out.log"),
        os.path.expanduser("~/.pm2/logs/ArbitrumBot-error.log"),
        "bot.log"
    ]
    lines = []
    for path in log_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r', errors='ignore') as f:
                    all_lines = f.readlines()
                    lines.extend(all_lines[-limit:])
            except Exception:
                continue

    processed = []
    for line in lines[-limit:]:
        processed.append({
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'level': 'INFO',
            'message': line.strip()
        })
    return pd.DataFrame(processed) if processed else pd.DataFrame()


# =====================================================================
# ARBITRAGE DATA LOADERS
# =====================================================================

def load_arb_executions(limit=50):
    """Load recent arbitrage executions for the data grid."""
    return safe_query(
        "SELECT timestamp, tx_hash, token_pair, dex_a, dex_b, profit_usd "
        "FROM arb_executions ORDER BY id DESC LIMIT ?", (limit,)
    )


def load_arb_spreads(limit=300):
    """Load recent spreads for the live chart."""
    return safe_query(
        "SELECT token_pair, dex_a, dex_b, spread_percent, timestamp "
        "FROM arb_spreads ORDER BY id DESC LIMIT ?", (limit,)
    )


def load_arb_total_profit():
    """Load total arb profit in USD."""
    conn = get_db_connection()
    if not conn:
        return 0.0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(profit_usd), 0) FROM arb_executions")
        r = cur.fetchone()
        conn.close()
        return r[0] if r[0] else 0.0
    except Exception:
        return 0.0


def load_arb_execution_count():
    """Total arb executions."""
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM arb_executions")
        r = cur.fetchone()
        conn.close()
        return r[0] if r[0] else 0
    except Exception:
        return 0


def load_active_spreads_count(minutes=60):
    """Count of spreads found in the last N minutes."""
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT COUNT(*) FROM arb_spreads
            WHERE timestamp >= datetime('now', ? || ' minutes')
        ''', (f"-{minutes}",))
        r = cur.fetchone()
        conn.close()
        return r[0] if r[0] else 0
    except Exception:
        return 0


# =====================================================================
# TERMINAL HIGHLIGHTER
# =====================================================================

def highlight_log_line(message, level):
    msg = str(message).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    if "[SNIPER]" in msg:
        msg = msg.replace("[SNIPER]", '<span class="log-sniper">[SNIPER]</span>')
    if "[SCOUT]" in msg:
        msg = msg.replace("[SCOUT]", '<span class="log-scout">[SCOUT]</span>')
    if "PROMOTED" in msg.upper():
        msg = re.sub(r'(PROMOTED)', r'<span class="log-promoted">\1</span>', msg, flags=re.IGNORECASE)
    if any(x in msg.upper() for x in ["PRE-FLIGHT", "SIMULATION"]):
        msg = re.sub(r'(Pre-flight|PRE-FLIGHT|Simulation|simulate)', r'<span class="log-preflight">\1</span>', msg, flags=re.IGNORECASE)
    if any(x in msg.upper() for x in ["TX SENT", "TX CONFIRMED", "TX REVERTED"]):
        msg = re.sub(r'(TX SENT|TX CONFIRMED|TX REVERTED)', r'<span class="log-tx">\1</span>', msg, flags=re.IGNORECASE)

    level_lower = str(level).lower()
    css_class = {
        'error': 'log-error', 'warning': 'log-warning',
        'success': 'log-success'
    }.get(level_lower, 'log-info')

    return f'<span class="{css_class}">{msg}</span>'


# =====================================================================
# UI LAYOUT
# =====================================================================

if HAS_AUTOREFRESH:
    st_autorefresh(interval=5000, limit=None, key="dashboard_refresh")

st.markdown(
    '<div class="header-bar">'
    '🛸 ANTI-GRAVITY — Mission Control &nbsp;|&nbsp; '
    '<span style="font-size: 0.8rem; opacity: 0.7;">MEV Platform v2.0 • Liquidations + DEX Arb • Arbitrum One</span>'
    '</div>',
    unsafe_allow_html=True
)

# Sidebar
with st.sidebar:
    st.title("🛸 Anti-Gravity")
    st.divider()
    st.metric("System Status", "ONLINE", delta="Active", delta_color="normal")
    total_eth, total_usdc = load_total_profits()
    st.metric("Liquidation Profit (USDC)", f"${total_usdc:,.2f}")
    arb_profit = load_arb_total_profit()
    st.metric("Arb Profit (USD)", f"${arb_profit:,.2f}")
    st.divider()
    t1_json, t2_json = load_targets_json()
    st.markdown(f'<span class="tier-1-badge">TIER 1</span> &nbsp; **{len(t1_json)}** targets', unsafe_allow_html=True)
    st.markdown(f'<span class="tier-2-badge">TIER 2</span> &nbsp; **{len(t2_json)}** targets', unsafe_allow_html=True)
    st.markdown(f'<span class="arb-badge">ARB ENGINE</span> &nbsp; **{load_arb_execution_count()}** trades', unsafe_allow_html=True)
    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

# KPIs
summary = load_summary()
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.markdown('<div class="kpi-info">', unsafe_allow_html=True)
    st.metric("📡 Total Live Targets", f"{summary.get('total_count', 0)}")
    st.markdown('</div>', unsafe_allow_html=True)
with k2:
    st.markdown('<div class="kpi-danger">', unsafe_allow_html=True)
    st.metric("🔴 Tier 1 (Danger)", f"{summary.get('danger_count', 0)}")
    st.markdown('</div>', unsafe_allow_html=True)
with k3:
    st.markdown('<div class="kpi-warning">', unsafe_allow_html=True)
    st.metric("🟠 Tier 2 (Watchlist)", f"{summary.get('watchlist_count', 0)}")
    st.markdown('</div>', unsafe_allow_html=True)
with k4:
    total_risk = summary.get('danger_debt', 0) + summary.get('watchlist_debt', 0)
    st.markdown('<div class="kpi-danger">', unsafe_allow_html=True)
    st.metric("💰 Value at Risk", f"${total_risk:,.0f}")
    st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ── TABS ──
tab_radar, tab_danger, tab_watch, tab_exec, tab_arb, tab_term = st.tabs([
    "📡 Radar", "🔴 Danger Zone", "🟠 Watchlist", "⚔️ Executions", "🔄 DEX Arbitrage", "📜 Live Terminal"
])

# ─── RADAR ────────────────────────────────
with tab_radar:
    st.subheader("🎯 Target Radar — Health Factor vs Debt")
    df_all = load_live_targets()

    if not df_all.empty and 'Health Factor' in df_all.columns:
        df_radar = df_all[(df_all['Health Factor'] > 0) & (df_all['Health Factor'] < 1.25)].copy()

        if not df_radar.empty:
            df_radar['Tier'] = df_radar['Health Factor'].apply(
                lambda hf: '🔴 Tier 1 (Danger)' if hf < 1.05 else '🟠 Tier 2 (Watchlist)'
            )
            df_radar['Short Address'] = df_radar['Address'].apply(
                lambda a: f"{str(a)[:6]}...{str(a)[-4:]}"
            )

            fig = px.scatter(
                df_radar, x='Health Factor', y='Debt (USD)',
                color='Tier',
                color_discrete_map={'🔴 Tier 1 (Danger)': '#ff4757', '🟠 Tier 2 (Watchlist)': '#ffa502'},
                size='Debt (USD)', size_max=40,
                hover_data=['Short Address', 'Collateral (USD)'],
                labels={'Health Factor': 'HF'}
            )

            fig.add_vline(x=1.0, line_dash="dash", line_color="#ff4757", annotation_text="LIQUIDATION")
            fig.update_layout(
                template="plotly_dark", height=500,
                xaxis=dict(range=[0.95, 1.25], gridcolor='rgba(255,255,255,0.05)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
                font=dict(family="JetBrains Mono"),
                legend=dict(orientation="h", y=1.02, x=1)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📡 No targets in visual range (0 < HF < 1.25).")
    else:
        st.info("🔍 No live target data available.")

# ─── DANGER ZONE ──────────────────────────
with tab_danger:
    df_all = load_live_targets()
    if not df_all.empty and 'Health Factor' in df_all.columns:
        df_t1 = df_all[(df_all['Health Factor'] > 0) & (df_all['Health Factor'] < 1.05)].copy()
        if not df_t1.empty:
            c1, c2 = st.columns([1, 2])
            with c1:
                fig = px.histogram(df_t1, x='Health Factor', nbins=20, color_discrete_sequence=['#ff4757'])
                fig.update_layout(template="plotly_dark", height=350, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.dataframe(
                    df_t1, height=350, hide_index=True,
                    column_config={
                        "Debt (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Collateral (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Health Factor": st.column_config.NumberColumn(format="%.4f"),
                    },
                    use_container_width=True,
                )
        else:
            st.success("✅ No Tier 1 targets.")
    else:
        st.info("🔍 No data.")

# ─── WATCHLIST ────────────────────────────
with tab_watch:
    df_all = load_live_targets()
    if not df_all.empty and 'Health Factor' in df_all.columns:
        df_t2 = df_all[(df_all['Health Factor'] >= 1.05) & (df_all['Health Factor'] < 1.20)].copy()
        if not df_t2.empty:
            c1, c2 = st.columns([1, 2])
            with c1:
                fig = px.histogram(df_t2, x='Health Factor', nbins=20, color_discrete_sequence=['#ffa502'])
                fig.update_layout(template="plotly_dark", height=350, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.dataframe(
                    df_t2, height=350, hide_index=True,
                    column_config={
                        "Debt (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Collateral (USD)": st.column_config.NumberColumn(format="$%.2f"),
                        "Health Factor": st.column_config.NumberColumn(format="%.4f"),
                    },
                    use_container_width=True,
                )
        else:
            st.info("📋 Watchlist empty.")
    else:
        st.info("🔍 No data.")

# ─── EXECUTIONS ───────────────────────────
with tab_exec:
    df_exec = load_executions()
    if not df_exec.empty:
        st.dataframe(
            df_exec, hide_index=True,
            column_config={
                "profit_usdc": st.column_config.NumberColumn("Profit (USD)", format="$%.2f"),
                "profit_eth": st.column_config.NumberColumn("Profit (ETH)", format="%.4f"),
            },
            use_container_width=True,
        )
    else:
        st.info("🏹 No liquidations yet.")

# ─── DEX ARBITRAGE ────────────────────────
with tab_arb:
    st.subheader("🔄 DEX Arbitrage — Cross-Exchange Spread Monitor")

    # ── Metrics Row ──
    arb_k1, arb_k2, arb_k3 = st.columns(3)
    with arb_k1:
        st.markdown('<div class="kpi-profit">', unsafe_allow_html=True)
        arb_total = load_arb_total_profit()
        st.metric("💰 Total Arb Profit (USD)", f"${arb_total:,.2f}")
        st.markdown('</div>', unsafe_allow_html=True)
    with arb_k2:
        st.markdown('<div class="kpi-arb">', unsafe_allow_html=True)
        active_count = load_active_spreads_count(60)
        st.metric("📊 Active Spreads (Last 1H)", f"{active_count}")
        st.markdown('</div>', unsafe_allow_html=True)
    with arb_k3:
        st.markdown('<div class="kpi-safe">', unsafe_allow_html=True)
        exec_count = load_arb_execution_count()
        st.metric("✅ Successful Executions", f"{exec_count}")
        st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    # ── Live Spread Chart ──
    st.subheader("📈 Live Spread % Over Time")
    df_spreads = load_arb_spreads(300)

    if not df_spreads.empty:
        # Ensure timestamp is datetime for proper plotting
        df_spreads['timestamp'] = pd.to_datetime(df_spreads['timestamp'], errors='coerce')
        df_spreads = df_spreads.dropna(subset=['timestamp'])
        df_spreads = df_spreads.sort_values('timestamp', ascending=True)

        # Create label column for legend
        df_spreads['pair_route'] = df_spreads['token_pair'] + " (" + df_spreads['dex_a'] + " → " + df_spreads['dex_b'] + ")"

        fig_spread = px.line(
            df_spreads,
            x='timestamp',
            y='spread_percent',
            color='token_pair',
            labels={'spread_percent': 'Spread %', 'timestamp': 'Time'},
            color_discrete_sequence=[
                '#1e90ff', '#ff4757', '#2ed573', '#ffa502',
                '#c56cf0', '#00d2d3', '#feca57',
            ],
        )

        fig_spread.update_layout(
            template="plotly_dark",
            height=400,
            xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
            yaxis=dict(
                gridcolor='rgba(255,255,255,0.05)',
                title="Spread %",
            ),
            font=dict(family="JetBrains Mono"),
            legend=dict(orientation="h", y=-0.15, x=0, font=dict(size=10)),
            margin=dict(l=40, r=20, t=30, b=60),
        )

        # Add profitability threshold line
        fig_spread.add_hline(
            y=0.08,
            line_dash="dot",
            line_color="rgba(46, 213, 115, 0.5)",
            annotation_text="Profit Threshold",
            annotation_font_color="rgba(46, 213, 115, 0.7)",
        )

        st.plotly_chart(fig_spread, use_container_width=True)
    else:
        st.info("📊 No spread data yet. Start the arb_engine.py to begin scanning.")

    st.divider()

    # ── Historical Arb Executions Grid ──
    st.subheader("📋 Arbitrage Execution History")
    df_arb_exec = load_arb_executions(50)

    if not df_arb_exec.empty:
        st.dataframe(
            df_arb_exec,
            hide_index=True,
            column_config={
                "timestamp": st.column_config.TextColumn("Timestamp"),
                "tx_hash": st.column_config.TextColumn("TX Hash"),
                "token_pair": st.column_config.TextColumn("Pair"),
                "dex_a": st.column_config.TextColumn("Buy DEX"),
                "dex_b": st.column_config.TextColumn("Sell DEX"),
                "profit_usd": st.column_config.NumberColumn("Profit (USD)", format="$%.2f"),
            },
            use_container_width=True,
        )
    else:
        st.info("🔄 No arbitrage executions recorded yet.")


# ─── TERMINAL ─────────────────────────────
with tab_term:
    st.caption("Highlights: [SNIPER] [SCOUT] PROMOTED Pre-flight TX SENT")
    df_logs = load_logs(200)

    # Fallback if DB empty
    if df_logs.empty:
        df_logs = load_logs_fallback(50)

    if not df_logs.empty:
        lines = []
        for _, row in df_logs.iterrows():
            msg = highlight_log_line(row['message'], row['level'])
            lines.append(f'<span style="opacity:0.5; font-size:10px">[{row["timestamp"]}]</span> {msg}')
        st.markdown(f'<div class="terminal-log">{"<br>".join(lines)}</div>', unsafe_allow_html=True)
    else:
        st.info("📜 No logs available.")

# ─── ANALYTICS ─────────────────────────────
with st.expander("📊 Scan Performance", expanded=False):
    df_m = load_metrics(100)
    if not df_m.empty:
        df_m = df_m.iloc[::-1]
        c1, c2 = st.columns(2)
        with c1:
            fig = px.line(df_m, x='block_number', y='scan_time_ms', labels={'scan_time_ms': 'ms'})
            fig.update_traces(line_color='#667eea')
            fig.update_layout(template="plotly_dark", height=300)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = go.Figure(data=[
                go.Bar(name='Tier 1', x=df_m['block_number'], y=df_m['tier_1_count'], marker_color='#ff4757'),
                go.Bar(name='Tier 2', x=df_m['block_number'], y=df_m['tier_2_count'], marker_color='#ffa502')
            ])
            fig.update_layout(barmode='stack', template="plotly_dark", height=300)
            st.plotly_chart(fig, use_container_width=True)
