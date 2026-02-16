import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import json
import time
import os
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
# CUSTOM CSS — Professional Dark-Mode Quant Trading Dashboard
# =====================================================================
st.markdown("""
    <style>
        .block-container { padding-top: 1rem; padding-bottom: 1rem; }

        div[data-testid="stMetricValue"] {
            font-family: 'JetBrains Mono', 'Courier New', monospace;
            font-size: 1.6rem;
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            opacity: 0.8;
        }

        .terminal-log {
            background-color: #0a0a0a;
            color: #00ff88;
            font-family: 'JetBrains Mono', 'Courier New', monospace;
            padding: 15px;
            border-radius: 8px;
            height: 450px;
            overflow-y: auto;
            font-size: 11.5px;
            border: 1px solid #1a1a2e;
            line-height: 1.6;
        }
        .terminal-log .log-error { color: #ff4757; }
        .terminal-log .log-warning { color: #ffa502; }
        .terminal-log .log-success { color: #2ed573; }
        .terminal-log .log-info { color: #70a1ff; }

        .header-bar {
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            padding: 12px 20px;
            border-radius: 8px;
            margin-bottom: 1.2rem;
            color: white;
            font-size: 1.1rem;
            font-weight: 600;
            letter-spacing: 0.02em;
        }

        .tier-badge-1 {
            background: linear-gradient(135deg, #ff4757, #c0392b);
            color: white;
            padding: 3px 10px;
            border-radius: 12px;
            font-weight: 700;
            font-size: 0.8rem;
        }
        .tier-badge-2 {
            background: linear-gradient(135deg, #ffa502, #e67e22);
            color: white;
            padding: 3px 10px;
            border-radius: 12px;
            font-weight: 700;
            font-size: 0.8rem;
        }
    </style>
""", unsafe_allow_html=True)


# =====================================================================
# DATABASE HELPERS
# =====================================================================

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except Exception as e:
        st.error(f"DB Connection Error: {e}")
        return None


def load_live_targets():
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                "SELECT address, health_factor, total_debt_usd, total_collateral_usd, updated_at "
                "FROM live_targets ORDER BY health_factor ASC",
                conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()


def load_summary():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('''
                SELECT
                    COALESCE(SUM(total_debt_usd), 0),
                    COALESCE(SUM(total_collateral_usd), 0),
                    COUNT(CASE WHEN health_factor < 1.05 AND health_factor > 0 THEN 1 END),
                    COUNT(*)
                FROM live_targets
            ''')
            res = cur.fetchone()
            conn.close()
            return {
                "total_debt": res[0], "total_collateral": res[1],
                "danger_count": res[2], "total_count": res[3]
            }
        except Exception:
            conn.close()
    return {"total_debt": 0, "total_collateral": 0, "danger_count": 0, "total_count": 0}


def load_metrics(limit=100):
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                f"SELECT block_number, target_count, scan_time_ms, timestamp "
                f"FROM system_metrics ORDER BY id DESC LIMIT {limit}", conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()


def load_avg_scan_time(limit=100):
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COALESCE(AVG(scan_time_ms), 0) "
                "FROM (SELECT scan_time_ms FROM system_metrics ORDER BY id DESC LIMIT ?)",
                (limit,))
            res = cur.fetchone()
            conn.close()
            return res[0]
        except Exception:
            conn.close()
    return 0.0


def load_logs(limit=100):
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                f"SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT {limit}", conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()


def load_executions(limit=50):
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                f"SELECT timestamp, tx_hash, user_address, profit_usdc, profit_eth "
                f"FROM executions ORDER BY id DESC LIMIT {limit}", conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()


def load_total_profits():
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


def load_targets_json():
    """Read structured targets.json for tier visualization."""
    paths = ["/root/Arbitrum/targets.json", "targets.json"]
    for path in paths:
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data.get("tier_1_danger", []), data.get("tier_2_watchlist", [])
                    elif isinstance(data, list):
                        return data, []  # Legacy flat format
            except Exception:
                pass
    return [], []


# =====================================================================
# AUTO-REFRESH
# =====================================================================

if HAS_AUTOREFRESH:
    st_autorefresh(interval=5000, limit=None, key="dashboard_refresh")


# =====================================================================
# HEADER
# =====================================================================

st.markdown(
    '<div class="header-bar">🛸 ANTI-GRAVITY — Mission Control Dashboard</div>',
    unsafe_allow_html=True
)


# =====================================================================
# SIDEBAR
# =====================================================================

with st.sidebar:
    st.title("🛸 Anti-Gravity")
    st.caption("MEV Sniper v2.0 — Arbitrum")
    st.divider()

    st.metric("System Status", "ONLINE", delta="Active", delta_color="normal")

    total_eth, total_usdc = load_total_profits()
    st.metric("Total Profit (USDC)", f"${total_usdc:,.2f}")
    st.metric("Total Profit (ETH)", f"Ξ {total_eth:.4f}")

    st.divider()

    # Tier overview from JSON
    t1_json, t2_json = load_targets_json()
    st.metric("🔴 Tier 1 (JSON)", f"{len(t1_json)}")
    st.metric("🟠 Tier 2 (JSON)", f"{len(t2_json)}")

    st.divider()

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

    st.caption(f"Last Update: {datetime.now().strftime('%H:%M:%S')}")
    if not HAS_AUTOREFRESH:
        st.caption("💡 Install `streamlit-autorefresh` for auto-refresh")


# =====================================================================
# TOP KPIs
# =====================================================================

summary = load_summary()
avg_scan = load_avg_scan_time()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric("💰 Monitored Debt", f"${summary['total_debt']:,.2f}")
with kpi2:
    st.metric("🏦 Monitored Collateral", f"${summary['total_collateral']:,.2f}")
with kpi3:
    danger = summary['danger_count']
    st.metric("⚠️ Users in Danger (HF < 1.05)", f"{danger}",
              delta=f"-{danger}" if danger > 0 else None,
              delta_color="inverse" if danger > 0 else "off")
with kpi4:
    st.metric("⚡ Avg Scan Speed", f"{avg_scan:.0f} ms")

st.divider()


# =====================================================================
# TABS
# =====================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "🎯 Live Targets (Tiered)",
    "📊 Performance Analytics",
    "⚔️ Battle Log",
    "📜 System Terminal"
])


# ─── TAB 1: Live Targets (Tiered) ────────────────────────────────────

with tab1:
    df_targets = load_live_targets()

    if not df_targets.empty:
        # Split into tiers based on HF from database
        df_t1 = df_targets[(df_targets['health_factor'] > 0) & (df_targets['health_factor'] < 1.05)].copy()
        df_t2 = df_targets[(df_targets['health_factor'] >= 1.05) & (df_targets['health_factor'] < 1.20)].copy()

        # --- Tier 1: Danger Zone ---
        st.markdown("### 🔴 Tier 1 — Danger Zone (HF < 1.05)")
        t1_col1, t1_col2 = st.columns([1, 2])

        with t1_col1:
            # HF distribution for Tier 1
            if not df_t1.empty:
                fig_t1 = px.histogram(
                    df_t1, x='health_factor', nbins=25,
                    color_discrete_sequence=['#ff4757'],
                    labels={'health_factor': 'Health Factor'}
                )
                fig_t1.add_vline(x=1.0, line_dash="dash", line_color="#fff",
                                line_width=2, annotation_text="Liquidation",
                                annotation_font=dict(color="#fff", size=10))
                fig_t1.update_layout(
                    template="plotly_dark", height=300,
                    margin=dict(l=20, r=20, t=30, b=40),
                    xaxis_title="HF", yaxis_title="# Users",
                    font=dict(family="JetBrains Mono, monospace")
                )
                st.plotly_chart(fig_t1, use_container_width=True)
            else:
                st.info("No Tier 1 targets currently.")

        with t1_col2:
            if not df_t1.empty:
                disp = df_t1.copy()
                disp.columns = ['Address', 'HF', 'Debt (USD)', 'Collateral (USD)', 'Updated']
                disp['Debt (USD)'] = disp['Debt (USD)'].apply(lambda x: f"${x:,.2f}")
                disp['Collateral (USD)'] = disp['Collateral (USD)'].apply(lambda x: f"${x:,.2f}")
                disp['HF'] = disp['HF'].apply(lambda x: f"{x:.4f}")
                disp['Address'] = disp['Address'].apply(lambda x: f"{x[:6]}...{x[-4:]}" if len(str(x)) > 10 else x)
                st.dataframe(disp, use_container_width=True, hide_index=True, height=300)
            else:
                st.info("No Tier 1 targets in database.")

        st.divider()

        # --- Tier 2: Watchlist ---
        st.markdown("### 🟠 Tier 2 — Watchlist (HF 1.05 – 1.20)")
        t2_col1, t2_col2 = st.columns([1, 2])

        with t2_col1:
            if not df_t2.empty:
                fig_t2 = px.histogram(
                    df_t2, x='health_factor', nbins=25,
                    color_discrete_sequence=['#ffa502'],
                    labels={'health_factor': 'Health Factor'}
                )
                fig_t2.add_vline(x=1.05, line_dash="dot", line_color="#fff",
                                line_width=1.5, annotation_text="Tier 1 Threshold",
                                annotation_font=dict(color="#fff", size=10))
                fig_t2.update_layout(
                    template="plotly_dark", height=300,
                    margin=dict(l=20, r=20, t=30, b=40),
                    xaxis_title="HF", yaxis_title="# Users",
                    font=dict(family="JetBrains Mono, monospace")
                )
                st.plotly_chart(fig_t2, use_container_width=True)
            else:
                st.info("No Tier 2 targets currently.")

        with t2_col2:
            if not df_t2.empty:
                disp2 = df_t2.copy()
                disp2.columns = ['Address', 'HF', 'Debt (USD)', 'Collateral (USD)', 'Updated']
                disp2['Debt (USD)'] = disp2['Debt (USD)'].apply(lambda x: f"${x:,.2f}")
                disp2['Collateral (USD)'] = disp2['Collateral (USD)'].apply(lambda x: f"${x:,.2f}")
                disp2['HF'] = disp2['HF'].apply(lambda x: f"{x:.4f}")
                disp2['Address'] = disp2['Address'].apply(lambda x: f"{x[:6]}...{x[-4:]}" if len(str(x)) > 10 else x)
                st.dataframe(disp2, use_container_width=True, hide_index=True, height=300)
            else:
                st.info("No Tier 2 targets in database.")

        # Summary row
        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        liq_count = len(df_targets[(df_targets['health_factor'] > 0) & (df_targets['health_factor'] < 1.0)])
        c1.metric("Total Monitored", f"{len(df_targets)}")
        c2.metric("🔴 Tier 1 (Danger)", f"{len(df_t1)}")
        c3.metric("🟠 Tier 2 (Watchlist)", f"{len(df_t2)}")
        c4.metric("💀 Liquidatable (HF < 1.0)", f"{liq_count}")
    else:
        st.info("🔍 No live target data yet. Waiting for bot to feed Multicall3 results...")


# ─── TAB 2: Performance Analytics ────────────────────────────────────

with tab2:
    st.subheader("RPC & Scan Performance")
    df_metrics = load_metrics(100)

    if not df_metrics.empty:
        df_metrics = df_metrics.iloc[::-1].reset_index(drop=True)

        perf_col1, perf_col2 = st.columns(2)

        with perf_col1:
            st.markdown("##### Scan Time per Block (last 100)")
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(
                x=df_metrics['block_number'].astype(str),
                y=df_metrics['scan_time_ms'],
                mode='lines+markers',
                line=dict(color='#667eea', width=2),
                marker=dict(size=4, color='#764ba2'),
                fill='tozeroy', fillcolor='rgba(102, 126, 234, 0.1)',
                name='Scan Time'
            ))
            fig_line.update_layout(
                template="plotly_dark", height=350,
                margin=dict(l=20, r=20, t=30, b=40),
                xaxis_title="Block", yaxis_title="ms",
                font=dict(family="JetBrains Mono, monospace"),
                showlegend=False
            )
            st.plotly_chart(fig_line, use_container_width=True)

        with perf_col2:
            st.markdown("##### Targets Scanned per Block")
            fig_bar = px.bar(
                df_metrics, x=df_metrics['block_number'].astype(str),
                y='target_count', color_discrete_sequence=['#2ed573'],
                labels={'target_count': '# Targets', 'x': 'Block'}
            )
            fig_bar.update_layout(
                template="plotly_dark", height=350,
                margin=dict(l=20, r=20, t=30, b=40),
                xaxis_title="Block", yaxis_title="Count",
                font=dict(family="JetBrains Mono, monospace"),
                showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Avg (ms)", f"{df_metrics['scan_time_ms'].mean():.0f}")
        s2.metric("Min (ms)", f"{df_metrics['scan_time_ms'].min():.0f}")
        s3.metric("Max (ms)", f"{df_metrics['scan_time_ms'].max():.0f}")
        s4.metric("Blocks", f"{len(df_metrics)}")
    else:
        st.info("📊 No scan metrics yet. Data appears once the bot processes blocks.")


# ─── TAB 3: Battle Log ───────────────────────────────────────────────

with tab3:
    st.subheader("⚔️ Liquidation History")
    df_exec = load_executions()
    if not df_exec.empty:
        pnl1, pnl2, pnl3 = st.columns(3)
        total_eth, total_usdc = load_total_profits()
        pnl1.metric("Total Liquidations", f"{len(df_exec)}")
        pnl2.metric("Profit (USDC)", f"${total_usdc:,.2f}")
        pnl3.metric("Profit (ETH)", f"Ξ {total_eth:.4f}")
        st.divider()
        st.dataframe(df_exec, use_container_width=True, hide_index=True,
                     column_config={
                         "tx_hash": st.column_config.TextColumn("TX Hash", width="medium"),
                         "user_address": st.column_config.TextColumn("Target", width="medium"),
                         "profit_usdc": st.column_config.NumberColumn("Profit (USDC)", format="$%.2f"),
                         "profit_eth": st.column_config.NumberColumn("Profit (ETH)", format="Ξ%.6f"),
                     })
    else:
        st.info("🏹 No liquidations recorded yet.")


# ─── TAB 4: System Terminal ──────────────────────────────────────────

with tab4:
    st.subheader("📡 Live System Logs")
    df_logs = load_logs(200)

    if not df_logs.empty:
        log_lines = []
        for _, row in df_logs.iterrows():
            level = str(row['level']).lower()
            css_map = {'error': 'log-error', 'warning': 'log-warning', 'success': 'log-success'}
            css_class = css_map.get(level, 'log-info')
            line = f'<span class="{css_class}">[{row["timestamp"]}] [{row["level"]}] {row["message"]}</span>'
            log_lines.append(line)

        st.markdown(
            f'<div class="terminal-log">{"<br>".join(log_lines)}</div>',
            unsafe_allow_html=True
        )
    else:
        st.info("📜 No system logs available yet.")


# =====================================================================
# FALLBACK AUTO-REFRESH
# =====================================================================
if not HAS_AUTOREFRESH:
    time.sleep(5)
    st.rerun()
