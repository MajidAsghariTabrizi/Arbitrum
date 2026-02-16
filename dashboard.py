import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
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
    page_title="⚡ Gravity Bot — Mission Control",
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
        /* Global dark overrides */
        .block-container { padding-top: 1rem; padding-bottom: 1rem; }

        /* KPI metric styling */
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

        /* Terminal log panel */
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

        /* Header accent bar */
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

        /* Danger badge */
        .danger-badge {
            background: linear-gradient(135deg, #ff4757, #c0392b);
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-weight: 700;
            font-size: 0.9rem;
        }
    </style>
""", unsafe_allow_html=True)


# =====================================================================
# DATABASE HELPERS
# =====================================================================

def get_db_connection():
    """Returns a WAL-mode SQLite connection for reads."""
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except Exception as e:
        st.error(f"DB Connection Error: {e}")
        return None

def load_live_targets():
    """Reads live targets from DB sorted by health_factor ASC."""
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
    """Fetches aggregated KPI data."""
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
                "total_debt": res[0],
                "total_collateral": res[1],
                "danger_count": res[2],
                "total_count": res[3]
            }
        except Exception:
            conn.close()
    return {"total_debt": 0, "total_collateral": 0, "danger_count": 0, "total_count": 0}

def load_metrics(limit=100):
    """Fetches recent system metrics for charting."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                f"SELECT block_number, target_count, scan_time_ms, timestamp "
                f"FROM system_metrics ORDER BY id DESC LIMIT {limit}",
                conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()

def load_avg_scan_time(limit=100):
    """Returns average scan time over last N entries."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COALESCE(AVG(scan_time_ms), 0) "
                "FROM (SELECT scan_time_ms FROM system_metrics ORDER BY id DESC LIMIT ?)",
                (limit,)
            )
            res = cur.fetchone()
            conn.close()
            return res[0]
        except Exception:
            conn.close()
    return 0.0

def load_logs(limit=100):
    """Fetches the most recent system logs."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                f"SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT {limit}",
                conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()

def load_executions(limit=50):
    """Fetches the most recent executions."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(
                f"SELECT timestamp, tx_hash, user_address, profit_usdc, profit_eth "
                f"FROM executions ORDER BY id DESC LIMIT {limit}",
                conn
            )
            conn.close()
            return df
        except Exception:
            conn.close()
    return pd.DataFrame()

def load_total_profits():
    """Returns (eth, usdc) profit totals."""
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


# =====================================================================
# COLOR CODING HELPER
# =====================================================================

def hf_color(hf):
    """Return CSS color string based on health factor threshold."""
    if hf <= 0:
        return "gray"
    elif hf < 1.0:
        return "#ff4757"   # Red — liquidatable
    elif hf < 1.05:
        return "#ffa502"   # Orange — danger zone
    else:
        return "#2ed573"   # Green — safe


# =====================================================================
# AUTO-REFRESH
# =====================================================================

if HAS_AUTOREFRESH:
    # Auto-refresh every 5 seconds without blocking
    st_autorefresh(interval=5000, limit=None, key="dashboard_refresh")


# =====================================================================
# HEADER
# =====================================================================

st.markdown(
    '<div class="header-bar">🛸 GRAVITY BOT — Mission Control Dashboard</div>',
    unsafe_allow_html=True
)


# =====================================================================
# SIDEBAR
# =====================================================================

with st.sidebar:
    st.title("🛸 Gravity Bot")
    st.caption("Arbitrum Liquidator v2.0")

    st.divider()

    st.metric("System Status", "ONLINE", delta="Active", delta_color="normal")

    total_eth, total_usdc = load_total_profits()
    st.metric("Total Profit (USDC)", f"${total_usdc:,.2f}")
    st.metric("Total Profit (ETH)", f"Ξ {total_eth:.4f}")

    st.divider()

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

    time_stamp = datetime.now().strftime("%H:%M:%S")
    st.caption(f"Last Update: {time_stamp}")

    if not HAS_AUTOREFRESH:
        st.caption("💡 Install `streamlit-autorefresh` for seamless auto-refresh")


# =====================================================================
# TOP KPIs
# =====================================================================

summary = load_summary()
avg_scan = load_avg_scan_time()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric(
        "💰 Monitored Debt",
        f"${summary['total_debt']:,.2f}",
    )

with kpi2:
    st.metric(
        "🏦 Monitored Collateral",
        f"${summary['total_collateral']:,.2f}",
    )

with kpi3:
    danger = summary['danger_count']
    st.metric(
        "⚠️ Users in Danger (HF < 1.05)",
        f"{danger}",
        delta=f"-{danger}" if danger > 0 else None,
        delta_color="inverse" if danger > 0 else "off"
    )

with kpi4:
    st.metric(
        "⚡ Avg Scan Speed",
        f"{avg_scan:.0f} ms",
    )

st.divider()


# =====================================================================
# TABS
# =====================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "🎯 Live Targets",
    "📊 Performance Analytics",
    "⚔️ Battle Log",
    "📜 System Terminal"
])


# ─── TAB 1: Live Targets ─────────────────────────────────────────────

with tab1:
    st.subheader("Real-Time Target Monitor")

    df_targets = load_live_targets()

    if not df_targets.empty:
        # --- Plotly HF Distribution Chart ---
        chart_col, grid_col = st.columns([1, 2])

        with chart_col:
            st.markdown("##### Health Factor Distribution")
            # Filter meaningful HF values for the histogram
            hf_data = df_targets[df_targets['health_factor'] > 0].copy()

            if not hf_data.empty:
                # Cap extremely large HF for visual clarity
                hf_data['hf_display'] = hf_data['health_factor'].clip(upper=5.0)

                fig_hist = px.histogram(
                    hf_data,
                    x='hf_display',
                    nbins=40,
                    color_discrete_sequence=['#667eea'],
                    labels={'hf_display': 'Health Factor'},
                    title=''
                )
                # Add danger threshold line
                fig_hist.add_vline(
                    x=1.0,
                    line_dash="dash",
                    line_color="#ff4757",
                    line_width=2,
                    annotation_text="Liquidation",
                    annotation_position="top right",
                    annotation_font=dict(color="#ff4757", size=11)
                )
                fig_hist.add_vline(
                    x=1.05,
                    line_dash="dot",
                    line_color="#ffa502",
                    line_width=1.5,
                    annotation_text="Danger",
                    annotation_position="top left",
                    annotation_font=dict(color="#ffa502", size=11)
                )
                fig_hist.update_layout(
                    template="plotly_dark",
                    height=350,
                    margin=dict(l=20, r=20, t=30, b=40),
                    xaxis_title="Health Factor",
                    yaxis_title="# Users",
                    bargap=0.05,
                    font=dict(family="JetBrains Mono, monospace")
                )
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("No HF data available yet.")

        with grid_col:
            st.markdown("##### Target List (sorted by HF ↑)")

            # Prepare display DataFrame
            display_df = df_targets.copy()
            display_df.columns = ['Address', 'Health Factor', 'Debt (USD)', 'Collateral (USD)', 'Updated At']

            # Format USD values
            display_df['Debt (USD)'] = display_df['Debt (USD)'].apply(lambda x: f"${x:,.2f}")
            display_df['Collateral (USD)'] = display_df['Collateral (USD)'].apply(lambda x: f"${x:,.2f}")
            display_df['Health Factor'] = display_df['Health Factor'].apply(lambda x: f"{x:.4f}")

            # Truncate addresses for readability
            display_df['Address'] = display_df['Address'].apply(
                lambda x: f"{x[:6]}...{x[-4:]}" if len(str(x)) > 10 else x
            )

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=350,
                column_config={
                    "Health Factor": st.column_config.TextColumn(
                        "Health Factor",
                        help="Red < 1.0 (liquidatable), Orange < 1.05 (danger), Green > 1.05 (safe)"
                    ),
                    "Debt (USD)": st.column_config.TextColumn("Debt (USD)"),
                    "Collateral (USD)": st.column_config.TextColumn("Collateral (USD)"),
                }
            )

        # Color-coded HF summary below
        if not hf_data.empty:
            liq_count = len(hf_data[hf_data['health_factor'] < 1.0])
            danger_count = len(hf_data[(hf_data['health_factor'] >= 1.0) & (hf_data['health_factor'] < 1.05)])
            safe_count = len(hf_data[hf_data['health_factor'] >= 1.05])

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Monitored", f"{len(df_targets)}")
            c2.metric("🔴 Liquidatable (HF < 1.0)", f"{liq_count}")
            c3.metric("🟠 Danger (1.0–1.05)", f"{danger_count}")
            c4.metric("🟢 Safe (HF > 1.05)", f"{safe_count}")
    else:
        st.info("🔍 No live target data yet. Waiting for bot to feed Multicall3 results...")


# ─── TAB 2: Performance Analytics ────────────────────────────────────

with tab2:
    st.subheader("RPC & Scan Performance")

    df_metrics = load_metrics(100)

    if not df_metrics.empty:
        # Reverse for chronological order
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
                fill='tozeroy',
                fillcolor='rgba(102, 126, 234, 0.1)',
                name='Scan Time'
            ))
            fig_line.update_layout(
                template="plotly_dark",
                height=350,
                margin=dict(l=20, r=20, t=30, b=40),
                xaxis_title="Block Number",
                yaxis_title="Scan Time (ms)",
                font=dict(family="JetBrains Mono, monospace"),
                showlegend=False
            )
            st.plotly_chart(fig_line, use_container_width=True)

        with perf_col2:
            st.markdown("##### Targets Scanned per Block")
            fig_bar = px.bar(
                df_metrics,
                x=df_metrics['block_number'].astype(str),
                y='target_count',
                color_discrete_sequence=['#2ed573'],
                labels={'target_count': '# Targets', 'x': 'Block Number'},
                title=''
            )
            fig_bar.update_layout(
                template="plotly_dark",
                height=350,
                margin=dict(l=20, r=20, t=30, b=40),
                xaxis_title="Block Number",
                yaxis_title="Target Count",
                font=dict(family="JetBrains Mono, monospace"),
                showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # Performance stats row
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Avg Scan (ms)", f"{df_metrics['scan_time_ms'].mean():.0f}")
        s2.metric("Min Scan (ms)", f"{df_metrics['scan_time_ms'].min():.0f}")
        s3.metric("Max Scan (ms)", f"{df_metrics['scan_time_ms'].max():.0f}")
        s4.metric("Blocks Tracked", f"{len(df_metrics)}")
    else:
        st.info("📊 No scan metrics yet. Performance data will appear once the bot starts processing blocks.")


# ─── TAB 3: Battle Log ───────────────────────────────────────────────

with tab3:
    st.subheader("⚔️ Liquidation History")

    df_exec = load_executions()
    if not df_exec.empty:
        # Quick profit summary
        pnl1, pnl2, pnl3 = st.columns(3)
        total_eth, total_usdc = load_total_profits()
        pnl1.metric("Total Liquidations", f"{len(df_exec)}")
        pnl2.metric("Total Profit (USDC)", f"${total_usdc:,.2f}")
        pnl3.metric("Total Profit (ETH)", f"Ξ {total_eth:.4f}")

        st.divider()
        st.dataframe(
            df_exec,
            use_container_width=True,
            hide_index=True,
            column_config={
                "tx_hash": st.column_config.TextColumn("TX Hash", width="medium"),
                "user_address": st.column_config.TextColumn("Target", width="medium"),
                "profit_usdc": st.column_config.NumberColumn("Profit (USDC)", format="$%.2f"),
                "profit_eth": st.column_config.NumberColumn("Profit (ETH)", format="Ξ%.6f"),
            }
        )
    else:
        st.info("🏹 No liquidations recorded yet. The bot will log executions here.")


# ─── TAB 4: System Terminal ──────────────────────────────────────────

with tab4:
    st.subheader("📡 Live System Logs")

    df_logs = load_logs(200)

    if not df_logs.empty:
        # Build terminal-style HTML output
        log_lines = []
        for _, row in df_logs.iterrows():
            level = str(row['level']).lower()
            if level == 'error':
                css_class = 'log-error'
            elif level == 'warning':
                css_class = 'log-warning'
            elif level == 'success':
                css_class = 'log-success'
            else:
                css_class = 'log-info'

            line = f'<span class="{css_class}">[{row["timestamp"]}] [{row["level"]}] {row["message"]}</span>'
            log_lines.append(line)

        log_html = "<br>".join(log_lines)
        st.markdown(
            f'<div class="terminal-log">{log_html}</div>',
            unsafe_allow_html=True
        )
    else:
        st.info("📜 No system logs available yet.")


# =====================================================================
# FALLBACK AUTO-REFRESH (if streamlit-autorefresh not installed)
# =====================================================================
if not HAS_AUTOREFRESH:
    time.sleep(5)
    st.rerun()
