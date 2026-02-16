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

        /* Tab Styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 8px 20px;
            font-weight: 600;
        }
    </style>
""", unsafe_allow_html=True)


# =====================================================================
# DATABASE HELPERS — Thread-safe reads with graceful fallbacks
# =====================================================================

def get_db_connection():
    """Thread-safe DB connection with WAL mode."""
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except Exception:
        return None


def safe_query(query, params=None, fallback=None):
    """Execute a read query with full error handling. Returns pandas DataFrame or fallback."""
    conn = get_db_connection()
    if not conn:
        return fallback if fallback is not None else pd.DataFrame()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception:
        conn.close()
        return fallback if fallback is not None else pd.DataFrame()


def safe_scalar(query, params=None, fallback=0):
    """Execute a scalar query (returns single value). Falls back gracefully."""
    conn = get_db_connection()
    if not conn:
        return fallback
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        result = cur.fetchone()
        conn.close()
        return result[0] if result and result[0] is not None else fallback
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return fallback


def load_live_targets():
    """Load all live targets sorted by HF ascending."""
    return safe_query(
        "SELECT address, health_factor, total_debt_usd, total_collateral_usd, updated_at "
        "FROM live_targets ORDER BY health_factor ASC"
    )


def load_summary():
    """Load tiered summary KPIs from live_targets."""
    conn = get_db_connection()
    if not conn:
        return {"total_debt": 0, "total_collateral": 0, "danger_count": 0,
                "watchlist_count": 0, "total_count": 0, "danger_debt": 0, "watchlist_debt": 0}
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
        try:
            conn.close()
        except Exception:
            pass
        return {"total_debt": 0, "total_collateral": 0, "danger_count": 0,
                "watchlist_count": 0, "total_count": 0, "danger_debt": 0, "watchlist_debt": 0}


def load_metrics(limit=100):
    """Load recent system metrics with tier breakdown."""
    try:
        return safe_query(
            "SELECT block_number, target_count, tier_1_count, tier_2_count, scan_time_ms, timestamp "
            "FROM system_metrics ORDER BY id DESC LIMIT ?", (limit,)
        )
    except Exception:
        # Fallback if tier columns don't exist
        return safe_query(
            "SELECT block_number, target_count, 0 as tier_1_count, 0 as tier_2_count, scan_time_ms, timestamp "
            "FROM system_metrics ORDER BY id DESC LIMIT ?", (limit,)
        )


def load_avg_scan_time(limit=100):
    return safe_scalar(
        "SELECT COALESCE(AVG(scan_time_ms), 0) "
        "FROM (SELECT scan_time_ms FROM system_metrics ORDER BY id DESC LIMIT ?)",
        (limit,), fallback=0.0
    )


def load_logs(limit=200):
    return safe_query(
        "SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT ?", (limit,)
    )


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
        try:
            conn.close()
        except Exception:
            pass
        return 0.0, 0.0


def load_targets_json():
    """
    Robustly parses the tiered targets.json file.
    Supports: {"tier_1_danger": [...], "tier_2_watchlist": [...]}
    Fallback: flat list treated as all Tier 1.
    Returns: (list, list) — (tier_1, tier_2)
    """
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
                        return data, []  # Legacy flat format
            except (json.JSONDecodeError, IOError):
                pass
    return [], []


# =====================================================================
# TERMINAL LOG HIGHLIGHTER — Keyword-aware CSS coloring
# =====================================================================

def highlight_log_line(message, level):
    """Apply CSS classes based on log level AND Anti-Gravity keywords."""
    # Escape HTML to prevent injection
    msg = str(message).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Priority keyword highlights (checked first, most specific)
    if "[SNIPER]" in msg:
        msg = msg.replace("[SNIPER]", '<span class="log-sniper">[SNIPER]</span>')
    if "[SCOUT]" in msg:
        msg = msg.replace("[SCOUT]", '<span class="log-scout">[SCOUT]</span>')
    if "PROMOTED" in msg.upper():
        msg = re.sub(r'(PROMOTED)', r'<span class="log-promoted">\1</span>', msg, flags=re.IGNORECASE)
    if "Pre-flight" in msg or "PRE-FLIGHT" in msg or "Simulation" in msg:
        msg = re.sub(r'(Pre-flight|PRE-FLIGHT|Simulation|simulate)', r'<span class="log-preflight">\1</span>', msg, flags=re.IGNORECASE)
    if "TX SENT" in msg or "TX CONFIRMED" in msg:
        msg = re.sub(r'(TX SENT|TX CONFIRMED|TX REVERTED)', r'<span class="log-tx">\1</span>', msg, flags=re.IGNORECASE)

    # Base level coloring
    level_lower = str(level).lower()
    css_map = {
        'error': 'log-error', 'warning': 'log-warning',
        'success': 'log-success', 'info': 'log-info'
    }
    css_class = css_map.get(level_lower, 'log-info')

    return f'<span class="{css_class}">{msg}</span>'


# =====================================================================
# AUTO-REFRESH
# =====================================================================

if HAS_AUTOREFRESH:
    st_autorefresh(interval=5000, limit=None, key="dashboard_refresh")


# =====================================================================
# HEADER
# =====================================================================

st.markdown(
    '<div class="header-bar">'
    '🛸 ANTI-GRAVITY — Mission Control &nbsp;|&nbsp; '
    '<span style="font-size: 0.8rem; opacity: 0.7;">MEV Sniper v2.0 • Arbitrum One</span>'
    '</div>',
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

    # Tier overview from JSON (file-based, independent of DB)
    t1_json, t2_json = load_targets_json()
    st.markdown(f'<span class="tier-1-badge">TIER 1</span> &nbsp; **{len(t1_json)}** targets', unsafe_allow_html=True)
    st.markdown(f'<span class="tier-2-badge">TIER 2</span> &nbsp; **{len(t2_json)}** targets', unsafe_allow_html=True)

    st.divider()

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

    st.caption(f"Last Update: {datetime.now().strftime('%H:%M:%S')}")
    if not HAS_AUTOREFRESH:
        st.caption("💡 Install `streamlit-autorefresh` for auto-refresh")


# =====================================================================
# TOP ROW KPIs — 4 columns with color-coded tiers
# =====================================================================

summary = load_summary()
avg_scan = load_avg_scan_time()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.markdown('<div class="kpi-info">', unsafe_allow_html=True)
    st.metric("📡 Total Live Targets", f"{summary['total_count']}")
    st.markdown('</div>', unsafe_allow_html=True)
with kpi2:
    st.markdown('<div class="kpi-danger">', unsafe_allow_html=True)
    st.metric("🔴 Tier 1 (Danger)", f"{summary['danger_count']}")
    st.markdown('</div>', unsafe_allow_html=True)
with kpi3:
    st.markdown('<div class="kpi-warning">', unsafe_allow_html=True)
    st.metric("🟠 Tier 2 (Watchlist)", f"{summary['watchlist_count']}")
    st.markdown('</div>', unsafe_allow_html=True)
with kpi4:
    total_risk = summary['danger_debt'] + summary['watchlist_debt']
    st.markdown('<div class="kpi-danger">', unsafe_allow_html=True)
    st.metric("💰 Value at Risk", f"${total_risk:,.0f}")
    st.markdown('</div>', unsafe_allow_html=True)

st.divider()


# =====================================================================
# TABS
# =====================================================================

tab_radar, tab_danger, tab_watch, tab_exec, tab_term = st.tabs([
    "📡 Radar",
    "🔴 Danger Zone",
    "🟠 Watchlist",
    "⚔️ Executions",
    "📜 Live Terminal"
])


# ─── TAB 1: RADAR SCATTER PLOT ───────────────────────────────────────

with tab_radar:
    st.subheader("🎯 Target Radar — Health Factor vs Debt")

    df_all = load_live_targets()

    if not df_all.empty:
        # Filter to HF range 0 < HF < 1.25 for meaningful visualization
        df_radar = df_all[
            (df_all['health_factor'] > 0) & (df_all['health_factor'] < 1.25)
        ].copy()

        if not df_radar.empty:
            # Assign tier labels for coloring
            df_radar['tier'] = df_radar['health_factor'].apply(
                lambda hf: '🔴 Tier 1 (Danger)' if hf < 1.05 else '🟠 Tier 2 (Watchlist)'
            )
            # Truncated address for tooltips
            df_radar['short_addr'] = df_radar['address'].apply(
                lambda a: f"{a[:8]}...{a[-6:]}" if len(str(a)) > 14 else a
            )

            fig_radar = px.scatter(
                df_radar,
                x='health_factor',
                y='total_debt_usd',
                color='tier',
                color_discrete_map={
                    '🔴 Tier 1 (Danger)': '#ff4757',
                    '🟠 Tier 2 (Watchlist)': '#ffa502'
                },
                size='total_debt_usd',
                size_max=35,
                hover_data={
                    'short_addr': True,
                    'health_factor': ':.4f',
                    'total_debt_usd': ':$,.2f',
                    'total_collateral_usd': ':$,.2f',
                    'tier': False
                },
                labels={
                    'health_factor': 'Health Factor',
                    'total_debt_usd': 'Debt (USD)',
                    'short_addr': 'Address',
                    'total_collateral_usd': 'Collateral'
                }
            )

            # Liquidation threshold line
            fig_radar.add_vline(
                x=1.0, line_dash="dash", line_color="#ff4757",
                line_width=2.5, annotation_text="⚡ LIQUIDATION",
                annotation_font=dict(color="#ff4757", size=11, family="JetBrains Mono")
            )
            # Tier boundary
            fig_radar.add_vline(
                x=1.05, line_dash="dot", line_color="#ffa502",
                line_width=1.5, annotation_text="Tier 1 | Tier 2",
                annotation_font=dict(color="#ffa502", size=10, family="JetBrains Mono"),
                annotation_position="top"
            )

            fig_radar.update_layout(
                template="plotly_dark",
                height=520,
                margin=dict(l=30, r=30, t=50, b=50),
                xaxis=dict(
                    title="Health Factor",
                    range=[0.95, 1.25],
                    dtick=0.025,
                    gridcolor='rgba(255,255,255,0.05)'
                ),
                yaxis=dict(
                    title="Debt Size (USD)",
                    gridcolor='rgba(255,255,255,0.05)'
                ),
                font=dict(family="JetBrains Mono, monospace", size=11),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1,
                    font=dict(size=12)
                ),
                plot_bgcolor='rgba(8,8,16,0.9)',
                paper_bgcolor='rgba(0,0,0,0)'
            )

            st.plotly_chart(fig_radar, use_container_width=True)

            # Summary stats below radar
            rc1, rc2, rc3, rc4 = st.columns(4)
            liq_count = len(df_radar[df_radar['health_factor'] < 1.0])
            rc1.metric("Radar Targets", f"{len(df_radar)}")
            rc2.metric("💀 Liquidatable (HF < 1.0)", f"{liq_count}")
            rc3.metric("🔴 Danger (HF < 1.05)", f"{len(df_radar[df_radar['health_factor'] < 1.05])}")
            rc4.metric("🟠 Watchlist (1.05–1.20)", f"{len(df_radar[df_radar['health_factor'] >= 1.05])}")
        else:
            st.info("📡 No targets in the HF 0–1.25 visualization range.")
    else:
        st.info("🔍 No live target data yet. Waiting for bot to feed Multicall3 results...")


# ─── TAB 2: DANGER ZONE (Tier 1) ─────────────────────────────────────

with tab_danger:
    st.markdown(
        '### <span class="tier-1-badge">TIER 1</span> &nbsp; Danger Zone — HF < 1.05',
        unsafe_allow_html=True
    )

    df_all_targets = load_live_targets()

    if not df_all_targets.empty:
        df_t1 = df_all_targets[
            (df_all_targets['health_factor'] > 0) & (df_all_targets['health_factor'] < 1.05)
        ].copy()

        if not df_t1.empty:
            d1, d2 = st.columns([1, 2])

            with d1:
                # HF distribution histogram
                fig_hist_t1 = px.histogram(
                    df_t1, x='health_factor', nbins=30,
                    color_discrete_sequence=['#ff4757'],
                    labels={'health_factor': 'Health Factor'}
                )
                fig_hist_t1.add_vline(
                    x=1.0, line_dash="dash", line_color="#fff",
                    line_width=2, annotation_text="Liquidation",
                    annotation_font=dict(color="#fff", size=10)
                )
                fig_hist_t1.update_layout(
                    template="plotly_dark", height=350,
                    margin=dict(l=20, r=20, t=30, b=40),
                    xaxis_title="HF", yaxis_title="# Users",
                    font=dict(family="JetBrains Mono, monospace", size=10)
                )
                st.plotly_chart(fig_hist_t1, use_container_width=True)

                # Tier 1 KPIs
                st.metric("🔴 Tier 1 Total Debt", f"${df_t1['total_debt_usd'].sum():,.0f}")
                st.metric("💀 Liquidatable (HF < 1.0)", f"{len(df_t1[df_t1['health_factor'] < 1.0])}")

            with d2:
                disp = df_t1.copy()
                disp.columns = ['Address', 'HF', 'Debt (USD)', 'Collateral (USD)', 'Updated']
                disp['Debt (USD)'] = disp['Debt (USD)'].apply(lambda x: f"${x:,.2f}")
                disp['Collateral (USD)'] = disp['Collateral (USD)'].apply(lambda x: f"${x:,.2f}")
                disp['HF'] = disp['HF'].apply(lambda x: f"{x:.4f}")
                disp['Address'] = disp['Address'].apply(
                    lambda x: f"{x[:8]}...{x[-6:]}" if len(str(x)) > 14 else x
                )
                st.dataframe(disp, use_container_width=True, hide_index=True, height=380)
        else:
            st.success("✅ No targets in the danger zone. All positions are safe.")
    else:
        st.info("🔍 No live target data available.")


# ─── TAB 3: WATCHLIST (Tier 2) ───────────────────────────────────────

with tab_watch:
    st.markdown(
        '### <span class="tier-2-badge">TIER 2</span> &nbsp; Watchlist — HF 1.05 – 1.20',
        unsafe_allow_html=True
    )

    df_all_w = load_live_targets()

    if not df_all_w.empty:
        df_t2 = df_all_w[
            (df_all_w['health_factor'] >= 1.05) & (df_all_w['health_factor'] < 1.20)
        ].copy()

        if not df_t2.empty:
            w1, w2 = st.columns([1, 2])

            with w1:
                fig_hist_t2 = px.histogram(
                    df_t2, x='health_factor', nbins=30,
                    color_discrete_sequence=['#ffa502'],
                    labels={'health_factor': 'Health Factor'}
                )
                fig_hist_t2.add_vline(
                    x=1.05, line_dash="dot", line_color="#fff",
                    line_width=1.5, annotation_text="Tier 1 Boundary",
                    annotation_font=dict(color="#fff", size=10)
                )
                fig_hist_t2.update_layout(
                    template="plotly_dark", height=350,
                    margin=dict(l=20, r=20, t=30, b=40),
                    xaxis_title="HF", yaxis_title="# Users",
                    font=dict(family="JetBrains Mono, monospace", size=10)
                )
                st.plotly_chart(fig_hist_t2, use_container_width=True)

                st.metric("🟠 Tier 2 Total Debt", f"${df_t2['total_debt_usd'].sum():,.0f}")
                st.metric("Watchlist Size", f"{len(df_t2)} targets")

            with w2:
                disp2 = df_t2.copy()
                disp2.columns = ['Address', 'HF', 'Debt (USD)', 'Collateral (USD)', 'Updated']
                disp2['Debt (USD)'] = disp2['Debt (USD)'].apply(lambda x: f"${x:,.2f}")
                disp2['Collateral (USD)'] = disp2['Collateral (USD)'].apply(lambda x: f"${x:,.2f}")
                disp2['HF'] = disp2['HF'].apply(lambda x: f"{x:.4f}")
                disp2['Address'] = disp2['Address'].apply(
                    lambda x: f"{x[:8]}...{x[-6:]}" if len(str(x)) > 14 else x
                )
                st.dataframe(disp2, use_container_width=True, hide_index=True, height=380)
        else:
            st.info("📋 No targets on the watchlist currently.")
    else:
        st.info("🔍 No live target data available.")


# ─── TAB 4: EXECUTIONS ───────────────────────────────────────────────

with tab_exec:
    st.subheader("⚔️ Liquidation History")

    df_exec = load_executions()
    if not df_exec.empty:
        pnl1, pnl2, pnl3 = st.columns(3)
        total_eth, total_usdc = load_total_profits()
        pnl1.metric("Total Liquidations", f"{len(df_exec)}")
        pnl2.metric("Profit (USDC)", f"${total_usdc:,.2f}")
        pnl3.metric("Profit (ETH)", f"Ξ {total_eth:.4f}")
        st.divider()
        st.dataframe(
            df_exec, use_container_width=True, hide_index=True,
            column_config={
                "tx_hash": st.column_config.TextColumn("TX Hash", width="medium"),
                "user_address": st.column_config.TextColumn("Target", width="medium"),
                "profit_usdc": st.column_config.NumberColumn("Profit (USDC)", format="$%.2f"),
                "profit_eth": st.column_config.NumberColumn("Profit (ETH)", format="Ξ%.6f"),
            }
        )
    else:
        st.info("🏹 No liquidations recorded yet. The sniper is watching...")


# ─── TAB 5: LIVE TERMINAL ────────────────────────────────────────────

with tab_term:
    st.subheader("📡 Live System Terminal")
    st.caption("Keyword highlights: "
               '<span class="log-sniper">[SNIPER]</span> · '
               '<span class="log-scout">[SCOUT]</span> · '
               '<span class="log-promoted">PROMOTED</span> · '
               '<span class="log-preflight">Pre-flight</span> · '
               '<span class="log-tx">TX SENT</span>',
               unsafe_allow_html=True)

    df_logs = load_logs(300)

    if not df_logs.empty:
        log_lines = []
        for _, row in df_logs.iterrows():
            ts = row['timestamp']
            level = row['level']
            msg = row['message']

            # Build highlighted line
            highlighted_msg = highlight_log_line(msg, level)
            line = f'<span style="opacity:0.5">[{ts}]</span> {highlighted_msg}'
            log_lines.append(line)

        st.markdown(
            f'<div class="terminal-log">{"<br>".join(log_lines)}</div>',
            unsafe_allow_html=True
        )
    else:
        st.info("📜 No system logs yet. Logs appear once the bot starts processing blocks.")


# =====================================================================
# PERFORMANCE ANALYTICS (Below Tabs)
# =====================================================================

with st.expander("📊 Scan Performance Analytics", expanded=False):
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
                marker=dict(size=3, color='#764ba2'),
                fill='tozeroy', fillcolor='rgba(102, 126, 234, 0.08)',
                name='Scan Time'
            ))
            fig_line.update_layout(
                template="plotly_dark", height=320,
                margin=dict(l=20, r=20, t=20, b=40),
                xaxis_title="Block", yaxis_title="ms",
                font=dict(family="JetBrains Mono, monospace", size=10),
                showlegend=False
            )
            st.plotly_chart(fig_line, use_container_width=True)

        with perf_col2:
            st.markdown("##### Tier Breakdown per Block")
            fig_tier = go.Figure()
            fig_tier.add_trace(go.Bar(
                x=df_metrics['block_number'].astype(str),
                y=df_metrics['tier_1_count'],
                name='Tier 1', marker_color='#ff4757'
            ))
            fig_tier.add_trace(go.Bar(
                x=df_metrics['block_number'].astype(str),
                y=df_metrics['tier_2_count'],
                name='Tier 2', marker_color='#ffa502'
            ))
            fig_tier.update_layout(
                template="plotly_dark", height=320,
                margin=dict(l=20, r=20, t=20, b=40),
                xaxis_title="Block", yaxis_title="Count",
                font=dict(family="JetBrains Mono, monospace", size=10),
                barmode='stack',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_tier, use_container_width=True)

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Avg (ms)", f"{df_metrics['scan_time_ms'].mean():.0f}")
        s2.metric("Min (ms)", f"{df_metrics['scan_time_ms'].min():.0f}")
        s3.metric("Max (ms)", f"{df_metrics['scan_time_ms'].max():.0f}")
        s4.metric("Blocks Tracked", f"{len(df_metrics)}")
    else:
        st.info("📊 No scan metrics yet. Data appears once the bot processes blocks.")


# =====================================================================
# FALLBACK AUTO-REFRESH (if streamlit-autorefresh not installed)
# =====================================================================
if not HAS_AUTOREFRESH:
    time.sleep(5)
    st.rerun()
