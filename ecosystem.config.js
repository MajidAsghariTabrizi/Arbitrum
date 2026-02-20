// ═══════════════════════════════════════════════════════════════
// 🛸 ANTI-GRAVITY — PM2 Ecosystem Config (Free-Tier Optimized)
// ═══════════════════════════════════════════════════════════════
//
// ARCHITECTURE DECISION: Scanners (scanner.py & radiant_scanner.py)
// are intentionally EXCLUDED from this startup config.
//
// WHY: Historical eth_getLogs calls in the scanners are I/O-heavy
// and trigger IP-wide 429 rate limits that cripple all arbitrage
// engines sharing the same free-tier RPC nodes.
//
// HOW TO RUN SCANNERS (manually, on-demand only):
//   pm2 start scanner.py --interpreter python3 --name scanner
//   pm2 start radiant_scanner.py --interpreter python3 --name radiant-scanner
//   (Wait for scan to complete, then pm2 stop scanner radiant-scanner)
//
// ═══════════════════════════════════════════════════════════════

module.exports = {
    apps: [
        // ── Block Emitter ──────────────────────────────────────────
        {
            name: "emitter",
            script: "block_emitter.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 20,
            restart_delay: 3000,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── Aave V3 Liquidation Sniper ──────────────────────────────
        {
            name: "gravity-bot",
            script: "gravity_bot.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 20,
            restart_delay: 5000,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── Radiant V2 Liquidation Sniper ───────────────────────────
        {
            name: "radiant-bot",
            script: "radiant_bot.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 20,
            restart_delay: 5000,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── DEX Arbitrage Engine ────────────────────────────────────
        {
            name: "arb-engine",
            script: "arb_engine.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 20,
            restart_delay: 5000,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── Triangular Arbitrage Engine ─────────────────────────────
        {
            name: "tri-arb-engine",
            script: "tri_arb_engine.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 20,
            restart_delay: 5000,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── Streamlit Dashboard (optional) ──────────────────────────
        {
            name: "dashboard",
            script: "streamlit",
            args: "run dashboard.py",
            interpreter: "none",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 10,
            restart_delay: 10000,
            env: {
                PYTHONUNBUFFERED: "1",
                STREAMLIT_SERVER_PORT: "8501",
                STREAMLIT_SERVER_HEADLESS: "true",
            },
        },
    ],
};
