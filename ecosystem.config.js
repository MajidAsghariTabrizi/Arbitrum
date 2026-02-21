// ═══════════════════════════════════════════════════════════════
// 🛸 ANTI-GRAVITY — PM2 Ecosystem Config (Free-Tier Optimized)
// ═══════════════════════════════════════════════════════════════
//
// HOW TO RUN:
//   pm2 start ecosystem.config.js
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

        // ── Lodestar Liquidation Sniper ───────────────────────────
        {
            name: "lodestar-bot",
            script: "lodestar_bot.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            max_restarts: 20,
            restart_delay: 5000,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── DEX Arbitrage Engine (DISABLED) ─────────────────────────────
        // {
        //     name: "arb-engine",
        //     script: "arb_engine.py",
        //     interpreter: "python3",
        //     cwd: "/root/Arbitrum",
        //     autorestart: true,
        //     watch: false,
        //     max_restarts: 20,
        //     restart_delay: 5000,
        //     env: { PYTHONUNBUFFERED: "1" },
        // },

        // ── Triangular Arbitrage Engine (DISABLED) ──────────────────────
        // {
        //     name: "tri-arb-engine",
        //     script: "tri_arb_engine.py",
        //     interpreter: "python3",
        //     cwd: "/root/Arbitrum",
        //     autorestart: true,
        //     watch: false,
        //     max_restarts: 20,
        //     restart_delay: 5000,
        //     env: { PYTHONUNBUFFERED: "1" },
        // },

        // ── Aave Background Scanner (24/7) ───────────────────────────
        {
            name: "scanner",
            script: "scanner.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
            env: { PYTHONUNBUFFERED: "1" },
        },

        // ── Lodestar Background Scanner (24/7) ────────────────────────
        {
            name: "lodestar-scanner",
            script: "lodestar_scanner.py",
            interpreter: "python3",
            cwd: "/root/Arbitrum",
            autorestart: true,
            watch: false,
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
