const hre = require("hardhat");

async function main() {
    console.log("═══════════════════════════════════════════════════════════");
    console.log("🛸 ANTI-GRAVITY — DexArbitrageur Deployment");
    console.log("═══════════════════════════════════════════════════════════");

    const [deployer] = await hre.ethers.getSigners();
    const balance = await hre.ethers.provider.getBalance(deployer.address);
    console.log(`📍 Deployer:  ${deployer.address}`);
    console.log(`💰 Balance:   ${hre.ethers.formatEther(balance)} ETH`);
    console.log("───────────────────────────────────────────────────────────");

    // ═══════════════════════════════════════════════════════════════════
    // Aave V3 PoolAddressesProvider on Arbitrum One (Mainnet)
    // Source: https://docs.aave.com/developers/deployed-contracts/v3-mainnet/arbitrum
    // ═══════════════════════════════════════════════════════════════════
    const AAVE_POOL_ADDRESSES_PROVIDER = "0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb";

    console.log(`🏦 Aave V3 Provider: ${AAVE_POOL_ADDRESSES_PROVIDER}`);
    console.log("🚀 Deploying DexArbitrageur...\n");

    // ── Deploy ──
    const DexArbitrageur = await hre.ethers.getContractFactory("DexArbitrageur");
    const arbitrageur = await DexArbitrageur.deploy(AAVE_POOL_ADDRESSES_PROVIDER);

    // ethers v6 (Hardhat Toolbox ≥ v3): use waitForDeployment() + getAddress()
    await arbitrageur.waitForDeployment();
    const contractAddress = await arbitrageur.getAddress();

    console.log("═══════════════════════════════════════════════════════════");
    console.log(`✅ DexArbitrageur deployed to: ${contractAddress}`);
    console.log("═══════════════════════════════════════════════════════════");
    console.log("");
    console.log("📋 Add this to your .env file:");
    console.log(`   DEX_ARBITRAGEUR_ADDRESS=${contractAddress}`);
    console.log("");

    // ── Wait for confirmations before verification ──
    console.log("⏳ Waiting for 5 block confirmations...");
    const deployTx = arbitrageur.deploymentTransaction();
    if (deployTx) {
        await deployTx.wait(5);
    }

    // ── Verify on Arbiscan ──
    console.log("🔗 Verifying contract on Arbiscan...");
    try {
        await hre.run("verify:verify", {
            address: contractAddress,
            constructorArguments: [AAVE_POOL_ADDRESSES_PROVIDER],
        });
        console.log("✅ Contract verified successfully on Arbiscan!");
    } catch (error) {
        if (error.message.includes("Already Verified")) {
            console.log("ℹ️  Contract already verified.");
        } else {
            console.log(`⚠️  Verification failed: ${error.message}`);
            console.log("   You can verify manually later with:");
            console.log(`   npx hardhat verify --network arbitrum ${contractAddress} ${AAVE_POOL_ADDRESSES_PROVIDER}`);
        }
    }

    console.log("\n═══════════════════════════════════════════════════════════");
    console.log("🛸 Deployment Complete. Ready for arbitrage operations.");
    console.log("═══════════════════════════════════════════════════════════");
}

main().catch((error) => {
    console.error("❌ Deployment failed:", error);
    process.exitCode = 1;
});
