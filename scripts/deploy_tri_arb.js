const hre = require("hardhat");

async function main() {
    const [deployer] = await hre.ethers.getSigners();
    console.log("Deploying TriArbitrageur with account:", deployer.address);

    // Aave V3 Pool Addresses Provider on Arbitrum One
    const AAVE_V3_PROVIDER = "0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb";

    const TriArbitrageur = await hre.ethers.getContractFactory("TriArbitrageur");

    // Deploy
    const arbitrageur = await TriArbitrageur.deploy(AAVE_V3_PROVIDER);
    await arbitrageur.waitForDeployment();

    const address = await arbitrageur.getAddress();
    console.log("✅ TriArbitrageur deployed to:", address);
    console.log("Aave V3 Provider:", AAVE_V3_PROVIDER);
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });