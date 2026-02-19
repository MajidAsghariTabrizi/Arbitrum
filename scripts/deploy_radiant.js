const hre = require("hardhat");

async function main() {
    const [deployer] = await hre.ethers.getSigners();
    console.log("Deploying RadiantLiquidator with account:", deployer.address);

    // Radiant Provider on Arbitrum (from user request)
    const RADIANT_PROVIDER = "0x091d52cce1d49c8ce620b250284d126422ce04f0";

    // Uniswap V3 SwapRouter (from user request)
    const SWAP_ROUTER = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45";

    const RadiantLiquidator = await hre.ethers.getContractFactory("RadiantLiquidator");

    // Deploy
    const liquidator = await RadiantLiquidator.deploy(RADIANT_PROVIDER, SWAP_ROUTER);
    await liquidator.waitForDeployment();

    const address = await liquidator.getAddress();
    console.log("✅ RadiantLiquidator deployed to:", address);
    console.log("Radiant Provider:", RADIANT_PROVIDER);
    console.log("SwapRouter:", SWAP_ROUTER);
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });
