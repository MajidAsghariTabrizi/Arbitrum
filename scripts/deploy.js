const { ethers } = require("hardhat");

async function main() {
    const [deployer] = await ethers.getSigners();

    console.log("Deploying contracts with the account:", deployer.address);

    // Arbitrum One Addresses
    const POOL_ADDRESS_PROVIDER = "0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb";
    const UNISWAP_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564";

    const FlashLoanLiquidator = await ethers.getContractFactory("FlashLoanLiquidator");
    const liquidator = await FlashLoanLiquidator.deploy(
        POOL_ADDRESS_PROVIDER,
        UNISWAP_ROUTER
    );

    await liquidator.waitForDeployment();

    console.log("FlashLoanLiquidator deployed to:", await liquidator.getAddress());
}

main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
