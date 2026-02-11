const { expect } = require("chai");
const { ethers, network } = require("hardhat");

describe("Aave V3 Liquidation Bot", function () {
    let liquidator;
    let owner;
    let whale;
    let targetUser;
    let mockOracle;

    // Addresses (Arbitrum One)
    const POOL_ADDRESS_PROVIDER = "0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb";
    const POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD";
    const USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
    const WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1";
    const UNISWAP_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564";

    // Large Holders for impersonation (to fund our test user)
    const USDC_WHALE = "0x47c031236e19d024b42f8AE6780E44A573170703"; // Arbitrary large holder
    const WETH_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"; // Arbitrary large holder

    before(async function () {
        [owner] = await ethers.getSigners();

        // Deploy Mock Oracle
        const MockOracle = await ethers.getContractFactory("MockOracle");
        mockOracle = await MockOracle.deploy();
        await mockOracle.waitForDeployment();
        console.log("Mock Oracle deployed to:", await mockOracle.getAddress());

        // Set Initial Prices (matching roughly current market or just consistent values)
        // WETH: $2000 (8 decimals on Aave Oracle usually, but let's check)
        // Aave V3 Oracles return prices in BASE CURRENCY (usually USD with 8 decimals).
        // Let's assume 8 decimals for USD base.
        await mockOracle.setAssetPrice(WETH, 200000000000); // $2000
        await mockOracle.setAssetPrice(USDC, 100000000);    // $1

        // Deploy Liquidator Contract
        const FlashLoanLiquidator = await ethers.getContractFactory("FlashLoanLiquidator");
        liquidator = await FlashLoanLiquidator.deploy(
            POOL_ADDRESS_PROVIDER,
            UNISWAP_ROUTER
        );
        await liquidator.waitForDeployment();
        console.log("Liquidator deployed to:", await liquidator.getAddress());

        // Setup Target User (Create a position that we can liquidate)
        // We will create a fresh wallet for the target user
        const TargetUserWallet = ethers.Wallet.createRandom().connect(ethers.provider);
        targetUser = TargetUserWallet;

        // Fund Target User with ETH for gas
        await owner.sendTransaction({
            to: targetUser.address,
            value: ethers.parseEther("1.0")
        });

        // Impersonate Whales to fund Target User with Collateral (WETH)
        await network.provider.request({
            method: "hardhat_impersonateAccount",
            params: [WETH_WHALE],
        });

        // Fund the whale with ETH
        // Fund the whale with huge ETH so they can wrap it
        await network.provider.send("hardhat_setBalance", [
            WETH_WHALE,
            "0x56BC75E2D63100000", // 100 ETH in hex
        ]);


        const wethWhaleSigner = await ethers.getSigner(WETH_WHALE);

        // Wrap ETH to WETH
        const IWETH = [
            "function deposit() payable",
            "function transfer(address to, uint amount) returns (bool)",
            "function balanceOf(address) view returns (uint)"
        ];
        const wethContract = new ethers.Contract(WETH, IWETH, wethWhaleSigner);

        await wethContract.deposit({ value: ethers.parseEther("20") });
        console.log("Whale wrapped 20 ETH to WETH");

        await wethContract.transfer(targetUser.address, ethers.parseEther("10"));
        console.log("Target User funded with 10 WETH");

        // Connect to Aave Pool
        const pool = await ethers.getContractAt("IPool", POOL);

        // Restore weth variable for later use
        const weth = await ethers.getContractAt("IERC20", WETH);

        // Target User: Deposit 10 WETH as Collateral
        await weth.connect(targetUser).approve(POOL, ethers.parseEther("10"));
        await pool.connect(targetUser).supply(WETH, ethers.parseEther("10"), targetUser.address, 0);
        console.log("Target User supplied 10 WETH");

        // Target User: Borrow USDC (Debt)
        // 10 WETH * $2000 = $20,000 Collateral
        // Borrow $15,000 USDC
        const borrowAmount = ethers.parseUnits("15000", 6);
        await pool.connect(targetUser).borrow(USDC, borrowAmount, 2, 0, targetUser.address);
        console.log("Target User borrowed 15,000 USDC");

        // Check Health Factor
        const accountData = await pool.getUserAccountData(targetUser.address);
        console.log("Initial Health Factor:", ethers.formatUnits(accountData.healthFactor, 18));
    });

    it("Should liquidate the user when health factor drops below 1", async function () {
        const poolAddressesProvider = await ethers.getContractAt("IPoolAddressesProvider", POOL_ADDRESS_PROVIDER);
        const poolAddress = await liquidator.POOL();
        const pool = await ethers.getContractAt("IPool", poolAddress);

        // 1. Replace Price Oracle using ACL Admin Impersonation
        // IPoolAddressesProvider doesn't expose owner() in the interface usually.
        // We cast to Ownable or hardcode.
        const ownable = await ethers.getContractAt("@openzeppelin/contracts/access/Ownable.sol:Ownable", POOL_ADDRESS_PROVIDER);
        const providerOwner = await ownable.owner();

        await network.provider.request({
            method: "hardhat_impersonateAccount",
            params: [providerOwner],
        });

        // Fund the provider owner with ETH
        await network.provider.send("hardhat_setBalance", [
            providerOwner,
            "0x8AC7230489E80000", // 10 ETH
        ]);

        const providerOwnerSigner = await ethers.getSigner(providerOwner);

        // Set Price Oracle to our Mock Oracle
        await poolAddressesProvider.connect(providerOwnerSigner).setPriceOracle(await mockOracle.getAddress());
        console.log("Price Oracle replaced with Mock Oracle");

        // 2. Drop WETH Price to $1600 triggers liquidation ($16,000 Collateral vs $15,000 Debt -> HF approx 1.06 -> Drop more)
        // Drop to $1400: Collateral $14,000. Debt $15,000. HF < 1.
        const newPrice = 140000000000; // $1400 (8 decimals)
        await mockOracle.setAssetPrice(WETH, newPrice);

        // Verify HF dropped
        const accountData = await pool.getUserAccountData(targetUser.address);
        const healthFactor = ethers.formatUnits(accountData.healthFactor, 18);
        console.log("New Health Factor:", healthFactor);
        expect(parseFloat(healthFactor)).to.be.lessThan(1.0);

        // 3. Execute Liquidation
        console.log("Executing Liquidation...");

        // Borrow amount to cover = 50% of debt usually allowed (Liquidatable amount)
        // Or simpler: request flash loan for $7500 USDC
        const debtToCover = ethers.parseUnits("7500", 6);

        // Check Owner Balance before
        const usdc = await ethers.getContractAt("IERC20", USDC);
        const balanceBefore = await usdc.balanceOf(owner.address);

        await liquidator.connect(owner).requestFlashLoan(
            targetUser.address,
            USDC,
            WETH,
            debtToCover
        );

        const balanceAfter = await usdc.balanceOf(owner.address);
        const profit = balanceAfter - balanceBefore;
        console.log("Profit made (USDC):", ethers.formatUnits(profit, 6));

        expect(profit).to.be.gt(0);
    });
});
