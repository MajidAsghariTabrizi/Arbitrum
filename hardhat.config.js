require("@nomicfoundation/hardhat-toolbox");
require("dotenv").config();

module.exports = {
  solidity: "0.8.10",
  networks: {
    hardhat: {
      forking: {
        url: process.env.RPC_URL,
      },
    },
    // این بخش جدید است که شبکه آربیتروم را به هاردت معرفی می‌کند
    arbitrum: {
      url: process.env.RPC_URL,
      accounts: process.env.PRIVATE_KEY ? [process.env.PRIVATE_KEY] : [],
    }
  },
};