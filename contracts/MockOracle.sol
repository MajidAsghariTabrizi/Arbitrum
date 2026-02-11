// SPDX-License-Identifier: MIT
pragma solidity ^0.8.10;

contract MockOracle {
    mapping(address => uint256) public assetPrices;
    
    function setAssetPrice(address asset, uint256 price) external {
        assetPrices[asset] = price;
    }
    
    function getAssetPrice(address asset) external view returns (uint256) {
        return assetPrices[asset];
    }
}
