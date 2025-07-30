# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Communication Language

All interactions with the user should be conducted in Japanese (日本語). The user prefers Japanese for all explanations, code comments, and documentation.

## Project Overview

This is a cryptocurrency price monitoring and arbitrage analysis system called "bittrade4". The system collects price data from multiple sources (CoinGecko API, Binance, and on-chain DEXes) and analyzes arbitrage opportunities across different blockchain networks.

## Core Architecture

The system consists of two main components:

1. **UnifiedPriceSystem** (`unified_price_system.py`) - Real-time price collection and arbitrage detection
2. **HistoricalDataAnalyzer** (`historical_data_analyzer.py`) - Historical data collection and volatility analysis

### UnifiedPriceSystem
- Collects real-time prices from Binance and Uniswap V3 pools across multiple chains (Ethereum, Arbitrum, Base, Optimism)
- Uses Web3 to interact with on-chain Uniswap V3 contracts for DEX pricing
- Saves data to daily CSV files in `data/` directory
- Performs arbitrage opportunity analysis with configurable spread thresholds

### HistoricalDataAnalyzer  
- Fetches historical price data from both traditional CoinGecko API and DEX-specific endpoints
- Supports network-specific token contract addresses for cross-chain analysis
- Calculates volatility metrics and arbitrage opportunity scores
- Saves historical data to CSV files in `data/historical/` directory

## Key Data Structures

- **TokenConfig**: Symbol, decimals, CoinGecko ID, Binance trading pair
- **ChainConfig**: Network details, RPC URLs, USDC addresses, Uniswap factory contracts
- **PriceData**: Standardized price data with source, chain, timestamp, and optional market data

## Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

Required packages: web3>=6.0.0, aiohttp>=3.8.0, requests>=2.28.0, pandas>=1.5.0

## Running the Applications

### Real-time Price Monitoring
```bash
python3 unified_price_system.py
```
Choose from:
1. Single collection test
2. Continuous collection (60min intervals) 
3. Test collection (5min intervals)

### Historical Data Analysis
```bash
python3 historical_data_analyzer.py
```
Choose from:
1. Collect DEX historical data (30 days)
2. Collect traditional CoinGecko data (30 days)
3. Analyze arbitrage opportunities (30 days)
4. Full DEX analysis (collect + analyze)
5. Full traditional analysis (collect + analyze)

### Arbitrage Analysis
```bash
python3 arbitrage_analyzer.py
```
Analyzes collected price data for arbitrage opportunities and profit potential.

## API Configuration

The system requires a CoinGecko API key which is currently hardcoded in `historical_data_analyzer.py:301`. For DEX endpoints, the system uses CoinGecko's onchain API endpoints at `https://api.coingecko.com/api/v3/onchain`.

## Data Storage

- Real-time data: `data/unified_prices_YYYYMMDD.csv`
- Historical data: `data/historical/SYMBOL_[network_]historical_YYYYMMDD_YYYYMMDD.csv`
- OHLC data: `data/historical/SYMBOL_ohlc_YYYYMMDD_YYYYMMDD.csv`

## Network Support

Configured networks with contract addresses:
- Ethereum (eth)
- Arbitrum (arbitrum) 
- Base (base)
- Optimism (optimism)

Each network has specific USDC addresses and Uniswap V3 factory contracts configured in the ChainConfig objects.