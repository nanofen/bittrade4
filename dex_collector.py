#!/usr/bin/env python3
"""
DEX（従来型分散取引所）専用のデータ収集システム
- Uniswap V3から1分間隔でデータ取得
- オンチェーンRPCコールのため低頻度で実行
"""

import asyncio
import aiohttp
import csv
import os
import signal
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from web3 import Web3

@dataclass
class TokenConfig:
    symbol: str
    decimals: int
    coingecko_id: str
    binance_symbol: str
    bybit_symbol: str

@dataclass
class ChainConfig:
    name: str
    rpc_url: str
    chain_id: int
    usdc_address: str
    uniswap_v3_factory: str
    tokens: Dict[str, str]
    pool_fee: int = 3000

@dataclass
class PriceData:
    source: str
    chain: str
    token: str
    price: Decimal
    timestamp: float
    market_cap: Optional[Decimal] = None
    volume_24h: Optional[Decimal] = None
    price_change_24h: Optional[Decimal] = None

class DEXCollector:
    def __init__(self):
        # トークン設定（既存のunified_price_systemと同じ）
        self.tokens = {
            'WETH': TokenConfig('WETH', 18, 'ethereum', 'ETHUSDC', 'ETHUSDT'),
            'WBTC': TokenConfig('WBTC', 8, 'wrapped-bitcoin', 'BTCUSDC', 'BTCUSDT'),
            'LINK': TokenConfig('LINK', 18, 'chainlink', 'LINKUSDC', 'LINKUSDT'),
            'UNI': TokenConfig('UNI', 18, 'uniswap', 'UNIUSDC', 'UNIUSDT'),
            'MATIC': TokenConfig('MATIC', 18, 'matic-network', 'MATICUSDC', 'MATICUSDT'),
            'BNB': TokenConfig('BNB', 18, 'binancecoin', 'BNBUSDC', 'BNBUSDT'),
            'AVAX': TokenConfig('AVAX', 18, 'avalanche-2', 'AVAXUSDC', 'AVAXUSDT'),
            'SOL': TokenConfig('SOL', 9, 'solana', 'SOLUSDC', 'SOLUSDT'),
            'ADA': TokenConfig('ADA', 18, 'cardano', 'ADAUSDC', 'ADAUSDT'),
            'DOT': TokenConfig('DOT', 10, 'polkadot', 'DOTUSDC', 'DOTUSDT'),
            'DOGE': TokenConfig('DOGE', 8, 'dogecoin', 'DOGEUSDC', 'DOGEUSDT'),
            'LTC': TokenConfig('LTC', 8, 'litecoin', 'LTCUSDC', 'LTCUSDT'),
            'PEPE': TokenConfig('PEPE', 18, 'pepe', 'PEPEUSDC', 'PEPEUSDT'),
            'AAVE': TokenConfig('AAVE', 18, 'aave', 'AAVEUSDC', 'AAVEUSDT'),
            'CRV': TokenConfig('CRV', 18, 'curve-dao-token', 'CRVUSDC', 'CRVUSDT'),
            'COMP': TokenConfig('COMP', 18, 'compound-governance-token', 'COMPUSDC', 'COMPUSDT'),
            'MKR': TokenConfig('MKR', 18, 'maker', 'MKRUSDC', 'MKRUSDT'),
            'SUSHI': TokenConfig('SUSHI', 18, 'sushi', 'SUSHIUSDC', 'SUSHIUSDT'),
            'OP': TokenConfig('OP', 18, 'optimism', 'OPUSDC', 'OPUSDT'),
            'ARB': TokenConfig('ARB', 18, 'arbitrum', 'ARBUSDC', 'ARBUSDT')
        }
        
        # チェーン設定（既存のunified_price_systemと同じ）
        self.chains = {
            'ethereum': ChainConfig(
                name='ethereum',
                rpc_url='https://eth.llamarpc.com',
                chain_id=1,
                usdc_address='0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
                uniswap_v3_factory='0x1F98431c8aD98523631AE4a59f267346ea31F984',
                tokens={
                    'WETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
                    'WBTC': '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
                    'LINK': '0x514910771AF9Ca656af840dff83E8264EcF986CA',
                    'UNI': '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
                    'MATIC': '0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0',
                    'AAVE': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9',
                    'CRV': '0xD533a949740bb3306d119CC777fa900bA034cd52',
                    'COMP': '0xc00e94Cb662C3520282E6f5717214004A7f26888',
                    'MKR': '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2',
                    'SUSHI': '0x6B3595068778DD592e39A122f4f5a5cF09C90fE2',
                    'BNB': '0xB8c77482e45F1F44dE1745F52C74426C631bDD52',
                    'AVAX': '0x85f138bfEE4ef8e540890CFb48F620571d67Eda3',
                    'SOL': '0xD31a59c85aE9D8edEFeC411D448f90841571b89c',
                    'ADA': '0x3EE2200Efb3400fAbB9AacF31297cBdD1d435D47',
                    'DOT': '0x7083609fCE4d1d8Dc0C979AAb8c869Ea2C873402',
                    'DOGE': '0x4206931337dc273a630d328dA6441786BfaD668f',
                    'LTC': '0x6c6EE5e31d828De241282B9606C8e98Ea48526E2',
                    'PEPE': '0x6982508145454Ce325dDbE47a25d4ec3d2311933'
                }
            ),
            'arbitrum': ChainConfig(
                name='arbitrum',
                rpc_url='https://arb1.arbitrum.io/rpc',
                chain_id=42161,
                usdc_address='0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
                uniswap_v3_factory='0x1F98431c8aD98523631AE4a59f267346ea31F984',
                tokens={
                    'WETH': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
                    'WBTC': '0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f',
                    'LINK': '0xf97f4df75117a78c1A5a0DBb814Af92458539FB4',
                    'UNI': '0xFa7F8980b0f1E64A2062791cc3b0871572f1F7f0',
                    'MATIC': '0x561877b6b3DD7651313794e5F2894B2F18bE0766',
                    'AAVE': '0xba5DdD1f9d7F570dc94a51479a000E3BCE967196',
                    'CRV': '0x11cDb42B0EB46D95f990BeDD4695A6e3fA034978',
                    'COMP': '0x354A6dA3fcde098F8389cad84b0182725c6C91dE',
                    'MKR': '0x2e9a6Df78E42a30712c10a9Dc4b1C8656f8F2879',
                    'SUSHI': '0xd4d42F0b6DEF4CE0383636770eF773390d85c61A',
                    'OP': '0x4200000000000000000000000000000000000042',
                    'ARB': '0x912CE59144191C1204E64559FE8253a0e49E6548'
                }
            ),
            'base': ChainConfig(
                name='base',
                rpc_url='https://mainnet.base.org',
                chain_id=8453,
                usdc_address='0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
                uniswap_v3_factory='0x33128a8fC17869897dcE68Ed026d694621f6FDfD',
                tokens={
                    'WETH': '0x4200000000000000000000000000000000000006',
                    'WBTC': '0x0555E30da8f98308EdB960aa94C0Db47230d2B9c',
                    'LINK': '0x88Fb150BDc53A65fe94Dea0c9BA0a6dAf8C6e196',
                    'UNI': '0xd3f1Da62CAFB7E7BC6531FF1ceF6F414291F03D3',
                    'AAVE': '0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB',
                    'CRV': '0x8Ee73c484A26e0A5df2Ee2a4960B789967dd0415',
                    'COMP': '0x9e1028F5F1D5eDE59748FFceE5532509976840E0',
                    'SUSHI': '0x7D49a065D17d6d4a55dc13649901fdBB98B2AFBA'
                }
            ),
            'optimism': ChainConfig(
                name='optimism',
                rpc_url='https://mainnet.optimism.io',
                chain_id=10,
                usdc_address='0x7F5c764cBc14f9669B88837ca1490cCa17c31607',
                uniswap_v3_factory='0x1F98431c8aD98523631AE4a59f267346ea31F984',
                tokens={
                    'WETH': '0x4200000000000000000000000000000000000006',
                    'WBTC': '0x68f180fcCe6836688e9084f035309E29Bf0A2095',
                    'LINK': '0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6',
                    'UNI': '0x6fd9d7AD17242c41f7131d257212c54A0e816691',
                    'MATIC': '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85',
                    'AAVE': '0x76FB31fb4af56892A25e32cFC43De717950c9278',
                    'CRV': '0xAdDb6A0412DE1BA0F936DCabc8b351d76aF731eF',
                    'COMP': '0x7e7d4467112689329f7E06571eD0E8CbAd4910eE',
                    'SUSHI': '0x3eaEb77b03dBc0F6321AE1b72b2E9aDb0F60112B',
                    'OP': '0x4200000000000000000000000000000000000042'
                }
            )
        }
        
        self.web3_instances = {}
        self.data_dir = "data/dex"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # プールアドレスキャッシュ
        self.pool_address_cache = {}
        
        # レート制限用セマフォ（チェーンごと）
        self.rpc_semaphores = {}
        
        # Uniswap V3の定数
        self.POOL_INIT_CODE_HASH = '0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54'
        
        # 共通ABI定義
        self.pool_abi = [{
            "inputs": [],
            "name": "slot0",
            "outputs": [
                {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
                {"internalType": "int24", "name": "tick", "type": "int24"},
                {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
                {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
                {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"},
                {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"},
                {"internalType": "bool", "name": "unlocked", "type": "bool"}
            ],
            "stateMutability": "view",
            "type": "function"
        }]
        
    async def initialize(self):
        """システム初期化"""
        # DEX専用にWeb3インスタンス作成
        for chain_name, config in self.chains.items():
            try:
                w3 = Web3(Web3.HTTPProvider(config.rpc_url))
                if w3.is_connected():
                    self.web3_instances[chain_name] = w3
                    # DEX特化のセマフォ設定（より保守的）
                    if chain_name == 'base':
                        self.rpc_semaphores[chain_name] = asyncio.Semaphore(1)
                    elif chain_name == 'ethereum':
                        self.rpc_semaphores[chain_name] = asyncio.Semaphore(2)
                    else:
                        self.rpc_semaphores[chain_name] = asyncio.Semaphore(3)
                    print(f"DEX: Connected to {chain_name}")
                else:
                    print(f"DEX: Failed to connect to {chain_name}")
            except Exception as e:
                print(f"DEX: Error connecting to {chain_name}: {e}")
    
    async def _get_pool_address_cached(self, w3: Web3, config, token_address: str, cache_key: str) -> Optional[Tuple[str, int]]:
        """プールアドレスを取得（複数feeレベル対応、キャッシュ付き）"""
        if cache_key in self.pool_address_cache:
            cached_data = self.pool_address_cache[cache_key]
            return cached_data['pool_address'], cached_data.get('pool_fee', config.pool_fee)

        try:
            factory_abi = [{
                "inputs": [
                    {"internalType": "address", "name": "tokenA", "type": "address"},
                    {"internalType": "address", "name": "tokenB", "type": "address"},
                    {"internalType": "uint24", "name": "fee", "type": "uint24"}
                ],
                "name": "getPool",
                "outputs": [{"internalType": "address", "name": "pool", "type": "address"}],
                "stateMutability": "view",
                "type": "function"
            }]

            factory = w3.eth.contract(
                address=Web3.to_checksum_address(config.uniswap_v3_factory),
                abi=factory_abi
            )

            # 複数のfeeレベルを試す（流動性の高い順）
            fee_levels = [3000, 500, 10000, 100]
            
            for fee in fee_levels:
                pool_address = factory.functions.getPool(
                    Web3.to_checksum_address(token_address),
                    Web3.to_checksum_address(config.usdc_address),
                    fee
                ).call()

                if pool_address != '0x0000000000000000000000000000000000000000':
                    return pool_address, fee

            return None, None

        except Exception as e:
            print(f"Error getting pool address: {e}")
            return None, None

    def _determine_token_order(self, token_address: str, usdc_address: str) -> Tuple[bool, str, str]:
        """トークンの順序を事前決定（token0が小さいアドレス）"""
        token_lower = token_address.lower()
        usdc_lower = usdc_address.lower()

        if token_lower < usdc_lower:
            return True, token_address, usdc_address
        else:
            return False, usdc_address, token_address

    async def fetch_uniswap_price(self, chain_name: str, token_symbol: str) -> Optional[PriceData]:
        """Uniswap V3から価格を取得"""
        if chain_name not in self.web3_instances or token_symbol not in self.tokens:
            return None

        if token_symbol not in self.chains[chain_name].tokens:
            return None

        # セマフォでレート制限
        async with self.rpc_semaphores[chain_name]:
            # RPC間隔を空ける（DEXは429エラー対策で長めに設定）
            await asyncio.sleep(0.2)
            try:
                w3 = self.web3_instances[chain_name]
                config = self.chains[chain_name]
                token_address = config.tokens[token_symbol]
                cache_key = f"{chain_name}_{token_symbol}"

                # プールアドレスをキャッシュから取得または新規取得
                if cache_key not in self.pool_address_cache:
                    pool_result = await self._get_pool_address_cached(w3, config, token_address, cache_key)
                    if not pool_result or not pool_result[0]:
                        return None

                    pool_address, pool_fee = pool_result

                    # トークン順序を事前決定してキャッシュ
                    token_is_token0, token0_addr, token1_addr = self._determine_token_order(token_address, config.usdc_address)

                    self.pool_address_cache[cache_key] = {
                        'pool_address': pool_address,
                        'pool_fee': pool_fee,
                        'token_is_token0': token_is_token0
                    }

                # キャッシュから情報取得
                pool_info = self.pool_address_cache[cache_key]
                pool_address = pool_info['pool_address']
                token_is_token0 = pool_info['token_is_token0']
                pool_fee = pool_info.get('pool_fee', config.pool_fee)

                # 価格取得
                pool = w3.eth.contract(
                    address=Web3.to_checksum_address(pool_address),
                    abi=self.pool_abi
                )

                # 非同期でslot0を取得
                slot0 = await asyncio.get_event_loop().run_in_executor(
                    None, pool.functions.slot0().call
                )
                sqrt_price_x96 = slot0[0]

                # Uniswap V3価格計算
                Q96 = 2**96
                sqrt_price_decimal = Decimal(sqrt_price_x96) / Decimal(Q96)
                price = sqrt_price_decimal * sqrt_price_decimal

                token_config = self.tokens[token_symbol]
                usdc_decimals = 6
                token_decimals = token_config.decimals

                # 価格計算
                if token_is_token0:
                    final_price = price * (Decimal(10) ** (token_decimals - usdc_decimals))
                else:
                    final_price = Decimal(1) / price * (Decimal(10) ** (token_decimals - usdc_decimals))

                # 価格の合理性チェック（異常値を除外）
                if final_price > Decimal(10**10) or final_price < Decimal(10**-10):
                    return None

                # bid/ask価格を計算（手数料とスリッページ考慮）
                pool_fee_decimal = Decimal(pool_fee) / Decimal(1000000)
                bid_price = final_price * (Decimal('1') - pool_fee_decimal)
                ask_price = final_price * (Decimal('1') + pool_fee_decimal)
                
                return PriceData(
                    source='uniswap_v3',
                    chain=chain_name,
                    token=token_symbol,
                    price=final_price,
                    timestamp=time.time(),
                    market_cap=bid_price,
                    volume_24h=ask_price,
                    price_change_24h=pool_fee_decimal
                )

            except Exception as e:
                return None

    async def get_all_dex_prices(self) -> List[PriceData]:
        """DEX価格を並行取得"""
        start_time = time.time()
        
        # 全DEXタスクを並行実行
        dex_tasks = []
        for chain_name in self.chains.keys():
            for token_symbol in self.tokens.keys():
                dex_tasks.append(self.fetch_uniswap_price(chain_name, token_symbol))

        dex_results = await asyncio.gather(*dex_tasks, return_exceptions=True)
        
        all_prices = []
        for result in dex_results:
            if isinstance(result, PriceData):
                all_prices.append(result)
        
        end_time = time.time()
        print(f"DEX prices: {len(all_prices)} records in {end_time - start_time:.2f}s")
        return all_prices
    
    def get_csv_filename(self) -> str:
        """DEX専用CSVファイル名を生成"""
        date = datetime.now()
        filename = f"dex_prices_{date.strftime('%Y%m%d')}.csv"
        return os.path.join(self.data_dir, filename)
    
    def ensure_csv_header(self, filename: str):
        """CSVヘッダーを確保"""
        if not os.path.exists(filename):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'datetime', 'source', 'chain', 'token', 
                    'price_usd', 'bid_price', 'ask_price', 'spread_pct'
                ])
    
    def save_prices_to_csv(self, prices: List[PriceData]):
        """価格データをCSVに保存"""
        if not prices:
            return
            
        csv_filename = self.get_csv_filename()
        self.ensure_csv_header(csv_filename)
        
        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            for price_data in prices:
                bid_price = float(price_data.market_cap) if price_data.market_cap else 0
                ask_price = float(price_data.volume_24h) if price_data.volume_24h else 0
                spread_pct = float(price_data.price_change_24h * 100) if price_data.price_change_24h else 0
                
                row = [
                    int(price_data.timestamp),
                    datetime.fromtimestamp(price_data.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                    price_data.source,
                    price_data.chain,
                    price_data.token,
                    float(price_data.price),
                    bid_price,
                    ask_price,
                    spread_pct
                ]
                writer.writerow(row)
        
        print(f"DEX: Saved {len(prices)} records to {csv_filename}")
    
    def display_prices(self, prices: List[PriceData]):
        """価格データを表示"""
        if not prices:
            return
            
        print(f"\n{'='*80}")
        print(f"DEX Prices at {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*80}")
        
        # トークン別にグループ化
        token_prices = {}
        for price in prices:
            if price.token not in token_prices:
                token_prices[price.token] = []
            token_prices[price.token].append(price)
        
        for token, price_list in sorted(token_prices.items()):
            print(f"\n{token}:")
            for price_data in price_list:
                if price_data.market_cap and price_data.volume_24h:
                    bid = price_data.market_cap
                    ask = price_data.volume_24h
                    spread_pct = float((ask - bid) / price_data.price * 100)
                    print(f"  {price_data.chain:>10}: ${price_data.price:>10,.4f} "
                          f"(bid: ${bid:,.4f}, ask: ${ask:,.4f}, spread: {spread_pct:.3f}%)")
                else:
                    print(f"  {price_data.chain:>10}: ${price_data.price:>10,.4f}")

class DEXRunner:
    def __init__(self):
        self.collector = DEXCollector()
        self.running = True
        
    def signal_handler(self, signum, frame):
        print("\nDEX Collector stopping...")
        self.running = False
    
    async def run_continuous(self, interval_seconds: float = 60.0):
        """継続的DEXデータ収集（1分間隔）"""
        await self.collector.initialize()
        
        collection_count = 0
        start_time = time.time()
        
        try:
            print(f"DEX Collector started (interval: {interval_seconds}s)")
            
            while self.running:
                collection_start = time.time()
                
                # DEX価格取得
                prices = await self.collector.get_all_dex_prices()
                
                if prices:
                    self.collector.save_prices_to_csv(prices)
                    # DEXは低頻度なので毎回表示
                    self.collector.display_prices(prices)
                    
                collection_count += 1
                collection_time = time.time() - collection_start
                
                # 統計表示
                avg_time = (time.time() - start_time) / collection_count
                print(f"\nDEX Stats: {collection_count} collections, "
                      f"this: {collection_time:.2f}s, avg: {avg_time:.2f}s")
                
                # 待機
                if self.running:
                    sleep_time = max(0, interval_seconds - collection_time)
                    if sleep_time > 0:
                        print(f"DEX: Waiting {sleep_time:.1f}s until next collection...")
                        await asyncio.sleep(sleep_time)
                    
        except Exception as e:
            print(f"DEX Collection error: {e}")
        finally:
            print("DEX Collector stopped")

async def main():
    """メイン実行"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DEX Low-frequency Price Collector')
    parser.add_argument('--interval', type=float, default=60.0,
                       help='収集間隔（秒、デフォルト: 60.0）')
    
    args = parser.parse_args()
    
    runner = DEXRunner()
    
    # シグナルハンドラー設定
    try:
        signal.signal(signal.SIGINT, runner.signal_handler)
        signal.signal(signal.SIGTERM, runner.signal_handler)
    except:
        pass
    
    await runner.run_continuous(args.interval)

if __name__ == "__main__":
    asyncio.run(main())