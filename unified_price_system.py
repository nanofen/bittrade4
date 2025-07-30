import argparse
import asyncio
import aiohttp
import csv
import os
import signal
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from web3 import Web3
from eth_utils import keccak

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

class UnifiedPriceSystem:
    def __init__(self):
        
        # トークン設定（Bybit取引量TOP20ベース + 主要DeFiトークン）
        self.tokens = {
            # 現在のトークン（ETH/BTC基準に変更）
            'WETH': TokenConfig('WETH', 18, 'ethereum', 'ETHUSDC', 'ETHUSDT'),
            'WBTC': TokenConfig('WBTC', 8, 'wrapped-bitcoin', 'BTCUSDC', 'BTCUSDT'),
            'LINK': TokenConfig('LINK', 18, 'chainlink', 'LINKUSDC', 'LINKUSDT'),
            'UNI': TokenConfig('UNI', 18, 'uniswap', 'UNIUSDC', 'UNIUSDT'),
            'MATIC': TokenConfig('MATIC', 18, 'matic-network', 'MATICUSDC', 'MATICUSDT'),
            
            # Bybit取引量上位の主要トークン
            'BNB': TokenConfig('BNB', 18, 'binancecoin', 'BNBUSDC', 'BNBUSDT'),
            'AVAX': TokenConfig('AVAX', 18, 'avalanche-2', 'AVAXUSDC', 'AVAXUSDT'),
            'SOL': TokenConfig('SOL', 9, 'solana', 'SOLUSDC', 'SOLUSDT'),
            'ADA': TokenConfig('ADA', 18, 'cardano', 'ADAUSDC', 'ADAUSDT'),
            'DOT': TokenConfig('DOT', 10, 'polkadot', 'DOTUSDC', 'DOTUSDT'),
            'DOGE': TokenConfig('DOGE', 8, 'dogecoin', 'DOGEUSDC', 'DOGEUSDT'),
            'LTC': TokenConfig('LTC', 8, 'litecoin', 'LTCUSDC', 'LTCUSDT'),
            'PEPE': TokenConfig('PEPE', 18, 'pepe', 'PEPEUSDC', 'PEPEUSDT'),
            
            # DeFi主要トークン
            'AAVE': TokenConfig('AAVE', 18, 'aave', 'AAVEUSDC', 'AAVEUSDT'),
            'CRV': TokenConfig('CRV', 18, 'curve-dao-token', 'CRVUSDC', 'CRVUSDT'),
            'COMP': TokenConfig('COMP', 18, 'compound-governance-token', 'COMPUSDC', 'COMPUSDT'),
            'MKR': TokenConfig('MKR', 18, 'maker', 'MKRUSDC', 'MKRUSDT'),
            'SUSHI': TokenConfig('SUSHI', 18, 'sushi', 'SUSHIUSDC', 'SUSHIUSDT'),
            
            # Layer 2トークン
            'OP': TokenConfig('OP', 18, 'optimism', 'OPUSDC', 'OPUSDT'),
            'ARB': TokenConfig('ARB', 18, 'arbitrum', 'ARBUSDC', 'ARBUSDT')
        }
        
        # CEX用のシンボルマッピング（流動性の高いペアを使用）
        self.cex_symbols = {
            'binance': {
                # 基本トークン（ETH/BTC基準）
                'WETH': 'ETHUSDC',   # ETHの方が流動性が高い
                'WBTC': 'BTCUSDC',   # BTCの方が流動性が高い
                'LINK': 'LINKUSDC',
                'UNI': 'UNIUSDC',
                'MATIC': 'MATICUSDC',
                
                # Bybit上位トークン
                'BNB': 'BNBUSDC',
                'AVAX': 'AVAXUSDC',
                'SOL': 'SOLUSDC',
                'ADA': 'ADAUSDC',
                'DOT': 'DOTUSDC',
                'DOGE': 'DOGEUSDC',
                'LTC': 'LTCUSDC',
                'PEPE': 'PEPEUSDC',
                
                # DeFiトークン
                'AAVE': 'AAVEUSDC',
                'CRV': 'CRVUSDC',
                'COMP': 'COMPUSDC',
                'MKR': 'MKRUSDC',
                'SUSHI': 'SUSHIUSDC',
                
                # Layer 2トークン
                'OP': 'OPUSDC',
                'ARB': 'ARBUSDC'
            },
            'bybit': {
                # 基本トークン（ETH/BTC基準）
                'WETH': 'ETHUSDT',   # ETHの方が流動性が高い
                'WBTC': 'BTCUSDT',   # BTCの方が流動性が高い
                'LINK': 'LINKUSDT',
                'UNI': 'UNIUSDT', 
                'MATIC': 'MATICUSDT',
                
                # Bybit上位トークン
                'BNB': 'BNBUSDT',
                'AVAX': 'AVAXUSDT',
                'SOL': 'SOLUSDT',
                'ADA': 'ADAUSDT',
                'DOT': 'DOTUSDT',
                'DOGE': 'DOGEUSDT',
                'LTC': 'LTCUSDT',
                'PEPE': 'PEPEUSDT',
                
                # DeFiトークン
                'AAVE': 'AAVEUSDT',
                'CRV': 'CRVUSDT',
                'COMP': 'COMPUSDT',
                'MKR': 'MKRUSDT',
                'SUSHI': 'SUSHIUSDT',
                
                # Layer 2トークン
                'OP': 'OPUSDT',
                'ARB': 'ARBUSDT'
            }
        }
        
        # チェーン設定
        self.chains = {
            'ethereum': ChainConfig(
                name='ethereum',
                rpc_url='https://eth.llamarpc.com',
                chain_id=1,
                usdc_address='0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
                uniswap_v3_factory='0x1F98431c8aD98523631AE4a59f267346ea31F984',
                tokens={
                    # 基本トークン
                    'WETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
                    'WBTC': '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
                    'LINK': '0x514910771AF9Ca656af840dff83E8264EcF986CA',
                    'UNI': '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
                    'MATIC': '0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0',
                    
                    # 主要DeFiトークン
                    'AAVE': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9',
                    'CRV': '0xD533a949740bb3306d119CC777fa900bA034cd52',
                    'COMP': '0xc00e94Cb662C3520282E6f5717214004A7f26888',
                    'MKR': '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2',
                    'SUSHI': '0x6B3595068778DD592e39A122f4f5a5cF09C90fE2',
                    
                    # Layer 1トークン（ラップ版）
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
                    # 基本トークン
                    'WETH': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
                    'WBTC': '0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f',
                    'LINK': '0xf97f4df75117a78c1A5a0DBb814Af92458539FB4',
                    'UNI': '0xFa7F8980b0f1E64A2062791cc3b0871572f1F7f0',
                    'MATIC': '0x561877b6b3DD7651313794e5F2894B2F18bE0766',
                    
                    # DeFiトークン
                    'AAVE': '0xba5DdD1f9d7F570dc94a51479a000E3BCE967196',
                    'CRV': '0x11cDb42B0EB46D95f990BeDD4695A6e3fA034978',
                    'COMP': '0x354A6dA3fcde098F8389cad84b0182725c6C91dE',
                    'MKR': '0x2e9a6Df78E42a30712c10a9Dc4b1C8656f8F2879',
                    'SUSHI': '0xd4d42F0b6DEF4CE0383636770eF773390d85c61A',
                    
                    # Layer 2トークン
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
                    # 基本トークン
                    'WETH': '0x4200000000000000000000000000000000000006',
                    'WBTC': '0x0555E30da8f98308EdB960aa94C0Db47230d2B9c',
                    'LINK': '0x88Fb150BDc53A65fe94Dea0c9BA0a6dAf8C6e196',
                    'UNI': '0xd3f1Da62CAFB7E7BC6531FF1ceF6F414291F03D3',
                    
                    # DeFiトークン
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
                    # 基本トークン
                    'WETH': '0x4200000000000000000000000000000000000006',
                    'WBTC': '0x68f180fcCe6836688e9084f035309E29Bf0A2095',
                    'LINK': '0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6',
                    'UNI': '0x6fd9d7AD17242c41f7131d257212c54A0e816691',
                    'MATIC': '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85',
                    
                    # DeFiトークン
                    'AAVE': '0x76FB31fb4af56892A25e32cFC43De717950c9278',
                    'CRV': '0xAdDb6A0412DE1BA0F936DCabc8b351d76aF731eF',
                    'COMP': '0x7e7d4467112689329f7E06571eD0E8CbAd4910eE',
                    'SUSHI': '0x3eaEb77b03dBc0F6321AE1b72b2E9aDb0F60112B',
                    
                    # Layer 2ネイティブトークン
                    'OP': '0x4200000000000000000000000000000000000042'
                }
            )
        }
        
        self.web3_instances = {}
        self.session = None
        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Uniswap V3の定数
        self.POOL_INIT_CODE_HASH = '0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54'
        
        # プールアドレスキャッシュ
        self.pool_address_cache = {}
        
        # レート制限用セマフォ（チェーンごと）
        self.rpc_semaphores = {}
        
        # CEX APIのレート制限用セマフォ
        self.binance_semaphore = None
        self.bybit_semaphore = None
        
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
        # HTTPタイムアウト（Bybit大量データ対応）
        timeout = aiohttp.ClientTimeout(total=10, connect=3)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
        # CEXのレート制限セマフォを初期化（高速化のため増加）
        self.binance_semaphore = asyncio.Semaphore(5)  # Binance: 5並行まで
        self.bybit_semaphore = asyncio.Semaphore(5)     # Bybit: 5並行まで
        
        # 新世代DEXのセマフォ
        self.hyperliquid_semaphore = asyncio.Semaphore(5)  # Hyperliquid: 5並行まで
        self.dydx_semaphore = asyncio.Semaphore(5)         # dYdX: 5並行まで
        
        # Web3インスタンス作成
        for chain_name, config in self.chains.items():
            try:
                w3 = Web3(Web3.HTTPProvider(config.rpc_url))
                if w3.is_connected():
                    self.web3_instances[chain_name] = w3
                    # チェーンごとにセマフォを作成（429エラー対策で大幅制限）
                    if chain_name == 'base':
                        self.rpc_semaphores[chain_name] = asyncio.Semaphore(1)  # Base: 1並行のみ
                    elif chain_name == 'ethereum':
                        self.rpc_semaphores[chain_name] = asyncio.Semaphore(2)  # Ethereum: 2並行のみ
                    else:
                        self.rpc_semaphores[chain_name] = asyncio.Semaphore(3)  # その他: 3並行
                    print(f"Connected to {chain_name}")
                else:
                    print(f"Failed to connect to {chain_name}")
            except Exception as e:
                print(f"Error connecting to {chain_name}: {e}")

    # ============= CoinGecko機能は削除 - historical_data_analyzer.pyを使用 =============

    # ============= Binance データ取得 =============

    async def fetch_all_binance_prices(self) -> List[PriceData]:
        """Binance APIから全トークン価格を一括取得（リトライ付き）"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                url = "https://api.binance.com/api/v3/ticker/price"
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        all_data = await response.json()
                        results = []
                        
                        # シンボルマッピングを作成（逆引き）
                        symbol_to_token = {v: k for k, v in self.cex_symbols['binance'].items()}
                        
                        for ticker in all_data:
                            symbol = ticker['symbol']
                            if symbol in symbol_to_token:
                                token = symbol_to_token[symbol]
                                results.append(PriceData(
                                    source='binance',
                                    chain='reference',
                                    token=token,
                                    price=Decimal(ticker['price']),
                                    timestamp=time.time()
                                ))
                        
                        print(f"Binance: Got {len(results)} prices")
                        return results
                        
                    elif response.status == 429:
                        print(f"Binance rate limit hit, attempt {attempt + 1}")
                        await asyncio.sleep(2 ** attempt)  # 指数バックオフ
                        continue
                        
            except asyncio.TimeoutError:
                print(f"Binance timeout, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
            except Exception as e:
                print(f"Binance error attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
        
        # 最終的に失敗した場合は個別取得を試行
        print("Binance bulk failed, trying individual requests...")
        return await self._fetch_binance_individual()
    
    async def _fetch_binance_individual(self) -> List[PriceData]:
        """Binance個別取得（フォールバック）"""
        results = []
        for token in list(self.tokens.keys())[:5]:  # 最初の5つだけ試行
            try:
                price_data = await self.fetch_binance_price(token)
                if price_data:
                    results.append(price_data)
            except:
                continue
        return results

    async def fetch_binance_price(self, token_symbol: str) -> Optional[PriceData]:
        """Binance APIから単一トークン価格を取得"""
        if token_symbol not in self.tokens:
            return None

        # セマフォでレート制限
        async with self.binance_semaphore:
            binance_symbol = self.cex_symbols['binance'][token_symbol]
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_symbol}"

            try:
                # CEXは0.2秒間隔でリクエスト（高速化）
                await asyncio.sleep(0.2)
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return PriceData(
                            source='binance',
                            chain='reference',
                            token=token_symbol,
                            price=Decimal(data['price']),
                            timestamp=time.time()
                        )
                    elif response.status == 429:
                        print(f"Binance rate limit hit for {token_symbol}")
                        await asyncio.sleep(1)  # レート制限時は1秒待機
                        return None
            except Exception as e:
                print(f"Error fetching Binance price for {token_symbol}: {e}")

            return None

    # ============= Bybit データ取得 =============

    async def fetch_all_bybit_prices(self) -> List[PriceData]:
        """Bybit APIから価格を一括取得（個別リクエスト方式）"""
        # Bybitは大量データで重いので、個別取得に変更
        print("Bybit: Using individual requests due to API size")
        
        results = []
        symbols = list(self.cex_symbols['bybit'].values())
        
        # 制限された並行リクエスト
        semaphore = asyncio.Semaphore(3)
        
        async def fetch_single_bybit(symbol):
            async with semaphore:
                try:
                    url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol}"
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get('retCode') == 0 and 'result' in data and 'list' in data['result']:
                                tickers = data['result']['list']
                                if tickers and tickers[0]['symbol'] == symbol:
                                    # 逆引きマッピング
                                    symbol_to_token = {v: k for k, v in self.cex_symbols['bybit'].items()}
                                    if symbol in symbol_to_token:
                                        token = symbol_to_token[symbol]
                                        return PriceData(
                                            source='bybit',
                                            chain='reference',
                                            token=token,
                                            price=Decimal(tickers[0]['lastPrice']),
                                            timestamp=time.time()
                                        )
                except Exception as e:
                    print(f"Bybit error for {symbol}: {e}")
                    pass
                return None
        
        # 全シンボルを並行取得
        tasks = [fetch_single_bybit(symbol) for symbol in symbols]
        results_raw = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 有効な結果のみ収集
        for result in results_raw:
            if isinstance(result, PriceData):
                results.append(result)
        
        print(f"Bybit: Got {len(results)} prices via individual requests")
        return results
    
    async def _fetch_bybit_individual(self) -> List[PriceData]:
        """Bybit個別取得（フォールバック）"""
        results = []
        for token in list(self.tokens.keys())[:5]:  # 最初の5つだけ試行
            try:
                price_data = await self.fetch_bybit_price(token)
                if price_data:
                    results.append(price_data)
            except:
                continue
        return results

    async def fetch_bybit_price(self, token_symbol: str) -> Optional[PriceData]:
        """Bybit APIから単一トークン価格を取得"""
        if token_symbol not in self.tokens:
            return None

        # セマフォでレート制限
        async with self.bybit_semaphore:
            bybit_symbol = self.cex_symbols['bybit'][token_symbol]
            url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={bybit_symbol}"

            try:
                # CEXは1秒間隔でリクエスト
                await asyncio.sleep(1.0)
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('retCode') == 0 and 'result' in data and 'list' in data['result']:
                            tickers = data['result']['list']
                            if tickers:
                                ticker = tickers[0]
                                return PriceData(
                                    source='bybit',
                                    chain='reference',
                                    token=token_symbol,
                                    price=Decimal(ticker['lastPrice']),
                                    timestamp=time.time()
                                )
                    elif response.status == 429:
                        print(f"Bybit rate limit hit for {token_symbol}")
                        await asyncio.sleep(1)  # レート制限時は1秒待機
                        return None
            except Exception as e:
                print(f"Error fetching Bybit price for {token_symbol}: {e}")

            return None

    # ============= Hyperliquid データ取得 =============

    async def fetch_all_hyperliquid_prices(self) -> List[PriceData]:
        """Hyperliquid APIから全トークン価格を一括取得"""
        try:
            url = "https://api.hyperliquid.xyz/info"
            payload = {"type": "allMids"}
            
            async with self.hyperliquid_semaphore:
                async with self.session.post(url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        
                        # 既存トークンとのマッピング
                        token_mappings = {
                            'BTC': 'WBTC',
                            'ETH': 'WETH', 
                            'LINK': 'LINK',
                            'UNI': 'UNI',
                            'MATIC': 'MATIC',
                            'AAVE': 'AAVE',
                            'CRV': 'CRV',
                            'COMP': 'COMP',
                            'MKR': 'MKR',
                            'SUSHI': 'SUSHI',
                            'OP': 'OP',
                            'ARB': 'ARB'
                        }
                        
                        for hl_symbol, price_str in data.items():
                            if hl_symbol in token_mappings:
                                token = token_mappings[hl_symbol]
                                results.append(PriceData(
                                    source='hyperliquid',
                                    chain='perpetual',
                                    token=token,
                                    price=Decimal(price_str),
                                    timestamp=time.time(),
                                    price_change_24h=Decimal('0.0002')  # 0.02% 手数料
                                ))
                        
                        print(f"Hyperliquid: Got {len(results)} prices")
                        return results
                        
                    elif response.status == 429:
                        print("Hyperliquid rate limit hit")
                        await asyncio.sleep(1)
                        
        except Exception as e:
            print(f"Error fetching Hyperliquid prices: {e}")
            
        return []

    # ============= dYdX v4 データ取得 =============

    async def fetch_all_dydx_prices(self) -> List[PriceData]:
        """dYdX v4 APIから全トークン価格を一括取得"""
        try:
            url = "https://indexer.dydx.trade/v4/perpetualMarkets"
            
            async with self.dydx_semaphore:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        
                        if 'markets' in data:
                            # トークンマッピング
                            token_mappings = {
                                'BTC-USD': 'WBTC',
                                'ETH-USD': 'WETH',
                                'LINK-USD': 'LINK', 
                                'UNI-USD': 'UNI',
                                'MATIC-USD': 'MATIC',
                                'AAVE-USD': 'AAVE',
                                'CRV-USD': 'CRV',
                                'COMP-USD': 'COMP',
                                'MKR-USD': 'MKR',
                                'SUSHI-USD': 'SUSHI',
                                'OP-USD': 'OP',
                                'ARB-USD': 'ARB'
                            }
                            
                            for market_id, market_data in data['markets'].items():
                                if market_id in token_mappings and 'oraclePrice' in market_data:
                                    token = token_mappings[market_id]
                                    results.append(PriceData(
                                        source='dydx_v4',
                                        chain='perpetual',
                                        token=token,
                                        price=Decimal(market_data['oraclePrice']),
                                        timestamp=time.time(),
                                        price_change_24h=Decimal('0.0002')  # 0.02% 手数料
                                    ))
                        
                        print(f"dYdX v4: Got {len(results)} prices")
                        return results
                        
                    elif response.status == 429:
                        print("dYdX rate limit hit")
                        await asyncio.sleep(1)
                        
        except Exception as e:
            print(f"Error fetching dYdX prices: {e}")
            
        return []

    # ============= Uniswap V3 最適化された価格取得 =============

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
            fee_levels = [3000, 500, 10000, 100]  # 0.3%, 0.05%, 1%, 0.01%
            
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
            return True, token_address, usdc_address  # token_is_token0, token0, token1
        else:
            return False, usdc_address, token_address  # token_is_token0, token0, token1

    async def fetch_uniswap_price(self, chain_name: str, token_symbol: str) -> Optional[PriceData]:
        """Uniswap V3から価格を取得（デバッグ版）"""
        if chain_name not in self.web3_instances or token_symbol not in self.tokens:
            return None

        if token_symbol not in self.chains[chain_name].tokens:
            return None

        # デバッグ機能を無効化（調査完了、異常値除外機能追加済み）
        debug_token = False

        # セマフォでレート制限
        async with self.rpc_semaphores[chain_name]:
            # RPC間隔を空ける（429エラー対策）
            await asyncio.sleep(0.1)
            try:
                w3 = self.web3_instances[chain_name]
                config = self.chains[chain_name]
                token_address = config.tokens[token_symbol]
                cache_key = f"{chain_name}_{token_symbol}"

                if debug_token:
                    print(f"\n=== デバッグ開始: {token_symbol} on {chain_name} ===")
                    print(f"Token Address: {token_address}")
                    print(f"USDC Address: {config.usdc_address}")

                # プールアドレスをキャッシュから取得または新規取得
                if cache_key not in self.pool_address_cache:
                    pool_result = await self._get_pool_address_cached(w3, config, token_address, cache_key)
                    if not pool_result or not pool_result[0]:
                        if debug_token:
                            print("プールアドレスが見つかりません")
                        return None

                    pool_address, pool_fee = pool_result

                    # トークン順序を事前決定してキャッシュ
                    token_is_token0, token0_addr, token1_addr = self._determine_token_order(token_address, config.usdc_address)

                    if debug_token:
                        print(f"Pool Address: {pool_address}")
                        print(f"Pool Fee: {pool_fee}")
                        print(f"Token0: {token0_addr}")
                        print(f"Token1: {token1_addr}")
                        print(f"Token is Token0: {token_is_token0}")

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

                # 価格取得（最適化されたRPCコール）
                pool = w3.eth.contract(
                    address=Web3.to_checksum_address(pool_address),
                    abi=self.pool_abi
                )

                # 非同期でslot0を取得
                slot0 = await asyncio.get_event_loop().run_in_executor(
                    None, pool.functions.slot0().call
                )
                sqrt_price_x96 = slot0[0]

                if debug_token:
                    print(f"Raw sqrtPriceX96: {sqrt_price_x96}")
                    print(f"Raw sqrtPriceX96 (hex): {hex(sqrt_price_x96)}")

                # Uniswap V3価格計算の完全再実装
                # sqrtPriceX96 は sqrt(price) * 2^96 で、price = token1 / token0 (both in wei units)
                
                if debug_token:
                    print(f"=== sqrtPriceX96の解析 ===")
                    print(f"Raw sqrtPriceX96: {sqrt_price_x96}")
                    print(f"Hex: {hex(sqrt_price_x96)}")

                # 正確な値でsqrt_priceを計算
                Q96 = 2**96
                sqrt_price_decimal = Decimal(sqrt_price_x96) / Decimal(Q96)
                
                # price = sqrt_price^2 (これは token1/token0 in raw units)
                price = sqrt_price_decimal * sqrt_price_decimal
                
                if debug_token:
                    print(f"sqrt_price = {sqrt_price_x96} / 2^96 = {sqrt_price_decimal}")
                    print(f"price (raw ratio) = sqrt_price^2 = {price}")

                token_config = self.tokens[token_symbol]
                usdc_decimals = 6
                token_decimals = token_config.decimals

                if debug_token:
                    print(f"\n=== トークン情報 ===")
                    print(f"Token decimals: {token_decimals}")
                    print(f"USDC decimals: {usdc_decimals}")

                # 現在の設定確認
                if debug_token:
                    print(f"\n=== プール設定 ===")
                    print(f"Token0 (smaller addr): {config.usdc_address if not token_is_token0 else token_address}")
                    print(f"Token1 (larger addr): {token_address if not token_is_token0 else config.usdc_address}")
                    print(f"Our token is token{'1' if not token_is_token0 else '0'}")
                    print(f"price = token1_amount / token0_amount")

                # 価格の意味を明確にする
                if token_is_token0:
                    # Token = token0, USDC = token1
                    # price = token1/token0 = USDC_amount/Token_amount (both in raw units)
                    # Token価格 = USDC_amount/Token_amount を real units に変換
                    # = (USDC_raw/10^6) / (Token_raw/10^18) 
                    # = (USDC_raw/Token_raw) * (10^18/10^6)
                    # = price * 10^12
                    
                    final_price = price * (Decimal(10) ** (token_decimals - usdc_decimals))
                    
                    if debug_token:
                        print(f"\n=== 価格計算 (Token=token0, USDC=token1) ===")
                        print(f"price = USDC_raw / Token_raw = {price}")
                        print(f"Token price in USD = price * 10^({token_decimals}-{usdc_decimals})")
                        print(f"Token price in USD = {price} * 10^{token_decimals - usdc_decimals}")
                        print(f"Token price in USD = {final_price}")
                        
                else:
                    # USDC = token0, Token = token1  
                    # price = token1/token0 = Token_amount/USDC_amount (both in raw units)
                    # 実際にはこれは逆！Token価格を求めるには逆数が必要
                    # Token価格 = USDC_amount / Token_amount を real units に変換
                    # = (USDC_raw/10^6) / (Token_raw/10^18)
                    # = (USDC_raw/Token_raw) * (10^18/10^6)
                    # = (1/price) * 10^12
                    
                    final_price = Decimal(1) / price * (Decimal(10) ** (token_decimals - usdc_decimals))
                    
                    if debug_token:
                        print(f"\n=== 価格計算 (USDC=token0, Token=token1) ===")
                        print(f"price = Token_raw / USDC_raw = {price}")
                        print(f"しかし我々が欲しいのは Token価格 = USDC per Token")
                        print(f"Token price in USD = (1/price) * 10^({token_decimals}-{usdc_decimals})")
                        print(f"Token price in USD = (1/{price}) * 10^{token_decimals - usdc_decimals}")
                        print(f"Token price in USD = {Decimal(1)/price} * {Decimal(10) ** (token_decimals - usdc_decimals)}")
                        print(f"Token price in USD = {final_price}")

                # 理論上の計算検証
                if debug_token:
                    print(f"\n=== 計算検証 ===")
                    if not token_is_token0:  # USDC=token0, ETH=token1の場合
                        # 期待値: 1 ETH = 3800 USDC くらい
                        # price = ETH_raw / USDC_raw なので、この比率は大きな数になるはず
                        # 実際の価格 = price * 10^(-12) で小数になってしまうのはおかしい
                        print(f"期待される動作:")
                        print(f"1 ETH = 3800 USDC の場合")
                        print(f"ETH_raw = 1 * 10^18, USDC_raw = 3800 * 10^6")
                        print(f"price = ETH_raw / USDC_raw = 10^18 / (3800 * 10^6) = 10^12 / 3800")
                        expected_price_ratio = Decimal(10**12) / Decimal(3800)
                        print(f"期待されるprice値: {expected_price_ratio}")
                        print(f"実際のprice値: {price}")
                        
                        if price > expected_price_ratio * 10:
                            print("警告: price値が期待値より大きすぎます")
                        elif price < expected_price_ratio / 10:
                            print("警告: price値が期待値より小さすぎます")

                # 価格の合理性チェック（異常値を除外）
                if final_price > Decimal(10**10) or final_price < Decimal(10**-10):
                    if debug_token:
                        print(f"警告: 異常な価格値 ${final_price} - データを破棄します")
                        print("=== デバッグ終了 ===\n")
                    return None

                if debug_token:
                    print(f"最終価格: ${final_price}")
                    print("=== デバッグ終了 ===\n")

                # bid/ask価格を計算（手数料とスリッページ考慮）
                pool_fee_decimal = Decimal(pool_fee) / Decimal(1000000)  # fee to decimal
                
                # 売り価格（受け取れる価格、手数料分低い）
                bid_price = final_price * (Decimal('1') - pool_fee_decimal)
                # 買い価格（支払う価格、手数料分高い）
                ask_price = final_price * (Decimal('1') + pool_fee_decimal)
                
                return PriceData(
                    source='uniswap_v3',
                    chain=chain_name,
                    token=token_symbol,
                    price=final_price,  # mid価格
                    timestamp=time.time(),
                    market_cap=bid_price,      # bid価格を一時的に格納
                    volume_24h=ask_price,      # ask価格を一時的に格納
                    price_change_24h=pool_fee_decimal  # 手数料を格納
                )

            except Exception as e:
                if debug_token:
                    print(f"ERROR in {token_symbol} on {chain_name}: {e}")
                    import traceback
                    traceback.print_exc()
                # エラーは簡潔にログ出力（デバッグ時のみ）
                if os.getenv('DEBUG'):
                    print(f"Error: {token_symbol} on {chain_name}: {e}")
                return None

    # ============= 統合データ取得 =============

    async def get_cex_prices(self) -> List[PriceData]:
        """CEX価格を高速一括取得"""
        print("Fetching CEX prices...")
        start_time = time.time()
        
        # BinanceとBybitを並行で一括取得
        tasks = [
            self.fetch_all_binance_prices(),
            self.fetch_all_bybit_prices()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_cex_prices = []
        for result in results:
            if isinstance(result, list):
                all_cex_prices.extend(result)
        
        end_time = time.time()
        print(f"CEX prices completed in {end_time - start_time:.2f}s")
        return all_cex_prices

    async def get_nextgen_dex_prices(self) -> List[PriceData]:
        """次世代DEX価格を高速一括取得"""
        print("Fetching Next-Gen DEX prices...")
        start_time = time.time()
        
        # HyperliquidとdYdX v4を並行で一括取得
        tasks = [
            self.fetch_all_hyperliquid_prices(),
            self.fetch_all_dydx_prices()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_nextgen_prices = []
        for result in results:
            if isinstance(result, list):
                all_nextgen_prices.extend(result)
        
        end_time = time.time()
        print(f"Next-Gen DEX prices completed in {end_time - start_time:.2f}s")
        return all_nextgen_prices

    async def get_dex_prices(self) -> List[PriceData]:
        """DEX価格を高速並行取得"""
        print("Fetching DEX prices...")
        start_time = time.time()
        
        # 全DEXタスクを並行実行
        dex_tasks = []
        for chain_name in self.chains.keys():
            for token_symbol in self.tokens.keys():
                dex_tasks.append(self.fetch_uniswap_price(chain_name, token_symbol))

        dex_results = await asyncio.gather(*dex_tasks, return_exceptions=True)
        
        all_dex_prices = []
        for result in dex_results:
            if isinstance(result, PriceData):
                all_dex_prices.append(result)
        
        end_time = time.time()
        print(f"DEX prices completed in {end_time - start_time:.2f}s")
        return all_dex_prices

    async def get_all_prices(self) -> Dict[str, List[PriceData]]:
        """全ソースから価格データを取得（次世代DEX対応版）"""
        print("Starting price collection...")
        start_time = time.time()

        # CEX、従来DEX、次世代DEXを並行実行
        cex_future = self.get_cex_prices()
        dex_future = self.get_dex_prices()
        nextgen_dex_future = self.get_nextgen_dex_prices()
        
        # 全ての結果を待つ
        cex_prices, dex_prices, nextgen_prices = await asyncio.gather(
            cex_future, dex_future, nextgen_dex_future
        )
        
        # 結果を統合
        all_results = {}
        all_prices_list = cex_prices + dex_prices + nextgen_prices
        
        for price_data in all_prices_list:
            token = price_data.token
            if token not in all_results:
                all_results[token] = []
            all_results[token].append(price_data)

        total_time = time.time() - start_time
        print(f"Total collection time: {total_time:.2f}s")
        return all_results

    # ============= CSV保存機能 =============

    def get_daily_csv_filename(self, date: datetime = None) -> str:
        """日付ベースのCSVファイル名を生成"""
        if date is None:
            date = datetime.now()
        filename = f"unified_prices_{date.strftime('%Y%m%d')}.csv"
        return os.path.join(self.data_dir, filename)

    def ensure_csv_header(self, filename: str):
        """CSVヘッダーを確保"""
        if not os.path.exists(filename):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'datetime', 'source', 'chain', 'token', 'price_usd'
                ])

    def save_prices_to_csv(self, all_prices: Dict[str, List[PriceData]]):
        """価格データをCSVに保存"""
        timestamp = time.time()
        dt = datetime.fromtimestamp(timestamp)
        csv_filename = self.get_daily_csv_filename(dt)

        self.ensure_csv_header(csv_filename)

        total_records = 0

        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            for token, price_list in all_prices.items():
                for price_data in price_list:
                    row = [
                        int(price_data.timestamp),
                        datetime.fromtimestamp(price_data.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                        price_data.source,
                        price_data.chain,
                        price_data.token,
                        float(price_data.price)
                    ]
                    writer.writerow(row)
                    total_records += 1

        print(f"Saved {total_records} price records to {csv_filename}")

    # ============= アービトラージ分析 =============

    def analyze_arbitrage_opportunities(self, all_prices: Dict[str, List[PriceData]]) -> List[Dict]:
        """アービトラージ機会を分析"""
        opportunities = []

        for token, price_list in all_prices.items():
            if len(price_list) < 2:
                continue

            # チェーン別価格（参照価格除く）
            chain_prices = {}
            for price_data in price_list:
                if price_data.chain != 'reference':
                    chain_prices[f"{price_data.source}_{price_data.chain}"] = price_data.price

            if len(chain_prices) < 2:
                continue

            prices = list(chain_prices.values())
            min_price = min(prices)
            max_price = max(prices)
            spread = max_price - min_price
            spread_pct = float(spread / min_price * 100)

            if spread_pct > 0.1:  # 0.1%以上の価格差
                max_source = [k for k, v in chain_prices.items() if v == max_price][0]
                min_source = [k for k, v in chain_prices.items() if v == min_price][0]

                opportunities.append({
                    'token': token,
                    'spread_usd': float(spread),
                    'spread_pct': spread_pct,
                    'sell_on': max_source,
                    'buy_on': min_source,
                    'max_price': float(max_price),
                    'min_price': float(min_price)
                })

        return sorted(opportunities, key=lambda x: x['spread_pct'], reverse=True)

    # ============= 表示機能 =============

    def display_prices(self, all_prices: Dict[str, List[PriceData]]):
        """価格データを表示"""
        print(f"\n{'='*80}")
        print(f"Price Data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")

        for token, price_list in all_prices.items():
            print(f"\n{token} Prices:")
            print("-" * 50)

            for price_data in price_list:
                source_chain = f"{price_data.source}_{price_data.chain}"
                
                if price_data.source == 'uniswap_v3' and price_data.market_cap and price_data.volume_24h:
                    # Uniswap V3の場合はbid/ask/spreadを表示
                    bid = price_data.market_cap
                    ask = price_data.volume_24h
                    spread_pct = float((ask - bid) / price_data.price * 100)
                    print(f"{source_chain:>20}: ${price_data.price:,.4f} (bid: ${bid:,.4f}, ask: ${ask:,.4f}, spread: {spread_pct:.3f}%)")
                elif price_data.source in ['hyperliquid', 'dydx_v4'] and price_data.price_change_24h:
                    # 次世代DEXの場合は手数料を表示
                    fee_pct = float(price_data.price_change_24h * 100)
                    print(f"{source_chain:>20}: ${price_data.price:,.4f} (fee: {fee_pct:.3f}%)")
                else:
                    # CEXの場合は通常表示
                    print(f"{source_chain:>20}: ${price_data.price:,.4f}")

            # 価格差分析
            chain_prices = [p.price for p in price_list if p.chain != 'reference']
            if len(chain_prices) > 1:
                min_price = min(chain_prices)
                max_price = max(chain_prices)
                spread_pct = float((max_price - min_price) / min_price * 100)
                print(f"{'Spread':>20}: {spread_pct:.3f}%")
    
    async def close(self):
        """リソースを閉じる"""
        if self.session:
            await self.session.close()

# ============= メイン実行部分 =============

class DataCollectionRunner:
    def __init__(self):
        self.system = UnifiedPriceSystem()
        self.running = True
        
    def signal_handler(self, signum, frame):
        self.running = False
    
    async def run_single_collection(self):
        """1回だけデータ収集"""
        await self.system.initialize()
        
        try:
            all_prices = await self.system.get_all_prices()
            
            if all_prices:
                self.system.display_prices(all_prices)
                self.system.save_prices_to_csv(all_prices)
                
                opportunities = self.system.analyze_arbitrage_opportunities(all_prices)
            
        finally:
            await self.system.close()
    
    async def run_debug_eth_only(self):
        """ETHのみのデバッグ収集"""
        await self.system.initialize()
        
        try:
            print("=== PEPE価格デバッグテスト ===")
            
            # Ethereum上のUniswap価格（デバッグ情報付き）
            uniswap_price = await self.system.fetch_uniswap_price('ethereum', 'PEPE')
            if uniswap_price:
                print(f"Uniswap V3 PEPE Price: ${uniswap_price.price}")
            else:
                print("Uniswap V3 PEPE価格取得失敗")
            
        finally:
            await self.system.close()
    
    async def run_continuous_collection(self, interval_seconds: float = 60):
        """継続的データ収集（高速対応）"""
        await self.system.initialize()
        
        pass
        
        collection_count = 0
        total_prices = 0
        start_time = time.time()
        
        try:
            while self.running:
                collection_start = time.time()
                all_prices = await self.system.get_all_prices()
                collection_end = time.time()
                
                if all_prices:
                    self.system.save_prices_to_csv(all_prices)
                    collection_count += 1
                    
                    # 価格数を集計
                    current_prices = sum(len(price_list) for price_list in all_prices.values())
                    total_prices += current_prices
                    
                    # 統計情報表示
                    collection_time = collection_end - collection_start
                    avg_collection_time = (collection_end - start_time) / collection_count
                    
                    pass
                
                if self.running:
                    # 高精度待機
                    if interval_seconds >= 1:
                        for i in range(int(interval_seconds)):
                            if not self.running:
                                break
                            await asyncio.sleep(1)
                        # 小数部分の待機
                        remaining = interval_seconds - int(interval_seconds)
                        if remaining > 0:
                            await asyncio.sleep(remaining)
                    else:
                        await asyncio.sleep(interval_seconds)
                
        except Exception:
            pass
        finally:
            await self.system.close()

async def main():
    parser = argparse.ArgumentParser(description='Unified Price System')
    parser.add_argument('interval', nargs='?', type=float, 
                       help='実行間隔（秒）。指定なしなら1回だけ実行')
    parser.add_argument('--debug-eth', action='store_true',
                       help='ETHのデバッグモードで実行')
    
    args = parser.parse_args()
    
    runner = DataCollectionRunner()
    
    # シグナルハンドラー設定
    try:
        signal.signal(signal.SIGINT, runner.signal_handler)
    except Exception:
        pass
    
    if args.debug_eth:
        # デバッグモード
        await runner.run_debug_eth_only()
    elif args.interval is None:
        # 引数なし -> 1回だけ実行
        await runner.run_single_collection()
    else:
        # 間隔指定 -> 継続実行
        if args.interval <= 0:
            return
        await runner.run_continuous_collection(args.interval)

if __name__ == "__main__":
    asyncio.run(main())