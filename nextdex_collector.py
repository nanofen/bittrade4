#!/usr/bin/env python3
"""
NextDEX（次世代DEX）専用の高頻度データ収集システム
- Hyperliquid、dYdX v4から5秒間隔でデータ取得
- 軽量化のため従来DEX処理は除外
"""

import asyncio
import aiohttp
import csv
import os
import signal
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class TokenConfig:
    symbol: str
    decimals: int
    coingecko_id: str
    binance_symbol: str
    bybit_symbol: str

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

class NextDEXCollector:
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
        
        self.session = None
        self.data_dir = "data/nextdex"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # セマフォ
        self.hyperliquid_semaphore = None
        self.dydx_semaphore = None
        
    async def initialize(self):
        """システム初期化"""
        timeout = aiohttp.ClientTimeout(total=10, connect=3)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
        # 高速化のためセマフォ数を増加
        self.hyperliquid_semaphore = asyncio.Semaphore(8)
        self.dydx_semaphore = asyncio.Semaphore(8)
        
        print("NextDEX Collector initialized")
    
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
                        
                        # トークンマッピング
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
                            'ARB': 'ARB',
                            'SOL': 'SOL',
                            'AVAX': 'AVAX',
                            'ADA': 'ADA',
                            'DOT': 'DOT',
                            'DOGE': 'DOGE',
                            'LTC': 'LTC'
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
                        
                        return results
                        
                    elif response.status == 429:
                        await asyncio.sleep(1)
                        
        except Exception as e:
            print(f"Hyperliquid error: {e}")
            
        return []
    
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
                                'ARB-USD': 'ARB',
                                'SOL-USD': 'SOL',
                                'AVAX-USD': 'AVAX',
                                'ADA-USD': 'ADA',
                                'DOT-USD': 'DOT',
                                'DOGE-USD': 'DOGE',
                                'LTC-USD': 'LTC'
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
                        
                        return results
                        
                    elif response.status == 429:
                        await asyncio.sleep(1)
                        
        except Exception as e:
            print(f"dYdX error: {e}")
            
        return []
    
    async def get_all_nextdex_prices(self) -> List[PriceData]:
        """NextDEX価格を高速一括取得"""
        start_time = time.time()
        
        tasks = [
            self.fetch_all_hyperliquid_prices(),
            self.fetch_all_dydx_prices()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_prices = []
        for result in results:
            if isinstance(result, list):
                all_prices.extend(result)
        
        end_time = time.time()
        print(f"NextDEX prices: {len(all_prices)} records in {end_time - start_time:.2f}s")
        return all_prices
    
    def get_csv_filename(self) -> str:
        """NextDEX専用CSVファイル名を生成"""
        date = datetime.now()
        filename = f"nextdex_prices_{date.strftime('%Y%m%d')}.csv"
        return os.path.join(self.data_dir, filename)
    
    def ensure_csv_header(self, filename: str):
        """CSVヘッダーを確保"""
        if not os.path.exists(filename):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'datetime', 'source', 'chain', 'token', 'price_usd', 'fee_pct'
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
                fee_pct = float(price_data.price_change_24h * 100) if price_data.price_change_24h else 0
                row = [
                    int(price_data.timestamp),
                    datetime.fromtimestamp(price_data.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                    price_data.source,
                    price_data.chain,
                    price_data.token,
                    float(price_data.price),
                    fee_pct
                ]
                writer.writerow(row)
        
        print(f"NextDEX: Saved {len(prices)} records to {csv_filename}")
    
    def display_prices(self, prices: List[PriceData]):
        """価格データを表示"""
        if not prices:
            return
            
        print(f"\n{'='*60}")
        print(f"NextDEX Prices at {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        
        # トークン別にグループ化
        token_prices = {}
        for price in prices:
            if price.token not in token_prices:
                token_prices[price.token] = []
            token_prices[price.token].append(price)
        
        for token, price_list in sorted(token_prices.items()):
            print(f"\n{token}:")
            for price_data in price_list:
                fee_pct = float(price_data.price_change_24h * 100) if price_data.price_change_24h else 0
                print(f"  {price_data.source:>12}: ${price_data.price:>10,.4f} (fee: {fee_pct:.3f}%)")
            
            # スプレッド計算
            if len(price_list) > 1:
                prices_vals = [p.price for p in price_list]
                min_price = min(prices_vals)
                max_price = max(prices_vals)
                spread_pct = float((max_price - min_price) / min_price * 100)
                print(f"  {'spread':>12}: {spread_pct:>10.3f}%")
    
    async def close(self):
        """リソースを閉じる"""
        if self.session:
            await self.session.close()

class NextDEXRunner:
    def __init__(self):
        self.collector = NextDEXCollector()
        self.running = True
        
    def signal_handler(self, signum, frame):
        print("\nNextDEX Collector stopping...")
        self.running = False
    
    async def run_continuous(self, interval_seconds: float = 5.0):
        """継続的NextDEXデータ収集（5秒間隔）"""
        await self.collector.initialize()
        
        collection_count = 0
        start_time = time.time()
        
        try:
            print(f"NextDEX Collector started (interval: {interval_seconds}s)")
            
            while self.running:
                collection_start = time.time()
                
                # NextDEX価格取得
                prices = await self.collector.get_all_nextdex_prices()
                
                if prices:
                    self.collector.save_prices_to_csv(prices)
                    # 頻繁な表示を避けるため10回に1回だけ表示
                    if collection_count % 10 == 0:
                        self.collector.display_prices(prices)
                    
                collection_count += 1
                collection_time = time.time() - collection_start
                
                # 統計表示（100回に1回）
                if collection_count % 100 == 0:
                    avg_time = (time.time() - start_time) / collection_count
                    print(f"NextDEX Stats: {collection_count} collections, avg: {avg_time:.2f}s")
                
                # 待機
                if self.running:
                    sleep_time = max(0, interval_seconds - collection_time)
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                    
        except Exception as e:
            print(f"NextDEX Collection error: {e}")
        finally:
            await self.collector.close()
            print("NextDEX Collector stopped")

async def main():
    """メイン実行"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NextDEX High-frequency Price Collector')
    parser.add_argument('--interval', type=float, default=5.0,
                       help='収集間隔（秒、デフォルト: 5.0）')
    
    args = parser.parse_args()
    
    runner = NextDEXRunner()
    
    # シグナルハンドラー設定
    try:
        signal.signal(signal.SIGINT, runner.signal_handler)
        signal.signal(signal.SIGTERM, runner.signal_handler)
    except:
        pass
    
    await runner.run_continuous(args.interval)

if __name__ == "__main__":
    asyncio.run(main())