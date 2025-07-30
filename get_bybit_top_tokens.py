#!/usr/bin/env python3
# coding: utf-8

import requests
import json
from decimal import Decimal

def get_bybit_top_tokens(limit=25):
    """
    Bybitの24時間取引量ベースでTop仮想通貨を取得
    """
    url = "https://api.bybit.com/v5/market/tickers"
    params = {
        'category': 'spot'
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        if data.get('retCode') != 0:
            print(f"API Error: {data.get('retMsg')}")
            return []
        
        tickers = data['result']['list']
        
        # USDTペアのみフィルタリング
        usdt_pairs = []
        for ticker in tickers:
            symbol = ticker['symbol']
            if symbol.endswith('USDT') and symbol != 'USDT':
                # ステーブルコインを除外
                base_token = symbol.replace('USDT', '')
                if base_token not in ['USDC', 'BUSD', 'DAI', 'TUSD', 'FDUSD']:
                    try:
                        volume_24h = float(ticker['turnover24h'])  # 24時間取引量（USDT）
                        price = float(ticker['lastPrice'])
                        
                        usdt_pairs.append({
                            'symbol': symbol,
                            'base_token': base_token,
                            'price': price,
                            'volume_24h_usdt': volume_24h,
                            'volume_24h_base': float(ticker['volume24h']),
                            'price_change_24h': ticker['price24hPcnt']
                        })
                    except (ValueError, TypeError):
                        continue
        
        # 24時間取引量でソート
        top_tokens = sorted(usdt_pairs, key=lambda x: x['volume_24h_usdt'], reverse=True)[:limit]
        
        return top_tokens
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def main():
    print("=== Bybit Top仮想通貨（24時間取引量ベース） ===")
    
    top_tokens = get_bybit_top_tokens(25)
    
    if not top_tokens:
        print("データ取得に失敗しました")
        return
    
    print(f"{'順位':<4} {'トークン':<8} {'価格(USDT)':<12} {'24h取引量(USDT)':<18} {'24h変動':<10}")
    print("-" * 65)
    
    for i, token in enumerate(top_tokens, 1):
        volume_str = f"${token['volume_24h_usdt']:,.0f}"
        price_str = f"${token['price']:.4f}"
        change_str = f"{float(token['price_change_24h'])*100:.2f}%"
        
        print(f"{i:<4} {token['base_token']:<8} {price_str:<12} {volume_str:<18} {change_str:<10}")
    
    # TokenConfig用のコード生成
    print(f"\n=== unified_price_system.py用のコード ===")
    
    # 主要なトークンを選択（取引量とDeFiでの利用頻度を考慮）
    selected_tokens = []
    for token in top_tokens[:20]:
        base = token['base_token']
        # 一般的にDEXで取引されているトークンを優先
        if base in ['BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'XRP', 'DOT', 'AVAX', 
                   'LINK', 'UNI', 'MATIC', 'LTC', 'ATOM', 'NEAR', 'FTM',
                   'AAVE', 'CRV', 'COMP', 'MKR', 'YFI', 'SUSHI', '1INCH']:
            selected_tokens.append(token)
    
    print("# 主要トークン設定（DEXで取引可能なもの優先）")
    for token in selected_tokens[:15]:  # Top15に制限
        base = token['base_token']
        print(f"'{base}': TokenConfig('{base}', 18, 'coingecko_id', '{base}USDC', '{base}USDT'),")

if __name__ == "__main__":
    main()