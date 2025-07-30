#!/usr/bin/env python3
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import datetime
import argparse

def analyze_leveraged_arbitrage(csv_file, time_window=30):
    """
    信用取引を活用した最適化アービトラージ戦略の分析
    
    戦略:
    1. DEXで現物購入 + CEXで先物ショート (同時実行)
    2. 価格変動リスクをヘッジ
    3. 送金コスト不要
    4. 高い資金効率
    """
    print(f"ファイルを読み込み中: {csv_file}")
    df = pd.read_csv(csv_file)
    
    print(f"\n=== 信用取引活用型アービトラージ分析 ===")
    print(f"総レコード数: {len(df)}")
    print(f"時間範囲: {df['datetime'].min()} ～ {df['datetime'].max()}")
    print(f"対象トークン: {', '.join(df['token'].unique())}")
    print(f"タイムウィンドウ: {time_window}秒")
    
    leveraged_opportunities = []
    
    # タイムスタンプでソート
    df = df.sort_values('timestamp')
    
    # 各トークンごとに信用取引戦略を分析
    for token in df['token'].unique():
        token_data = df[df['token'] == token].copy()
        
        # 各データポイントについて、同時間帯の価格差をチェック
        for idx, row in token_data.iterrows():
            current_time = row['timestamp']
            
            # タイムウィンドウ内のデータを取得
            window_mask = (
                (token_data['timestamp'] >= current_time) & 
                (token_data['timestamp'] <= current_time + time_window)
            )
            
            window_data = token_data[window_mask]
            
            if len(window_data) >= 2:
                # 信用取引戦略の機会を分析
                opportunity = find_leveraged_opportunity(window_data, token)
                if opportunity:
                    leveraged_opportunities.append(opportunity)
    
    return df, pd.DataFrame(leveraged_opportunities)

def find_leveraged_opportunity(window_data, token):
    """
    信用取引を活用したアービトラージ機会を分析
    
    戦略詳細:
    - DEX (Optimism/Arbitrum/Base): 現物ロング
    - CEX (Binance): 先物ショート
    - 同時実行で価格変動リスク排除
    """
    
    # 信用取引の設定
    FUTURES_MARGIN_RATE = 0.1      # 先物証拠金率 10%
    FUTURES_FEE_RATE = 0.0002      # 先物取引手数料 0.02%
    FUNDING_RATE_HOURLY = 0.0001   # 資金調達料（時間あたり）
    DEX_FEE_RATE = 0.003           # DEX手数料 0.3%
    GAS_COST = 3.0                 # DEXガス代

    
    best_opportunity = None
    max_profit = 0
    
    # CEX(Binance)価格を基準に、DEX価格との差を探す
    binance_data = window_data[window_data['source'] == 'bybit']
    dex_data = window_data[(window_data['source'] != 'binance')]
    #print("a",binance_data)
    #

    print("b",dex_data)
    
    for _, binance_row in binance_data.iterrows():
        binance_price = binance_row['price_usd']
        binance_time = binance_row['timestamp']
        
        # 近い時間帯のDEX価格を探す（±5分以内）
        time_window = 300  # 5分
        nearby_dex = dex_data[
            (dex_data['timestamp'] >= binance_time - time_window) &
            (dex_data['timestamp'] <= binance_time + time_window)
        ]
        
        for _, dex_row in nearby_dex.iterrows():
            dex_price = dex_row['price_usd']
            dex_time = dex_row['timestamp']
            
            # 時間差を記録
            time_diff = abs(binance_time - dex_time)
            
            # 戦略の方向性を決定
            if dex_price < binance_price:
                # DEXで買い + Binanceでショート
                buy_price = dex_price
                sell_price = binance_price
                buy_platform = f"{dex_row['chain']} DEX"
                sell_platform = "Binance Futures"
                strategy = "DEX Long + CEX Short"
            elif binance_price < dex_price:
                # Binanceで買い + DEXでショートは困難（DEXショートは複雑）
                continue
            else:
                continue
            
            # 最低価格差チェック（0.1%以上に緩和）
            price_diff_pct = ((sell_price - buy_price) / buy_price) * 100
            if price_diff_pct < 0.05:
                continue
            
            # 利益計算
            investment = 1000  # $1000での取引
            
            # Step 1: DEXで現物購入
            dex_fee = investment * DEX_FEE_RATE
            tokens_bought = (investment - dex_fee - GAS_COST) / buy_price
            
            # Step 2: Binance先物でショート
            futures_position_size = tokens_bought * sell_price
            futures_margin = futures_position_size * FUTURES_MARGIN_RATE
            futures_fee = futures_position_size * FUTURES_FEE_RATE

            # 必要資金チェック
            total_required = investment + futures_margin + futures_fee
            
            # Step 3: Option A - 同時決済による完了プロセス
            # 想定：価格差が50%縮小した時点で決済
            convergence_price = (buy_price + sell_price) / 2
            
            # DEX側: UNI → USDC売却
            dex_sell_proceeds = tokens_bought * convergence_price * (1 - DEX_FEE_RATE) - GAS_COST
            
            # CEX側: UNI先物ショート決済
            futures_pnl = tokens_bought * (sell_price - convergence_price)
            futures_close_fee = futures_position_size * FUTURES_FEE_RATE
            funding_cost = futures_position_size * FUNDING_RATE_HOURLY  # 1時間想定
            
            # 最終残高計算（Option A）
            dex_final_usdc = dex_sell_proceeds  # DEXチェーンの最終USDC
            cex_final_usdc = investment + futures_pnl - futures_close_fee - funding_cost  # CEXの最終USDC
            
            # 週次リバランスコスト（1/7で按分）
            weekly_rebalance_cost = 8.0  # 週1回のUSDC移動コスト
            daily_rebalance_cost = weekly_rebalance_cost / 7
            
            # 総利益計算（リバランスコスト含む）
            total_final = dex_final_usdc + cex_final_usdc
            final_profit = total_final - (investment * 2) - daily_rebalance_cost
            
            if final_profit > max_profit:
                max_profit = final_profit
                
                best_opportunity = {
                    'token': token,
                    'timestamp': binance_time,
                    'datetime': binance_row['datetime'],
                    'strategy': strategy,
                    'buy_platform': buy_platform,
                    'sell_platform': sell_platform,
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'convergence_price': convergence_price,
                    'price_diff_pct': price_diff_pct,
                    'investment_per_side': investment,
                    'total_investment': investment * 2,
                    'tokens_bought': tokens_bought,
                    'futures_margin': futures_margin,
                    'required_capital': total_required,
                    'dex_final_usdc': dex_final_usdc,
                    'cex_final_usdc': cex_final_usdc,
                    'total_final_usdc': total_final,
                    'futures_pnl': futures_pnl,
                    'total_fees': dex_fee + futures_fee + futures_close_fee + funding_cost + GAS_COST * 2 + daily_rebalance_cost,
                    'rebalance_cost_daily': daily_rebalance_cost,
                    'final_profit': final_profit,
                    'profit_pct': (final_profit / (investment * 2)) * 100,
                    'time_diff': time_diff,
                    'capital_efficiency': (final_profit / total_required) * 100
                }
    
    return best_opportunity if max_profit > 0 else None

def analyze_complete_arbitrage_cycle(csv_file, time_window=30):
    """
    完全なアービトラージサイクル（往復取引）を分析
    
    Args:
        csv_file: CSVファイルのパス  
        time_window: 価格比較用のタイムウィンドウ（秒）
    """
    print(f"ファイルを読み込み中: {csv_file}")
    df = pd.read_csv(csv_file)
    
    # データの基本情報
    print(f"\n=== データ概要 ===")
    print(f"総レコード数: {len(df)}")
    print(f"時間範囲: {df['datetime'].min()} ～ {df['datetime'].max()}")
    print(f"対象トークン: {', '.join(df['token'].unique())}")
    print(f"データソース: {', '.join(df['source'].unique())}")
    print(f"対象チェーン: {', '.join(df['chain'].unique())}")
    print(f"タイムウィンドウ: {time_window}秒")
    
    complete_arbitrage_opportunities = []
    
    # タイムスタンプでソート
    df = df.sort_values('timestamp')
    
    # 各トークンごとに完全なアービトラージサイクルを分析
    for token in df['token'].unique():
        token_data = df[df['token'] == token].copy()
        
        # 各データポイントについて、完全なサイクルの可能性をチェック
        for idx, row in token_data.iterrows():
            current_time = row['timestamp']
            
            # タイムウィンドウ内のデータを取得
            window_mask = (
                (token_data['timestamp'] >= current_time) & 
                (token_data['timestamp'] <= current_time + time_window)
            )
            
            window_data = token_data[window_mask]
            
            if len(window_data) >= 2:
                # 完全なアービトラージサイクルを探索
                cycle = find_profitable_cycle(window_data, token)
                if cycle:
                    complete_arbitrage_opportunities.append(cycle)
    
    return df, pd.DataFrame(complete_arbitrage_opportunities)

def find_profitable_cycle(window_data, token):
    """
    利益の出る完全なアービトラージサイクルを見つける
    
    完全サイクル：
    1. Chain A (USDC) → Chain A (Token) 
    2. Chain A (Token) → Chain B (Token) [送金/ブリッジ]
    3. Chain B (Token) → Chain B (USDC)
    4. Chain B (USDC) → Chain A (USDC) [送金/ブリッジ戻し]
    """
    
    # 現実的な送金コストとガス代の設定
    
    # ネットワーク間送金コスト（実測ベース）
    TRANSFER_COSTS = {
        # L2 → CEX (トークン送金)
        ('arbitrum', 'reference'): 3.0,   # Arbitrum → Binance
        ('optimism', 'reference'): 2.5,   # Optimism → Binance  
        ('base', 'reference'): 2.0,       # Base → Binance
        
        # CEX → L2 (USDC戻し送金) - より高額
        ('reference', 'arbitrum'): 8.0,   # Binance → Arbitrum
        ('reference', 'optimism'): 7.5,   # Binance → Optimism
        ('reference', 'base'): 6.0,       # Binance → Base
        
        # L2間ブリッジ（トークン）
        ('arbitrum', 'optimism'): 12.0,
        ('arbitrum', 'base'): 10.0,
        ('optimism', 'base'): 8.0,
        ('optimism', 'arbitrum'): 12.0,
        ('base', 'arbitrum'): 10.0,
        ('base', 'optimism'): 8.0,
        
        # L2間USDC戻し - さらに高額
        ('arbitrum', 'optimism'): 15.0,
        ('arbitrum', 'base'): 13.0,
        ('optimism', 'base'): 11.0,
        ('optimism', 'arbitrum'): 15.0,
        ('base', 'arbitrum'): 13.0,
        ('base', 'optimism'): 11.0,
    }
    
    GAS_COST_PER_TX = 2.0   # 各取引のガス代
    DEX_FEE_PCT = 0.003     # DEX取引手数料 0.3%
    CEX_FEE_PCT = 0.001     # CEX取引手数料 0.1%
    
    # Binanceの最低送金額制限
    MIN_TRANSFER_AMOUNTS = {
        'UNI': 0.1,     # UNI最低送金額
        'WETH': 0.001,  # WETH最低送金額
        'WBTC': 0.0001, # WBTC最低送金額
        'LINK': 0.1,    # LINK最低送金額
        'MATIC': 1.0    # MATIC最低送金額
    }
    
    best_cycle = None
    max_profit = 0
    
    # 全ての価格ペアを試す
    for i, buy_data in window_data.iterrows():
        for j, sell_data in window_data.iterrows():
            if i >= j:  # 時系列順序を保つ
                continue
                
            # 異なるチェーン/ソース間でのみアービトラージを検討
            if buy_data['source'] == sell_data['source'] and buy_data['chain'] == sell_data['chain']:
                continue
            
            buy_price = buy_data['price_usd']
            sell_price = sell_data['price_usd']
            
            # 最低限の価格差がある場合のみ計算
            if sell_price <= buy_price * 1.01:  # 1%以上の差が必要（送金コスト考慮）
                continue
            
            # 完全サイクルの利益計算
            investment = 1000  # $1000の投資額
            
            # Step 1: USDC → Token (買いチェーンで)
            buy_fee_rate = CEX_FEE_PCT if buy_data['source'] == 'binance' else DEX_FEE_PCT
            buy_gas = GAS_COST_PER_TX
            net_investment = investment - buy_gas
            tokens_bought = (net_investment * (1 - buy_fee_rate)) / buy_price
            
            # 最低送金額チェック
            min_transfer = MIN_TRANSFER_AMOUNTS.get(token, 0.1)
            if tokens_bought < min_transfer:
                continue
            
            # Step 2: Token送金コスト
            transfer_key = (buy_data['chain'], sell_data['chain'])
            token_transfer_cost = TRANSFER_COSTS.get(transfer_key, 0)
            
            # 同一チェーン/ソースの場合は送金不要
            if buy_data['chain'] == sell_data['chain'] and buy_data['source'] == sell_data['source']:
                token_transfer_cost = 0
                
            tokens_after_transfer = tokens_bought
            
            # Step 3: Token → USDC (売りチェーンで)  
            sell_fee_rate = CEX_FEE_PCT if sell_data['source'] == 'binance' else DEX_FEE_PCT
            sell_gas = GAS_COST_PER_TX
            usdc_received = (tokens_after_transfer * sell_price * (1 - sell_fee_rate)) - sell_gas
            
            # Step 4: USDC戻し送金コスト
            usdc_return_key = (sell_data['chain'], buy_data['chain'])
            usdc_return_cost = TRANSFER_COSTS.get(usdc_return_key, 0)
            
            # 同一チェーン/ソースの場合は戻し送金不要
            if buy_data['chain'] == sell_data['chain'] and buy_data['source'] == sell_data['source']:
                usdc_return_cost = 0
            
            usdc_final = usdc_received - usdc_return_cost
            
            # 総コスト計算
            total_costs = (
                buy_gas +                    # 購入時ガス代
                token_transfer_cost +        # トークン送金コスト
                sell_gas +                   # 売却時ガス代
                usdc_return_cost +           # USDC戻し送金コスト
                (investment * buy_fee_rate) +  # 購入手数料
                (tokens_bought * buy_price * sell_fee_rate)  # 売却手数料
            )
            
            final_profit = usdc_final - investment
            
            if final_profit > max_profit:
                max_profit = final_profit
                time_diff = sell_data['timestamp'] - buy_data['timestamp']
                
                # 送金が必要かどうかの判定
                needs_transfer = buy_data['chain'] != sell_data['chain'] or buy_data['source'] != sell_data['source']
                
                best_cycle = {
                    'token': token,
                    'timestamp': buy_data['timestamp'],
                    'datetime': buy_data['datetime'],
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'buy_source': buy_data['source'],
                    'buy_chain': buy_data['chain'],
                    'sell_source': sell_data['source'], 
                    'sell_chain': sell_data['chain'],
                    'investment': investment,
                    'tokens_bought': tokens_bought,
                    'tokens_after_transfer': tokens_after_transfer,
                    'usdc_received': usdc_received,
                    'usdc_final': usdc_final,
                    'total_costs': total_costs,
                    'token_transfer_cost': token_transfer_cost,
                    'usdc_return_cost': usdc_return_cost,
                    'final_profit': final_profit,
                    'profit_pct': (final_profit / investment) * 100,
                    'time_diff': time_diff,
                    'needs_transfer': needs_transfer,
                    'transfer_type': f"{buy_data['chain']}→{sell_data['chain']}" if needs_transfer else "同一プラットフォーム"
                }
    
    return best_cycle if max_profit > 0 else None

def load_and_analyze_arbitrage(csv_file, time_window=30):
    """
    unified_prices CSVファイルを読み込み、タイムウィンドウを考慮してアービトラージ機会を分析する
    
    Args:
        csv_file: CSVファイルのパス
        time_window: 価格比較用のタイムウィンドウ（秒）
    """
    print(f"ファイルを読み込み中: {csv_file}")
    df = pd.read_csv(csv_file)
    
    # データの基本情報
    print(f"\n=== データ概要 ===")
    print(f"総レコード数: {len(df)}")
    print(f"時間範囲: {df['datetime'].min()} ～ {df['datetime'].max()}")
    print(f"対象トークン: {', '.join(df['token'].unique())}")
    print(f"データソース: {', '.join(df['source'].unique())}")
    print(f"対象チェーン: {', '.join(df['chain'].unique())}")
    print(f"タイムウィンドウ: {time_window}秒")
    
    # アービトラージ分析 - タイムウィンドウを考慮
    arbitrage_opportunities = []
    
    # タイムスタンプでソート
    df = df.sort_values('timestamp')
    
    # 各トークンごとに分析
    for token in df['token'].unique():
        token_data = df[df['token'] == token].copy()
        print(token_data)
        
        # 各データポイントについて、タイムウィンドウ内の他の価格と比較
        for idx, row in token_data.iterrows():
            current_time = row['timestamp']
            
            # タイムウィンドウ内のデータを取得
            window_mask = (
                (token_data['timestamp'] >= current_time - time_window) & 
                (token_data['timestamp'] <= current_time + time_window) &
                (token_data.index != idx)  # 自分自身は除外
            )
            
            window_data = token_data[window_mask]
            print(current_time)
            print(window_data)
            
            if len(window_data) > 0:
                # 現在の価格と他の価格を比較
                current_price = row['price_usd']
                
                for _, other_row in window_data.iterrows():
                    other_price = other_row['price_usd']
                    
                    # 価格差計算（より安い価格を基準に）
                    min_price = min(current_price, other_price)
                    max_price = max(current_price, other_price)
                    
                    if min_price > 0:
                        spread_pct = ((max_price - min_price) / min_price) * 100
                        
                        # 0.05%以上のスプレッドを記録（より低い閾値）
                        if spread_pct >= 0.05:
                            # 時間差を計算
                            time_diff = abs(row['timestamp'] - other_row['timestamp'])
                            
                            # より安い方を買い、高い方を売り
                            if current_price < other_price:
                                buy_data, sell_data = row, other_row
                            else:
                                buy_data, sell_data = other_row, row
                            
                            # 重複チェック用のキー
                            arb_key = f"{token}_{min(row['timestamp'], other_row['timestamp'])}_{max(row['timestamp'], other_row['timestamp'])}"
                            
                            # 重複を避けるために既存の機会をチェック
                            existing = any(
                                opp['arb_key'] == arb_key 
                                for opp in arbitrage_opportunities
                            )
                            
                            if not existing:
                                arbitrage_opportunities.append({
                                    'arb_key': arb_key,
                                    'timestamp': row['timestamp'],
                                    'datetime': row['datetime'],
                                    'token': token,
                                    'min_price': min_price,
                                    'max_price': max_price,
                                    'spread_usd': max_price - min_price,
                                    'spread_pct': spread_pct,
                                    'time_diff': time_diff,
                                    'buy_source': buy_data['source'],
                                    'buy_chain': buy_data['chain'],
                                    'sell_source': sell_data['source'],
                                    'sell_chain': sell_data['chain'],
                                    'buy_timestamp': buy_data['timestamp'],
                                    'sell_timestamp': sell_data['timestamp']
                                })
    
    if not arbitrage_opportunities:
        print("\n=== アービトラージ機会分析 ===")
        print("有意なアービトラージ機会は見つかりませんでした（0.1%未満のスプレッド）")
        return df, pd.DataFrame()
    
    arb_df = pd.DataFrame(arbitrage_opportunities)
    
    print(f"\n=== アービトラージ機会分析 ===")
    print(f"検出された機会数: {len(arb_df)}")
    print(f"平均スプレッド: {arb_df['spread_pct'].mean():.3f}%")
    print(f"最大スプレッド: {arb_df['spread_pct'].max():.3f}%")
    print(f"中央値スプレッド: {arb_df['spread_pct'].median():.3f}%")
    print(f"平均時間差: {arb_df['time_diff'].mean():.1f}秒")
    print(f"最大時間差: {arb_df['time_diff'].max():.1f}秒")
    
    # トークンごとの分析
    print(f"\n=== トークン別分析 ===")
    for token in arb_df['token'].unique():
        token_arb = arb_df[arb_df['token'] == token]
        print(f"{token}:")
        print(f"  機会数: {len(token_arb)}")
        print(f"  平均スプレッド: {token_arb['spread_pct'].mean():.3f}%")
        print(f"  最大スプレッド: {token_arb['spread_pct'].max():.3f}%")
        print(f"  最大USD差額: ${token_arb['spread_usd'].max():.2f}")
        print(f"  平均時間差: {token_arb['time_diff'].mean():.1f}秒")
    
    # チェーン間の取引ペア分析
    print(f"\n=== 主要な取引ペア ===")
    trading_pairs = arb_df.apply(lambda x: f"{x['buy_chain']}→{x['sell_chain']}", axis=1)
    pair_counts = trading_pairs.value_counts().head(10)
    for pair, count in pair_counts.items():
        pair_data = arb_df[trading_pairs == pair]
        avg_spread = pair_data['spread_pct'].mean()
        avg_time_diff = pair_data['time_diff'].mean()
        print(f"{pair}: {count}回 (平均{avg_spread:.3f}%, 時間差{avg_time_diff:.1f}秒)")
    
    return df, arb_df

def filter_non_overlapping_trades(arb_df, execution_time=30):
    """
    時間的に重複しない取引のみを抽出
    
    Args:
        arb_df: アービトラージ機会のDataFrame
        execution_time: 1取引の実行時間（秒）
    """
    if len(arb_df) == 0:
        return arb_df
    
    # 利益順にソート
    sorted_arb = arb_df.sort_values('profit_per_trade', ascending=False).reset_index(drop=True)
    
    selected_trades = []
    occupied_times = []  # (start_time, end_time) のリスト
    
    for idx, trade in sorted_arb.iterrows():
        trade_start = min(trade['buy_timestamp'], trade['sell_timestamp'])
        trade_end = max(trade['buy_timestamp'], trade['sell_timestamp']) + execution_time
        
        # 既存の取引と時間的に重複していないかチェック
        is_overlapping = any(
            (trade_start < end_time and trade_end > start_time)
            for start_time, end_time in occupied_times
        )
        
        if not is_overlapping:
            selected_trades.append(trade)
            occupied_times.append((trade_start, trade_end))
    
    result_df = pd.DataFrame(selected_trades)
    print(f"\n=== 時間的重複除去 ===")
    print(f"重複除去前: {len(arb_df)}件")
    print(f"重複除去後: {len(result_df)}件")
    print(f"実行可能取引数: {len(result_df)}件")
    
    return result_df

def calculate_profit_potential(arb_df, investment_amount=1000):
    """
    収益ポテンシャルを計算（時間的重複を考慮）
    """
    if len(arb_df) == 0:
        print(f"\n=== 収益性分析（投資額: ${investment_amount}） ===")
        print("アービトラージ機会がないため、収益計算ができません。")
        return
    
    print(f"\n=== 収益性分析（投資額: ${investment_amount}） ===")
    
    # 手数料を考慮したより現実的な分析
    transaction_fee = 0.0025  # 0.25% (Binance 0.1% + Uniswap 0.3% + ガス代等)
    
    # 実際に利益が出る機会（スプレッド > 手数料）
    profitable_arb = arb_df[arb_df['spread_pct'] > (transaction_fee * 100 * 2)].copy()
    
    if len(profitable_arb) == 0:
        print(f"手数料（{transaction_fee*200:.1f}%）を考慮すると、利益の出る機会はありませんでした。")
        return
    
    # 各機会での利益計算
    profitable_arb['net_spread_pct'] = profitable_arb['spread_pct'] - (transaction_fee * 100 * 2)
    profitable_arb['profit_per_trade'] = investment_amount * (profitable_arb['net_spread_pct'] / 100)
    
    print(f"手数料考慮前の利益機会: {len(profitable_arb)} / {len(arb_df)}")
    
    # 時間的重複を除去
    executable_trades = filter_non_overlapping_trades(profitable_arb)
    
    if len(executable_trades) == 0:
        print("時間的重複を考慮すると、実行可能な取引はありませんでした。")
        return
    
    total_profit = executable_trades['profit_per_trade'].sum()
    avg_profit_per_trade = executable_trades['profit_per_trade'].mean()
    max_profit = executable_trades['profit_per_trade'].max()
    
    # 時間あたりの分析
    time_span_hours = (pd.to_datetime(arb_df['datetime']).max() - 
                      pd.to_datetime(arb_df['datetime']).min()).total_seconds() / 3600
    
    print(f"\n期間: {time_span_hours:.1f}時間")
    print(f"実行可能取引数: {len(executable_trades)}件")
    print(f"総利益（実行可能分のみ）: ${total_profit:.2f}")
    print(f"1取引あたり平均利益: ${avg_profit_per_trade:.2f}")
    print(f"最大利益（1取引）: ${max_profit:.2f}")
    
    if time_span_hours > 0:
        hourly_profit = total_profit / time_span_hours
        daily_profit = hourly_profit * 24
        print(f"時間あたり利益: ${hourly_profit:.2f}")
        print(f"1日あたり推定利益: ${daily_profit:.2f}")
        
        # 年利換算
        annual_return = (daily_profit * 365 / investment_amount) * 100
        print(f"年利換算: {annual_return:.1f}%")
    
    # 実際に実行された取引の詳細
    print_trade_details(executable_trades)

def print_trade_details(trades_df):
    """
    実際に実行される取引の詳細を表示
    """
    if len(trades_df) == 0:
        return
        
    print(f"\n=== 実行される取引の詳細 ===")
    
    # 時間順にソート
    trades_sorted = trades_df.sort_values('timestamp').reset_index(drop=True)
    
    print(f"総取引数: {len(trades_sorted)}件")
    print(f"取引パターン別内訳:")
    
    # 取引パターン別の集計
    pattern_summary = {}
    for idx, trade in trades_sorted.iterrows():
        pattern = f"{trade['buy_chain']} → {trade['sell_chain']}"
        if pattern not in pattern_summary:
            pattern_summary[pattern] = {'count': 0, 'total_profit': 0, 'tokens': set()}
        pattern_summary[pattern]['count'] += 1
        pattern_summary[pattern]['total_profit'] += trade['profit_per_trade']
        pattern_summary[pattern]['tokens'].add(trade['token'])
    
    for pattern, info in sorted(pattern_summary.items(), key=lambda x: x[1]['total_profit'], reverse=True):
        tokens_str = ', '.join(sorted(info['tokens']))
        print(f"  {pattern}: {info['count']}件, ${info['total_profit']:.2f}, トークン({tokens_str})")
    
    print(f"\n=== 実行される全取引リスト ===")
    for idx, trade in trades_sorted.iterrows():
        buy_time = datetime.fromtimestamp(trade['buy_timestamp']).strftime('%H:%M:%S')
        sell_time = datetime.fromtimestamp(trade['sell_timestamp']).strftime('%H:%M:%S')
        time_diff = abs(trade['buy_timestamp'] - trade['sell_timestamp'])
        
        print(f"{idx+1:2d}. {trade['datetime'][:16]} {trade['token']:4s}: "
              f"{trade['buy_chain']:9s}(${trade['min_price']:7.2f}, {buy_time}) → "
              f"{trade['sell_chain']:9s}(${trade['max_price']:7.2f}, {sell_time}) = "
              f"${trade['profit_per_trade']:5.2f} ({time_diff:.0f}秒差)")
    
    print(f"\n=== 収益性要約 ===")
    total_profit = trades_sorted['profit_per_trade'].sum()
    avg_profit = trades_sorted['profit_per_trade'].mean()
    print(f"総利益: ${total_profit:.2f}")
    print(f"平均利益/取引: ${avg_profit:.2f}")
    print(f"最高利益取引: ${trades_sorted['profit_per_trade'].max():.2f}")
    print(f"最低利益取引: ${trades_sorted['profit_per_trade'].min():.2f}")

def print_complete_cycle_details(cycles_df):
    """
    完全なアービトラージサイクルの詳細を表示（全送金コストを含む）
    """
    if len(cycles_df) == 0:
        print("\n利益の出る完全なアービトラージサイクルはありませんでした。")
        print("現実的な送金コストを考慮すると、アービトラージで利益を出すのは非常に困難です。")
        return
        
    print(f"\n=== 完全なアービトラージサイクル分析（全送金コスト込み） ===")
    print(f"利益の出るサイクル数: {len(cycles_df)}")
    
    total_profit = cycles_df['final_profit'].sum()
    avg_profit = cycles_df['final_profit'].mean()
    
    print(f"総利益: ${total_profit:.2f}")
    print(f"平均利益/サイクル: ${avg_profit:.2f}")
    print(f"最高利益: ${cycles_df['final_profit'].max():.2f}")
    print(f"最低利益: ${cycles_df['final_profit'].min():.2f}")
    
    # 送金タイプ別の分析
    transfer_needed = cycles_df[cycles_df['needs_transfer'] == True]
    no_transfer = cycles_df[cycles_df['needs_transfer'] == False]
    
    print(f"\n=== 送金タイプ別分析 ===")
    print(f"送金必要: {len(transfer_needed)}件 (平均利益: ${transfer_needed['final_profit'].mean():.2f})")
    print(f"送金不要: {len(no_transfer)}件 (平均利益: ${no_transfer['final_profit'].mean():.2f})")
    
    # コスト詳細分析
    if len(cycles_df) > 0:
        print(f"\n=== コスト詳細分析 ===")
        avg_token_cost = cycles_df['token_transfer_cost'].mean()
        avg_usdc_cost = cycles_df['usdc_return_cost'].mean()
        avg_total_cost = cycles_df['total_costs'].mean()
        
        print(f"平均トークン送金コスト: ${avg_token_cost:.2f}")
        print(f"平均USDC戻しコスト: ${avg_usdc_cost:.2f}")
        print(f"平均総コスト: ${avg_total_cost:.2f}")
    
    # 詳細なサイクル表示（上位20件のみ）
    print(f"\n=== 利益の出る全サイクル（上位20件） ===")
    cycles_sorted = cycles_df.sort_values('final_profit', ascending=False).reset_index(drop=True)
    
    display_count = min(20, len(cycles_sorted))
    for idx in range(display_count):
        cycle = cycles_sorted.iloc[idx]
        print(f"{idx+1:2d}. {cycle['datetime'][:16]} {cycle['token']} ({cycle['transfer_type']}):")
        print(f"    購入: {cycle['buy_chain']} ${cycle['buy_price']:.2f} → 売却: {cycle['sell_chain']} ${cycle['sell_price']:.2f}")
        print(f"    送金コスト: トークン${cycle['token_transfer_cost']:.2f} + USDC戻し${cycle['usdc_return_cost']:.2f}")
        print(f"    投資: ${cycle['investment']:.0f} → 最終利益: ${cycle['final_profit']:.2f} ({cycle['profit_pct']:.2f}%)")
        print(f"    実行時間: {cycle['time_diff']:.0f}秒")
        print()
    
    if len(cycles_sorted) > 20:
        print(f"... 他 {len(cycles_sorted) - 20} 件")

def print_leveraged_arbitrage_details(opportunities_df):
    """
    信用取引活用型アービトラージの詳細を表示
    """
    if len(opportunities_df) == 0:
        print("\n信用取引を活用したアービトラージ機会はありませんでした。")
        return
        
    print(f"\n=== 信用取引活用型アービトラージ分析結果 ===")
    print(f"利益機会数: {len(opportunities_df)}")
    
    total_profit = opportunities_df['final_profit'].sum()
    avg_profit = opportunities_df['final_profit'].mean()
    avg_capital_efficiency = opportunities_df['capital_efficiency'].mean()
    
    print(f"総利益: ${total_profit:.2f}")
    print(f"平均利益/取引: ${avg_profit:.2f}")
    print(f"最高利益: ${opportunities_df['final_profit'].max():.2f}")
    print(f"平均資金効率: {avg_capital_efficiency:.2f}%")
    
    # 戦略別分析
    strategy_summary = opportunities_df.groupby('strategy').agg({
        'final_profit': ['count', 'mean', 'sum'],
        'capital_efficiency': 'mean',
        'required_capital': 'mean'
    }).round(2)
    
    print(f"\n=== 戦略別分析 ===")
    for strategy in opportunities_df['strategy'].unique():
        strategy_data = opportunities_df[opportunities_df['strategy'] == strategy]
        print(f"{strategy}:")
        print(f"  機会数: {len(strategy_data)}")
        print(f"  平均利益: ${strategy_data['final_profit'].mean():.2f}")
        print(f"  平均必要資金: ${strategy_data['required_capital'].mean():.2f}")
        print(f"  平均資金効率: {strategy_data['capital_efficiency'].mean():.2f}%")
    
    # トークン別分析
    print(f"\n=== トークン別分析 ===")
    for token in opportunities_df['token'].unique():
        token_data = opportunities_df[opportunities_df['token'] == token]
        print(f"{token}: {len(token_data)}機会, 平均利益${token_data['final_profit'].mean():.2f}")
    
    # 詳細機会表示（上位15件）
    print(f"\n=== 最も利益性の高い機会（上位15件） ===")
    top_opportunities = opportunities_df.nlargest(15, 'final_profit')
    
    for idx, opp in top_opportunities.iterrows():
        print(f"{opp.name+1:2d}. {opp['datetime'][:16]} {opp['token']} ({opp['strategy']}):")
        print(f"    開始: {opp['buy_platform']} ${opp['buy_price']:.2f} vs {opp['sell_platform']} ${opp['sell_price']:.2f}")
        print(f"    価格差: {opp['price_diff_pct']:.2f}% → 収束価格: ${opp['convergence_price']:.2f}")
        print(f"    投資: ${opp['total_investment']:.0f} → 最終: ${opp['total_final_usdc']:.2f}")
        print(f"    DEX側: ${opp['dex_final_usdc']:.2f} | CEX側: ${opp['cex_final_usdc']:.2f}")
        print(f"    純利益: ${opp['final_profit']:.2f} ({opp['profit_pct']:.2f}%) | リバランス込み")
        print(f"    時間差: {opp['time_diff']:.0f}秒")
        print()

def main():
    parser = argparse.ArgumentParser(description='アービトラージ機会分析ツール')
    parser.add_argument('--file', default='./data/unified_prices_20250728.csv',
                       help='分析するCSVファイルのパス')
    parser.add_argument('--investment', type=float, default=100,
                       help='投資額（USD）')
    parser.add_argument('--window', type=int, default=30,
                       help='タイムウィンドウ（秒）')
    parser.add_argument('--strategy', choices=['leveraged', 'complete'], default='leveraged',
                       help='分析戦略（leveraged: 信用取引活用型, complete: 従来型）')
    
    args = parser.parse_args()
    
    try:
        if args.strategy == 'leveraged':
            print("=== 信用取引活用型アービトラージ分析を実行中 ===")
            df, opportunities_df = analyze_leveraged_arbitrage(args.file, args.window)
            print_leveraged_arbitrage_details(opportunities_df)
        else:
            print("=== 完全なアービトラージサイクル分析を実行中 ===")
            df, cycles_df = analyze_complete_arbitrage_cycle(args.file, args.window)
            print_complete_cycle_details(cycles_df)



        
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません: {args.file}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()