# socket_hyperliquid_sdk.py
# HyperLiquid公式Python SDKを使用した実装

import asyncio
import json
import time
import pandas as pd
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

class Socket_PyBotters_HyperLiquid():
    
    # 定数
    TIMEOUT = 3600               
    EXTEND_TOKEN_TIME = 3000     
    SYMBOL = 'BTC-USD'
    
    PUBLIC_CHANNELS = ['allMids', 'notification', 'webData2']
    PRIVATE_CHANNELS = ['fills', 'user']
    
    MAX_OHLCV_CAPACITY = 60 * 60 * 48
    df_ohlcv = pd.DataFrame(
        columns=["exec_date", "Open", "High", "Low", "Close", "Volume", "timestamp"]
    ).set_index("exec_date")
    
    def __init__(self, keys):
        """HyperLiquid公式SDKを使用した初期化"""
        self.KEYS = keys
        if 'hyperliquid' in keys and keys['hyperliquid']:
            hl_data = keys['hyperliquid']
            
            if isinstance(hl_data, tuple) and len(hl_data) == 2:
                # (private_key, wallet_address) の場合
                self.private_key = hl_data[0]
                self.wallet_address = hl_data[1]
                print(f"HyperLiquid SDK: 直接指定アドレス使用 {self.wallet_address}")
            elif isinstance(hl_data, list) and len(hl_data) > 0:
                # 従来形式 [private_key] の場合
                self.private_key = hl_data[0]
                if len(hl_data) > 1:
                    self.wallet_address = hl_data[1]
                    print(f"HyperLiquid SDK: 直接指定アドレス使用 {self.wallet_address}")
                else:
                    # プライベートキーからアドレス自動計算
                    from eth_account import Account
                    account = Account.from_key(self.private_key)
                    self.wallet_address = account.address
                    print(f"HyperLiquid SDK: 自動計算アドレス使用 {self.wallet_address}")
            elif isinstance(hl_data, str):
                # 単一のプライベートキー
                self.private_key = hl_data
                from eth_account import Account
                account = Account.from_key(self.private_key)
                self.wallet_address = account.address
                print(f"HyperLiquid SDK: 自動計算アドレス使用 {self.wallet_address}")
            else:
                print("HyperLiquid SDK: 認証情報が設定されていません")
                
        # 公式SDKのクライアント初期化
        try:
            # HyperLiquid SDKの正しい初期化方法（LocalAccountを使用）
            from eth_account import Account
            if not hasattr(self, 'private_key'):
                raise Exception("Private key not set")
                
            account = Account.from_key(self.private_key)
            self.exchange = Exchange(account, base_url=constants.MAINNET_API_URL)
            self.info = Info(base_url=constants.MAINNET_API_URL)
            print("HyperLiquid SDK: クライアント初期化成功")
        except Exception as e:
            print(f"HyperLiquid SDK: 初期化エラー {e}")
            import traceback
            traceback.print_exc()
            self.exchange = None
            self.info = None
    
    # ------------------------------------------------ #
    # 注文関数 (公式SDK使用)
    # ------------------------------------------------ #
    
    async def buy_in(self, price, qty=None):
        """買い注文"""
        try:
            if not self.exchange:
                print("[ERROR] Exchange client not initialized")
                return None
                
            # 価格の精度を調整（HyperLiquid SDK要件）
            rounded_price = round(float(price), 2)
            rounded_qty = round(float(qty), 6)
            
            # HyperLiquid SDKの注文形式
            order_result = self.exchange.order(
                "BTC",  # coin
                True,   # is_buy
                rounded_qty,    # sz (size) - 精度調整済み
                rounded_price,  # px (price) - 精度調整済み
                {"limit": {"tif": "Gtc"}}  # order_type
            )
            print(f"買い注文結果: {order_result}")
            return order_result
        except Exception as e:
            print(f"買い注文エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def buy_out(self, price, exec_qty, reduce_only=True):
        """買いの決済"""
        try:
            if not self.exchange:
                print("[ERROR] Exchange client not initialized")
                return None
                
            order_result = self.exchange.order(
                "BTC",  
                False,  # 決済なので売り
                exec_qty, 
                price,
                {"limit": {"tif": "Gtc"}},
                reduce_only=reduce_only
            )
            print(f"買い決済結果: {order_result}")
            return order_result
        except Exception as e:
            print(f"買い決済エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def sell_in(self, price, qty=None):
        """売り注文"""
        try:
            if not self.exchange:
                print("[ERROR] Exchange client not initialized")
                return None
                
            # 価格の精度を調整
            rounded_price = round(float(price), 2)
            rounded_qty = round(float(qty), 6)
                
            order_result = self.exchange.order(
                "BTC",
                False,  # is_buy = False (売り)
                rounded_qty,
                rounded_price,
                {"limit": {"tif": "Gtc"}}
            )
            print(f"売り注文結果: {order_result}")
            return order_result
        except Exception as e:
            print(f"売り注文エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def sell_out(self, price, exec_qty, reduce_only=True):
        """売りの決済"""
        try:
            if not self.exchange:
                print("[ERROR] Exchange client not initialized")
                return None
                
            order_result = self.exchange.order(
                "BTC",
                True,   # 決済なので買い
                exec_qty,
                price, 
                {"limit": {"tif": "Gtc"}},
                reduce_only=reduce_only
            )
            print(f"売り決済結果: {order_result}")
            return order_result
        except Exception as e:
            print(f"売り決済エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def order_cancel(self, order_id):
        """注文キャンセル"""
        try:
            if not self.exchange:
                print("[ERROR] Exchange client not initialized")
                return None
                
            # 注文IDでキャンセル
            cancel_result = self.exchange.cancel("BTC", order_id)
            print(f"注文キャンセル結果: {cancel_result}")
            return cancel_result
        except Exception as e:
            print(f"注文キャンセルエラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    # ------------------------------------------------ #
    # 市場データ取得 (公式SDK使用)
    # ------------------------------------------------ #
    
    def set_request(self, method, access_modifiers, target_path, params, base_url=None):
        """互換性のためのダミー関数"""
        # SDKを使用するため、この関数は使用しない
        pass
    
    async def fetch(self, req):
        """互換性のためのダミー関数"""
        # SDKを使用するため、この関数は使用しない
        return None
    
    async def send(self):
        """互換性のためのダミー関数"""
        # SDKを使用するため、この関数は使用しない
        return []
    
    async def get_current_mid_price(self):
        """現在の中間価格取得（SDK使用）"""
        try:
            if not self.info:
                print("[ERROR] Info client not initialized")
                return None
                
            # 全銘柄の中間価格取得
            all_mids = self.info.all_mids()
            if all_mids and "BTC" in all_mids:
                mid_price = float(all_mids["BTC"])
                print(f"現在のBTC中間価格: ${mid_price:,.2f}")
                return mid_price
            else:
                print("BTC価格取得失敗")
                return None
        except Exception as e:
            print(f"価格取得エラー: {e}")
            return None
    
    async def get_account_info(self):
        """アカウント情報取得（SDK使用）"""
        try:
            if not self.info:
                print("[ERROR] Info client not initialized")
                return 0
                
            # ユーザー状態取得
            user_state = self.info.user_state(self.wallet_address)
            if user_state:
                margin_summary = user_state.get('marginSummary', {})
                print("=== アカウント情報 ===")
                account_value = float(margin_summary.get('accountValue', 0))
                total_margin_used = float(margin_summary.get('totalMarginUsed', 0))
                total_raw_usd = float(margin_summary.get('totalRawUsd', 0))
                
                print(f"口座残高: ${account_value:,.2f}")
                print(f"証拠金使用額: ${total_margin_used:,.2f}")
                print(f"利用可能証拠金: ${total_raw_usd:,.2f}")
                
                return account_value
            else:
                print("アカウント情報取得失敗")
                return 0
        except Exception as e:
            print(f"アカウント情報エラー: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    async def get_open_orders(self):
        """未約定注文取得（SDK使用）"""
        try:
            if not self.info:
                print("[ERROR] Info client not initialized")
                return []
                
            open_orders = self.info.open_orders(self.wallet_address)
            print(f"未約定注文: {open_orders}")
            return open_orders
        except Exception as e:
            print(f"未約定注文取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def cancel_all_orders(self):
        """全注文キャンセル（SDK使用）"""
        try:
            if not self.exchange:
                print("[ERROR] Exchange client not initialized")
                return None
                
            # まず未約定注文を取得
            open_orders = await self.get_open_orders()
            if not open_orders:
                print("キャンセルする注文がありません")
                return []
                
            # 各注文を個別にキャンセル
            cancel_results = []
            for order in open_orders:
                try:
                    oid = order.get('oid')
                    if oid:
                        cancel_result = self.exchange.cancel("BTC", oid)
                        cancel_results.append(cancel_result)
                        print(f"注文 {oid} キャンセル: {cancel_result}")
                except Exception as e:
                    print(f"注文キャンセルエラー: {e}")
                    
            return cancel_results
        except Exception as e:
            print(f"全注文キャンセルエラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    # ------------------------------------------------ #
    # WebSocket (必要に応じて後で実装)
    # ------------------------------------------------ #
    
    async def ws_run(self):
        """WebSocket接続（SDK使用）"""
        try:
            print("HyperLiquid SDK WebSocket is not implemented yet")
            print("必要に応じてhyperliquid.utils.websocket を使用してください")
            
            # 簡単な待機ループ
            while True:
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"WebSocket エラー: {e}")
    
    def _on_message(self, msg):
        """WebSocketメッセージハンドラ"""
        try:
            print(f"Received: {msg}")
        except Exception as e:
            print(f"Message handling error: {e}")