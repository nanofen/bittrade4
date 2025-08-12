# socket_bybit_pybotters.py
# Bybit取引用Socket実装（アービトラージ対応）

import aiohttp
import asyncio
import json
import time
import hashlib
import hmac
import pandas as pd
from datetime import datetime
import pybotters

class Socket_PyBotters_Bybit():
    
    # 定数
    TIMEOUT = 3600
    EXTEND_TOKEN_TIME = 3000
    SYMBOL = 'BTCUSDT'
    
    # Bybit APIエンドポイント（本番環境）
    URLS = {
        'REST': 'https://api.bybit.com/',
        'REST_UNIFIED': 'https://api.bybit.com/v5',
        'WebSocket_Public': 'wss://stream.bybit.com/v5/public/linear',
        'WebSocket_Private': 'wss://stream.bybit.com/v5/private',
    }
    
    PUBLIC_CHANNELS = ['orderbook.1.BTCUSDT', 'publicTrade.BTCUSDT']
    PRIVATE_CHANNELS = ['order', 'execution', 'position']
    
    MAX_OHLCV_CAPACITY = 60 * 60 * 48
    df_ohlcv = pd.DataFrame(
        columns=["exec_date", "Open", "High", "Low", "Close", "Volume", "timestamp"]
    ).set_index("exec_date")
    
    def __init__(self, keys):
        """Bybit Socket初期化"""
        self.KEYS = keys
        self.api_key = ''
        self.api_secret = ''
        
        if 'bybit' in keys and keys['bybit']:
            bybit_data = keys['bybit']
            if isinstance(bybit_data, tuple) and len(bybit_data) == 2:
                self.api_key, self.api_secret = bybit_data
            elif isinstance(bybit_data, list) and len(bybit_data) >= 2:
                self.api_key = bybit_data[0]
                self.api_secret = bybit_data[1]
                
            # pybottersのKEYS形式に変換
            self.KEYS = {'bybit': [self.api_key, self.api_secret]}
            print(f"Bybit: API認証設定完了")
        else:
            print("Bybit: 認証情報が設定されていません")
        
        self.store = pybotters.BybitDataStore()
        self.session = None
        self.requests = []
    
    def set_request(self, method, access_modifiers, target_path, params, base_url=None):
        """リクエスト設定"""
        if base_url is None:
            base_url = self.URLS['REST']
        
        url = f"{base_url}{target_path}"
        
        self.requests.append({
            'method': method,
            'access_modifiers': access_modifiers,
            'target_path': target_path,
            'url': url,
            'params': params,
            'headers': {}
        })
    
    async def fetch(self, req):
        """HTTPリクエスト実行"""
        try:
            kwargs = {'timeout': aiohttp.ClientTimeout(total=10)}
            async with pybotters.Client(apis=self.KEYS, **kwargs) as client:
                if req["method"] == 'GET':
                    response = await client.get(req['url'], params=req['params'], headers=req['headers'])
                else:
                    response = await client.request(req["method"], req['url'], data=req['params'], headers=req['headers'])
                
                if response.status == 200:
                    content = await response.read()
                    return json.loads(content.decode('utf-8'))
                else:
                    error_text = await response.text()
                    print(f"Bybit HTTP error: {response.status}")
                    print(f"Response body: {error_text}")
                    return None
        except Exception as e:
            print(f"Bybit Request error: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def send(self):
        """リクエスト送信"""
        promises = [self.fetch(req) for req in self.requests]
        self.requests.clear()
        return await asyncio.gather(*promises)
    
    # ------------------------------------------------ #
    # 注文関数（アービトラージ用）
    # ------------------------------------------------ #
    
    async def buy_in(self, price, qty=None):
        """買い注文（Maker）"""
        try:
            self.order_create(
                side="Buy",
                order_type="Limit",
                qty=qty,
                price=str(price),
                time_in_force="PostOnly",  # Maker注文
                reduce_only=False
            )
            response = await self.send()
            print(f"Bybit買い注文結果: {response}")
            return response
        except Exception as e:
            print(f"Bybit買い注文エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def buy_out(self, price, exec_qty, reduce_only=True):
        """買いポジション決済"""
        try:
            self.order_create(
                side="Sell",
                order_type="Limit",
                qty=exec_qty,
                price=str(price),
                time_in_force="PostOnly",
                reduce_only=reduce_only
            )
            response = await self.send()
            print(f"Bybit買い決済結果: {response}")
            return response
        except Exception as e:
            print(f"Bybit買い決済エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def sell_in(self, price, qty=None):
        """売り注文（Maker）"""
        try:
            self.order_create(
                side="Sell",
                order_type="Limit",
                qty=qty,
                price=str(price),
                time_in_force="PostOnly",  # Maker注文
                reduce_only=False
            )
            response = await self.send()
            print(f"Bybit売り注文結果: {response}")
            return response
        except Exception as e:
            print(f"Bybit売り注文エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def sell_out(self, price, exec_qty, reduce_only=True):
        """売りポジション決済"""
        try:
            self.order_create(
                side="Buy",
                order_type="Limit",
                qty=exec_qty,
                price=str(price),
                time_in_force="PostOnly",
                reduce_only=reduce_only
            )
            response = await self.send()
            print(f"Bybit売り決済結果: {response}")
            return response
        except Exception as e:
            print(f"Bybit売り決済エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def order_cancel(self, order_id='', order_link_id=''):
        """注文キャンセル"""
        try:
            target_path = 'v5/order/cancel'
            params = {
                'symbol': self.SYMBOL,
                'category': 'linear',
            }
            
            if order_id:
                params['orderId'] = order_id
            if order_link_id:
                params['orderLinkId'] = order_link_id
            
            self.set_request(method='POST', access_modifiers='private',
                           target_path=target_path, params=params)
            response = await self.send()
            print(f"Bybit注文キャンセル結果: {response}")
            return response
        except Exception as e:
            print(f"Bybit注文キャンセルエラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    # ------------------------------------------------ #
    # 内部関数
    # ------------------------------------------------ #
    
    def order_create(self, side, order_type, qty, price='', time_in_force='', 
                    close_on_trigger=False, order_link_id='', reduce_only=False):
        """注文作成"""
        target_path = 'v5/order/create'
        params = {
            'category': 'linear',
            'side': side,
            'symbol': self.SYMBOL,
            'orderType': order_type,
            'qty': str(qty),
            'timeInForce': time_in_force,
            'positionIdx': 0,
        }
        
        if price:
            params['price'] = str(float(price))
        if close_on_trigger:
            params['closeOnTrigger'] = close_on_trigger
        if order_link_id:
            params['orderLinkId'] = order_link_id
        if reduce_only:
            params['reduceOnly'] = reduce_only
        
        self.set_request(method='POST', access_modifiers='private',
                        target_path=target_path, params=params)
    
    # ------------------------------------------------ #
    # 市場データ・アカウント情報
    # ------------------------------------------------ #
    
    async def get_current_mid_price(self):
        """現在の中間価格取得"""
        try:
            target_path = f'v5/market/tickers?category=linear&symbol={self.SYMBOL}'
            self.set_request(method='GET', access_modifiers='public',
                           target_path=target_path, params={})
            response = await self.send()
            
            if response and response[0] and 'result' in response[0]:
                ticker_data = response[0]['result']['list']
                if ticker_data and len(ticker_data) > 0:
                    ticker = ticker_data[0]
                    bid = float(ticker.get('bid1Price', 0))
                    ask = float(ticker.get('ask1Price', 0))
                    mid_price = (bid + ask) / 2
                    print(f"Bybit {self.SYMBOL} 中間価格: ${mid_price:,.2f}")
                    return mid_price
            
            print("Bybit価格取得失敗")
            return None
        except Exception as e:
            print(f"Bybit価格取得エラー: {e}")
            return None
    
    async def get_account_info(self):
        """アカウント情報取得"""
        try:
            target_path = 'v5/account/wallet-balance?accountType=UNIFIED'
            self.set_request(method='GET', access_modifiers='private',
                           target_path=target_path, params={})
            response = await self.send()
            
            if response and response[0] and 'result' in response[0]:
                wallet_data = response[0]['result']['list'][0]
                print("=== Bybitアカウント情報 ===")
                total_balance = float(wallet_data.get('totalWalletBalance', 0))
                available_balance = float(wallet_data.get('totalAvailableBalance', 0))
                print(f"口座残高: ${total_balance:,.2f}")
                print(f"利用可能残高: ${available_balance:,.2f}")
                return total_balance
            
            print("Bybitアカウント情報取得失敗")
            return 0
        except Exception as e:
            print(f"Bybitアカウント情報エラー: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    async def get_open_orders(self):
        """未約定注文取得"""
        try:
            target_path = f'v5/order/realtime?category=linear&symbol={self.SYMBOL}'
            self.set_request(method='GET', access_modifiers='private',
                           target_path=target_path, params={})
            response = await self.send()
            
            if response and response[0] and 'result' in response[0]:
                orders = response[0]['result']['list']
                print(f"Bybit未約定注文: {len(orders)}件")
                return orders
            
            return []
        except Exception as e:
            print(f"Bybit未約定注文取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def get_positions(self):
        """ポジション取得"""
        try:
            target_path = f'v5/position/list?category=linear&symbol={self.SYMBOL}'
            self.set_request(method='GET', access_modifiers='private',
                           target_path=target_path, params={})
            response = await self.send()
            
            if response and response[0] and 'result' in response[0]:
                positions = response[0]['result']['list']
                return positions
            
            return []
        except Exception as e:
            print(f"Bybitポジション取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def cancel_all_orders(self):
        """全注文キャンセル"""
        try:
            target_path = 'v5/order/cancel-all'
            params = {
                'symbol': self.SYMBOL,
                'category': 'linear',
            }
            self.set_request(method='POST', access_modifiers='private',
                           target_path=target_path, params=params)
            response = await self.send()
            print(f"Bybit全注文キャンセル完了")
            return response
        except Exception as e:
            print(f"Bybit全注文キャンセルエラー: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    # ------------------------------------------------ #
    # WebSocket
    # ------------------------------------------------ #
    
    async def ws_run(self):
        """WebSocket接続"""
        try:
            print("Bybit WebSocket開始...")
            async with pybotters.Client(apis=self.KEYS, base_url=self.URLS["REST"]) as client:
                # 初期化
                await self.store.initialize(
                    client.get("v5/position/list", params={'symbol': self.SYMBOL, 'category': 'linear'}),
                    client.get("v5/order/realtime", params={'symbol': self.SYMBOL, 'category': 'linear'}),
                    client.get("v5/account/wallet-balance", params={'accountType': 'UNIFIED'}),
                )
                
                # パブリックWebSocket
                public = await client.ws_connect(
                    self.URLS['WebSocket_Public'],
                    send_json={
                        'op': 'subscribe',
                        'args': self.PUBLIC_CHANNELS,
                    },
                    hdlr_json=self.store.onmessage,
                )
                
                # プライベートWebSocket
                private = await client.ws_connect(
                    self.URLS['WebSocket_Private'],
                    send_json={
                        'op': 'subscribe',
                        'args': self.PRIVATE_CHANNELS,
                    },
                    hdlr_json=self.store.onmessage,
                )
                
                print("Bybit WebSocket接続完了")
                while True:
                    await self.store.wait()
                    
        except Exception as e:
            print(f"Bybit WebSocket エラー: {e}")
            import traceback
            traceback.print_exc()