# socket_dydx_v4clients.py

import aiohttp
import asyncio
from enum import Enum
import json
import traceback
import pandas as pd
import time
import hashlib
import hmac
import base64
from datetime import datetime, timezone
try:
    import dydx_v4_client
    print("dYdX v4 client available")
    DYDX_CLIENT_AVAILABLE = True
except ImportError as e:
    print(f"dydx-v4-client not available: {e}")
    DYDX_CLIENT_AVAILABLE = False

class Socket_dYdX_V4Client():

    # 定数
    TIMEOUT = 3600               # タイムアウト
    EXTEND_TOKEN_TIME = 3000     # アクセストークン延長までの時間
    SYMBOL = 'BTC-USD'          # シンボル
    URLS = {'REST_INDEXER': 'https://indexer.dydx.trade',
            'REST_VALIDATOR': 'https://dydx-ops-rpc.kingnodes.com',
            'WebSocket_Public': 'wss://indexer.dydx.trade/v4/ws',
           }
                      
    PUBLIC_CHANNELS = ['v4_markets',
                       'v4_trades', 
                       'v4_orderbook',
                       'v4_candles',
                      ]
                      
    PRIVATE_CHANNELS = ['v4_accounts',
                        'v4_orders',
                        'v4_subaccounts',
                        ]

    KEYS = {
        'dydx': ['DYDX_MNEMONIC'],  # mnemonic phrase
    }
    
    MAX_OHLCV_CAPACITY = 60 * 60 * 48
    df_ohlcv = pd.DataFrame(
        columns=["exec_date", "Open", "High", "Low", "Close", "Volume", "timestamp"]).set_index("exec_date")

    # 変数
    mnemonic = ''
    client = None
    socket_client = None
    subaccount = None

    session = None          # セッション保持
    requests = []           # リクエストパラメータ
    heartbeat = 0
    orderbook_data = {}
    trades_data = []
    account_data = {}

    # ------------------------------------------------ #
    # init
    # ------------------------------------------------ #
    def __init__(self, keys):
        # APIキー・SECRETをセット
        self.KEYS = keys
        if 'dydx' in keys and keys['dydx'][0]:
            self.mnemonic = keys['dydx'][0]
            self._initialize_client()

    def _initialize_client(self):
        """dYdX clientを初期化"""
        try:
            if not DYDX_CLIENT_AVAILABLE:
                print("dYdX client not available")
                return
            
            # 現在はREST API clientとして動作
            print("dYdX initialized for REST API only")
            
        except Exception as e:
            print(f"Failed to initialize dYdX client: {e}")

    # ------------------------------------------------ #
    # WebSocket callbacks
    # ------------------------------------------------ #
    def _on_open(self, ws):
        print("dYdX WebSocket connection opened")

    def _on_close(self, ws):
        print("dYdX WebSocket connection closed")

    def _on_error(self, ws, error):
        print(f"dYdX WebSocket error: {error}")

    def _on_message(self, ws, message):
        """WebSocketメッセージ処理"""
        try:
            data = json.loads(message)
            channel = data.get('channel', '')
            
            if channel == 'v4_orderbook':
                self.orderbook_data = data.get('contents', {})
            elif channel == 'v4_trades':
                self.trades_data.extend(data.get('contents', []))
                # 最新100件のみ保持
                self.trades_data = self.trades_data[-100:]
            elif channel == 'v4_subaccounts':
                self.account_data.update(data.get('contents', {}))
                
        except Exception as e:
            print(f"Error processing WebSocket message: {e}")

    # ------------------------------------------------ #
    # async request for rest api
    # ------------------------------------------------ #
    async def get_info_dydx(self):
        """取引所情報を取得"""
        try:
            # REST APIを使ってマーケット情報を取得
            target_path = '/v4/perpetualMarkets'
            self.set_request(method='GET', access_modifiers='public',
                             target_path=target_path, params={})
            responses = await self.send()
            if responses and responses[0]:
                return responses[0].get('markets', {})
            return None
        except Exception as e:
            print(f"Error getting dYdX info: {e}")
            return None

    async def buy_in(self, sell_price, qty=None):
        """買い注文"""
        try:
            order = self.client.subaccounts.place_order(
                subaccount=self.subaccount,
                market=self.SYMBOL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                post_only=False,
                size=str(qty),
                price=str(sell_price),
                time_in_force=TimeInForce.GTT,
                execution=Order.TimeInForce.TIME_IN_FORCE_UNSPECIFIED,
                expiration_epoch_seconds=int(time.time()) + 3600,  # 1時間後に期限切れ
                good_til_block=0
            )
            response = await self._process_order_response(order)
            print(response)
            return response
        except Exception as e:
            print(f"Buy order error: {e}")
            return None

    async def buy_out(self, buy_price, exec_qty, reduce_only=True):
        """
        買いの決済
        """
        try:
            order = self.client.subaccounts.place_order(
                subaccount=self.subaccount,
                market=self.SYMBOL,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                post_only=False,
                size=str(exec_qty),
                price=str(buy_price),
                time_in_force=TimeInForce.GTT,
                execution=Order.TimeInForce.TIME_IN_FORCE_UNSPECIFIED,
                expiration_epoch_seconds=int(time.time()) + 3600,
                good_til_block=0,
                reduce_only=reduce_only
            )
            response = await self._process_order_response(order)
            print(response)
            return response
        except Exception as e:
            print(f"Buy close order error: {e}")
            return None

    async def sell_in(self, buy_price, qty=None):
        """売り注文"""
        try:
            order = self.client.subaccounts.place_order(
                subaccount=self.subaccount,
                market=self.SYMBOL,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                post_only=False,
                size=str(qty),
                price=str(buy_price),
                time_in_force=TimeInForce.GTT,
                execution=Order.TimeInForce.TIME_IN_FORCE_UNSPECIFIED,
                expiration_epoch_seconds=int(time.time()) + 3600,
                good_til_block=0
            )
            response = await self._process_order_response(order)
            print(response)
            return response
        except Exception as e:
            print(f"Sell order error: {e}")
            return None

    async def sell_out(self, sell_price, exec_qty, reduce_only=True):
        """
        売りの決済
        """
        try:
            order = self.client.subaccounts.place_order(
                subaccount=self.subaccount,
                market=self.SYMBOL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                post_only=False,
                size=str(exec_qty),
                price=str(sell_price),
                time_in_force=TimeInForce.GTT,
                execution=Order.TimeInForce.TIME_IN_FORCE_UNSPECIFIED,
                expiration_epoch_seconds=int(time.time()) + 3600,
                good_til_block=0,
                reduce_only=reduce_only
            )
            response = await self._process_order_response(order)
            print(response)
            return response
        except Exception as e:
            print(f"Sell close order error: {e}")
            return None

    async def order_cancel(self, order_id, good_til_block=None):
        """注文キャンセル"""
        try:
            if good_til_block is None:
                good_til_block = self.client.validator.get_latest_block_height() + 10
                
            result = self.client.subaccounts.cancel_order(
                subaccount=self.subaccount,
                client_id=order_id,
                order_flags=Order.OrderFlags.ORDER_FLAGS_UNSPECIFIED,
                clobpair_id=0,  # BTC-USDの場合は0
                good_til_block=good_til_block
            )
            print(result)
            return result
        except Exception as e:
            print(f"Cancel order error: {e}")
            return None

    def set_request(self, method, access_modifiers, target_path, params, base_url=None):
        """リクエスト設定（dYdX v4ではREST APIは主にindexer経由）"""
        if base_url is None:
            base_url = self.URLS['REST_INDEXER']
            
        url = ''.join([base_url, target_path])
        headers = {'Content-Type': 'application/json'}
        
        self.requests.append({'method': method,
                              'access_modifiers': access_modifiers,
                              'target_path': target_path, 'url': url,
                              'params': params, 'headers': headers})

    async def fetch(self, req):
        """HTTPリクエスト実行"""
        try:
            async with aiohttp.ClientSession() as session:
                if req["method"] == 'GET':
                    r = await session.get(req['url'], params=req['params'], headers=req['headers'])
                else:
                    r = await session.request(req["method"], req['url'], 
                                            json=req['params'], headers=req['headers'])
                if r.status == 200:
                    content = await r.text()
                    return json.loads(content) if content else {}
                return None
        except Exception as e:
            print(f"Fetch error: {e}")
            return None

    async def send(self):
        """リクエスト送信"""
        promises = [self.fetch(req) for req in self.requests]
        self.requests.clear()
        return await asyncio.gather(*promises)

    async def _process_order_response(self, order_response):
        """注文レスポンス処理"""
        # 注文レスポンスの処理ロジック
        return order_response

    # ------------------------------------------------ #
    # REST API(Market Data Endpoints)  
    # ------------------------------------------------ #
    
    def ticker(self, market=None):
        """ティッカー取得"""
        if market is None:
            market = self.SYMBOL
        # tickerはperpetualMarketsエンドポイントから特定マーケット情報を取得
        target_path = f'/v4/perpetualMarkets'
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params={})

    def orderbook(self, market=None):
        """オーダーブック取得"""
        if market is None:
            market = self.SYMBOL
        target_path = f'/v4/orderbooks/perpetualMarket/{market}'
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params={})

    def recent_trades(self, market=None, limit=100):
        """最近の取引履歴取得"""
        if market is None:
            market = self.SYMBOL
        target_path = f'/v4/trades/perpetualMarket/{market}'
        params = {'limit': limit}
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    def candles(self, market=None, resolution='1HOUR', from_iso=None, to_iso=None, limit=100):
        """キャンドルデータ取得"""
        if market is None:
            market = self.SYMBOL
        # 正しいエンドポイントに修正
        target_path = f'/v4/candles/perpetualMarkets/{market}'
        params = {
            'resolution': resolution,
            'limit': limit
        }
        if from_iso:
            params['fromISO'] = from_iso
        if to_iso:
            params['toISO'] = to_iso
            
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)
    
    # ------------------------------------------------ #
    # REST API(Account Data Endpoints)
    # ------------------------------------------------ #

    def account_info(self, address=None):
        """アカウント情報取得"""
        if address is None and self.client:
            address = self.client.subaccounts.wallet.address
        target_path = f'/v4/addresses/{address}'
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params={})

    def subaccount_info(self, address=None, subaccount_number=0):
        """サブアカウント情報取得"""
        if address is None and self.client:
            address = self.client.subaccounts.wallet.address
        target_path = f'/v4/addresses/{address}/subaccounts/{subaccount_number}'
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params={})

    def open_orders(self, address=None, subaccount_number=0, market=None):
        """有効注文一覧取得"""
        if address is None and self.client:
            address = self.client.subaccounts.wallet.address
        if market is None:
            market = self.SYMBOL
        target_path = f'/v4/orders'
        params = {
            'address': address,
            'subaccountNumber': subaccount_number,
            'market': market,
            'status': 'OPEN'
        }
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    def order_history(self, address=None, subaccount_number=0, market=None, limit=100):
        """注文履歴取得"""
        if address is None and self.client:
            address = self.client.subaccounts.wallet.address
        if market is None:
            market = self.SYMBOL
        target_path = f'/v4/orders'
        params = {
            'address': address,
            'subaccountNumber': subaccount_number,
            'market': market,
            'limit': limit
        }
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    def fills_history(self, address=None, subaccount_number=0, market=None, limit=100):
        """約定履歴取得"""
        if address is None and self.client:
            address = self.client.subaccounts.wallet.address
        if market is None:
            market = self.SYMBOL
        target_path = f'/v4/fills'
        params = {
            'address': address,
            'subaccountNumber': subaccount_number,
            'market': market,
            'limit': limit
        }
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    def positions(self, address=None, subaccount_number=0):
        """ポジション情報取得"""
        if address is None and self.client:
            address = self.client.subaccounts.wallet.address
        target_path = f'/v4/addresses/{address}/subaccounts/{subaccount_number}'
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params={})

    # ------------------------------------------------ #
    # WebSocket
    # ------------------------------------------------ #

    async def ws_run(self):
        """WebSocket実行"""
        try:
            print(f"dYdX WebSocket starting...")
            
            if not self.socket_client:
                print("Socket client not initialized")
                return
                
            # WebSocket接続
            print("Connecting to dYdX WebSocket...")
            self.socket_client.connect()
            
            # パブリックチャンネル購読
            self.socket_client.subscribe_to_markets()
            self.socket_client.subscribe_to_trades(self.SYMBOL)
            self.socket_client.subscribe_to_orderbook(self.SYMBOL)
            
            # プライベートチャンネル購読（認証が必要）
            if self.client:
                address = self.client.subaccounts.wallet.address
                self.socket_client.subscribe_to_subaccount(address, 0)
                
            print("dYdX WebSocket connected and subscribed")

            # メッセージループ
            while True:
                await asyncio.sleep(1)
                # WebSocketメッセージはコールバックで処理される
                
        except Exception as e:
            print(f"dYdX WebSocket error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.socket_client:
                self.socket_client.close()