#!/usr/bin/env python3
"""
マルチコレクター - 全データソースを同時に異なる頻度で実行
- CEX: 5秒間隔
- NextDEX: 5秒間隔  
- DEX: 1分間隔
"""

import asyncio
import signal
import sys
from datetime import datetime
import subprocess
import time

class MultiCollector:
    def __init__(self):
        self.running = True
        self.processes = {}
        
    def signal_handler(self, signum, frame):
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Multi Collector stopping...")
        self.running = False
        
        # 全プロセスを終了
        for name, process in self.processes.items():
            if process and process.returncode is None:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Terminating {name}...")
                process.terminate()
                
        sys.exit(0)
    
    async def start_collectors(self):
        """全コレクターを開始"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting Multi Collector...")
        print("=" * 60)
        
        # CEXコレクター開始（5秒間隔）
        try:
            self.processes['CEX'] = subprocess.Popen([
                sys.executable, 'cex_collector.py', '--interval', '5.0'
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
               universal_newlines=True, bufsize=1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CEX Collector started (5s interval)")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to start CEX Collector: {e}")
        
        # NextDEXコレクター開始（5秒間隔）
        try:
            self.processes['NextDEX'] = subprocess.Popen([
                sys.executable, 'nextdex_collector.py', '--interval', '5.0'
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
               universal_newlines=True, bufsize=1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] NextDEX Collector started (5s interval)")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to start NextDEX Collector: {e}")
        
        # DEXコレクター開始（60秒間隔）
        try:
            self.processes['DEX'] = subprocess.Popen([
                sys.executable, 'dex_collector.py', '--interval', '60.0'
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
               universal_newlines=True, bufsize=1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] DEX Collector started (60s interval)")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to start DEX Collector: {e}")
        
        print("=" * 60)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] All collectors started!")
        print("Press Ctrl+C to stop all collectors")
        print("=" * 60)
    
    async def monitor_collectors(self):
        """コレクターの監視とログ出力"""
        output_count = {'CEX': 0, 'NextDEX': 0, 'DEX': 0}
        
        while self.running:
            # 各プロセスの出力を読み取り
            for name, process in self.processes.items():
                if process and process.poll() is None:  # プロセスが生きている
                    try:
                        # 非ブロッキングで出力を読み取り
                        line = process.stdout.readline()
                        if line:
                            output_count[name] += 1
                            # 出力頻度を制限（最初の10行、その後100行ごと）
                            if output_count[name] <= 10 or output_count[name] % 100 == 0:
                                print(f"[{name}] {line.strip()}")
                    except:
                        pass
                elif process and process.poll() is not None:
                    # プロセスが終了した
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {name} process ended with code {process.returncode}")
                    if self.running:  # 意図しない終了の場合は再起動
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Restarting {name}...")
                        await self._restart_collector(name)
            
            # 統計情報表示（5分ごと）       
            if int(time.time()) % 300 == 0:  # 5分ごと
                await self._show_statistics()
            
            await asyncio.sleep(1)  # 1秒待機
    
    async def _restart_collector(self, name: str):
        """個別コレクターの再起動"""
        try:
            if name == 'CEX':
                self.processes[name] = subprocess.Popen([
                    sys.executable, 'cex_collector.py', '--interval', '5.0'
                ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                   universal_newlines=True, bufsize=1)
            elif name == 'NextDEX':
                self.processes[name] = subprocess.Popen([
                    sys.executable, 'nextdex_collector.py', '--interval', '5.0'
                ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                   universal_newlines=True, bufsize=1)
            elif name == 'DEX':
                self.processes[name] = subprocess.Popen([
                    sys.executable, 'dex_collector.py', '--interval', '60.0'
                ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                   universal_newlines=True, bufsize=1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {name} restarted successfully")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to restart {name}: {e}")
    
    async def _show_statistics(self):
        """統計情報の表示"""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] === Multi Collector Statistics ===")
        
        # 各プロセスの状態確認
        for name, process in self.processes.items():
            if process:
                status = "Running" if process.poll() is None else f"Stopped ({process.returncode})"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {name:>8}: {status}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {name:>8}: Not started")
        
        # データディレクトリの確認
        import os
        data_dirs = ['data/cex', 'data/nextdex', 'data/dex']
        for dir_path in data_dirs:
            if os.path.exists(dir_path):
                files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {dir_path:>12}: {len(files)} CSV files")
        
        print("=" * 50)
    
    async def run(self):
        """メインループ"""
        # シグナルハンドラー設定
        try:
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
        except:
            pass
        
        # コレクター開始
        await self.start_collectors()
        
        # 監視ループ
        await self.monitor_collectors()

class SimpleMultiCollector:
    """シンプル版：各コレクターを独立したasyncioタスクとして実行"""
    
    def __init__(self):
        self.running = True
        
    def signal_handler(self, signum, frame):
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Simple Multi Collector stopping...")
        self.running = False
    
    async def run_cex_task(self):
        """CEXタスク（5秒間隔）"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] CEX Task started")
        
        # CEXコレクターのインポートと実行
        try:
            from cex_collector import CEXRunner
            runner = CEXRunner()
            runner.running = self.running  # 停止フラグを共有
            await runner.run_continuous(5.0)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CEX Task error: {e}")
    
    async def run_nextdex_task(self):
        """NextDEXタスク（5秒間隔）"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] NextDEX Task started")
        
        try:
            from nextdex_collector import NextDEXRunner
            runner = NextDEXRunner()
            runner.running = self.running
            await runner.run_continuous(5.0)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] NextDEX Task error: {e}")
    
    async def run_dex_task(self):
        """DEXタスク（60秒間隔）"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] DEX Task started")
        
        try:
            from dex_collector import DEXRunner
            runner = DEXRunner()
            runner.running = self.running
            await runner.run_continuous(60.0)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] DEX Task error: {e}")
    
    async def run(self):
        """全タスクを並行実行"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Simple Multi Collector starting...")
        print("=" * 60)
        
        # シグナルハンドラー設定
        try:
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
        except:
            pass
        
        # 全タスクを並行実行
        tasks = [
            asyncio.create_task(self.run_cex_task()),
            asyncio.create_task(self.run_nextdex_task()),
            asyncio.create_task(self.run_dex_task())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Keyboard interrupt received")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: {e}")
        finally:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Simple Multi Collector stopped")

async def main():
    """メイン実行"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-source Price Data Collector')
    parser.add_argument('--mode', choices=['subprocess', 'simple'], default='simple',
                       help='実行モード（subprocess: 別プロセス、simple: 同一プロセス）')
    
    args = parser.parse_args()
    
    if args.mode == 'subprocess':
        collector = MultiCollector()
    else:
        collector = SimpleMultiCollector()
    
    await collector.run()

if __name__ == "__main__":
    asyncio.run(main())