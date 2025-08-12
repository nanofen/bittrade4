# upload_data.py
# data/cex, data/dex, data/nextdexのファイルをGoogle Driveにアップロード

import os
import glob
from google_drive import GoogleDrive

class DataUploader:
    """データファイルアップロード管理クラス"""
    
    def __init__(self):
        self.drive = GoogleDrive()
        self.target_folder_id = "1HaRzQ-BJn35SFY9ARD_tB6Hs0ZkFY9ft"
        
        # アップロード対象ディレクトリ
        self.data_dirs = [
            "data/cex",
            "data/dex", 
            "data/nextdex",
            #"data"  # data直下のファイルも含む
        ]
        
        print("Google Drive データアップローダー初期化完了")
        print(f"アップロード先フォルダID: {self.target_folder_id}")
    
    def get_local_files(self):
        """ローカルのファイル一覧を取得"""
        local_files = {}
        
        for data_dir in self.data_dirs:
            if os.path.exists(data_dir):
                # CSVファイルのみを対象
                csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
                
                for file_path in csv_files:
                    filename = os.path.basename(file_path)
                    local_files[filename] = file_path
                    
        print(f"ローカルファイル数: {len(local_files)}件")
        return local_files
    
    def get_drive_files(self):
        """Google Driveのファイル一覧を取得"""
        drive_files = self.drive.get_drive_files(self.target_folder_id)
        print(f"Google Driveファイル数: {len(drive_files)}件")
        return drive_files
    
    def upload_file(self, local_path, filename):
        """単一ファイルをアップロード"""
        try:
            if not os.path.exists(local_path):
                print(f"[ERROR] ファイルが存在しません: {local_path}")
                return False
                
            file_size = os.path.getsize(local_path)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"アップロード中: {filename} ({file_size_mb:.2f}MB)")
            self.drive.upload_to_drive(local_path, filename, self.target_folder_id)
            print(f"[OK] アップロード完了: {filename}")
            return True
            
        except Exception as e:
            print(f"[ERROR] アップロードエラー: {filename} - {e}")
            return False
    
    def delete_drive_file(self, filename, file_id):
        """Google Driveからファイルを削除"""
        try:
            print(f"削除中: {filename} (ID: {file_id})")
            self.drive.delete_file_from_drive(file_id)
            print(f"[OK] 削除完了: {filename}")
            return True
        except Exception as e:
            print(f"[ERROR] 削除エラー: {filename} - {e}")
            return False
    
    def upload_all_files(self, overwrite=True):
        """全ファイルをアップロード"""
        print("\n=== データファイル一括アップロード開始 ===")
        
        # ローカルファイル一覧取得
        local_files = self.get_local_files()
        if not local_files:
            print("アップロード対象のファイルがありません")
            return
        
        # Google Driveファイル一覧取得
        drive_files = self.get_drive_files()
        
        upload_count = 0
        skip_count = 0
        error_count = 0
        delete_count = 0
        
        print(f"\n--- アップロード対象ファイル一覧 ---")
        for filename, local_path in local_files.items():
            file_size = os.path.getsize(local_path)
            file_size_mb = file_size / (1024 * 1024)
            status = "新規" if filename not in drive_files else "上書き"
            print(f"{filename:30} {file_size_mb:8.2f}MB [{status}]")
        
        # 確認メッセージ
        print(f"\n{len(local_files)}個のファイルをアップロードします。")
        if overwrite:
            existing_files = [f for f in local_files.keys() if f in drive_files]
            if existing_files:
                print(f"既存ファイル{len(existing_files)}個を上書きします。")
        
        response = input("続行しますか？ (y/N): ")
        if response.lower() != 'y':
            print("アップロードをキャンセルしました")
            return
        
        # ファイルごとにアップロード処理
        for filename, local_path in local_files.items():
            print(f"\n--- {filename} 処理中 ---")
            
            # 既存ファイルの処理
            if filename in drive_files:
                if overwrite:
                    # 既存ファイルを削除
                    if self.delete_drive_file(filename, drive_files[filename]):
                        delete_count += 1
                    else:
                        print(f"[WARNING] {filename} の削除に失敗")
                        error_count += 1
                        continue
                else:
                    print(f"[SKIP] {filename} は既に存在します（上書きモード無効）")
                    skip_count += 1
                    continue
            
            # ファイルアップロード
            if self.upload_file(local_path, filename):
                upload_count += 1
            else:
                error_count += 1
        
        # 結果サマリー
        print(f"\n=== アップロード結果 ===")
        print(f"アップロード成功: {upload_count}件")
        print(f"削除成功: {delete_count}件")
        print(f"スキップ: {skip_count}件")
        print(f"エラー: {error_count}件")
        
        if error_count == 0:
            print("[SUCCESS] 全ファイルのアップロードが完了しました")
        else:
            print(f"[WARNING] {error_count}件のエラーが発生しました")
        
        # 最終的なGoogle Driveファイル一覧表示
        print(f"\n--- アップロード後のGoogle Driveファイル一覧 ---")
        final_files = self.get_drive_files()
        for filename, file_id in final_files.items():
            print(f"{filename:30} (ID: {file_id[:10]}...)")
    
    def show_file_status(self):
        """ローカルとGoogle Driveのファイル状況を表示"""
        print("\n=== ファイル状況確認 ===")
        
        local_files = self.get_local_files()
        drive_files = self.get_drive_files()
        
        print(f"\nローカルファイル: {len(local_files)}件")
        for filename, path in local_files.items():
            file_size = os.path.getsize(path)
            file_size_mb = file_size / (1024 * 1024)
            status = "✓" if filename in drive_files else "✗"
            print(f"  {status} {filename:25} {file_size_mb:8.2f}MB")
        
        print(f"\nGoogle Driveファイル: {len(drive_files)}件")
        for filename, file_id in drive_files.items():
            status = "✓" if filename in local_files else "?"
            print(f"  {status} {filename:25} (ID: {file_id[:10]}...)")
        
        # 同期状況
        local_only = set(local_files.keys()) - set(drive_files.keys())
        drive_only = set(drive_files.keys()) - set(local_files.keys())
        
        if local_only:
            print(f"\nローカルのみのファイル: {len(local_only)}件")
            for filename in local_only:
                print(f"  → {filename}")
        
        if drive_only:
            print(f"\nGoogle Driveのみのファイル: {len(drive_only)}件")
            for filename in drive_only:
                print(f"  → {filename}")
        
        if not local_only and not drive_only:
            print("\n[OK] ローカルとGoogle Driveは同期済みです")
    
    def download_all_files(self, overwrite=True):
        """Google Driveから全ファイルをダウンロード"""
        print("\n=== Google Driveからダウンロード開始 ===")
        
        # Google Driveファイル一覧取得
        drive_files = self.get_drive_files()
        if not drive_files:
            print("ダウンロード対象のファイルがありません")
            return
        
        # ローカルファイル一覧取得
        local_files = self.get_local_files()
        
        download_count = 0
        skip_count = 0
        error_count = 0
        
        print(f"\n--- ダウンロード対象ファイル一覧 ---")
        for filename, file_id in drive_files.items():
            status = "新規" if filename not in local_files else "上書き"
            print(f"{filename:30} (ID: {file_id[:10]}...) [{status}]")
        
        # 確認メッセージ
        print(f"\n{len(drive_files)}個のファイルをダウンロードします。")
        if overwrite:
            existing_files = [f for f in drive_files.keys() if f in local_files]
            if existing_files:
                print(f"既存ファイル{len(existing_files)}個を上書きします。")
        
        response = input("続行しますか？ (y/N): ")
        if response.lower() != 'y':
            print("ダウンロードをキャンセルしました")
            return
        
        # ファイルごとにダウンロード処理
        for filename, file_id in drive_files.items():
            print(f"\n--- {filename} ダウンロード中 ---")
            
            # ダウンロード先のパス決定
            download_path = self.determine_download_path(filename)
            
            # 既存ファイルの処理
            if os.path.exists(download_path):
                if overwrite:
                    print(f"既存ファイルを上書きします: {download_path}")
                else:
                    print(f"[SKIP] {filename} は既に存在します（上書きモード無効）")
                    skip_count += 1
                    continue
            
            # ダウンロード実行
            if self.download_file(file_id, filename, download_path):
                download_count += 1
            else:
                error_count += 1
        
        # 結果サマリー
        print(f"\n=== ダウンロード結果 ===")
        print(f"ダウンロード成功: {download_count}件")
        print(f"スキップ: {skip_count}件")
        print(f"エラー: {error_count}件")
        
        if error_count == 0:
            print("[SUCCESS] 全ファイルのダウンロードが完了しました")
        else:
            print(f"[WARNING] {error_count}件のエラーが発生しました")
    
    def determine_download_path(self, filename):
        """ファイル名からダウンロード先パスを決定"""
        # ファイル名のパターンに基づいてディレクトリを決定
        if filename.startswith('cex_'):
            download_dir = "data/cex"
        elif filename.startswith('dex_'):
            download_dir = "data/dex"
        elif filename.startswith('nextdex_'):
            download_dir = "data/nextdex"
        else:
            download_dir = "data"
        
        # ディレクトリが存在しない場合は作成
        os.makedirs(download_dir, exist_ok=True)
        
        return os.path.join(download_dir, filename)
    
    def download_file(self, file_id, filename, local_path):
        """単一ファイルをダウンロード"""
        try:
            print(f"ダウンロード中: {filename} → {local_path}")
            self.drive.download_file_from_drive(file_id, local_path)
            
            # ダウンロードしたファイルサイズ確認
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                file_size_mb = file_size / (1024 * 1024)
                print(f"[OK] ダウンロード完了: {filename} ({file_size_mb:.2f}MB)")
                return True
            else:
                print(f"[ERROR] ダウンロードファイルが見つかりません: {local_path}")
                return False
                
        except Exception as e:
            print(f"[ERROR] ダウンロードエラー: {filename} - {e}")
            return False

def main():
    """メイン実行関数"""
    print("Google Drive データ同期ツール")
    print("=" * 50)
    
    try:
        uploader = DataUploader()
        
        print("\n=== 操作選択 ===")
        print("1. アップロード（ローカル → Google Drive）")
        print("2. ダウンロード（Google Drive → ローカル）")
        
        choice = input("\n選択してください (1-2): ").strip()
        
        if choice == '1':
            print("\n[アップロードモード] ローカルファイルをGoogle Driveにアップロードします")
            uploader.upload_all_files(overwrite=True)
        elif choice == '2':
            print("\n[ダウンロードモード] Google Driveファイルをローカルにダウンロードします")
            uploader.download_all_files(overwrite=True)
        else:
            print("無効な選択です")
            return
                
    except KeyboardInterrupt:
        print("\n操作がキャンセルされました")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()