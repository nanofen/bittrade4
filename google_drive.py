from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
import io

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'service_account.json'  # 事前に作成した JSON キー


class GoogleDrive():

    def __init__(self):
        # Google Drive API 認証
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        self.drive_service = build('drive', 'v3', credentials=credentials)

    def get_drive_files(self, folder_id):
        """指定フォルダ内のファイルリストを取得"""
        query = f"'{folder_id}' in parents and trashed = false"
        results = self.drive_service.files().list(q=query, fields="files(id, name, size)",pageSize=1000, orderBy="createdTime desc").execute()
        #print(results)
        files = results.get('files', [])
        return {file["name"]: file["id"] for file in files}  # ファイル名とIDを返す

    def delete_file_from_drive(self, file_id):
        """Google Drive からファイルを削除"""
        try:
            self.drive_service.files().delete(fileId=file_id).execute()
            print(f"Deleted file ID: {file_id}")
        except Exception as e:
            print(f"Failed to delete file {file_id}: {e}")

    def empty_trash(self):
        """Google Drive のゴミ箱を空にする"""
        try:
            self.drive_service.files().emptyTrash().execute()
            print("ゴミ箱を空にしました。")
        except Exception as e:
            print(f"ゴミ箱の削除に失敗しました: {e}")

    def download_file_from_drive(self, file_id, local_filename):
        """Google Drive からファイルをダウンロード"""
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_filename, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            #print(f"Download {int(status.progress() * 100)}%.")
        print(f"Downloaded {local_filename}")
        return local_filename

    def upload_to_drive(self, local_file, filename, folder):
        """Google Drive にファイルをアップロード"""
        file_metadata = {
            'name': filename,
            'parents': [folder]  # リストにする必要あり
        }
        media = MediaFileUpload(local_file, resumable=True)
        file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded {filename} to Google Drive (File ID: {file['id']})")

    def get_drive_usage(self):
        """Google Drive の使用容量を取得"""
        try:
            storage_info = self.drive_service.about().get(fields="storageQuota").execute()
            usage = int(storage_info["storageQuota"]["usage"])  # 総使用量 (バイト)
            limit = int(storage_info["storageQuota"].get("limit", 0))  # 制限容量 (バイト, 無制限なら 0)

            usage_gb = usage / (1024 ** 3)
            limit_gb = limit / (1024 ** 3) if limit else "無制限"

            print(f"使用済みストレージ: {usage_gb:.2f} GB / {limit_gb} GB")
            return usage_gb, limit_gb
        except Exception as e:
            print(f"ストレージ情報の取得に失敗: {e}")

    def get_files_sorted_by_size(self):
        """Google Drive のファイルをサイズが大きい順に取得"""
        try:
            query = "trashed = false"
            files_list = []
            page_token = None

            while True:
                response = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, size)",
                    pageToken=page_token
                ).execute()

                for file in response.get("files", []):
                    if "size" in file:
                        files_list.append({
                            "name": file["name"],
                            "id": file["id"],
                            "size": int(file["size"])
                        })

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            # サイズ順にソート（降順）
            sorted_files = sorted(files_list, key=lambda x: x["size"], reverse=True)

            # 結果を表示
            for file in sorted_files[:10]:  # 上位10ファイルのみ表示
                print(f"Name: {file['name']}, Size: {file['size'] / (1024 ** 3):.2f} GB, ID: {file['id']}")

            return sorted_files

        except Exception as e:
            print(f"ファイルサイズ順の取得に失敗: {e}")


