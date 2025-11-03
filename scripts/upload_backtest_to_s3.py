"""
更新されたバックテストparquetファイルをS3にアップロードするスクリプト
"""

import os
import sys
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# データディレクトリ
DATA_DIR = project_root / "data" / "parquet" / "backtest"

# S3設定
S3_BUCKET = "stock-api-data"
S3_PREFIX = "parquet/backtest/"

def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """
    ファイルをS3にアップロード

    Args:
        local_path: ローカルファイルパス
        s3_key: S3のキー

    Returns:
        成功時True、失敗時False
    """
    try:
        s3_client = boto3.client('s3')

        print(f"  アップロード中: {local_path.name} -> s3://{S3_BUCKET}/{s3_key}")

        s3_client.upload_file(
            str(local_path),
            S3_BUCKET,
            s3_key,
            ExtraArgs={'ContentType': 'application/octet-stream'}
        )

        print(f"  ✓ アップロード完了")
        return True

    except ClientError as e:
        print(f"  ✗ S3アップロードエラー: {e}")
        return False
    except Exception as e:
        print(f"  ✗ エラー: {e}")
        return False


def main():
    """メイン処理"""
    print("=" * 80)
    print("バックテストファイルS3アップロードスクリプト")
    print("=" * 80)
    print(f"バケット: {S3_BUCKET}")
    print(f"プレフィックス: {S3_PREFIX}")
    print("=" * 80)

    # アップロード対象ファイル
    files = [
        "grok_trending_20251029.parquet",
        "grok_trending_20251030.parquet",
        "grok_trending_20251031.parquet",
        "grok_trending_archive.parquet",
        "manifest.json"
    ]

    success_count = 0
    failed_count = 0

    for file_name in files:
        file_path = DATA_DIR / file_name

        if not file_path.exists():
            print(f"\n警告: {file_name} が見つかりません")
            failed_count += 1
            continue

        print(f"\n処理中: {file_name}")
        s3_key = f"{S3_PREFIX}{file_name}"

        if upload_to_s3(file_path, s3_key):
            success_count += 1
        else:
            failed_count += 1

    # 結果サマリー
    print("\n" + "=" * 80)
    print("アップロード完了")
    print("=" * 80)
    print(f"成功: {success_count} ファイル")
    print(f"失敗: {failed_count} ファイル")

    if failed_count == 0:
        print("\n✓ すべてのファイルが正常にアップロードされました")
        print(f"\nS3バックアップ場所:")
        print(f"  s3://{S3_BUCKET}/{S3_PREFIX}")
    else:
        print(f"\n⚠ {failed_count} ファイルのアップロードに失敗しました")


if __name__ == "__main__":
    main()
