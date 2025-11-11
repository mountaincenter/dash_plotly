"""
バックテスト・推奨ファイルをS3にアップロード

初回セットアップ用スクリプト
"""

import boto3
import json
from pathlib import Path
from datetime import datetime
import os

# download_from_s3.pyから設定読み込み
import sys
sys.path.insert(0, str(Path(__file__).parent))
from download_from_s3 import load_s3_config

cfg = load_s3_config()
S3_BUCKET = cfg.bucket
AWS_REGION = cfg.region

BASE_DIR = Path(__file__).parent.parent.parent
PARQUET_DIR = BASE_DIR / 'data' / 'parquet'
BACKTEST_DIR = PARQUET_DIR / 'backtest'

# アップロード対象ファイル
UPLOAD_FILES = [
    ('backtest/grok_trending_analysis.parquet', 'grok_trending_analysis.parquet'),
    ('backtest/grok_recommendations_history.parquet', 'grok_recommendations_history.parquet'),
    ('backtest/grok_analysis_merged.parquet', 'grok_analysis_merged.parquet'),
    ('backtest/trading_recommendation.json', 'trading_recommendation.json'),
]


def upload_to_s3():
    """ファイルをS3にアップロード"""

    s3_client = boto3.client('s3', region_name=AWS_REGION)

    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Upload directory: parquet/\n")

    uploaded_files = []

    for s3_key, local_filename in UPLOAD_FILES:
        local_path = BACKTEST_DIR / local_filename

        if not local_path.exists():
            print(f"✗ File not found: {local_path}")
            continue

        s3_full_key = f"parquet/{s3_key}"

        try:
            # ファイルサイズ取得
            file_size = local_path.stat().st_size / 1024  # KB

            # アップロード
            s3_client.upload_file(
                str(local_path),
                S3_BUCKET,
                s3_full_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )

            print(f"✓ Uploaded: {s3_full_key} ({file_size:.1f} KB)")
            uploaded_files.append(s3_key)

        except Exception as e:
            print(f"✗ Failed to upload {s3_full_key}: {e}")

    return uploaded_files


def update_manifest(uploaded_files):
    """manifest.jsonを更新"""

    s3_client = boto3.client('s3', region_name=AWS_REGION)
    manifest_key = 'parquet/manifest.json'

    # 既存のmanifest.jsonをダウンロード
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=manifest_key)
        manifest = json.loads(response['Body'].read().decode('utf-8'))
        print(f"\n✓ Downloaded existing manifest.json")
    except s3_client.exceptions.NoSuchKey:
        # manifest.jsonが存在しない場合は新規作成
        manifest = {
            'generated_at': datetime.now().isoformat(),
            'update_flag': datetime.now().strftime('%Y-%m-%d'),
            'files': {}
        }
        print(f"\n! manifest.json not found, creating new one")

    # filesが辞書形式であることを確認
    if not isinstance(manifest.get('files'), dict):
        manifest['files'] = {}

    # 新しいファイルを追加（ファイル名をキーとする）
    for s3_key in uploaded_files:
        filename = s3_key.split('/')[-1]  # backtest/file.parquet → file.parquet

        # S3からファイル情報取得
        try:
            obj = s3_client.head_object(Bucket=S3_BUCKET, Key=f'parquet/{s3_key}')

            manifest['files'][filename] = {
                'exists': True,
                'size_bytes': obj['ContentLength'],
                'row_count': 0,  # parquetから取得する場合は別途処理が必要
                'columns': [],    # parquetから取得する場合は別途処理が必要
                'updated_at': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"✗ Failed to get metadata for {filename}: {e}")

    # manifest更新
    manifest['generated_at'] = datetime.now().isoformat()
    manifest['update_flag'] = datetime.now().strftime('%Y-%m-%d')

    # manifest.jsonをアップロード
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=4, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json'
    )

    print(f"✓ Updated manifest.json ({len(manifest['files'])} files)")

    return manifest


def main():
    print("=== Upload Backtest Files to S3 ===\n")

    # ファイルが存在するか確認
    for _, local_filename in UPLOAD_FILES:
        local_path = BACKTEST_DIR / local_filename
        if not local_path.exists():
            print(f"✗ Required file not found: {local_path}")
            print(f"\nPlease create files first by running:")
            print(f"  cd {BASE_DIR}")
            print(f"  python scripts/merge_grok_backtest_results.py")
            return

    # アップロード
    uploaded_files = upload_to_s3()

    if not uploaded_files:
        print("\n✗ No files uploaded")
        return

    # manifest.json更新
    manifest = update_manifest(uploaded_files)

    print(f"\n=== Summary ===")
    print(f"Uploaded {len(uploaded_files)} files to S3")
    print(f"Total files in manifest: {len(manifest['files'])}")
    print(f"\nTo sync locally:")
    print(f"  cd {BASE_DIR}")
    print(f"  python scripts/sync/download_from_s3.py --clean")


if __name__ == '__main__':
    main()
