#!/usr/bin/env python3
"""
一時スクリプト: manifest.json の grok_analysis_merged.parquet 情報を更新してS3にアップロード
"""
import json
import boto3
from pathlib import Path
from datetime import datetime
import os

ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT / 'data' / 'parquet' / 'backtest'
MANIFEST_PATH = BACKTEST_DIR / 'manifest.json'
GROK_ANALYSIS_PATH = BACKTEST_DIR / 'grok_analysis_merged.parquet'

S3_BUCKET = "stock-api-data"
S3_PREFIX = "parquet/"
AWS_REGION = "ap-northeast-1"

s3 = boto3.client('s3', region_name=AWS_REGION)

def main():
    print("=" * 60)
    print("Updating manifest.json for grok_analysis_merged.parquet")
    print("=" * 60)

    # manifest.json を読み込み
    print("\n[Step 1] Loading manifest.json...")
    with open(MANIFEST_PATH, 'r', encoding='utf-8') as f:
        manifest = json.load(f)

    print(f"✅ Loaded manifest with {len(manifest.get('files', []))} files")

    # grok_analysis_merged.parquet のファイル情報を取得
    print("\n[Step 2] Updating grok_analysis_merged.parquet info...")

    file_stat = os.stat(GROK_ANALYSIS_PATH)
    file_size = file_stat.st_size
    file_mtime = datetime.fromtimestamp(file_stat.st_mtime).isoformat()

    print(f"  File size: {file_size:,} bytes ({file_size / 1024:.1f} KB)")
    print(f"  Modified time: {file_mtime}")

    # manifest内のgrok_analysis_merged.parquet情報を更新
    updated = False
    for file_info in manifest.get('files', []):
        if file_info.get('name') == 'grok_analysis_merged.parquet':
            file_info['size'] = file_size
            file_info['last_modified'] = file_mtime
            updated = True
            print(f"  ✅ Updated existing entry")
            break

    if not updated:
        # エントリが存在しない場合は追加
        manifest.setdefault('files', []).append({
            'name': 'grok_analysis_merged.parquet',
            'size': file_size,
            'last_modified': file_mtime,
            'path': 'backtest/grok_analysis_merged.parquet'
        })
        print(f"  ✅ Added new entry")

    # manifest全体の更新時刻を更新
    manifest['last_updated'] = datetime.now().isoformat()

    # 保存
    print("\n[Step 3] Saving manifest.json...")
    with open(MANIFEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved to {MANIFEST_PATH}")

    # S3にアップロード
    print("\n[Step 4] Uploading to S3...")

    s3_key = f"{S3_PREFIX}backtest/manifest.json"
    s3.upload_file(str(MANIFEST_PATH), S3_BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")

    print("\n" + "=" * 60)
    print("✅ manifest.json update completed!")
    print("=" * 60)

if __name__ == '__main__':
    main()
