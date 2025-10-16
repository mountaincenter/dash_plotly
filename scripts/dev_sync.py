#!/usr/bin/env python3
"""
開発環境：本番S3から最新データを同期
manifest.jsonのタイムスタンプを比較して、S3の方が新しい場合のみ同期
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import MANIFEST_PATH, PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import download_file

import boto3


def main() -> int:
    load_dotenv_cascade()

    cfg = load_s3_config()
    bucket = cfg.bucket or "dash-plotly"
    prefix = cfg.prefix or "parquet/"

    print("📥 Checking for updates from S3...")
    print(f"   Bucket: s3://{bucket}/{prefix}")
    print(f"   Local:  {PARQUET_DIR}")
    print()

    # data/parquetディレクトリが存在しない場合は作成
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # S3クライアント作成
    session = boto3.Session(profile_name=cfg.profile if cfg.profile else None)
    s3 = session.client(
        "s3",
        region_name=cfg.region or "ap-northeast-1",
        endpoint_url=cfg.endpoint_url,
    )

    # S3のmanifest.jsonをダウンロード
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
        temp_manifest_path = Path(tmp.name)

    try:
        s3.download_file(bucket, f"{prefix}manifest.json", str(temp_manifest_path))
    except Exception as e:
        print(f"❌ Failed to download manifest.json from S3: {e}")
        temp_manifest_path.unlink(missing_ok=True)
        return 1

    # S3のmanifest.jsonを読み込み
    try:
        with open(temp_manifest_path) as f:
            s3_manifest = json.load(f)
        s3_time = s3_manifest.get("generated_at", "1970-01-01T00:00:00+00:00")
    except Exception as e:
        print(f"❌ Failed to parse S3 manifest.json: {e}")
        temp_manifest_path.unlink(missing_ok=True)
        return 1

    # ローカルのmanifest.jsonが存在しない場合は必ず同期
    if not MANIFEST_PATH.exists():
        print("ℹ️ Local manifest.json not found. Syncing all data...")
        _sync_from_s3(s3, bucket, prefix, PARQUET_DIR)
        temp_manifest_path.unlink(missing_ok=True)
        print()
        print("✅ Development environment initialized with production data")
        _list_files(PARQUET_DIR)
        return 0

    # ローカルのmanifest.jsonを読み込み
    try:
        with open(MANIFEST_PATH) as f:
            local_manifest = json.load(f)
        local_time = local_manifest.get("generated_at", "1970-01-01T00:00:00+00:00")
    except Exception:
        local_time = "1970-01-01T00:00:00+00:00"

    print(f"Local timestamp: {local_time}")
    print(f"S3 timestamp:    {s3_time}")
    print()

    # ISO 8601形式の文字列比較（辞書順で比較可能）
    force_sync = "--force" in sys.argv

    if s3_time > local_time:
        print("✅ S3 data is newer. Syncing...")
        _sync_from_s3(s3, bucket, prefix, PARQUET_DIR)
        print()
        print("✅ Development environment updated with production data")
        print()
        print("Updated files:")
        _list_files(PARQUET_DIR)
    elif s3_time == local_time:
        print("ℹ️ Local data is up to date. No sync needed.")
    else:
        print("⚠️ Local data is NEWER than S3 (development changes detected)")
        print("   Keeping local data. Run with --force to overwrite.")

        if force_sync:
            print()
            print("🔄 Force syncing from S3...")
            _sync_from_s3(s3, bucket, prefix, PARQUET_DIR)
            print("✅ Forced sync completed")

    # クリーンアップ
    temp_manifest_path.unlink(missing_ok=True)
    return 0


def _sync_from_s3(s3, bucket: str, prefix: str, local_dir: Path) -> None:
    """S3から.parquetファイルとmanifest.jsonを同期"""
    # List all objects in S3 with the prefix
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']
            filename = key[len(prefix):]  # Remove prefix

            # Only sync .parquet files and manifest.json
            if filename.endswith('.parquet') or filename == 'manifest.json':
                local_path = local_dir / filename
                print(f"  Downloading {filename}...")
                try:
                    s3.download_file(bucket, key, str(local_path))
                except Exception as e:
                    print(f"  ⚠️ Failed to download {filename}: {e}")


def _list_files(directory: Path) -> None:
    """List files in directory with sizes"""
    if not directory.exists():
        return

    for file_path in sorted(directory.glob("*")):
        if file_path.is_file():
            size = file_path.stat().st_size
            size_mb = size / (1024 * 1024)
            print(f"  {file_path.name:40s} {size_mb:8.2f} MB")


if __name__ == "__main__":
    raise SystemExit(main())
