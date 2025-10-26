#!/usr/bin/env python3
"""
update_manifest.py
manifest.jsonを生成してS3に全parquetファイルを一括アップロード
GitHub Actions対応: 最終ステップ、update_flag削除も実行
"""

from __future__ import annotations

import sys
from pathlib import Path
import json
from datetime import datetime
from typing import List, Dict

ROOT = Path(__file__).resolve().parents[2]  # scripts/pipeline/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.s3_manager import upload_to_s3
from common_cfg.paths import PARQUET_DIR

# S3にアップロードするファイル
UPLOAD_FILES = [
    "meta.parquet",
    "meta_jquants.parquet",
    "all_stocks.parquet",
    "grok_trending.parquet",
    "grok_backtest_meta.parquet",  # NEW: バックテストメタ情報
    "grok_top_stocks.parquet",     # NEW: Top5/Top10銘柄リスト
    "scalping_entry.parquet",
    "scalping_active.parquet",
    "prices_60d_15m.parquet",
    "prices_60d_5m.parquet",
    "prices_730d_1h.parquet",
    "prices_max_1d.parquet",
    "prices_max_1mo.parquet",
    "tech_snapshot_1d.parquet",
]

MANIFEST_PATH = PARQUET_DIR / "manifest.json"


def get_file_stats(file_path: Path) -> Dict[str, any]:
    """ファイルの統計情報を取得"""
    if not file_path.exists():
        return {
            "exists": False,
            "size_bytes": 0,
            "row_count": 0,
            "columns": [],
        }

    try:
        # ファイルサイズ
        size_bytes = file_path.stat().st_size

        # Parquetファイルの場合、行数とカラム情報を取得
        if file_path.suffix == ".parquet":
            df = pd.read_parquet(file_path)
            row_count = len(df)
            columns = df.columns.tolist()
        else:
            row_count = 0
            columns = []

        return {
            "exists": True,
            "size_bytes": size_bytes,
            "row_count": row_count,
            "columns": columns,
        }

    except Exception as e:
        print(f"  [WARN] Failed to read {file_path.name}: {e}")
        return {
            "exists": True,
            "size_bytes": file_path.stat().st_size if file_path.exists() else 0,
            "row_count": 0,
            "columns": [],
        }


def get_grok_metadata() -> Dict[str, any]:
    """grok_trending.parquet からメタデータを取得"""
    grok_file = PARQUET_DIR / "grok_trending.parquet"

    if not grok_file.exists():
        return {
            "grok_update_flag": False,
            "grok_last_update_date": None,
            "grok_last_update_time": None,
        }

    try:
        df = pd.read_parquet(grok_file)

        if df.empty:
            return {
                "grok_update_flag": False,
                "grok_last_update_date": None,
                "grok_last_update_time": None,
            }

        # 最新の date と selected_time を取得
        latest_date = df["date"].max() if "date" in df.columns else None

        # selected_time が存在するか確認
        if "selected_time" in df.columns:
            # 最新の selected_time を取得（26:00 があれば26:00、なければ16:00）
            times = df["selected_time"].unique()
            if "26:00" in times:
                latest_time = "26:00"
            elif "16:00" in times:
                latest_time = "16:00"
            else:
                latest_time = None
        else:
            latest_time = None

        # フラグ: データが存在し、日付と時刻が取得できた場合は true
        update_flag = bool(latest_date and latest_time)

        return {
            "grok_update_flag": update_flag,
            "grok_last_update_date": latest_date,
            "grok_last_update_time": latest_time,
        }

    except Exception as e:
        print(f"  [WARN] Failed to read grok metadata: {e}")
        return {
            "grok_update_flag": False,
            "grok_last_update_date": None,
            "grok_last_update_time": None,
        }


def generate_manifest() -> Dict[str, any]:
    """manifest.jsonを生成"""
    print("[INFO] Generating manifest.json...")

    # GROK メタデータを取得
    grok_meta = get_grok_metadata()

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "update_flag": datetime.now().strftime("%Y-%m-%d"),
        "grok_update_flag": grok_meta["grok_update_flag"],
        "grok_last_update_date": grok_meta["grok_last_update_date"],
        "grok_last_update_time": grok_meta["grok_last_update_time"],
        "files": {},
    }

    for filename in UPLOAD_FILES:
        file_path = PARQUET_DIR / filename
        stats = get_file_stats(file_path)

        manifest["files"][filename] = {
            "exists": stats["exists"],
            "size_bytes": stats["size_bytes"],
            "row_count": stats["row_count"],
            "columns": stats["columns"],
            "updated_at": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat() if file_path.exists() else None,
        }

        status = "✓" if stats["exists"] else "✗"
        print(f"  {status} {filename}: {stats['row_count']} rows, {stats['size_bytes']:,} bytes")

    print(f"  ✓ update_flag: {manifest['update_flag']}")
    return manifest


def save_manifest(manifest: Dict[str, any]) -> None:
    """manifest.jsonを保存"""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"[OK] Saved manifest.json: {MANIFEST_PATH}")


def upload_files_to_s3() -> bool:
    """parquetファイルとmanifest.jsonをS3にアップロード"""
    print("[INFO] Uploading files to S3...")

    # アップロード対象ファイルを収集（存在するもののみ）
    upload_targets = []

    for filename in UPLOAD_FILES:
        file_path = PARQUET_DIR / filename
        if file_path.exists():
            upload_targets.append(file_path)
        else:
            print(f"  [WARN] {filename} not found, skipping")

    # manifest.jsonも追加
    if MANIFEST_PATH.exists():
        upload_targets.append(MANIFEST_PATH)

    if not upload_targets:
        print("  [WARN] No files to upload")
        return False

    # S3にアップロード
    print(f"  [INFO] Uploading {len(upload_targets)} files...")
    success = upload_to_s3(upload_targets)

    if success:
        print(f"  ✓ Successfully uploaded {len(upload_targets)} files to S3")
    else:
        print(f"  ✗ S3 upload failed")

    return success


def cleanup_s3_old_files(keep_files: List[str]) -> None:
    """
    S3上の不要ファイルを削除（manifest.jsonに記載されたファイルのみ保持）

    Args:
        keep_files: 保持すべきファイル名のリスト
    """
    print("[INFO] Cleaning up old files from S3...")

    try:
        from common_cfg.s3cfg import load_s3_config
        import boto3

        cfg = load_s3_config()
        s3_client = boto3.client(
            "s3",
            region_name=cfg.region,
            endpoint_url=cfg.endpoint_url,
        )

        bucket = cfg.bucket or "stock-api-data"
        prefix = (cfg.prefix or "parquet/").rstrip("/") + "/"

        # S3上の全ファイルを取得
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            print("  [INFO] No files found in S3")
            return

        # 保持すべきファイルのキーを作成（manifest.json含む）
        keep_keys = {prefix + f for f in keep_files}
        keep_keys.add(prefix + "manifest.json")

        # 削除対象のファイルを抽出
        delete_targets = [
            obj for obj in response["Contents"]
            if obj["Key"] not in keep_keys and obj["Key"] != prefix  # ディレクトリ自体は除外
        ]

        if not delete_targets:
            print("  [INFO] No files to delete")
            return

        print(f"  [INFO] Found {len(delete_targets)} files to delete")
        for obj in delete_targets:
            try:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                filename = obj["Key"].replace(prefix, "")
                print(f"    ✓ Deleted from S3: {filename}")
            except Exception as e:
                print(f"    ✗ Failed to delete {obj['Key']}: {e}")

    except Exception as e:
        print(f"  [WARN] S3 cleanup failed: {e}")


def main() -> int:
    print("=" * 60)
    print("Update Manifest and Upload to S3")
    print("=" * 60)

    # [STEP 1] manifest.json生成
    print("\n[STEP 1] Generating manifest.json...")
    try:
        manifest = generate_manifest()
        save_manifest(manifest)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] S3アップロード
    print("\n[STEP 2] Uploading to S3...")
    try:
        upload_success = upload_files_to_s3()
        if not upload_success:
            print("  ⚠ S3 upload had issues (check configuration)")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 3] S3上の不要ファイルを削除（manifest.jsonに記載されたファイルのみ保持）
    print("\n[STEP 3] Cleaning up old files from S3...")
    try:
        cleanup_s3_old_files(UPLOAD_FILES)
    except Exception as e:
        print(f"  ⚠ S3 cleanup failed: {e}")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Manifest generated: {MANIFEST_PATH}")
    print(f"Files in manifest: {len(manifest['files'])}")
    print(f"S3 upload: {'✓ Success' if upload_success else '⚠ Failed or skipped'}")
    print("=" * 60)

    print("\n✅ Manifest update and S3 upload completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
