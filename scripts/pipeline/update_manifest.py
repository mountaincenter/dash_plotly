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
    "margin_code_master.parquet",  # 取引制限マスタ（信用取引制限コード）
    "all_stocks.parquet",
    "financials.parquet",  # J-Quants財務データ
    "announcements.parquet",  # J-Quants決算発表日推定
    "grok_trending.parquet",
    "grok_backtest_meta.parquet",  # NEW: バックテストメタ情報
    "grok_top_stocks.parquet",     # NEW: Top5/Top10銘柄リスト
    "scalping_entry.parquet",
    "scalping_active.parquet",
    "prices_5d_1m.parquet",
    "prices_60d_15m.parquet",
    "prices_60d_5m.parquet",
    "prices_730d_1h.parquet",
    "prices_max_1d.parquet",
    "prices_max_1mo.parquet",
    "tech_snapshot_1d.parquet",
    "intraday_analysis.parquet",  # 日中分析データ（23:00生成）
    "intraday_averages.parquet",  # 日中分析平均データ（23:00生成）
    # 指数・ETF
    "index_prices_60d_15m.parquet",
    "index_prices_60d_5m.parquet",
    "index_prices_730d_1h.parquet",
    "index_prices_max_1d.parquet",
    "index_prices_max_1mo.parquet",
    # 先物
    "futures_prices_60d_15m.parquet",
    "futures_prices_60d_5m.parquet",
    "futures_prices_730d_1h.parquet",
    "futures_prices_max_1d.parquet",
    "futures_prices_max_1mo.parquet",
    # 為替
    "currency_prices_730d_1h.parquet",
    "currency_prices_max_1d.parquet",
    "currency_prices_max_1mo.parquet",
    # J-Quants指数データ (Standard plan)
    "topix_prices_max_1d.parquet",
    "sectors_prices_max_1d.parquet",
    "series_prices_max_1d.parquet",
    # Static銘柄シグナル（manifest.jsonに含めて保護、アップロードはworkflowで実行）
    "static_signals.parquet",
    # 取引結果
    "stock_results.parquet",
    "stock_results_summary.parquet",
    # Grok Analysis (backtest配下は除外 - s3-sync.ymlで保護される)
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
    """ローカルのgrok_trending.parquet からメタデータを取得"""
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

    # backtest/ ディレクトリのアーカイブファイルを追加
    backtest_dir = PARQUET_DIR / "backtest"
    archive_file = backtest_dir / "grok_trending_archive.parquet"

    if archive_file.exists():
        upload_targets.append(archive_file)
        print(f"  [INFO] Added backtest archive: grok_trending_archive.parquet")
    else:
        print(f"  [INFO] No backtest archive found (expected after first 16:00 run)")

    # market_summary/ ディレクトリのファイルを追加
    market_summary_dir = PARQUET_DIR / "market_summary"
    if market_summary_dir.exists():
        # raw/*.md ファイル
        raw_dir = market_summary_dir / "raw"
        if raw_dir.exists():
            for md_file in raw_dir.glob("*.md"):
                upload_targets.append(md_file)

        # structured/*.json ファイル
        structured_dir = market_summary_dir / "structured"
        if structured_dir.exists():
            for json_file in structured_dir.glob("*.json"):
                upload_targets.append(json_file)

        md_count = len(list(raw_dir.glob("*.md"))) if raw_dir.exists() else 0
        json_count = len(list(structured_dir.glob("*.json"))) if structured_dir.exists() else 0
        if md_count > 0 or json_count > 0:
            print(f"  [INFO] Added market_summary: {md_count} markdown, {json_count} json files")

    # manifest.jsonも追加
    if MANIFEST_PATH.exists():
        upload_targets.append(MANIFEST_PATH)

    if not upload_targets:
        print("  [WARN] No files to upload")
        return False

    # S3にアップロード（PARQUET_DIRを基準にサブディレクトリ構造を保持）
    print(f"  [INFO] Uploading {len(upload_targets)} files...")
    success = upload_to_s3(upload_targets, base_dir=PARQUET_DIR)

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
        keep_keys.add(prefix + "backtest/grok_trending_archive.parquet")  # アーカイブファイルも保持
        keep_keys.add(prefix + "backtest/grok_analysis_merged.parquet")  # バックテスト統合データ（v2.0.3）も保持
        keep_keys.add(prefix + "backtest/grok_analysis_merged_v2_1.parquet")  # バックテスト統合データ（v2.1）も保持
        keep_keys.add(prefix + "backtest/trading_recommendation.json")  # 売買推奨データも保持
        keep_keys.add(prefix + "backtest/static_backtest.parquet")  # Static銘柄バックテストも保持
        keep_keys.add(prefix + "grok_day_trade_list.parquet")  # デイトレードリスト（手動管理）は絶対に削除しない
        keep_keys.add(prefix + "grok_prices_max_1d.parquet")  # ML学習・予測用の価格データ（ML Retrainingで使用）
        keep_keys.add(prefix + "ml/grok_lgbm_model.pkl")  # MLモデル（ML Retrainingで生成・使用）
        keep_keys.add(prefix + "ml/grok_lgbm_meta.json")  # MLメタ情報（ML Retrainingで生成・使用）
        keep_keys.add(prefix + "ml/archive_with_features.parquet")  # ML特徴量データ（ML Retrainingで生成）

        # backtest/grok_trending_YYYYMMDD.parquet ファイルも保護（7日分）
        # backtest/deep_analysis_YYYY-MM-DD.json ファイルも保護
        # backtest/analysis/deep_analysis_YYYY-MM-DD.json ファイルも保護
        # market_summary/ 配下のファイルも保護
        # これらは data-pipeline.yml の "Archive GROK trending for backtest" および "Generate market summary" ステップで管理される
        import re
        for obj in response.get("Contents", []):
            key = obj["Key"]
            # backtest/grok_trending_YYYYMMDD.parquet パターンにマッチするファイルは保持
            if re.match(rf"{prefix}backtest/grok_trending_\d{{8}}\.parquet$", key):
                keep_keys.add(key)
            # backtest/deep_analysis_YYYY-MM-DD.json パターンにマッチするファイルは保持
            if re.match(rf"{prefix}backtest/deep_analysis_\d{{4}}-\d{{2}}-\d{{2}}\.json$", key):
                keep_keys.add(key)
            # backtest/analysis/deep_analysis_YYYY-MM-DD.json パターンにマッチするファイルは保持
            if re.match(rf"{prefix}backtest/analysis/deep_analysis_\d{{4}}-\d{{2}}-\d{{2}}\.json$", key):
                keep_keys.add(key)
            # market_summary/raw/YYYY-MM-DD.md パターンにマッチするファイルは保持
            if re.match(rf"{prefix}market_summary/raw/\d{{4}}-\d{{2}}-\d{{2}}\.md$", key):
                keep_keys.add(key)
            # market_summary/structured/YYYY-MM-DD.json パターンにマッチするファイルは保持
            if re.match(rf"{prefix}market_summary/structured/\d{{4}}-\d{{2}}-\d{{2}}\.json$", key):
                keep_keys.add(key)

        # 削除対象のファイルを抽出
        delete_targets = [
            obj for obj in response["Contents"]
            if obj["Key"] not in keep_keys and obj["Key"] != prefix and not obj["Key"].endswith("/")  # ディレクトリ自体は除外
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
