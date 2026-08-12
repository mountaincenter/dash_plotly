#!/usr/bin/env python3
"""
update_manifest.py
manifest.jsonを生成してS3に全parquetファイルを一括アップロード
GitHub Actions対応: 最終ステップ、update_flag削除も実行
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict

ROOT = Path(__file__).resolve().parents[2]  # scripts/pipeline/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.s3_manager import upload_to_s3
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from scripts.lib.protected_archive_s3 import (
    file_sha256,
    read_remote_manifest,
)
from scripts.pipeline.manage_all_market_microstructure import (
    MINUTE_S3_ROOT,
    TICK_S3_ROOT,
)
from scripts.pipeline.manage_jquants_earnings_events import (
    EARNINGS_DATE_S3_ROOT,
    FINS_SUMMARY_S3_ROOT,
)

# S3にアップロードするファイル
UPLOAD_FILES = [
    "meta.parquet",
    "meta_jquants.parquet",
    "margin_code_master.parquet",  # 取引制限マスタ（信用取引制限コード）
    "all_stocks.parquet",
    "trading_value_top100.parquet",  # J-Quants日足売買代金Top100
    "trading_value_top_history.parquet",  # J-Quants日足売買代金Top150履歴
    "market_basket_turnover.parquet",  # N225/TOPIX basket turnover series
    "semicon_watch_universe.parquet",  # 半導体/AI/DC静的監視 universe
    "watch_minute_universe.parquet",  # grok + top100 + semicon の分足取得 universe
    "jquants_minute_watch.parquet",  # watch universe J-Quants分足
    "jquants_minute_watch_features.parquet",  # VWAP等の分足特徴量
    "jquants/watch_daily_features.parquet",  # MktCap/ExRT/AdjFactor nullable日次sidecar
    "market_flow_200a_forward.parquet",  # 200A固定ルールの前向きshadow記録
    "market_flow_200a_phase_status.json",  # Phase2実績とPhase3昇格ゲート
    "market_flow_checkpoint_validation.json",  # Market Flow先行公開の検証証跡
    "etf_0910_preopen.json",  # 200A 07:00判定スナップショット
    "etf_0910_us_daily.parquet",  # 200A 07:00判定用の米国日足cache
    "financials.parquet",  # J-Quants財務データ
    "announcements.parquet",  # J-Quants決算発表日推定
    "grok_trending.parquet",
    "grok_backtest_meta.parquet",  # NEW: バックテストメタ情報
    "grok_top_stocks.parquet",     # NEW: Top5/Top10銘柄リスト
    "jquants/grok_archive_minute.parquet",  # Grok対象日のJ-Quants累積分足cache
    "jquants/grok_jquants_daily.parquet",  # Grok対象日のJ-Quants累積日足cache
    "backtest/grok_master_jquants_segments.parquet",  # Grok分析用J-Quants基準master（archiveは不変）
    "backtest/grok_master_jquants_segments.validation.json",  # master公開前validation結果
    "backtest/grok_holding_returns.parquet",  # 正本を変更しないd1-d5派生損益台帳
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
    # 取引結果
    "stock_results.parquet",
    "stock_results_summary.parquet",
    # 日経VI
    "nikkei_vi_max_1d.parquet",
    # グランビルIFDシグナル（backtest配下は除外 - backtest_granville_ifd.pyで直接S3アップロード）
    "granville_ifd_signals.parquet",
    # カレンダーアノマリー分析（金曜生成）
    "market_anomaly.parquet",
    # signals.parquet（全戦略統合）
    "signals.parquet",
    # カレンダー
    "calendar.parquet",
    "etf_1306_prices.parquet",
    "prices_topix500_oc.parquet",
]

# Canonical files are recorded in manifest.json but are never part of the
# generic upload list. This pipeline treats them as read-only.
PROTECTED_FILES = [
    "backtest/grok_trending_archive.parquet",
]

# These are cumulative/validated outputs produced after the main pipeline
# runner. A full manifest pass must retain the last verified remote entry until
# the dedicated builder publishes its replacement later in the same workflow.
PRESERVE_IF_MISSING_FILES = {
    "backtest/grok_master_jquants_segments.parquet",
    "backtest/grok_master_jquants_segments.validation.json",
}

MANIFEST_PATH = PARQUET_DIR / "manifest.json"
TRENDING_NAME = "grok_trending.parquet"


def resolve_storage_mode() -> tuple[str, bool]:
    app_env = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("STAGE")
        or "local"
    ).strip().lower()
    production = app_env in {"production", "prod"}
    expected = "s3" if production else "local"
    configured = (os.getenv("STORAGE_MODE") or expected).strip().lower()
    if configured != expected:
        raise RuntimeError(
            f"storage mode mismatch: APP_ENV={app_env} requires "
            f"STORAGE_MODE={expected}, got {configured}"
        )
    return app_env, production


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
        size_bytes = file_path.stat().st_size
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
    except Exception as error:
        print(f"  [WARN] Failed to read {file_path.name}: {error}")
        return {
            "exists": True,
            "size_bytes": file_path.stat().st_size if file_path.exists() else 0,
            "row_count": 0,
            "columns": [],
        }


def load_existing_manifest(*, use_s3: bool) -> Dict[str, any]:
    """Load exactly one environment-owned manifest source."""
    if use_s3:
        try:
            remote = read_remote_manifest(load_s3_config())
            print("  [INFO] Existing manifest loaded from S3")
            return remote
        except Exception as error:
            raise RuntimeError(
                f"Production S3 manifest unavailable: {error}"
            ) from error
    if MANIFEST_PATH.exists():
        try:
            local = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
            if isinstance(local, dict) and isinstance(local.get("files"), dict):
                print("  [INFO] Existing manifest loaded from local fallback")
                return local
        except Exception as error:
            print(f"  [WARN] Existing local manifest is invalid: {error}")
    return {"files": {}}


def get_protected_file_entry(
    filename: str,
    existing_entry: Dict[str, any] | None = None,
) -> Dict[str, any]:
    """Build or preserve a checksum-pinned canonical manifest entry."""
    file_path = PARQUET_DIR / filename
    stats = get_file_stats(file_path)
    if not file_path.exists():
        if (
            isinstance(existing_entry, dict)
            and existing_entry.get("protected") is True
            and existing_entry.get("canonical") is True
            and existing_entry.get("sha256")
        ):
            print(f"  [INFO] Preserving remote protected entry: {filename}")
            return dict(existing_entry)
        raise RuntimeError(
            f"Protected file is absent and has no valid existing manifest entry: {filename}"
        )

    entry = {
        "exists": True,
        "size_bytes": stats["size_bytes"],
        "row_count": stats["row_count"],
        "columns": stats["columns"],
        "updated_at": (
            datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
            if file_path.exists()
            else None
        ),
        "protected": True,
        "canonical": True,
        "upload_policy": "manual_explicit_only",
        "automatic_bulk_upload": False,
        "protection_reason": "canonical archive; automated mutation prohibited",
    }
    entry["sha256"] = file_sha256(file_path)

    if file_path.suffix == ".parquet":
        frame = pd.read_parquet(file_path, columns=["backtest_date", "ticker"])
        dates = pd.to_datetime(frame["backtest_date"], errors="coerce")
        entry["date_min"] = dates.min().date().isoformat()
        entry["date_max"] = dates.max().date().isoformat()
        entry["unique_ticker_date_keys"] = int(
            frame[["ticker", "backtest_date"]].drop_duplicates().shape[0]
        )

    existing_sha = existing_entry.get("sha256") if existing_entry else None
    if existing_sha == entry["sha256"]:
        return {**existing_entry, **entry} if existing_entry else entry

    raise RuntimeError(
        "Protected canonical archive checksum differs from the existing manifest; "
        "automated manifest advancement is prohibited"
    )


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


def generate_manifest(
    existing_manifest: Dict[str, any] | None = None,
    *,
    preserve_missing: bool = False,
    preserve_grok: bool = False,
) -> Dict[str, any]:
    """manifest.jsonを生成"""
    print("[INFO] Generating manifest.json...")

    # GROK メタデータを取得
    grok_meta = get_grok_metadata()

    existing_datasets = (existing_manifest or {}).get("datasets", {})
    if not isinstance(existing_datasets, dict):
        raise RuntimeError("Existing manifest datasets must be an object")

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "update_flag": datetime.now().strftime("%Y-%m-%d"),
        "grok_update_flag": grok_meta["grok_update_flag"],
        "grok_last_update_date": grok_meta["grok_last_update_date"],
        "grok_last_update_time": grok_meta["grok_last_update_time"],
        "files": {},
        "datasets": dict(existing_datasets),
    }
    if preserve_grok:
        for field in [
            "grok_update_flag",
            "grok_last_update_date",
            "grok_last_update_time",
        ]:
            if field in (existing_manifest or {}):
                manifest[field] = existing_manifest[field]

    for filename in UPLOAD_FILES:
        file_path = PARQUET_DIR / filename
        stats = get_file_stats(file_path)

        existing_entry = (existing_manifest or {}).get("files", {}).get(filename)
        if preserve_grok and filename == TRENDING_NAME:
            if isinstance(existing_entry, dict):
                manifest["files"][filename] = dict(existing_entry)
                print(f"  ↻ {filename}: preserving existing remote entry")
            else:
                manifest["files"][filename] = {
                    "exists": False,
                    "size_bytes": 0,
                    "row_count": 0,
                    "columns": [],
                    "updated_at": None,
                }
                print(f"  - {filename}: no existing remote entry to preserve")
            continue
        if (
            (preserve_missing or filename in PRESERVE_IF_MISSING_FILES)
            and not stats["exists"]
            and isinstance(existing_entry, dict)
        ):
            manifest["files"][filename] = dict(existing_entry)
            print(f"  ↻ {filename}: preserving existing remote entry")
            continue

        manifest["files"][filename] = {
            "exists": stats["exists"],
            "size_bytes": stats["size_bytes"],
            "row_count": stats["row_count"],
            "columns": stats["columns"],
            "updated_at": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat() if file_path.exists() else None,
        }

        status = "✓" if stats["exists"] else "✗"
        print(f"  {status} {filename}: {stats['row_count']} rows, {stats['size_bytes']:,} bytes")

    for filename in PROTECTED_FILES:
        existing_entry = (
            (existing_manifest or {}).get("files", {}).get(filename)
            if isinstance((existing_manifest or {}).get("files", {}), dict)
            else None
        )
        entry = get_protected_file_entry(filename, existing_entry)
        manifest["files"][filename] = entry
        status = "✓" if entry["exists"] else "✗"
        print(
            f"  {status} {filename}: protected canonical, "
            f"{entry['row_count']} rows, sha256={entry['sha256']}"
        )

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
        if filename == TRENDING_NAME:
            print(
                "  [INFO] grok_trending.parquet is excluded from generic upload; "
                "it is published only after market-cap and ML processing"
            )
            continue
        file_path = PARQUET_DIR / filename
        if file_path.exists():
            upload_targets.append(file_path)
        else:
            print(f"  [WARN] {filename} not found, skipping")

    # 正本archiveはmanifestにchecksum付きで記録するが、一括アップロードはしない。
    # 更新はこのpipelineの対象外。正本は読取専用である。
    backtest_dir = PARQUET_DIR / "backtest"
    archive_file = backtest_dir / "grok_trending_archive.parquet"

    if archive_file.exists():
        print(f"  [INFO] Protected immutable archive (not uploaded): grok_trending_archive.parquet")
    else:
        print(f"  [INFO] No backtest archive found (expected after first 16:00 run)")

    granville_archive = backtest_dir / "granville_ifd_archive.parquet"
    if granville_archive.exists():
        upload_targets.append(granville_archive)
        print(f"  [INFO] Added Granville IFD archive: granville_ifd_archive.parquet")

    # CI先行指数（improvement/data/macro/ → S3: macro/estat_ci_index.parquet）
    # base_dir外なので直接S3アップロード
    ci_file = ROOT / "improvement" / "data" / "macro" / "estat_ci_index.parquet"
    if ci_file.exists():
        try:
            from common_cfg.s3cfg import load_s3_config
            import boto3
            ci_cfg = load_s3_config()
            ci_s3 = boto3.client("s3", region_name=ci_cfg.region, endpoint_url=ci_cfg.endpoint_url)
            ci_key = f"{ci_cfg.prefix}macro/estat_ci_index.parquet"
            ci_s3.upload_file(
                str(ci_file), ci_cfg.bucket, ci_key,
                ExtraArgs={"ContentType": "application/octet-stream", "CacheControl": "max-age=60", "ServerSideEncryption": "AES256"},
            )
            print(f"  [INFO] Uploaded CI index: {ci_key}")
        except Exception as e:
            print(f"  [WARN] CI index upload failed: {e}")

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


def upload_manifest_only() -> bool:
    """Upload manifest after the finalized daily Grok artifact was published."""
    if not MANIFEST_PATH.exists():
        print("  [ERROR] manifest.json does not exist")
        return False
    return upload_to_s3([MANIFEST_PATH], base_dir=PARQUET_DIR)


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
        objects = []
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(page.get("Contents", []))
        if not objects:
            print("  [INFO] No files found in S3")
            return

        # 保持すべきファイルのキーを作成（manifest.json含む）
        keep_keys = {prefix + f for f in keep_files}
        keep_keys.add(prefix + "manifest.json")
        keep_keys.add(prefix + "backtest/grok_trending_archive.parquet")  # アーカイブファイルも保持
        keep_keys.add(prefix + "jquants/grok_archive_minute.parquet")  # Grok J-Quants累積分足cacheも保持
        keep_keys.add(prefix + "backtest/grok_master_jquants_segments.parquet")  # J-Quants基準masterも保持
        keep_keys.add(prefix + "backtest/grok_master_jquants_segments.validation.json")  # master validationも保持
        keep_keys.add(prefix + "backtest/granville_b1b4_archive.parquet")  # グランビルB1-B4バックテストアーカイブ
        keep_keys.add(prefix + "positions.parquet")  # グランビルポジション管理（generate_granville_signalsで生成）
        keep_keys.add(prefix + "hold_stocks.parquet")  # MarketSpeed実保有ポジション（generate_stock_results_html.pyで管理）
        keep_keys.add(prefix + "orders.parquet")  # MarketSpeed注文履歴
        keep_keys.add(prefix + "credit_status.parquet")  # MarketSpeed資産状況
        keep_keys.add(prefix + "macro/estat_ci_index.parquet")  # CI先行指数
        keep_keys.add(prefix + "backtest/grok_analysis_merged.parquet")  # バックテスト統合データ（v2.0.3）も保持
        keep_keys.add(prefix + "backtest/grok_analysis_merged_v2_1.parquet")  # バックテスト統合データ（v2.1）も保持
        keep_keys.add(prefix + "backtest/trading_recommendation.json")  # 売買推奨データも保持
        keep_keys.add(prefix + "grok_day_trade_list.parquet")  # デイトレードリスト（手動管理）は絶対に削除しない
        keep_keys.add(prefix + "grok_prices_max_1d.parquet")  # ML学習・予測用の価格データ（ML Retrainingで使用）
        keep_keys.add(prefix + "earnings_disclosure.parquet")  # 決算開示データ（決算フィルタリング用）
        keep_keys.add(prefix + "fins_summary.parquet")  # J-Quants fins/summary 蓄積データ（決算ファクター分析用）
        keep_keys.add(prefix + "edinet_documents.parquet")  # EDINET書類一覧 蓄積データ（IR/適時開示フィルタ用）
        keep_keys.add(prefix + "ml/grok_lgbm_model.pkl")  # MLモデル（ML Retrainingで生成・使用）
        keep_keys.add(prefix + "ml/grok_lgbm_meta.json")  # MLメタ情報（ML Retrainingで生成・使用）
        keep_keys.add(prefix + "ml/archive_with_features.parquet")  # ML特徴量データ（ML Retrainingで生成）
        keep_keys.add(prefix + "ml/wfcv_predictions.parquet")  # WFCV予測（ML Retrainingで生成、analysis-mlで使用）
        keep_keys.add(prefix + "backtest/granville_ifd_archive.parquet")  # グランビルIFDアーカイブも保持
        keep_keys.add(prefix + "backtest/granville_ifd_comparison.parquet")  # グランビルIFD戦略比較も保持
        keep_keys.add(prefix + "granville_ifd_positions.parquet")  # グランビルIFDポジションも保持
        keep_keys.add(prefix + "breadth_daily.parquet")  # 騰落レシオ用日次データ（洗い替え蓄積）

        # backtest/grok_trending_YYYYMMDD.parquet ファイルも保護（7日分）
        # backtest/deep_analysis_YYYY-MM-DD.json ファイルも保護
        # backtest/analysis/deep_analysis_YYYY-MM-DD.json ファイルも保護
        # market_summary/ 配下のファイルも保護
        # これらは data-pipeline.yml の "Archive GROK trending for backtest" および "Generate market summary" ステップで管理される
        import re
        for obj in objects:
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
            # market_summary/structured/ 配下の全JSONを保持
            if re.match(rf"{prefix}market_summary/structured/.*\.json$", key):
                keep_keys.add(key)
            # granville/ 配下の全parquetを保持（signals, positions, recommendations, prices_topix）
            if key.startswith(f"{prefix}granville/"):
                keep_keys.add(key)
            # reversal/ 配下の全parquetを保持（bearish_signals, bearish_positions）
            if key.startswith(f"{prefix}reversal/"):
                keep_keys.add(key)
            # pairs/ 配下の全parquetを保持（pairs_signals）
            if key.startswith(f"{prefix}pairs/"):
                keep_keys.add(key)
            # 全市場tick/分足はappend-only正本として個別manifestで管理する。
            if key.startswith(f"{prefix}{MINUTE_S3_ROOT}/"):
                keep_keys.add(key)
            if key.startswith(f"{prefix}{TICK_S3_ROOT}/"):
                keep_keys.add(key)
            # 決算予定・実績は論理upsert、物理object不変で個別manifest管理する。
            if key.startswith(f"{prefix}{EARNINGS_DATE_S3_ROOT}/"):
                keep_keys.add(key)
            if key.startswith(f"{prefix}{FINS_SUMMARY_S3_ROOT}/"):
                keep_keys.add(key)

        # 削除対象のファイルを抽出
        delete_targets = [
            obj for obj in objects
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


def main(*, manifest_only: bool = False) -> int:
    app_env, use_s3 = resolve_storage_mode()
    print("=" * 60)
    print("Update Manifest and Upload to S3" if use_s3 else "Update Local Manifest")
    print("=" * 60)
    print(f"Environment: {app_env}")
    print(f"Storage    : {'s3' if use_s3 else 'local'}")

    # [STEP 1] manifest.json生成
    print("\n[STEP 1] Generating manifest.json...")
    try:
        existing_manifest = load_existing_manifest(use_s3=use_s3)
        manifest = generate_manifest(
            existing_manifest,
            preserve_missing=manifest_only,
            preserve_grok=not manifest_only,
        )
        save_manifest(manifest)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] S3アップロード
    print("\n[STEP 2] Uploading to S3...")
    try:
        if not use_s3:
            upload_success = True
            print("  [INFO] Development/local mode: S3 upload skipped")
        else:
            upload_success = (
                upload_manifest_only() if manifest_only else upload_files_to_s3()
            )
        if not upload_success:
            print("  ✗ S3 upload failed; manifest was not safely published")
            return 1
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 3] S3上の不要ファイルを削除（manifest.jsonに記載されたファイルのみ保持）
    if use_s3 and not manifest_only:
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
    s3_status = "✓ Success" if use_s3 and upload_success else "skipped (local mode)"
    print(f"S3 upload: {s3_status}")
    print("=" * 60)

    mode = "manifest-only" if manifest_only else "full"
    print(f"\n✅ Manifest update and S3 upload completed ({mode})!")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and publish manifest.json")
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Publish only manifest.json and preserve entries for missing local files",
    )
    args = parser.parse_args()
    raise SystemExit(main(manifest_only=args.manifest_only))
