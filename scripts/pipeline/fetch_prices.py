#!/usr/bin/env python3
"""
fetch_prices.py
all_stocks.parquetの銘柄に対してyfinanceで価格データを取得
prices_{period}_{interval}.parquet, tech_snapshot_1d.parquet を生成
GitHub Actions対応: all_stocks.parquetをローカルから読み込み
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]  # scripts/pipeline/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers
from common_cfg.paths import PARQUET_DIR
from server.services.tech_utils_v2 import evaluate_latest_snapshot

ALL_STOCKS_PATH = PARQUET_DIR / "all_stocks.parquet"

# 取得する価格データの設定
PRICE_CONFIGS = [
    {"period": "60d", "interval": "15m", "filename": "prices_60d_15m.parquet"},
    {"period": "60d", "interval": "5m", "filename": "prices_60d_5m.parquet"},
    {"period": "730d", "interval": "1h", "filename": "prices_730d_1h.parquet", "fallback_period": "max"},
    {"period": "max", "interval": "1d", "filename": "prices_max_1d.parquet"},
    {"period": "max", "interval": "1mo", "filename": "prices_max_1mo.parquet"},
]


def load_all_stocks() -> pd.DataFrame:
    """all_stocks.parquetを読み込み"""
    if not ALL_STOCKS_PATH.exists():
        raise FileNotFoundError(
            f"all_stocks.parquet not found: {ALL_STOCKS_PATH}\n"
            "Please run create_all_stocks.py first."
        )

    print(f"[INFO] Loading all_stocks.parquet: {ALL_STOCKS_PATH}")
    df = pd.read_parquet(ALL_STOCKS_PATH)
    print(f"  ✓ Loaded {len(df)} stocks")
    return df


def fetch_and_save_prices(tickers: List[str], period: str, interval: str, output_path: Path, fallback_period: str = None) -> bool:
    """
    指定銘柄の価格データを取得して保存

    Args:
        tickers: ティッカーリスト
        period: データ期間
        interval: データ間隔
        output_path: 保存先パス
        fallback_period: エラー時のフォールバック期間（730d→maxなど）

    Returns:
        成功時True、失敗時False
    """
    print(f"[INFO] Fetching prices: period={period}, interval={interval}")

    try:
        df = fetch_prices_for_tickers(tickers, period, interval)

        if df.empty:
            print(f"  ⚠ No data retrieved, creating empty file")
            df = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

        # 保存
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {output_path} ({len(df)} rows)")
        return True

    except Exception as e:
        print(f"  ✗ Failed: {e}")

        # fallback_periodがあればリトライ
        if fallback_period:
            print(f"  [INFO] Retrying with fallback period={fallback_period}")
            try:
                df = fetch_prices_for_tickers(tickers, fallback_period, interval)
                PARQUET_DIR.mkdir(parents=True, exist_ok=True)
                df.to_parquet(output_path, engine="pyarrow", index=False)
                print(f"  ✓ Saved with fallback: {output_path} ({len(df)} rows)")
                return True
            except Exception as e2:
                print(f"  ✗ Fallback also failed: {e2}")

        # エラー時は空ファイルを作成（パイプライン継続のため）
        empty_df = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
        empty_df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"  ⚠ Created empty file due to error")
        return False


def generate_tech_snapshot(prices_1d_path: Path, output_path: Path) -> bool:
    """
    1日足データからテクニカルスナップショットを生成

    Args:
        prices_1d_path: 1日足データのパス
        output_path: 出力先パス

    Returns:
        成功時True、失敗時False
    """
    print("[INFO] Generating technical snapshot from 1d data...")

    try:
        if not prices_1d_path.exists():
            print(f"  ✗ Input file not found: {prices_1d_path}")
            return False

        df = pd.read_parquet(prices_1d_path)

        if df.empty:
            print("  ⚠ Input data is empty, creating empty snapshot")
            empty_df = pd.DataFrame(columns=["ticker", "overall_rating", "overall_score"])
            empty_df.to_parquet(output_path, engine="pyarrow", index=False)
            return True

        # 各銘柄の最新スナップショットを評価
        snapshots = []
        for ticker, grp in df.groupby("ticker"):
            grp = grp.dropna(subset=["Close"]).copy()
            if grp.empty or len(grp) < 20:  # 最低20営業日分のデータが必要
                continue

            # dateをindexに設定
            grp_indexed = grp.set_index("date")

            try:
                snapshot = evaluate_latest_snapshot(grp_indexed)
                snapshots.append({
                    "ticker": ticker,
                    "overall_rating": snapshot["overall"]["label"],
                    "overall_score": snapshot["overall"]["score"],
                })
            except Exception as e:
                print(f"  [WARN] Failed to evaluate {ticker}: {e}")
                snapshots.append({
                    "ticker": ticker,
                    "overall_rating": "中立",
                    "overall_score": 0,
                })

        if not snapshots:
            print("  ⚠ No snapshots generated, creating empty file")
            snapshot_df = pd.DataFrame(columns=["ticker", "overall_rating", "overall_score"])
        else:
            snapshot_df = pd.DataFrame(snapshots)

        # 保存
        snapshot_df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {output_path} ({len(snapshot_df)} stocks)")
        return True

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        # エラー時も空ファイルを作成
        empty_df = pd.DataFrame(columns=["ticker", "overall_rating", "overall_score"])
        empty_df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"  ⚠ Created empty file due to error")
        return False


def main() -> int:
    print("=" * 60)
    print("Fetch Prices and Generate Tech Snapshot (yfinance)")
    print("=" * 60)

    # [STEP 1] all_stocks.parquet読み込み
    print("\n[STEP 1] Loading all_stocks.parquet...")
    try:
        all_stocks = load_all_stocks()
        tickers = all_stocks["ticker"].unique().tolist()
        print(f"  ✓ {len(tickers)} unique tickers")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] 価格データ取得（複数パターン）
    print("\n[STEP 2] Fetching prices from yfinance...")
    success_count = 0

    for i, config in enumerate(PRICE_CONFIGS, 1):
        print(f"\n  [{i}/{len(PRICE_CONFIGS)}] {config['filename']}")
        output_path = PARQUET_DIR / config["filename"]
        fallback = config.get("fallback_period")

        if fetch_and_save_prices(tickers, config["period"], config["interval"], output_path, fallback):
            success_count += 1

    print(f"\n  ✓ Successfully fetched {success_count}/{len(PRICE_CONFIGS)} price datasets")

    # [STEP 3] テクニカルスナップショット生成（1日足データから）
    print("\n[STEP 3] Generating technical snapshot...")
    prices_1d_path = PARQUET_DIR / "prices_max_1d.parquet"
    tech_snapshot_path = PARQUET_DIR / "tech_snapshot_1d.parquet"

    if generate_tech_snapshot(prices_1d_path, tech_snapshot_path):
        print("  ✓ Technical snapshot generated")
    else:
        print("  ⚠ Technical snapshot generation had issues")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total tickers: {len(tickers)}")
    print(f"Price datasets: {success_count}/{len(PRICE_CONFIGS)} successful")
    print(f"Tech snapshot: {'✓' if tech_snapshot_path.exists() else '✗'}")
    print("=" * 60)
    print("  ℹ S3アップロードは update_manifest.py で一括実行されます")

    print("\n✅ Price fetch and tech snapshot generation completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
