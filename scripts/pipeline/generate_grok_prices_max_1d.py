#!/usr/bin/env python3
"""
generate_grok_prices_max_1d.py
grok_trending_archive.parquetの銘柄に対してyfinanceで日足データを取得
grok_prices_max_1d.parquet を生成
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers
from common_cfg.paths import PARQUET_DIR

ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
OUTPUT_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"


def load_archive_tickers() -> list[str]:
    """grok_trending_archive.parquetからユニーク銘柄を取得"""
    if not ARCHIVE_PATH.exists():
        raise FileNotFoundError(
            f"grok_trending_archive.parquet not found: {ARCHIVE_PATH}"
        )

    print(f"[INFO] Loading archive: {ARCHIVE_PATH}")
    df = pd.read_parquet(ARCHIVE_PATH)
    tickers = df["ticker"].unique().tolist()
    print(f"  ✓ Found {len(tickers)} unique tickers")
    return tickers


def main():
    """メイン処理"""
    print("=" * 60)
    print("Generate grok_prices_max_1d.parquet")
    print("=" * 60)

    # 1. archive銘柄を取得
    tickers = load_archive_tickers()

    # 2. yfinanceで日足データを取得
    print(f"\n[INFO] Fetching daily prices for {len(tickers)} tickers...")
    try:
        df = fetch_prices_for_tickers(
            tickers=tickers,
            period="max",
            interval="1d",
            fallback_period=None
        )

        if df.empty:
            print("  ⚠ No data retrieved")
            return False

        # 3. 保存
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
        print(f"\n  ✓ Saved: {OUTPUT_PATH}")
        print(f"    - Rows: {len(df):,}")
        print(f"    - Tickers: {df['ticker'].nunique()}")
        if 'date' in df.columns:
            print(f"    - Date range: {df['date'].min()} ~ {df['date'].max()}")

        return True

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
