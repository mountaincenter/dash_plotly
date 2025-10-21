#!/usr/bin/env python3
"""
ランダム500銘柄、60日分のモックデータ生成
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import random

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from common_cfg.paths import PARQUET_DIR

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
TEST_DIR = PARQUET_DIR / "test"
OUTPUT_PATH = TEST_DIR / "mock_jquants_500stocks_20251020.parquet"


def main() -> int:
    print("=" * 60)
    print("Generate Mock Data: 500 Stocks, 60 Days")
    print("=" * 60)

    # Load meta
    print("\n[STEP 1] Loading meta_jquants.parquet...")
    meta_df = pd.read_parquet(META_JQUANTS_PATH)
    print(f"  ✓ Loaded {len(meta_df)} stocks")

    # Random 500 stocks
    print("\n[STEP 2] Selecting random 500 stocks...")
    random.seed(42)
    selected_tickers = random.sample(meta_df["ticker"].tolist(), 500)
    print(f"  ✓ Selected 500 stocks")
    print(f"  Sample: {selected_tickers[:5]}")

    # Fetch prices
    print("\n[STEP 3] Fetching stock prices (60 days)...")
    client = JQuantsClient()
    fetcher = JQuantsFetcher(client)

    # 2025-10-20 から60日分
    to_date = datetime(2025, 10, 20).date()
    from_date = to_date - timedelta(days=60)

    codes = [t.replace(".T", "") for t in selected_tickers]

    print(f"  Date range: {from_date} to {to_date}")
    print(f"  Fetching {len(codes)} stocks...")

    df_prices = fetcher.get_prices_daily_batch(codes, from_date=from_date, to_date=to_date, batch_delay=0.2)

    if df_prices.empty:
        print("  ✗ Failed to fetch data")
        return 1

    df_prices = fetcher.convert_to_yfinance_format(df_prices)
    print(f"  ✓ Retrieved {len(df_prices)} rows")

    # Save
    print("\n[STEP 4] Saving...")
    TEST_DIR.mkdir(parents=True, exist_ok=True)
    df_prices.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {OUTPUT_PATH}")
    print(f"  ✓ Size: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")

    print("\n" + "=" * 60)
    print("✅ Complete")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
