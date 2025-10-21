#!/usr/bin/env python3
"""
generate_mock_30stocks.py
30銘柄のモックデータを生成（Phase 1テスト用）

処理:
1. meta_jquants.parquetから30銘柄をランダム選定
2. J-Quants APIで5年分の株価取得
3. テクニカル指標計算
4. parquet保存
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from scripts.lib.screener import ScalpingScreener
from common_cfg.paths import PARQUET_DIR

TEST_DIR = PARQUET_DIR / "test"
META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
OUTPUT_PATH = TEST_DIR / "mock_30stocks_scored.parquet"


def main() -> int:
    """30銘柄のモックデータ生成"""
    print("=" * 60)
    print("Generate Mock 30 Stocks Data")
    print("=" * 60)

    # [STEP 1] meta_jquants.parquetから30銘柄選定
    print("\n[STEP 1] Loading meta_jquants.parquet...")
    try:
        if not META_JQUANTS_PATH.exists():
            print(f"  ✗ File not found: {META_JQUANTS_PATH}")
            print("  → Please run create_meta_jquants.py first")
            return 1

        meta_df = pd.read_parquet(META_JQUANTS_PATH)
        print(f"  ✓ Loaded {len(meta_df)} stocks")

        # ランダムに30銘柄選定
        meta_30 = meta_df.sample(n=30, random_state=42).reset_index(drop=True)
        print(f"  ✓ Selected 30 stocks:")
        print(meta_30[['ticker', 'stock_name']].to_string(index=False))

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] J-Quants APIで株価データ取得
    print("\n[STEP 2] Fetching stock prices from J-Quants API...")
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)

        # 最新取引日取得
        latest_trading_day = fetcher.get_latest_trading_day()
        print(f"  ✓ Latest trading day: {latest_trading_day}")

        to_date_obj = datetime.strptime(latest_trading_day, "%Y-%m-%d").date()
        from_date = to_date_obj - timedelta(days=365 * 5)  # 5年分

        print(f"  ✓ Date range: {from_date} to {to_date_obj}")

        # 30銘柄の株価取得
        tickers = meta_30["ticker"].tolist()
        codes = [ticker.replace(".T", "") for ticker in tickers]

        df_prices = fetcher.get_prices_daily_batch(
            codes,
            from_date=from_date,
            to_date=to_date_obj,
            batch_delay=0.2
        )

        if df_prices.empty:
            print("  ✗ No price data retrieved")
            return 1

        # yfinance互換形式に変換
        df_prices = fetcher.convert_to_yfinance_format(df_prices)
        print(f"  ✓ Retrieved {len(df_prices)} rows, {df_prices['ticker'].nunique()} stocks")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 3] テクニカル指標計算
    print("\n[STEP 3] Calculating technical indicators...")
    try:
        screener = ScalpingScreener(fetcher)
        df_with_tech = screener.calculate_technical_indicators(df_prices)
        print(f"  ✓ Calculated indicators for {df_with_tech['ticker'].nunique()} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 4] テクニカル評価
    print("\n[STEP 4] Evaluating technical ratings...")
    try:
        df_with_ratings = screener.evaluate_technical_ratings(df_with_tech)
        print(f"  ✓ Evaluated ratings")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 5] メタ情報マージ
    print("\n[STEP 5] Merging meta information...")
    try:
        df_final = df_with_ratings.merge(
            meta_30[['ticker', 'stock_name', 'market', 'sectors', 'series', 'topixnewindexseries']],
            on='ticker',
            how='left'
        )
        print(f"  ✓ Merged meta data")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 6] 保存
    print("\n[STEP 6] Saving mock data...")
    try:
        TEST_DIR.mkdir(parents=True, exist_ok=True)
        df_final.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {OUTPUT_PATH}")

        # サマリー
        print("\n--- Data Summary ---")
        print(f"Total rows: {len(df_final):,}")
        print(f"Unique stocks: {df_final['ticker'].nunique()}")
        print(f"Date range: {df_final['date'].min()} to {df_final['date'].max()}")
        print(f"Number of dates: {df_final['date'].nunique()}")
        print(f"File size: {OUTPUT_PATH.stat().st_size / 1024 / 1024:.2f} MB")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 60)
    print("✅ Mock 30 stocks data generated successfully!")
    print("=" * 60)
    print(f"\nOutput: {OUTPUT_PATH}")
    print("\nNext steps:")
    print("1. Use this data for Phase 1 development")
    print("2. Test Entry/Active selection logic")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
