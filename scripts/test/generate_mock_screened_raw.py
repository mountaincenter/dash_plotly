#!/usr/bin/env python3
"""
generate_mock_screened_raw.py
モックデータからテクニカル指標を計算（スコアリング前の生データ）

入力:
- data/parquet/test/mock_jquants_fetch_stock_prices_20251020.parquet

出力:
- data/parquet/test/mock_screened_raw_20251020.parquet

処理内容:
1. OHLCV データ読み込み
2. テクニカル指標計算 (calculate_technical_indicators)
3. テクニカル評価 (evaluate_technical_ratings)
4. 最新日データ抽出
5. スコアリング前の全銘柄を保存
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.screener import ScalpingScreener
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from common_cfg.paths import PARQUET_DIR

TEST_DIR = PARQUET_DIR / "test"
INPUT_PATH = TEST_DIR / "mock_jquants_500stocks_20251020.parquet"
OUTPUT_PATH = TEST_DIR / "mock_screened_500stocks_raw.parquet"
META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"


def main() -> int:
    """テクニカル指標計算（スコアリング前）"""
    print("=" * 60)
    print("Generate Mock Screened Raw Data")
    print("=" * 60)

    # [STEP 1] モックOHLCVデータ読み込み
    print("\n[STEP 1] Loading mock OHLCV data...")
    try:
        if not INPUT_PATH.exists():
            print(f"  ✗ File not found: {INPUT_PATH}")
            print(f"  → Please run generate_mock_fetch_stock_prices.py first")
            return 1

        df_prices = pd.read_parquet(INPUT_PATH)
        print(f"  ✓ Loaded {len(df_prices):,} rows, {df_prices['ticker'].nunique()} stocks")
        print(f"  ✓ Date range: {df_prices['date'].min()} to {df_prices['date'].max()}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 2] メタ情報読み込み
    print("\n[STEP 2] Loading meta_jquants.parquet...")
    try:
        if not META_JQUANTS_PATH.exists():
            print(f"  ✗ File not found: {META_JQUANTS_PATH}")
            return 1

        meta_df = pd.read_parquet(META_JQUANTS_PATH)
        print(f"  ✓ Loaded {len(meta_df)} stocks")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 3] テクニカル指標計算
    print("\n[STEP 3] Calculating technical indicators...")
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)
        screener = ScalpingScreener(fetcher)

        df_with_tech = screener.calculate_technical_indicators(df_prices)
        print(f"  ✓ Calculated indicators for {df_with_tech['ticker'].nunique()} stocks")
        print(f"  ✓ Columns added: {[c for c in df_with_tech.columns if c not in df_prices.columns]}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 4] テクニカル評価（overall_rating）
    print("\n[STEP 4] Evaluating technical ratings...")
    try:
        df_with_ratings = screener.evaluate_technical_ratings(df_with_tech)
        print(f"  ✓ Evaluated ratings")

        # Overall ratings分布を表示
        print("\n  --- Overall Ratings Distribution ---")
        rating_counts = df_with_ratings['overall_rating'].value_counts()
        for rating, count in rating_counts.items():
            print(f"    {rating}: {count} stocks ({count/len(df_with_ratings)*100:.1f}%)")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 5] 全日分データを保持（最新日のみ抽出しない）
    print("\n[STEP 5] Preparing all dates data...")
    try:
        df_all_dates = df_with_ratings.sort_values(['ticker', 'date']).copy()
        unique_dates = sorted(df_all_dates['date'].unique())
        print(f"  ✓ All dates: {len(df_all_dates)} rows")
        print(f"  ✓ Date range: {df_all_dates['date'].min()} to {df_all_dates['date'].max()}")
        print(f"  ✓ Number of dates: {len(unique_dates)}")
        print(f"  ✓ Stocks: {df_all_dates['ticker'].nunique()}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 6] メタ情報とマージ
    print("\n[STEP 6] Merging with meta information...")
    try:
        meta_cols = [c for c in ['ticker', 'stock_name', 'market', 'sectors', 'series', 'topixnewindexseries']
                     if c in meta_df.columns]

        df_merged = df_all_dates.merge(meta_df[meta_cols], on='ticker', how='left')
        print(f"  ✓ Merged with meta: {len(df_merged)} rows")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 7] 保存
    print("\n[STEP 7] Saving raw screened data...")
    try:
        TEST_DIR.mkdir(parents=True, exist_ok=True)

        df_merged.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {OUTPUT_PATH}")

        # データサマリー
        print("\n--- Data Summary ---")
        print(f"Total rows: {len(df_merged):,}")
        print(f"Unique stocks: {df_merged['ticker'].nunique():,}")
        print(f"Date range: {df_merged['date'].min()} to {df_merged['date'].max()}")
        print(f"Number of dates: {df_merged['date'].nunique()}")
        print(f"Columns: {len(df_merged.columns)}")
        print(f"Key columns: {list(df_merged.columns[:10])}...")
        print(f"File size: {OUTPUT_PATH.stat().st_size / 1024 / 1024:.2f} MB")

        # 基本統計
        print("\n--- Basic Statistics ---")
        print(f"Price range: ¥{df_merged['Close'].min():.0f} - ¥{df_merged['Close'].max():.0f}")
        print(f"Change%: {df_merged['change_pct'].min():.2f}% to {df_merged['change_pct'].max():.2f}%")
        print(f"ATR14%: {df_merged['atr14_pct'].min():.2f}% to {df_merged['atr14_pct'].max():.2f}%")
        print(f"RSI14: {df_merged['rsi14'].min():.1f} to {df_merged['rsi14'].max():.1f}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 60)
    print("✅ Raw screened data generated successfully!")
    print("=" * 60)
    print(f"\nOutput: {OUTPUT_PATH}")
    print("\nNext steps:")
    print("1. Run generate_mock_screened_scored.py to apply scoring")
    print("2. Analyze score distributions")
    print("3. Tune filter conditions and thresholds")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
