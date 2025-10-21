#!/usr/bin/env python3
"""
generate_mock_fetch_stock_prices.py
モックデータ生成: J-Quants APIから全銘柄の株価データを取得して保存

目的:
- スクリーニング条件のチューニングのため、再現可能なテストデータを作成
- API呼び出しを1回だけ行い、以降は高速にテスト可能にする

出力:
- data/parquet/test/mock_jquants_fetch_stock_prices_20251020.parquet
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
from common_cfg.paths import PARQUET_DIR

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
TEST_DIR = PARQUET_DIR / "test"
OUTPUT_PATH = TEST_DIR / "mock_jquants_fetch_stock_prices_20251020.parquet"


def fetch_stock_prices_mock(tickers: list[str], fetcher: JQuantsFetcher, lookback_days: int = 60) -> pd.DataFrame:
    """
    指定銘柄の株価データを取得（J-Quants API + 取引カレンダー）

    generate_scalping.py の fetch_stock_prices() と同じロジック

    Args:
        tickers: ティッカーリスト（例: ["7203.T", "6758.T"]）
        fetcher: JQuantsFetcher インスタンス
        lookback_days: 何日分のデータを取得するか

    Returns:
        株価データのDataFrame
    """
    print(f"[INFO] Fetching stock prices for {len(tickers)} stocks (last {lookback_days} days)...")

    # ティッカーを4桁コードに変換（例: "7203.T" -> "7203"）
    codes = [ticker.replace(".T", "") for ticker in tickers]

    # 取引カレンダーAPIから直近営業日を取得
    print("[INFO] Fetching latest trading day from J-Quants API...")
    latest_trading_day = fetcher.get_latest_trading_day()
    print(f"[OK] Latest trading day: {latest_trading_day}")

    to_date_obj = datetime.strptime(latest_trading_day, "%Y-%m-%d").date()
    from_date = to_date_obj - timedelta(days=lookback_days)

    print(f"[INFO] Date range: {from_date} to {to_date_obj}")

    # 株価データ取得（バッチ処理）
    df = fetcher.get_prices_daily_batch(codes, from_date=from_date, to_date=to_date_obj, batch_delay=0.2)

    if df.empty:
        print("[ERROR] No price data retrieved")
        return pd.DataFrame()

    # yfinance互換形式に変換
    df = fetcher.convert_to_yfinance_format(df)

    print(f"[OK] Retrieved price data: {len(df)} rows, {df['ticker'].nunique()} stocks")
    return df


def main() -> int:
    """モックデータ生成"""
    print("=" * 60)
    print("Generate Mock Stock Prices Data")
    print("=" * 60)

    # [STEP 1] meta_jquants.parquet読み込み
    print("\n[STEP 1] Loading meta_jquants.parquet...")
    try:
        if not META_JQUANTS_PATH.exists():
            print(f"  ✗ File not found: {META_JQUANTS_PATH}")
            return 1

        meta_df = pd.read_parquet(META_JQUANTS_PATH)
        print(f"  ✓ Loaded {len(meta_df)} stocks")

        if meta_df.empty:
            print("  ✗ meta_jquants.parquet is empty")
            return 1

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] J-Quants APIで株価データ取得
    print("\n[STEP 2] Fetching stock prices from J-Quants API...")
    print("[INFO] This will take approximately 45 minutes for 3788 stocks...")
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)

        # 全銘柄の株価を取得（60日分）
        tickers = meta_df["ticker"].tolist()
        df_prices = fetch_stock_prices_mock(tickers, fetcher, lookback_days=60)

        if df_prices.empty:
            print("  ✗ No price data retrieved")
            return 1

        print(f"  ✓ Retrieved {len(df_prices)} rows for {df_prices['ticker'].nunique()} stocks")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 3] データ保存
    print("\n[STEP 3] Saving mock data...")
    try:
        TEST_DIR.mkdir(parents=True, exist_ok=True)

        df_prices.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {OUTPUT_PATH}")

        # データサマリー
        print("\n--- Data Summary ---")
        print(f"Total rows: {len(df_prices):,}")
        print(f"Total stocks: {df_prices['ticker'].nunique():,}")
        print(f"Date range: {df_prices['date'].min()} to {df_prices['date'].max()}")
        print(f"Columns: {list(df_prices.columns)}")
        print(f"File size: {OUTPUT_PATH.stat().st_size / 1024 / 1024:.2f} MB")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 60)
    print("✅ Mock data generated successfully!")
    print("=" * 60)
    print(f"\nOutput: {OUTPUT_PATH}")
    print("\nNext steps:")
    print("1. Use this mock data for condition tuning")
    print("2. Adjust scoring thresholds and filters")
    print("3. Test different screening criteria")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
