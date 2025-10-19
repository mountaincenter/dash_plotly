#!/usr/bin/env python3
"""
Scalping Stock Selection (All Stocks)
全銘柄を対象にスキャルピング銘柄を選定し、S3にアップロード
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import os

ROOT = Path(__file__).resolve().parents[1]  # smoke_test/ から1階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_fetcher import JQuantsFetcher
from scripts.lib.screener import ScalpingScreener
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file
from common_cfg.s3cfg import load_s3_config

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
SCALPING_ENTRY_PATH = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE_PATH = PARQUET_DIR / "scalping_active.parquet"


def load_all_stocks() -> pd.DataFrame:
    """
    meta_jquants.parquetから全銘柄を読み込み

    Returns:
        全銘柄のDataFrame
    """
    print("[INFO] Loading all stocks from meta_jquants.parquet...")

    if not META_JQUANTS_PATH.exists():
        raise FileNotFoundError(f"meta_jquants.parquet not found: {META_JQUANTS_PATH}")

    meta = pd.read_parquet(META_JQUANTS_PATH)
    print(f"[OK] Loaded {len(meta)} stocks")

    return meta


def fetch_stock_prices(tickers: list[str], fetcher: JQuantsFetcher, lookback_days: int = 60) -> pd.DataFrame:
    """
    指定銘柄の株価データを取得（J-Quants API + 取引カレンダー）

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


def create_empty_scalping_files():
    """空のscalping_*.parquetファイルを作成"""
    empty_df = pd.DataFrame(columns=[
        'ticker', 'stock_name', 'market', 'sectors', 'date',
        'Close', 'change_pct', 'Volume', 'vol_ratio',
        'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
    ])

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    empty_df.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
    empty_df.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)

    print(f"  ✓ Saved empty: {SCALPING_ENTRY_PATH}")
    print(f"  ✓ Saved empty: {SCALPING_ACTIVE_PATH}")


def upload_to_s3():
    """scalping_*.parquetをS3にアップロード"""
    try:
        cfg = load_s3_config()

        # Entry list upload
        print(f"[INFO] Uploading {SCALPING_ENTRY_PATH.name} to S3...")
        success_entry = upload_file(cfg, SCALPING_ENTRY_PATH, "scalping_entry.parquet")
        if success_entry:
            print(f"  ✓ Uploaded: scalping_entry.parquet")
        else:
            print(f"  ✗ Failed to upload: scalping_entry.parquet")
            return False

        # Active list upload
        print(f"[INFO] Uploading {SCALPING_ACTIVE_PATH.name} to S3...")
        success_active = upload_file(cfg, SCALPING_ACTIVE_PATH, "scalping_active.parquet")
        if success_active:
            print(f"  ✓ Uploaded: scalping_active.parquet")
        else:
            print(f"  ✗ Failed to upload: scalping_active.parquet")
            return False

        return True

    except Exception as e:
        print(f"  ✗ S3 upload failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main() -> int:
    """スキャルピング銘柄リスト生成（全銘柄対象）"""
    # 環境変数 NUM_STOCKS で処理する銘柄数を指定（未指定=全件）
    num_stocks_env = os.getenv("NUM_STOCKS", "")
    num_stocks = int(num_stocks_env) if num_stocks_env else None

    print("=" * 60)
    if num_stocks:
        print(f"Scalping Stock Selection ({num_stocks} Random Stocks)")
    else:
        print("Scalping Stock Selection (All Stocks)")
    print("=" * 60)

    # [STEP 1] 全銘柄読み込み
    print("\n[STEP 1] Loading all stocks...")
    try:
        meta_df = load_all_stocks()

        # NUM_STOCKS が指定されていればランダムサンプリング
        if num_stocks and len(meta_df) > num_stocks:
            print(f"[INFO] Sampling {num_stocks} random stocks...")
            meta_df = meta_df.sample(n=num_stocks, random_state=42)
            print(f"  ✓ Selected {len(meta_df)} random stocks")
        else:
            print(f"  ✓ Loaded {len(meta_df)} stocks")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] J-Quants APIで株価データ取得（取引カレンダー使用）
    print("\n[STEP 2] Fetching stock prices from J-Quants API...")
    try:
        fetcher = JQuantsFetcher()

        # 全銘柄の株価を取得（60日分）
        tickers = meta_df["ticker"].tolist()
        df_prices = fetch_stock_prices(tickers, fetcher, lookback_days=60)

        if df_prices.empty:
            print("  ✗ No price data retrieved")
            print("  ⚠ Creating empty scalping files...")
            create_empty_scalping_files()

            print("\n[STEP S3] Uploading to S3...")
            if upload_to_s3():
                print("  ✓ Empty files uploaded to S3")
            else:
                print("  ✗ S3 upload failed")
                return 1

            print("\n✅ Empty scalping lists created and uploaded")
            return 0

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()

        print("  ⚠ Creating empty scalping files...")
        create_empty_scalping_files()

        print("\n[STEP S3] Uploading to S3...")
        if upload_to_s3():
            print("  ✓ Empty files uploaded to S3")
        else:
            print("  ✗ S3 upload failed")
            return 1

        print("\n✅ Empty scalping lists created and uploaded")
        return 0

    # [STEP 3] テクニカル指標計算
    print("\n[STEP 3] Calculating technical indicators...")
    try:
        screener = ScalpingScreener(fetcher)
        df_with_tech = screener.calculate_technical_indicators(df_prices)
        print(f"  ✓ Calculated indicators for {df_with_tech['ticker'].nunique()} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 4] テクニカル評価（overall_rating）
    print("\n[STEP 4] Evaluating technical ratings...")
    try:
        df_with_ratings = screener.evaluate_technical_ratings(df_with_tech)
        print(f"  ✓ Evaluated ratings")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 5] 最新日のデータのみ抽出
    print("\n[STEP 5] Extracting latest data...")
    try:
        df_latest = df_with_ratings.sort_values(['ticker', 'date']).groupby('ticker', as_index=False).last()
        print(f"  ✓ Latest data: {len(df_latest)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 6] エントリー向け銘柄リスト生成
    print("\n[STEP 6] Generating entry list...")
    try:
        df_entry = screener.generate_entry_list(df_latest, meta_df, top_n=20)

        if df_entry.empty:
            print("  ⚠ No entry stocks found (creating empty file)")
            df_entry = pd.DataFrame(columns=[
                'ticker', 'stock_name', 'market', 'sectors', 'date',
                'Close', 'change_pct', 'Volume', 'vol_ratio',
                'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
            ])

        print(f"  ✓ Entry list: {len(df_entry)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 7] アクティブ向け銘柄リスト生成
    print("\n[STEP 7] Generating active list...")
    try:
        entry_tickers = set(df_entry['ticker'].tolist()) if not df_entry.empty else set()
        df_active = screener.generate_active_list(df_latest, meta_df, entry_tickers, top_n=20)

        if df_active.empty:
            print("  ⚠ No active stocks found (creating empty file)")
            df_active = pd.DataFrame(columns=[
                'ticker', 'stock_name', 'market', 'sectors', 'date',
                'Close', 'change_pct', 'Volume', 'vol_ratio',
                'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
            ])

        print(f"  ✓ Active list: {len(df_active)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 8] ファイル保存
    print("\n[STEP 8] Saving scalping lists...")
    try:
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        df_entry.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
        df_active.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)

        print(f"  ✓ Saved: {SCALPING_ENTRY_PATH}")
        print(f"  ✓ Saved: {SCALPING_ACTIVE_PATH}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 9] S3アップロード
    print("\n[STEP 9] Uploading to S3...")
    if upload_to_s3():
        print("  ✓ Files uploaded to S3")
    else:
        print("  ✗ S3 upload failed")
        return 1

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Processed stocks: {len(meta_df)}")
    print(f"Entry candidates: {len(df_entry)}")
    print(f"Active candidates: {len(df_active)}")
    print("=" * 60)

    print("\n✅ Scalping lists generated and uploaded successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
