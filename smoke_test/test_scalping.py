#!/usr/bin/env python3
"""
Smoke Test for Scalping Stock Selection
取引カレンダーAPIを使用して直近営業日を取得し、ランダム5銘柄でスキャルピングロジックをテスト
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]  # smoke_test/ から1階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import random
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from scripts.lib.screener import ScalpingScreener
from common_cfg.paths import PARQUET_DIR

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"


def load_random_stocks(n: int = 5) -> pd.DataFrame:
    """
    meta_jquants.parquetからランダムにN銘柄を選定

    Args:
        n: 選定する銘柄数

    Returns:
        選定された銘柄のDataFrame
    """
    print(f"[INFO] Loading {n} random stocks from meta_jquants.parquet...")

    if not META_JQUANTS_PATH.exists():
        raise FileNotFoundError(f"meta_jquants.parquet not found: {META_JQUANTS_PATH}")

    meta = pd.read_parquet(META_JQUANTS_PATH)
    print(f"[OK] Loaded {len(meta)} stocks")

    # ランダムにN銘柄を選定
    if len(meta) < n:
        print(f"[WARN] Only {len(meta)} stocks available, using all")
        selected = meta
    else:
        selected = meta.sample(n=n, random_state=42)

    print(f"[OK] Selected {len(selected)} random stocks:")
    for ticker in selected["ticker"].values:
        print(f"  - {ticker}")

    return selected


def get_latest_trading_day(client: JQuantsClient) -> str:
    """
    取引カレンダーAPIから直近の営業日を取得

    Returns:
        YYYY-MM-DD形式の日付文字列
    """
    print("[INFO] Fetching latest trading day from J-Quants API...")

    # 過去30日分の取引カレンダーを取得
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=30)

    params = {
        "from": str(from_date),
        "to": str(to_date)
    }

    response = client.request("/markets/trading_calendar", params=params)

    if not response or "trading_calendar" not in response:
        raise RuntimeError("Failed to fetch trading calendar from J-Quants")

    calendar = pd.DataFrame(response["trading_calendar"])

    # holiday_division が "0" (営業日) のレコードのみ
    trading_days = calendar[calendar["HolidayDivision"] == "0"].copy()

    if trading_days.empty:
        raise RuntimeError("No trading days found in the calendar")

    # Date列をdatetimeに変換してソート
    trading_days["Date"] = pd.to_datetime(trading_days["Date"])
    trading_days = trading_days.sort_values("Date", ascending=False)

    # 最新の営業日を取得
    latest_trading_day = trading_days.iloc[0]["Date"].strftime("%Y-%m-%d")

    print(f"[OK] Latest trading day: {latest_trading_day}")
    return latest_trading_day


def fetch_stock_prices(tickers: list[str], fetcher: JQuantsFetcher, client: JQuantsClient, lookback_days: int = 60) -> pd.DataFrame:
    """
    指定銘柄の株価データを取得（J-Quants API + 取引カレンダー）

    Args:
        tickers: ティッカーリスト（例: ["7203.T", "6758.T"]）
        fetcher: JQuantsFetcher インスタンス
        client: JQuantsClient インスタンス（取引カレンダー取得用）
        lookback_days: 何日分のデータを取得するか

    Returns:
        株価データのDataFrame
    """
    print(f"[INFO] Fetching stock prices for {len(tickers)} stocks (last {lookback_days} days)...")

    # ティッカーを4桁コードに変換（例: "7203.T" -> "7203"）
    codes = [ticker.replace(".T", "") for ticker in tickers]

    # 取引カレンダーAPIから直近営業日を取得
    latest_trading_day = get_latest_trading_day(client)
    to_date_obj = datetime.strptime(latest_trading_day, "%Y-%m-%d").date()
    from_date = to_date_obj - timedelta(days=lookback_days)

    print(f"[INFO] Date range: {from_date} to {to_date_obj}")

    # 株価データ取得（バッチ処理）
    df = fetcher.get_prices_daily_batch(codes, from_date=from_date, to_date=to_date_obj, batch_delay=0.5)

    if df.empty:
        print("[ERROR] No price data retrieved")
        return pd.DataFrame()

    # yfinance互換形式に変換
    df = fetcher.convert_to_yfinance_format(df)

    print(f"[OK] Retrieved price data: {len(df)} rows, {df['ticker'].nunique()} stocks")
    return df


def main() -> int:
    """スキャルピング銘柄リストのスモークテスト（ランダムN銘柄）"""
    import os

    # 環境変数から銘柄数を取得（デフォルト5）
    num_stocks = int(os.getenv("NUM_STOCKS", "5"))

    print("=" * 60)
    print(f"Smoke Test: Scalping Stock Selection ({num_stocks} Random Stocks)")
    print("=" * 60)

    # [STEP 1] ランダムN銘柄を選定
    print(f"\n[STEP 1] Selecting {num_stocks} random stocks...")
    try:
        meta_df = load_random_stocks(n=num_stocks)
        print(f"  ✓ Selected {len(meta_df)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] J-Quants APIで株価データ取得（取引カレンダー使用）
    print("\n[STEP 2] Fetching stock prices from J-Quants API...")
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)

        # ランダム5銘柄の株価を取得（60日分）
        tickers = meta_df["ticker"].tolist()
        df_prices = fetch_stock_prices(tickers, fetcher, client, lookback_days=60)

        if df_prices.empty:
            print("  ✗ No price data retrieved")
            return 1

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

    # [STEP 5.5] スキャルピング判定根拠を出力
    print("\n[STEP 5.5] Analyzing screening criteria...")
    try:
        print("\n--- Entry Criteria ---")
        print("Price: 100-1500円")
        print("Liquidity: Volume × Close >= 100,000,000円")
        print("Volatility (ATR14%): 1.0-3.5%")
        print("Change%: -3.0% to +3.0%")
        print("Overall Rating: 買い または 強い買い")
        print()

        # 各条件での合格銘柄数を表示
        entry_criteria = {
            "Price (100-1500)": (df_latest['Close'] >= 100) & (df_latest['Close'] <= 1500),
            "Liquidity >= 100M": (df_latest['Volume'] * df_latest['Close'] >= 100_000_000),
            "ATR14% (1.0-3.5)": (df_latest['atr14_pct'] >= 1.0) & (df_latest['atr14_pct'] <= 3.5),
            "Change% (-3 to +3)": (df_latest['change_pct'] >= -3.0) & (df_latest['change_pct'] <= 3.0),
            "Rating (買い系)": df_latest['overall_rating'].isin(['買い', '強い買い'])
        }

        for criteria_name, condition in entry_criteria.items():
            passed_count = condition.sum()
            print(f"  {criteria_name}: {passed_count}/{len(df_latest)} stocks passed")

        # 全条件合格
        all_conditions = pd.Series([True] * len(df_latest), index=df_latest.index)
        for condition in entry_criteria.values():
            all_conditions &= condition
        print(f"\n  All criteria met: {all_conditions.sum()}/{len(df_latest)} stocks")
        print()

        print("--- Active Criteria ---")
        print("Price: 200-2000円")
        print("Liquidity: Volume × Close >= 200,000,000円")
        print("Volatility (ATR14%): 1.5-5.0%")
        print("Change%: -4.0% to +4.0%")
        print("Overall Rating: 買い または 強い買い")
        print("Exclude: Entry stocks")
        print()

    except Exception as e:
        print(f"  ✗ Failed to analyze criteria: {e}")
        import traceback
        traceback.print_exc()

    # [STEP 6] エントリー向け銘柄リスト生成
    print("\n[STEP 6] Generating entry list...")
    try:
        df_entry = screener.generate_entry_list(df_latest, meta_df, top_n=num_stocks)

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
        df_active = screener.generate_active_list(df_latest, meta_df, entry_tickers, top_n=num_stocks)

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

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Tested stocks: {len(meta_df)}")
    print(f"Entry candidates: {len(df_entry)}")
    print(f"Active candidates: {len(df_active)}")

    if len(df_entry) > 0:
        print("\nEntry stocks:")
        for ticker in df_entry["ticker"].values:
            print(f"  - {ticker}")

    if len(df_active) > 0:
        print("\nActive stocks:")
        for ticker in df_active["ticker"].values:
            print(f"  - {ticker}")

    print("=" * 60)

    print("\n✅ Smoke test completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
