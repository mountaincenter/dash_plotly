#!/usr/bin/env python3
"""
Smoke Test for Scalping Stock Selection
取引カレンダーAPIを使用して直近営業日を取得し、ランダム5銘柄でスキャルピングロジックをテスト
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import random

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from scripts.lib.screener import ScalpingScreener
from common_cfg.paths import PARQUET_DIR


def get_latest_trading_day(client: JQuantsClient) -> str:
    """
    取引カレンダーAPIから直近の営業日を取得

    Returns:
        YYYY-MM-DD形式の日付文字列
    """
    print("[INFO] Fetching trading calendar from J-Quants API...")

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


def load_random_stocks(meta_path: Path, n: int = 5) -> pd.DataFrame:
    """
    meta_jquants.parquetからランダムにN銘柄を選定

    Args:
        meta_path: meta_jquants.parquetのパス
        n: 選定する銘柄数

    Returns:
        選定された銘柄のDataFrame
    """
    print(f"[INFO] Loading meta_jquants.parquet from {meta_path}...")

    if not meta_path.exists():
        raise FileNotFoundError(f"meta_jquants.parquet not found: {meta_path}")

    meta = pd.read_parquet(meta_path)
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


def fetch_stock_prices_with_trading_day(
    tickers: list[str],
    fetcher: JQuantsFetcher,
    to_date: str,
    lookback_days: int = 60
) -> pd.DataFrame:
    """
    指定銘柄の株価データを取得（取引カレンダーAPIの直近営業日を使用）

    Args:
        tickers: ティッカーリスト
        fetcher: JQuantsFetcher インスタンス
        to_date: 取得終了日（直近営業日）
        lookback_days: 何日分のデータを取得するか

    Returns:
        株価データのDataFrame
    """
    print(f"[INFO] Fetching stock prices for {len(tickers)} stocks...")
    print(f"[INFO] Date range: last {lookback_days} days until {to_date}")

    # ティッカーを4桁コードに変換
    codes = [ticker.replace(".T", "") for ticker in tickers]

    # 期間設定
    to_date_obj = datetime.strptime(to_date, "%Y-%m-%d").date()
    from_date = to_date_obj - timedelta(days=lookback_days)

    # 株価データ取得
    df = fetcher.get_prices_daily_batch(codes, from_date=from_date, to_date=to_date, batch_delay=0.5)

    if df.empty:
        print("[ERROR] No price data retrieved")
        return pd.DataFrame()

    # yfinance互換形式に変換
    df = fetcher.convert_to_yfinance_format(df)

    print(f"[OK] Retrieved price data: {len(df)} rows, {df['ticker'].nunique()} stocks")
    return df


def main() -> int:
    """スモークテスト実行"""
    print("=" * 60)
    print("Smoke Test: Scalping Stock Selection (5 Random Stocks)")
    print("=" * 60)

    # [STEP 1] J-Quantsクライアント初期化
    print("\n[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] 直近営業日を取得
    print("\n[STEP 2] Fetching latest trading day...")
    try:
        latest_trading_day = get_latest_trading_day(client)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 3] ランダム5銘柄を選定
    print("\n[STEP 3] Selecting 5 random stocks...")
    try:
        meta_path = PARQUET_DIR / "meta_jquants.parquet"
        selected_stocks = load_random_stocks(meta_path, n=5)
        tickers = selected_stocks["ticker"].tolist()
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 4] 株価データを取得
    print("\n[STEP 4] Fetching stock prices...")
    try:
        fetcher = JQuantsFetcher(client)
        prices = fetch_stock_prices_with_trading_day(tickers, fetcher, latest_trading_day, lookback_days=60)

        if prices.empty:
            print("  ✗ No price data retrieved")
            return 1
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 5] テクニカル指標を計算
    print("\n[STEP 5] Calculating technical indicators...")
    try:
        screener = ScalpingScreener()
        prices_with_indicators = screener.add_technical_indicators(prices)
        print(f"  ✓ Calculated indicators for {prices_with_indicators['ticker'].nunique()} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 6] テクニカル評価
    print("\n[STEP 6] Evaluating technical ratings...")
    try:
        evaluated = screener.evaluate_technicals(prices_with_indicators)
        print(f"  ✓ Evaluated {len(evaluated)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 7] 最新データを抽出
    print("\n[STEP 7] Extracting latest data...")
    try:
        latest = screener.get_latest_by_ticker(evaluated)
        print(f"  ✓ Latest data: {len(latest)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 8] エントリー・アクティブ銘柄を選定
    print("\n[STEP 8] Screening for entry/active stocks...")
    try:
        entry_stocks = screener.screen_entry_stocks(latest, selected_stocks)
        active_stocks = screener.screen_active_stocks(latest, selected_stocks)

        print(f"  ✓ Entry stocks: {len(entry_stocks)}")
        print(f"  ✓ Active stocks: {len(active_stocks)}")

        # 結果サマリー
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"Latest trading day: {latest_trading_day}")
        print(f"Tested stocks: {len(tickers)}")
        print(f"Entry candidates: {len(entry_stocks)}")
        print(f"Active candidates: {len(active_stocks)}")

        if len(entry_stocks) > 0:
            print("\nEntry stocks:")
            for ticker in entry_stocks["ticker"].values:
                print(f"  - {ticker}")

        if len(active_stocks) > 0:
            print("\nActive stocks:")
            for ticker in active_stocks["ticker"].values:
                print(f"  - {ticker}")

        print("=" * 60)

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n✅ Smoke test completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
