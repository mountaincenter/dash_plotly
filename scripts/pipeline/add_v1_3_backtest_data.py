#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v1.3 archive に yfinance でバックテストデータを追加

5分足・日足を取得して Phase1/Phase2 のパフォーマンスを計算
"""

import sys
from pathlib import Path
import pandas as pd
import yfinance as yf
from datetime import datetime, time, timedelta
import time as time_module

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from scripts.lib.jquants_fetcher import JQuantsFetcher


def get_next_trading_day(selection_date: str, fetcher: JQuantsFetcher) -> str:
    """Get next trading day after selection_date"""

    # Parse selection_date
    current = datetime.strptime(selection_date, "%Y-%m-%d").date()

    # Fetch calendar for next 10 days
    start_date = current + timedelta(days=1)
    end_date = current + timedelta(days=15)

    params = {
        "from": str(start_date),
        "to": str(end_date)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # Filter trading days (HolidayDivision == "1")
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    # Get first trading day after selection_date
    future_trading_days = trading_days[trading_days["Date"] > current]

    if future_trading_days.empty:
        return None

    return str(future_trading_days.iloc[0]["Date"])


def fetch_intraday_data(ticker: str, backtest_date: str) -> pd.DataFrame:
    """Fetch 5-minute intraday data for backtest_date"""
    try:
        # ticker already includes .T suffix
        yf_ticker = ticker

        # Fetch 5-day data to ensure we get the target date
        end_date = datetime.strptime(backtest_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = end_date - timedelta(days=5)

        data = yf.download(
            yf_ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="5m",
            progress=False,
            auto_adjust=True
        )

        if data.empty:
            return None

        # Handle multi-index columns (flatten if needed)
        if isinstance(data.columns, pd.MultiIndex):
            # Flatten multi-index columns by taking first level
            data.columns = data.columns.get_level_values(0)

        # Convert UTC to JST (Asia/Tokyo)
        data.index = pd.to_datetime(data.index)
        if data.index.tz is not None:
            data.index = data.index.tz_convert('Asia/Tokyo')

        # Filter to target date (JST)
        target_data = data[data.index.date == datetime.strptime(backtest_date, "%Y-%m-%d").date()]

        if target_data.empty:
            return None

        return target_data

    except Exception as e:
        print(f"  Error fetching intraday {ticker}: {e}")
        return None


def fetch_daily_data(ticker: str, backtest_date: str, selection_date: str) -> dict:
    """Fetch daily OHLCV for backtest_date and previous day close"""
    try:
        # ticker already includes .T suffix
        yf_ticker = ticker

        # Fetch 10-day window
        end_date = datetime.strptime(backtest_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = end_date - timedelta(days=10)

        data = yf.download(
            yf_ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False
        )

        if data.empty:
            return None

        # Get target date row (backtest_date)
        data.index = pd.to_datetime(data.index)
        target_row = data[data.index.date == datetime.strptime(backtest_date, "%Y-%m-%d").date()]

        if target_row.empty:
            return None

        row = target_row.iloc[0]

        # Handle both single-index and multi-index columns
        def get_value(row_data, col_name):
            if col_name in row_data.index:
                return float(row_data[col_name])
            # Try multi-index format
            for col in row_data.index:
                if isinstance(col, tuple) and col_name in col:
                    return float(row_data[col])
            return None

        # Get previous day close (selection_date)
        prev_row = data[data.index.date == datetime.strptime(selection_date, "%Y-%m-%d").date()]
        prev_close = None
        if not prev_row.empty:
            prev_close = get_value(prev_row.iloc[0], "Close")

        return {
            "open": get_value(row, "Open"),
            "high": get_value(row, "High"),
            "low": get_value(row, "Low"),
            "close": get_value(row, "Close"),
            "volume": get_value(row, "Volume"),
            "prev_close": prev_close
        }

    except Exception as e:
        print(f"  Error fetching daily {ticker}: {e}")
        return None


def calculate_phase1_performance(intraday_df: pd.DataFrame, daily_open: float) -> dict:
    """
    Phase1: 寄り付き買い → 11:30前場引け売り
    """
    if intraday_df is None or intraday_df.empty:
        return None

    # Find 11:30 candle (morning close)
    intraday_df.index = pd.to_datetime(intraday_df.index)

    # JST timezone-aware
    target_time = time(11, 30)

    # Find morning data (9:00-12:00)
    morning_data = intraday_df[
        (intraday_df.index.time >= time(9, 0)) &
        (intraday_df.index.time <= time(12, 0))
    ]

    if morning_data.empty:
        return None

    # Use daily open as entry price
    entry_price = daily_open

    # Find 11:30 exit price (or closest available)
    exit_candidates = morning_data[morning_data.index.time >= target_time]
    if exit_candidates.empty:
        # Use last available price in morning session
        exit_price = morning_data.iloc[-1]["Close"]
    else:
        # Use first candle at or after 11:30
        exit_price = exit_candidates.iloc[0]["Close"]

    # Calculate return
    phase1_return_pct = ((exit_price - entry_price) / entry_price) * 100

    return {
        "phase1_entry_price": entry_price,
        "phase1_exit_price": exit_price,
        "phase1_return_pct": phase1_return_pct
    }


def calculate_phase2_performance(daily_open: float, daily_close: float) -> dict:
    """
    Phase2: 寄り付き買い → 引け売り
    """
    phase2_return_pct = ((daily_close - daily_open) / daily_open) * 100

    return {
        "phase2_entry_price": daily_open,
        "phase2_exit_price": daily_close,
        "phase2_return_pct": phase2_return_pct
    }


def add_backtest_data(archive_path: Path, output_path: Path):
    """Add backtest data to v1.3 archive"""

    # Load archive
    df = pd.read_parquet(archive_path)
    print(f"Loaded {len(df)} stocks from archive")

    # Initialize J-Quants fetcher
    fetcher = JQuantsFetcher()

    # Add backtest columns (same format as save_backtest_to_archive.py)
    df["selection_date"] = None  # 選定日（前営業日）
    df["backtest_date"] = None   # バックテスト実行日
    df["buy_price"] = None       # 寄り付き価格
    df["sell_price"] = None      # Phase1売却価格（前場引け近似）
    df["daily_close"] = None     # 終値
    df["high"] = None
    df["low"] = None
    df["volume"] = None
    df["phase1_return"] = None   # Phase1リターン率
    df["phase1_win"] = None
    df["profit_per_100_shares_phase1"] = None  # 100株あたり利益
    df["phase2_return"] = None   # Phase2リターン率
    df["phase2_win"] = None
    df["profit_per_100_shares_phase2"] = None  # 100株あたり利益

    # Process each stock
    for idx, row in df.iterrows():
        ticker = row["ticker"]
        backtest_date = row["date"]  # dateカラムはバックテスト実行日
        stock_name = row.get("stock_name", "")

        print(f"\n[{idx+1}/{len(df)}] {ticker} ({stock_name}) - Backtest: {backtest_date}")

        df.at[idx, "backtest_date"] = backtest_date

        # Get previous trading day for selection_date
        # backtest_dateの前営業日を取得（これが選定日）
        params = {
            "from": str(datetime.strptime(backtest_date, "%Y-%m-%d").date() - timedelta(days=10)),
            "to": str(backtest_date)
        }
        response = fetcher.client.request("/markets/trading_calendar", params=params)
        calendar = pd.DataFrame(response["trading_calendar"])
        trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
        trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
        trading_days = trading_days.sort_values("Date")

        past_trading_days = trading_days[trading_days["Date"] < datetime.strptime(backtest_date, "%Y-%m-%d").date()]
        if past_trading_days.empty:
            print(f"  ⚠️  No previous trading day found")
            continue

        selection_date = str(past_trading_days.iloc[-1]["Date"])
        df.at[idx, "selection_date"] = selection_date
        print(f"  Selection date: {selection_date}")

        # Fetch daily data (backtest_date and prev day close)
        daily_data = fetch_daily_data(ticker, backtest_date, selection_date)
        if not daily_data:
            print(f"  ⚠️  No daily data")
            continue

        buy_price = daily_data["open"]
        daily_close = daily_data["close"]

        df.at[idx, "buy_price"] = buy_price
        df.at[idx, "daily_close"] = daily_close
        df.at[idx, "high"] = daily_data["high"]
        df.at[idx, "low"] = daily_data["low"]
        df.at[idx, "volume"] = daily_data["volume"]

        # Fetch 5-minute intraday data for Phase1
        intraday_df = fetch_intraday_data(ticker, backtest_date)

        if intraday_df is not None:
            print(f"  5m data: {len(intraday_df)} candles")
        else:
            print(f"  5m data: None")

        # Calculate Phase1 (open → 11:30 morning close)
        if intraday_df is not None and not intraday_df.empty:
            phase1_perf = calculate_phase1_performance(intraday_df, buy_price)
            if phase1_perf:
                sell_price = phase1_perf["phase1_exit_price"]
                phase1_return = phase1_perf["phase1_return_pct"] / 100
                phase1_win = phase1_return > 0
                profit_per_100_shares_phase1 = (sell_price - buy_price) * 100

                df.at[idx, "sell_price"] = sell_price
                df.at[idx, "phase1_return"] = phase1_return
                df.at[idx, "phase1_win"] = phase1_win
                df.at[idx, "profit_per_100_shares_phase1"] = profit_per_100_shares_phase1
            else:
                # Fallback: use daily close
                df.at[idx, "sell_price"] = daily_close
                df.at[idx, "phase1_return"] = None
                df.at[idx, "phase1_win"] = None
                df.at[idx, "profit_per_100_shares_phase1"] = None
        else:
            # No intraday data available
            df.at[idx, "sell_price"] = daily_close
            df.at[idx, "phase1_return"] = None
            df.at[idx, "phase1_win"] = None
            df.at[idx, "profit_per_100_shares_phase1"] = None

        # Phase2: 寄り付き → 大引け
        phase2_return = (daily_close - buy_price) / buy_price
        phase2_win = phase2_return > 0
        profit_per_100_shares_phase2 = (daily_close - buy_price) * 100

        df.at[idx, "phase2_return"] = phase2_return
        df.at[idx, "phase2_win"] = phase2_win
        df.at[idx, "profit_per_100_shares_phase2"] = profit_per_100_shares_phase2

        # Print results
        phase1_str = "N/A"
        if df.at[idx, "phase1_return"] is not None:
            phase1_str = f"{df.at[idx, 'phase1_return']*100:+.2f}% (¥{df.at[idx, 'profit_per_100_shares_phase1']:+,.0f}/100株)"

        print(f"  Buy={buy_price:.1f}, Close={daily_close:.1f}")
        print(f"  Phase1 (→11:30): {phase1_str}")
        print(f"  Phase2 (→15:00): {phase2_return*100:+.2f}% (¥{profit_per_100_shares_phase2:+,.0f}/100株)")

        # Rate limit
        time_module.sleep(0.5)

    # Save
    df.to_parquet(output_path, index=False)
    print(f"\n✅ Saved to: {output_path}")

    # Summary
    phase1_available = df["phase1_return"].notna().sum()
    phase2_available = df["phase2_return"].notna().sum()

    print(f"\n📊 Summary:")
    print(f"  Total stocks: {len(df)}")
    print(f"  Phase1 data: {phase1_available}/{len(df)}")
    print(f"  Phase2 data: {phase2_available}/{len(df)}")

    if phase1_available > 0:
        phase1_avg_return = df["phase1_return"].mean() * 100
        phase1_avg_profit = df["profit_per_100_shares_phase1"].mean()
        phase1_win_rate = df["phase1_win"].sum() / phase1_available * 100
        print(f"  Phase1 Avg Return: {phase1_avg_return:+.2f}%")
        print(f"  Phase1 Avg Profit: ¥{phase1_avg_profit:+,.0f}/100株")
        print(f"  Phase1 Win Rate: {phase1_win_rate:.1f}%")

    if phase2_available > 0:
        phase2_avg_return = df["phase2_return"].mean() * 100
        phase2_avg_profit = df["profit_per_100_shares_phase2"].mean()
        phase2_win_rate = df["phase2_win"].sum() / phase2_available * 100
        print(f"  Phase2 Avg Return: {phase2_avg_return:+.2f}%")
        print(f"  Phase2 Avg Profit: ¥{phase2_avg_profit:+,.0f}/100株")
        print(f"  Phase2 Win Rate: {phase2_win_rate:.1f}%")


if __name__ == "__main__":
    print("=" * 60)
    print("v1.3 Backtest Data Addition (yfinance)")
    print("=" * 60)
    print()

    archive_path = project_root / "data" / "parquet" / "backtest" / "v1.3_grok_trending_archive.parquet"
    output_path = project_root / "data" / "parquet" / "backtest" / "v1.3_grok_trending_archive_with_backtest.parquet"

    add_backtest_data(archive_path, output_path)

    print("\n🎉 Completed!")
