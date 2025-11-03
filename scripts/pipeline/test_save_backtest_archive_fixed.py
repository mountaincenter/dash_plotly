#!/usr/bin/env python3
"""
test_save_backtest_archive_fixed.py
Grok Trending銘柄のバックテスト結果を test_output に保存（修正版）

機能:
    - 過去日付のGrok trending銘柄parquetファイルを読み込み
    - 正しいロジックでバックテスト: 選定時刻の次の取引日の始値で買い、翌取引日の始値で売り
    - 結果を test_output/test_backtest_YYYYMMDD.parquet として保存

使い方:
    python3 scripts/pipeline/test_save_backtest_archive_fixed.py --dates 2025-10-29 2025-10-30 --time 16:00
    python3 scripts/pipeline/test_save_backtest_archive_fixed.py --dates 2025-10-29 2025-10-30 --time 23:00
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf

# 出力先ディレクトリ
TEST_OUTPUT_DIR = ROOT / "data" / "test_output"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Save backtest results to test_output (FIXED)")
    parser.add_argument(
        "--dates",
        type=str,
        nargs="+",
        required=True,
        help="Selection dates in YYYY-MM-DD format (e.g., 2025-10-31 2025-10-30)"
    )
    parser.add_argument(
        "--time",
        type=str,
        default="16:00",
        help="Selection time in HH:MM format (default: 16:00)"
    )
    return parser.parse_args()


def load_grok_trending(date: str, time: str = "16:00") -> pd.DataFrame | None:
    """
    test_output から Grok trending parquet を読み込み

    Args:
        date: YYYY-MM-DD format
        time: HH:MM format (default: 16:00)

    Returns:
        DataFrame or None
    """
    date_str = date.replace("-", "")
    time_str = time.replace(":", "")

    # 16:00の場合は時刻なし、23:00の場合は2300を付ける
    if time == "16:00":
        parquet_path = TEST_OUTPUT_DIR / f"grok_trending_{date_str}.parquet"
    else:
        parquet_path = TEST_OUTPUT_DIR / f"grok_trending_{date_str}{time_str}.parquet"

    if not parquet_path.exists():
        print(f"[WARN] File not found: {parquet_path}")
        return None

    df = pd.read_parquet(parquet_path)
    print(f"[OK] Loaded {len(df)} stocks from {parquet_path.name}")
    return df


def fetch_price_data_for_backtest(ticker: str, selection_date: str) -> dict[str, Any]:
    """
    Yahoo Finance から株価データを取得してバックテスト

    ロジック:
    - 選定日の次の取引日の始値で買い
    - その翌取引日の始値で売り

    Args:
        ticker: 銘柄コード (例: "6857.T")
        selection_date: 選定日 YYYY-MM-DD

    Returns:
        dict with buy_date, buy_price, sell_date, sell_price, etc.
    """
    try:
        # 選定日から10営業日後まで取得（余裕を持たせる）
        start_date = selection_date
        end_date = (datetime.strptime(selection_date, "%Y-%m-%d") + timedelta(days=15)).strftime("%Y-%m-%d")

        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date, interval="1d")

        if hist.empty or len(hist) < 2:
            return {}

        # インデックスを日付のみに変換
        hist.index = pd.to_datetime(hist.index).date
        selection_date_obj = datetime.strptime(selection_date, "%Y-%m-%d").date()

        # 選定日より後の取引日を取得
        future_dates = [d for d in hist.index if d > selection_date_obj]

        if len(future_dates) < 2:
            return {}

        # 次の取引日（買い日）と翌取引日（売り日）
        buy_date = future_dates[0]
        sell_date = future_dates[1]

        buy_row = hist.loc[buy_date]
        sell_row = hist.loc[sell_date]

        return {
            "buy_date": buy_date.strftime("%Y-%m-%d"),
            "buy_price": float(buy_row["Open"]),
            "sell_date": sell_date.strftime("%Y-%m-%d"),
            "sell_price": float(sell_row["Open"]),
            "buy_high": float(buy_row["High"]),
            "buy_low": float(buy_row["Low"]),
            "buy_close": float(buy_row["Close"]),
            "buy_volume": int(buy_row["Volume"]),
            "sell_high": float(sell_row["High"]),
            "sell_low": float(sell_row["Low"]),
            "sell_close": float(sell_row["Close"]),
            "sell_volume": int(sell_row["Volume"]),
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch {ticker}: {e}")
        return {}


def calculate_backtest_results(df: pd.DataFrame, selection_date: str) -> pd.DataFrame:
    """
    バックテスト結果を計算（修正版）

    Args:
        df: Grok trending DataFrame
        selection_date: 選定日 YYYY-MM-DD

    Returns:
        DataFrame with backtest results
    """
    results = []

    for _, row in df.iterrows():
        ticker = row["ticker"]
        print(f"[INFO] Fetching {ticker}...", end=" ")

        price_data = fetch_price_data_for_backtest(ticker, selection_date)

        if not price_data:
            print("SKIP (no data)")
            continue

        # バックテスト計算
        buy_price = price_data["buy_price"]
        sell_price = price_data["sell_price"]
        phase1_return = (sell_price - buy_price) / buy_price
        phase1_win = phase1_return > 0
        profit_per_100_shares = (sell_price - buy_price) * 100

        print(f"OK (買:{price_data['buy_date']}, 売:{price_data['sell_date']}, return: {phase1_return*100:+.2f}%)")

        result = {
            "selection_date": selection_date,
            "buy_date": price_data["buy_date"],
            "sell_date": price_data["sell_date"],
            "ticker": ticker,
            "company_name": row.get("stock_name", ""),
            "category": row.get("category", ""),
            "reason": row.get("reason", ""),
            "grok_rank": row.get("grok_rank", row.name + 1),
            "selection_score": row.get("selection_score", 0),
            "buy_price": buy_price,
            "sell_price": sell_price,
            "buy_high": price_data["buy_high"],
            "buy_low": price_data["buy_low"],
            "buy_close": price_data["buy_close"],
            "buy_volume": price_data["buy_volume"],
            "sell_high": price_data["sell_high"],
            "sell_low": price_data["sell_low"],
            "sell_close": price_data["sell_close"],
            "sell_volume": price_data["sell_volume"],
            "phase1_return": phase1_return,
            "phase1_win": phase1_win,
            "profit_per_100_shares": profit_per_100_shares,
            "prompt_version": row.get("prompt_version", "v1_1_web_search"),
        }

        results.append(result)

    return pd.DataFrame(results)


def main() -> int:
    """メイン処理"""
    args = parse_args()

    print("=" * 80)
    print("Test Save Backtest Archive (FIXED VERSION)")
    print("バックテストロジック: 選定時刻の次の取引日の始値で買い、翌取引日の始値で売り")
    print("=" * 80)

    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []
    time_str = args.time.replace(":", "")

    for date in args.dates:
        print(f"\n[STEP] Processing selection date: {date} at {args.time}...")

        # 1. Load Grok trending stocks
        df = load_grok_trending(date, args.time)
        if df is None or df.empty:
            print(f"[SKIP] No data for {date}")
            continue

        # 2. Calculate backtest results (FIXED)
        backtest_df = calculate_backtest_results(df, date)

        if backtest_df.empty:
            print(f"[WARN] No backtest results for {date}")
            continue

        all_results.append(backtest_df)

        # 3. Save individual file
        date_str = date.replace("-", "")
        output_path = TEST_OUTPUT_DIR / f"test_backtest_{date_str}{time_str}.parquet"
        backtest_df.to_parquet(output_path, index=False)
        print(f"[OK] Saved: {output_path}")
        print(f"     Total records: {len(backtest_df)}")
        print(f"     Win rate: {backtest_df['phase1_win'].mean()*100:.1f}%")
        print(f"     Avg return: {backtest_df['phase1_return'].mean()*100:+.2f}%")

    # 4. Save combined file
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        combined_path = TEST_OUTPUT_DIR / f"test_backtest_archive{time_str}.parquet"
        combined_df.to_parquet(combined_path, index=False)
        print(f"\n[OK] Saved combined archive: {combined_path}")
        print(f"     Total records: {len(combined_df)}")
        print(f"     Selection dates: {combined_df['selection_date'].nunique()}")
        print(f"     Overall win rate: {combined_df['phase1_win'].mean()*100:.1f}%")
        print(f"     Overall avg return: {combined_df['phase1_return'].mean()*100:+.2f}%")

    print("\n" + "=" * 80)
    print("✅ Backtest archive saved successfully (FIXED)!")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
