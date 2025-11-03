#!/usr/bin/env python3
"""
test_save_backtest_archive.py
Grok Trending銘柄のバックテスト結果を test_output に保存

機能:
    - 過去日付のGrok trending銘柄parquetファイルを読み込み
    - 実際の株価データを取得してバックテスト
    - 結果を test_output/test_backtest_YYYYMMDD.parquet として保存

使い方:
    # 過去3日分のバックテストを実行
    python3 scripts/pipeline/test_save_backtest_archive.py --dates 2025-10-31 2025-10-30 2025-10-29
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
    parser = argparse.ArgumentParser(description="Save backtest results to test_output")
    parser.add_argument(
        "--dates",
        type=str,
        nargs="+",
        required=True,
        help="Target dates in YYYY-MM-DD format (e.g., 2025-10-31 2025-10-30)"
    )
    parser.add_argument(
        "--time",
        type=str,
        default="16:00",
        help="Target time in HH:MM format (default: 16:00)"
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


def fetch_price_data(ticker: str, target_date: str) -> dict[str, Any]:
    """
    Yahoo Finance から前場戦略＋大引け戦略＋利確損切り戦略用の株価データを取得

    前場戦略 (phase1):
    - 買い: target_dateの5分足の最初のopen または 1日足のopen
    - 売り: target_dateの5分足の11:30のclose または それ以降の最初のopen

    大引け戦略 (phase2):
    - 買い: target_dateの1日足のopen
    - 売り: target_dateの1日足のclose

    利確損切り戦略 (phase3):
    - 買い: target_dateの5分足の最初のopen
    - 売り: 利確(+1%/+2%/+3%)または損切り(-3%)または大引け

    Args:
        ticker: 銘柄コード (例: "6857.T")
        target_date: YYYY-MM-DD (選定日の翌日 = バックテスト実行日)

    Returns:
        dict with buy_price, sell_price, daily_close, intraday_5m, volume, etc.
    """
    try:
        from datetime import time

        stock = yf.Ticker(ticker)

        # 1日足データ取得（大引け用・フォールバック用）
        next_date = (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        hist_1d = stock.history(start=target_date, end=next_date, interval="1d")

        if hist_1d.empty:
            return {}

        daily_data = hist_1d.iloc[0]
        daily_open = float(daily_data["Open"])
        daily_close = float(daily_data["Close"])

        # 5分足データ取得（前場用・phase3用）
        hist_5m = stock.history(period="5d", interval="5m")

        if not hist_5m.empty:
            # タイムゾーン削除
            hist_5m.index = hist_5m.index.tz_localize(None)

            # target_dateの全日データを抽出
            target_day_data = hist_5m[hist_5m.index.date == pd.Timestamp(target_date).date()]

            # 前場セッション（9:00-11:30）
            morning_session = target_day_data[
                (target_day_data.index.time >= time(9, 0)) &
                (target_day_data.index.time <= time(11, 30))
            ]

            if not morning_session.empty:
                # 買い価格: 前場最初のopen
                buy_price = float(morning_session.iloc[0]["Open"])

                # 売り価格: 11:30のclose または それ以降の最初のopen
                sell_price = float(morning_session.iloc[-1]["Close"])

                # 前場の高値・安値・出来高
                high = float(morning_session["High"].max())
                low = float(morning_session["Low"].min())
                volume = int(morning_session["Volume"].sum())

                # Phase3用: 全日5分足データ（9:00-15:30）
                intraday_5m = target_day_data[
                    (target_day_data.index.time >= time(9, 0)) &
                    (target_day_data.index.time <= time(15, 0))
                ].copy()

                return {
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "daily_close": daily_close,  # phase2用
                    "intraday_5m": intraday_5m,  # phase3用
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "data_source": "5min"
                }

        # 5分足データがない場合、1日足のopenをbuy_priceとして使用
        # sell_priceは取得できないのでNone
        return {
            "buy_price": daily_open,
            "sell_price": None,  # 前場引けデータなし
            "daily_close": daily_close,  # phase2用
            "intraday_5m": None,  # phase3不可
            "high": float(daily_data["High"]),
            "low": float(daily_data["Low"]),
            "volume": int(daily_data["Volume"]),
            "data_source": "1d_fallback"
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch {ticker}: {e}")
        return {}


def simulate_profit_loss_exit(buy_price: float, intraday_5m: pd.DataFrame, daily_close: float, profit_target_pct: float, loss_limit_pct: float = 3.0) -> dict[str, Any]:
    """
    利確・損切りシミュレーション

    Args:
        buy_price: 買値
        intraday_5m: 5分足データ（9:00-15:30）
        daily_close: 大引け価格
        profit_target_pct: 利確目標（%）例: 1.0, 2.0, 3.0
        loss_limit_pct: 損切りライン（%）デフォルト: 3.0

    Returns:
        dict with exit_price, exit_reason, exit_time, return, etc.
    """
    if intraday_5m is None or intraday_5m.empty:
        return {
            "exit_price": None,
            "exit_reason": "no_data",
            "exit_time": None,
            "return": None
        }

    profit_target_price = buy_price * (1 + profit_target_pct / 100)
    loss_limit_price = buy_price * (1 - loss_limit_pct / 100)

    # 5分足を時系列順に見る
    for idx, row in intraday_5m.iterrows():
        # 損切りチェック（優先）
        if row["Low"] <= loss_limit_price:
            return {
                "exit_price": loss_limit_price,
                "exit_reason": f"loss_cut_{loss_limit_pct}%",
                "exit_time": idx,
                "return": -loss_limit_pct / 100
            }

        # 利確チェック
        if row["High"] >= profit_target_price:
            return {
                "exit_price": profit_target_price,
                "exit_reason": f"profit_take_{profit_target_pct}%",
                "exit_time": idx,
                "return": profit_target_pct / 100
            }

    # どちらにも到達しなければ大引けで売る
    return {
        "exit_price": daily_close,
        "exit_reason": "eod_close",
        "exit_time": None,
        "return": (daily_close - buy_price) / buy_price
    }


def calculate_backtest_results(df: pd.DataFrame, selection_date: str) -> pd.DataFrame:
    """
    バックテスト結果を計算（前場戦略 + 大引け戦略）

    Args:
        df: Grok trending DataFrame
        selection_date: 選定日 YYYY-MM-DD

    Returns:
        DataFrame with backtest results
    """
    # 選定日の翌日がバックテスト実行日
    backtest_date = (datetime.strptime(selection_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"[INFO] Selection date: {selection_date}, Backtest date (next day): {backtest_date}")

    results = []

    for _, row in df.iterrows():
        ticker = row["ticker"]
        print(f"[INFO] Fetching {ticker}...", end=" ")

        price_data = fetch_price_data(ticker, backtest_date)

        if not price_data:
            print("SKIP (no data)")
            continue

        # 前場戦略 (phase1): 始値で買い、前場引けで売る
        buy_price = price_data["buy_price"]
        sell_price = price_data.get("sell_price")

        if sell_price is None:
            print("SKIP (no morning session data)")
            continue

        phase1_return = (sell_price - buy_price) / buy_price
        phase1_win = phase1_return > 0
        profit_per_100_shares_phase1 = (sell_price - buy_price) * 100

        # 大引け戦略 (phase2): 始値で買い、大引けで売る
        daily_close = price_data.get("daily_close")
        if daily_close:
            phase2_return = (daily_close - buy_price) / buy_price
            phase2_win = phase2_return > 0
            profit_per_100_shares_phase2 = (daily_close - buy_price) * 100
        else:
            phase2_return = None
            phase2_win = False
            profit_per_100_shares_phase2 = None

        # 利確損切り戦略 (phase3): 1%, 2%, 3%利確 または 3%損切り
        intraday_5m = price_data.get("intraday_5m")

        # Phase3-1%
        phase3_1pct = simulate_profit_loss_exit(buy_price, intraday_5m, daily_close, profit_target_pct=1.0)
        phase3_1pct_return = phase3_1pct["return"]
        phase3_1pct_win = phase3_1pct_return > 0 if phase3_1pct_return is not None else False
        phase3_1pct_exit_reason = phase3_1pct["exit_reason"]
        profit_per_100_shares_phase3_1pct = (phase3_1pct["exit_price"] - buy_price) * 100 if phase3_1pct["exit_price"] else None

        # Phase3-2%
        phase3_2pct = simulate_profit_loss_exit(buy_price, intraday_5m, daily_close, profit_target_pct=2.0)
        phase3_2pct_return = phase3_2pct["return"]
        phase3_2pct_win = phase3_2pct_return > 0 if phase3_2pct_return is not None else False
        phase3_2pct_exit_reason = phase3_2pct["exit_reason"]
        profit_per_100_shares_phase3_2pct = (phase3_2pct["exit_price"] - buy_price) * 100 if phase3_2pct["exit_price"] else None

        # Phase3-3%
        phase3_3pct = simulate_profit_loss_exit(buy_price, intraday_5m, daily_close, profit_target_pct=3.0)
        phase3_3pct_return = phase3_3pct["return"]
        phase3_3pct_win = phase3_3pct_return > 0 if phase3_3pct_return is not None else False
        phase3_3pct_exit_reason = phase3_3pct["exit_reason"]
        profit_per_100_shares_phase3_3pct = (phase3_3pct["exit_price"] - buy_price) * 100 if phase3_3pct["exit_price"] else None

        data_source = price_data.get("data_source", "unknown")
        phase2_str = f"{phase2_return*100:+.2f}%" if phase2_return is not None else "N/A"
        phase3_1pct_str = f"{phase3_1pct_return*100:+.2f}%" if phase3_1pct_return is not None else "N/A"
        phase3_2pct_str = f"{phase3_2pct_return*100:+.2f}%" if phase3_2pct_return is not None else "N/A"
        phase3_3pct_str = f"{phase3_3pct_return*100:+.2f}%" if phase3_3pct_return is not None else "N/A"
        print(f"OK (p1:{phase1_return*100:+.2f}%, p2:{phase2_str}, p3-1%:{phase3_1pct_str}, p3-2%:{phase3_2pct_str}, p3-3%:{phase3_3pct_str})")

        result = {
            "selection_date": selection_date,
            "backtest_date": backtest_date,
            "ticker": ticker,
            "company_name": row.get("stock_name", ""),
            "category": row.get("category", ""),
            "reason": row.get("reason", ""),
            "grok_rank": row.get("grok_rank", row.name + 1),
            "selection_score": row.get("selection_score", 0),
            "buy_price": buy_price,
            "sell_price": sell_price,
            "daily_close": daily_close,
            "high": price_data["high"],
            "low": price_data["low"],
            "volume": price_data["volume"],
            "phase1_return": phase1_return,
            "phase1_win": phase1_win,
            "profit_per_100_shares_phase1": profit_per_100_shares_phase1,
            "phase2_return": phase2_return,
            "phase2_win": phase2_win,
            "profit_per_100_shares_phase2": profit_per_100_shares_phase2,
            "phase3_1pct_return": phase3_1pct_return,
            "phase3_1pct_win": phase3_1pct_win,
            "phase3_1pct_exit_reason": phase3_1pct_exit_reason,
            "profit_per_100_shares_phase3_1pct": profit_per_100_shares_phase3_1pct,
            "phase3_2pct_return": phase3_2pct_return,
            "phase3_2pct_win": phase3_2pct_win,
            "phase3_2pct_exit_reason": phase3_2pct_exit_reason,
            "profit_per_100_shares_phase3_2pct": profit_per_100_shares_phase3_2pct,
            "phase3_3pct_return": phase3_3pct_return,
            "phase3_3pct_win": phase3_3pct_win,
            "phase3_3pct_exit_reason": phase3_3pct_exit_reason,
            "profit_per_100_shares_phase3_3pct": profit_per_100_shares_phase3_3pct,
            "prompt_version": row.get("prompt_version", "v1_1_web_search"),
            "data_source": data_source,
        }

        results.append(result)

    return pd.DataFrame(results)


def main() -> int:
    """メイン処理"""
    args = parse_args()

    print("=" * 80)
    print("Test Save Backtest Archive (Phase1 + Phase2 + Phase3 Strategy)")
    print("Phase1 (前場戦略): 選定日翌日の寄付買い→前場引け売り")
    print("Phase2 (大引け戦略): 選定日翌日の寄付買い→大引け売り")
    print("Phase3 (利確損切り): 寄付買い→1%/2%/3%利確 or 3%損切り or 大引け売り")
    print("=" * 80)

    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []
    time_str = args.time.replace(":", "")

    for date in args.dates:
        print(f"\n[STEP] Processing {date} at {args.time}...")

        # 1. Load Grok trending stocks
        df = load_grok_trending(date, args.time)
        if df is None or df.empty:
            print(f"[SKIP] No data for {date}")
            continue

        # 2. Calculate backtest results
        backtest_df = calculate_backtest_results(df, date)

        if backtest_df.empty:
            print(f"[WARN] No backtest results for {date}")
            continue

        all_results.append(backtest_df)

        # 3. Save individual file
        date_str = date.replace("-", "")
        # 16:00の場合は時刻なし、23:00の場合は2300を付ける
        if args.time == "16:00":
            output_path = TEST_OUTPUT_DIR / f"test_backtest_{date_str}.parquet"
        else:
            output_path = TEST_OUTPUT_DIR / f"test_backtest_{date_str}{time_str}.parquet"
        backtest_df.to_parquet(output_path, index=False)
        print(f"[OK] Saved: {output_path}")
        print(f"     Total records: {len(backtest_df)}")
        print(f"     Phase1 - Win: {backtest_df['phase1_win'].mean()*100:.1f}%, Avg: {backtest_df['phase1_return'].mean()*100:+.2f}%")
        print(f"     Phase2 - Win: {backtest_df['phase2_win'].mean()*100:.1f}%, Avg: {backtest_df['phase2_return'].mean()*100:+.2f}%")
        print(f"     Phase3-1% - Win: {backtest_df['phase3_1pct_win'].mean()*100:.1f}%, Avg: {backtest_df['phase3_1pct_return'].mean()*100:+.2f}%")
        print(f"     Phase3-2% - Win: {backtest_df['phase3_2pct_win'].mean()*100:.1f}%, Avg: {backtest_df['phase3_2pct_return'].mean()*100:+.2f}%")
        print(f"     Phase3-3% - Win: {backtest_df['phase3_3pct_win'].mean()*100:.1f}%, Avg: {backtest_df['phase3_3pct_return'].mean()*100:+.2f}%")

    # 4. Save combined file
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        # 16:00の場合は時刻なし、23:00の場合は2300を付ける
        if args.time == "16:00":
            combined_path = TEST_OUTPUT_DIR / "test_backtest_archive.parquet"
        else:
            combined_path = TEST_OUTPUT_DIR / f"test_backtest_archive{time_str}.parquet"
        combined_df.to_parquet(combined_path, index=False)
        print(f"\n[OK] Saved combined: {combined_path}")
        print(f"     Total records: {len(combined_df)}")
        print(f"     Selection dates: {combined_df['selection_date'].nunique()}")
        print(f"     Phase1 - Win: {combined_df['phase1_win'].mean()*100:.1f}%, Avg: {combined_df['phase1_return'].mean()*100:+.2f}%")
        print(f"     Phase2 - Win: {combined_df['phase2_win'].mean()*100:.1f}%, Avg: {combined_df['phase2_return'].mean()*100:+.2f}%")
        print(f"     Phase3-1% - Win: {combined_df['phase3_1pct_win'].mean()*100:.1f}%, Avg: {combined_df['phase3_1pct_return'].mean()*100:+.2f}%")
        print(f"     Phase3-2% - Win: {combined_df['phase3_2pct_win'].mean()*100:.1f}%, Avg: {combined_df['phase3_2pct_return'].mean()*100:+.2f}%")
        print(f"     Phase3-3% - Win: {combined_df['phase3_3pct_win'].mean()*100:.1f}%, Avg: {combined_df['phase3_3pct_return'].mean()*100:+.2f}%")

    print("\n" + "=" * 60)
    print("✅ Backtest archive saved successfully!")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
