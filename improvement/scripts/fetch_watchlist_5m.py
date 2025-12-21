#!/usr/bin/env python3
"""
常習犯リスト銘柄の5分足データ取得

morning_peak_watchlist.parquet の銘柄のみ対象。
surge_candidates_5m.parquet と比較して必要なら更新。
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, date
import time
import sys

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
WATCHLIST_PATH = DATA_DIR / "morning_peak_watchlist.parquet"
OUTPUT_PATH = DATA_DIR / "watchlist_5m_latest.parquet"

BATCH_SIZE = 30
SLEEP_BETWEEN_TICKERS = 0.3
SLEEP_BETWEEN_BATCHES = 3


def main():
    print("=" * 60)
    print("常習犯リスト銘柄 5分足取得")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 常習犯リスト読み込み
    watchlist = pd.read_parquet(WATCHLIST_PATH)
    tickers = sorted(watchlist['ticker'].unique().tolist())
    print(f"\n[INFO] 対象銘柄: {len(tickers)}銘柄")

    # バッチ分割
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"[INFO] {len(tickers)}銘柄 → {total_batches}バッチ")

    all_results = []

    for batch_num, batch_tickers in enumerate(batches, 1):
        print(f"\nバッチ {batch_num}/{total_batches} ({len(batch_tickers)}銘柄)")

        for i, ticker in enumerate(batch_tickers, 1):
            print(f"  [{i}/{len(batch_tickers)}] {ticker}...", end=" ", flush=True)

            try:
                stock = yf.Ticker(ticker)
                df = stock.history(period="60d", interval="5m")

                if df.empty:
                    print("SKIP")
                    continue

                df = df.reset_index()
                df['ticker'] = ticker

                if 'Datetime' in df.columns:
                    df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)

                all_results.append(df)
                print(f"OK ({len(df)} rows)")

            except Exception as e:
                print(f"ERROR: {e}")

            time.sleep(SLEEP_BETWEEN_TICKERS)

        if batch_num < total_batches:
            print(f"  待機 {SLEEP_BETWEEN_BATCHES}秒...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    if all_results:
        df_final = pd.concat(all_results, ignore_index=True)
        df_final = df_final.sort_values(['ticker', 'Datetime']).reset_index(drop=True)

        df_final.to_parquet(OUTPUT_PATH, index=False)

        print(f"\n{'='*60}")
        print(f"[OK] 保存完了: {OUTPUT_PATH}")
        print(f"  総件数: {len(df_final):,} rows")
        print(f"  銘柄数: {df_final['ticker'].nunique()}")
        df_final['date'] = pd.to_datetime(df_final['Datetime']).dt.date
        print(f"  期間: {df_final['date'].min()} 〜 {df_final['date'].max()}")
        print(f"{'='*60}")
    else:
        print("\n[ERROR] データが取得できませんでした")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
