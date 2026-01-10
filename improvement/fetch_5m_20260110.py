"""
5分足データ取得スクリプト（使い捨て）

grok_trending_20260106-20260109 の銘柄について
yfinanceから5分足データを取得して保存

yfinanceの5分足は過去60日のみ取得可能
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import time

BACKTEST_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet" / "backtest"
OUTPUT_PATH = BACKTEST_DIR / "grok_5m_60d_20260110.parquet"


def get_target_tickers():
    """対象銘柄を取得"""
    dates = ['20260106', '20260107', '20260108', '20260109']
    tickers = set()

    for d in dates:
        f = BACKTEST_DIR / f'grok_trending_{d}.parquet'
        if f.exists():
            df = pd.read_parquet(f)
            tickers.update(df['ticker'].tolist())

    return sorted(list(tickers))


def fetch_5m_data(ticker):
    """yfinanceから5分足データを取得（過去60日）"""
    try:
        stock = yf.Ticker(ticker)
        # period="60d" で直近60日分を取得
        df = stock.history(period="60d", interval="5m")

        if df.empty:
            return None

        df = df.reset_index()
        df = df.rename(columns={
            "Datetime": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })
        df["ticker"] = ticker
        df = df[["datetime", "open", "high", "low", "close", "volume", "ticker"]]

        return df

    except Exception as e:
        print(f"  [ERROR] {ticker}: {e}")
        return None


def main():
    print("=== 5分足データ取得 ===")

    tickers = get_target_tickers()
    print(f"対象銘柄数: {len(tickers)}")
    print(f"取得方法: yfinance period='60d' interval='5m'")

    all_data = []

    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] {ticker}...", end=" ", flush=True)
        df = fetch_5m_data(ticker)

        if df is not None:
            all_data.append(df)
            print(f"OK ({len(df)}行)")
        else:
            print("SKIP")

        # レートリミット対策
        if i % 10 == 0:
            time.sleep(1)

    if not all_data:
        print("データなし")
        return

    result = pd.concat(all_data, ignore_index=True)
    print(f"\n合計: {len(result)}行, {result['ticker'].nunique()}銘柄")
    print(f"日付範囲: {result['datetime'].min()} - {result['datetime'].max()}")

    # 保存
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"保存完了: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
