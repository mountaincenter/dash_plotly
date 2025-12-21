#!/usr/bin/env python3
"""
fetch_grok_prices.py
Grok銘柄の価格データをyfinanceから取得

使用方法:
    python3 improvement/yfinance/scripts/fetch_grok_prices.py

出力:
    improvement/yfinance/data/prices_60d_5m.parquet  - 5分足60日分
    improvement/yfinance/data/prices_max_1d.parquet  - 日足全期間
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import time

# パス定義
ROOT = Path(__file__).resolve().parents[3]
IMPROVEMENT_DIR = ROOT / "improvement"
DATA_DIR = IMPROVEMENT_DIR / "yfinance" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Grok銘柄アーカイブ
GROK_ARCHIVE_PATH = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"


def get_grok_tickers() -> list:
    """Grok銘柄リストを取得"""
    if not GROK_ARCHIVE_PATH.exists():
        print(f"[ERROR] {GROK_ARCHIVE_PATH} not found")
        return []

    df = pd.read_parquet(GROK_ARCHIVE_PATH)
    tickers = df['ticker'].unique().tolist()
    print(f"[INFO] Found {len(tickers)} unique Grok tickers")
    return tickers


def fetch_5m_data(tickers: list, days: int = 60) -> pd.DataFrame:
    """5分足データを取得（最大60日）"""
    print(f"\n=== Fetching 5min data ({days} days) ===")

    all_data = []
    failed = []

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}...", end=" ", flush=True)
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period=f"{days}d", interval="5m")

            if df.empty:
                print("SKIP (no data)")
                failed.append(ticker)
                continue

            df = df.reset_index()
            df['ticker'] = ticker

            # タイムゾーン除去
            if 'Datetime' in df.columns:
                df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)

            all_data.append(df)
            print(f"OK ({len(df)} rows)")

            time.sleep(0.3)  # レート制限対策

        except Exception as e:
            print(f"ERROR: {e}")
            failed.append(ticker)

    if not all_data:
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    print(f"\n[OK] Total: {len(result)} rows, {len(tickers) - len(failed)}/{len(tickers)} tickers")

    if failed:
        print(f"[WARN] Failed tickers: {failed}")

    return result


def fetch_daily_data(tickers: list, period: str = "max") -> pd.DataFrame:
    """日足データを取得"""
    print(f"\n=== Fetching daily data (period={period}) ===")

    all_data = []
    failed = []

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}...", end=" ", flush=True)
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period=period, interval="1d")

            if df.empty:
                print("SKIP (no data)")
                failed.append(ticker)
                continue

            df = df.reset_index()
            df['ticker'] = ticker

            # Date列の処理
            if 'Date' in df.columns:
                df['date'] = pd.to_datetime(df['Date']).dt.date

            all_data.append(df)
            print(f"OK ({len(df)} rows)")

            time.sleep(0.2)

        except Exception as e:
            print(f"ERROR: {e}")
            failed.append(ticker)

    if not all_data:
        return pd.DataFrame()

    result = pd.concat(all_data, ignore_index=True)
    print(f"\n[OK] Total: {len(result)} rows, {len(tickers) - len(failed)}/{len(tickers)} tickers")

    if failed:
        print(f"[WARN] Failed tickers: {failed}")

    return result


def main():
    print("=" * 60)
    print("Grok銘柄 価格データ取得 (yfinance)")
    print("=" * 60)

    # 銘柄リスト取得
    tickers = get_grok_tickers()
    if not tickers:
        return 1

    # 5分足データ取得
    df_5m = fetch_5m_data(tickers, days=60)
    if not df_5m.empty:
        output_5m = DATA_DIR / "prices_60d_5m.parquet"
        df_5m.to_parquet(output_5m, index=False)
        print(f"[OK] Saved: {output_5m}")

    # 日足データ取得
    df_1d = fetch_daily_data(tickers, period="max")
    if not df_1d.empty:
        output_1d = DATA_DIR / "prices_max_1d.parquet"
        df_1d.to_parquet(output_1d, index=False)
        print(f"[OK] Saved: {output_1d}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
