#!/usr/bin/env python3
"""
update_granville_prices.py
TOPIX 1,660銘柄の日足データを差分更新（yfinance period="5d"）

prices_max_1d.parquet の最終日付以降のデータを取得し追記する。
GHA pipeline (18:00 JST) で実行。
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
PRICES_PATH = GRANVILLE_DIR / "prices_topix.parquet"
BATCH_SIZE = 50
SLEEP_BETWEEN = 2


def load_existing() -> pd.DataFrame:
    """既存の prices_topix.parquet を読み込み"""
    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)
    if not PRICES_PATH.exists():
        # S3フォールバック
        try:
            cfg = load_s3_config()
            if cfg and cfg.bucket:
                from common_cfg.s3io import download_file
                download_file(cfg, "granville/prices_topix.parquet", PRICES_PATH)
        except Exception:
            pass
    if not PRICES_PATH.exists():
        print(f"[ERROR] {PRICES_PATH} not found")
        sys.exit(1)
    df = pd.read_parquet(PRICES_PATH)
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_batch(tickers: list[str], period: str = "5d") -> pd.DataFrame:
    """yfinance でバッチ取得"""
    yf_tickers = [t.replace("_", ".") for t in tickers]
    try:
        df = yf.download(yf_tickers, period=period, interval="1d", progress=False, threads=True)
    except Exception as e:
        print(f"  [WARN] Batch failed: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    rows = []
    if isinstance(df.columns, pd.MultiIndex):
        for i, t_yf in enumerate(yf_tickers):
            t_orig = tickers[i]
            try:
                sub = df.xs(t_yf, level=1, axis=1).copy()
                sub = sub.dropna(subset=["Close"])
                if sub.empty:
                    continue
                sub["ticker"] = t_orig
                sub["date"] = sub.index
                sub = sub.reset_index(drop=True)
                rows.append(sub[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]])
            except Exception:
                pass
    else:
        df = df.dropna(subset=["Close"])
        if not df.empty:
            df["ticker"] = tickers[0]
            df["date"] = df.index
            df = df.reset_index(drop=True)
            rows.append(df[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]])

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def main() -> int:
    print("=" * 60)
    print("Update Granville Prices (TOPIX daily differential)")
    print("=" * 60)

    print("\n[1/3] Loading existing prices...")
    existing = load_existing()
    tickers = existing["ticker"].unique().tolist()
    last_date = existing["date"].max()
    print(f"  {len(tickers)} tickers, last date: {last_date.date()}")

    print(f"\n[2/3] Fetching 5d data for {len(tickers)} tickers...")
    all_new = []
    failed = 0
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        df = fetch_batch(batch)
        if not df.empty:
            all_new.append(df)
        else:
            failed += len(batch)
        if i > 0 and i % (BATCH_SIZE * 10) == 0:
            print(f"  {min(i + BATCH_SIZE, len(tickers))}/{len(tickers)} processed")
        time.sleep(SLEEP_BETWEEN)

    if not all_new:
        print("[ERROR] No data fetched")
        return 1

    new_df = pd.concat(all_new, ignore_index=True)
    new_df["date"] = pd.to_datetime(new_df["date"]).dt.tz_localize(None)

    new_df = new_df[new_df["date"] > last_date]
    new_dates = sorted(new_df["date"].unique())
    print(f"  New rows: {len(new_df)}")
    print(f"  New dates: {[d.strftime('%Y-%m-%d') for d in pd.to_datetime(new_dates)]}")

    if new_df.empty:
        print("[INFO] Already up to date. No new data.")
        return 0

    # クリーニング: Volume=0 かつ OHLC全同値の行は除外（休場データ）
    before = len(new_df)
    new_df = new_df[~(
        (new_df["Volume"] == 0) &
        (new_df["Open"] == new_df["Close"]) &
        (new_df["High"] == new_df["Close"]) &
        (new_df["Low"] == new_df["Close"])
    )]
    if len(new_df) < before:
        print(f"  Cleaned: {before - len(new_df)} zero-volume rows removed")

    print(f"\n[3/3] Merging and saving...")
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    combined.to_parquet(PRICES_PATH, index=False)
    print(f"  Saved: {PRICES_PATH.name}")
    print(f"  {combined['ticker'].nunique()} tickers, {len(combined):,} rows")
    print(f"  Date range: {combined['date'].min().date()} ~ {combined['date'].max().date()}")

    # S3アップロード
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, PRICES_PATH, "granville/prices_topix.parquet")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    if failed > 0:
        print(f"\n  [WARN] {failed} tickers failed to fetch")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
