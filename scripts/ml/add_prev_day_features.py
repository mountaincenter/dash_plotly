#!/usr/bin/env python3
"""
grok_trending_archive.parquet に前日OHLCVを付与し、
archive_with_features.parquet に新特徴量を追加する。

新特徴量:
  - prev_close_position: 前日引け位置 (prev_close - prev_low) / (prev_high - prev_low)
  - gap_ratio: 当日ギャップ率 (buy_price - prev_close) / prev_close
  - prev_candle: 前日陰陽線 (prev_close - prev_open) / prev_open
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf
import time

ARCHIVE_PATH = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
FEATURES_PATH = ROOT / "data" / "parquet" / "ml" / "archive_with_features.parquet"

BATCH_SIZE = 50


def fetch_prev_day_ohlcv(arc: pd.DataFrame) -> pd.DataFrame:
    """yfinanceから前日OHLCVを取得して archiveの各行に付与"""

    tickers = arc["ticker"].unique().tolist()
    start = pd.Timestamp(arc["backtest_date"].min()) - pd.Timedelta(days=10)
    end = pd.Timestamp(arc["backtest_date"].max()) + pd.Timedelta(days=1)

    print(f"Fetching {len(tickers)} tickers, {start.date()} ~ {end.date()}")

    # バッチでyfinance取得
    all_prices = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_str = " ".join(batch)
        print(f"  Batch {i // BATCH_SIZE + 1}/{len(tickers) // BATCH_SIZE + 1}: {len(batch)} tickers")

        try:
            data = yf.download(
                batch_str,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                interval="1d",
                group_by="ticker",
                progress=False,
                auto_adjust=True,
            )

            if len(batch) == 1:
                # 1銘柄の場合はMultiIndexにならない
                ticker = batch[0]
                df_t = data[["Open", "High", "Low", "Close"]].copy()
                df_t["ticker"] = ticker
                df_t["date"] = df_t.index
                all_prices.append(df_t.reset_index(drop=True))
            else:
                for ticker in batch:
                    try:
                        df_t = data[ticker][["Open", "High", "Low", "Close"]].copy()
                        df_t = df_t.dropna(subset=["Close"])
                        df_t["ticker"] = ticker
                        df_t["date"] = df_t.index
                        all_prices.append(df_t.reset_index(drop=True))
                    except (KeyError, TypeError):
                        pass
        except Exception as e:
            print(f"  Error in batch: {e}")

        if i + BATCH_SIZE < len(tickers):
            time.sleep(1)

    prices = pd.concat(all_prices, ignore_index=True)
    prices["date"] = pd.to_datetime(prices["date"]).dt.tz_localize(None)
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    print(f"Fetched {len(prices)} price rows for {prices['ticker'].nunique()} tickers")

    # 前日のOHLCVをshift
    prices["prev_open"] = prices.groupby("ticker")["Open"].shift(1)
    prices["prev_high"] = prices.groupby("ticker")["High"].shift(1)
    prices["prev_low"] = prices.groupby("ticker")["Low"].shift(1)
    prices["prev_close_yf"] = prices.groupby("ticker")["Close"].shift(1)

    # archiveとマージ (backtest_date = 当日の日付)
    arc = arc.copy()
    arc["backtest_date_dt"] = pd.to_datetime(arc["backtest_date"]).dt.tz_localize(None)

    prev_cols = prices[["ticker", "date", "prev_open", "prev_high", "prev_low", "prev_close_yf"]].copy()

    merged = arc.merge(
        prev_cols,
        left_on=["ticker", "backtest_date_dt"],
        right_on=["ticker", "date"],
        how="left",
    )

    matched = merged["prev_high"].notna().sum()
    total = len(merged)
    print(f"Matched {matched}/{total} rows ({matched / total * 100:.1f}%)")

    merged = merged.drop(columns=["date", "backtest_date_dt"])

    return merged


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """新特徴量を計算"""
    df = df.copy()

    # 全てyfinance由来のprev_*を使用（prev_closeもyf版に統一）
    # 前日引け位置: (prev_close - prev_low) / (prev_high - prev_low)
    prev_range = df["prev_high"] - df["prev_low"]
    df["prev_close_position"] = np.where(
        prev_range > 0,
        (df["prev_close_yf"] - df["prev_low"]) / prev_range,
        0.5,  # 値幅ゼロ（ストップ高/安）は中立
    )

    # 当日ギャップ率: (buy_price - prev_close) / prev_close
    df["gap_ratio"] = np.where(
        df["prev_close_yf"] > 0,
        (df["buy_price"] - df["prev_close_yf"]) / df["prev_close_yf"],
        0,
    )

    # 前日陰陽線: (prev_close - prev_open) / prev_open
    df["prev_candle"] = np.where(
        df["prev_open"] > 0,
        (df["prev_close_yf"] - df["prev_open"]) / df["prev_open"],
        0,
    )

    return df


def main():
    print("=" * 60)
    print("Step 1: Fetch prev day OHLCV from yfinance")
    print("=" * 60)

    arc = pd.read_parquet(ARCHIVE_PATH)
    print(f"Archive: {len(arc)} rows, {arc['ticker'].nunique()} tickers")

    arc_with_prev = fetch_prev_day_ohlcv(arc)

    print("\n" + "=" * 60)
    print("Step 2: Compute new features")
    print("=" * 60)

    arc_with_features = compute_features(arc_with_prev)

    # 確認
    new_cols = ["prev_close_position", "gap_ratio", "prev_candle"]
    for col in new_cols:
        valid = arc_with_features[col].notna().sum()
        print(f"  {col}: {valid}/{len(arc_with_features)} valid, mean={arc_with_features[col].mean():.4f}")

    print("\n" + "=" * 60)
    print("Step 3: Update archive_with_features.parquet")
    print("=" * 60)

    # 既存のarchive_with_featuresに新カラムを追加
    features_df = pd.read_parquet(FEATURES_PATH)
    print(f"Existing features: {len(features_df)} rows, {len(features_df.columns)} cols")

    # tickerとbacktest_dateでマージ（型を揃える）
    new_feature_cols = ["ticker", "backtest_date", "prev_open", "prev_high", "prev_low", "prev_close_yf"] + new_cols
    merge_src = arc_with_features[new_feature_cols].copy()
    features_df["backtest_date"] = pd.to_datetime(features_df["backtest_date"])
    merge_src["backtest_date"] = pd.to_datetime(merge_src["backtest_date"])

    # 既存カラムと重複があれば除外
    for c in new_cols + ["prev_open", "prev_high", "prev_low", "prev_close_yf"]:
        if c in features_df.columns:
            features_df = features_df.drop(columns=[c])

    features_df = features_df.merge(merge_src, on=["ticker", "backtest_date"], how="left")
    print(f"Updated features: {len(features_df)} rows, {len(features_df.columns)} cols")

    for col in new_cols:
        valid = features_df[col].notna().sum()
        print(f"  {col}: {valid}/{len(features_df)} valid")

    features_df.to_parquet(FEATURES_PATH, index=False)
    print(f"\nSaved to {FEATURES_PATH}")


if __name__ == "__main__":
    main()
