#!/usr/bin/env python3
"""
06_add_entry_features.py
========================
トレードデータにエントリー時点の市場特徴量を追加する。

入力:
  - strategy_verification/data/processed/trades_with_mae_mfe.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet

出力:
  - strategy_verification/data/processed/trades_with_features.parquet

追加カラム:
  - sma20_dev:    signal_date時点の SMA20乖離率 (%)
  - atr14_pct:    signal_date時点の ATR(14) / Close (%)
  - consec_down:  signal_dateまでの連続陰線日数
  - vol_ratio:    signal_date時点の Volume / 20日平均Volume

結合キー: (ticker, signal_date) ← signal_date = シグナル発火日の Close ベース
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PROCESSED = ROOT / "strategy_verification" / "data" / "processed"


def compute_price_features(prices: pd.DataFrame) -> pd.DataFrame:
    """全銘柄の日次特徴量を一括計算"""
    print("[2/4] Computing price features...")
    dfs = []
    tickers = prices["ticker"].unique()
    for i, t in enumerate(tickers):
        p = prices[prices["ticker"] == t].sort_values("date").copy()

        # SMA20 deviation
        p["sma20"] = p["Close"].rolling(20, min_periods=20).mean()
        p["sma20_dev"] = (p["Close"] - p["sma20"]) / p["sma20"] * 100

        # ATR14 as % of Close
        tr = pd.concat([
            p["High"] - p["Low"],
            (p["High"] - p["Close"].shift(1)).abs(),
            (p["Low"] - p["Close"].shift(1)).abs(),
        ], axis=1).max(axis=1)
        p["atr14"] = tr.rolling(14, min_periods=14).mean()
        p["atr14_pct"] = p["atr14"] / p["Close"] * 100

        # Consecutive down days
        down = (p["Close"] < p["Close"].shift(1)).astype(int)
        consec = []
        count = 0
        for d in down:
            if d == 1:
                count += 1
            else:
                count = 0
            consec.append(count)
        p["consec_down"] = consec

        # Volume ratio (vs 20-day MA)
        vol_ma20 = p["Volume"].rolling(20, min_periods=20).mean()
        p["vol_ratio"] = p["Volume"] / vol_ma20

        dfs.append(p[["ticker", "date", "sma20_dev", "atr14_pct", "consec_down", "vol_ratio"]])

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(tickers)} tickers...")

    result = pd.concat(dfs, ignore_index=True)
    print(f"  features computed: {len(result):,} rows, {result['ticker'].nunique()} tickers")
    return result


def main():
    t0 = time.time()
    print("[1/4] Loading data...")
    trades = pd.read_parquet(PROCESSED / "trades_with_mae_mfe.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned.parquet")
    print(f"  trades: {len(trades):,}, prices: {len(prices):,}")

    features = compute_price_features(prices)

    print("[3/4] Joining features to trades...")
    # signal_date の Close ベースで特徴量を結合
    features["date"] = pd.to_datetime(features["date"])
    trades["signal_date"] = pd.to_datetime(trades["signal_date"])

    merged = trades.merge(
        features,
        left_on=["ticker", "signal_date"],
        right_on=["ticker", "date"],
        how="left",
    )
    merged = merged.drop(columns=["date"])

    n_missing = merged["sma20_dev"].isna().sum()
    print(f"  matched: {len(merged) - n_missing:,} / {len(merged):,} ({n_missing} missing)")

    # Drop rows with missing features (early data, no SMA20 yet)
    if n_missing > 0:
        merged = merged.dropna(subset=["sma20_dev"]).reset_index(drop=True)
        print(f"  after drop: {len(merged):,} trades")

    print("[4/4] Saving...")
    out_path = PROCESSED / "trades_with_features.parquet"
    merged.to_parquet(out_path, index=False)
    print(f"  output: {out_path}")
    print(f"  columns: {list(merged.columns)}")

    # Quick validation
    long_b4 = merged[(merged["direction"] == "LONG") & (merged["rule"] == "B4")]
    print(f"\n=== B4 LONG feature summary ===")
    for col in ["sma20_dev", "atr14_pct", "consec_down", "vol_ratio"]:
        s = long_b4[col]
        print(f"  {col}: mean={s.mean():.2f}, median={s.median():.2f}, min={s.min():.2f}, max={s.max():.2f}")

    print(f"\n=== Done in {time.time()-t0:.1f}s ===")


if __name__ == "__main__":
    main()
