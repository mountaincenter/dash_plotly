#!/usr/bin/env python3
"""
16_entry_priority_analysis_topix.py
====================================
TOPIX 1,660銘柄版 エントリー優先順位分析

資金制約下で複数シグナルから「どれを選ぶか」の判断材料を定量化する。
capital_efficiency = PnL / 必要証拠金 / 保有日数 × 10000 (bps/day)
"""
from __future__ import annotations

import warnings
import time
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "09_entry_priority"

_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 80), (700, 100),
    (1000, 150), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
    (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
    (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
    (500000, 70000), (700000, 100000), (1000000, 150000),
]


def _upper_limit(price: float) -> float:
    for threshold, limit in _LIMIT_TABLE:
        if price < threshold:
            return price + limit
    return price + 150000


def required_margin(entry_price: float) -> float:
    return _upper_limit(entry_price) * 100


def main():
    t0 = time.time()

    # ========== 1. データ読込 ==========
    print("[1/5] Loading TOPIX data...")
    trades = pd.read_parquet(PROCESSED / "trades_cleaned_topix_v2.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned_topix_v3.parquet")
    long = trades[trades["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")
    print(f"  Price records: {len(prices):,}")

    # ========== 2. テクニカル指標計算 ==========
    print("[2/5] Computing technical indicators...")
    t1 = time.time()
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    prices["sma20"] = prices.groupby("ticker")["Close"].transform(
        lambda x: x.rolling(20, min_periods=20).mean()
    )
    prices["sma50"] = prices.groupby("ticker")["Close"].transform(
        lambda x: x.rolling(50, min_periods=50).mean()
    )
    prices["prev_close"] = prices.groupby("ticker")["Close"].shift(1)
    prices["tr"] = np.maximum(
        prices["High"] - prices["Low"],
        np.maximum(
            abs(prices["High"] - prices["prev_close"]),
            abs(prices["Low"] - prices["prev_close"]),
        ),
    )
    prices["atr14"] = prices.groupby("ticker")["tr"].transform(
        lambda x: x.rolling(14, min_periods=14).mean()
    )
    delta = prices.groupby("ticker")["Close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.groupby(prices["ticker"]).transform(
        lambda x: x.rolling(14, min_periods=14).mean()
    )
    avg_loss = loss.groupby(prices["ticker"]).transform(
        lambda x: x.rolling(14, min_periods=14).mean()
    )
    rs = avg_gain / avg_loss.replace(0, np.nan)
    prices["rsi14"] = 100 - (100 / (1 + rs))

    prices["daily_ret"] = prices.groupby("ticker")["Close"].pct_change()
    prices["vol20"] = prices.groupby("ticker")["daily_ret"].transform(
        lambda x: x.rolling(20, min_periods=20).std()
    )
    prices["ret5d"] = prices.groupby("ticker")["Close"].pct_change(5)

    prices["vol_avg20"] = prices.groupby("ticker")["Volume"].transform(
        lambda x: x.rolling(20, min_periods=20).mean()
    )
    prices["vol_ratio"] = prices["Volume"] / prices["vol_avg20"].replace(0, np.nan)
    print(f"  Done ({time.time()-t1:.1f}s)")

    # ========== 3. トレードに特徴量付与 ==========
    print("[3/5] Merging features...")
    price_features = prices[["ticker", "date", "Close", "sma20", "sma50",
                             "atr14", "rsi14", "vol20", "ret5d", "vol_ratio"]].copy()
    price_features = price_features.rename(columns={"date": "signal_date"})

    long["signal_date"] = pd.to_datetime(long["signal_date"])
    price_features["signal_date"] = pd.to_datetime(price_features["signal_date"])

    df = long.merge(price_features, on=["ticker", "signal_date"], how="left")

    df["margin"] = df["entry_price"].apply(required_margin)
    df["hold_days"] = df["hold_days"].clip(lower=1)
    df["cap_eff"] = df["pnl"] / df["margin"] / df["hold_days"] * 10000
    df["sma20_dist"] = (df["Close"] - df["sma20"]) / df["sma20"] * 100
    df["sma50_dist"] = (df["Close"] - df["sma50"]) / df["sma50"] * 100
    df["atr14_pct"] = df["atr14"] / df["Close"] * 100

    print(f"  特徴量付与完了: {len(df):,} trades, NaN率:")
    for col in ["sma20_dist", "atr14_pct", "rsi14", "vol20", "ret5d", "vol_ratio"]:
        print(f"    {col}: {df[col].isna().mean():.1%}")

    # ========== 4. 分析 ==========
    print("[4/5] Analysis...")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # --- 4a. ルール別基本統計 ---
    rule_stats = df.groupby("rule").agg(
        n=("pnl", "count"),
        win_rate=("win", "mean"),
        avg_pnl=("pnl", "mean"),
        median_pnl=("pnl", "median"),
        avg_ret=("ret_pct", "mean"),
        avg_hold=("hold_days", "mean"),
        avg_margin=("margin", "mean"),
        avg_cap_eff=("cap_eff", "mean"),
        median_cap_eff=("cap_eff", "median"),
    ).round(2)
    print("\n=== ルール別統計 ===")
    print(rule_stats.to_string())

    # --- 4b. 特徴量とcap_effの相関 ---
    num_cols = ["entry_price", "margin", "sma20_dist", "sma50_dist",
                "atr14_pct", "rsi14", "vol20", "ret5d", "vol_ratio"]
    corr = df[num_cols + ["cap_eff", "ret_pct", "pnl"]].corr()
    print("\n=== cap_eff との相関 ===")
    print(corr["cap_eff"].drop("cap_eff").sort_values(ascending=False).round(4).to_string())

    # --- 4c. ルール×レジーム ---
    if "regime" in df.columns:
        rule_regime = df.groupby(["rule", "regime"]).agg(
            n=("pnl", "count"),
            win_rate=("win", "mean"),
            avg_cap_eff=("cap_eff", "mean"),
        ).round(2)
        print("\n=== ルール×レジーム ===")
        print(rule_regime.to_string())

    # --- 4d. 特徴量の五分位分析 ---
    print("\n=== 特徴量 五分位分析 (cap_eff) ===")
    for col in ["entry_price", "sma20_dist", "atr14_pct", "rsi14", "vol20", "ret5d", "vol_ratio"]:
        valid = df.dropna(subset=[col])
        if len(valid) < 100:
            continue
        valid["quintile"] = pd.qcut(valid[col], 5, labels=["Q1(low)", "Q2", "Q3", "Q4", "Q5(high)"], duplicates="drop")
        q_stats = valid.groupby("quintile", observed=True).agg(
            n=("pnl", "count"),
            win_rate=("win", "mean"),
            avg_cap_eff=("cap_eff", "mean"),
            avg_pnl=("pnl", "mean"),
        ).round(2)
        print(f"\n--- {col} ---")
        print(q_stats.to_string())

    # --- 4e. 同日複数シグナル時のランキング検証 ---
    print("\n=== 同日複数シグナル: ランキング方法比較 ===")
    df["entry_date_str"] = df["entry_date"].astype(str)
    day_counts = df.groupby("entry_date_str").size()
    multi_days = day_counts[day_counts >= 3].index
    multi = df[df["entry_date_str"].isin(multi_days)].copy()
    print(f"3件以上シグナル日: {len(multi_days):,}日, トレード数: {len(multi):,}")

    if len(multi) > 0:
        rule_rank = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
        multi["rank_rule"] = multi["rule"].map(rule_rank)
        multi["rank_cheap"] = multi.groupby("entry_date_str")["margin"].rank()
        multi["rank_atr"] = multi.groupby("entry_date_str")["atr14_pct"].rank(ascending=False)
        multi["rank_rsi"] = multi.groupby("entry_date_str")["rsi14"].rank()
        multi["rank_ret5d"] = multi.groupby("entry_date_str")["ret5d"].rank()

        for k in [1, 3, 5]:
            print(f"\n  Top-{k} 選択時の平均cap_eff:")
            for method, rank_col in [
                ("ルール優先", "rank_rule"),
                ("証拠金安い順", "rank_cheap"),
                ("ATR%高い順", "rank_atr"),
                ("RSI低い順", "rank_rsi"),
                ("5日リターン低い順", "rank_ret5d"),
            ]:
                selected = multi[multi[rank_col] <= k]
                if len(selected) > 0:
                    ce = selected["cap_eff"].mean()
                    wr = selected["win"].mean()
                    pnl = selected["pnl"].mean()
                    print(f"    {method:20s}: cap_eff={ce:+.2f}, 勝率={wr:.1%}, 平均PnL={pnl:+,.0f}円 (n={len(selected):,})")

        print(f"\n  ランダム選択(参考): 全体平均 cap_eff={multi['cap_eff'].mean():+.2f}, 勝率={multi['win'].mean():.1%}")

    # --- 4f. セクター別 ---
    if "sector" in df.columns:
        sector_stats = df.groupby("sector").agg(
            n=("pnl", "count"),
            win_rate=("win", "mean"),
            avg_cap_eff=("cap_eff", "mean"),
            avg_pnl=("pnl", "mean"),
        ).sort_values("avg_cap_eff", ascending=False).round(2)
        print("\n=== セクター別 cap_eff ===")
        print(sector_stats.head(10).to_string())
        print("...")
        print(sector_stats.tail(5).to_string())

    # ========== 5. 保存 ==========
    print(f"\n[5/5] Saving...")
    df.to_parquet(REPORT_DIR / "trades_with_features_topix.parquet", index=False)
    print(f"  保存: {REPORT_DIR / 'trades_with_features_topix.parquet'}")
    print(f"  Done in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
