#!/usr/bin/env python3
"""
clean_prices_jquants_crossval.py
yfinance価格データをJQuants調整済み価格と突合し、分割未調整を修正する。

処理:
  1. prices_max_1d.parquet の異常リターン(±30%)を検出
  2. 該当銘柄をJQuants APIで取得し、AdjustmentFactor != 1 を分割と判定
  3. 分割銘柄のOHLCVをJQuants調整済み価格で置換
  4. 修正済みparquetを上書き保存
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

PRICES_PATH = PARQUET_DIR / "prices_max_1d.parquet"
EXTREME_RET_THRESHOLD = 30.0  # ±30%以上を異常リターンとして検出


def detect_extreme_returns(df: pd.DataFrame) -> pd.DataFrame:
    """異常日次リターンを検出"""
    df = df.sort_values(["ticker", "date"]).copy()
    df["prev_close"] = df.groupby("ticker")["Close"].shift(1)
    df["daily_ret"] = np.where(
        df["prev_close"] > 0,
        (df["Close"] / df["prev_close"] - 1) * 100,
        np.nan,
    )
    extreme = df[df["daily_ret"].abs() > EXTREME_RET_THRESHOLD].copy()
    return extreme


def check_jquants_splits(
    tickers: list[str], fetcher: JQuantsFetcher
) -> dict[str, dict]:
    """JQuantsでAdjustmentFactorを確認し、分割銘柄を特定"""
    splits = {}
    import time as _time
    for tk in tickers:
        # yfinance ticker -> JQuants code (4桁 + "0")
        code = tk.replace(".T", "") + "0"
        try:
            jq = fetcher.get_prices_daily(code, from_date="2024-01-01", to_date="2026-03-31")
            _time.sleep(0.3)  # rate limit
            if jq.empty:
                print(f"  {tk}: JQuants data not found")
                continue

            # AdjustmentFactor != 1 の日があれば分割あり
            if "AdjFactor" in jq.columns:
                adj_factors = jq["AdjFactor"].unique()
                non_one = [f for f in adj_factors if abs(float(f) - 1.0) > 0.001]
                if non_one:
                    # 分割日を特定（AdjFactorが変わるポイント）
                    jq = jq.sort_values("Date")
                    jq["ratio"] = jq["Close"] / jq["AdjustmentClose"]
                    ratio_changes = jq[jq["ratio"].diff().abs() > 0.01]
                    split_date = ratio_changes["Date"].iloc[0] if len(ratio_changes) > 0 else None
                    splits[tk] = {
                        "adj_factors": non_one,
                        "split_date": split_date,
                        "jq_data": jq,
                    }
                    print(f"  {tk}: SPLIT DETECTED! factor={non_one}, date~{split_date}")
                else:
                    print(f"  {tk}: No split (AdjFactor=1.0)")
            elif "AdjustmentClose" in jq.columns:
                # AdjFactorがなくてもAdjustmentCloseで判定
                jq["ratio"] = jq["Close"] / jq["AdjustmentClose"]
                ratios = jq["ratio"].round(4).unique()
                non_one = [r for r in ratios if abs(r - 1.0) > 0.001]
                if non_one:
                    ratio_changes = jq[jq["ratio"].diff().abs() > 0.01]
                    split_date = ratio_changes["Date"].iloc[0] if len(ratio_changes) > 0 else None
                    splits[tk] = {
                        "adj_factors": non_one,
                        "split_date": split_date,
                        "jq_data": jq,
                    }
                    print(f"  {tk}: SPLIT DETECTED! Close/AdjClose ratios={non_one}, date~{split_date}")
                else:
                    print(f"  {tk}: No split (Close==AdjClose)")
        except Exception as e:
            print(f"  {tk}: Error fetching JQuants data: {e}")

    return splits


def needs_fix(prices: pd.DataFrame, ticker: str, jq_data: pd.DataFrame) -> bool:
    """yfinanceがJQuants調整済み価格と一致するかチェック。一致していれば修正不要"""
    jq = jq_data.copy()
    jq["date"] = pd.to_datetime(jq["Date"])
    jq = jq.sort_values("date")

    yf_tk = prices[prices["ticker"] == ticker].copy().sort_values("date")

    # 直近の共通日付で比較
    jq_last = jq.iloc[-1]
    yf_match = yf_tk[yf_tk["date"].dt.strftime("%Y-%m-%d") == jq_last["date"].strftime("%Y-%m-%d")]
    if yf_match.empty:
        return True  # 日付が合わない場合は要確認

    yf_close = float(yf_match.iloc[0]["Close"])
    jq_adj_close = float(jq_last["AdjustmentClose"])

    # 1%以上乖離していたら修正必要
    if jq_adj_close > 0 and abs(yf_close - jq_adj_close) / jq_adj_close > 0.01:
        print(f"    {ticker}: yf={yf_close:.1f} vs jq_adj={jq_adj_close:.1f} → NEEDS FIX")
        return True
    else:
        print(f"    {ticker}: yf={yf_close:.1f} vs jq_adj={jq_adj_close:.1f} → OK (already adjusted)")
        return False


def fix_split_ticker(
    prices: pd.DataFrame, ticker: str, jq_data: pd.DataFrame
) -> tuple[pd.DataFrame, int]:
    """JQuants調整済み価格で分割銘柄を修正"""
    jq = jq_data.copy()
    jq["date"] = pd.to_datetime(jq["Date"])
    jq = jq.sort_values("date")

    mask = prices["ticker"] == ticker
    yf_tk = prices[mask].copy()

    # JQuantsの調整済みOHLCVでマッピング作成
    jq_map = {}
    for _, r in jq.iterrows():
        d = r["date"].strftime("%Y-%m-%d")
        jq_map[d] = {
            "Open": float(r["AdjustmentOpen"]),
            "High": float(r["AdjustmentHigh"]),
            "Low": float(r["AdjustmentLow"]),
            "Close": float(r["AdjustmentClose"]),
            "Volume": float(r.get("AdjustmentVolume", r["Volume"])),
        }

    n_fixed = 0
    for idx in yf_tk.index:
        d = prices.loc[idx, "date"]
        d_str = pd.to_datetime(d).strftime("%Y-%m-%d")
        if d_str in jq_map:
            jq_vals = jq_map[d_str]
            yf_close = float(prices.loc[idx, "Close"])
            jq_close = jq_vals["Close"]
            if jq_close > 0 and abs(yf_close - jq_close) / jq_close > 0.01:
                prices.loc[idx, "Open"] = jq_vals["Open"]
                prices.loc[idx, "High"] = jq_vals["High"]
                prices.loc[idx, "Low"] = jq_vals["Low"]
                prices.loc[idx, "Close"] = jq_vals["Close"]
                prices.loc[idx, "Volume"] = jq_vals["Volume"]
                n_fixed += 1

    return prices, n_fixed


def crossval_sample(
    prices: pd.DataFrame, fetcher: JQuantsFetcher, n_sample: int = 20
) -> dict:
    """ランダム抽出した銘柄でyfinance vs JQuants日次リターンを突合"""
    tickers = prices["ticker"].unique()
    rng = np.random.default_rng(42)
    sample_tickers = rng.choice(tickers, size=min(n_sample, len(tickers)), replace=False)

    results = []
    for tk in sample_tickers:
        code = tk.replace(".T", "") + "0"
        try:
            jq = fetcher.get_prices_daily(code, from_date="2025-01-01", to_date="2026-03-11")
            if jq.empty:
                continue

            # JQuants日次リターン（調整済み）
            jq = jq.sort_values("Date")
            jq["date"] = jq["Date"].dt.strftime("%Y-%m-%d")
            jq["jq_ret"] = jq["AdjustmentClose"].pct_change() * 100

            # yfinance日次リターン
            yf_tk = prices[prices["ticker"] == tk].copy().sort_values("date")
            yf_tk["date_str"] = yf_tk["date"].dt.strftime("%Y-%m-%d")
            yf_tk["yf_ret"] = yf_tk["Close"].pct_change() * 100

            # マージ
            merged = pd.merge(
                jq[["date", "jq_ret"]],
                yf_tk[["date_str", "yf_ret"]].rename(columns={"date_str": "date"}),
                on="date",
                how="inner",
            ).dropna()

            if merged.empty:
                continue

            merged["diff"] = (merged["jq_ret"] - merged["yf_ret"]).abs()
            n_severe = (merged["diff"] >= 1.0).sum()
            max_diff = merged["diff"].max()
            mean_diff = merged["diff"].mean()

            results.append({
                "ticker": tk,
                "n_days": len(merged),
                "mean_diff": round(mean_diff, 4),
                "max_diff": round(max_diff, 2),
                "n_severe": int(n_severe),
            })

        except Exception as e:
            print(f"  {tk}: crossval error: {e}")

    return results


def main() -> int:
    print("=" * 60)
    print("Price Data Cleaning: yfinance × JQuants Cross-validation")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    # 1. Load
    print("\n[1/5] Loading prices...")
    prices = pd.read_parquet(PRICES_PATH)
    prices["date"] = pd.to_datetime(prices["date"])
    n_total = len(prices)
    n_tickers = prices["ticker"].nunique()
    print(f"  {n_total:,} rows, {n_tickers} tickers")
    print(f"  Date range: {prices['date'].min().date()} ~ {prices['date'].max().date()}")

    # 2. 異常リターン検出
    print(f"\n[2/5] Detecting extreme returns (±{EXTREME_RET_THRESHOLD}%)...")
    extreme = detect_extreme_returns(prices)
    print(f"  Found {len(extreme)} extreme returns in {extreme['ticker'].nunique()} tickers")

    # 負の異常リターンのみ（分割候補）
    neg_extreme = extreme[extreme["daily_ret"] < -EXTREME_RET_THRESHOLD]
    neg_tickers = neg_extreme["ticker"].unique().tolist()
    print(f"  Negative extremes (split candidates): {len(neg_extreme)} events in {len(neg_tickers)} tickers")
    for _, r in neg_extreme.sort_values("daily_ret").iterrows():
        print(f"    {r['date'].strftime('%Y-%m-%d')} {r['ticker']:10s} {r['daily_ret']:+.1f}%  ¥{r['prev_close']:.1f} → ¥{r['Close']:.1f}")

    # 3. JQuantsで分割確認
    print(f"\n[3/5] Checking JQuants for split adjustments...")
    fetcher = JQuantsFetcher()
    splits = check_jquants_splits(neg_tickers, fetcher)

    if not splits:
        print("  No splits detected. Data is clean.")
    else:
        print(f"\n  Split-affected tickers: {list(splits.keys())}")

    # 4. 分割修正（yfinanceが未調整の場合のみ）
    print(f"\n[4/5] Fixing split-affected tickers...")
    total_fixed = 0
    import time as _time
    for tk, info in splits.items():
        jq_data = info["jq_data"]

        # まず2Y分のデータで修正必要か判定
        if not needs_fix(prices, tk, jq_data):
            continue

        # 修正必要 → JQuantsの可能な範囲でフルデータ取得（最大7年）
        code = tk.replace(".T", "") + "0"
        print(f"  Fetching extended history for {tk}...")
        jq_frames = []
        # 7年ずつ区切って取得（API上限対策）
        ranges = [
            ("2019-01-01", "2026-03-31"),
            ("2013-01-01", "2018-12-31"),
        ]
        for from_d, to_d in ranges:
            try:
                chunk = fetcher.get_prices_daily(code, from_date=from_d, to_date=to_d)
                if not chunk.empty:
                    jq_frames.append(chunk)
                _time.sleep(0.5)  # rate limit
            except Exception as e:
                print(f"    chunk {from_d}~{to_d}: {e}")
                break

        if not jq_frames:
            print(f"    WARN: No JQuants data for {tk}")
            continue
        jq_full = pd.concat(jq_frames, ignore_index=True)

        if jq_full.empty:
            print(f"    WARN: No data for {tk}")
            continue

        prices, n_fixed = fix_split_ticker(prices, tk, jq_full)
        total_fixed += n_fixed
        print(f"    Fixed {n_fixed} rows for {tk}")

    # 修正後の検証
    if total_fixed > 0:
        print(f"\n  Total rows fixed: {total_fixed}")
        extreme_after = detect_extreme_returns(prices)
        neg_after = extreme_after[extreme_after["daily_ret"] < -EXTREME_RET_THRESHOLD]
        print(f"  Negative extremes after fix: {len(neg_after)}")
        for _, r in neg_after.sort_values("daily_ret").iterrows():
            print(f"    {r['date'].strftime('%Y-%m-%d')} {r['ticker']:10s} {r['daily_ret']:+.1f}%  ¥{r['prev_close']:.1f} → ¥{r['Close']:.1f}")

    # 5. ランダム突合（品質確認）
    print(f"\n[5/5] Random cross-validation (20 tickers)...")
    crossval = crossval_sample(prices, fetcher, n_sample=20)
    if crossval:
        print(f"  {'Ticker':10s} {'Days':>5s} {'Mean|Δ|':>8s} {'Max|Δ|':>8s} {'≥1%':>4s}")
        all_clean = True
        for r in sorted(crossval, key=lambda x: -x["max_diff"]):
            flag = " ⚠" if r["n_severe"] > 0 else ""
            if r["n_severe"] > 0:
                all_clean = False
            print(f"  {r['ticker']:10s} {r['n_days']:5d} {r['mean_diff']:8.4f} {r['max_diff']:8.2f} {r['n_severe']:4d}{flag}")
        if all_clean:
            print("\n  ALL CLEAN: No severe discrepancies found in random sample.")

    # 保存
    print(f"\n{'='*60}")
    print(f"Saving cleaned prices to {PRICES_PATH}...")
    prices.to_parquet(PRICES_PATH, index=False)
    print(f"  Done. {len(prices):,} rows, {prices['ticker'].nunique()} tickers")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
