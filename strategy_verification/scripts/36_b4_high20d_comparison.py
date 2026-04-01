#!/usr/bin/env python3
"""
36_b4_high20d_comparison.py
Codex指摘1: 20日高値Exit判定窓の3パターン比較

A: 現状（エントリー以降のrolling 20日高値）
B: エントリー前20営業日を含む真の20日高値
C: エントリー前5営業日を含む

仮説: B案はPF大幅低下（暴落前高値が基準→MH15到達率激増）、A案が最適
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"
MAX_HOLD = 15


def load_prices(lookback_years: int = 2) -> pd.DataFrame:
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    cutoff = ps["date"].max() - pd.DateOffset(years=lookback_years)
    buffer_cutoff = cutoff - pd.Timedelta(days=120)  # SMA+pre-entry用に余裕
    ps = ps[ps["date"] >= buffer_cutoff].copy()
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["prev_close"] = g["Close"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    # 急騰フィルター
    ps["sma60"] = g["Close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    ps["sma100"] = g["Close"].transform(lambda x: x.rolling(100, min_periods=100).mean())
    ps["dev60"] = (ps["Close"] - ps["sma60"]) / ps["sma60"] * 100
    ps["dev100"] = (ps["Close"] - ps["sma100"]) / ps["sma100"] * 100
    ps["max_up20"] = g["dev_from_sma20"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up60"] = g["dev60"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up100"] = g["dev100"].transform(lambda x: x.rolling(60, min_periods=1).max())

    ps = ps.dropna(subset=["sma20"])
    ps = ps[ps["date"] >= cutoff].copy()
    return ps


def run_b4_backtest(ps: pd.DataFrame, mode: str, pre_entry_days: int = 0) -> pd.DataFrame:
    """
    mode:
      "A" = 現状（エントリー以降のみ）
      "B" = エントリー前20日含む
      "C" = エントリー前5日含む
    """
    # B4シグナル検出
    dev = ps["dev_from_sma20"]
    b4_mask = (dev < -15) & ps["up_day"]
    surge = (ps["max_up20"] >= 15) | (ps["max_up60"] >= 20) | (ps["max_up100"] >= 30)
    b4_mask = b4_mask & ~surge
    signals = ps[b4_mask].copy()

    # ticker別データ（エントリー前も含めるためフルデータ必要）
    all_data = pd.read_parquet(PRICES_PATH)
    all_data["date"] = pd.to_datetime(all_data["date"])
    cutoff_buffer = ps["date"].min() - pd.Timedelta(days=60)
    all_data = all_data[all_data["date"] >= cutoff_buffer]
    all_data = all_data.sort_values(["ticker", "date"]).reset_index(drop=True)

    ticker_full = {}
    for tk, gdf in all_data.groupby("ticker"):
        ticker_full[tk] = gdf.sort_values("date").reset_index(drop=True)

    trades = []
    for _, sig in signals.iterrows():
        tk = sig["ticker"]
        if tk not in ticker_full:
            continue

        tk_df = ticker_full[tk]
        sig_date = sig["date"]

        # シグナル日のインデックス
        sig_indices = tk_df.index[tk_df["date"] == sig_date]
        if len(sig_indices) == 0:
            continue
        sig_iloc = tk_df.index.get_loc(sig_indices[0])

        # エントリー: シグナル翌営業日
        if sig_iloc + 1 >= len(tk_df):
            continue
        entry_row = tk_df.iloc[sig_iloc + 1]
        ep = float(entry_row["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        entry_iloc = sig_iloc + 1

        # Exit判定
        exit_price = 0.0
        exit_date = None
        exit_type = ""
        exit_day = 0
        mae_pct = 0.0

        hold_end = min(entry_iloc + MAX_HOLD, len(tk_df) - 1)
        hold_limit = hold_end - entry_iloc

        for i in range(min(hold_limit, MAX_HOLD)):
            cur_iloc = entry_iloc + i
            if cur_iloc >= len(tk_df):
                break
            row = tk_df.iloc[cur_iloc]
            cur_high = float(row["High"])
            cur_low = float(row["Low"])

            # MAE
            day_low_pct = (cur_low / ep - 1) * 100
            mae_pct = min(mae_pct, day_low_pct)

            # 20日高値判定（モード別）
            if i > 0:  # 初日スキップは全モード共通
                if mode == "A":
                    # 現状: エントリー以降のみ
                    start = max(entry_iloc, cur_iloc - 19)
                    window = tk_df.iloc[start:cur_iloc + 1]["High"]
                elif mode == "B":
                    # エントリー前20日含む
                    start = max(0, cur_iloc - 19)
                    window = tk_df.iloc[start:cur_iloc + 1]["High"]
                elif mode == "C":
                    # エントリー前5日含む
                    start = max(entry_iloc - 5, cur_iloc - 19)
                    start = max(0, start)
                    window = tk_df.iloc[start:cur_iloc + 1]["High"]

                high_20d = float(window.max())
                if cur_high >= high_20d:
                    # 翌営業日寄付で決済
                    next_iloc = cur_iloc + 1
                    if next_iloc < len(tk_df):
                        exit_price = float(tk_df.iloc[next_iloc]["Open"])
                        exit_date = tk_df.iloc[next_iloc]["date"]
                        exit_type = "high_update"
                        exit_day = i + 2
                        break

            # MAX_HOLD
            if i >= min(hold_limit, MAX_HOLD) - 1:
                next_iloc = cur_iloc + 1
                if next_iloc < len(tk_df):
                    exit_price = float(tk_df.iloc[next_iloc]["Open"])
                    exit_date = tk_df.iloc[next_iloc]["date"]
                    exit_type = "max_hold"
                    exit_day = i + 2
                    break

        if exit_date is None:
            last_iloc = min(entry_iloc + MAX_HOLD, len(tk_df) - 1)
            exit_price = float(tk_df.iloc[last_iloc]["Close"])
            exit_date = tk_df.iloc[last_iloc]["date"]
            exit_type = "end_of_data"
            exit_day = last_iloc - entry_iloc + 1

        if exit_price <= 0:
            continue

        ret_pct = (exit_price / ep - 1) * 100
        trades.append({
            "signal_date": sig_date,
            "ticker": tk,
            "ret_pct": round(ret_pct, 3),
            "pnl_yen": int((exit_price - ep) * 100),
            "exit_type": exit_type,
            "exit_day": exit_day,
            "mae_pct": round(mae_pct, 2),
            "dev_from_sma20": round(float(sig["dev_from_sma20"]), 2),
        })

    return pd.DataFrame(trades)


def summarize(df: pd.DataFrame, label: str) -> dict:
    n = len(df)
    if n == 0:
        return {"label": label, "n": 0}
    wins = (df["ret_pct"] > 0).sum()
    wr = wins / n * 100
    gross_win = df[df["ret_pct"] > 0]["ret_pct"].sum()
    gross_loss = abs(df[df["ret_pct"] <= 0]["ret_pct"].sum())
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    avg_hold = df["exit_day"].mean()
    h_exits = (df["exit_type"] == "high_update").sum()
    mh_exits = (df["exit_type"] == "max_hold").sum()
    return {
        "label": label,
        "n": n,
        "wr": round(wr, 1),
        "pf": round(pf, 2),
        "avg_ret": round(df["ret_pct"].mean(), 2),
        "pnl": df["pnl_yen"].sum(),
        "avg_hold": round(avg_hold, 1),
        "high_exit%": round(h_exits / n * 100, 1),
        "mh_exit%": round(mh_exits / n * 100, 1),
        "mae_median": round(df["mae_pct"].median(), 1),
    }


def main():
    print("=" * 70)
    print("B4 20日高値Exit判定: 3パターン比較")
    print("=" * 70)

    print("\nLoading prices...")
    ps = load_prices(lookback_years=2)
    print(f"  {ps['ticker'].nunique()} tickers, {ps['date'].min().date()} ~ {ps['date'].max().date()}")

    results = []
    for mode, label in [
        ("A", "A: エントリー以降のみ（現状）"),
        ("B", "B: エントリー前20日含む"),
        ("C", "C: エントリー前5日含む"),
    ]:
        print(f"\n--- {label} ---")
        trades = run_b4_backtest(ps, mode)
        s = summarize(trades, label)
        results.append(s)
        print(f"  件数={s['n']} WR={s.get('wr',0)}% PF={s.get('pf',0)} "
              f"avg={s.get('avg_ret',0)}% PnL=¥{s.get('pnl',0):,} "
              f"avg_hold={s.get('avg_hold',0)}日 "
              f"high_exit={s.get('high_exit%',0)}% mh_exit={s.get('mh_exit%',0)}%")

    print("\n" + "=" * 70)
    print("比較サマリー")
    print("=" * 70)
    print(f"{'パターン':<30} {'件数':>5} {'WR%':>6} {'PF':>6} {'avg%':>7} {'PnL':>12} {'保有日':>6} {'高値%':>6} {'MH%':>5}")
    for s in results:
        if s["n"] == 0:
            continue
        print(f"{s['label']:<30} {s['n']:>5} {s['wr']:>6} {s['pf']:>6} {s['avg_ret']:>7} {s['pnl']:>12,} {s['avg_hold']:>6} {s.get('high_exit%',0):>6} {s.get('mh_exit%',0):>5}")


if __name__ == "__main__":
    main()
