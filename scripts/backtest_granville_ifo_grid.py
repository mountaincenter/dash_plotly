#!/usr/bin/env python3
"""
グランビル buyシグナル × IFO SL/TPグリッドサーチ
================================================
SL: 1.5%, 2.0%, 2.5%, 3.0%, 3.5%, 4.0%, 5.0%
TP: 1.5%, 2.0%, 2.5%, 3.0%, 3.5%, 4.0%, 5.0%
保有日数: 3, 5, 7, 10
+ IFOなし（引け決済のみ）

フィルター: uptrend + CI拡大
悪セクター除外: 医薬品, 輸送用機器, 小売業, その他製品, 陸運業, サービス業
"""
from __future__ import annotations

import sys
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO_DIR = ROOT / "improvement" / "data" / "macro"

BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]

SL_RANGE = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
TP_RANGE = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
HOLD_RANGE = [3, 5, 7, 10]


def load_data():
    m = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    idx = pd.read_parquet(ROOT / "data" / "parquet" / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]

    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"})
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)

    daily = pd.DataFrame({"date": ps["date"].drop_duplicates().sort_values()})
    daily = daily.merge(ci, on="date", how="left").sort_values("date").ffill()

    # 指標追加
    g = ps.groupby("ticker")
    ps["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["sma5_above_sma20"] = ps["sma5"] > ps["sma20"]
    ps["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)
    ps = ps.dropna(subset=["sma20"])

    # マージ
    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0
    ps = ps.merge(m[["ticker", "sectors"]], on="ticker", how="left")

    return ps


def detect_signals(df):
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]
    above = df["Close"] > df["sma20"]

    df["sig_A"] = (dev.between(-8, -3)) & (df["Close"] > df["prev_close"])
    df["sig_B"] = (
        sma_up & above &
        (dev.between(0, 2)) & (df["prev_dev"] <= 0.5) &
        (df["Close"] > df["prev_close"])
    )
    df["sig_C"] = (
        df["sma5_above_sma20"] &
        ~df["prev_sma5_above"].fillna(False).astype(bool)
    )
    df["sig_D"] = (dev <= -5) & (df["Close"] > df["prev_close"])
    return df


def simulate_ifo_batch(prices_df, signal_rows, sl_pct, tp_pct, hold_days):
    """高速IFOシミュレーション"""
    results = []
    for ticker in signal_rows["ticker"].unique():
        tk = prices_df[prices_df["ticker"] == ticker].sort_values("date")
        dates = tk["date"].values
        opens = tk["Open"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        closes = tk["Close"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        sig_dates = signal_rows[signal_rows["ticker"] == ticker]["date"].values
        for sd in sig_dates:
            if sd not in date_idx:
                continue
            idx = date_idx[sd]
            if idx + 1 >= len(dates):
                continue

            entry_idx = idx + 1
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            sl_price = entry_price * (1 - sl_pct / 100)
            tp_price = entry_price * (1 + tp_pct / 100)

            exit_type = "expire"
            exit_price = closes[min(entry_idx + hold_days - 1, len(dates) - 1)]
            for d in range(hold_days):
                ci = entry_idx + d
                if ci >= len(dates):
                    exit_price = closes[len(dates) - 1]
                    break
                if lows[ci] <= sl_price:
                    exit_type = "SL"
                    exit_price = sl_price
                    break
                if highs[ci] >= tp_price:
                    exit_type = "TP"
                    exit_price = tp_price
                    break

            ret_pct = (exit_price / entry_price - 1) * 100
            results.append((exit_type, ret_pct))

    if not results:
        return None
    types, rets = zip(*results)
    rets = np.array(rets)
    n = len(rets)
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    pf = round(wins.sum() / abs(losses.sum()), 2) if len(losses) > 0 and losses.sum() != 0 else 999
    tp_pct_hit = sum(1 for t in types if t == "TP") / n * 100
    sl_pct_hit = sum(1 for t in types if t == "SL") / n * 100
    return {
        "n": n,
        "mean": round(rets.mean(), 3),
        "win%": round((rets > 0).mean() * 100, 1),
        "pf": pf,
        "tp_hit%": round(tp_pct_hit, 1),
        "sl_hit%": round(sl_pct_hit, 1),
    }


def raw_hold_stats(prices_df, signal_rows, hold_days):
    """IFOなし（引け決済のみ）"""
    results = []
    for ticker in signal_rows["ticker"].unique():
        tk = prices_df[prices_df["ticker"] == ticker].sort_values("date")
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        sig_dates = signal_rows[signal_rows["ticker"] == ticker]["date"].values
        for sd in sig_dates:
            if sd not in date_idx:
                continue
            idx = date_idx[sd]
            if idx + 1 >= len(dates):
                continue
            entry_idx = idx + 1
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue
            exit_idx = min(entry_idx + hold_days - 1, len(dates) - 1)
            exit_price = closes[exit_idx]
            ret_pct = (exit_price / entry_price - 1) * 100
            results.append(ret_pct)

    if not results:
        return None
    rets = np.array(results)
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    pf = round(wins.sum() / abs(losses.sum()), 2) if len(losses) > 0 and losses.sum() != 0 else 999
    return {
        "n": len(rets),
        "mean": round(rets.mean(), 3),
        "win%": round((rets > 0).mean() * 100, 1),
        "pf": pf,
    }


def main():
    print("=" * 72)
    print("グランビル IFO SL/TP グリッドサーチ")
    print("フィルター: uptrend + CI拡大 / 悪セクター除外")
    print("=" * 72)

    prices = load_data()
    prices = detect_signals(prices)

    # フィルター適用
    filtered = prices[
        (prices["market_uptrend"] == True) &
        (prices["macro_ci_expand"] == True) &
        (~prices["sectors"].isin(BAD_SECTORS))
    ].copy()
    print(f"  フィルター後レコード: {len(filtered):,}")

    signal_names = {
        "sig_A": "A_dip_buy",
        "sig_B": "B_sma_support",
        "sig_C": "C_mini_gc",
    }

    # A, B, C のみ（Dは不採用）
    for col, name in signal_names.items():
        sig_rows = filtered[filtered[col]].copy()
        cnt = len(sig_rows)
        print(f"\n{'='*72}")
        print(f"{name}（{cnt:,}件）")
        print(f"{'='*72}")

        # IFOなし（引け決済のみ）
        print(f"\n  --- IFOなし（引け決済） ---")
        print(f"  {'保有日':>5s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s}")
        print(f"  {'-'*35}")
        for hd in HOLD_RANGE:
            st = raw_hold_stats(prices, sig_rows, hd)
            if st:
                print(f"  {hd:>5d}日 {st['n']:>6,d} {st['mean']:>+7.3f} {st['win%']:>5.1f}% {st['pf']:>5.02f}")

        # IFOグリッド（保有5日と7日）
        for hd in [5, 7]:
            print(f"\n  --- IFO グリッド（保有{hd}日） ---")
            print(f"  {'SL':>5s} {'TP':>5s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s} "
                  f"{'TP%':>5s} {'SL%':>5s}")
            print(f"  {'-'*52}")

            best_pf = 0
            best_combo = ""
            for sl, tp in product(SL_RANGE, TP_RANGE):
                if tp <= sl:
                    continue  # TP > SL のみ
                st = simulate_ifo_batch(prices, sig_rows, sl, tp, hd)
                if st is None or st["n"] < 50:
                    continue
                marker = ""
                if st["pf"] > best_pf:
                    best_pf = st["pf"]
                    best_combo = f"SL{sl}/TP{tp}/{hd}d"
                if st["pf"] >= 1.3:
                    marker = " ★"
                elif st["pf"] >= 1.2:
                    marker = " ◎"
                elif st["pf"] >= 1.1:
                    marker = " ○"
                print(
                    f"  {sl:>4.1f}% {tp:>4.1f}% {st['n']:>6,d} {st['mean']:>+7.3f} "
                    f"{st['win%']:>5.1f}% {st['pf']:>5.02f} "
                    f"{st['tp_hit%']:>4.1f}% {st['sl_hit%']:>4.1f}%{marker}"
                )
            if best_combo:
                print(f"  ベスト: {best_combo} PF={best_pf}")

    print(f"\n{'='*72}")
    print("完了")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
