#!/usr/bin/env python3
"""
B4∨sigA 乖離率閾値の最適化
============================
現在 -3% で固定している上限閾値を変数化し、最適値を探索する。
SMA20は固定。

テスト閾値: -1%, -1.5%, -2%, -2.5%, -3%, -3.5%, -4%, -5%, -6%, -7%, -8%
Exit: (1) ベースライン（Close >= SMA20）  (2) SL-3% trail50%
レジーム: Uptrend / Downtrend / 全体
期間: 全期間 / 2015-2026 / 直近2年
同一銘柄重複除去済み
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = ROOT / "data" / "parquet"
MAX_HOLD = 60

THRESHOLDS = [-1, -1.5, -2, -2.5, -3, -3.5, -4, -5, -6, -7, -8]


def load_data() -> pd.DataFrame:
    t0 = time.time()
    m = pd.read_parquet(PARQUET_DIR / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)

    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(m["ticker"].tolist())].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps = ps.dropna(subset=["sma20", "prev_sma20"])

    ps["up_day"] = ps["Close"] > ps["prev_close"]

    # N225 uptrend
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225"})
    nk["nk_sma20"] = nk["nk225"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225"] > nk["nk_sma20"]
    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")
    ps = ps.merge(m[["ticker", "stock_name"]], on="ticker", how="left")

    print(f"Data loaded in {time.time()-t0:.1f}s")
    return ps


def backtest_threshold(
    df: pd.DataFrame,
    threshold: float,
    sl_pct: float = 0,
    trail_pct: float = 0,
) -> pd.DataFrame:
    """
    乖離率 < threshold & up_day でシグナル生成 → バックテスト
    Exit: Close >= SMA20 (mean reversion) + optional SL/trail
    同一銘柄重複除去（保有中は新規エントリーしない）
    """
    results = []

    for ticker in df["ticker"].unique():
        tk = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        sma20s = tk["sma20"].values
        devs = tk["dev"].values
        up_days = tk["up_day"].values
        uptrends = tk["market_uptrend"].values
        n = len(tk)

        # 同一銘柄重複除去: 保有中フラグ
        in_position = False
        position_exit_idx = -1

        for i in range(n):
            # 保有終了チェック
            if in_position and i > position_exit_idx:
                in_position = False

            # シグナル: dev < threshold & up_day
            if not (devs[i] < threshold and up_days[i]):
                continue

            # 重複除去
            if in_position:
                continue

            # エントリー: 翌営業日Open
            entry_idx = i + 1
            if entry_idx >= n:
                continue
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            # SL価格
            sl_price = entry_price * (1 - sl_pct / 100) if sl_pct > 0 else 0

            # イグジット探索
            exit_idx = None
            exit_type = "expire"
            max_price = entry_price  # trailing用

            for j in range(entry_idx, min(entry_idx + MAX_HOLD, n)):
                c = closes[j]
                s = sma20s[j]
                h = highs[j]
                lo = lows[j]

                if np.isnan(c) or np.isnan(s):
                    continue

                # SL判定（日中Low）— エントリー当日もチェック
                if sl_pct > 0 and lo <= sl_price:
                    exit_idx = j
                    exit_type = "SL"
                    break

                # エントリー当日はSLのみ。trail/signal exitは翌日以降
                if j == entry_idx:
                    continue

                # trailing SL更新（翌日以降）
                if trail_pct > 0 and h > max_price:
                    max_price = h
                    profit = max_price - entry_price
                    if profit > 0:
                        trail_sl = entry_price + profit * trail_pct
                        if trail_sl > sl_price:
                            sl_price = trail_sl

                # trailing SL判定
                if trail_pct > 0 and sl_price > 0 and lo <= sl_price:
                    exit_idx = j
                    exit_type = "trail"
                    break

                # シグナルexit: Close >= SMA20（平均回帰完了）
                if c >= s:
                    # イグジットシグナル翌営業日Open
                    if j + 1 < n:
                        exit_idx = j + 1
                        exit_type = "signal"
                    else:
                        exit_idx = j
                        exit_type = "signal"
                    break

            if exit_idx is None:
                exit_idx = min(entry_idx + MAX_HOLD, n - 1)
                exit_type = "expire"

            # SL/trailは当日の損切り価格で約定
            if exit_type in ("SL", "trail"):
                exit_price = sl_price
            else:
                exit_price = opens[exit_idx] if not np.isnan(opens[exit_idx]) else closes[exit_idx]

            if np.isnan(exit_price) or exit_price <= 0:
                continue

            ret_pct = (exit_price / entry_price - 1) * 100
            pnl = int(round(entry_price * 100 * ret_pct / 100))
            hold_days = int(exit_idx - entry_idx)

            # 保有中フラグ設定
            in_position = True
            position_exit_idx = exit_idx

            ut = uptrends[i]
            regime = "uptrend" if ut else "downtrend" if ut is not None and not ut else "unknown"

            results.append({
                "threshold": threshold,
                "ticker": ticker,
                "signal_date": dates[i],
                "entry_date": dates[entry_idx],
                "exit_date": dates[exit_idx],
                "entry_price": round(float(entry_price), 1),
                "exit_price": round(float(exit_price), 1),
                "ret_pct": round(ret_pct, 3),
                "pnl": pnl,
                "hold_days": hold_days,
                "exit_type": exit_type,
                "regime": regime,
                "dev_at_signal": round(float(devs[i]), 2),
            })

    return pd.DataFrame(results)


def calc_stats(trades: pd.DataFrame) -> dict:
    if len(trades) == 0:
        return {"n": 0, "wr": 0, "avg": 0, "pf": 0, "pnl": 0, "hold": 0}
    n = len(trades)
    wins = trades[trades["ret_pct"] > 0]
    losses = trades[trades["ret_pct"] <= 0]
    ws = wins["ret_pct"].sum()
    ls = abs(losses["ret_pct"].sum())
    return {
        "n": n,
        "wr": round(wins.shape[0] / n * 100, 1),
        "avg": round(trades["ret_pct"].mean(), 2),
        "pf": round(ws / ls, 2) if ls > 0 else 999,
        "pnl": round(trades["pnl"].sum() / 10000, 1),
        "hold": round(trades["hold_days"].mean(), 1),
    }


def print_table(label: str, rows: list[dict]):
    print(f"\n{'='*100}")
    print(f"■ {label}")
    print(f"{'='*100}")
    print(f"  {'閾値':>6s}  {'件数':>7s}  {'勝率':>6s}  {'平均%':>7s}  {'PF':>6s}  {'PnL(万)':>9s}  {'保有':>5s}")
    print(f"  {'-'*70}")
    best_pnl = max(r["pnl"] for r in rows) if rows else 0
    best_pf = max(r["pf"] for r in rows if r["n"] >= 100) if rows else 0
    for r in rows:
        marker = ""
        if r["n"] >= 100:
            if r["pnl"] == best_pnl:
                marker += " ◆PnL"
            if r["pf"] == best_pf:
                marker += " ◆PF"
        print(
            f"  {r['threshold']:>5.1f}%  {r['n']:>7,d}  {r['wr']:>5.1f}%  "
            f"{r['avg']:>+6.2f}%  {r['pf']:>5.2f}  {r['pnl']:>+8.1f}  "
            f"{r['hold']:>4.1f}d{marker}"
        )


def main():
    print("=" * 100)
    print("B4∨sigA 乖離率閾値の最適化")
    print("  SMA20固定、乖離率上限のみ変数化")
    print("  同一銘柄重複除去、ポジション数無制限")
    print("=" * 100)

    ps = load_data()

    # === (A) ベースライン（シグナルexitのみ） ===
    print("\n\n" + "#" * 100)
    print("# (A) ベースライン: シグナルexit のみ（Close >= SMA20 で翌日Open決済）")
    print("#" * 100)

    all_baseline = []
    for th in THRESHOLDS:
        trades = backtest_threshold(ps, threshold=th, sl_pct=0, trail_pct=0)
        if len(trades) == 0:
            continue
        all_baseline.append(trades)

        # 期間フィルター
        t_2015 = trades[trades["signal_date"] >= "2015-01-01"]
        t_2y = trades[trades["signal_date"] >= "2024-03-01"]

        for period_label, t in [("全期間", trades), ("2015-2026", t_2015), ("直近2年", t_2y)]:
            for regime_label, regime_filter in [("全体", None), ("Uptrend", "uptrend"), ("Downtrend", "downtrend")]:
                sub = t[t["regime"] == regime_filter] if regime_filter else t
                s = calc_stats(sub)
                s["threshold"] = th
                s["period"] = period_label
                s["regime"] = regime_label
                # store for later
                if not hasattr(main, '_baseline_results'):
                    main._baseline_results = []
                main._baseline_results.append(s)

    # 表示: 期間×レジーム の9マトリクス
    for period in ["全期間", "2015-2026", "直近2年"]:
        for regime in ["全体", "Uptrend", "Downtrend"]:
            rows = [r for r in main._baseline_results if r["period"] == period and r["regime"] == regime]
            print_table(f"ベースライン / {period} / {regime}", rows)

    # === (B) SL-3% + trail50% ===
    print("\n\n" + "#" * 100)
    print("# (B) SL-3% + trail50%")
    print("#" * 100)

    main._sl_results = []
    for th in THRESHOLDS:
        trades = backtest_threshold(ps, threshold=th, sl_pct=3, trail_pct=0.5)
        if len(trades) == 0:
            continue

        t_2015 = trades[trades["signal_date"] >= "2015-01-01"]
        t_2y = trades[trades["signal_date"] >= "2024-03-01"]

        for period_label, t in [("全期間", trades), ("2015-2026", t_2015), ("直近2年", t_2y)]:
            for regime_label, regime_filter in [("全体", None), ("Uptrend", "uptrend"), ("Downtrend", "downtrend")]:
                sub = t[t["regime"] == regime_filter] if regime_filter else t
                s = calc_stats(sub)
                s["threshold"] = th
                s["period"] = period_label
                s["regime"] = regime_label
                main._sl_results.append(s)

    for period in ["全期間", "2015-2026", "直近2年"]:
        for regime in ["全体", "Uptrend", "Downtrend"]:
            rows = [r for r in main._sl_results if r["period"] == period and r["regime"] == regime]
            print_table(f"SL-3% trail50% / {period} / {regime}", rows)

    # === (C) ベスト閾値サマリー ===
    print("\n\n" + "#" * 100)
    print("# サマリー: 各セグメントのベスト閾値")
    print("#" * 100)

    print(f"\n  {'Exit':>15s}  {'期間':>10s}  {'レジーム':>10s}  {'PnL最大':>8s}  {'PF最大':>8s}")
    print(f"  {'-'*65}")

    for exit_label, results in [("ベースライン", main._baseline_results), ("SL3%trail50%", main._sl_results)]:
        for period in ["全期間", "2015-2026", "直近2年"]:
            for regime in ["全体", "Uptrend", "Downtrend"]:
                rows = [r for r in results if r["period"] == period and r["regime"] == regime and r["n"] >= 100]
                if not rows:
                    continue
                best_pnl_r = max(rows, key=lambda x: x["pnl"])
                best_pf_r = max(rows, key=lambda x: x["pf"])
                print(
                    f"  {exit_label:>15s}  {period:>10s}  {regime:>10s}  "
                    f"{best_pnl_r['threshold']:>+5.1f}%({best_pnl_r['pnl']:>+.0f}万)  "
                    f"{best_pf_r['threshold']:>+5.1f}%(PF{best_pf_r['pf']:.2f})"
                )

    print(f"\n  Completed in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    t0 = time.time()
    main()
