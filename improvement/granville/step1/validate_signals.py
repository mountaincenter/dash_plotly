#!/usr/bin/env python3
"""
Step 1: グランビル8法則 エッジの存在確認
=========================================
全銘柄・制約なし・指定SLで B1-B4 + S1-S4 を検証。
N225 vs SMA20 によるレジーム(Uptrend/Downtrend)情報を付与。

Usage:
    python3 validate_signals.py              # SLなし
    python3 validate_signals.py --sl 3       # SL -3%
    python3 validate_signals.py --sl 5       # SL -5%

Output:
    step1/trades_no_sl.parquet
    step1/trades_sl3.parquet
    step1/trades_sl5.parquet
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # dash_plotly/improvement
PARQUET_DIR = ROOT.parent / "data" / "parquet"
OUT_DIR = Path(__file__).resolve().parent  # step1/

MAX_HOLD = 60  # 安全弁（グランビル理論に規定なし、Step 2.5で検証予定）


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """株価 + メタ + N225レジーム情報を読み込み"""
    print("[1/4] Loading data...")
    t0 = time.time()

    m = pd.read_parquet(PARQUET_DIR / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)

    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    # 価格フィルターなし（全銘柄）
    ps = p[p["ticker"].isin(m["ticker"].tolist())].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # SMA20 テクニカル指標
    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev"].shift(1)
    ps = ps.dropna(subset=["sma20", "prev_sma20"])

    # 派生フラグ
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["sma20_down"] = ps["sma20_slope"] < 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_above"] = ps["prev_close"] > ps["prev_sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]
    ps["down_day"] = ps["Close"] < ps["prev_close"]

    # N225 レジーム (SMA20)
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225"})
    nk["nk_sma20"] = nk["nk225"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225"] > nk["nk_sma20"]
    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")

    # メタ結合
    ps = ps.merge(m[["ticker", "sectors", "stock_name"]], on="ticker", how="left")

    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")
    print(f"  {ps['date'].min().date()} ~ {ps['date'].max().date()}")
    print(f"  Done in {time.time()-t0:.1f}s")
    return ps, m


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """グランビル8法則のシグナルを検出"""
    print("[2/4] Detecting signals...")
    t0 = time.time()
    df = df.copy()

    dev = df["dev"]
    sma_up = df["sma20_up"]
    sma_down = df["sma20_down"]
    above = df["above"]
    below = df["below"]
    prev_above = df["prev_above"]
    prev_below = df["prev_below"]
    up_day = df["up_day"]
    down_day = df["down_day"]

    # LONG
    df["B1"] = prev_below & above & sma_up
    df["B2"] = sma_up & dev.between(-5, 0) & up_day & below
    df["B3"] = sma_up & above & dev.between(0, 3) & (df["prev_dev"] > dev) & up_day
    df["B4"] = (dev < -8) & up_day

    # SHORT
    df["S1"] = prev_above & below & sma_down
    df["S2"] = sma_down & dev.between(0, 5) & down_day & above
    df["S3"] = sma_down & below & dev.between(-3, 0) & (df["prev_dev"] < dev) & down_day
    df["S4"] = (dev > 8) & down_day

    rules = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]
    for r in rules:
        print(f"  {r}: {df[r].sum():>7,} signals")
    print(f"  Done in {time.time()-t0:.1f}s")
    return df


def run_backtest(df: pd.DataFrame, sl_pct: float | None) -> pd.DataFrame:
    """全シグナルのバックテスト"""
    sl_label = f"SL-{sl_pct}%" if sl_pct else "SLなし"
    print(f"[3/4] Running backtest ({sl_label})...")
    t0 = time.time()

    rules = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]
    long_rules = {"B1", "B2", "B3", "B4"}
    contrarian = {"B4", "S4"}  # 逆張り: SMA到達でexit

    results: list[dict] = []

    for ticker in df["ticker"].unique():
        tk = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        sma20s = tk["sma20"].values
        uptrends = tk["market_uptrend"].values
        n = len(tk)

        for rule in rules:
            is_long = rule in long_rules
            is_contra = rule in contrarian
            sig_mask = tk[rule].values

            for i in range(n):
                if not sig_mask[i]:
                    continue

                entry_idx = i + 1
                if entry_idx >= n:
                    continue
                entry_price = opens[entry_idx]
                if np.isnan(entry_price) or entry_price <= 0:
                    continue

                entry_date = dates[entry_idx]
                sig_date = dates[i]

                # レジーム
                mkt_up = bool(uptrends[i]) if not pd.isna(uptrends[i]) else False

                # SL価格
                if sl_pct is not None:
                    if is_long:
                        sl_price = entry_price * (1 - sl_pct / 100)
                    else:
                        sl_price = entry_price * (1 + sl_pct / 100)
                else:
                    sl_price = None

                # Exit探索
                exit_signal_idx = None
                exit_by_sl = False

                for j in range(entry_idx, min(entry_idx + MAX_HOLD, n)):
                    # SLチェック（当日の高安で判定）
                    if sl_price is not None:
                        if is_long and lows[j] <= sl_price:
                            exit_signal_idx = j
                            exit_by_sl = True
                            break
                        elif not is_long and highs[j] >= sl_price:
                            exit_signal_idx = j
                            exit_by_sl = True
                            break

                    # シグナルExitチェック
                    c = closes[j]
                    s = sma20s[j]
                    if np.isnan(c) or np.isnan(s):
                        continue

                    if is_long:
                        if is_contra:
                            if c >= s:
                                exit_signal_idx = j
                                break
                        else:
                            if j > entry_idx and c < s:
                                exit_signal_idx = j
                                break
                    else:
                        if is_contra:
                            if c <= s:
                                exit_signal_idx = j
                                break
                        else:
                            if j > entry_idx and c > s:
                                exit_signal_idx = j
                                break

                # Exit価格決定
                if exit_by_sl:
                    exit_price = sl_price
                    exit_date = dates[exit_signal_idx]
                    exit_type = "sl"
                elif exit_signal_idx is not None:
                    exit_exec_idx = exit_signal_idx + 1
                    if exit_exec_idx >= n:
                        exit_exec_idx = n - 1
                    exit_price = opens[exit_exec_idx]
                    if np.isnan(exit_price) or exit_price <= 0:
                        exit_price = closes[min(exit_exec_idx, n - 1)]
                        if np.isnan(exit_price):
                            continue
                    exit_date = dates[exit_exec_idx]
                    exit_type = "signal"
                else:
                    exit_exec_idx = min(entry_idx + MAX_HOLD, n - 1)
                    exit_price = opens[exit_exec_idx]
                    if np.isnan(exit_price) or exit_price <= 0:
                        exit_price = closes[min(exit_exec_idx, n - 1)]
                        if np.isnan(exit_price):
                            continue
                    exit_date = dates[exit_exec_idx]
                    exit_type = "expire"

                hold_days = int(
                    np.busday_count(
                        np.datetime64(entry_date, "D"),
                        np.datetime64(exit_date, "D"),
                    )
                ) if not exit_by_sl else int(exit_signal_idx - entry_idx)

                if is_long:
                    ret_pct = (exit_price / entry_price - 1) * 100
                else:
                    ret_pct = (entry_price / exit_price - 1) * 100

                pnl = int(round(entry_price * 100 * ret_pct / 100))

                results.append({
                    "rule": rule,
                    "direction": "LONG" if is_long else "SHORT",
                    "ticker": ticker,
                    "sector": tk["sectors"].iloc[0] if "sectors" in tk.columns else "",
                    "signal_date": sig_date,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_price": round(entry_price, 1),
                    "exit_price": round(exit_price, 1),
                    "ret_pct": round(ret_pct, 3),
                    "pnl": pnl,
                    "hold_days": hold_days,
                    "exit_type": exit_type,
                    "regime": "Uptrend" if mkt_up else "Downtrend",
                })

    out = pd.DataFrame(results)
    if len(out) > 0:
        out["win"] = out["ret_pct"] > 0
        out["year"] = pd.to_datetime(out["signal_date"]).dt.year
    print(f"  Total trades: {len(out):,}")
    print(f"  Done in {time.time()-t0:.1f}s")
    return out


def print_summary(trades: pd.DataFrame, sl_pct: float | None) -> None:
    """サマリー表示"""
    sl_label = f"SL-{sl_pct}%" if sl_pct else "SLなし"
    print(f"\n{'='*100}")
    print(f"■ Step 1 サマリー ({sl_label})")
    print(f"{'='*100}")

    rules_order = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]

    # 全体サマリー
    print(f"\n--- 全体（全期間） ---")
    print(f"{'法則':<5s} {'方向':<6s} {'件数':>7s} {'勝率':>6s} {'PF':>6s} "
          f"{'平均損益':>8s} {'PnL万':>8s} {'保有日':>6s} {'SL%':>5s} {'sig%':>5s} {'exp%':>5s}")
    print("-" * 90)

    for rule in rules_order:
        sub = trades[trades["rule"] == rule]
        if len(sub) == 0:
            continue
        _print_row(rule, sub)

    # レジーム別
    for regime in ["Uptrend", "Downtrend"]:
        print(f"\n--- {regime}（全期間） ---")
        print(f"{'法則':<5s} {'方向':<6s} {'件数':>7s} {'勝率':>6s} {'PF':>6s} "
              f"{'平均損益':>8s} {'PnL万':>8s} {'保有日':>6s} {'SL%':>5s} {'sig%':>5s} {'exp%':>5s}")
        print("-" * 90)
        for rule in rules_order:
            sub = trades[(trades["rule"] == rule) & (trades["regime"] == regime)]
            if len(sub) == 0:
                continue
            _print_row(rule, sub)

    # 年別PnL（構造変化点の特定用）
    if "year" in trades.columns:
        years = sorted(trades["year"].unique())
        print(f"\n--- 年別PnL(万円) ---")
        print(f"{'年':>6s}", end="")
        for rule in rules_order:
            print(f" {rule:>8s}", end="")
        print(f" {'LONG合計':>9s} {'SHORT合計':>9s}")
        print("-" * 100)
        for year in years:
            yt = trades[trades["year"] == year]
            print(f"{year:>6d}", end="")
            for rule in rules_order:
                r = yt[yt["rule"] == rule]
                pnl = r["pnl"].sum() / 10000
                print(f" {pnl:>+7.0f}", end="")
            long_pnl = yt[yt["direction"] == "LONG"]["pnl"].sum() / 10000
            short_pnl = yt[yt["direction"] == "SHORT"]["pnl"].sum() / 10000
            print(f" {long_pnl:>+8.0f} {short_pnl:>+8.0f}")


def _print_row(rule: str, sub: pd.DataFrame) -> None:
    n = len(sub)
    wr = sub["win"].mean() * 100
    ws = sub[sub["ret_pct"] > 0]["ret_pct"].sum()
    ls = abs(sub[sub["ret_pct"] <= 0]["ret_pct"].sum())
    pf = round(ws / ls, 2) if ls > 0 else 999.0
    avg_pnl = sub["pnl"].mean()
    total_pnl = sub["pnl"].sum() / 10000
    avg_hold = sub["hold_days"].mean()
    sl_rate = (sub["exit_type"] == "sl").mean() * 100
    sig_rate = (sub["exit_type"] == "signal").mean() * 100
    exp_rate = (sub["exit_type"] == "expire").mean() * 100
    direction = sub["direction"].iloc[0]
    print(
        f"{rule:<5s} {direction:<6s} {n:>7,d} {wr:>5.1f}% {pf:>5.02f} "
        f"{avg_pnl:>+7.0f} {total_pnl:>+7.0f} {avg_hold:>5.1f}d "
        f"{sl_rate:>4.1f}% {sig_rate:>4.1f}% {exp_rate:>4.1f}%"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sl", type=float, default=None, help="SL percentage (e.g. 3 for -3%%)")
    args = parser.parse_args()

    sl_pct = args.sl
    sl_label = f"SL-{sl_pct}%" if sl_pct else "SLなし"
    print(f"{'='*80}")
    print(f"グランビル8法則 Step 1 検証 ({sl_label})")
    print(f"  全銘柄・価格制限なし・SMA20")
    print(f"  MAX_HOLD={MAX_HOLD}")
    print(f"{'='*80}")

    ps, meta = load_data()
    ps = detect_signals(ps)
    trades = run_backtest(ps, sl_pct)
    print_summary(trades, sl_pct)

    # 保存
    suffix = f"sl{int(sl_pct)}" if sl_pct else "no_sl"
    out_path = OUT_DIR / f"trades_{suffix}.parquet"
    trades.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(trades):,} trades → {out_path}")


if __name__ == "__main__":
    main()
