#!/usr/bin/env python3
"""
Step 1 (全銘柄版): グランビル8法則 エッジの存在確認
====================================================
prices/ の8セグメントparquetを読み込み、全3,769銘柄で検証。
ロジックは step1/validate_signals.py と完全同一。

Usage:
    python3 validate_signals_full.py              # SLなし
    python3 validate_signals_full.py --sl 3       # SL -3%
    python3 validate_signals_full.py --sl 5       # SL -5%

Output:
    step1_full/trades_full_no_sl.parquet
    step1_full/trades_full_sl3.parquet
    step1_full/trades_full_sl5.parquet
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

GRANVILLE_DIR = Path(__file__).resolve().parents[1]  # improvement/granville/
PRICES_DIR = GRANVILLE_DIR / "prices"
PARQUET_DIR = GRANVILLE_DIR.parents[1] / "data" / "parquet"  # dash_plotly/data/parquet
OUT_DIR = Path(__file__).resolve().parent  # step1_full/

MAX_HOLD = 60

SEGMENT_FILES = [
    "core30.parquet",
    "large70.parquet",
    "mid400.parquet",
    "small1.parquet",
    "small2.parquet",
    "prime_other.parquet",
    "standard.parquet",
    "growth.parquet",
]


def load_data() -> pd.DataFrame:
    """全セグメントの株価 + メタ + N225レジーム情報を読み込み"""
    print("[1/4] Loading data...")
    t0 = time.time()

    # 全セグメント読み込み
    frames = []
    for fname in SEGMENT_FILES:
        fpath = PRICES_DIR / fname
        if not fpath.exists():
            print(f"  SKIP {fname} (not found)")
            continue
        df = pd.read_parquet(fpath)
        df["segment"] = fname.replace(".parquet", "")
        frames.append(df)
        print(f"  {fname}: {df['ticker'].nunique()} tickers, {len(df):,} rows")

    p = pd.concat(frames, ignore_index=True)
    p["date"] = pd.to_datetime(p["date"])
    p = p.sort_values(["ticker", "date"]).reset_index(drop=True)

    # メタ情報（セクター取得用）
    meta = pd.read_parquet(PRICES_DIR / "meta_all.parquet")

    # SMA20 テクニカル指標
    g = p.groupby("ticker")
    p["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    p["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    p["prev_close"] = g["Close"].shift(1)
    p["prev_sma20"] = g["sma20"].shift(1)
    p["dev"] = (p["Close"] - p["sma20"]) / p["sma20"] * 100
    p["prev_dev"] = g["dev"].shift(1)
    p = p.dropna(subset=["sma20", "prev_sma20"])

    # 派生フラグ
    p["sma20_up"] = p["sma20_slope"] > 0
    p["sma20_down"] = p["sma20_slope"] < 0
    p["above"] = p["Close"] > p["sma20"]
    p["below"] = p["Close"] < p["sma20"]
    p["prev_above"] = p["prev_close"] > p["prev_sma20"]
    p["prev_below"] = p["prev_close"] < p["prev_sma20"]
    p["up_day"] = p["Close"] > p["prev_close"]
    p["down_day"] = p["Close"] < p["prev_close"]

    # N225 レジーム (SMA20)
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225"})
    nk["nk_sma20"] = nk["nk225"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225"] > nk["nk_sma20"]
    p = p.merge(nk[["date", "market_uptrend"]], on="date", how="left")

    # メタ結合
    p = p.merge(meta[["ticker", "sectors", "stock_name", "segment"]], on=["ticker", "segment"], how="left")

    print(f"  Total: {len(p):,} rows, {p['ticker'].nunique()} tickers")
    print(f"  {p['date'].min().date()} ~ {p['date'].max().date()}")
    print(f"  Done in {time.time()-t0:.1f}s")
    return p


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
    contrarian = {"B4", "S4"}

    results: list[dict] = []
    tickers = df["ticker"].unique()
    total_tickers = len(tickers)

    for idx, ticker in enumerate(tickers):
        if (idx + 1) % 500 == 0:
            print(f"  ... {idx+1}/{total_tickers} tickers processed")

        tk = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        sma20s = tk["sma20"].values
        uptrends = tk["market_uptrend"].values
        n = len(tk)
        sector = tk["sectors"].iloc[0] if "sectors" in tk.columns else ""
        segment = tk["segment"].iloc[0] if "segment" in tk.columns else ""

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

                mkt_up = bool(uptrends[i]) if not pd.isna(uptrends[i]) else False

                if sl_pct is not None:
                    if is_long:
                        sl_price = entry_price * (1 - sl_pct / 100)
                    else:
                        sl_price = entry_price * (1 + sl_pct / 100)
                else:
                    sl_price = None

                exit_signal_idx = None
                exit_by_sl = False

                for j in range(entry_idx, min(entry_idx + MAX_HOLD, n)):
                    if sl_price is not None:
                        if is_long and lows[j] <= sl_price:
                            exit_signal_idx = j
                            exit_by_sl = True
                            break
                        elif not is_long and highs[j] >= sl_price:
                            exit_signal_idx = j
                            exit_by_sl = True
                            break

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
                    "sector": sector,
                    "segment": segment,
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
    print(f"■ Step 1 全銘柄版 サマリー ({sl_label}) — {trades['ticker'].nunique()} tickers")
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

    # セグメント別サマリー（全銘柄版の追加分析）
    print(f"\n--- セグメント別 LONG PF ---")
    print(f"{'セグメント':<16s}", end="")
    for rule in ["B1", "B2", "B3", "B4"]:
        print(f" {rule:>8s}", end="")
    print(f" {'LONG計':>8s}")
    print("-" * 60)
    for seg in ["core30", "large70", "mid400", "small1", "small2", "prime_other", "standard", "growth"]:
        seg_trades = trades[trades["segment"] == seg]
        if len(seg_trades) == 0:
            continue
        print(f"{seg:<16s}", end="")
        for rule in ["B1", "B2", "B3", "B4"]:
            sub = seg_trades[seg_trades["rule"] == rule]
            if len(sub) == 0:
                print(f" {'--':>8s}", end="")
                continue
            ws = sub[sub["ret_pct"] > 0]["ret_pct"].sum()
            ls = abs(sub[sub["ret_pct"] <= 0]["ret_pct"].sum())
            pf = round(ws / ls, 2) if ls > 0 else 999.0
            print(f" {pf:>7.2f}", end="")
        long_sub = seg_trades[seg_trades["direction"] == "LONG"]
        if len(long_sub) > 0:
            ws = long_sub[long_sub["ret_pct"] > 0]["ret_pct"].sum()
            ls = abs(long_sub[long_sub["ret_pct"] <= 0]["ret_pct"].sum())
            pf = round(ws / ls, 2) if ls > 0 else 999.0
            print(f" {pf:>7.02f}")
        else:
            print(f" {'--':>8s}")

    # 年別PnL
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
    print(f"グランビル8法則 Step 1 全銘柄版 ({sl_label})")
    print(f"  全3,769銘柄・価格制限なし・SMA20")
    print(f"  MAX_HOLD={MAX_HOLD}")
    print(f"{'='*80}")

    ps = load_data()
    ps = detect_signals(ps)
    trades = run_backtest(ps, sl_pct)
    print_summary(trades, sl_pct)

    # 保存
    suffix = f"sl{int(sl_pct)}" if sl_pct else "no_sl"
    out_path = OUT_DIR / f"trades_full_{suffix}.parquet"
    trades.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(trades):,} trades → {out_path}")


if __name__ == "__main__":
    main()
