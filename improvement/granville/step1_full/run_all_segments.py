#!/usr/bin/env python3
"""
セグメント単位でStep1バックテストを分割実行し、最後にマージする。
1,700万行を一括処理するとgroupby.rollingが遅すぎるため。

Usage:
    python3 run_all_segments.py --sl 3
"""
from __future__ import annotations

import argparse
import time
import json
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import os

GRANVILLE_DIR = Path(__file__).resolve().parents[1]
PRICES_DIR = GRANVILLE_DIR / "prices"
PARQUET_DIR = GRANVILLE_DIR.parents[1] / "data" / "parquet"
OUT_DIR = Path(__file__).resolve().parent
MAX_HOLD = 60

SEGMENTS = ["core30", "large70", "mid400", "small1", "small2", "prime_other", "standard", "growth"]


def load_n225_regime() -> pd.DataFrame:
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225"})
    nk["nk_sma20"] = nk["nk225"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225"] > nk["nk_sma20"]
    return nk[["date", "market_uptrend"]]


def process_segment(seg: str, nk_regime: pd.DataFrame, sl_pct: float | None) -> pd.DataFrame:
    fpath = PRICES_DIR / f"{seg}.parquet"
    meta = pd.read_parquet(PRICES_DIR / "meta_all.parquet")

    p = pd.read_parquet(fpath)
    p["date"] = pd.to_datetime(p["date"])
    p = p.sort_values(["ticker", "date"]).reset_index(drop=True)

    # SMA20
    g = p.groupby("ticker")
    p["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    p["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    p["prev_close"] = g["Close"].shift(1)
    p["prev_sma20"] = g["sma20"].shift(1)
    p["dev"] = (p["Close"] - p["sma20"]) / p["sma20"] * 100
    p["prev_dev"] = g["dev"].shift(1)
    p = p.dropna(subset=["sma20", "prev_sma20"]).copy()

    p["sma20_up"] = p["sma20_slope"] > 0
    p["sma20_down"] = p["sma20_slope"] < 0
    p["above"] = p["Close"] > p["sma20"]
    p["below"] = p["Close"] < p["sma20"]
    p["prev_above"] = p["prev_close"] > p["prev_sma20"]
    p["prev_below"] = p["prev_close"] < p["prev_sma20"]
    p["up_day"] = p["Close"] > p["prev_close"]
    p["down_day"] = p["Close"] < p["prev_close"]

    # N225 regime
    p = p.merge(nk_regime, on="date", how="left")
    # meta
    seg_meta = meta[meta["segment"] == seg][["ticker", "sectors", "stock_name"]]
    p = p.merge(seg_meta, on="ticker", how="left")

    # シグナル検出
    dev = p["dev"]
    p["B1"] = p["prev_below"] & p["above"] & p["sma20_up"]
    p["B2"] = p["sma20_up"] & dev.between(-5, 0) & p["up_day"] & p["below"]
    p["B3"] = p["sma20_up"] & p["above"] & dev.between(0, 3) & (p["prev_dev"] > dev) & p["up_day"]
    p["B4"] = (dev < -8) & p["up_day"]
    p["S1"] = p["prev_above"] & p["below"] & p["sma20_down"]
    p["S2"] = p["sma20_down"] & dev.between(0, 5) & p["down_day"] & p["above"]
    p["S3"] = p["sma20_down"] & p["below"] & dev.between(-3, 0) & (p["prev_dev"] < dev) & p["down_day"]
    p["S4"] = (dev > 8) & p["down_day"]

    sig_counts = {r: int(p[r].sum()) for r in ["B1","B2","B3","B4","S1","S2","S3","S4"]}
    print(f"  Signals: {sig_counts}")

    # バックテスト
    rules = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]
    long_rules = {"B1", "B2", "B3", "B4"}
    contrarian = {"B4", "S4"}
    results: list[dict] = []

    tickers = p["ticker"].unique()
    for ti, ticker in enumerate(tickers):
        if (ti + 1) % 200 == 0:
            print(f"    {ti+1}/{len(tickers)}")

        tk = p[p["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        sma20s = tk["sma20"].values
        uptrends = tk["market_uptrend"].values
        n = len(tk)
        sector = tk["sectors"].iloc[0] if "sectors" in tk.columns else ""

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
                    sl_price = entry_price * (1 - sl_pct / 100) if is_long else entry_price * (1 + sl_pct / 100)
                else:
                    sl_price = None

                exit_signal_idx = None
                exit_by_sl = False

                for j in range(entry_idx, min(entry_idx + MAX_HOLD, n)):
                    if sl_price is not None:
                        if is_long and lows[j] <= sl_price:
                            exit_signal_idx = j; exit_by_sl = True; break
                        elif not is_long and highs[j] >= sl_price:
                            exit_signal_idx = j; exit_by_sl = True; break

                    c, s = closes[j], sma20s[j]
                    if np.isnan(c) or np.isnan(s):
                        continue
                    if is_long:
                        if is_contra:
                            if c >= s: exit_signal_idx = j; break
                        else:
                            if j > entry_idx and c < s: exit_signal_idx = j; break
                    else:
                        if is_contra:
                            if c <= s: exit_signal_idx = j; break
                        else:
                            if j > entry_idx and c > s: exit_signal_idx = j; break

                if exit_by_sl:
                    exit_price = sl_price
                    exit_date = dates[exit_signal_idx]
                    exit_type = "sl"
                elif exit_signal_idx is not None:
                    eidx = min(exit_signal_idx + 1, n - 1)
                    exit_price = opens[eidx]
                    if np.isnan(exit_price) or exit_price <= 0:
                        exit_price = closes[min(eidx, n - 1)]
                        if np.isnan(exit_price): continue
                    exit_date = dates[eidx]
                    exit_type = "signal"
                else:
                    eidx = min(entry_idx + MAX_HOLD, n - 1)
                    exit_price = opens[eidx]
                    if np.isnan(exit_price) or exit_price <= 0:
                        exit_price = closes[min(eidx, n - 1)]
                        if np.isnan(exit_price): continue
                    exit_date = dates[eidx]
                    exit_type = "expire"

                hold_days = int(np.busday_count(
                    np.datetime64(entry_date, "D"), np.datetime64(exit_date, "D"),
                )) if not exit_by_sl else int(exit_signal_idx - entry_idx)

                ret_pct = (exit_price / entry_price - 1) * 100 if is_long else (entry_price / exit_price - 1) * 100
                pnl = int(round(entry_price * 100 * ret_pct / 100))

                results.append({
                    "rule": rule, "direction": "LONG" if is_long else "SHORT",
                    "ticker": ticker, "sector": sector, "segment": seg,
                    "signal_date": sig_date, "entry_date": entry_date, "exit_date": exit_date,
                    "entry_price": round(entry_price, 1), "exit_price": round(exit_price, 1),
                    "ret_pct": round(ret_pct, 3), "pnl": pnl, "hold_days": hold_days,
                    "exit_type": exit_type, "regime": "Uptrend" if mkt_up else "Downtrend",
                })

    out = pd.DataFrame(results)
    if len(out) > 0:
        out["win"] = out["ret_pct"] > 0
        out["year"] = pd.to_datetime(out["signal_date"]).dt.year
    return out


def print_summary(trades: pd.DataFrame, sl_pct: float | None) -> None:
    sl_label = f"SL-{sl_pct}%" if sl_pct else "SLなし"
    print(f"\n{'='*100}")
    print(f"■ Step 1 全銘柄版 ({sl_label}) — {trades['ticker'].nunique()} tickers, {len(trades):,} trades")
    print(f"{'='*100}")

    rules_order = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]

    print(f"\n--- 全体 ---")
    _print_header()
    for rule in rules_order:
        sub = trades[trades["rule"] == rule]
        if len(sub) > 0: _print_row(rule, sub)

    for regime in ["Uptrend", "Downtrend"]:
        print(f"\n--- {regime} ---")
        _print_header()
        for rule in rules_order:
            sub = trades[(trades["rule"] == rule) & (trades["regime"] == regime)]
            if len(sub) > 0: _print_row(rule, sub)

    print(f"\n--- セグメント別 LONG PF ---")
    print(f"{'seg':<16s}", end="")
    for r in ["B1","B2","B3","B4"]:
        print(f" {r:>8s}", end="")
    print(f" {'LONG計':>8s} {'件数':>8s}")
    print("-" * 72)
    for seg in SEGMENTS:
        st = trades[trades["segment"] == seg]
        if len(st) == 0: continue
        print(f"{seg:<16s}", end="")
        for r in ["B1","B2","B3","B4"]:
            sub = st[st["rule"] == r]
            if len(sub) == 0: print(f" {'--':>8s}", end=""); continue
            ws = sub[sub["ret_pct"]>0]["ret_pct"].sum()
            ls = abs(sub[sub["ret_pct"]<=0]["ret_pct"].sum())
            pf = round(ws/ls, 2) if ls > 0 else 999.0
            print(f" {pf:>7.2f}", end="")
        ls_ = st[st["direction"]=="LONG"]
        if len(ls_) > 0:
            ws = ls_[ls_["ret_pct"]>0]["ret_pct"].sum()
            ls2 = abs(ls_[ls_["ret_pct"]<=0]["ret_pct"].sum())
            pf = round(ws/ls2, 2) if ls2 > 0 else 999.0
            print(f" {pf:>7.02f} {len(ls_):>7,d}")
        else:
            print()

    if "year" in trades.columns:
        years = sorted(trades["year"].unique())
        print(f"\n--- 年別PnL(万円) ---")
        print(f"{'年':>6s}", end="")
        for r in rules_order: print(f" {r:>8s}", end="")
        print(f" {'LONG計':>9s} {'SHORT計':>9s}")
        print("-" * 100)
        for y in years:
            yt = trades[trades["year"]==y]
            print(f"{y:>6d}", end="")
            for r in rules_order:
                pnl = yt[yt["rule"]==r]["pnl"].sum()/10000
                print(f" {pnl:>+7.0f}", end="")
            lp = yt[yt["direction"]=="LONG"]["pnl"].sum()/10000
            sp = yt[yt["direction"]=="SHORT"]["pnl"].sum()/10000
            print(f" {lp:>+8.0f} {sp:>+8.0f}")


def _print_header():
    print(f"{'法則':<5s} {'方向':<6s} {'件数':>7s} {'勝率':>6s} {'PF':>6s} "
          f"{'平均損益':>8s} {'PnL万':>8s} {'保有日':>6s} {'SL%':>5s} {'sig%':>5s} {'exp%':>5s}")
    print("-" * 90)


def _print_row(rule, sub):
    n = len(sub); wr = sub["win"].mean()*100
    ws = sub[sub["ret_pct"]>0]["ret_pct"].sum()
    ls = abs(sub[sub["ret_pct"]<=0]["ret_pct"].sum())
    pf = round(ws/ls,2) if ls>0 else 999.0
    avg = sub["pnl"].mean(); total = sub["pnl"].sum()/10000
    hd = sub["hold_days"].mean()
    sl_r = (sub["exit_type"]=="sl").mean()*100
    sig_r = (sub["exit_type"]=="signal").mean()*100
    exp_r = (sub["exit_type"]=="expire").mean()*100
    d = sub["direction"].iloc[0]
    print(f"{rule:<5s} {d:<6s} {n:>7,d} {wr:>5.1f}% {pf:>5.02f} "
          f"{avg:>+7.0f} {total:>+7.0f} {hd:>5.1f}d {sl_r:>4.1f}% {sig_r:>4.1f}% {exp_r:>4.1f}%")


def send_slack(message: str) -> None:
    from common_cfg.env import load_dotenv_cascade
    load_dotenv_cascade()
    url = os.getenv("SLACK_WEBHOOK_CLAUDE")
    if not url:
        print("[WARN] SLACK_WEBHOOK_CLAUDE not set")
        return
    payload = {"text": message}
    try:
        requests.post(url, json=payload, timeout=10)
        print("[OK] Slack notification sent")
    except Exception as e:
        print(f"[WARN] Slack failed: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sl", type=float, default=None)
    args = parser.parse_args()
    sl_pct = args.sl
    sl_label = f"SL-{sl_pct}%" if sl_pct else "SLなし"

    print(f"{'='*80}")
    print(f"グランビル8法則 Step 1 全銘柄版 ({sl_label}) — セグメント分割実行")
    print(f"{'='*80}")

    t_total = time.time()
    nk_regime = load_n225_regime()

    all_trades = []
    for seg in SEGMENTS:
        print(f"\n[{seg}] Processing...")
        t0 = time.time()
        trades = process_segment(seg, nk_regime, sl_pct)
        elapsed = time.time() - t0
        print(f"  => {len(trades):,} trades, {trades['ticker'].nunique() if len(trades)>0 else 0} tickers, {elapsed:.1f}s")
        all_trades.append(trades)

    merged = pd.concat(all_trades, ignore_index=True)
    total_time = time.time() - t_total

    print_summary(merged, sl_pct)

    suffix = f"sl{int(sl_pct)}" if sl_pct else "no_sl"
    out_path = OUT_DIR / f"trades_full_{suffix}.parquet"
    merged.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(merged):,} trades → {out_path} ({total_time:.0f}s)")

    # Slack通知
    msg = (
        f":chart_with_upwards_trend: *グランビル Step1 全銘柄版 完了*\n"
        f"• {sl_label} | {merged['ticker'].nunique()} tickers | {len(merged):,} trades\n"
        f"• 所要時間: {total_time/60:.1f}分\n"
        f"• 出力: `{out_path.name}`"
    )
    send_slack(msg)


if __name__ == "__main__":
    main()
