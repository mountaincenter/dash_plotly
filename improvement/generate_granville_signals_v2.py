#!/usr/bin/env python3
"""
generate_granville_signals_v2.py
グランビルLONG戦略: 翌日エントリー候補シグナル生成（improvement版）

シグナル: SMA20からの乖離率 < DEV_THRESHOLD かつ 陽線
Exit: SL -3% IFD逆指値 + trail50%
フィルター: CI先行指数 > 0, bad_sectors除外, 株価 < ¥20,000
  ※ uptrendフィルターなし（全レジームで有効）

検証後に scripts/pipeline/generate_granville_signals.py へ移行する。
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = ROOT / "data" / "parquet"

# === 調整可能パラメータ ===
DEV_THRESHOLD = -4.0   # 乖離率閾値: dev < この値 かつ陽線で発火
SL_PCT = 3.0           # 損切り -3%（IFD逆指値）
TRAIL_PCT = 0.5        # トレイリングSL: 含み益の50%をSLに引き上げ
MAX_HOLD = 60          # 最大保有日数（安全弁）

BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]


def load_data() -> pd.DataFrame:
    """株価 + N225 uptrend + メタデータを統合"""
    t0 = time.time()

    m = pd.read_parquet(PARQUET_DIR / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)

    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(m["ticker"].tolist())].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # テクニカル指標
    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps = ps.dropna(subset=["sma20"])

    # N225 uptrend（レジーム判定用、フィルターには使わない）
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225"})
    nk["nk225_sma20"] = nk["nk225"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225"] > nk["nk225_sma20"]
    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")

    ps = ps.merge(m[["ticker", "sectors", "stock_name"]], on="ticker", how="left")

    print(f"Data loaded: {len(ps):,} rows, {ps['ticker'].nunique()} tickers in {time.time()-t0:.1f}s")
    return ps


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """乖離率 < DEV_THRESHOLD かつ陽線 でシグナル検出"""
    df = df.copy()
    dev = df["dev_from_sma20"]
    up_day = df["Close"] > df["prev_close"]
    df["sig_dev"] = (dev < DEV_THRESHOLD) & up_day
    return df


def simulate_trade(tk: pd.DataFrame, sig_idx: int) -> dict | None:
    """1トレードをSL + trail50%でシミュレート

    Entry: シグナル翌営業日Open
    Exit優先順: SL/trail → Close >= SMA20（平均回帰）→ MAX_HOLD到達
    """
    n = len(tk)
    entry_idx = sig_idx + 1
    if entry_idx >= n:
        return None

    dates = tk["date"].values
    opens = tk["Open"].values
    highs = tk["High"].values
    lows = tk["Low"].values
    closes = tk["Close"].values
    sma20s = tk["sma20"].values

    entry_price = float(opens[entry_idx])
    if np.isnan(entry_price) or entry_price <= 0:
        return None

    sl_price = entry_price * (1 - SL_PCT / 100)
    max_price = entry_price

    for j in range(entry_idx, min(entry_idx + MAX_HOLD, n)):
        h, lo, c, s = float(highs[j]), float(lows[j]), float(closes[j]), float(sma20s[j])
        if np.isnan(c) or np.isnan(s):
            continue

        # SL判定（日中Low）— エントリー当日もチェック（IFD逆指値）
        if lo <= sl_price:
            return _result(dates, sig_idx, entry_idx, j, entry_price, sl_price, "SL")

        # エントリー当日はSLのみ。trail/signal exitは翌日以降
        if j == entry_idx:
            continue

        # trail更新（翌日以降）
        if TRAIL_PCT > 0 and h > max_price:
            max_price = h
            profit = max_price - entry_price
            if profit > 0:
                trail_sl = entry_price + profit * TRAIL_PCT
                if trail_sl > sl_price:
                    sl_price = trail_sl

        # trail SL判定（翌日以降）
        if TRAIL_PCT > 0 and lo <= sl_price:
            return _result(dates, sig_idx, entry_idx, j, entry_price, sl_price, "trail")

        # 平均回帰exit: Close >= SMA20 → 翌営業日Open
        if c >= s:
            exit_idx = j + 1 if j + 1 < n else j
            exit_price = float(opens[exit_idx]) if exit_idx < n and not np.isnan(opens[exit_idx]) else c
            return _result(dates, sig_idx, entry_idx, exit_idx, entry_price, exit_price, "signal")

    # MAX_HOLD到達
    exit_idx = min(entry_idx + MAX_HOLD, n - 1)
    exit_price = float(opens[exit_idx]) if not np.isnan(opens[exit_idx]) else float(closes[exit_idx])
    return _result(dates, sig_idx, entry_idx, exit_idx, entry_price, exit_price, "expire")


def _result(dates, sig_idx, entry_idx, exit_idx, entry_price, exit_price, exit_type):
    ret_pct = (exit_price / entry_price - 1) * 100
    pnl = int(round(entry_price * 100 * ret_pct / 100))
    return {
        "signal_date": dates[sig_idx],
        "entry_date": dates[entry_idx],
        "exit_date": dates[exit_idx],
        "entry_price": round(entry_price, 1),
        "exit_price": round(exit_price, 1),
        "ret_pct": round(ret_pct, 3),
        "pnl": pnl,
        "hold_days": int(exit_idx - entry_idx),
        "exit_type": exit_type,
    }


def run_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """全銘柄のバックテスト実行（同一銘柄重複除去）"""
    t0 = time.time()
    results = []

    for ticker in df["ticker"].unique():
        tk = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        sig_mask = tk["sig_dev"].values
        uptrends = tk["market_uptrend"].values
        sectors = tk["sectors"].values
        stock_names = tk["stock_name"].values
        devs = tk["dev_from_sma20"].values
        closes = tk["Close"].values
        n = len(tk)

        in_position = False
        position_exit_idx = -1

        for i in range(n):
            if in_position and i > position_exit_idx:
                in_position = False

            if not sig_mask[i] or in_position:
                continue

            trade = simulate_trade(tk, i)
            if trade is None:
                continue

            in_position = True
            # exit_dateのindexを探す
            exit_date = trade["exit_date"]
            exit_matches = tk.index[tk["date"] == exit_date]
            position_exit_idx = int(exit_matches[0]) if len(exit_matches) > 0 else i + trade["hold_days"]

            trade["ticker"] = ticker
            trade["stock_name"] = stock_names[i] if i < len(stock_names) else ""
            trade["sector"] = sectors[i] if i < len(sectors) else ""
            trade["dev_at_signal"] = round(float(devs[i]), 2)
            trade["regime"] = "uptrend" if uptrends[i] else "downtrend"
            trade["close_at_signal"] = round(float(closes[i]), 1)
            results.append(trade)

    out = pd.DataFrame(results)
    print(f"Backtest: {len(out):,} trades in {time.time()-t0:.1f}s")
    return out


def show_summary(trades: pd.DataFrame, label: str = ""):
    """統計サマリー表示"""
    if trades.empty:
        print(f"  {label}: no trades")
        return

    n = len(trades)
    wr = (trades["ret_pct"] > 0).mean() * 100
    avg = trades["ret_pct"].mean()
    ws = trades[trades["ret_pct"] > 0]["ret_pct"].sum()
    ls = abs(trades[trades["ret_pct"] <= 0]["ret_pct"].sum())
    pf = round(ws / ls, 2) if ls > 0 else 999
    pnl = trades["pnl"].sum() / 10000
    hold = trades["hold_days"].mean()

    sl_pct = (trades["exit_type"] == "SL").mean() * 100
    trail_pct = (trades["exit_type"] == "trail").mean() * 100
    sig_pct = (trades["exit_type"] == "signal").mean() * 100

    print(f"  {label:30s}  {n:>6,d}  {wr:>5.1f}%  {avg:>+6.2f}%  PF{pf:>5.2f}  {pnl:>+8.1f}万  {hold:>4.1f}d  SL{sl_pct:.0f}%/tr{trail_pct:.0f}%/sig{sig_pct:.0f}%")


def main():
    print("=" * 100)
    print(f"グランビルLONG戦略 v2 (improvement版)")
    print(f"  DEV_THRESHOLD={DEV_THRESHOLD}%, SL={SL_PCT}%, TRAIL={TRAIL_PCT*100:.0f}%")
    print(f"  BAD_SECTORS除外, 株価<¥20,000")
    print("=" * 100)

    ps = load_data()
    ps = detect_signals(ps)

    # フィルター: bad_sectors除外, 株価 < ¥20,000
    before = len(ps)
    ps = ps[~ps["sectors"].isin(BAD_SECTORS)]
    ps = ps[ps["Close"] < 20000]
    print(f"Filters: {before:,} → {len(ps):,} rows")

    trades = run_backtest(ps)
    if trades.empty:
        print("No trades generated")
        return

    # サマリー
    print(f"\n{'='*100}")
    print(f"{'区分':30s}  {'件数':>6s}  {'勝率':>6s}  {'平均%':>7s}  {'PF':>7s}  {'PnL(万)':>9s}  {'保有':>5s}  {'exit内訳'}")
    print(f"{'-'*100}")

    # 期間別 × レジーム別
    for period_label, start in [("全期間", None), ("2015-2026", "2015-01-01"), ("直近2年", "2024-03-01")]:
        t = trades[trades["signal_date"] >= start] if start else trades
        for regime_label, regime_filter in [("全体", None), ("Uptrend", "uptrend"), ("Downtrend", "downtrend")]:
            sub = t[t["regime"] == regime_filter] if regime_filter else t
            show_summary(sub, f"{period_label} / {regime_label}")
        print()

    # 年別PnL（2015-2026）
    trades["year"] = pd.to_datetime(trades["signal_date"]).dt.year
    print(f"\n{'='*100}")
    print("年別PnL（万円）")
    print(f"{'='*100}")
    print(f"  {'年':>6s}  {'全体':>8s}  {'Uptrend':>8s}  {'Downtrend':>9s}  {'件数':>6s}")
    print(f"  {'-'*50}")

    for year in range(2015, 2027):
        yt = trades[trades["year"] == year]
        if yt.empty:
            continue
        total = yt["pnl"].sum() / 10000
        up = yt[yt["regime"] == "uptrend"]["pnl"].sum() / 10000
        down = yt[yt["regime"] == "downtrend"]["pnl"].sum() / 10000
        print(f"  {year:>6d}  {total:>+7.1f}  {up:>+7.1f}  {down:>+8.1f}  {len(yt):>6,d}")

    # 出力保存
    out_path = ROOT / "improvement" / "output" / "granville_v2_trades.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    trades.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(trades):,} trades → {out_path}")

    # 最新日のシグナル表示
    latest = ps["date"].max()
    latest_sigs = ps[(ps["date"] == latest) & ps["sig_dev"]]
    print(f"\n{'='*100}")
    print(f"最新シグナル ({latest.date()}): {len(latest_sigs)}件")
    if not latest_sigs.empty:
        for _, row in latest_sigs.head(20).iterrows():
            print(f"  {row['ticker']} {row['stock_name']:10s}  "
                  f"¥{row['Close']:>8,.0f}  dev={row['dev_from_sma20']:>+5.1f}%  "
                  f"SL=¥{row['Close'] * (1 - SL_PCT/100):>8,.0f}")
        if len(latest_sigs) > 20:
            print(f"  ... +{len(latest_sigs) - 20}件")
    print("=" * 100)


if __name__ == "__main__":
    main()
