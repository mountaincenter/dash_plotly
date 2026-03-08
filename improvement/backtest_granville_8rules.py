#!/usr/bin/env python3
"""
グランビル8法則バックテスト
===========================
8法則すべてを検証。エントリー/イグジットは定数:
  - エントリー: シグナル日翌営業日の寄付(Open)
  - イグジット: イグジットシグナル発生翌営業日の寄付(Open)

Buy(LONG) 4法則:
  B1: ゴールデンクロス — 上昇SMA20を下から上抜け
  B2: 押し目買い — 上昇トレンド中に一時下落、SMA20付近で反発
  B3: SMA支持 — 上昇SMA20上で接近しつつ反発（下抜けず）
  B4: 売られすぎ反発 — SMA20から大幅下方乖離→反発（逆張り）

Sell/Short 4法則:
  S1: デッドクロス — 下降SMA20を上から下抜け
  S2: 戻り売り — 下降トレンド中に一時上昇、SMA20付近で反落
  S3: SMA抵抗 — 下降SMA20下で接近しつつ反落（上抜けず）
  S4: 買われすぎ反落 — SMA20から大幅上方乖離→反落（逆張り）

イグジットシグナル:
  B1/B2/B3: Close < SMA20 (デッドクロス)
  B4: Close >= SMA20 (平均回帰完了)
  S1/S2/S3: Close > SMA20 (ゴールデンクロス)
  S4: Close <= SMA20 (平均回帰完了)
  共通: 最大60営業日で強制イグジット
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PARQUET_DIR = ROOT / "data" / "parquet"
MAX_HOLD = 60  # 最大保有日数


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """株価データ + メタデータを読み込み、テクニカル指標を付与"""
    print("[1/4] Loading data...")
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

    # N225 uptrend
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

    # === BUY (LONG) ===

    # B1: ゴールデンクロス — 前日SMA20以下、当日SMA20超え、SMA上昇中
    df["B1"] = prev_below & above & sma_up

    # B2: 押し目買い — SMA上昇中、乖離-5%〜0%で反発（SMA付近まで下落して戻る）
    df["B2"] = sma_up & dev.between(-5, 0) & up_day & below

    # B3: SMA支持 — SMA上昇中、SMA上方で乖離0-3%、前日より乖離縮小、反発
    df["B3"] = sma_up & above & dev.between(0, 3) & (df["prev_dev"] > dev) & up_day

    # B4: 売られすぎ反発 — SMA20から8%以上下方乖離、反発開始
    df["B4"] = (dev < -8) & up_day

    # === SELL/SHORT ===

    # S1: デッドクロス — 前日SMA20以上、当日SMA20割れ、SMA下降中
    df["S1"] = prev_above & below & sma_down

    # S2: 戻り売り — SMA下降中、乖離0%〜+5%で反落（SMA付近まで上昇して戻る）
    df["S2"] = sma_down & dev.between(0, 5) & down_day & above

    # S3: SMA抵抗 — SMA下降中、SMA下方で乖離-3%〜0%、前日より乖離拡大（失速）、反落
    df["S3"] = sma_down & below & dev.between(-3, 0) & (df["prev_dev"] < dev) & down_day

    # S4: 買われすぎ反落 — SMA20から8%以上上方乖離、反落開始
    df["S4"] = (dev > 8) & down_day

    rules = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]
    for r in rules:
        cnt = df[r].sum()
        print(f"  {r}: {cnt:>7,} signals")

    print(f"  Done in {time.time()-t0:.1f}s")
    return df


def run_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """全シグナルのバックテストを実行"""
    print("[3/4] Running backtest...")
    t0 = time.time()

    rules = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]
    long_rules = {"B1", "B2", "B3", "B4"}
    # B4, S4 は逆張り（mean reversion）: イグジットは SMA20 到達
    contrarian = {"B4", "S4"}

    results = []

    for ticker in df["ticker"].unique():
        tk = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        sma20s = tk["sma20"].values
        n = len(tk)

        for rule in rules:
            is_long = rule in long_rules
            is_contra = rule in contrarian
            sig_mask = tk[rule].values

            for i in range(n):
                if not sig_mask[i]:
                    continue

                # エントリー: シグナル日翌営業日のOpen
                entry_idx = i + 1
                if entry_idx >= n:
                    continue
                entry_price = opens[entry_idx]
                if np.isnan(entry_price) or entry_price <= 0:
                    continue

                entry_date = dates[entry_idx]

                # イグジットシグナル探索
                exit_signal_idx = None
                for j in range(entry_idx, min(entry_idx + MAX_HOLD, n)):
                    c = closes[j]
                    s = sma20s[j]
                    if np.isnan(c) or np.isnan(s):
                        continue

                    if is_long:
                        if is_contra:
                            # B4: SMA20到達で利確
                            if c >= s:
                                exit_signal_idx = j
                                break
                        else:
                            # B1/B2/B3: デッドクロス（Close < SMA20）
                            if j > entry_idx and c < s:
                                exit_signal_idx = j
                                break
                    else:
                        if is_contra:
                            # S4: SMA20到達で利確
                            if c <= s:
                                exit_signal_idx = j
                                break
                        else:
                            # S1/S2/S3: ゴールデンクロス（Close > SMA20）
                            if j > entry_idx and c > s:
                                exit_signal_idx = j
                                break

                # イグジット: イグジットシグナル翌営業日のOpen
                if exit_signal_idx is not None:
                    exit_exec_idx = exit_signal_idx + 1
                    exit_type = "signal"
                else:
                    # MAX_HOLD到達
                    exit_exec_idx = min(entry_idx + MAX_HOLD, n - 1)
                    exit_type = "expire"

                if exit_exec_idx >= n:
                    exit_exec_idx = n - 1

                exit_price = opens[exit_exec_idx]
                if np.isnan(exit_price) or exit_price <= 0:
                    exit_price = closes[min(exit_exec_idx, n - 1)]
                    if np.isnan(exit_price):
                        continue

                exit_date = dates[exit_exec_idx]
                hold_days = int(exit_exec_idx - entry_idx)

                if is_long:
                    ret_pct = (exit_price / entry_price - 1) * 100
                else:
                    ret_pct = (entry_price / exit_price - 1) * 100

                pnl = int(round(entry_price * 100 * ret_pct / 100))

                results.append({
                    "rule": rule,
                    "ticker": ticker,
                    "signal_date": dates[i],
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_price": round(entry_price, 1),
                    "exit_price": round(exit_price, 1),
                    "ret_pct": round(ret_pct, 3),
                    "pnl": pnl,
                    "hold_days": hold_days,
                    "exit_type": exit_type,
                    "direction": "LONG" if is_long else "SHORT",
                })

    out = pd.DataFrame(results)
    out["win"] = out["ret_pct"] > 0
    print(f"  Total trades: {len(out):,}")
    print(f"  Done in {time.time()-t0:.1f}s")
    return out


def analyze(trades: pd.DataFrame, df: pd.DataFrame) -> None:
    """結果を分析・表示"""
    print("\n[4/4] Analysis")

    # --- 全期間 ---
    print("\n" + "=" * 110)
    print("■ 全期間サマリー（8法則 × 全銘柄）")
    print("=" * 110)

    header = (
        f"{'法則':<5s} {'方向':<6s} {'件数':>6s} {'勝率':>6s} {'平均%':>7s} "
        f"{'中央%':>7s} {'PF':>6s} {'PnL(万)':>9s} {'avg保有':>6s} "
        f"{'exit%':>6s} {'exp%':>5s}"
    )
    print(header)
    print("-" * 110)

    rules_order = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]

    for rule in rules_order:
        sub = trades[trades["rule"] == rule]
        if len(sub) == 0:
            print(f"{rule:<5s} — no trades")
            continue

        n = len(sub)
        wr = sub["win"].mean() * 100
        avg_ret = sub["ret_pct"].mean()
        med_ret = sub["ret_pct"].median()
        wins_sum = sub[sub["ret_pct"] > 0]["ret_pct"].sum()
        loss_sum = abs(sub[sub["ret_pct"] <= 0]["ret_pct"].sum())
        pf = round(wins_sum / loss_sum, 2) if loss_sum > 0 else 999
        total_pnl = sub["pnl"].sum()
        avg_hold = sub["hold_days"].mean()
        sig_rate = (sub["exit_type"] == "signal").mean() * 100
        exp_rate = (sub["exit_type"] == "expire").mean() * 100
        direction = sub["direction"].iloc[0]

        marker = ""
        if pf >= 2.0:
            marker = " ★★"
        elif pf >= 1.5:
            marker = " ★"

        print(
            f"{rule:<5s} {direction:<6s} {n:>6,d} {wr:>5.1f}% {avg_ret:>+6.2f}% "
            f"{med_ret:>+6.2f}% {pf:>5.2f} {total_pnl/10000:>+8.1f} {avg_hold:>5.1f}d "
            f"{sig_rate:>5.1f}% {exp_rate:>4.1f}%{marker}"
        )

    # LONG合計 / SHORT合計
    print("-" * 110)
    for direction in ["LONG", "SHORT"]:
        sub = trades[trades["direction"] == direction]
        if len(sub) == 0:
            continue
        n = len(sub)
        wr = sub["win"].mean() * 100
        avg_ret = sub["ret_pct"].mean()
        wins_sum = sub[sub["ret_pct"] > 0]["ret_pct"].sum()
        loss_sum = abs(sub[sub["ret_pct"] <= 0]["ret_pct"].sum())
        pf = round(wins_sum / loss_sum, 2) if loss_sum > 0 else 999
        total_pnl = sub["pnl"].sum()
        avg_hold = sub["hold_days"].mean()
        print(
            f"{'合計':<5s} {direction:<6s} {n:>6,d} {wr:>5.1f}% {avg_ret:>+6.2f}% "
            f"{'':>7s} {pf:>5.2f} {total_pnl/10000:>+8.1f} {avg_hold:>5.1f}d"
        )

    # --- 直近2年 ---
    print("\n" + "=" * 110)
    print("■ 直近2年（2024-03〜）")
    print("=" * 110)

    recent = trades[trades["signal_date"] >= "2024-03-01"]
    print(header)
    print("-" * 110)

    for rule in rules_order:
        sub = recent[recent["rule"] == rule]
        if len(sub) == 0:
            print(f"{rule:<5s} — no trades")
            continue

        n = len(sub)
        wr = sub["win"].mean() * 100
        avg_ret = sub["ret_pct"].mean()
        med_ret = sub["ret_pct"].median()
        wins_sum = sub[sub["ret_pct"] > 0]["ret_pct"].sum()
        loss_sum = abs(sub[sub["ret_pct"] <= 0]["ret_pct"].sum())
        pf = round(wins_sum / loss_sum, 2) if loss_sum > 0 else 999
        total_pnl = sub["pnl"].sum()
        avg_hold = sub["hold_days"].mean()
        sig_rate = (sub["exit_type"] == "signal").mean() * 100
        exp_rate = (sub["exit_type"] == "expire").mean() * 100
        direction = sub["direction"].iloc[0]

        marker = ""
        if pf >= 2.0:
            marker = " ★★"
        elif pf >= 1.5:
            marker = " ★"

        print(
            f"{rule:<5s} {direction:<6s} {n:>6,d} {wr:>5.1f}% {avg_ret:>+6.2f}% "
            f"{med_ret:>+6.2f}% {pf:>5.2f} {total_pnl/10000:>+8.1f} {avg_hold:>5.1f}d "
            f"{sig_rate:>5.1f}% {exp_rate:>4.1f}%{marker}"
        )

    # --- market_uptrend フィルター比較 ---
    print("\n" + "=" * 110)
    print("■ Market Uptrend フィルター効果（直近2年）")
    print("=" * 110)

    # uptrendを結合
    uptrend_map = df.drop_duplicates("date")[["date", "market_uptrend"]].dropna()
    recent_up = recent.merge(
        uptrend_map.rename(columns={"date": "signal_date"}),
        on="signal_date", how="left"
    )

    for label, mask in [("uptrend=True", True), ("uptrend=False", False)]:
        sub = recent_up[recent_up["market_uptrend"] == mask]
        print(f"\n  --- {label} ---")
        print(f"  {'法則':<5s} {'件数':>6s} {'勝率':>6s} {'PF':>6s} {'PnL(万)':>9s}")
        print(f"  {'-'*40}")

        for rule in rules_order:
            r = sub[sub["rule"] == rule]
            if len(r) == 0:
                continue
            n = len(r)
            wr = r["win"].mean() * 100
            ws = r[r["ret_pct"] > 0]["ret_pct"].sum()
            ls = abs(r[r["ret_pct"] <= 0]["ret_pct"].sum())
            pf = round(ws / ls, 2) if ls > 0 else 999
            pnl = r["pnl"].sum()
            print(f"  {rule:<5s} {n:>6,d} {wr:>5.1f}% {pf:>5.2f} {pnl/10000:>+8.1f}")

    # --- 保有日数分布 ---
    print("\n" + "=" * 110)
    print("■ 保有日数分布")
    print("=" * 110)

    for rule in rules_order:
        sub = trades[trades["rule"] == rule]
        if len(sub) == 0:
            continue
        q = sub["hold_days"].quantile([0.25, 0.5, 0.75])
        print(
            f"  {rule}: min={sub['hold_days'].min():>3d}  "
            f"Q1={q[0.25]:>5.0f}  median={q[0.5]:>5.0f}  "
            f"Q3={q[0.75]:>5.0f}  max={sub['hold_days'].max():>3d}  "
            f"avg={sub['hold_days'].mean():>5.1f}"
        )

    # --- 年別PnL ---
    print("\n" + "=" * 110)
    print("■ 年別PnL（万円）")
    print("=" * 110)

    trades["year"] = pd.to_datetime(trades["signal_date"]).dt.year
    years = sorted(trades["year"].unique())
    # 直近5年に限定
    years = [y for y in years if y >= 2021]

    print(f"  {'年':>6s}", end="")
    for rule in rules_order:
        print(f" {rule:>8s}", end="")
    print(f" {'LONG':>9s} {'SHORT':>9s}")
    print(f"  {'-'*90}")

    for year in years:
        yt = trades[trades["year"] == year]
        print(f"  {year:>6d}", end="")
        for rule in rules_order:
            r = yt[yt["rule"] == rule]
            pnl = r["pnl"].sum() / 10000
            print(f" {pnl:>+7.1f}", end="")
        long_pnl = yt[yt["direction"] == "LONG"]["pnl"].sum() / 10000
        short_pnl = yt[yt["direction"] == "SHORT"]["pnl"].sum() / 10000
        print(f" {long_pnl:>+8.1f} {short_pnl:>+8.1f}")


def main():
    print("=" * 80)
    print("グランビル8法則 バックテスト")
    print("  Entry: シグナル翌営業日Open")
    print("  Exit:  イグジットシグナル翌営業日Open")
    print("  Max hold: 60 trading days")
    print("=" * 80)

    ps, meta = load_data()
    ps = detect_signals(ps)
    trades = run_backtest(ps)
    analyze(trades, ps)

    # trades保存
    out_path = ROOT / "improvement" / "output" / "granville_8rules_trades.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    trades.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(trades):,} trades → {out_path}")


if __name__ == "__main__":
    main()
