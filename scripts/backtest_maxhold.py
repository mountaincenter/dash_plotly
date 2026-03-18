#!/usr/bin/env python3
"""MAX_HOLD検証: 10日統一 vs ルール別(B1=7,B2=30,B3=5,B4=13)"""
import pandas as pd
import numpy as np
from pathlib import Path

PARQUET = Path(__file__).resolve().parents[1] / "data" / "parquet" / "prices_max_1d.parquet"

print("Loading prices...")
p = pd.read_parquet(PARQUET)
p["date"] = pd.to_datetime(p["date"])
p = p.sort_values(["ticker", "date"]).reset_index(drop=True)
print(f"  {p['ticker'].nunique()} tickers, {len(p):,} rows")

# テクニカル指標（ベクトル化）
print("Computing indicators...")
g = p.groupby("ticker")
p["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
p["prev_close"] = g["Close"].shift(1)
p["dev"] = (p["Close"] - p["sma20"]) / p["sma20"] * 100
p["prev_dev"] = g["dev"].shift(1)
p["sma20_slope"] = g["sma20"].transform(lambda x: x.diff())
p["up_day"] = p["Close"] > p["prev_close"]
p["above"] = p["Close"] > p["sma20"]
p["below"] = p["Close"] < p["sma20"]
p["prev_below"] = g["below"].shift(1).fillna(False)
p["sma_up"] = p["sma20_slope"] > 0
p["high_20"] = g["High"].transform(lambda x: x.rolling(20, min_periods=20).max())

# シグナル検出（ベクトル化）
print("Detecting signals...")
valid = p["sma20"].notna() & p["prev_close"].notna()

b4 = valid & (p["dev"] < -8) & p["up_day"]
b1 = valid & p["prev_below"] & p["above"] & p["sma_up"]
b3 = valid & p["sma_up"] & (p["dev"] >= 0) & (p["dev"] <= 3) & p["above"] & (p["prev_dev"] > p["dev"]) & p["up_day"]
b2 = valid & p["sma_up"] & (p["dev"] >= -5) & (p["dev"] <= 0) & p["below"] & p["up_day"]

# 優先順位: B4 > B1 > B3 > B2
p["rule"] = None
p.loc[b2, "rule"] = "B2"
p.loc[b3, "rule"] = "B3"
p.loc[b1, "rule"] = "B1"
p.loc[b4, "rule"] = "B4"

signals = p[p["rule"].notna()][["date", "ticker", "rule"]].copy()

# 直近2年
cutoff = p["date"].max() - pd.Timedelta(days=730)
signals = signals[signals["date"] >= cutoff].reset_index(drop=True)
print(f"  Signals: {len(signals):,} ({cutoff.date()} ~ {p['date'].max().date()})")
for r in ["B4", "B1", "B3", "B2"]:
    print(f"    {r}: {(signals['rule']==r).sum():,}")

# バックテスト（ベクトル最適化）
def backtest(signals_df, prices, max_hold_map):
    # 銘柄ごとにインデックスマッピング
    results = []
    ticker_groups = prices.groupby("ticker")

    for ticker, tk in ticker_groups:
        tk_sigs = signals_df[signals_df["ticker"] == ticker]
        if tk_sigs.empty:
            continue

        tk = tk.reset_index(drop=True)
        dates = tk["date"].values.astype("datetime64[ns]")
        opens = tk["Open"].values
        closes = tk["Close"].values
        highs = tk["High"].values
        n = len(tk)

        # 20日high (precompute)
        high20 = np.full(n, np.nan)
        for i in range(19, n):
            high20[i] = highs[i-19:i+1].max()

        for _, sig in tk_sigs.iterrows():
            # エントリー日を探す
            entry_idx = np.searchsorted(dates, np.datetime64(sig["date"]), side="right")
            if entry_idx >= n:
                continue
            ep = opens[entry_idx]
            if np.isnan(ep) or ep <= 0:
                continue

            rule = sig["rule"]
            max_hold = max_hold_map[rule]

            exited = False
            exit_price = 0.0
            hold_days = 0
            exit_type = ""

            for i in range(entry_idx, min(entry_idx + max_hold, n)):
                day = i - entry_idx
                # 20日高値exit
                if day > 0 and i >= 19:
                    if highs[i] >= high20[i]:
                        exit_price = closes[i]
                        hold_days = day + 1
                        exit_type = "20d_high"
                        exited = True
                        break
                # MAX_HOLD
                if day >= max_hold - 1:
                    exit_price = closes[i]
                    hold_days = max_hold
                    exit_type = "max_hold"
                    exited = True
                    break

            if exited:
                pnl = (exit_price - ep) * 100
                results.append({
                    "rule": rule,
                    "pnl": pnl,
                    "ret": (exit_price / ep - 1) * 100,
                    "hold_days": hold_days,
                    "exit_type": exit_type,
                })

    return pd.DataFrame(results)

# 1. 全ルール10日統一
print("\n=== 全ルール10日統一 ===")
mh1 = {"B4": 10, "B1": 10, "B3": 10, "B2": 10}
r1 = backtest(signals, p, mh1)
for rule in ["B4", "B1", "B3", "B2"]:
    sub = r1[r1["rule"] == rule]
    if sub.empty:
        continue
    wr = (sub["pnl"] > 0).mean() * 100
    avg = sub["pnl"].mean()
    total = sub["pnl"].sum()
    print(f"  {rule}: {len(sub):>6} trades  WR={wr:.1f}%  avgPnL=¥{avg:>+,.0f}  totalPnL=¥{total:>+,.0f}")
t1 = r1["pnl"].sum()
print(f"  TOTAL: {len(r1)} trades  totalPnL=¥{t1:>+,.0f}")

# 2. ルール別MAX_HOLD
print("\n=== ルール別MAX_HOLD (B1=7, B2=30, B3=5, B4=13) ===")
mh2 = {"B4": 13, "B1": 7, "B3": 5, "B2": 30}
r2 = backtest(signals, p, mh2)
for rule in ["B4", "B1", "B3", "B2"]:
    sub = r2[r2["rule"] == rule]
    if sub.empty:
        continue
    wr = (sub["pnl"] > 0).mean() * 100
    avg = sub["pnl"].mean()
    total = sub["pnl"].sum()
    print(f"  {rule}: {len(sub):>6} trades  WR={wr:.1f}%  avgPnL=¥{avg:>+,.0f}  totalPnL=¥{total:>+,.0f}")
t2 = r2["pnl"].sum()
print(f"  TOTAL: {len(r2)} trades  totalPnL=¥{t2:>+,.0f}")

# 差分
print(f"\n=== 差分 ===")
print(f"  ルール別 - 統一10日 = ¥{t2 - t1:>+,.0f}")
for rule in ["B4", "B1", "B3", "B2"]:
    s1 = r1[r1["rule"] == rule]["pnl"].sum() if not r1.empty else 0
    s2 = r2[r2["rule"] == rule]["pnl"].sum() if not r2.empty else 0
    print(f"  {rule}: ¥{s2 - s1:>+,.0f}")
