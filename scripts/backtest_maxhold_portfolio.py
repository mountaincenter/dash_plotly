#!/usr/bin/env python3
"""MAX_HOLD検証（ポートフォリオシミュレーション、資金制約あり）
10日統一 vs ルール別(B1=7,B2=30,B3=5,B4=13)
"""
import pandas as pd
import numpy as np
from pathlib import Path

PARQUET = Path(__file__).resolve().parents[1] / "data" / "parquet" / "prices_max_1d.parquet"
CAPITAL = 4_650_000
MARGIN_LIMIT_PCT = 0.15  # 1銘柄集中制限15%
RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}

# 証拠金テーブル
_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 80), (700, 100),
    (1000, 150), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
    (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
    (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
    (500000, 70000), (700000, 100000), (1000000, 150000),
]

def upper_limit(price):
    for threshold, limit in _LIMIT_TABLE:
        if price * 10 <= threshold:
            return limit * 100
    return 150000 * 100

def required_margin(price):
    return upper_limit(price)

print("Loading prices...")
p = pd.read_parquet(PARQUET)
p["date"] = pd.to_datetime(p["date"])
p = p.sort_values(["ticker", "date"]).reset_index(drop=True)
print(f"  {p['ticker'].nunique()} tickers, {len(p):,} rows")

# テクニカル指標
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

# シグナル検出
print("Detecting signals...")
valid = p["sma20"].notna() & p["prev_close"].notna()
b4 = valid & (p["dev"] < -8) & p["up_day"]
b1 = valid & p["prev_below"] & p["above"] & p["sma_up"]
b3 = valid & p["sma_up"] & (p["dev"] >= 0) & (p["dev"] <= 3) & p["above"] & (p["prev_dev"] > p["dev"]) & p["up_day"]
b2 = valid & p["sma_up"] & (p["dev"] >= -5) & (p["dev"] <= 0) & p["below"] & p["up_day"]
p["rule"] = None
p.loc[b2, "rule"] = "B2"
p.loc[b3, "rule"] = "B3"
p.loc[b1, "rule"] = "B1"
p.loc[b4, "rule"] = "B4"

signals = p[p["rule"].notna()][["date", "ticker", "rule", "Close"]].copy()
cutoff = p["date"].max() - pd.Timedelta(days=730)
signals = signals[signals["date"] >= cutoff].reset_index(drop=True)
print(f"  Signals: {len(signals):,}")

# 銘柄ごとの価格データをdict化（高速化）
print("Building price lookup...")
price_lookup = {}
for ticker, tk in p.groupby("ticker"):
    tk = tk.reset_index(drop=True)
    dates = tk["date"].values.astype("datetime64[ns]")
    price_lookup[ticker] = {
        "dates": dates,
        "Open": tk["Open"].values,
        "High": tk["High"].values,
        "Low": tk["Low"].values,
        "Close": tk["Close"].values,
    }
    # 20日高値プリコンピュート
    highs = tk["High"].values
    n = len(highs)
    h20 = np.full(n, np.nan)
    for i in range(19, n):
        h20[i] = highs[i-19:i+1].max()
    price_lookup[ticker]["high20"] = h20


def simulate_portfolio(signals_df, max_hold_map, capital):
    """資金制約ありのポートフォリオシミュレーション"""
    # シグナルを日付→優先順位でソート
    signals_df = signals_df.copy()
    signals_df["_priority"] = signals_df["rule"].map(RULE_PRIORITY)
    signals_df = signals_df.sort_values(["date", "_priority"]).reset_index(drop=True)

    # 日別にグループ化
    dates_all = sorted(signals_df["date"].unique())

    open_positions = []  # [(ticker, entry_date_idx, entry_price, margin, rule, max_hold)]
    closed_trades = []
    available = capital

    for sig_date in dates_all:
        sig_date_np = np.datetime64(sig_date)
        day_signals = signals_df[signals_df["date"] == sig_date]

        # 1. 既存ポジションのexit判定（シグナル翌営業日=エントリー日ベース）
        still_open = []
        for pos in open_positions:
            tk_ticker, entry_idx, ep, margin, rule, mh = pos
            pl = price_lookup.get(tk_ticker)
            if pl is None:
                still_open.append(pos)
                continue

            # 今日の日付インデックス
            today_idx = np.searchsorted(pl["dates"], sig_date_np, side="left")
            if today_idx >= len(pl["dates"]) or pl["dates"][today_idx] != sig_date_np:
                still_open.append(pos)
                continue

            hold_days = today_idx - entry_idx
            exited = False

            # 20日高値exit
            if hold_days > 0 and today_idx >= 19:
                if pl["High"][today_idx] >= pl["high20"][today_idx]:
                    exit_price = pl["Close"][today_idx]
                    pnl = (exit_price - ep) * 100
                    closed_trades.append({"rule": rule, "pnl": pnl, "hold_days": hold_days, "exit_type": "20d_high"})
                    available += margin
                    exited = True

            # MAX_HOLD
            if not exited and hold_days >= mh:
                exit_price = pl["Close"][today_idx]
                pnl = (exit_price - ep) * 100
                closed_trades.append({"rule": rule, "pnl": pnl, "hold_days": hold_days, "exit_type": "max_hold"})
                available += margin
                exited = True

            if not exited:
                still_open.append(pos)

        open_positions = still_open

        # 2. 新規エントリー（翌営業日Open）
        for _, sig in day_signals.iterrows():
            ticker = sig["ticker"]
            rule = sig["rule"]
            mh = max_hold_map[rule]
            pl = price_lookup.get(ticker)
            if pl is None:
                continue

            # エントリー日（シグナル翌営業日）
            entry_idx = np.searchsorted(pl["dates"], sig_date_np, side="right")
            if entry_idx >= len(pl["dates"]):
                continue
            ep = pl["Open"][entry_idx]
            if np.isnan(ep) or ep <= 0:
                continue

            # 既に保有中の銘柄はスキップ
            if any(pos[0] == ticker for pos in open_positions):
                continue

            margin = required_margin(ep)
            # 集中制限15%
            if margin > capital * MARGIN_LIMIT_PCT:
                continue
            # 残余力チェック
            if margin > available:
                continue

            available -= margin
            open_positions.append((ticker, entry_idx, ep, margin, rule, mh))

    # 未決済ポジションは無視（期間末）
    return pd.DataFrame(closed_trades)


# 1. 全ルール10日統一
print("\n=== 全ルール10日統一（ポートフォリオ） ===")
mh1 = {"B4": 10, "B1": 10, "B3": 10, "B2": 10}
r1 = simulate_portfolio(signals, mh1, CAPITAL)
for rule in ["B4", "B1", "B3", "B2"]:
    sub = r1[r1["rule"] == rule]
    if sub.empty:
        continue
    wr = (sub["pnl"] > 0).mean() * 100
    print(f"  {rule}: {len(sub):>5} trades  WR={wr:.1f}%  avgPnL=¥{sub['pnl'].mean():>+,.0f}  totalPnL=¥{sub['pnl'].sum():>+,.0f}  avgHold={sub['hold_days'].mean():.1f}d")
t1 = r1["pnl"].sum()
print(f"  TOTAL: {len(r1)} trades  totalPnL=¥{t1:>+,.0f}  資金効率={t1/CAPITAL*100:.1f}%")

# 2. ルール別MAX_HOLD
print("\n=== ルール別MAX_HOLD (B1=7, B2=30, B3=5, B4=13)（ポートフォリオ） ===")
mh2 = {"B4": 13, "B1": 7, "B3": 5, "B2": 30}
r2 = simulate_portfolio(signals, mh2, CAPITAL)
for rule in ["B4", "B1", "B3", "B2"]:
    sub = r2[r2["rule"] == rule]
    if sub.empty:
        continue
    wr = (sub["pnl"] > 0).mean() * 100
    print(f"  {rule}: {len(sub):>5} trades  WR={wr:.1f}%  avgPnL=¥{sub['pnl'].mean():>+,.0f}  totalPnL=¥{sub['pnl'].sum():>+,.0f}  avgHold={sub['hold_days'].mean():.1f}d")
t2 = r2["pnl"].sum()
print(f"  TOTAL: {len(r2)} trades  totalPnL=¥{t2:>+,.0f}  資金効率={t2/CAPITAL*100:.1f}%")

# 差分
print(f"\n=== 差分 ===")
print(f"  ルール別 - 統一10日 = ¥{t2 - t1:>+,.0f}")
print(f"  トレード数: {len(r1)} → {len(r2)} ({len(r2) - len(r1):+d})")
for rule in ["B4", "B1", "B3", "B2"]:
    s1 = r1[r1["rule"] == rule]["pnl"].sum() if not r1.empty else 0
    s2 = r2[r2["rule"] == rule]["pnl"].sum() if not r2.empty else 0
    n1 = len(r1[r1["rule"] == rule])
    n2 = len(r2[r2["rule"] == rule])
    print(f"  {rule}: ¥{s2 - s1:>+,.0f} ({n1}→{n2} trades)")
