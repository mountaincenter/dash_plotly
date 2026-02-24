#!/usr/bin/env python3
"""
上昇トレンド中の押し目買い戦略 バックテスト
SMA 5, 20 × 静的銘柄(meta.parquet 73銘柄) × 日足

戦略ロジック:
  地合いフィルター: 日経225がSMA20の上 = 上昇トレンド
  エントリー条件（3パターン）:
    A) 押し目買い: 個別銘柄がSMA20から-3%以上乖離 → 買い
    B) SMA支持反発: SMA20上向き + 価格がSMA20に接近して反発
    C) GC回帰: SMA5がSMA20を下→上にクロス（個別銘柄のミニGC）
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_data():
    """データロード"""
    m = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    static_tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(static_tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 日経225
    idx = pd.read_parquet(ROOT / "data" / "parquet" / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["nk225_sma60"] = nk["nk225_close"].rolling(60).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]
    nk["market_strong_uptrend"] = (
        (nk["nk225_close"] > nk["nk225_sma20"]) &
        (nk["nk225_sma20"] > nk["nk225_sma60"])
    )

    return ps, nk, m


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """テクニカル指標追加"""
    df = df.copy()
    g = df.groupby("ticker")

    df["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    df["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    df["sma60"] = g["Close"].transform(lambda x: x.rolling(60).mean())

    # SMA方向
    df["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    df["sma20_up"] = df["sma20_slope"] > 0

    # SMA5 vs SMA20 のクロス
    df["sma5_above_sma20"] = df["sma5"] > df["sma20"]
    df["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)

    # 乖離率
    df["dev_from_sma20"] = (df["Close"] - df["sma20"]) / df["sma20"] * 100
    df["prev_dev"] = g["dev_from_sma20"].shift(1)

    # 前日の価格位置
    df["prev_close"] = g["Close"].shift(1)
    df["prev_above_sma20"] = (df["prev_close"] > g["sma20"].shift(1))

    # RSI(14)
    delta = g["Close"].transform(lambda x: x.diff())
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = g["Close"].transform(lambda x: x.diff().clip(lower=0).rolling(14).mean())
    avg_loss = g["Close"].transform(lambda x: (-x.diff()).clip(lower=0).rolling(14).mean())
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))

    # 出来高比率
    df["vol_sma20"] = g["Volume"].transform(lambda x: x.rolling(20).mean())
    df["vol_ratio"] = df["Volume"] / df["vol_sma20"].replace(0, np.nan)

    # 将来リターン
    for n in [1, 3, 5, 10]:
        df[f"ret_{n}d"] = g["Close"].transform(lambda x: x.shift(-n) / x - 1) * 100

    return df


def detect_long_signals(df: pd.DataFrame) -> pd.DataFrame:
    """ロングエントリーシグナル検出"""
    df = df.copy()
    signals = pd.Series("", index=df.index)

    above_sma20 = df["Close"] > df["sma20"]
    sma_up = df["sma20_up"]
    dev = df["dev_from_sma20"]
    prev_dev = df["prev_dev"]

    # --- エントリーA: 押し目買い（乖離-3%以上から反発） ---
    # SMA20から-3%〜-8%乖離 + 当日は前日より上（反発の兆し）
    entry_a = (
        (dev.between(-8, -3)) &
        (df["Close"] > df["prev_close"])
    )
    signals[entry_a] = "A_dip_buy"

    # --- エントリーB: SMA支持反発 ---
    # SMA20上向き + 乖離0〜2% + 前日は乖離0%以下（SMA20にタッチして反発）
    entry_b = (
        sma_up &
        above_sma20 &
        (dev.between(0, 2)) &
        (prev_dev <= 0.5) &
        (df["Close"] > df["prev_close"])
    )
    signals[entry_b] = "B_sma_support"

    # --- エントリーC: ミニGC（SMA5がSMA20を上抜け） ---
    entry_c = (
        df["sma5_above_sma20"] &
        ~df["prev_sma5_above"].fillna(False).astype(bool)
    )
    signals[entry_c] = "C_mini_gc"

    # --- エントリーD: 深押し買い（-5%以上乖離、buy4相当） ---
    entry_d = (
        (dev <= -5) &
        (df["Close"] > df["prev_close"])
    )
    signals[entry_d] = "D_deep_dip"

    df["signal"] = signals
    return df


def evaluate(df: pd.DataFrame, label: str = "") -> pd.DataFrame:
    """シグナル別評価"""
    sig = df[df["signal"] != ""].copy()
    if sig.empty:
        return pd.DataFrame()

    results = []
    for signal_name in sorted(sig["signal"].unique()):
        s = sig[sig["signal"] == signal_name]
        for n in [1, 3, 5, 10]:
            col = f"ret_{n}d"
            rets = s[col].dropna()
            if len(rets) == 0:
                continue
            results.append({
                "signal": signal_name,
                "holding_days": n,
                "count": len(rets),
                "mean_ret_pct": round(rets.mean(), 3),
                "median_ret_pct": round(rets.median(), 3),
                "win_rate_pct": round((rets > 0).mean() * 100, 1),
                "avg_win_pct": round(rets[rets > 0].mean(), 3) if (rets > 0).any() else 0,
                "avg_loss_pct": round(rets[rets <= 0].mean(), 3) if (rets <= 0).any() else 0,
                "sharpe": round(rets.mean() / rets.std(), 3) if rets.std() > 0 else 0,
                "profit_factor": round(
                    rets[rets > 0].sum() / abs(rets[rets <= 0].sum()), 2
                ) if (rets <= 0).any() and rets[rets <= 0].sum() != 0 else 999,
            })
    return pd.DataFrame(results)


def print_results(results: pd.DataFrame, title: str):
    """結果表示"""
    print(f"\n{'='*75}")
    print(title)
    print(f"{'='*75}")
    for n in [1, 3, 5, 10]:
        r = results[results["holding_days"] == n].sort_values("mean_ret_pct", ascending=False)
        if r.empty:
            continue
        print(f"\n  保持{n}日:")
        print(f"  {'シグナル':<18s} {'件数':>7s} {'平均%':>8s} {'中央%':>8s} {'勝率':>6s} {'PF':>6s} {'Sharpe':>7s}")
        print(f"  {'-'*60}")
        for _, row in r.iterrows():
            print(
                f"  {row['signal']:<18s} {row['count']:>7,d} {row['mean_ret_pct']:>+8.3f} "
                f"{row['median_ret_pct']:>+8.3f} {row['win_rate_pct']:>5.1f}% "
                f"{row['profit_factor']:>5.2f} {row['sharpe']:>7.3f}"
            )


def main():
    print("=" * 75)
    print("上昇トレンド中の押し目買い戦略 バックテスト")
    print("meta.parquet 73銘柄 × 日足 × SMA 5, 20")
    print("=" * 75)

    prices, nk, meta = load_data()
    print(f"銘柄数: {prices['ticker'].nunique()}")
    print(f"期間: {prices['date'].min().date()} ~ {prices['date'].max().date()}")

    prices = add_features(prices)
    prices = prices.dropna(subset=["sma20"])

    # 日経データをマージ
    prices = prices.merge(
        nk[["date", "market_uptrend", "market_strong_uptrend"]],
        on="date", how="left"
    )

    # シグナル検出
    prices = detect_long_signals(prices)
    sig_counts = prices[prices["signal"] != ""]["signal"].value_counts()
    print(f"\nシグナル総数: {sig_counts.sum():,}")
    for name, cnt in sig_counts.items():
        print(f"  {name}: {cnt:,}")

    # ========== 1. フィルターなし（ベースライン） ==========
    results_all = evaluate(prices)
    print_results(results_all, "1. フィルターなし（全期間ベースライン）")

    # ========== 2. 地合いフィルター: 日経 > SMA20 ==========
    up = prices[prices["market_uptrend"] == True]
    results_up = evaluate(up)
    print_results(results_up, "2. 地合いフィルター: 日経225 > SMA20（上昇トレンド）")

    # ========== 3. 強い上昇トレンド: 日経 > SMA20 & SMA20 > SMA60 ==========
    strong = prices[prices["market_strong_uptrend"] == True]
    results_strong = evaluate(strong)
    print_results(results_strong, "3. 強い上昇トレンド: 日経 > SMA20 & SMA20 > SMA60")

    # ========== 4. 下降トレンド（比較用） ==========
    down = prices[prices["market_uptrend"] == False]
    results_down = evaluate(down)
    print_results(results_down, "4. 比較: 日経225 < SMA20（下降トレンド時）")

    # ========== 5. 直近5年 × 上昇トレンド ==========
    recent_up = up[up["date"] >= "2021-01-01"]
    results_recent_up = evaluate(recent_up)
    print_results(results_recent_up, "5. 直近5年 × 上昇トレンド（2021-2026）")

    # ========== 6. RSIフィルター追加: RSI < 40 で売られすぎ ==========
    rsi_filter = up[up["rsi14"] < 40]
    results_rsi = evaluate(rsi_filter)
    print_results(results_rsi, "6. 上昇トレンド + RSI < 40（売られすぎ）")

    # ========== 7. 出来高フィルター: 出来高が平均以上 ==========
    vol_filter = up[up["vol_ratio"] >= 1.0]
    results_vol = evaluate(vol_filter)
    print_results(results_vol, "7. 上昇トレンド + 出来高 >= 平均")

    # ========== 8. 複合フィルター: 上昇トレンド + RSI<50 + 出来高>=平均 ==========
    combo = up[(up["rsi14"] < 50) & (up["vol_ratio"] >= 1.0)]
    results_combo = evaluate(combo)
    print_results(results_combo, "8. 複合: 上昇トレンド + RSI<50 + 出来高>=平均")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
