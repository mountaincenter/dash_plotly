#!/usr/bin/env python3
"""
グランビルの法則バックテスト
SMA 5, 20 × 静的銘柄(Core30 + Large70) × 日足

グランビルの法則 8シグナル:
  買い1: SMA20上向き + 価格がSMA20を下→上に抜ける（ゴールデンクロス的）
  買い2: SMA20上向き + 価格がSMA20を下回るも再度上抜け（押し目）
  買い3: SMA20上向き + 価格がSMA20に接近して反発（SMA支持）
  買い4: SMA20下向き + 価格がSMA20から大きく乖離して下落（逆張り）

  売り1: SMA20下向き + 価格がSMA20を上→下に抜ける（デッドクロス的）
  売り2: SMA20下向き + 価格がSMA20を上回るも再度下抜け（戻り売り）
  売り3: SMA20下向き + 価格がSMA20に接近して反落（SMA抵抗）
  売り4: SMA20上向き + 価格がSMA20から大きく乖離して上昇（逆張り）

評価: シグナル発生後 1日/3日/5日/10日のリターン
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_static_prices() -> pd.DataFrame:
    """静的銘柄（meta.parquet: 政策+Core30等 73銘柄）の日足データを取得"""
    m = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    static_tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(static_tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    return ps


def add_sma_features(df: pd.DataFrame) -> pd.DataFrame:
    """SMA 5, 20 と関連指標を追加"""
    df = df.copy()
    g = df.groupby("ticker")

    df["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    df["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())

    # SMA20のトレンド方向（3日間の変化で判定）
    df["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    df["sma20_up"] = df["sma20_slope"] > 0
    df["sma20_down"] = df["sma20_slope"] < 0

    # 前日の位置関係
    df["prev_close"] = g["Close"].shift(1)
    df["prev_sma20"] = g["sma20"].shift(1)
    df["prev_above_sma20"] = g.apply(
        lambda x: (x["Close"] > x["sma20"]).shift(1)
    ).reset_index(level=0, drop=True)

    # 乖離率
    df["deviation_pct"] = (df["Close"] - df["sma20"]) / df["sma20"] * 100

    # SMA20との距離（前日）
    df["prev_deviation"] = g["deviation_pct"].shift(1)

    # 将来リターン
    for n in [1, 3, 5, 10]:
        df[f"ret_{n}d"] = g["Close"].transform(
            lambda x: x.shift(-n) / x - 1
        ) * 100

    return df


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """グランビルの法則 8シグナルを検出"""
    above = df["Close"] > df["sma20"]
    prev_above = df["prev_above_sma20"].fillna(False).astype(bool)
    sma_up = df["sma20_up"]
    sma_down = df["sma20_down"]
    dev = df["deviation_pct"]
    prev_dev = df["prev_deviation"]

    signals = pd.Series("", index=df.index)

    # --- 買いシグナル ---
    # 買い1: SMA20上向き + 下→上クロス
    buy1 = sma_up & above & ~prev_above
    signals[buy1] = "buy1_cross_up"

    # 買い2: SMA20上向き + 一旦下回った後に再上抜け（前日乖離が-2%〜0%から上抜け）
    buy2 = sma_up & above & ~prev_above & (prev_dev.between(-3, 0))
    # buy2はbuy1と重複するが、押し目からの復帰を区別
    # buy1後の再エントリーを買い2とする
    signals[buy2] = "buy2_pushback"

    # 買い3: SMA20上向き + SMA20に接近して反発（乖離0〜2%、前日は0%以下）
    buy3 = sma_up & above & (dev.between(0, 2)) & (prev_dev <= 0.5) & prev_above
    signals[buy3] = "buy3_support"

    # 買い4: SMA20下向き + 大きく乖離して下落（逆張り、乖離-5%以下）
    buy4 = sma_down & (dev <= -5)
    signals[buy4] = "buy4_contrarian"

    # --- 売りシグナル ---
    # 売り1: SMA20下向き + 上→下クロス
    sell1 = sma_down & ~above & prev_above
    signals[sell1] = "sell1_cross_down"

    # 売り2: SMA20下向き + 一旦上回った後に再下抜け（戻り売り）
    sell2 = sma_down & ~above & prev_above & (prev_dev.between(0, 3))
    signals[sell2] = "sell2_pullback"

    # 売り3: SMA20下向き + SMA20に接近して反落（抵抗確認）
    sell3 = sma_down & ~above & (dev.between(-2, 0)) & (prev_dev >= -0.5) & ~prev_above
    signals[sell3] = "sell3_resistance"

    # 売り4: SMA20上向き + 大きく乖離して上昇（逆張り、乖離+5%以上）
    sell4 = sma_up & (dev >= 5)
    signals[sell4] = "sell4_contrarian"

    df = df.copy()
    df["signal"] = signals
    return df


def evaluate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """シグナル別のリターン統計"""
    sig = df[df["signal"] != ""].copy()

    if sig.empty:
        print("[WARN] シグナルが0件")
        return pd.DataFrame()

    results = []
    for signal_name in sorted(sig["signal"].unique()):
        s = sig[sig["signal"] == signal_name]
        for n in [1, 3, 5, 10]:
            col = f"ret_{n}d"
            rets = s[col].dropna()
            if len(rets) == 0:
                continue

            # 売りシグナルはショート → リターン反転
            if signal_name.startswith("sell"):
                rets = -rets

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
                "max_ret_pct": round(rets.max(), 2),
                "min_ret_pct": round(rets.min(), 2),
            })

    return pd.DataFrame(results)


def evaluate_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """年別のシグナル有効性を確認"""
    sig = df[df["signal"] != ""].copy()
    sig["year"] = sig["date"].dt.year

    results = []
    for signal_name in sorted(sig["signal"].unique()):
        s = sig[sig["signal"] == signal_name]
        for year in sorted(s["year"].unique()):
            sy = s[s["year"] == year]
            rets = sy["ret_5d"].dropna()
            if len(rets) < 5:
                continue
            if signal_name.startswith("sell"):
                rets = -rets
            results.append({
                "signal": signal_name,
                "year": year,
                "count": len(rets),
                "mean_ret_pct": round(rets.mean(), 3),
                "win_rate_pct": round((rets > 0).mean() * 100, 1),
            })

    return pd.DataFrame(results)


def main():
    print("=" * 70)
    print("グランビルの法則 バックテスト")
    print("SMA 5, 20 × 静的銘柄(meta.parquet 73銘柄) × 日足")
    print("=" * 70)

    # データロード
    print("\n[1/4] データロード...")
    prices = load_static_prices()
    print(f"  銘柄数: {prices['ticker'].nunique()}")
    print(f"  レコード数: {len(prices):,}")
    print(f"  期間: {prices['date'].min().date()} ~ {prices['date'].max().date()}")

    # SMA計算
    print("\n[2/4] SMA計算...")
    prices = add_sma_features(prices)
    prices = prices.dropna(subset=["sma20"])
    print(f"  有効レコード数: {len(prices):,}")

    # シグナル検出
    print("\n[3/4] シグナル検出...")
    prices = detect_signals(prices)
    sig_counts = prices[prices["signal"] != ""]["signal"].value_counts()
    print(f"  シグナル総数: {sig_counts.sum():,}")
    for name, cnt in sig_counts.items():
        print(f"    {name}: {cnt:,}")

    # 評価
    print("\n[4/4] シグナル評価...")
    results = evaluate_signals(prices)

    if results.empty:
        print("[ERROR] 評価結果なし")
        return 1

    # 保持日数別に表示
    for n in [1, 3, 5, 10]:
        r = results[results["holding_days"] == n].sort_values("mean_ret_pct", ascending=False)
        print(f"\n{'='*70}")
        print(f"保持期間: {n}日")
        print(f"{'='*70}")
        print(f"{'シグナル':<22s} {'件数':>7s} {'平均%':>8s} {'中央%':>8s} {'勝率%':>7s} {'平均勝%':>8s} {'平均負%':>8s} {'Sharpe':>7s}")
        print("-" * 70)
        for _, row in r.iterrows():
            print(
                f"{row['signal']:<22s} {row['count']:>7,d} {row['mean_ret_pct']:>8.3f} "
                f"{row['median_ret_pct']:>8.3f} {row['win_rate_pct']:>6.1f}% "
                f"{row['avg_win_pct']:>8.3f} {row['avg_loss_pct']:>8.3f} {row['sharpe']:>7.3f}"
            )

    # 直近5年に絞った評価
    print(f"\n{'='*70}")
    print("直近5年（2021-2026）の5日リターン")
    print(f"{'='*70}")
    recent = prices[prices["date"] >= "2021-01-01"]
    recent_results = evaluate_signals(recent)
    if not recent_results.empty:
        r5 = recent_results[recent_results["holding_days"] == 5].sort_values("mean_ret_pct", ascending=False)
        print(f"{'シグナル':<22s} {'件数':>7s} {'平均%':>8s} {'勝率%':>7s} {'Sharpe':>7s}")
        print("-" * 55)
        for _, row in r5.iterrows():
            print(
                f"{row['signal']:<22s} {row['count']:>7,d} {row['mean_ret_pct']:>8.3f} "
                f"{row['win_rate_pct']:>6.1f}% {row['sharpe']:>7.3f}"
            )

    # 年別推移（有望シグナルの安定性確認）
    print(f"\n{'='*70}")
    print("年別推移（5日リターン、主要シグナル）")
    print(f"{'='*70}")
    yearly = evaluate_by_year(prices)
    if not yearly.empty:
        for sig in ["buy1_cross_up", "sell1_cross_down", "buy4_contrarian", "sell4_contrarian"]:
            sy = yearly[yearly["signal"] == sig].sort_values("year")
            if sy.empty:
                continue
            print(f"\n  {sig}:")
            for _, row in sy.tail(10).iterrows():
                bar = "+" * max(0, int(row["mean_ret_pct"] * 10)) + "-" * max(0, int(-row["mean_ret_pct"] * 10))
                print(f"    {row['year']}: {row['mean_ret_pct']:>+7.3f}%  勝率{row['win_rate_pct']:>5.1f}%  ({row['count']:>3d}件) {bar}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
