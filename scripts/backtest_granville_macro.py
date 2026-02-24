#!/usr/bin/env python3
"""
グランビル押し目買い × マクロレジーム統合バックテスト
meta.parquet 73銘柄 × 日足 × SMA 5,20

マクロフィルター:
  - 市場トレンド: 日経225 > SMA20
  - CI先行指数: 3ヶ月変化率 > 0 = 景気拡大局面
  - DI一致指数: > 50 = 景気拡大
  - JGB10年利回り: トレンド方向
  - USD/JPY: 円安/円高トレンド
  - 失業率: 改善/悪化

データソース:
  prices_max_1d.parquet, index_prices_max_1d.parquet, meta.parquet
  improvement/data/macro/*.parquet (e-Stat, FRED)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO_DIR = ROOT / "improvement" / "data" / "macro"


# ========== データロード ==========

def load_prices():
    """meta.parquet 73銘柄 + 日経225"""
    m = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 日経225
    idx = pd.read_parquet(ROOT / "data" / "parquet" / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]

    return ps, nk, m


def load_macro() -> pd.DataFrame:
    """マクロデータを月次テーブルにマージ"""
    frames = []

    # CI先行指数
    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading", "coincident"]].rename(
        columns={"leading": "ci_leading", "coincident": "ci_coincident"}
    )
    # 3ヶ月変化率
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)
    ci["ci_coincident_chg3m"] = ci["ci_coincident"].diff(3)
    frames.append(ci)

    # DI一致指数
    di = pd.read_parquet(MACRO_DIR / "estat_di_index.parquet")
    di = di[["date", "di_leading", "di_coincident"]]
    frames.append(di)

    # FRED: JGB利回り、失業率等
    fred = pd.read_parquet(MACRO_DIR / "fred_japan_macro.parquet")
    fred = fred[["date", "jgb_10y_yield", "unemployment_rate"]].copy()
    fred["jgb10y_chg3m"] = fred["jgb_10y_yield"].diff(3)
    fred["unemp_chg3m"] = fred["unemployment_rate"].diff(3)
    frames.append(fred)

    # USD/JPY
    fx = pd.read_parquet(MACRO_DIR / "fred_usdjpy.parquet")
    fx["usdjpy_chg3m"] = fx["usdjpy"].diff(3)
    frames.append(fx)

    # 全マージ（月次 outer join）
    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on="date", how="outer")
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


def macro_to_daily(macro: pd.DataFrame, daily_dates: pd.Series) -> pd.DataFrame:
    """月次マクロデータを日次にマッピング（月初 → 翌月初まで前方充填）"""
    daily = pd.DataFrame({"date": daily_dates.drop_duplicates().sort_values()})
    daily = daily.merge(macro, on="date", how="left")
    daily = daily.sort_values("date").ffill()
    return daily


# ========== テクニカル指標 ==========

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """テクニカル指標追加"""
    df = df.copy()
    g = df.groupby("ticker")

    df["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    df["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    df["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    df["sma20_up"] = df["sma20_slope"] > 0

    df["sma5_above_sma20"] = df["sma5"] > df["sma20"]
    df["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)

    df["dev_from_sma20"] = (df["Close"] - df["sma20"]) / df["sma20"] * 100
    df["prev_dev"] = g["dev_from_sma20"].shift(1)
    df["prev_close"] = g["Close"].shift(1)

    # RSI(14)
    avg_gain = g["Close"].transform(lambda x: x.diff().clip(lower=0).rolling(14).mean())
    avg_loss = g["Close"].transform(lambda x: (-x.diff()).clip(lower=0).rolling(14).mean())
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))

    # 将来リターン
    for n in [1, 3, 5, 10]:
        df[f"ret_{n}d"] = g["Close"].transform(lambda x: x.shift(-n) / x - 1) * 100

    return df


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """エントリーシグナル検出"""
    df = df.copy()
    signals = pd.Series("", index=df.index)

    dev = df["dev_from_sma20"]
    prev_dev = df["prev_dev"]
    sma_up = df["sma20_up"]
    above = df["Close"] > df["sma20"]

    # A: 押し目買い（乖離-3~-8% + 反発）
    entry_a = (dev.between(-8, -3)) & (df["Close"] > df["prev_close"])
    signals[entry_a] = "A_dip_buy"

    # B: SMA支持反発
    entry_b = (
        sma_up & above &
        (dev.between(0, 2)) & (prev_dev <= 0.5) &
        (df["Close"] > df["prev_close"])
    )
    signals[entry_b] = "B_sma_support"

    # C: ミニGC（SMA5 > SMA20 クロス）
    entry_c = (
        df["sma5_above_sma20"] &
        ~df["prev_sma5_above"].fillna(False).astype(bool)
    )
    signals[entry_c] = "C_mini_gc"

    # D: 深押し買い（-5%以上乖離 + 反発）
    entry_d = (dev <= -5) & (df["Close"] > df["prev_close"])
    signals[entry_d] = "D_deep_dip"

    df["signal"] = signals
    return df


# ========== 評価 ==========

def evaluate(df: pd.DataFrame) -> pd.DataFrame:
    """シグナル別リターン統計"""
    sig = df[df["signal"] != ""].copy()
    if sig.empty:
        return pd.DataFrame()

    results = []
    for sn in sorted(sig["signal"].unique()):
        s = sig[sig["signal"] == sn]
        for n in [3, 5, 10]:
            col = f"ret_{n}d"
            rets = s[col].dropna()
            if len(rets) < 10:
                continue
            results.append({
                "signal": sn,
                "days": n,
                "n": len(rets),
                "mean": round(rets.mean(), 3),
                "median": round(rets.median(), 3),
                "win%": round((rets > 0).mean() * 100, 1),
                "pf": round(
                    rets[rets > 0].sum() / abs(rets[rets <= 0].sum()), 2
                ) if (rets <= 0).any() and rets[rets <= 0].sum() != 0 else 999,
                "sharpe": round(rets.mean() / rets.std(), 3) if rets.std() > 0 else 0,
            })
    return pd.DataFrame(results)


def print_eval(results: pd.DataFrame, title: str):
    """結果表示（5日リターン中心）"""
    print(f"\n{'='*70}")
    print(title)
    print(f"{'='*70}")
    for n in [5]:
        r = results[results["days"] == n].sort_values("mean", ascending=False)
        if r.empty:
            continue
        print(f"  {'シグナル':<16s} {'件数':>6s} {'平均%':>7s} {'中央%':>7s} {'勝率':>6s} {'PF':>6s} {'Sharpe':>7s}")
        print(f"  {'-'*56}")
        for _, row in r.iterrows():
            print(
                f"  {row['signal']:<16s} {row['n']:>6,d} {row['mean']:>+7.3f} "
                f"{row['median']:>+7.3f} {row['win%']:>5.1f}% "
                f"{row['pf']:>5.2f} {row['sharpe']:>7.3f}"
            )


# ========== メイン ==========

def main():
    print("=" * 70)
    print("グランビル押し目買い × マクロレジーム統合バックテスト")
    print("meta.parquet 73銘柄 × 日足 × SMA 5,20")
    print("=" * 70)

    # --- データロード ---
    print("\n[1] データロード...")
    prices, nk, meta = load_prices()
    macro = load_macro()
    print(f"  銘柄数: {prices['ticker'].nunique()}")
    print(f"  価格期間: {prices['date'].min().date()} ~ {prices['date'].max().date()}")
    print(f"  マクロ期間: {macro['date'].min().date()} ~ {macro['date'].max().date()}")
    print(f"  マクロカラム: {[c for c in macro.columns if c != 'date']}")

    # --- テクニカル指標 ---
    print("\n[2] テクニカル指標計算...")
    prices = add_features(prices)
    prices = prices.dropna(subset=["sma20"])

    # 日経マージ
    prices = prices.merge(
        nk[["date", "market_uptrend"]], on="date", how="left"
    )

    # マクロデータを日次にマッピング
    daily_macro = macro_to_daily(macro, prices["date"])
    prices = prices.merge(daily_macro, on="date", how="left")

    # --- マクロレジーム列を作成 ---
    print("\n[3] マクロレジーム分類...")

    # CI先行指数: 3ヶ月変化 > 0 = 拡大局面
    prices["macro_ci_expand"] = prices["ci_leading_chg3m"] > 0

    # DI一致 > 50 = 景気拡大
    prices["macro_di_expand"] = prices["di_coincident"] > 50

    # JGB10年利回り: 3ヶ月変化 > 0 = 金利上昇
    prices["macro_rate_up"] = prices["jgb10y_chg3m"] > 0

    # USD/JPY: 3ヶ月変化 > 0 = 円安
    prices["macro_yen_weak"] = prices["usdjpy_chg3m"] > 0

    # 失業率: 3ヶ月変化 < 0 = 改善
    prices["macro_unemp_improve"] = prices["unemp_chg3m"] < 0

    # データカバレッジ確認
    for col in ["macro_ci_expand", "macro_di_expand", "macro_rate_up", "macro_yen_weak", "macro_unemp_improve"]:
        valid = prices[col].notna().sum()
        true_pct = prices[col].mean() * 100 if valid > 0 else 0
        print(f"  {col}: {valid:,} valid ({true_pct:.0f}% True)")

    # --- シグナル検出 ---
    print("\n[4] シグナル検出...")
    prices = detect_signals(prices)
    sig_counts = prices[prices["signal"] != ""]["signal"].value_counts()
    print(f"  シグナル総数: {sig_counts.sum():,}")
    for name, cnt in sig_counts.items():
        print(f"    {name}: {cnt:,}")

    # --- 評価 ---
    print("\n[5] フィルター別評価（5日リターン）...")

    # 1. ベースライン（フィルターなし）
    r1 = evaluate(prices)
    print_eval(r1, "1. ベースライン（フィルターなし・全期間）")

    # 2. 市場トレンドのみ（日経 > SMA20）
    up = prices[prices["market_uptrend"] == True]
    r2 = evaluate(up)
    print_eval(r2, "2. 市場トレンド: 日経225 > SMA20")

    # 3. 市場トレンド + CI先行指数拡大
    f3 = up[up["macro_ci_expand"] == True]
    r3 = evaluate(f3)
    print_eval(r3, "3. 日経上昇 + CI先行指数拡大（3ヶ月↑）")

    # 4. 市場トレンド + DI一致>50
    f4 = up[up["macro_di_expand"] == True]
    r4 = evaluate(f4)
    print_eval(r4, "4. 日経上昇 + DI一致 > 50（景気拡大）")

    # 5. 市場トレンド + 円安
    f5 = up[up["macro_yen_weak"] == True]
    r5 = evaluate(f5)
    print_eval(r5, "5. 日経上昇 + 円安トレンド（3ヶ月）")

    # 6. 市場トレンド + 金利低下（緩和局面）
    f6 = up[up["macro_rate_up"] == False]
    r6 = evaluate(f6)
    print_eval(r6, "6. 日経上昇 + 金利低下（緩和局面）")

    # 7. 市場トレンド + 失業率改善
    f7 = up[up["macro_unemp_improve"] == True]
    r7 = evaluate(f7)
    print_eval(r7, "7. 日経上昇 + 失業率改善")

    # 8. ベストコンボ: 日経上昇 + CI拡大 + 円安
    f8 = up[(up["macro_ci_expand"] == True) & (up["macro_yen_weak"] == True)]
    r8 = evaluate(f8)
    print_eval(r8, "8. 日経上昇 + CI拡大 + 円安")

    # 9. ベストコンボ: 日経上昇 + CI拡大 + DI>50
    f9 = up[(up["macro_ci_expand"] == True) & (up["macro_di_expand"] == True)]
    r9 = evaluate(f9)
    print_eval(r9, "9. 日経上昇 + CI拡大 + DI>50")

    # 10. 逆環境（比較用）: 日経下落 + CI縮小
    f10 = prices[(prices["market_uptrend"] == False) & (prices["macro_ci_expand"] == False)]
    r10 = evaluate(f10)
    print_eval(r10, "10. 逆環境: 日経下落 + CI縮小（比較用）")

    # --- 直近5年 × ベストフィルター ---
    print(f"\n{'='*70}")
    print("直近5年（2021-2026）× 各フィルター比較")
    print(f"{'='*70}")

    recent = prices[prices["date"] >= "2021-01-01"]
    recent_up = recent[recent["market_uptrend"] == True]

    combos = {
        "日経上昇のみ": recent_up,
        "＋CI拡大": recent_up[recent_up["macro_ci_expand"] == True],
        "＋DI>50": recent_up[recent_up["macro_di_expand"] == True],
        "＋円安": recent_up[recent_up["macro_yen_weak"] == True],
        "＋金利低下": recent_up[recent_up["macro_rate_up"] == False],
        "＋CI拡大＋円安": recent_up[
            (recent_up["macro_ci_expand"] == True) & (recent_up["macro_yen_weak"] == True)
        ],
    }

    # A_dip_buy の5日リターンで横比較
    print(f"\n  A_dip_buy 5日リターン比較:")
    print(f"  {'フィルター':<20s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s}")
    print(f"  {'-'*48}")
    for label, subset in combos.items():
        sig = subset[(subset["signal"] == "A_dip_buy")]
        rets = sig["ret_5d"].dropna()
        if len(rets) < 5:
            print(f"  {label:<20s} {len(rets):>6d}   (データ不足)")
            continue
        pf = round(rets[rets > 0].sum() / abs(rets[rets <= 0].sum()), 2) if (rets <= 0).any() and rets[rets <= 0].sum() != 0 else 999
        print(
            f"  {label:<20s} {len(rets):>6,d} {rets.mean():>+7.3f} "
            f"{(rets > 0).mean() * 100:>5.1f}% {pf:>5.2f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
