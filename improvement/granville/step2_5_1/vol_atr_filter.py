#!/usr/bin/env python3
"""
Step 2.5.1: 出来高・ATRフィルターの検証
========================================
Step 2.5（Exit戦略確定済み）に対して、出来高・ATRフィルターを
1変数ずつ追加し、エッジの変化を検証する。

原則: 変数を最小にした理論値から、1つずつ追加して差分を検証
過学習防止: 前半(2014-2019)で最適化 → 後半(2020-2026)で検証

B4(逆張り)とB1/B3(順張り)で効く方向が逆だった:
  B4: 高ボラ・高出来高で PF 改善
  B1/B3: 低ボラで PF 改善
→ これが構造的なのか過学習なのかを out-of-sample で確認

Input:  step1/trades_sl3.parquet, prices_max_1d.parquet
Output: step2_5_1/vol_atr_results.parquet, step2_5_1/vol_atr_presentation.html
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # improvement/
PARQUET_DIR = ROOT.parent / "data" / "parquet"
STEP1_DIR = ROOT / "granville" / "step1"
OUT_DIR = Path(__file__).resolve().parent

# 前半/後半の分割点
SPLIT_DATE = "2020-01-01"


def load_prices_with_indicators() -> pd.DataFrame:
    """価格データ + ATR/出来高比を計算"""
    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    p["date"] = pd.to_datetime(p["date"])
    p = p.sort_values(["ticker", "date"]).reset_index(drop=True)

    g = p.groupby("ticker")

    # ATR(14) → ATR%
    high_low = p["High"] - p["Low"]
    high_pc = abs(p["High"] - g["Close"].shift(1))
    low_pc = abs(p["Low"] - g["Close"].shift(1))
    tr = pd.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)
    p["atr14"] = tr.groupby(p["ticker"]).transform(
        lambda x: x.ewm(span=14, adjust=False).mean()
    )
    p["atr_pct"] = p["atr14"] / p["Close"] * 100

    # 出来高 20日平均比
    p["vol_ma20"] = g["Volume"].transform(lambda x: x.rolling(20).mean())
    p["vol_ratio"] = p["Volume"] / p["vol_ma20"]

    return p[["ticker", "date", "atr_pct", "vol_ratio"]]


def calc_pf(df: pd.DataFrame) -> dict:
    """PF・勝率・平均損益を計算"""
    if len(df) < 30:
        return {"n": len(df), "pf": 0, "wr": 0, "avg_pnl": 0, "total_man": 0}
    w = df[df["ret_pct"] > 0]["ret_pct"].sum()
    l = abs(df[df["ret_pct"] <= 0]["ret_pct"].sum())
    pf = round(w / l, 3) if l > 0 else 999
    return {
        "n": len(df),
        "pf": pf,
        "wr": round(df["win"].mean() * 100, 1),
        "avg_pnl": round(df["pnl"].mean()),
        "total_man": round(df["pnl"].sum() / 10000),
    }


def sensitivity_analysis(
    trades: pd.DataFrame,
    col: str,
    thresholds: list[float],
    direction: str,  # "above" or "below"
    label: str,
) -> list[dict]:
    """連続値の閾値を動かして PF の感度を分析"""
    results = []
    for th in thresholds:
        if direction == "above":
            filtered = trades[trades[col] >= th]
            excluded = trades[trades[col] < th]
        else:
            filtered = trades[trades[col] <= th]
            excluded = trades[trades[col] > th]

        m = calc_pf(filtered)
        m_ex = calc_pf(excluded)
        m.update({
            "filter": label,
            "threshold": th,
            "direction": direction,
            "col": col,
            "pf_excluded": m_ex["pf"],
            "n_excluded": m_ex["n"],
        })
        results.append(m)
    return results


def main():
    t0 = time.time()
    print("=" * 80)
    print("Step 2.5.1: 出来高・ATRフィルター検証")
    print("=" * 80)

    # データ読み込み
    print("\n[1/4] Loading data...")
    tech = load_prices_with_indicators()
    trades = pd.read_parquet(STEP1_DIR / "trades_sl3.parquet")
    long = trades[trades["direction"] == "LONG"].copy()
    long["signal_date"] = pd.to_datetime(long["signal_date"])

    # テクニカル指標をマージ
    tech_renamed = tech.rename(columns={"date": "signal_date"})
    long = long.merge(tech_renamed, on=["ticker", "signal_date"], how="left")
    long = long.dropna(subset=["atr_pct", "vol_ratio"])
    print(f"  {len(long):,} trades with indicators")

    # 前半/後半分割
    long["period"] = np.where(
        long["signal_date"] < SPLIT_DATE, "train", "test"
    )
    print(f"  Train (< {SPLIT_DATE}): {(long['period']=='train').sum():,}")
    print(f"  Test  (>= {SPLIT_DATE}): {(long['period']=='test').sum():,}")

    # 対象シグナル
    targets = [
        ("B4", "Downtrend"),
        ("B1", "Uptrend"),
        ("B3", "Uptrend"),
    ]

    all_results = []

    for rule, regime in targets:
        sub = long[(long["rule"] == rule) & (long["regime"] == regime)]
        train = sub[sub["period"] == "train"]
        test = sub[sub["period"] == "test"]

        print(f"\n{'='*60}")
        print(f"{rule} {regime}: train={len(train):,}, test={len(test):,}")
        print(f"{'='*60}")

        # ベースライン
        for period_label, period_df in [("train", train), ("test", test), ("all", sub)]:
            m = calc_pf(period_df)
            m.update({
                "rule": rule, "regime": regime, "period": period_label,
                "filter": "なし", "threshold": 0, "direction": "-", "col": "-",
                "pf_excluded": 0, "n_excluded": 0,
            })
            all_results.append(m)

        # --- 出来高の感度分析 ---
        vol_thresholds = [0.5, 0.7, 0.8, 1.0, 1.2, 1.5, 2.0, 3.0]
        # B4は高出来高が良い、B1/B3は要確認
        vol_dir = "above" if rule == "B4" else "both"

        for period_label, period_df in [("train", train), ("test", test), ("all", sub)]:
            if vol_dir == "both":
                # 両方向テスト
                for d in ["above", "below"]:
                    res = sensitivity_analysis(
                        period_df, "vol_ratio", vol_thresholds, d,
                        f"Vol {'>' if d == 'above' else '<'}"
                    )
                    for r in res:
                        r.update({"rule": rule, "regime": regime, "period": period_label})
                    all_results.extend(res)
            else:
                res = sensitivity_analysis(
                    period_df, "vol_ratio", vol_thresholds, "above", "Vol >"
                )
                for r in res:
                    r.update({"rule": rule, "regime": regime, "period": period_label})
                all_results.extend(res)

        # --- ATR%の感度分析 ---
        atr_thresholds = [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0]
        # B4は高ATRが良い、B1/B3は低ATRが良い
        for period_label, period_df in [("train", train), ("test", test), ("all", sub)]:
            for d in ["above", "below"]:
                res = sensitivity_analysis(
                    period_df, "atr_pct", atr_thresholds, d,
                    f"ATR% {'>' if d == 'above' else '<'}"
                )
                for r in res:
                    r.update({"rule": rule, "regime": regime, "period": period_label})
                all_results.extend(res)

    # 結果保存
    results_df = pd.DataFrame(all_results)
    out_path = OUT_DIR / "vol_atr_results.parquet"
    results_df.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(results_df):,} rows → {out_path}")

    # --- コンソールサマリー ---
    print("\n" + "=" * 90)
    print("■ Out-of-Sample 検証結果")
    print("=" * 90)

    for rule, regime in targets:
        print(f"\n--- {rule} {regime} ---")
        sub_res = results_df[
            (results_df["rule"] == rule) & (results_df["regime"] == regime)
        ]

        # ベースライン
        for period in ["train", "test"]:
            base = sub_res[
                (sub_res["period"] == period) & (sub_res["filter"] == "なし")
            ]
            if len(base) > 0:
                b = base.iloc[0]
                print(f"  {period}: N={b['n']:>5,d}  PF={b['pf']:.3f}")

        # train で PF が最も高い出来高フィルター
        print(f"\n  出来高フィルター (trainでPF最良 → testで検証):")
        vol_train = sub_res[
            (sub_res["period"] == "train")
            & (sub_res["col"] == "vol_ratio")
            & (sub_res["n"] >= 100)
        ].sort_values("pf", ascending=False)
        if len(vol_train) > 0:
            best_vol = vol_train.iloc[0]
            # 同条件のtest結果
            test_match = sub_res[
                (sub_res["period"] == "test")
                & (sub_res["filter"] == best_vol["filter"])
                & (sub_res["threshold"] == best_vol["threshold"])
            ]
            if len(test_match) > 0:
                tv = test_match.iloc[0]
                print(f"    Train: {best_vol['filter']}{best_vol['threshold']:.1f}  N={best_vol['n']:,d}  PF={best_vol['pf']:.3f}")
                print(f"    Test:  同条件  N={tv['n']:,d}  PF={tv['pf']:.3f}")

        # train で PF が最も高いATRフィルター
        print(f"\n  ATRフィルター (trainでPF最良 → testで検証):")
        atr_train = sub_res[
            (sub_res["period"] == "train")
            & (sub_res["col"] == "atr_pct")
            & (sub_res["n"] >= 100)
        ].sort_values("pf", ascending=False)
        if len(atr_train) > 0:
            best_atr = atr_train.iloc[0]
            test_match = sub_res[
                (sub_res["period"] == "test")
                & (sub_res["filter"] == best_atr["filter"])
                & (sub_res["threshold"] == best_atr["threshold"])
            ]
            if len(test_match) > 0:
                ta = test_match.iloc[0]
                print(f"    Train: {best_atr['filter']}{best_atr['threshold']:.1f}  N={best_atr['n']:,d}  PF={best_atr['pf']:.3f}")
                print(f"    Test:  同条件  N={ta['n']:,d}  PF={ta['pf']:.3f}")

    print(f"\nTotal time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
