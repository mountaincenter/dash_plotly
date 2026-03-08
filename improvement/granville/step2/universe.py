#!/usr/bin/env python3
"""
Step 2: ユニバース制限
======================
Step 1 の trades parquet をフィルタリングし、
ユニバース × 株価帯 × シグナル × レジーム の全組み合わせで PF を比較する。

LONG のみ（Step 1 で SHORT は不採用確定）。
SL-3% をメイン分析、SLなし/SL-5% は参考比較。

Input:  step1/trades_sl3.parquet (+ trades_no_sl.parquet, trades_sl5.parquet)
Output: step2/universe_summary.parquet, step2/universe.html
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # dash_plotly/improvement
PARQUET_DIR = ROOT.parent / "data" / "parquet"
STEP1_DIR = ROOT / "granville" / "step1"
OUT_DIR = Path(__file__).resolve().parent  # step2/

# ユニバース定義
def load_universes() -> dict[str, set[str]]:
    """meta.parquet からユニバースを構築"""
    m = pd.read_parquet(PARQUET_DIR / "meta.parquet")

    core30 = set(m[m["topixnewindexseries"] == "TOPIX Core30"]["ticker"].tolist())
    large70 = set(m[m["topixnewindexseries"] == "TOPIX Large70"]["ticker"].tolist())
    topix100 = core30 | large70

    policy = set()
    for _, row in m.iterrows():
        cats = row.get("categories", [])
        if isinstance(cats, (list, np.ndarray)) and "政策銘柄" in list(cats):
            policy.add(row["ticker"])

    core30_policy = core30 | policy
    all_tickers = set(m["ticker"].tolist())

    return {
        "Core30": core30,
        "TOPIX100": topix100,
        "政策銘柄": policy,
        "Core30+政策銘柄": core30_policy,
        "全銘柄": all_tickers,
    }


# 株価帯定義
PRICE_RANGES = {
    "<5000": (0, 5000),
    "<10000": (0, 10000),
    "<20000": (0, 20000),
    "制限なし": (0, float("inf")),
}


def compute_metrics(df: pd.DataFrame) -> dict:
    """トレード集合からメトリクスを算出"""
    if len(df) == 0:
        return {
            "trades": 0, "win_rate": 0, "pf": 0,
            "avg_pnl": 0, "total_pnl_man": 0,
            "avg_hold": 0, "avg_ret": 0,
            "sl_rate": 0, "median_entry_price": 0,
        }
    n = len(df)
    wr = df["win"].mean() * 100
    ws = df[df["ret_pct"] > 0]["ret_pct"].sum()
    ls = abs(df[df["ret_pct"] <= 0]["ret_pct"].sum())
    pf = round(ws / ls, 3) if ls > 0 else 999.0
    avg_pnl = df["pnl"].mean()
    total_pnl = df["pnl"].sum() / 10000
    avg_hold = df["hold_days"].mean()
    avg_ret = df["ret_pct"].mean()
    sl_rate = (df["exit_type"] == "sl").mean() * 100 if "exit_type" in df.columns else 0
    med_price = df["entry_price"].median()

    return {
        "trades": n, "win_rate": round(wr, 1), "pf": pf,
        "avg_pnl": round(avg_pnl), "total_pnl_man": round(total_pnl),
        "avg_hold": round(avg_hold, 1), "avg_ret": round(avg_ret, 3),
        "sl_rate": round(sl_rate, 1), "median_entry_price": round(med_price),
    }


def main():
    t0 = time.time()
    print("=" * 80)
    print("Step 2: ユニバース × 株価帯 分析")
    print("=" * 80)

    # Load universes
    universes = load_universes()
    for name, tickers in universes.items():
        print(f"  {name}: {len(tickers)} 銘柄")

    # Load Step 1 trades (LONG only)
    results_all = []

    for sl_label, filename in [
        ("SLなし", "trades_no_sl.parquet"),
        ("SL-3%", "trades_sl3.parquet"),
        ("SL-5%", "trades_sl5.parquet"),
    ]:
        path = STEP1_DIR / filename
        if not path.exists():
            print(f"  ⚠️ {path} not found, skipping")
            continue

        trades = pd.read_parquet(path)
        long = trades[trades["direction"] == "LONG"].copy()
        print(f"\n  {sl_label}: {len(long):,} LONG trades loaded")

        for uni_name, uni_tickers in universes.items():
            for price_label, (pmin, pmax) in PRICE_RANGES.items():
                filtered = long[
                    (long["ticker"].isin(uni_tickers))
                    & (long["entry_price"] >= pmin)
                    & (long["entry_price"] < pmax)
                ]

                # 全シグナル合算
                m = compute_metrics(filtered)
                m.update({
                    "sl": sl_label, "universe": uni_name,
                    "price_range": price_label,
                    "rule": "LONG合計", "regime": "全体",
                })
                results_all.append(m)

                # シグナル別
                for rule in ["B1", "B2", "B3", "B4"]:
                    rule_df = filtered[filtered["rule"] == rule]
                    m = compute_metrics(rule_df)
                    m.update({
                        "sl": sl_label, "universe": uni_name,
                        "price_range": price_label,
                        "rule": rule, "regime": "全体",
                    })
                    results_all.append(m)

                    # レジーム別
                    for regime in ["Uptrend", "Downtrend"]:
                        reg_df = rule_df[rule_df["regime"] == regime]
                        m = compute_metrics(reg_df)
                        m.update({
                            "sl": sl_label, "universe": uni_name,
                            "price_range": price_label,
                            "rule": rule, "regime": regime,
                        })
                        results_all.append(m)

    summary = pd.DataFrame(results_all)
    out_path = OUT_DIR / "universe_summary.parquet"
    summary.to_parquet(out_path, index=False)
    print(f"\n[OK] Saved {len(summary):,} rows → {out_path}")

    # --- Console summary (SL-3%, 制限なし価格帯, 全体レジーム) ---
    print("\n" + "=" * 90)
    print("■ ユニバース別サマリー (SL-3%, 株価制限なし, 全レジーム)")
    print("=" * 90)
    sl3 = summary[(summary["sl"] == "SL-3%") & (summary["price_range"] == "制限なし") & (summary["regime"] == "全体")]
    print(f"{'Universe':<18s} {'Signal':<8s} {'Trades':>7s} {'WR':>6s} {'PF':>6s} {'AvgPnL':>8s} {'PnL万':>8s} {'Hold':>6s}")
    print("-" * 75)
    for uni in ["Core30", "TOPIX100", "政策銘柄", "Core30+政策銘柄", "全銘柄"]:
        for rule in ["B1", "B2", "B3", "B4", "LONG合計"]:
            row = sl3[(sl3["universe"] == uni) & (sl3["rule"] == rule)]
            if len(row) == 0:
                continue
            r = row.iloc[0]
            print(f"{uni:<18s} {rule:<8s} {r['trades']:>7,d} {r['win_rate']:>5.1f}% {r['pf']:>5.02f} {r['avg_pnl']:>+7.0f} {r['total_pnl_man']:>+7.0f} {r['avg_hold']:>5.1f}d")
        print()

    # --- 株価帯の影響 (SL-3%, 全銘柄, LONG合計) ---
    print("=" * 90)
    print("■ 株価帯別 (SL-3%, 全銘柄, LONG合計)")
    print("=" * 90)
    sl3_all = summary[(summary["sl"] == "SL-3%") & (summary["universe"] == "全銘柄") & (summary["rule"] == "LONG合計") & (summary["regime"] == "全体")]
    print(f"{'Price':<12s} {'Trades':>7s} {'WR':>6s} {'PF':>6s} {'AvgPnL':>8s} {'PnL万':>8s} {'MedPrice':>9s}")
    print("-" * 60)
    for _, r in sl3_all.iterrows():
        print(f"{r['price_range']:<12s} {r['trades']:>7,d} {r['win_rate']:>5.1f}% {r['pf']:>5.02f} {r['avg_pnl']:>+7.0f} {r['total_pnl_man']:>+7.0f} {r['median_entry_price']:>8.0f}")

    # --- ユニバース × 株価帯 クロス (SL-3%, B1 Uptrend + B4 Downtrend) ---
    print("\n" + "=" * 90)
    print("■ ユニバース × 株価帯 (SL-3%, B1 Uptrend)")
    print("=" * 90)
    b1u = summary[(summary["sl"] == "SL-3%") & (summary["rule"] == "B1") & (summary["regime"] == "Uptrend")]
    print(f"{'Universe':<18s} {'Price':<12s} {'Trades':>7s} {'PF':>6s} {'AvgPnL':>8s} {'PnL万':>8s}")
    print("-" * 60)
    for uni in ["Core30", "TOPIX100", "政策銘柄", "Core30+政策銘柄", "全銘柄"]:
        for pr in ["<5000", "<10000", "<20000", "制限なし"]:
            row = b1u[(b1u["universe"] == uni) & (b1u["price_range"] == pr)]
            if len(row) == 0 or row.iloc[0]["trades"] == 0:
                continue
            r = row.iloc[0]
            print(f"{uni:<18s} {pr:<12s} {r['trades']:>7,d} {r['pf']:>5.02f} {r['avg_pnl']:>+7.0f} {r['total_pnl_man']:>+7.0f}")
        print()

    print("\n" + "=" * 90)
    print("■ ユニバース × 株価帯 (SL-3%, B4 Downtrend)")
    print("=" * 90)
    b4d = summary[(summary["sl"] == "SL-3%") & (summary["rule"] == "B4") & (summary["regime"] == "Downtrend")]
    print(f"{'Universe':<18s} {'Price':<12s} {'Trades':>7s} {'PF':>6s} {'AvgPnL':>8s} {'PnL万':>8s}")
    print("-" * 60)
    for uni in ["Core30", "TOPIX100", "政策銘柄", "Core30+政策銘柄", "全銘柄"]:
        for pr in ["<5000", "<10000", "<20000", "制限なし"]:
            row = b4d[(b4d["universe"] == uni) & (b4d["price_range"] == pr)]
            if len(row) == 0 or row.iloc[0]["trades"] == 0:
                continue
            r = row.iloc[0]
            print(f"{uni:<18s} {pr:<12s} {r['trades']:>7,d} {r['pf']:>5.02f} {r['avg_pnl']:>+7.0f} {r['total_pnl_man']:>+7.0f}")
        print()

    print(f"\nTotal time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
