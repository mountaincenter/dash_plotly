#!/usr/bin/env python3
"""
SQ-4 銘柄選定方法の比較分析
前営業日終値のみで判断できる選定基準を複数パターン検証

現行: 当日Gap-down Top10（寄付価格使用 = 事前確定不可）
目標: 前日終値ベースで事前確定可能な選定方法を見つける
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
FUTURES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"

BACKTEST_START = "2022-04-01"
PRICE_MIN = 1000
PRICE_MAX = 20000
TOP_N = 10


def load_data():
    ps = pd.read_parquet(PRICES_PATH)
    ps["Date"] = pd.to_datetime(ps["Date"])
    ps["Code"] = ps["Code"].astype(str)
    ps = ps.sort_values(["Code", "Date"]).drop_duplicates(subset=["Code", "Date"])

    # prev close for each row
    ps["prev_close"] = ps.groupby("Code")["AdjC"].shift(1)
    # day return (close-to-close)
    ps["ret_1d"] = ps["AdjC"] / ps["prev_close"] - 1
    # 5-day return
    ps["ret_5d"] = ps.groupby("Code")["AdjC"].pct_change(5)
    # gap (open vs prev close)
    ps["gap"] = ps["AdjO"] / ps["prev_close"] - 1

    return ps


def load_sq_schedule():
    cal = pd.read_parquet(CALENDAR_PATH)
    cal["date"] = pd.to_datetime(cal["date"])
    start_ts = pd.Timestamp(BACKTEST_START)

    sq4_dates = cal[cal["sq4_entry"] == True]["date"].tolist()
    sq3_dates = cal[cal["sq3_exit"] == True]["date"].tolist()

    ps = pd.read_parquet(PRICES_PATH)
    ps["Date"] = pd.to_datetime(ps["Date"])
    bdays = sorted(ps["Date"].unique())
    bday_set = set(bdays)
    last_bday = bdays[-1]

    results = []
    for sq4 in sq4_dates:
        if sq4 < start_ts or sq4 not in bday_set:
            continue
        sq3_after = [d for d in sq3_dates if d > sq4]
        if not sq3_after:
            continue
        sq3 = sq3_after[0]
        if sq3 > last_bday:
            continue
        sq4_idx = bdays.index(sq4)
        if sq4_idx < 5:
            continue
        prev_day = bdays[sq4_idx - 1]
        prev5_day = bdays[sq4_idx - 5]
        results.append({
            "sq4_entry": sq4,
            "sq3_exit": sq3,
            "prev_day": prev_day,
            "prev5_day": prev5_day,
        })
    return results


def calc_pf(rets):
    if not rets:
        return None
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    return round(gp / gl, 2) if gl > 0 else None


def run_strategy(ps, sq_schedule, select_fn, label):
    """指定の選定関数でバックテスト実行"""
    all_rets = []
    all_pnl = []

    for sq in sq_schedule:
        sq4 = sq["sq4_entry"]
        sq3 = sq["sq3_exit"]
        prev = sq["prev_day"]

        # 前日データ（選定用）
        prev_data = ps[ps["Date"] == prev][["Code", "AdjC", "ret_1d", "ret_5d", "gap"]].copy()
        prev_data = prev_data.rename(columns={"AdjC": "prev_close"})
        prev_data = prev_data[
            (prev_data["prev_close"] >= PRICE_MIN) &
            (prev_data["prev_close"] <= PRICE_MAX)
        ]

        if prev_data.empty:
            continue

        # 選定
        picks = select_fn(prev_data)
        if picks.empty:
            continue

        codes = picks["Code"].tolist()

        # エントリー（寄成）& イグジット（大引成）
        entry = ps[(ps["Date"] == sq4) & (ps["Code"].isin(codes))][["Code", "AdjO"]].rename(columns={"AdjO": "entry_open"})
        exit_ = ps[(ps["Date"] == sq3) & (ps["Code"].isin(codes))][["Code", "AdjC"]].rename(columns={"AdjC": "exit_close"})

        merged = entry.merge(exit_, on="Code")
        if merged.empty:
            continue

        merged["ret"] = merged["exit_close"] / merged["entry_open"] - 1
        merged["pnl_100"] = (merged["exit_close"] - merged["entry_open"]) * 100

        all_rets.extend(merged["ret"].tolist())
        all_pnl.extend(merged["pnl_100"].tolist())

    n = len(all_rets)
    if n == 0:
        return {"label": label, "n": 0}

    wins = [r for r in all_rets if r > 0]
    pf = calc_pf([r * 100 for r in all_rets])
    avg_ret = np.mean(all_rets) * 100
    total_pnl = int(sum(all_pnl))
    wr = len(wins) / n * 100

    return {
        "label": label,
        "n": n,
        "pf": pf,
        "wr": round(wr, 1),
        "avg_ret": round(avg_ret, 3),
        "total_pnl": total_pnl,
    }


def main():
    print("=" * 70)
    print("SQ-4 Selection Method Comparison (prev_day close only)")
    print("=" * 70)

    print("\nLoading data...")
    ps = load_data()
    sq_schedule = load_sq_schedule()
    print(f"  {len(sq_schedule)} SQ-4 dates, {ps['Code'].nunique()} codes")

    # === 選定方法の定義 ===

    # 0. 現行: 当日Gap-down Top10（ベースライン、事前確定不可）
    def select_actual_gap(prev_data, ps=ps, sq_schedule=sq_schedule):
        """This is special - uses entry day open, for comparison only"""
        pass  # handled separately

    # 1. 前日リターン worst 10（前日最も下がった銘柄）
    def select_prev_ret_worst(prev_data):
        valid = prev_data.dropna(subset=["ret_1d"])
        return valid.nsmallest(TOP_N, "ret_1d")

    # 2. 前日リターン best 10（前日最も上がった銘柄 = 反転狙い？）
    def select_prev_ret_best(prev_data):
        valid = prev_data.dropna(subset=["ret_1d"])
        return valid.nlargest(TOP_N, "ret_1d")

    # 3. 5日リターン worst 10（5日間で最も弱い）
    def select_5d_ret_worst(prev_data):
        valid = prev_data.dropna(subset=["ret_5d"])
        return valid.nsmallest(TOP_N, "ret_5d")

    # 4. 5日リターン best 10
    def select_5d_ret_best(prev_data):
        valid = prev_data.dropna(subset=["ret_5d"])
        return valid.nlargest(TOP_N, "ret_5d")

    # 5. 最安値帯（価格が低い = ボラ高い傾向）
    def select_lowest_price(prev_data):
        return prev_data.nsmallest(TOP_N, "prev_close")

    # 6. 最高値帯（価格が高い = 5000+で強い傾向）
    def select_highest_price(prev_data):
        return prev_data.nlargest(TOP_N, "prev_close")

    # 7. 5000円以上 × 前日worst
    def select_5000plus_prev_worst(prev_data):
        hi = prev_data[prev_data["prev_close"] >= 5000]
        if len(hi) < TOP_N:
            return hi
        valid = hi.dropna(subset=["ret_1d"])
        return valid.nsmallest(TOP_N, "ret_1d")

    # 8. 5000円以上 × 5日worst
    def select_5000plus_5d_worst(prev_data):
        hi = prev_data[prev_data["prev_close"] >= 5000]
        if len(hi) < TOP_N:
            return hi
        valid = hi.dropna(subset=["ret_5d"])
        return valid.nsmallest(TOP_N, "ret_5d")

    # 9. 前日Gap-down top10（前日の寄付Gap）
    def select_prev_gap_worst(prev_data):
        valid = prev_data.dropna(subset=["gap"])
        return valid.nsmallest(TOP_N, "gap")

    # 10. ランダム（ベースライン: SQ-4効果のみ）
    def select_random(prev_data):
        if len(prev_data) <= TOP_N:
            return prev_data
        return prev_data.sample(n=TOP_N, random_state=42)

    # 11. 全銘柄均等（SQ-4市場効果の純粋測定）
    def select_all(prev_data):
        return prev_data

    # === 現行Gap-downのバックテスト（別処理） ===
    print("\n[Baseline] 当日Gap-down Top10...")
    baseline_rets = []
    for sq in sq_schedule:
        sq4 = sq["sq4_entry"]
        sq3 = sq["sq3_exit"]
        prev = sq["prev_day"]

        prev_data = ps[ps["Date"] == prev][["Code", "AdjC"]].rename(columns={"AdjC": "prev_close"})
        entry_data = ps[ps["Date"] == sq4][["Code", "AdjO"]].rename(columns={"AdjO": "entry_open"})
        exit_data = ps[ps["Date"] == sq3][["Code", "AdjC"]].rename(columns={"AdjC": "exit_close"})

        merged = prev_data.merge(entry_data, on="Code").merge(exit_data, on="Code")
        merged = merged[(merged["prev_close"] >= PRICE_MIN) & (merged["prev_close"] <= PRICE_MAX)]
        merged["gap_pct"] = (merged["entry_open"] / merged["prev_close"] - 1) * 100
        merged = merged[merged["gap_pct"] >= -10.0]
        picks = merged.nsmallest(TOP_N, "gap_pct")
        if picks.empty:
            continue
        picks["ret"] = (picks["exit_close"] / picks["entry_open"] - 1) * 100
        baseline_rets.extend(picks["ret"].tolist())

    baseline_pf = calc_pf(baseline_rets)
    print(f"  N={len(baseline_rets)}, PF={baseline_pf}, WR={len([r for r in baseline_rets if r>0])/len(baseline_rets)*100:.1f}%, avg={np.mean(baseline_rets):+.3f}%")

    # === 各方法のバックテスト ===
    methods = [
        (select_prev_ret_worst, "前日ret worst10"),
        (select_prev_ret_best, "前日ret best10"),
        (select_5d_ret_worst, "5日ret worst10"),
        (select_5d_ret_best, "5日ret best10"),
        (select_lowest_price, "最安値10"),
        (select_highest_price, "最高値10"),
        (select_5000plus_prev_worst, "5000+×前日worst"),
        (select_5000plus_5d_worst, "5000+×5日worst"),
        (select_prev_gap_worst, "前日Gap worst10"),
        (select_random, "ランダム10"),
        (select_all, "全銘柄均等"),
    ]

    print(f"\n{'Method':<22} {'N':>5} {'PF':>6} {'WR':>6} {'AvgRet':>8} {'TotalPnL':>12}")
    print("-" * 70)
    print(f"{'[現行]Gap-down Top10':<22} {len(baseline_rets):>5} {baseline_pf:>6} {len([r for r in baseline_rets if r>0])/len(baseline_rets)*100:>5.1f}% {np.mean(baseline_rets):>+7.3f}% {int(sum([r/100*1000 for r in baseline_rets])):>12,}")
    print("-" * 70)

    results = []
    for fn, label in methods:
        r = run_strategy(ps, sq_schedule, fn, label)
        results.append(r)
        if r["n"] > 0:
            print(f"{label:<22} {r['n']:>5} {r['pf']:>6} {r['wr']:>5.1f}% {r['avg_ret']:>+7.3f}% {r['total_pnl']:>12,}")
        else:
            print(f"{label:<22} {'N/A':>5}")

    # === CMEフィルタ付き ===
    print("\n\n--- CME下落時のみ ---")
    cme_df = pd.read_parquet(FUTURES_PATH)
    cme = cme_df[cme_df["ticker"] == "NKD=F"][["date", "Close"]].copy()
    cme["date"] = pd.to_datetime(cme["date"])
    cme = cme.dropna(subset=["Close"]).sort_values("date").reset_index(drop=True)
    cme_map = {row["date"]: row["Close"] for _, row in cme.iterrows()}

    bdays = sorted(ps["Date"].unique())

    def is_cme_down(prev_day):
        """prev_day付近のCME 2日を探して下落判定"""
        prev_idx = bdays.index(prev_day) if prev_day in bdays else None
        if prev_idx is None:
            return None
        fri = None
        thu = None
        for offset in range(3):
            idx = prev_idx - offset
            if idx >= 0 and bdays[idx] in cme_map:
                if fri is None:
                    fri = cme_map[bdays[idx]]
                elif thu is None:
                    thu = cme_map[bdays[idx]]
                    break
        if fri is not None and thu is not None and thu != 0:
            return fri < thu
        return None

    cme_down_schedule = [sq for sq in sq_schedule if is_cme_down(sq["prev_day"]) == True]
    print(f"  CME下落月: {len(cme_down_schedule)} / {len(sq_schedule)}")

    # CME下落時の各手法
    print(f"\n{'Method':<22} {'N':>5} {'PF':>6} {'WR':>6} {'AvgRet':>8} {'TotalPnL':>12}")
    print("-" * 70)

    # Baseline with CME filter
    cme_baseline_rets = []
    for sq in cme_down_schedule:
        sq4 = sq["sq4_entry"]
        sq3 = sq["sq3_exit"]
        prev = sq["prev_day"]
        prev_data = ps[ps["Date"] == prev][["Code", "AdjC"]].rename(columns={"AdjC": "prev_close"})
        entry_data = ps[ps["Date"] == sq4][["Code", "AdjO"]].rename(columns={"AdjO": "entry_open"})
        exit_data = ps[ps["Date"] == sq3][["Code", "AdjC"]].rename(columns={"AdjC": "exit_close"})
        merged = prev_data.merge(entry_data, on="Code").merge(exit_data, on="Code")
        merged = merged[(merged["prev_close"] >= PRICE_MIN) & (merged["prev_close"] <= PRICE_MAX)]
        merged["gap_pct"] = (merged["entry_open"] / merged["prev_close"] - 1) * 100
        merged = merged[merged["gap_pct"] >= -10.0]
        picks = merged.nsmallest(TOP_N, "gap_pct")
        if picks.empty:
            continue
        picks["ret"] = (picks["exit_close"] / picks["entry_open"] - 1) * 100
        cme_baseline_rets.extend(picks["ret"].tolist())

    if cme_baseline_rets:
        print(f"{'[現行]Gap-down Top10':<22} {len(cme_baseline_rets):>5} {calc_pf(cme_baseline_rets):>6} {len([r for r in cme_baseline_rets if r>0])/len(cme_baseline_rets)*100:>5.1f}% {np.mean(cme_baseline_rets):>+7.3f}% {int(sum([r/100*1000 for r in cme_baseline_rets])):>12,}")
    print("-" * 70)

    for fn, label in methods:
        r = run_strategy(ps, cme_down_schedule, fn, label)
        if r["n"] > 0:
            print(f"{label:<22} {r['n']:>5} {r['pf']:>6} {r['wr']:>6.1f}% {r['avg_ret']:>+7.3f}% {r['total_pnl']:>12,}")

    print("\n[OK] Done")


if __name__ == "__main__":
    main()
