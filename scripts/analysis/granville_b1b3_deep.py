#!/usr/bin/env python3
"""
granville_b1b3_deep.py
Granville B1-B3 買いシグナルの深掘り分析

目的:
  - 現行システム: 15日max_hold + high_update exitで PF 0.14（大赤字）
  - Signal ret5d: B1 +1.45% (勝率72%), B3 +0.70% (勝率73%)
  - 乖離の原因解明と、B1-B3が機能する条件の特定

分析内容:
  1. 保有期間別リターン（1/3/5/7/10/15日）
  2. 市場レジーム別（N225上昇/下降局面）
  3. SMA20傾き強度別
  4. Exit戦略比較（固定保有 vs 現行high_update）
  5. IS/OOS検証

期間:
  IS:  2020-01-01 ~ 2024-12-31
  OOS: 2025-01-01 ~ 2026-04-07
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

PRICES_FILE = ROOT / "data" / "parquet" / "granville" / "prices_topix.parquet"
INDEX_FILE = ROOT / "data" / "parquet" / "index_prices_max_1d.parquet"
FUTURES_FILE = ROOT / "data" / "parquet" / "futures_prices_max_1d.parquet"
VI_FILE = ROOT / "data" / "parquet" / "nikkei_vi_history.parquet"


def load_prices() -> pd.DataFrame:
    """TOPIX価格 + テクニカル指標"""
    print("[1] Loading prices...")
    ps = pd.read_parquet(PRICES_FILE)
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)

    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    # 将来N日後のOpen（エントリー翌日寄付基準）
    for d in [1, 2, 3, 5, 7, 10, 15]:
        ps[f"open_{d}d_later"] = g["Open"].shift(-d)

    # SMA20 slope強度（正規化: slope / price * 100）
    ps["sma20_slope_pct"] = ps["sma20_slope"] / ps["sma20"] * 100

    ps = ps.dropna(subset=["sma20"])
    return ps


def load_market_regime() -> pd.DataFrame:
    """N225のトレンド判定 + CMEギャップ + VI"""
    idx = pd.read_parquet(INDEX_FILE)
    idx["date"] = pd.to_datetime(idx["date"])
    n225 = idx[idx["ticker"] == "^N225"][["date", "Close"]].sort_values("date")
    n225["n225_sma20"] = n225["Close"].rolling(20).mean()
    n225["n225_sma60"] = n225["Close"].rolling(60).mean()
    n225["n225_above_sma20"] = n225["Close"] > n225["n225_sma20"]
    n225["n225_above_sma60"] = n225["Close"] > n225["n225_sma60"]
    n225["n225_ret20"] = n225["Close"].pct_change(20) * 100
    n225["n225_prev_close"] = n225["Close"].shift(1)

    # CMEギャップ: NKD終値 vs N225前日終値
    fut = pd.read_parquet(FUTURES_FILE)
    fut["date"] = pd.to_datetime(fut["date"])
    nkd = fut[fut["ticker"] == "NKD=F"][["date", "Close"]].sort_values("date")
    nkd = nkd.rename(columns={"Close": "nkd_close"})
    n225 = n225.merge(nkd, on="date", how="left")
    n225["cme_gap"] = (n225["nkd_close"] / n225["n225_prev_close"] - 1) * 100

    # 日経VI
    vi_path = VI_FILE
    if vi_path.exists():
        vi = pd.read_parquet(vi_path)
        vi["date"] = pd.to_datetime(vi["date"])
        if "close" in vi.columns:
            vi = vi[["date", "close"]].rename(columns={"close": "vi"})
        elif "Close" in vi.columns:
            vi = vi[["date", "Close"]].rename(columns={"Close": "vi"})
        n225 = n225.merge(vi, on="date", how="left")
    else:
        n225["vi"] = np.nan

    cols = ["date", "n225_above_sma20", "n225_above_sma60", "n225_ret20", "cme_gap", "vi"]
    return n225[cols].dropna(subset=["n225_above_sma20"])


def detect_b1b3(ps: pd.DataFrame) -> pd.DataFrame:
    """B1-B3シグナル検出（B4除外）"""
    print("[2] Detecting B1-B3 signals...")
    df = ps.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]

    df["B1"] = df["prev_below"] & df["above"] & sma_up
    df["B2"] = sma_up & dev.between(-5, 0) & df["up_day"] & df["below"]
    df["B3"] = sma_up & df["above"] & dev.between(0, 3) & (df["prev_dev"] > dev) & df["up_day"]

    # ルール割り当て（B1 > B3 > B2 優先）
    sigs = []
    for rule in ["B1", "B3", "B2"]:
        mask = df[rule]
        s = df[mask].copy()
        s["rule"] = rule
        sigs.append(s)

    if not sigs:
        return pd.DataFrame()

    out = pd.concat(sigs)
    # 同一ticker/dateで重複除去（優先順位順にconcatしたのでdrop_duplicatesでOK）
    out = out.drop_duplicates(subset=["ticker", "date"], keep="first")
    return out


def evaluate_fixed_hold(signals: pd.DataFrame, hold_days: int, label: str) -> dict:
    """固定保有期間でのリターン評価（エントリー=翌日Open、イグジット=N日後Open）"""
    # エントリー: シグナル翌日Open = open_1d_later
    # イグジット: シグナルから(1+hold_days)日後のOpen = open_{1+hold_days}d_later に近い
    entry_col = "open_1d_later"
    # hold_days日保有 = エントリーからhold_days営業日後 = signal日から(1+hold_days)日後のOpen
    exit_map = {1: 2, 2: 3, 3: 5, 5: 7, 7: 10, 10: 15}  # 近似
    # 正確に: entry=signal+1日目Open, exit=signal+(1+hold)日目Open
    # open_Xd_laterはsignal日からX日後のOpen
    exit_day = hold_days + 1
    # 利用可能なカラムから最も近いものを使用
    available = [1, 2, 3, 5, 7, 10, 15]
    if exit_day in available:
        exit_col = f"open_{exit_day}d_later"
    else:
        # 近いカラムを使う
        closest = min(available, key=lambda x: abs(x - exit_day))
        exit_col = f"open_{closest}d_later"

    valid = signals.dropna(subset=[entry_col, exit_col])
    if len(valid) < 30:
        return {"label": label, "n": len(valid), "skip": True}

    ret = (valid[exit_col] - valid[entry_col]) / valid[entry_col] * 100
    wins = ret[ret > 0]
    losses = ret[ret <= 0]
    pf = wins.sum() / abs(losses.sum()) if losses.sum() != 0 else float("inf")

    return {
        "label": label,
        "n": len(valid),
        "ret_mean": round(ret.mean(), 3),
        "ret_median": round(ret.median(), 3),
        "win_rate": round(len(wins) / len(ret) * 100, 1),
        "pf": round(pf, 2),
        "ret_std": round(ret.std(), 3),
        "skip": False,
    }


def run_analysis(signals: pd.DataFrame, start: str, end: str, period_label: str) -> list[dict]:
    """期間を絞って多角的に分析"""
    sub = signals[(signals["date"] >= start) & (signals["date"] <= end)]
    print(f"\n  [{period_label}] {start} ~ {end}")
    print(f"  シグナル: {len(sub):,}  銘柄: {sub['ticker'].nunique()}")

    if len(sub) < 30:
        print("  → サンプル不足、スキップ")
        return []

    rows = []

    # === 1. ルール別 × 保有期間別 ===
    print(f"\n  --- ルール別 × 保有期間 ---")
    for rule in ["B1", "B2", "B3", "ALL"]:
        s = sub if rule == "ALL" else sub[sub["rule"] == rule]
        for hold in [1, 2, 4, 6, 9, 14]:
            r = evaluate_fixed_hold(s, hold, f"{rule}_hold{hold}d")
            r["rule"] = rule
            r["hold_days"] = hold
            r["period"] = period_label
            r["filter"] = "none"
            rows.append(r)

    # === 2. 市場レジーム別 ===
    print(f"\n  --- 市場レジーム別 ---")
    for regime_col, regime_label in [
        ("n225_above_sma20", "N225>SMA20"),
        ("n225_above_sma60", "N225>SMA60"),
    ]:
        if regime_col not in sub.columns:
            continue
        for val, val_label in [(True, "上昇"), (False, "下降")]:
            s = sub[sub[regime_col] == val]
            for rule in ["B1", "B3", "ALL"]:
                rs = s if rule == "ALL" else s[s["rule"] == rule]
                for hold in [4, 9]:
                    r = evaluate_fixed_hold(rs, hold, f"{rule}_{regime_label}_{val_label}_hold{hold}d")
                    r["rule"] = rule
                    r["hold_days"] = hold
                    r["period"] = period_label
                    r["filter"] = f"{regime_label}_{val_label}"
                    rows.append(r)

    # === 3. SMA20 slope強度別 ===
    print(f"\n  --- SMA20 slope強度別 ---")
    if "sma20_slope_pct" in sub.columns:
        for lo, hi, slope_label in [
            (0, 0.5, "weak_up"),
            (0.5, 1.0, "mid_up"),
            (1.0, 999, "strong_up"),
        ]:
            s = sub[(sub["sma20_slope_pct"] > lo) & (sub["sma20_slope_pct"] <= hi)]
            for rule in ["B1", "B3"]:
                rs = s if rule == "ALL" else s[s["rule"] == rule]
                for hold in [4, 9]:
                    r = evaluate_fixed_hold(rs, hold, f"{rule}_slope_{slope_label}_hold{hold}d")
                    r["rule"] = rule
                    r["hold_days"] = hold
                    r["period"] = period_label
                    r["filter"] = f"slope_{slope_label}"
                    rows.append(r)

    # === 4. N225リターン20日別（モメンタム環境） ===
    print(f"\n  --- N225 20日リターン別 ---")
    if "n225_ret20" in sub.columns:
        for lo, hi, mom_label in [
            (-999, -5, "strong_down"),
            (-5, 0, "weak_down"),
            (0, 5, "weak_up"),
            (5, 999, "strong_up"),
        ]:
            s = sub[(sub["n225_ret20"] > lo) & (sub["n225_ret20"] <= hi)]
            for rule in ["B1", "B3", "ALL"]:
                rs = s if rule == "ALL" else s[s["rule"] == rule]
                r = evaluate_fixed_hold(rs, 4, f"{rule}_n225mom_{mom_label}_hold4d")
                r["rule"] = rule
                r["hold_days"] = 4
                r["period"] = period_label
                r["filter"] = f"n225mom_{mom_label}"
                rows.append(r)

    # === 5. CMEギャップ別 ===
    print(f"\n  --- CMEギャップ別 ---")
    if "cme_gap" in sub.columns:
        for lo, hi, cme_label in [
            (-999, -2, "cme_strong_gd"),      # CME大幅GD（-2%以下）
            (-2, -0.5, "cme_mild_gd"),         # CME軽GD
            (-0.5, 0.5, "cme_flat"),           # CME横ばい
            (0.5, 2, "cme_mild_gu"),           # CME軽GU
            (2, 999, "cme_strong_gu"),         # CME大幅GU
        ]:
            s = sub[(sub["cme_gap"] > lo) & (sub["cme_gap"] <= hi)]
            for rule in ["B1", "B3", "ALL"]:
                rs = s if rule == "ALL" else s[s["rule"] == rule]
                for hold in [4, 9]:
                    r = evaluate_fixed_hold(rs, hold, f"{rule}_cme_{cme_label}_hold{hold}d")
                    r["rule"] = rule
                    r["hold_days"] = hold
                    r["period"] = period_label
                    r["filter"] = f"cme_{cme_label}"
                    rows.append(r)

    # === 6. CMEギャップ × N225レジーム複合 ===
    print(f"\n  --- CME × N225レジーム複合 ---")
    if "cme_gap" in sub.columns and "n225_above_sma20" in sub.columns:
        combos = [
            ("N225上昇&CME_GD", (sub["n225_above_sma20"]) & (sub["cme_gap"] < -0.5)),
            ("N225上昇&CME_GU", (sub["n225_above_sma20"]) & (sub["cme_gap"] > 0.5)),
            ("N225上昇&CME_flat", (sub["n225_above_sma20"]) & (sub["cme_gap"].between(-0.5, 0.5))),
            ("N225下降&CME_GD", (~sub["n225_above_sma20"]) & (sub["cme_gap"] < -0.5)),
            ("N225下降&CME_GU", (~sub["n225_above_sma20"]) & (sub["cme_gap"] > 0.5)),
        ]
        for combo_label, mask in combos:
            s = sub[mask]
            for rule in ["B1", "B3", "ALL"]:
                rs = s if rule == "ALL" else s[s["rule"] == rule]
                for hold in [4, 9]:
                    r = evaluate_fixed_hold(rs, hold, f"{rule}_{combo_label}_hold{hold}d")
                    r["rule"] = rule
                    r["hold_days"] = hold
                    r["period"] = period_label
                    r["filter"] = f"combo_{combo_label}"
                    rows.append(r)

    # === 7. VI別 ===
    print(f"\n  --- 日経VI別 ---")
    if "vi" in sub.columns:
        for lo, hi, vi_label in [
            (0, 20, "vi_low"),
            (20, 25, "vi_mid"),
            (25, 30, "vi_high"),
            (30, 999, "vi_very_high"),
        ]:
            s = sub[(sub["vi"] >= lo) & (sub["vi"] < hi)]
            for rule in ["B1", "B3", "ALL"]:
                rs = s if rule == "ALL" else s[s["rule"] == rule]
                for hold in [4, 9]:
                    r = evaluate_fixed_hold(rs, hold, f"{rule}_vi_{vi_label}_hold{hold}d")
                    r["rule"] = rule
                    r["hold_days"] = hold
                    r["period"] = period_label
                    r["filter"] = f"vi_{vi_label}"
                    rows.append(r)

    # === 8. ベースライン（ランダムエントリー） ===
    print(f"\n  --- ベースライン ---")
    # 全日のランダムエントリーと比較
    all_days = sub.drop_duplicates(subset=["ticker", "date"])
    r = evaluate_fixed_hold(all_days, 4, "BASE_hold4d")
    r["rule"] = "BASE"
    r["hold_days"] = 4
    r["period"] = period_label
    r["filter"] = "none"
    rows.append(r)

    return rows


def print_results(all_rows: list[dict]) -> None:
    df = pd.DataFrame(all_rows)
    df = df[~df.get("skip", False)].copy()

    for period in ["in_sample", "out_of_sample"]:
        p_df = df[df["period"] == period]
        if p_df.empty:
            continue

        label = "In-Sample (2020-2024)" if period == "in_sample" else "Out-of-Sample (2025-2026)"
        print(f"\n{'=' * 120}")
        print(f"  {label}")

        # ルール別×保有期間
        print(f"\n  --- ルール別 × 保有期間 ---")
        print(f"  {'ルール':<6} {'保有日':<6} {'件数':>7} {'平均ret':>8} {'中央ret':>8} {'勝率':>6} {'PF':>6}")
        print(f"  {'─' * 55}")
        subset = p_df[p_df["filter"] == "none"]
        for _, r in subset.sort_values(["rule", "hold_days"]).iterrows():
            if r.get("skip"):
                continue
            print(f"  {r['rule']:<6} {r['hold_days']:<6} {r['n']:>7} "
                  f"{r.get('ret_mean', 0):>7.3f}% {r.get('ret_median', 0):>7.3f}% "
                  f"{r.get('win_rate', 0):>5.1f}% {r.get('pf', 0):>5.2f}")

        # フィルター別
        for filt_prefix, filt_label in [
            ("N225>SMA20", "市場レジーム (N225 vs SMA20)"),
            ("N225>SMA60", "市場レジーム (N225 vs SMA60)"),
            ("slope_", "SMA20 slope強度"),
            ("n225mom_", "N225 20日モメンタム"),
            ("cme_cme_", "CMEギャップ"),
            ("combo_", "CME × N225レジーム複合"),
            ("vi_vi_", "日経VI"),
        ]:
            subset = p_df[p_df["filter"].str.startswith(filt_prefix)]
            if subset.empty:
                continue
            print(f"\n  --- {filt_label} ---")
            print(f"  {'ラベル':<40} {'件数':>7} {'平均ret':>8} {'勝率':>6} {'PF':>6}")
            print(f"  {'─' * 75}")
            for _, r in subset.iterrows():
                if r.get("skip"):
                    continue
                print(f"  {r['label']:<40} {r['n']:>7} "
                      f"{r.get('ret_mean', 0):>7.3f}% "
                      f"{r.get('win_rate', 0):>5.1f}% {r.get('pf', 0):>5.2f}")

    # IS vs OOS 比較（PF > 1.0のもの）
    print(f"\n{'=' * 120}")
    print("  IS vs OOS 比較（IS PF > 1.0）")
    print(f"{'=' * 120}")
    print(f"  {'ラベル':<40} {'IS件数':>7} {'IS PF':>6} {'OOS件数':>7} {'OOS PF':>6} {'判定'}")
    print(f"  {'─' * 85}")

    is_rows = {r["label"]: r for _, r in df[df["period"] == "in_sample"].iterrows() if not r.get("skip")}
    oos_rows = {r["label"]: r for _, r in df[df["period"] == "out_of_sample"].iterrows() if not r.get("skip")}

    for label in sorted(is_rows.keys()):
        if label not in oos_rows:
            continue
        is_r = is_rows[label]
        oos_r = oos_rows[label]
        is_pf = is_r.get("pf", 0)
        oos_pf = oos_r.get("pf", 0)
        if is_pf < 1.0:
            continue

        if is_pf > 1.2 and oos_pf > 1.2:
            verdict = "✅ 両期間有効"
        elif is_pf > 1.0 and oos_pf > 1.0:
            verdict = "△ 弱いが一貫"
        elif oos_pf <= 1.0:
            verdict = "❌ OOSで崩壊"
        else:
            verdict = "？"

        print(f"  {label:<40} {is_r['n']:>7} {is_pf:>5.2f} {oos_r['n']:>7}  {oos_pf:>5.2f}  {verdict}")


def main() -> int:
    print("=" * 70)
    print("Granville B1-B3 深掘り分析")
    print("=" * 70)

    ps = load_prices()

    print("\n[2] Loading market regime...")
    regime = load_market_regime()
    print(f"  N225: {len(regime)} days")

    print("\n[3] Detecting signals...")
    signals = detect_b1b3(ps)
    print(f"  Total B1-B3 signals: {len(signals):,}")
    print(f"  Rule distribution:")
    print(signals["rule"].value_counts().to_string(header=False))

    print("\n[4] Merging market regime...")
    signals = signals.merge(regime, on="date", how="left")

    print("\n[5] Running analysis...")
    all_rows = []
    for period, (start, end) in [
        ("in_sample", ("2020-01-01", "2024-12-31")),
        ("out_of_sample", ("2025-01-01", "2026-04-07")),
    ]:
        rows = run_analysis(signals, start, end, period)
        all_rows.extend(rows)

    print_results(all_rows)

    # CSV保存
    out = ROOT / "data" / "analysis" / "granville_b1b3_deep.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(all_rows).to_csv(out, index=False)
    print(f"\n📁 詳細: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
