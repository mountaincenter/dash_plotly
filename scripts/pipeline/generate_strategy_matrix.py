#!/usr/bin/env python3
"""
generate_strategy_matrix.py
戦略×曜日PFマトリクスを算出して strategy_matrix.json に保存

データソース:
- grok_trending_archive.parquet (grok SHORT)
- weekday_edge_trades.json (曜日アノマリー20銘柄)
- V2_PAIRS + prices_topix.parquet (ペアトレード)
- sq4_trades.json, sq_plus1_trades.json, quarter_end_effect.json (Calendar)

実行方法:
    python3 scripts/pipeline/generate_strategy_matrix.py
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

GROK_ARCHIVE = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
PRICES_TOPIX = PARQUET_DIR / "granville" / "prices_topix.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
ANALYSIS_DIR = ROOT / "data" / "analysis"
WEEKDAY_EDGE_JSON = ANALYSIS_DIR / "weekday_edge_trades.json"
SQ4_JSON = ANALYSIS_DIR / "sq4_trades.json"
SQ_PLUS1_JSON = ANALYSIS_DIR / "sq_plus1_trades.json"
QE_JSON = ANALYSIS_DIR / "quarter_end_effect.json"
OUTPUT_PATH = ANALYSIS_DIR / "strategy_matrix.json"

PAIRS_PIPELINE = ROOT / "scripts" / "pipeline" / "generate_pairs_signals.py"

DOW_LABELS = ["月", "火", "水", "木", "金"]

# ── Pairs 設定 ──
Z_ENTRY = 2.0
PF_MIN = 1.5
EXCLUDE_SECTORS = [(9000, 9099)]
EXCLUDE_PAIRS = {
    ("2768.T", "8020.T"), ("6103.T", "6305.T"), ("6103.T", "6473.T"),
    ("7167.T", "8411.T"), ("7272.T", "7313.T"), ("8031.T", "8058.T"),
    ("8306.T", "8316.T"), ("8338.T", "8399.T"), ("8354.T", "8381.T"),
    ("8360.T", "8386.T"), ("8386.T", "8392.T"), ("8411.T", "8524.T"),
    ("6995.T", "7282.T"), ("4088.T", "4401.T"),
    ("4187.T", "4401.T"), ("4187.T", "4203.T"),
}


def _calc_pf(pnl_series: pd.Series) -> float | None:
    wins = pnl_series[pnl_series > 0].sum()
    losses = abs(pnl_series[pnl_series < 0].sum())
    if losses == 0:
        return None
    return round(float(wins / losses), 2)


def _calc_stats(pnl_series: pd.Series) -> dict:
    n = len(pnl_series)
    wins = int((pnl_series > 0).sum())
    return {
        "n": n,
        "wins": wins,
        "wr": round(wins / n * 100, 1) if n > 0 else 0,
        "pf": _calc_pf(pnl_series),
        "total_pnl": int(pnl_series.sum()),
    }


# ── 1. Grok SHORT ──
def _detect_prob_direction(df: pd.DataFrame) -> str:
    """archiveのml_prob方向を自動検出。
    4/25以前: y=phase2_win → HIGH prob = SHORT有利
    4/25以降: y=1-phase2_win → LOW prob = SHORT有利
    """
    valid = df.dropna(subset=["ml_prob", "phase2_win"])
    win_mean = valid[valid["phase2_win"] == True]["ml_prob"].mean()
    lose_mean = valid[valid["phase2_win"] == False]["ml_prob"].mean()
    if win_mean > lose_mean:
        return "high"  # HIGH prob = SHORT wins (pre-fix)
    return "low"  # LOW prob = SHORT wins (post-fix)


def compute_grok_weekday() -> dict:
    print("\n[1] Grok SHORT")
    df = pd.read_parquet(GROK_ARCHIVE)

    # shortable: 制度 or いちにち(残0除外)
    shortable = df[
        (df["is_shortable"] == True)
        | ((df["day_trade"] == True) & (df["day_trade_available_shares"] > 0))
    ].copy()

    # prob方向の自動検出
    prob_dir = _detect_prob_direction(shortable)
    print(f"  prob方向: {prob_dir} ({'HIGH prob=SHORT有利' if prob_dir == 'high' else 'LOW prob=SHORT有利'})")
    print(f"  shortable銘柄: {len(shortable)}件, {shortable['selection_date'].nunique()}日")

    # 日次Top3選定
    trades = []
    for date, grp in shortable.groupby("selection_date"):
        if prob_dir == "high":
            top = grp.nlargest(3, "ml_prob")
        else:
            top = grp.nsmallest(3, "ml_prob")
        trades.append(top)
    tdf = pd.concat(trades)
    print(f"  Top3トレード: {len(tdf)}件")

    result = {}
    for dow in range(5):
        sub = tdf[pd.to_datetime(tdf["selection_date"]).dt.weekday == dow]
        pnl = sub["profit_per_100_shares_phase2"]
        stats = _calc_stats(pnl)
        result[DOW_LABELS[dow]] = stats
        pf_str = f"PF{stats['pf']}" if stats["pf"] else "N/A"
        print(f"  {DOW_LABELS[dow]}: N={stats['n']}, WR={stats['wr']}%, {pf_str}, 累計{stats['total_pnl']:+,}円")

    overall = _calc_stats(tdf["profit_per_100_shares_phase2"])
    result["overall"] = overall
    result["_meta"] = {"prob_direction": prob_dir, "total_trades": len(tdf)}
    return result


# ── 2. Weekday Edge 20銘柄 ──
def compute_weekday_edge() -> dict:
    print("\n[2] Weekday Edge 20銘柄")
    with open(WEEKDAY_EDGE_JSON, encoding="utf-8") as f:
        data = json.load(f)

    picks = []
    for week in data.get("weekly", []):
        for p in week.get("picks", []):
            picks.append(p)

    if not picks:
        print("  picks なし")
        return {}

    pdf = pd.DataFrame(picks)
    pdf["dow"] = pd.to_datetime(pdf["date"]).dt.weekday

    result = {}
    for dow in range(5):
        sub = pdf[pdf["dow"] == dow]
        if sub.empty:
            result[DOW_LABELS[dow]] = {"n": 0, "wins": 0, "wr": 0, "pf": None, "total_pnl": 0}
            continue
        pnl = sub["pnl_100"]
        stats = _calc_stats(pnl)
        result[DOW_LABELS[dow]] = stats
        pf_str = f"PF{stats['pf']}" if stats["pf"] else "N/A"
        print(f"  {DOW_LABELS[dow]}: N={stats['n']}, WR={stats['wr']}%, {pf_str}")

    all_pnl = pdf["pnl_100"]
    result["overall"] = _calc_stats(all_pnl)
    return result


# ── 3. Pairs (V2_PAIRS Top3) ──
def _load_v2_pairs() -> list[tuple[str, str, int, float]]:
    text = PAIRS_PIPELINE.read_text()
    pairs = []
    for m in re.finditer(r'\("(\d+\.T)",\s*"(\d+\.T)",\s*(\d+),\s*([\d.]+)', text):
        pairs.append((m.group(1), m.group(2), int(m.group(3)), float(m.group(4))))
    return pairs


def _filter_pairs(pairs: list[tuple]) -> list[tuple]:
    active = []
    for tk1, tk2, lb, pf in pairs:
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        if any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in EXCLUDE_SECTORS):
            continue
        if (tk1, tk2) in EXCLUDE_PAIRS:
            continue
        active.append((tk1, tk2, lb, pf))
    return active


def compute_pairs_weekday() -> dict:
    print("\n[3] Pairs Top3 (V2_PAIRS)")
    pairs = _filter_pairs(_load_v2_pairs())
    print(f"  アクティブペア: {len(pairs)}")

    prices = pd.read_parquet(PRICES_TOPIX)
    prices = prices.sort_values(["ticker", "date"])
    prices["date"] = pd.to_datetime(prices["date"])

    meta = pd.read_parquet(META_PATH)
    sector_map = {}
    if "sectors" in meta.columns:
        for _, row in meta.iterrows():
            sector_map[str(row.get("ticker", ""))] = str(row.get("sectors", ""))

    tickers_needed = sorted({t for tk1, tk2, *_ in pairs for t in (tk1, tk2)})
    prices = prices[prices["ticker"].isin(tickers_needed)]

    pivot_close = prices.pivot_table(index="date", columns="ticker", values="Close")
    pivot_open = prices.pivot_table(index="date", columns="ticker", values="Open")

    all_dates = pivot_close.index
    # 2020年以降
    start_idx = all_dates.searchsorted(pd.Timestamp("2020-01-01"))

    print("  z-score算出中...")
    rows = []
    for tk1, tk2, lb, pf in pairs:
        if tk1 not in pivot_close.columns or tk2 not in pivot_close.columns:
            continue
        c1 = pivot_close[tk1]
        c2 = pivot_close[tk2]
        o1 = pivot_open[tk1]
        o2 = pivot_open[tk2]
        valid = c1.notna() & c2.notna() & (c1 > 0) & (c2 > 0)
        spread = np.log(c1 / c2)

        for i in range(max(start_idx, lb + 1), len(all_dates) - 1):
            if not valid.iloc[i]:
                continue
            window = spread.iloc[max(0, i - lb):i].dropna()
            if len(window) < lb * 0.8:
                continue
            mu = window.mean()
            sigma = window.std()
            if sigma == 0:
                continue
            z = float((spread.iloc[i] - mu) / sigma)
            if abs(z) < Z_ENTRY:
                continue
            if pf < PF_MIN:
                continue

            o1_next = float(o1.iloc[i + 1]) if pd.notna(o1.iloc[i + 1]) else np.nan
            c1_next = float(c1.iloc[i + 1]) if pd.notna(c1.iloc[i + 1]) else np.nan
            o2_next = float(o2.iloc[i + 1]) if pd.notna(o2.iloc[i + 1]) else np.nan
            c2_next = float(c2.iloc[i + 1]) if pd.notna(c2.iloc[i + 1]) else np.nan

            rows.append({
                "date": all_dates[i],
                "trade_date": all_dates[i + 1],
                "tk1": tk1, "tk2": tk2,
                "z": z, "abs_z": abs(z),
                "pf": pf,
                "o1": o1_next, "c1": c1_next,
                "o2": o2_next, "c2": c2_next,
            })

    if not rows:
        print("  シグナルなし")
        return {}

    sig = pd.DataFrame(rows)
    print(f"  シグナル: {len(sig)}件 ({sig['date'].nunique()}日)")

    # Top3選定（セクター非重複）
    daily_trades = []
    for dt, day_df in sig.groupby("date"):
        sorted_df = day_df.sort_values("abs_z", ascending=False)
        selected = []
        used_sectors: set[str] = set()
        for _, row in sorted_df.iterrows():
            s1 = sector_map.get(row["tk1"], "")
            s2 = sector_map.get(row["tk2"], "")
            if s1 in used_sectors or s2 in used_sectors:
                continue
            selected.append(row)
            if s1:
                used_sectors.add(s1)
            if s2:
                used_sectors.add(s2)
            if len(selected) == 3:
                break
        daily_trades.extend(selected)

    trades = pd.DataFrame(daily_trades)
    print(f"  Top3トレード: {len(trades)}件")

    # PnL算出 (固定比率: z>0 → sell tk1 100, buy tk2 200)
    def calc_pnl(row):
        o1, c1, o2, c2, z = row["o1"], row["c1"], row["o2"], row["c2"], row["z"]
        if pd.isna(o1) or pd.isna(c1) or pd.isna(o2) or pd.isna(c2):
            return 0.0
        if o1 == 0 or o2 == 0:
            return 0.0
        if z > 0:
            return (o1 - c1) * 100 + (c2 - o2) * 200
        else:
            return (c1 - o1) * 200 + (o2 - c2) * 100

    trades["pnl"] = trades.apply(calc_pnl, axis=1)
    trades["trade_dow"] = pd.to_datetime(trades["trade_date"]).dt.weekday

    result = {}
    for dow in range(5):
        sub = trades[trades["trade_dow"] == dow]
        if sub.empty:
            result[DOW_LABELS[dow]] = {"n": 0, "wins": 0, "wr": 0, "pf": None, "total_pnl": 0}
            continue
        stats = _calc_stats(sub["pnl"])
        result[DOW_LABELS[dow]] = stats
        pf_str = f"PF{stats['pf']}" if stats["pf"] else "N/A"
        print(f"  {DOW_LABELS[dow]}: N={stats['n']}, WR={stats['wr']}%, {pf_str}")

    result["overall"] = _calc_stats(trades["pnl"])
    return result


# ── 4. Calendar ──
def compute_calendar() -> dict:
    print("\n[4] Calendar (SQ-4, SQ+1, 1306)")
    result = {}

    for name, path, key in [
        ("sq4", SQ4_JSON, "stats"),
        ("sq_plus1", SQ_PLUS1_JSON, "stats"),
        ("etf1306", QE_JSON, "stats"),
    ]:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            stats = data.get(key, {})
            result[name] = {
                "pf": stats.get("pf"),
                "n": stats.get("total", 0),
                "wr": stats.get("wr", 0),
                "total_ret": stats.get("total_ret", 0),
            }
            print(f"  {name}: PF={stats.get('pf')}, N={stats.get('total', 0)}")
        except Exception as e:
            print(f"  {name}: 読み込み失敗 ({e})")
            result[name] = {"pf": None, "n": 0, "wr": 0, "total_ret": 0}

    # SQ-4 CME↓ subset
    try:
        with open(SQ4_JSON, encoding="utf-8") as f:
            sq4_data = json.load(f)
        cme_down = sq4_data.get("stats_cme_down", {})
        result["sq4_cme_down"] = {
            "pf": cme_down.get("pf"),
            "n": cme_down.get("total", 0),
            "wr": cme_down.get("wr", 0),
        }
    except Exception:
        pass

    return result


def main() -> int:
    print("=" * 60)
    print("Generate Strategy × Weekday Matrix")
    print("=" * 60)

    grok = compute_grok_weekday()
    weekday_edge = compute_weekday_edge()
    pairs = compute_pairs_weekday()
    calendar = compute_calendar()

    # 評価ラベル: PF基準で◎○△×
    def rating(pf: float | None) -> str:
        if pf is None:
            return "-"
        if pf >= 3.0:
            return "◎"
        if pf >= 1.5:
            return "○"
        if pf >= 1.0:
            return "△"
        return "×"

    matrix = {
        "generated_at": datetime.now().isoformat(),
        "weekdays": DOW_LABELS,
        "strategies": {
            "grok_short": {
                "label": "grok SHORT",
                "description": "prob<0.35 制度/いちにち",
                "by_weekday": grok,
            },
            "weekday_edge": {
                "label": "曜日アノマリー",
                "description": "20銘柄 L/S",
                "by_weekday": weekday_edge,
            },
            "pairs": {
                "label": "ペアトレード",
                "description": "V2_PAIRS Top3",
                "by_weekday": pairs,
            },
            "calendar": {
                "label": "Calendar",
                "description": "SQ-4/SQ+1/1306",
                "summary": calendar,
            },
        },
        "ratings": {},
    }

    # サマリーテーブル
    print("\n" + "=" * 60)
    print("Strategy × Weekday Matrix")
    print("=" * 60)
    header = f"{'曜日':>4} | {'grok SHORT':>12} | {'曜日20銘柄':>12} | {'pair Top3':>12}"
    print(header)
    print("-" * len(header))

    for dow_label in DOW_LABELS:
        g = grok.get(dow_label, {})
        w = weekday_edge.get(dow_label, {})
        p = pairs.get(dow_label, {})
        g_pf = g.get("pf")
        w_pf = w.get("pf")
        p_pf = p.get("pf")

        matrix["ratings"][dow_label] = {
            "grok_short": rating(g_pf),
            "weekday_edge": rating(w_pf),
            "pairs": rating(p_pf),
        }

        g_str = f"{rating(g_pf)} PF{g_pf:.2f}" if g_pf else "-"
        w_str = f"{rating(w_pf)} PF{w_pf:.2f}" if w_pf else "-"
        p_str = f"{rating(p_pf)} PF{p_pf:.2f}" if p_pf else "-"
        print(f"{dow_label:>4} | {g_str:>12} | {w_str:>12} | {p_str:>12}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(matrix, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
