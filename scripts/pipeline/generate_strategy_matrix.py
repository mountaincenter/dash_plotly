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
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from scripts.pipeline.generate_pairs_signals import (
    EXCLUDE_PAIRS as PAIR_EXCLUDE_PAIRS,
    EXCLUDE_SECTORS as PAIR_EXCLUDE_SECTORS,
    PF_MIN as PAIR_PF_MIN,
    V2_PAIRS as PAIR_DEFS,
    Z_ENTRY as PAIR_Z_ENTRY,
    calc_shares_min_lot,
    get_pair_health,
    load_pair_health_state,
)

GROK_ARCHIVE = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
PRICES_TOPIX = PARQUET_DIR / "granville" / "prices_topix.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
FINS_SUMMARY = PARQUET_DIR / "fins_summary.parquet"
ANALYSIS_DIR = ROOT / "data" / "analysis"
WEEKDAY_EDGE_JSON = ANALYSIS_DIR / "weekday_edge_trades.json"
SQ4_JSON = ANALYSIS_DIR / "sq4_trades.json"
SQ_PLUS1_JSON = ANALYSIS_DIR / "sq_plus1_trades.json"
QE_JSON = ANALYSIS_DIR / "quarter_end_effect.json"
OUTPUT_PATH = ANALYSIS_DIR / "strategy_matrix.json"

DOW_LABELS = ["月", "火", "水", "木", "金"]
PAIR_RISK_RET1_SPREAD_MAX = 0.08
GROK_PROB_SHORT_THRESHOLD = 0.45
GROK_PF_START_DATE = pd.Timestamp("2025-12-01")


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


def _bool_series(df: pd.DataFrame, column: str, default: bool = False) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index)
    return df[column].fillna(default).astype(bool)


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[column], errors="coerce")


def compute_grok_weekday() -> dict:
    print("\n[1] Grok SHORT")
    df = pd.read_parquet(GROK_ARCHIVE)

    date_col = "backtest_date" if "backtest_date" in df.columns else "selection_date"
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    if "ml_prob" not in df.columns:
        df["ml_prob"] = np.nan
    if "ml_prob_live" not in df.columns:
        df["ml_prob_live"] = np.nan
    df["ml_prob_effective"] = df["ml_prob"].combine_first(df["ml_prob_live"])

    pnl_source = "seg_1530" if "seg_1530" in df.columns else "profit_per_100_shares_phase2"
    df["expected_pnl_for_pf"] = _numeric_series(df, pnl_source)

    shortable_col = "shortable" if "shortable" in df.columns else "is_shortable"
    shortable = _bool_series(df, shortable_col)
    day_trade = _bool_series(df, "day_trade")
    ng = _bool_series(df, "ng")
    available_shares = _numeric_series(df, "day_trade_available_shares")

    executable_short = (
        ~ng
        & (
            shortable
            | (day_trade & ~shortable & (available_shares > 0))
        )
    )

    tdf = df[
        (df[date_col] >= GROK_PF_START_DATE)
        & df["ml_prob_effective"].notna()
        & df["expected_pnl_for_pf"].notna()
        & executable_short
        & (df["ml_prob_effective"] < GROK_PROB_SHORT_THRESHOLD)
    ].copy()
    tdf["credit_bucket"] = np.where(shortable.loc[tdf.index], "制度", "いちにち除0")

    print(
        "  ".join(
            [
                f"母集団: {GROK_PF_START_DATE.date()}以降",
                f"pnl={pnl_source}",
                f"SHORT=prob<{GROK_PROB_SHORT_THRESHOLD}",
                "制度+いちにち残あり",
                f"trades={len(tdf)}",
            ]
        )
    )

    def build_result(subset: pd.DataFrame, label: str) -> dict:
        result = {}
        print(f"  [{label}]")
        for dow in range(5):
            sub = subset[subset[date_col].dt.weekday == dow]
            pnl = sub["expected_pnl_for_pf"]
            stats = _calc_stats(pnl)
            result[DOW_LABELS[dow]] = stats
            pf_str = f"PF{stats['pf']}" if stats["pf"] else "N/A"
            print(f"    {DOW_LABELS[dow]}: N={stats['n']}, WR={stats['wr']}%, {pf_str}, 累計{stats['total_pnl']:+,}円")

        overall = _calc_stats(subset["expected_pnl_for_pf"])
        result["overall"] = overall
        result["_meta"] = {
            "definition": "recommendations_aligned_short",
            "period_start": GROK_PF_START_DATE.date().isoformat(),
            "pnl_source": pnl_source,
            "prob_threshold": GROK_PROB_SHORT_THRESHOLD,
            "credit_filter": label,
            "total_trades": len(subset),
        }
        return result

    return {
        "all": build_result(tdf, "制度 + いちにち残あり（NG除外）"),
        "seido": build_result(tdf[tdf["credit_bucket"] == "制度"], "制度"),
        "daytrade": build_result(tdf[tdf["credit_bucket"] == "いちにち除0"], "いちにち除0"),
    }


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


def _filter_pairs(pairs: list[tuple]) -> list[tuple]:
    active = []
    pair_health = load_pair_health_state()
    for tk1, tk2, lb, pf, full_n, revert_1d in pairs:
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        if any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in PAIR_EXCLUDE_SECTORS):
            continue
        if (tk1, tk2) in PAIR_EXCLUDE_PAIRS:
            continue
        if get_pair_health(pair_health, tk1, tk2)["pair_health_state"] == "SUSPENDED":
            continue
        if pf < PAIR_PF_MIN:
            continue
        active.append((tk1, tk2, lb, pf, full_n, revert_1d))
    return active


def _load_earnings_dates() -> dict[str, set[pd.Timestamp]]:
    dates: dict[str, set[pd.Timestamp]] = {}
    if FINS_SUMMARY.exists():
        fins = pd.read_parquet(FINS_SUMMARY)
        if {"Code", "DiscDate"}.issubset(fins.columns):
            fins = fins.copy()
            fins["date"] = pd.to_datetime(fins["DiscDate"], errors="coerce").dt.normalize()
            for _, row in fins.dropna(subset=["date"]).iterrows():
                code = str(row.get("Code", "")).replace(".T", "")[:4].zfill(4)
                ticker = f"{code}.T"
                dates.setdefault(ticker, set()).add(row["date"])

    announcements_path = PARQUET_DIR / "announcements.parquet"
    if announcements_path.exists():
        ann = pd.read_parquet(announcements_path)
        if "announcementDate" in ann.columns:
            ann = ann.copy()
            ann["date"] = pd.to_datetime(ann["announcementDate"], errors="coerce").dt.normalize()
            for _, row in ann.dropna(subset=["date"]).iterrows():
                ticker = str(row.get("ticker", row.get("code", ""))).replace(".T", "")
                if not ticker:
                    continue
                dates.setdefault(f"{ticker[:4]}.T", set()).add(row["date"])
    return dates


def _has_near_earnings(
    earnings_dates: dict[str, set[pd.Timestamp]],
    ticker: str,
    signal_date: pd.Timestamp,
    trade_date: pd.Timestamp,
) -> bool:
    dates = earnings_dates.get(ticker)
    if not dates:
        return False
    signal_date = pd.Timestamp(signal_date).normalize()
    trade_date = pd.Timestamp(trade_date).normalize()
    for base_date in (signal_date, trade_date):
        for delta in (-1, 0, 1):
            if base_date + pd.Timedelta(days=delta) in dates:
                return True
    return False


def compute_pairs_weekday() -> dict:
    print("\n[3] Pairs Top3 (risk filtered)")
    pairs = _filter_pairs(PAIR_DEFS)
    print(f"  アクティブペア: {len(pairs)}")

    prices = pd.read_parquet(PRICES_TOPIX)
    prices = prices.sort_values(["ticker", "date"])
    prices["date"] = pd.to_datetime(prices["date"])

    earnings_dates = _load_earnings_dates()

    tickers_needed = sorted({t for tk1, tk2, *_ in pairs for t in (tk1, tk2)})
    prices = prices[prices["ticker"].isin(tickers_needed)]

    pivot_close = prices.pivot_table(index="date", columns="ticker", values="Close")
    pivot_open = prices.pivot_table(index="date", columns="ticker", values="Open")
    pivot_ret1 = pivot_close.pct_change(fill_method=None)

    all_dates = pivot_close.index
    # 2020年以降
    start_idx = all_dates.searchsorted(pd.Timestamp("2020-01-01"))

    print("  z-score算出中...")
    rows = []
    skipped_ret1 = 0
    skipped_earnings = 0
    for tk1, tk2, lb, pf, full_n, revert_1d in pairs:
        if tk1 not in pivot_close.columns or tk2 not in pivot_close.columns:
            continue
        c1 = pivot_close[tk1]
        c2 = pivot_close[tk2]
        o1 = pivot_open[tk1]
        o2 = pivot_open[tk2]
        r1 = pivot_ret1[tk1]
        r2 = pivot_ret1[tk2]
        valid = c1.notna() & c2.notna() & (c1 > 0) & (c2 > 0)
        spread = np.log(c1 / c2)

        for i in range(max(start_idx, lb), len(all_dates) - 1):
            if not valid.iloc[i]:
                continue
            window = spread.iloc[i - lb:i].dropna()
            if len(window) < lb:
                continue
            mu = window.mean()
            sigma = window.std()
            if sigma == 0 or pd.isna(sigma):
                continue
            z = float((spread.iloc[i] - mu) / sigma)
            if abs(z) < PAIR_Z_ENTRY:
                continue

            signal_date = all_dates[i]
            trade_date = all_dates[i + 1]
            o1_next = float(o1.iloc[i + 1]) if pd.notna(o1.iloc[i + 1]) else np.nan
            c1_next = float(c1.iloc[i + 1]) if pd.notna(c1.iloc[i + 1]) else np.nan
            o2_next = float(o2.iloc[i + 1]) if pd.notna(o2.iloc[i + 1]) else np.nan
            c2_next = float(c2.iloc[i + 1]) if pd.notna(c2.iloc[i + 1]) else np.nan
            if pd.isna(o1_next) or pd.isna(c1_next) or pd.isna(o2_next) or pd.isna(c2_next):
                continue
            if o1_next <= 0 or o2_next <= 0:
                continue

            ret1_spread_abs = abs(float(r1.iloc[i]) - float(r2.iloc[i]))
            if pd.isna(ret1_spread_abs):
                ret1_spread_abs = 0.0
            if ret1_spread_abs >= PAIR_RISK_RET1_SPREAD_MAX:
                skipped_ret1 += 1
                continue

            earnings_near = _has_near_earnings(earnings_dates, tk1, signal_date, trade_date) or _has_near_earnings(
                earnings_dates, tk2, signal_date, trade_date
            )
            if earnings_near:
                skipped_earnings += 1
                continue

            shares1, shares2 = calc_shares_min_lot(o1_next, o2_next)
            if shares1 <= 0 or shares2 <= 0:
                continue

            if z > 0:
                pnl = (o1_next - c1_next) * shares1 + (c2_next - o2_next) * shares2
            else:
                pnl = (c1_next - o1_next) * shares1 + (o2_next - c2_next) * shares2

            rows.append({
                "date": signal_date,
                "trade_date": trade_date,
                "tk1": tk1, "tk2": tk2,
                "z": z, "abs_z": abs(z),
                "pf": pf, "full_n": full_n, "revert_1d": revert_1d,
                "o1": o1_next, "c1": c1_next,
                "o2": o2_next, "c2": c2_next,
                "shares1": shares1, "shares2": shares2,
                "ret1_spread_abs": ret1_spread_abs,
                "pnl": pnl,
            })

    if not rows:
        print("  シグナルなし")
        return {}

    sig = pd.DataFrame(rows)
    print(f"  シグナル: {len(sig)}件 ({sig['date'].nunique()}日)")
    print(f"  除外: ret1 spread>=8% {skipped_ret1}件, 決算近接 {skipped_earnings}件")

    # Top3選定（実運用dashboardと同じく |z| 降順）
    daily_trades = []
    for dt, day_df in sig.groupby("date"):
        daily_trades.extend(day_df.sort_values("abs_z", ascending=False).head(3).to_dict("records"))

    trades = pd.DataFrame(daily_trades)
    print(f"  Top3トレード: {len(trades)}件")

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
    result["_meta"] = {
        "rule": "V2_PAIRS Top3 by abs_z, correct lots, ret1_spread_abs<0.08, no earnings near signal/trade date",
        "ret1_spread_max": PAIR_RISK_RET1_SPREAD_MAX,
        "z_entry": PAIR_Z_ENTRY,
        "pf_min": PAIR_PF_MIN,
        "skipped_ret1_spread": skipped_ret1,
        "skipped_earnings_near": skipped_earnings,
    }
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

    grok_parts = compute_grok_weekday()
    grok = grok_parts["all"]
    grok_seido = grok_parts["seido"]
    grok_daytrade = grok_parts["daytrade"]
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
                "description": "recommendations準拠: prob<0.45 / 2025-12-01以降 / 制度+いちにち残あり",
                "by_weekday": grok,
            },
            "grok_short_seido": {
                "label": "Grok 制度",
                "description": "recommendations準拠: prob<0.45 / 2025-12-01以降 / 制度信用",
                "by_weekday": grok_seido,
            },
            "grok_short_daytrade": {
                "label": "Grok いちにち除0",
                "description": "recommendations準拠: prob<0.45 / 2025-12-01以降 / いちにち信用 残あり",
                "by_weekday": grok_daytrade,
            },
            "weekday_edge": {
                "label": "曜日アノマリー",
                "description": "20銘柄 L/S",
                "by_weekday": weekday_edge,
            },
            "pairs": {
                "label": "リスク回避Pair Top3",
                "description": "V2_PAIRS Top3 / 正ロット / ret1差<8% / 決算近接除外",
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
        gs = grok_seido.get(dow_label, {})
        gd = grok_daytrade.get(dow_label, {})
        w = weekday_edge.get(dow_label, {})
        p = pairs.get(dow_label, {})
        g_pf = g.get("pf")
        gs_pf = gs.get("pf")
        gd_pf = gd.get("pf")
        w_pf = w.get("pf")
        p_pf = p.get("pf")

        matrix["ratings"][dow_label] = {
            "grok_short": rating(g_pf),
            "grok_short_seido": rating(gs_pf),
            "grok_short_daytrade": rating(gd_pf),
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
