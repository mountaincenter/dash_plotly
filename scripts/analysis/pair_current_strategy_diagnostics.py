#!/usr/bin/env python3
"""Diagnostics for the currently implemented pair strategy.

This script uses the same launch point as the production pair pipeline:

- scripts.pipeline.generate_pairs_signals.V2_PAIRS
- data/parquet/granville/prices_topix.parquet
- Z_ENTRY / PF_MIN / EXCLUDE_PAIRS / EXCLUDE_SECTORS
- signal day close -> next business trading day open entry -> close exit
- lot sizing based on the signal-day close, matching generate_pairs_signals.py

Pair health is reported separately. Applying today's manual SUSPENDED state to
past trades would be future information, so the primary diagnostics do not use
pair_health_state. A secondary reference table shows what current exclusions
would remove.
"""

from __future__ import annotations

import html
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.pipeline.generate_pairs_signals import (  # noqa: E402
    EXCLUDE_PAIRS,
    EXCLUDE_SECTORS,
    PF_MIN,
    PRICES_TOPIX,
    Z_ENTRY,
    V2_PAIRS,
    calc_shares_min_lot,
    has_near_earnings,
    load_earnings_dates,
)

OUT_DIR = ROOT / "data" / "analysis"
HEALTH_STATE_PATH = OUT_DIR / "pair_health_state.json"

OUT_TRADES = OUT_DIR / "pair_current_strategy_diagnostics_trades.parquet"
OUT_SUMMARY = OUT_DIR / "pair_current_strategy_diagnostics_summary.csv"
OUT_MONTHLY = OUT_DIR / "pair_current_strategy_diagnostics_monthly.csv"
OUT_YEARLY = OUT_DIR / "pair_current_strategy_diagnostics_yearly.csv"
OUT_TAIL = OUT_DIR / "pair_current_strategy_diagnostics_tail.csv"
OUT_RANDOM = OUT_DIR / "pair_current_strategy_diagnostics_random.csv"
OUT_PAIR = OUT_DIR / "pair_current_strategy_diagnostics_pair.csv"
OUT_HTML = OUT_DIR / "pair_current_strategy_diagnostics.html"


def load_health_state() -> dict[str, str]:
    if not HEALTH_STATE_PATH.exists():
        return {}
    with HEALTH_STATE_PATH.open(encoding="utf-8") as f:
        data = json.load(f)
    pairs = data.get("pairs", {})
    if not isinstance(pairs, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in pairs.items():
        if isinstance(value, dict):
            out[str(key)] = str(value.get("state", "ACTIVE")).upper()
    return out


def current_health_state(health: dict[str, str], tk1: str, tk2: str) -> str:
    return health.get(f"{tk1}/{tk2}") or health.get(f"{tk2}/{tk1}") or "ACTIVE"


def profit_factor(pnl: pd.Series) -> float:
    gains = pnl[pnl > 0].sum()
    losses = -pnl[pnl < 0].sum()
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def max_drawdown(pnl: pd.Series) -> float:
    equity = pnl.fillna(0).cumsum()
    dd = equity - equity.cummax()
    return float(dd.min()) if len(dd) else 0.0


def max_tuw_days(df: pd.DataFrame, pnl_col: str) -> int:
    if df.empty:
        return 0
    daily = df[["trade_date", pnl_col]].dropna().groupby("trade_date", as_index=False)[pnl_col].sum()
    if daily.empty:
        return 0
    daily = daily.sort_values("trade_date")
    equity = daily[pnl_col].cumsum()
    high = equity.cummax()
    below = equity < high
    max_days = 0
    start: pd.Timestamp | None = None
    prev_date: pd.Timestamp | None = None
    for date, is_below in zip(daily["trade_date"], below, strict=False):
        if is_below and start is None:
            start = prev_date if prev_date is not None else date
        if not is_below and start is not None:
            max_days = max(max_days, int((date - start).days))
            start = None
        prev_date = date
    if start is not None and prev_date is not None:
        max_days = max(max_days, int((prev_date - start).days))
    return max_days


def cvar5(pnl: pd.Series) -> float:
    pnl = pnl.dropna()
    if pnl.empty:
        return np.nan
    threshold = pnl.quantile(0.05)
    return float(pnl[pnl <= threshold].mean())


def metrics(df: pd.DataFrame, pnl_col: str = "pnl") -> dict[str, float | int]:
    pnl = df[pnl_col].dropna()
    if pnl.empty:
        return {
            "n": 0,
            "days": 0,
            "pf": np.nan,
            "win_rate": np.nan,
            "total": 0.0,
            "avg": np.nan,
            "max_dd": np.nan,
            "max_loss": np.nan,
            "p05": np.nan,
            "cvar5": np.nan,
            "tuw_days": 0,
        }
    return {
        "n": int(len(pnl)),
        "days": int(df["trade_date"].nunique()),
        "pf": profit_factor(pnl),
        "win_rate": float((pnl > 0).mean() * 100),
        "total": float(pnl.sum()),
        "avg": float(pnl.mean()),
        "max_dd": max_drawdown(pnl),
        "max_loss": float(pnl.min()),
        "p05": float(pnl.quantile(0.05)),
        "cvar5": cvar5(pnl),
        "tuw_days": max_tuw_days(df, pnl_col),
    }


def active_pair_defs() -> list[tuple[str, str, int, float, int, float]]:
    rows = []
    for tk1, tk2, lb, full_pf, full_n, revert_1d in V2_PAIRS:
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        if any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in EXCLUDE_SECTORS):
            continue
        if (tk1, tk2) in EXCLUDE_PAIRS:
            continue
        rows.append((tk1, tk2, lb, full_pf, full_n, revert_1d))
    return rows


def load_sector_map() -> dict[str, str]:
    path = ROOT / "data" / "parquet" / "meta_jquants.parquet"
    if not path.exists():
        return {}
    meta = pd.read_parquet(path)
    if "ticker" not in meta.columns or "sectors" not in meta.columns:
        return {}
    return dict(zip(meta["ticker"].astype(str), meta["sectors"].astype(str)))


def build_trades() -> pd.DataFrame:
    if not PRICES_TOPIX.exists():
        raise FileNotFoundError(PRICES_TOPIX)
    prices = pd.read_parquet(PRICES_TOPIX)
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.dropna(subset=["Open", "Close"]).sort_values(["ticker", "date"])

    pair_defs = active_pair_defs()
    tickers = sorted({t for tk1, tk2, *_ in pair_defs for t in (tk1, tk2)})
    px = prices[prices["ticker"].isin(tickers)].copy()
    px["ret1"] = px.groupby("ticker")["Close"].pct_change(fill_method=None)
    px["ret5"] = px.groupby("ticker")["Close"].pct_change(5, fill_method=None)
    px["ret20"] = px.groupby("ticker")["Close"].pct_change(20, fill_method=None)
    px["ma25"] = px.groupby("ticker")["Close"].transform(lambda s: s.shift(1).rolling(25, min_periods=20).mean())
    px["vs25"] = px["Close"] / px["ma25"] - 1
    px["vol_med20"] = px.groupby("ticker")["Volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).median())
    px["vol_ratio20"] = px["Volume"] / px["vol_med20"]

    close = px.pivot_table(index="date", columns="ticker", values="Close")
    open_ = px.pivot_table(index="date", columns="ticker", values="Open")
    ret1 = px.pivot_table(index="date", columns="ticker", values="ret1")
    ret5 = px.pivot_table(index="date", columns="ticker", values="ret5")
    ret20 = px.pivot_table(index="date", columns="ticker", values="ret20")
    vs25 = px.pivot_table(index="date", columns="ticker", values="vs25")
    vol_ratio20 = px.pivot_table(index="date", columns="ticker", values="vol_ratio20")

    all_dates = close.index.sort_values()
    start_idx = all_dates.searchsorted(pd.Timestamp("2020-01-01"))
    earnings_dates = load_earnings_dates()
    health = load_health_state()
    sector_map = load_sector_map()

    rows: list[dict[str, object]] = []
    for tk1, tk2, lb, full_pf, full_n, revert_1d in pair_defs:
        if tk1 not in close.columns or tk2 not in close.columns:
            continue
        c1s = close[tk1]
        c2s = close[tk2]
        o1s = open_[tk1]
        o2s = open_[tk2]
        valid = c1s.notna() & c2s.notna() & (c1s > 0) & (c2s > 0)
        spread = np.log(c1s / c2s)
        for i in range(max(start_idx, lb), len(all_dates) - 1):
            if not valid.iloc[i]:
                continue
            window = spread.iloc[i - lb : i].dropna()
            if len(window) < lb:
                continue
            sigma = window.std()
            if sigma == 0 or pd.isna(sigma):
                continue
            signal_date = pd.Timestamp(all_dates[i]).normalize()
            trade_date = pd.Timestamp(all_dates[i + 1]).normalize()
            z = float((spread.iloc[i] - window.mean()) / sigma)
            if abs(z) < Z_ENTRY:
                continue

            signal_c1 = float(c1s.iloc[i])
            signal_c2 = float(c2s.iloc[i])
            trade_o1 = o1s.iloc[i + 1]
            trade_c1 = c1s.iloc[i + 1]
            trade_o2 = o2s.iloc[i + 1]
            trade_c2 = c2s.iloc[i + 1]
            vals = [trade_o1, trade_c1, trade_o2, trade_c2]
            if any(pd.isna(x) or float(x) <= 0 for x in vals):
                continue

            shares1, shares2 = calc_shares_min_lot(signal_c1, signal_c2)
            if z > 0:
                direction = "short_tk1"
                long_tk, short_tk = tk2, tk1
                pnl = (trade_o1 - trade_c1) * shares1 + (trade_c2 - trade_o2) * shares2
            else:
                direction = "long_tk1"
                long_tk, short_tk = tk1, tk2
                pnl = (trade_c1 - trade_o1) * shares1 + (trade_o2 - trade_c2) * shares2

            r1 = ret1.at[signal_date, tk1] if tk1 in ret1.columns and signal_date in ret1.index else np.nan
            r2 = ret1.at[signal_date, tk2] if tk2 in ret1.columns and signal_date in ret1.index else np.nan
            r5_1 = ret5.at[signal_date, tk1] if tk1 in ret5.columns and signal_date in ret5.index else np.nan
            r5_2 = ret5.at[signal_date, tk2] if tk2 in ret5.columns and signal_date in ret5.index else np.nan
            r20_1 = ret20.at[signal_date, tk1] if tk1 in ret20.columns and signal_date in ret20.index else np.nan
            r20_2 = ret20.at[signal_date, tk2] if tk2 in ret20.columns and signal_date in ret20.index else np.nan
            vs25_1 = vs25.at[signal_date, tk1] if tk1 in vs25.columns and signal_date in vs25.index else np.nan
            vs25_2 = vs25.at[signal_date, tk2] if tk2 in vs25.columns and signal_date in vs25.index else np.nan
            vol1 = vol_ratio20.at[signal_date, tk1] if tk1 in vol_ratio20.columns and signal_date in vol_ratio20.index else np.nan
            vol2 = vol_ratio20.at[signal_date, tk2] if tk2 in vol_ratio20.columns and signal_date in vol_ratio20.index else np.nan
            ret1_spread_abs = abs(float(r1) - float(r2)) if pd.notna(r1) and pd.notna(r2) else np.nan
            earnings_near = has_near_earnings(earnings_dates, tk1, signal_date, trade_date) or has_near_earnings(
                earnings_dates, tk2, signal_date, trade_date
            )
            risk_ok = bool((pd.isna(ret1_spread_abs) or ret1_spread_abs < 0.08) and not earnings_near)
            gap1 = float(trade_o1) / signal_c1 - 1
            gap2 = float(trade_o2) / signal_c2 - 1
            current_health = current_health_state(health, tk1, tk2)
            notional1 = signal_c1 * shares1
            notional2 = signal_c2 * shares2
            long_ret1 = r1 if long_tk == tk1 else r2
            short_ret1 = r1 if short_tk == tk1 else r2
            long_ret5 = r5_1 if long_tk == tk1 else r5_2
            short_ret5 = r5_1 if short_tk == tk1 else r5_2
            long_ret20 = r20_1 if long_tk == tk1 else r20_2
            short_ret20 = r20_1 if short_tk == tk1 else r20_2
            long_vs25 = vs25_1 if long_tk == tk1 else vs25_2
            short_vs25 = vs25_1 if short_tk == tk1 else vs25_2
            long_vol_ratio20 = vol1 if long_tk == tk1 else vol2
            short_vol_ratio20 = vol1 if short_tk == tk1 else vol2
            rows.append(
                {
                    "signal_date": signal_date,
                    "trade_date": trade_date,
                    "tk1": tk1,
                    "tk2": tk2,
                    "pair": f"{tk1}/{tk2}",
                    "z": z,
                    "abs_z": abs(z),
                    "direction": direction,
                    "signal_c1": signal_c1,
                    "signal_c2": signal_c2,
                    "o1": float(trade_o1),
                    "c1": float(trade_c1),
                    "o2": float(trade_o2),
                    "c2": float(trade_c2),
                    "shares1": shares1,
                    "shares2": shares2,
                    "notional1": notional1,
                    "notional2": notional2,
                    "imbalance_pct": abs(notional1 - notional2) / max(notional1, notional2) * 100,
                    "pnl": float(pnl),
                    "full_pf": full_pf,
                    "full_n": full_n,
                    "revert_1d": revert_1d,
                    "ret1_tk1": r1,
                    "ret1_tk2": r2,
                    "ret5_tk1": r5_1,
                    "ret5_tk2": r5_2,
                    "ret20_tk1": r20_1,
                    "ret20_tk2": r20_2,
                    "vs25_tk1": vs25_1,
                    "vs25_tk2": vs25_2,
                    "vol_ratio20_tk1": vol1,
                    "vol_ratio20_tk2": vol2,
                    "gap1": gap1,
                    "gap2": gap2,
                    "ret1_spread_abs": ret1_spread_abs,
                    "earnings_near": earnings_near,
                    "risk_ok": risk_ok,
                    "current_health_state": current_health,
                    "long_tk": long_tk,
                    "short_tk": short_tk,
                    "long_ret1": long_ret1,
                    "short_ret1": short_ret1,
                    "long_ret5": long_ret5,
                    "short_ret5": short_ret5,
                    "long_ret20": long_ret20,
                    "short_ret20": short_ret20,
                    "long_vs25": long_vs25,
                    "short_vs25": short_vs25,
                    "long_vol_ratio20": long_vol_ratio20,
                    "short_vol_ratio20": short_vol_ratio20,
                    "sector1": sector_map.get(tk1, ""),
                    "sector2": sector_map.get(tk2, ""),
                }
            )
    out = pd.DataFrame(rows).sort_values(["signal_date", "abs_z"], ascending=[True, False])
    return add_entry_guard_flags(out)


def add_entry_guard_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add non-optimized defensive flags for practical entry selection.

    These flags are not fitted for PF. They mark situations where pair
    homogeneity is likely broken or one leg is hard to fade in practice.
    """
    df = df.copy()
    abs_gap_max = df[["gap1", "gap2"]].abs().max(axis=1)
    abs_gap_min = df[["gap1", "gap2"]].abs().min(axis=1)
    abs_ret1_max = df[["ret1_tk1", "ret1_tk2"]].abs().max(axis=1)
    abs_ret1_min = df[["ret1_tk1", "ret1_tk2"]].abs().min(axis=1)
    vol_max = df[["vol_ratio20_tk1", "vol_ratio20_tk2"]].max(axis=1)
    vol_min = df[["vol_ratio20_tk1", "vol_ratio20_tk2"]].min(axis=1)

    flags = pd.DataFrame(index=df.index)
    flags["short_leg_very_strong"] = df["short_ret5"].ge(0.10) | df["short_ret1"].ge(0.05)
    flags["long_leg_very_weak"] = df["long_ret5"].le(-0.10) | df["long_ret1"].le(-0.05)
    flags["short_leg_overheated"] = df["short_vs25"].ge(0.20)
    flags["long_leg_broken"] = df["long_vs25"].le(-0.15)
    flags["one_side_gap"] = abs_gap_max.ge(0.03) & abs_gap_min.lt(0.015)
    flags["one_side_move"] = abs_ret1_max.ge(0.04) & abs_ret1_min.lt(0.015)
    flags["one_side_volume"] = vol_max.ge(3.0) & vol_min.lt(2.0)

    df["guard_short_strong"] = flags["short_leg_very_strong"]
    df["guard_long_weak"] = flags["long_leg_very_weak"]
    df["guard_one_side"] = flags[["one_side_gap", "one_side_move", "one_side_volume"]].any(axis=1)
    df["guard_extreme"] = flags[["short_leg_overheated", "long_leg_broken"]].any(axis=1)

    def reasons(row: pd.Series) -> str:
        names = []
        if row["guard_short_strong"]:
            names.append("short脚が強すぎる")
        if row["guard_long_weak"]:
            names.append("long脚が弱すぎる")
        if row["guard_one_side"]:
            names.append("片脚だけgap/出来高/前日変化")
        if row["guard_extreme"]:
            names.append("25日線乖離が極端")
        return ",".join(names)

    df["guard_reason"] = df.apply(reasons, axis=1)
    df["entry_guard_defensive_ok"] = ~(df["guard_short_strong"] | df["guard_long_weak"] | df["guard_one_side"])
    df["entry_guard_strict_ok"] = df["entry_guard_defensive_ok"] & ~df["guard_extreme"]
    return df


def select_daily(
    df: pd.DataFrame,
    top_n: int,
    *,
    apply_current_health: bool,
    sector_dedupe: bool,
    guard: str = "none",
) -> pd.DataFrame:
    base = df[(df["full_pf"] >= PF_MIN) & (df["risk_ok"])].copy()
    if apply_current_health:
        base = base[base["current_health_state"].eq("ACTIVE")].copy()
    if guard == "defensive":
        base = base[base["entry_guard_defensive_ok"]].copy()
    elif guard == "strict":
        base = base[base["entry_guard_strict_ok"]].copy()
    elif guard != "none":
        raise ValueError(f"unknown guard: {guard}")
    selected: list[dict[str, object]] = []
    for _, day in base.groupby("signal_date"):
        used_sectors: set[str] = set()
        count = 0
        for _, row in day.sort_values("abs_z", ascending=False).iterrows():
            sector = str(row.get("sector1") or "")
            if sector_dedupe and sector and sector in used_sectors:
                continue
            selected.append(row.to_dict())
            if sector_dedupe and sector:
                used_sectors.add(sector)
            count += 1
            if count >= top_n:
                break
    return pd.DataFrame(selected)


def summary_rows(trades: pd.DataFrame) -> pd.DataFrame:
    variants = [
        ("Top1 現行基準", select_daily(trades, 1, apply_current_health=False, sector_dedupe=True)),
        ("Top3 現行基準", select_daily(trades, 3, apply_current_health=False, sector_dedupe=True)),
        ("Top1 防御選別", select_daily(trades, 1, apply_current_health=False, sector_dedupe=True, guard="defensive")),
        ("Top3 防御選別", select_daily(trades, 3, apply_current_health=False, sector_dedupe=True, guard="defensive")),
        ("Top1 厳格選別", select_daily(trades, 1, apply_current_health=False, sector_dedupe=True, guard="strict")),
        ("Top3 厳格選別", select_daily(trades, 3, apply_current_health=False, sector_dedupe=True, guard="strict")),
        ("Top1 health現在除外参考", select_daily(trades, 1, apply_current_health=True, sector_dedupe=True)),
        ("Top3 health現在除外参考", select_daily(trades, 3, apply_current_health=True, sector_dedupe=True)),
        ("Top3 sector重複許容", select_daily(trades, 3, apply_current_health=False, sector_dedupe=False)),
        ("全entry候補", trades[(trades["full_pf"] >= PF_MIN) & (trades["risk_ok"])].copy()),
    ]
    rows: list[dict[str, object]] = []
    periods = [
        ("all", lambda d: d),
        ("train_2020_2023", lambda d: d[d["trade_date"] < pd.Timestamp("2024-01-01")]),
        ("test_2024_plus", lambda d: d[d["trade_date"] >= pd.Timestamp("2024-01-01")]),
        ("y2026", lambda d: d[d["trade_date"] >= pd.Timestamp("2026-01-01")]),
    ]
    for label, df in variants:
        for period, func in periods:
            sub = func(df).sort_values("trade_date")
            rows.append({"label": label, "period": period, **metrics(sub)})
    return pd.DataFrame(rows)


def tail_rows(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for label, df in [
        ("Top1 現行基準", select_daily(trades, 1, apply_current_health=False, sector_dedupe=True)),
        ("Top3 現行基準", select_daily(trades, 3, apply_current_health=False, sector_dedupe=True)),
    ]:
        df = df.sort_values("trade_date")
        for cut in [0.0, 0.01, 0.05, 0.10]:
            sub = df.copy()
            if cut > 0 and not sub.empty:
                threshold = sub["pnl"].quantile(1 - cut)
                sub = sub[sub["pnl"] < threshold]
            rows.append({"label": label, "top_win_removed": f"{cut:.0%}", **metrics(sub)})
    return pd.DataFrame(rows)


def monthly_yearly_rows(trades: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows_m: list[dict[str, object]] = []
    rows_y: list[dict[str, object]] = []
    for top_n in [1, 3]:
        df = select_daily(trades, top_n, apply_current_health=False, sector_dedupe=True, guard="defensive").copy()
        df["month"] = df["trade_date"].dt.to_period("M").astype(str)
        df["year"] = df["trade_date"].dt.year.astype(str)
        for month, sub in df.groupby("month"):
            rows_m.append({"top_n": top_n, "month": month, **metrics(sub.sort_values("trade_date"))})
        for year, sub in df.groupby("year"):
            rows_y.append({"top_n": top_n, "year": year, **metrics(sub.sort_values("trade_date"))})
    return pd.DataFrame(rows_m), pd.DataFrame(rows_y)


def random_rows(trades: pd.DataFrame, trials: int = 1000) -> pd.DataFrame:
    rng = np.random.default_rng(20260527)
    rows: list[dict[str, object]] = []
    for top_n in [1, 3]:
        df = select_daily(trades, top_n, apply_current_health=False, sector_dedupe=True).sort_values("trade_date")
        pnl = df["pnl"].fillna(0).to_numpy()
        actual_total = float(pnl.sum())
        actual_pf = profit_factor(pd.Series(pnl))
        totals = []
        pfs = []
        for _ in range(trials):
            trial = pd.Series(pnl * rng.choice(np.array([-1.0, 1.0]), size=len(pnl)))
            totals.append(float(trial.sum()))
            pfs.append(profit_factor(trial))
        totals_a = np.array(totals)
        pfs_a = np.array(pfs)
        rows.append(
            {
                "label": f"Top{top_n} 現行基準",
                "actual_total": actual_total,
                "actual_pf": actual_pf,
                "random_total_mean": float(np.mean(totals_a)),
                "random_total_p95": float(np.quantile(totals_a, 0.95)),
                "actual_total_percentile": float((totals_a <= actual_total).mean() * 100),
                "random_pf_mean": float(np.nanmean(pfs_a)),
                "random_pf_p95": float(np.nanquantile(pfs_a, 0.95)),
                "actual_pf_percentile": float((pfs_a <= actual_pf).mean() * 100),
                "trials": trials,
            }
        )
    return pd.DataFrame(rows)


def pair_rows(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    selected = select_daily(trades, 3, apply_current_health=False, sector_dedupe=True, guard="defensive")
    for pair, sub in selected.groupby("pair"):
        rows.append({"pair": pair, **metrics(sub.sort_values("trade_date"))})
    return pd.DataFrame(rows).sort_values("total", ascending=False)


def blocked_rows(trades: pd.DataFrame) -> pd.DataFrame:
    base = trades[(trades["full_pf"] >= PF_MIN) & (trades["risk_ok"])].copy()
    rows = []
    checks = [
        ("short脚が強すぎる", base["guard_short_strong"]),
        ("long脚が弱すぎる", base["guard_long_weak"]),
        ("片脚だけgap/出来高/前日変化", base["guard_one_side"]),
        ("25日線乖離が極端", base["guard_extreme"]),
        ("防御選別で除外", ~base["entry_guard_defensive_ok"]),
        ("厳格選別で除外", ~base["entry_guard_strict_ok"]),
    ]
    for label, mask in checks:
        sub = base[mask].sort_values("trade_date")
        rows.append({"blocked_reason": label, **metrics(sub)})
    return pd.DataFrame(rows)


def fmt(value: object, kind: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    if isinstance(value, float) and np.isinf(value):
        return "inf"
    if kind == "pf":
        return f"{float(value):.2f}"
    if kind == "pct":
        return f"{float(value):.1f}%"
    if kind == "int":
        return f"{int(round(float(value))):,}"
    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):,.0f}"
    return html.escape(str(value))


def table(df: pd.DataFrame, cols: list[tuple[str, str, str]], limit: int | None = None) -> str:
    if df.empty:
        return "<p class='note'>データなし</p>"
    show = df.head(limit) if limit else df
    head = "".join(f"<th class='{'r' if kind else ''}'>{html.escape(label)}</th>" for _, label, kind in cols)
    body = []
    for _, row in show.iterrows():
        cells = []
        for key, _, kind in cols:
            value = row.get(key)
            cls = "r" if kind else ""
            try:
                num = float(value)
                if key in {"total", "avg", "max_dd", "max_loss", "p05", "cvar5", "actual_total"}:
                    cls += " pos" if num > 0 else " neg" if num < 0 else ""
                if key in {"pf", "actual_pf"}:
                    cls += " pos" if num >= 1.5 else " warn" if num >= 1.0 else " neg"
            except Exception:
                pass
            cells.append(f"<td class='{cls}'>{fmt(value, kind)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def render_html(
    trades: pd.DataFrame,
    summary: pd.DataFrame,
    monthly: pd.DataFrame,
    yearly: pd.DataFrame,
    tail: pd.DataFrame,
    random_df: pd.DataFrame,
    pair_df: pd.DataFrame,
    blocked_df: pd.DataFrame,
) -> str:
    generated = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    date_min = trades["trade_date"].min().strftime("%Y-%m-%d")
    date_max = trades["trade_date"].max().strftime("%Y-%m-%d")
    metric_cols = [
        ("label", "対象", ""),
        ("period", "期間", ""),
        ("n", "N", "int"),
        ("days", "日数", "int"),
        ("pf", "PF", "pf"),
        ("win_rate", "勝率", "pct"),
        ("total", "損益", "int"),
        ("avg", "平均", "int"),
        ("max_dd", "DD", "int"),
        ("max_loss", "最大損失", "int"),
        ("p05", "左尾p05", "int"),
        ("cvar5", "CVaR5", "int"),
        ("tuw_days", "TUW日", "int"),
    ]
    short_cols = metric_cols[:1] + metric_cols[2:]
    tail_cols = [
        ("label", "対象", ""),
        ("top_win_removed", "上位勝ち除外", ""),
        ("n", "N", "int"),
        ("pf", "PF", "pf"),
        ("total", "損益", "int"),
        ("max_dd", "DD", "int"),
        ("max_loss", "最大損失", "int"),
        ("cvar5", "CVaR5", "int"),
    ]
    random_cols = [
        ("label", "対象", ""),
        ("actual_total", "実損益", "int"),
        ("actual_pf", "実PF", "pf"),
        ("random_total_mean", "ランダム平均", "int"),
        ("random_total_p95", "ランダム95%", "int"),
        ("actual_total_percentile", "実損益pctile", "pct"),
        ("random_pf_p95", "ランダムPF95%", "pf"),
        ("actual_pf_percentile", "実PFpctile", "pct"),
    ]
    year_cols = [
        ("top_n", "Top", "int"),
        ("year", "年", ""),
        ("n", "N", "int"),
        ("pf", "PF", "pf"),
        ("total", "損益", "int"),
        ("max_dd", "DD", "int"),
        ("max_loss", "最大損失", "int"),
        ("cvar5", "CVaR5", "int"),
    ]
    month_cols = [
        ("top_n", "Top", "int"),
        ("month", "月", ""),
        ("n", "N", "int"),
        ("pf", "PF", "pf"),
        ("total", "損益", "int"),
        ("max_dd", "DD", "int"),
        ("max_loss", "最大損失", "int"),
    ]
    pair_cols = [
        ("pair", "pair", ""),
        ("n", "N", "int"),
        ("pf", "PF", "pf"),
        ("total", "損益", "int"),
        ("max_dd", "DD", "int"),
        ("max_loss", "最大損失", "int"),
        ("cvar5", "CVaR5", "int"),
    ]
    main = summary[summary["period"].eq("all")].copy()
    oos = summary[
        summary["label"].isin(["Top1 現行基準", "Top3 現行基準", "Top1 防御選別", "Top3 防御選別"])
        & summary["period"].isin(["train_2020_2023", "test_2024_plus", "y2026"])
    ].copy()
    blocked_cols = [
        ("blocked_reason", "除外理由", ""),
        ("n", "N", "int"),
        ("pf", "PF", "pf"),
        ("win_rate", "勝率", "pct"),
        ("total", "損益", "int"),
        ("max_dd", "DD", "int"),
        ("max_loss", "最大損失", "int"),
        ("cvar5", "CVaR5", "int"),
    ]
    return f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Current Pair Strategy Diagnostics</title>
  <style>
    :root {{ --bg:#09090b; --card:#18181b; --line:#27272a; --text:#fafafa; --muted:#a1a1aa; --pos:#34d399; --neg:#fb7185; --warn:#fbbf24; }}
    body {{ margin:0; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif; line-height:1.6; }}
    main {{ max-width:1360px; margin:0 auto; padding:24px; }}
    h1 {{ font-size:28px; margin:0 0 4px; }}
    h2 {{ font-size:20px; margin:0 0 10px; }}
    .note {{ color:var(--muted); font-size:14px; }}
    .grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:16px 0; }}
    .card,section {{ background:var(--card); border:1px solid var(--line); border-radius:10px; }}
    .card {{ padding:14px; }}
    .label {{ color:var(--muted); font-size:12px; }}
    .value {{ font-size:24px; font-weight:700; font-variant-numeric:tabular-nums; }}
    section {{ padding:18px; margin:14px 0; overflow-x:auto; }}
    table {{ border-collapse:collapse; width:100%; min-width:980px; font-size:13px; }}
    th,td {{ padding:7px 10px; border-bottom:1px solid rgba(255,255,255,.07); vertical-align:top; }}
    th {{ text-align:left; color:var(--muted); background:rgba(255,255,255,.03); }}
    .r {{ text-align:right; font-variant-numeric:tabular-nums; }}
    .pos {{ color:var(--pos); font-weight:650; }}
    .neg {{ color:var(--neg); font-weight:650; }}
    .warn {{ color:var(--warn); font-weight:650; }}
  </style>
</head>
<body>
<main>
  <h1>Current Pair Strategy Diagnostics</h1>
  <p class="note">現行実装準拠。レジーム未導入。health現在除外は未来情報を含むため参考扱い。期間: {date_min} - {date_max} / 生成: {generated}</p>
  <div class="grid">
    <div class="card"><div class="label">価格元</div><div class="value">prices_topix</div></div>
    <div class="card"><div class="label">pair定義</div><div class="value">V2_PAIRS</div></div>
    <div class="card"><div class="label">entry</div><div class="value">Z>=2 / PF>=1.5</div></div>
    <div class="card"><div class="label">決済</div><div class="value">翌寄→大引</div></div>
  </div>
  <section><h2>1. 現行基準 vs 防御選別</h2><p class="note">防御選別はPF最大化ではなく、short脚が強すぎる/long脚が弱すぎる/片脚だけ異常を除外する。厳格選別は25日線乖離極端も除外。</p>{table(main, metric_cols)}</section>
  <section><h2>2. dev/OOS/2026</h2><p class="note">2020-2023をdev、2024以降をOOS相当として固定条件で確認。防御選別がOOSでも壊れないかを見る。</p>{table(oos, metric_cols)}</section>
  <section><h2>3. 右テール依存</h2><p class="note">上位勝ちを除外しても構造が残るか。崩れるなら少数の大勝ち頼み。</p>{table(tail, tail_cols)}</section>
  <section><h2>4. ランダム方向比較</h2><p class="note">同じタイミングで方向だけランダム反転。実績がランダム95%を超えるか確認。</p>{table(random_df, random_cols)}</section>
  <section><h2>5. 除外された取引の損益</h2><p class="note">ここがマイナスまたは左尾悪化なら、防御条件として意味がある。プラスなら機会損失。</p>{table(blocked_df, blocked_cols)}</section>
  <section><h2>6. 年次安定性（防御選別）</h2>{table(yearly.sort_values(["top_n", "year"]), year_cols)}</section>
  <section><h2>7. 月次安定性 直近36か月（防御選別）</h2>{table(monthly.sort_values(["top_n", "month"]).tail(72), month_cols)}</section>
  <section><h2>8. pair集中 Top3（防御選別）</h2><p class="note">Top3に採用されたpairごとの損益。偏りが強ければ理論ではなく個別pair依存。</p>{table(pair_df, pair_cols, 80)}</section>
</main>
</body>
</html>
"""


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    trades = build_trades()
    summary = summary_rows(trades)
    tail = tail_rows(trades)
    monthly, yearly = monthly_yearly_rows(trades)
    random_df = random_rows(trades)
    pair_df = pair_rows(trades)
    blocked_df = blocked_rows(trades)

    trades.to_parquet(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)
    monthly.to_csv(OUT_MONTHLY, index=False)
    yearly.to_csv(OUT_YEARLY, index=False)
    tail.to_csv(OUT_TAIL, index=False)
    random_df.to_csv(OUT_RANDOM, index=False)
    pair_df.to_csv(OUT_PAIR, index=False)
    blocked_df.to_csv(OUT_DIR / "pair_current_strategy_diagnostics_blocked.csv", index=False)
    OUT_HTML.write_text(render_html(trades, summary, monthly, yearly, tail, random_df, pair_df, blocked_df), encoding="utf-8")

    print(f"[OK] trades: {OUT_TRADES} rows={len(trades):,}")
    print(f"[OK] summary: {OUT_SUMMARY}")
    print(f"[OK] monthly: {OUT_MONTHLY}")
    print(f"[OK] yearly: {OUT_YEARLY}")
    print(f"[OK] tail: {OUT_TAIL}")
    print(f"[OK] random: {OUT_RANDOM}")
    print(f"[OK] pair: {OUT_PAIR}")
    print(f"[OK] html: {OUT_HTML}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
