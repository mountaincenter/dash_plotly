#!/usr/bin/env python3
"""MECE analysis for pair leg momentum risk.

The pair signal defines one long leg and one short leg.  This analysis classifies
every selected trade into a 3x3 grid:

    long leg state  ∈ {weak, neutral, strong}
    short leg state ∈ {weak, neutral, strong}

The goal is not to invent a filter first.  It is to find where losses, drawdown,
and left-tail risk actually sit.
"""
from __future__ import annotations

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
    V2_PAIRS,
    Z_ENTRY,
    calc_shares_min_lot,
)

PARQUET_DIR = ROOT / "data" / "parquet"
OUT_DIR = ROOT / "data" / "analysis"

RET1_STRONG = 0.03
RET1_WEAK = -0.03
RET5_STRONG = 0.05
RET5_WEAK = -0.05


def active_pairs() -> list[tuple[str, str, int, float, int, float]]:
    rows = []
    for tk1, tk2, lb, pf, n, r1d in V2_PAIRS:
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        if any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in EXCLUDE_SECTORS):
            continue
        if (tk1, tk2) in EXCLUDE_PAIRS:
            continue
        if pf < PF_MIN:
            continue
        rows.append((tk1, tk2, lb, pf, n, r1d))
    return rows


def stats(pnls: pd.Series) -> dict[str, float | int | None]:
    p = pd.Series(pnls).dropna()
    if p.empty:
        return {"n": 0, "pf": None, "wr": 0.0, "pnl": 0, "avg": 0.0, "max_dd": 0, "max_loss": 0, "p05": 0}
    wins = p[p > 0]
    losses = p[p < 0]
    gp = float(wins.sum())
    gl = float(-losses.sum())
    eq = p.cumsum()
    dd = eq - eq.cummax()
    return {
        "n": int(len(p)),
        "pf": round(gp / gl, 3) if gl > 0 else None,
        "wr": round(float((p > 0).mean() * 100), 1),
        "pnl": round(float(p.sum())),
        "avg": round(float(p.mean()), 1),
        "max_dd": round(float(dd.min())),
        "max_loss": round(float(p.min())),
        "p05": round(float(p.quantile(0.05))),
    }


def classify_state(ret1: float, ret5: float) -> str:
    if pd.notna(ret1) and ret1 >= RET1_STRONG:
        return "strong"
    if pd.notna(ret5) and ret5 >= RET5_STRONG:
        return "strong"
    if pd.notna(ret1) and ret1 <= RET1_WEAK:
        return "weak"
    if pd.notna(ret5) and ret5 <= RET5_WEAK:
        return "weak"
    return "neutral"


def load_sector_returns() -> pd.DataFrame:
    sectors = pd.read_parquet(PARQUET_DIR / "sectors_prices_max_1d.parquet")
    sectors["date"] = pd.to_datetime(sectors["date"])
    sectors = sectors.sort_values(["name", "date"])
    sectors["sector_ret1"] = sectors.groupby("name")["close"].pct_change(fill_method=None)
    sectors["sector_ret5"] = sectors.groupby("name")["close"].pct_change(5, fill_method=None)
    return sectors[["date", "name", "sector_ret1", "sector_ret5"]].rename(columns={"name": "sector"})


def build_raw_signals() -> pd.DataFrame:
    prices = pd.read_parquet(PARQUET_DIR / "granville" / "prices_topix.parquet")
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.dropna(subset=["Open", "Close"]).sort_values(["ticker", "date"])

    pairs = active_pairs()
    tickers = sorted({t for tk1, tk2, *_ in pairs for t in (tk1, tk2)})
    px = prices[prices["ticker"].isin(tickers)].copy()
    px["ret1"] = px.groupby("ticker")["Close"].pct_change(fill_method=None)
    px["ret5"] = px.groupby("ticker")["Close"].pct_change(5, fill_method=None)
    px["ret20"] = px.groupby("ticker")["Close"].pct_change(20, fill_method=None)
    px["high20"] = px.groupby("ticker")["Close"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).max())
    px["low20"] = px.groupby("ticker")["Close"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).min())
    px["is_high20"] = px["Close"] >= px["high20"]
    px["is_low20"] = px["Close"] <= px["low20"]

    close = px.pivot_table(index="date", columns="ticker", values="Close")
    open_ = px.pivot_table(index="date", columns="ticker", values="Open")
    ret1 = px.pivot_table(index="date", columns="ticker", values="ret1")
    ret5 = px.pivot_table(index="date", columns="ticker", values="ret5")
    ret20 = px.pivot_table(index="date", columns="ticker", values="ret20")
    high20 = px.pivot_table(index="date", columns="ticker", values="is_high20")
    low20 = px.pivot_table(index="date", columns="ticker", values="is_low20")

    all_dates = close.index
    start_idx = all_dates.searchsorted(pd.Timestamp("2020-01-01"))

    rows = []
    for tk1, tk2, lb, pf, full_n, r1d in pairs:
        if tk1 not in close.columns or tk2 not in close.columns:
            continue
        c1s, c2s = close[tk1], close[tk2]
        o1s, o2s = open_[tk1], open_[tk2]
        spread = np.log(c1s / c2s)
        valid = c1s.notna() & c2s.notna() & (c1s > 0) & (c2s > 0)
        for i in range(max(start_idx, lb), len(all_dates) - 1):
            if not valid.iloc[i]:
                continue
            window = spread.iloc[i - lb : i].dropna()
            if len(window) < lb:
                continue
            sigma = window.std()
            if sigma == 0 or pd.isna(sigma):
                continue
            z = float((spread.iloc[i] - window.mean()) / sigma)
            if abs(z) < Z_ENTRY:
                continue
            vals = [o1s.iloc[i + 1], c1s.iloc[i + 1], o2s.iloc[i + 1], c2s.iloc[i + 1]]
            if any(pd.isna(x) or x <= 0 for x in vals):
                continue

            signal_date = all_dates[i]
            trade_date = all_dates[i + 1]
            shares1, shares2 = calc_shares_min_lot(float(vals[0]), float(vals[2]))
            if z > 0:
                long_tk, short_tk = tk2, tk1
                long_open, long_close, short_open, short_close = float(vals[2]), float(vals[3]), float(vals[0]), float(vals[1])
                long_shares, short_shares = shares2, shares1
                pnl = (vals[0] - vals[1]) * shares1 + (vals[3] - vals[2]) * shares2
            else:
                long_tk, short_tk = tk1, tk2
                long_open, long_close, short_open, short_close = float(vals[0]), float(vals[1]), float(vals[2]), float(vals[3])
                long_shares, short_shares = shares1, shares2
                pnl = (vals[1] - vals[0]) * shares1 + (vals[2] - vals[3]) * shares2

            rows.append(
                {
                    "signal_date": signal_date,
                    "trade_date": trade_date,
                    "tk1": tk1,
                    "tk2": tk2,
                    "long_tk": long_tk,
                    "short_tk": short_tk,
                    "z": z,
                    "abs_z": abs(z),
                    "pf": pf,
                    "full_n": full_n,
                    "long_open": long_open,
                    "long_close": long_close,
                    "short_open": short_open,
                    "short_close": short_close,
                    "long_shares": long_shares,
                    "short_shares": short_shares,
                    "pnl": float(pnl),
                    "long_ret1": ret1.at[signal_date, long_tk] if long_tk in ret1.columns else np.nan,
                    "long_ret5": ret5.at[signal_date, long_tk] if long_tk in ret5.columns else np.nan,
                    "long_ret20": ret20.at[signal_date, long_tk] if long_tk in ret20.columns else np.nan,
                    "short_ret1": ret1.at[signal_date, short_tk] if short_tk in ret1.columns else np.nan,
                    "short_ret5": ret5.at[signal_date, short_tk] if short_tk in ret5.columns else np.nan,
                    "short_ret20": ret20.at[signal_date, short_tk] if short_tk in ret20.columns else np.nan,
                    "long_high20": bool(high20.at[signal_date, long_tk]) if long_tk in high20.columns and pd.notna(high20.at[signal_date, long_tk]) else False,
                    "long_low20": bool(low20.at[signal_date, long_tk]) if long_tk in low20.columns and pd.notna(low20.at[signal_date, long_tk]) else False,
                    "short_high20": bool(high20.at[signal_date, short_tk]) if short_tk in high20.columns and pd.notna(high20.at[signal_date, short_tk]) else False,
                    "short_low20": bool(low20.at[signal_date, short_tk]) if short_tk in low20.columns and pd.notna(low20.at[signal_date, short_tk]) else False,
                }
            )
    return pd.DataFrame(rows)


def add_sector_features(df: pd.DataFrame) -> pd.DataFrame:
    meta = pd.read_parquet(PARQUET_DIR / "meta_jquants.parquet")
    sector_map = dict(zip(meta["ticker"].astype(str), meta["sectors"].astype(str)))
    sectors = load_sector_returns()

    df = df.copy()
    df["long_sector"] = df["long_tk"].map(sector_map)
    df["short_sector"] = df["short_tk"].map(sector_map)

    long_sec = sectors.rename(columns={"sector": "long_sector", "sector_ret1": "long_sector_ret1", "sector_ret5": "long_sector_ret5"})
    short_sec = sectors.rename(columns={"sector": "short_sector", "sector_ret1": "short_sector_ret1", "sector_ret5": "short_sector_ret5"})
    df = df.merge(long_sec, left_on=["signal_date", "long_sector"], right_on=["date", "long_sector"], how="left").drop(columns=["date"])
    df = df.merge(short_sec, left_on=["signal_date", "short_sector"], right_on=["date", "short_sector"], how="left").drop(columns=["date"])
    return df


def select_topn(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    selected = []
    for _, day in df.groupby("signal_date"):
        selected.extend(day.sort_values("abs_z", ascending=False).head(top_n).to_dict("records"))
    return pd.DataFrame(selected)


def summarize_groups(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for keys, g in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row.update(stats(g["pnl"]))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["n", "pnl"], ascending=[False, False])


def evaluate_filters(df: pd.DataFrame) -> pd.DataFrame:
    candidates = {
        "base": pd.Series(True, index=df.index),
        "block_both_adverse_1d": ~((df["long_ret1"] <= RET1_WEAK) & (df["short_ret1"] >= RET1_STRONG)),
        "block_both_adverse_5d": ~((df["long_ret5"] <= RET5_WEAK) & (df["short_ret5"] >= RET5_STRONG)),
        "block_long_weak_short_strong_state": ~((df["long_state"] == "weak") & (df["short_state"] == "strong")),
        "block_long_sector_weak_short_sector_strong": ~((df["long_sector_state"] == "weak") & (df["short_sector_state"] == "strong")),
        "block_short_strong_only": ~(df["short_state"] == "strong"),
        "block_long_weak_only": ~(df["long_state"] == "weak"),
        "block_any_leg_adverse": ~((df["long_state"] == "weak") | (df["short_state"] == "strong")),
    }
    rows = []
    base_n = len(df)
    for name, mask in candidates.items():
        kept = df[mask]
        row = {"filter": name, "kept": len(kept), "blocked": base_n - len(kept)}
        row.update(stats(kept["pnl"]))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["pf", "pnl"], ascending=[False, False])


def md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_empty_"
    cols = list(df.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in df.iterrows():
        vals = []
        for col in cols:
            val = row[col]
            if isinstance(val, float):
                vals.append(f"{val:.3f}".rstrip("0").rstrip("."))
            else:
                vals.append(str(val))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def write_md(path: Path, top1: pd.DataFrame, top3: pd.DataFrame, f1: pd.DataFrame, f3: pd.DataFrame) -> None:
    lines = [
        "# Pair MECE Leg Momentum Analysis",
        "",
        "分類は全トレードを long_leg_state × short_leg_state の3×3に必ず入れる。",
        "",
        f"- strong: ret1 >= {RET1_STRONG:.0%} または ret5 >= {RET5_STRONG:.0%}",
        f"- weak: ret1 <= {RET1_WEAK:.0%} または ret5 <= {RET5_WEAK:.0%}",
        "- neutral: 上記以外",
        "",
        "## TOP1 3x3",
        md_table(top1),
        "",
        "## TOP3 3x3",
        md_table(top3),
        "",
        "## TOP1 filter candidates",
        md_table(f1),
        "",
        "## TOP3 filter candidates",
        md_table(f3),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = add_sector_features(build_raw_signals())
    raw["long_state"] = raw.apply(lambda r: classify_state(r.long_ret1, r.long_ret5), axis=1)
    raw["short_state"] = raw.apply(lambda r: classify_state(r.short_ret1, r.short_ret5), axis=1)
    raw["long_sector_state"] = raw.apply(lambda r: classify_state(r.long_sector_ret1, r.long_sector_ret5), axis=1)
    raw["short_sector_state"] = raw.apply(lambda r: classify_state(r.short_sector_ret1, r.short_sector_ret5), axis=1)
    raw["leg_grid"] = raw["long_state"] + "/" + raw["short_state"]
    raw["sector_grid"] = raw["long_sector_state"] + "/" + raw["short_sector_state"]

    top1 = select_topn(raw, 1)
    top3 = select_topn(raw, 3)

    raw.to_parquet(OUT_DIR / "pair_mece_leg_momentum_raw_20260523.parquet", index=False)
    top1.to_csv(OUT_DIR / "pair_mece_leg_momentum_top1_20260523.csv", index=False)
    top3.to_csv(OUT_DIR / "pair_mece_leg_momentum_top3_20260523.csv", index=False)

    top1_grid = summarize_groups(top1, ["long_state", "short_state"])
    top3_grid = summarize_groups(top3, ["long_state", "short_state"])
    top1_sector_grid = summarize_groups(top1, ["long_sector_state", "short_sector_state"])
    top3_sector_grid = summarize_groups(top3, ["long_sector_state", "short_sector_state"])
    f1 = evaluate_filters(top1)
    f3 = evaluate_filters(top3)

    top1_grid.to_csv(OUT_DIR / "pair_mece_leg_momentum_grid_top1_20260523.csv", index=False)
    top3_grid.to_csv(OUT_DIR / "pair_mece_leg_momentum_grid_top3_20260523.csv", index=False)
    top1_sector_grid.to_csv(OUT_DIR / "pair_mece_sector_momentum_grid_top1_20260523.csv", index=False)
    top3_sector_grid.to_csv(OUT_DIR / "pair_mece_sector_momentum_grid_top3_20260523.csv", index=False)
    f1.to_csv(OUT_DIR / "pair_mece_leg_momentum_filters_top1_20260523.csv", index=False)
    f3.to_csv(OUT_DIR / "pair_mece_leg_momentum_filters_top3_20260523.csv", index=False)
    write_md(OUT_DIR / "pair_mece_leg_momentum_20260523.md", top1_grid, top3_grid, f1, f3)

    print("TOP1")
    print(top1_grid.to_string(index=False))
    print("\nTOP3")
    print(top3_grid.to_string(index=False))
    print("\nFILTER TOP1")
    print(f1.to_string(index=False))
    print("\nFILTER TOP3")
    print(f3.to_string(index=False))
    print("WROTE", OUT_DIR / "pair_mece_leg_momentum_20260523.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
