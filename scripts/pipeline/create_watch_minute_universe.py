#!/usr/bin/env python3
"""
Create focused J-Quants minute universe from all_stocks.parquet.

The output is intentionally small: grok + trading value top100 + static semicon.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

ALL_STOCKS_PATH = PARQUET_DIR / "all_stocks.parquet"
PARQUET_OUT = PARQUET_DIR / "watch_minute_universe.parquet"
CSV_OUT = ROOT / "data" / "jquants_csv" / "master" / "watch_minute_universe.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create J-Quants minute universe from all_stocks.parquet.")
    parser.add_argument("--all-stocks", type=Path, default=ALL_STOCKS_PATH)
    parser.add_argument("--parquet-out", type=Path, default=PARQUET_OUT)
    parser.add_argument("--csv-out", type=Path, default=CSV_OUT)
    parser.add_argument("--no-csv", action="store_true")
    return parser.parse_args()


def as_list(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, np.ndarray):
        return [str(v) for v in value if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if isinstance(value, str):
        return [value]
    return []


def infer_instrument_type(row: pd.Series) -> str:
    parts: list[str] = []
    for col in ["stock_name", "market", "sectors"]:
        value = row.get(col)
        if value is not None and not (isinstance(value, float) and pd.isna(value)):
            parts.append(str(value))
    parts.extend(as_list(row.get("tags")))
    text = " ".join(parts)
    if "ETF" in text or "上場投信" in text:
        return "etf"
    return "stock"


def build_universe(all_stocks_path: Path) -> pd.DataFrame:
    if not all_stocks_path.exists():
        raise FileNotFoundError(f"all_stocks.parquet not found: {all_stocks_path}")
    src = pd.read_parquet(all_stocks_path)
    if src.empty:
        raise RuntimeError(f"all_stocks.parquet is empty: {all_stocks_path}")

    rows: list[dict[str, object]] = []
    for _, row in src.iterrows():
        ticker = str(row.get("ticker") or "").strip()
        code = str(row.get("code") or ticker.replace(".T", "")).strip().removesuffix(".0")
        if not ticker.endswith(".T") or not code:
            continue
        cats = as_list(row.get("categories"))
        tags = as_list(row.get("tags"))
        groups = []
        for item in cats + tags:
            clean = item.strip()
            if clean and clean not in groups:
                groups.append(clean)
        universe_group = "semicon" if "SEMICON" in cats else "top100" if "TOP100" in cats else "grok" if "GROK" in cats else "watch"
        priority = 10 if universe_group == "semicon" else 20 if universe_group == "top100" else 30
        rows.append(
            {
                "universe": "grok_top100_semicon",
                "instrument_type": infer_instrument_type(row),
                "source": "|".join(cats) if cats else "all_stocks",
                "ticker": ticker,
                "code": code,
                "jquants_query_code": code,
                "stock_name": row.get("stock_name"),
                "market": row.get("market"),
                "sectors": row.get("sectors"),
                "series": row.get("series"),
                "topixnewindexseries": row.get("topixnewindexseries"),
                "topix_class": row.get("topixnewindexseries"),
                "universe_group": universe_group,
                "groups": "|".join(groups),
                "priority": priority,
                "display_order": row.get("rank") if pd.notna(row.get("rank", np.nan)) else None,
                "execution_rank": None,
                "fetch_minute": True,
                "fetch_tick": False,
                "active": True,
                "notes": "",
            }
        )
    df = pd.DataFrame(rows).drop_duplicates("ticker", keep="first")
    if df.empty:
        raise RuntimeError("no domestic tickers found in all_stocks")
    return df.sort_values(["priority", "display_order", "code"], na_position="last").reset_index(drop=True)


def main() -> int:
    args = parse_args()
    print("=== Create watch minute universe ===")
    universe = build_universe(args.all_stocks)
    args.parquet_out.parent.mkdir(parents=True, exist_ok=True)
    universe.to_parquet(args.parquet_out, engine="pyarrow", index=False)
    print(f"[OK] saved parquet: {args.parquet_out} rows={len(universe)}")
    if not args.no_csv:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        universe.to_csv(args.csv_out, index=False)
        print(f"[OK] saved csv: {args.csv_out}")
    print(universe[["ticker", "stock_name", "universe_group", "groups"]].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
