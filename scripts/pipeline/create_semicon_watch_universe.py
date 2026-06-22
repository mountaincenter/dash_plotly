#!/usr/bin/env python3
"""
Create static semiconductor / AI / data-center watch universe.

This deliberately excludes TOPIX500 broad coverage. The output is a focused
watch list for the stock app and J-Quants minute fetch.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from scripts.pipeline.create_jquants_universe_master import (
    build_etf_rows,
    build_semiconductor_rows,
    merge_duplicate_tickers,
)

META_PATH = PARQUET_DIR / "meta_jquants.parquet"
PARQUET_OUT = PARQUET_DIR / "semicon_watch_universe.parquet"
CSV_OUT = ROOT / "data" / "jquants_csv" / "master" / "semicon_watch_universe.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create static semiconductor watch universe.")
    parser.add_argument("--meta-path", type=Path, default=META_PATH)
    parser.add_argument("--parquet-out", type=Path, default=PARQUET_OUT)
    parser.add_argument("--csv-out", type=Path, default=CSV_OUT)
    parser.add_argument("--no-csv", action="store_true")
    return parser.parse_args()


def build_universe(meta_path: Path) -> pd.DataFrame:
    etf = build_etf_rows()
    semicon = build_semiconductor_rows(meta_path)
    df = pd.concat([etf, semicon], ignore_index=True)
    df = merge_duplicate_tickers(df)
    df["universe"] = "semicon_watch"
    df["categories"] = [["SEMICON"] for _ in range(len(df))]
    df["tags"] = df["groups"].fillna("").map(lambda x: [p for p in str(x).split("|") if p])
    df["fetch_minute"] = True
    df["active"] = True
    columns = [
        "universe",
        "instrument_type",
        "source",
        "ticker",
        "code",
        "jquants_query_code",
        "stock_name",
        "market",
        "sectors",
        "series",
        "topixnewindexseries",
        "topix_class",
        "universe_group",
        "groups",
        "priority",
        "display_order",
        "execution_rank",
        "fetch_minute",
        "fetch_tick",
        "active",
        "role_live_training",
        "role_live_candidate",
        "role_direction",
        "role_market",
        "notes",
        "categories",
        "tags",
    ]
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns].sort_values(["display_order", "priority", "code"], na_position="last").reset_index(drop=True)


def main() -> int:
    args = parse_args()
    print("=== Create semicon watch universe ===")
    universe = build_universe(args.meta_path)
    args.parquet_out.parent.mkdir(parents=True, exist_ok=True)
    universe.to_parquet(args.parquet_out, engine="pyarrow", index=False)
    print(f"[OK] saved parquet: {args.parquet_out} rows={len(universe)}")
    if not args.no_csv:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        universe.to_csv(args.csv_out, index=False)
        print(f"[OK] saved csv: {args.csv_out}")
    print(universe[["ticker", "stock_name", "groups", "notes"]].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
