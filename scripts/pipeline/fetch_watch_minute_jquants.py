#!/usr/bin/env python3
"""
Wrapper around scripts/data/fetch_jquants_minute_universe.py for the focused app universe.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

UNIVERSE_PATH = PARQUET_DIR / "watch_minute_universe.parquet"
TOP100_PATH = PARQUET_DIR / "trading_value_top100.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
OUTPUT = PARQUET_DIR / "jquants_minute_watch.parquet"
FEATURES_OUTPUT = PARQUET_DIR / "jquants_minute_watch_features.parquet"
RAW_DIR = ROOT / "data" / "jquants_csv" / "watch_minute"
FETCH_SCRIPT = ROOT / "scripts" / "data" / "fetch_jquants_minute_universe.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch focused watch-list J-Quants minute data.")
    parser.add_argument("--date", help="Target date YYYY-MM-DD. Default: top100 date, then prices_max_1d latest.")
    parser.add_argument("--from", dest="date_from")
    parser.add_argument("--to", dest="date_to")
    parser.add_argument("--max-targets", type=int, default=0)
    parser.add_argument("--tickers", nargs="*")
    parser.add_argument("--groups", nargs="*")
    parser.add_argument("--requests-per-minute", type=float, default=20.0)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--checkpoint-every", type=int, default=25)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def default_date() -> str:
    if TOP100_PATH.exists():
        df = pd.read_parquet(TOP100_PATH, columns=["date"])
        if not df.empty:
            return pd.to_datetime(df["date"], errors="coerce").dropna().max().strftime("%Y-%m-%d")
    if PRICES_1D_PATH.exists():
        df = pd.read_parquet(PRICES_1D_PATH, columns=["date"])
        if not df.empty:
            return pd.to_datetime(df["date"], errors="coerce").dropna().max().strftime("%Y-%m-%d")
    raise FileNotFoundError("cannot resolve default date from top100 or prices_max_1d")


def main() -> int:
    args = parse_args()
    date = args.date or (None if args.date_from or args.date_to else default_date())
    cmd = [
        sys.executable,
        str(FETCH_SCRIPT),
        "--universe-path",
        str(UNIVERSE_PATH),
        "--output",
        str(OUTPUT),
        "--features-output",
        str(FEATURES_OUTPUT),
        "--raw-dir",
        str(RAW_DIR),
        "--requests-per-minute",
        str(args.requests_per_minute),
        "--sleep",
        str(args.sleep),
        "--checkpoint-every",
        str(args.checkpoint_every),
    ]
    if date:
        cmd.extend(["--date", date])
    if args.date_from:
        cmd.extend(["--from", args.date_from])
    if args.date_to:
        cmd.extend(["--to", args.date_to])
    if args.max_targets:
        cmd.extend(["--max-targets", str(args.max_targets)])
    if args.tickers:
        cmd.extend(["--tickers", *args.tickers])
    if args.groups:
        cmd.extend(["--groups", *args.groups])
    if args.refresh:
        cmd.append("--refresh")
    if args.dry_run:
        cmd.append("--dry-run")

    print("=== Fetch watch minute data ===")
    print(" ".join(cmd))
    result = subprocess.run(cmd, cwd=ROOT, check=False)
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
