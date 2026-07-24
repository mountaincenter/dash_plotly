#!/usr/bin/env python3
"""Fetch the current semicon universe before the broader watch-minute stage."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR


UNIVERSE_PATH = PARQUET_DIR / "semicon_watch_universe.parquet"
TOP100_PATH = PARQUET_DIR / "trading_value_top100.parquet"
OUTPUT = PARQUET_DIR / "jquants_minute_watch.parquet"
FEATURES_OUTPUT = PARQUET_DIR / "jquants_minute_watch_features.parquet"
RAW_DIR = ROOT / "data" / "jquants_csv" / "watch_minute"
FETCH_SCRIPT = ROOT / "scripts" / "data" / "fetch_jquants_minute_universe.py"


def target_date() -> str:
    if not TOP100_PATH.exists():
        raise FileNotFoundError(f"trading-value Top100 not found: {TOP100_PATH}")
    frame = pd.read_parquet(TOP100_PATH, columns=["date"])
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        raise RuntimeError(f"trading-value Top100 has no valid date: {TOP100_PATH}")
    return dates.max().strftime("%Y-%m-%d")


def main() -> int:
    date = target_date()
    command = [
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
        "--date",
        date,
        "--requests-per-minute",
        "20",
        "--sleep",
        "0.25",
        "--checkpoint-every",
        "25",
    ]
    print("=== Fetch Market Flow minute data ===")
    print(f"date: {date}")
    print(f"universe: {UNIVERSE_PATH}")
    result = subprocess.run(command, cwd=ROOT, check=False)
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
