#!/usr/bin/env python3
"""
J-Quants CLIでTOPIX500構成銘柄の出来高(AdjVo)を取得
prices_topix500_oc.parquetの日付・銘柄範囲に合わせる
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
OUTPUT_PATH = PARQUET_DIR / "volume_topix500.parquet"

BATCH_MONTHS = 6


def fetch_daily(code_5digit: str, from_date: str, to_date: str) -> list[dict]:
    """jquants CLI で日足取得"""
    code_4 = code_5digit[:-1]
    cmd = [
        "jquants", "--output", "json",
        "eq", "daily",
        "--code", code_4,
        "--from", from_date,
        "--to", to_date,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def main():
    prices = pd.read_parquet(PRICES_PATH)
    prices["Date"] = pd.to_datetime(prices["Date"])
    codes = sorted(prices["Code"].unique())
    date_min = prices["Date"].min().strftime("%Y-%m-%d")
    date_max = prices["Date"].max().strftime("%Y-%m-%d")

    print(f"Fetching volume for {len(codes)} codes, {date_min} to {date_max}")

    # J-Quants Standard plan: bulk download available
    # Try bulk first, fallback to per-code
    print("Trying bulk download...")
    cmd = ["jquants", "--output", "json", "bulk", "--help"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(f"  bulk help: {result.stdout[:200]}")

    # Per-code approach (with date batching)
    all_rows = []
    total = len(codes)

    for i, code in enumerate(codes):
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{total}] {code}...")

        records = fetch_daily(code, date_min, date_max)
        for rec in records:
            if rec.get("AdjVo") is not None:
                all_rows.append({
                    "Date": rec["Date"],
                    "Code": code,
                    "volume": rec["AdjVo"],
                })

    if not all_rows:
        print("No volume data fetched!")
        return

    df = pd.DataFrame(all_rows)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\n[OK] Saved {len(df)} rows to {OUTPUT_PATH}")
    print(f"  Codes: {df['Code'].nunique()}, Dates: {df['Date'].nunique()}")


if __name__ == "__main__":
    main()
