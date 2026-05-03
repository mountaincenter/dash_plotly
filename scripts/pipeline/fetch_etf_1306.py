#!/usr/bin/env python3
"""
fetch_etf_1306.py
1306.T (TOPIX ETF) の調整済み終値を jquants から取得して parquet 保存

日次パイプラインで実行。yfinance は分割調整が壊れているため jquants の AdjC を使用。

実行方法:
    python3 scripts/pipeline/fetch_etf_1306.py
"""
from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

OUTPUT_PATH = PARQUET_DIR / "etf_1306_prices.parquet"
CODE = "13060"
START_DATE = "2016-05-03"


def fetch_1306_prices() -> pd.DataFrame:
    result = subprocess.run(
        [
            "jquants", "--output", "csv",
            "-f", "Date,AdjO,AdjC",
            "eq", "daily",
            "--code", CODE,
            "--from", START_DATE,
        ],
        capture_output=True, text=True, check=True,
    )
    df = pd.read_csv(io.StringIO(result.stdout))
    df = df.rename(columns={"Date": "date", "AdjO": "Open", "AdjC": "Close"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date").reset_index(drop=True)
    return df


def main() -> int:
    print("=" * 60)
    print("Fetch 1306.T ETF Prices (J-Quants)")
    print("=" * 60)

    print("\n[1] Fetching from J-Quants...")
    df = fetch_1306_prices()
    print(f"  Rows: {len(df)}")
    print(f"  Range: {df['date'].iloc[0]} → {df['date'].iloc[-1]}")
    print(f"  Latest Close: {df['Close'].iloc[-1]:.1f}")

    print("\n[2] Saving parquet...")
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"  {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size:,} bytes)")

    print("\n[OK] Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
