#!/usr/bin/env python3
"""
fetch_prices_mid400.py
TOPIX Mid400（約400銘柄）の日足価格データを取得

出力:
  - data/parquet/screening/prices_max_1d_mid400.parquet (日足・全期間)
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers
from common_cfg.paths import PARQUET_DIR

UNIVERSE_PATH = PARQUET_DIR / "universe.parquet"
OUT_DIR = PARQUET_DIR / "screening"


def get_mid400_tickers() -> list[str]:
    """universe.parquetからMid400の銘柄を取得"""
    uni = pd.read_parquet(UNIVERSE_PATH)
    tickers = uni[uni["topix_class"] == "Mid400"]["ticker"].tolist()
    print(f"  Mid400: {len(tickers)} tickers")
    return tickers


def main() -> int:
    print("=" * 60)
    print("Fetch Prices: TOPIX Mid400")
    print("=" * 60)

    print("\n[1/2] Loading universe...")
    tickers = get_mid400_tickers()
    if not tickers:
        print("  ⚠ No Mid400 tickers found")
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n[2/2] Fetching daily prices (full history) for {len(tickers)} tickers...")
    out_path = OUT_DIR / "prices_max_1d_mid400.parquet"
    df = fetch_prices_for_tickers(tickers, "max", "1d")

    if df.empty:
        print("  ⚠ No data retrieved")
        return 1

    n_tickers = df["ticker"].nunique()
    print(f"  Retrieved: {len(df):,} rows, {n_tickers} tickers")

    df.to_parquet(out_path, engine="pyarrow", index=False)
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  Saved: {out_path} ({size_mb:.1f} MB)")

    if "date" in df.columns:
        print(f"  Period: {df['date'].min()} ~ {df['date'].max()}")

    # core_largeと最終日比較
    cl_path = OUT_DIR / "prices_max_1d_core_large.parquet"
    if cl_path.exists():
        cl = pd.read_parquet(cl_path)
        cl_max = cl["date"].max()
        mid_max = df["date"].max()
        print(f"\n  core_large最終日: {cl_max}")
        print(f"  mid400最終日:     {mid_max}")
        if cl_max != mid_max:
            print(f"  ⚠ 日付不一致")

    print("\n" + "=" * 60)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
