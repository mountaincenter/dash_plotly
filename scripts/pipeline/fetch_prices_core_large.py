#!/usr/bin/env python3
"""
fetch_prices_core_large.py
Core30 + Large70（100銘柄）の価格データを取得

出力:
  - data/parquet/screening/prices_max_1d_core_large.parquet   (日足・全期間)
  - data/parquet/screening/prices_60d_5m_core_large.parquet   (5分足・直近60日)
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


def get_core_large_tickers() -> list[str]:
    """universe.parquetからCore30+Large70の銘柄を取得"""
    uni = pd.read_parquet(UNIVERSE_PATH)
    mask = uni["topix_class"].isin(["Core30", "Large70"])
    tickers = uni[mask]["ticker"].tolist()
    print(f"  Core30: {(uni['topix_class'] == 'Core30').sum()}")
    print(f"  Large70: {(uni['topix_class'] == 'Large70').sum()}")
    print(f"  Total: {len(tickers)} tickers")
    return tickers


def fetch_and_save(
    tickers: list[str], period: str, interval: str, filename: str
) -> None:
    """取得して保存"""
    out_path = OUT_DIR / filename
    print(f"\n  Fetching: period={period}, interval={interval}")

    df = fetch_prices_for_tickers(tickers, period, interval)

    if df.empty:
        print("  ⚠ No data retrieved")
        return

    # 銘柄数の確認
    n_tickers = df["ticker"].nunique() if "ticker" in df.columns else 0
    print(f"  Retrieved: {len(df):,} rows, {n_tickers} tickers")

    df.to_parquet(out_path, engine="pyarrow", index=False)
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"  Saved: {out_path} ({size_mb:.1f} MB)")


def main() -> int:
    print("=" * 60)
    print("Fetch Prices: Core30 + Large70")
    print("=" * 60)

    print("\n[1/3] Loading universe...")
    tickers = get_core_large_tickers()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[2/3] Fetching daily prices (full history)...")
    fetch_and_save(tickers, "max", "1d", "prices_max_1d_core_large.parquet")

    print("\n[3/3] Fetching 5-min prices (60 days)...")
    fetch_and_save(tickers, "60d", "5m", "prices_60d_5m_core_large.parquet")

    # サマリー
    print("\n" + "=" * 60)
    print("Done.")
    for f in ["prices_max_1d_core_large.parquet", "prices_60d_5m_core_large.parquet"]:
        p = OUT_DIR / f
        if p.exists():
            df = pd.read_parquet(p)
            n = df["ticker"].nunique() if "ticker" in df.columns else 0
            dates = ""
            if "date" in df.columns:
                dates = f", {df['date'].min()} ~ {df['date'].max()}"
            print(f"  {f}: {len(df):,} rows, {n} tickers{dates}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
