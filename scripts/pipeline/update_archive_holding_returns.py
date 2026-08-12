#!/usr/bin/env python3
"""Build a derived d1-d5 holding-return ledger from the protected archive.

``grok_trending_archive.parquet`` is an immutable/read-only input here. The
calculated holding returns are keyed by ``(backtest_date, ticker)`` and written
to a separate replaceable artifact.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
PRICES_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"
OUTPUT_PATH = PARQUET_DIR / "backtest" / "grok_holding_returns.parquet"
OUTPUT_S3_KEY = "backtest/grok_holding_returns.parquet"
HOLD_DAYS = [1, 2, 3, 5]


def build_holding_returns(
    archive: pd.DataFrame,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate SHORT PnL without changing archive fields or row order."""
    archive_required = {"backtest_date", "ticker", "buy_price"}
    price_required = {"date", "ticker", "Close"}
    archive_missing = sorted(archive_required - set(archive.columns))
    price_missing = sorted(price_required - set(prices.columns))
    if archive_missing or price_missing:
        raise ValueError(
            "holding-return inputs are missing columns: "
            f"archive={archive_missing}, prices={price_missing}"
        )
    if archive[["backtest_date", "ticker"]].duplicated().any():
        raise ValueError("protected archive has duplicate ticker-date keys")

    source = archive.loc[:, ["backtest_date", "ticker", "buy_price"]].copy()
    source["backtest_date"] = pd.to_datetime(
        source["backtest_date"], errors="raise"
    ).dt.normalize()
    source["ticker"] = source["ticker"].astype(str)
    source["buy_price"] = pd.to_numeric(source["buy_price"], errors="coerce")

    daily = prices.loc[:, ["date", "ticker", "Close"]].copy()
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    daily["ticker"] = daily["ticker"].astype(str)
    daily["Close"] = pd.to_numeric(daily["Close"], errors="coerce")
    daily = daily.dropna(subset=["date", "ticker", "Close"])
    daily = daily.sort_values(["ticker", "date"])
    daily = daily.drop_duplicates(["ticker", "date"], keep="last")

    result = source.copy()
    for hold_day in HOLD_DAYS:
        result[f"close_d{hold_day}"] = pd.NA
        result[f"short_profit_d{hold_day}"] = pd.NA

    by_ticker = {
        ticker: frame.reset_index(drop=True)
        for ticker, frame in daily.groupby("ticker", sort=False)
    }
    for index, row in result.iterrows():
        buy_price = row["buy_price"]
        if pd.isna(buy_price) or float(buy_price) <= 0:
            continue
        future = by_ticker.get(row["ticker"])
        if future is None:
            continue
        future = future[future["date"].gt(row["backtest_date"])]
        for hold_day in HOLD_DAYS:
            if len(future) < hold_day:
                continue
            close = float(future.iloc[hold_day - 1]["Close"])
            result.at[index, f"close_d{hold_day}"] = close
            result.at[index, f"short_profit_d{hold_day}"] = (
                float(buy_price) - close
            ) * 100.0

    result["backtest_date"] = result["backtest_date"].dt.strftime("%Y-%m-%d")
    result["source"] = "derived_from_protected_archive_and_grok_prices"
    return result


def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        frame.to_parquet(temporary_path, index=False)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> int:
    print("=== Build derived d1-d5 holding returns ===")
    if not ARCHIVE_PATH.exists() or not PRICES_PATH.exists():
        print(
            "[ERROR] required read-only inputs are absent: "
            f"archive={ARCHIVE_PATH.exists()}, prices={PRICES_PATH.exists()}"
        )
        return 1

    result = build_holding_returns(
        pd.read_parquet(ARCHIVE_PATH),
        pd.read_parquet(PRICES_PATH),
    )
    write_parquet_atomic(result, OUTPUT_PATH)
    print(f"[OK] Derived ledger saved: {OUTPUT_PATH} rows={len(result):,}")
    print(f"[OK] Protected archive remained read-only: {ARCHIVE_PATH}")

    cfg = load_s3_config()
    if cfg and not upload_file(cfg, OUTPUT_PATH, OUTPUT_S3_KEY):
        print(f"[ERROR] Failed to upload derived ledger: {OUTPUT_S3_KEY}")
        return 1
    if cfg:
        print(f"[OK] Derived ledger uploaded: {OUTPUT_S3_KEY}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
