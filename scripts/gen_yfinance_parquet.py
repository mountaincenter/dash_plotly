#!/usr/bin/env python3
"""
Generate yfinance-smoke parquet files for multiple tickers across
period/interval combinations that mirror the main pipeline granularity.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import yfinance as yf

OUTPUT_TEMPLATE = "yfinance-smoke-test-{period}-{interval}.parquet"
TICKERS = [
    "2914.T",
    "3382.T",
    "4063.T",
    "4502.T",
    "4568.T",
    "6098.T",
    "6367.T",
    "6501.T",
    "6503.T",
    "6758.T",
    "6861.T",
    "6981.T",
    "7011.T",
    "7203.T",
    "7267.T",
    "7741.T",
    "7974.T",
    "8001.T",
    "8031.T",
    "8035.T",
    "8058.T",
    "8306.T",
    "8316.T",
    "8411.T",
    "8766.T",
    "9432.T",
    "9433.T",
    "9434.T",
    "9983.T",
    "9984.T",
    "1605.T",
    "186A.T",
    "2168.T",
    "2749.T",
    "3692.T",
    "4204.T",
    "5020.T",
    "5595.T",
    "5631.T",
    "6232.T",
    "6330.T",
    "6701.T",
    "6762.T",
    "6920.T",
    "6946.T",
    "6965.T",
    "7013.T",
    "7711.T",
    "8060.T",
    "9142.T",
    "9501.T",
]
SPECS: List[Tuple[str, str]] = [
    ("max", "1d"),
    ("730d", "1h"),
    ("60d", "15m"),
    ("60d", "5m"),
]
SLEEP_BETWEEN_CALLS = 1.0


def _prepare_frame(df: pd.DataFrame, *, ticker: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
        )

    df = df.reset_index()
    ts_col = None
    for candidate in ("Datetime", "Date", "index"):
        if candidate in df.columns:
            ts_col = candidate
            break
    if not ts_col:
        raise KeyError("Unexpected yfinance frame without Datetime/Date column")
    df = df.rename(columns={ts_col: "date"})

    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        try:
            if df["date"].dt.tz is not None:
                df["date"] = df["date"].dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
        except Exception:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.assign(ticker=ticker)
    return df


def fetch_for_spec(period: str, interval: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for ticker in TICKERS:
        print(f"[INFO] downloading {ticker} period={period} interval={interval}")
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
        prepared = _prepare_frame(df, ticker=ticker)
        frames.append(prepared)
        time.sleep(SLEEP_BETWEEN_CALLS)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined[
        ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
    ]
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    return combined


def main() -> int:
    generated = []
    for period, interval in SPECS:
        df = fetch_for_spec(period, interval)
        if df.empty:
            raise RuntimeError(f"No data retrieved for period={period} interval={interval}")
        out_path = Path(OUTPUT_TEMPLATE.format(period=period, interval=interval))
        df.to_parquet(out_path, index=False)
        print(f"[OK] saved parquet: {out_path} rows={len(df)}")
        generated.append(out_path)

    summary = ", ".join(str(p) for p in generated)
    print(f"[INFO] generated files: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
