#!/usr/bin/env python3
"""
Generate yfinance-smoke-test.parquet using period=5d and intervals=1d/5m.
Intended for temporary smoke checks in CI.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
import yfinance as yf

OUT_PATH = Path("yfinance-smoke-test.parquet")
TICKER = "7203.T"
PERIOD = "5d"
INTERVALS = ["1d", "5m"]


def _prepare_frame(df: pd.DataFrame, *, interval: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "interval", "ticker"])

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

    df = df.assign(
        interval=interval,
        ticker=TICKER,
    )
    return df


def fetch_all(ticker: str, *, period: str, intervals: List[str]) -> pd.DataFrame:
    frames = []
    for interval in intervals:
        print(f"[INFO] downloading {ticker} period={period} interval={interval}")
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
        prepared = _prepare_frame(df, interval=interval)
        frames.append(prepared)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined[["date", "Open", "High", "Low", "Close", "Volume", "interval", "ticker"]]
    combined = combined.sort_values(["interval", "date"]).reset_index(drop=True)
    return combined


def main() -> int:
    df = fetch_all(TICKER, period=PERIOD, intervals=INTERVALS)
    if df.empty:
        raise RuntimeError("No data retrieved from yfinance; check network or API status.")
    df.to_parquet(OUT_PATH, index=False)
    print(f"[OK] saved parquet: {OUT_PATH} rows={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
