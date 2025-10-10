#!/usr/bin/env python3
"""
Generate yfinance-smoke-test.parquet using the same period/interval pairs
as the main parquet pipeline, for quick connectivity diagnostics.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
import yfinance as yf

OUT_PATH = Path("yfinance-smoke-test.parquet")
TICKER = "7203.T"
SPECS = [
    ("max", "1d"),
    ("max", "1wk"),
    ("max", "1mo"),
    ("730d", "1h"),
    ("60d", "5m"),
    ("60d", "15m"),
]


def _prepare_frame(df: pd.DataFrame, *, period: str, interval: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "Open", "High", "Low", "Close", "Volume", "period", "interval", "ticker"])

    df = df.reset_index()
    ts_col = None
    for candidate in ("Datetime", "Date", "index"):
        if candidate in df.columns:
            ts_col = candidate
            break
    if not ts_col:
        raise KeyError("Unexpected yfinance frame without Datetime/Date column")
    df = df.rename(columns={ts_col: "timestamp"})

    if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        try:
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
        except Exception:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df = df.assign(
        period=period,
        interval=interval,
        ticker=TICKER,
    )
    return df


def fetch_all(ticker: str, specs: List[tuple[str, str]]) -> pd.DataFrame:
    frames = []
    for period, interval in specs:
        print(f"[INFO] downloading {ticker} period={period} interval={interval}")
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
        prepared = _prepare_frame(df, period=period, interval=interval)
        prepared["source_rows"] = len(df)
        frames.append(prepared)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined[["timestamp", "Open", "High", "Low", "Close", "Volume", "period", "interval", "ticker", "source_rows"]]
    return combined


def main() -> int:
    df = fetch_all(TICKER, SPECS)
    df.to_parquet(OUT_PATH, index=False)
    print(f"[OK] saved parquet: {OUT_PATH} rows={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
