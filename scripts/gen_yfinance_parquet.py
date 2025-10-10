#!/usr/bin/env python3
"""
Generate yfinance-smoke-test.parquet for multiple tickers across
period/interval combinations that mirror the main pipeline granularity.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import yfinance as yf

OUT_PATH = Path("yfinance-smoke-test.parquet")
TICKERS = [
    "7203.T",  # Toyota
    "8035.T",  # Tokyo Electron
    "6501.T",  # Hitachi
    "9432.T",  # NTT
    "3382.T",  # Seven & i
    "2914.T",  # Japan Tobacco
    "4502.T",  # Takeda Pharmaceutical
    "9984.T",  # SoftBank Group
    "6758.T",  # Sony Group
    "9983.T",  # Fast Retailing
]
SPECS: List[Tuple[str, str]] = [
    ("max", "1d"),
    ("730d", "1h"),
    ("60d", "15m"),
    ("60d", "5m"),
]
SLEEP_BETWEEN_CALLS = 1.0


def _prepare_frame(df: pd.DataFrame, *, ticker: str, period: str, interval: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["date", "Open", "High", "Low", "Close", "Volume", "interval", "period", "ticker"]
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

    df = df.assign(
        interval=interval,
        period=period,
        ticker=ticker,
    )
    return df


def fetch_specs(ticker: str, specs: Iterable[Tuple[str, str]]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for period, interval in specs:
        print(f"[INFO] downloading {ticker} period={period} interval={interval}")
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
        prepared = _prepare_frame(df, ticker=ticker, period=period, interval=interval)
        frames.append(prepared)
        time.sleep(SLEEP_BETWEEN_CALLS)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined[
        ["date", "Open", "High", "Low", "Close", "Volume", "interval", "period", "ticker"]
    ]
    combined = combined.sort_values(["ticker", "interval", "date"]).reset_index(drop=True)
    return combined


def main() -> int:
    frames: List[pd.DataFrame] = []
    for ticker in TICKERS:
        frames.append(fetch_specs(ticker, SPECS))
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        raise RuntimeError("No data retrieved from yfinance; check network or API status.")
    df.to_parquet(OUT_PATH, index=False)
    print(f"[OK] saved parquet: {OUT_PATH} rows={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
