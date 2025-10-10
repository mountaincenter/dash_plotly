#!/usr/bin/env python3
"""
Quick smoke test for yfinance connectivity.
Downloads 7203.T for the past 10 days at 1d and 5m intervals
and prints the first few rows for inspection.
"""

from __future__ import annotations

import yfinance as yf


def main() -> int:
    ticker = "7203.T"
    period = "10d"
    intervals = ["1d", "5m"]

    for interval in intervals:
        print(f"\n=== {ticker} period={period} interval={interval} ===")
        df = yf.download(
            ticker,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            print("No data returned.")
            continue
        print(df.head().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
