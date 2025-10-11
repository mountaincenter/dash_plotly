#!/usr/bin/env python3
"""
Quick smoke test for yfinance connectivity.
Downloads 7203.T for the past 10 days at 1d and 5m intervals
and prints the first few rows for inspection.
"""

from __future__ import annotations

import yfinance as yf


TICKERS = [
    "7203.T",  # Toyota
    "8035.T",  # Tokyo Electron
    "6501.T",  # Hitachi
    "9432.T",  # NTT
    "3382.T",  # Seven & i
]
PERIOD = "10d"
INTERVALS = ["1d", "5m"]


def main() -> int:
    for ticker in TICKERS:
        for interval in INTERVALS:
            print(f"\n=== {ticker} period={PERIOD} interval={interval} ===")
            try:
                df = yf.download(
                    ticker,
                    period=PERIOD,
                    interval=interval,
                    auto_adjust=True,
                    progress=False,
                )
            except Exception as exc:
                print(f"Failed to download {ticker} ({interval}): {exc}")
                continue

            if df.empty:
                print("No data returned.")
                continue
            print(df.head().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
