#!/usr/bin/env python3
"""
Generate yfinance-smoke parquet files for multiple tickers across
period/interval combinations that mirror the main pipeline granularity.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PRICE_SPECS

OUTPUT_TEMPLATE = "yfinance-smoke-test-{period}-{interval}.parquet"
DEFAULT_COLUMNS = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]

# Tickers extracted from meta.parquet (TOPIX Core30 + 高市銘柄ユニバース)
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
    "7013.T",
    "5631.T",
    "6946.T",
    "6701.T",
    "3692.T",
    "6232.T",
    "8060.T",
    "6920.T",
    "6965.T",
    "7711.T",
    "6762.T",
    "6330.T",
    "1605.T",
    "5020.T",
    "4204.T",
    "186A.T",
    "5595.T",
    "2168.T",
    "9142.T",
    "2749.T",
    "9501.T",
]


def _flatten_multi(raw: pd.DataFrame, tickers: Sequence[str], interval: str) -> pd.DataFrame:
    frames = []
    if isinstance(raw.columns, pd.MultiIndex):
        aligned = raw
        ticker_level = None
        for level in range(aligned.columns.nlevels):
            level_values = aligned.columns.get_level_values(level)
            if any(ticker in level_values for ticker in tickers):
                ticker_level = level
                break
        if ticker_level is not None and ticker_level != 0:
            aligned = aligned.swaplevel(0, ticker_level, axis=1)
        if isinstance(aligned.columns, pd.MultiIndex):
            aligned = aligned.sort_index(axis=1)
            lv0 = aligned.columns.get_level_values(0)
            for ticker in tickers:
                if ticker not in lv0:
                    continue
                sub = aligned[ticker].copy()
                if sub.empty:
                    continue
                if isinstance(sub.columns, pd.MultiIndex):
                    sub.columns = sub.columns.get_level_values(-1)
                sub = sub.reset_index()
                if "Datetime" in sub.columns:
                    sub = sub.rename(columns={"Datetime": "date"})
                elif "Date" in sub.columns:
                    sub = sub.rename(columns={"Date": "date"})
                elif "index" in sub.columns:
                    sub = sub.rename(columns={"index": "date"})
                else:
                    sub.columns = ["date"] + list(sub.columns[1:])
                sub["ticker"] = ticker
                keep = [c for c in DEFAULT_COLUMNS if c in sub.columns]
                frames.append(sub[keep].copy())
    else:
        sub = raw.reset_index()
        if "Datetime" in sub.columns:
            sub = sub.rename(columns={"Datetime": "date"})
        elif "Date" in sub.columns:
            sub = sub.rename(columns={"Date": "date"})
        elif "index" in sub.columns:
            sub = sub.rename(columns={"index": "date"})
        sub["ticker"] = tickers[0] if tickers else "UNKNOWN"
        keep = [c for c in DEFAULT_COLUMNS if c in sub.columns]
        frames.append(sub[keep].copy())

    if not frames:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    if interval in ("5m", "15m", "1h"):
        try:
            if out["date"].dt.tz is not None:
                out["date"] = out["date"].dt.tz_convert("Asia/Tokyo")
            else:
                out["date"] = out["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Tokyo")
            out["date"] = out["date"].dt.tz_localize(None)
        except Exception:
            try:
                out["date"] = out["date"].dt.tz_localize(None)
            except Exception:
                pass
    else:
        try:
            out["date"] = out["date"].dt.tz_localize(None)
        except Exception:
            pass

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out[out["date"].notna()].copy()
    need_ohlc = [c for c in ["Open", "High", "Low", "Close"] if c in out.columns]
    if need_ohlc:
        out = out.dropna(subset=need_ohlc, how="any")

    if "ticker" in out.columns:
        out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    else:
        out = out.sort_values(["date"]).reset_index(drop=True)

    return out


def _fetch_prices(tickers: Sequence[str], period: str, interval: str) -> pd.DataFrame:
    try:
        raw = yf.download(
            tickers,
            period=period,
            interval=interval,
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=True,
        )
        df = _flatten_multi(raw, tickers, interval)
        if df.empty:
            raise RuntimeError("yf.download returned empty. fallback to per-ticker.")
    except Exception:
        frames = []
        for ticker in tickers:
            try:
                raw_single = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    threads=True,
                    progress=False,
                    auto_adjust=True,
                )
                flattened = _flatten_multi(raw_single, [ticker], interval)
                if not flattened.empty:
                    frames.append(flattened)
            except Exception:
                continue
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    need = {"date", "Open", "High", "Low", "Close", "ticker"}
    if df.empty or not need.issubset(df.columns):
        raise RuntimeError(
            f"No price data collected or required columns missing for period={period} interval={interval}."
        )
    return df


def fetch_for_spec(tickers: Sequence[str], period: str, interval: str) -> pd.DataFrame:
    df = _fetch_prices(tickers, period, interval)
    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[DEFAULT_COLUMNS].copy()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def main() -> int:
    tickers = TICKERS
    if not tickers:
        raise RuntimeError("Ticker universe is empty.")
    print(f"[INFO] universe size: {len(tickers)}")

    generated = []
    for period, interval in PRICE_SPECS:
        print(f"[INFO] fetching prices period={period} interval={interval}")
        df = fetch_for_spec(tickers, period, interval)
        out_path = Path(OUTPUT_TEMPLATE.format(period=period, interval=interval))
        df.to_parquet(out_path, index=False)
        print(f"[OK] saved parquet: {out_path} rows={len(df)}")
        generated.append(out_path)

    summary = ", ".join(str(p) for p in generated)
    print(f"[INFO] generated files: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
