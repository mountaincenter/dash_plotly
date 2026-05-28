#!/usr/bin/env python3
"""
generate_b4_etf_calendar_signals.py
===================================
calendar.parquet に Granville/B4 ETF リバウンド用フラグを追記する。

Signal date:
  B4銘柄が1つ以上発生
  + N225 < SMA20
  + VI >= 25
  + N225 SMA20 < SMA200

Action:
  - b4_etf_signal: 条件が大引け後に発火した日
  - b4_etf_buy_next: 翌営業日寄りでETFを買う日
  - b4_etf_sell_10d: no-overlap運用で10営業日後寄り売りの日

このスクリプトは generate_calendar.py 実行後、価格/VI更新後に実行する。
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"
INDEX_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
VI_RECENT_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
VI_HISTORY_PATH = Path(
    "/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly-staging/data/parquet/nikkei_vi_history.parquet"
)

LOOKBACK_YEARS = 10
HOLD_DAYS = 10
SIGNAL_COUNT_STRONG = 10
SIGNAL_COUNT_PANIC = 50


def _load_vi() -> pd.DataFrame:
    frames = []
    if VI_HISTORY_PATH.exists():
        hist = pd.read_parquet(VI_HISTORY_PATH, memory_map=False)
        hist["date"] = pd.to_datetime(hist["date"])
        frames.append(hist[["date", "close"]])
    if VI_RECENT_PATH.exists():
        recent = pd.read_parquet(VI_RECENT_PATH)
        recent["date"] = pd.to_datetime(recent["date"])
        close_col = "close" if "close" in recent.columns else "Close"
        frames.append(recent[["date", close_col]].rename(columns={close_col: "close"}))
    if not frames:
        return pd.DataFrame(columns=["date", "vi_close"])
    vi = pd.concat(frames, ignore_index=True)
    vi = vi.dropna(subset=["close"]).sort_values("date").drop_duplicates("date", keep="last")
    return vi.rename(columns={"close": "vi_close"})


def _load_context() -> pd.DataFrame:
    idx = pd.read_parquet(INDEX_PATH)
    idx["date"] = pd.to_datetime(idx["date"])
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].dropna().sort_values("date").copy()
    nk = nk.rename(columns={"Close": "n225_close"})
    nk["n225_sma20"] = nk["n225_close"].rolling(20, min_periods=20).mean()
    nk["n225_sma200"] = nk["n225_close"].rolling(200, min_periods=200).mean()
    nk["n225_below_sma20"] = nk["n225_close"] < nk["n225_sma20"]
    nk["n225_sma20_below_sma200"] = nk["n225_sma20"] < nk["n225_sma200"]
    nk = nk.merge(_load_vi(), on="date", how="left")
    nk["vi_ge25"] = nk["vi_close"] >= 25
    return nk


def build_b4_signals() -> pd.DataFrame:
    prices = pd.read_parquet(PRICES_PATH)
    prices["date"] = pd.to_datetime(prices["date"])
    cutoff = prices["date"].max() - pd.DateOffset(years=LOOKBACK_YEARS)
    prices = prices[prices["date"] >= cutoff - pd.Timedelta(days=260)].copy()
    prices = prices.dropna(subset=["Open", "High", "Low", "Close"])
    prices = prices[(prices[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    prices = prices[prices["Volume"].fillna(0) > 0]
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    g = prices.groupby("ticker")
    prices["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    prices["prev_close"] = g["Close"].shift(1)
    prices["dev20"] = (prices["Close"] / prices["sma20"] - 1) * 100
    prices["b4"] = (prices["dev20"] < -15) & (prices["Close"] > prices["prev_close"])
    prices = prices[prices["date"] >= cutoff].copy()
    prices = prices.merge(_load_context(), on="date", how="left")
    prices["b4_etf_condition"] = (
        prices["b4"].fillna(False)
        & prices["n225_below_sma20"].fillna(False)
        & prices["vi_ge25"].fillna(False)
        & prices["n225_sma20_below_sma200"].fillna(False)
    )

    signals = (
        prices[prices["b4_etf_condition"]]
        .groupby("date")
        .agg(
            b4_etf_signal_count=("ticker", "size"),
            b4_etf_avg_dev20=("dev20", "mean"),
            b4_etf_vi_close=("vi_close", "first"),
            b4_etf_n225_close=("n225_close", "first"),
            b4_etf_n225_sma20=("n225_sma20", "first"),
            b4_etf_n225_sma200=("n225_sma200", "first"),
        )
        .reset_index()
        .sort_values("date")
    )
    signals["b4_etf_signal"] = True
    signals["b4_etf_signal_strong"] = signals["b4_etf_signal_count"] >= SIGNAL_COUNT_STRONG
    signals["b4_etf_signal_panic"] = signals["b4_etf_signal_count"] >= SIGNAL_COUNT_PANIC
    return signals


def add_trade_dates(calendar: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    cal = calendar.sort_values("date").reset_index(drop=True).copy()
    cal_dates = list(cal["date"])
    signal_dates = set(signals["date"])

    cal["b4_etf_buy_next"] = False
    cal["b4_etf_sell_10d"] = False
    cal["b4_etf_buy_no_overlap"] = False
    cal["b4_etf_sell_10d_no_overlap"] = False

    last_exit_idx = -1
    date_to_idx = {d: i for i, d in enumerate(cal_dates)}
    for signal_date in sorted(signal_dates):
        signal_idx = date_to_idx.get(signal_date)
        if signal_idx is None:
            continue
        idx = signal_idx + 1
        exit_idx = idx + HOLD_DAYS
        if idx >= len(cal):
            continue
        cal.loc[idx, "b4_etf_buy_next"] = True
        if exit_idx < len(cal):
            cal.loc[exit_idx, "b4_etf_sell_10d"] = True

        if idx <= last_exit_idx:
            continue
        cal.loc[idx, "b4_etf_buy_no_overlap"] = True
        if exit_idx < len(cal):
            cal.loc[exit_idx, "b4_etf_sell_10d_no_overlap"] = True
            last_exit_idx = exit_idx

    return cal


def main() -> int:
    if not CALENDAR_PATH.exists():
        raise FileNotFoundError(CALENDAR_PATH)

    print("=" * 72)
    print("Generate B4 ETF calendar signals")
    print("=" * 72)
    calendar = pd.read_parquet(CALENDAR_PATH)
    calendar["date"] = pd.to_datetime(calendar["date"])
    signals = build_b4_signals()

    signal_cols = [
        "b4_etf_signal",
        "b4_etf_signal_count",
        "b4_etf_signal_strong",
        "b4_etf_signal_panic",
        "b4_etf_avg_dev20",
        "b4_etf_vi_close",
        "b4_etf_n225_close",
        "b4_etf_n225_sma20",
        "b4_etf_n225_sma200",
    ]
    calendar = calendar.drop(columns=[c for c in signal_cols if c in calendar.columns], errors="ignore")
    calendar = calendar.merge(signals[["date", *signal_cols]], on="date", how="left")
    calendar["b4_etf_signal"] = calendar["b4_etf_signal"].fillna(False)
    calendar["b4_etf_signal_strong"] = calendar["b4_etf_signal_strong"].fillna(False)
    calendar["b4_etf_signal_panic"] = calendar["b4_etf_signal_panic"].fillna(False)
    calendar["b4_etf_signal_count"] = calendar["b4_etf_signal_count"].fillna(0).astype("Int64")

    trade_cols = ["b4_etf_buy_next", "b4_etf_sell_10d", "b4_etf_buy_no_overlap", "b4_etf_sell_10d_no_overlap"]
    calendar = calendar.drop(columns=[c for c in trade_cols if c in calendar.columns], errors="ignore")
    calendar = add_trade_dates(calendar, signals)
    calendar["date"] = calendar["date"].dt.date
    calendar.to_parquet(CALENDAR_PATH, index=False)

    print(f"  signal days: {int(calendar['b4_etf_signal'].sum())}")
    print(f"  strong days(count>={SIGNAL_COUNT_STRONG}): {int(calendar['b4_etf_signal_strong'].sum())}")
    print(f"  panic days(count>={SIGNAL_COUNT_PANIC}): {int(calendar['b4_etf_signal_panic'].sum())}")
    print(f"  buy_next days: {int(calendar['b4_etf_buy_next'].sum())}")
    print(f"  buy_no_overlap days: {int(calendar['b4_etf_buy_no_overlap'].sum())}")
    print(f"[OK] {CALENDAR_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
