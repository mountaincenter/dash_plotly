#!/usr/bin/env python3
"""
Build a J-Quants based Grok segment master without modifying the archive.

The archive is treated as immutable metadata. Prices and executable segment
PnL columns prefixed with jq_ are derived from J-Quants minute bars.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
MINUTE_PATH = (
    BASE_DIR
    / "data"
    / "analysis"
    / "grok_intraday_jquants"
    / "grok_master_minute_jquants_basis.parquet"
)
OUTPUT_PATH = (
    BASE_DIR
    / "data"
    / "parquet"
    / "backtest"
    / "grok_master_jquants_segments.parquet"
)

SEG_TIMES = ["0930", "1000", "1030", "1100", "1130", "1300", "1330", "1400", "1430", "1500", "1530"]
TARGET_TIMES = {seg: f"{seg[:2]}:{seg[2:]}" for seg in SEG_TIMES}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Grok J-Quants segment master.")
    parser.add_argument("--archive-path", type=Path, default=ARCHIVE_PATH)
    parser.add_argument("--minute-path", type=Path, default=MINUTE_PATH)
    parser.add_argument("--output-path", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--csv-out", type=Path, default=None)
    return parser.parse_args()


def key_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = pd.to_numeric(numerator, errors="coerce")
    denominator = pd.to_numeric(denominator, errors="coerce")
    return numerator.where(denominator.ne(0)) / denominator.replace({0: np.nan})


def load_archive(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"archive not found: {path}")
    archive = pd.read_parquet(path).copy()
    archive["_archive_row_id"] = np.arange(len(archive))
    archive["_key_backtest_date"] = key_date(archive["backtest_date"])
    archive["_key_ticker"] = archive["ticker"].astype(str).str.strip()
    return archive


def load_minute(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"J-Quants minute basis file not found: {path}")
    cols = [
        "backtest_date",
        "ticker",
        "datetime",
        "time",
        "analysis_open",
        "analysis_high",
        "analysis_low",
        "analysis_close",
        "analysis_session_vwap",
        "volume",
        "value",
        "jq_first_time",
        "jq_last_time",
        "open_trade_status",
        "price_alignment_status",
        "archive_price_discrepancy",
        "archive_price_date_suspect",
    ]
    minute = pd.read_parquet(path, columns=cols).copy()
    minute["_key_backtest_date"] = key_date(minute["backtest_date"])
    minute["_key_ticker"] = minute["ticker"].astype(str).str.strip()
    minute["datetime"] = pd.to_datetime(minute["datetime"], errors="coerce")
    minute["time"] = minute["time"].astype(str)
    for col in [
        "analysis_open",
        "analysis_high",
        "analysis_low",
        "analysis_close",
        "analysis_session_vwap",
        "volume",
        "value",
    ]:
        minute[col] = pd.to_numeric(minute[col], errors="coerce")
    minute = minute[minute["_key_backtest_date"].notna() & minute["_key_ticker"].ne("") & minute["datetime"].notna()]
    return minute.sort_values(["_key_backtest_date", "_key_ticker", "datetime"]).reset_index(drop=True)


def build_segment_rows(minute: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    grouped = minute.groupby(["_key_backtest_date", "_key_ticker"], sort=False)

    for (date, ticker), bars in grouped:
        bars = bars.sort_values("datetime").reset_index(drop=True)
        first = bars.iloc[0]
        last = bars.iloc[-1]
        entry_price = float(first["analysis_open"]) if pd.notna(first["analysis_open"]) else np.nan
        close_price = float(last["analysis_close"]) if pd.notna(last["analysis_close"]) else np.nan

        row: dict[str, object] = {
            "_key_backtest_date": date,
            "_key_ticker": ticker,
            "jq_bar_count": int(len(bars)),
            "jq_buy_price": entry_price,
            "jq_daily_close": close_price,
            "jq_first_time": str(first["time"]),
            "jq_last_time": str(last["time"]),
            "jq_first_datetime": first["datetime"],
            "jq_last_datetime": last["datetime"],
            "jq_open_trade_status": first.get("open_trade_status"),
            "jq_price_alignment_status": first.get("price_alignment_status"),
            "jq_archive_price_discrepancy": bool(first.get("archive_price_discrepancy")),
            "jq_archive_price_date_suspect": bool(first.get("archive_price_date_suspect")),
            "jq_total_volume": float(bars["volume"].sum()),
            "jq_total_value": float(bars["value"].sum()),
            "jq_high": float(bars["analysis_high"].max()),
            "jq_low": float(bars["analysis_low"].min()),
            "jq_phase2_exit_price": close_price,
            "jq_profit_per_100_shares_phase2": (entry_price - close_price) * 100
            if pd.notna(entry_price) and pd.notna(close_price)
            else np.nan,
            "jq_phase2_return": (entry_price - close_price) / entry_price
            if pd.notna(entry_price) and entry_price != 0 and pd.notna(close_price)
            else np.nan,
            "jq_phase2_win": bool(entry_price > close_price) if pd.notna(entry_price) and pd.notna(close_price) else pd.NA,
        }

        times = bars["time"].astype(str)
        for seg in SEG_TIMES:
            target_time = TARGET_TIMES[seg]

            if seg == "1530":
                exit_price = close_price
                source_time = str(last["time"])
                source_datetime = last["datetime"]
                source_kind = "last_close"
                missing_reason = None
            else:
                has_prior_trade = bool((times < target_time).any())
                future = bars[times >= target_time]
                if not has_prior_trade:
                    exit_price = np.nan
                    source_time = None
                    source_datetime = pd.NaT
                    source_kind = None
                    missing_reason = "no_trade_before_target"
                elif future.empty:
                    exit_price = np.nan
                    source_time = None
                    source_datetime = pd.NaT
                    source_kind = None
                    missing_reason = "no_trade_at_or_after_target"
                else:
                    source = future.iloc[0]
                    exit_price = float(source["analysis_open"]) if pd.notna(source["analysis_open"]) else np.nan
                    source_time = str(source["time"])
                    source_datetime = source["datetime"]
                    source_kind = "exact_open" if source_time == target_time else "next_open"
                    missing_reason = None

            row[f"jq_seg_{seg}_exit_price"] = exit_price
            row[f"jq_seg_{seg}_source_time"] = source_time
            row[f"jq_seg_{seg}_source_datetime"] = source_datetime
            row[f"jq_seg_{seg}_source_kind"] = source_kind
            row[f"jq_seg_{seg}_missing_reason"] = missing_reason
            row[f"jq_seg_{seg}"] = (
                (entry_price - exit_price) * 100
                if pd.notna(entry_price) and pd.notna(exit_price)
                else np.nan
            )

        row["jq_phase1_exit_price"] = row["jq_seg_1130_exit_price"]
        row["jq_sell_price"] = row["jq_phase1_exit_price"]
        row["jq_profit_per_100_shares_phase1"] = row["jq_seg_1130"]
        row["jq_phase1_return"] = (
            row["jq_profit_per_100_shares_phase1"] / 100 / entry_price
            if pd.notna(row["jq_profit_per_100_shares_phase1"]) and pd.notna(entry_price) and entry_price != 0
            else np.nan
        )
        row["jq_phase1_win"] = (
            bool(row["jq_profit_per_100_shares_phase1"] > 0)
            if pd.notna(row["jq_profit_per_100_shares_phase1"])
            else pd.NA
        )

        rows.append(row)

    return pd.DataFrame(rows)


def build_master(archive: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    master = archive.merge(segments, on=["_key_backtest_date", "_key_ticker"], how="left")

    if "profit_per_100_shares_phase2" in master.columns:
        master["jq_minus_archive_phase2"] = (
            pd.to_numeric(master["jq_profit_per_100_shares_phase2"], errors="coerce")
            - pd.to_numeric(master["profit_per_100_shares_phase2"], errors="coerce")
        )
    if "profit_per_100_shares_phase1" in master.columns:
        master["jq_minus_archive_phase1"] = (
            pd.to_numeric(master["jq_profit_per_100_shares_phase1"], errors="coerce")
            - pd.to_numeric(master["profit_per_100_shares_phase1"], errors="coerce")
        )

    if "buy_price" in master.columns:
        master["jq_minus_archive_buy_price"] = (
            pd.to_numeric(master["jq_buy_price"], errors="coerce") - pd.to_numeric(master["buy_price"], errors="coerce")
        )
    if "daily_close" in master.columns:
        master["jq_minus_archive_daily_close"] = (
            pd.to_numeric(master["jq_daily_close"], errors="coerce") - pd.to_numeric(master["daily_close"], errors="coerce")
        )

    master = master.drop(columns=["_key_backtest_date", "_key_ticker"], errors="ignore")
    return master.sort_values("_archive_row_id").drop(columns=["_archive_row_id"], errors="ignore").reset_index(drop=True)


def print_summary(master: pd.DataFrame, output_path: Path) -> None:
    print("\nSummary")
    print("=" * 72)
    print(f"output             : {output_path}")
    print(f"rows               : {len(master):,}")
    print(f"jq matched rows    : {int(master['jq_buy_price'].notna().sum()):,}")
    print(f"jq missing rows    : {int(master['jq_buy_price'].isna().sum()):,}")
    print("\nJ-Quants segment missing counts")
    rows = []
    for seg in SEG_TIMES:
        rows.append(
            {
                "seg": f"jq_seg_{seg}",
                "missing": int(master[f"jq_seg_{seg}"].isna().sum()),
                "present": int(master[f"jq_seg_{seg}"].notna().sum()),
            }
        )
    print(pd.DataFrame(rows).to_string(index=False))

    if "jq_archive_price_date_suspect" in master.columns:
        suspect = master[master["jq_archive_price_date_suspect"].fillna(False)]
        if not suspect.empty:
            print("\narchive/yfinance suspect dates")
            print(
                suspect.groupby("backtest_date", as_index=False)
                .agg(
                    rows=("ticker", "count"),
                    phase2_abs_diff=("jq_minus_archive_phase2", lambda s: s.abs().sum()),
                )
                .to_string(index=False)
            )


def main() -> int:
    args = parse_args()
    archive = load_archive(args.archive_path)
    minute = load_minute(args.minute_path)
    segments = build_segment_rows(minute)
    master = build_master(archive, segments)

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_parquet(args.output_path, engine="pyarrow", index=False)
    print(f"[OK] saved parquet: {args.output_path}")

    if args.csv_out:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        master.to_csv(args.csv_out, index=False)
        print(f"[OK] saved csv: {args.csv_out}")

    print_summary(master, args.output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
