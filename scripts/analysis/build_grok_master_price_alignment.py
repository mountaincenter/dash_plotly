#!/usr/bin/env python3
"""
Build archive-safe price alignment masters for Grok intraday analysis.

This script reads grok_trending_archive.parquet as an immutable source of
truth and compares it with J-Quants minute bars. It does not modify the
archive.

Outputs:
- price alignment: one row per (backtest_date, ticker)
- minute J-Quants basis: one row per J-Quants minute bar. J-Quants
  prices are the primary analysis/execution basis; archive prices are kept
  only as immutable reference and discrepancy checks.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
PARQUET_DIR = BASE_DIR / "data" / "parquet"
ANALYSIS_DIR = BASE_DIR / "data" / "analysis" / "grok_intraday_jquants"

ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
MINUTE_PATHS = [
    ANALYSIS_DIR / "grok_jquants_minute.parquet",
    PARQUET_DIR / "jquants" / "grok_archive_minute.parquet",
]
OUTPUT_PATH = ANALYSIS_DIR / "grok_master_price_alignment.parquet"
MINUTE_OUTPUT_PATH = ANALYSIS_DIR / "grok_master_minute_jquants_basis.parquet"

PRICE_ABS_TOL = 1.0
FACTOR_REL_TOL_PCT = 0.2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Grok archive/J-Quants price alignment master.")
    parser.add_argument("--archive-path", type=Path, default=ARCHIVE_PATH)
    parser.add_argument("--minute-path", type=Path, action="append", default=None)
    parser.add_argument("--output-path", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--minute-output-path", type=Path, default=MINUTE_OUTPUT_PATH)
    parser.add_argument("--skip-minute-output", action="store_true")
    parser.add_argument("--csv-out", type=Path, default=None)
    return parser.parse_args()


def ticker_to_code(ticker: object) -> str | None:
    if ticker is None or pd.isna(ticker):
        return None
    text = str(ticker).strip()
    if not text:
        return None
    return text.split(".", 1)[0]


def load_archive(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"archive not found: {path}")

    cols = [
        "selection_date",
        "backtest_date",
        "ticker",
        "stock_name",
        "buy_price",
        "daily_close",
        "profit_per_100_shares_phase2",
        "shortable",
        "day_trade",
        "ng",
        "day_trade_available_shares",
        "margin_code",
        "margin_code_name",
        "is_shortable",
        "ml_prob",
        "prob_up",
        "ml_prob_wfcv",
        "weekday",
        "reason",
        "categories",
    ]
    archive = pd.read_parquet(path, columns=[c for c in cols if c])
    archive = archive.copy()
    archive["selection_date"] = pd.to_datetime(archive.get("selection_date"), errors="coerce").dt.strftime("%Y-%m-%d")
    archive["backtest_date"] = pd.to_datetime(archive["backtest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    archive["ticker"] = archive["ticker"].astype(str).str.strip()
    archive["code"] = archive["ticker"].map(ticker_to_code)
    archive = archive[archive["backtest_date"].notna() & archive["ticker"].ne("")]
    archive = archive.drop_duplicates(["backtest_date", "ticker"], keep="last").reset_index(drop=True)
    return archive


def load_minutes(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    cols = [
        "ticker",
        "query_code",
        "jquants_code",
        "stock_name",
        "selection_date",
        "trading_date",
        "time",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "session_vwap",
        "source",
        "interval",
        "raw_csv",
        "fetched_at",
    ]

    for priority, path in enumerate(paths):
        if not path.exists():
            print(f"[WARN] minute file not found: {path}")
            continue
        df = pd.read_parquet(path, columns=[c for c in cols if c])
        df = df.copy()
        df["minute_source_path"] = str(path)
        df["minute_source_priority"] = priority
        frames.append(df)
        print(f"[INFO] loaded minute file: {path} rows={len(df):,}")

    if not frames:
        raise FileNotFoundError("no minute files found")

    minute = pd.concat(frames, ignore_index=True)
    minute["ticker"] = minute["ticker"].astype(str).str.strip()
    minute["trading_date"] = pd.to_datetime(minute["trading_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    minute["datetime"] = pd.to_datetime(minute["datetime"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "value", "session_vwap"]:
        if col in minute.columns:
            minute[col] = pd.to_numeric(minute[col], errors="coerce")

    minute = minute[minute["ticker"].ne("") & minute["trading_date"].notna() & minute["datetime"].notna()]
    minute = minute.sort_values(["trading_date", "ticker", "minute_source_priority", "datetime"])
    return minute


def build_minute_summary(minute: pd.DataFrame) -> pd.DataFrame:
    pair_cols = ["trading_date", "ticker"]
    first = minute.groupby(pair_cols, as_index=False).first()
    last = minute.groupby(pair_cols, as_index=False).last()
    counts = (
        minute.groupby(pair_cols, as_index=False)
        .agg(
            jq_bar_count=("datetime", "count"),
            jq_total_volume=("volume", "sum"),
            jq_total_value=("value", "sum"),
            jq_high=("high", "max"),
            jq_low=("low", "min"),
        )
    )

    first_keep = first[
        [
            "trading_date",
            "ticker",
            "query_code",
            "jquants_code",
            "stock_name",
            "datetime",
            "time",
            "open",
            "close",
            "volume",
            "value",
            "session_vwap",
            "source",
            "interval",
            "raw_csv",
            "minute_source_path",
        ]
    ].rename(
        columns={
            "trading_date": "backtest_date",
            "stock_name": "stock_name_jquants",
            "datetime": "jq_first_datetime",
            "time": "jq_first_time",
            "open": "jq_raw_open",
            "close": "jq_first_close",
            "volume": "jq_first_volume",
            "value": "jq_first_value",
            "session_vwap": "jq_first_session_vwap",
            "source": "jq_source",
            "interval": "jq_interval",
            "raw_csv": "jq_raw_csv",
        }
    )
    last_keep = last[["trading_date", "ticker", "datetime", "time", "close", "volume", "value", "session_vwap"]].rename(
        columns={
            "trading_date": "backtest_date",
            "datetime": "jq_last_datetime",
            "time": "jq_last_time",
            "close": "jq_raw_close",
            "volume": "jq_last_volume",
            "value": "jq_last_value",
            "session_vwap": "jq_last_session_vwap",
        }
    )
    counts = counts.rename(columns={"trading_date": "backtest_date"})

    summary = first_keep.merge(last_keep, on=["backtest_date", "ticker"], how="outer")
    summary = summary.merge(counts, on=["backtest_date", "ticker"], how="outer")
    return summary


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce")
    numerator = pd.to_numeric(numerator, errors="coerce")
    return numerator.where(denominator.ne(0)) / denominator.replace({0: np.nan})


def prob_heat_bucket(prob: object) -> str:
    if prob is None or pd.isna(prob):
        return "UNKNOWN_PROB_HEAT"
    p = float(prob)
    if p < 0.4:
        return "LOW_PROB_HEAT"
    if p < 0.5:
        return "MID_PROB_HEAT"
    return "HIGH_PROB_HEAT"


def classify_credit(row: pd.Series) -> str:
    shortable = bool(row.get("shortable")) if not pd.isna(row.get("shortable")) else False
    day_trade = bool(row.get("day_trade")) if not pd.isna(row.get("day_trade")) else False
    shares = pd.to_numeric(pd.Series([row.get("day_trade_available_shares")]), errors="coerce").iloc[0]
    shares = 0 if pd.isna(shares) else shares
    if shortable:
        return "信用"
    if day_trade and shares > 0:
        return "いちにち除0"
    if day_trade:
        return "いちにち残0"
    return "空売り不可"


def status_for_row(row: pd.Series) -> str:
    if pd.isna(row.get("jq_raw_open")):
        return "no_intraday_data"
    if pd.isna(row.get("archive_open")) or row.get("archive_open") == 0 or row.get("jq_raw_open") == 0:
        return "invalid_open"
    if row.get("jq_first_time") == "15:30":
        return "close_only_or_special_quote"
    if abs(float(row.get("archive_open_diff", np.nan))) <= PRICE_ABS_TOL:
        if pd.isna(row.get("adjusted_close_diff_abs")):
            return "open_aligned_close_missing"
        if float(row["adjusted_close_diff_abs"]) <= PRICE_ABS_TOL:
            return "raw_open_close_aligned"
        return "raw_open_aligned_close_diff"
    if pd.isna(row.get("factor_diff_pct_abs")):
        return "open_adjustment_needed"
    if float(row["factor_diff_pct_abs"]) <= FACTOR_REL_TOL_PCT:
        return "open_factor_aligned"
    return "factor_mismatch"


def open_trade_status(first_time: object) -> str:
    if first_time is None or pd.isna(first_time):
        return "no_intraday_data"
    text = str(first_time)
    if text == "09:00":
        return "normal_0900"
    if text == "15:30":
        return "close_only_1530"
    return "delayed_open"


def build_alignment(archive: pd.DataFrame, minute_summary: pd.DataFrame) -> pd.DataFrame:
    out = archive.rename(columns={"stock_name": "stock_name_archive", "buy_price": "archive_open", "daily_close": "archive_close"}).merge(
        minute_summary,
        on=["backtest_date", "ticker"],
        how="left",
    )

    out["stock_name_master"] = out["stock_name_archive"].where(out["stock_name_archive"].notna(), out["stock_name_jquants"])
    out["archive_open"] = pd.to_numeric(out["archive_open"], errors="coerce")
    out["archive_close"] = pd.to_numeric(out["archive_close"], errors="coerce")
    out["profit_per_100_shares_phase2"] = pd.to_numeric(out.get("profit_per_100_shares_phase2"), errors="coerce")
    out["jq_raw_open"] = pd.to_numeric(out["jq_raw_open"], errors="coerce")
    out["jq_raw_close"] = pd.to_numeric(out["jq_raw_close"], errors="coerce")

    out["archive_open_diff"] = out["archive_open"] - out["jq_raw_open"]
    out["archive_open_diff_abs"] = out["archive_open_diff"].abs()
    out["archive_open_diff_pct"] = safe_div(out["archive_open_diff"], out["archive_open"]) * 100

    out["archive_close_diff"] = out["archive_close"] - out["jq_raw_close"]
    out["archive_close_diff_abs"] = out["archive_close_diff"].abs()
    out["archive_close_diff_pct"] = safe_div(out["archive_close_diff"], out["archive_close"]) * 100

    out["open_adjust_factor"] = safe_div(out["archive_open"], out["jq_raw_open"])
    out["close_adjust_factor"] = safe_div(out["archive_close"], out["jq_raw_close"])
    out["factor_diff"] = out["close_adjust_factor"] - out["open_adjust_factor"]
    out["factor_diff_pct"] = safe_div(out["factor_diff"], out["open_adjust_factor"]) * 100
    out["factor_diff_pct_abs"] = out["factor_diff_pct"].abs()

    out["jq_close_adjusted_by_open_factor"] = out["jq_raw_close"] * out["open_adjust_factor"]
    out["adjusted_close_diff"] = out["archive_close"] - out["jq_close_adjusted_by_open_factor"]
    out["adjusted_close_diff_abs"] = out["adjusted_close_diff"].abs()
    out["adjusted_close_diff_pct"] = safe_div(out["adjusted_close_diff"], out["archive_close"]) * 100

    out["phase2_recalc_from_archive"] = (out["archive_open"] - out["archive_close"]) * 100
    out["phase2_recalc_diff"] = out["profit_per_100_shares_phase2"] - out["phase2_recalc_from_archive"]
    out["phase2_recalc_from_jquants"] = (out["jq_raw_open"] - out["jq_raw_close"]) * 100
    out["phase2_archive_minus_jquants"] = out["profit_per_100_shares_phase2"] - out["phase2_recalc_from_jquants"]
    out["analysis_price_basis"] = np.where(out["jq_raw_open"].notna(), "jquants_minute", "archive_fallback")
    out["analysis_open"] = out["jq_raw_open"].where(out["jq_raw_open"].notna(), out["archive_open"])
    out["analysis_close"] = out["jq_raw_close"].where(out["jq_raw_close"].notna(), out["archive_close"])
    out["analysis_phase2_short_pnl_100"] = (out["analysis_open"] - out["analysis_close"]) * 100
    out["archive_price_discrepancy"] = out["archive_open_diff_abs"].gt(PRICE_ABS_TOL) | out["archive_close_diff_abs"].gt(PRICE_ABS_TOL)
    out["archive_price_discrepancy_count_by_date"] = out.groupby("backtest_date")["archive_price_discrepancy"].transform("sum")
    out["archive_price_discrepancy_rate_by_date"] = out.groupby("backtest_date")["archive_price_discrepancy"].transform("mean")
    out["archive_price_date_suspect"] = (
        out["archive_price_discrepancy_count_by_date"].ge(4)
        & out["archive_price_discrepancy_rate_by_date"].ge(0.2)
    )
    out["open_trade_status"] = out["jq_first_time"].map(open_trade_status)
    out["price_alignment_status"] = out.apply(status_for_row, axis=1)

    if "ml_prob" in out.columns:
        out["prob_heat_bucket"] = out["ml_prob"].map(prob_heat_bucket)
    else:
        out["prob_heat_bucket"] = "UNKNOWN_PROB_HEAT"
    out["credit_group"] = out.apply(classify_credit, axis=1)

    preferred = [
        "selection_date",
        "backtest_date",
        "ticker",
        "code",
        "stock_name_archive",
        "stock_name_jquants",
        "stock_name_master",
        "archive_open",
        "jq_raw_open",
        "archive_open_diff",
        "archive_open_diff_abs",
        "archive_open_diff_pct",
        "archive_close",
        "jq_raw_close",
        "archive_close_diff",
        "archive_close_diff_abs",
        "archive_close_diff_pct",
        "open_adjust_factor",
        "close_adjust_factor",
        "factor_diff_pct",
        "factor_diff_pct_abs",
        "jq_close_adjusted_by_open_factor",
        "adjusted_close_diff",
        "adjusted_close_diff_abs",
        "adjusted_close_diff_pct",
        "analysis_price_basis",
        "analysis_open",
        "analysis_close",
        "analysis_phase2_short_pnl_100",
        "phase2_recalc_from_jquants",
        "phase2_archive_minus_jquants",
        "archive_price_discrepancy",
        "archive_price_discrepancy_count_by_date",
        "archive_price_discrepancy_rate_by_date",
        "archive_price_date_suspect",
        "jq_first_time",
        "jq_last_time",
        "jq_first_datetime",
        "jq_last_datetime",
        "open_trade_status",
        "price_alignment_status",
        "jq_bar_count",
        "jq_total_volume",
        "jq_total_value",
        "jq_high",
        "jq_low",
        "jq_first_volume",
        "jq_last_volume",
        "jq_source",
        "jq_interval",
        "jq_raw_csv",
        "minute_source_path",
        "profit_per_100_shares_phase2",
        "phase2_recalc_from_archive",
        "phase2_recalc_diff",
        "credit_group",
        "shortable",
        "day_trade",
        "ng",
        "day_trade_available_shares",
        "margin_code",
        "margin_code_name",
        "is_shortable",
        "ml_prob",
        "prob_heat_bucket",
        "prob_up",
        "ml_prob_wfcv",
        "weekday",
        "reason",
        "categories",
    ]
    ordered = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[ordered].sort_values(["backtest_date", "ticker"]).reset_index(drop=True)


def build_jquants_basis_minute(minute: pd.DataFrame, alignment: pd.DataFrame) -> pd.DataFrame:
    minute = minute.copy()
    minute = minute.sort_values(["trading_date", "ticker", "minute_source_priority", "datetime"])
    minute = minute.drop_duplicates(["trading_date", "ticker", "datetime"], keep="first")

    align_cols = [
        "selection_date",
        "backtest_date",
        "ticker",
        "code",
        "stock_name_archive",
        "stock_name_jquants",
        "stock_name_master",
        "archive_open",
        "archive_close",
        "open_adjust_factor",
        "analysis_price_basis",
        "jq_first_time",
        "jq_last_time",
        "open_trade_status",
        "price_alignment_status",
        "archive_price_discrepancy",
        "archive_price_date_suspect",
        "credit_group",
        "shortable",
        "day_trade",
        "ng",
        "day_trade_available_shares",
        "ml_prob",
        "prob_heat_bucket",
        "weekday",
    ]
    align = alignment[[c for c in align_cols if c in alignment.columns]].copy()
    out = minute.merge(
        align,
        left_on=["trading_date", "ticker"],
        right_on=["backtest_date", "ticker"],
        how="inner",
        suffixes=("_minute", ""),
    )

    out = out.sort_values(["backtest_date", "ticker", "datetime"]).reset_index(drop=True)
    out["minute_index"] = out.groupby(["backtest_date", "ticker"]).cumcount() + 1
    out["is_first_bar"] = out["minute_index"].eq(1)
    out["is_last_bar"] = out["minute_index"].eq(
        out.groupby(["backtest_date", "ticker"])["minute_index"].transform("max")
    )

    out["raw_open"] = pd.to_numeric(out["open"], errors="coerce")
    out["raw_high"] = pd.to_numeric(out["high"], errors="coerce")
    out["raw_low"] = pd.to_numeric(out["low"], errors="coerce")
    out["raw_close"] = pd.to_numeric(out["close"], errors="coerce")
    out["raw_session_vwap"] = pd.to_numeric(out.get("session_vwap"), errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")

    factor = pd.to_numeric(out["open_adjust_factor"], errors="coerce")
    for raw_col, adj_col in [
        ("raw_open", "archive_basis_open"),
        ("raw_high", "archive_basis_high"),
        ("raw_low", "archive_basis_low"),
        ("raw_close", "archive_basis_close"),
        ("raw_session_vwap", "archive_basis_session_vwap"),
    ]:
        out[adj_col] = pd.to_numeric(out[raw_col], errors="coerce") * factor

    out["cum_volume"] = out.groupby(["backtest_date", "ticker"])["volume"].cumsum()
    out["cum_value"] = out.groupby(["backtest_date", "ticker"])["value"].cumsum()
    out["calc_session_vwap_raw"] = safe_div(out["cum_value"], out["cum_volume"])
    out["calc_session_vwap_archive_basis"] = out["calc_session_vwap_raw"] * factor
    out["archive_basis_session_vwap"] = out["archive_basis_session_vwap"].where(
        out["archive_basis_session_vwap"].notna(), out["calc_session_vwap_archive_basis"]
    )
    out["analysis_open"] = out["raw_open"]
    out["analysis_high"] = out["raw_high"]
    out["analysis_low"] = out["raw_low"]
    out["analysis_close"] = out["raw_close"]
    out["analysis_session_vwap"] = out["raw_session_vwap"].where(
        out["raw_session_vwap"].notna(), out["calc_session_vwap_raw"]
    )

    out["archive_open_minus_analysis_open"] = np.where(
        out["is_first_bar"],
        out["archive_open"] - out["analysis_open"],
        np.nan,
    )
    out["archive_close_minus_analysis_close"] = np.where(
        out["is_last_bar"],
        out["archive_close"] - out["analysis_close"],
        np.nan,
    )

    preferred = [
        "selection_date",
        "backtest_date",
        "trading_date",
        "ticker",
        "code",
        "stock_name_master",
        "stock_name_archive",
        "stock_name_jquants",
        "datetime",
        "time",
        "minute_index",
        "is_first_bar",
        "is_last_bar",
        "raw_open",
        "raw_high",
        "raw_low",
        "raw_close",
        "analysis_open",
        "analysis_high",
        "analysis_low",
        "analysis_close",
        "volume",
        "value",
        "raw_session_vwap",
        "analysis_session_vwap",
        "calc_session_vwap_raw",
        "cum_volume",
        "cum_value",
        "archive_open",
        "archive_close",
        "open_adjust_factor",
        "archive_open_minus_analysis_open",
        "archive_close_minus_analysis_close",
        "archive_basis_open",
        "archive_basis_high",
        "archive_basis_low",
        "archive_basis_close",
        "archive_basis_session_vwap",
        "calc_session_vwap_archive_basis",
        "analysis_price_basis",
        "jq_first_time",
        "jq_last_time",
        "open_trade_status",
        "price_alignment_status",
        "archive_price_discrepancy",
        "archive_price_date_suspect",
        "credit_group",
        "shortable",
        "day_trade",
        "ng",
        "day_trade_available_shares",
        "ml_prob",
        "prob_heat_bucket",
        "weekday",
        "source",
        "interval",
        "raw_csv",
        "minute_source_path",
    ]
    ordered = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[ordered]


def print_summary(alignment: pd.DataFrame, output_path: Path) -> None:
    print("\nSummary")
    print("=" * 72)
    print(f"output              : {output_path}")
    print(f"rows                : {len(alignment):,}")
    print(f"unique pairs        : {alignment[['backtest_date', 'ticker']].drop_duplicates().shape[0]:,}")
    print(f"date range          : {alignment['backtest_date'].min()} - {alignment['backtest_date'].max()}")
    print(f"minute matched      : {int(alignment['jq_raw_open'].notna().sum()):,}")
    print(f"minute missing      : {int(alignment['jq_raw_open'].isna().sum()):,}")
    print(f"open exact match    : {int(alignment['archive_open_diff_abs'].eq(0).sum()):,}")
    print(f"open <= 1 yen diff  : {int(alignment['archive_open_diff_abs'].le(1).sum()):,}")
    print(f"open > 1 yen diff   : {int(alignment['archive_open_diff_abs'].gt(1).sum()):,}")
    print(f"open > 10 yen diff  : {int(alignment['archive_open_diff_abs'].gt(10).sum()):,}")
    print("\nopen_trade_status")
    print(alignment["open_trade_status"].value_counts(dropna=False).to_string())
    print("\nprice_alignment_status")
    print(alignment["price_alignment_status"].value_counts(dropna=False).to_string())
    suspect_dates = (
        alignment[alignment["archive_price_date_suspect"]]
        .groupby("backtest_date", as_index=False)
        .agg(
            rows=("ticker", "count"),
            discrepancy_count=("archive_price_discrepancy", "sum"),
            discrepancy_rate=("archive_price_discrepancy", "mean"),
            phase2_archive_minus_jquants_abs_sum=("phase2_archive_minus_jquants", lambda s: s.abs().sum()),
        )
        .sort_values(["discrepancy_count", "backtest_date"], ascending=[False, True])
    )
    if not suspect_dates.empty:
        print("\narchive/yfinance suspect dates (J-Quants preferred)")
        print(suspect_dates.to_string(index=False))

    diff = alignment[alignment["archive_open_diff_abs"].gt(1)]
    if not diff.empty:
        cols = [
            "backtest_date",
            "ticker",
            "stock_name_master",
            "archive_open",
            "jq_raw_open",
            "archive_open_diff",
            "jq_first_time",
            "archive_close",
            "jq_raw_close",
            "adjusted_close_diff",
            "price_alignment_status",
            "jq_raw_csv",
        ]
        print("\nopen diff > 1 yen")
        print(
            diff.sort_values("archive_open_diff_abs", ascending=False)[cols].to_string(
                index=False
            )
        )


def main() -> int:
    args = parse_args()
    minute_paths = args.minute_path if args.minute_path else MINUTE_PATHS
    archive = load_archive(args.archive_path)
    minute = load_minutes(minute_paths)
    minute_summary = build_minute_summary(minute)
    alignment = build_alignment(archive, minute_summary)

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    alignment.to_parquet(args.output_path, engine="pyarrow", index=False)
    print(f"[OK] saved parquet: {args.output_path}")

    if not args.skip_minute_output:
        aligned_minute = build_jquants_basis_minute(minute, alignment)
        args.minute_output_path.parent.mkdir(parents=True, exist_ok=True)
        aligned_minute.to_parquet(args.minute_output_path, engine="pyarrow", index=False)
        print(f"[OK] saved minute parquet: {args.minute_output_path} rows={len(aligned_minute):,}")

    if args.csv_out:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        alignment.to_csv(args.csv_out, index=False)
        print(f"[OK] saved csv: {args.csv_out}")

    print_summary(alignment, args.output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
