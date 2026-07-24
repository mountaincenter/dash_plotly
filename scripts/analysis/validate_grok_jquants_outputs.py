#!/usr/bin/env python3
"""
Validate Grok J-Quants derived outputs before publishing them.

This script treats grok_trending_archive.parquet as read-only source-of-truth.
It does not modify archive, minute cache, or master files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_ARCHIVE = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
DEFAULT_MINUTE = BASE_DIR / "data" / "parquet" / "jquants" / "grok_archive_minute.parquet"
DEFAULT_MASTER = BASE_DIR / "data" / "parquet" / "backtest" / "grok_master_jquants_segments.parquet"
DEFAULT_OUTPUT_JSON = (
    BASE_DIR / "data" / "parquet" / "backtest" / "grok_master_jquants_segments.validation.json"
)
NO_MARKET_TRADE_DATA_SOURCE = "jquants_no_market_trade"
NO_MARKET_TRADE_VALIDATION = "daily_all_null_and_minute_empty"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Grok J-Quants minute cache and segment master.")
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--minute-path", type=Path, default=DEFAULT_MINUTE)
    parser.add_argument("--master-path", type=Path, default=DEFAULT_MASTER)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--min-minute-coverage", type=float, default=0.80)
    parser.add_argument("--min-master-coverage-of-minute", type=float, default=0.95)
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def key_frame(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "date": normalize_date(df[date_col]),
            "ticker": df["ticker"].astype(str).str.strip(),
        }
    )
    out = out[out["date"].notna() & out["ticker"].ne("")]
    return out.drop_duplicates(["date", "ticker"]).reset_index(drop=True)


def key_set(df: pd.DataFrame) -> set[tuple[str, str]]:
    return set(zip(df["date"], df["ticker"]))


def pct(n: int, d: int) -> float | None:
    if d == 0:
        return None
    return round(n / d, 6)


def require_columns(df: pd.DataFrame, cols: set[str], label: str, failures: list[str]) -> None:
    missing = sorted(cols - set(df.columns))
    if missing:
        failures.append(f"{label} missing columns: {missing}")


def build_report(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    failures: list[str] = []
    warnings: list[str] = []

    for label, path in [
        ("archive", args.archive_path),
        ("minute", args.minute_path),
        ("master", args.master_path),
    ]:
        if not path.exists():
            failures.append(f"{label} file not found: {path}")

    if failures:
        return {
            "status": "failed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "failures": failures,
            "warnings": warnings,
        }, 1

    archive = pd.read_parquet(args.archive_path)
    minute = pd.read_parquet(args.minute_path)
    master = pd.read_parquet(args.master_path)

    require_columns(archive, {"backtest_date", "ticker"}, "archive", failures)
    require_columns(minute, {"trading_date", "ticker", "datetime"}, "minute", failures)
    require_columns(master, {"backtest_date", "ticker"}, "master", failures)
    require_columns(
        master,
        {
            "jq_bar_count",
            "jq_buy_price",
            "jq_seg_1530",
            "jq_close_execution_status",
        },
        "master",
        failures,
    )
    if failures:
        return {
            "status": "failed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "failures": failures,
            "warnings": warnings,
        }, 1

    archive_keys = key_frame(archive, "backtest_date")
    minute_keys = key_frame(minute, "trading_date")
    master_keys = key_frame(master, "backtest_date")

    archive_set = key_set(archive_keys)
    minute_set = key_set(minute_keys)
    master_set = key_set(master_keys)
    minute_in_archive = minute_set & archive_set

    archive_date = pd.to_datetime(archive["backtest_date"], errors="coerce")
    latest_archive_date = archive_date.max().strftime("%Y-%m-%d")
    latest_archive_keys = key_set(archive_keys[archive_keys["date"].eq(latest_archive_date)])

    archive_no_market_trade = pd.Series(False, index=archive.index)
    if {
        "data_source",
        "close_execution_status",
        "jquants_price_validation",
        "jquants_bar_count",
    }.issubset(archive.columns):
        archive_no_market_trade = (
            archive["data_source"].eq(NO_MARKET_TRADE_DATA_SOURCE)
            & archive["close_execution_status"].eq("no_market_trade")
            & archive["jquants_price_validation"].eq(
                NO_MARKET_TRADE_VALIDATION
            )
            & pd.to_numeric(
                archive["jquants_bar_count"], errors="coerce"
            ).eq(0)
        )
    archive_claimed_no_market_trade = pd.Series(
        False, index=archive.index
    )
    if "close_execution_status" in archive.columns:
        archive_claimed_no_market_trade = archive[
            "close_execution_status"
        ].eq("no_market_trade")
    invalid_no_market_trade_claims = int(
        (archive_claimed_no_market_trade & ~archive_no_market_trade).sum()
    )
    if invalid_no_market_trade_claims:
        failures.append(
            "unvalidated archive no-market-trade rows: "
            f"{invalid_no_market_trade_claims}"
        )

    no_market_trade_execution = pd.DataFrame(
        {
            "date": normalize_date(
                archive.loc[archive_no_market_trade, "backtest_date"]
            ),
            "ticker": archive.loc[
                archive_no_market_trade, "ticker"
            ].astype(str).str.strip(),
            "expected_close_execution_status": "no_market_trade",
        }
    )
    no_market_trade_keys = key_set(
        no_market_trade_execution[["date", "ticker"]]
    )
    no_market_trade_with_minutes = len(no_market_trade_keys & minute_set)
    if no_market_trade_with_minutes:
        failures.append(
            "no-market-trade archive rows unexpectedly have minute bars: "
            f"{no_market_trade_with_minutes}"
        )

    expected_minute_keys = archive_set - no_market_trade_keys
    minute_in_expected = minute_set & expected_minute_keys
    latest_expected_minute_keys = latest_archive_keys - no_market_trade_keys
    latest_minute_keys = minute_set & latest_expected_minute_keys

    if len(master) != len(archive):
        failures.append(f"master row count mismatch: master={len(master)} archive={len(archive)}")

    missing_master_keys = archive_set - master_set
    extra_master_keys = master_set - archive_set
    if missing_master_keys:
        failures.append(f"master missing archive keys: {len(missing_master_keys)}")
    if extra_master_keys:
        failures.append(f"master has non-archive keys: {len(extra_master_keys)}")

    raw_minute_coverage = pct(len(minute_in_archive), len(archive_set))
    expected_minute_coverage = (
        1.0
        if not expected_minute_keys
        else pct(len(minute_in_expected), len(expected_minute_keys))
    )
    if (
        expected_minute_coverage is None
        or expected_minute_coverage < args.min_minute_coverage
    ):
        failures.append(
            "minute cache coverage too low: "
            f"{len(minute_in_expected)}/{len(expected_minute_keys)} "
            f"({0 if expected_minute_coverage is None else expected_minute_coverage:.2%})"
        )

    if latest_expected_minute_keys and not latest_minute_keys:
        failures.append(f"latest archive date has no minute cache coverage: {latest_archive_date}")

    expected_master_jq = int(len(minute_in_archive) * args.min_master_coverage_of_minute)
    jq_buy_non_null = int(master["jq_buy_price"].notna().sum())
    jq_seg_1530_non_null = int(master["jq_seg_1530"].notna().sum())
    if jq_buy_non_null < expected_master_jq:
        failures.append(f"jq_buy_price coverage too low: {jq_buy_non_null}/{len(minute_in_archive)}")
    if jq_seg_1530_non_null < expected_master_jq:
        failures.append(f"jq_seg_1530 coverage too low: {jq_seg_1530_non_null}/{len(minute_in_archive)}")

    minute_execution = minute[["trading_date", "ticker", "datetime"]].copy()
    minute_execution["date"] = normalize_date(minute_execution["trading_date"])
    minute_execution["datetime"] = pd.to_datetime(
        minute_execution["datetime"], errors="coerce"
    )
    minute_execution = minute_execution.dropna(subset=["date", "ticker", "datetime"])
    minute_execution = minute_execution.groupby(
        ["date", "ticker"], as_index=False
    ).agg(first_datetime=("datetime", "min"), last_datetime=("datetime", "max"))
    minute_execution["expected_close_execution_status"] = "executable"
    minute_execution.loc[
        minute_execution["first_datetime"].eq(minute_execution["last_datetime"]),
        "expected_close_execution_status",
    ] = "mark_only_no_round_trip"

    minute_execution_keys = list(
        zip(minute_execution["date"], minute_execution["ticker"])
    )
    minute_execution = minute_execution.loc[
        [key not in no_market_trade_keys for key in minute_execution_keys]
    ]
    expected_execution = pd.concat(
        [minute_execution, no_market_trade_execution],
        ignore_index=True,
    )

    master_execution = pd.DataFrame(
        {
            "date": normalize_date(master["backtest_date"]),
            "ticker": master["ticker"].astype(str).str.strip(),
            "jq_close_execution_status": master[
                "jq_close_execution_status"
            ].astype(str),
        }
    )
    master_execution_keys = list(
        zip(master_execution["date"], master_execution["ticker"])
    )
    master_no_market_trade = pd.Series(
        [key in no_market_trade_keys for key in master_execution_keys],
        index=master.index,
    )
    invalid_master_no_market_trade = int(
        (
            master_no_market_trade
            & (
                master["jq_close_execution_status"].ne("no_market_trade")
                | pd.to_numeric(
                    master["jq_bar_count"], errors="coerce"
                ).ne(0)
                | master["jq_buy_price"].notna()
                | master["jq_seg_1530"].notna()
            )
        ).sum()
    )
    if invalid_master_no_market_trade:
        failures.append(
            "invalid derived no-market-trade rows: "
            f"{invalid_master_no_market_trade}"
        )
    execution_check = master_execution.merge(
        expected_execution[
            ["date", "ticker", "expected_close_execution_status"]
        ],
        on=["date", "ticker"],
        how="left",
        validate="one_to_one",
    )
    execution_mismatches = int(
        execution_check["expected_close_execution_status"]
        .ne(execution_check["jq_close_execution_status"])
        .sum()
    )
    if execution_mismatches:
        failures.append(
            f"close execution status mismatches: {execution_mismatches}"
        )
    close_execution_counts = {
        str(key): int(value)
        for key, value in master["jq_close_execution_status"]
        .value_counts(dropna=False)
        .items()
    }

    if minute_set - archive_set:
        warnings.append(f"minute has non-archive keys: {len(minute_set - archive_set)}")

    report: dict[str, Any] = {
        "status": "passed" if not failures else "failed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "archive": {
            "path": str(args.archive_path),
            "sha256": file_sha256(args.archive_path),
            "rows": int(len(archive)),
            "keys": int(len(archive_set)),
            "date_start": archive_date.min().strftime("%Y-%m-%d"),
            "date_end": latest_archive_date,
            "dates": int(archive_date.dt.date.nunique()),
        },
        "minute": {
            "path": str(args.minute_path),
            "sha256": file_sha256(args.minute_path),
            "rows": int(len(minute)),
            "keys": int(len(minute_set)),
            "keys_in_archive": int(len(minute_in_archive)),
            "coverage_of_archive_keys": raw_minute_coverage,
            "coverage_of_expected_keys": expected_minute_coverage,
            "latest_archive_date_keys": int(len(latest_archive_keys)),
            "latest_archive_date_minute_keys": int(len(latest_minute_keys)),
            "expected_keys_excluding_no_market_trade": int(
                len(expected_minute_keys)
            ),
        },
        "master": {
            "path": str(args.master_path),
            "sha256": file_sha256(args.master_path),
            "rows": int(len(master)),
            "keys": int(len(master_set)),
            "jq_buy_price_non_null": jq_buy_non_null,
            "jq_seg_1530_non_null": jq_seg_1530_non_null,
            "expected_jq_non_null_min": expected_master_jq,
            "close_execution_status": close_execution_counts,
            "close_execution_status_mismatches": execution_mismatches,
            "no_market_trade_rows": int(archive_no_market_trade.sum()),
            "invalid_no_market_trade_rows": invalid_master_no_market_trade,
        },
        "thresholds": {
            "min_minute_coverage": args.min_minute_coverage,
            "min_master_coverage_of_minute": args.min_master_coverage_of_minute,
        },
        "failures": failures,
        "warnings": warnings,
    }
    return report, 0 if not failures else 1


def main() -> int:
    args = parse_args()
    report, exit_code = build_report(args)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
