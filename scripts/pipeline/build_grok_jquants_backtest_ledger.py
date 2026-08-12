#!/usr/bin/env python3
"""Build a cumulative, reproducible Grok ledger without modifying the archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from scripts.lib.grok_jquants_backtest import (
    JQuantsBacktestDataError,
    MARKET_CAP_PROVENANCE_COLUMNS,
    TARGET_DAILY_PROVENANCE_COLUMNS,
    align_rows_to_archive_schema,
    assert_archive_schema_unchanged,
    validate_backtest_execution_states,
    validate_selection_market_cap,
    validate_target_daily_corporate_actions,
)


CANONICAL_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
DERIVED_DIR = PARQUET_DIR / "backtest"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
OUTPUT_PATH = PARQUET_DIR / "backtest" / "grok_jquants_backtest_ledger.parquet"
VALIDATION_PATH = (
    PARQUET_DIR / "backtest" / "grok_jquants_backtest_ledger.validation.json"
)
DATED_PATTERN = re.compile(r"^grok_trending_(\d{8})\.parquet$")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the cumulative derived Grok J-Quants backtest ledger."
    )
    parser.add_argument("--canonical-path", type=Path, default=CANONICAL_PATH)
    parser.add_argument("--derived-dir", type=Path, default=DERIVED_DIR)
    parser.add_argument("--calendar-path", type=Path, default=CALENDAR_PATH)
    parser.add_argument("--output-path", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--validation-path", type=Path, default=VALIDATION_PATH)
    return parser.parse_args()


def dated_files(directory: Path) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    for path in directory.glob("grok_trending_20*.parquet"):
        match = DATED_PATTERN.fullmatch(path.name)
        if not match:
            continue
        date_text = pd.Timestamp(match.group(1)).strftime("%Y-%m-%d")
        files.append((date_text, path))
    return sorted(files)


def validate_derived_day(
    frame: pd.DataFrame,
    filename_date: str,
    canonical: pd.DataFrame,
    calendar: pd.DataFrame,
) -> pd.DataFrame:
    if frame.empty:
        raise JQuantsBacktestDataError(
            f"Derived daily artifact is empty: {filename_date}"
        )
    required = {
        "backtest_date",
        "ticker",
        *MARKET_CAP_PROVENANCE_COLUMNS,
        *TARGET_DAILY_PROVENANCE_COLUMNS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise JQuantsBacktestDataError(
            f"Derived daily artifact lacks audit provenance: "
            f"date={filename_date}, missing={missing}"
        )
    missing_canonical = sorted(set(canonical.columns) - set(frame.columns))
    if missing_canonical:
        raise JQuantsBacktestDataError(
            "Derived daily artifact lacks canonical columns: "
            f"date={filename_date}, missing={missing_canonical}"
        )
    unexpected = sorted(
        set(frame.columns)
        - set(canonical.columns)
        - MARKET_CAP_PROVENANCE_COLUMNS
        - TARGET_DAILY_PROVENANCE_COLUMNS
    )
    if unexpected:
        raise JQuantsBacktestDataError(
            f"Derived daily artifact has unreviewed columns: {unexpected}"
        )
    target_dates = pd.to_datetime(
        frame["backtest_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    if not target_dates.eq(filename_date).all():
        raise JQuantsBacktestDataError(
            "Derived daily filename date does not match row backtest_date"
        )
    if frame[["backtest_date", "ticker"]].duplicated().any():
        raise JQuantsBacktestDataError(
            f"Derived daily artifact has duplicate keys: {filename_date}"
        )

    selection = frame.copy()
    selection["date"] = filename_date
    validate_selection_market_cap(selection, filename_date, calendar)
    target_features = frame[
        [
            "ticker",
            "jq_daily_target_date",
            "jq_daily_trade_status_target",
            "jq_mkt_cap_million_yen_target",
            "jq_market_cap_yen_target",
            "jq_ex_rights_type_target",
            "jq_adjustment_factor_target",
            "jq_daily_source_target",
            "jq_daily_fetched_at_target",
        ]
    ].rename(
        columns={
            "jq_daily_target_date": "trading_date",
            "jq_daily_trade_status_target": "jq_daily_trade_status",
            "jq_mkt_cap_million_yen_target": "jq_mkt_cap_million_yen",
            "jq_market_cap_yen_target": "jq_market_cap_yen",
            "jq_ex_rights_type_target": "jq_ex_rights_type",
            "jq_adjustment_factor_target": "jq_adjustment_factor",
            "jq_daily_source_target": "source",
            "jq_daily_fetched_at_target": "fetched_at",
        }
    )
    validate_target_daily_corporate_actions(
        selection,
        filename_date,
        target_features,
    )
    if {
        "data_source",
        "phase1_mark_status",
        "close_execution_status",
    }.issubset(canonical.columns):
        validate_backtest_execution_states(frame)
    return align_rows_to_archive_schema(
        canonical,
        frame,
        allowed_extra_columns=(
            MARKET_CAP_PROVENANCE_COLUMNS | TARGET_DAILY_PROVENANCE_COLUMNS
        ),
    )


def build_ledger(
    canonical: pd.DataFrame,
    derived: list[tuple[str, Path]],
    calendar: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    required = {"backtest_date", "ticker"}
    if not required.issubset(canonical.columns):
        raise JQuantsBacktestDataError("Canonical archive lacks key columns")
    if canonical.empty or canonical[["backtest_date", "ticker"]].duplicated().any():
        raise JQuantsBacktestDataError("Canonical archive is empty or has duplicate keys")
    canonical_dates = pd.to_datetime(canonical["backtest_date"], errors="raise")
    canonical_max = canonical_dates.max().strftime("%Y-%m-%d")
    if "date" not in calendar.columns:
        raise JQuantsBacktestDataError("Trading calendar lacks date column")
    trading_dates = sorted(
        pd.to_datetime(calendar["date"], errors="raise")
        .dt.strftime("%Y-%m-%d")
        .unique()
        .tolist()
    )
    if canonical_max not in trading_dates:
        raise JQuantsBacktestDataError(
            "Canonical maximum date is absent from the trading calendar"
        )

    additions: list[pd.DataFrame] = []
    receipts: list[dict[str, object]] = []
    for filename_date, path in derived:
        if filename_date <= canonical_max:
            continue
        rows = validate_derived_day(
            pd.read_parquet(path),
            filename_date,
            canonical,
            calendar,
        )
        additions.append(rows)
        receipts.append(
            {
                "path": str(path),
                "date": filename_date,
                "rows": int(len(rows)),
                "sha256": file_sha256(path),
            }
        )

    if receipts:
        receipt_dates = {str(item["date"]) for item in receipts}
        latest_derived = max(receipt_dates)
        expected_dates = {
            value
            for value in trading_dates
            if canonical_max < value <= latest_derived
        }
        missing_dates = sorted(expected_dates - receipt_dates)
        unexpected_dates = sorted(receipt_dates - set(trading_dates))
        if missing_dates or unexpected_dates:
            raise JQuantsBacktestDataError(
                "Derived ledger trading-date sequence is incomplete or invalid: "
                f"missing={missing_dates}, unexpected={unexpected_dates}"
            )

    ledger = pd.concat([canonical, *additions], ignore_index=True)
    assert_archive_schema_unchanged(canonical, ledger)
    if ledger[["backtest_date", "ticker"]].duplicated().any():
        raise JQuantsBacktestDataError(
            "Canonical and derived daily artifacts produce duplicate ledger keys"
        )
    try:
        pd.testing.assert_frame_equal(
            canonical.reset_index(drop=True),
            ledger.iloc[: len(canonical)].reset_index(drop=True),
            check_dtype=True,
            check_exact=True,
            check_categorical=True,
        )
    except AssertionError as error:
        raise JQuantsBacktestDataError(
            f"Canonical rows changed while building derived ledger: {error}"
        ) from error
    return ledger, receipts


def write_ledger_atomic(ledger: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        ledger.to_parquet(temporary, index=False)
        reloaded = pd.read_parquet(temporary)
        try:
            pd.testing.assert_frame_equal(
                ledger.reset_index(drop=True),
                reloaded.reset_index(drop=True),
                check_dtype=True,
                check_exact=True,
                check_categorical=True,
            )
        except AssertionError as error:
            raise JQuantsBacktestDataError(
                f"Derived ledger changed after serialization: {error}"
            ) from error
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    if args.output_path.resolve() == args.canonical_path.resolve():
        raise ValueError("Derived ledger output must not be the canonical archive")
    canonical_sha_before = file_sha256(args.canonical_path)
    canonical = pd.read_parquet(args.canonical_path)
    calendar = pd.read_parquet(args.calendar_path)
    ledger, receipts = build_ledger(
        canonical,
        dated_files(args.derived_dir),
        calendar,
    )
    write_ledger_atomic(ledger, args.output_path)
    canonical_sha_after = file_sha256(args.canonical_path)
    if canonical_sha_after != canonical_sha_before:
        raise JQuantsBacktestDataError(
            "Canonical archive changed while building derived ledger"
        )
    dates = pd.to_datetime(ledger["backtest_date"], errors="raise")
    report = {
        "status": "passed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "canonical_path": str(args.canonical_path),
        "canonical_sha256": canonical_sha_before,
        "canonical_rows": int(len(canonical)),
        "canonical_unchanged": True,
        "derived_files": receipts,
        "derived_rows": int(sum(int(item["rows"]) for item in receipts)),
        "ledger_path": str(args.output_path),
        "ledger_sha256": file_sha256(args.output_path),
        "ledger_rows": int(len(ledger)),
        "ledger_columns": int(len(ledger.columns)),
        "ledger_unique_keys": int(
            ledger[["backtest_date", "ticker"]].drop_duplicates().shape[0]
        ),
        "date_start": dates.min().strftime("%Y-%m-%d"),
        "date_end": dates.max().strftime("%Y-%m-%d"),
    }
    args.validation_path.parent.mkdir(parents=True, exist_ok=True)
    args.validation_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
