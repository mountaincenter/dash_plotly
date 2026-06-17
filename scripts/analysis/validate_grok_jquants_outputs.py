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
    require_columns(minute, {"trading_date", "ticker"}, "minute", failures)
    require_columns(master, {"backtest_date", "ticker"}, "master", failures)
    require_columns(master, {"jq_buy_price", "jq_seg_1530"}, "master", failures)
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
    latest_minute_keys = minute_set & latest_archive_keys

    if len(master) != len(archive):
        failures.append(f"master row count mismatch: master={len(master)} archive={len(archive)}")

    missing_master_keys = archive_set - master_set
    extra_master_keys = master_set - archive_set
    if missing_master_keys:
        failures.append(f"master missing archive keys: {len(missing_master_keys)}")
    if extra_master_keys:
        failures.append(f"master has non-archive keys: {len(extra_master_keys)}")

    minute_coverage = pct(len(minute_in_archive), len(archive_set))
    if minute_coverage is None or minute_coverage < args.min_minute_coverage:
        failures.append(
            "minute cache coverage too low: "
            f"{len(minute_in_archive)}/{len(archive_set)} "
            f"({0 if minute_coverage is None else minute_coverage:.2%})"
        )

    if latest_archive_keys and not latest_minute_keys:
        failures.append(f"latest archive date has no minute cache coverage: {latest_archive_date}")

    expected_master_jq = int(len(minute_in_archive) * args.min_master_coverage_of_minute)
    jq_buy_non_null = int(master["jq_buy_price"].notna().sum())
    jq_seg_1530_non_null = int(master["jq_seg_1530"].notna().sum())
    if jq_buy_non_null < expected_master_jq:
        failures.append(f"jq_buy_price coverage too low: {jq_buy_non_null}/{len(minute_in_archive)}")
    if jq_seg_1530_non_null < expected_master_jq:
        failures.append(f"jq_seg_1530 coverage too low: {jq_seg_1530_non_null}/{len(minute_in_archive)}")

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
            "coverage_of_archive_keys": minute_coverage,
            "latest_archive_date_keys": int(len(latest_archive_keys)),
            "latest_archive_date_minute_keys": int(len(latest_minute_keys)),
        },
        "master": {
            "path": str(args.master_path),
            "sha256": file_sha256(args.master_path),
            "rows": int(len(master)),
            "keys": int(len(master_set)),
            "jq_buy_price_non_null": jq_buy_non_null,
            "jq_seg_1530_non_null": jq_seg_1530_non_null,
            "expected_jq_non_null_min": expected_master_jq,
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
