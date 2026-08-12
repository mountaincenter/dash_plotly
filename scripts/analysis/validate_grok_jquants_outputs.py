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
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.lib.jquants_daily_fields import (
    JQ_ADJUSTMENT_FACTOR,
    JQ_EX_RIGHTS_TYPE,
    JQ_MARKET_CAP_YEN,
    JQ_MKT_CAP_MILLION_YEN,
    JQUANTS_DAILY_FIELD_COLUMNS,
    VALID_EX_RIGHTS_TYPES,
)

DEFAULT_ARCHIVE = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
DEFAULT_MINUTE = BASE_DIR / "data" / "parquet" / "jquants" / "grok_archive_minute.parquet"
DEFAULT_DAILY = BASE_DIR / "data" / "parquet" / "jquants" / "grok_jquants_daily.parquet"
DEFAULT_CALENDAR = BASE_DIR / "data" / "parquet" / "calendar.parquet"
DEFAULT_MASTER = BASE_DIR / "data" / "parquet" / "backtest" / "grok_master_jquants_segments.parquet"
DEFAULT_OUTPUT_JSON = (
    BASE_DIR / "data" / "parquet" / "backtest" / "grok_master_jquants_segments.validation.json"
)

JQ_MKT_CAP_MILLION_YEN_TARGET = "jq_mkt_cap_million_yen_target"
JQ_MARKET_CAP_YEN_TARGET = "jq_market_cap_yen_target"
JQ_EX_RIGHTS_TYPE_TARGET = "jq_ex_rights_type_target"
JQ_ADJUSTMENT_FACTOR_TARGET = "jq_adjustment_factor_target"
JQ_MARKET_CAP_ASOF_DATE = "jq_market_cap_asof_date"
JQ_MKT_CAP_MILLION_YEN_ASOF = "jq_mkt_cap_million_yen_asof"
JQ_MARKET_CAP_YEN_ASOF = "jq_market_cap_yen_asof"
JQ_ADJUSTMENT_FACTOR_ASOF = "jq_adjustment_factor_asof"
JQ_CLOSE_ASOF = "jq_close_asof"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Grok J-Quants minute cache and segment master.")
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--minute-path", type=Path, default=DEFAULT_MINUTE)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY)
    parser.add_argument("--calendar-path", type=Path, default=DEFAULT_CALENDAR)
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


def load_trading_dates(frame: pd.DataFrame) -> list[str]:
    if "date" not in frame.columns:
        raise ValueError("trading calendar missing date column")
    dates = sorted(normalize_date(frame["date"]).dropna().unique().tolist())
    if not dates:
        raise ValueError("trading calendar has no valid dates")
    return dates


def previous_trading_date_map(
    target_dates: pd.Series,
    trading_dates: list[str],
) -> dict[str, str]:
    positions = {date: idx for idx, date in enumerate(trading_dates)}
    result: dict[str, str] = {}
    for target_date in target_dates.dropna().astype(str).unique().tolist():
        position = positions.get(target_date)
        if position is None:
            raise ValueError(
                f"archive target date not found in trading calendar: {target_date}"
            )
        if position == 0:
            raise ValueError(f"trading calendar has no prior date for: {target_date}")
        result[target_date] = trading_dates[position - 1]
    return result


def numeric_mismatch_count(
    left: pd.Series,
    right: pd.Series,
    *,
    atol: float = 0.0,
) -> int:
    left_numeric = pd.to_numeric(left, errors="coerce")
    right_numeric = pd.to_numeric(right, errors="coerce")
    both_null = left_numeric.isna() & right_numeric.isna()
    both_values = left_numeric.notna() & right_numeric.notna()
    equal_values = both_values & left_numeric.sub(right_numeric).abs().le(atol)
    return int((~(both_null | equal_values)).sum())


def key_frame(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "date": normalize_date(df[date_col]),
            "ticker": df["ticker"].astype(str).str.strip(),
        }
    )
    out = out[out["date"].notna() & out["ticker"].ne("")]
    return out.drop_duplicates(["date", "ticker"]).reset_index(drop=True)


def normalize_jquants_code(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().removesuffix(".0")
    if len(text) == 5 and text.endswith("0"):
        text = text[:-1]
    return text


def daily_key_frame(df: pd.DataFrame, *, deduplicate: bool = True) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "date": normalize_date(df["trading_date"]),
            "ticker": df["jquants_code"].map(normalize_jquants_code) + ".T",
        }
    )
    out = out[out["date"].notna() & out["ticker"].ne(".T")]
    if deduplicate:
        out = out.drop_duplicates(["date", "ticker"])
    return out.reset_index(drop=True)


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
        ("daily", args.daily_path),
        ("calendar", args.calendar_path),
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
    daily = pd.read_parquet(args.daily_path)
    calendar = pd.read_parquet(args.calendar_path)
    master = pd.read_parquet(args.master_path)

    require_columns(archive, {"backtest_date", "ticker"}, "archive", failures)
    require_columns(minute, {"trading_date", "ticker", "datetime"}, "minute", failures)
    require_columns(
        daily,
        {"trading_date", "jquants_code", "close", *JQUANTS_DAILY_FIELD_COLUMNS},
        "daily",
        failures,
    )
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
    require_columns(master, set(JQUANTS_DAILY_FIELD_COLUMNS), "master", failures)
    require_columns(
        master,
        {
            JQ_MKT_CAP_MILLION_YEN_TARGET,
            JQ_MARKET_CAP_YEN_TARGET,
            JQ_EX_RIGHTS_TYPE_TARGET,
            JQ_ADJUSTMENT_FACTOR_TARGET,
            JQ_MARKET_CAP_ASOF_DATE,
            JQ_MKT_CAP_MILLION_YEN_ASOF,
            JQ_MARKET_CAP_YEN_ASOF,
            JQ_ADJUSTMENT_FACTOR_ASOF,
            JQ_CLOSE_ASOF,
        },
        "master",
        failures,
    )
    require_columns(calendar, {"date"}, "calendar", failures)
    if failures:
        return {
            "status": "failed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "failures": failures,
            "warnings": warnings,
        }, 1

    archive_keys = key_frame(archive, "backtest_date")
    minute_keys = key_frame(minute, "trading_date")
    daily_keys_all = daily_key_frame(daily, deduplicate=False)
    daily_keys = daily_keys_all.drop_duplicates(["date", "ticker"]).reset_index(drop=True)
    master_keys = key_frame(master, "backtest_date")

    archive_set = key_set(archive_keys)
    minute_set = key_set(minute_keys)
    daily_set = key_set(daily_keys)
    master_set = key_set(master_keys)
    minute_in_archive = minute_set & archive_set
    daily_in_archive = daily_set & archive_set
    no_market_trade_set: set[tuple[str, str]] = set()
    no_market_status_columns = {
        "data_source",
        "phase1_mark_status",
        "close_execution_status",
    }
    if no_market_status_columns.issubset(archive.columns):
        no_market_mask = (
            archive["data_source"].eq("jquants_no_market_trade")
            & archive["phase1_mark_status"].eq("no_market_trade")
            & archive["close_execution_status"].eq("no_market_trade")
        )
        no_market_trade_set = key_set(archive_keys.loc[no_market_mask])
    logically_covered_minute_keys = minute_in_archive | no_market_trade_set
    unresolved_minute_keys = archive_set - logically_covered_minute_keys
    no_market_trade_with_bars = no_market_trade_set & minute_set
    if no_market_trade_with_bars:
        failures.append(
            "archive keys marked no_market_trade unexpectedly have minute bars: "
            f"{sorted(no_market_trade_with_bars)[:10]}"
        )

    archive_date = pd.to_datetime(archive["backtest_date"], errors="coerce")
    latest_archive_date = archive_date.max().strftime("%Y-%m-%d")
    latest_archive_keys = key_set(archive_keys[archive_keys["date"].eq(latest_archive_date)])
    latest_minute_keys = minute_set & latest_archive_keys
    latest_logically_covered_keys = (
        minute_set | no_market_trade_set
    ) & latest_archive_keys
    latest_daily_keys = daily_set & latest_archive_keys

    daily_work = daily.copy()
    daily_work["_key_date"] = normalize_date(daily_work["trading_date"])
    daily_work["_key_ticker"] = (
        daily_work["jquants_code"].map(normalize_jquants_code) + ".T"
    )
    daily_work = daily_work[
        daily_work["_key_date"].notna() & daily_work["_key_ticker"].ne(".T")
    ].copy()
    daily_duplicate_keys = int(
        daily_work.duplicated(["_key_date", "_key_ticker"], keep=False).sum()
    )
    if daily_duplicate_keys:
        failures.append(f"daily cache duplicate keys: {daily_duplicate_keys}")

    mkt_cap_million = pd.to_numeric(
        daily_work[JQ_MKT_CAP_MILLION_YEN], errors="coerce"
    )
    market_cap_yen = pd.to_numeric(
        daily_work[JQ_MARKET_CAP_YEN], errors="coerce"
    )
    ex_rights_type = pd.to_numeric(
        daily_work[JQ_EX_RIGHTS_TYPE], errors="coerce"
    )
    adjustment_factor = pd.to_numeric(
        daily_work[JQ_ADJUSTMENT_FACTOR], errors="coerce"
    )

    unit_rows = mkt_cap_million.notna() & market_cap_yen.notna()
    unit_mismatches = int(
        (
            market_cap_yen[unit_rows]
            .sub(mkt_cap_million[unit_rows] * 1_000_000.0)
            .abs()
            .gt(0.5)
        ).sum()
    )
    if unit_mismatches:
        failures.append(f"MktCap million-yen conversion mismatches: {unit_mismatches}")
    negative_mkt_cap = int((mkt_cap_million.dropna() < 0).sum())
    if negative_mkt_cap:
        failures.append(f"negative MktCap rows: {negative_mkt_cap}")
    invalid_ex_rights = int(
        (ex_rights_type.notna() & ~ex_rights_type.isin(VALID_EX_RIGHTS_TYPES)).sum()
    )
    if invalid_ex_rights:
        failures.append(f"invalid ExRT rows: {invalid_ex_rights}")
    invalid_adjustment_factor = int((adjustment_factor.dropna() <= 0).sum())
    if invalid_adjustment_factor:
        failures.append(
            f"non-positive AdjFactor rows: {invalid_adjustment_factor}"
        )

    etf_200a = daily_work["_key_ticker"].eq("200A.T")
    etf_200a_rows = int(etf_200a.sum())
    etf_200a_mkt_cap_non_null = int(mkt_cap_million[etf_200a].notna().sum())
    if etf_200a_mkt_cap_non_null:
        failures.append(
            "ETF 200A unexpectedly has non-null MktCap rows: "
            f"{etf_200a_mkt_cap_non_null}"
        )

    latest_daily_mask = pd.Series(
        list(zip(daily_work["_key_date"], daily_work["_key_ticker"])),
        index=daily_work.index,
    ).isin(latest_archive_keys)
    latest_archive_daily_mkt_cap = int(
        mkt_cap_million[latest_daily_mask].notna().sum()
    )
    if latest_archive_keys and not latest_daily_keys:
        failures.append(
            f"latest archive date has no daily cache coverage: {latest_archive_date}"
        )

    trading_dates = load_trading_dates(calendar)
    archive_asof = pd.DataFrame(
        {
            "target_date": normalize_date(archive["backtest_date"]),
            "ticker": archive["ticker"].astype(str).str.strip(),
        }
    )
    asof_map = previous_trading_date_map(
        archive_asof["target_date"], trading_dates
    )
    archive_asof["expected_asof_date"] = archive_asof["target_date"].map(
        asof_map
    )
    daily_asof_lookup = daily_work[
        [
            "_key_date",
            "_key_ticker",
            "close",
            JQ_MKT_CAP_MILLION_YEN,
            JQ_MARKET_CAP_YEN,
            JQ_ADJUSTMENT_FACTOR,
        ]
    ].rename(
        columns={
            "_key_date": "expected_asof_date",
            "_key_ticker": "ticker",
            "close": "expected_close_asof",
            JQ_MKT_CAP_MILLION_YEN: "expected_mkt_cap_million_yen_asof",
            JQ_MARKET_CAP_YEN: "expected_market_cap_yen_asof",
            JQ_ADJUSTMENT_FACTOR: "expected_adjustment_factor_asof",
        }
    )
    expected_asof = archive_asof.merge(
        daily_asof_lookup,
        on=["expected_asof_date", "ticker"],
        how="left",
        validate="one_to_one",
        indicator="asof_daily_merge",
    )
    asof_daily_rows = int(expected_asof["asof_daily_merge"].eq("both").sum())
    if asof_daily_rows != len(archive_asof):
        failures.append(
            "selection as-of daily coverage incomplete: "
            f"{asof_daily_rows}/{len(archive_asof)}"
        )

    master_asof = pd.DataFrame(
        {
            "target_date": normalize_date(master["backtest_date"]),
            "ticker": master["ticker"].astype(str).str.strip(),
            "observed_asof_date": normalize_date(master[JQ_MARKET_CAP_ASOF_DATE]),
            "observed_close_asof": master[JQ_CLOSE_ASOF],
            "observed_mkt_cap_million_yen_asof": master[
                JQ_MKT_CAP_MILLION_YEN_ASOF
            ],
            "observed_market_cap_yen_asof": master[JQ_MARKET_CAP_YEN_ASOF],
            "observed_adjustment_factor_asof": master[
                JQ_ADJUSTMENT_FACTOR_ASOF
            ],
        }
    )
    asof_check = expected_asof.merge(
        master_asof,
        on=["target_date", "ticker"],
        how="left",
        validate="one_to_one",
    )
    asof_date_mismatches = int(
        asof_check["observed_asof_date"]
        .ne(asof_check["expected_asof_date"])
        .sum()
    )
    asof_target_dates = pd.to_datetime(
        asof_check["target_date"], errors="coerce"
    )
    asof_observed_dates = pd.to_datetime(
        asof_check["observed_asof_date"], errors="coerce"
    )
    asof_not_before_target = int(
        (asof_observed_dates.isna() | asof_observed_dates.ge(asof_target_dates)).sum()
    )
    asof_close_mismatches = numeric_mismatch_count(
        asof_check["observed_close_asof"],
        asof_check["expected_close_asof"],
        atol=0.011,
    )
    asof_mkt_cap_million_mismatches = numeric_mismatch_count(
        asof_check["observed_mkt_cap_million_yen_asof"],
        asof_check["expected_mkt_cap_million_yen_asof"],
        atol=0.5,
    )
    asof_market_cap_yen_mismatches = numeric_mismatch_count(
        asof_check["observed_market_cap_yen_asof"],
        asof_check["expected_market_cap_yen_asof"],
        atol=0.5,
    )
    asof_adjustment_factor_mismatches = numeric_mismatch_count(
        asof_check["observed_adjustment_factor_asof"],
        asof_check["expected_adjustment_factor_asof"],
        atol=1e-12,
    )
    if asof_date_mismatches:
        failures.append(
            f"selection as-of date mismatches: {asof_date_mismatches}"
        )
    if asof_not_before_target:
        failures.append(
            "selection as-of dates are not strictly before target: "
            f"{asof_not_before_target}"
        )
    if asof_close_mismatches:
        failures.append(
            f"selection as-of close propagation mismatches: {asof_close_mismatches}"
        )
    if asof_mkt_cap_million_mismatches:
        failures.append(
            "selection as-of MktCap(million yen) propagation mismatches: "
            f"{asof_mkt_cap_million_mismatches}"
        )
    if asof_market_cap_yen_mismatches:
        failures.append(
            "selection as-of market cap(yen) propagation mismatches: "
            f"{asof_market_cap_yen_mismatches}"
        )
    if asof_adjustment_factor_mismatches:
        failures.append(
            "selection as-of AdjFactor propagation mismatches: "
            f"{asof_adjustment_factor_mismatches}"
        )

    target_alias_mismatches = {
        JQ_MKT_CAP_MILLION_YEN_TARGET: numeric_mismatch_count(
            master[JQ_MKT_CAP_MILLION_YEN_TARGET],
            master[JQ_MKT_CAP_MILLION_YEN],
            atol=0.5,
        ),
        JQ_MARKET_CAP_YEN_TARGET: numeric_mismatch_count(
            master[JQ_MARKET_CAP_YEN_TARGET],
            master[JQ_MARKET_CAP_YEN],
            atol=0.5,
        ),
        JQ_EX_RIGHTS_TYPE_TARGET: numeric_mismatch_count(
            master[JQ_EX_RIGHTS_TYPE_TARGET],
            master[JQ_EX_RIGHTS_TYPE],
        ),
        JQ_ADJUSTMENT_FACTOR_TARGET: numeric_mismatch_count(
            master[JQ_ADJUSTMENT_FACTOR_TARGET],
            master[JQ_ADJUSTMENT_FACTOR],
            atol=1e-12,
        ),
    }
    target_alias_mismatch_total = sum(target_alias_mismatches.values())
    if target_alias_mismatch_total:
        failures.append(
            "target-day compatibility alias mismatches: "
            f"{target_alias_mismatches}"
        )

    if len(master) != len(archive):
        failures.append(f"master row count mismatch: master={len(master)} archive={len(archive)}")

    missing_master_keys = archive_set - master_set
    extra_master_keys = master_set - archive_set
    if missing_master_keys:
        failures.append(f"master missing archive keys: {len(missing_master_keys)}")
    if extra_master_keys:
        failures.append(f"master has non-archive keys: {len(extra_master_keys)}")

    minute_bar_coverage = pct(len(minute_in_archive), len(archive_set))
    minute_coverage = pct(len(logically_covered_minute_keys), len(archive_set))
    if unresolved_minute_keys:
        failures.append(
            "minute coverage has unresolved archive keys: "
            f"{sorted(unresolved_minute_keys)[:10]}"
        )
    if minute_coverage is None or minute_coverage < args.min_minute_coverage:
        failures.append(
            "logical minute coverage too low: "
            f"{len(logically_covered_minute_keys)}/{len(archive_set)} "
            f"({0 if minute_coverage is None else minute_coverage:.2%})"
        )

    if latest_archive_keys and latest_logically_covered_keys != latest_archive_keys:
        failures.append(
            "latest archive date has unresolved minute coverage: "
            f"{latest_archive_date}"
        )

    expected_daily = daily_work[
        pd.Series(
            list(zip(daily_work["_key_date"], daily_work["_key_ticker"])),
            index=daily_work.index,
        ).isin(archive_set)
    ]
    expected_master_mkt_cap = int(
        expected_daily[JQ_MKT_CAP_MILLION_YEN].notna().sum()
    )
    expected_master_ex_rights = int(
        expected_daily[JQ_EX_RIGHTS_TYPE].notna().sum()
    )
    master_mkt_cap_non_null = int(master[JQ_MKT_CAP_MILLION_YEN].notna().sum())
    master_ex_rights_non_null = int(master[JQ_EX_RIGHTS_TYPE].notna().sum())
    if master_mkt_cap_non_null != expected_master_mkt_cap:
        failures.append(
            "master MktCap propagation mismatch: "
            f"master={master_mkt_cap_non_null} daily={expected_master_mkt_cap}"
        )
    if master_ex_rights_non_null != expected_master_ex_rights:
        failures.append(
            "master ExRT propagation mismatch: "
            f"master={master_ex_rights_non_null} daily={expected_master_ex_rights}"
        )

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

    master_execution = pd.DataFrame(
        {
            "date": normalize_date(master["backtest_date"]),
            "ticker": master["ticker"].astype(str).str.strip(),
            "jq_close_execution_status": master[
                "jq_close_execution_status"
            ].astype(str),
        }
    )
    no_market_without_bars = no_market_trade_set - minute_set
    if no_market_without_bars:
        no_market_execution = pd.DataFrame(
            sorted(no_market_without_bars),
            columns=["date", "ticker"],
        )
        no_market_execution["expected_close_execution_status"] = "no_market_trade"
        minute_execution = pd.concat(
            [minute_execution, no_market_execution],
            ignore_index=True,
        )
    execution_check = master_execution.merge(
        minute_execution[
            ["date", "ticker", "expected_close_execution_status"]
        ],
        on=["date", "ticker"],
        how="left",
        validate="one_to_one",
    )
    execution_observed = execution_check["expected_close_execution_status"].notna()
    execution_mismatches = int(
        execution_check.loc[execution_observed, "expected_close_execution_status"]
        .ne(execution_check.loc[execution_observed, "jq_close_execution_status"])
        .sum()
    )
    if execution_mismatches:
        failures.append(
            f"close execution status mismatches: {execution_mismatches}"
        )
    master_key_tuples = pd.Series(
        list(zip(master_execution["date"], master_execution["ticker"])),
        index=master.index,
    )
    master_no_market = master_key_tuples.isin(no_market_trade_set)
    no_market_bar_count_mismatches = int(
        pd.to_numeric(master.loc[master_no_market, "jq_bar_count"], errors="coerce")
        .ne(0)
        .sum()
    )
    no_market_status_mismatches = int(
        master.loc[master_no_market, "jq_close_execution_status"]
        .ne("no_market_trade")
        .sum()
    )
    if no_market_bar_count_mismatches or no_market_status_mismatches:
        failures.append(
            "explicit no-market-trade state mismatches: "
            f"bar_count={no_market_bar_count_mismatches}, "
            f"status={no_market_status_mismatches}"
        )
    close_execution_counts = {
        str(key): int(value)
        for key, value in master["jq_close_execution_status"]
        .value_counts(dropna=False)
        .items()
    }

    if minute_set - archive_set:
        warnings.append(f"minute has non-archive keys: {len(minute_set - archive_set)}")

    daily_coverage = pct(len(daily_in_archive), len(archive_set))
    asof_daily_coverage = pct(asof_daily_rows, len(archive_asof))
    expected_asof_mkt_cap_non_null = int(
        pd.to_numeric(
            expected_asof["expected_market_cap_yen_asof"], errors="coerce"
        ).notna().sum()
    )
    master_asof_mkt_cap_non_null = int(
        pd.to_numeric(master[JQ_MARKET_CAP_YEN_ASOF], errors="coerce")
        .notna()
        .sum()
    )

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
            "bar_coverage_of_archive_keys": minute_bar_coverage,
            "no_market_trade_keys": int(len(no_market_trade_set)),
            "unresolved_archive_keys": int(len(unresolved_minute_keys)),
            "coverage_of_archive_keys": minute_coverage,
            "latest_archive_date_keys": int(len(latest_archive_keys)),
            "latest_archive_date_minute_keys": int(len(latest_minute_keys)),
            "latest_archive_date_logically_covered_keys": int(
                len(latest_logically_covered_keys)
            ),
        },
        "daily": {
            "path": str(args.daily_path),
            "sha256": file_sha256(args.daily_path),
            "rows": int(len(daily)),
            "keys": int(len(daily_set)),
            "duplicate_key_rows": daily_duplicate_keys,
            "keys_in_archive": int(len(daily_in_archive)),
            "coverage_of_archive_keys": daily_coverage,
            "latest_archive_date_daily_keys": int(len(latest_daily_keys)),
            "latest_archive_date_mkt_cap_non_null": latest_archive_daily_mkt_cap,
            "mkt_cap_million_yen_non_null": int(mkt_cap_million.notna().sum()),
            "market_cap_yen_non_null": int(market_cap_yen.notna().sum()),
            "mkt_cap_unit_mismatches": unit_mismatches,
            "ex_rights_non_null": int(ex_rights_type.notna().sum()),
            "invalid_ex_rights": invalid_ex_rights,
            "non_positive_adjustment_factor": invalid_adjustment_factor,
            "etf_200a_rows": etf_200a_rows,
            "etf_200a_mkt_cap_non_null": etf_200a_mkt_cap_non_null,
            "selection_asof_keys": int(len(archive_asof)),
            "selection_asof_keys_in_daily": asof_daily_rows,
            "selection_asof_coverage": asof_daily_coverage,
        },
        "calendar": {
            "path": str(args.calendar_path),
            "sha256": file_sha256(args.calendar_path),
            "trading_dates": int(len(trading_dates)),
        },
        "master": {
            "path": str(args.master_path),
            "sha256": file_sha256(args.master_path),
            "rows": int(len(master)),
            "keys": int(len(master_set)),
            "jq_buy_price_non_null": jq_buy_non_null,
            "jq_seg_1530_non_null": jq_seg_1530_non_null,
            "expected_jq_non_null_min": expected_master_jq,
            "jq_mkt_cap_non_null": master_mkt_cap_non_null,
            "expected_jq_mkt_cap_non_null": expected_master_mkt_cap,
            "jq_ex_rights_non_null": master_ex_rights_non_null,
            "expected_jq_ex_rights_non_null": expected_master_ex_rights,
            "jq_market_cap_asof_non_null": master_asof_mkt_cap_non_null,
            "expected_jq_market_cap_asof_non_null": expected_asof_mkt_cap_non_null,
            "selection_asof_date_mismatches": asof_date_mismatches,
            "selection_asof_not_before_target": asof_not_before_target,
            "selection_asof_close_mismatches": asof_close_mismatches,
            "selection_asof_mkt_cap_million_mismatches": (
                asof_mkt_cap_million_mismatches
            ),
            "selection_asof_market_cap_yen_mismatches": (
                asof_market_cap_yen_mismatches
            ),
            "selection_asof_adjustment_factor_mismatches": (
                asof_adjustment_factor_mismatches
            ),
            "target_alias_mismatches": target_alias_mismatches,
            "close_execution_status": close_execution_counts,
            "close_execution_status_mismatches": execution_mismatches,
            "no_market_trade_bar_count_mismatches": (
                no_market_bar_count_mismatches
            ),
            "no_market_trade_status_mismatches": no_market_status_mismatches,
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
