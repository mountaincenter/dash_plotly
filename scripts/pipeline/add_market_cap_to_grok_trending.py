#!/usr/bin/env python3
"""Attach official D-1 J-Quants market cap to ``grok_trending.parquet``.

The selection date stored in ``grok_trending.date`` is the next trading day.
Only the immediately preceding exchange trading day's official ``MktCap`` is
therefore eligible for recommendation and ML inference.  The protected Grok
archive is not read or written by this script.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_daily_fields import (
    JQ_ADJUSTMENT_FACTOR,
    JQ_EX_RIGHTS_TYPE,
    JQ_MARKET_CAP_YEN,
    JQ_MKT_CAP_MILLION_YEN,
    normalize_jquants_daily_fields,
)
from scripts.lib.grok_jquants_backtest import validate_selection_market_cap
from scripts.lib.price_limit import (
    calc_max_cost_100,
    calc_price_limit,
    calc_upper_limit_price,
)


GROK_TRENDING_FILE = Path(
    os.getenv(
        "GROK_TRENDING_FILE",
        ROOT / "data" / "parquet" / "grok_trending.parquet",
    )
)
DAILY_FEATURES_FILE = Path(
    os.getenv(
        "JQUANTS_WATCH_DAILY_FEATURES_FILE",
        ROOT / "data" / "parquet" / "jquants" / "watch_daily_features.parquet",
    )
)
CALENDAR_FILE = Path(
    os.getenv(
        "TRADING_CALENDAR_FILE",
        ROOT / "data" / "parquet" / "calendar.parquet",
    )
)

JQ_MARKET_CAP_ASOF_DATE = "jq_market_cap_asof_date"
JQ_MKT_CAP_MILLION_YEN_ASOF = "jq_mkt_cap_million_yen_asof"
JQ_MARKET_CAP_YEN_ASOF = "jq_market_cap_yen_asof"
JQ_EX_RIGHTS_TYPE_ASOF = "jq_ex_rights_type_asof"
JQ_ADJUSTMENT_FACTOR_ASOF = "jq_adjustment_factor_asof"
JQ_DAILY_SOURCE_ASOF = "jq_daily_source_asof"
JQ_DAILY_FETCHED_AT_ASOF = "jq_daily_fetched_at_asof"
MARKET_CAP_SOURCE = "jquants_eq_daily_mktcap_d_minus_1"


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _normalize_ticker(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    if text.endswith(".T"):
        return text
    if len(text) == 5 and text.endswith("0"):
        text = text[:-1]
    return f"{text}.T"


def attach_official_market_cap_asof(
    grok: pd.DataFrame,
    daily_features: pd.DataFrame,
    calendar: pd.DataFrame,
) -> pd.DataFrame:
    """Attach exact previous-trading-day official fields without look-ahead.

    A matched source row with nullable ``MktCap`` is valid (for example an
    ETF/ETN).  A missing source row is not the same thing and fails loudly.
    """
    _require_columns(grok, {"date", "ticker"}, "grok_trending")
    _require_columns(
        daily_features,
        {
            "trading_date",
            "ticker",
            JQ_MKT_CAP_MILLION_YEN,
            JQ_MARKET_CAP_YEN,
            JQ_EX_RIGHTS_TYPE,
            JQ_ADJUSTMENT_FACTOR,
        },
        "watch_daily_features",
    )
    _require_columns(calendar, {"date"}, "calendar")

    left = grok.copy()
    left["_pipeline_row_order"] = np.arange(len(left), dtype=np.int64)
    left["_target_date"] = pd.to_datetime(
        left["date"], errors="coerce"
    ).dt.normalize()
    if left["_target_date"].isna().any():
        raise ValueError("grok_trending contains invalid date values")
    left["ticker"] = left["ticker"].map(_normalize_ticker)
    if left["ticker"].eq("").any():
        raise ValueError("grok_trending contains invalid ticker values")

    trading_dates = pd.DatetimeIndex(
        pd.to_datetime(calendar["date"], errors="coerce").dropna().unique()
    ).normalize().sort_values()
    if trading_dates.empty:
        raise ValueError("calendar contains no trading dates")

    previous_by_target: dict[pd.Timestamp, pd.Timestamp] = {}
    for target_date in left["_target_date"].drop_duplicates():
        previous = trading_dates[trading_dates < target_date]
        if previous.empty:
            raise ValueError(
                f"calendar has no trading date before target {target_date.date()}"
            )
        previous_by_target[target_date] = previous[-1]
    left["_expected_asof_date"] = left["_target_date"].map(previous_by_target)

    right = normalize_jquants_daily_fields(daily_features)
    right["ticker"] = right["ticker"].map(_normalize_ticker)
    right["_expected_asof_date"] = pd.to_datetime(
        right["trading_date"], errors="coerce"
    ).dt.normalize()
    right = right.dropna(subset=["_expected_asof_date"])
    right = right[right["ticker"].ne("")]
    duplicate_keys = right.duplicated(
        ["ticker", "_expected_asof_date"], keep=False
    )
    if duplicate_keys.any():
        examples = (
            right.loc[duplicate_keys, ["ticker", "trading_date"]]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(f"watch_daily_features has duplicate keys: {examples}")

    source_columns = {
        JQ_MKT_CAP_MILLION_YEN: "_source_mkt_cap_million_yen",
        JQ_MARKET_CAP_YEN: "_source_market_cap_yen",
        JQ_EX_RIGHTS_TYPE: "_source_ex_rights_type",
        JQ_ADJUSTMENT_FACTOR: "_source_adjustment_factor",
        "source": "_source_name",
        "fetched_at": "_source_fetched_at",
    }
    available_source_columns = {
        column: renamed
        for column, renamed in source_columns.items()
        if column in right.columns
    }
    right = right[
        ["ticker", "_expected_asof_date", *available_source_columns]
    ].rename(columns=available_source_columns)

    merged = left.merge(
        right,
        on=["ticker", "_expected_asof_date"],
        how="left",
        sort=False,
        validate="many_to_one",
        indicator="_daily_merge",
    )
    missing_rows = merged["_daily_merge"].ne("both")
    if missing_rows.any():
        examples = (
            merged.loc[
                missing_rows,
                ["date", "ticker", "_expected_asof_date"],
            ]
            .head(20)
            .assign(
                _expected_asof_date=lambda frame: frame[
                    "_expected_asof_date"
                ].dt.strftime("%Y-%m-%d")
            )
            .to_dict("records")
        )
        raise ValueError(
            "official J-Quants D-1 source rows are missing; "
            f"missing={int(missing_rows.sum())}, examples={examples}"
        )

    cap_million = pd.to_numeric(
        merged["_source_mkt_cap_million_yen"], errors="coerce"
    ).astype("Float64")
    cap_yen = pd.to_numeric(
        merged["_source_market_cap_yen"], errors="coerce"
    ).astype("Float64")
    comparable = cap_million.notna() & cap_yen.notna()
    unit_mismatch = comparable & ~np.isclose(
        cap_yen.astype(float),
        cap_million.astype(float) * 1_000_000.0,
        rtol=0.0,
        atol=0.5,
    )
    if unit_mismatch.any():
        raise ValueError(
            "J-Quants market-cap unit conversion mismatch: "
            f"{int(unit_mismatch.sum())} rows"
        )

    merged[JQ_MARKET_CAP_ASOF_DATE] = merged[
        "_expected_asof_date"
    ].dt.strftime("%Y-%m-%d")
    merged[JQ_MKT_CAP_MILLION_YEN_ASOF] = cap_million
    merged[JQ_MARKET_CAP_YEN_ASOF] = cap_yen
    merged[JQ_EX_RIGHTS_TYPE_ASOF] = pd.to_numeric(
        merged["_source_ex_rights_type"], errors="coerce"
    ).astype("Int64")
    merged[JQ_ADJUSTMENT_FACTOR_ASOF] = pd.to_numeric(
        merged["_source_adjustment_factor"], errors="coerce"
    ).astype("Float64")
    merged[JQ_DAILY_SOURCE_ASOF] = merged.get("_source_name", pd.NA)
    merged[JQ_DAILY_FETCHED_AT_ASOF] = merged.get(
        "_source_fetched_at", pd.NA
    )
    merged["market_cap_source"] = MARKET_CAP_SOURCE
    # Backward-compatible ML feature name, now backed by official D-1 MktCap.
    merged["market_cap"] = cap_yen

    if not (
        pd.to_datetime(merged[JQ_MARKET_CAP_ASOF_DATE])
        < merged["_target_date"]
    ).all():
        raise ValueError("market-cap as-of date is not strictly before target date")

    helper_columns = [
        "_pipeline_row_order",
        "_target_date",
        "_expected_asof_date",
        "_daily_merge",
        *available_source_columns.values(),
    ]
    merged = merged.sort_values("_pipeline_row_order")
    return merged.drop(columns=helper_columns, errors="ignore").reset_index(drop=True)


def _write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    """Write a replaceable pipeline artifact without exposing partial bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        frame.to_parquet(temporary_path, index=False)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _add_price_limit_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "Close" not in out.columns:
        out["price_limit"] = None
        out["limit_price_upper"] = None
        out["max_cost_100"] = None
        return out
    close = pd.to_numeric(out["Close"], errors="coerce")
    out["price_limit"] = close.map(
        lambda value: calc_price_limit(value)
        if pd.notna(value) and value > 0
        else None
    )
    out["limit_price_upper"] = close.map(
        lambda value: calc_upper_limit_price(value)
        if pd.notna(value) and value > 0
        else None
    )
    out["max_cost_100"] = close.map(
        lambda value: calc_max_cost_100(value)
        if pd.notna(value) and value > 0
        else None
    )
    return out


def main() -> int:
    print("=== Attach official J-Quants D-1 market cap ===")
    for label, path in [
        ("grok_trending", GROK_TRENDING_FILE),
        ("watch_daily_features", DAILY_FEATURES_FILE),
        ("calendar", CALENDAR_FILE),
    ]:
        if not path.exists():
            print(f"[ERROR] {label} not found: {path}")
            return 1

    grok = pd.read_parquet(GROK_TRENDING_FILE)
    daily_features = pd.read_parquet(DAILY_FEATURES_FILE)
    calendar = pd.read_parquet(CALENDAR_FILE)
    print(
        f"inputs: grok={len(grok):,}, daily_features={len(daily_features):,}, "
        f"calendar={len(calendar):,}"
    )

    enriched = attach_official_market_cap_asof(grok, daily_features, calendar)
    validate_selection_market_cap(
        enriched,
        pd.to_datetime(enriched["date"], errors="raise").iloc[0],
        calendar,
    )
    enriched = _add_price_limit_columns(enriched)
    _write_parquet_atomic(enriched, GROK_TRENDING_FILE)

    cap_count = int(enriched["market_cap"].notna().sum())
    null_count = len(enriched) - cap_count
    source_dates = sorted(enriched[JQ_MARKET_CAP_ASOF_DATE].unique().tolist())
    print(
        f"[OK] official D-1 source rows={len(enriched):,}/{len(enriched):,}, "
        f"MktCap non-null={cap_count:,}, nullable={null_count:,}, "
        f"asof_dates={source_dates}"
    )
    print(f"[OK] atomically saved: {GROK_TRENDING_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
