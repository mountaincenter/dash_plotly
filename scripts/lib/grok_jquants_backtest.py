"""J-Quants based price and execution rules for the Grok archive."""

from __future__ import annotations

import warnings
from datetime import date, datetime, time
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from scripts.lib.jquants_daily_fields import (
    DAILY_TRADE_STATUS_NO_MARKET_TRADE,
    DAILY_TRADE_STATUS_TRADED,
    JQ_DAILY_TRADE_STATUS,
)


PRICE_ATOL = 0.011
VOLUME_ATOL = 0.5
MARKET_CAP_SOURCE = "jquants_eq_daily_mktcap_d_minus_1"

MARKET_CAP_PROVENANCE_COLUMN_ORDER = (
    "market_cap_source",
    "jq_market_cap_asof_date",
    "jq_mkt_cap_million_yen_asof",
    "jq_market_cap_yen_asof",
    "jq_ex_rights_type_asof",
    "jq_adjustment_factor_asof",
    "jq_daily_source_asof",
    "jq_daily_fetched_at_asof",
)
MARKET_CAP_PROVENANCE_COLUMNS = set(MARKET_CAP_PROVENANCE_COLUMN_ORDER)

TARGET_DAILY_PROVENANCE_COLUMN_ORDER = (
    "jq_daily_target_date",
    "jq_daily_trade_status_target",
    "jq_mkt_cap_million_yen_target",
    "jq_market_cap_yen_target",
    "jq_ex_rights_type_target",
    "jq_adjustment_factor_target",
    "jq_daily_source_target",
    "jq_daily_fetched_at_target",
)
TARGET_DAILY_PROVENANCE_COLUMNS = set(TARGET_DAILY_PROVENANCE_COLUMN_ORDER)

SEGMENT_TARGETS = {
    "seg_0930": time(9, 30),
    "seg_1000": time(10, 0),
    "seg_1030": time(10, 30),
    "seg_1100": time(11, 0),
    "seg_1130": time(11, 30),
    "seg_1300": time(13, 0),
    "seg_1330": time(13, 30),
    "seg_1400": time(14, 0),
    "seg_1430": time(14, 30),
    "seg_1500": time(15, 0),
    "seg_1530": time(15, 30),
}


class JQuantsBacktestDataError(RuntimeError):
    """Raised when official data cannot safely produce a complete archive row."""


def _target_date(value: date | datetime | pd.Timestamp | str) -> date:
    parsed = pd.Timestamp(value)
    if pd.isna(parsed):
        raise JQuantsBacktestDataError(f"Invalid target date: {value!r}")
    return parsed.date()


def validate_selection_asof(
    selection: pd.DataFrame,
    target_date: date | datetime | pd.Timestamp | str,
) -> None:
    """Require proof that selection-time price inputs predate the target day."""
    required = {"date", "ticker", "price_asof_date", "price_source_date"}
    missing = sorted(required - set(selection.columns))
    if missing:
        raise JQuantsBacktestDataError(
            f"Selection data is missing as-of proof columns: {missing}"
        )
    target = pd.Timestamp(_target_date(target_date))
    selection_targets = pd.to_datetime(selection["date"], errors="raise").dt.normalize()
    if not selection_targets.eq(target).all():
        raise JQuantsBacktestDataError(
            "Selection target date does not match the backtest date"
        )
    for column in ["price_asof_date", "price_source_date"]:
        values = pd.to_datetime(selection[column], errors="raise").dt.normalize()
        if values.isna().any() or not values.lt(target).all():
            invalid = selection.loc[~values.lt(target), ["ticker", column]].to_dict(
                "records"
            )
            raise JQuantsBacktestDataError(
                f"Selection contains target-day/future {column}: {invalid[:5]}"
            )


def validate_selection_market_cap(
    selection: pd.DataFrame,
    target_date: date | datetime | pd.Timestamp | str,
    calendar: pd.DataFrame,
) -> None:
    """Prove that every saved market cap is official J-Quants D-1 data."""
    required = {
        "date",
        "ticker",
        "market_cap",
        *MARKET_CAP_PROVENANCE_COLUMNS,
    }
    missing = sorted(required - set(selection.columns))
    if missing:
        raise JQuantsBacktestDataError(
            f"Selection is missing official market-cap proof columns: {missing}"
        )
    if "date" not in calendar.columns:
        raise JQuantsBacktestDataError("Trading calendar has no date column")

    target = pd.Timestamp(_target_date(target_date))
    trading_dates = pd.DatetimeIndex(
        pd.to_datetime(calendar["date"], errors="coerce").dropna().unique()
    ).normalize()
    previous = trading_dates[trading_dates < target]
    if previous.empty:
        raise JQuantsBacktestDataError(
            f"Trading calendar has no date before {target.date()}"
        )
    expected_asof = previous.max()

    selection_targets = pd.to_datetime(
        selection["date"], errors="raise"
    ).dt.normalize()
    if not selection_targets.eq(target).all():
        raise JQuantsBacktestDataError(
            "Selection target date does not match market-cap validation date"
        )
    asof = pd.to_datetime(
        selection["jq_market_cap_asof_date"], errors="raise"
    ).dt.normalize()
    if asof.isna().any() or not asof.eq(expected_asof).all():
        invalid = selection.loc[
            ~asof.eq(expected_asof), ["ticker", "jq_market_cap_asof_date"]
        ].head(10).to_dict("records")
        raise JQuantsBacktestDataError(
            "Selection market cap is not from the immediately preceding trading "
            f"day {expected_asof.date()}: {invalid}"
        )
    if not selection["market_cap_source"].eq(MARKET_CAP_SOURCE).all():
        raise JQuantsBacktestDataError(
            "Selection contains a non-official market-cap source"
        )
    if not selection["jq_daily_source_asof"].eq("jquants_api_v2").all():
        raise JQuantsBacktestDataError(
            "Selection D-1 market cap was not fetched from the direct J-Quants API"
        )
    fetched_at = pd.to_datetime(
        selection["jq_daily_fetched_at_asof"], errors="coerce", utc=True
    )
    if fetched_at.isna().any():
        raise JQuantsBacktestDataError(
            "Selection D-1 market-cap provenance has no valid fetched_at"
        )

    cap_million = pd.to_numeric(
        selection["jq_mkt_cap_million_yen_asof"], errors="coerce"
    ).astype("float64")
    cap_yen = pd.to_numeric(
        selection["jq_market_cap_yen_asof"], errors="coerce"
    ).astype("float64")
    saved_cap = pd.to_numeric(selection["market_cap"], errors="coerce").astype(
        "float64"
    )
    source_null_mismatch = cap_million.isna().ne(cap_yen.isna())
    unit_comparable = cap_million.notna() & cap_yen.notna()
    unit_mismatch = unit_comparable & ~np.isclose(
        cap_yen,
        cap_million * 1_000_000.0,
        rtol=0.0,
        atol=0.5,
    )
    saved_null_mismatch = saved_cap.isna().ne(cap_yen.isna())
    saved_comparable = saved_cap.notna() & cap_yen.notna()
    saved_value_mismatch = saved_comparable & ~np.isclose(
        saved_cap,
        cap_yen,
        rtol=0.0,
        atol=0.5,
    )
    if (
        source_null_mismatch.any()
        or unit_mismatch.any()
        or saved_null_mismatch.any()
        or saved_value_mismatch.any()
    ):
        raise JQuantsBacktestDataError(
            "Selection market_cap does not exactly match official J-Quants D-1 "
            "MktCap in yen"
        )

    adjustment = pd.to_numeric(
        selection["jq_adjustment_factor_asof"], errors="coerce"
    )
    if adjustment.isna().any() or adjustment.le(0).any():
        raise JQuantsBacktestDataError(
            "Selection has missing or non-positive D-1 AdjFactor"
        )
    ex_rights = pd.to_numeric(
        selection["jq_ex_rights_type_asof"], errors="coerce"
    )
    invalid_ex_rights = ex_rights.notna() & (
        ex_rights.mod(1).ne(0) | ~ex_rights.isin([1, 2, 3])
    )
    if invalid_ex_rights.any():
        raise JQuantsBacktestDataError("Selection has invalid D-1 ExRT values")


def validate_target_daily_corporate_actions(
    selection: pd.DataFrame,
    target_date: date | datetime | pd.Timestamp | str,
    daily_features: pd.DataFrame,
) -> None:
    """Require target-day ExRT/AdjFactor coverage for every selected ticker."""
    required_selection = {"ticker"}
    required_daily = {
        "trading_date",
        "ticker",
        JQ_DAILY_TRADE_STATUS,
        "jq_mkt_cap_million_yen",
        "jq_market_cap_yen",
        "jq_ex_rights_type",
        "jq_adjustment_factor",
        "source",
        "fetched_at",
    }
    missing_selection = sorted(required_selection - set(selection.columns))
    missing_daily = sorted(required_daily - set(daily_features.columns))
    if missing_selection:
        raise JQuantsBacktestDataError(
            f"Selection is missing target-day QC columns: {missing_selection}"
        )
    if missing_daily:
        raise JQuantsBacktestDataError(
            f"J-Quants daily sidecar is missing QC columns: {missing_daily}"
        )

    target = pd.Timestamp(_target_date(target_date))
    selected = selection["ticker"].astype(str).str.strip()
    if selected.eq("").any() or selected.duplicated().any():
        raise JQuantsBacktestDataError(
            "Selection tickers are empty or duplicated before target-day QC"
        )

    features = daily_features.copy()
    feature_dates = pd.to_datetime(
        features["trading_date"], errors="coerce"
    ).dt.normalize()
    features["_target_date"] = feature_dates
    features["ticker"] = features["ticker"].astype(str).str.strip()
    target_rows = features[
        features["_target_date"].eq(target)
        & features["ticker"].isin(set(selected))
    ].copy()
    if target_rows.duplicated(["ticker", "_target_date"]).any():
        raise JQuantsBacktestDataError(
            "J-Quants target-day sidecar contains duplicate ticker-date keys"
        )
    missing_tickers = sorted(set(selected) - set(target_rows["ticker"]))
    if missing_tickers:
        raise JQuantsBacktestDataError(
            "J-Quants target-day corporate-action coverage is incomplete: "
            f"{missing_tickers}"
        )
    if not target_rows["source"].eq("jquants_api_v2").all():
        raise JQuantsBacktestDataError(
            "J-Quants target-day sidecar contains an unrecognized source"
        )
    fetched_at = pd.to_datetime(target_rows["fetched_at"], errors="coerce", utc=True)
    if fetched_at.isna().any():
        raise JQuantsBacktestDataError(
            "J-Quants target-day sidecar contains an invalid fetched_at"
        )

    valid_trade_statuses = {
        DAILY_TRADE_STATUS_TRADED,
        DAILY_TRADE_STATUS_NO_MARKET_TRADE,
    }
    invalid_trade_status = ~target_rows[JQ_DAILY_TRADE_STATUS].isin(
        valid_trade_statuses
    )
    if invalid_trade_status.any():
        invalid = target_rows.loc[
            invalid_trade_status, ["ticker", JQ_DAILY_TRADE_STATUS]
        ].to_dict("records")
        raise JQuantsBacktestDataError(
            f"J-Quants target-day trade status is invalid: {invalid}"
        )

    million_yen = pd.to_numeric(
        target_rows["jq_mkt_cap_million_yen"], errors="coerce"
    )
    yen = pd.to_numeric(target_rows["jq_market_cap_yen"], errors="coerce")
    cap_null_mismatch = million_yen.isna().ne(yen.isna())
    cap_comparable = million_yen.notna() & yen.notna()
    cap_unit_mismatch = cap_comparable & ~pd.Series(
        np.isclose(
            million_yen.fillna(0).astype(float) * 1_000_000.0,
            yen.fillna(0).astype(float),
            rtol=0.0,
            atol=0.5,
        ),
        index=target_rows.index,
    )
    if cap_null_mismatch.any() or cap_unit_mismatch.any():
        raise JQuantsBacktestDataError(
            "J-Quants target-day MktCap units are inconsistent"
        )

    adjustment = pd.to_numeric(
        target_rows["jq_adjustment_factor"], errors="coerce"
    )
    if adjustment.isna().any() or adjustment.le(0).any():
        invalid = target_rows.loc[
            adjustment.isna() | adjustment.le(0),
            ["ticker", "jq_adjustment_factor"],
        ].to_dict("records")
        raise JQuantsBacktestDataError(
            f"Target-day AdjFactor is missing or non-positive: {invalid}"
        )

    raw_ex_rights = target_rows["jq_ex_rights_type"]
    ex_rights = pd.to_numeric(raw_ex_rights, errors="coerce")
    invalid_parse = raw_ex_rights.notna() & ex_rights.isna()
    invalid_domain = ex_rights.notna() & (
        ex_rights.mod(1).ne(0) | ~ex_rights.isin([1, 2, 3])
    )
    if invalid_parse.any() or invalid_domain.any():
        raise JQuantsBacktestDataError("Target-day ExRT contains invalid values")


def attach_target_daily_provenance(
    rows: pd.DataFrame,
    target_date: date | datetime | pd.Timestamp | str,
    daily_features: pd.DataFrame,
) -> pd.DataFrame:
    """Attach target-day MktCap/ExRT/AdjFactor evidence to a derived artifact."""
    validate_target_daily_corporate_actions(rows, target_date, daily_features)
    target = pd.Timestamp(_target_date(target_date)).normalize()
    features = daily_features.copy()
    features["_target_date"] = pd.to_datetime(
        features["trading_date"], errors="raise"
    ).dt.normalize()
    features["ticker"] = features["ticker"].astype(str).str.strip()
    selected = set(rows["ticker"].astype(str).str.strip())
    features = features[
        features["_target_date"].eq(target)
        & features["ticker"].isin(selected)
    ].copy()
    features["jq_daily_target_date"] = features["_target_date"].dt.strftime(
        "%Y-%m-%d"
    )
    proof = features[
        [
            "ticker",
            "jq_daily_target_date",
            JQ_DAILY_TRADE_STATUS,
            "jq_mkt_cap_million_yen",
            "jq_market_cap_yen",
            "jq_ex_rights_type",
            "jq_adjustment_factor",
            "source",
            "fetched_at",
        ]
    ].rename(
        columns={
            JQ_DAILY_TRADE_STATUS: "jq_daily_trade_status_target",
            "jq_mkt_cap_million_yen": "jq_mkt_cap_million_yen_target",
            "jq_market_cap_yen": "jq_market_cap_yen_target",
            "jq_ex_rights_type": "jq_ex_rights_type_target",
            "jq_adjustment_factor": "jq_adjustment_factor_target",
            "source": "jq_daily_source_target",
            "fetched_at": "jq_daily_fetched_at_target",
        }
    )
    enriched = rows.copy()
    enriched["ticker"] = enriched["ticker"].astype(str).str.strip()
    enriched = enriched.merge(proof, on="ticker", how="left", validate="one_to_one")
    missing = [
        column
        for column in TARGET_DAILY_PROVENANCE_COLUMN_ORDER
        if column not in enriched.columns
        or (
            column
            not in {
                "jq_mkt_cap_million_yen_target",
                "jq_market_cap_yen_target",
                "jq_ex_rights_type_target",
            }
            and enriched[column].isna().any()
        )
    ]
    if missing:
        raise JQuantsBacktestDataError(
            f"Derived target-day provenance is incomplete: {missing}"
        )
    return enriched


def build_derived_backtest_rows(
    archive: pd.DataFrame,
    new_rows: pd.DataFrame,
    target_date: date | datetime | pd.Timestamp | str,
    daily_features: pd.DataFrame,
) -> pd.DataFrame:
    """Build canonical-compatible daily rows without mutating the canonical archive."""
    enriched = attach_target_daily_provenance(
        new_rows,
        target_date,
        daily_features,
    )
    canonical_rows = align_rows_to_archive_schema(
        archive,
        enriched,
        allowed_extra_columns=(
            MARKET_CAP_PROVENANCE_COLUMNS | TARGET_DAILY_PROVENANCE_COLUMNS
        ),
    )
    proof_order = [
        *MARKET_CAP_PROVENANCE_COLUMN_ORDER,
        *TARGET_DAILY_PROVENANCE_COLUMN_ORDER,
    ]
    proof_order = [column for column in proof_order if column not in canonical_rows]
    derived = pd.concat(
        [
            canonical_rows.reset_index(drop=True),
            enriched[proof_order].reset_index(drop=True),
        ],
        axis=1,
    )
    expected_columns = archive.columns.tolist() + proof_order
    if derived.columns.tolist() != expected_columns:
        raise JQuantsBacktestDataError("Derived backtest column order changed")
    return derived


def validate_backtest_execution_states(frame: pd.DataFrame) -> None:
    """Reject invented values and inconsistent traded/no-market states."""
    required = {
        "data_source",
        "phase1_mark_status",
        "close_execution_status",
        "buy_price",
        "daily_close",
        "jquants_bar_count",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise JQuantsBacktestDataError(
            f"Backtest execution-state columns are missing: {missing}"
        )

    phase_no_market = frame["phase1_mark_status"].eq("no_market_trade")
    close_no_market = frame["close_execution_status"].eq("no_market_trade")
    if not phase_no_market.eq(close_no_market).all():
        raise JQuantsBacktestDataError(
            "Backtest no-market statuses are not paired"
        )
    no_market = phase_no_market & close_no_market
    traded = ~no_market

    valid_phase = frame["phase1_mark_status"].isin(
        ["available", "no_morning_price", "no_market_trade"]
    )
    valid_close = frame["close_execution_status"].isin(
        ["executable", "mark_only_no_round_trip", "no_market_trade"]
    )
    if not valid_phase.all() or not valid_close.all():
        raise JQuantsBacktestDataError("Backtest has an unknown execution status")
    if not frame.loc[traded, "data_source"].eq("jquants_1m").all():
        raise JQuantsBacktestDataError("Traded row has an invalid data source")
    if frame.loc[traded, ["buy_price", "daily_close"]].isna().any().any():
        raise JQuantsBacktestDataError("Traded row lacks buy/daily close values")
    if not frame.loc[no_market, "data_source"].eq(
        "jquants_no_market_trade"
    ).all():
        raise JQuantsBacktestDataError(
            "Official no-market row has an invalid data source"
        )
    if (
        pd.to_numeric(frame.loc[no_market, "jquants_bar_count"], errors="coerce")
        .fillna(-1)
        .ne(0)
        .any()
    ):
        raise JQuantsBacktestDataError(
            "Official no-market row must have zero J-Quants bars"
        )

    null_columns = [
        "buy_price",
        "sell_price",
        "daily_close",
        "high",
        "low",
        "volume",
        "Close",
        "Volume",
        "Value",
        "phase1_return",
        "phase1_win",
        "profit_per_100_shares_phase1",
        "phase2_return",
        "phase2_win",
        "profit_per_100_shares_phase2",
        *SEGMENT_TARGETS,
        "profit_per_100_shares_morning_early",
        "profit_per_100_shares_afternoon_early",
    ]
    for threshold in ["1pct", "2pct", "3pct"]:
        null_columns.extend(
            [
                f"phase3_{threshold}_return",
                f"phase3_{threshold}_win",
                f"profit_per_100_shares_phase3_{threshold}",
            ]
        )
    missing_null_columns = sorted(set(null_columns) - set(frame.columns))
    if missing_null_columns:
        raise JQuantsBacktestDataError(
            "Backtest price/P&L state columns are missing: "
            f"{missing_null_columns}"
        )
    if frame.loc[no_market, null_columns].notna().any().any():
        raise JQuantsBacktestDataError(
            "Official no-market row contains invented price or P&L values"
        )
    exit_columns = [
        f"phase3_{threshold}_exit_reason"
        for threshold in ["1pct", "2pct", "3pct"]
    ]
    if not frame.loc[no_market, exit_columns].eq("no_market_trade").all().all():
        raise JQuantsBacktestDataError(
            "Official no-market row lacks explicit Phase3 no-trade reasons"
        )


def align_rows_to_archive_schema(
    archive: pd.DataFrame,
    new_rows: pd.DataFrame,
    *,
    allowed_extra_columns: set[str] | None = None,
) -> pd.DataFrame:
    """Return target rows in the exact canonical schema, rejecting silent drift."""
    if archive.columns.empty:
        raise JQuantsBacktestDataError("Canonical archive has no schema")
    required = {"backtest_date", "ticker", "market_cap"}
    if not required.issubset(archive.columns) or not required.issubset(new_rows.columns):
        raise JQuantsBacktestDataError(
            "Canonical archive and new rows must contain key/market_cap columns"
        )
    allowed = allowed_extra_columns or set()
    extra = set(new_rows.columns) - set(archive.columns)
    unexpected = sorted(extra - allowed)
    if unexpected:
        raise JQuantsBacktestDataError(
            f"New rows would expand the canonical archive schema: {unexpected}"
        )

    aligned = new_rows.drop(columns=sorted(extra), errors="ignore").copy()
    for column in archive.columns:
        if column not in aligned.columns:
            dtype = archive[column].dtype
            if pd.api.types.is_float_dtype(dtype) or pd.api.types.is_complex_dtype(
                dtype
            ):
                missing_value = np.nan
            elif pd.api.types.is_datetime64_any_dtype(
                dtype
            ) or pd.api.types.is_timedelta64_dtype(dtype):
                missing_value = pd.NaT
            else:
                missing_value = pd.NA
            try:
                aligned[column] = pd.Series(
                    missing_value,
                    index=aligned.index,
                    dtype=dtype,
                )
            except (TypeError, ValueError) as error:
                raise JQuantsBacktestDataError(
                    "Target rows are missing a canonical column whose dtype "
                    "cannot represent a missing value: "
                    f"column={column}, dtype={dtype}"
                ) from error
    aligned = aligned.loc[:, archive.columns]
    for column, dtype in archive.dtypes.items():
        try:
            aligned[column] = aligned[column].astype(dtype)
        except (TypeError, ValueError) as error:
            raise JQuantsBacktestDataError(
                "Target rows cannot preserve canonical pandas dtype: "
                f"column={column}, expected={dtype}, actual={aligned[column].dtype}"
            ) from error
    if aligned.columns.tolist() != archive.columns.tolist():
        raise JQuantsBacktestDataError("Canonical archive column order changed")
    return aligned


def assert_archive_schema_unchanged(
    archive: pd.DataFrame,
    candidate: pd.DataFrame,
) -> None:
    """Reject column-order or pandas dtype changes in the canonical artifact."""
    if archive.columns.tolist() != candidate.columns.tolist():
        raise JQuantsBacktestDataError("Canonical archive column order changed")
    differences = [
        {
            "column": column,
            "source": str(archive[column].dtype),
            "candidate": str(candidate[column].dtype),
        }
        for column in archive.columns
        if archive[column].dtype != candidate[column].dtype
    ]
    if differences:
        raise JQuantsBacktestDataError(
            f"Canonical archive pandas dtype changed: {differences}"
        )


def assert_parquet_schema_unchanged(source_path: Any, candidate_path: Any) -> None:
    """Reject physical Arrow schema or pandas metadata drift after serialization."""
    source_schema = pq.read_schema(source_path)
    candidate_schema = pq.read_schema(candidate_path)
    if not source_schema.equals(candidate_schema, check_metadata=True):
        raise JQuantsBacktestDataError(
            "Canonical archive Parquet/Arrow schema or metadata changed"
        )


def normalize_minute_bars(
    minute: pd.DataFrame,
    ticker: str,
    target_date: date | datetime | pd.Timestamp | str,
) -> pd.DataFrame:
    """Return one ticker-day of J-Quants minute bars in archive column form."""
    required = {
        "ticker",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
    }
    missing = sorted(required - set(minute.columns))
    if missing:
        raise JQuantsBacktestDataError(
            f"J-Quants minute file is missing columns: {missing}"
        )

    day = _target_date(target_date)
    datetimes = pd.to_datetime(minute["datetime"], errors="coerce")
    mask = minute["ticker"].astype(str).eq(str(ticker)) & datetimes.dt.date.eq(day)
    bars = minute.loc[mask].copy()
    if bars.empty:
        raise JQuantsBacktestDataError(
            f"{ticker}: no J-Quants minute bars for {day.isoformat()}"
        )

    bars["datetime"] = pd.to_datetime(bars["datetime"], errors="raise")
    bars = bars.sort_values("datetime")
    if bars["datetime"].duplicated().any():
        duplicates = bars.loc[bars["datetime"].duplicated(), "datetime"].tolist()
        raise JQuantsBacktestDataError(
            f"{ticker}: duplicate J-Quants minute timestamps: {duplicates[:3]}"
        )

    rename = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "value": "Value",
    }
    bars = bars.rename(columns=rename)
    numeric = list(rename.values())
    for column in numeric:
        bars[column] = pd.to_numeric(bars[column], errors="coerce")

    if bars[numeric].isna().any().any():
        bad_columns = bars[numeric].columns[bars[numeric].isna().any()].tolist()
        raise JQuantsBacktestDataError(
            f"{ticker}: null/non-numeric J-Quants minute values in {bad_columns}"
        )
    if (bars[["Open", "High", "Low", "Close"]] <= 0).any().any():
        raise JQuantsBacktestDataError(f"{ticker}: non-positive minute price")
    if (bars[["Volume", "Value"]] < 0).any().any():
        raise JQuantsBacktestDataError(f"{ticker}: negative minute volume/value")

    invalid_ohlc = (
        bars["High"].lt(bars[["Open", "Close"]].max(axis=1))
        | bars["Low"].gt(bars[["Open", "Close"]].min(axis=1))
        | bars["High"].lt(bars["Low"])
    )
    if invalid_ohlc.any():
        raise JQuantsBacktestDataError(
            f"{ticker}: internally inconsistent minute OHLC"
        )

    return bars.set_index("datetime")


def normalize_daily_prices(daily: pd.DataFrame) -> pd.DataFrame:
    """Normalize the pipeline's J-Quants daily file for strict lookups."""
    required = ["date", "ticker", "Open", "High", "Low", "Close", "Volume"]
    missing = sorted(set(required) - set(daily.columns))
    if missing:
        raise JQuantsBacktestDataError(
            f"J-Quants daily file is missing columns: {missing}"
        )

    result = daily[required].copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result["ticker"] = result["ticker"].astype(str)
    numeric = ["Open", "High", "Low", "Close", "Volume"]
    for column in numeric:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(subset=["date", "ticker"])
    if result[["ticker", "date"]].duplicated().any():
        raise JQuantsBacktestDataError(
            "J-Quants daily file contains duplicate ticker-date keys"
        )
    return result.sort_values(["ticker", "date"]).reset_index(drop=True)


def aggregate_minute_day(bars: pd.DataFrame) -> dict[str, float | str | int]:
    """Aggregate one normalized minute day without changing its price basis."""
    if bars.empty:
        raise JQuantsBacktestDataError("Cannot aggregate an empty minute day")
    first = bars.iloc[0]
    last = bars.iloc[-1]
    return {
        "Open": float(first["Open"]),
        "High": float(bars["High"].max()),
        "Low": float(bars["Low"].min()),
        "Close": float(last["Close"]),
        "Volume": float(bars["Volume"].sum()),
        "Value": float(bars["Value"].sum()),
        "first_time": bars.index[0].strftime("%H:%M"),
        "last_time": bars.index[-1].strftime("%H:%M"),
        "bar_count": int(len(bars)),
    }


def validate_daily_alignment(
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    ticker: str,
    target_date: date | datetime | pd.Timestamp | str,
) -> tuple[dict[str, float | str | int], float]:
    """Cross-check minute OHLCV and return the comparable previous close."""
    day = _target_date(target_date)
    ticker_daily = daily[daily["ticker"].astype(str).eq(str(ticker))].copy()
    ticker_daily["date"] = pd.to_datetime(ticker_daily["date"], errors="coerce")
    target = ticker_daily[ticker_daily["date"].dt.date.eq(day)]
    if len(target) != 1:
        raise JQuantsBacktestDataError(
            f"{ticker}: expected one J-Quants daily row for {day}, got {len(target)}"
        )

    prior = ticker_daily[ticker_daily["date"].dt.date.lt(day)].sort_values("date")
    if prior.empty or pd.isna(prior.iloc[-1]["Close"]):
        raise JQuantsBacktestDataError(
            f"{ticker}: previous J-Quants daily close is unavailable before {day}"
        )

    aggregate = aggregate_minute_day(bars)
    daily_row = target.iloc[0]
    mismatches: list[str] = []
    for column in ["Open", "High", "Low", "Close"]:
        actual = float(aggregate[column])
        expected = float(daily_row[column])
        if not np.isclose(actual, expected, atol=PRICE_ATOL, rtol=1e-9):
            mismatches.append(f"{column}: minute={actual}, daily={expected}")
    actual_volume = float(aggregate["Volume"])
    expected_volume = float(daily_row["Volume"])
    if not np.isclose(actual_volume, expected_volume, atol=VOLUME_ATOL, rtol=1e-9):
        mismatches.append(
            f"Volume: minute={actual_volume}, daily={expected_volume}"
        )
    if mismatches:
        raise JQuantsBacktestDataError(
            f"{ticker}: minute/daily J-Quants mismatch for {day}: "
            + "; ".join(mismatches)
        )

    return aggregate, float(prior.iloc[-1]["Close"])


def executable_exit(
    bars: pd.DataFrame,
    target: time,
    *,
    require_trade_before_target: bool = True,
) -> dict[str, Any]:
    """Find the first executable bar open at or after a target time."""
    bar_times = pd.Series(bars.index.time, index=bars.index)
    if require_trade_before_target and not bool(bar_times.lt(target).any()):
        return {
            "price": None,
            "source_time": None,
            "source_kind": None,
            "missing_reason": "no_trade_before_target",
        }

    future = bars.loc[bar_times.ge(target).to_numpy()]
    if future.empty:
        return {
            "price": None,
            "source_time": None,
            "source_kind": None,
            "missing_reason": "no_trade_at_or_after_target",
        }

    first = future.iloc[0]
    source_time = future.index[0].strftime("%H:%M")
    target_text = target.strftime("%H:%M")
    return {
        "price": float(first["Open"]),
        "source_time": source_time,
        "source_kind": "exact_open" if source_time == target_text else "next_open",
        "missing_reason": None,
    }


def has_trade_after_entry(
    bars: pd.DataFrame,
    *,
    end: str | None = None,
) -> bool:
    """Return whether a distinct later minute exists after the entry bar."""
    if bars.empty:
        return False
    candidates = bars.between_time("00:00", end) if end else bars
    if candidates.empty:
        return False
    return bool((candidates.index > bars.index[0]).any())


def session_last_close(
    bars: pd.DataFrame,
    start: str,
    end: str,
) -> float | None:
    """Return the last observed close in a session, without a later fallback."""
    session = bars.between_time(start, end)
    if session.empty:
        return None
    return float(session.iloc[-1]["Close"])


def calculate_segment_pnl(bars: pd.DataFrame, entry_price: float) -> dict[str, Any]:
    """Calculate short PnL per 100 shares using the canonical segment rules."""
    segments: dict[str, Any] = {}
    for name, target in SEGMENT_TARGETS.items():
        if name == "seg_1530":
            exit_price = float(bars.iloc[-1]["Close"])
        else:
            exit_price = executable_exit(bars, target)["price"]
        segments[name] = (
            (float(entry_price) - float(exit_price)) * 100.0
            if exit_price is not None
            else None
        )
    return segments


def _assert_frame_content_equal(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
) -> None:
    """Compare exact content while treating all missing sentinels as equivalent."""
    if expected.shape != actual.shape:
        raise AssertionError(
            f"DataFrame shape mismatch: {expected.shape} != {actual.shape}"
        )
    if not expected.columns.equals(actual.columns):
        raise AssertionError(
            f"DataFrame columns mismatch: {list(expected.columns)} != "
            f"{list(actual.columns)}"
        )
    for column in expected.columns:
        expected_values = expected[column].reset_index(drop=True)
        actual_values = actual[column].reset_index(drop=True)
        expected_missing = expected_values.isna().to_numpy(dtype=bool)
        actual_missing = actual_values.isna().to_numpy(dtype=bool)
        if not np.array_equal(expected_missing, actual_missing):
            changed_rows = np.flatnonzero(expected_missing != actual_missing)[:10]
            raise AssertionError(
                f"Column {column!r} missing-value positions changed at rows "
                f"{changed_rows.tolist()}"
            )
        present_rows = np.flatnonzero(~expected_missing)
        try:
            pd.testing.assert_series_equal(
                expected_values.iloc[present_rows].reset_index(drop=True),
                actual_values.iloc[present_rows].reset_index(drop=True),
                check_dtype=False,
                check_exact=True,
                check_categorical=False,
                check_names=False,
            )
        except AssertionError as error:
            raise AssertionError(
                f"Column {column!r} non-missing values changed: {error}"
            ) from error


def merge_archive_date(
    archive: pd.DataFrame,
    new_rows: pd.DataFrame,
    backtest_date: str,
) -> pd.DataFrame:
    """Replace only one archive date and prove all other row values are unchanged."""
    required = {"backtest_date", "ticker"}
    if not required.issubset(archive.columns) or not required.issubset(new_rows.columns):
        raise JQuantsBacktestDataError(
            "Archive and new rows must contain backtest_date and ticker"
        )
    archive_dates = pd.to_datetime(archive["backtest_date"], errors="raise")
    archive_keys = pd.DataFrame(
        {
            "backtest_date": archive_dates.dt.strftime("%Y-%m-%d"),
            "ticker": archive["ticker"].astype(str),
        }
    )
    new_key_dates = pd.to_datetime(new_rows["backtest_date"], errors="raise")
    new_keys = pd.DataFrame(
        {
            "backtest_date": new_key_dates.dt.strftime("%Y-%m-%d"),
            "ticker": new_rows["ticker"].astype(str),
        }
    )
    if archive_keys.duplicated().any():
        raise JQuantsBacktestDataError("Source archive contains duplicate ticker-date keys")
    if new_keys.duplicated().any():
        raise JQuantsBacktestDataError("New rows contain duplicate ticker-date keys")

    target = pd.Timestamp(backtest_date).strftime("%Y-%m-%d")
    new_dates = new_key_dates.dt.strftime("%Y-%m-%d")
    if not new_dates.eq(target).all():
        raise JQuantsBacktestDataError(
            f"New rows contain dates other than the target date {target}"
        )

    if not archive.empty and pd.Timestamp(target) < archive_dates.max().normalize():
        raise JQuantsBacktestDataError(
            f"Refusing historical replacement: {target} is before archive max "
            f"{archive_dates.max().date()}"
        )

    keep_mask = archive_dates.dt.strftime("%Y-%m-%d").ne(target)
    unchanged = archive.loc[keep_mask].copy().reset_index(drop=True)
    normalized_new = new_rows.copy()
    normalized_new["backtest_date"] = new_dates
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries",
            category=FutureWarning,
        )
        merged = pd.concat([unchanged, normalized_new], ignore_index=True, sort=False)

    expected_rows = len(unchanged) + len(normalized_new)
    if len(merged) != expected_rows:
        raise JQuantsBacktestDataError(
            f"Archive row count mismatch: expected {expected_rows}, got {len(merged)}"
        )
    if merged[["backtest_date", "ticker"]].duplicated().any():
        raise JQuantsBacktestDataError("Merged archive contains duplicate ticker-date keys")

    if not unchanged.empty:
        _assert_frame_content_equal(
            unchanged,
            merged.iloc[: len(unchanged)][unchanged.columns].reset_index(drop=True),
        )
    return merged


def assert_archive_history_unchanged(
    source: pd.DataFrame,
    candidate: pd.DataFrame,
    backtest_date: str,
) -> None:
    """Assert that parquet serialization did not alter any non-target cell."""
    target = pd.Timestamp(backtest_date).strftime("%Y-%m-%d")
    source_dates = pd.to_datetime(source["backtest_date"], errors="raise")
    historical = source.loc[
        source_dates.dt.strftime("%Y-%m-%d").ne(target)
    ].reset_index(drop=True)
    if any(column not in candidate.columns for column in source.columns):
        raise JQuantsBacktestDataError("Candidate archive lost one or more source columns")
    candidate_history = candidate.iloc[: len(historical)][source.columns].reset_index(
        drop=True
    )
    try:
        _assert_frame_content_equal(
            historical,
            candidate_history,
        )
    except AssertionError as error:
        raise JQuantsBacktestDataError(
            f"Non-target archive history changed after serialization: {error}"
        ) from error


def assert_archive_target_rows_preserved(
    new_rows: pd.DataFrame,
    candidate: pd.DataFrame,
    backtest_date: str,
) -> None:
    """Assert that every generated target value survived parquet serialization."""
    target = pd.Timestamp(backtest_date).strftime("%Y-%m-%d")
    expected = new_rows.copy().reset_index(drop=True)
    expected["backtest_date"] = pd.to_datetime(
        expected["backtest_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    candidate_dates = pd.to_datetime(candidate["backtest_date"], errors="raise")
    actual = candidate.loc[
        candidate_dates.dt.strftime("%Y-%m-%d").eq(target), expected.columns
    ].reset_index(drop=True)
    try:
        _assert_frame_content_equal(
            expected,
            actual,
        )
    except AssertionError as error:
        raise JQuantsBacktestDataError(
            f"Generated target rows changed after serialization: {error}"
        ) from error
