#!/usr/bin/env python3
"""Normalize nullable J-Quants daily market-cap and ex-rights fields."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


JQ_MKT_CAP_MILLION_YEN = "jq_mkt_cap_million_yen"
JQ_MARKET_CAP_YEN = "jq_market_cap_yen"
JQ_EX_RIGHTS_TYPE = "jq_ex_rights_type"
JQ_ADJUSTMENT_FACTOR = "jq_adjustment_factor"
JQ_DAILY_TRADE_STATUS = "jq_daily_trade_status"

DAILY_TRADE_STATUS_TRADED = "traded"
DAILY_TRADE_STATUS_NO_MARKET_TRADE = "no_market_trade"

JQUANTS_DAILY_FIELD_COLUMNS = [
    JQ_MKT_CAP_MILLION_YEN,
    JQ_MARKET_CAP_YEN,
    JQ_EX_RIGHTS_TYPE,
    JQ_ADJUSTMENT_FACTOR,
]

JQUANTS_DAILY_RAW_FIELDS = {"MktCap", "ExRT", "AdjFactor"}
VALID_EX_RIGHTS_TYPES = {1, 2, 3}


def missing_raw_daily_fields(columns: Iterable[object]) -> list[str]:
    """Return fields that prove the upstream response supports the new schema."""
    available = {str(column) for column in columns}
    return sorted(JQUANTS_DAILY_RAW_FIELDS - available)


def _coalesce(frame: pd.DataFrame, names: list[str]) -> pd.Series:
    result = pd.Series(pd.NA, index=frame.index, dtype="object")
    for name in names:
        if name in frame.columns:
            result = result.where(result.notna(), frame[name])
    return result


def _nullable_numeric(frame: pd.DataFrame, names: list[str]) -> pd.Series:
    return pd.to_numeric(_coalesce(frame, names), errors="coerce").astype("Float64")


def classify_jquants_daily_trade_status(frame: pd.DataFrame) -> pd.Series:
    """Classify an official daily row without confusing no-trade with data loss.

    J-Quants represents a listed security with no market trade on that day by
    returning the daily row with all OHLCV values null.  A fully populated row
    is ``traded``.  A partially-null row is neither state and is rejected.
    """
    values = pd.DataFrame(
        {
            "open": _nullable_numeric(
                frame, ["Open", "O", "AdjustmentOpen", "AdjO"]
            ),
            "high": _nullable_numeric(
                frame, ["High", "H", "AdjustmentHigh", "AdjH"]
            ),
            "low": _nullable_numeric(
                frame, ["Low", "L", "AdjustmentLow", "AdjL"]
            ),
            "close": _nullable_numeric(
                frame, ["Close", "C", "AdjustmentClose", "AdjC"]
            ),
            "volume": _nullable_numeric(
                frame, ["Volume", "Vo", "AdjustmentVolume", "AdjVo"]
            ),
        },
        index=frame.index,
    )
    all_null = values.isna().all(axis=1)
    all_present = values.notna().all(axis=1)
    partial = ~(all_null | all_present)
    if bool(partial.any()):
        examples = values.loc[partial].head(10).to_dict("records")
        raise ValueError(
            "J-Quants daily OHLCV is partially null; trade status is ambiguous: "
            f"rows={int(partial.sum())}, examples={examples}"
        )

    invalid_prices = all_present & values[
        ["open", "high", "low", "close"]
    ].le(0).any(axis=1)
    invalid_volume = all_present & values["volume"].lt(0)
    invalid_ohlc = all_present & (
        values["high"].lt(values[["open", "close"]].max(axis=1))
        | values["low"].gt(values[["open", "close"]].min(axis=1))
        | values["high"].lt(values["low"])
    )
    invalid = invalid_prices | invalid_volume | invalid_ohlc
    if bool(invalid.any()):
        examples = values.loc[invalid].head(10).to_dict("records")
        raise ValueError(
            "J-Quants daily OHLCV is invalid: "
            f"rows={int(invalid.sum())}, examples={examples}"
        )

    status = pd.Series(
        DAILY_TRADE_STATUS_TRADED,
        index=frame.index,
        dtype="string",
    )
    return status.mask(all_null, DAILY_TRADE_STATUS_NO_MARKET_TRADE)


def normalize_jquants_daily_fields(
    frame: pd.DataFrame,
    *,
    strict_ex_rights: bool = True,
) -> pd.DataFrame:
    """Return a copy with stable nullable ``jq_*`` daily fields.

    ``MktCap`` is published in million yen.  The existing project-wide
    ``market_cap`` convention is yen, so both units are retained explicitly.
    Raw fields are kept for provenance and backward compatibility.
    """
    out = frame.copy()

    mkt_cap_million = _nullable_numeric(
        out,
        ["MktCap", JQ_MKT_CAP_MILLION_YEN],
    )
    market_cap_yen_existing = _nullable_numeric(out, [JQ_MARKET_CAP_YEN])
    market_cap_yen = (mkt_cap_million * 1_000_000.0).astype("Float64")
    market_cap_yen = market_cap_yen.where(
        market_cap_yen.notna(), market_cap_yen_existing
    )

    ex_rights_raw = _coalesce(out, ["ExRT", JQ_EX_RIGHTS_TYPE])
    ex_rights_text = ex_rights_raw.astype("string").str.strip()
    ex_rights_text = ex_rights_text.mask(
        ex_rights_text.str.lower().isin({"", "nan", "none", "null", "<na>"})
    )
    ex_rights_numeric = pd.to_numeric(ex_rights_text, errors="coerce")
    invalid_parse = ex_rights_text.notna() & ex_rights_numeric.isna()
    invalid_integer = ex_rights_numeric.notna() & ex_rights_numeric.mod(1).ne(0)
    invalid_domain = ex_rights_numeric.notna() & ~ex_rights_numeric.isin(
        VALID_EX_RIGHTS_TYPES
    )
    invalid = invalid_parse | invalid_integer | invalid_domain
    if strict_ex_rights and bool(invalid.any()):
        values = sorted(ex_rights_text[invalid].dropna().astype(str).unique().tolist())
        raise ValueError(f"invalid J-Quants ExRT values: {values}")

    ex_rights_numeric = ex_rights_numeric.mask(invalid).astype("Int64")
    adjustment_factor = _nullable_numeric(
        out,
        ["AdjFactor", "AdjustmentFactor", JQ_ADJUSTMENT_FACTOR],
    )

    out[JQ_MKT_CAP_MILLION_YEN] = mkt_cap_million
    out[JQ_MARKET_CAP_YEN] = market_cap_yen
    out[JQ_EX_RIGHTS_TYPE] = ex_rights_numeric
    out[JQ_ADJUSTMENT_FACTOR] = adjustment_factor
    return out
