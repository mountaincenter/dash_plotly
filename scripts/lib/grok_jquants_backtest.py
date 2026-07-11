"""J-Quants based price and execution rules for the Grok archive."""

from __future__ import annotations

import warnings
from datetime import date, datetime, time
from typing import Any

import numpy as np
import pandas as pd


PRICE_ATOL = 0.011
VOLUME_ATOL = 0.5

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
        pd.testing.assert_frame_equal(
            unchanged,
            merged.iloc[: len(unchanged)][unchanged.columns].reset_index(drop=True),
            check_dtype=False,
            check_exact=True,
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
        pd.testing.assert_frame_equal(
            historical,
            candidate_history,
            check_dtype=False,
            check_exact=True,
            check_categorical=False,
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
        pd.testing.assert_frame_equal(
            expected,
            actual,
            check_dtype=False,
            check_exact=True,
            check_categorical=False,
        )
    except AssertionError as error:
        raise JQuantsBacktestDataError(
            f"Generated target rows changed after serialization: {error}"
        ) from error
