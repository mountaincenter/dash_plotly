#!/usr/bin/env python3
"""Publish the frozen 200A pre-open shadow decision after the US close."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = ROOT / "data/parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
US_DAILY_PATH = PARQUET_DIR / "etf_0910_us_daily.parquet"
OUTPUT_PATH = PARQUET_DIR / "etf_0910_preopen.json"

STRATEGY_VERSION = "etf0910_v2_20260811"
TARGET_TICKER = "200A.T"
JST = ZoneInfo("Asia/Tokyo")
NEW_YORK = ZoneInfo("America/New_York")

# This source set and the thresholds below are frozen from the validated
# 200A semiconductor selector. This publisher does not optimise them.
SEMICON_REQUIRED_SOURCES = ("SMH", "QQQ")
SEMICON_BREADTH_SOURCES = ("^SOX", "SMH", "NVDA", "MU", "AVGO", "AMD")
US_SYMBOLS = tuple(sorted(set(SEMICON_REQUIRED_SOURCES + SEMICON_BREADTH_SOURCES)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO timestamp for deterministic replay")
    parser.add_argument("--calendar", type=Path, default=CALENDAR_PATH)
    parser.add_argument("--us-daily-output", type=Path, default=US_DAILY_PATH)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--no-fetch-us", action="store_true")
    return parser.parse_args()


def parse_as_of(value: str | None) -> datetime:
    if value is None:
        return datetime.now(JST)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=JST)
    return parsed.astimezone(JST)


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def clean_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): clean_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return finite(value)
    if value is pd.NA or (
        not isinstance(value, (str, bool)) and pd.isna(value)
    ):
        return None
    return value


def atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(clean_json(payload), ensure_ascii=False, indent=2, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, engine="pyarrow", index=False)
    os.replace(temporary, path)


def ticker_frame(downloaded: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if downloaded.empty:
        return pd.DataFrame()
    if isinstance(downloaded.columns, pd.MultiIndex):
        level_zero = set(downloaded.columns.get_level_values(0))
        level_one = set(downloaded.columns.get_level_values(1))
        if ticker in level_zero:
            return downloaded[ticker].copy()
        if ticker in level_one:
            return downloaded.xs(ticker, axis=1, level=1).copy()
        return pd.DataFrame()
    return downloaded.copy()


def fetch_us_daily() -> pd.DataFrame:
    downloaded = yf.download(
        list(US_SYMBOLS),
        period="2y",
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True,
        progress=False,
    )
    rows: list[pd.DataFrame] = []
    for ticker in US_SYMBOLS:
        frame = ticker_frame(downloaded, ticker)
        if frame.empty or "Close" not in frame.columns:
            continue
        frame = frame.reset_index()
        date_column = next(
            (
                column
                for column in ("Date", "Datetime", "index")
                if column in frame.columns
            ),
            frame.columns[0],
        )
        frame = frame.rename(columns={date_column: "timestamp"})
        frame["ticker"] = ticker
        keep = [
            column
            for column in (
                "timestamp",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "ticker",
            )
            if column in frame.columns
        ]
        rows.append(frame[keep])
    if not rows:
        raise RuntimeError("Yahoo Finance returned no US daily rows")
    return normalize_us_daily(pd.concat(rows, ignore_index=True))


def normalize_us_daily(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "ticker", "Open", "High", "Low", "Close"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"US daily cache missing columns: {missing}")
    result = frame.copy()
    timestamps = pd.to_datetime(result["timestamp"], errors="coerce")
    if timestamps.dt.tz is not None:
        timestamps = timestamps.dt.tz_localize(None)
    result["timestamp"] = timestamps
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(
        subset=["timestamp", "ticker", "Open", "High", "Low", "Close"]
    )
    result = result[(result[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    result["ticker"] = result["ticker"].astype(str)
    result["session_date"] = result["timestamp"].dt.date.astype(str)
    return (
        result.sort_values(["ticker", "timestamp"])
        .drop_duplicates(["ticker", "timestamp"], keep="last")
        .reset_index(drop=True)
    )


def update_us_cache(path: Path, *, fetch: bool) -> pd.DataFrame:
    existing = (
        normalize_us_daily(pd.read_parquet(path)) if path.exists() else pd.DataFrame()
    )
    if not fetch:
        if existing.empty:
            raise FileNotFoundError(f"US daily cache not found: {path}")
        return existing

    fetched = fetch_us_daily()
    if existing.empty:
        combined = fetched
    else:
        combined = normalize_us_daily(pd.concat([existing, fetched], ignore_index=True))
    atomic_parquet(combined, path)
    return combined


def next_calendar_session(calendar_path: Path, as_of: datetime) -> pd.Timestamp:
    calendar = pd.read_parquet(calendar_path)
    if "date" not in calendar.columns:
        raise ValueError("calendar.parquet has no date column")
    sessions = (
        pd.to_datetime(calendar["date"], errors="coerce")
        .dropna()
        .dt.normalize()
        .sort_values()
        .drop_duplicates()
    )
    candidates = sessions[sessions.ge(pd.Timestamp(as_of.date()))]
    if candidates.empty:
        raise ValueError(f"calendar has no session on or after {as_of.date()}")
    return pd.Timestamp(candidates.iloc[0]).normalize()


def unavailable_payload(
    *, as_of: datetime, target: pd.Timestamp, status: str, reason: str
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "strategy_version": STRATEGY_VERSION,
        "generated_at": as_of.isoformat(),
        "target_session": str(target.date()),
        "status": status,
        "source_provider": "yfinance",
        "decision": None,
        "sources": [],
        "reason": reason,
    }


def build_decision(
    *, as_of: datetime, target: pd.Timestamp, us_daily: pd.DataFrame
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    normalized = normalize_us_daily(us_daily)
    normalized["date"] = normalized["timestamp"].dt.normalize()
    normalized["ret1_pct"] = (
        normalized.groupby("ticker")["Close"].pct_change(fill_method=None) * 100.0
    )

    available_dates = normalized.loc[
        normalized["date"].lt(target), "date"
    ].dropna()
    if available_dates.empty:
        raise ValueError("completed US semiconductor context is unavailable")
    context_date = pd.Timestamp(available_dates.max()).normalize()
    context_age_days = int((target - context_date).days)
    if not 1 <= context_age_days <= 3:
        raise ValueError(
            f"US context is too stale for a 07:00 decision: age_days={context_age_days}"
        )

    now_et = as_of.astimezone(NEW_YORK)
    if context_date.date() > now_et.date() or (
        context_date.date() == now_et.date() and now_et.time() < time(16, 15)
    ):
        raise ValueError(
            "US session is not confirmed closed: "
            f"context={context_date.date()} now_et={now_et.isoformat()}"
        )

    day = normalized[normalized["date"].eq(context_date)].copy()
    day = day[pd.to_numeric(day["ret1_pct"], errors="coerce").notna()]
    available = set(day["ticker"])
    missing_required = sorted(set(SEMICON_REQUIRED_SOURCES) - available)
    breadth_available = sorted(set(SEMICON_BREADTH_SOURCES) & available)
    if missing_required:
        raise ValueError(f"required US sources missing: {missing_required}")
    if len(breadth_available) < 5:
        raise ValueError(
            "US semiconductor breadth is incomplete: "
            f"available={breadth_available} required_count=5"
        )

    def return_for(ticker: str) -> float:
        values = pd.to_numeric(
            day.loc[day["ticker"].eq(ticker), "ret1_pct"], errors="coerce"
        ).dropna()
        if values.empty:
            raise ValueError(f"US return unavailable: {ticker}")
        return float(values.iloc[-1])

    semiconductor_return = return_for("SMH")
    market_return = return_for("QQQ")
    breadth_returns = pd.to_numeric(
        day.loc[day["ticker"].isin(SEMICON_BREADTH_SOURCES), "ret1_pct"],
        errors="coerce",
    ).dropna()
    positive = int((breadth_returns > 0).sum())
    negative = int((breadth_returns < 0).sum())
    relative = semiconductor_return - market_return

    if semiconductor_return > 1.0 and positive >= 5 and relative >= 0:
        label = "SEMIS_RISK_ON"
        external_direction = "LONG"
    elif semiconductor_return < -1.0 and negative >= 5:
        label = "SEMIS_RISK_OFF"
        external_direction = "SHORT"
    else:
        label = "NEUTRAL"
        external_direction = "NO_TRADE"

    if external_direction == "NO_TRADE":
        decision_status = "selector_no_trade"
    elif not 1.0 <= abs(semiconductor_return) < 2.0:
        decision_status = "v11_strength_gate_fail"
        external_direction = "NO_TRADE"
    else:
        decision_status = "eligible_external"

    eligible = decision_status == "eligible_external"
    if eligible and label == "SEMIS_RISK_ON":
        watch_direction = "SHORT"
    elif eligible and label == "SEMIS_RISK_OFF":
        watch_direction = "LONG"
    else:
        watch_direction = "NO_TRADE"

    sources: list[dict[str, Any]] = []
    for ticker in sorted(
        set(SEMICON_REQUIRED_SOURCES) | set(SEMICON_BREADTH_SOURCES)
    ):
        rows = day[day["ticker"].eq(ticker)]
        if rows.empty:
            continue
        row = rows.iloc[-1]
        sources.append(
            {
                "ticker": ticker,
                "session_date": str(context_date.date()),
                "close": finite(row["Close"]),
                "return_1d_pct": finite(row["ret1_pct"]),
            }
        )

    decision = {
        "ticker": TARGET_TICKER,
        "action": "WATCH" if eligible else "NO_TRADE",
        "external_context_date": str(context_date.date()),
        "external_label": label,
        "external_value": semiconductor_return,
        "external_direction": external_direction,
        "watch_direction": watch_direction,
        "decision_status": decision_status,
    }
    return decision, sources, context_age_days


def run(args: argparse.Namespace) -> int:
    as_of = parse_as_of(args.as_of)
    if not args.calendar.exists():
        raise FileNotFoundError(args.calendar)
    target = next_calendar_session(args.calendar, as_of)
    decision_at = datetime.combine(target.date(), time(7, 0), tzinfo=JST)

    if target.date() != as_of.date():
        payload = unavailable_payload(
            as_of=as_of,
            target=target,
            status="waiting_target_date",
            reason=(
                f"{as_of.date()} is not a JPX trading session; "
                f"decide at 07:00 JST on {target.date()}"
            ),
        )
    elif as_of < decision_at:
        payload = unavailable_payload(
            as_of=as_of,
            target=target,
            status="waiting_0700",
            reason=f"pre-open decision is scheduled for {decision_at.isoformat()}",
        )
    else:
        try:
            us_daily = update_us_cache(
                args.us_daily_output, fetch=not args.no_fetch_us
            )
            decision, sources, context_age_days = build_decision(
                as_of=as_of, target=target, us_daily=us_daily
            )
            payload = {
                "schema_version": 1,
                "strategy_version": STRATEGY_VERSION,
                "generated_at": as_of.isoformat(),
                "target_session": str(target.date()),
                "status": "ready",
                "source_provider": "yfinance",
                "decision": decision,
                "freshness": {
                    "us_context_age_days": context_age_days,
                    "us_session_completed": True,
                    "required_sources": list(SEMICON_REQUIRED_SOURCES),
                    "breadth_sources_available": [
                        item["ticker"]
                        for item in sources
                        if item["ticker"] in SEMICON_BREADTH_SOURCES
                    ],
                },
                "sources": sources,
                "reason": None,
                "input_sha256": {
                    "calendar": sha256_file(args.calendar),
                    "us_daily": sha256_file(args.us_daily_output),
                },
            }
        except Exception as exc:
            payload = unavailable_payload(
                as_of=as_of,
                target=target,
                status="data_unavailable",
                reason=str(exc),
            )

    atomic_json(payload, args.output)
    decision_payload = payload.get("decision") or {}
    print("=== ETF 07:00 pre-open ===")
    print(f"target : {target.date()}")
    print(f"status : {payload['status']}")
    print(f"action : {decision_payload.get('action', 'NONE')}")
    print(f"output : {args.output}")
    if payload.get("reason"):
        print(f"reason : {payload['reason']}")
    return 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
