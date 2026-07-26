from __future__ import annotations

import sys
from os import environ
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.routers.trading_replay import (
    PUBLIC_PATH,
    ResultRequest,
    _load,
    get_daily_stage,
    get_intraday_stage,
    get_result_stage,
    list_cases,
)
from server.routers import trading_replay


def test_case_list_uses_0930_and_has_no_payload_data() -> None:
    payload = list_cases("")
    assert payload["cases"]
    assert payload["defaultCutoff"] == "09:30"
    assert all(case["cutoff"] == "09:30" for case in payload["cases"])
    assert all("daily" not in case for case in payload["cases"])
    assert all("intraday" not in case for case in payload["cases"])


def test_stage_endpoints_keep_future_data_separate() -> None:
    case = list_cases("")["cases"][0]
    case_id = case["id"]
    cutoff = pd.Timestamp(f"{case['date']} {case['cutoff']}")

    daily = get_daily_stage(case_id)
    assert "intraday" not in daily
    assert "tickTempo" not in daily
    assert max(pd.Timestamp(row["date"]) for row in daily["daily"]) < pd.Timestamp(
        case["date"]
    )

    intraday = get_intraday_stage(case_id)
    assert "fullIntraday" not in intraday
    assert "guidance" not in intraday
    assert max(pd.Timestamp(row["datetime"]) for row in intraday["intraday"]) < cutoff
    assert max(pd.Timestamp(row["end"]) for row in intraday["tickTempo"]) <= cutoff


def test_result_returns_only_selected_outcome() -> None:
    case_id = list_cases("")["cases"][0]["id"]
    result = get_result_stage(
        case_id,
        ResultRequest(daily_decision="wait", intraday_decision="buy"),
    )
    assert result["decisions"] == {"daily": "wait", "intraday": "buy"}
    assert result["outcome"]["side"] == "buy"
    assert "outcomes" not in result
    assert pd.Timestamp(result["outcome"]["entryTime"]) > pd.Timestamp(
        f"{result['case']['date']} {result['case']['cutoff']}"
    )


def test_2459_cases_are_available_with_verified_latest_result() -> None:
    payload = list_cases("2459")
    assert payload["ticker"] == "2459.T"
    assert len(payload["cases"]) == 4
    assert {case["date"] for case in payload["cases"]} == {
        "2026-07-21",
        "2026-07-22",
        "2026-07-23",
        "2026-07-24",
    }

    latest = payload["cases"][0]
    assert latest["id"] == "2459_20260724_0930"
    result = get_result_stage(
        latest["id"],
        ResultRequest(daily_decision="wait", intraday_decision="sell"),
    )
    assert result["outcome"]["side"] == "sell"
    assert result["outcome"]["entryPrice"] == 196.0
    assert result["outcome"]["closePrice"] == 185.0
    assert result["outcome"]["closePnl"] == 1100.0


def test_s3_version_pointer_is_preferred_over_local_payload() -> None:
    s3_payload = {"cases": [{"id": "s3-case"}]}
    with (
        patch.dict(
            environ,
            {
                "DATA_BUCKET": "stock-api-data",
                "TRADING_REPLAY_S3_PREFIX": "training/replay/v1",
            },
        ),
        patch.object(
            trading_replay,
            "_read_s3_current",
            return_value=("a" * 64, None),
        ) as current,
        patch.object(
            trading_replay,
            "_read_s3_versioned",
            return_value=(s3_payload, None),
        ) as versioned,
    ):
        assert _load(PUBLIC_PATH) == s3_payload
        assert current.call_args.args[:2] == (
            "stock-api-data",
            "training/replay/v1",
        )
        assert versioned.call_args.args == (
            "stock-api-data",
            "training/replay/v1",
            "a" * 64,
            "public.json",
        )


if __name__ == "__main__":
    test_case_list_uses_0930_and_has_no_payload_data()
    test_stage_endpoints_keep_future_data_separate()
    test_result_returns_only_selected_outcome()
    test_2459_cases_are_available_with_verified_latest_result()
    test_s3_version_pointer_is_preferred_over_local_payload()
    print("5 trading replay tests passed")
