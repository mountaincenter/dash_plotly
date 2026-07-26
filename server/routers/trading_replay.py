from __future__ import annotations

import json
import os
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel


router = APIRouter(prefix="/api/trading/replay")

DASH_ROOT = Path(__file__).resolve().parents[2]
PAYLOAD_ROOT = (
    DASH_ROOT
    / "data/research/grok_session_handoffs_20260718/04_technical_entry_training/output/nextjs"
)
PUBLIC_PATH = PAYLOAD_ROOT / "public.json"
RESULT_PATH = PAYLOAD_ROOT / "results.json"
DEFAULT_CUTOFF = "09:30"
DEFAULT_S3_PREFIX = "training/replay/v1"
DEFAULT_S3_CACHE_SECONDS = 60

Decision = Literal["buy", "sell", "wait"]


class ResultRequest(BaseModel):
    daily_decision: Decision
    intraday_decision: Decision


def _normalize_ticker(value: str) -> str:
    ticker = value.strip().upper()
    if ticker.isdigit():
        ticker = f"{ticker}.T"
    return ticker


def _s3_bucket() -> str:
    return (os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET") or "").strip()


def _s3_prefix() -> str:
    return (
        os.getenv("TRADING_REPLAY_S3_PREFIX") or DEFAULT_S3_PREFIX
    ).strip("/")


def _s3_cache_epoch() -> int:
    raw = os.getenv("TRADING_REPLAY_S3_CACHE_SECONDS", "")
    try:
        seconds = max(1, int(raw)) if raw else DEFAULT_S3_CACHE_SECONDS
    except ValueError:
        seconds = DEFAULT_S3_CACHE_SECONDS
    return int(time.monotonic() // seconds)


def _fetch_s3_json(bucket: str, key: str) -> dict[str, Any]:
    import boto3

    client_kwargs: dict[str, str] = {}
    region = os.getenv("AWS_REGION")
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if region:
        client_kwargs["region_name"] = region
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    client = boto3.Session().client("s3", **client_kwargs)
    response = client.get_object(Bucket=bucket, Key=key)
    value = json.loads(response["Body"].read())
    if not isinstance(value, dict):
        raise RuntimeError(f"S3 payload root must be object: s3://{bucket}/{key}")
    return value


@lru_cache(maxsize=4)
def _read_s3_current(
    bucket: str,
    prefix: str,
    cache_epoch: int,
) -> tuple[str | None, str | None]:
    del cache_epoch
    key = f"{prefix}/current.json"
    try:
        pointer = _fetch_s3_json(bucket, key)
        version = str(pointer.get("version", ""))
        if re.fullmatch(r"[0-9a-f]{64}", version) is None:
            raise RuntimeError(f"invalid replay version in s3://{bucket}/{key}")
        return version, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


@lru_cache(maxsize=16)
def _read_s3_versioned(
    bucket: str,
    prefix: str,
    version: str,
    filename: str,
) -> tuple[dict[str, Any] | None, str | None]:
    key = f"{prefix}/versions/{version}/{filename}"
    try:
        return _fetch_s3_json(bucket, key), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


@lru_cache(maxsize=2)
def _read_json(path_text: str, modified_ns: int) -> dict[str, Any]:
    del modified_ns
    path = Path(path_text)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"payload root must be object: {path}")
    return value


def _load(path: Path) -> dict[str, Any]:
    bucket = _s3_bucket()
    s3_error: str | None = None
    if bucket:
        prefix = _s3_prefix()
        version, current_error = _read_s3_current(
            bucket,
            prefix,
            _s3_cache_epoch(),
        )
        s3_error = current_error
        if version:
            value, version_error = _read_s3_versioned(
                bucket,
                prefix,
                version,
                path.name,
            )
            if value is not None:
                return value
            s3_error = version_error

    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail=(
                "training replay payload is unavailable; "
                "run export_nextjs_replay_payloads.py"
                + (f"; S3 read failed: {s3_error}" if s3_error else "")
            ),
        )
    return _read_json(str(path), path.stat().st_mtime_ns)


def _public_cases() -> list[dict[str, Any]]:
    cases = _load(PUBLIC_PATH).get("cases", [])
    return cases if isinstance(cases, list) else []


def _result_cases() -> list[dict[str, Any]]:
    cases = _load(RESULT_PATH).get("cases", [])
    return cases if isinstance(cases, list) else []


def _find_case(case_id: str, *, result: bool = False) -> dict[str, Any]:
    source = _result_cases() if result else _public_cases()
    case = next((item for item in source if item.get("id") == case_id), None)
    if case is None:
        raise HTTPException(status_code=404, detail=f"unknown replay case: {case_id}")
    return case


def _case_summary(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": case["id"],
        "ticker": case["ticker"],
        "name": case["name"],
        "date": case["date"],
        "cutoff": case["cutoff"],
    }


@router.get("/cases")
def list_cases(
    ticker: str = Query(default="", description="例: 8306.T"),
) -> dict[str, Any]:
    normalized = _normalize_ticker(ticker) if ticker else ""
    all_cases = _public_cases()
    selected = [
        _case_summary(case)
        for case in all_cases
        if not normalized or case.get("ticker") == normalized
    ]
    selected.sort(key=lambda item: (item["date"], item["id"]), reverse=True)
    return {
        "defaultCutoff": DEFAULT_CUTOFF,
        "ticker": normalized or None,
        "cases": selected,
        "availableTickers": sorted(
            {str(case["ticker"]) for case in all_cases if case.get("ticker")}
        ),
    }


@router.get("/cases/{case_id}/daily")
def get_daily_stage(case_id: str) -> dict[str, Any]:
    case = _find_case(case_id)
    return {
        "case": _case_summary(case),
        "daily": case["daily"],
        "dailyContext": case["dailyContext"],
    }


@router.get("/cases/{case_id}/intraday")
def get_intraday_stage(case_id: str) -> dict[str, Any]:
    case = _find_case(case_id)
    return {
        "case": _case_summary(case),
        "intraday": case["intraday"],
        "tickTempo": case["tickTempo"],
        "intradayContext": case["intradayContext"],
    }


@router.post("/cases/{case_id}/result")
def get_result_stage(case_id: str, request: ResultRequest) -> dict[str, Any]:
    public_case = _find_case(case_id)
    result_case = _find_case(case_id, result=True)
    outcomes = result_case.get("outcomes", {})
    outcome = outcomes.get(request.intraday_decision)
    if not isinstance(outcome, dict):
        raise HTTPException(status_code=500, detail="selected outcome is unavailable")
    return {
        "case": _case_summary(public_case),
        "decisions": {
            "daily": request.daily_decision,
            "intraday": request.intraday_decision,
        },
        "guidance": result_case["guidance"],
        "fullIntraday": result_case["fullIntraday"],
        "fullTickTempo": result_case["fullTickTempo"],
        "outcome": outcome,
    }
