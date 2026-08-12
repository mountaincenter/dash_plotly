"""Read-only API for the 200A 07:00 pre-open shadow decision."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException


ROOT = Path(__file__).resolve().parents[2]
PREOPEN_PATH = ROOT / "data" / "parquet" / "etf_0910_preopen.json"
BACKTEST_PATH = ROOT / "server" / "data" / "daytrade_etf_backtest_v11.json"
EXPECTED_STRATEGY_VERSION = "etf0910_v2_20260811"
S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET")
S3_PREFIX = (os.getenv("PARQUET_PREFIX") or "parquet/").strip("/")
AWS_REGION = os.getenv("AWS_REGION") or "ap-northeast-1"
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
JST = ZoneInfo("Asia/Tokyo")

router = APIRouter()


def _s3_key(filename: str) -> str:
    return f"{S3_PREFIX}/{filename}" if S3_PREFIX else filename


@lru_cache(maxsize=1)
def _s3_client() -> Any:
    import boto3

    options: dict[str, str] = {"region_name": AWS_REGION}
    if AWS_ENDPOINT_URL:
        options["endpoint_url"] = AWS_ENDPOINT_URL
    return boto3.client("s3", **options)


def _load_snapshot() -> tuple[dict[str, Any], dict[str, Any]]:
    if S3_BUCKET:
        key = _s3_key(PREOPEN_PATH.name)
        response = _s3_client().get_object(Bucket=S3_BUCKET, Key=key)
        payload = json.loads(response["Body"].read().decode("utf-8"))
        source = {
            "kind": "s3",
            "bucket": S3_BUCKET,
            "key": key,
            "etag": str(response.get("ETag") or "").strip('"') or None,
            "version_id": response.get("VersionId"),
            "last_modified": (
                response["LastModified"].isoformat()
                if response.get("LastModified") is not None
                else None
            ),
        }
        if not isinstance(payload, dict):
            raise ValueError("ETF 07:00 snapshot must be a JSON object")
        return payload, source

    if not PREOPEN_PATH.exists():
        raise FileNotFoundError(PREOPEN_PATH)
    payload = json.loads(PREOPEN_PATH.read_text(encoding="utf-8"))
    stat = PREOPEN_PATH.stat()
    source = {
        "kind": "local",
        "path": str(PREOPEN_PATH),
        "last_modified": datetime.fromtimestamp(stat.st_mtime, tz=JST).isoformat(),
    }
    if not isinstance(payload, dict):
        raise ValueError("ETF 07:00 snapshot must be a JSON object")
    return payload, source


@lru_cache(maxsize=1)
def _load_backtest() -> dict[str, Any]:
    if not BACKTEST_PATH.exists():
        raise FileNotFoundError(BACKTEST_PATH)
    payload = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("ETF backtest evidence must be a JSON object")

    supplied_hash = str(payload.get("payload_sha256") or "")
    canonical_payload = dict(payload)
    canonical_payload.pop("payload_sha256", None)
    canonical = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    actual_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if supplied_hash != actual_hash:
        raise ValueError("ETF backtest payload hash mismatch")
    if payload.get("strategy_version") != EXPECTED_STRATEGY_VERSION:
        raise ValueError("ETF backtest strategy version mismatch")

    headline = payload.get("headline")
    trades = payload.get("trades")
    if not isinstance(headline, dict) or not isinstance(trades, list):
        raise ValueError("ETF backtest headline or trades is missing")
    expected = {
        "trades": 51,
        "profit_factor_bps": 2.5299650025572635,
        "total_pnl_yen": 6550.0,
        "win_rate_pct": 66.66666666666666,
        "worst_pnl_yen": -500.0,
        "max_drawdown_yen": -1000.0,
    }
    for field, expected_value in expected.items():
        actual = float(headline.get(field))
        if abs(actual - expected_value) > 1e-9:
            raise ValueError(f"ETF backtest headline mismatch: {field}")
    if len(trades) != 51:
        raise ValueError("ETF backtest trade count mismatch")
    if len({trade.get("trading_date") for trade in trades}) != 51:
        raise ValueError("ETF backtest trading dates must be unique")
    return payload


def _validate_snapshot(payload: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "strategy_version",
        "generated_at",
        "target_session",
        "status",
        "source_provider",
        "decision",
        "sources",
        "reason",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"ETF 07:00 snapshot fields missing: {missing}")
    if payload["strategy_version"] != EXPECTED_STRATEGY_VERSION:
        raise ValueError(
            "ETF 07:00 strategy version mismatch: "
            f"{payload['strategy_version']} != {EXPECTED_STRATEGY_VERSION}"
        )

    status = str(payload["status"])
    allowed_statuses = {
        "ready",
        "waiting_target_date",
        "waiting_0700",
        "data_unavailable",
    }
    if status not in allowed_statuses:
        raise ValueError(f"unknown ETF 07:00 status: {status}")

    datetime.fromisoformat(str(payload["generated_at"]))
    datetime.fromisoformat(str(payload["target_session"]))
    sources = payload["sources"]
    if not isinstance(sources, list):
        raise ValueError("ETF 07:00 sources must be a list")

    decision = payload["decision"]
    if status == "ready":
        if not isinstance(decision, dict):
            raise ValueError("ready ETF 07:00 snapshot has no decision")
        if decision.get("ticker") != "200A.T":
            raise ValueError("ETF 07:00 decision ticker must be 200A.T")
        if decision.get("action") not in {"WATCH", "NO_TRADE"}:
            raise ValueError("ETF 07:00 action must be WATCH or NO_TRADE")
        if decision.get("watch_direction") not in {
            "LONG",
            "SHORT",
            "NO_TRADE",
        }:
            raise ValueError("ETF 07:00 watch_direction is invalid")
        if decision["action"] == "WATCH" and decision["watch_direction"] not in {
            "LONG",
            "SHORT",
        }:
            raise ValueError("WATCH requires LONG or SHORT watch_direction")
        if (
            decision["action"] == "NO_TRADE"
            and decision["watch_direction"] != "NO_TRADE"
        ):
            raise ValueError("NO_TRADE requires NO_TRADE watch_direction")
    elif decision is not None:
        raise ValueError(f"{status} ETF 07:00 snapshot must not have a decision")


def _operational(payload: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    current = (now or datetime.now(JST)).astimezone(JST)
    target = datetime.fromisoformat(str(payload["target_session"])).date()
    status = str(payload["status"])
    is_today = target == current.date()
    decision = payload.get("decision") or {}

    if status == "ready" and is_today:
        action = str(decision.get("action"))
        if action == "WATCH":
            state = "watch"
            headline = "200Aを監視"
            guidance = "寄前条件は成立しています。"
        else:
            state = "no_trade"
            headline = "200Aはノートレ"
            guidance = "寄前条件の対象外です。200Aを取引しません。"
    elif status == "data_unavailable" and is_today:
        state = "data_unavailable"
        headline = "判定不能"
        guidance = "必要データを検証できないため、200Aを取引しません。"
    elif status in {"waiting_target_date", "waiting_0700"}:
        state = "waiting"
        headline = "07:00判定待ち"
        guidance = "判定がreadyになるまでは200Aを取引しません。"
    else:
        state = "stale"
        headline = "当日判定なし"
        guidance = "表示中の判定は当日分ではないため、取引判断に使用しません。"

    return {
        "as_of": current.isoformat(),
        "current_jst_date": current.date().isoformat(),
        "target_session": target.isoformat(),
        "is_current_session": is_today,
        "state": state,
        "headline": headline,
        "guidance": guidance,
        "automatic_entry": False,
        "automatic_ordering": False,
        "intraday_decision": "manual_marketspeed",
    }


@router.get("/api/dev/daytrade-etf")
def get_daytrade_etf() -> dict[str, Any]:
    try:
        payload, source = _load_snapshot()
        _validate_snapshot(payload)
        operational = _operational(payload)
        backtest = _load_backtest()
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"ETF Daytrade data is not generated: {exc}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"ETF Daytrade data read failed: {exc}",
        ) from exc

    return {
        "snapshot": payload,
        "operational": operational,
        "backtest": backtest,
        "source": source,
    }
