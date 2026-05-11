"""
Pre-market Allocation API
/api/dev/allocation — 寄前アロケータ結果
"""
from __future__ import annotations

import io
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

router = APIRouter(prefix="/api/dev/allocation")

ALLOCATION_PATH = PARQUET_DIR / "allocation.parquet"

S3_BUCKET = os.getenv("S3_BUCKET", os.getenv("DATA_BUCKET", "stock-api-data"))
_S3_PREFIX_RAW = os.getenv("PARQUET_PREFIX", "parquet")
S3_PREFIX = _S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
_IS_LOCAL = PARQUET_DIR.exists()

_cache: dict[str, tuple[datetime, object]] = {}
CACHE_TTL = 120


def _cached(key: str) -> Optional[object]:
    if key in _cache:
        ts, data = _cache[key]
        if (datetime.now() - ts).total_seconds() < CACHE_TTL:
            return data
    return None


def _set_cache(key: str, data: object) -> None:
    _cache[key] = (datetime.now(), data)


def _read_parquet() -> pd.DataFrame:
    if _IS_LOCAL:
        if not ALLOCATION_PATH.exists():
            return pd.DataFrame()
        return pd.read_parquet(ALLOCATION_PATH)

    import boto3

    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/allocation.parquet")
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return pd.DataFrame()


@router.get("")
def get_allocation():
    cached = _cached("allocation")
    if cached is not None:
        return cached

    df = _read_parquet()
    if df.empty:
        result = {"signals": [], "summary": {}}
        _set_cache("allocation", result)
        return result

    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].astype(str)

    signals = df.to_dict(orient="records")

    level_counts = df["rec_level"].value_counts().to_dict() if "rec_level" in df.columns else {}
    dir_counts = {}
    if "net_direction" in df.columns:
        for d in ("long", "short", "neutral"):
            subset = df[df["net_direction"] == d]
            dir_counts[d] = sorted(subset["strategy"].unique().tolist()) if not subset.empty else []

    summary = {
        "target_date": str(df["signal_date"].iloc[0]) if "signal_date" in df.columns else None,
        "total_signals": len(df),
        "active_signals": int((~df["blocked"]).sum()) if "blocked" in df.columns else len(df),
        "blocked_signals": int(df["blocked"].sum()) if "blocked" in df.columns else 0,
        "level_counts": level_counts,
        "vi_close": float(df["vi_close"].iloc[0]) if "vi_close" in df.columns and pd.notna(df["vi_close"].iloc[0]) else None,
        "vi_regime": str(df["vi_regime"].iloc[0]) if "vi_regime" in df.columns else None,
        "dd_daily_pct": float(df["dd_daily_pct"].iloc[0]) if "dd_daily_pct" in df.columns else None,
        "dd_20d_pct": float(df["dd_20d_pct"].iloc[0]) if "dd_20d_pct" in df.columns else None,
        "cme_change_pct": float(df["cme_change_pct"].iloc[0]) if "cme_change_pct" in df.columns and pd.notna(df["cme_change_pct"].iloc[0]) else None,
        "strategies_by_direction": dir_counts,
    }

    result = {"signals": signals, "summary": summary}
    _set_cache("allocation", result)
    return result
