# common_cfg/s3cfg.py
# -*- coding: utf-8 -*-
"""
common_cfg.s3cfg: S3 環境変数読み
"""
import os
from dataclasses import dataclass

try:
    from dotenv import dotenv_values  # type: ignore
except Exception:  # pragma: no cover
    dotenv_values = None

try:
    from .env import _iter_search_dirs  # type: ignore
except Exception:  # pragma: no cover
    _iter_search_dirs = None

@dataclass(frozen=True)
class S3Config:
    bucket: str | None
    prefix: str
    region: str | None
    profile: str | None
    endpoint_url: str | None  # LocalStack/MinIO用 任意

def _env_fallback(key: str) -> str | None:
    if not dotenv_values or not _iter_search_dirs:
        return None
    for base in _iter_search_dirs():
        for name in (".env.s3", ".env.dev"):
            path = base / name
            if path.exists():
                values = dotenv_values(path, verbose=False) or {}
                val = values.get(key)
                if val:
                    return val
    return None

def load_s3_config() -> S3Config:
    # GitHub Actions uses S3_BUCKET, local dev may use DATA_BUCKET
    bucket = os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET") or _env_fallback("S3_BUCKET") or _env_fallback("DATA_BUCKET")
    prefix = os.getenv("PARQUET_PREFIX") or _env_fallback("PARQUET_PREFIX") or "parquet/"
    region = os.getenv("AWS_REGION") or _env_fallback("AWS_REGION")
    profile = os.getenv("AWS_PROFILE") or _env_fallback("AWS_PROFILE")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL") or _env_fallback("AWS_ENDPOINT_URL")
    return S3Config(
        bucket=bucket or None,
        prefix=prefix,
        region=region or None,
        profile=profile or None,
        endpoint_url=endpoint_url or None,
    )

# ---- 後方互換：定数としても参照できるように（ノートブック互換）----
_cfg = load_s3_config()
DATA_BUCKET = _cfg.bucket
PARQUET_PREFIX = _cfg.prefix
AWS_REGION = _cfg.region
AWS_PROFILE = _cfg.profile
AWS_ENDPOINT_URL = _cfg.endpoint_url
