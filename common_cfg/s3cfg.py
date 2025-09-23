# common_cfg/s3cfg.py
# -*- coding: utf-8 -*-
"""
common_cfg.s3cfg: S3 環境変数読み
"""
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class S3Config:
    bucket: str | None
    prefix: str
    region: str | None
    profile: str | None
    endpoint_url: str | None  # LocalStack/MinIO用 任意

def load_s3_config() -> S3Config:
    return S3Config(
        bucket=os.getenv("DATA_BUCKET") or None,
        prefix=os.getenv("PARQUET_PREFIX", "parquet/"),
        region=os.getenv("AWS_REGION") or None,
        profile=os.getenv("AWS_PROFILE") or None,
        endpoint_url=os.getenv("AWS_ENDPOINT_URL") or None,
    )

# ---- 後方互換：定数としても参照できるように（ノートブック互換）----
_cfg = load_s3_config()
DATA_BUCKET = _cfg.bucket
PARQUET_PREFIX = _cfg.prefix
AWS_REGION = _cfg.region
AWS_PROFILE = _cfg.profile
AWS_ENDPOINT_URL = _cfg.endpoint_url
