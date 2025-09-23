# -*- coding: utf-8 -*-
"""
app.config: Dashアプリ用の環境設定リーダ
- PORT, DEBUG
- S3 読み取り系（DATA_BUCKET, CORE30_*_KEY, AWS_REGION, AWS_PROFILE, AWS_ENDPOINT_URL）
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    port: int
    debug: bool
    # S3 読み取り
    data_bucket: str | None
    core30_meta_key: str | None
    core30_prices_key: str | None
    core30_anomaly_key: str | None
    aws_region: str | None
    aws_profile: str | None
    aws_endpoint_url: str | None  # LocalStack/MinIO 等を使う場合に利用（任意）


def load_app_config() -> AppConfig:
    port = int(os.getenv("PORT", "8050"))
    debug = os.getenv("DEBUG", "false").lower() == "true"

    return AppConfig(
        port=port,
        debug=debug,
        data_bucket=os.getenv("DATA_BUCKET") or None,
        core30_meta_key=os.getenv("CORE30_META_KEY") or None,
        core30_prices_key=os.getenv("CORE30_PRICES_KEY") or None,
        core30_anomaly_key=os.getenv("CORE30_ANOMALY_KEY") or None,
        aws_region=os.getenv("AWS_REGION") or None,
        aws_profile=os.getenv("AWS_PROFILE") or None,
        aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL") or None,
    )
