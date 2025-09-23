# -*- coding: utf-8 -*-
"""
app.data_loader: Parquet 読み込み（S3 or ローカル）
- S3 が設定済みなら S3 を優先、失敗時はローカルへフォールバック
"""
from io import BytesIO
from pathlib import Path
import sys
import pandas as pd
from .config import AppConfig


# ローカル既定パス（分析側で生成）
PARQUET_DIR = Path("./data/parquet")
META_PARQUET_LOCAL = PARQUET_DIR / "core30_meta.parquet"
PRICES_PARQUET_LOCAL = PARQUET_DIR / "core30_prices_1y_1d.parquet"
ANOMALY_PARQUET_LOCAL = PARQUET_DIR / "core30_anomaly.parquet"


def _read_parquet_local(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"not found: {path}")
    return pd.read_parquet(path, engine="pyarrow")


def _read_parquet_s3(cfg: AppConfig, key: str) -> pd.DataFrame:
    import boto3
    session_kwargs: dict = {}
    if cfg.aws_profile:
        session_kwargs["profile_name"] = cfg.aws_profile
    session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()

    client_kwargs: dict = {}
    if cfg.aws_region:
        client_kwargs["region_name"] = cfg.aws_region
    if cfg.aws_endpoint_url:
        client_kwargs["endpoint_url"] = cfg.aws_endpoint_url

    s3 = session.client("s3", **client_kwargs)
    obj = s3.get_object(Bucket=cfg.data_bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_parquet(BytesIO(data), engine="pyarrow")


def load_meta_and_prices(cfg: AppConfig) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    戻り値: (meta_df, prices_df, source)  # source in {"s3", "local"}
    """
    if cfg.data_bucket and cfg.core30_meta_key and cfg.core30_prices_key:
        try:
            meta = _read_parquet_s3(cfg, cfg.core30_meta_key)
            prices = _read_parquet_s3(cfg, cfg.core30_prices_key)
            return meta, prices, "s3"
        except Exception as e:
            print(f"[WARN] S3 読み込みに失敗。ローカルへフォールバック: {e}", file=sys.stderr)

    meta = _read_parquet_local(META_PARQUET_LOCAL)
    prices = _read_parquet_local(PRICES_PARQUET_LOCAL)
    return meta, prices, "local"


def load_anomaly(cfg: AppConfig) -> tuple[pd.DataFrame, str]:
    """
    戻り値: (anomaly_df, source)  # source in {"s3", "local", "none"}
    """
    if cfg.data_bucket and cfg.core30_anomaly_key:
        try:
            df = _read_parquet_s3(cfg, cfg.core30_anomaly_key)
            return df, "s3"
        except Exception as e:
            print(f"[WARN] anomaly S3 読み込み失敗。ローカルへフォールバック: {e}", file=sys.stderr)

    try:
        df = _read_parquet_local(ANOMALY_PARQUET_LOCAL)
        return df, "local"
    except Exception as e:
        print(f"[WARN] anomaly ローカル読み込み失敗: {e}", file=sys.stderr)
        import pandas as pd
        return pd.DataFrame(columns=["ticker", "year", "month", "return_pct"]), "none"
