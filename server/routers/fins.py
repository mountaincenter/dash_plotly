# server/routers/fins.py
"""
J-Quants財務データエンドポイント
financials.parquetからの読み込み（S3フォールバック付き）
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/fins")

# ==============================
# パス・S3設定
# ==============================
PARQUET_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "parquet"
FINANCIALS_PATH = PARQUET_DIR / "financials.parquet"
ANNOUNCEMENTS_PATH = PARQUET_DIR / "announcements.parquet"


def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else None


_S3_BUCKET = _get_env("DATA_BUCKET")
_S3_PREFIX_RAW = _get_env("PARQUET_PREFIX")
_S3_PREFIX = (_S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet")
_AWS_REGION = _get_env("AWS_REGION")
_AWS_PROFILE = _get_env("AWS_PROFILE")
_AWS_ENDPOINT = _get_env("AWS_ENDPOINT_URL")


def _s3_key(filename: str) -> str:
    return f"{_S3_PREFIX}/{filename}" if _S3_PREFIX else filename


# ==============================
# データ読み込み
# ==============================
def _read_parquet_local(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(str(path), engine="pyarrow")
    except Exception:
        return None


def _read_parquet_s3(bucket: Optional[str], key: Optional[str]) -> Optional[pd.DataFrame]:
    if not bucket or not key:
        return None
    try:
        import boto3
        from io import BytesIO

        session_kwargs = {}
        if _AWS_PROFILE:
            session_kwargs["profile_name"] = _AWS_PROFILE
        session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()

        client_kwargs = {}
        if _AWS_REGION:
            client_kwargs["region_name"] = _AWS_REGION
        if _AWS_ENDPOINT:
            client_kwargs["endpoint_url"] = _AWS_ENDPOINT

        s3 = session.client("s3", **client_kwargs)
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_parquet(BytesIO(data), engine="pyarrow")
    except Exception as e:
        print(f"!!! S3 READ ERROR: Failed to read s3://{bucket}/{key}. Error: {e}")
        return None


def load_financials_df() -> Optional[pd.DataFrame]:
    """financials.parquetを読み込む（ローカル優先、S3フォールバック）"""
    df = _read_parquet_local(FINANCIALS_PATH)
    if (df is None or df.empty) and _S3_BUCKET:
        s3_key = _s3_key("financials.parquet")
        df = _read_parquet_s3(_S3_BUCKET, s3_key)
    return df


def load_announcements_df() -> Optional[pd.DataFrame]:
    """announcements.parquetを読み込む（ローカル優先、S3フォールバック）"""
    df = _read_parquet_local(ANNOUNCEMENTS_PATH)
    if (df is None or df.empty) and _S3_BUCKET:
        s3_key = _s3_key("announcements.parquet")
        df = _read_parquet_s3(_S3_BUCKET, s3_key)
    return df


# ==============================
# エンドポイント
# ==============================
@router.get("/summary/{ticker}")
@cache(expire=3600)
async def get_financial_summary(ticker: str) -> dict[str, Any]:
    """銘柄の財務サマリーを取得（financials.parquetから）"""
    # ticker正規化: "7203" -> "7203.T", "7203.T" -> "7203.T"
    normalized_ticker = ticker if ticker.endswith(".T") else f"{ticker}.T"

    df = load_financials_df()
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Financial data not available")

    # tickerで検索
    row = df[df["ticker"] == normalized_ticker]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Financial data not found for {ticker}")

    # 最初のレコードを取得
    record = row.iloc[0]

    def safe_value(val: Any) -> Any:
        """NaN/Noneをnullに変換"""
        if pd.isna(val):
            return None
        return val

    return {
        "ticker": normalized_ticker,
        "fiscalPeriod": safe_value(record.get("fiscalPeriod")),
        "periodEnd": safe_value(record.get("periodEnd")),
        "disclosureDate": safe_value(record.get("disclosureDate")),
        "sales": safe_value(record.get("sales")),
        "operatingProfit": safe_value(record.get("operatingProfit")),
        "ordinaryProfit": safe_value(record.get("ordinaryProfit")),
        "netProfit": safe_value(record.get("netProfit")),
        "eps": safe_value(record.get("eps")),
        "totalAssets": safe_value(record.get("totalAssets")),
        "equity": safe_value(record.get("equity")),
        "equityRatio": safe_value(record.get("equityRatio")),
        "bps": safe_value(record.get("bps")),
        "sharesOutstanding": safe_value(record.get("sharesOutstanding")),
    }


@router.get("/all")
@cache(expire=3600)
async def get_all_financials() -> list[dict[str, Any]]:
    """全銘柄の財務サマリーを取得"""
    df = load_financials_df()
    if df is None or df.empty:
        return []

    # NaNをNoneに変換
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")


@router.get("/announcement/{ticker}")
@cache(expire=3600)
async def get_announcement(ticker: str) -> dict[str, Any]:
    """銘柄の次回決算発表予定日を取得"""
    normalized_ticker = ticker if ticker.endswith(".T") else f"{ticker}.T"

    df = load_announcements_df()
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Announcement data not available")

    row = df[df["ticker"] == normalized_ticker]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Announcement not found for {ticker}")

    record = row.iloc[0]

    def safe_value(val: Any) -> Any:
        if pd.isna(val):
            return None
        return val

    return {
        "ticker": normalized_ticker,
        "announcementDate": safe_value(record.get("announcementDate")),
        "nextQuarter": safe_value(record.get("nextQuarter")),
        "confidence": safe_value(record.get("confidence")),
    }
