"""
開発用: 日中分析API

事前計算済みデータ (intraday_analysis.parquet) を使用
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi_cache.decorator import cache
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Optional
import os
import tempfile
import math

router = APIRouter()


def sanitize_for_json(obj):
    """NaN/Inf を None に変換（JSONシリアライズ対応）"""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif pd.isna(obj):
        return None
    return obj

# データキャッシュ
_data_cache: dict = {}

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
INTRADAY_PATH = BASE_DIR / "data" / "parquet" / "intraday_analysis.parquet"
PRICES_5M_PATH = BASE_DIR / "data" / "parquet" / "prices_60d_5m.parquet"
PRICES_1D_PATH = BASE_DIR / "data" / "parquet" / "prices_max_1d.parquet"
INDEX_5M_PATH = BASE_DIR / "data" / "parquet" / "index_prices_60d_5m.parquet"
INDEX_1D_PATH = BASE_DIR / "data" / "parquet" / "index_prices_max_1d.parquet"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_parquet_with_s3_fallback(local_path: Path, s3_key: str) -> pd.DataFrame:
    """ローカルファイルがなければS3から取得"""
    cache_key = s3_key
    if cache_key in _data_cache:
        return _data_cache[cache_key]

    if local_path.exists():
        df = pd.read_parquet(local_path)
        _data_cache[cache_key] = df
        return df

    try:
        import boto3
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            s3_client.download_fileobj(S3_BUCKET, s3_key, tmp_file)
            tmp_path = tmp_file.name
        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        _data_cache[cache_key] = df
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"データ読み込みエラー: {str(e)}")


def load_intraday_table(ticker: str) -> list:
    """事前計算済みテーブルを読み込み"""
    df = load_parquet_with_s3_fallback(INTRADAY_PATH, "parquet/intraday_analysis.parquet")
    df_ticker = df[df["ticker"] == ticker].copy()

    if len(df_ticker) == 0:
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} のデータが見つかりません")

    # 新しい順にソート
    df_ticker = df_ticker.sort_values("date", ascending=False)

    # tickerカラムを除外してdict化
    cols = [c for c in df_ticker.columns if c != "ticker"]
    # NaN → None に変換（JSONシリアライズ対応）
    records = df_ticker[cols].to_dict("records")
    for row in records:
        for key, val in row.items():
            if pd.isna(val):
                row[key] = None
    return records


def calc_summary(table: list) -> dict:
    """サマリー統計を計算"""
    if len(table) == 0:
        return {}

    high_am_count = sum(1 for row in table if row.get("highTime") and row["highTime"] < "12:30")
    low_am_count = sum(1 for row in table if row.get("lowTime") and row["lowTime"] < "12:30")
    long_wins = sum(1 for row in table if row.get("dayPnl") is not None and row["dayPnl"] > 0)
    am_wins = sum(1 for row in table if row.get("amPnl") is not None and row["amPnl"] > 0)
    am_count = sum(1 for row in table if row.get("amPnl") is not None)

    total = len(table)

    return {
        "highAmPct": round(high_am_count / total * 100, 1) if total > 0 else 0,
        "lowAmPct": round(low_am_count / total * 100, 1) if total > 0 else 0,
        "longWinRate": round(long_wins / total * 100, 1) if total > 0 else 0,
        "amLongWinRate": round(am_wins / am_count * 100, 1) if am_count > 0 else 0,
        "totalDays": total,
    }


def calc_normalized_prices(ticker: str, date_str: str) -> list:
    """指定日の正規化価格を計算"""
    try:
        df_5m = load_parquet_with_s3_fallback(PRICES_5M_PATH, "parquet/prices_60d_5m.parquet")
        df_1d = load_parquet_with_s3_fallback(PRICES_1D_PATH, "parquet/prices_max_1d.parquet")

        df_5m = df_5m[df_5m["ticker"] == ticker].copy()
        df_1d = df_1d[df_1d["ticker"] == ticker].copy()

        if len(df_5m) == 0 or len(df_1d) == 0:
            return []

        df_5m["date"] = pd.to_datetime(df_5m["date"])
        df_1d["date"] = pd.to_datetime(df_1d["date"])

        target_date = pd.Timestamp(date_str).date()
        df_1d_sorted = df_1d.sort_values("date")
        prev_dates = df_1d_sorted[df_1d_sorted["date"].dt.date < target_date]

        if len(prev_dates) == 0:
            return []

        prev_close = prev_dates.iloc[-1]["Close"]
        day_5m = df_5m[df_5m["date"].dt.date == target_date].copy()

        if len(day_5m) == 0:
            return []

        day_5m = day_5m.sort_values("date")
        day_5m["time"] = day_5m["date"].dt.strftime("%H:%M")
        day_5m["value"] = (day_5m["Close"] / prev_close * 100).round(2)

        # NaN/nullを除外
        day_5m = day_5m[day_5m["value"].notna()]
        return day_5m[["time", "value"]].to_dict("records")
    except:
        return []


def calc_index_normalized(index_ticker: str, date_str: str) -> list:
    """指数の正規化価格を計算"""
    try:
        df_5m = load_parquet_with_s3_fallback(INDEX_5M_PATH, "parquet/index_prices_60d_5m.parquet")
        df_1d = load_parquet_with_s3_fallback(INDEX_1D_PATH, "parquet/index_prices_max_1d.parquet")

        df_5m = df_5m[df_5m["ticker"] == index_ticker].copy()
        df_1d = df_1d[df_1d["ticker"] == index_ticker].copy()

        if len(df_5m) == 0 or len(df_1d) == 0:
            return []

        df_5m["date"] = pd.to_datetime(df_5m["date"])
        df_1d["date"] = pd.to_datetime(df_1d["date"])

        target_date = pd.Timestamp(date_str).date()
        df_1d_sorted = df_1d.sort_values("date")
        prev_dates = df_1d_sorted[df_1d_sorted["date"].dt.date < target_date]

        if len(prev_dates) == 0:
            return []

        prev_close = prev_dates.iloc[-1]["Close"]
        day_5m = df_5m[df_5m["date"].dt.date == target_date].copy()

        if len(day_5m) == 0:
            return []

        day_5m = day_5m.sort_values("date")
        day_5m["time"] = day_5m["date"].dt.strftime("%H:%M")
        day_5m["value"] = (day_5m["Close"] / prev_close * 100).round(2)

        # NaN/nullを除外
        day_5m = day_5m[day_5m["value"].notna()]
        return day_5m[["time", "value"]].to_dict("records")
    except:
        return []


@router.get("/dev/intraday-analysis")
@cache(expire=1800)
async def get_intraday_analysis(
    ticker: str = Query(..., description="ティッカー（例: 7729.T）"),
    date: Optional[str] = Query(None, description="正規化価格の基準日（YYYY-MM-DD）"),
):
    """
    高値安値時間帯分析（事前計算済みデータ使用）
    """
    # 事前計算済みテーブルを読み込み
    table = load_intraday_table(ticker)

    # 基準日
    if date is None:
        date = table[0]["date"] if len(table) > 0 else None

    # 正規化価格（チャート用）
    normalized_ticker = calc_normalized_prices(ticker, date) if date else []
    normalized_nikkei = calc_index_normalized("^N225", date) if date else []
    normalized_topix = calc_index_normalized("1489.T", date) if date else []

    # サマリー
    summary = calc_summary(table)

    response_data = {
        "table": table,
        "normalizedPrices": {
            "date": date,
            "ticker": normalized_ticker,
            "nikkei": normalized_nikkei,
            "topix": normalized_topix,
        },
        "summary": summary,
        "meta": {
            "generatedAt": datetime.now().isoformat(),
            "ticker": ticker,
        },
    }
    return JSONResponse(content=sanitize_for_json(response_data))
