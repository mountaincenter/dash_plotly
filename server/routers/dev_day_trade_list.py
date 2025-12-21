"""
開発用: デイトレードリスト管理API

grok_day_trade_list.parquet の読み込み・編集を提供
- GET /dev/day-trade-list: 一覧取得
- PUT /dev/day-trade-list/{ticker}: 個別銘柄のフラグ更新
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from pathlib import Path
import pandas as pd
import tempfile
import os

router = APIRouter()

# ファイル名
FILENAME = "grok_day_trade_list.parquet"
LOCAL_PATH = Path(__file__).resolve().parents[2] / "data" / "parquet" / FILENAME


def load_day_trade_list() -> pd.DataFrame:
    """ローカルまたはS3からデイトレードリストを読み込み"""
    # ローカルファイルが存在すればそれを使用
    if LOCAL_PATH.exists():
        return pd.read_parquet(LOCAL_PATH)

    # S3から取得
    try:
        import boto3
        from botocore.exceptions import ClientError

        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = f"parquet/{FILENAME}"
        region = os.getenv("AWS_REGION", "ap-northeast-1")

        s3_client = boto3.client("s3", region_name=region)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            s3_client.download_fileobj(bucket, key, tmp_file)
            tmp_path = tmp_file.name

        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        return df

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail="grok_day_trade_list.parquet が見つかりません")
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")


def save_day_trade_list(df: pd.DataFrame) -> None:
    """ローカルとS3にデイトレードリストを保存"""
    import io

    # ローカルに保存（Dockerの場合はマウントされている）
    LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(LOCAL_PATH, index=False)

    # S3にも保存
    try:
        import boto3

        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = f"parquet/{FILENAME}"
        region = os.getenv("AWS_REGION", "ap-northeast-1")

        s3_client = boto3.client("s3", region_name=region)

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    except Exception as e:
        # S3保存失敗はログに出すが、ローカル保存は成功しているのでエラーにしない
        print(f"Warning: S3保存失敗（ローカルには保存済み）: {str(e)}")


class DayTradeUpdateRequest(BaseModel):
    """銘柄フラグ更新リクエスト"""
    shortable: Optional[bool] = None  # 制度信用（空売り可）
    day_trade: Optional[bool] = None  # いちにち信用対象
    ng: Optional[bool] = None         # トレード不可
    day_trade_available_shares: Optional[int] = None  # 1人当たり売り可能株数


def load_trading_recommendation_full() -> dict:
    """trading_recommendation.json全体を読み込み"""
    import json

    local_path = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest" / "trading_recommendation.json"
    if local_path.exists():
        with open(local_path, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        import boto3
        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = "parquet/backtest/trading_recommendation.json"
        region = os.getenv("AWS_REGION", "ap-northeast-1")
        s3_client = boto3.client("s3", region_name=region)

        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"trading_recommendation読み込みエラー: {str(e)}")


def load_trading_recommendation() -> list[dict]:
    """trading_recommendation.jsonを読み込み（stocks部分のみ）"""
    data = load_trading_recommendation_full()
    return data.get("stocks", [])


def save_trading_recommendation(data: dict) -> None:
    """trading_recommendation.jsonを保存（ローカルとS3）"""
    import json

    local_path = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest" / "trading_recommendation.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    try:
        import boto3
        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = "parquet/backtest/trading_recommendation.json"
        region = os.getenv("AWS_REGION", "ap-northeast-1")
        s3_client = boto3.client("s3", region_name=region)

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
    except Exception as e:
        print(f"Warning: S3保存失敗（ローカルには保存済み）: {str(e)}")


@router.get("/dev/day-trade-list")
async def get_day_trade_list():
    """
    デイトレードリスト一覧を取得（trading_recommendation.jsonとマージ）

    Returns:
    - total: 総銘柄数
    - summary: {shortable, day_trade, ng}
    - stocks: 銘柄リスト [{ticker, stock_name, grok_rank, close, change_pct, atr_pct, shortable, day_trade, ng}, ...]
    """
    day_trade_df = load_day_trade_list()
    recommendation_stocks = load_trading_recommendation()

    # day_trade_listをdictに変換（tickerでルックアップ）
    dtl_map = {row["ticker"]: row for _, row in day_trade_df.iterrows()}

    # マージ
    stocks = []
    for rec in recommendation_stocks:
        stock_name = rec.get("stock_name", "")
        ticker = rec.get("ticker", "")
        dtl = dtl_map.get(ticker, {})

        # デフォルト値
        shortable = bool(dtl.get("shortable", False))
        day_trade = bool(dtl.get("day_trade", True))
        ng = bool(dtl.get("ng", False))

        # 売り可能株数（None許容）
        day_trade_available_shares = dtl.get("day_trade_available_shares")
        if pd.notna(day_trade_available_shares):
            day_trade_available_shares = int(day_trade_available_shares)
        else:
            day_trade_available_shares = None

        # 時価総額（億円換算）
        market_cap = rec.get("market_cap")
        market_cap_oku = round(market_cap / 1e8, 0) if market_cap else None

        # 500-1000億は見送り判定
        skip_by_market_cap = False
        if market_cap_oku and 500 <= market_cap_oku < 1000:
            skip_by_market_cap = True

        stocks.append({
            "ticker": rec.get("ticker", ""),
            "stock_name": stock_name,
            "grok_rank": rec.get("grok_rank"),
            "close": rec.get("prev_day_close"),
            "change_pct": rec.get("prev_day_change_pct"),
            "atr_pct": rec.get("atr_pct"),
            "market_cap_oku": market_cap_oku,
            "skip_by_market_cap": skip_by_market_cap,
            "shortable": shortable,
            "day_trade": day_trade,
            "ng": ng,
            "day_trade_available_shares": day_trade_available_shares,
        })

    # ソート: 制度 → いちにち → NG → grok_rank
    stocks.sort(key=lambda s: (
        0 if s["shortable"] else (1 if s["day_trade"] else 2),
        s.get("grok_rank") or 999
    ))

    summary = {
        "unchecked": sum(1 for s in stocks if not s["shortable"] and not s["day_trade"] and not s["ng"]),
        "shortable": sum(1 for s in stocks if s["shortable"]),
        "day_trade": sum(1 for s in stocks if s["day_trade"] and not s["shortable"]),
        "ng": sum(1 for s in stocks if s["ng"]),
        "skip_by_market_cap": sum(1 for s in stocks if s["skip_by_market_cap"]),
    }

    return JSONResponse(content={
        "total": len(stocks),
        "summary": summary,
        "stocks": stocks
    })


@router.get("/dev/day-trade-list/{ticker}")
async def get_day_trade_item(ticker: str):
    """
    個別銘柄のフラグを取得

    Parameters:
    - ticker: ティッカーシンボル (例: 2492)
    """
    df = load_day_trade_list()

    # tickerで検索（文字列として比較）
    ticker_str = str(ticker)
    match = df[df["ticker"].astype(str) == ticker_str]

    if match.empty:
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} が見つかりません")

    stock = match.iloc[0].to_dict()
    return JSONResponse(content=stock)


@router.put("/dev/day-trade-list/{ticker}")
async def update_day_trade_item(ticker: str, request: DayTradeUpdateRequest):
    """
    個別銘柄のフラグを更新

    Parameters:
    - ticker: ティッカーシンボル (例: 2492)
    - request: 更新するフラグ (shortable, day_trade, ng)

    Note:
    - 指定されたフィールドのみ更新（None は無視）
    - grok_day_trade_list.parquet と trading_recommendation.json の両方を更新
    """
    df = load_day_trade_list()

    # tickerで検索
    ticker_str = str(ticker)
    mask = df["ticker"].astype(str) == ticker_str

    if not mask.any():
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} が見つかりません")

    # 更新
    if request.shortable is not None:
        df.loc[mask, "shortable"] = request.shortable
    if request.day_trade is not None:
        df.loc[mask, "day_trade"] = request.day_trade
    if request.ng is not None:
        df.loc[mask, "ng"] = request.ng
    if request.day_trade_available_shares is not None:
        df.loc[mask, "day_trade_available_shares"] = request.day_trade_available_shares

    # grok_day_trade_list.parquet を保存
    save_day_trade_list(df)

    # trading_recommendation.json も更新
    try:
        rec_data = load_trading_recommendation_full()
        stocks = rec_data.get("stocks", [])

        # stock_name（銘柄コード）で検索して更新
        for stock in stocks:
            if stock.get("stock_name") == ticker_str or stock.get("ticker") == ticker_str:
                if request.shortable is not None:
                    stock["shortable"] = request.shortable
                if request.day_trade is not None:
                    stock["day_trade"] = request.day_trade
                if request.ng is not None:
                    stock["ng"] = request.ng
                if request.day_trade_available_shares is not None:
                    stock["day_trade_available_shares"] = request.day_trade_available_shares
                break

        rec_data["stocks"] = stocks
        save_trading_recommendation(rec_data)
    except Exception as e:
        print(f"Warning: trading_recommendation.json更新失敗: {str(e)}")

    # 更新後のデータを返す
    updated = df[mask].iloc[0].to_dict()
    return JSONResponse(content={
        "message": "更新しました",
        "stock": updated
    })


@router.put("/dev/day-trade-list")
async def bulk_update_day_trade_list(updates: list[dict]):
    """
    複数銘柄を一括更新

    Parameters:
    - updates: [{ticker, shortable?, day_trade?, ng?}, ...]

    Returns:
    - updated: 更新された銘柄数
    - errors: エラーがあった銘柄

    Note:
    - grok_day_trade_list.parquet と trading_recommendation.json の両方を更新
    """
    df = load_day_trade_list()

    updated_count = 0
    errors = []
    updated_tickers = []

    for item in updates:
        ticker = str(item.get("ticker", ""))
        mask = df["ticker"].astype(str) == ticker

        if not mask.any():
            errors.append({"ticker": ticker, "error": "見つかりません"})
            continue

        if "shortable" in item:
            df.loc[mask, "shortable"] = item["shortable"]
        if "day_trade" in item:
            df.loc[mask, "day_trade"] = item["day_trade"]
        if "ng" in item:
            df.loc[mask, "ng"] = item["ng"]
        if "day_trade_available_shares" in item:
            df.loc[mask, "day_trade_available_shares"] = item["day_trade_available_shares"]

        updated_count += 1
        updated_tickers.append((ticker, item))

    # grok_day_trade_list.parquet を保存
    save_day_trade_list(df)

    # trading_recommendation.json も更新
    try:
        rec_data = load_trading_recommendation_full()
        stocks = rec_data.get("stocks", [])

        for ticker, item in updated_tickers:
            for stock in stocks:
                if stock.get("stock_name") == ticker or stock.get("ticker") == ticker:
                    if "shortable" in item:
                        stock["shortable"] = item["shortable"]
                    if "day_trade" in item:
                        stock["day_trade"] = item["day_trade"]
                    if "ng" in item:
                        stock["ng"] = item["ng"]
                    if "day_trade_available_shares" in item:
                        stock["day_trade_available_shares"] = item["day_trade_available_shares"]
                    break

        rec_data["stocks"] = stocks
        save_trading_recommendation(rec_data)
    except Exception as e:
        print(f"Warning: trading_recommendation.json更新失敗: {str(e)}")

    return JSONResponse(content={
        "updated": updated_count,
        "errors": errors
    })
