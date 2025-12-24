"""
開発用: デイトレードリスト管理API

grok_trending.parquet と grok_day_trade_list.parquet を使用
- GET /dev/day-trade-list: 一覧取得（grok_trending.parquetから）
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
import io

router = APIRouter()

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
DAY_TRADE_LIST_PATH = BASE_DIR / "data" / "parquet" / "grok_day_trade_list.parquet"
GROK_TRENDING_PATH = BASE_DIR / "data" / "parquet" / "grok_trending.parquet"
GROK_ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"

# 曜日名
WEEKDAY_NAMES = ['月', '火', '水', '木', '金', '土', '日']


def load_day_trade_list() -> pd.DataFrame:
    """ローカルまたはS3からデイトレードリストを読み込み"""
    if DAY_TRADE_LIST_PATH.exists():
        return pd.read_parquet(DAY_TRADE_LIST_PATH)

    # S3から取得
    try:
        import boto3
        from botocore.exceptions import ClientError

        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = "parquet/grok_day_trade_list.parquet"
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
    import boto3

    # ローカルに保存
    DAY_TRADE_LIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(DAY_TRADE_LIST_PATH, index=False)

    # S3にも保存（必須）
    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    key = "parquet/grok_day_trade_list.parquet"
    region = os.getenv("AWS_REGION", "ap-northeast-1")

    s3_client = boto3.client("s3", region_name=region)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def load_grok_trending() -> pd.DataFrame:
    """ローカルまたはS3からgrok_trending.parquetを読み込み"""
    if GROK_TRENDING_PATH.exists():
        return pd.read_parquet(GROK_TRENDING_PATH)

    # S3から取得
    try:
        import boto3
        from botocore.exceptions import ClientError

        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = "parquet/grok_trending.parquet"
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
            raise HTTPException(status_code=404, detail="grok_trending.parquet が見つかりません")
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")


def load_grok_archive() -> pd.DataFrame:
    """ローカルまたはS3からgrok_trending_archive.parquetを読み込み"""
    if GROK_ARCHIVE_PATH.exists():
        return pd.read_parquet(GROK_ARCHIVE_PATH)

    # S3から取得
    try:
        import boto3
        from botocore.exceptions import ClientError

        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = "parquet/backtest/grok_trending_archive.parquet"
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
            return pd.DataFrame()  # 空のDataFrameを返す
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")


def save_grok_trending(df: pd.DataFrame) -> None:
    """ローカルとS3にgrok_trending.parquetを保存"""
    import boto3

    # ローカルに保存
    GROK_TRENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(GROK_TRENDING_PATH, index=False)

    # S3にも保存
    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    key = "parquet/grok_trending.parquet"
    region = os.getenv("AWS_REGION", "ap-northeast-1")

    s3_client = boto3.client("s3", region_name=region)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


class DayTradeUpdateRequest(BaseModel):
    """銘柄フラグ更新リクエスト"""
    shortable: Optional[bool] = None  # 制度信用（空売り可）
    day_trade: Optional[bool] = None  # いちにち信用対象
    ng: Optional[bool] = None         # トレード不可
    day_trade_available_shares: Optional[int] = None  # 1人当たり売り可能株数


@router.get("/dev/day-trade-list")
async def get_day_trade_list():
    """
    デイトレードリスト一覧を取得（grok_trending.parquetから）

    Returns:
    - total: 総銘柄数
    - summary: {shortable, day_trade, ng}
    - stocks: 銘柄リスト（appearance_count付き）
    """
    grok_df = load_grok_trending()
    day_trade_df = load_day_trade_list()

    # archiveから登場回数を計算（2025-11-04以降）
    try:
        archive_df = load_grok_archive()
        if not archive_df.empty:
            archive_df = archive_df[archive_df['selection_date'] >= '2025-11-04']
            appearance_counts = archive_df['ticker'].value_counts().to_dict()
        else:
            appearance_counts = {}
    except Exception:
        appearance_counts = {}

    # day_trade_listをdictに変換（tickerでルックアップ）
    dtl_map = {row["ticker"]: row for _, row in day_trade_df.iterrows()}

    stocks = []
    for _, row in grok_df.iterrows():
        ticker = row.get("ticker", "")
        dtl = dtl_map.get(ticker, {})

        # grok_trending.parquetに信用区分カラムがあればそれを使用、なければday_trade_listから
        if "shortable" in row and pd.notna(row["shortable"]):
            shortable = bool(row["shortable"])
        else:
            shortable = bool(dtl.get("shortable", False))

        if "day_trade" in row and pd.notna(row["day_trade"]):
            day_trade = bool(row["day_trade"])
        else:
            day_trade = bool(dtl.get("day_trade", False))

        if "ng" in row and pd.notna(row["ng"]):
            ng = bool(row["ng"])
        else:
            ng = bool(dtl.get("ng", False))

        # 売り可能株数（None許容）
        if "day_trade_available_shares" in row and pd.notna(row["day_trade_available_shares"]):
            day_trade_available_shares = int(row["day_trade_available_shares"])
        elif "day_trade_available_shares" in dtl and pd.notna(dtl.get("day_trade_available_shares")):
            day_trade_available_shares = int(dtl["day_trade_available_shares"])
        else:
            day_trade_available_shares = None

        # 時価総額（億円換算）
        market_cap = row.get("market_cap")
        if pd.notna(market_cap):
            market_cap_oku = round(market_cap / 1e8, 0)
        else:
            market_cap_oku = None

        # 500-1000億は見送り判定
        skip_by_market_cap = False
        if market_cap_oku and 500 <= market_cap_oku < 1000:
            skip_by_market_cap = True

        stocks.append({
            "ticker": ticker,
            "stock_name": row.get("stock_name", ""),
            "grok_rank": row.get("grok_rank") if pd.notna(row.get("grok_rank")) else None,
            "close": row.get("Close") if pd.notna(row.get("Close")) else None,
            "change_pct": row.get("change_pct") if pd.notna(row.get("change_pct")) else None,
            "atr_pct": row.get("atr14_pct") if pd.notna(row.get("atr14_pct")) else None,
            "market_cap_oku": market_cap_oku,
            "skip_by_market_cap": skip_by_market_cap,
            "shortable": shortable,
            "day_trade": day_trade,
            "ng": ng,
            "day_trade_available_shares": day_trade_available_shares,
            "appearance_count": appearance_counts.get(ticker, 0),
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
    - ticker: ティッカーシンボル (例: 2492.T)
    """
    df = load_day_trade_list()

    # tickerで検索
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
    - ticker: ティッカーシンボル (例: 2492.T)
    - request: 更新するフラグ (shortable, day_trade, ng)

    Note:
    - 指定されたフィールドのみ更新（None は無視）
    - grok_day_trade_list.parquet と grok_trending.parquet の両方を更新
    """
    ticker_str = str(ticker)

    # grok_day_trade_list.parquet を更新
    dtl_df = load_day_trade_list()
    dtl_mask = dtl_df["ticker"].astype(str) == ticker_str

    if not dtl_mask.any():
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} が見つかりません")

    if request.shortable is not None:
        dtl_df.loc[dtl_mask, "shortable"] = request.shortable
    if request.day_trade is not None:
        dtl_df.loc[dtl_mask, "day_trade"] = request.day_trade
    if request.ng is not None:
        dtl_df.loc[dtl_mask, "ng"] = request.ng
    if request.day_trade_available_shares is not None:
        dtl_df.loc[dtl_mask, "day_trade_available_shares"] = request.day_trade_available_shares

    save_day_trade_list(dtl_df)

    # grok_trending.parquet も更新
    try:
        grok_df = load_grok_trending()
        grok_mask = grok_df["ticker"].astype(str) == ticker_str

        if grok_mask.any():
            if request.shortable is not None:
                grok_df.loc[grok_mask, "shortable"] = request.shortable
            if request.day_trade is not None:
                grok_df.loc[grok_mask, "day_trade"] = request.day_trade
            if request.ng is not None:
                grok_df.loc[grok_mask, "ng"] = request.ng
            if request.day_trade_available_shares is not None:
                grok_df.loc[grok_mask, "day_trade_available_shares"] = request.day_trade_available_shares

            save_grok_trending(grok_df)
    except Exception as e:
        print(f"Warning: grok_trending.parquet更新失敗: {str(e)}")

    # 更新後のデータを返す
    updated = dtl_df[dtl_mask].iloc[0].to_dict()
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
    - grok_day_trade_list.parquet と grok_trending.parquet の両方を更新
    """
    dtl_df = load_day_trade_list()

    updated_count = 0
    errors = []
    updated_tickers = []

    for item in updates:
        ticker = str(item.get("ticker", ""))
        mask = dtl_df["ticker"].astype(str) == ticker

        if not mask.any():
            errors.append({"ticker": ticker, "error": "見つかりません"})
            continue

        if "shortable" in item:
            dtl_df.loc[mask, "shortable"] = item["shortable"]
        if "day_trade" in item:
            dtl_df.loc[mask, "day_trade"] = item["day_trade"]
        if "ng" in item:
            dtl_df.loc[mask, "ng"] = item["ng"]
        if "day_trade_available_shares" in item:
            dtl_df.loc[mask, "day_trade_available_shares"] = item["day_trade_available_shares"]

        updated_count += 1
        updated_tickers.append((ticker, item))

    # grok_day_trade_list.parquet を保存
    save_day_trade_list(dtl_df)

    # grok_trending.parquet も更新
    try:
        grok_df = load_grok_trending()

        for ticker, item in updated_tickers:
            mask = grok_df["ticker"].astype(str) == ticker
            if mask.any():
                if "shortable" in item:
                    grok_df.loc[mask, "shortable"] = item["shortable"]
                if "day_trade" in item:
                    grok_df.loc[mask, "day_trade"] = item["day_trade"]
                if "ng" in item:
                    grok_df.loc[mask, "ng"] = item["ng"]
                if "day_trade_available_shares" in item:
                    grok_df.loc[mask, "day_trade_available_shares"] = item["day_trade_available_shares"]

        save_grok_trending(grok_df)
    except Exception as e:
        print(f"Warning: grok_trending.parquet更新失敗: {str(e)}")

    return JSONResponse(content={
        "updated": updated_count,
        "errors": errors
    })


@router.get("/dev/day-trade-list/history/{ticker}")
async def get_day_trade_history(ticker: str):
    """
    銘柄の過去登場履歴を取得（grok_trending_archive.parquetから）

    Parameters:
    - ticker: ティッカーシンボル (例: 6993.T)

    Returns:
    - ticker: ティッカー
    - stock_name: 銘柄名
    - appearance_count: 登場回数
    - history: 過去の履歴リスト（日付降順）
        - date: 選定日 (YYYY-MM-DD)
        - weekday: 曜日 (月/火/水/木/金)
        - buy_price: 始値（寄付き）
        - high: 日中高値
        - low: 日中安値
        - sell_price: 前場終値
        - daily_close: 大引け終値
        - volume: 出来高
        - profit_phase1_short: 前場損益（ショート基準）
        - profit_phase2_short: 大引損益（ショート基準）
        - profit_phase1_long: 前場損益（ロング基準）
        - profit_phase2_long: 大引損益（ロング基準）
    """
    try:
        archive_df = load_grok_archive()
        if archive_df.empty:
            raise HTTPException(status_code=404, detail="アーカイブデータがありません")

        # 2025-11-04以降のみ
        archive_df = archive_df[archive_df['selection_date'] >= '2025-11-04']

        # 指定銘柄のデータを抽出
        ticker_df = archive_df[archive_df['ticker'] == ticker].copy()

        if ticker_df.empty:
            raise HTTPException(status_code=404, detail=f"ティッカー {ticker} の履歴がありません")

        # 日付でソート（降順）
        ticker_df = ticker_df.sort_values('selection_date', ascending=False)

        # 銘柄名を取得
        stock_name = ticker_df.iloc[0].get('stock_name', '')

        history = []
        for _, row in ticker_df.iterrows():
            selection_date = pd.to_datetime(row['selection_date'])
            weekday = WEEKDAY_NAMES[selection_date.weekday()]

            # 損益計算（ロング基準のデータを取得）
            profit_p1_long = row.get('profit_per_100_shares_phase1')
            profit_p2_long = row.get('profit_per_100_shares_phase2')

            # ショート基準は符号反転
            profit_p1_short = -profit_p1_long if pd.notna(profit_p1_long) else None
            profit_p2_short = -profit_p2_long if pd.notna(profit_p2_long) else None

            # 前日終値（アーカイブから取得）
            prev_close = int(row.get('prev_close')) if pd.notna(row.get('prev_close')) else None

            history.append({
                "date": selection_date.strftime('%Y-%m-%d'),
                "weekday": weekday,
                "prev_close": prev_close,
                "open": int(row.get('buy_price')) if pd.notna(row.get('buy_price')) else None,
                "high": int(row.get('high')) if pd.notna(row.get('high')) else None,
                "low": int(row.get('low')) if pd.notna(row.get('low')) else None,
                "close": int(row.get('daily_close')) if pd.notna(row.get('daily_close')) else None,
                "volume": int(row.get('volume')) if pd.notna(row.get('volume')) else None,
                "profit_phase1": int(profit_p1_short) if profit_p1_short is not None else None,
                "profit_phase2": int(profit_p2_short) if profit_p2_short is not None else None,
            })

        return JSONResponse(content={
            "ticker": ticker,
            "stock_name": stock_name,
            "appearance_count": len(history),
            "history": history
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"エラー: {str(e)}")
