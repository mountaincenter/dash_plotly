# server/routers/grok.py
from fastapi import APIRouter, HTTPException, Response
from typing import List, Dict
import pandas as pd
from pathlib import Path
import os

router = APIRouter()

# S3からデータを取得する共通ユーティリティ
def get_parquet_from_s3_or_local(filename: str) -> pd.DataFrame:
    """
    S3またはローカルからparquetファイルを取得
    """
    import boto3
    from botocore.exceptions import ClientError

    # ローカルパス
    local_path = Path(__file__).resolve().parents[2] / "data" / "parquet" / filename

    # ローカルファイルが存在すればそれを使用
    if local_path.exists():
        return pd.read_parquet(local_path)

    # S3から取得
    try:
        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = f"parquet/{filename}"
        region = os.getenv("AWS_REGION", "ap-northeast-1")

        s3_client = boto3.client("s3", region_name=region)

        # 一時ファイルにダウンロード
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            s3_client.download_fileobj(bucket, key, tmp_file)
            tmp_path = tmp_file.name

        # 読み込み
        df = pd.read_parquet(tmp_path)

        # 一時ファイル削除
        os.unlink(tmp_path)

        return df

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"{filename} not found in S3")
        raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading {filename}: {str(e)}")


@router.get("/grok_backtest_meta")
async def get_grok_backtest_meta(response: Response) -> List[Dict[str, str]]:
    """
    Grokバックテストのメタ情報を取得

    Returns:
        List[Dict]: [{"metric": "...", "value": "..."}, ...]
    """
    try:
        df = get_parquet_from_s3_or_local("grok_backtest_meta.parquet")

        if df.empty:
            return []

        # DataFrameをリストに変換
        result = df.to_dict(orient="records")

        # キャッシュヘッダーを設定（1時間）
        response.headers["Cache-Control"] = "public, max-age=3600, stale-while-revalidate=600"

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@router.get("/grok_top_stocks")
async def get_grok_top_stocks(response: Response, category: str = "top5") -> List[Dict]:
    """
    GrokバックテストのTop5/Top10銘柄リストを取得
    grok_trending.parquetから選定スコア順にTop5またはTop10を抽出

    Args:
        category: "top5" or "top10"

    Returns:
        List[Dict]: 銘柄リスト
    """
    try:
        # grok_trending.parquetから読み込み
        df = get_parquet_from_s3_or_local("grok_trending.parquet")

        if df.empty:
            return []

        # selection_scoreで降順ソート
        if "selection_score" in df.columns:
            df = df.sort_values("selection_score", ascending=False)

        # Top5またはTop10を取得
        limit = 5 if category == "top5" else 10
        df = df.head(limit)

        # ランクを追加（1始まり）
        df["rank"] = range(1, len(df) + 1)
        df["category"] = category

        # 必要なカラムのみ選択
        cols = ["ticker", "stock_name", "selection_score", "rank", "category"]
        # company_nameがあればそれを使用、なければstock_nameをcompany_nameとして使用
        if "company_name" not in df.columns:
            df["company_name"] = df["stock_name"]
            cols.append("company_name")

        available_cols = [c for c in cols if c in df.columns]
        df = df[available_cols]

        # DataFrameをリストに変換
        result = df.to_dict(orient="records")

        # キャッシュヘッダーを設定（1時間）
        response.headers["Cache-Control"] = "public, max-age=3600, stale-while-revalidate=600"

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
