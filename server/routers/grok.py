# server/routers/grok.py
from fastapi import APIRouter, HTTPException
from typing import Dict
import pandas as pd
from pathlib import Path
import os

router = APIRouter()

# S3からデータを取得する共通ユーティリティ
def get_parquet_from_s3_or_local(filename: str) -> pd.DataFrame:
    """
    S3またはローカルからparquetファイルを取得
    filename can include subdirectories like "backtest/grok_trending_archive.parquet"
    """
    import boto3
    from botocore.exceptions import ClientError

    # ローカルパス（filenameにサブディレクトリが含まれる場合も対応）
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
