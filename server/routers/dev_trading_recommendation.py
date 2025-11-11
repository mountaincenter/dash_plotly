# server/routers/dev_trading_recommendation.py
"""
Grok推奨銘柄の売買判断データAPI
/api/trading-recommendations - JSON APIエンドポイント
"""
from fastapi import APIRouter, HTTPException
from pathlib import Path
import json
import sys
import os
import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

router = APIRouter()

# JSONファイルのパス（新パイプライン: S3同期対象）
JSON_FILE = ROOT / "data" / "parquet" / "backtest" / "trading_recommendation.json"

# S3設定（環境変数から取得）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_recommendation_data() -> dict:
    """
    推奨データを読み込み
    - ローカルにファイルがあればそれを使用（開発環境）
    - なければS3から直接読み込み（本番環境）
    """
    # ローカルファイルが存在する場合
    if JSON_FILE.exists():
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)

    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/trading_recommendation.json"

        print(f"[INFO] Loading recommendation data from S3: s3://{S3_BUCKET}/{s3_key}")

        s3_client = boto3.client('s3', region_name=AWS_REGION)
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        print(f"[INFO] Successfully loaded recommendation data from S3")
        return data

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchKey':
            print(f"[ERROR] Recommendation data not found in S3: {s3_key}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "NOT_FOUND",
                        "message": "推奨データが見つかりません",
                        "details": f"S3にファイルが存在しません: {s3_key}"
                    }
                }
            )
        else:
            print(f"[ERROR] S3 error: {error_code}: {e}")
            raise

    except Exception as e:
        print(f"[ERROR] Could not load recommendation data: {type(e).__name__}: {e}")
        raise


@router.get("/api/trading-recommendations")
async def get_trading_recommendations():
    """
    Grok推奨銘柄の売買判断データを取得

    Returns:
        TradingRecommendationResponse: 売買判断データ

    Raises:
        HTTPException: ファイルが見つからない場合は404/500
    """
    try:
        # データ読み込み（ローカルまたはS3から）
        data = load_recommendation_data()
        return data

    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "INVALID_JSON",
                    "message": "JSONファイルの解析に失敗しました",
                    "details": str(e)
                }
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "データ取得中にエラーが発生しました",
                    "details": str(e)
                }
            }
        )
