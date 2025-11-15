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
import pandas as pd
import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

router = APIRouter()

# Parquetファイルのパス
HISTORY_FILE = ROOT / "data" / "parquet" / "backtest" / "trading_recommendation_history.parquet"

# S3設定（環境変数から取得）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_recommendation_data() -> dict:
    """
    推奨データを読み込み（最新日付のみ）
    - trading_recommendation_history.parquet から最新日付のデータを取得
    - S3から読み込み（本番環境、常に最新）
    - S3が失敗したらローカルファイルを使用（開発環境）
    """
    df = None

    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/trading_recommendation_history.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"

        print(f"[INFO] Loading recommendation history from S3: {s3_url}")

        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })

        print(f"[INFO] Successfully loaded {len(df)} records from S3")

    except Exception as e:
        print(f"[WARNING] Could not load from S3: {type(e).__name__}: {e}")

        # ローカルファイルにフォールバック
        if HISTORY_FILE.exists():
            print(f"[INFO] Loading from local file: {HISTORY_FILE}")
            df = pd.read_parquet(HISTORY_FILE)
        else:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "code": "NOT_FOUND",
                        "message": "推奨データが見つかりません",
                        "details": "S3・ローカル共に存在しません"
                    }
                }
            )

    # 最新日付のデータのみ抽出
    df['recommendation_date'] = pd.to_datetime(df['recommendation_date'])
    latest_date = df['recommendation_date'].max()
    latest_df = df[df['recommendation_date'] == latest_date].copy()

    print(f"[INFO] Latest recommendation date: {latest_date.date()}, {len(latest_df)} stocks")

    # JSON形式に変換
    stocks = []
    for _, row in latest_df.iterrows():
        # reasons_json をパース
        try:
            reasons = json.loads(row['recommendation_reasons_json']) if pd.notna(row.get('recommendation_reasons_json')) else []
        except:
            reasons = []

        stock = {
            'ticker': row['ticker'],
            'recommendation': {
                'action': row['recommendation_action'],
                'score': float(row['recommendation_score']) if pd.notna(row.get('recommendation_score')) else 0.0,
                'confidence': str(row['recommendation_confidence']) if pd.notna(row.get('recommendation_confidence')) else 'medium',
                'reasons': reasons
            }
        }
        stocks.append(stock)

    # サマリー計算
    buy_count = (latest_df['recommendation_action'] == 'buy').sum()
    sell_count = (latest_df['recommendation_action'] == 'sell').sum()
    hold_count = (latest_df['recommendation_action'] == 'hold').sum()

    # レスポンス構築
    response = {
        'generatedAt': latest_date.isoformat(),
        'dataSource': {
            'backtestCount': len(df),
            'backtestPeriod': {
                'start': df['recommendation_date'].min().strftime('%Y-%m-%d'),
                'end': df['recommendation_date'].max().strftime('%Y-%m-%d')
            },
            'technicalDataDate': latest_date.strftime('%Y-%m-%d')
        },
        'totalStocks': len(stocks),
        'summary': {
            'buy': int(buy_count),
            'sell': int(sell_count),
            'hold': int(hold_count)
        },
        'stocks': stocks
    }

    return response


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
