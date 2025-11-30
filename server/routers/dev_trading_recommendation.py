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

# JSONファイルのパス
JSON_FILE = ROOT / "data" / "parquet" / "backtest" / "trading_recommendation.json"
DEEP_ANALYSIS_DIR = ROOT / "data" / "parquet" / "backtest" / "analysis"

# S3設定（環境変数から取得）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def map_action(action_jp: str) -> str:
    """日本語アクションを英語にマッピング"""
    mapping = {"買い": "buy", "売り": "sell", "静観": "hold"}
    return mapping.get(action_jp, "hold")


def calculate_confidence(score: int) -> str:
    """スコアから信頼度を計算"""
    abs_score = abs(score)
    if abs_score >= 50:
        return "high"
    elif abs_score >= 30:
        return "medium"
    else:
        return "low"


def get_action_sort_order(action: str, is_restricted: bool) -> int:
    """表示順序を決定: 買い(0) → 静観(1) → 売り(2) → 取引制限(3)"""
    if is_restricted:
        return 3
    order_map = {"buy": 0, "hold": 1, "sell": 2}
    return order_map.get(action, 1)


def convert_v2_1_to_frontend_format(trading_data: dict) -> dict:
    """
    v2.1 のスキーマをフロントエンドが期待する形式に変換
    """
    if trading_data.get("strategy_version") != "v2.1":
        # v2.1 でない場合はそのまま返す
        return trading_data

    converted_stocks = []
    for stock in trading_data.get("stocks", []):
        # v2.1 フィールドを recommendation 形式に変換
        action = map_action(stock.get("v2_1_action", "静観"))
        score = stock.get("v2_1_score", 0)
        confidence = calculate_confidence(score)

        # 取引制限情報
        is_restricted = stock.get("is_restricted", False)
        restriction_reason = stock.get("restriction_reason")

        # reasons を配列から Reason オブジェクトに変換
        v2_1_reasons = stock.get("v2_1_reasons", [])
        reasons = []
        for i, reason_text in enumerate(v2_1_reasons):
            reasons.append({
                "type": "grok_rank" if i == 0 else "category",
                "description": reason_text,
                "impact": 10  # デフォルト値
            })

        converted_stock = {
            "ticker": stock.get("ticker"),
            "stockName": stock.get("stock_name", stock.get("company_name", stock.get("ticker"))),
            "grokRank": stock.get("grok_rank"),
            "technicalData": {
                "prevClose": stock.get("prev_day_close"),
                "prevDayChangePct": stock.get("prev_day_change_pct"),
                "atr": {
                    "value": stock.get("atr_pct"),
                    "level": "high" if (stock.get("atr_pct") or 0) > 5 else "medium" if (stock.get("atr_pct") or 0) > 3 else "low"
                },
                "volatilityLevel": "高ボラ" if (stock.get("atr_pct") or 0) > 5 else "中ボラ" if (stock.get("atr_pct") or 0) > 3 else "低ボラ"
            },
            "recommendation": {
                "action": action,
                "score": score,
                "confidence": confidence,
                "stopLoss": {
                    "percent": stock.get("stop_loss_pct", 5.0),
                    "calculation": f"ATR {(stock.get('atr_pct') or 0):.1f}% × 0.8"
                },
                "reasons": reasons,
                # v2_0_3 情報（比較用）
                "v2_0_3_action": map_action(stock.get("v2_0_3_action", "静観")),
                "v2_0_3_score": stock.get("v2_0_3_score"),
                "v2_0_3_reasons": stock.get("v2_0_3_reasons", "")
            },
            "categories": [],
            # 取引制限情報
            "tradingRestriction": {
                "isRestricted": is_restricted,
                "reason": restriction_reason,
                "marginCode": stock.get("margin_code"),
                "marginCodeName": stock.get("margin_code_name"),
                "jsfRestricted": stock.get("jsf_restricted", False),
                "isShortable": stock.get("is_shortable", True)
            },
            # deepAnalysis は後でマージされる
            "deepAnalysis": stock.get("deepAnalysis")
        }
        converted_stocks.append(converted_stock)

    # 表示順序でソート: 買い → 静観 → 売り → 取引制限
    converted_stocks.sort(key=lambda x: (
        get_action_sort_order(
            x.get("recommendation", {}).get("action", "hold"),
            x.get("tradingRestriction", {}).get("isRestricted", False)
        ),
        -x.get("recommendation", {}).get("score", 0)  # 同じカテゴリ内はスコア順
    ))

    # 変換後のレスポンス
    return {
        "version": "v2.1-compatible",
        "generatedAt": trading_data.get("generated_at"),
        "dataSource": {
            "backtestCount": 0,  # v2.1 には含まれていない
            "backtestPeriod": {
                "start": "2025-01-01",
                "end": "2025-11-24"
            },
            "technicalDataDate": trading_data.get("dataSource", {}).get("technicalDataDate", "2025-11-24")
        },
        "summary": {
            "total": trading_data.get("total_stocks", 0),
            "buy": trading_data.get("buy_count", 0),
            "sell": trading_data.get("sell_count", 0),
            "hold": trading_data.get("hold_count", 0),
            "restricted": trading_data.get("restricted_count", 0)
        },
        "warnings": [],
        "stocks": converted_stocks,
        # 既存のフィールドを保持
        "deepAnalysisVersion": trading_data.get("deepAnalysisVersion"),
        "deepAnalysisDate": trading_data.get("deepAnalysisDate"),
        "deepAnalysisUpdated": trading_data.get("deepAnalysisUpdated")
    }


def load_recommendation_data() -> dict:
    """
    推奨データを読み込み
    - trading_recommendation.json から読み込み（手動深掘り分析の結果）
    - ローカルファイルを最優先（開発環境）
    - ローカルがなければS3から読み込み（本番環境）
    """
    # ローカルファイルを最優先
    if JSON_FILE.exists():
        print(f"[INFO] Loading from local file: {JSON_FILE}")
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    # ローカルがなければS3から読み込み
    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        s3_key = f"{S3_PREFIX}backtest/trading_recommendation.json"

        print(f"[INFO] Loading recommendation from S3: s3://{S3_BUCKET}/{s3_key}")

        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        print(f"[INFO] Successfully loaded from S3: {data.get('summary', {}).get('total', 0)} stocks")
        return data

    except ClientError as e:
        raise HTTPException(
            status_code=404,
            detail={
                "error": {
                    "code": "NOT_FOUND",
                    "message": "推奨データが見つかりません",
                    "details": f"ローカル: {JSON_FILE}, S3: {e}"
                }
            }
        )


@router.get("/api/trading-recommendations")
async def get_trading_recommendations():
    """
    Grok推奨銘柄の売買判断データを取得
    - trading_recommendation.json (v2スコア)
    - deep_analysis_YYYYMMDD.json (深掘り分析結果)
    - finalScore順にソート

    Returns:
        TradingRecommendationResponse: 売買判断データ

    Raises:
        HTTPException: ファイルが見つからない場合は404/500
    """
    try:
        # データ読み込み（ローカルまたはS3から）
        data = load_recommendation_data()

        # v2.1 の場合はフロントエンド形式に変換（deep_analysis マージ前）
        data = convert_v2_1_to_frontend_format(data)

        # deep_analysis を読み込んでマージ
        try:
            # trading_recommendation.json の technicalDataDate + 1 を計算
            from datetime import datetime, timedelta
            technical_date = data.get('dataSource', {}).get('technicalDataDate')
            if not technical_date:
                print("[WARNING] No technicalDataDate found in trading_recommendation.json")
                raise Exception("technicalDataDate not found")

            technical_dt = datetime.strptime(technical_date, '%Y-%m-%d')
            target_dt = technical_dt + timedelta(days=1)
            target_date = target_dt.strftime('%Y-%m-%d')

            # ローカルファイルを最優先でdeep_analysisを読み込み
            deep_data = None
            local_file = DEEP_ANALYSIS_DIR / f"deep_analysis_{target_date}.json"
            if local_file.exists():
                with open(local_file, 'r', encoding='utf-8') as f:
                    deep_data = json.load(f)
                print(f"[INFO] Loaded deep_analysis from local: {local_file.name}")
            else:
                # 最新のローカルファイルを探す
                deep_files = sorted(DEEP_ANALYSIS_DIR.glob("deep_analysis_*.json"), reverse=True)
                if deep_files:
                    with open(deep_files[0], 'r', encoding='utf-8') as f:
                        deep_data = json.load(f)
                    print(f"[INFO] Loaded deep_analysis from local: {deep_files[0].name}")
                else:
                    # ローカルがなければS3から読み込み
                    try:
                        s3 = boto3.client('s3', region_name=AWS_REGION)
                        s3_key = f"{S3_PREFIX}backtest/analysis/deep_analysis_{target_date}.json"

                        print(f"[INFO] Loading deep_analysis from S3: s3://{S3_BUCKET}/{s3_key}")
                        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                        deep_data = json.loads(response['Body'].read().decode('utf-8'))
                        print(f"[INFO] Successfully loaded deep_analysis from S3")
                    except ClientError as e:
                        print(f"[WARNING] Could not load deep_analysis: {e}")

            if deep_data:

                # deep_analysis のデータをマージ
                deep_scores = {stock["ticker"]: stock for stock in deep_data.get("stockAnalyses", [])}

                for stock in data.get("stocks", []):
                    ticker = stock.get("ticker")
                    if ticker in deep_scores:
                        deep_stock = deep_scores[ticker]
                        stock["deepAnalysis"] = {
                            "v2Score": deep_stock.get("v2Score"),
                            "finalScore": deep_stock.get("finalScore"),
                            "scoreAdjustment": deep_stock.get("scoreAdjustment"),
                            "recommendation": deep_stock.get("recommendation"),
                            "confidence": deep_stock.get("confidence"),
                            "verdict": deep_stock.get("verdict"),
                            "adjustmentReasons": deep_stock.get("adjustmentReasons", []),
                            "risks": deep_stock.get("risks", []),
                            "opportunities": deep_stock.get("opportunities", []),
                            "latestNews": deep_stock.get("latestNews", []),
                            "sectorTrend": deep_stock.get("sectorTrend", ""),
                            "marketSentiment": deep_stock.get("marketSentiment", "neutral"),
                            "newsHeadline": deep_stock.get("newsHeadline", "")
                        }

                # finalScore順にソート
                data["stocks"] = sorted(
                    data.get("stocks", []),
                    key=lambda x: x.get("deepAnalysis", {}).get("finalScore", x.get("recommendation", {}).get("score", 0)),
                    reverse=True
                )

                # メタデータ追加
                data["deepAnalysisVersion"] = deep_data.get("version", "unknown")
                data["deepAnalysisDate"] = deep_data.get("sourceDate")
                data["deepAnalysisUpdated"] = deep_data.get("lastUpdated")

        except Exception as e:
            print(f"[WARNING] Could not load deep_analysis: {e}")
            # deep_analysis が読み込めなくてもエラーにしない

        # v2.1 の場合はフロントエンド形式に変換
        data = convert_v2_1_to_frontend_format(data)

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
