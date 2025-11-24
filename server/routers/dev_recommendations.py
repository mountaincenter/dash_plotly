"""
開発用: 投資推奨データAPI
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import json
from typing import Optional
from datetime import datetime

router = APIRouter()

BASE_DIR = Path(__file__).parent.parent.parent
TRADING_REC_FILE = BASE_DIR / "data/parquet/backtest/trading_recommendation.json"
DEEP_ANALYSIS_DIR = BASE_DIR / "data/parquet/backtest/analysis"


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
            "stockName": stock.get("company_name", stock.get("ticker")),
            "grokRank": stock.get("grok_rank"),
            "technicalData": {
                "prevClose": stock.get("prev_day_close"),
                "prevDayChangePct": stock.get("prev_day_change_pct"),
                "atr": {
                    "value": stock.get("atr_pct"),
                    "level": "high" if stock.get("atr_pct", 0) > 5 else "medium" if stock.get("atr_pct", 0) > 3 else "low"
                },
                "volatilityLevel": "高ボラ" if stock.get("atr_pct", 0) > 5 else "中ボラ" if stock.get("atr_pct", 0) > 3 else "低ボラ"
            },
            "recommendation": {
                "action": action,
                "score": score,
                "v2Score": stock.get("v2_0_3_score"),  # 参考情報
                "confidence": confidence,
                "stopLoss": {
                    "percent": stock.get("stop_loss_pct", 5.0),
                    "calculation": f"ATR {stock.get('atr_pct', 0):.1f}% × 0.8"
                },
                "reasons": reasons
            },
            "categories": []
        }
        converted_stocks.append(converted_stock)

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
            "technicalDataDate": "2025-11-24"
        },
        "summary": {
            "total": trading_data.get("total_stocks", 0),
            "buy": trading_data.get("buy_count", 0),
            "sell": trading_data.get("sell_count", 0),
            "hold": trading_data.get("hold_count", 0)
        },
        "warnings": [],
        "stocks": converted_stocks
    }


@router.get("/recommendations")
async def get_recommendations(date: Optional[str] = None):
    """
    投資推奨データを取得

    Parameters:
    - date: 深掘り分析の日付 (YYYY-MM-DD形式、省略時は最新)

    Returns:
    - trading: trading_recommendation.json の内容
    - deepAnalysis: deep_analysis_{date}.json の内容
    - metadata: メタデータ
    """
    try:
        # trading_recommendation.json を読み込み
        if not TRADING_REC_FILE.exists():
            raise HTTPException(status_code=404, detail="trading_recommendation.json が見つかりません")

        with open(TRADING_REC_FILE, 'r', encoding='utf-8') as f:
            trading_data = json.load(f)

        # v2.1 の場合はフロントエンド形式に変換
        trading_data = convert_v2_1_to_frontend_format(trading_data)

        # deep_analysis ファイルを取得
        if date:
            deep_file = DEEP_ANALYSIS_DIR / f"deep_analysis_{date}.json"
        else:
            # 最新の deep_analysis ファイルを探す
            deep_files = sorted(DEEP_ANALYSIS_DIR.glob("deep_analysis_*.json"), reverse=True)
            if not deep_files:
                raise HTTPException(status_code=404, detail="deep_analysis ファイルが見つかりません")
            deep_file = deep_files[0]

        if not deep_file.exists():
            raise HTTPException(status_code=404, detail=f"{deep_file.name} が見つかりません")

        with open(deep_file, 'r', encoding='utf-8') as f:
            deep_data = json.load(f)

        # レスポンス
        response = {
            "trading": trading_data,
            "deepAnalysis": deep_data,
            "metadata": {
                "tradingFile": "trading_recommendation.json",
                "deepAnalysisFile": deep_file.name,
                "generatedAt": datetime.now().isoformat(),
                "deepSearchApplied": trading_data.get("deepSearchApplied", False),
                "deepSearchDate": trading_data.get("deepSearchDate")
            }
        }

        return JSONResponse(content=response)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"エラー: {str(e)}")


@router.get("/recommendations/summary")
async def get_recommendations_summary():
    """
    投資推奨サマリーを取得

    Returns:
    - topPicks: トップ推奨銘柄
    - avoid: 避けるべき銘柄
    - statistics: 統計情報
    """
    try:
        # 最新の deep_analysis ファイルを取得
        deep_files = sorted(DEEP_ANALYSIS_DIR.glob("deep_analysis_*.json"), reverse=True)
        if not deep_files:
            raise HTTPException(status_code=404, detail="deep_analysis ファイルが見つかりません")

        with open(deep_files[0], 'r', encoding='utf-8') as f:
            deep_data = json.load(f)

        # サマリー生成
        summary = {
            "date": deep_data.get("sourceDate"),
            "topPicks": deep_data.get("tradingStrategy", {}).get("aggressiveBuy", []),
            "buyOnDip": deep_data.get("tradingStrategy", {}).get("buyOnDip", []),
            "avoid": deep_data.get("tradingStrategy", {}).get("avoid", []),
            "statistics": deep_data.get("summary", {}),
            "generatedAt": datetime.now().isoformat()
        }

        return JSONResponse(content=summary)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"エラー: {str(e)}")


@router.get("/recommendations/stock/{ticker}")
async def get_stock_recommendation(ticker: str):
    """
    個別銘柄の推奨データを取得

    Parameters:
    - ticker: ティッカーシンボル (例: 2492.T)

    Returns:
    - 個別銘柄の詳細分析結果
    """
    try:
        # 最新の deep_analysis ファイルを取得
        deep_files = sorted(DEEP_ANALYSIS_DIR.glob("deep_analysis_*.json"), reverse=True)
        if not deep_files:
            raise HTTPException(status_code=404, detail="deep_analysis ファイルが見つかりません")

        with open(deep_files[0], 'r', encoding='utf-8') as f:
            deep_data = json.load(f)

        # ティッカーで検索
        for stock in deep_data.get("stockAnalyses", []):
            if stock["ticker"] == ticker:
                return JSONResponse(content=stock)

        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} が見つかりません")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"エラー: {str(e)}")
