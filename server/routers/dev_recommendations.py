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
