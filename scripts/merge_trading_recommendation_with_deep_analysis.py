#!/usr/bin/env python3
"""
trading_recommendation.json と deep_analysis_2025-11-18.json をマージ
スコアリングモデルはv2を尊重（2025-11-17と統一）
"""
import json
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
TRADING_REC_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'trading_recommendation.json'
DEEP_ANALYSIS_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'analysis' / 'deep_analysis_2025-11-18.json'
OUTPUT_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'trading_recommendation_2025-11-18.json'

def calculate_stop_loss(action: str, atr_value: float) -> dict:
    """損切りラインを計算"""
    if action == 'buy':
        # 買いの場合: -1.5% ~ -3% (ATRに応じて)
        percent = max(-3.0, min(-1.5, -atr_value * 0.5))
        calculation = f"ATR {atr_value:.1f}% × 0.5"
    elif action == 'sell':
        # 売りの場合: +1.5% ~ +3% (損切りは上昇方向)
        percent = min(3.0, max(1.5, atr_value * 0.5))
        calculation = f"ATR {atr_value:.1f}% × 0.5"
    else:  # hold
        percent = -2.0
        calculation = "固定 -2.0%"

    return {
        "percent": round(percent, 2),
        "calculation": calculation
    }

def merge_data():
    """trading_recommendation.jsonにdeep_analysisの情報をマージ（v2スコアを尊重）"""

    # Load both files
    with open(TRADING_REC_PATH, 'r', encoding='utf-8') as f:
        trading_rec = json.load(f)

    with open(DEEP_ANALYSIS_PATH, 'r', encoding='utf-8') as f:
        deep_analysis = json.load(f)

    # Create a mapping of ticker to deep analysis data
    deep_analysis_map = {}
    for stock in deep_analysis.get('stockAnalyses', []):
        ticker = stock.get('ticker')
        if ticker:
            deep_analysis_map[ticker] = stock

    # Merge data with proper frontend type structure
    merged_stocks = []
    for stock in trading_rec.get('stocks', []):
        ticker = stock.get('ticker')
        deep_data = deep_analysis_map.get(ticker, {})

        # Extract ATR value for stop-loss calculation
        atr_value = stock.get('technicalData', {}).get('atr', {}).get('value', 3.0)
        if atr_value is None:
            atr_value = 3.0

        # Get action, score, and confidence from recommendation (v2 output)
        v2_recommendation = stock.get('recommendation', {})
        action = v2_recommendation.get('action', 'hold')
        confidence = v2_recommendation.get('confidence', 'medium')

        # ★★★ v2スコアとfinalスコアを分離（2025-11-17と統一） ★★★
        v2_score = v2_recommendation.get('score', 0)
        # deep_analysisのfinalScoreを使用（deep_search調整後）
        final_score = deep_data.get('finalScore', v2_score) if deep_data else v2_score

        # Get reasons from v2 output
        v2_reasons = v2_recommendation.get('reasons', [])

        # Build recommendation object (v2 + deep_search)
        recommendation = {
            "action": action,
            "score": final_score,  # deep_search調整後のfinalScore
            "v2Score": v2_score,   # v2の基礎スコア
            "confidence": confidence,
            "stopLoss": calculate_stop_loss(action, atr_value),
            "reasons": v2_reasons
        }

        # Build deep analysis object (補助情報のみ)
        deep_analysis_obj = None
        if deep_data:
            deep_analysis_obj = {
                "verdict": deep_data.get('verdict', ''),
                "adjustmentReasons": deep_data.get('adjustmentReasons', []),
                "risks": deep_data.get('risks', []),
                "opportunities": deep_data.get('opportunities', []),
                "latestNews": deep_data.get('latestNews', []),
                "sectorTrend": deep_data.get('sectorTrend'),
                "marketSentiment": deep_data.get('marketSentiment'),
                "newsHeadline": deep_data.get('newsHeadline')
            }

            # Add earnings if available
            if 'earnings' in deep_data:
                deep_analysis_obj['fundamentals'] = {
                    "operatingProfitGrowth": deep_data['earnings'].get('operatingProfitGrowth'),
                    "nextEarningsDate": deep_data['earnings'].get('date')
                }

            # Add web materials if available
            if 'webMaterials' in deep_data:
                deep_analysis_obj['webMaterials'] = deep_data['webMaterials']

            # Add daytrading analysis if available
            if 'daytradingAnalysis' in deep_data:
                deep_analysis_obj['daytradingAnalysis'] = deep_data['daytradingAnalysis']

        # Build merged stock object (フロントエンド型定義準拠)
        merged_stock = {
            "ticker": ticker,
            "stockName": stock.get('stockName', ''),
            "grokRank": stock.get('grokRank', 0),
            "technicalData": stock.get('technicalData', {}),
            "recommendation": recommendation,
            "categories": stock.get('categories', [])
        }

        # Add deepAnalysis if available
        if deep_analysis_obj:
            merged_stock['deepAnalysis'] = deep_analysis_obj

        merged_stocks.append(merged_stock)

    # Update metadata
    output_data = {
        "version": "3.0_merged_v2_scoring",
        "generatedAt": datetime.now().isoformat(),
        "dataSource": trading_rec.get('dataSource', {}),
        "summary": {
            "total": len(merged_stocks),
            "buy": sum(1 for s in merged_stocks if s['recommendation']['action'] == 'buy'),
            "sell": sum(1 for s in merged_stocks if s['recommendation']['action'] == 'sell'),
            "hold": sum(1 for s in merged_stocks if s['recommendation']['action'] == 'hold')
        },
        "stocks": merged_stocks,
        "mergedFrom": {
            "scoringModel": "v2 (2025-11-17統一)",
            "deepAnalysis": "3.0 (補助情報のみ)",
            "deepAnalysisDate": "2025-11-18"
        }
    }

    # Save merged data
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"✅ マージ完了（v2スコアリングモデル、2025-11-17と統一）")
    print(f"   入力1: {TRADING_REC_PATH}")
    print(f"   入力2: {DEEP_ANALYSIS_PATH}")
    print(f"   出力: {OUTPUT_PATH}")
    print(f"\n📊 サマリー:")
    print(f"   銘柄数: {output_data['summary']['total']}")
    print(f"   買い: {output_data['summary']['buy']}")
    print(f"   売り: {output_data['summary']['sell']}")
    print(f"   静観: {output_data['summary']['hold']}")

    # スコア範囲を確認
    scores = [s['recommendation']['score'] for s in merged_stocks]
    print(f"\n📈 スコア範囲（2025-11-17と統一）:")
    print(f"   最小: {min(scores)}")
    print(f"   平均: {sum(scores)/len(scores):.1f}")
    print(f"   最大: {max(scores)}")

if __name__ == '__main__':
    merge_data()
