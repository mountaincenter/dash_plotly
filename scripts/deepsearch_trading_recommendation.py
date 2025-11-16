#!/usr/bin/env python3
"""
trading_recommendation.json の深掘り分析
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# パス設定
BASE_DIR = Path(__file__).parent.parent
INPUT_JSON = BASE_DIR / "data/parquet/backtest/trading_recommendation.json"
OUTPUT_DIR = BASE_DIR / "data/parquet/backtest/analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_trading_recommendation() -> dict:
    """trading_recommendation.json を読み込み"""
    with open(INPUT_JSON, 'r', encoding='utf-8') as f:
        return json.load(f)

def evaluate_technical_risk(stock: dict) -> dict:
    """テクニカル指標からリスク評価"""
    tech = stock['technicalData']

    # 各指標のリスクレベル判定
    risks = {
        'volatility_risk': None,
        'momentum_risk': None,
        'volume_risk': None,
        'price_risk': None,
        'overall_risk': None,
    }

    # ボラティリティリスク（ATR14Pct）
    if tech['atr14Pct'] is not None:
        atr = tech['atr14Pct']
        if atr > 8.0:
            risks['volatility_risk'] = 'HIGH'
        elif atr > 5.0:
            risks['volatility_risk'] = 'MEDIUM'
        else:
            risks['volatility_risk'] = 'LOW'

    # モメンタムリスク（RSI14）
    if tech['rsi14'] is not None:
        rsi = tech['rsi14']
        if rsi < 30 or rsi > 70:
            risks['momentum_risk'] = 'HIGH'  # 買われすぎ/売られすぎ
        elif rsi < 40 or rsi > 60:
            risks['momentum_risk'] = 'MEDIUM'
        else:
            risks['momentum_risk'] = 'LOW'

    # 出来高リスク（volRatio）
    if tech['volRatio'] is not None:
        vol_ratio = tech['volRatio']
        if vol_ratio < 0.5 or vol_ratio > 3.0:
            risks['volume_risk'] = 'HIGH'  # 異常な出来高
        elif vol_ratio < 0.8 or vol_ratio > 1.5:
            risks['volume_risk'] = 'MEDIUM'
        else:
            risks['volume_risk'] = 'LOW'

    # 価格変動リスク（changePct）
    if tech['changePct'] is not None:
        change = abs(tech['changePct'])
        if change > 10.0:
            risks['price_risk'] = 'HIGH'
        elif change > 5.0:
            risks['price_risk'] = 'MEDIUM'
        else:
            risks['price_risk'] = 'LOW'

    # 総合リスク判定
    risk_scores = {
        'HIGH': 3,
        'MEDIUM': 2,
        'LOW': 1,
        None: 0
    }
    total_score = sum(risk_scores[v] for v in [
        risks['volatility_risk'],
        risks['momentum_risk'],
        risks['volume_risk'],
        risks['price_risk']
    ])

    if total_score >= 10:
        risks['overall_risk'] = 'HIGH'
    elif total_score >= 6:
        risks['overall_risk'] = 'MEDIUM'
    else:
        risks['overall_risk'] = 'LOW'

    return risks

def evaluate_social_credibility(stock: dict) -> dict:
    """SNS指標から信頼性評価"""
    social = stock['socialMetrics']

    credibility = {
        'has_mention': social['hasMention'],
        'mention_count': len(social['mentionedBy']) if social['mentionedBy'] else 0,
        'sentiment_score': social['sentimentScore'],
        'selection_score': social['selectionScore'],
        'credibility_level': None,
    }

    # 信頼性レベル判定
    if credibility['has_mention'] and credibility['sentiment_score'] is not None:
        if credibility['sentiment_score'] >= 0.8 and credibility['selection_score'] >= 140:
            credibility['credibility_level'] = 'HIGH'
        elif credibility['sentiment_score'] >= 0.6 and credibility['selection_score'] >= 100:
            credibility['credibility_level'] = 'MEDIUM'
        else:
            credibility['credibility_level'] = 'LOW'
    else:
        credibility['credibility_level'] = 'NO_MENTION'

    return credibility

def evaluate_market_position(stock: dict) -> dict:
    """市場ポジション評価"""
    market_info = stock['marketInfo']

    position = {
        'market': market_info['market'],
        'sectors': market_info['sectors'],
        'series': market_info['series'],
        'market_tier': None,
    }

    # 市場ティア判定
    if market_info['market'] == 'プライム':
        position['market_tier'] = 'TOP'
    elif market_info['market'] == 'スタンダード':
        position['market_tier'] = 'MIDDLE'
    elif market_info['market'] == 'グロース':
        position['market_tier'] = 'GROWTH'
    else:
        position['market_tier'] = 'OTHER'

    return position

def generate_stock_analysis(stock: dict) -> dict:
    """個別銘柄の総合分析"""

    # 各種評価
    tech_risk = evaluate_technical_risk(stock)
    social_cred = evaluate_social_credibility(stock)
    market_pos = evaluate_market_position(stock)

    # 推奨度スコア計算
    recommendation_score = 0

    # リスクレベルによる減点
    risk_penalty = {
        'HIGH': -30,
        'MEDIUM': -15,
        'LOW': 0,
        None: 0
    }
    recommendation_score += risk_penalty[tech_risk['overall_risk']]

    # 信頼性による加点
    credibility_bonus = {
        'HIGH': 40,
        'MEDIUM': 20,
        'LOW': 10,
        'NO_MENTION': 0
    }
    recommendation_score += credibility_bonus[social_cred['credibility_level']]

    # 選択スコアによる加点
    if social_cred['selection_score'] is not None:
        recommendation_score += int(social_cred['selection_score'] * 0.3)

    # ランキングによる加点
    if stock['grokRank'] <= 3:
        recommendation_score += 20
    elif stock['grokRank'] <= 5:
        recommendation_score += 10

    # 総合判定
    if recommendation_score >= 60:
        overall_recommendation = 'STRONG_BUY'
    elif recommendation_score >= 40:
        overall_recommendation = 'BUY'
    elif recommendation_score >= 20:
        overall_recommendation = 'HOLD'
    else:
        overall_recommendation = 'AVOID'

    return {
        'ticker': stock['ticker'],
        'stockName': stock['stockName'],
        'grokRank': stock['grokRank'],
        'selectionRank': stock['selectionRank'],
        'technicalRisk': tech_risk,
        'socialCredibility': social_cred,
        'marketPosition': market_pos,
        'recommendationScore': recommendation_score,
        'overallRecommendation': overall_recommendation,
        'reason': stock['trendingData']['reason'],
        'technicalData': stock['technicalData'],
    }

def generate_summary_statistics(analyses: List[dict]) -> dict:
    """サマリー統計"""

    # リスク分布
    risk_distribution = {
        'HIGH': sum(1 for a in analyses if a['technicalRisk']['overall_risk'] == 'HIGH'),
        'MEDIUM': sum(1 for a in analyses if a['technicalRisk']['overall_risk'] == 'MEDIUM'),
        'LOW': sum(1 for a in analyses if a['technicalRisk']['overall_risk'] == 'LOW'),
    }

    # 推奨分布
    recommendation_distribution = {
        'STRONG_BUY': sum(1 for a in analyses if a['overallRecommendation'] == 'STRONG_BUY'),
        'BUY': sum(1 for a in analyses if a['overallRecommendation'] == 'BUY'),
        'HOLD': sum(1 for a in analyses if a['overallRecommendation'] == 'HOLD'),
        'AVOID': sum(1 for a in analyses if a['overallRecommendation'] == 'AVOID'),
    }

    # 市場分布
    market_distribution = {}
    for a in analyses:
        market = a['marketPosition']['market']
        if market:
            market_distribution[market] = market_distribution.get(market, 0) + 1

    # セクター分布
    sector_distribution = {}
    for a in analyses:
        sector = a['marketPosition']['sectors']
        if sector:
            sector_distribution[sector] = sector_distribution.get(sector, 0) + 1

    # 平均スコア
    avg_recommendation_score = sum(a['recommendationScore'] for a in analyses) / len(analyses)

    return {
        'totalStocks': len(analyses),
        'riskDistribution': risk_distribution,
        'recommendationDistribution': recommendation_distribution,
        'marketDistribution': market_distribution,
        'sectorDistribution': sector_distribution,
        'avgRecommendationScore': avg_recommendation_score,
    }

def main():
    """メイン処理"""
    print("[INFO] Starting deep analysis of trading_recommendation.json...")

    # データ読み込み
    data = load_trading_recommendation()

    # 各銘柄を分析
    analyses = []
    for stock in data['stocks']:
        analysis = generate_stock_analysis(stock)
        analyses.append(analysis)
        print(f"[INFO] Analyzed: {stock['stockName']} - {analysis['overallRecommendation']} (Score: {analysis['recommendationScore']})")

    # サマリー統計
    summary = generate_summary_statistics(analyses)

    # 結果をJSON出力
    output = {
        'version': '1.0',
        'generatedAt': datetime.now().isoformat(),
        'sourceFile': 'trading_recommendation.json',
        'sourceDate': data['dataSource']['date'],
        'summary': summary,
        'stockAnalyses': analyses,
    }

    output_file = OUTPUT_DIR / f"deep_analysis_{data['dataSource']['date']}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n[SUCCESS] Generated: {output_file}")
    print(f"\n=== Summary Statistics ===")
    print(f"Total Stocks: {summary['totalStocks']}")
    print(f"Risk Distribution: {summary['riskDistribution']}")
    print(f"Recommendation Distribution: {summary['recommendationDistribution']}")
    print(f"Market Distribution: {summary['marketDistribution']}")
    print(f"Sector Distribution: {summary['sectorDistribution']}")
    print(f"Avg Recommendation Score: {summary['avgRecommendationScore']:.2f}")

    # Top推奨銘柄を表示
    print(f"\n=== Top Recommendations ===")
    top_recommendations = sorted(analyses, key=lambda x: x['recommendationScore'], reverse=True)[:5]
    for i, rec in enumerate(top_recommendations, 1):
        print(f"{i}. {rec['stockName']} ({rec['ticker']})")
        print(f"   Recommendation: {rec['overallRecommendation']} (Score: {rec['recommendationScore']})")
        print(f"   Risk: {rec['technicalRisk']['overall_risk']}, Credibility: {rec['socialCredibility']['credibility_level']}")
        print(f"   Close: {rec['technicalData']['close']}, Change: {rec['technicalData']['changePct']:.2f}%")
        print()

if __name__ == "__main__":
    main()
