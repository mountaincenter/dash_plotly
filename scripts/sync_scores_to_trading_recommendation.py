#!/usr/bin/env python3
"""
deep_analysis_2025-11-17.jsonのスコアをtrading_recommendation.jsonに反映
"""
import json
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
DEEP_ANALYSIS = BASE_DIR / "data/parquet/backtest/analysis/deep_analysis_2025-11-17.json"
TRADING_REC = BASE_DIR / "data/parquet/backtest/trading_recommendation.json"

def main():
    # データ読み込み
    print("[INFO] Loading files...")
    with open(DEEP_ANALYSIS, 'r', encoding='utf-8') as f:
        deep_analysis = json.load(f)

    with open(TRADING_REC, 'r', encoding='utf-8') as f:
        trading_rec = json.load(f)

    # deep_analysisからスコアマップを作成
    score_map = {}
    for stock in deep_analysis['stockAnalyses']:
        ticker = stock['ticker']
        score_map[ticker] = {
            'finalScore': stock['finalScore'],
            'recommendation': stock['recommendation'],
            'confidence': stock.get('confidence', 'medium'),
            'risks': stock.get('risks', []),
            'opportunities': stock.get('opportunities', [])
        }

    # trading_recommendation.jsonを更新
    updated_count = 0
    for stock in trading_rec['stocks']:
        ticker = stock['ticker']

        if ticker in score_map:
            v2_score = stock['recommendation']['score']  # 元のv2スコアを保存
            final_score = score_map[ticker]['finalScore']
            new_action = score_map[ticker]['recommendation']

            # v2スコアを別フィールドで保持
            stock['recommendation']['v2Score'] = v2_score
            # finalScoreをメインスコアとして設定
            stock['recommendation']['score'] = final_score
            stock['recommendation']['action'] = new_action.lower()
            stock['recommendation']['confidence'] = score_map[ticker]['confidence']

            # deepAnalysisセクションを追加
            if 'deepAnalysis' not in stock:
                stock['deepAnalysis'] = {}

            stock['deepAnalysis']['v2Score'] = v2_score
            stock['deepAnalysis']['finalScore'] = final_score
            stock['deepAnalysis']['risks'] = score_map[ticker].get('risks', [])
            stock['deepAnalysis']['opportunities'] = score_map[ticker].get('opportunities', [])

            print(f"[INFO] {ticker}: v2Score={v2_score}, finalScore={final_score}, action → {new_action}")
            updated_count += 1

    # サマリー更新
    buy_count = sum(1 for s in trading_rec['stocks'] if s['recommendation']['action'] == 'buy')
    sell_count = sum(1 for s in trading_rec['stocks'] if s['recommendation']['action'] == 'sell')
    hold_count = sum(1 for s in trading_rec['stocks'] if s['recommendation']['action'] == 'hold')

    trading_rec['summary'] = {
        'total': len(trading_rec['stocks']),
        'buy': buy_count,
        'sell': sell_count,
        'hold': hold_count
    }

    # タイムスタンプ更新
    trading_rec['generatedAt'] = datetime.now().isoformat()

    # 保存
    with open(TRADING_REC, 'w', encoding='utf-8') as f:
        json.dump(trading_rec, f, ensure_ascii=False, indent=2)

    print(f"\n[SUCCESS] Updated {updated_count} stocks in trading_recommendation.json")
    print(f"\n=== Summary ===")
    print(f"BUY: {buy_count}")
    print(f"HOLD: {hold_count}")
    print(f"SELL: {sell_count}")
    print(f"\nSaved: {TRADING_REC}")

if __name__ == '__main__':
    main()
