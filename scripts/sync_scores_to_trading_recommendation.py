#!/usr/bin/env python3
"""
deep_analysis_YYYY-MM-DD.jsonのスコアをtrading_recommendation.jsonに反映
trading_recommendation.jsonの日付と一致するdeep_analysis_*.jsonを自動検出
"""
import json
from pathlib import Path
from datetime import datetime
import sys

BASE_DIR = Path(__file__).parent.parent
ANALYSIS_DIR = BASE_DIR / "data/parquet/backtest/analysis"
TRADING_REC = BASE_DIR / "data/parquet/backtest/trading_recommendation.json"

def main():
    # trading_recommendation.json読み込み
    print("[INFO] Loading trading_recommendation.json...")
    if not TRADING_REC.exists():
        print(f"[ERROR] trading_recommendation.json not found: {TRADING_REC}")
        sys.exit(1)

    with open(TRADING_REC, 'r', encoding='utf-8') as f:
        trading_rec = json.load(f)

    # 日付を取得 (technicalDataDate + 1 = target_date)
    technical_date = trading_rec.get('dataSource', {}).get('technicalDataDate')

    if not technical_date:
        print("[ERROR] No technicalDataDate found in trading_recommendation.json")
        sys.exit(1)

    # target_date = technicalDataDate + 1
    from datetime import datetime, timedelta
    technical_dt = datetime.strptime(technical_date, '%Y-%m-%d')
    target_dt = technical_dt + timedelta(days=1)
    target_date = target_dt.strftime('%Y-%m-%d')

    print(f"[INFO] Trading recommendation date: {target_date} (technicalDataDate: {technical_date})")

    # 対応するdeep_analysis_*.jsonを探す
    deep_analysis_file = ANALYSIS_DIR / f"deep_analysis_{target_date}.json"

    if not deep_analysis_file.exists():
        print(f"[ERROR] deep_analysis_{target_date}.json not found")
        print(f"[INFO] Looking for files in {ANALYSIS_DIR}...")
        analysis_files = list(ANALYSIS_DIR.glob("deep_analysis_*.json"))
        if analysis_files:
            print(f"[INFO] Available files:")
            for f in sorted(analysis_files):
                print(f"  - {f.name}")
        sys.exit(1)

    print(f"[INFO] Loading {deep_analysis_file.name}...")
    with open(deep_analysis_file, 'r', encoding='utf-8') as f:
        deep_analysis = json.load(f)

    # deep_analysisからスコアマップを作成
    score_map = {}
    for stock in deep_analysis['stockAnalyses']:
        ticker = stock['ticker']
        score_map[ticker] = {
            'v2Score': stock.get('v2Score'),
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
            # deep_analysisからv2ScoreとfinalScoreを取得
            v2_score = score_map[ticker]['v2Score']
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
