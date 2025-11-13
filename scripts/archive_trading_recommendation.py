"""
売買判断レポートのアーカイブ管理

trading_recommendation.json を grok_analysis_merged.parquet にマージする。
recommendation_action, recommendation_score, recommendation_confidence列を追加。
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
JSON_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'trading_recommendation.json'
GROK_ANALYSIS_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'


def archive_recommendation():
    """現在のtrading_recommendation.jsonをgrok_analysis_merged.parquetにマージ"""

    if not JSON_PATH.exists():
        logger.error(f"JSON file not found: {JSON_PATH}")
        return None

    if not GROK_ANALYSIS_PATH.exists():
        logger.error(f"Grok analysis file not found: {GROK_ANALYSIS_PATH}")
        return None

    logger.info(f"Loading JSON from {JSON_PATH}")
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 生成日時を取得
    if 'generatedAt' in data:
        generated_at = pd.to_datetime(data['generatedAt'])
    else:
        generated_at = pd.to_datetime(datetime.fromtimestamp(JSON_PATH.stat().st_mtime))

    recommendation_date = generated_at.date()

    logger.info(f"  Generated at: {generated_at}")
    logger.info(f"  Recommendation date: {recommendation_date}")
    logger.info(f"  Total stocks: {len(data.get('stocks', []))}")

    # 推奨データをマッピング形式に変換
    recommendation_map = {}
    for stock in data.get('stocks', []):
        rec = stock.get('recommendation', {})
        reasons = rec.get('reasons', [])

        recommendation_map[stock['ticker']] = {
            'recommendation_action': rec.get('action'),
            'recommendation_score': rec.get('score'),
            'recommendation_confidence': rec.get('confidence'),
            'recommendation_reasons_json': json.dumps(reasons, ensure_ascii=False)
        }

    # grok_analysis_merged.parquetを読み込み
    logger.info(f"Loading grok analysis from {GROK_ANALYSIS_PATH}")
    grok_df = pd.read_parquet(GROK_ANALYSIS_PATH)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    logger.info(f"  Existing records: {len(grok_df)}")
    logger.info(f"  Date range: {grok_df['backtest_date'].min().date()} to {grok_df['backtest_date'].max().date()}")

    # 該当日付のデータにrecommendation情報を追加
    target_df = grok_df[grok_df['backtest_date'].dt.date == recommendation_date].copy()
    logger.info(f"  Target date {recommendation_date}: {len(target_df)} records")

    if len(target_df) == 0:
        logger.warning(f"No data found for date {recommendation_date}")
        return None

    # recommendation情報をマージ
    updated_count = 0
    for idx in target_df.index:
        ticker = grok_df.at[idx, 'ticker']
        if ticker in recommendation_map:
            rec_data = recommendation_map[ticker]
            grok_df.at[idx, 'recommendation_action'] = rec_data['recommendation_action']
            grok_df.at[idx, 'recommendation_score'] = rec_data['recommendation_score']
            grok_df.at[idx, 'recommendation_confidence'] = rec_data['recommendation_confidence']
            grok_df.at[idx, 'recommendation_reasons_json'] = rec_data['recommendation_reasons_json']
            updated_count += 1

    logger.info(f"  Updated {updated_count} records with recommendation data")

    # 保存
    grok_df.to_parquet(GROK_ANALYSIS_PATH, index=False)
    logger.info(f"\nSaved to {GROK_ANALYSIS_PATH}")

    # サマリー表示
    rec_df = grok_df[grok_df['recommendation_action'].notna()]
    logger.info("\n=== Archive Summary ===")
    logger.info(f"Total records with recommendations: {len(rec_df)}")

    if len(rec_df) > 0:
        buy_count = (rec_df['recommendation_action'] == 'buy').sum()
        sell_count = (rec_df['recommendation_action'] == 'sell').sum()
        hold_count = (rec_df['recommendation_action'] == 'hold').sum()
        logger.info(f"  買い: {buy_count}, 売り: {sell_count}, 静観: {hold_count}")

    return grok_df


def main():
    logger.info("Starting trading recommendation archive...")
    archive_recommendation()


if __name__ == '__main__':
    main()
