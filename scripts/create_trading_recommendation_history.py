#!/usr/bin/env python3
"""
trading_recommendation.json を trading_recommendation_history.parquet に変換
独立したparquetファイルとして保存
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
OUTPUT_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'trading_recommendation_history.parquet'


def create_recommendation_history():
    """trading_recommendation.jsonを読み込んでparquetとして保存"""

    if not JSON_PATH.exists():
        logger.error(f"JSON file not found: {JSON_PATH}")
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

    # DataFrameに変換
    records = []
    for stock in data.get('stocks', []):
        rec = stock.get('recommendation', {})
        reasons = rec.get('reasons', [])

        records.append({
            'recommendation_date': recommendation_date,
            'generated_at': generated_at,
            'ticker': stock['ticker'],
            'recommendation_action': rec.get('action'),
            'recommendation_score': rec.get('score'),
            'recommendation_confidence': rec.get('confidence'),
            'recommendation_reasons_json': json.dumps(reasons, ensure_ascii=False) if reasons else None
        })

    df = pd.DataFrame(records)

    # 既存ファイルがあれば読み込んで追加
    if OUTPUT_PATH.exists():
        logger.info(f"Loading existing file: {OUTPUT_PATH}")
        existing_df = pd.read_parquet(OUTPUT_PATH)
        existing_df['recommendation_date'] = pd.to_datetime(existing_df['recommendation_date']).dt.date

        logger.info(f"  Existing records: {len(existing_df)}")
        logger.info(f"  Date range: {existing_df['recommendation_date'].min()} to {existing_df['recommendation_date'].max()}")

        # 同じ日付のデータを削除してから追加
        existing_df = existing_df[existing_df['recommendation_date'] != recommendation_date]
        df = pd.concat([existing_df, df], ignore_index=True)

        logger.info(f"  Removed {len(existing_df[existing_df['recommendation_date'] == recommendation_date])} records from {recommendation_date}")

    # ソート
    df = df.sort_values(['recommendation_date', 'ticker']).reset_index(drop=True)

    # 保存
    df.to_parquet(OUTPUT_PATH, index=False)
    logger.info(f"\nSaved to {OUTPUT_PATH}")
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Date range: {df['recommendation_date'].min()} to {df['recommendation_date'].max()}")

    # サマリー表示
    latest_df = df[df['recommendation_date'] == recommendation_date]
    buy_count = (latest_df['recommendation_action'] == 'buy').sum()
    sell_count = (latest_df['recommendation_action'] == 'sell').sum()
    hold_count = (latest_df['recommendation_action'] == 'hold').sum()

    logger.info(f"\nLatest ({recommendation_date}): {len(latest_df)} records")
    logger.info(f"  買い: {buy_count}, 売り: {sell_count}, 静観: {hold_count}")

    return df


def main():
    logger.info("Creating trading recommendation history...")
    create_recommendation_history()


if __name__ == '__main__':
    main()
