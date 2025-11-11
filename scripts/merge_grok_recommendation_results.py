"""
Grok分析データと売買判断レポートをマージ

grok_analysis_base_latest.parquet に trading_recommendation_history.parquet の
判断結果（買い/売り/静観）を追加し、振り返り分析用データを作成する。
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'test_output'

# 入力ファイル
GROK_ANALYSIS_PATH = OUTPUT_DIR / 'grok_analysis_base_latest.parquet'
TRADING_REC_HISTORY_PATH = OUTPUT_DIR / 'trading_recommendation_history.parquet'

# 出力ファイル
OUTPUT_PATH = OUTPUT_DIR / 'grok_analysis_base_latest.parquet'


def main():
    logger.info("Loading Grok analysis data...")
    grok_df = pd.read_parquet(GROK_ANALYSIS_PATH)
    logger.info(f"  Loaded {len(grok_df)} records")
    logger.info(f"  Date range: {grok_df['backtest_date'].min().date()} to {grok_df['backtest_date'].max().date()}")

    logger.info("\nLoading trading recommendation history...")
    rec_df = pd.read_parquet(TRADING_REC_HISTORY_PATH)

    # recommendation_date をdatetime型に統一
    rec_df['recommendation_date'] = pd.to_datetime(rec_df['recommendation_date'])

    logger.info(f"  Loaded {len(rec_df)} records")
    logger.info(f"  Date range: {rec_df['recommendation_date'].min().date()} to {rec_df['recommendation_date'].max().date()}")
    logger.info(f"  Unique dates: {rec_df['recommendation_date'].nunique()}")

    # 日付別サマリー
    for date in sorted(rec_df['recommendation_date'].unique()):
        date_df = rec_df[rec_df['recommendation_date'] == date]
        buy_count = (date_df['action'] == 'buy').sum()
        sell_count = (date_df['action'] == 'sell').sum()
        hold_count = (date_df['action'] == 'hold').sum()
        logger.info(f"    {date.date()}: {len(date_df)} stocks (買い:{buy_count}, 売り:{sell_count}, 静観:{hold_count})")

    # マージ用にカラムを準備（backtest_date と recommendation_date でマッチング）
    grok_df['backtest_date_normalized'] = pd.to_datetime(grok_df['backtest_date']).dt.date
    rec_df['recommendation_date_normalized'] = rec_df['recommendation_date'].dt.date

    # マージ用のカラムを選択
    rec_merge_df = rec_df[['ticker', 'recommendation_date_normalized', 'action', 'score', 'confidence']].copy()
    rec_merge_df = rec_merge_df.rename(columns={
        'action': 'recommendation_action',
        'score': 'recommendation_score',
        'confidence': 'recommendation_confidence'
    })

    logger.info(f"\n  Recommendation data prepared: {len(rec_merge_df)} stocks")

    # マージ前のカラム確認
    logger.info(f"\nGrok analysis columns before merge: {len(grok_df.columns)}")

    # 既存のrecommendationカラムを削除（存在する場合）
    recommendation_cols = [col for col in grok_df.columns if col.startswith('recommendation_')]
    if recommendation_cols:
        logger.info(f"  Dropping existing recommendation columns: {recommendation_cols}")
        grok_df = grok_df.drop(columns=recommendation_cols)

    # マージ（left join: ticker + 日付）
    logger.info("\nMerging data...")
    merged_df = grok_df.merge(
        rec_merge_df,
        left_on=['ticker', 'backtest_date_normalized'],
        right_on=['ticker', 'recommendation_date_normalized'],
        how='left'
    )

    # 不要なカラムを削除
    merged_df = merged_df.drop(columns=['backtest_date_normalized', 'recommendation_date_normalized'])

    logger.info(f"  Merged data: {len(merged_df)} records")
    logger.info(f"  Columns after merge: {len(merged_df.columns)}")

    # マージ結果の確認
    has_recommendation = merged_df['recommendation_action'].notna().sum()
    logger.info(f"\n  Records with recommendation: {has_recommendation}/{len(merged_df)}")

    # 保存
    merged_df.to_parquet(OUTPUT_PATH, index=False)
    logger.info(f"\nSaved to {OUTPUT_PATH}")

    # サマリー表示
    logger.info("\n=== Summary ===")
    logger.info(f"Total records: {len(merged_df)}")
    logger.info(f"Records with recommendation: {has_recommendation}")

    if has_recommendation > 0:
        logger.info("\nRecommendation distribution (all dates):")
        rec_counts = merged_df['recommendation_action'].value_counts()
        for action, count in rec_counts.items():
            logger.info(f"  {action}: {count}")

        # 日付別の振り返り
        unique_dates = sorted(merged_df[merged_df['recommendation_action'].notna()]['backtest_date'].unique())
        logger.info(f"\nRecommendations by date:")
        for date in unique_dates:
            date_with_rec = merged_df[
                (merged_df['backtest_date'] == date) &
                (merged_df['recommendation_action'].notna())
            ]

            if len(date_with_rec) > 0:
                logger.info(f"\n  {pd.to_datetime(date).date()}: {len(date_with_rec)} stocks")
                for action in ['buy', 'sell', 'hold']:
                    action_df = date_with_rec[date_with_rec['recommendation_action'] == action]
                    if len(action_df) > 0:
                        win_rate = (action_df['phase2_win'].sum() / len(action_df)) * 100
                        avg_return = action_df['phase2_return_pct'].mean()
                        logger.info(f"    {action.upper()}: {len(action_df)} stocks, 勝率 {win_rate:.1f}%, 平均リターン {avg_return:.2f}%")


if __name__ == '__main__':
    main()
