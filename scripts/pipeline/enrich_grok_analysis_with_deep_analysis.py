#!/usr/bin/env python3
"""
grok_analysis_merged.parquet に deep_analysis_YYYY-MM-DD.json のデータをマージ

Usage:
  python3 scripts/pipeline/enrich_grok_analysis_with_deep_analysis.py
"""
import sys
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]

# パス設定
GROK_ANALYSIS_PARQUET = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'
DEEP_ANALYSIS_DIR = ROOT / 'data' / 'parquet' / 'backtest' / 'analysis'


def main():
    print("=" * 60)
    print("Enrich grok_analysis_merged.parquet with deep_analysis data")
    print("=" * 60)

    # Step 1: Load grok_analysis_merged.parquet
    print("\n[Step 1] Loading grok_analysis_merged.parquet...")
    if not GROK_ANALYSIS_PARQUET.exists():
        print(f"❌ File not found: {GROK_ANALYSIS_PARQUET}")
        sys.exit(1)

    grok_df = pd.read_parquet(GROK_ANALYSIS_PARQUET)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    print(f"✅ Loaded {len(grok_df)} records")
    print(f"  Date range: {grok_df['backtest_date'].min().date()} to {grok_df['backtest_date'].max().date()}")

    # Step 2: 各日付ごとにdeep_analysis_YYYY-MM-DD.jsonをマージ
    print("\n[Step 2] Merging deep_analysis files...")

    unique_dates = grok_df['backtest_date'].dt.date.unique()
    enriched_dfs = []

    for date in sorted(unique_dates):
        date_str = date.strftime('%Y-%m-%d')
        deep_analysis_file = DEEP_ANALYSIS_DIR / f'deep_analysis_{date_str}.json'

        # この日付のデータ
        date_df = grok_df[grok_df['backtest_date'].dt.date == date].copy()

        if not deep_analysis_file.exists():
            print(f"  ⚠️  {date_str}: No deep_analysis file, skipping")
            enriched_dfs.append(date_df)
            continue

        # deep_analysis_YYYY-MM-DD.jsonを読み込み
        with open(deep_analysis_file, 'r', encoding='utf-8') as f:
            deep_data = json.load(f)

        stock_analyses = deep_data.get('stockAnalyses', [])

        if not stock_analyses:
            print(f"  ⚠️  {date_str}: No stockAnalyses in file, skipping")
            enriched_dfs.append(date_df)
            continue

        # DataFrameに変換
        deep_records = []
        for stock in stock_analyses:
            record = {
                'ticker': stock['ticker'],
                # Scores
                'deep_v2_score': stock.get('v2Score'),
                'deep_final_score': stock.get('finalScore'),
                'deep_score_adjustment': stock.get('scoreAdjustment'),
                # Recommendation
                'deep_recommendation': stock.get('recommendation'),
                'deep_confidence': stock.get('confidence'),
                'deep_verdict': stock.get('verdict'),
                # Day trade
                'deep_day_trade_score': stock.get('dayTradeScore'),
                'deep_day_trade_recommendation': stock.get('dayTradeRecommendation'),
                'deep_day_trade_reasons_json': json.dumps(stock.get('dayTradeReasons', []), ensure_ascii=False) if stock.get('dayTradeReasons') else None,
                # Market/Sector
                'deep_sector_trend': stock.get('sectorTrend'),
                'deep_market_sentiment': stock.get('marketSentiment'),
                'deep_news_headline': stock.get('newsHeadline'),
                # Analysis details (JSON)
                'deep_latest_news_json': json.dumps(stock.get('latestNews', []), ensure_ascii=False) if stock.get('latestNews') else None,
                'deep_risks_json': json.dumps(stock.get('risks', []), ensure_ascii=False) if stock.get('risks') else None,
                'deep_opportunities_json': json.dumps(stock.get('opportunities', []), ensure_ascii=False) if stock.get('opportunities') else None,
                'deep_adjustment_reasons_json': json.dumps(stock.get('adjustmentReasons', []), ensure_ascii=False) if stock.get('adjustmentReasons') else None,
                # Additional data (JSON)
                'deep_earnings_json': json.dumps(stock.get('earnings', {}), ensure_ascii=False) if stock.get('earnings') else None,
                'deep_fundamentals_json': json.dumps(stock.get('fundamentals', {}), ensure_ascii=False) if stock.get('fundamentals') else None,
                'deep_company_info_json': json.dumps(stock.get('companyInfo', {}), ensure_ascii=False) if stock.get('companyInfo') else None,
                'deep_price_analysis_json': json.dumps(stock.get('priceAnalysis', {}), ensure_ascii=False) if stock.get('priceAnalysis') else None,
            }
            deep_records.append(record)

        deep_df = pd.DataFrame(deep_records)

        # 既存のdeep_*カラムを削除
        existing_deep_cols = [col for col in date_df.columns if col.startswith('deep_')]
        if existing_deep_cols:
            date_df = date_df.drop(columns=existing_deep_cols)

        # マージ
        enriched_date_df = date_df.merge(deep_df, on='ticker', how='left')

        matched_count = enriched_date_df['deep_final_score'].notna().sum()
        print(f"  ✅ {date_str}: Merged {matched_count}/{len(date_df)} records")

        enriched_dfs.append(enriched_date_df)

    # Step 3: 全日付のデータを結合
    print("\n[Step 3] Combining all dates...")
    final_df = pd.concat(enriched_dfs, ignore_index=True)
    final_df = final_df.sort_values('backtest_date').reset_index(drop=True)

    print(f"✅ Final data: {len(final_df)} records")

    # deep_*カラムの統計
    deep_cols = [col for col in final_df.columns if col.startswith('deep_')]
    if deep_cols:
        print(f"\nDeep analysis columns added: {len(deep_cols)}")
        sample_col = 'deep_final_score'
        if sample_col in final_df.columns:
            count = final_df[sample_col].notna().sum()
            print(f"  Records with deep_final_score: {count}/{len(final_df)}")

    # Step 4: 保存
    print("\n[Step 4] Saving...")
    final_df.to_parquet(GROK_ANALYSIS_PARQUET, index=False)
    print(f"✅ Saved to {GROK_ANALYSIS_PARQUET}")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(final_df)}")
    print(f"Total columns: {len(final_df.columns)}")
    print(f"Deep analysis columns: {len(deep_cols)}")
    print("\n✅ Deep analysis enrichment completed successfully!")


if __name__ == '__main__':
    main()
