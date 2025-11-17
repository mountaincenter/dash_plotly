#!/usr/bin/env python3
"""
grok_analysis_merged.parquet に trading_recommendation.json の売買推奨データをマージ

Usage:
  python3 scripts/pipeline/enrich_grok_analysis_with_recommendations.py
"""
import sys
import pandas as pd
import json
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import os

ROOT = Path(__file__).resolve().parents[2]

# パス設定
TRADING_REC_JSON = ROOT / 'data' / 'parquet' / 'backtest' / 'trading_recommendation.json'
GROK_ANALYSIS_PARQUET = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def download_from_s3(s3_key: str, local_path: Path) -> bool:
    """S3からファイルをダウンロード"""
    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        s3.download_file(S3_BUCKET, s3_key, str(local_path))
        print(f"✅ Downloaded from s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        print(f"⚠️  Could not download from S3: {e}")
        return False


def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """S3にファイルをアップロード"""
    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        s3.upload_file(str(local_path), S3_BUCKET, s3_key)
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")
        return False


def main():
    print("=" * 60)
    print("Enrich grok_analysis_merged.parquet with trading recommendations")
    print("=" * 60)

    # Step 1: S3から trading_recommendation.json をダウンロード（存在する場合）
    print("\n[Step 1] Downloading trading_recommendation.json from S3...")

    json_s3_key = f"{S3_PREFIX}backtest/trading_recommendation.json"
    TRADING_REC_JSON.parent.mkdir(parents=True, exist_ok=True)

    if not download_from_s3(json_s3_key, TRADING_REC_JSON):
        # S3にない場合、ローカルファイルをチェック
        if not TRADING_REC_JSON.exists():
            print("ℹ️  No trading_recommendation.json found in S3 or local")
            print("ℹ️  Skipping recommendation enrichment (this is normal if no manual analysis was done)")
            sys.exit(0)
        else:
            print(f"ℹ️  Using local file: {TRADING_REC_JSON}")

    # Step 2: JSONを読み込み
    print("\n[Step 2] Loading trading_recommendation.json...")
    with open(TRADING_REC_JSON, 'r', encoding='utf-8') as f:
        rec_data = json.load(f)

    print(f"✅ Loaded {len(rec_data['stocks'])} stocks from trading_recommendation.json")

    # 推奨データの日付を取得
    rec_date_str = rec_data.get('dataSource', {}).get('technicalDataDate')
    if not rec_date_str:
        print("❌ No technicalDataDate found in trading_recommendation.json")
        sys.exit(1)

    rec_date = pd.to_datetime(rec_date_str)
    print(f"  Recommendation date: {rec_date.date()}")

    # 推奨データをDataFrameに変換
    rec_list = []
    for stock in rec_data['stocks']:
        rec = stock['recommendation']
        deep = stock.get('deepAnalysis', {})

        rec_data_item = {
            'ticker': stock['ticker'],
            'recommendation_action': rec['action'],
            'recommendation_score': rec['score'],  # finalScore (メイン)
            'recommendation_confidence': rec['confidence']
        }

        # v2Score を追加（recommendationまたはdeepAnalysisから）
        if 'v2Score' in rec:
            rec_data_item['recommendation_v2_score'] = rec['v2Score']
        elif 'v2Score' in deep:
            rec_data_item['recommendation_v2_score'] = deep['v2Score']

        # finalScore を追加（deepAnalysisから、なければscoreを使用）
        if 'finalScore' in deep:
            rec_data_item['recommendation_final_score'] = deep['finalScore']
        else:
            rec_data_item['recommendation_final_score'] = rec['score']

        rec_list.append(rec_data_item)

    rec_df = pd.DataFrame(rec_list)
    print(f"  Actions: {rec_df['recommendation_action'].value_counts().to_dict()}")

    # v2ScoreとfinalScoreの統計
    if 'recommendation_v2_score' in rec_df.columns:
        print(f"  Records with v2Score: {rec_df['recommendation_v2_score'].notna().sum()}")
    if 'recommendation_final_score' in rec_df.columns:
        print(f"  Records with finalScore: {rec_df['recommendation_final_score'].notna().sum()}")

    # Step 3: grok_analysis_merged.parquet を読み込み
    print("\n[Step 3] Loading grok_analysis_merged.parquet...")

    if not GROK_ANALYSIS_PARQUET.exists():
        print(f"❌ File not found: {GROK_ANALYSIS_PARQUET}")
        sys.exit(1)

    grok_df = pd.read_parquet(GROK_ANALYSIS_PARQUET)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    print(f"✅ Loaded {len(grok_df)} records")
    print(f"  Date range: {grok_df['backtest_date'].min().date()} to {grok_df['backtest_date'].max().date()}")

    # Step 4: マージ
    print("\n[Step 4] Merging recommendation data...")

    # 対象日のデータのみをマージ対象とする
    grok_target = grok_df[grok_df['backtest_date'] == rec_date].copy()
    grok_other = grok_df[grok_df['backtest_date'] != rec_date].copy()

    print(f"  {rec_date.date()} records: {len(grok_target)}")
    print(f"  Other dates records: {len(grok_other)}")

    if len(grok_target) == 0:
        print(f"⚠️  No backtest data found for {rec_date.date()}")
        print("ℹ️  Recommendation data cannot be merged (backtest must be run first)")
        sys.exit(0)

    # 既存のrecommendationカラムを削除（存在する場合）
    recommendation_cols = [col for col in grok_target.columns if col.startswith('recommendation_')]
    if recommendation_cols:
        print(f"  Dropping existing recommendation columns: {recommendation_cols}")
        grok_target = grok_target.drop(columns=recommendation_cols)

    # マージ
    merged_target = grok_target.merge(
        rec_df,
        on='ticker',
        how='left'
    )

    print(f"  Merged {rec_date.date()} data: {len(merged_target)} records")
    print(f"  Records with recommendation: {merged_target['recommendation_action'].notna().sum()}")

    # 他の日付のデータと結合
    final_df = pd.concat([grok_other, merged_target], ignore_index=True)
    final_df = final_df.sort_values('backtest_date').reset_index(drop=True)

    print(f"\n✅ Final data: {len(final_df)} records")
    print(f"  Total with recommendation: {final_df['recommendation_action'].notna().sum()}")

    # Step 5: 保存
    print("\n[Step 5] Saving...")

    final_df.to_parquet(GROK_ANALYSIS_PARQUET, index=False)
    print(f"✅ Saved to {GROK_ANALYSIS_PARQUET}")

    # Step 6: S3にアップロード
    print("\n[Step 6] Uploading to S3...")

    parquet_s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged.parquet"
    if upload_to_s3(GROK_ANALYSIS_PARQUET, parquet_s3_key):
        print("✅ Upload successful")
    else:
        print("❌ Upload failed")
        sys.exit(1)

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(final_df)}")
    print(f"Records with recommendation: {final_df['recommendation_action'].notna().sum()}")

    if final_df['recommendation_action'].notna().sum() > 0:
        print("\nRecommendation distribution:")
        print(final_df['recommendation_action'].value_counts())

    print("\n✅ Recommendation enrichment completed successfully!")


if __name__ == '__main__':
    main()
