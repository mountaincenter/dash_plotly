#!/usr/bin/env python3
"""
grok_analysis_merged.parquet の v2_action を v2.0.3 ロジックで修正

v2.0.3 ルール:
- 5,000円 ≤ prev_day_close < 10,000円 → 強制「買い」
- prev_day_close ≥ 10,000円 → 強制「売り」
- それ以外 → 既存のスコアベース判定を維持
"""
import pandas as pd
import boto3
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT / 'data' / 'parquet' / 'backtest'
PARQUET_PATH = BACKTEST_DIR / 'grok_analysis_merged.parquet'

S3_BUCKET = "stock-api-data"
S3_PREFIX = "parquet/"
AWS_REGION = "ap-northeast-1"

s3 = boto3.client('s3', region_name=AWS_REGION)

def apply_v2_0_3_logic(df: pd.DataFrame) -> pd.DataFrame:
    """v2.0.3 価格帯ロジックを適用"""

    # v2_actionが存在する行のみ処理
    mask = df['v2_action'].notna()

    # 5,000-10,000円: 強制「買い」
    price_5k_10k = (df['prev_day_close'] >= 5000) & (df['prev_day_close'] < 10000)
    df.loc[mask & price_5k_10k, 'v2_action'] = '買い'

    # 10,000円以上: 強制「売り」
    price_over_10k = df['prev_day_close'] >= 10000
    df.loc[mask & price_over_10k, 'v2_action'] = '売り'

    return df

def main():
    print("=" * 60)
    print("Fix v2_action with v2.0.3 Logic")
    print("=" * 60)

    # バックアップ作成
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = BACKTEST_DIR / f'grok_analysis_merged_backup_before_v2_0_3_fix_{timestamp}.parquet'

    print(f"\n[Step 1] Creating backup...")
    df = pd.read_parquet(PARQUET_PATH)
    df.to_parquet(backup_path, index=False)
    print(f"✅ Backup created: {backup_path.name}")

    # 修正前の状態を確認
    print(f"\n[Step 2] Current state (before fix):")
    v2_df = df[df['v2_action'].notna()].copy()

    for band_name, condition in [
        ('<5000', df['prev_day_close'] < 5000),
        ('5000-10000', (df['prev_day_close'] >= 5000) & (df['prev_day_close'] < 10000)),
        ('>10000', df['prev_day_close'] >= 10000)
    ]:
        band_df = v2_df[condition & v2_df['v2_action'].notna()]
        if len(band_df) > 0:
            print(f"\n  {band_name}円帯 ({len(band_df)}件):")
            for action in ['買い', '売り', '静観']:
                count = (band_df['v2_action'] == action).sum()
                if count > 0:
                    print(f"    {action}: {count}件")

    # v2.0.3ロジック適用
    print(f"\n[Step 3] Applying v2.0.3 logic...")
    df = apply_v2_0_3_logic(df)

    # 修正後の状態を確認
    print(f"\n[Step 4] After fix:")
    v2_df = df[df['v2_action'].notna()].copy()

    for band_name, condition in [
        ('<5000', df['prev_day_close'] < 5000),
        ('5000-10000', (df['prev_day_close'] >= 5000) & (df['prev_day_close'] < 10000)),
        ('>10000', df['prev_day_close'] >= 10000)
    ]:
        band_df = v2_df[condition & v2_df['v2_action'].notna()]
        if len(band_df) > 0:
            print(f"\n  {band_name}円帯 ({len(band_df)}件):")
            for action in ['買い', '売り', '静観']:
                count = (band_df['v2_action'] == action).sum()
                if count > 0:
                    print(f"    {action}: {count}件")

    # 保存
    print(f"\n[Step 5] Saving...")
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"✅ Saved to {PARQUET_PATH}")

    # S3にアップロード
    print(f"\n[Step 6] Uploading to S3...")
    s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged.parquet"
    s3.upload_file(str(PARQUET_PATH), S3_BUCKET, s3_key)
    print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(df)}")
    print(f"v2_action records: {df['v2_action'].notna().sum()}")

    v2_summary = df[df['v2_action'].notna()]['v2_action'].value_counts()
    print(f"\nv2_action distribution:")
    for action, count in v2_summary.items():
        print(f"  {action}: {count}件")

    print("\n✅ v2.0.3 logic applied successfully!")

if __name__ == '__main__':
    main()
