#!/usr/bin/env python3
"""
一時スクリプト: grok_trending_20251119.parquet と 20251120.parquet を使って
grok_analysis_merged.parquet を更新
"""
import pandas as pd
import boto3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / 'data' / 'parquet'
BACKTEST_DIR = DATA_DIR / 'backtest'

S3_BUCKET = "stock-api-data"
S3_PREFIX = "parquet/"

s3 = boto3.client('s3', region_name='ap-northeast-1')

def download_from_s3(s3_key: str, local_path: Path):
    """S3からファイルをダウンロード"""
    try:
        s3.download_file(S3_BUCKET, s3_key, str(local_path))
        print(f"✅ Downloaded: {s3_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {s3_key}: {e}")
        return False

def upload_to_s3(local_path: Path, s3_key: str):
    """S3にアップロード"""
    try:
        s3.upload_file(str(local_path), S3_BUCKET, s3_key)
        print(f"✅ Uploaded: {s3_key}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload {s3_key}: {e}")
        return False

def run_backtest(grok_df: pd.DataFrame, prices_df: pd.DataFrame, backtest_date: pd.Timestamp) -> pd.DataFrame:
    """バックテスト実行"""
    print(f"\n[Backtest] Running for {backtest_date.date()}...")

    # 日付をYYYYMMDD形式の文字列に変換
    date_str = backtest_date.strftime('%Y%m%d')

    results = []

    for _, row in grok_df.iterrows():
        ticker = row['ticker']

        # 株価データを取得
        price_data = prices_df[
            (prices_df['ticker'] == ticker) &
            (prices_df['date'] == backtest_date)
        ]

        if price_data.empty:
            print(f"  ⚠️  No price data for {ticker} on {backtest_date.date()}")
            continue

        price_row = price_data.iloc[0]

        # バックテスト結果を生成
        result = {
            'backtest_date': backtest_date,
            'ticker': ticker,
            'stock_name': row.get('stock_name', ''),
            'grok_rank': row.get('grok_rank', None),
            'selected_time': row.get('selected_time', ''),
            'reason': row.get('reason', ''),
            'tags': row.get('tags', ''),

            # 株価データ
            'prev_close': price_row.get('prev_close'),
            'open': price_row.get('open'),
            'high': price_row.get('high'),
            'low': price_row.get('low'),
            'close': price_row.get('close'),
            'volume': price_row.get('volume'),

            # 利益計算（100株単位、終値ベース）
            'profit_per_100_shares': (price_row.get('close', 0) - price_row.get('open', 0)) * 100 if pd.notna(price_row.get('open')) and pd.notna(price_row.get('close')) else None,
            'profit_pct': ((price_row.get('close', 0) - price_row.get('open', 0)) / price_row.get('open', 1) * 100) if pd.notna(price_row.get('open')) and price_row.get('open', 0) != 0 and pd.notna(price_row.get('close')) else None,

            # 勝敗判定（終値ベース）
            'is_win': (price_row.get('close', 0) > price_row.get('open', 0)) if pd.notna(price_row.get('open')) and pd.notna(price_row.get('close')) else None,
        }

        results.append(result)

    result_df = pd.DataFrame(results)
    print(f"  ✅ Backtest completed: {len(result_df)} records")

    return result_df

def main():
    print("=" * 60)
    print("Temporary Script: Update grok_analysis_merged.parquet")
    print("Adding data for 2025-11-19 and 2025-11-20")
    print("=" * 60)

    # ディレクトリ作成
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: S3から必要なファイルをダウンロード
    print("\n[Step 1] Downloading required files from S3...")

    files_to_download = [
        (f"{S3_PREFIX}backtest/grok_trending_20251119.parquet", BACKTEST_DIR / "grok_trending_20251119.parquet"),
        (f"{S3_PREFIX}backtest/grok_trending_20251120.parquet", BACKTEST_DIR / "grok_trending_20251120.parquet"),
        (f"{S3_PREFIX}prices_max_1d.parquet", DATA_DIR / "prices_max_1d.parquet"),
    ]

    for s3_key, local_path in files_to_download:
        if not download_from_s3(s3_key, local_path):
            print(f"❌ Required file missing: {s3_key}")
            sys.exit(1)

    # ローカルの既存ファイルを確認
    existing_merged_path = BACKTEST_DIR / "grok_analysis_merged.parquet"
    if not existing_merged_path.exists():
        print(f"❌ Local grok_analysis_merged.parquet not found")
        sys.exit(1)
    print(f"✅ Using local grok_analysis_merged.parquet")

    # Step 2: 既存データを読み込み
    print("\n[Step 2] Loading existing data...")

    existing_df = pd.read_parquet(BACKTEST_DIR / "grok_analysis_merged.parquet")
    existing_df['backtest_date'] = pd.to_datetime(existing_df['backtest_date'])

    print(f"  Existing records: {len(existing_df)}")
    print(f"  Latest date: {existing_df['backtest_date'].max().date()}")

    prices_df = pd.read_parquet(DATA_DIR / "prices_max_1d.parquet")
    prices_df['date'] = pd.to_datetime(prices_df['date'])

    print(f"  Price data records: {len(prices_df)}")
    print(f"  Price date range: {prices_df['date'].min().date()} to {prices_df['date'].max().date()}")

    # Step 3: 2025-11-19のバックテスト
    print("\n[Step 3] Processing 2025-11-19...")

    grok_1119 = pd.read_parquet(BACKTEST_DIR / "grok_trending_20251119.parquet")
    backtest_date_1119 = pd.to_datetime('2025-11-19')

    result_1119 = run_backtest(grok_1119, prices_df, backtest_date_1119)

    # Step 4: 2025-11-20のバックテスト
    print("\n[Step 4] Processing 2025-11-20...")

    grok_1120 = pd.read_parquet(BACKTEST_DIR / "grok_trending_20251120.parquet")
    backtest_date_1120 = pd.to_datetime('2025-11-20')

    result_1120 = run_backtest(grok_1120, prices_df, backtest_date_1120)

    # Step 5: マージ
    print("\n[Step 5] Merging data...")

    # 既存データから2025-11-19と2025-11-20を削除（重複防止）
    existing_df_filtered = existing_df[
        ~existing_df['backtest_date'].isin([backtest_date_1119, backtest_date_1120])
    ].copy()

    print(f"  Existing data (excluding 2025-11-19, 2025-11-20): {len(existing_df_filtered)}")
    print(f"  New data for 2025-11-19: {len(result_1119)}")
    print(f"  New data for 2025-11-20: {len(result_1120)}")

    # 結合
    merged_df = pd.concat([existing_df_filtered, result_1119, result_1120], ignore_index=True)
    merged_df = merged_df.sort_values('backtest_date').reset_index(drop=True)

    print(f"  Total records: {len(merged_df)}")
    print(f"  Date range: {merged_df['backtest_date'].min().date()} to {merged_df['backtest_date'].max().date()}")

    # Step 6: 保存
    print("\n[Step 6] Saving...")

    output_path = BACKTEST_DIR / "grok_analysis_merged.parquet"
    merged_df.to_parquet(output_path, index=False)
    print(f"✅ Saved to {output_path}")

    # Step 7: S3にアップロード
    print("\n[Step 7] Uploading to S3...")

    s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged.parquet"
    if upload_to_s3(output_path, s3_key):
        print("✅ Upload successful")
    else:
        print("❌ Upload failed")
        sys.exit(1)

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(merged_df)}")
    print(f"Date range: {merged_df['backtest_date'].min().date()} to {merged_df['backtest_date'].max().date()}")
    print(f"Unique dates: {merged_df['backtest_date'].nunique()}")
    print("\n✅ Update completed successfully!")

if __name__ == '__main__':
    main()
