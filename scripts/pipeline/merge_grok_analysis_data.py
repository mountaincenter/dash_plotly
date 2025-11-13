#!/usr/bin/env python3
"""
既存のgrok_analysis_merged.parquetと新データをマージ

Usage:
  python3 scripts/pipeline/merge_grok_analysis_data.py <existing_file> <new_file> <output_file>
"""
import sys
import pandas as pd
from pathlib import Path


def main():
    if len(sys.argv) != 4:
        print("Usage: python3 merge_grok_analysis_data.py <existing_file> <new_file> <output_file>")
        sys.exit(1)

    existing_file = Path(sys.argv[1])
    new_file = Path(sys.argv[2])
    output_file = Path(sys.argv[3])

    # 既存データと新データを読み込み
    existing = pd.read_parquet(existing_file)
    new_data = pd.read_parquet(new_file)

    existing['backtest_date'] = pd.to_datetime(existing['backtest_date'])
    new_data['backtest_date'] = pd.to_datetime(new_data['backtest_date'])

    print(f'Existing data: {len(existing)} records ({existing["backtest_date"].min().date()} to {existing["backtest_date"].max().date()})')
    print(f'New data: {len(new_data)} records ({new_data["backtest_date"].min().date()} to {new_data["backtest_date"].max().date()})')

    # 既存データから新データの日付を削除（重複回避）
    new_dates = new_data['backtest_date'].unique()
    existing_filtered = existing[~existing['backtest_date'].isin(new_dates)]

    print(f'Existing data after filtering: {len(existing_filtered)} records')

    # マージ
    merged = pd.concat([existing_filtered, new_data], ignore_index=True)
    merged = merged.sort_values('backtest_date').reset_index(drop=True)

    print(f'Merged data: {len(merged)} records ({merged["backtest_date"].min().date()} to {merged["backtest_date"].max().date()})')

    # 保存
    merged.to_parquet(output_file, index=False)
    print(f'✅ Saved merged data to {output_file}')


if __name__ == "__main__":
    main()
