#!/usr/bin/env python3
"""
prices_{period}_{interval}.parquetを元の7カラム形式に戻す

余計なメタデータカラムを削除し、本番環境と同じ形式にする
"""

import pandas as pd
from pathlib import Path

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'

# 本来のカラム（yfinance形式）
ORIGINAL_COLUMNS = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']

prices_files = [
    'prices_max_1d.parquet',
    'prices_60d_5m.parquet',
]

for prices_file in prices_files:
    print(f"\n{'='*60}")
    print(f"処理中: {prices_file}")
    print(f"{'='*60}")

    prices_path = DATA_DIR / prices_file
    df = pd.read_parquet(prices_path)

    print(f"元カラム数: {len(df.columns)}")
    print(f"元カラム: {list(df.columns)}")

    # 7カラムのみ抽出
    df_clean = df[ORIGINAL_COLUMNS].copy()

    print(f"\n新カラム数: {len(df_clean.columns)}")
    print(f"新カラム: {list(df_clean.columns)}")

    # 保存
    df_clean.to_parquet(prices_path, index=False)
    print(f"✅ 保存: {prices_path}")

    # サイズ確認
    size_mb = prices_path.stat().st_size / 1024 / 1024
    print(f"サイズ: {size_mb:.1f}MB")

print("\n" + "="*60)
print("完了: prices_{period}_{interval}.parquetを本番環境形式に復元")
print("="*60)
