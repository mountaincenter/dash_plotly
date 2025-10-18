#!/usr/bin/env python3
"""
空のスキャルピングparquetファイルを作成するスクリプト
"""
import os
import pandas as pd

def main():
    # 空のDataFrameを作成（最低限必要なカラム）
    empty_df = pd.DataFrame({'ticker': []})

    # ファイルが存在しない場合のみ空のparquetファイルを生成
    for fname in ['scalping_entry.parquet', 'scalping_active.parquet']:
        if not os.path.exists(fname):
            empty_df.to_parquet(fname, index=False)
            print(f'✅ Created empty {fname}')
        else:
            print(f'ℹ️ {fname} already exists, skipping')

if __name__ == '__main__':
    main()
