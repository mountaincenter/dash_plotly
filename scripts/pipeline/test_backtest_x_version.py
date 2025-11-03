#!/usr/bin/env python3
"""
test_backtest_x_version.py
x_search追加版のGrok Trending銘柄のバックテスト

使い方:
    python3 scripts/pipeline/test_backtest_x_version.py --date 2025-10-30
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 既存のバックテストスクリプトから関数をインポート
from scripts.pipeline.test_save_backtest_archive import (
    fetch_price_data,
    calculate_backtest_results,
)

import pandas as pd

TEST_OUTPUT_DIR = ROOT / "data" / "test_output"


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest x_search version")
    parser.add_argument("--date", type=str, required=True, help="Target date (YYYY-MM-DD)")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date

    # x_search版ファイルを読み込み
    date_str = target_date.replace("-", "")
    input_file = TEST_OUTPUT_DIR / f"grok_trending_add_x_{date_str}.parquet"

    if not input_file.exists():
        print(f"[ERROR] File not found: {input_file}")
        return 1

    print(f"[INFO] Loading {input_file.name}...")
    df_grok = pd.read_parquet(input_file)
    print(f"[OK] Loaded {len(df_grok)} stocks")

    # バックテスト実行（DataFrame全体を渡す）
    df_result = calculate_backtest_results(df_grok, target_date)
    
    # 保存
    output_file = TEST_OUTPUT_DIR / f"test_backtest_x_{date_str}.parquet"
    df_result.to_parquet(output_file, index=False)
    print(f"\n[OK] Saved: {output_file}")
    
    # アーカイブにも追加
    archive_file = TEST_OUTPUT_DIR / "test_backtest_x_archive.parquet"
    if archive_file.exists():
        df_archive = pd.read_parquet(archive_file)
        df_combined = pd.concat([df_archive, df_result], ignore_index=True)
    else:
        df_combined = df_result
    
    df_combined.to_parquet(archive_file, index=False)
    print(f"[OK] Updated archive: {archive_file}")
    
    # サマリー表示
    print(f"\n=== Backtest Summary ===")
    print(f"Date: {target_date}")
    print(f"Stocks: {len(df_result)}")
    print(f"Phase1 Win Rate: {df_result['phase1_win'].mean()*100:.1f}%")
    print(f"Phase2 Win Rate: {df_result['phase2_win'].mean()*100:.1f}%")
    print(f"Phase3-1% Win Rate: {df_result['phase3_1pct_win'].mean()*100:.1f}%")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
