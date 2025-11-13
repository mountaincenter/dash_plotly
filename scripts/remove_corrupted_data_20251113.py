#!/usr/bin/env python3
"""
2025-11-13の改竄データを削除

問題:
- trading_recommendation_history.parquet には 2025-11-13 のデータが存在しない
- grok_analysis_merged.parquet の 2025-11-13 データは無許可実行時の不正データ
- この日のrecommendation_action/scoreは信頼できない

対応:
- 2025-11-13 のデータを完全に削除
- 改竄データを grok_analysis_merged_corrupted_20251113_only.parquet に保存
- 正しいデータ (2025-11-04 ~ 2025-11-12) のみを残す
"""
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT / "data" / "parquet" / "backtest"

CURRENT_FILE = BACKTEST_DIR / "grok_analysis_merged.parquet"
CORRUPTED_BACKUP = BACKTEST_DIR / "grok_analysis_merged_corrupted_20251113_only.parquet"
CLEAN_FILE = BACKTEST_DIR / "grok_analysis_merged.parquet"


def main():
    print("[INFO] Removing corrupted 2025-11-13 data...")

    # 現在のデータを読み込み
    df = pd.read_parquet(CURRENT_FILE)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    print(f"[INFO] Current data: {len(df)} records")
    print(f"       Date range: {df['backtest_date'].min().date()} to {df['backtest_date'].max().date()}")
    print()

    # 2025-11-13のデータを抽出（改竄データ）
    corrupted = df[df['backtest_date'] == '2025-11-13'].copy()
    print(f"[INFO] Corrupted data (2025-11-13): {len(corrupted)} records")
    print(f"       Tickers: {corrupted['ticker'].tolist()}")
    print(f"       Recommendation actions: {corrupted['recommendation_action'].tolist() if 'recommendation_action' in corrupted.columns else 'N/A'}")
    print()

    # 改竄データをバックアップ
    corrupted.to_parquet(CORRUPTED_BACKUP, index=False)
    print(f"[SUCCESS] Corrupted data backed up to: {CORRUPTED_BACKUP}")

    # 2025-11-13以外のデータ（正しいデータ）
    clean = df[df['backtest_date'] != '2025-11-13'].copy()
    print(f"[INFO] Clean data: {len(clean)} records")
    print(f"       Date range: {clean['backtest_date'].min().date()} to {clean['backtest_date'].max().date()}")
    print()
    print("Records by date:")
    for date in sorted(clean['backtest_date'].unique()):
        count = len(clean[clean['backtest_date'] == date])
        print(f"  {date.date()}: {count} records")

    # 正しいデータを保存
    clean.to_parquet(CLEAN_FILE, index=False)
    print(f"\n[SUCCESS] Clean data saved to: {CLEAN_FILE}")
    print(f"[INFO] Removed {len(corrupted)} corrupted records from 2025-11-13")


if __name__ == "__main__":
    main()
