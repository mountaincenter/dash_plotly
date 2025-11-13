#!/usr/bin/env python3
"""
grok_analysis_merged.parquet のデータ改竄を修正

背景:
- 2025-11-13に generate_trading_recommendation_v2.py を無許可実行
- その結果、recommendation_action/score などの不完全なデータが混入
- 正しいデータ: grok_trending + バックテスト結果のマージのみ

処理:
1. 現在の grok_analysis_merged.parquet をバックアップ（改竄データとして保存）
2. 元のgrok_trendingファイルから正しいデータを再構築
3. 正しい grok_analysis_merged.parquet を生成

注: trading_recommendation_history.parquetは廃止され、grok_analysis_merged.parquetに統合されました
"""
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import shutil

ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT / "data" / "parquet" / "backtest"

CORRUPTED_FILE = BACKTEST_DIR / "grok_analysis_merged.parquet"
BACKUP_FILE = BACKTEST_DIR / "grok_analysis_merged_corrupted_20251113.parquet"
CORRECT_FILE = BACKTEST_DIR / "grok_analysis_merged.parquet"


def main():
    print("[INFO] Starting data corruption fix...")

    # 1. 改竄データをバックアップ
    if CORRUPTED_FILE.exists():
        print(f"[INFO] Backing up corrupted data to: {BACKUP_FILE}")
        shutil.copy2(CORRUPTED_FILE, BACKUP_FILE)

        df_corrupted = pd.read_parquet(CORRUPTED_FILE)
        print(f"[INFO] Corrupted data backed up: {len(df_corrupted)} records")
        print(f"       Date range: {df_corrupted['backtest_date'].min()} to {df_corrupted['backtest_date'].max()}")
    else:
        print(f"[ERROR] Corrupted file not found: {CORRUPTED_FILE}")
        sys.exit(1)

    # 2. 正しいデータを再構築
    # create_grok_analysis_base_latest.py と merge_grok_recommendation_results.py を実行
    print("\n[INFO] Rebuilding correct data...")
    print("[INFO] Running create_grok_analysis_base_latest.py...")

    import subprocess

    # create_grok_analysis_base_latest.py を実行
    result1 = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "create_grok_analysis_base_latest.py")],
        cwd=ROOT,
        capture_output=True,
        text=True
    )

    if result1.returncode != 0:
        print(f"[ERROR] create_grok_analysis_base_latest.py failed:")
        print(result1.stderr)
        sys.exit(1)

    print("[SUCCESS] create_grok_analysis_base_latest.py completed")

    # merge_grok_recommendation_results.py を実行
    print("[INFO] Running merge_grok_recommendation_results.py...")
    result2 = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "merge_grok_recommendation_results.py")],
        cwd=ROOT,
        capture_output=True,
        text=True
    )

    if result2.returncode != 0:
        print(f"[ERROR] merge_grok_recommendation_results.py failed:")
        print(result2.stderr)
        sys.exit(1)

    print("[SUCCESS] merge_grok_recommendation_results.py completed")

    # 3. 正しいデータを確認
    if CORRECT_FILE.exists():
        df_correct = pd.read_parquet(CORRECT_FILE)
        df_correct['backtest_date'] = pd.to_datetime(df_correct['backtest_date'])

        print(f"\n[SUCCESS] Correct data generated: {len(df_correct)} records")
        print(f"          Date range: {df_correct['backtest_date'].min()} to {df_correct['backtest_date'].max()}")
        print(f"\nRecords by date:")
        print(df_correct.groupby('backtest_date').size().sort_index())

        # recommendation データが正しくマージされているか確認
        has_rec = df_correct['recommendation_action'].notna().sum()
        print(f"\n[INFO] Records with recommendation data: {has_rec}/{len(df_correct)}")

        print(f"\n[SUCCESS] Data corruption fix completed!")
        print(f"[INFO] Corrupted data backed up to: {BACKUP_FILE}")
        print(f"[INFO] Correct data saved to: {CORRECT_FILE}")
    else:
        print(f"[ERROR] Correct file not generated: {CORRECT_FILE}")
        sys.exit(1)


if __name__ == "__main__":
    main()
