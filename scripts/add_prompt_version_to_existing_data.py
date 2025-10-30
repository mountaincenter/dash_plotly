#!/usr/bin/env python3
"""
既存のGROKデータに prompt_version カラムを追加

実行:
    python3 scripts/add_prompt_version_to_existing_data.py
"""

import sys
from pathlib import Path
import pandas as pd

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR


def add_prompt_version_to_parquet(file_path: Path, default_version: str = "v1_0_baseline"):
    """
    parquetファイルに prompt_version カラムを追加

    Args:
        file_path: 対象ファイルのパス
        default_version: デフォルトのバージョン（既存データは全てv1.0とみなす）
    """
    if not file_path.exists():
        print(f"⚠️  File not found: {file_path}")
        return

    print(f"\n{'='*80}")
    print(f"Processing: {file_path.name}")
    print(f"{'='*80}")

    # データ読み込み
    df = pd.read_parquet(file_path)
    print(f"[INFO] Loaded {len(df)} rows")
    print(f"[INFO] Columns: {df.columns.tolist()}")

    # prompt_version カラムがすでに存在するか確認
    if 'prompt_version' in df.columns:
        print(f"[INFO] prompt_version column already exists")
        print(f"[INFO] Value counts:")
        print(df['prompt_version'].value_counts())
        return

    # prompt_version カラムを追加
    df['prompt_version'] = default_version
    print(f"[OK] Added prompt_version column with value: {default_version}")

    # 保存
    df.to_parquet(file_path, index=False)
    print(f"[OK] Saved to: {file_path}")
    print(f"[OK] New columns: {df.columns.tolist()}")


def main():
    print("=" * 80)
    print("Add prompt_version to existing GROK data")
    print("=" * 80)

    # 対象ファイルのリスト
    files_to_process = [
        PARQUET_DIR / "grok_trending.parquet",
        PARQUET_DIR / "backtest" / "grok_trending_archive.parquet",
        PARQUET_DIR / "backtest" / "grok_trending_20251028.parquet",
        PARQUET_DIR / "backtest" / "grok_trending_20251030.parquet",
    ]

    # 実際に存在するファイルのみ処理
    existing_files = [f for f in files_to_process if f.exists()]

    print(f"\n[INFO] Found {len(existing_files)} files to process")
    for file_path in existing_files:
        print(f"  - {file_path.name}")

    # 各ファイルを処理
    for file_path in existing_files:
        add_prompt_version_to_parquet(file_path, default_version="v1_0_baseline")

    print("\n" + "=" * 80)
    print("[OK] All files processed successfully")
    print("=" * 80)


if __name__ == "__main__":
    main()
