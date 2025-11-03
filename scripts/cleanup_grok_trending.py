#!/usr/bin/env python3
"""
cleanup_grok_trending.py

grok_trending.parquet のデータをクリーンアップ
カラム構造は維持したまま、全レコードを削除

⚠️ 重要: このスクリプトは必ずバックアップ確認後に実行すること
"""

import sys
import argparse
from pathlib import Path

import pandas as pd


def cleanup_grok_trending(parquet_path: str, dry_run: bool = False) -> bool:
    """
    grok_trending.parquet をクリーンアップ

    Args:
        parquet_path: grok_trending.parquet のパス
        dry_run: True の場合は実際には書き込まない

    Returns:
        bool: 成功した場合 True
    """
    parquet_file = Path(parquet_path)

    if not parquet_file.exists():
        print(f"❌ File not found: {parquet_path}")
        return False

    print("=" * 60)
    print("Cleanup grok_trending.parquet")
    print("=" * 60)
    print(f"Target file: {parquet_path}")

    # 現在のファイルを読み込み
    try:
        df_current = pd.read_parquet(parquet_path)
        print(f"\nCurrent data:")
        print(f"  Rows: {len(df_current)}")
        print(f"  Columns: {len(df_current.columns)}")

        if not df_current.empty:
            print(f"  Date: {df_current['date'].iloc[0] if 'date' in df_current.columns else 'N/A'}")

            if 'code' in df_current.columns:
                codes = df_current['code'].tolist()
                print(f"  Codes ({len(codes)}): {', '.join(str(c) for c in codes[:10])}")
                if len(codes) > 10:
                    print(f"           ... and {len(codes) - 10} more")

    except Exception as e:
        print(f"❌ Error reading current file: {e}")
        return False

    # カラム構造を維持したまま空のDataFrameを作成
    try:
        # 元のスキーマを取得
        schema = df_current.dtypes.to_dict()

        # 空のDataFrameを作成
        df_empty = pd.DataFrame(columns=df_current.columns)

        # 型を復元
        for col, dtype in schema.items():
            df_empty[col] = df_empty[col].astype(dtype)

        print(f"\nNew data (empty):")
        print(f"  Rows: {len(df_empty)}")
        print(f"  Columns: {len(df_empty.columns)}")
        print(f"  Column names: {df_empty.columns.tolist()[:5]}...")
        print(f"  Dtypes preserved: {all(df_empty.dtypes == df_current.dtypes)}")

    except Exception as e:
        print(f"❌ Error creating empty DataFrame: {e}")
        return False

    # ファイルに書き込み
    if dry_run:
        print("\n⚠️ DRY RUN mode - not writing to file")
        print("✅ Cleanup would succeed")
        return True

    try:
        df_empty.to_parquet(parquet_path, index=False, engine='pyarrow')
        print(f"\n✅ Successfully cleaned up: {parquet_path}")

        # 確認のため読み込み
        df_verify = pd.read_parquet(parquet_path)
        print(f"Verification:")
        print(f"  Rows: {len(df_verify)}")
        print(f"  Columns: {len(df_verify.columns)}")

        if len(df_verify) == 0 and len(df_verify.columns) == len(df_current.columns):
            print("✅ Cleanup verified successfully")
            return True
        else:
            print("⚠️ Unexpected verification result")
            return False

    except Exception as e:
        print(f"❌ Error writing cleaned file: {e}")
        import traceback
        traceback.print_exc()
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="grok_trending.parquet をクリーンアップ（カラム構造維持、全レコード削除）"
    )
    parser.add_argument(
        '--parquet-path',
        default='data/parquet/grok_trending.parquet',
        help='grok_trending.parquet のパス（デフォルト: data/parquet/grok_trending.parquet）'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='DRY RUN モード（実際には書き込まない）'
    )

    args = parser.parse_args()

    print("\n⚠️ IMPORTANT: This script should only run AFTER backup verification")
    print("   Make sure verify_grok_backup.py has completed successfully\n")

    success = cleanup_grok_trending(args.parquet_path, args.dry_run)

    if success:
        print("\n" + "=" * 60)
        print("✅ Cleanup completed successfully")
        print("=" * 60)
        return 0
    else:
        print("\n" + "=" * 60)
        print("❌ Cleanup failed")
        print("=" * 60)
        return 1


if __name__ == '__main__':
    sys.exit(main())
