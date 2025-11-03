#!/usr/bin/env python3
"""
cleanup_grok_trending.py

S3上のgrok_trending.parquet のデータをクリーンアップ
カラム構造は維持したまま、全レコードを削除

⚠️ 重要: このスクリプトは必ずバックアップ確認後に実行すること
"""

import sys
import argparse
from pathlib import Path

import pandas as pd

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.s3io import upload_file, download_file
from common_cfg.s3cfg import load_s3_config


def cleanup_grok_trending(s3_key: str = "grok_trending.parquet", dry_run: bool = False) -> bool:
    """
    S3上のgrok_trending.parquet をクリーンアップ

    Args:
        s3_key: S3キー（デフォルト: grok_trending.parquet）
        dry_run: True の場合は実際には書き込まない

    Returns:
        bool: 成功した場合 True
    """
    # S3設定を読み込み
    cfg = load_s3_config()
    if not cfg:
        print("❌ S3 not configured")
        return False

    print("=" * 60)
    print("Cleanup grok_trending.parquet on S3")
    print("=" * 60)
    print(f"S3 Key: s3://{cfg.bucket}/{s3_key}")

    # 一時ファイルパス
    temp_dir = Path("data/parquet/temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_file = temp_dir / "grok_trending_temp.parquet"

    # S3から現在のファイルをダウンロード
    try:
        print(f"\n📥 Downloading from S3...")
        if not download_file(cfg, s3_key, temp_file):
            print(f"❌ Failed to download from S3: {s3_key}")
            return False

        df_current = pd.read_parquet(temp_file)
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

    # S3にアップロード
    if dry_run:
        print("\n⚠️ DRY RUN mode - not uploading to S3")
        print("✅ Cleanup would succeed")
        # クリーンアップ
        temp_file.unlink(missing_ok=True)
        return True

    try:
        # 空のDataFrameをローカルファイルに保存
        df_empty.to_parquet(temp_file, index=False, engine='pyarrow')
        print(f"\n✅ Created empty file locally: {temp_file}")

        # S3にアップロード
        print(f"📤 Uploading to S3...")
        upload_file(cfg, temp_file, s3_key)
        print(f"✅ Successfully uploaded to S3: s3://{cfg.bucket}/{s3_key}")

        # 確認のため読み込み
        df_verify = pd.read_parquet(temp_file)
        print(f"\nVerification:")
        print(f"  Rows: {len(df_verify)}")
        print(f"  Columns: {len(df_verify.columns)}")

        # クリーンアップ
        temp_file.unlink(missing_ok=True)

        if len(df_verify) == 0 and len(df_verify.columns) == len(df_current.columns):
            print("✅ Cleanup verified successfully")
            return True
        else:
            print("⚠️ Unexpected verification result")
            return False

    except Exception as e:
        print(f"❌ Error uploading cleaned file: {e}")
        import traceback
        traceback.print_exc()
        # クリーンアップ
        temp_file.unlink(missing_ok=True)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="S3上のgrok_trending.parquet をクリーンアップ（カラム構造維持、全レコード削除）"
    )
    parser.add_argument(
        '--s3-key',
        default='grok_trending.parquet',
        help='S3キー（デフォルト: grok_trending.parquet）'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='DRY RUN モード（実際にはS3にアップロードしない）'
    )

    args = parser.parse_args()

    print("\n⚠️ IMPORTANT: This script should only run AFTER backup verification")
    print("   Make sure verify_grok_backup.py has completed successfully\n")

    success = cleanup_grok_trending(args.s3_key, args.dry_run)

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
