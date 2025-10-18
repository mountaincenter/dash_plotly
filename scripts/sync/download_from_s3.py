#!/usr/bin/env python3
"""
sync/download_from_s3.py
ローカル開発環境用: S3から全データファイルをダウンロードして同期

使用場面:
- ローカル開発環境でGitHub Actionsが更新した最新データを取得したい場合
- S3をシングルソースとして、ローカルデータを最新に同期したい場合
- 新しい開発環境をセットアップする際の初期データ取得

実行方法:
  python scripts/sync/download_from_s3.py
  python scripts/sync/download_from_s3.py --dry-run  # ダウンロードせずに確認のみ
  python scripts/sync/download_from_s3.py --files meta_jquants.parquet prices_max_1d.parquet  # 特定ファイルのみ
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file, list_s3_files
from common_cfg.s3cfg import load_s3_config


def download_all_from_s3(dry_run: bool = False, file_filter: List[str] = None) -> tuple[int, int]:
    """
    S3から全parquetファイルをダウンロード

    Args:
        dry_run: Trueの場合、ダウンロードせずに確認のみ
        file_filter: 特定のファイル名リスト（指定された場合はそれらのみダウンロード）

    Returns:
        (成功数, 失敗数)
    """
    print("=" * 60)
    print("Download from S3 to Local")
    print("=" * 60)

    # S3設定読み込み
    try:
        cfg = load_s3_config()
        print(f"\nS3 Bucket: {cfg['bucket']}")
        print(f"S3 Prefix: {cfg['prefix']}")
        print(f"Local Dir: {PARQUET_DIR}")
    except Exception as e:
        print(f"\n✗ Failed to load S3 config: {e}")
        return 0, 0

    if dry_run:
        print("\n⚠️  DRY RUN MODE - No files will be downloaded\n")

    # S3のファイル一覧を取得
    print("\n[STEP 1] Listing S3 files...")
    try:
        s3_files = list_s3_files(cfg)
        if not s3_files:
            print("  ⚠ No files found in S3")
            return 0, 0

        # .parquetファイルのみフィルタ（manifest.jsonは除外）
        parquet_files = [f for f in s3_files if f.endswith('.parquet')]

        # file_filterが指定されている場合は、さらにフィルタ
        if file_filter:
            parquet_files = [f for f in parquet_files if f in file_filter]

        print(f"  ✓ Found {len(parquet_files)} parquet file(s)")

        if not parquet_files:
            print("\n⚠️  No parquet files to download")
            return 0, 0

        # ファイル一覧表示
        print("\nFiles to download:")
        for f in sorted(parquet_files):
            print(f"  - {f}")

    except Exception as e:
        print(f"  ✗ Failed to list S3 files: {e}")
        return 0, 0

    if dry_run:
        print(f"\n✅ Dry run completed - {len(parquet_files)} file(s) would be downloaded")
        return len(parquet_files), 0

    # ダウンロード実行
    print("\n[STEP 2] Downloading files...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    success_count = 0
    fail_count = 0

    for i, filename in enumerate(sorted(parquet_files), 1):
        local_path = PARQUET_DIR / filename
        print(f"\n  [{i}/{len(parquet_files)}] {filename}")

        try:
            if download_file(cfg, filename, local_path):
                print(f"    ✓ Downloaded: {local_path}")
                success_count += 1
            else:
                print(f"    ✗ Failed to download")
                fail_count += 1
        except Exception as e:
            print(f"    ✗ Error: {e}")
            fail_count += 1

    return success_count, fail_count


def main() -> int:
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="Download parquet files from S3 to local environment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all files
  python scripts/sync/download_from_s3.py

  # Dry run (check only, no download)
  python scripts/sync/download_from_s3.py --dry-run

  # Download specific files only
  python scripts/sync/download_from_s3.py --files meta_jquants.parquet prices_max_1d.parquet
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without downloading'
    )
    parser.add_argument(
        '--files',
        nargs='+',
        metavar='FILE',
        help='Specific file names to download (e.g., meta_jquants.parquet)'
    )

    args = parser.parse_args()

    success_count, fail_count = download_all_from_s3(
        dry_run=args.dry_run,
        file_filter=args.files
    )

    # サマリー表示
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Success: {success_count}")
    print(f"Failed:  {fail_count}")
    print("=" * 60)

    if not args.dry_run:
        if fail_count == 0 and success_count > 0:
            print("\n✅ All files downloaded successfully!")
        elif success_count > 0:
            print(f"\n⚠️  Partial success: {fail_count} file(s) failed")
        else:
            print("\n❌ No files downloaded")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
