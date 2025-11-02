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
  python scripts/sync/download_from_s3.py --clean  # manifest.json以外を削除してから同期
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


def cleanup_local_parquet_files(exclude_manifest: bool = True) -> int:
    """
    ローカルのparquetファイルを削除

    注意: backtest/とmarket_summary/ディレクトリは保護されます（ローカル蓄積データのため）

    Args:
        exclude_manifest: Trueの場合、manifest.jsonは削除しない

    Returns:
        削除したファイル数
    """
    print("\n[CLEANUP] Removing local parquet files...")
    print("  ℹ backtest/ and market_summary/ directories are protected (local archive data)")

    if not PARQUET_DIR.exists():
        print("  ℹ No parquet directory found, skipping cleanup")
        return 0

    deleted_count = 0

    # ルートディレクトリの.parquetファイル削除
    for file_path in PARQUET_DIR.glob("*.parquet"):
        try:
            file_path.unlink()
            print(f"  ✓ Deleted: {file_path.name}")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete {file_path.name}: {e}")

    # backtestディレクトリは保護（削除しない）
    backtest_dir = PARQUET_DIR / "backtest"
    if backtest_dir.exists() and backtest_dir.is_dir():
        backtest_count = len(list(backtest_dir.glob("*.parquet")))
        print(f"  ℹ Protected: backtest/ ({backtest_count} archive files preserved)")

    # market_summaryディレクトリも保護（削除しない）
    market_summary_dir = PARQUET_DIR / "market_summary"
    if market_summary_dir.exists() and market_summary_dir.is_dir():
        raw_count = len(list((market_summary_dir / "raw").glob("*.md"))) if (market_summary_dir / "raw").exists() else 0
        structured_count = len(list((market_summary_dir / "structured").glob("*.json"))) if (market_summary_dir / "structured").exists() else 0
        print(f"  ℹ Protected: market_summary/ ({raw_count} markdown, {structured_count} json files preserved)")

    # manifest.jsonの扱い
    manifest_path = PARQUET_DIR / "manifest.json"
    if manifest_path.exists() and not exclude_manifest:
        try:
            manifest_path.unlink()
            print(f"  ✓ Deleted: manifest.json")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete manifest.json: {e}")
    elif manifest_path.exists():
        print(f"  ℹ Kept: manifest.json (excluded from cleanup)")

    print(f"  → Deleted {deleted_count} file(s)")
    return deleted_count


def download_all_from_s3(dry_run: bool = False, file_filter: List[str] = None, clean: bool = False) -> tuple[int, int]:
    """
    S3から全parquetファイルをダウンロード

    Args:
        dry_run: Trueの場合、ダウンロードせずに確認のみ
        file_filter: 特定のファイル名リスト（指定された場合はそれらのみダウンロード）
        clean: Trueの場合、ダウンロード前にローカルファイルをクリーンアップ

    Returns:
        (成功数, 失敗数)
    """
    print("=" * 60)
    print("Download from S3 to Local")
    if clean:
        print("Mode: CLEAN SYNC (delete local files first)")
    print("=" * 60)

    # S3設定読み込み
    try:
        cfg = load_s3_config()
        print(f"\nS3 Bucket: {cfg.bucket}")
        print(f"S3 Prefix: {cfg.prefix}")
        print(f"Local Dir: {PARQUET_DIR}")
    except Exception as e:
        print(f"\n✗ Failed to load S3 config: {e}")
        return 0, 0

    if dry_run:
        print("\n⚠️  DRY RUN MODE - No files will be downloaded\n")

    # クリーンアップ実行（cleanフラグが指定されている場合）
    if clean and not dry_run:
        cleanup_local_parquet_files(exclude_manifest=True)

    # S3のファイル一覧を取得
    print("\n[STEP 1] Listing S3 files...")
    try:
        s3_files = list_s3_files(cfg)
        if not s3_files:
            print("  ⚠ No files found in S3")
            return 0, 0

        # .parquetファイルのみフィルタ（manifest.jsonは除外）
        # backtest/配下のファイルも含めてダウンロード
        parquet_files = [f for f in s3_files if f.endswith('.parquet')]

        # file_filterが指定されている場合は、さらにフィルタ
        if file_filter:
            parquet_files = [f for f in parquet_files if f in file_filter]

        # ルートとbacktestに分類
        root_files = [f for f in parquet_files if not f.startswith('backtest/')]
        backtest_files = [f for f in parquet_files if f.startswith('backtest/')]

        print(f"  ✓ Found {len(parquet_files)} parquet file(s)")
        print(f"    - Root: {len(root_files)} file(s)")
        print(f"    - Backtest: {len(backtest_files)} file(s)")

        if not parquet_files:
            print("\n⚠️  No parquet files to download")
            return 0, 0

        # ファイル一覧表示
        print("\nFiles to download:")
        if root_files:
            print("  [Root]")
            for f in sorted(root_files):
                print(f"    - {f}")
        if backtest_files:
            print("  [Backtest]")
            for f in sorted(backtest_files):
                print(f"    - {f}")

    except Exception as e:
        print(f"  ✗ Failed to list S3 files: {e}")
        return 0, 0

    if dry_run:
        print(f"\n✅ Dry run completed - {len(parquet_files)} file(s) would be downloaded")
        return len(parquet_files), 0

    # ダウンロード実行
    print("\n[STEP 2] Downloading files...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # backtestディレクトリも作成
    backtest_dir = PARQUET_DIR / "backtest"
    backtest_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    fail_count = 0

    for i, filename in enumerate(sorted(parquet_files), 1):
        # 相対パスを保持してダウンロード（backtest/xxx.parquet -> PARQUET_DIR/backtest/xxx.parquet）
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

  # Clean sync (delete local parquet files first, then download)
  python scripts/sync/download_from_s3.py --clean

  # Download specific files only
  python scripts/sync/download_from_s3.py meta_jquants.parquet prices_max_1d.parquet
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without downloading'
    )
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Delete local parquet files before downloading (manifest.json is kept)'
    )
    parser.add_argument(
        'files',
        nargs='*',
        metavar='FILE',
        help='Specific file names to download (default: all files)'
    )

    args = parser.parse_args()

    success_count, fail_count = download_all_from_s3(
        dry_run=args.dry_run,
        file_filter=args.files if args.files else None,
        clean=args.clean
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
