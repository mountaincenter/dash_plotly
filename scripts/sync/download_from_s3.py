#!/usr/bin/env python3
"""
sync/download_from_s3.py
ローカル開発環境用: S3から全データファイル（.parquet, .md, .json）をダウンロードして同期

使用場面:
- ローカル開発環境でGitHub Actionsが更新した最新データを取得したい場合
- S3をシングルソースとして、ローカルデータを最新に同期したい場合
- 新しい開発環境をセットアップする際の初期データ取得

ダウンロード対象:
- ルートディレクトリの .parquet ファイル
- backtest/ ディレクトリの .parquet ファイル（直近7日分のgrok_trending_*のみ）
- market_summary/raw/ ディレクトリの .md ファイル（直近7日分のみ）
- market_summary/structured/ ディレクトリの .json ファイル（直近7日分のみ）

実行方法:
  python scripts/sync/download_from_s3.py
  python scripts/sync/download_from_s3.py --dry-run  # ダウンロードせずに確認のみ
  python scripts/sync/download_from_s3.py --clean  # manifest.json以外を削除してから同期
  python scripts/sync/download_from_s3.py --files meta_jquants.parquet prices_max_1d.parquet  # 特定ファイルのみ
  python scripts/sync/download_from_s3.py --days 14  # 直近14日分をダウンロード（デフォルト7日）
"""

from __future__ import annotations

import re
import shutil
import sys
import argparse
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file, list_s3_files
from common_cfg.s3cfg import load_s3_config


def extract_date_from_filename(filename: str) -> Optional[datetime]:
    """
    ファイル名から日付を抽出

    対応フォーマット:
    - YYYY-MM-DD.md / YYYY-MM-DD.json (market_summary)
    - grok_trending_YYYYMMDD.parquet (backtest)

    Returns:
        datetime or None
    """
    # YYYY-MM-DD パターン（market_summary）
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d')
        except ValueError:
            pass

    # YYYYMMDD パターン（grok_trending_）
    match = re.search(r'grok_trending_(\d{8})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y%m%d')
        except ValueError:
            pass

    return None


def archive_old_files(days: int = 7, dry_run: bool = False) -> int:
    """
    7日以上前のファイルをarchiveディレクトリに移動

    Args:
        days: この日数より古いファイルをアーカイブ
        dry_run: Trueの場合、移動せずに確認のみ

    Returns:
        移動したファイル数
    """
    print(f"\n[ARCHIVE] Moving files older than {days} days to archive/...")

    cutoff_date = datetime.now() - timedelta(days=days)
    archive_count = 0

    # アーカイブ先ディレクトリ
    archive_base = PARQUET_DIR / "archive"

    # market_summary/raw/*.md
    raw_dir = PARQUET_DIR / "market_summary" / "raw"
    if raw_dir.exists():
        archive_raw = archive_base / "market_summary" / "raw"
        for file_path in raw_dir.glob("*.md"):
            file_date = extract_date_from_filename(file_path.name)
            if file_date and file_date < cutoff_date:
                if dry_run:
                    print(f"  [DRY] Would move: {file_path.name}")
                else:
                    archive_raw.mkdir(parents=True, exist_ok=True)
                    dest = archive_raw / file_path.name
                    shutil.move(str(file_path), str(dest))
                    print(f"  ✓ Archived: market_summary/raw/{file_path.name}")
                archive_count += 1

    # market_summary/structured/*.json
    structured_dir = PARQUET_DIR / "market_summary" / "structured"
    if structured_dir.exists():
        archive_structured = archive_base / "market_summary" / "structured"
        for file_path in structured_dir.glob("*.json"):
            file_date = extract_date_from_filename(file_path.name)
            if file_date and file_date < cutoff_date:
                if dry_run:
                    print(f"  [DRY] Would move: {file_path.name}")
                else:
                    archive_structured.mkdir(parents=True, exist_ok=True)
                    dest = archive_structured / file_path.name
                    shutil.move(str(file_path), str(dest))
                    print(f"  ✓ Archived: market_summary/structured/{file_path.name}")
                archive_count += 1

    # backtest/grok_trending_*.parquet
    backtest_dir = PARQUET_DIR / "backtest"
    if backtest_dir.exists():
        archive_backtest = archive_base / "backtest"
        for file_path in backtest_dir.glob("grok_trending_202*.parquet"):
            file_date = extract_date_from_filename(file_path.name)
            if file_date and file_date < cutoff_date:
                if dry_run:
                    print(f"  [DRY] Would move: {file_path.name}")
                else:
                    archive_backtest.mkdir(parents=True, exist_ok=True)
                    dest = archive_backtest / file_path.name
                    shutil.move(str(file_path), str(dest))
                    print(f"  ✓ Archived: backtest/{file_path.name}")
                archive_count += 1

    print(f"  → {'Would archive' if dry_run else 'Archived'} {archive_count} file(s)")
    return archive_count


def is_recent_dated_file(filename: str, days: int = 7) -> bool:
    """
    ファイルが直近N日以内かどうかを判定

    日付を含まないファイルはTrue（常にダウンロード対象）

    Args:
        filename: ファイル名
        days: 直近何日以内を対象とするか

    Returns:
        直近N日以内 or 日付なしファイルならTrue
    """
    file_date = extract_date_from_filename(filename)
    if file_date is None:
        # 日付を含まないファイルは常にダウンロード対象
        return True

    cutoff_date = datetime.now() - timedelta(days=days)
    return file_date >= cutoff_date


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


def download_all_from_s3(
    dry_run: bool = False,
    file_filter: List[str] = None,
    clean: bool = False,
    days: int = 7
) -> tuple[int, int]:
    """
    S3から全データファイル（.parquet, .md, .json）をダウンロード

    Args:
        dry_run: Trueの場合、ダウンロードせずに確認のみ
        file_filter: 特定のファイル名リスト（指定された場合はそれらのみダウンロード）
        clean: Trueの場合、ダウンロード前にローカルファイルをクリーンアップ
        days: 直近何日分の日付付きファイルをダウンロードするか（デフォルト7日）

    Returns:
        (成功数, 失敗数)
    """
    print("=" * 60)
    print("Download from S3 to Local")
    if clean:
        print("Mode: CLEAN SYNC (delete local files first)")
    print(f"Date filter: Last {days} days for dated files")
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

    # 古いファイルをアーカイブ
    archive_old_files(days=days, dry_run=dry_run)

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

        # .parquet, .md, .jsonファイルをフィルタ（manifest.jsonは除外）
        # backtest/配下およびmarket_summary/配下のファイルも含めてダウンロード
        all_files = [f for f in s3_files if (f.endswith('.parquet') or f.endswith('.md') or f.endswith('.json')) and f != 'manifest.json']

        # 直近N日分のみフィルタ（日付を含むファイルのみ）
        download_files = [f for f in all_files if is_recent_dated_file(f, days=days)]

        # file_filterが指定されている場合は、さらにフィルタ
        if file_filter:
            download_files = [f for f in download_files if f in file_filter]

        # ルート、backtest、market_summaryに分類
        root_files = [f for f in download_files if not f.startswith('backtest/') and not f.startswith('market_summary/')]
        backtest_files = [f for f in download_files if f.startswith('backtest/')]
        market_summary_files = [f for f in download_files if f.startswith('market_summary/')]

        # スキップされたファイル数
        skipped_count = len(all_files) - len(download_files)

        print(f"  ✓ Found {len(all_files)} total file(s) in S3")
        print(f"  ✓ Downloading {len(download_files)} file(s) (last {days} days)")
        if skipped_count > 0:
            print(f"  ℹ Skipped {skipped_count} older file(s)")
        print(f"    - Root: {len(root_files)} file(s)")
        print(f"    - Backtest: {len(backtest_files)} file(s)")
        print(f"    - Market Summary: {len(market_summary_files)} file(s)")

        if not download_files:
            print("\n⚠️  No files to download")
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
        if market_summary_files:
            print("  [Market Summary]")
            for f in sorted(market_summary_files):
                print(f"    - {f}")

    except Exception as e:
        print(f"  ✗ Failed to list S3 files: {e}")
        return 0, 0

    if dry_run:
        print(f"\n✅ Dry run completed - {len(download_files)} file(s) would be downloaded")
        return len(download_files), 0

    # ダウンロード実行
    print("\n[STEP 2] Downloading files...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # backtestディレクトリも作成
    backtest_dir = PARQUET_DIR / "backtest"
    backtest_dir.mkdir(parents=True, exist_ok=True)

    # market_summaryディレクトリも作成
    market_summary_dir = PARQUET_DIR / "market_summary"
    market_summary_dir.mkdir(parents=True, exist_ok=True)
    (market_summary_dir / "raw").mkdir(parents=True, exist_ok=True)
    (market_summary_dir / "structured").mkdir(parents=True, exist_ok=True)

    success_count = 0
    fail_count = 0

    for i, filename in enumerate(sorted(download_files), 1):
        # 相対パスを保持してダウンロード
        # 例: backtest/xxx.parquet -> PARQUET_DIR/backtest/xxx.parquet
        #     market_summary/raw/xxx.md -> PARQUET_DIR/market_summary/raw/xxx.md
        local_path = PARQUET_DIR / filename
        # 親ディレクトリが存在しない場合は作成
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"\n  [{i}/{len(download_files)}] {filename}")

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
  # Download all files (last 7 days for dated files)
  python scripts/sync/download_from_s3.py

  # Dry run (check only, no download)
  python scripts/sync/download_from_s3.py --dry-run

  # Clean sync (delete local parquet files first, then download)
  python scripts/sync/download_from_s3.py --clean

  # Download last 14 days of dated files
  python scripts/sync/download_from_s3.py --days 14

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
        '--days',
        type=int,
        default=7,
        help='Number of days to download for dated files (default: 7)'
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
        clean=args.clean,
        days=args.days
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
