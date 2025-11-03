#!/usr/bin/env python3
"""
verify_grok_backup.py

S3上のGrok trending銘柄のバックアップファイルの存在を確認

Exit codes:
  0: バックアップが正常に存在する
  1: バックアップが不完全または存在しない
"""

import argparse
import sys
import os
from datetime import datetime
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
import pandas as pd


def verify_s3_backup(bucket: str, date: str) -> bool:
    """
    S3上のバックアップファイルの存在を確認

    Args:
        bucket: S3バケット名
        date: YYYYMMDD形式の日付

    Returns:
        bool: バックアップが存在すればTrue
    """
    s3_client = boto3.client('s3')

    print(f"🔍 Verifying S3 backups for date: {date}")
    print(f"📦 Bucket: {bucket}")

    # 1. YYYYMMDD.parquet の確認
    key_daily = f"parquet/backtest/grok_trending_{date}.parquet"
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key_daily)
        size = response['ContentLength']
        last_modified = response['LastModified']
        print(f"✅ S3 daily backup exists: s3://{bucket}/{key_daily}")
        print(f"   Size: {size:,} bytes, Last modified: {last_modified}")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"❌ S3 daily backup NOT found: s3://{bucket}/{key_daily}")
            return False
        else:
            print(f"❌ Error checking daily backup: {e}")
            raise

    # 2. archive.parquet の確認
    key_archive = "parquet/backtest/grok_trending_archive.parquet"
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key_archive)
        size = response['ContentLength']
        last_modified = response['LastModified']
        print(f"✅ S3 archive exists: s3://{bucket}/{key_archive}")
        print(f"   Size: {size:,} bytes, Last modified: {last_modified}")

        # ダウンロードして該当日のデータを確認
        print(f"   Downloading archive to verify date: {date}")
        obj_response = s3_client.get_object(Bucket=bucket, Key=key_archive)
        df_archive = pd.read_parquet(BytesIO(obj_response['Body'].read()))

        if df_archive.empty:
            print(f"⚠️ Archive is empty")
            return False

        # 該当日のデータが存在するか確認
        target_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"

        if 'backtest_date' not in df_archive.columns:
            print(f"❌ Archive does not have 'backtest_date' column")
            return False

        # backtest_date が datetime.date オブジェクトの場合もあるため、文字列に変換
        df_archive['backtest_date_str'] = df_archive['backtest_date'].astype(str)
        df_day = df_archive[df_archive['backtest_date_str'] == target_date]

        if len(df_day) > 0:
            print(f"✅ Archive contains data for {target_date}: {len(df_day)} records")

            # 詳細情報を表示
            if 'code' in df_day.columns:
                codes = df_day['code'].unique()
                print(f"   Codes: {', '.join(str(c) for c in codes[:10])}")
                if len(codes) > 10:
                    print(f"   ... and {len(codes) - 10} more")

            return True
        else:
            print(f"❌ Archive does NOT contain data for {target_date}")
            print(f"   Available dates in archive: {df_archive['backtest_date'].unique()[:10]}")
            return False

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"❌ S3 archive NOT found: s3://{bucket}/{key_archive}")
            return False
        else:
            print(f"❌ Error checking archive: {e}")
            raise
    except Exception as e:
        print(f"❌ Error processing archive: {e}")
        raise


def get_target_date_from_parquet(parquet_path: str) -> str | None:
    """
    grok_trending.parquet から対象日付を取得

    Args:
        parquet_path: grok_trending.parquet のパス

    Returns:
        YYYYMMDD形式の日付、取得できない場合はNone
    """
    try:
        if not os.path.exists(parquet_path):
            print(f"⚠️ Local file not found: {parquet_path}")
            return None

        df = pd.read_parquet(parquet_path)

        if df.empty:
            print(f"⚠️ grok_trending.parquet is empty")
            return None

        if 'date' not in df.columns:
            print(f"❌ grok_trending.parquet does not have 'date' column")
            return None

        # 最初の行の日付を取得（全行同じ日付のはず）
        date_str = df['date'].iloc[0]

        # YYYY-MM-DD -> YYYYMMDD
        date_yyyymmdd = date_str.replace('-', '')

        return date_yyyymmdd

    except Exception as e:
        print(f"❌ Error reading grok_trending.parquet: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="S3上のGrok trending銘柄のバックアップを確認"
    )
    parser.add_argument(
        '--bucket',
        required=True,
        help='S3バケット名'
    )
    parser.add_argument(
        '--date',
        help='確認する日付（YYYYMMDD形式）。指定しない場合はgrok_trending.parquetから取得'
    )
    parser.add_argument(
        '--parquet-path',
        default='data/parquet/grok_trending.parquet',
        help='grok_trending.parquetのパス（デフォルト: data/parquet/grok_trending.parquet）'
    )

    args = parser.parse_args()

    # 日付の取得
    if args.date:
        target_date = args.date
        print(f"📅 Using specified date: {target_date}")
    else:
        print(f"📅 Reading date from: {args.parquet_path}")
        target_date = get_target_date_from_parquet(args.parquet_path)

        if not target_date:
            print("❌ Could not determine target date")
            sys.exit(1)

        print(f"📅 Detected date: {target_date}")

    # 日付フォーマットの検証
    try:
        datetime.strptime(target_date, '%Y%m%d')
    except ValueError:
        print(f"❌ Invalid date format: {target_date} (expected YYYYMMDD)")
        sys.exit(1)

    # S3バックアップの確認
    try:
        backup_ok = verify_s3_backup(args.bucket, target_date)

        if backup_ok:
            print("\n✅ All backups verified successfully")
            sys.exit(0)
        else:
            print("\n❌ Backup verification failed")
            print("⚠️ Aborting to prevent data loss")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ Verification error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
