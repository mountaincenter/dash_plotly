#!/usr/bin/env python3
"""
grok_day_trade_list.parquet に grok_trending.parquet の銘柄を追加するスクリプト

処理フロー:
1. S3から grok_day_trade_list.parquet をダウンロード
2. grok_trending.parquet から新規銘柄を抽出
3. 既存銘柄は上書きせず、新規銘柄のみ追加
4. S3に grok_day_trade_list.parquet をアップロード

重要: 既存データの上書きは厳禁
"""

import os
import sys
import re
import pandas as pd
import boto3
from io import BytesIO

# 設定
S3_BUCKET = os.environ.get("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.environ.get("S3_PREFIX", "parquet/")
DAY_TRADE_LIST_FILE = "grok_day_trade_list.parquet"
META_JQUANTS_FILE = "meta_jquants.parquet"
GROK_TRENDING_PATH = "data/parquet/grok_trending.parquet"

# ticker形式の検証パターン（XXXX.T）
TICKER_PATTERN = re.compile(r'^\d+[A-Z]?\.T$')

# 必須カラム
EXPECTED_COLUMNS = ['ticker', 'stock_name', 'shortable', 'day_trade', 'ng', 'day_trade_available_shares']


def download_from_s3(s3_client, key: str) -> pd.DataFrame | None:
    """S3からparquetファイルをダウンロード"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response["Body"].read()))
    except s3_client.exceptions.NoSuchKey:
        print(f"  ⚠️ ファイルが存在しません: s3://{S3_BUCKET}/{key}")
        return None
    except Exception as e:
        print(f"  ❌ ダウンロードエラー: {e}")
        return None


def upload_to_s3(s3_client, df: pd.DataFrame, key: str) -> bool:
    """DataFrameをS3にparquet形式でアップロード"""
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
        return True
    except Exception as e:
        print(f"  ❌ アップロードエラー: {e}")
        return False


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """DataFrameの検証: ticker形式とカラムをチェック"""
    errors = []

    # カラムチェック
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        errors.append(f"カラム不足: {missing_cols}")

    # ticker形式チェック
    invalid_tickers = df[~df['ticker'].apply(lambda x: bool(TICKER_PATTERN.match(str(x))))]['ticker'].tolist()
    if invalid_tickers:
        errors.append(f"不正なticker形式: {invalid_tickers[:5]}...")  # 最初の5件のみ表示

    return len(errors) == 0, errors


def main():
    print("=" * 60)
    print("grok_day_trade_list.parquet 銘柄追加処理")
    print("=" * 60)
    print()

    # S3クライアント初期化
    s3_client = boto3.client("s3")
    s3_key = f"{S3_PREFIX}{DAY_TRADE_LIST_FILE}"

    # Step 1: grok_trending.parquet を読み込み（ローカル）
    print("📥 Step 1: grok_trending.parquet を読み込み...")
    if not os.path.exists(GROK_TRENDING_PATH):
        print(f"  ❌ ERROR: {GROK_TRENDING_PATH} が見つかりません")
        sys.exit(1)

    df_grok = pd.read_parquet(GROK_TRENDING_PATH)
    grok_tickers = set(df_grok["ticker"].unique())
    print(f"  ✅ grok_trending: {len(grok_tickers)} 銘柄")

    # Step 2: S3から grok_day_trade_list.parquet をダウンロード
    print()
    print(f"📥 Step 2: S3から {DAY_TRADE_LIST_FILE} をダウンロード...")
    df_master = download_from_s3(s3_client, s3_key)

    if df_master is None:
        # 既存データが取得できない場合は処理を中断（上書き防止）
        print("  ❌ ERROR: S3から既存データを取得できません")
        print("  ⚠️ 上書き防止のため処理を中断します")
        sys.exit(1)
    else:
        # 検証
        is_valid, errors = validate_dataframe(df_master)
        if not is_valid:
            print(f"  ❌ ERROR: S3のファイルが破損しています")
            for err in errors:
                print(f"    - {err}")
            print("  ⚠️ 処理を中断します。手動で修正してください。")
            sys.exit(1)
        existing_tickers = set(df_master["ticker"].unique())
        print(f"  ✅ 既存: {len(existing_tickers)} 銘柄（検証OK）")

    # Step 2.5: meta_jquants.parquet をダウンロード（銘柄名取得用）
    print()
    print(f"📥 Step 2.5: S3から {META_JQUANTS_FILE} をダウンロード...")
    meta_key = f"{S3_PREFIX}{META_JQUANTS_FILE}"
    df_meta = download_from_s3(s3_client, meta_key)
    if df_meta is None:
        print("  ❌ ERROR: meta_jquants.parquet が取得できません")
        sys.exit(1)
    print(f"  ✅ meta_jquants: {len(df_meta)} 銘柄")

    # Step 3: 新規銘柄のみ抽出（上書き厳禁）
    print()
    print("🔍 Step 3: 新規銘柄を抽出...")
    new_tickers = grok_tickers - existing_tickers

    if not new_tickers:
        print("  ℹ️ 新規銘柄なし - 処理終了")
        print()
        print("✅ 完了（変更なし）")
        return

    print(f"  📊 新規銘柄: {len(new_tickers)} 件")
    for ticker in sorted(new_tickers):
        print(f"    - {ticker}")

    # Step 4: 新規銘柄を追加（デフォルト値 + 銘柄名取得）
    print()
    print("➕ Step 4: 新規銘柄を追加...")

    # meta_jquantsから銘柄名を取得
    meta_dict = df_meta.set_index('ticker')['stock_name'].to_dict()

    new_rows = pd.DataFrame(
        {
            "ticker": list(new_tickers),
            "stock_name": [meta_dict.get(t, None) for t in new_tickers],
            "shortable": False,  # デフォルト: 空売り不可
            "day_trade": True,  # デフォルト: いちにち信用可
            "ng": False,  # デフォルト: 取引可
            "day_trade_available_shares": None,  # デフォルト: 未設定
        }
    )

    # 銘柄名が取得できなかった銘柄を警告
    missing_names = new_rows[new_rows['stock_name'].isna()]['ticker'].tolist()
    if missing_names:
        print(f"  ⚠️ 銘柄名が取得できない銘柄: {missing_names}")

    # 既存データを保持しつつ新規追加
    df_updated = pd.concat([df_master, new_rows], ignore_index=True)

    # カラム順序を統一
    df_updated = df_updated[EXPECTED_COLUMNS]

    # ticker でソート
    df_updated = df_updated.sort_values("ticker").reset_index(drop=True)

    print(f"  ✅ 更新後: {len(df_updated)} 銘柄 (+{len(new_tickers)})")

    # 最終検証
    is_valid, errors = validate_dataframe(df_updated)
    if not is_valid:
        print(f"  ❌ ERROR: 更新後データの検証に失敗")
        for err in errors:
            print(f"    - {err}")
        sys.exit(1)

    # Step 5: S3にアップロード
    print()
    print(f"📤 Step 5: S3に {DAY_TRADE_LIST_FILE} をアップロード...")
    if upload_to_s3(s3_client, df_updated, s3_key):
        print(f"  ✅ アップロード完了: s3://{S3_BUCKET}/{s3_key}")
    else:
        print("  ❌ アップロード失敗")
        sys.exit(1)

    print()
    print("=" * 60)
    print(f"✅ 完了: {len(new_tickers)} 銘柄を追加しました")
    print("=" * 60)


if __name__ == "__main__":
    main()
