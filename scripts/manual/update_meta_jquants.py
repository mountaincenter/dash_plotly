#!/usr/bin/env python3
"""
manual/update_meta_jquants.py
緊急時用: meta_jquants.parquetを強制的に再作成してS3にアップロード

使用場面:
- J-Quants APIが復旧後、すぐに最新データに更新したい場合
- GitHub Actionsのスケジュール外で手動更新が必要な場合
- ローカル環境からS3を直接更新したい場合

実行方法:
  python scripts/manual/update_meta_jquants.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file
from common_cfg.s3cfg import load_s3_config

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"


def create_meta_jquants_forced() -> tuple[bool, pd.DataFrame]:
    """
    J-Quants APIから全銘柄情報を強制的に取得してmeta_jquants.parquet作成

    Returns:
        (成功/失敗, DataFrame)
    """
    print("=" * 60)
    print("Manual Update: meta_jquants.parquet")
    print("=" * 60)

    # J-Quants APIから上場銘柄情報を取得
    print("\n[STEP 1] Fetching listed stocks from J-Quants API...")
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)
        df = fetcher.get_listed_info()
        print(f"  ✓ Retrieved {len(df)} stocks")
    except Exception as e:
        print(f"  ✗ Failed to fetch listed info: {e}")
        return False, pd.DataFrame()

    if df.empty:
        print("  ✗ No data received from J-Quants API")
        return False, pd.DataFrame()

    # 必要なカラムを抽出・変換
    print("\n[STEP 2] Processing data...")

    # Code列を4桁に変換（5桁目のチェックデジット削除）
    df["code"] = df["Code"].astype(str).str[:-1]

    # ticker列を作成（code + .T）
    df["ticker"] = df["code"] + ".T"

    # stock_name列を作成
    df["stock_name"] = df["CompanyName"]

    # market列を作成（内国株式の括弧を削除）
    if "MarketCodeName" in df.columns:
        df["market"] = df["MarketCodeName"].fillna("").astype(str).str.replace(r'（内国株式）$', '', regex=True)
    else:
        df["market"] = "不明"

    # sectors列を作成（Sector33CodeNameを使用）
    if "Sector33CodeName" in df.columns:
        df["sectors"] = df["Sector33CodeName"]
    elif "Sector17CodeName" in df.columns:
        df["sectors"] = df["Sector17CodeName"]
    else:
        df["sectors"] = "不明"

    # series列を作成（Sector17CodeName）
    if "Sector17CodeName" in df.columns:
        df["series"] = df["Sector17CodeName"].replace({pd.NA: None, "": None, "-": None})
    else:
        df["series"] = None

    # topixnewindexseries列を作成（ScaleCategory）
    if "ScaleCategory" in df.columns:
        df["topixnewindexseries"] = df["ScaleCategory"].replace({pd.NA: None, "": None, "-": None})
    else:
        df["topixnewindexseries"] = None

    # 最終的なカラムを選択
    output_df = df[[
        "ticker",
        "code",
        "stock_name",
        "market",
        "sectors",
        "series",
        "topixnewindexseries"
    ]].copy()

    # 重複削除
    output_df = output_df.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    print(f"  ✓ Processed {len(output_df)} stocks (before market filtering)")

    # 市場フィルタリング: プライム/スタンダード/グロースのみ
    target_markets = ["プライム", "スタンダード", "グロース"]
    before_filter = len(output_df)
    output_df = output_df[output_df["market"].str.contains("|".join(target_markets), na=False, regex=True)].copy()
    after_filter = len(output_df)
    removed = before_filter - after_filter

    print(f"  ✓ Market filtering: {after_filter} stocks (removed {removed} from その他)")
    print(f"  ✓ Final columns: {', '.join(output_df.columns)}")

    # 保存
    print("\n[STEP 3] Saving meta_jquants.parquet...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(META_JQUANTS_PATH, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {META_JQUANTS_PATH}")

    # サマリー表示
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total stocks: {len(output_df)}")
    print(f"\nMarket breakdown:")
    print(output_df["market"].value_counts())
    print(f"\nTop 10 sectors:")
    print(output_df["sectors"].value_counts().head(10))
    print("=" * 60)

    return True, output_df


def upload_to_s3(file_path: Path) -> bool:
    """
    meta_jquants.parquetをS3にアップロード

    Args:
        file_path: アップロードするファイルのパス

    Returns:
        成功/失敗
    """
    print("\n[STEP 4] Uploading to S3...")
    try:
        cfg = load_s3_config()
        success = upload_file(cfg, file_path, file_path.name)

        if success:
            print(f"  ✓ Uploaded to S3: s3://{cfg.bucket}/{cfg.prefix}{file_path.name}")
            return True
        else:
            print(f"  ✗ Failed to upload to S3")
            return False

    except Exception as e:
        print(f"  ✗ S3 upload error: {e}")
        return False


def main() -> int:
    """メイン処理"""
    print("\n⚠️  Manual Update Mode")
    print("This script will force update meta_jquants.parquet and upload to S3.\n")

    # 確認プロンプト
    try:
        confirm = input("Continue? [y/N]: ").strip().lower()
        if confirm != 'y':
            print("Cancelled.")
            return 0
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        return 0

    # meta_jquants.parquet作成
    success, df = create_meta_jquants_forced()

    if not success or df.empty:
        print("\n❌ Failed to create meta_jquants.parquet")
        return 1

    print("\n✅ meta_jquants.parquet created successfully!")

    # S3アップロード
    if upload_to_s3(META_JQUANTS_PATH):
        print("\n✅ Successfully uploaded to S3!")
        print("\n📝 Next steps:")
        print("  1. GitHub Actionsの次回実行時に、このS3ファイルが使用されます")
        print("  2. ローカル開発環境では scripts/sync/download_from_s3.py で同期できます")
        return 0
    else:
        print("\n⚠️  Local file created but S3 upload failed")
        print("Please check S3 credentials and try again.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
