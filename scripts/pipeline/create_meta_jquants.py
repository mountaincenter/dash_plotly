#!/usr/bin/env python3
"""
create_meta_jquants.py
J-Quants APIから全銘柄の基本情報を取得してmeta_jquants.parquetを作成
週次強制更新対応、S3からダウンロード優先
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]  # scripts/pipeline/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file
from common_cfg.s3cfg import load_s3_config

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"


def should_force_update() -> bool:
    """
    週次強制更新が必要かチェック

    優先順位:
    1. 環境変数 FORCE_META_UPDATE=true（check_trading_day.pyから設定）
    2. フォールバック: 金曜日判定（ローカル開発用）
    """
    import os

    # GitHub Actionsからの環境変数チェック
    force_flag = os.getenv("FORCE_META_UPDATE", "").lower()
    if force_flag == "true":
        return True

    # フォールバック: 金曜日判定（ローカル開発用）
    return datetime.now().weekday() == 4  # 4 = Friday


def download_from_s3_if_exists() -> bool:
    """S3からmeta_jquants.parquetをダウンロード（存在すれば）"""
    try:
        print("[DEBUG] Loading S3 config...")
        cfg = load_s3_config()
        print(f"[DEBUG] S3 config loaded: bucket={cfg.bucket}, prefix={cfg.prefix}, region={cfg.region}")

        if not cfg.bucket:
            print("[INFO] S3 download skipped: bucket not set")
            return False

        print("[INFO] Trying to download meta_jquants.parquet from S3...")
        success = download_file(cfg, "meta_jquants.parquet", META_JQUANTS_PATH)
        if success:
            print(f"[OK] Downloaded from S3: {META_JQUANTS_PATH}")
            return True
        else:
            print("[WARN] meta_jquants.parquet not found in S3")
            return False
    except Exception as e:
        print(f"[WARN] S3 download failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_meta_jquants() -> int:
    """J-Quants APIから全銘柄情報を取得してmeta_jquants.parquet作成（障害時は空ファイル作成）"""
    print("=" * 60)
    print("Create meta_jquants.parquet from J-Quants API")
    print("=" * 60)

    # J-Quants APIから上場銘柄情報を取得
    print("\n[STEP 1] Fetching listed stocks from J-Quants API...")
    try:
        print("[DEBUG] Initializing J-Quants client...")
        client = JQuantsClient()
        print("[DEBUG] J-Quants client initialized, creating fetcher...")
        fetcher = JQuantsFetcher(client)
        print("[DEBUG] Fetcher created, fetching listed info...")
        df = fetcher.get_listed_info()
        print(f"  ✓ Retrieved {len(df)} stocks")
    except Exception as e:
        print(f"  ✗ Failed to fetch listed info: {e}")
        print("  ⚠ J-Quants障害時対応: 空のmeta_jquants.parquetを作成します")
        print("  → スキャルピング銘柄なしで、静的銘柄(meta.parquet)のみで処理を継続します")
        df = pd.DataFrame(columns=[
            "Code", "CompanyName", "MarketCodeName", "Sector33CodeName",
            "Sector17CodeName", "ScaleCategory"
        ])

    if df.empty:
        print("  ⚠ No data received from J-Quants API")
        print("  → 空のmeta_jquants.parquetを作成します")

    # 必要なカラムを抽出・変換
    print("\n[STEP 2] Processing data...")

    if df.empty:
        # 空のDataFrameを正しいスキーマで作成（J-Quants障害時対応）
        output_df = pd.DataFrame(columns=[
            "ticker",
            "code",
            "stock_name",
            "market",
            "sectors",
            "series",
            "topixnewindexseries"
        ])
        print("  ⚠ 空のデータフレームを作成しました（J-Quants障害時）")
    else:
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
    print("  ℹ S3アップロードは update_manifest.py で一括実行されます")

    # サマリー表示
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total stocks: {len(output_df)}")

    if len(output_df) > 0:
        print(f"\nMarket breakdown:")
        print(output_df["market"].value_counts())
        print(f"\nTop 10 sectors:")
        print(output_df["sectors"].value_counts().head(10))
    else:
        print("\n⚠ J-Quants障害により空のファイルを作成しました")
        print("→ スキャルピング銘柄は選定されません")
        print("→ 静的銘柄(meta.parquet)のみでyfinance更新を継続します")

    print("=" * 60)

    print("\n✅ meta_jquants.parquet created successfully!")
    return 0


def is_file_fresh(file_path: Path, max_age_days: int = 7) -> bool:
    """ファイルが指定日数以内に更新されているかチェック"""
    if not file_path.exists():
        return False

    import time
    file_age_seconds = time.time() - file_path.stat().st_mtime
    file_age_days = file_age_seconds / (24 * 3600)

    return file_age_days <= max_age_days


def main() -> int:
    """
    メイン処理（GitHub Actions対応: S3優先）

    処理順序:
    1. 週次強制更新チェック（環境変数 or 金曜日）→ 強制再作成
    2. S3からダウンロード試行（常に最初に実行、GitHub Actions対応）
    3. S3成功 → 鮮度チェック（7日以内なら使用、古ければ再作成）
    4. S3失敗 + ローカル存在 → 鮮度チェック（フォールバック）
    5. 両方ない → 新規作成
    """
    print("\n[DEBUG] ===== create_meta_jquants main() started =====")

    # 1. 週次強制更新チェック（環境変数 FORCE_META_UPDATE or 金曜日）
    print("[DEBUG] Checking force update...")
    force_update = should_force_update()
    print(f"[DEBUG] Force update: {force_update}")
    if force_update:
        print("[INFO] 週次強制更新: meta_jquants.parquetを再作成します")
        return create_meta_jquants()

    # 2. S3からダウンロード試行（ローカル存在に関わらず常に試行）
    print("[DEBUG] Starting S3 download attempt...")
    print("[INFO] S3からmeta_jquants.parquetのダウンロードを試行します...")
    download_success = download_from_s3_if_exists()
    print(f"[DEBUG] S3 download result: {download_success}")

    if download_success:
        # ダウンロード成功後、鮮度チェック
        print("[DEBUG] Checking file freshness...")
        is_fresh = is_file_fresh(META_JQUANTS_PATH, max_age_days=7)
        print(f"[DEBUG] File freshness: {is_fresh}")
        if is_fresh:
            print("[OK] S3からダウンロードしたファイルは最新です（7日以内）")
            return 0
        else:
            print("[WARN] S3のファイルが古い（7日以上経過）ため、再作成します")
            return create_meta_jquants()

    # 3. S3失敗時、ローカルファイルをフォールバック（ローカル開発用）
    print("[DEBUG] Checking local file existence...")
    local_exists = META_JQUANTS_PATH.exists()
    print(f"[DEBUG] Local file exists: {local_exists}")

    if local_exists:
        print("[WARN] S3からのダウンロードに失敗しましたが、ローカルファイルが存在します")
        is_fresh = is_file_fresh(META_JQUANTS_PATH, max_age_days=7)
        print(f"[DEBUG] Local file freshness: {is_fresh}")
        if is_fresh:
            print(f"[INFO] ローカルのmeta_jquants.parquetは最新です（7日以内）: {META_JQUANTS_PATH}")
            return 0
        else:
            print(f"[WARN] ローカルのmeta_jquants.parquetが古い（7日以上経過）")
            print("[INFO] 再作成します")
            return create_meta_jquants()

    # 4. 両方ない場合は新規作成
    print("[DEBUG] No existing file found, creating new...")
    print("[INFO] S3にもローカルにもmeta_jquants.parquetが存在しません")
    print("[INFO] 新規作成します")
    return create_meta_jquants()


if __name__ == "__main__":
    raise SystemExit(main())
