#!/usr/bin/env python3
"""
Create meta_jquants.parquet from J-Quants API
全銘柄の基本情報を取得してmeta_jquants.parquetを作成
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from jquants.client import JQuantsClient
from jquants.fetcher import JQuantsFetcher
from common_cfg.paths import PARQUET_DIR

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
DATA_J_CSV = ROOT / "data" / "csv" / "data_j.csv"


def main() -> int:
    print("=" * 60)
    print("Create meta_jquants.parquet from J-Quants API")
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
        return 1

    if df.empty:
        print("  ✗ No data received")
        return 1

    # 必要なカラムを抽出・変換
    print("\n[STEP 2] Processing data...")

    # Code列を4桁に変換（5桁目のチェックデジット削除）
    df["code"] = df["Code"].astype(str).str[:-1]

    # ticker列を作成（code + .T）
    df["ticker"] = df["code"] + ".T"

    # stock_name列を作成
    df["stock_name"] = df["CompanyName"]

    # market列を作成
    if "MarketCode" in df.columns:
        # MarketCodeをわかりやすい名前に変換
        market_map = {
            "0111": "プライム（内国株式）",
            "0112": "スタンダード（内国株式）",
            "0113": "グロース（内国株式）",
        }
        df["market"] = df["MarketCode"].map(market_map).fillna("その他")
    else:
        df["market"] = "不明"

    # sectors列を作成（Sector33CodeNameを使用）
    if "Sector33CodeName" in df.columns:
        df["sectors"] = df["Sector33CodeName"]
    elif "Sector17CodeName" in df.columns:
        df["sectors"] = df["Sector17CodeName"]
    else:
        df["sectors"] = "不明"

    # 最終的なカラムを選択
    output_df = df[[
        "code",
        "ticker",
        "stock_name",
        "market",
        "sectors"
    ]].copy()

    # 重複削除
    output_df = output_df.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    print(f"  ✓ Processed {len(output_df)} stocks (before merging with data_j.csv)")

    # data_j.csv から series と topixnewindexseries を取得してマージ
    print("\n[STEP 2.5] Merging series and topixnewindexseries from data_j.csv...")
    if DATA_J_CSV.exists():
        df_data_j = pd.read_csv(DATA_J_CSV, dtype=str)

        # ticker列を作成（コード[4桁] + ".T"）
        df_data_j["ticker"] = df_data_j["コード"].astype(str) + ".T"

        # 必要なカラムのみ抽出
        df_data_j = df_data_j[["ticker", "17業種区分", "規模区分"]].rename(columns={
            "17業種区分": "series",
            "規模区分": "topixnewindexseries"
        })

        # マージ
        output_df = output_df.merge(df_data_j, on="ticker", how="left")

        # "-" を null に統一（高市銘柄との整合性）
        output_df["topixnewindexseries"] = output_df["topixnewindexseries"].replace("-", None)

        # マージ結果確認
        missing_series = output_df["series"].isna().sum()
        missing_topix = output_df["topixnewindexseries"].isna().sum()

        print(f"  ✓ Merged with data_j.csv")
        if missing_series > 0:
            print(f"  ⚠ {missing_series} stocks missing 'series'")
        if missing_topix > 0:
            print(f"  ⚠ {missing_topix} stocks with null 'topixnewindexseries' (TOPIX非構成)")
    else:
        print(f"  ⚠ data_j.csv not found: {DATA_J_CSV}")
        print(f"  → series and topixnewindexseries will be empty")
        output_df["series"] = None
        output_df["topixnewindexseries"] = None

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

    print("\n✅ meta_jquants.parquet created successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
