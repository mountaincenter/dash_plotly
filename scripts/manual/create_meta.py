#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_meta.py
静的銘柄（Core30 + Large70 + 政策銘柄）のmeta.parquetを生成
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # scripts/manual/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR, MASTER_META_PARQUET
from common_cfg.s3io import upload_file
from common_cfg.s3cfg import load_s3_config
from scripts.lib.jquants_client import JQuantsClient


def fetch_core30_large70_from_local() -> pd.DataFrame:
    """meta_jquants.parquetからCore30 + Large70銘柄を取得（APIフォールバック）"""
    path = PARQUET_DIR / "meta_jquants.parquet"
    if not path.exists():
        raise FileNotFoundError(f"meta_jquants.parquet not found: {path}")

    mj = pd.read_parquet(path)
    target_scales = ["TOPIX Core30", "TOPIX Large70"]
    stocks = mj[mj["topixnewindexseries"].isin(target_scales)].copy()

    stocks["categories"] = stocks["topixnewindexseries"].apply(
        lambda x: ["TOPIX_CORE30"] if x == "TOPIX Core30" else ["TOPIX_LARGE70"]
    )
    stocks["tags"] = stocks.apply(lambda x: [], axis=1)

    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags"
    ]
    stocks = stocks[[c for c in cols if c in stocks.columns]].copy()

    n_core30 = len(stocks[stocks["topixnewindexseries"] == "TOPIX Core30"])
    n_large70 = len(stocks[stocks["topixnewindexseries"] == "TOPIX Large70"])
    print(f"[OK] Loaded {len(stocks)} stocks from meta_jquants (Core30: {n_core30}, Large70: {n_large70})")
    return stocks


def fetch_core30_large70_from_jquants(client: JQuantsClient) -> pd.DataFrame:
    """J-QuantsからCore30 + Large70銘柄を取得"""
    print("[INFO] Fetching Core30 + Large70 stocks from J-Quants...")

    # 上場銘柄一覧を取得
    response = client.request("/listed/info")

    if not response or "info" not in response:
        raise RuntimeError("Failed to fetch listed info from J-Quants")

    df = pd.DataFrame(response["info"])

    # Core30 + Large70 フィルタ
    target_scales = ["TOPIX Core30", "TOPIX Large70"]
    stocks = df[df["ScaleCategory"].isin(target_scales)].copy()

    if stocks.empty:
        raise RuntimeError("No Core30/Large70 stocks found in J-Quants data")

    # 必要なカラムを整形
    # 1. コードの5桁目の0を削除（例: 70110 -> 7011, 202A0 -> 202A）
    stocks["code"] = stocks["Code"].astype(str).str.replace(r'^(.{4})0$', r'\1', regex=True)
    stocks["ticker"] = stocks["code"] + ".T"
    stocks["stock_name"] = stocks["CompanyName"]

    # 2. market の揺れを統一（プライム（内国株式）→ プライム）
    stocks["market"] = stocks["MarketCodeName"].str.replace(r'（内国株式）$', '', regex=True)

    # 3. sectors=33業種、series=17業種
    stocks["sectors"] = stocks["Sector33CodeName"]
    stocks["series"] = stocks["Sector17CodeName"]
    stocks["topixnewindexseries"] = stocks["ScaleCategory"]

    # categories と tags を追加
    stocks["categories"] = stocks["ScaleCategory"].apply(
        lambda x: ["TOPIX_CORE30"] if x == "TOPIX Core30" else ["TOPIX_LARGE70"]
    )
    stocks["tags"] = stocks.apply(lambda x: [], axis=1)

    # 9カラム構造（静的銘柄のメタデータのみ）
    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags"
    ]
    stocks = stocks[cols].copy()

    n_core30 = len(stocks[stocks["topixnewindexseries"] == "TOPIX Core30"])
    n_large70 = len(stocks[stocks["topixnewindexseries"] == "TOPIX Large70"])
    print(f"[OK] Fetched {len(stocks)} stocks (Core30: {n_core30}, Large70: {n_large70})")
    return stocks


def load_policy_stocks() -> pd.DataFrame:
    """政策銘柄をCSVから読み込み"""
    print("[INFO] Loading Policy stocks from CSV...")

    csv_path = ROOT / "data" / "csv" / "policy_stock_issue.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"Policy CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # 日本語カラム名を英語にマッピング
    df = df.rename(columns={
        "銘柄名": "stock_name",
        "コード": "code",
        "市場・商品区分": "market",
        "ニューインデックス区分": "topixnewindexseries",
        "33業種区分": "sectors",  # 33業種 -> sectors
        "17業種区分": "series",   # 17業種 -> series
    })

    # 1. コードの5桁目の0を削除（例: 70110 -> 7011, 202A0 -> 202A）
    df["code"] = df["code"].astype(str).str.replace(r'^(.{4})0$', r'\1', regex=True)
    df["ticker"] = df["code"] + ".T"
    df["stock_name"] = df["stock_name"].astype(str)

    # 2. market の揺れを統一
    df["market"] = df["market"].str.replace(r'（内国株式）$', '', regex=True)

    # 4. categories: 政策銘柄
    df["categories"] = df.apply(lambda x: ["政策銘柄"], axis=1)

    # 5. tags: tag2~tag8 から7大分類政策カテゴリを配列で取得（tag1は「政策銘柄」なので除外）
    tag_cols = ["tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8"]
    df["tags"] = df.apply(
        lambda row: [row[col] for col in tag_cols if col in row.index and pd.notna(row[col]) and row[col] != ""],
        axis=1
    )

    # 9カラム構造（静的銘柄のメタデータのみ）
    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags"
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    print(f"[OK] Loaded {len(df)} Policy stocks")
    return df


def merge_and_deduplicate(core30: pd.DataFrame, policy: pd.DataFrame) -> pd.DataFrame:
    """Core30と政策銘柄をマージして重複を処理"""
    print("[INFO] Merging Core30 and Policy stocks...")

    # ticker で重複チェック
    core30_tickers = set(core30["ticker"])
    policy_tickers = set(policy["ticker"])
    overlap = core30_tickers & policy_tickers

    if overlap:
        print(f"[INFO] {len(overlap)} stocks overlap between Core30 and Policy: {sorted(overlap)}")
        print("[INFO] Merging categories and tags for overlapping stocks")

        # 重複する銘柄: categoriesとtagsをマージ
        for ticker in overlap:
            core30_row = core30[core30["ticker"] == ticker].iloc[0]
            policy_row = policy[policy["ticker"] == ticker].iloc[0]

            # categories をマージ（重複なし）
            combined_categories = list(set(core30_row["categories"] + policy_row["categories"]))

            # tags をマージ（政策銘柄のtagsを追加）
            combined_tags = policy_row["tags"]  # 政策銘柄のtagsを使用

            # core30のrowを更新
            core30.loc[core30["ticker"] == ticker, "categories"] = [combined_categories]
            core30.loc[core30["ticker"] == ticker, "tags"] = [combined_tags]

        # 重複する政策銘柄を除外（既にcore30に統合済み）
        policy = policy[~policy["ticker"].isin(overlap)].copy()

    # マージ
    merged = pd.concat([core30, policy], ignore_index=True)

    # カラムの型を統一
    merged["ticker"] = merged["ticker"].astype(str)
    merged["code"] = merged["code"].astype(str)
    merged["stock_name"] = merged["stock_name"].astype(str)

    # NaN を適切な型に変換
    for col in ["market", "sectors", "series", "topixnewindexseries"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna("").astype(str)
            merged[col] = merged[col].replace("", None)

    print(f"[OK] Merged: {len(merged)} stocks (Core30: {len(core30)}, Policy: {len(policy)})")
    return merged


def main() -> int:
    load_dotenv_cascade()

    print("=" * 60)
    print("Generate meta.parquet (Core30 + Large70 + Policy)")
    print("=" * 60)

    # J-Quantsクライアント初期化
    print("\n[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # Core30 + Large70 取得
    print("\n[STEP 2] Fetching Core30 + Large70 stocks...")
    try:
        core30 = fetch_core30_large70_from_jquants(client)
    except Exception as e:
        print(f"  ⚠ J-Quants API failed: {e}")
        print("  [INFO] Falling back to meta_jquants.parquet...")
        core30 = fetch_core30_large70_from_local()

    # 政策銘柄取得
    print("\n[STEP 3] Loading Policy stocks...")
    try:
        policy = load_policy_stocks()
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # マージ
    print("\n[STEP 4] Merging stocks...")
    meta = merge_and_deduplicate(core30, policy)

    # 保存
    print("\n[STEP 5] Saving meta.parquet...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # 既存のmeta.parquetを読み込んでカラム構造を保持
    if MASTER_META_PARQUET.exists():
        print("  [INFO] Loading existing meta.parquet to preserve column structure...")
        existing = pd.read_parquet(MASTER_META_PARQUET)
        # 中身を空にする（カラムのみ保持）
        empty_df = pd.DataFrame(columns=existing.columns)
        print(f"  [INFO] Cleared {len(existing)} existing records, preserving schema")
        # 新しいデータを追加
        final_df = pd.concat([empty_df, meta], ignore_index=True)
    else:
        print("  [INFO] No existing meta.parquet found, creating new file")
        final_df = meta

    final_df.to_parquet(MASTER_META_PARQUET, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {MASTER_META_PARQUET}")

    # S3アップロード
    print("\n[STEP 6] Uploading to S3...")
    try:
        cfg = load_s3_config()
        success = upload_file(cfg, MASTER_META_PARQUET, "meta.parquet")
        if success:
            print(f"  ✓ Uploaded to S3: meta.parquet")
        else:
            print(f"  ✗ Failed to upload to S3")
            return 1
    except Exception as e:
        print(f"  ✗ S3 upload failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # サマリー
    print("\n" + "=" * 60)
    print(f"Total stocks: {len(meta)}")
    print(f"Core30 + Large70: {len(core30)}")
    print(f"Policy (unique): {len(policy)}")
    print("=" * 60)

    print("\n✅ meta.parquet generated and uploaded successfully!")
    return 0


if __name__ == "__main__":
    """
    create_meta.py: 静的銘柄マスター生成（手動実行専用）

    データソース:
    - data/csv/policy_stock_issue.csv（Git管理、手動更新）
    - J-Quants API（Core30 + Large70）

    実行タイミング:
    - policy_stock_issue.csv更新時
    - Core30/Large70構成銘柄変更時

    GitHub Actions: 実行しない（静的データのため）

    使用方法:
    python scripts/manual/create_meta.py  # meta.parquet生成 + S3アップロード

    処理内容:
    1. Core30銘柄をJ-Quants APIから取得
    2. 高市銘柄をCSVから読み込み
    3. 重複銘柄をマージ（categories, tagsを統合）
    4. meta.parquetをローカルに保存
    5. S3に自動アップロード
    """
    raise SystemExit(main())
