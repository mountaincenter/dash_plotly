#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_meta.py
静的銘柄（Core30 + 高市銘柄）のmeta.parquetを生成
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


def fetch_core30_from_jquants(client: JQuantsClient) -> pd.DataFrame:
    """J-QuantsからCore30銘柄を取得"""
    print("[INFO] Fetching Core30 stocks from J-Quants...")

    # 上場銘柄一覧を取得
    response = client.request("/listed/info")

    if not response or "info" not in response:
        raise RuntimeError("Failed to fetch listed info from J-Quants")

    df = pd.DataFrame(response["info"])

    # Core30フィルタ: ScaleCategory == "TOPIX Core30"
    core30 = df[df["ScaleCategory"] == "TOPIX Core30"].copy()

    if core30.empty:
        raise RuntimeError("No Core30 stocks found in J-Quants data")

    # 必要なカラムを整形
    # 1. コードの5桁目の0を削除（例: 70110 -> 7011, 202A0 -> 202A）
    core30["code"] = core30["Code"].astype(str).str.replace(r'^(.{4})0$', r'\1', regex=True)
    core30["ticker"] = core30["code"] + ".T"
    core30["stock_name"] = core30["CompanyName"]

    # 2. market の揺れを統一（プライム（内国株式）→ プライム）
    core30["market"] = core30["MarketCodeName"].str.replace(r'（内国株式）$', '', regex=True)

    # 3. sectors=33業種、series=17業種
    core30["sectors"] = core30["Sector33CodeName"]
    core30["series"] = core30["Sector17CodeName"]
    core30["topixnewindexseries"] = core30["ScaleCategory"]

    # categories と tags を追加
    core30["categories"] = core30.apply(lambda x: ["TOPIX_CORE30"], axis=1)
    core30["tags"] = core30.apply(lambda x: [], axis=1)

    # 9カラム構造（静的銘柄のメタデータのみ）
    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags"
    ]
    core30 = core30[cols].copy()

    print(f"[OK] Fetched {len(core30)} Core30 stocks")
    return core30


def load_takaichi_stocks() -> pd.DataFrame:
    """高市銘柄をCSVから読み込み"""
    print("[INFO] Loading Takaichi stocks from CSV...")

    csv_path = ROOT / "data" / "csv" / "takaichi_stock_issue.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"Takaichi CSV not found: {csv_path}")

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

    # 4. categories: 高市銘柄
    df["categories"] = df.apply(lambda x: ["高市銘柄"], axis=1)

    # 5. tags: tag2~tag8 から7大分類政策カテゴリを配列で取得（tag1は「高市銘柄」なので除外）
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

    print(f"[OK] Loaded {len(df)} Takaichi stocks")
    return df


def merge_and_deduplicate(core30: pd.DataFrame, takaichi: pd.DataFrame) -> pd.DataFrame:
    """Core30と高市銘柄をマージして重複を処理"""
    print("[INFO] Merging Core30 and Takaichi stocks...")

    # ticker で重複チェック
    core30_tickers = set(core30["ticker"])
    takaichi_tickers = set(takaichi["ticker"])
    overlap = core30_tickers & takaichi_tickers

    if overlap:
        print(f"[INFO] {len(overlap)} stocks overlap between Core30 and Takaichi: {sorted(overlap)}")
        print("[INFO] Merging categories and tags for overlapping stocks")

        # 重複する銘柄: categoriesとtagsをマージ
        for ticker in overlap:
            core30_row = core30[core30["ticker"] == ticker].iloc[0]
            takaichi_row = takaichi[takaichi["ticker"] == ticker].iloc[0]

            # categories をマージ（重複なし）
            combined_categories = list(set(core30_row["categories"] + takaichi_row["categories"]))

            # tags をマージ（高市銘柄のtagsを追加）
            combined_tags = takaichi_row["tags"]  # 高市銘柄のtagsを使用

            # core30のrowを更新
            core30.loc[core30["ticker"] == ticker, "categories"] = [combined_categories]
            core30.loc[core30["ticker"] == ticker, "tags"] = [combined_tags]

        # 重複する高市銘柄を除外（既にcore30に統合済み）
        takaichi = takaichi[~takaichi["ticker"].isin(overlap)].copy()

    # マージ
    merged = pd.concat([core30, takaichi], ignore_index=True)

    # カラムの型を統一
    merged["ticker"] = merged["ticker"].astype(str)
    merged["code"] = merged["code"].astype(str)
    merged["stock_name"] = merged["stock_name"].astype(str)

    # NaN を適切な型に変換
    for col in ["market", "sectors", "series", "topixnewindexseries"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna("").astype(str)
            merged[col] = merged[col].replace("", None)

    print(f"[OK] Merged: {len(merged)} stocks (Core30: {len(core30)}, Takaichi: {len(takaichi)})")
    return merged


def main() -> int:
    load_dotenv_cascade()

    print("=" * 60)
    print("Generate meta.parquet (Core30 + Takaichi)")
    print("=" * 60)

    # J-Quantsクライアント初期化
    print("\n[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # Core30取得
    print("\n[STEP 2] Fetching Core30 stocks...")
    try:
        core30 = fetch_core30_from_jquants(client)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # 高市銘柄取得
    print("\n[STEP 3] Loading Takaichi stocks...")
    try:
        takaichi = load_takaichi_stocks()
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # マージ
    print("\n[STEP 4] Merging stocks...")
    meta = merge_and_deduplicate(core30, takaichi)

    # 保存
    print("\n[STEP 5] Saving meta.parquet...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    meta.to_parquet(MASTER_META_PARQUET, engine="pyarrow", index=False)
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
    print(f"Core30: {len(core30)}")
    print(f"Takaichi: {len(takaichi)}")
    print("=" * 60)

    print("\n✅ meta.parquet generated and uploaded successfully!")
    return 0


if __name__ == "__main__":
    """
    create_meta.py: 静的銘柄マスター生成（手動実行専用）

    データソース:
    - data/csv/takaichi_stock_issue.csv（Git管理、手動更新）
    - J-Quants API（Core30のみ）

    実行タイミング:
    - takaichi_stock_issue.csv更新時
    - Core30構成銘柄変更時

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
