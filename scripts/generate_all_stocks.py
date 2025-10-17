#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_all_stocks.py
meta.parquet + scalping_*.parquet をマージして all_stocks.parquet を生成
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from jquants.client import JQuantsClient


def add_missing_columns_to_scalping(df: pd.DataFrame, category_name: str, client: JQuantsClient) -> pd.DataFrame:
    """
    スキャルピングDataFrameに欠落しているメタデータカラムを追加

    Args:
        df: scalping_*.parquet のDataFrame（14カラム）
        category_name: "SCALPING_ENTRY" or "SCALPING_ACTIVE"
        client: J-QuantsClient

    Returns:
        18カラム構造のDataFrame
    """
    if df.empty:
        print(f"  [INFO] {category_name} is empty")
        # 空の場合でも18カラム構造を維持
        empty_df = pd.DataFrame(columns=[
            "ticker", "code", "stock_name", "market", "sectors", "series",
            "topixnewindexseries", "categories", "tags",
            "date", "Close", "change_pct", "Volume", "vol_ratio",
            "atr14_pct", "rsi14", "score", "key_signal"
        ])
        return empty_df

    print(f"  [INFO] Adding metadata to {len(df)} {category_name} stocks")

    # J-Quantsから全銘柄情報を取得
    response = client.request("/listed/info")
    if not response or "info" not in response:
        raise RuntimeError("Failed to fetch listed info from J-Quants")

    jq_df = pd.DataFrame(response["info"])

    # Code列を正規化（5桁目の0を削除）
    # 例: "70110" -> "7011", "202A0" -> "202A"
    jq_df["code_normalized"] = jq_df["Code"].astype(str).str.replace(r'^(.{4})0$', r'\1', regex=True)
    jq_df["ticker_normalized"] = jq_df["code_normalized"] + ".T"

    # market の揺れを統一（NaN対策も含む）
    jq_df["market_normalized"] = jq_df["MarketCodeName"].fillna("").astype(str).str.replace(r'（内国株式）$', '', regex=True)

    # NaN対策: Sector17CodeNameとScaleCategoryをNoneに変換
    # "-" も None に統一（該当なしを意味する）
    jq_df["Sector17CodeName"] = jq_df["Sector17CodeName"].replace({pd.NA: None, "": None, "-": None})
    jq_df["ScaleCategory"] = jq_df["ScaleCategory"].replace({pd.NA: None, "": None, "-": None})

    # 重複を削除してマッピング
    jq_df_unique = jq_df.drop_duplicates(subset=["ticker_normalized"], keep="first")
    jq_map = jq_df_unique.set_index("ticker_normalized")[[
        "code_normalized", "Sector17CodeName", "ScaleCategory", "market_normalized"
    ]].to_dict('index')

    # 欠落カラムを追加
    def get_jq_value(ticker, key, default=None):
        return jq_map.get(ticker, {}).get(key, default)

    df["code"] = df["ticker"].apply(lambda t: get_jq_value(t, "code_normalized", t.replace(".T", "")))
    df["series"] = df["ticker"].apply(lambda t: get_jq_value(t, "Sector17CodeName", None))
    df["topixnewindexseries"] = df["ticker"].apply(lambda t: get_jq_value(t, "ScaleCategory", None))
    df["categories"] = df.apply(lambda x: [category_name], axis=1)

    # marketを統一された形式に上書き
    df["market"] = df["ticker"].apply(lambda t: get_jq_value(t, "market_normalized", df.loc[df["ticker"]==t, "market"].iloc[0] if len(df[df["ticker"]==t]) > 0 else ""))

    # 18カラム構造に並び替え
    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags",
        "date", "Close", "change_pct", "Volume", "vol_ratio",
        "atr14_pct", "rsi14", "score", "key_signal"
    ]

    return df[cols].copy()


def add_technical_columns_to_meta(df: pd.DataFrame) -> pd.DataFrame:
    """
    meta.parquetにテクニカル指標カラムをnullで追加

    Args:
        df: meta.parquet のDataFrame（9カラム）

    Returns:
        18カラム構造のDataFrame
    """
    df["date"] = None
    df["Close"] = None
    df["change_pct"] = None
    df["Volume"] = None
    df["vol_ratio"] = None
    df["atr14_pct"] = None
    df["rsi14"] = None
    df["score"] = None
    df["key_signal"] = None

    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags",
        "date", "Close", "change_pct", "Volume", "vol_ratio",
        "atr14_pct", "rsi14", "score", "key_signal"
    ]

    return df[cols].copy()


def merge_stocks(meta: pd.DataFrame, scalping_entry: pd.DataFrame, scalping_active: pd.DataFrame) -> pd.DataFrame:
    """
    meta + scalping_entry + scalping_active をマージ

    重複するticker（静的銘柄がスキャルピング銘柄にも選定された場合）:
    - categoriesを統合
    - tagsは静的銘柄のtagsを維持
    - テクニカル指標はスキャルピングのデータを使用

    Args:
        meta: 静的銘柄（9カラム）
        scalping_entry: Entry銘柄（14カラム）
        scalping_active: Active銘柄（14カラム）

    Returns:
        マージ済みのDataFrame（18カラム）
    """
    print("[INFO] Merging stocks...")

    # 重複チェック
    meta_tickers = set(meta["ticker"])
    entry_tickers = set(scalping_entry["ticker"]) if not scalping_entry.empty else set()
    active_tickers = set(scalping_active["ticker"]) if not scalping_active.empty else set()

    overlap_entry = meta_tickers & entry_tickers
    overlap_active = meta_tickers & active_tickers

    if overlap_entry:
        print(f"  [INFO] {len(overlap_entry)} stocks overlap between meta and scalping_entry")
    if overlap_active:
        print(f"  [INFO] {len(overlap_active)} stocks overlap between meta and scalping_active")

    # 重複処理: categoriesを統合、tagsとテクニカル指標はスキャルピングから取得
    for ticker in overlap_entry:
        meta_row = meta[meta["ticker"] == ticker].iloc[0]
        entry_row = scalping_entry[scalping_entry["ticker"] == ticker].iloc[0]

        # categoriesを統合
        combined_categories = list(set(meta_row["categories"] + entry_row["categories"]))

        # scalping_entryのrowを更新（categoriesとtagsはmeta優先）
        scalping_entry.loc[scalping_entry["ticker"] == ticker, "categories"] = [combined_categories]
        scalping_entry.loc[scalping_entry["ticker"] == ticker, "tags"] = [meta_row["tags"]]

    for ticker in overlap_active:
        meta_row = meta[meta["ticker"] == ticker].iloc[0]
        active_row = scalping_active[scalping_active["ticker"] == ticker].iloc[0]

        combined_categories = list(set(meta_row["categories"] + active_row["categories"]))

        scalping_active.loc[scalping_active["ticker"] == ticker, "categories"] = [combined_categories]
        scalping_active.loc[scalping_active["ticker"] == ticker, "tags"] = [meta_row["tags"]]

    # 重複する静的銘柄を除外（スキャルピングに統合済み）
    meta_clean = meta[~meta["ticker"].isin(overlap_entry | overlap_active)].copy()

    # マージ
    all_stocks = pd.concat([meta_clean, scalping_entry, scalping_active], ignore_index=True)

    print(f"[OK] Merged: {len(all_stocks)} stocks (Meta: {len(meta_clean)}, Entry: {len(scalping_entry)}, Active: {len(scalping_active)})")
    return all_stocks


def main() -> int:
    load_dotenv_cascade()

    print("=" * 60)
    print("Generate all_stocks.parquet (Meta + Scalping)")
    print("=" * 60)

    # J-Quantsクライアント初期化
    print("\n[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # meta.parquet読み込み
    print("\n[STEP 2] Loading meta.parquet...")
    meta_path = PARQUET_DIR / "meta.parquet"
    if not meta_path.exists():
        print(f"  ✗ Not found: {meta_path}")
        return 1

    meta = pd.read_parquet(meta_path)
    print(f"  ✓ Loaded: {len(meta)} stocks, {len(meta.columns)} columns")

    # meta.parquetにテクニカル指標カラムを追加（nullで）
    print("\n[STEP 3] Adding technical columns to meta...")
    meta_18col = add_technical_columns_to_meta(meta)
    print(f"  ✓ Expanded to 18 columns")

    # scalping_entry.parquet読み込み
    print("\n[STEP 4] Loading scalping_entry.parquet...")
    entry_path = PARQUET_DIR / "scalping_entry.parquet"
    if entry_path.exists():
        scalping_entry = pd.read_parquet(entry_path)
        print(f"  ✓ Loaded: {len(scalping_entry)} stocks")
        scalping_entry_18col = add_missing_columns_to_scalping(scalping_entry, "SCALPING_ENTRY", client)
    else:
        print(f"  [WARN] Not found: {entry_path}, using empty DataFrame")
        scalping_entry_18col = pd.DataFrame(columns=[
            "ticker", "code", "stock_name", "market", "sectors", "series",
            "topixnewindexseries", "categories", "tags",
            "date", "Close", "change_pct", "Volume", "vol_ratio",
            "atr14_pct", "rsi14", "score", "key_signal"
        ])

    # scalping_active.parquet読み込み
    print("\n[STEP 5] Loading scalping_active.parquet...")
    active_path = PARQUET_DIR / "scalping_active.parquet"
    if active_path.exists():
        scalping_active = pd.read_parquet(active_path)
        print(f"  ✓ Loaded: {len(scalping_active)} stocks")
        scalping_active_18col = add_missing_columns_to_scalping(scalping_active, "SCALPING_ACTIVE", client)
    else:
        print(f"  [WARN] Not found: {active_path}, using empty DataFrame")
        scalping_active_18col = pd.DataFrame(columns=[
            "ticker", "code", "stock_name", "market", "sectors", "series",
            "topixnewindexseries", "categories", "tags",
            "date", "Close", "change_pct", "Volume", "vol_ratio",
            "atr14_pct", "rsi14", "score", "key_signal"
        ])

    # マージ
    print("\n[STEP 6] Merging all stocks...")
    all_stocks = merge_stocks(meta_18col, scalping_entry_18col, scalping_active_18col)

    # 保存
    print("\n[STEP 7] Saving all_stocks.parquet...")
    output_path = PARQUET_DIR / "all_stocks.parquet"
    all_stocks.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {output_path}")

    # サマリー
    print("\n" + "=" * 60)
    print(f"Total stocks: {len(all_stocks)}")
    print(f"  - Meta (static): {len(meta)}")
    print(f"  - Scalping Entry: {len(scalping_entry_18col)}")
    print(f"  - Scalping Active: {len(scalping_active_18col)}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
