#!/usr/bin/env python3
"""
create_all_stocks.py
meta.parquet + scalping_*.parquet + grok_trending.parquet をマージして all_stocks.parquet を生成
GitHub Actions対応: S3優先、全必要ファイルをロード
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # scripts/pipeline/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file
from common_cfg.s3cfg import load_s3_config

META_PATH = PARQUET_DIR / "meta.parquet"
SCALPING_ENTRY_PATH = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE_PATH = PARQUET_DIR / "scalping_active.parquet"
ALL_STOCKS_PATH = PARQUET_DIR / "all_stocks.parquet"


def download_from_s3_if_exists(filename: str, local_path: Path) -> bool:
    """S3からファイルをダウンロード（存在すれば）"""
    try:
        cfg = load_s3_config()
        print(f"[INFO] Trying to download {filename} from S3...")
        success = download_file(cfg, filename, local_path)
        if success:
            print(f"[OK] Downloaded from S3: {local_path}")
            return True
        else:
            print(f"[WARN] {filename} not found in S3")
            return False
    except Exception as e:
        print(f"[WARN] S3 download failed: {e}")
        return False


def load_required_files() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """必要なparquetファイルを読み込み（S3優先）"""

    # meta.parquet（S3優先、静的銘柄マスタ）
    print("[INFO] Loading meta.parquet...")
    s3_success = download_from_s3_if_exists("meta.parquet", META_PATH)

    if not META_PATH.exists():
        raise FileNotFoundError(
            f"meta.parquet not found in S3 or locally.\n"
            f"Expected path: {META_PATH}\n"
            f"Run scripts/manual/create_meta.py and update_manifest.py first."
        )

    meta = pd.read_parquet(META_PATH)
    print(f"  ✓ Loaded meta.parquet: {len(meta)} stocks")

    # scalping_entry.parquet（ローカルから読み込み、03が生成済み）
    print("[INFO] Loading scalping_entry.parquet...")
    if SCALPING_ENTRY_PATH.exists():
        scalping_entry = pd.read_parquet(SCALPING_ENTRY_PATH)
        print(f"  ✓ Loaded scalping_entry.parquet: {len(scalping_entry)} stocks")
    else:
        print("  [WARN] scalping_entry.parquet not found, using empty DataFrame")
        scalping_entry = pd.DataFrame()

    # scalping_active.parquet（ローカルから読み込み、03が生成済み）
    print("[INFO] Loading scalping_active.parquet...")
    if SCALPING_ACTIVE_PATH.exists():
        scalping_active = pd.read_parquet(SCALPING_ACTIVE_PATH)
        print(f"  ✓ Loaded scalping_active.parquet: {len(scalping_active)} stocks")
    else:
        print("  [WARN] scalping_active.parquet not found, using empty DataFrame")
        scalping_active = pd.DataFrame()

    # grok_trending.parquet（ローカル優先、なければS3から取得）
    print("[INFO] Loading grok_trending.parquet...")

    # ローカルパスをチェック
    local_grok_path = PARQUET_DIR / "grok_trending.parquet"

    if local_grok_path.exists():
        # ローカルファイルが存在する場合はそれを使用
        grok_trending = pd.read_parquet(local_grok_path)
        print(f"  ✓ Loaded grok_trending.parquet from local: {len(grok_trending)} stocks")
    else:
        # ローカルにない場合のみS3からダウンロード
        print("  [INFO] Local file not found, trying S3...")
        temp_dir = PARQUET_DIR / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_grok_path = temp_dir / "grok_trending_temp.parquet"

        s3_success = download_from_s3_if_exists("grok_trending.parquet", temp_grok_path)

        if s3_success and temp_grok_path.exists():
            grok_trending = pd.read_parquet(temp_grok_path)
            print(f"  ✓ Loaded grok_trending.parquet from S3: {len(grok_trending)} stocks")
            temp_grok_path.unlink(missing_ok=True)
        else:
            print("  [WARN] grok_trending.parquet not found in S3 or locally, using empty DataFrame")
            grok_trending = pd.DataFrame()

    return meta, scalping_entry, scalping_active, grok_trending


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
    jq_df["code_normalized"] = jq_df["Code"].astype(str).str.replace(r'^(.{4})0$', r'\1', regex=True)
    jq_df["ticker_normalized"] = jq_df["code_normalized"] + ".T"

    # market の揺れを統一
    jq_df["market_normalized"] = jq_df["MarketCodeName"].fillna("").astype(str).str.replace(r'（内国株式）$', '', regex=True)

    # NaN対策: Sector17CodeNameとScaleCategoryをNoneに変換
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
        df: meta.parquet のDataFrame（9カラム: categories, tags含む）

    Returns:
        18カラム構造のDataFrame
    """
    # categoriesとtagsが存在しない場合は追加（後方互換性）
    if 'categories' not in df.columns:
        df["categories"] = df.apply(lambda x: [], axis=1)
    if 'tags' not in df.columns:
        df["tags"] = df.apply(lambda x: [], axis=1)

    # テクニカル指標カラムを追加
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


def process_grok_trending(df: pd.DataFrame, client: JQuantsClient) -> pd.DataFrame:
    """
    grok_trending.parquetを18カラム構造に変換し、J-Quantsメタデータと照合

    Args:
        df: grok_trending.parquet のDataFrame
        client: J-QuantsClient

    Returns:
        18カラム構造のDataFrame（J-Quantsメタデータを補完）
    """
    if df.empty:
        print("  [INFO] Grok trending is empty")
        empty_df = pd.DataFrame(columns=[
            "ticker", "code", "stock_name", "market", "sectors", "series",
            "topixnewindexseries", "categories", "tags",
            "date", "Close", "change_pct", "Volume", "vol_ratio",
            "atr14_pct", "rsi14", "score", "key_signal"
        ])
        return empty_df

    print(f"  [INFO] Processing {len(df)} Grok trending stocks")

    # 必須カラムを確認
    required_cols = ["ticker", "code", "stock_name", "categories", "tags", "date"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column in grok_trending.parquet: {col}")

    # tagsを配列に統一（文字列の場合は1要素の配列に変換）
    df["tags"] = df["tags"].apply(lambda x: [x] if isinstance(x, str) else x if isinstance(x, list) else [])

    # tagsにreasonの内容を追加（reason列が存在する場合）
    if "reason" in df.columns:
        df["tags"] = df.apply(
            lambda row: list(row["tags"]) + [row["reason"]] if pd.notna(row.get("reason")) else list(row["tags"]),
            axis=1
        )

    # meta_jquants.parquet をロード（既に正規化済み）
    print("  [INFO] Loading meta_jquants.parquet for sector info...")
    from common_cfg.paths import PARQUET_DIR
    META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"

    if not META_JQUANTS_PATH.exists():
        raise FileNotFoundError(f"meta_jquants.parquet not found: {META_JQUANTS_PATH}")

    meta_jq = pd.read_parquet(META_JQUANTS_PATH)

    # meta_jquants.parquetは既に正規化済み（ticker, code, stock_name, market, sectors, series, topixnewindexseries）
    # 重複を削除してマッピング
    meta_jq_unique = meta_jq.drop_duplicates(subset=["ticker"], keep="first")
    jq_map = meta_jq_unique.set_index("ticker")[[
        "code", "stock_name", "market", "sectors", "series", "topixnewindexseries"
    ]].to_dict('index')

    # メタデータを補完
    def get_jq_value(ticker, key, default=None):
        return jq_map.get(ticker, {}).get(key, default)

    df["code"] = df["ticker"].apply(lambda t: get_jq_value(t, "code", t.replace(".T", "")))
    df["stock_name"] = df["ticker"].apply(lambda t: get_jq_value(t, "stock_name", df.loc[df["ticker"]==t, "stock_name"].iloc[0] if len(df[df["ticker"]==t]) > 0 else ""))
    df["market"] = df["ticker"].apply(lambda t: get_jq_value(t, "market", None))
    df["series"] = df["ticker"].apply(lambda t: get_jq_value(t, "series", None))
    df["topixnewindexseries"] = df["ticker"].apply(lambda t: get_jq_value(t, "topixnewindexseries", None))

    # sectorsは meta_jquants から取得（series と同じ値）
    df["sectors"] = df["ticker"].apply(lambda t: get_jq_value(t, "sectors", None))

    # 18カラム構造に整形
    cols = [
        "ticker", "code", "stock_name", "market", "sectors", "series",
        "topixnewindexseries", "categories", "tags",
        "date", "Close", "change_pct", "Volume", "vol_ratio",
        "atr14_pct", "rsi14", "score", "key_signal"
    ]

    # 欠落カラムを追加
    for col in cols:
        if col not in df.columns:
            df[col] = None

    print(f"  [OK] Metadata enriched for {len(df)} Grok stocks (with sector info from meta_jquants.parquet)")
    return df[cols].copy()


def merge_stocks(meta: pd.DataFrame, scalping_entry: pd.DataFrame, scalping_active: pd.DataFrame, grok_trending: pd.DataFrame) -> pd.DataFrame:
    """
    meta + scalping_entry + scalping_active + grok_trending をマージ

    重複するticker（静的銘柄がスキャルピング/Grok銘柄にも選定された場合）:
    - categoriesを統合
    - tagsは静的銘柄のtagsを維持（Grokのtagsは別途保持）
    - テクニカル指標はスキャルピング/Grokのデータを使用
    """
    print("[INFO] Merging stocks...")

    # 重複チェック
    meta_tickers = set(meta["ticker"])
    entry_tickers = set(scalping_entry["ticker"]) if not scalping_entry.empty else set()
    active_tickers = set(scalping_active["ticker"]) if not scalping_active.empty else set()
    grok_tickers = set(grok_trending["ticker"]) if not grok_trending.empty else set()

    overlap_entry = meta_tickers & entry_tickers
    overlap_active = meta_tickers & active_tickers
    overlap_grok = meta_tickers & grok_tickers

    if overlap_entry:
        print(f"  [INFO] {len(overlap_entry)} stocks overlap between meta and scalping_entry")
    if overlap_active:
        print(f"  [INFO] {len(overlap_active)} stocks overlap between meta and scalping_active")
    if overlap_grok:
        print(f"  [INFO] {len(overlap_grok)} stocks overlap between meta and grok_trending")

    # 重複処理: categoriesを統合、tagsとテクニカル指標はスキャルピング/Grokから取得
    for ticker in overlap_entry:
        meta_row = meta[meta["ticker"] == ticker].iloc[0]
        entry_row = scalping_entry[scalping_entry["ticker"] == ticker].iloc[0]
        combined_categories = list(set(meta_row["categories"] + entry_row["categories"]))
        scalping_entry.loc[scalping_entry["ticker"] == ticker, "categories"] = [combined_categories]
        scalping_entry.loc[scalping_entry["ticker"] == ticker, "tags"] = [meta_row["tags"]]

    for ticker in overlap_active:
        meta_row = meta[meta["ticker"] == ticker].iloc[0]
        active_row = scalping_active[scalping_active["ticker"] == ticker].iloc[0]
        combined_categories = list(set(meta_row["categories"] + active_row["categories"]))
        scalping_active.loc[scalping_active["ticker"] == ticker, "categories"] = [combined_categories]
        scalping_active.loc[scalping_active["ticker"] == ticker, "tags"] = [meta_row["tags"]]

    for ticker in overlap_grok:
        meta_row = meta[meta["ticker"] == ticker].iloc[0]
        grok_row = grok_trending[grok_trending["ticker"] == ticker].iloc[0]
        combined_categories = list(set(meta_row["categories"] + grok_row["categories"]))
        # Grokの場合、tagsはGrok自身のcategoryを保持（上書きしない）
        grok_trending.loc[grok_trending["ticker"] == ticker, "categories"] = [combined_categories]

    # 重複する静的銘柄を除外（スキャルピング/Grokに統合済み）
    meta_clean = meta[~meta["ticker"].isin(overlap_entry | overlap_active | overlap_grok)].copy()

    # マージ
    all_stocks = pd.concat([meta_clean, scalping_entry, scalping_active, grok_trending], ignore_index=True)

    # date カラムの型を統一（文字列に変換、Noneはそのまま）
    all_stocks["date"] = all_stocks["date"].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)

    print(f"[OK] Merged: {len(all_stocks)} stocks (Meta: {len(meta_clean)}, Entry: {len(scalping_entry)}, Active: {len(scalping_active)}, Grok: {len(grok_trending)})")
    return all_stocks


def main() -> int:
    print("=" * 60)
    print("Generate all_stocks.parquet (Meta + Scalping + Grok)")
    print("=" * 60)

    # [STEP 1] J-Quantsクライアント初期化
    print("\n[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] 必要なファイルを読み込み（S3優先）
    print("\n[STEP 2] Loading required files (S3 priority)...")
    try:
        meta, scalping_entry, scalping_active, grok_trending = load_required_files()
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 3] meta.parquetにテクニカル指標カラムを追加
    print("\n[STEP 3] Adding technical columns to meta...")
    meta_18col = add_technical_columns_to_meta(meta)
    print(f"  ✓ Expanded to 18 columns")

    # [STEP 4] scalping_entry.parquetに欠落カラムを追加
    print("\n[STEP 4] Processing scalping_entry...")
    scalping_entry_18col = add_missing_columns_to_scalping(scalping_entry, "SCALPING_ENTRY", client)

    # [STEP 5] scalping_active.parquetに欠落カラムを追加
    print("\n[STEP 5] Processing scalping_active...")
    scalping_active_18col = add_missing_columns_to_scalping(scalping_active, "SCALPING_ACTIVE", client)

    # [STEP 6] grok_trending.parquetを処理
    print("\n[STEP 6] Processing grok_trending...")
    grok_trending_18col = process_grok_trending(grok_trending, client)

    # [STEP 7] マージ
    print("\n[STEP 7] Merging all stocks...")
    all_stocks = merge_stocks(meta_18col, scalping_entry_18col, scalping_active_18col, grok_trending_18col)

    # [STEP 8] 保存
    print("\n[STEP 8] Saving all_stocks.parquet...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    all_stocks.to_parquet(ALL_STOCKS_PATH, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {ALL_STOCKS_PATH}")
    print("  ℹ S3アップロードは update_manifest.py で一括実行されます")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total stocks: {len(all_stocks)}")
    print(f"  - Meta (static): {len(meta_18col)}")
    print(f"  - Scalping Entry: {len(scalping_entry_18col)}")
    print(f"  - Scalping Active: {len(scalping_active_18col)}")
    print(f"  - Grok Trending: {len(grok_trending_18col)}")
    print("=" * 60)

    print("\n✅ all_stocks.parquet generated successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
