#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scalping_lists_jquants.py
J-Quants APIを使用したスキャルピング銘柄選定

注意: このスクリプトで生成されるデータは内部利用のみ。
選定結果（ティッカーリストのみ）を外部公開し、
具体的な株価データはyfinanceで配信する。
"""

from __future__ import annotations

from pathlib import Path
import sys
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR, MASTER_META_PARQUET
from jquants.client import JQuantsClient
from jquants.fetcher import JQuantsFetcher
from jquants.screener import ScalpingScreener

load_dotenv_cascade()

# ==== Paths ====
JQUANTS_ENTRY_PATH = PARQUET_DIR / "jquants_scalping_entry.parquet"
JQUANTS_ACTIVE_PATH = PARQUET_DIR / "jquants_scalping_active.parquet"
JQUANTS_TICKERS_ENTRY = PARQUET_DIR / "jquants_tickers_entry.parquet"
JQUANTS_TICKERS_ACTIVE = PARQUET_DIR / "jquants_tickers_active.parquet"


def fetch_jquants_prices(
    client: JQuantsClient,
    codes: list[str],
    lookback_days: int = 60,
) -> pd.DataFrame:
    """
    J-Quants APIから複数銘柄の株価データを取得

    Args:
        client: JQuantsClient
        codes: 銘柄コードのリスト（4桁、.Tなし）
        lookback_days: 取得する日数

    Returns:
        株価データのDataFrame
    """
    print(f"[INFO] Fetching prices for {len(codes)} stocks from J-Quants API...")

    fetcher = JQuantsFetcher(client)

    # 無料プランは12週間遅延のため、過去のデータを取得
    to_date = date.today() - timedelta(days=84)  # 12週間前
    from_date = to_date - timedelta(days=lookback_days)

    print(f"[INFO] Date range: {from_date} to {to_date}")

    df = fetcher.get_prices_daily_batch(
        codes=codes,
        from_date=from_date,
        to_date=to_date,
        batch_delay=0.5,  # レート制限対策
    )

    if df.empty:
        raise RuntimeError("Failed to fetch price data from J-Quants API")

    # yfinance互換形式に変換
    df = fetcher.convert_to_yfinance_format(df)

    print(f"[INFO] Fetched {len(df)} rows for {df['ticker'].nunique()} stocks")
    return df


def load_meta_and_codes() -> tuple[pd.DataFrame, list[str]]:
    """メタデータと銘柄コードを読み込む"""
    print("[INFO] Loading meta data...")

    if not MASTER_META_PARQUET.exists():
        raise FileNotFoundError(f"Meta data not found: {MASTER_META_PARQUET}")

    meta_df = pd.read_parquet(MASTER_META_PARQUET, engine="pyarrow")
    print(f"[INFO] Loaded {len(meta_df)} stocks from meta data")

    # 銘柄コード（4桁）を抽出
    codes = meta_df["code"].dropna().astype(str).str.zfill(4).unique().tolist()
    print(f"[INFO] Extracted {len(codes)} unique codes")

    return meta_df, codes


def main() -> int:
    print("=" * 60)
    print("J-Quants Scalping List Generator")
    print("=" * 60)

    # J-Quants クライアント初期化
    print("\n[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Client initialized (Plan: {client.plan})")
    except Exception as e:
        print(f"  ✗ Failed to initialize client: {e}")
        print("  → Please check your .env.jquants file")
        return 1

    # メタデータと銘柄コード読み込み
    print("\n[STEP 2] Loading meta data...")
    try:
        meta_df, codes = load_meta_and_codes()
    except Exception as e:
        print(f"  ✗ Failed to load meta data: {e}")
        return 1

    # 株価データ取得
    print("\n[STEP 3] Fetching price data from J-Quants...")
    try:
        # テスト用に最初の100銘柄のみ取得（本番は全銘柄）
        test_codes = codes[:100]
        print(f"[INFO] Testing with first {len(test_codes)} stocks...")

        df = fetch_jquants_prices(client, test_codes, lookback_days=60)
    except Exception as e:
        print(f"  ✗ Failed to fetch price data: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # テクニカル指標計算
    print("\n[STEP 4] Calculating technical indicators...")
    screener = ScalpingScreener()
    df = screener.calculate_technical_indicators(df)
    print(f"  ✓ Technical indicators calculated")

    # 最新日のデータを抽出
    latest_date = df["date"].max()
    df_latest = df[df["date"] == latest_date].copy()
    print(f"  ✓ Latest date: {latest_date}, {len(df_latest)} stocks")

    # エントリーリスト生成
    print("\n[STEP 5] Generating entry list...")
    df_entry = screener.generate_entry_list(df_latest, meta_df, top_n=20)

    # アクティブリスト生成
    print("\n[STEP 6] Generating active list...")
    entry_tickers = set(df_entry["ticker"].tolist()) if not df_entry.empty else set()
    df_active = screener.generate_active_list(df_latest, meta_df, entry_tickers, top_n=20)

    # 結果を保存
    print("\n[STEP 7] Saving results...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # 完全なデータを保存（内部利用のみ）
    df_entry.to_parquet(JQUANTS_ENTRY_PATH, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {JQUANTS_ENTRY_PATH}")

    df_active.to_parquet(JQUANTS_ACTIVE_PATH, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {JQUANTS_ACTIVE_PATH}")

    # ティッカーのみを保存（外部公開可能）
    if not df_entry.empty:
        tickers_entry = df_entry[["ticker"]].copy()
        tickers_entry.to_parquet(JQUANTS_TICKERS_ENTRY, engine="pyarrow", index=False)
        print(f"  ✓ Saved tickers only: {JQUANTS_TICKERS_ENTRY}")

    if not df_active.empty:
        tickers_active = df_active[["ticker"]].copy()
        tickers_active.to_parquet(JQUANTS_TICKERS_ACTIVE, engine="pyarrow", index=False)
        print(f"  ✓ Saved tickers only: {JQUANTS_TICKERS_ACTIVE}")

    # サマリー表示
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Entry list:  {len(df_entry)} stocks")
    print(f"Active list: {len(df_active)} stocks")
    print(f"Data date:   {latest_date}")
    print()
    print("📌 Note: Full data is for internal use only")
    print("📌 Ticker-only files can be published externally")
    print("=" * 60)

    if not df_entry.empty:
        print("\n🎯 Entry List (Top 5):")
        print(df_entry[["ticker", "stock_name", "Close", "change_pct", "score"]].head())

    if not df_active.empty:
        print("\n🚀 Active List (Top 5):")
        print(df_active[["ticker", "stock_name", "Close", "change_pct", "score"]].head())

    print("\n✅ Scalping lists generated successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
