#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scalping_final.py
J-Quants日足データのみでスキャルピング銘柄選定（最終版）
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
from common_cfg.paths import PARQUET_DIR
from jquants.client import JQuantsClient
from jquants.fetcher import JQuantsFetcher
from jquants.screener import ScalpingScreener

load_dotenv_cascade()

# ==== Paths ====
META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
SCALPING_ENTRY_PATH = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE_PATH = PARQUET_DIR / "scalping_active.parquet"
TICKERS_ENTRY_PATH = PARQUET_DIR / "tickers_entry.parquet"
TICKERS_ACTIVE_PATH = PARQUET_DIR / "tickers_active.parquet"


def fetch_jquants_prices_batch(
    client: JQuantsClient,
    codes: list[str],
    lookback_days: int = 60,
    batch_size: int = 500,
) -> pd.DataFrame:
    """J-Quants APIから株価データを一括取得"""
    print(f"[INFO] Fetching prices for {len(codes)} stocks from J-Quants...")

    fetcher = JQuantsFetcher(client)

    # ライトプランは最新データ取得可能
    to_date = date.today()
    from_date = to_date - timedelta(days=lookback_days)

    print(f"[INFO] Date range: {from_date} to {to_date}")

    # バッチ処理
    all_frames = []
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        print(f"[INFO] Batch {i//batch_size + 1}/{(len(codes)-1)//batch_size + 1} ({len(batch)} stocks)...")

        df_batch = fetcher.get_prices_daily_batch(
            codes=batch,
            from_date=from_date,
            to_date=to_date,
            batch_delay=0.3,
        )

        if not df_batch.empty:
            df_converted = fetcher.convert_to_yfinance_format(df_batch)
            all_frames.append(df_converted)

    if not all_frames:
        raise RuntimeError("No price data retrieved")

    df = pd.concat(all_frames, ignore_index=True)
    print(f"[INFO] Fetched {len(df)} rows for {df['ticker'].nunique()} stocks")

    return df


def main() -> int:
    print("=" * 60)
    print("J-Quants Scalping Screening (Daily Data Only)")
    print("=" * 60)

    # meta_jquants読み込み
    print("\n[STEP 1] Loading meta_jquants.parquet...")
    if not META_JQUANTS_PATH.exists():
        print(f"  ✗ Not found: {META_JQUANTS_PATH}")
        print("  → Run: python jquants/create_meta_jquants.py")
        return 1

    meta_df = pd.read_parquet(META_JQUANTS_PATH)

    # 株式のみに絞る（ETF、投資信託を除外）
    stock_only = meta_df[
        meta_df['market'].isin([
            'プライム（内国株式）',
            'スタンダード（内国株式）',
            'グロース（内国株式）'
        ])
    ].copy()

    print(f"  ✓ Total stocks: {len(meta_df)} → Stock only: {len(stock_only)}")

    codes = stock_only["code"].dropna().astype(str).unique().tolist()

    # 全銘柄で実行
    print(f"  ✓ Running with all {len(codes)} stocks (stock only)")

    # J-Quants初期化
    print("\n[STEP 2] Initializing J-Quants...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # 株価取得
    print("\n[STEP 3] Fetching prices...")
    df = fetch_jquants_prices_batch(client, codes, lookback_days=30, batch_size=500)

    # テクニカル指標計算
    print("\n[STEP 4] Calculating indicators...")
    screener = ScalpingScreener()
    df = screener.calculate_technical_indicators(df)

    latest_date = df["date"].max()
    df_latest = df[df["date"] == latest_date].copy()
    print(f"  ✓ Latest: {latest_date}, {len(df_latest)} stocks")

    # スクリーニング
    print("\n[STEP 5] Screening...")

    # エントリー向け（初心者）
    entry_list = screener.generate_entry_list(df_latest, stock_only, top_n=20)
    print(f"  ✓ Entry: {len(entry_list)} stocks")

    # アクティブ向け（上級者）
    entry_tickers = set(entry_list["ticker"].tolist()) if not entry_list.empty else set()
    active_list = screener.generate_active_list(df_latest, stock_only, entry_tickers, top_n=20)
    print(f"  ✓ Active: {len(active_list)} stocks")

    # 保存
    print("\n[STEP 6] Saving...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    entry_list.to_parquet(SCALPING_ENTRY_PATH, index=False)
    active_list.to_parquet(SCALPING_ACTIVE_PATH, index=False)
    print(f"  ✓ {SCALPING_ENTRY_PATH}")
    print(f"  ✓ {SCALPING_ACTIVE_PATH}")

    # ティッカーのみ
    if not entry_list.empty:
        entry_list[['ticker']].to_parquet(TICKERS_ENTRY_PATH, index=False)
        print(f"  ✓ {TICKERS_ENTRY_PATH}")
    if not active_list.empty:
        active_list[['ticker']].to_parquet(TICKERS_ACTIVE_PATH, index=False)
        print(f"  ✓ {TICKERS_ACTIVE_PATH}")

    # サマリー
    print("\n" + "=" * 60)
    print(f"Entry:  {len(entry_list)} stocks")
    print(f"Active: {len(active_list)} stocks")
    print(f"Date:   {latest_date}")
    print("=" * 60)

    if not entry_list.empty:
        print("\n🎯 Entry List:")
        cols = [c for c in ['ticker', 'stock_name', 'Close', 'change_pct', 'atr14_pct', 'score'] if c in entry_list.columns]
        print(entry_list[cols].to_string(index=False))

    if not active_list.empty:
        print("\n🚀 Active List:")
        cols = [c for c in ['ticker', 'stock_name', 'Close', 'change_pct', 'atr14_pct', 'score'] if c in active_list.columns]
        print(active_list[cols].to_string(index=False))

    print("\n✅ Screening completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
