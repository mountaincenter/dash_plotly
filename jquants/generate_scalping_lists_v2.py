#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scalping_lists_v2.py
2段階スクリーニング: J-Quants日足 → yfinance分足で最終選定
"""

from __future__ import annotations

from pathlib import Path
import sys
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf

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
    """
    J-Quants APIから複数銘柄の株価データを一括取得（バッチ処理）

    Args:
        client: JQuantsClient
        codes: 銘柄コードのリスト（4桁）
        lookback_days: 取得する日数
        batch_size: 一度に処理する銘柄数

    Returns:
        株価データのDataFrame
    """
    print(f"[INFO] Fetching prices for {len(codes)} stocks from J-Quants...")

    fetcher = JQuantsFetcher(client)

    # 無料プランは12週間遅延
    to_date = date.today() - timedelta(days=84)
    from_date = to_date - timedelta(days=lookback_days)

    print(f"[INFO] Date range: {from_date} to {to_date}")

    # バッチ処理
    all_frames = []
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        print(f"[INFO] Processing batch {i//batch_size + 1}/{(len(codes)-1)//batch_size + 1} ({len(batch)} stocks)...")

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
        raise RuntimeError("No price data retrieved from J-Quants")

    df = pd.concat(all_frames, ignore_index=True)
    print(f"[INFO] Fetched {len(df)} rows for {df['ticker'].nunique()} stocks")

    return df


def stage1_screening_jquants(
    df_latest: pd.DataFrame,
    entry_target: int = 50,
    active_target: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1次スクリーニング: J-Quants日足データで粗選定

    Args:
        df_latest: 最新日のテクニカル指標付きデータ
        entry_target: エントリー向けの目標銘柄数
        active_target: アクティブ向けの目標銘柄数

    Returns:
        (entry候補, active候補)のDataFrame tuple
    """
    print("\n[STAGE 1] J-Quants screening (daily data)...")

    # エントリー向け候補
    entry_candidates = df_latest[
        (df_latest['Close'] >= 100) &
        (df_latest['Close'] <= 1500) &
        (df_latest['Volume'] * df_latest['Close'] >= 100_000_000) &
        (df_latest['atr14_pct'] >= 1.0) &
        (df_latest['atr14_pct'] <= 3.5) &
        (df_latest['change_pct'].abs() <= 3.0)
    ].copy()

    # スコアリング
    entry_candidates['stage1_score'] = 50.0
    entry_candidates['stage1_score'] += entry_candidates['Close'].apply(
        lambda p: 30 if 300 <= p <= 800 else 15
    )
    entry_candidates['stage1_score'] += entry_candidates['vol_ratio'].apply(
        lambda v: 25 if 90 <= v <= 130 else 10
    )

    # 上位N件を選択
    entry_candidates = entry_candidates.sort_values('stage1_score', ascending=False).head(entry_target).reset_index(drop=True)
    print(f"  ✓ Entry candidates: {len(entry_candidates)} stocks")

    # アクティブ向け候補
    active_candidates = df_latest[
        ~df_latest['ticker'].isin(entry_candidates['ticker']) &
        (df_latest['Close'] >= 100) &
        (df_latest['Close'] <= 3000) &
        ((df_latest['Volume'] * df_latest['Close'] >= 50_000_000) | (df_latest['vol_ratio'] >= 150)) &
        (df_latest['atr14_pct'] >= 2.5) &
        (df_latest['change_pct'].abs() >= 2.0)
    ].copy()

    # スコアリング
    active_candidates['stage1_score'] = 50.0
    active_candidates['stage1_score'] += active_candidates['change_pct'].apply(
        lambda c: min(35, abs(c) / 7.0 * 35)
    )
    active_candidates['stage1_score'] += active_candidates['vol_ratio'].apply(
        lambda v: 30 if v >= 200 else max(0, v / 150 * 30)
    )

    # 上位N件を選択
    active_candidates = active_candidates.sort_values('stage1_score', ascending=False).head(active_target).reset_index(drop=True)
    print(f"  ✓ Active candidates: {len(active_candidates)} stocks")

    return entry_candidates, active_candidates


def stage2_screening_yfinance_intraday(
    candidates: pd.DataFrame,
    final_count: int = 20,
) -> pd.DataFrame:
    """
    2次スクリーニング: yfinance分足データで最終選定

    Args:
        candidates: 1次スクリーニング通過銘柄
        final_count: 最終的な銘柄数

    Returns:
        最終選定されたDataFrame
    """
    print(f"\n[STAGE 2] yfinance intraday screening ({len(candidates)} candidates)...")

    if candidates.empty:
        return pd.DataFrame()

    tickers = candidates['ticker'].tolist()
    intraday_scores = []

    # 直近5営業日の1時間足を取得
    for ticker in tickers:
        try:
            df_intraday = yf.download(
                ticker,
                period="5d",
                interval="1h",
                progress=False,
                threads=False,
            )

            if df_intraday.empty:
                intraday_scores.append(0)
                continue

            # 日中ボラティリティを計算
            if 'High' in df_intraday.columns and 'Low' in df_intraday.columns and 'Close' in df_intraday.columns:
                df_intraday['intraday_range'] = (df_intraday['High'] - df_intraday['Low']) / df_intraday['Close'] * 100
                avg_intraday_volatility = df_intraday['intraday_range'].mean()

                # 出来高の安定性
                volume_std = df_intraday['Volume'].std() / df_intraday['Volume'].mean() if 'Volume' in df_intraday.columns else 1.0

                # スコア計算
                score = avg_intraday_volatility * (1 / (1 + volume_std))
                intraday_scores.append(score)
            else:
                intraday_scores.append(0)

        except Exception as e:
            print(f"[WARN] Failed to fetch intraday data for {ticker}: {e}")
            intraday_scores.append(0)

    candidates = candidates.copy().reset_index(drop=True)
    candidates['stage2_score'] = intraday_scores
    candidates['final_score'] = candidates['stage1_score'] * 0.6 + candidates['stage2_score'] * 40

    # 最終選定
    final = candidates.sort_values('final_score', ascending=False).head(final_count)
    print(f"  ✓ Final selection: {len(final)} stocks")

    return final


def main() -> int:
    print("=" * 60)
    print("2-Stage Scalping Screening (J-Quants + yfinance)")
    print("=" * 60)

    # meta_jquants.parquetを読み込み
    print("\n[STEP 1] Loading meta_jquants.parquet...")
    if not META_JQUANTS_PATH.exists():
        print(f"  ✗ File not found: {META_JQUANTS_PATH}")
        print("  → Run: python jquants/create_meta_jquants.py")
        return 1

    meta_df = pd.read_parquet(META_JQUANTS_PATH)
    codes = meta_df["code"].dropna().astype(str).unique().tolist()

    # 小規模テスト用: 最初の500銘柄のみ
    codes = codes[:500]
    print(f"  ✓ Loaded {len(meta_df)} stocks (testing with first {len(codes)} codes)")

    # J-Quants APIクライアント初期化
    print("\n[STEP 2] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Client initialized (Plan: {client.plan})")
    except Exception as e:
        print(f"  ✗ Failed to initialize: {e}")
        return 1

    # J-Quantsから株価データ取得
    print("\n[STEP 3] Fetching prices from J-Quants...")
    df = fetch_jquants_prices_batch(client, codes, lookback_days=60, batch_size=500)

    # テクニカル指標計算
    print("\n[STEP 4] Calculating technical indicators...")
    screener = ScalpingScreener()
    df = screener.calculate_technical_indicators(df)

    latest_date = df["date"].max()
    df_latest = df[df["date"] == latest_date].copy()
    print(f"  ✓ Latest date: {latest_date}, {len(df_latest)} stocks")

    # 1次スクリーニング（J-Quants日足）
    entry_candidates, active_candidates = stage1_screening_jquants(df_latest, entry_target=50, active_target=50)

    # 2次スクリーニング（yfinance分足）
    print("\n[STEP 5] Stage 2: yfinance intraday screening...")
    entry_final = stage2_screening_yfinance_intraday(entry_candidates, final_count=20)
    active_final = stage2_screening_yfinance_intraday(active_candidates, final_count=20)

    # メタデータマージ
    if not entry_final.empty:
        entry_final = entry_final.merge(meta_df[['ticker', 'stock_name', 'market', 'sectors']], on='ticker', how='left')
    if not active_final.empty:
        active_final = active_final.merge(meta_df[['ticker', 'stock_name', 'market', 'sectors']], on='ticker', how='left')

    # 保存
    print("\n[STEP 6] Saving results...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    entry_final.to_parquet(SCALPING_ENTRY_PATH, index=False)
    active_final.to_parquet(SCALPING_ACTIVE_PATH, index=False)
    print(f"  ✓ Saved: {SCALPING_ENTRY_PATH}")
    print(f"  ✓ Saved: {SCALPING_ACTIVE_PATH}")

    # ティッカーのみ保存
    if not entry_final.empty:
        entry_final[['ticker']].to_parquet(TICKERS_ENTRY_PATH, index=False)
        print(f"  ✓ Saved: {TICKERS_ENTRY_PATH}")
    if not active_final.empty:
        active_final[['ticker']].to_parquet(TICKERS_ACTIVE_PATH, index=False)
        print(f"  ✓ Saved: {TICKERS_ACTIVE_PATH}")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Entry list:  {len(entry_final)} stocks")
    print(f"Active list: {len(active_final)} stocks")
    print("=" * 60)

    if not entry_final.empty:
        print("\n🎯 Entry List (Top 10):")
        display_cols = [c for c in ['ticker', 'stock_name', 'Close', 'change_pct', 'final_score'] if c in entry_final.columns]
        print(entry_final[display_cols].head(10))

    if not active_final.empty:
        print("\n🚀 Active List (Top 10):")
        display_cols = [c for c in ['ticker', 'stock_name', 'Close', 'change_pct', 'final_score'] if c in active_final.columns]
        print(active_final[display_cols].head(10))

    print("\n✅ 2-stage screening completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
