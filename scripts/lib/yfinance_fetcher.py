#!/usr/bin/env python3
"""
yfinance価格データ取得モジュール
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import time

import pandas as pd
import yfinance as yf

BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2  # seconds


def _normalize_downloaded(df: pd.DataFrame, tickers: List[str], interval: str) -> pd.DataFrame:
    """yf.downloadの結果を縦持ち形式に正規化"""
    if df.empty:
        return pd.DataFrame()

    # Intraday data: convert UTC to JST and remove timezone info
    if interval in ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h']:
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Tokyo').tz_localize(None)

    # MultiIndex列を正規化（縦持ち形式に変換）
    if isinstance(df.columns, pd.MultiIndex):
        df_stacked = df.stack(level=0, future_stack=True).reset_index()
        if 'level_1' in df_stacked.columns:
            df_stacked.rename(columns={'level_1': 'ticker'}, inplace=True)
        elif 'Ticker' in df_stacked.columns:
            df_stacked.rename(columns={'Ticker': 'ticker'}, inplace=True)

        if 'level_0' in df_stacked.columns:
            df_stacked.rename(columns={'level_0': 'date'}, inplace=True)
        elif 'Date' in df_stacked.columns:
            df_stacked.rename(columns={'Date': 'date'}, inplace=True)
        elif 'Datetime' in df_stacked.columns:
            df_stacked.rename(columns={'Datetime': 'date'}, inplace=True)

        df = df_stacked
    else:
        df = df.reset_index()
        if 'Date' in df.columns:
            df.rename(columns={'Date': 'date'}, inplace=True)
        elif 'Datetime' in df.columns:
            df.rename(columns={'Datetime': 'date'}, inplace=True)

        if len(tickers) == 1:
            df['ticker'] = tickers[0]

    # 必要なカラムのみ保持
    required_cols = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']
    available_cols = [c for c in required_cols if c in df.columns]
    df = df[available_cols]

    if 'ticker' in df.columns:
        df = df[df['ticker'].notna()].copy()
        df = df.dropna(subset=['Close'])

    return df


def _download_batch(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
    """単一バッチのダウンロード"""
    df = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by='ticker',
        threads=True,
        progress=False,
        auto_adjust=True,
    )
    return _normalize_downloaded(df, tickers, interval)


def fetch_prices_for_tickers(
    tickers: List[str],
    period: str,
    interval: str,
    fallback_period: str = None
) -> pd.DataFrame:
    """
    指定された銘柄の価格データをバッチで取得

    大量銘柄（50超）は自動的にバッチ分割してyfinanceのレート制限を回避。

    Args:
        tickers: ティッカーリスト
        period: データ期間（例: "60d", "max"）
        interval: データ間隔（例: "1d", "1h"）
        fallback_period: 個別銘柄の取得失敗時のフォールバック期間

    Returns:
        価格データのDataFrame
    """
    try:
        if not tickers:
            return pd.DataFrame()

        # バッチ分割で取得
        frames = []
        total = len(tickers)
        for i in range(0, total, BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

            if total > BATCH_SIZE:
                print(f"    batch {batch_num}/{total_batches} ({len(batch)} tickers)")

            df = _download_batch(batch, period, interval)
            if not df.empty:
                frames.append(df)

            # バッチ間のsleep（最後のバッチ以外）
            if i + BATCH_SIZE < total:
                time.sleep(SLEEP_BETWEEN_BATCHES)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)

        # fallback: データが取得できなかった銘柄を個別にリトライ
        if fallback_period and 'ticker' in result.columns:
            valid_tickers = set(result[result['Close'].notna()]['ticker'].unique())
            missing_tickers = set(tickers) - valid_tickers

            if missing_tickers:
                print(f"  [INFO] {len(missing_tickers)} ticker(s) failed with period={period}, retrying with period={fallback_period}")
                fallback_dfs = []

                for ticker in missing_tickers:
                    try:
                        ticker_df = _download_batch([ticker], fallback_period, interval)
                        if not ticker_df.empty:
                            fallback_dfs.append(ticker_df)
                    except Exception as e:
                        print(f"      ✗ Fallback failed for {ticker}: {e}")

                if fallback_dfs:
                    fallback_df = pd.concat(fallback_dfs, ignore_index=True)
                    result = pd.concat([result, fallback_df], ignore_index=True)
                    print(f"  [OK] Added {len(fallback_dfs)} ticker(s) via fallback")

        return result
    except Exception as e:
        print(f"[ERROR] Failed to fetch prices for {period}_{interval}: {e}")
        return pd.DataFrame()
