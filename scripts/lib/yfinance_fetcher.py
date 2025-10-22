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

import pandas as pd
import yfinance as yf


def fetch_prices_for_tickers(
    tickers: List[str],
    period: str,
    interval: str,
    fallback_period: str = None
) -> pd.DataFrame:
    """
    指定された銘柄の価格データを取得

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

        # yfinance.download()を使用
        df = yf.download(
            tickers=tickers,
            period=period,
            interval=interval,
            group_by='ticker',
            threads=True,
            progress=False,
            auto_adjust=True
        )

        if df.empty:
            return pd.DataFrame()

        # MultiIndex列を正規化（縦持ち形式に変換）
        if isinstance(df.columns, pd.MultiIndex):
            # 複数銘柄の場合: (ticker, column)形式
            # stack(level=0)でtickerをindexに移動
            df_stacked = df.stack(level=0, future_stack=True).reset_index()
            # カラム名を修正
            if 'level_1' in df_stacked.columns:
                df_stacked.rename(columns={'level_1': 'ticker'}, inplace=True)
            elif 'Ticker' in df_stacked.columns:
                df_stacked.rename(columns={'Ticker': 'ticker'}, inplace=True)

            if 'Date' in df_stacked.columns:
                df_stacked.rename(columns={'Date': 'date'}, inplace=True)
            elif 'Datetime' in df_stacked.columns:
                df_stacked.rename(columns={'Datetime': 'date'}, inplace=True)

            df = df_stacked
        else:
            # 単一銘柄の場合
            df = df.reset_index()
            if 'Date' in df.columns:
                df.rename(columns={'Date': 'date'}, inplace=True)
            elif 'Datetime' in df.columns:
                df.rename(columns={'Datetime': 'date'}, inplace=True)

            if len(tickers) == 1:
                df['ticker'] = tickers[0]

        # fallback_periodが指定されている場合、データが取得できなかった銘柄を個別にリトライ
        if fallback_period and 'ticker' in df.columns:
            # 有効なデータがある銘柄を特定（Close列がすべてNaNでない銘柄）
            valid_tickers = set()
            for ticker in tickers:
                ticker_data = df[df['ticker'] == ticker]
                if not ticker_data.empty and ticker_data['Close'].notna().any():
                    valid_tickers.add(ticker)

            missing_tickers = set(tickers) - valid_tickers

            if missing_tickers:
                print(f"  [INFO] {len(missing_tickers)} ticker(s) failed with period={period}, retrying with period={fallback_period}")
                fallback_dfs = []

                for ticker in missing_tickers:
                    try:
                        print(f"    - Retrying {ticker} with period={fallback_period}...")
                        ticker_df = yf.download(
                            tickers=[ticker],
                            period=fallback_period,
                            interval=interval,
                            progress=False,
                            auto_adjust=True
                        )

                        if not ticker_df.empty:
                            ticker_df = ticker_df.reset_index()
                            if 'Date' in ticker_df.columns:
                                ticker_df.rename(columns={'Date': 'date'}, inplace=True)
                            elif 'Datetime' in ticker_df.columns:
                                ticker_df.rename(columns={'Datetime': 'date'}, inplace=True)
                            ticker_df['ticker'] = ticker
                            fallback_dfs.append(ticker_df)
                            print(f"      ✓ Success: {len(ticker_df)} rows")
                        else:
                            print(f"      ✗ Still no data available")
                    except Exception as e:
                        print(f"      ✗ Fallback failed: {e}")

                # フォールバックデータを結合
                if fallback_dfs:
                    fallback_df = pd.concat(fallback_dfs, ignore_index=True)
                    df = pd.concat([df, fallback_df], ignore_index=True)
                    print(f"  [OK] Added {len(fallback_dfs)} ticker(s) via fallback")

        # 必要なカラムのみ保持
        required_cols = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']
        available_cols = [c for c in required_cols if c in df.columns]
        df = df[available_cols]

        # ticker列がNaNの行を除外
        if 'ticker' in df.columns:
            df = df[df['ticker'].notna()].copy()

        return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch prices for {period}_{interval}: {e}")
        return pd.DataFrame()
