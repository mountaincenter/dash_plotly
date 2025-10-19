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
    interval: str
) -> pd.DataFrame:
    """
    指定された銘柄の価格データを取得

    Args:
        tickers: ティッカーリスト
        period: データ期間（例: "60d", "max"）
        interval: データ間隔（例: "1d", "1h"）

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
            progress=False
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

        # 必要なカラムのみ保持
        required_cols = ['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']
        available_cols = [c for c in required_cols if c in df.columns]
        df = df[available_cols]

        return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch prices for {period}_{interval}: {e}")
        return pd.DataFrame()
