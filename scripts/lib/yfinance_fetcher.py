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
        return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch prices for {period}_{interval}: {e}")
        return pd.DataFrame()
