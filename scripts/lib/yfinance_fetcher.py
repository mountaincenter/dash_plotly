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
from analyze import fetch_prices as fp


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
        df = fp._fetch_prices(tickers, period, interval)
        return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch prices for {period}_{interval}: {e}")
        return pd.DataFrame()
