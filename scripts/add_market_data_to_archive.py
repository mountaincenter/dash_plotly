#!/usr/bin/env python3
"""
add_market_data_to_archive.py
既存のバックテストアーカイブにマーケットデータ（指数変動率）を追加

実行方法:
    python3 scripts/add_market_data_to_archive.py

出力:
    data/parquet/backtest/grok_trending_archive_with_market.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, time
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR

# パス定義
BACKTEST_DIR = PARQUET_DIR / "backtest"
ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"
OUTPUT_PATH = BACKTEST_DIR / "grok_trending_archive_with_market.parquet"

INDEX_5M_PATH = PARQUET_DIR / "index_prices_60d_5m.parquet"
INDEX_1D_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
TOPIX_1D_PATH = PARQUET_DIR / "topix_prices_max_1d.parquet"
CURRENCY_1H_PATH = PARQUET_DIR / "currency_prices_730d_1h.parquet"
CURRENCY_1D_PATH = PARQUET_DIR / "currency_prices_max_1d.parquet"


def calculate_morning_return(df_5m: pd.DataFrame, ticker: str, date: datetime) -> Optional[float]:
    """
    前場（9:00-11:30）の変動率を計算

    Args:
        df_5m: 5分足データ
        ticker: ティッカー
        date: 対象日

    Returns:
        変動率（小数、例: 0.015 = 1.5%）、または None
    """
    try:
        # 対象日のデータを抽出
        df_5m['date'] = pd.to_datetime(df_5m['date'])
        target_date = pd.Timestamp(date.date())

        df_day = df_5m[
            (df_5m['ticker'] == ticker) &
            (df_5m['date'].dt.date == target_date.date())
        ].copy()

        if df_day.empty:
            return None

        # 時刻でフィルタ（9:00-11:30）
        df_day['time'] = df_day['date'].dt.time
        morning_start = time(9, 0)
        morning_end = time(11, 30)

        df_morning = df_day[
            (df_day['time'] >= morning_start) &
            (df_day['time'] <= morning_end)
        ].sort_values('date')

        if len(df_morning) < 2:
            return None

        # 9:00の始値と11:30の終値
        open_price = df_morning.iloc[0]['Open']
        close_price = df_morning.iloc[-1]['Close']

        if pd.isna(open_price) or pd.isna(close_price) or open_price == 0:
            return None

        return (close_price - open_price) / open_price

    except Exception as e:
        print(f"[WARN] Failed to calculate morning return for {ticker} on {date.date()}: {e}")
        return None


def calculate_daily_return(df_1d: pd.DataFrame, ticker: str, date: datetime) -> Optional[float]:
    """
    全日の変動率を計算

    Args:
        df_1d: 日足データ
        ticker: ティッカー
        date: 対象日

    Returns:
        変動率（小数、例: 0.02 = 2%）、または None
    """
    try:
        # 対象日のデータを抽出
        df_1d['date'] = pd.to_datetime(df_1d['date'])
        target_date = pd.Timestamp(date.date())

        df_day = df_1d[
            (df_1d['ticker'] == ticker) &
            (df_1d['date'].dt.date == target_date.date())
        ]

        if df_day.empty:
            return None

        row = df_day.iloc[0]
        open_price = row['Open']
        close_price = row['Close']

        if pd.isna(open_price) or pd.isna(close_price) or open_price == 0:
            return None

        return (close_price - open_price) / open_price

    except Exception as e:
        print(f"[WARN] Failed to calculate daily return for {ticker} on {date.date()}: {e}")
        return None


def calculate_topix_daily_return(df_topix: pd.DataFrame, code: str, date: datetime) -> Optional[float]:
    """
    TOPIX（J-Quants）の全日変動率を計算

    Args:
        df_topix: TOPIX日足データ
        code: コード（0000, 0500, 0501, 0502）
        date: 対象日

    Returns:
        変動率、または None
    """
    try:
        df_topix['date'] = pd.to_datetime(df_topix['date'])
        target_date = pd.Timestamp(date.date())

        df_day = df_topix[
            (df_topix['code'] == code) &
            (df_topix['date'].dt.date == target_date.date())
        ]

        if df_day.empty:
            return None

        row = df_day.iloc[0]
        open_price = row['open']
        close_price = row['close']

        if pd.isna(open_price) or pd.isna(close_price) or open_price == 0:
            return None

        return (close_price - open_price) / open_price

    except Exception as e:
        print(f"[WARN] Failed to calculate TOPIX return for {code} on {date.date()}: {e}")
        return None


def add_market_data():
    """バックテストアーカイブにマーケットデータを追加"""

    print("=" * 80)
    print("バックテストアーカイブにマーケットデータを追加")
    print("=" * 80)
    print()

    # 1. アーカイブ読み込み
    if not ARCHIVE_PATH.exists():
        print(f"[ERROR] アーカイブが見つかりません: {ARCHIVE_PATH}")
        return

    df = pd.read_parquet(ARCHIVE_PATH)
    print(f"[OK] アーカイブ読み込み: {len(df)}銘柄")

    # 2. マーケットデータ読み込み
    print("[INFO] マーケットデータ読み込み中...")
    df_index_5m = pd.read_parquet(INDEX_5M_PATH)
    df_index_1d = pd.read_parquet(INDEX_1D_PATH)
    df_topix_1d = pd.read_parquet(TOPIX_1D_PATH)
    df_currency_1h = pd.read_parquet(CURRENCY_1H_PATH)
    df_currency_1d = pd.read_parquet(CURRENCY_1D_PATH)
    print("[OK] マーケットデータ読み込み完了")
    print()

    # 3. 各銘柄にマーケットデータを追加
    print("[INFO] マーケットデータ計算中...")

    market_columns = [
        'morning_nikkei_return',
        'morning_topix_etf_return',
        'morning_core30_etf_return',
        'morning_mothers_return',
        'morning_usdjpy_return',
        'daily_nikkei_return',
        'daily_topix_return',
        'daily_topix_etf_return',
        'daily_core30_return',
        'daily_mothers_return',
        'daily_usdjpy_return',
    ]

    for col in market_columns:
        df[col] = None

    for idx, row in df.iterrows():
        backtest_date = datetime.strptime(str(row['backtest_date']), '%Y-%m-%d')

        # Phase1用（前場）
        df.at[idx, 'morning_nikkei_return'] = calculate_morning_return(df_index_5m, '^N225', backtest_date)
        df.at[idx, 'morning_topix_etf_return'] = calculate_morning_return(df_index_5m, '1306.T', backtest_date)
        df.at[idx, 'morning_core30_etf_return'] = calculate_morning_return(df_index_5m, '1311.T', backtest_date)
        df.at[idx, 'morning_mothers_return'] = calculate_morning_return(df_index_5m, '2516.T', backtest_date)

        # USD/JPY（1時間足なので前場の平均的な変動を近似）
        df.at[idx, 'morning_usdjpy_return'] = calculate_daily_return(df_currency_1d, 'JPY=X', backtest_date)

        # Phase2用（全日）
        df.at[idx, 'daily_nikkei_return'] = calculate_daily_return(df_index_1d, '^N225', backtest_date)
        df.at[idx, 'daily_topix_return'] = calculate_topix_daily_return(df_topix_1d, '0000', backtest_date)
        df.at[idx, 'daily_topix_etf_return'] = calculate_daily_return(df_index_1d, '1306.T', backtest_date)
        df.at[idx, 'daily_core30_return'] = calculate_topix_daily_return(df_topix_1d, '0500', backtest_date)
        df.at[idx, 'daily_mothers_return'] = calculate_daily_return(df_index_1d, '2516.T', backtest_date)
        df.at[idx, 'daily_usdjpy_return'] = calculate_daily_return(df_currency_1d, 'JPY=X', backtest_date)

        if (idx + 1) % 10 == 0:
            print(f"  {idx + 1}/{len(df)} 完了")

    print()
    print("[OK] マーケットデータ計算完了")
    print()

    # 4. 統計情報表示
    print("=" * 80)
    print("【追加されたマーケットデータ】")
    print("=" * 80)
    print()

    for col in market_columns:
        non_null_count = df[col].notna().sum()
        pct = (non_null_count / len(df) * 100)
        if non_null_count > 0:
            mean_val = df[col].mean() * 100
            print(f"{col:<30} 取得: {non_null_count:>3}/{len(df)} ({pct:>5.1f}%)  平均: {mean_val:>+6.2f}%")
        else:
            print(f"{col:<30} 取得: {non_null_count:>3}/{len(df)} ({pct:>5.1f}%)")

    print()

    # 5. 保存
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"[OK] 保存完了: {OUTPUT_PATH}")
    print(f"     ファイルサイズ: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")
    print()


if __name__ == "__main__":
    add_market_data()
