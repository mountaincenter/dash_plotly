#!/usr/bin/env python3
"""
archive_with_features.parquet に前日5分足イントラデイ特徴量を追加

予測時点（前夜Grokシグナル発行時）で確定済みの前営業日5分足データを使用。
大引け = 日足Close（5分足は~15:20まで、15:25-15:30は取れない）

追加特徴量 (6個, 全て前営業日の値):
- prev_intraday_range: 日中レンジ (High-Low)/Open %
- prev_intraday_volatility: 5分足リターン標準偏差 %
- prev_volume_am_ratio: 前場出来高 / 全体出来高
- prev_close_gap: (日足Close - 5分足最終Close) / 5分足最終Close %
- prev_am_return: 前場リターン (始値→11:30) %
- prev_pm_return: 後場リターン (12:30→日足Close) %
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from datetime import time
from common_cfg.paths import PARQUET_DIR

FEATURES_PATH = PARQUET_DIR / "ml" / "archive_with_features.parquet"
FIVEMIN_PATH = PARQUET_DIR / "backtest" / "grok_5m_archive_all.parquet"
PRICES_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"

INTRADAY_COLS = [
    'prev_intraday_range', 'prev_intraday_volatility', 'prev_volume_am_ratio',
    'prev_close_gap', 'prev_am_return', 'prev_pm_return',
]

# 旧カラム名（上書き用）
OLD_INTRADAY_COLS = [
    'intraday_range', 'intraday_volatility', 'volume_am_ratio',
    'close_gap', 'am_return', 'pm_return',
]

AM_END = time(11, 30)
PM_START = time(12, 30)


def main():
    print("=== Add PREV-DAY intraday features from 5-min bars ===")

    # 既存データ読み込み
    df = pd.read_parquet(FEATURES_PATH)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])
    print(f"Archive: {len(df)} rows, {len(df.columns)} cols")

    # 旧カラム削除
    for col in OLD_INTRADAY_COLS + INTRADAY_COLS:
        if col in df.columns:
            df = df.drop(columns=[col])

    # 5分足
    fm = pd.read_parquet(FIVEMIN_PATH)
    fm['datetime'] = pd.to_datetime(fm['datetime'])
    fm['_date'] = fm['datetime'].dt.date
    fm['_time'] = fm['datetime'].dt.time
    print(f"5min: {len(fm):,} rows")

    # 日足（Closeのみ使用 + 前営業日マッピング）
    prices = pd.read_parquet(PRICES_PATH)
    prices['date'] = pd.to_datetime(prices['date'])
    daily_close = prices.set_index(['ticker', prices['date'].dt.date])['Close'].to_dict()
    print(f"Daily prices: {len(prices):,} rows")

    # 銘柄ごとの前営業日マッピング構築
    prev_trading_date = {}
    for ticker, grp in prices.groupby('ticker'):
        dates_sorted = sorted(grp['date'].dt.date.unique())
        for i in range(1, len(dates_sorted)):
            prev_trading_date[(ticker, dates_sorted[i])] = dates_sorted[i - 1]
    print(f"Prev-date mappings: {len(prev_trading_date):,}")

    # 5分足を ticker×date でグループ化して事前集計
    print("\nComputing intraday features...")
    fm_grouped = fm.groupby(['ticker', '_date'])

    results = {}
    for (ticker, dt), bars in fm_grouped:
        if len(bars) < 10:
            continue

        bars = bars.sort_values('datetime')
        am = bars[bars['_time'] <= AM_END]
        pm = bars[bars['_time'] >= PM_START]

        intra_open = bars.iloc[0]['open']
        last_close = bars.iloc[-1]['close']
        d_close = daily_close.get((ticker, dt), np.nan)

        f = {}

        f['prev_intraday_range'] = (
            (bars['high'].max() - bars['low'].min()) / intra_open * 100
            if intra_open > 0 else np.nan
        )

        c = bars['close'].values
        rets = np.diff(c) / c[:-1]
        f['prev_intraday_volatility'] = np.std(rets) * 100 if len(rets) >= 5 else np.nan

        total_vol = bars['volume'].sum()
        f['prev_volume_am_ratio'] = am['volume'].sum() / total_vol if total_vol > 0 else np.nan

        f['prev_close_gap'] = (
            (d_close - last_close) / last_close * 100
            if last_close > 0 and not np.isnan(d_close) else np.nan
        )

        f['prev_am_return'] = (
            (am.iloc[-1]['close'] - intra_open) / intra_open * 100
            if len(am) >= 2 and intra_open > 0 else np.nan
        )

        f['prev_pm_return'] = (
            (d_close - pm.iloc[0]['open']) / pm.iloc[0]['open'] * 100
            if len(pm) >= 2 and pm.iloc[0]['open'] > 0 and not np.isnan(d_close) else np.nan
        )

        results[(ticker, dt)] = f

    print(f"  Computed: {len(results)} ticker×date pairs")

    # archiveにマッチング（前営業日の5分足を使用）
    for col in INTRADAY_COLS:
        df[col] = np.nan

    matched = 0
    for i, row in df.iterrows():
        ticker = row['ticker']
        bt_date = row['backtest_date'].date()
        prev_date = prev_trading_date.get((ticker, bt_date))
        if prev_date is None:
            continue
        key = (ticker, prev_date)
        if key in results:
            for col in INTRADAY_COLS:
                df.at[i, col] = results[key].get(col, np.nan)
            matched += 1

    print(f"  Matched: {matched}/{len(df)} ({matched/len(df)*100:.1f}%)")

    for col in INTRADAY_COLS:
        valid = int(df[col].notna().sum())
        print(f"  {col:30s} {valid}/{len(df)} ({valid/len(df)*100:.0f}%)")

    # 保存
    df.to_parquet(FEATURES_PATH, index=False)
    print(f"\nSaved: {FEATURES_PATH}")
    print(f"  {len(df)} rows, {len(df.columns)} cols")


if __name__ == "__main__":
    main()
