#!/usr/bin/env python3
"""
イグジットタイミング別バックテスト
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time, timedelta

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


def load_data():
    df_5m = pd.read_parquet(DATA_DIR / "surge_candidates_5m.parquet")
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime']).dt.tz_localize(None)
    df_5m['date'] = df_5m['Datetime'].dt.date
    df_5m['time'] = df_5m['Datetime'].dt.time
    df_watchlist = pd.read_parquet(DATA_DIR / "morning_peak_watchlist.parquet")
    return df_5m, df_watchlist


def calc_daily_volume(df_5m):
    daily = df_5m.groupby(['ticker', 'date']).agg({'Volume': 'sum'}).reset_index()
    daily = daily.sort_values(['ticker', 'date'])
    daily['vol_ma20'] = daily.groupby('ticker')['Volume'].transform(
        lambda x: x.rolling(20, min_periods=10).mean().shift(1)
    )
    daily['vol_ratio'] = daily['Volume'] / daily['vol_ma20']
    return daily


def identify_volume_surge_days(daily, threshold=2.0):
    surge_days = daily[daily['vol_ratio'] >= threshold][['ticker', 'date']].copy()
    surge_days['date'] = pd.to_datetime(surge_days['date'])
    surge_days['next_date'] = surge_days['date'] + timedelta(days=1)
    return set((row['ticker'], row['next_date'].date()) for _, row in surge_days.iterrows())


def backtest_with_exits(day_data, entry_threshold=-2.0):
    """
    複数のイグジットタイミングで検証
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 10:
        return None

    # エントリーポイントを探す
    running_high = day_data.iloc[0]['High']
    entry_idx = None
    entry_price = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']

        drop = (row['Low'] - running_high) / running_high * 100
        if drop <= entry_threshold and entry_idx is None:
            entry_idx = i
            entry_price = running_high * (1 + entry_threshold / 100)
            break

    if entry_idx is None:
        return None

    # エントリー後のデータ
    post_entry = day_data.iloc[entry_idx:]

    results = {
        'entry_price': entry_price,
        'entry_time': day_data.iloc[entry_idx]['Datetime'],
    }

    # 大引け (最後の足)
    results['exit_close'] = day_data.iloc[-1]['Close']
    results['pnl_close'] = (entry_price - results['exit_close']) / entry_price * 100

    # 14:00時点
    at_1400 = post_entry[post_entry['time'] >= time(14, 0)]
    if len(at_1400) > 0:
        results['exit_1400'] = at_1400.iloc[0]['Close']
        results['pnl_1400'] = (entry_price - results['exit_1400']) / entry_price * 100
    else:
        results['exit_1400'] = results['exit_close']
        results['pnl_1400'] = results['pnl_close']

    # 14:30時点
    at_1430 = post_entry[post_entry['time'] >= time(14, 30)]
    if len(at_1430) > 0:
        results['exit_1430'] = at_1430.iloc[0]['Close']
        results['pnl_1430'] = (entry_price - results['exit_1430']) / entry_price * 100
    else:
        results['exit_1430'] = results['exit_close']
        results['pnl_1430'] = results['pnl_close']

    # エントリー後の最安値（理論最大利益）
    results['lowest_after_entry'] = post_entry['Low'].min()
    results['pnl_max'] = (entry_price - results['lowest_after_entry']) / entry_price * 100

    # トレーリングストップ（高値から-1%で利確）
    trail_exit = None
    post_entry_high = entry_price
    for _, row in post_entry.iterrows():
        # ショートなので安値が更新されたら追跡
        if row['Low'] < post_entry_high:
            post_entry_high = row['Low']
        # 安値から1%戻したら利確
        bounce = (row['High'] - post_entry_high) / post_entry_high * 100
        if bounce >= 1.0 and trail_exit is None:
            trail_exit = post_entry_high * 1.01
            break

    if trail_exit:
        results['exit_trail'] = trail_exit
        results['pnl_trail'] = (entry_price - trail_exit) / entry_price * 100
    else:
        results['exit_trail'] = results['exit_close']
        results['pnl_trail'] = results['pnl_close']

    return results


def run_backtest():
    print("=== イグジットタイミング別バックテスト ===\n")

    print("1. データ読み込み...")
    df_5m, df_watchlist = load_data()
    target_tickers = set(df_watchlist['ticker'].unique())
    df_5m = df_5m[df_5m['ticker'].isin(target_tickers)]

    print("2. 出来高フィルタ準備...")
    daily = calc_daily_volume(df_5m)
    target_days = identify_volume_surge_days(daily, 2.0)
    print(f"   出来高2倍翌日: {len(target_days)} 日")

    print("\n3. バックテスト実行...")
    results = []

    grouped = df_5m.groupby(['ticker', 'date'])
    total = len(grouped)

    for i, ((ticker, date), day_data) in enumerate(grouped):
        if (i + 1) % 5000 == 0:
            print(f"   処理中: {i+1:,}/{total:,}")

        # 出来高2倍翌日のみ
        if (ticker, date) not in target_days:
            continue

        result = backtest_with_exits(day_data, -2.0)
        if result:
            result['ticker'] = ticker
            result['date'] = date
            results.append(result)

    df_results = pd.DataFrame(results)

    print(f"\n   トレード数: {len(df_results):,}")

    print("\n" + "=" * 70)
    print("イグジットタイミング別結果（出来高2倍翌日、エントリー: 高値-2%）")
    print("=" * 70)
    print(f"{'イグジット':<20} {'勝率':>10} {'平均損益':>10} {'中央値':>10} {'コスト後':>10}")
    print("-" * 70)

    exits = {
        '大引け': 'pnl_close',
        '14:00': 'pnl_1400',
        '14:30': 'pnl_1430',
        'トレーリング1%': 'pnl_trail',
        '理論最大': 'pnl_max',
    }

    cost = 0.25

    for name, col in exits.items():
        win_rate = (df_results[col] > 0).mean() * 100
        avg = df_results[col].mean()
        med = df_results[col].median()
        net = avg - cost

        mark = "✓" if net > 0 else "✗"
        print(f"{name:<20} {win_rate:>9.1f}% {avg:>9.2f}% {med:>9.2f}% {net:>9.2f}% {mark}")

    # 保存
    df_results.to_parquet(DATA_DIR / "backtest_exit_timing.parquet", index=False)

    return df_results


if __name__ == '__main__':
    run_backtest()
