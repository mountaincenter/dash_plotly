#!/usr/bin/env python3
"""
後知恵なしのバックテスト

エントリー時点で得られる情報のみを使用
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


def backtest_pm_open(day_data):
    """
    後場寄付でエントリー（後知恵なし）
    条件: 後場寄付 < 前場高値
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    am_data = day_data[day_data['time'] < time(12, 0)]
    pm_data = day_data[day_data['time'] >= time(12, 30)]

    if len(am_data) < 5 or len(pm_data) < 5:
        return None

    am_high = am_data['High'].max()
    pm_open = pm_data.iloc[0]['Open']

    # 後場寄りが前場高値より低い場合のみエントリー
    if pm_open >= am_high:
        return None

    entry_price = pm_open
    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    # 後場で高値更新されたかチェック（結果の分析用）
    pm_high = pm_data['High'].max()
    high_renewed = pm_high > am_high

    return {
        'pnl': pnl,
        'entry_price': entry_price,
        'am_high': am_high,
        'high_renewed': high_renewed
    }


def backtest_pm_open_with_gap(day_data, gap_pct=-1.0):
    """
    後場寄付でエントリー（ギャップ条件付き）
    条件: 後場寄付 < 前場高値 * (1 + gap_pct/100)
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    am_data = day_data[day_data['time'] < time(12, 0)]
    pm_data = day_data[day_data['time'] >= time(12, 30)]

    if len(am_data) < 5 or len(pm_data) < 5:
        return None

    am_high = am_data['High'].max()
    pm_open = pm_data.iloc[0]['Open']

    threshold = am_high * (1 + gap_pct / 100)
    if pm_open >= threshold:
        return None

    entry_price = pm_open
    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl}


def backtest_am_high_drop(day_data, drop_pct=-2.0):
    """
    前場高値から-X%でエントリー（後知恵なし）
    後場に入ってから、前場高値基準で-X%になったらエントリー
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    am_data = day_data[day_data['time'] < time(12, 0)]
    pm_data = day_data[day_data['time'] >= time(12, 30)]

    if len(am_data) < 5 or len(pm_data) < 5:
        return None

    am_high = am_data['High'].max()
    threshold = am_high * (1 + drop_pct / 100)

    # 後場でエントリーポイントを探す
    entry_idx = None
    for i, row in pm_data.iterrows():
        if row['Low'] <= threshold:
            entry_idx = i
            entry_price = threshold
            break

    if entry_idx is None:
        return None

    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl}


def backtest_am_high_drop_with_stoploss(day_data, drop_pct=-2.0, stop_pct=2.0):
    """
    前場高値から-X%でエントリー + 損切り
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    am_data = day_data[day_data['time'] < time(12, 0)]
    pm_data = day_data[day_data['time'] >= time(12, 30)]

    if len(am_data) < 5 or len(pm_data) < 5:
        return None

    am_high = am_data['High'].max()
    threshold = am_high * (1 + drop_pct / 100)

    # 後場でエントリーポイントを探す
    entry_idx = None
    entry_price = None
    for i, row in pm_data.iterrows():
        if row['Low'] <= threshold:
            entry_idx = i
            entry_price = threshold
            break

    if entry_idx is None:
        return None

    # エントリー後の損切りチェック
    post_entry = pm_data.loc[entry_idx:]
    exit_price = None
    stopped = False
    stop_level = entry_price * (1 + stop_pct / 100)

    for _, row in post_entry.iterrows():
        if row['High'] >= stop_level:
            exit_price = stop_level
            stopped = True
            break

    if exit_price is None:
        exit_price = day_data.iloc[-1]['Close']

    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl, 'stopped': stopped}


def run_backtest():
    print("=== 後知恵なしバックテスト ===\n")

    print("1. データ読み込み...")
    df_5m, df_watchlist = load_data()
    target_tickers = set(df_watchlist['ticker'].unique())
    df_5m = df_5m[df_5m['ticker'].isin(target_tickers)]

    print("2. 出来高フィルタ準備...")
    daily = calc_daily_volume(df_5m)
    target_days = identify_volume_surge_days(daily, 2.0)
    print(f"   対象日: {len(target_days)}")

    strategies = {
        '後場寄付（前場高値未満）': backtest_pm_open,
        '後場寄付（前場高値-1%）': lambda d: backtest_pm_open_with_gap(d, -1.0),
        '後場寄付（前場高値-2%）': lambda d: backtest_pm_open_with_gap(d, -2.0),
        '後場 前場高値-2%': lambda d: backtest_am_high_drop(d, -2.0),
        '後場 前場高値-3%': lambda d: backtest_am_high_drop(d, -3.0),
        '後場 前場高値-2% 損切2%': lambda d: backtest_am_high_drop_with_stoploss(d, -2.0, 2.0),
        '後場 前場高値-2% 損切3%': lambda d: backtest_am_high_drop_with_stoploss(d, -2.0, 3.0),
    }

    results = {name: [] for name in strategies}

    print("\n3. バックテスト実行...")
    grouped = df_5m.groupby(['ticker', 'date'])

    for i, ((ticker, date), day_data) in enumerate(grouped):
        if (i + 1) % 5000 == 0:
            print(f"   処理中: {i+1:,}")

        if (ticker, date) not in target_days:
            continue

        for name, func in strategies.items():
            try:
                result = func(day_data)
                if result:
                    results[name].append(result)
            except:
                pass

    print("\n" + "=" * 85)
    print("後知恵なしバックテスト結果（出来高2倍翌日）")
    print("=" * 85)
    print(f"{'条件':<30} {'トレード数':>10} {'勝率':>10} {'平均損益':>10} {'コスト後':>10}")
    print("-" * 85)

    cost = 0.25

    for name, trades in results.items():
        if not trades:
            print(f"{name:<30} {'N/A':>10}")
            continue

        df = pd.DataFrame(trades)
        n = len(df)
        win_rate = (df['pnl'] > 0).mean() * 100
        avg = df['pnl'].mean()
        net = avg - cost

        mark = "✓" if net > 0 else "✗"
        print(f"{name:<30} {n:>10,} {win_rate:>9.1f}% {avg:>9.2f}% {net:>9.2f}% {mark}")

        # 追加情報
        if 'high_renewed' in df.columns:
            renewed_rate = df['high_renewed'].mean() * 100
            print(f"  └─ 後場で高値更新された率: {renewed_rate:.1f}%")

        if 'stopped' in df.columns:
            stop_rate = df['stopped'].mean() * 100
            print(f"  └─ 損切り発動率: {stop_rate:.1f}%")

    # 高値更新時/非更新時の内訳
    print("\n" + "=" * 85)
    print("後場寄付エントリー: 後場で高値更新された場合/されなかった場合")
    print("=" * 85)

    pm_open_trades = results.get('後場寄付（前場高値未満）', [])
    if pm_open_trades:
        df = pd.DataFrame(pm_open_trades)

        renewed = df[df['high_renewed'] == True]
        not_renewed = df[df['high_renewed'] == False]

        print(f"{'状況':<30} {'件数':>10} {'勝率':>10} {'平均損益':>10}")
        print("-" * 70)

        if len(renewed) > 0:
            print(f"{'後場で高値更新あり':<30} {len(renewed):>10,} {(renewed['pnl'] > 0).mean()*100:>9.1f}% {renewed['pnl'].mean():>9.2f}%")

        if len(not_renewed) > 0:
            print(f"{'後場で高値更新なし':<30} {len(not_renewed):>10,} {(not_renewed['pnl'] > 0).mean()*100:>9.1f}% {not_renewed['pnl'].mean():>9.2f}%")


if __name__ == '__main__':
    run_backtest()
