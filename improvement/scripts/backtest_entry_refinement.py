#!/usr/bin/env python3
"""
エントリー条件の精緻化検証

- 高値更新リスクへの対応
- 時間条件の追加
- 損切り条件の追加
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


def backtest_basic(day_data, entry_drop=-2.0):
    """基本: 高値から-2%でエントリー、大引け決済"""
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    running_high = day_data.iloc[0]['High']
    entry_idx = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']
        drop = (row['Low'] - running_high) / running_high * 100
        if drop <= entry_drop and entry_idx is None:
            entry_idx = i
            entry_price = running_high * (1 + entry_drop / 100)
            break

    if entry_idx is None:
        return None

    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl, 'entry_idx': entry_idx, 'entry_price': entry_price}


def backtest_am_high_only(day_data, entry_drop=-2.0):
    """前場高値条件: 前場で高値をつけた場合のみエントリー"""
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    am_data = day_data[day_data['time'] < time(12, 0)]
    if len(am_data) < 5:
        return None

    am_high = am_data['High'].max()
    am_high_idx = am_data['High'].idxmax()

    # エントリーポイントを探す（前場高値基準）
    entry_idx = None
    for i, row in day_data.iterrows():
        if i <= am_high_idx:
            continue
        drop = (row['Low'] - am_high) / am_high * 100
        if drop <= entry_drop and entry_idx is None:
            entry_idx = i
            entry_price = am_high * (1 + entry_drop / 100)
            break

    if entry_idx is None:
        return None

    # 前場高値が日中高値かチェック
    day_high = day_data['High'].max()
    if day_high > am_high * 1.005:  # 0.5%以上更新された場合は前場高値ではない
        return None

    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl, 'entry_idx': entry_idx, 'am_high': am_high}


def backtest_with_stoploss(day_data, entry_drop=-2.0, stop_loss=2.0):
    """損切り付き: エントリー後に+X%戻したら損切り"""
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    running_high = day_data.iloc[0]['High']
    entry_idx = None
    entry_price = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']
        drop = (row['Low'] - running_high) / running_high * 100
        if drop <= entry_drop and entry_idx is None:
            entry_idx = i
            entry_price = running_high * (1 + entry_drop / 100)
            break

    if entry_idx is None:
        return None

    # エントリー後の値動きをチェック
    post_entry = day_data.iloc[entry_idx:]
    exit_price = None
    stopped_out = False

    for _, row in post_entry.iterrows():
        # 損切りチェック（エントリー価格から+X%上昇）
        loss = (row['High'] - entry_price) / entry_price * 100
        if loss >= stop_loss:
            exit_price = entry_price * (1 + stop_loss / 100)
            stopped_out = True
            break

    if exit_price is None:
        exit_price = day_data.iloc[-1]['Close']

    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl, 'stopped_out': stopped_out}


def backtest_confirm_reversal(day_data, entry_drop=-2.0, confirm_drop=-3.0):
    """反転確認: -2%で監視開始、-3%で本エントリー"""
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    running_high = day_data.iloc[0]['High']
    watch_started = False
    watch_high = None
    entry_idx = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']
            watch_started = False  # 高値更新でリセット

        drop = (row['Low'] - running_high) / running_high * 100

        if not watch_started and drop <= entry_drop:
            watch_started = True
            watch_high = running_high

        if watch_started and drop <= confirm_drop:
            entry_idx = i
            entry_price = watch_high * (1 + confirm_drop / 100)
            break

    if entry_idx is None:
        return None

    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl}


def backtest_time_filter(day_data, entry_drop=-2.0, min_time=time(10, 0)):
    """時間フィルタ: 指定時刻以降のみエントリー"""
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)
    if len(day_data) < 10:
        return None

    running_high = day_data.iloc[0]['High']
    entry_idx = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']

        if row['time'] < min_time:
            continue

        drop = (row['Low'] - running_high) / running_high * 100
        if drop <= entry_drop and entry_idx is None:
            entry_idx = i
            entry_price = running_high * (1 + entry_drop / 100)
            break

    if entry_idx is None:
        return None

    exit_price = day_data.iloc[-1]['Close']
    pnl = (entry_price - exit_price) / entry_price * 100

    return {'pnl': pnl}


def run_backtest():
    print("=== エントリー条件精緻化検証 ===\n")

    print("1. データ読み込み...")
    df_5m, df_watchlist = load_data()
    target_tickers = set(df_watchlist['ticker'].unique())
    df_5m = df_5m[df_5m['ticker'].isin(target_tickers)]

    print("2. 出来高フィルタ準備...")
    daily = calc_daily_volume(df_5m)
    target_days = identify_volume_surge_days(daily, 2.0)

    print(f"   対象日: {len(target_days)}")

    strategies = {
        '基本（-2%）': lambda d: backtest_basic(d, -2.0),
        '前場高値のみ': backtest_am_high_only,
        '損切り1%': lambda d: backtest_with_stoploss(d, -2.0, 1.0),
        '損切り2%': lambda d: backtest_with_stoploss(d, -2.0, 2.0),
        '損切り3%': lambda d: backtest_with_stoploss(d, -2.0, 3.0),
        '反転確認（-3%）': lambda d: backtest_confirm_reversal(d, -2.0, -3.0),
        '10時以降のみ': lambda d: backtest_time_filter(d, -2.0, time(10, 0)),
        '11時以降のみ': lambda d: backtest_time_filter(d, -2.0, time(11, 0)),
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

    print("\n" + "=" * 80)
    print("エントリー条件別結果（出来高2倍翌日）")
    print("=" * 80)
    print(f"{'条件':<20} {'トレード数':>10} {'勝率':>10} {'平均損益':>10} {'コスト後':>10}")
    print("-" * 80)

    cost = 0.25
    summary = []

    for name, trades in results.items():
        if not trades:
            continue

        df = pd.DataFrame(trades)
        n = len(df)
        win_rate = (df['pnl'] > 0).mean() * 100
        avg = df['pnl'].mean()
        net = avg - cost

        mark = "✓" if net > 0 else "✗"
        print(f"{name:<20} {n:>10,} {win_rate:>9.1f}% {avg:>9.2f}% {net:>9.2f}% {mark}")

        summary.append({
            '条件': name,
            'トレード数': n,
            '勝率': win_rate,
            '平均損益': avg,
            'コスト後': net
        })

        # 損切り率を表示
        if 'stopped_out' in df.columns:
            stop_rate = df['stopped_out'].mean() * 100
            print(f"  └─ 損切り発動率: {stop_rate:.1f}%")

    pd.DataFrame(summary).to_csv(DATA_DIR / "backtest_entry_refinement.csv", index=False)

    return summary


if __name__ == '__main__':
    run_backtest()
