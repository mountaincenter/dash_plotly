#!/usr/bin/env python3
"""
一次スクリーニング（前日出来高急騰）を加えたバックテスト
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time, timedelta

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


def load_data():
    """データ読み込み"""
    df_5m = pd.read_parquet(DATA_DIR / "surge_candidates_5m.parquet")
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime']).dt.tz_localize(None)
    df_5m['date'] = df_5m['Datetime'].dt.date
    df_5m['time'] = df_5m['Datetime'].dt.time

    df_watchlist = pd.read_parquet(DATA_DIR / "morning_peak_watchlist.parquet")

    return df_5m, df_watchlist


def calc_daily_volume(df_5m: pd.DataFrame) -> pd.DataFrame:
    """日次出来高を計算"""
    daily = df_5m.groupby(['ticker', 'date']).agg({
        'Volume': 'sum',
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    }).reset_index()

    # 20日移動平均出来高
    daily = daily.sort_values(['ticker', 'date'])
    daily['vol_ma20'] = daily.groupby('ticker')['Volume'].transform(
        lambda x: x.rolling(20, min_periods=10).mean().shift(1)
    )
    daily['vol_ratio'] = daily['Volume'] / daily['vol_ma20']

    return daily


def identify_volume_surge_days(daily: pd.DataFrame, threshold: float = 2.0) -> set:
    """出来高急騰日の翌日を特定"""
    surge_days = daily[daily['vol_ratio'] >= threshold][['ticker', 'date']].copy()
    surge_days['date'] = pd.to_datetime(surge_days['date'])
    surge_days['next_date'] = surge_days['date'] + timedelta(days=1)

    # 翌営業日を取得（簡易版：単純に+1日）
    target_days = set()
    for _, row in surge_days.iterrows():
        target_days.add((row['ticker'], row['next_date'].date()))

    return target_days


def backtest_pattern_a(day_data: pd.DataFrame, threshold: float = -2.0) -> dict:
    """パターンA: 日中高値から-X%下落でエントリー"""
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 10:
        return None

    running_high = day_data.iloc[0]['High']
    entry_idx = None
    entry_price = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']

        drop_from_high = (row['Low'] - running_high) / running_high * 100

        if drop_from_high <= threshold and entry_idx is None:
            entry_idx = i
            entry_price = running_high * (1 + threshold / 100)
            break

    if entry_idx is None:
        return None

    exit_price = day_data.iloc[-1]['Close']
    pnl_pct = (entry_price - exit_price) / entry_price * 100

    return {
        'entry_time': day_data.iloc[entry_idx]['Datetime'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct,
        'high_at_entry': running_high
    }


def run_backtest():
    print("=== 一次スクリーニング付きバックテスト ===\n")

    print("1. データ読み込み...")
    df_5m, df_watchlist = load_data()

    target_tickers = set(df_watchlist['ticker'].unique())
    df_5m = df_5m[df_5m['ticker'].isin(target_tickers)]

    print(f"   常習犯銘柄: {len(target_tickers)}")

    print("\n2. 日次出来高計算...")
    daily = calc_daily_volume(df_5m)

    print("\n3. 出来高急騰翌日を特定...")
    for thresh in [1.5, 2.0, 3.0, 5.0]:
        target_days = identify_volume_surge_days(daily, thresh)
        print(f"   出来高{thresh}倍以上の翌日: {len(target_days)} 日")

    print("\n4. バックテスト実行...")

    results = {
        '全日': [],
        '出来高1.5倍翌日': [],
        '出来高2倍翌日': [],
        '出来高3倍翌日': [],
        '出来高5倍翌日': [],
    }

    target_days_15 = identify_volume_surge_days(daily, 1.5)
    target_days_20 = identify_volume_surge_days(daily, 2.0)
    target_days_30 = identify_volume_surge_days(daily, 3.0)
    target_days_50 = identify_volume_surge_days(daily, 5.0)

    grouped = df_5m.groupby(['ticker', 'date'])
    total = len(grouped)

    for i, ((ticker, date), day_data) in enumerate(grouped):
        if (i + 1) % 5000 == 0:
            print(f"   処理中: {i+1:,}/{total:,}")

        result = backtest_pattern_a(day_data, -2.0)
        if result:
            result['ticker'] = ticker
            result['date'] = date

            results['全日'].append(result)

            if (ticker, date) in target_days_15:
                results['出来高1.5倍翌日'].append(result)
            if (ticker, date) in target_days_20:
                results['出来高2倍翌日'].append(result)
            if (ticker, date) in target_days_30:
                results['出来高3倍翌日'].append(result)
            if (ticker, date) in target_days_50:
                results['出来高5倍翌日'].append(result)

    print("\n5. 結果集計...")
    print("\n" + "=" * 70)
    print("パターンA（高値から-2%）+ 一次スクリーニング比較")
    print("=" * 70)
    print(f"{'条件':<20} {'トレード数':>10} {'勝率':>10} {'平均損益':>10} {'中央値':>10}")
    print("-" * 70)

    summary_data = []

    for name, trades in results.items():
        if not trades:
            continue

        df_trades = pd.DataFrame(trades)

        win_rate = (df_trades['pnl_pct'] > 0).mean() * 100
        avg_pnl = df_trades['pnl_pct'].mean()
        median_pnl = df_trades['pnl_pct'].median()
        total_trades = len(df_trades)

        print(f"{name:<20} {total_trades:>10,} {win_rate:>9.1f}% {avg_pnl:>9.2f}% {median_pnl:>9.2f}%")

        summary_data.append({
            '条件': name,
            'トレード数': total_trades,
            '勝率': win_rate,
            '平均損益': avg_pnl,
            '中央値': median_pnl
        })

    # コスト考慮後の試算
    print("\n" + "=" * 70)
    print("コスト考慮後の試算（手数料+スリッページ = 0.25%/回と仮定）")
    print("=" * 70)
    print(f"{'条件':<20} {'純損益':>10} {'100回で':>15}")
    print("-" * 70)

    cost_per_trade = 0.25

    for row in summary_data:
        net_pnl = row['平均損益'] - cost_per_trade
        total_100 = net_pnl * 100
        status = "✓" if net_pnl > 0 else "✗"

        print(f"{row['条件']:<20} {net_pnl:>9.2f}% {total_100:>13.1f}% {status}")

    # CSVで保存
    df_summary = pd.DataFrame(summary_data)
    df_summary.to_csv(DATA_DIR / "backtest_volume_filter_summary.csv", index=False)

    return summary_data


if __name__ == '__main__':
    run_backtest()
