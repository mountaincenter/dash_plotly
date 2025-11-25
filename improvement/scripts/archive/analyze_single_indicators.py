#!/usr/bin/env python3
"""
各指標の単独分析（RSI / 出来高 / SMA5）
目的: 各指標がどの範囲で最も利益が出るかを特定
出力: 各指標の範囲別の勝率・平均利益・件数
"""
from pathlib import Path
import pandas as pd
import numpy as np

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'parquet'

MIN_DATA_POINTS = 30

def load_target_stocks():
    """政策銘柄とCORE30のリストを取得"""
    stocks_file = DATA_DIR / 'all_stocks.parquet'
    df = pd.read_parquet(stocks_file)
    target_stocks = df[
        df['categories'].apply(lambda x: 'TOPIX_CORE30' in x or '政策銘柄' in x)
    ]['ticker'].tolist()
    return target_stocks

def calculate_technical_indicators(df):
    """テクニカル指標を計算"""
    df = df.sort_values('date').copy()
    df['prev_close'] = df['Close'].shift(1)

    # RSI (14日)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi_14d'] = 100 - (100 / (1 + rs))

    # 出来高変化率
    df['volume_avg_20d'] = df['Volume'].rolling(window=20).mean()
    df['volume_change_20d'] = df['Volume'] / df['volume_avg_20d']

    # SMA5乖離率
    df['sma_5d'] = df['Close'].rolling(window=5).mean()
    df['price_vs_sma5_pct'] = ((df['Close'] - df['sma_5d']) / df['sma_5d']) * 100

    return df

def analyze_single_ticker(ticker, prices_df):
    """1銘柄の分析"""
    ticker_df = prices_df[prices_df['ticker'] == ticker].copy()
    if len(ticker_df) < MIN_DATA_POINTS:
        return None

    ticker_df = calculate_technical_indicators(ticker_df)

    results = []
    for i in range(len(ticker_df) - 1):
        row = ticker_df.iloc[i]
        next_row = ticker_df.iloc[i + 1]

        # 買いの利益（翌日始値→終値）
        buy_profit = (next_row['Close'] - next_row['Open']) * 100
        buy_win = next_row['Close'] > next_row['Open']

        # 売りの利益（翌日始値→終値のショート）
        sell_profit = (next_row['Open'] - next_row['Close']) * 100
        sell_win = next_row['Open'] > next_row['Close']

        results.append({
            'date': row['date'],
            'ticker': ticker,
            'rsi_14d': row.get('rsi_14d'),
            'volume_change_20d': row.get('volume_change_20d'),
            'price_vs_sma5_pct': row.get('price_vs_sma5_pct'),
            'buy_profit_100': buy_profit,
            'buy_win': buy_win,
            'sell_profit_100': sell_profit,
            'sell_win': sell_win
        })

    return results

def main():
    print("=" * 80)
    print("各指標の単独分析（RSI / 出来高 / SMA5）")
    print("=" * 80)

    # データ読み込み
    target_stocks = load_target_stocks()
    print(f"\n対象銘柄数: {len(target_stocks)}")

    prices_file = DATA_DIR / 'prices_max_1d.parquet'
    prices_df = pd.read_parquet(prices_file)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df[prices_df['date'] >= '2020-01-01']
    prices_df = prices_df[prices_df['ticker'].isin(target_stocks)]

    # 全銘柄分析
    print("\n分析実行中...")
    all_results = []
    for i, ticker in enumerate(target_stocks, 1):
        print(f"  [{i}/{len(target_stocks)}] {ticker}")
        ticker_results = analyze_single_ticker(ticker, prices_df)
        if ticker_results:
            all_results.extend(ticker_results)

    df = pd.DataFrame(all_results)
    print(f"\n総分析件数: {len(df):,}件")

    # === RSI分析（買い） ===
    print("\n" + "=" * 80)
    print("📊 RSI範囲別分析（買いシグナル）")
    print("=" * 80)

    rsi_ranges = [
        (0, 10, '0-10'),
        (10, 20, '10-20'),
        (20, 30, '20-30'),
        (30, 40, '30-40'),
        (40, 50, '40-50'),
        (50, 60, '50-60'),
        (60, 70, '60-70'),
        (70, 80, '70-80'),
        (80, 100, '80-100')
    ]

    print(f"{'RSI範囲':<12} {'件数':>8} {'勝率':>8} {'平均利益':>12} {'合計利益':>15}")
    print("-" * 80)

    for min_val, max_val, label in rsi_ranges:
        subset = df[(df['rsi_14d'] >= min_val) & (df['rsi_14d'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['buy_win'] == True).sum() / count * 100
            avg_profit = subset['buy_profit_100'].mean()
            total_profit = subset['buy_profit_100'].sum()
            print(f"{label:<12} {count:>8,} {win_rate:>7.2f}% {avg_profit:>11,.0f}円 {total_profit:>14,.0f}円")

    # === 出来高分析（買い） ===
    print("\n" + "=" * 80)
    print("📊 出来高変化率別分析（買いシグナル）")
    print("=" * 80)

    volume_ranges = [
        (0, 0.5, '< 0.5倍'),
        (0.5, 0.8, '0.5-0.8倍'),
        (0.8, 1.0, '0.8-1.0倍'),
        (1.0, 1.2, '1.0-1.2倍'),
        (1.2, 1.5, '1.2-1.5倍'),
        (1.5, 2.0, '1.5-2.0倍'),
        (2.0, 3.0, '2.0-3.0倍'),
        (3.0, 100, '> 3.0倍')
    ]

    print(f"{'出来高変化率':<15} {'件数':>8} {'勝率':>8} {'平均利益':>12} {'合計利益':>15}")
    print("-" * 80)

    for min_val, max_val, label in volume_ranges:
        subset = df[(df['volume_change_20d'] >= min_val) & (df['volume_change_20d'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['buy_win'] == True).sum() / count * 100
            avg_profit = subset['buy_profit_100'].mean()
            total_profit = subset['buy_profit_100'].sum()
            print(f"{label:<15} {count:>8,} {win_rate:>7.2f}% {avg_profit:>11,.0f}円 {total_profit:>14,.0f}円")

    # === SMA5乖離率分析（買い） ===
    print("\n" + "=" * 80)
    print("📊 SMA5乖離率別分析（買いシグナル）")
    print("=" * 80)

    sma5_ranges = [
        (-100, -10, '< -10%'),
        (-10, -5, '-10% ~ -5%'),
        (-5, -2, '-5% ~ -2%'),
        (-2, 0, '-2% ~ 0%'),
        (0, 2, '0% ~ 2%'),
        (2, 5, '2% ~ 5%'),
        (5, 10, '5% ~ 10%'),
        (10, 100, '> 10%')
    ]

    print(f"{'SMA5乖離率':<15} {'件数':>8} {'勝率':>8} {'平均利益':>12} {'合計利益':>15}")
    print("-" * 80)

    for min_val, max_val, label in sma5_ranges:
        subset = df[(df['price_vs_sma5_pct'] >= min_val) & (df['price_vs_sma5_pct'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['buy_win'] == True).sum() / count * 100
            avg_profit = subset['buy_profit_100'].mean()
            total_profit = subset['buy_profit_100'].sum()
            print(f"{label:<15} {count:>8,} {win_rate:>7.2f}% {avg_profit:>11,.0f}円 {total_profit:>14,.0f}円")

    # === 売りシグナルも分析 ===
    print("\n" + "=" * 80)
    print("📊 RSI範囲別分析（売りシグナル）")
    print("=" * 80)

    print(f"{'RSI範囲':<12} {'件数':>8} {'勝率':>8} {'平均利益':>12} {'合計利益':>15}")
    print("-" * 80)

    for min_val, max_val, label in rsi_ranges:
        subset = df[(df['rsi_14d'] >= min_val) & (df['rsi_14d'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['sell_win'] == True).sum() / count * 100
            avg_profit = subset['sell_profit_100'].mean()
            total_profit = subset['sell_profit_100'].sum()
            print(f"{label:<12} {count:>8,} {win_rate:>7.2f}% {avg_profit:>11,.0f}円 {total_profit:>14,.0f}円")

    print("\n✅ 分析完了")
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
