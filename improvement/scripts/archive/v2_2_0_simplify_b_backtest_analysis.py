#!/usr/bin/env python3
"""
v2.2.0_simplify_b: 引き算アプローチ（RSI + SMA5のみ）
ベース: v2.1.1から 出来高を除外
対象: 政策銘柄 + TOPIX_CORE30（2020-2025）全価格帯
"""
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'parquet'
OUTPUT_DIR = BASE_DIR / 'improvement' / 'data'
OUTPUT_FILE = OUTPUT_DIR / 'v2_2_0_simplify_b_backtest_results.parquet'

# 設定
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

    # SMA5との乖離率
    df['sma_5d'] = df['Close'].rolling(window=5).mean()
    df['price_vs_sma5_pct'] = ((df['Close'] - df['sma_5d']) / df['sma_5d']) * 100

    return df

def calculate_score(row):
    """
    RSI + SMA5のみのスコアリング
    """
    score = 0
    reasons = []

    # RSI（7段階）
    rsi_14d = row.get('rsi_14d')
    if pd.notna(rsi_14d):
        if rsi_14d < 20:
            score += 30
            reasons.append(f'RSI {rsi_14d:.1f}（極度の売られすぎ）')
        elif rsi_14d < 30:
            score += 20
            reasons.append(f'RSI {rsi_14d:.1f}（売られすぎ）')
        elif rsi_14d < 40:
            score += 10
            reasons.append(f'RSI {rsi_14d:.1f}（やや売られすぎ）')
        elif rsi_14d >= 80:
            score -= 15
            reasons.append(f'RSI {rsi_14d:.1f}（極度の買われすぎ）')
        elif rsi_14d >= 70:
            score -= 10
            reasons.append(f'RSI {rsi_14d:.1f}（買われすぎ）')
        elif rsi_14d >= 60:
            score -= 5
            reasons.append(f'RSI {rsi_14d:.1f}（やや買われすぎ）')

    # SMA5乖離率（6段階）
    price_vs_sma5 = row.get('price_vs_sma5_pct')
    if pd.notna(price_vs_sma5):
        if price_vs_sma5 < -5.0:
            score += 5
            reasons.append(f'SMA5 {price_vs_sma5:.1f}%（大幅下落）')
        elif price_vs_sma5 < -2.0:
            score += 10
            reasons.append(f'SMA5 {price_vs_sma5:.1f}%（下落）')
        elif price_vs_sma5 < 0:
            score += 15
            reasons.append(f'SMA5 {price_vs_sma5:.1f}%（押し目）')
        elif price_vs_sma5 < 2.0:
            score += 5
            reasons.append(f'SMA5 +{price_vs_sma5:.1f}%（微上昇）')
        elif price_vs_sma5 >= 5.0:
            score -= 10
            reasons.append(f'SMA5 +{price_vs_sma5:.1f}%（過熱）')

    # アクション判定
    if score >= 25:
        action = '買い'
    elif score <= -15:
        action = '売り'
    else:
        action = '静観'

    return (score, action, reasons)

def backtest_single_ticker(ticker, prices_df):
    """1銘柄のバックテスト"""
    ticker_df = prices_df[prices_df['ticker'] == ticker].copy()
    if len(ticker_df) < MIN_DATA_POINTS:
        return None

    ticker_df = calculate_technical_indicators(ticker_df)

    results = []
    for i in range(len(ticker_df) - 1):
        row = ticker_df.iloc[i]
        next_row = ticker_df.iloc[i + 1]

        score, action, reasons = calculate_score(row)

        if action == '買い':
            profit = (next_row['Close'] - next_row['Open']) * 100
            win = next_row['Close'] > next_row['Open']
        elif action == '売り':
            profit = (next_row['Open'] - next_row['Close']) * 100
            win = next_row['Open'] > next_row['Close']
        else:
            profit = 0
            win = None

        results.append({
            'date': row['date'],
            'ticker': ticker,
            'action': action,
            'score': score,
            'prev_close': row.get('prev_close'),
            'close': row['Close'],
            'next_close': next_row['Close'],
            'rsi_14d': row.get('rsi_14d'),
            'price_vs_sma5_pct': row.get('price_vs_sma5_pct'),
            'profit_100': profit,
            'win': win,
            'reasons': ' / '.join(reasons) if reasons else ''
        })

    return results

def main():
    print("=" * 60)
    print("v2.2.0_simplify_b: RSI + SMA5のみ")
    print("=" * 60)

    target_stocks = load_target_stocks()
    print(f"\n対象銘柄数: {len(target_stocks)}")

    prices_file = DATA_DIR / 'prices_max_1d.parquet'
    prices_df = pd.read_parquet(prices_file)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df[prices_df['date'] >= '2020-01-01']
    prices_df = prices_df[prices_df['ticker'].isin(target_stocks)]

    all_results = []
    for i, ticker in enumerate(target_stocks, 1):
        print(f"  [{i}/{len(target_stocks)}] {ticker} 分析中...")
        ticker_results = backtest_single_ticker(ticker, prices_df)
        if ticker_results:
            all_results.extend(ticker_results)

    results_df = pd.DataFrame(all_results)
    print(f"\n総判定数: {len(results_df):,}件")
    print(f"買い: {(results_df['action'] == '買い').sum():,}件")
    print(f"売り: {(results_df['action'] == '売り').sum():,}件")

    buy_df = results_df[results_df['action'] == '買い']
    sell_df = results_df[results_df['action'] == '売り']

    if len(buy_df) > 0:
        buy_win_rate = (buy_df['win'] == True).sum() / len(buy_df) * 100
        buy_avg_profit = buy_df['profit_100'].mean()
        print(f"\n買い勝率: {buy_win_rate:.2f}%")
        print(f"買い平均利益: {buy_avg_profit:,.0f}円/100株")

    if len(sell_df) > 0:
        sell_win_rate = (sell_df['win'] == True).sum() / len(sell_df) * 100
        sell_avg_profit = sell_df['profit_100'].mean()
        print(f"\n売り勝率: {sell_win_rate:.2f}%")
        print(f"売り平均利益: {sell_avg_profit:,.0f}円/100株")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results_df.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n✅ 保存完了: {OUTPUT_FILE}")
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
