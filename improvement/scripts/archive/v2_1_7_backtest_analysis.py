#!/usr/bin/env python3
"""
v2.1.7スコアリングロジックの静的バックテスト分析
データドリブンスコアリング + 買いシグナルの2段階分類 + アラート機能
strong_buy内で危険パターンを検出してアラート
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
OUTPUT_FILE = OUTPUT_DIR / 'v2_1_7_backtest_results.parquet'

# 設定
MIN_DATA_POINTS = 30
STRONG_BUY_THRESHOLD = 50  # strong_buyの閾値

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

def calculate_v2_1_7_score(row):
    """
    v2.1.7データドリブンスコアリング + アラート判定
    各指標の実績データに基づいた配点
    strong_buy: score_buy >= 50
    buy: 25 <= score_buy < 50

    アラート条件（データから判明した危険パターン）:
    - RSI 40-60（特に40-50は危険）
    - 出来高 1.2-1.5倍（危険ゾーン）
    - SMA5 -5%~0%（中途半端な押し目）

    Returns:
        (score_buy, score_sell, action, reasons, alert_flags)
    """
    score_buy = 0
    score_sell = 0
    reasons = []
    alert_flags = []  # アラート理由

    # === RSI（買い）: 実績ベース ===
    rsi_14d = row.get('rsi_14d')
    if pd.notna(rsi_14d):
        if rsi_14d < 10:
            score_buy += 50  # 平均+1,491円
            reasons.append(f'RSI {rsi_14d:.1f}（超強力買いシグナル）')
        elif rsi_14d < 20:
            # 平均-792円なので無視
            pass
        elif rsi_14d < 30:
            score_buy += 15  # 平均+323円
            reasons.append(f'RSI {rsi_14d:.1f}（売られすぎ）')
        elif rsi_14d < 40:
            score_buy += 15  # 平均+336円
            reasons.append(f'RSI {rsi_14d:.1f}（やや売られすぎ）')
        elif rsi_14d >= 80:
            # 平均+127円なので無視（売りシグナルとして使う）
            score_sell -= 5
        elif rsi_14d >= 70:
            score_buy -= 10  # 平均-182円
            score_sell -= 10  # 売りで+182円
            reasons.append(f'RSI {rsi_14d:.1f}（買われすぎ）')
        elif rsi_14d >= 60:
            score_buy -= 15  # 平均-298円
            score_sell -= 15  # 売りで+298円
            reasons.append(f'RSI {rsi_14d:.1f}（やや買われすぎ）')
            alert_flags.append(f'⚠️ RSI {rsi_14d:.1f}（60-70: 買われすぎ警告）')
        elif rsi_14d >= 50:
            score_buy -= 10  # 平均-199円
            alert_flags.append(f'⚠️ RSI {rsi_14d:.1f}（50-60: 中立域警告）')
        elif rsi_14d >= 40:
            # RSI 40-50は危険（平均-2,599円）
            alert_flags.append(f'🚨 RSI {rsi_14d:.1f}（40-50: 危険ゾーン、平均-2,599円）')

    # === 出来高（買い）: 実績ベース ===
    volume_change = row.get('volume_change_20d')
    if pd.notna(volume_change):
        if volume_change < 0.5:
            score_buy -= 20  # 平均-538円
            reasons.append(f'出来高{volume_change:.2f}倍（極端な低調）')
        elif volume_change < 0.8:
            score_buy += 10  # 平均+172円
            reasons.append(f'出来高{volume_change:.2f}倍（低調だが買いチャンス）')
        elif volume_change < 1.0:
            # 平均-185円なので無視
            pass
        elif volume_change < 1.2:
            score_buy += 5  # 平均+99円
        elif volume_change < 1.5:
            score_buy -= 20  # 平均-557円（要注意！）
            reasons.append(f'出来高{volume_change:.2f}倍（危険ゾーン）')
            alert_flags.append(f'🚨 出来高{volume_change:.2f}倍（1.2-1.5倍: 超危険ゾーン、平均-10,337円）')
        elif volume_change < 2.0:
            # 平均-31円なので無視
            pass
        elif volume_change < 3.0:
            score_buy += 5  # 平均+87円
            reasons.append(f'出来高{volume_change:.1f}倍（やや活発）')
        else:
            score_buy -= 15  # 平均-372円

    # === SMA5乖離率（買い）: 最重要指標！ ===
    price_vs_sma5 = row.get('price_vs_sma5_pct')
    if pd.notna(price_vs_sma5):
        if price_vs_sma5 < -10.0:
            score_buy += 80  # 平均+3,089円（超強力！）
            reasons.append(f'SMA5 {price_vs_sma5:.1f}%（超強力押し目）')
        elif price_vs_sma5 < -5.0:
            # 平均-155円なので無視
            alert_flags.append(f'⚠️ SMA5 {price_vs_sma5:.1f}%（-5~-10%: やや弱い押し目）')
        elif price_vs_sma5 < -2.0:
            # 平均-177円なので無視
            alert_flags.append(f'⚠️ SMA5 {price_vs_sma5:.1f}%（-2~-5%: 弱い押し目）')
        elif price_vs_sma5 < 0:
            # 平均-55円なので無視
            alert_flags.append(f'⚠️ SMA5 {price_vs_sma5:.1f}%（-2~0%: 中途半端な押し目、平均-1,417円）')
        elif price_vs_sma5 < 2.0:
            score_buy += 5  # 平均+90円
        elif price_vs_sma5 < 5.0:
            score_buy -= 15  # 平均-259円
        elif price_vs_sma5 < 10.0:
            score_buy -= 30  # 平均-923円
            reasons.append(f'SMA5 +{price_vs_sma5:.1f}%（過熱）')
        else:
            score_buy -= 100  # 平均-5,253円（絶対避ける！）
            reasons.append(f'SMA5 +{price_vs_sma5:.1f}%（超危険ゾーン）')

    # === アクション判定（買いシグナルを2段階 + アラート） ===
    if score_buy >= STRONG_BUY_THRESHOLD:
        if len(alert_flags) > 0:
            action = 'strong_buy_alert'  # アラート付きstrong_buy
        else:
            action = 'strong_buy_clean'  # クリーンなstrong_buy
    elif score_buy >= 25:
        action = 'buy'
    elif score_sell <= -15:
        action = 'sell'
    else:
        action = 'hold'

    return (score_buy, score_sell, action, reasons, alert_flags)

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

        score_buy, score_sell, action, reasons, alert_flags = calculate_v2_1_7_score(row)

        if action in ['strong_buy_clean', 'strong_buy_alert', 'buy']:
            profit = (next_row['Close'] - next_row['Open']) * 100
            win = next_row['Close'] > next_row['Open']
        elif action == 'sell':
            profit = (next_row['Open'] - next_row['Close']) * 100
            win = next_row['Open'] > next_row['Close']
        else:
            profit = 0
            win = None

        results.append({
            'date': row['date'],
            'ticker': ticker,
            'action': action,
            'score_buy': score_buy,
            'score_sell': score_sell,
            'prev_close': row.get('prev_close'),
            'close': row['Close'],
            'next_open': next_row['Open'],
            'next_close': next_row['Close'],
            'rsi_14d': row.get('rsi_14d'),
            'volume_change_20d': row.get('volume_change_20d'),
            'price_vs_sma5_pct': row.get('price_vs_sma5_pct'),
            'profit_100': profit,
            'win': win,
            'reasons': ' / '.join(reasons) if reasons else '',
            'alert_flags': ' | '.join(alert_flags) if alert_flags else ''
        })

    return results

def main():
    print("=" * 80)
    print("v2.1.7スコアリングロジック 静的バックテスト分析")
    print("データドリブンスコアリング + 買いシグナルの2段階分類 + アラート機能")
    print(f"strong_buy_clean: 安全 / strong_buy_alert: 要注意")
    print("=" * 80)

    target_stocks = load_target_stocks()
    print(f"\n対象銘柄数: {len(target_stocks)}")

    prices_file = DATA_DIR / 'prices_max_1d.parquet'
    prices_df = pd.read_parquet(prices_file)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df[prices_df['date'] >= '2020-01-01']
    prices_df = prices_df[prices_df['ticker'].isin(target_stocks)]

    print("\nバックテスト実行中...")
    all_results = []
    for i, ticker in enumerate(target_stocks, 1):
        print(f"  [{i}/{len(target_stocks)}] {ticker} 分析中...")
        ticker_results = backtest_single_ticker(ticker, prices_df)
        if ticker_results:
            all_results.extend(ticker_results)

    results_df = pd.DataFrame(all_results)
    print(f"\n総判定数: {len(results_df):,}件")
    print(f"strong_buy_clean: {(results_df['action'] == 'strong_buy_clean').sum():,}件")
    print(f"strong_buy_alert: {(results_df['action'] == 'strong_buy_alert').sum():,}件")
    print(f"buy: {(results_df['action'] == 'buy').sum():,}件")
    print(f"sell: {(results_df['action'] == 'sell').sum():,}件")
    print(f"hold: {(results_df['action'] == 'hold').sum():,}件")

    strong_buy_clean_df = results_df[results_df['action'] == 'strong_buy_clean']
    strong_buy_alert_df = results_df[results_df['action'] == 'strong_buy_alert']
    buy_df = results_df[results_df['action'] == 'buy']
    sell_df = results_df[results_df['action'] == 'sell']

    if len(strong_buy_clean_df) > 0:
        clean_win_rate = (strong_buy_clean_df['win'] == True).sum() / len(strong_buy_clean_df) * 100
        clean_avg_profit = strong_buy_clean_df['profit_100'].mean()
        clean_total_profit = strong_buy_clean_df['profit_100'].sum()
        print(f"\nstrong_buy_clean勝率: {clean_win_rate:.2f}%")
        print(f"strong_buy_clean平均利益: {clean_avg_profit:,.0f}円/100株")
        print(f"strong_buy_clean合計利益: {clean_total_profit:,.0f}円")

    if len(strong_buy_alert_df) > 0:
        alert_win_rate = (strong_buy_alert_df['win'] == True).sum() / len(strong_buy_alert_df) * 100
        alert_avg_profit = strong_buy_alert_df['profit_100'].mean()
        alert_total_profit = strong_buy_alert_df['profit_100'].sum()
        print(f"\nstrong_buy_alert勝率: {alert_win_rate:.2f}%")
        print(f"strong_buy_alert平均利益: {alert_avg_profit:,.0f}円/100株")
        print(f"strong_buy_alert合計利益: {alert_total_profit:,.0f}円")

    if len(buy_df) > 0:
        buy_win_rate = (buy_df['win'] == True).sum() / len(buy_df) * 100
        buy_avg_profit = buy_df['profit_100'].mean()
        buy_total_profit = buy_df['profit_100'].sum()
        print(f"\nbuy勝率: {buy_win_rate:.2f}%")
        print(f"buy平均利益: {buy_avg_profit:,.0f}円/100株")
        print(f"buy合計利益: {buy_total_profit:,.0f}円")

    if len(sell_df) > 0:
        sell_win_rate = (sell_df['win'] == True).sum() / len(sell_df) * 100
        sell_avg_profit = sell_df['profit_100'].mean()
        sell_total_profit = sell_df['profit_100'].sum()
        print(f"\nsell勝率: {sell_win_rate:.2f}%")
        print(f"sell平均利益: {sell_avg_profit:,.0f}円/100株")
        print(f"sell合計利益: {sell_total_profit:,.0f}円")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results_df.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n✅ 保存完了: {OUTPUT_FILE}")
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
