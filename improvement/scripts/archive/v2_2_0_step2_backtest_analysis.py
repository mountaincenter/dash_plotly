#!/usr/bin/env python3
"""
v2.2.0_step2スコアリングロジックの静的バックテスト分析
ベース: v2.1.1（RSI 7段階 + 出来高 7段階 + SMA5 6段階）
追加: 移動平均パーフェクトオーダー（SMA5/25/75）
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
OUTPUT_FILE = OUTPUT_DIR / 'v2_2_0_step2_backtest_results.parquet'

# 設定
MIN_DATA_POINTS = 75  # SMA75に必要な最低データ数

def load_target_stocks():
    """政策銘柄とCORE30のリストを取得"""
    stocks_file = DATA_DIR / 'all_stocks.parquet'
    df = pd.read_parquet(stocks_file)

    # TOPIX_CORE30 または 政策銘柄
    target_stocks = df[
        df['categories'].apply(lambda x: 'TOPIX_CORE30' in x or '政策銘柄' in x)
    ]['ticker'].tolist()

    return target_stocks

def calculate_technical_indicators(df):
    """テクニカル指標を計算"""
    df = df.sort_values('date').copy()

    # 前日終値
    df['prev_close'] = df['Close'].shift(1)

    # === v2.1.1の指標 ===

    # RSI (14日)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi_14d'] = 100 - (100 / (1 + rs))

    # 出来高変化率 (20日平均との比較)
    df['volume_avg_20d'] = df['Volume'].rolling(window=20).mean()
    df['volume_change_20d'] = df['Volume'] / df['volume_avg_20d']

    # SMA5との乖離率
    df['sma_5d'] = df['Close'].rolling(window=5).mean()
    df['price_vs_sma5_pct'] = ((df['Close'] - df['sma_5d']) / df['sma_5d']) * 100

    # === v2.2.0_step2: 移動平均パーフェクトオーダー追加 ===

    # SMA25, SMA75を追加
    df['sma_25d'] = df['Close'].rolling(window=25).mean()
    df['sma_75d'] = df['Close'].rolling(window=75).mean()

    return df

def calculate_v2_2_0_step2_score(row):
    """
    v2.2.0_step2スコアリングロジック
    v2.1.1 + 移動平均パーフェクトオーダー

    Returns:
        (score, action, reasons)
    """
    score = 0
    reasons = []

    # === v2.1.1: RSI（7段階細分化） ===
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

    # === v2.1.1: 出来高変化率（7段階細分化） ===
    volume_change = row.get('volume_change_20d')
    if pd.notna(volume_change):
        if volume_change < 0.5:
            score -= 10
            reasons.append(f'出来高{volume_change:.2f}倍（極端な低調）')
        elif volume_change < 0.8:
            score -= 5
            reasons.append(f'出来高{volume_change:.2f}倍（低調）')
        elif volume_change >= 3.0:
            score += 10
            reasons.append(f'出来高{volume_change:.1f}倍（過熱気味）')
        elif volume_change >= 2.0:
            score += 15
            reasons.append(f'出来高{volume_change:.1f}倍（急増）')
        elif volume_change >= 1.5:
            score += 10
            reasons.append(f'出来高{volume_change:.2f}倍（活発）')
        elif volume_change >= 1.2:
            score += 5
            reasons.append(f'出来高{volume_change:.2f}倍（やや活発）')

    # === v2.1.1: SMA5乖離率（6段階細分化） ===
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

    # === v2.2.0_step2: 移動平均パーフェクトオーダー追加 ===
    sma_5d = row.get('sma_5d')
    sma_25d = row.get('sma_25d')
    sma_75d = row.get('sma_75d')

    if pd.notna(sma_5d) and pd.notna(sma_25d) and pd.notna(sma_75d):
        # 完全なパーフェクトオーダー（買い）
        if sma_5d > sma_25d > sma_75d:
            score += 15
            reasons.append('パーフェクトオーダー（上昇トレンド）')
        # 完全なパーフェクトオーダー（売り）
        elif sma_5d < sma_25d < sma_75d:
            score -= 15
            reasons.append('逆パーフェクトオーダー（下降トレンド）')
        # 部分的な配置
        elif sma_5d > sma_25d and sma_25d > sma_75d:
            # 実質的に上と同じだが、念のため
            score += 15
            reasons.append('パーフェクトオーダー（上昇トレンド）')
        elif sma_5d < sma_25d and sma_25d < sma_75d:
            # 実質的に上と同じだが、念のため
            score -= 15
            reasons.append('逆パーフェクトオーダー（下降トレンド）')
        # 中期的な上昇トレンド（短期は調整中）
        elif sma_25d > sma_75d and sma_5d < sma_25d:
            score += 5
            reasons.append('中期上昇トレンド（短期調整中）')
        # 中期的な下降トレンド（短期は反発中）
        elif sma_25d < sma_75d and sma_5d > sma_25d:
            score -= 5
            reasons.append('中期下降トレンド（短期反発中）')

    # === アクション判定（v2.1.1と同じ閾値） ===
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

    # テクニカル指標計算
    ticker_df = calculate_technical_indicators(ticker_df)

    # 各日の判定
    results = []
    for i in range(len(ticker_df) - 1):  # 翌日が必要なので-1
        row = ticker_df.iloc[i]
        next_row = ticker_df.iloc[i + 1]

        # スコア計算
        score, action, reasons = calculate_v2_2_0_step2_score(row)

        # 翌日の結果（翌日始値でエントリー、翌日終値で決済）
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
            'volume_change_20d': row.get('volume_change_20d'),
            'price_vs_sma5_pct': row.get('price_vs_sma5_pct'),
            'sma_5d': row.get('sma_5d'),
            'sma_25d': row.get('sma_25d'),
            'sma_75d': row.get('sma_75d'),
            'profit_100': profit,
            'win': win,
            'reasons': ' / '.join(reasons) if reasons else ''
        })

    return results

def main():
    print("=" * 60)
    print("v2.2.0_step2スコアリングロジック 静的バックテスト分析")
    print("v2.1.1 + 移動平均パーフェクトオーダー（SMA5/25/75）")
    print("=" * 60)

    # [STEP 1] 対象銘柄リスト取得
    print("\n[STEP 1] 対象銘柄リスト取得...")
    target_stocks = load_target_stocks()
    print(f"  対象銘柄数: {len(target_stocks)}")

    # [STEP 2] 株価データ読み込み
    print("\n[STEP 2] 株価データ読み込み...")
    prices_file = DATA_DIR / 'prices_max_1d.parquet'
    prices_df = pd.read_parquet(prices_file)
    prices_df['date'] = pd.to_datetime(prices_df['date'])

    # 2020年以降のデータのみ
    prices_df = prices_df[prices_df['date'] >= '2020-01-01']

    # 対象銘柄のみ
    prices_df = prices_df[prices_df['ticker'].isin(target_stocks)]
    print(f"  読み込みレコード数: {len(prices_df):,}")
    print(f"  日付範囲: {prices_df['date'].min()} ~ {prices_df['date'].max()}")

    # [STEP 3] バックテスト実行
    print("\n[STEP 3] バックテスト実行中...")
    all_results = []

    for i, ticker in enumerate(target_stocks, 1):
        print(f"  [{i}/{len(target_stocks)}] {ticker} 分析中...")
        ticker_results = backtest_single_ticker(ticker, prices_df)
        if ticker_results:
            all_results.extend(ticker_results)

    # [STEP 4] 結果集計
    print("\n[STEP 4] 結果集計中...")
    results_df = pd.DataFrame(all_results)

    if len(results_df) == 0:
        print("  ✗ 分析可能なデータがありません")
        return 1

    print(f"  総判定数: {len(results_df):,}件")
    print(f"  買い: {(results_df['action'] == '買い').sum():,}件")
    print(f"  売り: {(results_df['action'] == '売り').sum():,}件")
    print(f"  静観: {(results_df['action'] == '静観').sum():,}件")

    # 勝率計算
    buy_df = results_df[results_df['action'] == '買い']
    sell_df = results_df[results_df['action'] == '売り']

    if len(buy_df) > 0:
        buy_win_rate = (buy_df['win'] == True).sum() / len(buy_df) * 100
        buy_avg_profit = buy_df['profit_100'].mean()
        buy_total_profit = buy_df['profit_100'].sum()
        print(f"\n  買い勝率: {buy_win_rate:.2f}%")
        print(f"  買い平均利益: {buy_avg_profit:,.0f}円/100株")
        print(f"  買い合計利益: {buy_total_profit:,.0f}円")

    if len(sell_df) > 0:
        sell_win_rate = (sell_df['win'] == True).sum() / len(sell_df) * 100
        sell_avg_profit = sell_df['profit_100'].mean()
        sell_total_profit = sell_df['profit_100'].sum()
        print(f"\n  売り勝率: {sell_win_rate:.2f}%")
        print(f"  売り平均利益: {sell_avg_profit:,.0f}円/100株")
        print(f"  売り合計利益: {sell_total_profit:,.0f}円")

    # [STEP 5] データ保存
    print("\n[STEP 5] データ保存...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results_df.to_parquet(OUTPUT_FILE, index=False)
    print(f"  保存完了: {OUTPUT_FILE}")

    print("\n✅ バックテスト完了")
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
