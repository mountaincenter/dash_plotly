#!/usr/bin/env python3
"""
v2.1.4のスコア分布分析
strong_buy/buy、strong_sell/sellの閾値を決定するため
"""
from pathlib import Path
import pandas as pd
import numpy as np

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'improvement' / 'data'
RESULTS_FILE = DATA_DIR / 'v2_1_4_backtest_results.parquet'

def main():
    print("=" * 80)
    print("v2.1.4スコア分布分析（strong_buy/sell閾値決定）")
    print("=" * 80)

    # データ読み込み
    df = pd.read_parquet(RESULTS_FILE)
    buy_df = df[df['action'] == '買い']
    sell_df = df[df['action'] == '売り']

    print(f"\n買いシグナル: {len(buy_df):,}件")
    print(f"売りシグナル: {len(sell_df):,}件")

    # === 買いスコア分布分析 ===
    print("\n" + "=" * 80)
    print("📊 買いスコア (score_buy) 範囲別分析")
    print("=" * 80)

    buy_score_ranges = [
        (25, 30, '25-30'),
        (30, 35, '30-35'),
        (35, 40, '35-40'),
        (40, 45, '40-45'),
        (45, 50, '45-50'),
        (50, 60, '50-60'),
        (60, 70, '60-70'),
        (70, 80, '70-80'),
        (80, 100, '80-100'),
        (100, 200, '100+')
    ]

    print(f"{'スコア範囲':<12} {'件数':>8} {'勝率':>8} {'平均利益':>12} {'合計利益':>15}")
    print("-" * 80)

    for min_val, max_val, label in buy_score_ranges:
        subset = buy_df[(buy_df['score_buy'] >= min_val) & (buy_df['score_buy'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['win'] == True).sum() / count * 100
            avg_profit = subset['profit_100'].mean()
            total_profit = subset['profit_100'].sum()
            print(f"{label:<12} {count:>8,} {win_rate:>7.2f}% {avg_profit:>11,.0f}円 {total_profit:>14,.0f}円")

    # === 売りスコア分布分析 ===
    print("\n" + "=" * 80)
    print("📊 売りスコア (score_sell) 範囲別分析")
    print("=" * 80)

    sell_score_ranges = [
        (-200, -100, '<-100'),
        (-100, -80, '-100~-80'),
        (-80, -70, '-80~-70'),
        (-70, -60, '-70~-60'),
        (-60, -50, '-60~-50'),
        (-50, -45, '-50~-45'),
        (-45, -40, '-45~-40'),
        (-40, -35, '-40~-35'),
        (-35, -30, '-35~-30'),
        (-30, -25, '-30~-25'),
        (-25, -20, '-25~-20'),
        (-20, -15, '-20~-15')
    ]

    print(f"{'スコア範囲':<12} {'件数':>8} {'勝率':>8} {'平均利益':>12} {'合計利益':>15}")
    print("-" * 80)

    for min_val, max_val, label in sell_score_ranges:
        subset = sell_df[(sell_df['score_sell'] >= min_val) & (sell_df['score_sell'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['win'] == True).sum() / count * 100
            avg_profit = subset['profit_100'].mean()
            total_profit = subset['profit_100'].sum()
            print(f"{label:<12} {count:>8,} {win_rate:>7.2f}% {avg_profit:>11,.0f}円 {total_profit:>14,.0f}円")

    # === 推奨閾値の提案 ===
    print("\n" + "=" * 80)
    print("💡 推奨閾値の提案")
    print("=" * 80)

    # 買いスコアの統計
    print("\n【買いシグナル】")
    for threshold in [30, 35, 40, 45, 50]:
        strong = buy_df[buy_df['score_buy'] >= threshold]
        normal = buy_df[(buy_df['score_buy'] >= 25) & (buy_df['score_buy'] < threshold)]

        if len(strong) > 0 and len(normal) > 0:
            strong_wr = (strong['win'] == True).sum() / len(strong) * 100
            normal_wr = (normal['win'] == True).sum() / len(normal) * 100
            strong_avg = strong['profit_100'].mean()
            normal_avg = normal['profit_100'].mean()

            print(f"\n閾値 >= {threshold}:")
            print(f"  strong_buy: {len(strong):,}件, 勝率{strong_wr:.2f}%, 平均{strong_avg:,.0f}円")
            print(f"  buy:        {len(normal):,}件, 勝率{normal_wr:.2f}%, 平均{normal_avg:,.0f}円")
            print(f"  差分:       勝率{strong_wr - normal_wr:+.2f}%, 平均{strong_avg - normal_avg:+,.0f}円")

    # 売りスコアの統計
    print("\n【売りシグナル】")
    for threshold in [-20, -25, -30, -35, -40]:
        strong = sell_df[sell_df['score_sell'] <= threshold]
        normal = sell_df[(sell_df['score_sell'] > threshold) & (sell_df['score_sell'] <= -15)]

        if len(strong) > 0 and len(normal) > 0:
            strong_wr = (strong['win'] == True).sum() / len(strong) * 100
            normal_wr = (normal['win'] == True).sum() / len(normal) * 100
            strong_avg = strong['profit_100'].mean()
            normal_avg = normal['profit_100'].mean()

            print(f"\n閾値 <= {threshold}:")
            print(f"  strong_sell: {len(strong):,}件, 勝率{strong_wr:.2f}%, 平均{strong_avg:,.0f}円")
            print(f"  sell:        {len(normal):,}件, 勝率{normal_wr:.2f}%, 平均{normal_avg:,.0f}円")
            print(f"  差分:        勝率{strong_wr - normal_wr:+.2f}%, 平均{strong_avg - normal_avg:+,.0f}円")

    print("\n✅ 分析完了")
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
