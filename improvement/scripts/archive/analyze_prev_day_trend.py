#!/usr/bin/env python3
"""
前日の騰落と売り成績の関係を分析
"""
from pathlib import Path
import pandas as pd

def calculate_result(row):
    """勝敗判定: 始値で信用売り、終値で買い戻し"""
    buy_price = row['buy_price']
    sell_price = row['sell_price']

    if buy_price > sell_price:
        return '勝'
    elif buy_price < sell_price:
        return '負'
    else:
        return '引分'

def calculate_profit(row):
    """100株あたりの利益: (始値 - 終値) × 100"""
    return (row['buy_price'] - row['sell_price']) * 100

def main():
    # パス設定
    base_dir = Path(__file__).parent.parent.parent
    data_file = base_dir / 'improvement' / 'data' / 'v2_1_0_comparison_results.parquet'

    # データ読み込み
    print(f"データ読み込み: {data_file}")
    df = pd.read_parquet(data_file)

    # 静観→売り の銘柄を抽出
    hold_to_sell = df[(df['v2_0_3_action'] == '静観') & (df['v2_1_0_action'] == '売り')].copy()

    if len(hold_to_sell) == 0:
        print("静観→売りの銘柄がありません")
        return

    # 勝敗と利益を計算
    hold_to_sell['result'] = hold_to_sell.apply(calculate_result, axis=1)
    hold_to_sell['profit_100'] = hold_to_sell.apply(calculate_profit, axis=1)

    # 前日の騰落を計算
    hold_to_sell['prev_day_change'] = hold_to_sell['prev_day_close'] - hold_to_sell['prev_2day_close']
    hold_to_sell['prev_day_trend'] = hold_to_sell['prev_day_change'].apply(
        lambda x: '上昇' if x > 0 else ('下落' if x < 0 else '変わらず')
    )

    print("\n" + "=" * 60)
    print("前日の騰落と売り成績の関係")
    print("=" * 60)

    # 前日騰落別の分析
    for trend in ['上昇', '下落', '変わらず']:
        trend_df = hold_to_sell[hold_to_sell['prev_day_trend'] == trend]

        if len(trend_df) == 0:
            continue

        total_count = len(trend_df)
        win_count = (trend_df['result'] == '勝').sum()
        lose_count = (trend_df['result'] == '負').sum()
        draw_count = (trend_df['result'] == '引分').sum()
        win_rate = (win_count / total_count) * 100
        avg_profit = trend_df['profit_100'].mean()
        total_profit = trend_df['profit_100'].sum()

        print(f"\n【前日{trend}】")
        print(f"  対象銘柄数: {total_count}")
        print(f"  勝率: {win_rate:.2f}% (勝: {win_count}, 負: {lose_count}, 引分: {draw_count})")
        print(f"  100株あたり平均利益: {avg_profit:,.0f}円")
        print(f"  合計利益: {total_profit:,.0f}円")

    # 全体サマリー
    print(f"\n" + "=" * 60)
    print("全体サマリー")
    print("=" * 60)

    total_count = len(hold_to_sell)
    win_count = (hold_to_sell['result'] == '勝').sum()
    lose_count = (hold_to_sell['result'] == '負').sum()
    draw_count = (hold_to_sell['result'] == '引分').sum()
    win_rate = (win_count / total_count) * 100
    avg_profit = hold_to_sell['profit_100'].mean()

    print(f"  対象銘柄数: {total_count}")
    print(f"  勝率: {win_rate:.2f}% (勝: {win_count}, 負: {lose_count}, 引分: {draw_count})")
    print(f"  100株あたり平均利益: {avg_profit:,.0f}円")

    # 詳細データ（前日上昇の銘柄）
    print(f"\n" + "=" * 60)
    print("前日上昇の銘柄詳細（勝敗順）")
    print("=" * 60)

    trend_up = hold_to_sell[hold_to_sell['prev_day_trend'] == '上昇'].sort_values('profit_100', ascending=False)
    for _, row in trend_up.iterrows():
        prev_change_pct = (row['prev_day_change'] / row['prev_2day_close']) * 100 if row['prev_2day_close'] > 0 else 0
        print(f"  {row['ticker']} {row['company_name']}: 前日+{prev_change_pct:.2f}% → {row['result']} (利益: {row['profit_100']:,.0f}円)")

    # 詳細データ（前日下落の銘柄）
    print(f"\n" + "=" * 60)
    print("前日下落の銘柄詳細（勝敗順）")
    print("=" * 60)

    trend_down = hold_to_sell[hold_to_sell['prev_day_trend'] == '下落'].sort_values('profit_100', ascending=False)
    for _, row in trend_down.iterrows():
        prev_change_pct = (row['prev_day_change'] / row['prev_2day_close']) * 100 if row['prev_2day_close'] > 0 else 0
        print(f"  {row['ticker']} {row['company_name']}: 前日{prev_change_pct:.2f}% → {row['result']} (利益: {row['profit_100']:,.0f}円)")

if __name__ == '__main__':
    main()
