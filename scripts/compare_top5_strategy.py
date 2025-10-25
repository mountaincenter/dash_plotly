#!/usr/bin/env python3
"""
選定時点スコアリングによるTop5銘柄戦略の比較
Compare Top 5 (scored at selection) vs Top 10 (sentiment only) strategy
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 最新のバックテスト結果を読み込み
backtest_dir = ROOT / "data/parquet/backtest_results"
latest_result = sorted(backtest_dir.glob("*/summary.csv"))[-1].parent

print(f"Loading results from: {latest_result}")

# summary.csvを読み込み
df = pd.read_csv(latest_result / "summary.csv")

# パラメータ設定
INITIAL_CAPITAL = 1_000_000  # 初期資金 100万円

print(f"\n{'='*70}")
print(f"🎯 選定時点スコアリング：Top5戦略 vs Top10戦略")
print(f"{'='*70}\n")

def calculate_selection_score(row):
    """
    選定時点でのスコアリング（結果ではなく選定時の情報のみ使用）
    """
    score = row['sentiment_score'] * 100  # ベーススコア（0-100）

    # 政策リンク強度ボーナス
    policy_bonus = {
        'High': 30,
        'Med': 20,
        'Low': 10
    }
    score += policy_bonus.get(row['policy_link'], 0)

    # プレミアムユーザー言及ボーナス
    if row['has_mention']:
        score += 50

    return score

# 各銘柄にスコアを付与
df['selection_score'] = df.apply(calculate_selection_score, axis=1)

print("スコアリング方式:")
print("  - ベーススコア: sentiment_score × 100")
print("  - 政策リンク: High +30, Med +20, Low +10")
print("  - プレミアム言及: +50")
print()

# 戦略比較
strategies = {
    'Top 10 (Sentiment Only)': {'count': 10, 'sort_by': 'sentiment_score'},
    'Top 5 (Selection Score)': {'count': 5, 'sort_by': 'selection_score'}
}

results_comparison = []

for strategy_name, config in strategies.items():
    daily_results = []

    for date in df['target_date'].unique():
        daily_data = df[df['target_date'] == date].copy()

        # 戦略に応じて銘柄選択
        top_stocks = daily_data.nlargest(config['count'], config['sort_by'])

        # 1銘柄あたり投資額
        capital_per_stock = INITIAL_CAPITAL / config['count']

        # デイリー戦略
        daily_profit = (top_stocks['daily_change_pct'].fillna(0) / 100 * capital_per_stock).sum()
        daily_return_pct = (daily_profit / INITIAL_CAPITAL) * 100

        # 前場戦略
        morning_profit = (top_stocks['morning_change_pct'].fillna(0) / 100 * capital_per_stock).sum()
        morning_return_pct = (morning_profit / INITIAL_CAPITAL) * 100

        daily_results.append({
            'date': date,
            'daily_profit': daily_profit,
            'daily_return_pct': daily_return_pct,
            'morning_profit': morning_profit,
            'morning_return_pct': morning_return_pct,
            'stocks_traded': len(top_stocks),
            'daily_wins': (top_stocks['daily_change_pct'] > 0).sum(),
            'morning_wins': (top_stocks['morning_change_pct'] > 0).sum(),
            'avg_selection_score': top_stocks['selection_score'].mean()
        })

    df_strategy = pd.DataFrame(daily_results)

    # 累積利益
    df_strategy['cumulative_daily_profit'] = df_strategy['daily_profit'].cumsum()
    df_strategy['cumulative_morning_profit'] = df_strategy['morning_profit'].cumsum()

    # サマリー
    total_daily_profit = df_strategy['daily_profit'].sum()
    total_morning_profit = df_strategy['morning_profit'].sum()

    total_daily_return = (total_daily_profit / INITIAL_CAPITAL) * 100
    total_morning_return = (total_morning_profit / INITIAL_CAPITAL) * 100

    final_capital_daily = INITIAL_CAPITAL + total_daily_profit
    final_capital_morning = INITIAL_CAPITAL + total_morning_profit

    daily_win_days = (df_strategy['daily_profit'] > 0).sum()
    morning_win_days = (df_strategy['morning_profit'] > 0).sum()
    total_days = len(df_strategy)

    results_comparison.append({
        'strategy': strategy_name,
        'stocks_per_day': config['count'],
        'total_daily_profit': total_daily_profit,
        'total_morning_profit': total_morning_profit,
        'total_daily_return': total_daily_return,
        'total_morning_return': total_morning_return,
        'final_capital_daily': final_capital_daily,
        'final_capital_morning': final_capital_morning,
        'daily_win_days': daily_win_days,
        'morning_win_days': morning_win_days,
        'total_days': total_days,
        'df_daily': df_strategy
    })

# 結果表示
print(f"{'='*70}")
print(f"📊 戦略比較結果（5営業日）")
print(f"{'='*70}\n")

for result in results_comparison:
    print(f"【{result['strategy']}】")
    print(f"  投資銘柄数: {result['stocks_per_day']}銘柄/日")
    print(f"  1銘柄投資額: ¥{INITIAL_CAPITAL / result['stocks_per_day']:,.0f}\n")

    print(f"  デイリー戦略（9:00寄付 → 15:00大引け）:")
    print(f"    総利益: ¥{result['total_daily_profit']:,.0f}")
    print(f"    リターン: {result['total_daily_return']:+.2f}%")
    print(f"    最終資産: ¥{result['final_capital_daily']:,.0f}")
    print(f"    勝ち日: {result['daily_win_days']}/{result['total_days']}日\n")

    print(f"  前場戦略（9:00寄付 → 11:30前引け）:")
    print(f"    総利益: ¥{result['total_morning_profit']:,.0f}")
    print(f"    リターン: {result['total_morning_return']:+.2f}%")
    print(f"    最終資産: ¥{result['final_capital_morning']:,.0f}")
    print(f"    勝ち日: {result['morning_win_days']}/{result['total_days']}日\n")

    print(f"{'-'*70}\n")

# 戦略間の差分
top10_result = results_comparison[0]
top5_result = results_comparison[1]

daily_diff = top5_result['total_daily_profit'] - top10_result['total_daily_profit']
morning_diff = top5_result['total_morning_profit'] - top10_result['total_morning_profit']

print(f"{'='*70}")
print(f"🎯 Top5戦略 vs Top10戦略 比較")
print(f"{'='*70}\n")

print(f"デイリー戦略の差分:")
print(f"  利益差: ¥{daily_diff:+,.0f}")
print(f"  リターン差: {top5_result['total_daily_return'] - top10_result['total_daily_return']:+.2f}%")
if daily_diff > 0:
    print(f"  ✅ Top5の方が ¥{daily_diff:,.0f} 有利\n")
else:
    print(f"  ❌ Top10の方が ¥{-daily_diff:,.0f} 有利\n")

print(f"前場戦略の差分:")
print(f"  利益差: ¥{morning_diff:+,.0f}")
print(f"  リターン差: {top5_result['total_morning_return'] - top10_result['total_morning_return']:+.2f}%")
if morning_diff > 0:
    print(f"  ✅ Top5の方が ¥{morning_diff:,.0f} 有利\n")
else:
    print(f"  ❌ Top10の方が ¥{-morning_diff:,.0f} 有利\n")

print(f"{'='*70}\n")

# 推奨戦略
best_strategy = "Top 5 (Selection Score)" if morning_diff > 0 else "Top 10 (Sentiment Only)"
best_profit = max(top5_result['total_morning_profit'], top10_result['total_morning_profit'])

print(f"💡 推奨戦略: {best_strategy}")
print(f"   前場戦略で ¥{best_profit:,.0f} (+{(best_profit/INITIAL_CAPITAL)*100:.2f}%) の利益\n")

print(f"{'='*70}\n")

# 日別詳細比較
print(f"📅 日別損益比較（前場戦略）")
print(f"{'='*70}")
print(f"{'日付':12} | {'Top10損益':>12} | {'Top5損益':>12} | {'差額':>12}")
print(f"{'-'*70}")

df_top10 = top10_result['df_daily']
df_top5 = top5_result['df_daily']

for i in range(len(df_top10)):
    date = df_top10.iloc[i]['date']
    profit_10 = df_top10.iloc[i]['morning_profit']
    profit_5 = df_top5.iloc[i]['morning_profit']
    diff = profit_5 - profit_10
    marker = "🎯" if diff > 0 else "  "
    print(f"{date:12} | ¥{profit_10:>11,.0f} | ¥{profit_5:>11,.0f} | ¥{diff:>11,.0f} {marker}")

print(f"{'='*70}")
print(f"{'合計':12} | ¥{top10_result['total_morning_profit']:>11,.0f} | ¥{top5_result['total_morning_profit']:>11,.0f} | ¥{morning_diff:>11,.0f}")
print(f"{'='*70}\n")

# HTML可視化
fig = make_subplots(
    rows=4,
    cols=1,
    row_heights=[600, 600, 600, 600],
    subplot_titles=(
        '💰 Cumulative Profit: Top 10 vs Top 5 (Morning Session)',
        '📊 Daily Profit Comparison',
        '📈 Daily Return % Comparison',
        '🎯 Selection Score Distribution (Top 5 Strategy)'
    ),
    vertical_spacing=0.06
)

# Row 1: 累積利益（前場戦略）
fig.add_trace(
    go.Scatter(
        x=df_top10['date'],
        y=df_top10['cumulative_morning_profit'],
        mode='lines+markers',
        name='Top 10 (Sentiment)',
        line=dict(color='lightblue', width=3),
        marker=dict(size=10)
    ),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(
        x=df_top5['date'],
        y=df_top5['cumulative_morning_profit'],
        mode='lines+markers',
        name='Top 5 (Score)',
        line=dict(color='orange', width=3),
        marker=dict(size=10)
    ),
    row=1, col=1
)

# Row 2: 日別利益（前場戦略）
fig.add_trace(
    go.Bar(
        x=df_top10['date'],
        y=df_top10['morning_profit'],
        name='Top 10',
        marker=dict(color='lightblue'),
        text=df_top10['morning_profit'].apply(lambda x: f'¥{x:,.0f}'),
        textposition='outside'
    ),
    row=2, col=1
)

fig.add_trace(
    go.Bar(
        x=df_top5['date'],
        y=df_top5['morning_profit'],
        name='Top 5',
        marker=dict(color='orange'),
        text=df_top5['morning_profit'].apply(lambda x: f'¥{x:,.0f}'),
        textposition='outside'
    ),
    row=2, col=1
)

# Row 3: リターン%（前場戦略）
fig.add_trace(
    go.Bar(
        x=df_top10['date'],
        y=df_top10['morning_return_pct'],
        name='Top 10 %',
        marker=dict(color='lightcoral'),
        text=df_top10['morning_return_pct'].apply(lambda x: f'{x:+.2f}%'),
        textposition='outside',
        showlegend=False
    ),
    row=3, col=1
)

fig.add_trace(
    go.Bar(
        x=df_top5['date'],
        y=df_top5['morning_return_pct'],
        name='Top 5 %',
        marker=dict(color='lightgreen'),
        text=df_top5['morning_return_pct'].apply(lambda x: f'{x:+.2f}%'),
        textposition='outside',
        showlegend=False
    ),
    row=3, col=1
)

# Row 4: Selection Score分布（Top5戦略の選定スコア平均）
fig.add_trace(
    go.Bar(
        x=df_top5['date'],
        y=df_top5['avg_selection_score'],
        name='Avg Score',
        marker=dict(color='purple'),
        text=df_top5['avg_selection_score'].apply(lambda x: f'{x:.1f}'),
        textposition='outside',
        showlegend=False
    ),
    row=4, col=1
)

# レイアウト
fig.update_yaxes(title_text="Cumulative Profit (¥)", row=1, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)

fig.update_yaxes(title_text="Daily Profit (¥)", row=2, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

fig.update_yaxes(title_text="Return %", row=3, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)

fig.update_yaxes(title_text="Selection Score", row=4, col=1)

fig.update_layout(
    height=2600,
    title_text=(
        f"🎯 Strategy Comparison: Top 5 (Selection Score) vs Top 10 (Sentiment Only)<br>"
        f"<sub>Morning Session Strategy | "
        f"Top 10: ¥{top10_result['final_capital_morning']:,.0f} ({top10_result['total_morning_return']:+.2f}%) | "
        f"Top 5: ¥{top5_result['final_capital_morning']:,.0f} ({top5_result['total_morning_return']:+.2f}%) | "
        f"Diff: ¥{morning_diff:+,.0f}</sub>"
    ),
    template='plotly_white',
    barmode='group'
)

# HTML保存
output_file = latest_result / "top5_vs_top10_comparison.html"
fig.write_html(output_file, include_plotlyjs='cdn')

print(f"✅ Strategy comparison HTML: {output_file}")

# CSV保存（Top5戦略の選定銘柄詳細）
top5_details = []

for date in df['target_date'].unique():
    daily_data = df[df['target_date'] == date].copy()
    top5_stocks = daily_data.nlargest(5, 'selection_score')

    for idx, row in top5_stocks.iterrows():
        top5_details.append({
            'date': date,
            'rank': list(range(1, 6))[list(top5_stocks.index).index(idx)],
            'ticker': row['ticker'],
            'company_name': row['company_name'],
            'selection_score': row['selection_score'],
            'sentiment_score': row['sentiment_score'],
            'policy_link': row['policy_link'],
            'has_mention': row['has_mention'],
            'mentioned_by': row['mentioned_by'],
            'daily_change_pct': row['daily_change_pct'],
            'morning_change_pct': row['morning_change_pct']
        })

df_top5_details = pd.DataFrame(top5_details)
csv_file = latest_result / "top5_selection_details.csv"
df_top5_details.to_csv(csv_file, index=False, encoding='utf-8-sig')

print(f"✅ Top 5 selection details CSV: {csv_file}")

# Top5戦略で選ばれた銘柄の統計
print(f"\n{'='*70}")
print(f"🎯 Top5戦略で選ばれた銘柄の統計")
print(f"{'='*70}\n")

top5_tickers = df_top5_details['ticker'].value_counts()
print(f"選定回数が多い銘柄（Top5に入った回数）:")
for ticker, count in top5_tickers.head(10).items():
    company_name = df[df['ticker'] == ticker]['company_name'].iloc[0]
    print(f"  {ticker:6} {company_name:20} - {count}回選定")

print(f"\n平均選定スコア: {df_top5_details['selection_score'].mean():.1f}")
print(f"最高選定スコア: {df_top5_details['selection_score'].max():.1f}")
print(f"最低選定スコア: {df_top5_details['selection_score'].min():.1f}")

print(f"\nプレミアム言及銘柄: {(df_top5_details['has_mention'] == True).sum()}/{len(df_top5_details)} ({(df_top5_details['has_mention'] == True).sum()/len(df_top5_details)*100:.1f}%)")

print(f"\n{'='*70}\n")
