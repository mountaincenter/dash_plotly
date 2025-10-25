#!/usr/bin/env python3
"""
実際の利益シミュレーション
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
STOCKS_PER_DAY = 10  # 1日に買う銘柄数（Grokが10-15選定するので10固定）
CAPITAL_PER_STOCK = INITIAL_CAPITAL / STOCKS_PER_DAY  # 1銘柄あたり10万円

print(f"\n{'='*70}")
print(f"💰 利益シミュレーション設定")
print(f"{'='*70}")
print(f"初期資金: ¥{INITIAL_CAPITAL:,}")
print(f"1日の投資銘柄数: {STOCKS_PER_DAY}銘柄")
print(f"1銘柄あたり投資額: ¥{CAPITAL_PER_STOCK:,}")
print(f"{'='*70}\n")

# 日別にグループ化して利益計算
daily_results = []

for date in df['target_date'].unique():
    daily_data = df[df['target_date'] == date].copy()

    # 上位10銘柄を選択（実際のトレードをシミュレート）
    daily_data_sorted = daily_data.sort_values('sentiment_score', ascending=False).head(STOCKS_PER_DAY)

    # デイリー戦略（全日保有）
    daily_profit = (daily_data_sorted['daily_change_pct'].fillna(0) / 100 * CAPITAL_PER_STOCK).sum()
    daily_return_pct = (daily_profit / INITIAL_CAPITAL) * 100

    # 前場戦略（11:30で全利確）
    morning_profit = (daily_data_sorted['morning_change_pct'].fillna(0) / 100 * CAPITAL_PER_STOCK).sum()
    morning_return_pct = (morning_profit / INITIAL_CAPITAL) * 100

    daily_results.append({
        'date': date,
        'daily_profit': daily_profit,
        'daily_return_pct': daily_return_pct,
        'morning_profit': morning_profit,
        'morning_return_pct': morning_return_pct,
        'stocks_traded': len(daily_data_sorted),
        'daily_wins': (daily_data_sorted['daily_change_pct'] > 0).sum(),
        'morning_wins': (daily_data_sorted['morning_change_pct'] > 0).sum()
    })

df_daily = pd.DataFrame(daily_results)

# 累積利益計算
df_daily['cumulative_daily_profit'] = df_daily['daily_profit'].cumsum()
df_daily['cumulative_morning_profit'] = df_daily['morning_profit'].cumsum()

# 最終結果サマリー
total_daily_profit = df_daily['daily_profit'].sum()
total_morning_profit = df_daily['morning_profit'].sum()

total_daily_return = (total_daily_profit / INITIAL_CAPITAL) * 100
total_morning_return = (total_morning_profit / INITIAL_CAPITAL) * 100

final_capital_daily = INITIAL_CAPITAL + total_daily_profit
final_capital_morning = INITIAL_CAPITAL + total_morning_profit

# 結果表示
print(f"\n{'='*70}")
print(f"📊 5営業日トレード結果")
print(f"{'='*70}\n")

print(f"【デイリー戦略】（9:00寄付 → 15:00大引け）")
print(f"  総利益: ¥{total_daily_profit:,.0f}")
print(f"  リターン: {total_daily_return:+.2f}%")
print(f"  最終資産: ¥{final_capital_daily:,.0f}")
print(f"  勝ち日: {(df_daily['daily_profit'] > 0).sum()}/{len(df_daily)}日\n")

print(f"【前場戦略】（9:00寄付 → 11:30前引け）")
print(f"  総利益: ¥{total_morning_profit:,.0f}")
print(f"  リターン: {total_morning_return:+.2f}%")
print(f"  最終資産: ¥{final_capital_morning:,.0f}")
print(f"  勝ち日: {(df_daily['morning_profit'] > 0).sum()}/{len(df_daily)}日\n")

profit_diff = total_morning_profit - total_daily_profit
print(f"🎯 【前場戦略の優位性】")
print(f"  利益差: ¥{profit_diff:+,.0f}")
print(f"  リターン差: {total_morning_return - total_daily_return:+.2f}%")

if profit_diff > 0:
    print(f"  ✅ 前場戦略の方が ¥{profit_diff:,.0f} 多く稼げた！")
else:
    print(f"  ❌ デイリー戦略の方が ¥{-profit_diff:,.0f} 多く稼げた")

print(f"\n{'='*70}\n")

# 日別詳細
print(f"📅 日別損益詳細")
print(f"{'='*70}")
print(f"{'日付':12} | {'デイリー損益':>12} | {'前場損益':>12} | {'差額':>12}")
print(f"{'-'*70}")
for idx, row in df_daily.iterrows():
    diff = row['morning_profit'] - row['daily_profit']
    marker = "🌅" if diff > 0 else "  "
    print(f"{row['date']:12} | ¥{row['daily_profit']:>11,.0f} | ¥{row['morning_profit']:>11,.0f} | ¥{diff:>11,.0f} {marker}")

print(f"{'='*70}")
print(f"{'合計':12} | ¥{total_daily_profit:>11,.0f} | ¥{total_morning_profit:>11,.0f} | ¥{profit_diff:>11,.0f}")
print(f"{'='*70}\n")

# プレミアムユーザー言及銘柄のみでの利益計算
print(f"\n{'='*70}")
print(f"👥 プレミアムユーザー言及銘柄のみの戦略")
print(f"{'='*70}\n")

mentioned_only_results = []

for date in df['target_date'].unique():
    daily_data = df[(df['target_date'] == date) & (df['has_mention'] == True)].copy()

    if len(daily_data) == 0:
        continue

    # 言及銘柄に均等配分
    capital_per_stock_mentioned = INITIAL_CAPITAL / len(daily_data)

    daily_profit_mentioned = (daily_data['daily_change_pct'].fillna(0) / 100 * capital_per_stock_mentioned).sum()
    morning_profit_mentioned = (daily_data['morning_change_pct'].fillna(0) / 100 * capital_per_stock_mentioned).sum()

    mentioned_only_results.append({
        'date': date,
        'daily_profit': daily_profit_mentioned,
        'morning_profit': morning_profit_mentioned,
        'stocks_count': len(daily_data)
    })

if mentioned_only_results:
    df_mentioned = pd.DataFrame(mentioned_only_results)

    total_daily_profit_mentioned = df_mentioned['daily_profit'].sum()
    total_morning_profit_mentioned = df_mentioned['morning_profit'].sum()

    total_daily_return_mentioned = (total_daily_profit_mentioned / INITIAL_CAPITAL) * 100
    total_morning_return_mentioned = (total_morning_profit_mentioned / INITIAL_CAPITAL) * 100

    print(f"デイリー戦略（言及のみ）:")
    print(f"  総利益: ¥{total_daily_profit_mentioned:,.0f}")
    print(f"  リターン: {total_daily_return_mentioned:+.2f}%\n")

    print(f"前場戦略（言及のみ）:")
    print(f"  総利益: ¥{total_morning_profit_mentioned:,.0f}")
    print(f"  リターン: {total_morning_return_mentioned:+.2f}%\n")

    print(f"通常戦略との比較:")
    print(f"  デイリー差: ¥{total_daily_profit_mentioned - total_daily_profit:+,.0f}")
    print(f"  前場差: ¥{total_morning_profit_mentioned - total_morning_profit:+,.0f}")

print(f"\n{'='*70}\n")

# HTML可視化
fig = make_subplots(
    rows=3,
    cols=1,
    row_heights=[600, 600, 600],
    subplot_titles=(
        f'💰 Cumulative Profit: Daily vs Morning (Initial: ¥{INITIAL_CAPITAL:,})',
        '📊 Daily Profit Comparison',
        '📈 Daily Return % Comparison'
    ),
    vertical_spacing=0.08
)

# Row 1: 累積利益
fig.add_trace(
    go.Scatter(
        x=df_daily['date'],
        y=df_daily['cumulative_daily_profit'],
        mode='lines+markers',
        name='Daily Strategy',
        line=dict(color='lightblue', width=3),
        marker=dict(size=10)
    ),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(
        x=df_daily['date'],
        y=df_daily['cumulative_morning_profit'],
        mode='lines+markers',
        name='Morning Strategy',
        line=dict(color='orange', width=3),
        marker=dict(size=10)
    ),
    row=1, col=1
)

# Row 2: 日別利益
fig.add_trace(
    go.Bar(
        x=df_daily['date'],
        y=df_daily['daily_profit'],
        name='Daily',
        marker=dict(color='lightblue'),
        text=df_daily['daily_profit'].apply(lambda x: f'¥{x:,.0f}'),
        textposition='outside'
    ),
    row=2, col=1
)

fig.add_trace(
    go.Bar(
        x=df_daily['date'],
        y=df_daily['morning_profit'],
        name='Morning',
        marker=dict(color='orange'),
        text=df_daily['morning_profit'].apply(lambda x: f'¥{x:,.0f}'),
        textposition='outside'
    ),
    row=2, col=1
)

# Row 3: リターン%
fig.add_trace(
    go.Bar(
        x=df_daily['date'],
        y=df_daily['daily_return_pct'],
        name='Daily %',
        marker=dict(color='lightcoral'),
        text=df_daily['daily_return_pct'].apply(lambda x: f'{x:+.2f}%'),
        textposition='outside',
        showlegend=False
    ),
    row=3, col=1
)

fig.add_trace(
    go.Bar(
        x=df_daily['date'],
        y=df_daily['morning_return_pct'],
        name='Morning %',
        marker=dict(color='lightgreen'),
        text=df_daily['morning_return_pct'].apply(lambda x: f'{x:+.2f}%'),
        textposition='outside',
        showlegend=False
    ),
    row=3, col=1
)

# レイアウト
fig.update_yaxes(title_text="Cumulative Profit (¥)", row=1, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)

fig.update_yaxes(title_text="Daily Profit (¥)", row=2, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

fig.update_yaxes(title_text="Return %", row=3, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)

fig.update_layout(
    height=2000,
    title_text=(
        f"💰 Profit Simulation - 5 Trading Days<br>"
        f"<sub>Initial Capital: ¥{INITIAL_CAPITAL:,} | "
        f"Daily Final: ¥{final_capital_daily:,.0f} ({total_daily_return:+.2f}%) | "
        f"Morning Final: ¥{final_capital_morning:,.0f} ({total_morning_return:+.2f}%)</sub>"
    ),
    template='plotly_white',
    barmode='group'
)

# HTML保存
output_file = latest_result / "profit_simulation.html"
fig.write_html(output_file, include_plotlyjs='cdn')

print(f"✅ Profit simulation HTML: {output_file}")

# CSV保存
csv_file = latest_result / "daily_profit.csv"
df_daily.to_csv(csv_file, index=False, encoding='utf-8-sig')
print(f"✅ Daily profit CSV: {csv_file}")
