#!/usr/bin/env python3
"""
銘柄別詳細HTMLレポート生成
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

print(f"Total stocks: {len(df)}")

# 銘柄ごとに集計
stock_summary = []

for ticker in df['ticker'].unique():
    stock_data = df[df['ticker'] == ticker]

    # 基本情報
    company_name = stock_data['company_name'].iloc[0] if len(stock_data) > 0 else ticker
    has_mention = stock_data['has_mention'].iloc[0] if len(stock_data) > 0 else False
    mentioned_by = stock_data['mentioned_by'].iloc[0] if len(stock_data) > 0 else ''

    # デイリーパフォーマンス
    daily_data = stock_data[stock_data['daily_change_pct'].notna()]
    daily_count = len(daily_data)
    daily_wins = (daily_data['daily_change_pct'] > 0).sum() if len(daily_data) > 0 else 0
    daily_win_rate = (daily_wins / daily_count * 100) if daily_count > 0 else 0
    daily_avg = daily_data['daily_change_pct'].mean() if len(daily_data) > 0 else 0

    # 前場パフォーマンス
    morning_data = stock_data[stock_data['morning_change_pct'].notna()]
    morning_count = len(morning_data)
    morning_wins = (morning_data['morning_change_pct'] > 0).sum() if len(morning_data) > 0 else 0
    morning_win_rate = (morning_wins / morning_count * 100) if morning_count > 0 else 0
    morning_avg = morning_data['morning_change_pct'].mean() if len(morning_data) > 0 else 0

    stock_summary.append({
        'ticker': ticker,
        'company_name': company_name,
        'has_mention': has_mention,
        'mentioned_by': mentioned_by,
        'appearances': daily_count,
        'daily_win_rate': daily_win_rate,
        'daily_avg': daily_avg,
        'morning_win_rate': morning_win_rate,
        'morning_avg': morning_avg,
        'morning_better': morning_avg > daily_avg
    })

df_stocks = pd.DataFrame(stock_summary)
df_stocks['ticker'] = df_stocks['ticker'].astype(str)
df_stocks = df_stocks.sort_values('daily_avg', ascending=False)

print(f"\nUnique stocks: {len(df_stocks)}")

# HTMLレポート生成
fig = make_subplots(
    rows=4,
    cols=1,
    row_heights=[800, 800, 800, 800],
    subplot_titles=(
        '📊 All Stocks: Daily Performance Ranking',
        '🌅 All Stocks: Morning Session Performance Ranking',
        '🎯 Top 10 Stocks: Daily vs Morning Comparison',
        '❌ Bottom 10 Stocks: Daily vs Morning Comparison'
    ),
    vertical_spacing=0.05,
    specs=[[{"type": "bar"}], [{"type": "bar"}], [{"type": "bar"}], [{"type": "bar"}]]
)

# Row 1: デイリーパフォーマンスランキング
df_sorted_daily = df_stocks.sort_values('daily_avg', ascending=True)
colors_daily = ['green' if x > 0 else 'red' for x in df_sorted_daily['daily_avg']]

fig.add_trace(
    go.Bar(
        y=df_sorted_daily['ticker'] + ' - ' + df_sorted_daily['company_name'],
        x=df_sorted_daily['daily_avg'],
        orientation='h',
        marker=dict(color=colors_daily),
        text=df_sorted_daily['daily_avg'].apply(lambda x: f"{x:+.2f}%"),
        textposition='outside',
        showlegend=False,
        name='Daily',
        hovertemplate=(
            '<b>%{y}</b><br>' +
            'Avg Daily: %{x:.2f}%<br>' +
            'Appearances: ' + df_sorted_daily['appearances'].astype(str) + '<br>' +
            'Win Rate: ' + df_sorted_daily['daily_win_rate'].apply(lambda x: f'{x:.0f}%') +
            '<extra></extra>'
        )
    ),
    row=1, col=1
)

# Row 2: 前場パフォーマンスランキング
df_sorted_morning = df_stocks.sort_values('morning_avg', ascending=True)
colors_morning = ['green' if x > 0 else 'red' for x in df_sorted_morning['morning_avg']]

fig.add_trace(
    go.Bar(
        y=df_sorted_morning['ticker'] + ' - ' + df_sorted_morning['company_name'],
        x=df_sorted_morning['morning_avg'],
        orientation='h',
        marker=dict(color=colors_morning),
        text=df_sorted_morning['morning_avg'].apply(lambda x: f"{x:+.2f}%"),
        textposition='outside',
        showlegend=False,
        name='Morning',
        hovertemplate=(
            '<b>%{y}</b><br>' +
            'Avg Morning: %{x:.2f}%<br>' +
            'Appearances: ' + df_sorted_morning['appearances'].astype(str) + '<br>' +
            'Win Rate: ' + df_sorted_morning['morning_win_rate'].apply(lambda x: f'{x:.0f}%') +
            '<extra></extra>'
        )
    ),
    row=2, col=1
)

# Row 3: Top 10銘柄 Daily vs Morning
top_10 = df_stocks.nlargest(10, 'daily_avg')
x_labels = top_10['ticker'] + ' - ' + top_10['company_name']

fig.add_trace(
    go.Bar(
        x=x_labels,
        y=top_10['daily_avg'],
        name='Daily',
        marker=dict(color='lightblue'),
        text=top_10['daily_avg'].apply(lambda x: f"{x:+.1f}%"),
        textposition='outside'
    ),
    row=3, col=1
)

fig.add_trace(
    go.Bar(
        x=x_labels,
        y=top_10['morning_avg'],
        name='Morning',
        marker=dict(color='orange'),
        text=top_10['morning_avg'].apply(lambda x: f"{x:+.1f}%"),
        textposition='outside'
    ),
    row=3, col=1
)

# Row 4: Bottom 10銘柄 Daily vs Morning
bottom_10 = df_stocks.nsmallest(10, 'daily_avg')
x_labels_bottom = bottom_10['ticker'] + ' - ' + bottom_10['company_name']

fig.add_trace(
    go.Bar(
        x=x_labels_bottom,
        y=bottom_10['daily_avg'],
        name='Daily',
        marker=dict(color='lightcoral'),
        text=bottom_10['daily_avg'].apply(lambda x: f"{x:+.1f}%"),
        textposition='outside',
        showlegend=False
    ),
    row=4, col=1
)

fig.add_trace(
    go.Bar(
        x=x_labels_bottom,
        y=bottom_10['morning_avg'],
        name='Morning',
        marker=dict(color='lightyellow'),
        text=bottom_10['morning_avg'].apply(lambda x: f"{x:+.1f}%"),
        textposition='outside',
        showlegend=False
    ),
    row=4, col=1
)

# レイアウト調整
fig.update_xaxes(title_text="Avg Change %", row=1, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="black", row=1, col=1)

fig.update_xaxes(title_text="Avg Change %", row=2, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="black", row=2, col=1)

fig.update_yaxes(title_text="Avg Change %", row=3, col=1)
fig.update_xaxes(tickangle=-45, row=3, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)

fig.update_yaxes(title_text="Avg Change %", row=4, col=1)
fig.update_xaxes(tickangle=-45, row=4, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=4, col=1)

fig.update_layout(
    height=3500,
    title_text=(
        f"🎯 Stock-by-Stock Performance Analysis<br>"
        f"<sub>Total: {len(df_stocks)} unique stocks across 5 trading days</sub>"
    ),
    template='plotly_white',
    barmode='group'
)

# HTML保存
output_file = latest_result / "stock_detail_report.html"
fig.write_html(output_file, include_plotlyjs='cdn')

# CSVも保存
csv_file = latest_result / "stock_summary.csv"
df_stocks.to_csv(csv_file, index=False, encoding='utf-8-sig')

print(f"\n✅ Stock detail HTML report: {output_file}")
print(f"✅ Stock summary CSV: {csv_file}")

# Top 5とBottom 5を表示
print("\n" + "="*60)
print("TOP 5 PERFORMERS (Daily Avg)")
print("="*60)
for idx, row in df_stocks.head(5).iterrows():
    mention_str = f" [{row['mentioned_by']}]" if row['has_mention'] else ""
    morning_indicator = " 🌅" if row['morning_better'] else ""
    print(f"{row['ticker']:6} {row['company_name']:20} | Daily: {row['daily_avg']:+6.2f}% | Morning: {row['morning_avg']:+6.2f}%{mention_str}{morning_indicator}")

print("\n" + "="*60)
print("BOTTOM 5 PERFORMERS (Daily Avg)")
print("="*60)
for idx, row in df_stocks.tail(5).iterrows():
    mention_str = f" [{row['mentioned_by']}]" if row['has_mention'] else ""
    morning_indicator = " 🌅" if row['morning_better'] else ""
    print(f"{row['ticker']:6} {row['company_name']:20} | Daily: {row['daily_avg']:+6.2f}% | Morning: {row['morning_avg']:+6.2f}%{mention_str}{morning_indicator}")

print("\n🌅 = Morning session performed better than daily")
