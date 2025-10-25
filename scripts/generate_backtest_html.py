#!/usr/bin/env python3
"""
バックテスト結果のHTML可視化レポート生成
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
print(f"Columns: {df.columns.tolist()}")

# データクリーニング
df_valid = df[df['daily_change_pct'].notna()].copy()
df_morning = df[df['morning_change_pct'].notna()].copy()

# サマリー統計
total_stocks = len(df_valid)
daily_win_rate = (df_valid['daily_change_pct'] > 0).sum() / total_stocks * 100
daily_avg = df_valid['daily_change_pct'].mean()

morning_win_rate = (df_morning['morning_change_pct'] > 0).sum() / len(df_morning) * 100
morning_avg = df_morning['morning_change_pct'].mean()

mentioned_stocks = df_valid[df_valid['has_mention'] == True]
not_mentioned = df_valid[df_valid['has_mention'] == False]

mentioned_win_rate = (mentioned_stocks['daily_change_pct'] > 0).sum() / len(mentioned_stocks) * 100 if len(mentioned_stocks) > 0 else 0
not_mentioned_win_rate = (not_mentioned['daily_change_pct'] > 0).sum() / len(not_mentioned) * 100 if len(not_mentioned) > 0 else 0

print("\n=== Summary ===")
print(f"Daily win rate: {daily_win_rate:.1f}%")
print(f"Morning win rate: {morning_win_rate:.1f}%")
print(f"Mentioned win rate: {mentioned_win_rate:.1f}%")
print(f"Not mentioned win rate: {not_mentioned_win_rate:.1f}%")

# Plotly Figure作成
fig = make_subplots(
    rows=6,
    cols=1,
    row_heights=[500, 500, 600, 600, 600, 500],
    subplot_titles=(
        '📊 Win Rate Comparison: Daily vs Morning Session',
        '💰 Average Return Comparison',
        '📈 Daily Performance by Date',
        '🌅 Morning Session Performance by Date',
        '👥 Premium User Mention Effect',
        '📉 Daily vs Morning Scatter Plot'
    ),
    vertical_spacing=0.04,
    specs=[[{"type": "bar"}], [{"type": "bar"}], [{"type": "box"}],
           [{"type": "box"}], [{"type": "bar"}], [{"type": "scatter"}]]
)

# Row 1: 勝率比較
fig.add_trace(
    go.Bar(
        x=['Daily (Full Day)', 'Morning Session'],
        y=[daily_win_rate, morning_win_rate],
        text=[f"{daily_win_rate:.1f}%", f"{morning_win_rate:.1f}%"],
        textposition='outside',
        marker=dict(color=['red' if daily_win_rate < 50 else 'green',
                           'red' if morning_win_rate < 50 else 'green']),
        showlegend=False
    ),
    row=1, col=1
)

# Row 2: 平均リターン比較
fig.add_trace(
    go.Bar(
        x=['Daily (Full Day)', 'Morning Session'],
        y=[daily_avg, morning_avg],
        text=[f"{daily_avg:+.2f}%", f"{morning_avg:+.2f}%"],
        textposition='outside',
        marker=dict(color=['red' if daily_avg < 0 else 'green',
                           'red' if morning_avg < 0 else 'green']),
        showlegend=False
    ),
    row=2, col=1
)

# Row 3: 日別デイリーパフォーマンス
for date in df_valid['target_date'].unique():
    subset = df_valid[df_valid['target_date'] == date]
    fig.add_trace(
        go.Box(
            y=subset['daily_change_pct'],
            name=date,
            showlegend=False
        ),
        row=3, col=1
    )

# Row 4: 日別前場パフォーマンス
for date in df_morning['target_date'].unique():
    subset = df_morning[df_morning['target_date'] == date]
    fig.add_trace(
        go.Box(
            y=subset['morning_change_pct'],
            name=date,
            showlegend=False
        ),
        row=4, col=1
    )

# Row 5: プレミアムユーザー言及効果
fig.add_trace(
    go.Bar(
        x=['With Mention', 'No Mention'],
        y=[mentioned_win_rate, not_mentioned_win_rate],
        text=[f"{mentioned_win_rate:.1f}%<br>({len(mentioned_stocks)} stocks)",
              f"{not_mentioned_win_rate:.1f}%<br>({len(not_mentioned)} stocks)"],
        textposition='outside',
        marker=dict(color=['green', 'orange']),
        showlegend=False
    ),
    row=5, col=1
)

# Row 6: Daily vs Morning散布図（両方のデータがある銘柄のみ）
df_both = df_morning[['ticker', 'morning_change_pct', 'has_mention', 'target_date']].copy()
df_both = df_both.merge(df_valid[['ticker', 'daily_change_pct', 'target_date']],
                        on=['ticker', 'target_date'],
                        how='inner',
                        suffixes=('_morning', '_daily'))

if len(df_both) > 0:
    fig.add_trace(
        go.Scatter(
            x=df_both['morning_change_pct'],
            y=df_both['daily_change_pct'],
            mode='markers',
            text=df_both['ticker'],
            marker=dict(
                size=8,
                color=df_both['has_mention'].map({True: 'red', False: 'blue'}),
            ),
            showlegend=False,
            hovertemplate='<b>%{text}</b><br>Morning: %{x:.2f}%<br>Daily: %{y:.2f}%<extra></extra>'
        ),
        row=6, col=1
    )

# レイアウト調整
fig.update_xaxes(title_text="Strategy", row=1, col=1)
fig.update_yaxes(title_text="Win Rate (%)", row=1, col=1)
fig.add_hline(y=50, line_dash="dash", line_color="gray", row=1, col=1)

fig.update_xaxes(title_text="Strategy", row=2, col=1)
fig.update_yaxes(title_text="Average Return (%)", row=2, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

fig.update_xaxes(title_text="Date", row=3, col=1)
fig.update_yaxes(title_text="Daily Change (%)", row=3, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)

fig.update_xaxes(title_text="Date", row=4, col=1)
fig.update_yaxes(title_text="Morning Change (%)", row=4, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=4, col=1)

fig.update_xaxes(title_text="Premium User Mention", row=5, col=1)
fig.update_yaxes(title_text="Win Rate (%)", row=5, col=1)
fig.add_hline(y=50, line_dash="dash", line_color="gray", row=5, col=1)

fig.update_xaxes(title_text="Morning Session Change (%)", row=6, col=1)
fig.update_yaxes(title_text="Daily Change (%)", row=6, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=6, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="gray", row=6, col=1)

fig.update_layout(
    height=3500,
    title_text=(
        f"🚀 Grok Backtest Results - 5 Trading Days<br>"
        f"<sub>Total: {total_stocks} stocks | "
        f"Daily Win Rate: {daily_win_rate:.1f}% | "
        f"Morning Win Rate: {morning_win_rate:.1f}% (+{morning_win_rate - daily_win_rate:.1f}%)</sub>"
    ),
    showlegend=False,
    template='plotly_white'
)

# HTML保存
output_file = latest_result / "backtest_report.html"
fig.write_html(output_file, include_plotlyjs='cdn')

print(f"\n✅ HTML report generated: {output_file}")
print(f"\nKey Findings:")
print(f"  🎯 Morning session win rate is {morning_win_rate - daily_win_rate:.1f}% HIGHER than daily")
print(f"  💰 Morning session avg return is {morning_avg - daily_avg:+.2f}% better than daily")
print(f"  👥 Premium user mentions improve win rate by {mentioned_win_rate - not_mentioned_win_rate:.1f}%")
