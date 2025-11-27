"""
完全版分析レポート：デイリー + 前場パフォーマンス比較
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
from datetime import datetime, time
import warnings
warnings.filterwarnings('ignore')

print("Loading data...")

# Grok予想データ
grok_predictions = [
    {"ticker": "9348", "name_jp": "ispace", "name_en": "ispace", "mentioned_by": ["@kabuchenko"], "categories": "Premium+IR+X+Policy", "policy_link": "High", "sentiment_score": 0.85},
    {"ticker": "3929", "name_jp": "Synspective", "name_en": "Synspective", "mentioned_by": ["@kaikai2120621"], "categories": "Premium+Bio+Geo", "policy_link": "High", "sentiment_score": 0.82},
    {"ticker": "5595", "name_jp": "QPS研究所", "name_en": "QPS Kenkyujo", "mentioned_by": [], "categories": "Theme+X+Policy", "policy_link": "Med", "sentiment_score": 0.70},
    {"ticker": "6237", "name_jp": "ウエスト", "name_en": "West", "mentioned_by": ["@kabu777b"], "categories": "Premium+IR+X+Policy", "policy_link": "High", "sentiment_score": 0.88},
    {"ticker": "6264", "name_jp": "イワキ", "name_en": "Iwaki", "mentioned_by": [], "categories": "IR+X+Policy", "policy_link": "Med", "sentiment_score": 0.78},
    {"ticker": "186A", "name_jp": "アストロスケールHD", "name_en": "Astroscale HD", "mentioned_by": ["@daykabu2021"], "categories": "Premium+Theme+X", "policy_link": "High", "sentiment_score": 0.75},
    {"ticker": "2459", "name_jp": "アウンコンサルティング", "name_en": "Aun Consulting", "mentioned_by": ["@jestryoR"], "categories": "Premium+IR+X+Policy", "policy_link": "Med", "sentiment_score": 0.80},
    {"ticker": "3079", "name_jp": "ディーブイエックス", "name_en": "DVx", "mentioned_by": [], "categories": "IR+X+Geo", "policy_link": "Low", "sentiment_score": 0.72},
    {"ticker": "3664", "name_jp": "モブキャストHD", "name_en": "Mobcast HD", "mentioned_by": [], "categories": "News+X+Policy", "policy_link": "Med", "sentiment_score": 0.70},
    {"ticker": "2158", "name_jp": "FRONTEO", "name_en": "FRONTEO", "mentioned_by": ["@tesuta001"], "categories": "Premium+News+X", "policy_link": "Low", "sentiment_score": 0.76},
    {"ticker": "3769", "name_jp": "ランディックス", "name_en": "RANDIX", "mentioned_by": [], "categories": "Earnings+X+Policy", "policy_link": "Med", "sentiment_score": 0.74},
    {"ticker": "4398", "name_jp": "情報戦略テクノロジー", "name_en": "IT Strategy", "mentioned_by": [], "categories": "News+X+Policy", "policy_link": "Low", "sentiment_score": 0.68}
]

df_predictions = pd.DataFrame(grok_predictions)
df_predictions['has_mention'] = df_predictions['mentioned_by'].apply(lambda x: len(x) > 0)
df_predictions['mentioned_by_str'] = df_predictions['mentioned_by'].apply(lambda x: ', '.join(x) if x else 'None')

# 日次データ取得
target_date = "2025-10-24"
results = []

print("Fetching daily data...")
for idx, row in df_predictions.iterrows():
    ticker_code = row['ticker']
    ticker_symbol = f"{ticker_code}.T"

    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(start=target_date, end="2025-10-25")

        if not hist.empty:
            data = hist.iloc[0]
            open_price = data['Open']
            close_price = data['Close']
            high_price = data['High']
            low_price = data['Low']
            volume = int(data['Volume'])

            change_pct = ((close_price - open_price) / open_price) * 100
            range_pct = ((high_price - low_price) / open_price) * 100

            results.append({
                'ticker': ticker_code,
                'name_en': row['name_en'],
                'name_jp': row['name_jp'],
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'change_pct': change_pct,
                'range_pct': range_pct,
                'result': 'Up' if change_pct > 0 else 'Down',
                'mentioned_by_str': row['mentioned_by_str'],
                'policy_link': row['policy_link'],
                'sentiment_score': row['sentiment_score'],
                'categories': row['categories']
            })
    except Exception as e:
        pass

df_results = pd.DataFrame(results)

# 5分足データ取得 + 前場パフォーマンス計算
print("Fetching 5-minute data and calculating morning session performance...")
intraday_data = {}
morning_performance = []

for idx, row in df_predictions.iterrows():
    ticker_code = row['ticker']
    ticker_symbol = f"{ticker_code}.T"

    try:
        ticker = yf.Ticker(ticker_symbol)
        hist_5m = ticker.history(period="5d", interval="5m")

        if not hist_5m.empty:
            hist_5m.index = hist_5m.index.tz_localize(None)
            target_day_data = hist_5m[hist_5m.index.date == pd.Timestamp(target_date).date()]

            morning_session = target_day_data[
                (target_day_data.index.time >= time(9, 0)) &
                (target_day_data.index.time <= time(11, 30))
            ]

            if not morning_session.empty:
                # 前場パフォーマンス計算（9:00始値 → 11:30終値）
                morning_open = morning_session.iloc[0]['Open']
                morning_close = morning_session.iloc[-1]['Close']
                morning_high = morning_session['High'].max()
                morning_low = morning_session['Low'].min()

                morning_change_pct = ((morning_close - morning_open) / morning_open) * 100
                morning_range_pct = ((morning_high - morning_low) / morning_open) * 100

                intraday_data[ticker_code] = {
                    'data': morning_session,
                    'name_en': row['name_en'],
                    'name_jp': row['name_jp'],
                    'morning_change_pct': morning_change_pct,
                    'morning_range_pct': morning_range_pct
                }

                morning_performance.append({
                    'ticker': ticker_code,
                    'name_en': row['name_en'],
                    'name_jp': row['name_jp'],
                    'morning_change_pct': morning_change_pct,
                    'morning_range_pct': morning_range_pct
                })

                print(f"  {ticker_code}: {len(morning_session)} bars, morning change: {morning_change_pct:+.2f}%")
    except Exception as e:
        pass

df_morning = pd.DataFrame(morning_performance)

# デイリーと前場のパフォーマンスをマージ
df_combined = df_results.merge(df_morning[['ticker', 'morning_change_pct', 'morning_range_pct']],
                                on='ticker', how='left')

# サマリー統計
df_valid = df_combined[df_combined['change_pct'].notna()].copy()
total_stocks = len(df_valid)
win_count_daily = (df_valid['change_pct'] > 0).sum()
win_rate_daily = (win_count_daily / total_stocks * 100) if total_stocks > 0 else 0
avg_change_daily = df_valid['change_pct'].mean()

df_valid_morning = df_valid[df_valid['morning_change_pct'].notna()].copy()
win_count_morning = (df_valid_morning['morning_change_pct'] > 0).sum()
win_rate_morning = (win_count_morning / len(df_valid_morning) * 100) if len(df_valid_morning) > 0 else 0
avg_change_morning = df_valid_morning['morning_change_pct'].mean()

print(f"\n{'='*60}")
print(f"Daily Performance: Win Rate {win_rate_daily:.1f}%, Avg Change {avg_change_daily:+.2f}%")
print(f"Morning Session:   Win Rate {win_rate_morning:.1f}%, Avg Change {avg_change_morning:+.2f}%")
print(f"{'='*60}\n")

# 上位3と下位3（デイリー基準）
df_sorted_by_change = df_valid.sort_values('change_pct', ascending=False)
top_3 = df_sorted_by_change.head(3)['ticker'].tolist()
bottom_3 = df_sorted_by_change.tail(3)['ticker'].tolist()
selected_tickers = top_3 + bottom_3

# Figureレイアウト:
# Row 1: Daily Performance Ranking
# Row 2: Morning Session Performance Ranking
# Row 3: Daily vs Morning Comparison Scatter
# Row 4: Sentiment Score vs Actual
# Rows 5-10: 5分足チャート × 6
print("Creating comprehensive figure...")

n_rows = 10
fig = make_subplots(
    rows=n_rows,
    cols=1,
    row_heights=[600, 600, 500, 500, 450, 450, 450, 450, 450, 450],
    subplot_titles=(
        '📊 Daily Performance Ranking (Open → Close)',
        '🌅 Morning Session Performance Ranking (09:00 → 11:30)',
        '📈 Daily vs Morning Session Comparison',
        '🎯 Sentiment Score vs Daily Performance',
        f'TOP 1: {top_3[0]}',
        f'TOP 2: {top_3[1]}',
        f'TOP 3: {top_3[2]}',
        f'BOTTOM 1: {bottom_3[0]}',
        f'BOTTOM 2: {bottom_3[1]}',
        f'BOTTOM 3: {bottom_3[2]}'
    ),
    vertical_spacing=0.04,
    specs=[[{"type": "bar"}], [{"type": "bar"}], [{"type": "scatter"}], [{"type": "scatter"}],
           [{"type": "candlestick"}], [{"type": "candlestick"}], [{"type": "candlestick"}],
           [{"type": "candlestick"}], [{"type": "candlestick"}], [{"type": "candlestick"}]]
)

# Row 1: Daily Performance Ranking
df_sorted = df_valid.sort_values('change_pct', ascending=True)
df_sorted['display_name'] = df_sorted['name_en'] + ' (' + df_sorted['name_jp'] + ')'
colors_daily = ['green' if x > 0 else 'red' for x in df_sorted['change_pct']]

fig.add_trace(
    go.Bar(
        y=df_sorted['display_name'],
        x=df_sorted['change_pct'],
        orientation='h',
        marker=dict(color=colors_daily),
        text=df_sorted['change_pct'].apply(lambda x: f"{x:+.2f}%"),
        textposition='outside',
        showlegend=False,
        name='Daily',
        hovertemplate='<b>%{y}</b><br>Daily Change: %{x:.2f}%<extra></extra>'
    ),
    row=1, col=1
)

# Row 2: Morning Session Performance Ranking
df_morning_sorted = df_valid_morning.sort_values('morning_change_pct', ascending=True)
df_morning_sorted['display_name'] = df_morning_sorted['name_en'] + ' (' + df_morning_sorted['name_jp'] + ')'
colors_morning = ['green' if x > 0 else 'red' for x in df_morning_sorted['morning_change_pct']]

fig.add_trace(
    go.Bar(
        y=df_morning_sorted['display_name'],
        x=df_morning_sorted['morning_change_pct'],
        orientation='h',
        marker=dict(color=colors_morning),
        text=df_morning_sorted['morning_change_pct'].apply(lambda x: f"{x:+.2f}%"),
        textposition='outside',
        showlegend=False,
        name='Morning',
        hovertemplate='<b>%{y}</b><br>Morning Change: %{x:.2f}%<extra></extra>'
    ),
    row=2, col=1
)

# Row 3: Daily vs Morning Comparison Scatter
df_valid_both = df_valid[df_valid['morning_change_pct'].notna()].copy()
fig.add_trace(
    go.Scatter(
        x=df_valid_both['morning_change_pct'],
        y=df_valid_both['change_pct'],
        mode='markers+text',
        text=df_valid_both['name_en'],
        textposition='top center',
        textfont=dict(size=9),
        marker=dict(
            size=10,
            color=df_valid_both['sentiment_score'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Sentiment", x=1.15, len=0.3, y=0.55)
        ),
        showlegend=False,
        hovertemplate='<b>%{text}</b><br>Morning: %{x:.2f}%<br>Daily: %{y:.2f}%<extra></extra>'
    ),
    row=3, col=1
)

# Row 4: Sentiment Score vs Daily Performance
for policy, color in [('High', 'red'), ('Med', 'orange'), ('Low', 'blue')]:
    subset = df_valid[df_valid['policy_link'] == policy]
    fig.add_trace(
        go.Scatter(
            x=subset['sentiment_score'],
            y=subset['change_pct'],
            mode='markers+text',
            name=f'Policy: {policy}',
            marker=dict(color=color, size=subset['range_pct']*2),
            text=subset['name_en'],
            textposition='top center',
            textfont=dict(size=9),
            hovertemplate='<b>%{text}</b><br>Sentiment: %{x}<br>Daily: %{y:.2f}%<extra></extra>'
        ),
        row=4, col=1
    )

# Rows 5-10: 5分足チャート
current_row = 5
for ticker_code in selected_tickers:
    if ticker_code in intraday_data:
        data_5m = intraday_data[ticker_code]['data']
        name_en = intraday_data[ticker_code]['name_en']
        name_jp = intraday_data[ticker_code]['name_jp']
        daily_change = df_results[df_results['ticker'] == ticker_code]['change_pct'].values[0]
        morning_change = intraday_data[ticker_code]['morning_change_pct']

        time_labels = [t.strftime('%H:%M') for t in data_5m.index]

        fig.add_trace(
            go.Candlestick(
                x=time_labels,
                open=data_5m['Open'],
                high=data_5m['High'],
                low=data_5m['Low'],
                close=data_5m['Close'],
                name=f'{ticker_code}',
                showlegend=False
            ),
            row=current_row, col=1
        )

        # サブタイトル更新
        fig.layout.annotations[current_row-1].text = (
            f'{ticker_code} - {name_en} ({name_jp})<br>'
            f'<sub>Morning: {morning_change:+.2f}% | Daily: {daily_change:+.2f}%</sub>'
        )

        current_row += 1

# レイアウト調整
fig.update_xaxes(title_text="Change %", row=1, col=1)
fig.update_yaxes(title_text="", row=1, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=1, row=1, col=1)

fig.update_xaxes(title_text="Change %", row=2, col=1)
fig.update_yaxes(title_text="", row=2, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=1, row=2, col=1)

fig.update_xaxes(title_text="Morning Session Change %", row=3, col=1)
fig.update_yaxes(title_text="Daily Change %", row=3, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1, row=3, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="gray", line_width=1, row=3, col=1)

fig.update_xaxes(title_text="Sentiment Score", row=4, col=1)
fig.update_yaxes(title_text="Daily Change %", row=4, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1, row=4, col=1)

for i in range(5, 11):
    fig.update_xaxes(title_text="Time (Morning Session)", type='category', row=i, col=1)
    fig.update_yaxes(title_text="Price (JPY)", row=i, col=1)
    fig.update_xaxes(rangeslider_visible=False, row=i, col=1)

fig.update_layout(
    height=4500,
    title_text=(
        f"🚀 Grok Backtest Complete Analysis - 2025-10-24<br>"
        f"<sub>Daily: Win Rate {win_rate_daily:.1f}%, Avg {avg_change_daily:+.2f}% | "
        f"Morning: Win Rate {win_rate_morning:.1f}%, Avg {avg_change_morning:+.2f}%</sub>"
    ),
    showlegend=True,
    template='plotly_white'
)

# HTML保存
output_file = 'notebooks/grok_backtest_full_analysis.html'
fig.write_html(output_file, include_plotlyjs='cdn')

print(f"\n✅ Complete analysis saved: {output_file}")
print("\nKey Insights:")
print(f"  - Stocks analyzed: {total_stocks}")
print(f"  - Daily win rate: {win_rate_daily:.1f}%")
print(f"  - Morning session win rate: {win_rate_morning:.1f}%")
print(f"  - Average daily change: {avg_change_daily:+.2f}%")
print(f"  - Average morning change: {avg_change_morning:+.2f}%")
