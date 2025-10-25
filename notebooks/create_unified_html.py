"""
統合HTMLレポート生成（write_html方式）
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
    {"ticker": "9348", "name_jp": "ispace", "name_en": "ispace", "mentioned_by": ["@kabuchenko"], "category": "Premium+IR+X+Policy", "policy_link": "High", "sentiment_score": 0.85},
    {"ticker": "3929", "name_jp": "Synspective", "name_en": "Synspective", "mentioned_by": ["@kaikai2120621"], "category": "Premium+Bio+Geo", "policy_link": "High", "sentiment_score": 0.82},
    {"ticker": "5595", "name_jp": "QPS研究所", "name_en": "QPS Kenkyujo", "mentioned_by": [], "category": "Theme+X+Policy", "policy_link": "Med", "sentiment_score": 0.70},
    {"ticker": "6237", "name_jp": "ウエスト", "name_en": "West", "mentioned_by": ["@kabu777b"], "category": "Premium+IR+X+Policy", "policy_link": "High", "sentiment_score": 0.88},
    {"ticker": "6264", "name_jp": "イワキ", "name_en": "Iwaki", "mentioned_by": [], "category": "IR+X+Policy", "policy_link": "Med", "sentiment_score": 0.78},
    {"ticker": "186A", "name_jp": "アストロスケールHD", "name_en": "Astroscale HD", "mentioned_by": ["@daykabu2021"], "category": "Premium+Theme+X", "policy_link": "High", "sentiment_score": 0.75},
    {"ticker": "2459", "name_jp": "アウンコンサルティング", "name_en": "Aun Consulting", "mentioned_by": ["@jestryoR"], "category": "Premium+IR+X+Policy", "policy_link": "Med", "sentiment_score": 0.80},
    {"ticker": "3079", "name_jp": "ディーブイエックス", "name_en": "DVx", "mentioned_by": [], "category": "IR+X+Geo", "policy_link": "Low", "sentiment_score": 0.72},
    {"ticker": "3664", "name_jp": "モブキャストHD", "name_en": "Mobcast HD", "mentioned_by": [], "category": "News+X+Policy", "policy_link": "Med", "sentiment_score": 0.70},
    {"ticker": "2158", "name_jp": "FRONTEO", "name_en": "FRONTEO", "mentioned_by": ["@tesuta001"], "category": "Premium+News+X", "policy_link": "Low", "sentiment_score": 0.76},
    {"ticker": "3769", "name_jp": "ランディックス", "name_en": "RANDIX", "mentioned_by": [], "category": "Earnings+X+Policy", "policy_link": "Med", "sentiment_score": 0.74},
    {"ticker": "4398", "name_jp": "情報戦略テクノロジー", "name_en": "IT Strategy", "mentioned_by": [], "category": "News+X+Policy", "policy_link": "Low", "sentiment_score": 0.68}
]

df_predictions = pd.DataFrame(grok_predictions)
df_predictions['has_mention'] = df_predictions['mentioned_by'].apply(lambda x: len(x) > 0)
df_predictions['mentioned_by_str'] = df_predictions['mentioned_by'].apply(lambda x: ', '.join(x) if x else 'None')

# 日次データ取得
target_date = "2025-10-24"
results = []

print("Fetching stock data...")
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
                'category': row['category']
            })
    except Exception as e:
        pass

df_results = pd.DataFrame(results)

# 5分足データ取得
print("Fetching 5-minute data...")
intraday_data = {}

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
                intraday_data[ticker_code] = {
                    'data': morning_session,
                    'name_en': row['name_en'],
                    'name_jp': row['name_jp']
                }
                print(f"  {ticker_code}: {len(morning_session)} bars")
    except Exception as e:
        pass

# サマリー統計
df_valid = df_results[df_results['change_pct'].notna()].copy()
total_stocks = len(df_valid)
win_count = (df_valid['change_pct'] > 0).sum()
win_rate = (win_count / total_stocks * 100) if total_stocks > 0 else 0
avg_change = df_valid['change_pct'].mean()
avg_range = df_valid['range_pct'].mean()
volatile_count = (df_valid['range_pct'] >= 2.0).sum()

print(f"\nWin Rate: {win_rate:.1f}%")
print(f"Avg Change: {avg_change:+.2f}%\n")

# 上位3と下位3
df_sorted_by_change = df_valid.sort_values('change_pct', ascending=False)
top_3 = df_sorted_by_change.head(3)['ticker'].tolist()
bottom_3 = df_sorted_by_change.tail(3)['ticker'].tolist()
selected_tickers = top_3 + bottom_3

# subplotsで全てのグラフを1つのFigureに統合
print("Creating unified figure with all charts...")

# 合計: 2 (サマリーグラフ) + 6 (5分足) = 8 rows
n_rows = 8
fig = make_subplots(
    rows=n_rows,
    cols=1,
    row_heights=[600, 600, 450, 450, 450, 450, 450, 450],
    subplot_titles=(
        '📈 Daily Performance Ranking',
        '🎯 Sentiment Score vs Actual Performance',
        f'TOP 1: {top_3[0]} (+)',
        f'TOP 2: {top_3[1]} (+)',
        f'TOP 3: {top_3[2]} (+)',
        f'BOTTOM 1: {bottom_3[0]} (-)',
        f'BOTTOM 2: {bottom_3[1]} (-)',
        f'BOTTOM 3: {bottom_3[2]} (-)'
    ),
    vertical_spacing=0.05,
    specs=[[{"type": "bar"}], [{"type": "scatter"}],
           [{"type": "candlestick"}], [{"type": "candlestick"}], [{"type": "candlestick"}],
           [{"type": "candlestick"}], [{"type": "candlestick"}], [{"type": "candlestick"}]]
)

# Row 1: 変化率ランキング
df_sorted = df_valid.sort_values('change_pct', ascending=True)
df_sorted['display_name'] = df_sorted['name_en'] + ' (' + df_sorted['name_jp'] + ')'
colors = ['green' if x > 0 else 'red' for x in df_sorted['change_pct']]

fig.add_trace(
    go.Bar(
        y=df_sorted['display_name'],
        x=df_sorted['change_pct'],
        orientation='h',
        marker=dict(color=colors),
        text=df_sorted['change_pct'].apply(lambda x: f"{x:+.2f}%"),
        textposition='outside',
        showlegend=False,
        hovertemplate='<b>%{y}</b><br>Change: %{x:.2f}%<extra></extra>'
    ),
    row=1, col=1
)

# Row 2: センチメントスコア vs 実績
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
            hovertemplate='<b>%{text}</b><br>Sentiment: %{x}<br>Change: %{y:.2f}%<extra></extra>'
        ),
        row=2, col=1
    )

# Rows 3-8: 5分足チャート
current_row = 3
for ticker_code in selected_tickers:
    if ticker_code in intraday_data:
        data_5m = intraday_data[ticker_code]['data']
        name_en = intraday_data[ticker_code]['name_en']
        name_jp = intraday_data[ticker_code]['name_jp']
        daily_change = df_results[df_results['ticker'] == ticker_code]['change_pct'].values[0]

        time_labels = [t.strftime('%H:%M') for t in data_5m.index]

        fig.add_trace(
            go.Candlestick(
                x=time_labels,
                open=data_5m['Open'],
                high=data_5m['High'],
                low=data_5m['Low'],
                close=data_5m['Close'],
                name=f'{ticker_code} - {name_en}',
                showlegend=False
            ),
            row=current_row, col=1
        )

        # サブタイトルを更新
        fig.layout.annotations[current_row-1].text = f'{ticker_code} - {name_en} ({name_jp}) | Daily: {daily_change:+.2f}%'

        current_row += 1

# レイアウト調整
fig.update_xaxes(title_text="Change %", row=1, col=1)
fig.update_yaxes(title_text="Stock", row=1, col=1)

fig.update_xaxes(title_text="Sentiment Score", row=2, col=1)
fig.update_yaxes(title_text="Change %", row=2, col=1)

for i in range(3, 9):
    fig.update_xaxes(title_text="Time", type='category', row=i, col=1)
    fig.update_yaxes(title_text="Price (JPY)", row=i, col=1)
    fig.update_xaxes(rangeslider_visible=False, row=i, col=1)

# 0ラインを追加
fig.add_hline(y=0, line_dash="dash", line_color="black", line_width=1, row=1, col=1)
fig.add_hline(y=0, line_dash="dash", line_color="black", line_width=1, row=2, col=1)
fig.add_vline(x=0, line_dash="dash", line_color="black", line_width=1, row=1, col=1)

fig.update_layout(
    height=3800,  # 全体の高さ
    title_text=f"🚀 Grok Backtest Analysis - 2025-10-24<br><sub>Win Rate: {win_rate:.1f}% | Avg Change: {avg_change:+.2f}% | Total: {total_stocks} stocks</sub>",
    showlegend=True,
    template='plotly_white'
)

# HTML保存
output_file = 'notebooks/grok_backtest_complete.html'
fig.write_html(output_file, include_plotlyjs='cdn')

print(f"✅ Complete report saved: {output_file}")
