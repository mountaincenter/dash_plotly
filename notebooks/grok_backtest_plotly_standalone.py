"""
Grok銘柄選定バックテスト分析（Plotly Standalone HTML版）
2025-10-23予想 → 2025-10-24実績（5分足含む）
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

print("Starting analysis...")

# 1. Grok予想データ（英語表記）
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

print(f"Loaded {len(df_predictions)} predictions")

# 2. 日次データ取得（2025-10-24）
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
                'category': row['category']
            })
            print(f"  {ticker_code}: {change_pct:+.2f}%")
    except Exception as e:
        print(f"  Error: {ticker_code} - {e}")

df_results = pd.DataFrame(results)

# 3. 5分足データ取得（前場 9:00-11:30）
print("\nFetching 5-minute data...")
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

# 4. パフォーマンスサマリー
df_valid = df_results[df_results['change_pct'].notna()].copy()

total_stocks = len(df_valid)
win_count = (df_valid['change_pct'] > 0).sum()
lose_count = (df_valid['change_pct'] <= 0).sum()
win_rate = (win_count / total_stocks * 100) if total_stocks > 0 else 0
avg_change = df_valid['change_pct'].mean()
avg_range = df_valid['range_pct'].mean()
volatile_count = (df_valid['range_pct'] >= 2.0).sum()

print("\n" + "="*60)
print(f"Win Rate: {win_rate:.1f}% ({win_count}/{total_stocks})")
print(f"Avg Change: {avg_change:+.2f}%")
print(f"Avg Range: {avg_range:.2f}%")
print("="*60)

# 5. HTMLファイル生成
print("\nGenerating HTML charts...")

html_parts = []
html_header = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Grok Backtest Results</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1, h2 {{ color: #333; }}
        .summary {{ background: white; padding: 20px; margin: 20px 0; border-radius: 5px; }}
        .chart {{ background: white; padding: 20px; margin: 20px 0; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>Grok銘柄選定バックテスト分析</h1>
    <h2>2025-10-23 予想 → 2025-10-24 実績</h2>

    <div class="summary">
        <h3>Performance Summary</h3>
        <p><strong>Total Stocks:</strong> {total_stocks}</p>
        <p><strong>Win Rate:</strong> {win_rate:.1f}% ({win_count}/{total_stocks})</p>
        <p><strong>Average Change:</strong> {avg_change:+.2f}%</p>
        <p><strong>Average Range:</strong> {avg_range:.2f}%</p>
        <p><strong>Volatile Stocks (Range ≥ 2%):</strong> {volatile_count}</p>
    </div>
"""
html_parts.append(html_header)

# Chart 1: 変化率ランキング
df_sorted = df_valid.sort_values('change_pct', ascending=True)
df_sorted['display_name'] = df_sorted['name_en'] + ' (' + df_sorted['name_jp'] + ')'
colors = ['green' if x > 0 else 'red' for x in df_sorted['change_pct']]

fig1 = go.Figure()
fig1.add_trace(go.Bar(
    y=df_sorted['display_name'],
    x=df_sorted['change_pct'],
    orientation='h',
    marker=dict(color=colors),
    text=df_sorted['change_pct'].apply(lambda x: f"{x:+.2f}%"),
    textposition='outside',
    hovertemplate='<b>%{y}</b><br>Change: %{x:.2f}%<extra></extra>'
))
fig1.update_layout(
    title='Stock Performance Ranking',
    xaxis_title='Change % (Close - Open)',
    yaxis_title='Stock',
    height=600,
    showlegend=False,
    template='plotly_white'
)
fig1.add_vline(x=0, line_dash="dash", line_color="black", line_width=1)

html_parts.append('<div class="chart"><div id="chart1"></div></div>')
html_parts.append(f'<script>var data1 = {fig1.to_json()}; Plotly.newPlot("chart1", data1.data, data1.layout);</script>')

# Chart 2: センチメントスコア vs 実績
fig2 = px.scatter(
    df_valid,
    x='sentiment_score',
    y='change_pct',
    color='policy_link',
    size='range_pct',
    hover_data=['name_en', 'name_jp'],
    text='name_en',
    color_discrete_map={'High': 'red', 'Med': 'orange', 'Low': 'blue'},
    title='Sentiment Score vs Actual Performance'
)
fig2.update_traces(textposition='top center', textfont_size=9)
fig2.add_hline(y=0, line_dash="dash", line_color="black", line_width=1)
fig2.update_layout(height=600, template='plotly_white')

html_parts.append('<div class="chart"><div id="chart2"></div></div>')
html_parts.append(f'<script>var data2 = {fig2.to_json()}; Plotly.newPlot("chart2", data2.data, data2.layout);</script>')

# Chart 3-8: 5分足チャート（上位3+下位3）
df_sorted_by_change = df_valid.sort_values('change_pct', ascending=False)
top_3 = df_sorted_by_change.head(3)['ticker'].tolist()
bottom_3 = df_sorted_by_change.tail(3)['ticker'].tolist()
selected_tickers = top_3 + bottom_3

chart_num = 3
for ticker_code in selected_tickers:
    if ticker_code in intraday_data:
        data_5m = intraday_data[ticker_code]['data']
        name_en = intraday_data[ticker_code]['name_en']
        name_jp = intraday_data[ticker_code]['name_jp']
        daily_change = df_results[df_results['ticker'] == ticker_code]['change_pct'].values[0]

        # 時刻を文字列に変換（時:分のみ）
        time_labels = [t.strftime('%H:%M') for t in data_5m.index]

        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=time_labels,
            open=data_5m['Open'],
            high=data_5m['High'],
            low=data_5m['Low'],
            close=data_5m['Close'],
            name='Price'
        ))
        fig.update_layout(
            title=f'{ticker_code} - {name_en} ({name_jp}) | 2025-10-24 Morning Session | Daily Change: {daily_change:+.2f}%',
            xaxis_title='Time (09:00 - 11:30)',
            yaxis_title='Price (JPY)',
            height=400,
            template='plotly_white',
            xaxis_rangeslider_visible=False,
            xaxis=dict(type='category')
        )

        html_parts.append(f'<div class="chart"><div id="chart{chart_num}"></div></div>')
        html_parts.append(f'<script>var data{chart_num} = {fig.to_json()}; Plotly.newPlot("chart{chart_num}", data{chart_num}.data, data{chart_num}.layout);</script>')
        chart_num += 1

html_parts.append("""
</body>
</html>
""")

output_file = "notebooks/grok_backtest_report.html"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('\n'.join(html_parts))

print(f"\n✅ HTML report generated: {output_file}")
print("Opening in browser...")
