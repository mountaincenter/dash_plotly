"""
統合Plotly HTMLレポート生成
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
            print(f"  {ticker_code}: {change_pct:+.2f}%")
    except Exception as e:
        pass

df_results = pd.DataFrame(results)

# 5分足データ取得
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

# サマリー統計
df_valid = df_results[df_results['change_pct'].notna()].copy()
total_stocks = len(df_valid)
win_count = (df_valid['change_pct'] > 0).sum()
win_rate = (win_count / total_stocks * 100) if total_stocks > 0 else 0
avg_change = df_valid['change_pct'].mean()
avg_range = df_valid['range_pct'].mean()
volatile_count = (df_valid['range_pct'] >= 2.0).sum()

print("\n" + "="*60)
print(f"Win Rate: {win_rate:.1f}% ({win_count}/{total_stocks})")
print(f"Avg Change: {avg_change:+.2f}%")
print("="*60)

# 上位3と下位3の銘柄
df_sorted_by_change = df_valid.sort_values('change_pct', ascending=False)
top_3 = df_sorted_by_change.head(3)['ticker'].tolist()
bottom_3 = df_sorted_by_change.tail(3)['ticker'].tolist()
selected_tickers = top_3 + bottom_3

print(f"\nGenerating unified report with {len(selected_tickers)} stocks...")

# HTMLレポート生成
with open('notebooks/grok_backtest_unified.html', 'w', encoding='utf-8') as f:
    # ヘッダー
    f.write(f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Grok Backtest Analysis - 2025-10-24</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 10px;
        }}
        h2 {{
            color: #666;
            text-align: center;
            font-weight: normal;
            margin-top: 0;
        }}
        .summary {{
            background: white;
            padding: 25px;
            margin: 20px 0;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary h3 {{
            margin-top: 0;
            color: #333;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .summary-item {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
        }}
        .summary-item .label {{
            font-size: 0.9em;
            color: #666;
            margin-bottom: 5px;
        }}
        .summary-item .value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #333;
        }}
        .chart-section {{
            background: white;
            padding: 25px;
            margin: 20px 0;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .chart-section h3 {{
            margin-top: 0;
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }}
        .performance-badge {{
            display: inline-block;
            padding: 5px 10px;
            border-radius: 3px;
            font-weight: bold;
            margin-left: 10px;
        }}
        .badge-top {{
            background: #28a745;
            color: white;
        }}
        .badge-bottom {{
            background: #dc3545;
            color: white;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Grok銘柄選定バックテスト分析</h1>
        <h2>2025-10-23 予想 → 2025-10-24 実績</h2>

        <div class="summary">
            <h3>📊 Performance Summary</h3>
            <div class="summary-grid">
                <div class="summary-item">
                    <div class="label">Total Stocks</div>
                    <div class="value">{total_stocks}</div>
                </div>
                <div class="summary-item">
                    <div class="label">Win Rate</div>
                    <div class="value" style="color: {'#28a745' if win_rate > 50 else '#dc3545'};">{win_rate:.1f}%</div>
                </div>
                <div class="summary-item">
                    <div class="label">Winners</div>
                    <div class="value" style="color: #28a745;">{win_count}</div>
                </div>
                <div class="summary-item">
                    <div class="label">Average Change</div>
                    <div class="value" style="color: {'#28a745' if avg_change > 0 else '#dc3545'};">{avg_change:+.2f}%</div>
                </div>
                <div class="summary-item">
                    <div class="label">Average Range</div>
                    <div class="value">{avg_range:.2f}%</div>
                </div>
                <div class="summary-item">
                    <div class="label">High Volatility</div>
                    <div class="value">{volatile_count}</div>
                </div>
            </div>
        </div>
""")

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
        yaxis_title='',
        height=600,
        showlegend=False,
        template='plotly_white'
    )
    fig1.add_vline(x=0, line_dash="dash", line_color="black", line_width=1)

    f.write('<div class="chart-section"><h3>📈 Daily Performance Ranking</h3>')
    f.write('<div id="chart1"></div></div>')

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

    f.write('<div class="chart-section"><h3>🎯 Sentiment Score vs Actual Performance</h3>')
    f.write('<div id="chart2"></div></div>')

    # 5分足チャート（上位3 + 下位3）
    f.write('<div class="chart-section"><h3>⏰ 5-Minute Charts - Morning Session (09:00-11:30)</h3>')

    chart_num = 3
    for i, ticker_code in enumerate(selected_tickers):
        if ticker_code in intraday_data:
            data_5m = intraday_data[ticker_code]['data']
            name_en = intraday_data[ticker_code]['name_en']
            name_jp = intraday_data[ticker_code]['name_jp']
            daily_change = df_results[df_results['ticker'] == ticker_code]['change_pct'].values[0]

            time_labels = [t.strftime('%H:%M') for t in data_5m.index]

            # トップ3かボトム3かのバッジ
            badge = ''
            if i < 3:
                badge = f'<span class="performance-badge badge-top">TOP {i+1}: {daily_change:+.2f}%</span>'
            else:
                badge = f'<span class="performance-badge badge-bottom">BOTTOM {i-2}: {daily_change:+.2f}%</span>'

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
                title=f'{ticker_code} - {name_en} ({name_jp})',
                xaxis_title='Time',
                yaxis_title='Price (JPY)',
                height=450,
                template='plotly_white',
                xaxis_rangeslider_visible=False,
                xaxis=dict(type='category')
            )

            f.write(f'<h4 style="margin: 30px 0 10px 0;">{ticker_code} - {name_en} ({name_jp}) {badge}</h4>')
            f.write(f'<div id="chart{chart_num}"></div>')
            chart_num += 1

    f.write('</div>')  # close chart-section

    # フッター
    f.write("""
    </div>
</body>
</html>
""")

    # JavaScriptでPlotlyグラフを描画
    f.write(f'\n<script>\n')
    f.write(f'var data1 = {fig1.to_json()};\n')
    f.write(f'Plotly.newPlot("chart1", data1.data, data1.layout);\n\n')

    f.write(f'var data2 = {fig2.to_json()};\n')
    f.write(f'Plotly.newPlot("chart2", data2.data, data2.layout);\n\n')

    chart_num = 3
    for ticker_code in selected_tickers:
        if ticker_code in intraday_data:
            data_5m = intraday_data[ticker_code]['data']
            name_en = intraday_data[ticker_code]['name_en']
            name_jp = intraday_data[ticker_code]['name_jp']
            daily_change = df_results[df_results['ticker'] == ticker_code]['change_pct'].values[0]

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
                title=f'{ticker_code} - {name_en} ({name_jp})',
                xaxis_title='Time',
                yaxis_title='Price (JPY)',
                height=450,
                template='plotly_white',
                xaxis_rangeslider_visible=False,
                xaxis=dict(type='category')
            )

            f.write(f'var data{chart_num} = {fig.to_json()};\n')
            f.write(f'Plotly.newPlot("chart{chart_num}", data{chart_num}.data, data{chart_num}.layout);\n\n')
            chart_num += 1

    f.write('</script>\n')

print("\n✅ Unified HTML report generated: notebooks/grok_backtest_unified.html")
