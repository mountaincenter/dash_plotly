"""
Plotly HTMLレポート生成（個別ファイル方式）
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
                'categories': row['categories']
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
    except Exception as e:
        pass

# サマリー統計
df_valid = df_results[df_results['change_pct'].notna()].copy()
total_stocks = len(df_valid)
win_count = (df_valid['change_pct'] > 0).sum()
win_rate = (win_count / total_stocks * 100) if total_stocks > 0 else 0
avg_change = df_valid['change_pct'].mean()

print(f"\nWin Rate: {win_rate:.1f}% ({win_count}/{total_stocks})")
print(f"Avg Change: {avg_change:+.2f}%")

# 上位3と下位3の銘柄
df_sorted_by_change = df_valid.sort_values('change_pct', ascending=False)
top_3 = df_sorted_by_change.head(3)['ticker'].tolist()
bottom_3 = df_sorted_by_change.tail(3)['ticker'].tolist()
selected_tickers = top_3 + bottom_3

print(f"\nGenerating charts for: {selected_tickers}")

# 5分足チャート生成（個別に保存）
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
            title=f'{ticker_code} - {name_en} ({name_jp})<br>2025-10-24 Morning Session | Daily Change: {daily_change:+.2f}%',
            xaxis_title='Time (09:00 - 11:30)',
            yaxis_title='Price (JPY)',
            height=500,
            template='plotly_white',
            xaxis_rangeslider_visible=False,
            xaxis=dict(type='category')
        )

        output_file = f"notebooks/{ticker_code}_5min.html"
        fig.write_html(output_file, include_plotlyjs='cdn')
        print(f"  ✓ {output_file}")

print("\n✅ All charts generated successfully!")
print("\nGenerated files:")
for ticker_code in selected_tickers:
    if ticker_code in intraday_data:
        print(f"  - notebooks/{ticker_code}_5min.html")
