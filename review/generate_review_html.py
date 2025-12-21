"""
トレードレビューHTML生成スクリプト
- 5分足ローソク足チャート
- RSIライン（70/30水平線）
- エントリー/イグジットポイント表示
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json


def calc_rsi(prices, period=9):
    """RSI計算（EMA方式、期間9 - 楽天MARKETSPEED互換）"""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def find_signals(df):
    """RSI>70下落開始とRSI<30のシグナルを検出"""
    signals = []

    for i in range(1, len(df)):
        # RSI>70から下落開始（前が70超、今が70以下または下落中）
        if df['RSI'].iloc[i-1] > 70 and df['RSI'].iloc[i] < df['RSI'].iloc[i-1]:
            if df['RSI'].iloc[i-1] > 70:  # 直前が70超
                signals.append({
                    'time': df.index[i],
                    'type': 'entry',
                    'price': df['Close'].iloc[i],
                    'rsi': df['RSI'].iloc[i]
                })

        # RSI<30
        if df['RSI'].iloc[i] < 30 and df['RSI'].iloc[i-1] >= 30:
            signals.append({
                'time': df.index[i],
                'type': 'exit',
                'price': df['Close'].iloc[i],
                'rsi': df['RSI'].iloc[i]
            })

    return signals


def generate_html(df_5m, ticker, name, date_str, trade_info=None):
    """HTMLチャート生成（5分足、楽天MARKETSPEED互換RSI1）"""

    target_date = pd.Timestamp(date_str).date()

    # 当日データのみ抽出してRSI計算（楽天方式は当日データのみ）
    df = df_5m[df_5m.index.date == target_date].copy()
    df['RSI'] = calc_rsi(df['Close'])

    if len(df) == 0:
        print(f"No data for {date_str}")
        return None

    # シグナル検出
    signals = find_signals(df)

    # Plotly用データ準備
    ohlc_data = []
    for idx, row in df.iterrows():
        ohlc_data.append({
            'x': idx.strftime('%Y-%m-%d %H:%M'),
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close']
        })

    rsi_data = []
    for idx, row in df.iterrows():
        if not np.isnan(row['RSI']):
            rsi_data.append({
                'x': idx.strftime('%Y-%m-%d %H:%M'),
                'y': row['RSI']
            })

    # シグナルマーカー
    entry_markers = []
    exit_markers = []
    for sig in signals:
        marker = {
            'x': sig['time'].strftime('%Y-%m-%d %H:%M'),
            'price': sig['price'],
            'rsi': sig['rsi']
        }
        if sig['type'] == 'entry':
            entry_markers.append(marker)
        else:
            exit_markers.append(marker)

    # 実際のトレード情報
    actual_trade_html = ""
    if trade_info:
        actual_trade_html = f"""
        <div class="trade-info">
            <h3>実際のトレード</h3>
            <table>
                <tr><th>エントリー</th><td>{trade_info.get('entry_price', '-')}円 @ {trade_info.get('entry_time', '-')}</td></tr>
                <tr><th>イグジット</th><td>{trade_info.get('exit_price', '-')}円 @ {trade_info.get('exit_time', '-')}</td></tr>
                <tr><th>損益</th><td>{trade_info.get('pnl', '-')}円</td></tr>
            </table>
        </div>
        """

    html = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{name} ({ticker}) - {date_str} トレードレビュー</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0a0a0f;
            color: #e0e0e0;
            padding: 20px;
        }}
        h1 {{
            color: #fff;
            margin-bottom: 20px;
            font-size: 1.5rem;
        }}
        .chart-container {{
            background: #1a1a2e;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .signals-table {{
            background: #1a1a2e;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #333;
        }}
        th {{ color: #888; }}
        .entry {{ color: #ff6b6b; }}
        .exit {{ color: #4ecdc4; }}
        .trade-info {{
            background: #2a2a4e;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .trade-info h3 {{
            color: #fff;
            margin-bottom: 10px;
        }}
    </style>
</head>
<body>
    <h1>{name} ({ticker}) - {date_str}</h1>

    {actual_trade_html}

    <div class="chart-container">
        <div id="candlestick-chart"></div>
    </div>

    <div class="chart-container">
        <div id="rsi-chart"></div>
    </div>

    <div class="signals-table">
        <h3 style="color: #fff; margin-bottom: 15px;">機械判定シグナル</h3>
        <table>
            <tr>
                <th>時刻</th>
                <th>タイプ</th>
                <th>価格</th>
                <th>RSI</th>
            </tr>
            {"".join([f'<tr class="{sig["type"]}"><td>{sig["time"].strftime("%H:%M")}</td><td>{"エントリー" if sig["type"] == "entry" else "イグジット"}</td><td>{sig["price"]:.0f}円</td><td>{sig["rsi"]:.1f}</td></tr>' for sig in signals])}
        </table>
    </div>

    <script>
        // OHLCデータ
        const ohlcData = {json.dumps(ohlc_data)};
        const rsiData = {json.dumps(rsi_data)};
        const entryMarkers = {json.dumps(entry_markers)};
        const exitMarkers = {json.dumps(exit_markers)};

        // ローソク足チャート
        const candlestickTrace = {{
            x: ohlcData.map(d => d.x),
            open: ohlcData.map(d => d.open),
            high: ohlcData.map(d => d.high),
            low: ohlcData.map(d => d.low),
            close: ohlcData.map(d => d.close),
            type: 'candlestick',
            increasing: {{ line: {{ color: '#4ecdc4' }} }},
            decreasing: {{ line: {{ color: '#ff6b6b' }} }}
        }};

        // エントリーマーカー
        const entryTrace = {{
            x: entryMarkers.map(d => d.x),
            y: entryMarkers.map(d => d.price),
            mode: 'markers',
            marker: {{
                symbol: 'triangle-down',
                size: 15,
                color: '#ff6b6b'
            }},
            name: 'Entry (RSI>70下落)'
        }};

        // イグジットマーカー
        const exitTrace = {{
            x: exitMarkers.map(d => d.x),
            y: exitMarkers.map(d => d.price),
            mode: 'markers',
            marker: {{
                symbol: 'triangle-up',
                size: 15,
                color: '#4ecdc4'
            }},
            name: 'Exit (RSI<30)'
        }};

        const candlestickLayout = {{
            title: '5分足チャート',
            paper_bgcolor: '#1a1a2e',
            plot_bgcolor: '#1a1a2e',
            font: {{ color: '#e0e0e0' }},
            xaxis: {{
                gridcolor: '#333',
                rangeslider: {{ visible: false }}
            }},
            yaxis: {{
                gridcolor: '#333',
                title: '価格 (円)'
            }},
            showlegend: true,
            legend: {{ x: 0, y: 1.1, orientation: 'h' }}
        }};

        Plotly.newPlot('candlestick-chart', [candlestickTrace, entryTrace, exitTrace], candlestickLayout);

        // RSIチャート
        const rsiTrace = {{
            x: rsiData.map(d => d.x),
            y: rsiData.map(d => d.y),
            type: 'scatter',
            mode: 'lines',
            line: {{ color: '#ffd93d', width: 2 }},
            name: 'RSI(14)'
        }};

        // RSI 70/30ライン
        const rsi70 = {{
            x: [rsiData[0]?.x, rsiData[rsiData.length-1]?.x],
            y: [70, 70],
            type: 'scatter',
            mode: 'lines',
            line: {{ color: '#ff6b6b', dash: 'dash', width: 1 }},
            name: 'RSI 70'
        }};

        const rsi30 = {{
            x: [rsiData[0]?.x, rsiData[rsiData.length-1]?.x],
            y: [30, 30],
            type: 'scatter',
            mode: 'lines',
            line: {{ color: '#4ecdc4', dash: 'dash', width: 1 }},
            name: 'RSI 30'
        }};

        const rsiLayout = {{
            title: 'RSI (14)',
            paper_bgcolor: '#1a1a2e',
            plot_bgcolor: '#1a1a2e',
            font: {{ color: '#e0e0e0' }},
            xaxis: {{ gridcolor: '#333' }},
            yaxis: {{
                gridcolor: '#333',
                range: [0, 100],
                title: 'RSI'
            }},
            showlegend: true,
            legend: {{ x: 0, y: 1.15, orientation: 'h' }}
        }};

        Plotly.newPlot('rsi-chart', [rsiTrace, rsi70, rsi30], rsiLayout);
    </script>
</body>
</html>
"""
    return html


if __name__ == '__main__':
    import sys

    date_str = '2025-12-17'
    review_dir = f'/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/review/{date_str.replace("-", "")}'

    # kudan（5分足使用 - 楽天MARKETSPEED互換）
    df_kudan = pd.read_parquet(f'{review_dir}/kudan_5m_20251217.parquet')
    html_kudan = generate_html(
        df_kudan,
        '4425.T',
        'kudan',
        date_str,
        trade_info={
            'entry_price': 1191,
            'entry_time': '12:35頃',
            'exit_price': 1178,
            'exit_time': '13:30頃',
            'pnl': '+1,300'
        }
    )

    with open(f'{review_dir}/kudan_20251217.html', 'w', encoding='utf-8') as f:
        f.write(html_kudan)
    print(f'Generated: {review_dir}/kudan_20251217.html')

    # サイバーリンクス（5分足使用 - 楽天MARKETSPEED互換）
    df_cyber = pd.read_parquet(f'{review_dir}/cyberlinks_5m_20251217.parquet')
    html_cyber = generate_html(
        df_cyber,
        '3683.T',
        'サイバーリンクス',
        date_str,
        trade_info={
            'entry_price': 1337,
            'entry_time': '10:18頃',
            'exit_price': 1334.8,
            'exit_time': '13:30頃',
            'pnl': '+220'
        }
    )

    with open(f'{review_dir}/cyberlinks_20251217.html', 'w', encoding='utf-8') as f:
        f.write(html_cyber)
    print(f'Generated: {review_dir}/cyberlinks_20251217.html')
