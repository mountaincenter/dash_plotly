"""
grok_trending銘柄の日中パターン分析
前日終値を0とした相対的な値動きをHTMLで表示
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_data():
    """データ読み込み"""
    grok = pd.read_parquet(DATA_DIR / "grok_trending.parquet")
    df_1d = pd.read_parquet(DATA_DIR / "prices_max_1d.parquet")
    df_5m = pd.read_parquet(DATA_DIR / "prices_60d_5m.parquet")

    print(f"[INFO] grok_trending: {len(grok)}件")
    print(f"[INFO] 日足: {len(df_1d)}行, {df_1d['ticker'].nunique()}銘柄")
    print(f"[INFO] 5分足: {len(df_5m)}行, {df_5m['ticker'].nunique()}銘柄")

    return grok, df_1d, df_5m


def get_prev_and_current_close(df_1d: pd.DataFrame, ticker: str, target_date) -> tuple:
    """日足から前日終値と当日終値を取得"""
    ticker_data = df_1d[df_1d['ticker'] == ticker].copy()
    ticker_data['date'] = pd.to_datetime(ticker_data['date']).dt.date
    ticker_data = ticker_data.sort_values('date')

    target = pd.to_datetime(target_date).date()

    # 当日終値
    current_row = ticker_data[ticker_data['date'] == target]
    current_close = current_row['Close'].iloc[0] if not current_row.empty else None

    # 前日終値（target_dateより前の最新）
    prev_data = ticker_data[ticker_data['date'] < target]
    prev_close = prev_data['Close'].iloc[-1] if not prev_data.empty else None

    return prev_close, current_close


def get_intraday_data(df_5m: pd.DataFrame, ticker: str, target_date) -> pd.DataFrame:
    """5分足から当日の日中データを取得（取引時間のみ）"""
    ticker_data = df_5m[df_5m['ticker'] == ticker].copy()

    # datetime処理（データはすでにJSTで保存されている）
    ticker_data['datetime'] = pd.to_datetime(ticker_data['date'])
    ticker_data['date_only'] = ticker_data['datetime'].dt.date
    target = pd.to_datetime(target_date).date()

    day_data = ticker_data[ticker_data['date_only'] == target].copy()
    day_data['time'] = day_data['datetime'].dt.strftime('%H:%M')

    # 取引時間のみフィルタ（9:00-15:25）
    valid_times = ['09:00'] + [f'{h:02d}:{m:02d}' for h in range(9, 16) for m in range(0, 60, 5) if not (h == 9 and m == 0)]
    valid_times = valid_times[:valid_times.index('15:25')+1]
    day_data = day_data[day_data['time'].isin(valid_times)]

    return day_data.sort_values('datetime')


def calculate_relative_values(intraday: pd.DataFrame, prev_close: float, current_close: float) -> pd.DataFrame:
    """当日始値基準の相対値を計算"""
    if intraday.empty:
        return pd.DataFrame()

    result = intraday[['time', 'Open', 'High', 'Low', 'Close']].copy()

    # 当日始値を基準（0%）にする
    first_valid = result[result['Open'].notna()]
    if first_valid.empty:
        return pd.DataFrame()
    open_price = first_valid.iloc[0]['Open']

    result['relative_open'] = (result['Open'] - open_price) / open_price * 100
    result['relative_high'] = (result['High'] - open_price) / open_price * 100
    result['relative_low'] = (result['Low'] - open_price) / open_price * 100
    result['relative_close'] = (result['Close'] - open_price) / open_price * 100

    # 15:30（当日終値）を追加
    if current_close is not None:
        close_row = pd.DataFrame({
            'time': ['15:30'],
            'Open': [current_close],
            'High': [current_close],
            'Low': [current_close],
            'Close': [current_close],
            'relative_open': [(current_close - open_price) / open_price * 100],
            'relative_high': [(current_close - open_price) / open_price * 100],
            'relative_low': [(current_close - open_price) / open_price * 100],
            'relative_close': [(current_close - open_price) / open_price * 100],
        })
        result = pd.concat([result, close_row], ignore_index=True)

    # 時間順にソート
    time_order = ['09:00'] + [f'{h:02d}:{m:02d}' for h in range(9, 16) for m in range(0, 60, 5) if not (h == 9 and m == 0)]
    time_order = time_order[:time_order.index('15:25')+1] + ['15:30']
    result['time_order'] = result['time'].apply(lambda x: time_order.index(x) if x in time_order else 999)
    result = result.sort_values('time_order').reset_index(drop=True)

    return result


def generate_html(all_data: dict, grok: pd.DataFrame, target_date: str):
    """HTMLファイルを生成"""

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>grok_trending 日中パターン分析 ({target_date})</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: 'Helvetica Neue', Arial, sans-serif; background: #1a1a2e; color: #eee; margin: 20px; }}
        h1 {{ text-align: center; color: #fff; }}
        h2 {{ color: #4CAF50; border-bottom: 1px solid #4CAF50; padding-bottom: 5px; margin-top: 40px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .info {{ background: #2d2d44; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        .chart-container {{ margin-bottom: 20px; }}
        .summary-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .summary-table th, .summary-table td {{ border: 1px solid #444; padding: 8px; text-align: center; }}
        .summary-table th {{ background: #2d2d44; }}
        .positive {{ color: #4CAF50; }}
        .negative {{ color: #f44336; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>grok_trending 日中パターン分析</h1>
        <p style="text-align: center; color: #888;">前日終値を0%として、日中の相対的な価格推移を可視化</p>

        <div class="info">
            <strong>対象日:</strong> {target_date}<br>
            <strong>銘柄数:</strong> {len(all_data)}銘柄
        </div>

        <h2>サマリー</h2>
        <table class="summary-table">
            <tr>
                <th>銘柄</th>
                <th>コード</th>
                <th>前日終値</th>
                <th>当日終値</th>
                <th>終値変化率</th>
                <th>日中高値</th>
                <th>日中安値</th>
            </tr>
"""

    for ticker, data in all_data.items():
        info = data['info']
        relative = data['relative']

        if relative.empty:
            continue

        final_return = relative[relative['time'] == '15:30']['relative_close'].values
        final_return = final_return[0] if len(final_return) > 0 else 0

        high_return = relative['relative_high'].max()
        low_return = relative['relative_low'].min()

        final_class = 'positive' if final_return >= 0 else 'negative'
        high_class = 'positive' if high_return >= 0 else 'negative'
        low_class = 'positive' if low_return >= 0 else 'negative'

        html += f"""<tr>
            <td>{info['stock_name']}</td>
            <td>{info['code']}</td>
            <td>{data['prev_close']:,.0f}</td>
            <td>{data['current_close']:,.0f}</td>
            <td class='{final_class}'>{final_return:+.2f}%</td>
            <td class='{high_class}'>{high_return:+.2f}%</td>
            <td class='{low_class}'>{low_return:+.2f}%</td>
        </tr>"""

    html += """
        </table>

        <h2>全銘柄オーバーレイ</h2>
        <div class="chart-container">
            <div id="chart_all" style="width:100%; height:500px;"></div>
        </div>
        <script>
            Plotly.newPlot('chart_all', [
"""

    # 全銘柄のトレースを追加
    traces = []
    for ticker, data in all_data.items():
        info = data['info']
        relative = data['relative']
        if relative.empty:
            continue
        times = relative['time'].tolist()
        closes = [None if pd.isna(x) else x for x in relative['relative_close'].tolist()]
        traces.append(f"""                {{
                    x: {json.dumps(times)},
                    y: {json.dumps(closes)},
                    type: 'scatter',
                    mode: 'lines',
                    name: '{info['stock_name']}'
                }}""")

    html += ",\n".join(traces)
    html += """
            ], {
                title: '全銘柄 日中パターン（前日比%）',
                paper_bgcolor: '#1a1a2e',
                plot_bgcolor: '#1a1a2e',
                font: { color: '#eee' },
                xaxis: { title: '時刻', gridcolor: '#333' },
                yaxis: { title: '前日比 (%)', gridcolor: '#333', zeroline: true, zerolinecolor: '#666' },
                showlegend: true,
                legend: { x: 1.02, y: 1 },
                margin: { t: 40, b: 40, l: 60, r: 150 }
            });
        </script>

        <h2>個別チャート</h2>
"""

    # 各銘柄のチャート
    for ticker, data in all_data.items():
        info = data['info']
        relative = data['relative']

        if relative.empty:
            continue

        chart_id = ticker.replace('.', '_')
        times = relative['time'].tolist()
        # NaNをnullに変換するためjson.dumpsを使用
        closes = [None if pd.isna(x) else x for x in relative['relative_close'].tolist()]
        highs = [None if pd.isna(x) else x for x in relative['relative_high'].tolist()]
        lows = [None if pd.isna(x) else x for x in relative['relative_low'].tolist()]

        html += f"""
        <div class="chart-container">
            <div id="chart_{chart_id}" style="width:100%; height:300px;"></div>
        </div>
        <script>
            Plotly.newPlot('chart_{chart_id}', [
                {{
                    x: {json.dumps(times)},
                    y: {json.dumps(closes)},
                    type: 'scatter',
                    mode: 'lines',
                    name: '終値',
                    line: {{ color: '#4CAF50', width: 2 }}
                }},
                {{
                    x: {json.dumps(times)},
                    y: {json.dumps(highs)},
                    type: 'scatter',
                    mode: 'lines',
                    name: '高値',
                    line: {{ color: '#ff9800', width: 1, dash: 'dot' }}
                }},
                {{
                    x: {json.dumps(times)},
                    y: {json.dumps(lows)},
                    type: 'scatter',
                    mode: 'lines',
                    name: '安値',
                    line: {{ color: '#2196F3', width: 1, dash: 'dot' }}
                }}
            ], {{
                title: '{info['stock_name']} ({info['code']})',
                paper_bgcolor: '#1a1a2e',
                plot_bgcolor: '#1a1a2e',
                font: {{ color: '#eee' }},
                xaxis: {{ title: '時刻', gridcolor: '#333' }},
                yaxis: {{ title: '前日比 (%)', gridcolor: '#333', zeroline: true, zerolinecolor: '#666' }},
                showlegend: true,
                legend: {{ x: 0, y: 1 }},
                margin: {{ t: 40, b: 40, l: 60, r: 20 }}
            }});
        </script>
"""

    html += """
    </div>
</body>
</html>
"""

    output_path = OUTPUT_DIR / "grok_intraday_pattern.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"[INFO] 出力: {output_path}")


def main():
    print("[INFO] データ読み込み中...")
    grok, df_1d, df_5m = load_data()

    target_date = grok['date'].iloc[0]
    print(f"[INFO] 対象日: {target_date}")

    all_data = {}

    print("[INFO] 相対値計算中...")
    for idx, row in grok.iterrows():
        ticker = row['ticker']

        # 前日終値と当日終値を日足から取得
        prev_close, current_close = get_prev_and_current_close(df_1d, ticker, target_date)

        if prev_close is None:
            print(f"[WARN] {ticker}: 前日終値なし")
            continue

        # 5分足から日中データ取得
        intraday = get_intraday_data(df_5m, ticker, target_date)

        if intraday.empty:
            print(f"[WARN] {ticker}: 5分足データなし")
            continue

        # 相対値計算
        relative = calculate_relative_values(intraday, prev_close, current_close)

        all_data[ticker] = {
            'info': row.to_dict(),
            'prev_close': prev_close,
            'current_close': current_close if current_close else prev_close,
            'relative': relative
        }

    print(f"[INFO] 有効データ: {len(all_data)}銘柄")

    # HTML生成
    generate_html(all_data, grok, target_date)

    print("[INFO] 完了")


if __name__ == "__main__":
    main()
