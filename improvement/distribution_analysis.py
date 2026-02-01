"""
価格帯別リターン分析（曜日別 + 除外チェックボックス）
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "parquet" / "backtest"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_archive() -> pd.DataFrame:
    """grok_trending_archiveを読み込み（analysis pageと同じフィルタ）"""
    archive = pd.read_parquet(DATA_DIR / "grok_trending_archive.parquet")

    archive = archive[archive['buy_price'].notna()].copy()
    archive['backtest_date'] = pd.to_datetime(archive['backtest_date'])
    archive = archive[archive['backtest_date'] >= '2025-11-04'].copy()

    archive = archive[
        (archive['shortable'] == True) |
        ((archive['day_trade'] == True) & (archive['shortable'] == False))
    ].copy()

    archive = archive[archive['high'] != archive['low']].copy()

    archive['weekday'] = archive['backtest_date'].dt.day_name()

    def get_trade_type(row):
        if row['margin_code_name'] == '貸借' and row['shortable'] == True:
            return '制度信用'
        elif row['day_trade'] == True:
            return 'いちにち信用'
        else:
            return 'その他'

    archive['trade_type'] = archive.apply(get_trade_type, axis=1)

    # いちにち信用のうち株数0かどうか（NaNはFalse=除0に含める）
    archive['is_zero_shares'] = (
        (archive['day_trade'] == True) &
        (archive['day_trade_available_shares'] == 0)
    )

    def get_price_band(price):
        if price < 1000:
            return '~1,000円'
        elif price < 3000:
            return '1,000~3,000円'
        elif price < 5000:
            return '3,000~5,000円'
        elif price < 10000:
            return '5,000~10,000円'
        else:
            return '10,000円~'

    archive['price_band'] = archive['buy_price'].apply(get_price_band)

    # ショート戦略用: 符号反転
    archive['calc_me'] = -archive['profit_per_100_shares_morning_early'].fillna(0)
    archive['calc_p1'] = -archive['profit_per_100_shares_phase1'].fillna(0)
    archive['calc_ae'] = -archive['profit_per_100_shares_afternoon_early'].fillna(0)
    archive['calc_p2'] = -archive['profit_per_100_shares_phase2'].fillna(0)

    archive['date_str'] = archive['backtest_date'].dt.strftime('%Y-%m-%d')

    print(f"[INFO] 読み込み完了: {len(archive)}件")
    return archive


def generate_html(df: pd.DataFrame):
    """曜日別 + 除外チェックボックス付きHTML"""

    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekdays_jp = {'Monday': '月曜日', 'Tuesday': '火曜日', 'Wednesday': '水曜日', 'Thursday': '木曜日', 'Friday': '金曜日'}
    trade_types = ['制度信用', 'いちにち信用', 'いちにち信用除0']
    price_bands = ['~1,000円', '1,000~3,000円', '3,000~5,000円', '5,000~10,000円', '10,000円~']
    price_band_colors = {
        '~1,000円': '#ff6b6b',
        '1,000~3,000円': '#ffd93d',
        '3,000~5,000円': '#6bcb77',
        '5,000~10,000円': '#4d96ff',
        '10,000円~': '#9d65c9'
    }

    # 除外対象日（2026年選挙相場）
    exclude_dates = ['2026-01-13', '2026-01-15', '2026-01-16']

    # 全データをJSONで埋め込み
    records = []
    for _, row in df.iterrows():
        records.append({
            'date': row['date_str'],
            'weekday': row['weekday'],
            'trade_type': row['trade_type'],
            'price_band': row['price_band'],
            'calc_me': float(row['calc_me']),
            'calc_p1': float(row['calc_p1']),
            'calc_ae': float(row['calc_ae']),
            'calc_p2': float(row['calc_p2']),
            'is_zero_shares': bool(row['is_zero_shares']),
        })

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>価格帯別リターン分析</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: 'Helvetica Neue', Arial, 'Hiragino Sans', sans-serif;
            background: #0d1117;
            color: #e6edf3;
            line-height: 1.6;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ text-align: center; color: #fff; font-size: 1.6em; margin-bottom: 10px; }}
        .subtitle {{ text-align: center; color: #8b949e; margin-bottom: 20px; font-size: 0.9em; }}
        .filter-section {{
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 15px 20px;
            margin-bottom: 25px;
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        .filter-section label {{ cursor: pointer; display: flex; align-items: center; gap: 8px; }}
        .filter-section input {{ width: 18px; height: 18px; cursor: pointer; }}
        .info {{
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 25px;
        }}
        .weekday-section {{ margin-bottom: 40px; }}
        .weekday-title {{
            font-size: 1.2em;
            color: #fff;
            margin-bottom: 15px;
            padding-bottom: 8px;
            border-bottom: 1px solid #30363d;
        }}
        .trade-type-section {{
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .trade-type-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        .trade-type-title {{ font-size: 1.1em; }}
        .trade-type-title.seido {{ color: #4CAF50; }}
        .trade-type-title.ichinichi {{ color: #2196F3; }}
        .trade-type-title.ichinichi_ex0 {{ color: #9C27B0; }}
        .summary-row {{ display: flex; gap: 20px; margin-bottom: 15px; font-size: 0.95em; }}
        .summary-item {{ text-align: right; }}
        .summary-label {{ color: #8b949e; font-size: 0.85em; }}
        .summary-value {{ font-size: 1.2em; font-weight: bold; }}
        .summary-value.positive {{ color: #4CAF50; }}
        .summary-value.negative {{ color: #f85149; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
        th, td {{ padding: 10px 8px; text-align: center; border-bottom: 1px solid #21262d; }}
        th {{ color: #8b949e; font-weight: normal; font-size: 0.85em; }}
        td.price-band {{ text-align: left; color: #e6edf3; }}
        td.count {{ color: #8b949e; }}
        td.positive {{ color: #4CAF50; }}
        td.negative {{ color: #f85149; }}
        td.win-rate {{ font-size: 0.85em; }}
        td.win-rate.high {{ color: #ffd93d; }}
        .chart-section {{ margin-top: 20px; }}
        .chart-title {{ color: #8b949e; font-size: 0.9em; margin-bottom: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>価格帯別リターン分析</h1>
        <p class="subtitle">曜日別・信用区分別・価格帯別の損益集計</p>

        <div class="filter-section">
            <label>
                <input type="checkbox" id="exclude_election" onchange="updateDisplay()">
                選挙相場除外（1/13, 1/15, 1/16）
            </label>
        </div>

        <div class="info" id="info-section"></div>
        <div id="content"></div>
    </div>

    <script>
        const allData = {json.dumps(records, ensure_ascii=False)};
        const excludeDates = ['2026-01-13', '2026-01-15', '2026-01-16'];
        const weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'];
        const weekdaysJp = {{'Monday': '月曜日', 'Tuesday': '火曜日', 'Wednesday': '水曜日', 'Thursday': '木曜日', 'Friday': '金曜日'}};
        const tradeTypes = ['制度信用', 'いちにち信用', 'いちにち信用除0'];
        const priceBands = ['~1,000円', '1,000~3,000円', '3,000~5,000円', '5,000~10,000円', '10,000円~'];
        const priceBandColors = {{
            '~1,000円': '#ff6b6b',
            '1,000~3,000円': '#ffd93d',
            '3,000~5,000円': '#6bcb77',
            '5,000~10,000円': '#4d96ff',
            '10,000円~': '#9d65c9'
        }};

        function getFilteredData() {{
            if (document.getElementById('exclude_election').checked) {{
                return allData.filter(r => !excludeDates.includes(r.date));
            }}
            return allData;
        }}

        function formatNumber(n) {{
            const sign = n >= 0 ? '+' : '';
            return sign + n.toLocaleString('ja-JP', {{maximumFractionDigits: 0}});
        }}

        function buildTradeSection(tradeType, data, weekday) {{
            let filtered;
            if (tradeType === 'いちにち信用除0') {{
                // いちにち信用から株数0を除外（NaNは含める）
                filtered = data.filter(r => r.trade_type === 'いちにち信用' && r.weekday === weekday && !r.is_zero_shares);
            }} else {{
                filtered = data.filter(r => r.trade_type === tradeType && r.weekday === weekday);
            }}
            if (filtered.length === 0) return '';

            const ttClass = tradeType === '制度信用' ? 'seido' : (tradeType === 'いちにち信用除0' ? 'ichinichi_ex0' : 'ichinichi');
            const chartId = `chart_${{weekday}}_${{ttClass}}`;

            const sumME = filtered.reduce((s, r) => s + r.calc_me, 0);
            const sumP1 = filtered.reduce((s, r) => s + r.calc_p1, 0);
            const sumAE = filtered.reduce((s, r) => s + r.calc_ae, 0);
            const sumP2 = filtered.reduce((s, r) => s + r.calc_p2, 0);

            let html = `
            <div class="trade-type-section">
                <div class="trade-type-header">
                    <span class="trade-type-title ${{ttClass}}">${{tradeType}}</span>
                    <span style="color: #8b949e;">${{filtered.length}}件</span>
                </div>
                <div class="summary-row" style="justify-content: flex-end;">
                    <div class="summary-item"><div class="summary-label">10:25</div><div class="summary-value ${{sumME >= 0 ? 'positive' : 'negative'}}">${{formatNumber(sumME)}}</div></div>
                    <div class="summary-item"><div class="summary-label">前場引</div><div class="summary-value ${{sumP1 >= 0 ? 'positive' : 'negative'}}">${{formatNumber(sumP1)}}</div></div>
                    <div class="summary-item"><div class="summary-label">14:45</div><div class="summary-value ${{sumAE >= 0 ? 'positive' : 'negative'}}">${{formatNumber(sumAE)}}</div></div>
                    <div class="summary-item"><div class="summary-label">大引</div><div class="summary-value ${{sumP2 >= 0 ? 'positive' : 'negative'}}">${{formatNumber(sumP2)}}</div></div>
                </div>
                <table>
                    <tr>
                        <th style="text-align: left;">価格帯</th><th>件</th>
                        <th>10:25</th><th>%</th><th>前場引</th><th>%</th><th>14:45</th><th>%</th><th>大引</th><th>%</th><th>期待値</th>
                    </tr>`;

            for (const pb of priceBands) {{
                const pbData = filtered.filter(r => r.price_band === pb);
                if (pbData.length === 0) continue;

                const count = pbData.length;
                const cols = [
                    [pbData.reduce((s,r) => s + r.calc_me, 0), pbData.filter(r => r.calc_me > 0).length],
                    [pbData.reduce((s,r) => s + r.calc_p1, 0), pbData.filter(r => r.calc_p1 > 0).length],
                    [pbData.reduce((s,r) => s + r.calc_ae, 0), pbData.filter(r => r.calc_ae > 0).length],
                    [pbData.reduce((s,r) => s + r.calc_p2, 0), pbData.filter(r => r.calc_p2 > 0).length],
                ];

                const expectedValue = count > 0 ? cols[3][0] / count : 0;  // 大引けの期待値
                html += `<tr><td class="price-band">${{pb}}</td><td class="count">${{count}}</td>`;
                for (const [total, wins] of cols) {{
                    const winRate = count > 0 ? (wins / count * 100) : 0;
                    const valClass = total > 0 ? 'positive' : (total < 0 ? 'negative' : '');
                    const wrClass = winRate >= 60 ? 'high' : '';
                    html += `<td class="${{valClass}}">${{formatNumber(total)}}</td><td class="win-rate ${{wrClass}}">${{winRate.toFixed(0)}}%</td>`;
                }}
                const evClass = expectedValue > 0 ? 'positive' : (expectedValue < 0 ? 'negative' : '');
                html += `<td class="${{evClass}}">${{formatNumber(expectedValue)}}</td>`;
                html += `</tr>`;
            }}

            html += `</table>
                <div class="chart-section">
                    <div class="chart-title">大引け損益分布（価格帯別）</div>
                    <div id="${{chartId}}" style="height:280px;"></div>
                </div>
            </div>`;

            return html;
        }}

        function drawChart(tradeType, data, weekday) {{
            const ttClass = tradeType === '制度信用' ? 'seido' : (tradeType === 'いちにち信用除0' ? 'ichinichi_ex0' : 'ichinichi');
            const chartId = `chart_${{weekday}}_${{ttClass}}`;
            let filtered;
            if (tradeType === 'いちにち信用除0') {{
                filtered = data.filter(r => r.trade_type === 'いちにち信用' && r.weekday === weekday && !r.is_zero_shares);
            }} else {{
                filtered = data.filter(r => r.trade_type === tradeType && r.weekday === weekday);
            }}

            const traces = [];
            for (const pb of priceBands) {{
                const profits = filtered.filter(r => r.price_band === pb).map(r => r.calc_p2);
                if (profits.length > 0) {{
                    traces.push({{
                        x: profits,
                        type: 'histogram',
                        name: pb,
                        marker: {{ color: priceBandColors[pb] }},
                        opacity: 0.7,
                        nbinsx: 20
                    }});
                }}
            }}

            if (traces.length > 0 && document.getElementById(chartId)) {{
                Plotly.newPlot(chartId, traces, {{
                    barmode: 'overlay',
                    paper_bgcolor: '#161b22',
                    plot_bgcolor: '#161b22',
                    font: {{ color: '#e6edf3' }},
                    xaxis: {{ title: '損益（円/100株）', gridcolor: '#30363d', zeroline: true, zerolinecolor: '#f85149', zerolinewidth: 2 }},
                    yaxis: {{ title: '件数', gridcolor: '#30363d' }},
                    margin: {{ t: 10, b: 50, l: 50, r: 20 }},
                    legend: {{ x: 1, y: 1, xanchor: 'right' }},
                    showlegend: true
                }});
            }}
        }}

        function updateDisplay() {{
            const data = getFilteredData();

            const dates = [...new Set(data.map(r => r.date))].sort();
            document.getElementById('info-section').innerHTML = `
                <strong>データ期間:</strong> ${{dates[0] || '-'}} ~ ${{dates[dates.length - 1] || '-'}}<br>
                <strong>総件数:</strong> ${{data.length}}件
            `;

            let content = '';
            for (const wd of weekdays) {{
                const wdData = data.filter(r => r.weekday === wd);
                if (wdData.length === 0) continue;

                content += `<div class="weekday-section"><h2 class="weekday-title">${{weekdaysJp[wd]}}</h2>`;
                for (const tt of tradeTypes) {{
                    content += buildTradeSection(tt, data, wd);
                }}
                content += `</div>`;
            }}
            document.getElementById('content').innerHTML = content;

            // チャート描画
            for (const wd of weekdays) {{
                for (const tt of tradeTypes) {{
                    drawChart(tt, data, wd);
                }}
            }}
        }}

        updateDisplay();
    </script>
</body>
</html>
"""

    output_path = OUTPUT_DIR / "distribution_analysis.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"[INFO] 出力: {output_path}")


def main():
    print("[INFO] データ読み込み中...")
    df = load_archive()

    print("[INFO] HTML生成中...")
    generate_html(df)

    print("[INFO] 完了")


if __name__ == "__main__":
    main()
