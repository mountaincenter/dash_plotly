#!/usr/bin/env python3
"""
エントリー定義バックテスト結果のHTML生成
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"


def generate_html():
    # サマリー読み込み
    df_summary = pd.read_csv(DATA_DIR / "backtest_entry_summary.csv")

    # 詳細データ読み込み
    patterns = ['A', 'B', 'C', 'D', 'E']
    details = {}
    for p in patterns:
        path = DATA_DIR / f"backtest_{p}.parquet"
        if path.exists():
            details[p] = pd.read_parquet(path)

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>エントリー定義バックテスト結果</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        :root {{
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --border-color: #30363d;
            --accent-red: #f85149;
            --accent-green: #3fb950;
            --accent-blue: #58a6ff;
            --accent-orange: #d29922;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ font-size: 1.8rem; margin-bottom: 10px; }}
        .subtitle {{ color: var(--text-secondary); margin-bottom: 20px; }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .summary-card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
        }}
        .summary-card.best {{
            border-color: var(--accent-green);
            background: rgba(63, 185, 80, 0.1);
        }}
        .summary-card.worst {{
            border-color: var(--accent-red);
            background: rgba(248, 81, 73, 0.1);
        }}
        .card-title {{
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-bottom: 5px;
        }}
        .card-value {{
            font-size: 1.5rem;
            font-weight: 600;
        }}
        .card-value.positive {{ color: var(--accent-green); }}
        .card-value.negative {{ color: var(--accent-red); }}

        .section {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .section-title {{
            font-size: 1.2rem;
            margin-bottom: 15px;
            color: var(--accent-blue);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        th {{
            background: var(--bg-tertiary);
            font-weight: 600;
            color: var(--text-secondary);
        }}
        tr:hover {{ background: var(--bg-tertiary); }}

        .chart-container {{
            height: 400px;
            margin-top: 20px;
        }}

        .pattern-desc {{
            background: var(--bg-tertiary);
            padding: 15px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-family: monospace;
            font-size: 0.9rem;
        }}

        .metric-row {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            margin-bottom: 15px;
        }}
        .metric {{
            background: var(--bg-tertiary);
            padding: 10px 15px;
            border-radius: 6px;
        }}
        .metric-label {{
            font-size: 0.8rem;
            color: var(--text-secondary);
        }}
        .metric-value {{
            font-size: 1.2rem;
            font-weight: 600;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>エントリー定義バックテスト結果</h1>
        <p class="subtitle">生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 対象: 常習犯496銘柄 × 60日</p>

        <div class="section">
            <h2 class="section-title">パターン比較サマリー</h2>
            <table>
                <thead>
                    <tr>
                        <th>パターン</th>
                        <th>トレード数</th>
                        <th>勝率</th>
                        <th>平均損益</th>
                        <th>中央値</th>
                        <th>シャープ様</th>
                    </tr>
                </thead>
                <tbody>'''

    # サマリーテーブル
    for _, row in df_summary.iterrows():
        avg_pnl = float(row['平均損益'].replace('%', ''))
        pnl_class = 'positive' if avg_pnl > 0 else 'negative'

        html += f'''
                    <tr>
                        <td>{row['パターン']}</td>
                        <td>{row['トレード数']:,}</td>
                        <td>{row['勝率']}</td>
                        <td class="{pnl_class}">{row['平均損益']}</td>
                        <td>{row['中央値']}</td>
                        <td>{row['シャープ様']}</td>
                    </tr>'''

    html += '''
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2 class="section-title">パターン定義</h2>
            <div class="pattern-desc">
A: 高値から-2%/-3%下落した時点でショートエントリー → 大引けイグジット
B: 後場寄りが前場高値を下回っていたらショートエントリー → 大引けイグジット
C: 陰線3本連続でショートエントリー → 大引けイグジット
D: RSIが70超→60割れでショートエントリー → 大引けイグジット
E: 出来高急増後、価格が高値から-1%下落でショートエントリー → 大引けイグジット
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">損益分布</h2>
            <div id="pnl-distribution" class="chart-container"></div>
        </div>

        <div class="section">
            <h2 class="section-title">考察</h2>
            <div class="metric-row">
                <div class="metric">
                    <div class="metric-label">最も有効なパターン</div>
                    <div class="metric-value" style="color: var(--accent-green);">A: 高値から-2%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">理由</div>
                    <div class="metric-value">勝率53.6%、平均+0.25%</div>
                </div>
            </div>
            <div class="pattern-desc">
注意点:
- 一次スクリーニング（前日出来高急騰）未適用
- 手数料・スリッページ未考慮
- 空売り可否未確認
- 荒いバックテストなので参考値
            </div>
        </div>
    </div>

    <script>
'''

    # 損益分布のデータを準備
    for p, df in details.items():
        if len(df) > 0:
            pnl_data = df['pnl_pct'].clip(-10, 10).tolist()  # -10%〜+10%でクリップ
            html += f'''
        var pnl_{p} = {pnl_data[:1000]};  // サンプリング
'''

    html += '''
        var traces = ['''

    colors = {'A': '#3fb950', 'B': '#58a6ff', 'C': '#d29922', 'D': '#f85149', 'E': '#a371f7'}
    labels = {'A': 'A: 高値-2%', 'B': 'B: 後場寄り', 'C': 'C: 陰線3本', 'D': 'D: RSI', 'E': 'E: 出来高'}

    for p in patterns:
        if p in details:
            html += f'''
            {{
                x: pnl_{p},
                type: 'histogram',
                name: '{labels.get(p, p)}',
                opacity: 0.6,
                marker: {{ color: '{colors.get(p, '#888')}' }},
                xbins: {{ size: 0.5 }}
            }},'''

    html += '''
        ];

        var layout = {
            paper_bgcolor: '#0d1117',
            plot_bgcolor: '#161b22',
            font: { color: '#e6edf3' },
            barmode: 'overlay',
            xaxis: {
                title: '損益 (%)',
                gridcolor: '#30363d',
                range: [-10, 10]
            },
            yaxis: {
                title: '頻度',
                gridcolor: '#30363d'
            },
            legend: {
                orientation: 'h',
                y: -0.2
            }
        };

        Plotly.newPlot('pnl-distribution', traces, layout);
    </script>
</body>
</html>'''

    output_path = OUTPUT_DIR / "entry_backtest_results.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"HTML生成完了: {output_path}")


if __name__ == '__main__':
    generate_html()
