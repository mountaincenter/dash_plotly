#!/usr/bin/env python3
"""
バックテスト結果HTML生成（統合版）
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"


def generate_html():
    # データ読み込み
    df_vol = pd.read_csv(DATA_DIR / "backtest_volume_filter_summary.csv")
    df_exit = pd.read_parquet(DATA_DIR / "backtest_exit_timing.parquet")

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ショート戦略バックテスト結果</title>
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
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ font-size: 1.8rem; margin-bottom: 10px; }}
        .subtitle {{ color: var(--text-secondary); margin-bottom: 30px; }}

        .strategy-box {{
            background: linear-gradient(135deg, rgba(63, 185, 80, 0.1), rgba(88, 166, 255, 0.1));
            border: 2px solid var(--accent-green);
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 30px;
        }}
        .strategy-title {{
            font-size: 1.3rem;
            color: var(--accent-green);
            margin-bottom: 15px;
        }}
        .strategy-flow {{
            font-family: monospace;
            font-size: 1.1rem;
            background: var(--bg-tertiary);
            padding: 20px;
            border-radius: 8px;
            line-height: 2;
        }}
        .flow-arrow {{ color: var(--accent-blue); }}
        .flow-highlight {{ color: var(--accent-green); font-weight: bold; }}

        .result-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .result-card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
            text-align: center;
        }}
        .result-card.highlight {{
            border-color: var(--accent-green);
            background: rgba(63, 185, 80, 0.1);
        }}
        .card-label {{
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-bottom: 8px;
        }}
        .card-value {{
            font-size: 1.8rem;
            font-weight: bold;
        }}
        .card-value.positive {{ color: var(--accent-green); }}
        .card-value.negative {{ color: var(--accent-red); }}

        .section {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 25px;
            margin-bottom: 25px;
        }}
        .section-title {{
            font-size: 1.2rem;
            color: var(--accent-blue);
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
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
            font-size: 0.85rem;
        }}
        tr:hover {{ background: rgba(88, 166, 255, 0.05); }}
        tr.best {{ background: rgba(63, 185, 80, 0.1); }}

        .chart-container {{
            height: 350px;
            margin-top: 15px;
        }}

        .note {{
            background: var(--bg-tertiary);
            padding: 15px;
            border-radius: 6px;
            font-size: 0.9rem;
            color: var(--text-secondary);
            margin-top: 20px;
        }}
        .note-title {{
            color: var(--accent-orange);
            font-weight: 600;
            margin-bottom: 8px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>ショート戦略バックテスト結果</h1>
        <p class="subtitle">生成: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 対象: 常習犯496銘柄 × 60日</p>

        <div class="strategy-box">
            <h2 class="strategy-title">確定した戦略（後知恵なし）</h2>
            <div class="strategy-flow">
                <span class="flow-highlight">一次スクリーニング（前日）</span><br>
                　出来高が過去20日平均の <span class="flow-highlight">2倍以上</span> に急騰<br>
                <span class="flow-arrow">↓</span><br>
                <span class="flow-highlight">二次スクリーニング（当日・後場）</span><br>
                　後場に入ってから、<span class="flow-highlight">前場高値-2%</span> でショートエントリー<br>
                　※後場寄付でそのまま入らない（高値更新リスク22%）<br>
                <span class="flow-arrow">↓</span><br>
                <span class="flow-highlight">イグジット</span><br>
                　<span class="flow-highlight">大引け</span>で決済
            </div>
        </div>

        <div class="result-cards">
            <div class="result-card highlight">
                <div class="card-label">勝率</div>
                <div class="card-value positive">76.0%</div>
            </div>
            <div class="result-card highlight">
                <div class="card-label">平均損益</div>
                <div class="card-value positive">+2.94%</div>
            </div>
            <div class="result-card highlight">
                <div class="card-label">コスト後純損益</div>
                <div class="card-value positive">+2.69%</div>
            </div>
            <div class="result-card">
                <div class="card-label">トレード数</div>
                <div class="card-value">1,517</div>
            </div>
            <div class="result-card">
                <div class="card-label">100回で</div>
                <div class="card-value positive">+269%</div>
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">一次スクリーニング（出来高フィルタ）の効果</h2>
            <table>
                <thead>
                    <tr>
                        <th>条件</th>
                        <th>トレード数</th>
                        <th>勝率</th>
                        <th>平均損益</th>
                        <th>コスト後</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>全日（フィルタなし）</td>
                        <td>20,274</td>
                        <td>53.6%</td>
                        <td>+0.25%</td>
                        <td>±0%</td>
                    </tr>
                    <tr>
                        <td>出来高1.5倍翌日</td>
                        <td>2,631</td>
                        <td>57.0%</td>
                        <td>+0.59%</td>
                        <td>+0.34%</td>
                    </tr>
                    <tr class="best">
                        <td><strong>出来高2倍翌日</strong></td>
                        <td>1,725</td>
                        <td><strong>59.5%</strong></td>
                        <td><strong>+0.91%</strong></td>
                        <td><strong>+0.66%</strong></td>
                    </tr>
                    <tr>
                        <td>出来高3倍翌日</td>
                        <td>1,053</td>
                        <td>60.8%</td>
                        <td>+1.21%</td>
                        <td>+0.96%</td>
                    </tr>
                    <tr>
                        <td>出来高5倍翌日</td>
                        <td>556</td>
                        <td>62.8%</td>
                        <td>+1.69%</td>
                        <td>+1.44%</td>
                    </tr>
                </tbody>
            </table>
            <div id="volume-chart" class="chart-container"></div>
        </div>

        <div class="section">
            <h2 class="section-title">イグジットタイミング比較</h2>
            <table>
                <thead>
                    <tr>
                        <th>イグジット</th>
                        <th>勝率</th>
                        <th>平均損益</th>
                        <th>中央値</th>
                        <th>コスト後</th>
                    </tr>
                </thead>
                <tbody>
                    <tr class="best">
                        <td><strong>大引け</strong></td>
                        <td><strong>59.5%</strong></td>
                        <td><strong>+0.91%</strong></td>
                        <td>+0.62%</td>
                        <td><strong>+0.66%</strong></td>
                    </tr>
                    <tr>
                        <td>14:00</td>
                        <td>59.4%</td>
                        <td>+0.78%</td>
                        <td>+0.52%</td>
                        <td>+0.53%</td>
                    </tr>
                    <tr>
                        <td>14:30</td>
                        <td>59.2%</td>
                        <td>+0.85%</td>
                        <td>+0.56%</td>
                        <td>+0.60%</td>
                    </tr>
                    <tr>
                        <td>トレーリング1%</td>
                        <td>52.7%</td>
                        <td>+0.61%</td>
                        <td>+0.07%</td>
                        <td>+0.36%</td>
                    </tr>
                    <tr>
                        <td>理論最大（底値）</td>
                        <td>99.8%</td>
                        <td>+4.08%</td>
                        <td>+2.78%</td>
                        <td>+3.83%</td>
                    </tr>
                </tbody>
            </table>
            <div id="exit-chart" class="chart-container"></div>
        </div>

        <div class="section">
            <h2 class="section-title">損益分布</h2>
            <div id="pnl-hist" class="chart-container"></div>
        </div>

        <div class="note">
            <div class="note-title">注意事項</div>
            <ul>
                <li>コスト: 手数料+スリッページで0.25%/回を想定</li>
                <li>空売り可否: 未考慮（実際は制度/いちにち信用の確認が必要）</li>
                <li>流動性: 約定価格のスリッページは別途発生する可能性あり</li>
                <li>データ期間: 60日間（市場環境によって結果は変動）</li>
            </ul>
        </div>
    </div>

    <script>
        var darkLayout = {{
            paper_bgcolor: '#0d1117',
            plot_bgcolor: '#161b22',
            font: {{ color: '#e6edf3' }},
            xaxis: {{ gridcolor: '#30363d' }},
            yaxis: {{ gridcolor: '#30363d' }}
        }};

        // 出来高フィルタ効果チャート
        Plotly.newPlot('volume-chart', [
            {{
                x: ['全日', '1.5倍', '2倍', '3倍', '5倍'],
                y: [0.25, 0.59, 0.91, 1.21, 1.69],
                type: 'bar',
                name: '平均損益',
                marker: {{ color: '#3fb950' }}
            }},
            {{
                x: ['全日', '1.5倍', '2倍', '3倍', '5倍'],
                y: [53.6, 57.0, 59.5, 60.8, 62.8],
                type: 'scatter',
                mode: 'lines+markers',
                name: '勝率(%)',
                yaxis: 'y2',
                marker: {{ color: '#58a6ff' }}
            }}
        ], {{
            ...darkLayout,
            title: '出来高倍率別の成績',
            xaxis: {{ title: '出来高倍率', gridcolor: '#30363d' }},
            yaxis: {{ title: '平均損益(%)', gridcolor: '#30363d' }},
            yaxis2: {{
                title: '勝率(%)',
                overlaying: 'y',
                side: 'right',
                gridcolor: '#30363d'
            }},
            legend: {{ orientation: 'h', y: -0.2 }}
        }});

        // イグジット比較チャート
        Plotly.newPlot('exit-chart', [{{
            x: ['大引け', '14:00', '14:30', 'トレーリング'],
            y: [0.91, 0.78, 0.85, 0.61],
            type: 'bar',
            marker: {{
                color: ['#3fb950', '#58a6ff', '#58a6ff', '#8b949e']
            }}
        }}], {{
            ...darkLayout,
            title: 'イグジットタイミング別 平均損益',
            xaxis: {{ title: 'イグジット', gridcolor: '#30363d' }},
            yaxis: {{ title: '平均損益(%)', gridcolor: '#30363d' }},
            showlegend: false
        }});

        // 損益分布'''

    # 損益データ
    pnl_data = df_exit['pnl_close'].clip(-10, 10).tolist()

    html += f'''
        var pnlData = {pnl_data};

        Plotly.newPlot('pnl-hist', [{{
            x: pnlData,
            type: 'histogram',
            marker: {{ color: '#58a6ff' }},
            xbins: {{ size: 0.5 }}
        }}], {{
            ...darkLayout,
            title: '損益分布（大引け決済）',
            xaxis: {{ title: '損益(%)', range: [-10, 10], gridcolor: '#30363d' }},
            yaxis: {{ title: '頻度', gridcolor: '#30363d' }},
            shapes: [{{
                type: 'line',
                x0: 0, x1: 0,
                y0: 0, y1: 1,
                yref: 'paper',
                line: {{ color: '#f85149', width: 2, dash: 'dash' }}
            }}]
        }});
    </script>
</body>
</html>'''

    output_path = OUTPUT_DIR / "short_strategy_backtest.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"HTML生成完了: {output_path}")


if __name__ == '__main__':
    generate_html()
