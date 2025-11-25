#!/usr/bin/env python3
"""
売買タイミング分析結果をHTMLファイルにまとめる
"""
import pandas as pd
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'test_output'
DATA_DIR = ROOT / 'data' / 'parquet'

# 入力ファイル
TIMING_ANALYSIS = OUTPUT_DIR / 'timing_analysis_results.parquet'
META_JQUANTS = DATA_DIR / 'meta_jquants.parquet'

# 出力ファイル
HTML_OUTPUT = OUTPUT_DIR / 'timing_analysis_report.html'


def generate_html_report():
    """HTMLレポートを生成"""

    # データ読み込み
    df = pd.read_parquet(TIMING_ANALYSIS)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    # meta_jquants.parquetから企業名を取得
    meta_df = pd.read_parquet(META_JQUANTS)
    ticker_to_name = dict(zip(meta_df['ticker'], meta_df['stock_name']))

    # 企業名をマージ
    df['company_name'] = df['ticker'].map(ticker_to_name).fillna('N/A')

    # サマリー統計
    total_count = len(df)
    morning_better_count = (df['better_profit_timing'] == 'morning_close').sum()
    day_close_better_count = (df['better_profit_timing'] == 'day_close').sum()

    avg_profit_morning = df['profit_morning_pct'].mean()
    avg_profit_day_close = df['profit_day_close_pct'].mean()

    morning_win_rate = df['is_win_morning'].sum() / total_count * 100
    day_close_win_rate = df['is_win_day_close'].sum() / total_count * 100

    morning_better_loss_count = (df['better_loss_timing'] == 'morning_close').sum()
    day_close_better_loss_count = (df['better_loss_timing'] == 'day_close').sum()

    # HTML生成
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>売買タイミング最適化分析レポート</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #e2e8f0;
            padding: 2rem;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        h1 {{
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
            color: #60a5fa;
            text-align: center;
        }}

        .subtitle {{
            text-align: center;
            color: #94a3b8;
            margin-bottom: 2rem;
            font-size: 1.1rem;
        }}

        .summary {{
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
            backdrop-filter: blur(10px);
        }}

        .summary h2 {{
            font-size: 1.5rem;
            margin-bottom: 1rem;
            color: #60a5fa;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-top: 1.5rem;
        }}

        .stat-card {{
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 1.5rem;
        }}

        .stat-label {{
            font-size: 0.9rem;
            color: #94a3b8;
            margin-bottom: 0.5rem;
        }}

        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
        }}

        .stat-value.positive {{
            color: #34d399;
        }}

        .stat-value.negative {{
            color: #f87171;
        }}

        .stat-value.neutral {{
            color: #60a5fa;
        }}

        .comparison {{
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
        }}

        .comparison h2 {{
            font-size: 1.5rem;
            margin-bottom: 1.5rem;
            color: #60a5fa;
        }}

        .comparison-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem;
            background: rgba(15, 23, 42, 0.4);
            border-radius: 8px;
            margin-bottom: 1rem;
        }}

        .comparison-label {{
            font-size: 1.1rem;
            font-weight: 600;
        }}

        .comparison-value {{
            font-size: 1.3rem;
            font-weight: bold;
        }}

        .progress-bar {{
            width: 100%;
            height: 30px;
            background: rgba(15, 23, 42, 0.6);
            border-radius: 15px;
            overflow: hidden;
            margin: 1rem 0;
            position: relative;
        }}

        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #3b82f6, #60a5fa);
            transition: width 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 1rem;
            font-size: 0.9rem;
            font-weight: bold;
        }}

        .table-container {{
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
            overflow-x: auto;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}

        thead {{
            background: rgba(15, 23, 42, 0.8);
        }}

        th {{
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            color: #60a5fa;
            border-bottom: 2px solid #334155;
        }}

        td {{
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #334155;
        }}

        tr:hover {{
            background: rgba(51, 65, 85, 0.3);
        }}

        .positive {{
            color: #34d399;
        }}

        .negative {{
            color: #f87171;
        }}

        .badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.85rem;
            font-weight: 600;
        }}

        .badge-morning {{
            background: rgba(251, 191, 36, 0.2);
            color: #fbbf24;
            border: 1px solid #fbbf24;
        }}

        .badge-day {{
            background: rgba(96, 165, 250, 0.2);
            color: #60a5fa;
            border: 1px solid #60a5fa;
        }}

        .footer {{
            text-align: center;
            color: #64748b;
            margin-top: 3rem;
            padding-top: 2rem;
            border-top: 1px solid #334155;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 売買タイミング最適化分析レポート</h1>
        <p class="subtitle">利確・損切りタイミングの比較分析（2025-11-04 ~ 2025-11-14）</p>

        <!-- サマリー統計 -->
        <div class="summary">
            <h2>📈 サマリー統計</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">分析銘柄数</div>
                    <div class="stat-value neutral">{total_count}件</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">平均利益率（前場不成）</div>
                    <div class="stat-value {'positive' if avg_profit_morning > 0 else 'negative'}">{avg_profit_morning:.2f}%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">平均利益率（大引不成）</div>
                    <div class="stat-value {'positive' if avg_profit_day_close > 0 else 'negative'}">{avg_profit_day_close:.2f}%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">勝率（前場不成）</div>
                    <div class="stat-value neutral">{morning_win_rate:.1f}%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">勝率（大引不成）</div>
                    <div class="stat-value neutral">{day_close_win_rate:.1f}%</div>
                </div>
            </div>
        </div>

        <!-- 利確タイミング比較 -->
        <div class="comparison">
            <h2>💰 利確タイミング比較</h2>
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">どちらのタイミングで売却した方が利益が大きかったか</p>

            <div class="comparison-row">
                <span class="comparison-label">前場不成（11:30）</span>
                <span class="comparison-value" style="color: #fbbf24;">{morning_better_count}件 ({morning_better_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {morning_better_count/total_count*100}%; background: linear-gradient(90deg, #fbbf24, #fcd34d);">
                    {morning_better_count/total_count*100:.1f}%
                </div>
            </div>

            <div class="comparison-row">
                <span class="comparison-label">大引不成（15:30）</span>
                <span class="comparison-value" style="color: #60a5fa;">{day_close_better_count}件 ({day_close_better_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {day_close_better_count/total_count*100}%;">
                    {day_close_better_count/total_count*100:.1f}%
                </div>
            </div>

            <div style="margin-top: 2rem; padding: 1rem; background: rgba(59, 130, 246, 0.1); border-left: 4px solid #3b82f6; border-radius: 4px;">
                <strong style="color: #60a5fa;">結論:</strong>
                <span style="color: #e2e8f0;">
                    大引不成（15:30）の方が有利なケースが多い（{day_close_better_count/total_count*100:.1f}%）
                </span>
            </div>
        </div>

        <!-- 損切りタイミング比較 -->
        <div class="comparison">
            <h2>🛡️ 損切りタイミング比較</h2>
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">どちらのタイミングで損切りした方が損失が小さかったか</p>

            <div class="comparison-row">
                <span class="comparison-label">前場損切り（11:30）</span>
                <span class="comparison-value" style="color: #fbbf24;">{morning_better_loss_count}件 ({morning_better_loss_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {morning_better_loss_count/total_count*100}%; background: linear-gradient(90deg, #fbbf24, #fcd34d);">
                    {morning_better_loss_count/total_count*100:.1f}%
                </div>
            </div>

            <div class="comparison-row">
                <span class="comparison-label">大引精算（15:30）</span>
                <span class="comparison-value" style="color: #60a5fa;">{day_close_better_loss_count}件 ({day_close_better_loss_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {day_close_better_loss_count/total_count*100}%;">
                    {day_close_better_loss_count/total_count*100:.1f}%
                </div>
            </div>

            <div style="margin-top: 2rem; padding: 1rem; background: rgba(59, 130, 246, 0.1); border-left: 4px solid #3b82f6; border-radius: 4px;">
                <strong style="color: #60a5fa;">結論:</strong>
                <span style="color: #e2e8f0;">
                    大引精算（15:30）の方が損失が小さいケースが多い（{day_close_better_loss_count/total_count*100:.1f}%）
                </span>
            </div>
        </div>

        <!-- 詳細データテーブル -->
        <div class="table-container">
            <h2 style="color: #60a5fa; margin-bottom: 1.5rem;">📋 銘柄別詳細データ</h2>
            <table>
                <thead>
                    <tr>
                        <th>日付</th>
                        <th>銘柄コード</th>
                        <th>企業名</th>
                        <th>売買推奨</th>
                        <th>買値</th>
                        <th>前場終値</th>
                        <th>前場利益率</th>
                        <th>大引値</th>
                        <th>大引利益率</th>
                        <th>有利なタイミング</th>
                        <th>最高値</th>
                        <th>最安値</th>
                    </tr>
                </thead>
                <tbody>
"""

    # テーブル行を生成
    for _, row in df.sort_values('backtest_date', ascending=False).iterrows():
        better_timing_badge = f'<span class="badge badge-morning">前場</span>' if row['better_profit_timing'] == 'morning_close' else f'<span class="badge badge-day">大引</span>'
        profit_morning_class = 'positive' if row['profit_morning_pct'] > 0 else 'negative'
        profit_day_class = 'positive' if row['profit_day_close_pct'] > 0 else 'negative'

        # 売買推奨のバッジ
        rec_action = row.get('recommendation_action')
        if rec_action == 'buy':
            rec_badge = '<span class="badge" style="background: rgba(34, 197, 94, 0.2); color: #22c55e; border: 1px solid #22c55e;">買い</span>'
        elif rec_action == 'sell':
            rec_badge = '<span class="badge" style="background: rgba(239, 68, 68, 0.2); color: #ef4444; border: 1px solid #ef4444;">売り</span>'
        elif rec_action == 'hold':
            rec_badge = '<span class="badge" style="background: rgba(251, 191, 36, 0.2); color: #fbbf24; border: 1px solid #fbbf24;">静観</span>'
        else:
            rec_badge = '<span class="badge" style="background: rgba(100, 116, 139, 0.2); color: #64748b; border: 1px solid #64748b;">N/A</span>'

        html += f"""                    <tr>
                        <td>{row['backtest_date'].strftime('%Y-%m-%d')}</td>
                        <td><strong>{row['ticker']}</strong></td>
                        <td>{row['company_name']}</td>
                        <td>{rec_badge}</td>
                        <td>{row['buy_price']:.0f}円</td>
                        <td>{row['morning_close_price']:.0f}円</td>
                        <td class="{profit_morning_class}">{row['profit_morning_pct']:+.2f}%</td>
                        <td>{row['day_close_price']:.0f}円</td>
                        <td class="{profit_day_class}">{row['profit_day_close_pct']:+.2f}%</td>
                        <td>{better_timing_badge}</td>
                        <td class="positive">{row['day_high']:.0f}円 ({row['max_gain_pct']:+.2f}%)</td>
                        <td class="negative">{row['day_low']:.0f}円 ({row['max_loss_pct']:+.2f}%)</td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>

        <div class="footer">
            <p>分析期間: 2025-11-04 ~ 2025-11-14</p>
            <p>データソース: Yahoo Finance (5分足データ)</p>
            <p>生成日時: """ + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
        </div>
    </div>
</body>
</html>
"""

    # HTMLファイルを保存
    with open(HTML_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ HTMLレポートを生成しました: {HTML_OUTPUT}")
    print(f"   ファイルサイズ: {HTML_OUTPUT.stat().st_size / 1024:.1f} KB")


if __name__ == '__main__':
    generate_html_report()
