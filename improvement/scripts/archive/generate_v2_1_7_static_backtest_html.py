#!/usr/bin/env python3
"""
v2.1.7バックテスト結果のHTML可視化
データドリブンスコアリング + 買いシグナルの2段階分類 + アラート機能
"""
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'improvement' / 'data'
OUTPUT_DIR = BASE_DIR / 'improvement'
RESULTS_FILE = DATA_DIR / 'v2_1_7_backtest_results.parquet'
OUTPUT_HTML = OUTPUT_DIR / 'v2_1_7_static_backtest_report.html'

def generate_html_report():
    """HTML レポート生成"""

    # データ読み込み
    df = pd.read_parquet(RESULTS_FILE)
    df['date'] = pd.to_datetime(df['date'])

    # 銘柄名マッピング作成
    stocks_file = BASE_DIR / 'data' / 'parquet' / 'all_stocks.parquet'
    stocks_df = pd.read_parquet(stocks_file)
    ticker_to_name = dict(zip(stocks_df['ticker'], stocks_df['stock_name']))

    # 基本統計
    total_count = len(df)
    strong_buy_clean_count = (df['action'] == 'strong_buy_clean').sum()
    strong_buy_alert_count = (df['action'] == 'strong_buy_alert').sum()
    buy_count = (df['action'] == 'buy').sum()
    sell_count = (df['action'] == 'sell').sum()
    hold_count = (df['action'] == 'hold').sum()

    strong_buy_clean_df = df[df['action'] == 'strong_buy_clean']
    strong_buy_alert_df = df[df['action'] == 'strong_buy_alert']
    buy_df = df[df['action'] == 'buy']
    sell_df = df[df['action'] == 'sell']
    hold_df = df[df['action'] == 'hold']

    # strong_buy_cleanシグナル統計
    clean_win_rate = (strong_buy_clean_df['win'] == True).sum() / len(strong_buy_clean_df) * 100 if len(strong_buy_clean_df) > 0 else 0
    clean_avg_profit = strong_buy_clean_df['profit_100'].mean() if len(strong_buy_clean_df) > 0 else 0
    clean_total_profit = strong_buy_clean_df['profit_100'].sum() if len(strong_buy_clean_df) > 0 else 0

    # strong_buy_alertシグナル統計
    alert_win_rate = (strong_buy_alert_df['win'] == True).sum() / len(strong_buy_alert_df) * 100 if len(strong_buy_alert_df) > 0 else 0
    alert_avg_profit = strong_buy_alert_df['profit_100'].mean() if len(strong_buy_alert_df) > 0 else 0
    alert_total_profit = strong_buy_alert_df['profit_100'].sum() if len(strong_buy_alert_df) > 0 else 0

    # buyシグナル統計
    buy_win_rate = (buy_df['win'] == True).sum() / len(buy_df) * 100 if len(buy_df) > 0 else 0
    buy_avg_profit = buy_df['profit_100'].mean() if len(buy_df) > 0 else 0
    buy_total_profit = buy_df['profit_100'].sum() if len(buy_df) > 0 else 0

    # 売りシグナル統計
    sell_win_rate = (sell_df['win'] == True).sum() / len(sell_df) * 100 if len(sell_df) > 0 else 0
    sell_avg_profit = sell_df['profit_100'].mean() if len(sell_df) > 0 else 0
    sell_total_profit = sell_df['profit_100'].sum() if len(sell_df) > 0 else 0

    # 2025年10月・11月のシグナル抽出
    oct_nov_2025 = df[(df['date'] >= '2025-10-01') & (df['date'] < '2025-12-01')]
    signals_2025 = oct_nov_2025[oct_nov_2025['action'].isin(['strong_buy_clean', 'strong_buy_alert', 'buy', 'sell'])]
    signals_2025 = signals_2025.sort_values('date', ascending=False)

    # HTML生成
    html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>v2.1.7 バックテストレポート（アラート機能付き）</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            line-height: 1.6;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-left: 4px solid #3498db;
            padding-left: 10px;
        }}
        .summary {{
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        th {{
            background: #3498db;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .positive {{
            color: #27ae60;
            font-weight: bold;
        }}
        .negative {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
        }}
        .badge-clean {{
            background: #27ae60;
            color: white;
        }}
        .badge-alert {{
            background: #f39c12;
            color: white;
        }}
        .badge-buy {{
            background: #3498db;
            color: white;
        }}
        .badge-sell {{
            background: #e74c3c;
            color: white;
        }}
        .success-box {{
            background: #d4edda;
            border-left: 4px solid #27ae60;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }}
        .alert-box {{
            background: #fff3cd;
            border-left: 4px solid #f39c12;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }}
        .danger-box {{
            background: #f8d7da;
            border-left: 4px solid #e74c3c;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }}
        .signal-item {{
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 12px;
            margin: 8px 0;
        }}
        .signal-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }}
        .signal-details {{
            font-size: 13px;
            color: #666;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 8px;
        }}
        .alert-flags {{
            background: #fff3cd;
            border-left: 3px solid #f39c12;
            padding: 8px;
            margin-top: 8px;
            font-size: 12px;
            color: #856404;
        }}
        .danger-flags {{
            background: #f8d7da;
            border-left: 3px solid #e74c3c;
            padding: 8px;
            margin-top: 8px;
            font-size: 12px;
            color: #721c24;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 v2.1.7 静的バックテスト分析レポート</h1>

        <div class="summary">
            <strong>データドリブンスコアリング + 買いシグナルの2段階分類 + アラート機能</strong><br>
            対象: 政策銘柄 + TOPIX_CORE30（2020-2025）<br>
            総判定数: {total_count:,}件
        </div>

        <h2>📈 アクション別統計（全期間）</h2>
        <table>
            <thead>
                <tr>
                    <th>アクション</th>
                    <th>件数</th>
                    <th>勝率</th>
                    <th>平均利益/100株</th>
                    <th>合計利益</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><span class="badge badge-clean">✅ strong_buy_clean</span></td>
                    <td>{strong_buy_clean_count:,}件</td>
                    <td class="positive">{clean_win_rate:.2f}%</td>
                    <td class="positive">{clean_avg_profit:,.0f}円</td>
                    <td class="positive">{clean_total_profit:,.0f}円</td>
                </tr>
                <tr>
                    <td><span class="badge badge-alert">⚠️ strong_buy_alert</span></td>
                    <td>{strong_buy_alert_count:,}件</td>
                    <td>{alert_win_rate:.2f}%</td>
                    <td>{alert_avg_profit:,.0f}円</td>
                    <td>{alert_total_profit:,.0f}円</td>
                </tr>
                <tr>
                    <td><span class="badge badge-buy">🔵 buy</span></td>
                    <td>{buy_count:,}件</td>
                    <td>{buy_win_rate:.2f}%</td>
                    <td>{buy_avg_profit:,.0f}円</td>
                    <td>{buy_total_profit:,.0f}円</td>
                </tr>
                <tr>
                    <td><span class="badge badge-sell">🔴 sell</span></td>
                    <td>{sell_count:,}件</td>
                    <td>{sell_win_rate:.2f}%</td>
                    <td>{sell_avg_profit:,.0f}円</td>
                    <td>{sell_total_profit:,.0f}円</td>
                </tr>
            </tbody>
        </table>

        <h2>🎯 strong_buy の詳細分析</h2>

        <div class="success-box">
            <h3>✅ strong_buy_clean（アラートなし）</h3>
            <ul>
                <li>件数: {strong_buy_clean_count:,}件</li>
                <li>勝率: <strong>{clean_win_rate:.2f}%</strong></li>
                <li>平均利益: <strong>{clean_avg_profit:,.0f}円/100株</strong></li>
                <li>合計利益: <strong>{clean_total_profit:,.0f}円</strong></li>
            </ul>
        </div>

        <div class="alert-box">
            <h3>⚠️ strong_buy_alert（アラート付き）</h3>
            <ul>
                <li>件数: {strong_buy_alert_count:,}件</li>
                <li>勝率: {alert_win_rate:.2f}%</li>
                <li>平均利益: {alert_avg_profit:,.0f}円/100株</li>
                <li>合計利益: {alert_total_profit:,.0f}円</li>
            </ul>
        </div>

        <div class="danger-box">
            <h3>🚨 アラート条件（データから判明した危険パターン）</h3>
            <ul>
                <li><strong>RSI 40-50:</strong> 危険ゾーン（平均-2,599円）</li>
                <li><strong>RSI 50-60:</strong> 中立域警告（平均-199円）</li>
                <li><strong>RSI 60-70:</strong> 買われすぎ警告（平均-298円）</li>
                <li><strong>出来高 1.2-1.5倍:</strong> 超危険ゾーン（平均-10,337円）※最も危険！</li>
                <li><strong>SMA5 -5%～0%:</strong> 中途半端な押し目（平均-1,417円）</li>
            </ul>
        </div>

        <p>
            <strong>比較:</strong> strong_buy_clean の平均利益は strong_buy_alert の
            <strong class="positive">{clean_avg_profit / alert_avg_profit if alert_avg_profit > 0 else 0:.1f}倍</strong>
        </p>
        <p>
            strong_buy全体（{strong_buy_clean_count + strong_buy_alert_count:,}件）のうち、
            <strong>{strong_buy_alert_count / (strong_buy_clean_count + strong_buy_alert_count) * 100:.1f}%</strong>
            がアラート付き
        </p>

        <h2>📅 2025年10月・11月のシグナルリスト</h2>
        <p>全{len(signals_2025):,}件のシグナル（降順）</p>

        <table style="font-size: 13px;">
            <thead>
                <tr>
                    <th>日付</th>
                    <th>銘柄</th>
                    <th>アクション</th>
                    <th>スコア<br>(買/売)</th>
                    <th>終値</th>
                    <th>RSI</th>
                    <th>出来高</th>
                    <th>SMA5<br>乖離</th>
                    <th>結果</th>
                    <th>アラート</th>
                </tr>
            </thead>
            <tbody>
"""

    # 2025年10月・11月のシグナルリスト生成（テーブル形式）
    for _, signal in signals_2025.iterrows():
        action = signal['action']
        if action == 'strong_buy_clean':
            badge_html = '<span class="badge badge-clean">✅clean</span>'
            row_bg = 'background: #d4edda;'
        elif action == 'strong_buy_alert':
            badge_html = '<span class="badge badge-alert">⚠️alert</span>'
            row_bg = 'background: #fff3cd;'
        elif action == 'buy':
            badge_html = '<span class="badge badge-buy">🔵buy</span>'
            row_bg = ''
        elif action == 'sell':
            badge_html = '<span class="badge badge-sell">🔴sell</span>'
            row_bg = ''
        else:
            badge_html = '<span class="badge">⚪' + action + '</span>'
            row_bg = ''

        date_str = signal['date'].strftime('%m/%d')
        ticker = signal['ticker']
        stock_name = ticker_to_name.get(ticker, ticker)
        score_buy = signal['score_buy']
        score_sell = signal['score_sell']
        close = signal['close']
        next_open = signal.get('next_open', None)
        next_close = signal.get('next_close', None)
        profit_100 = signal.get('profit_100', 0)
        win = signal.get('win', None)
        rsi = signal.get('rsi_14d', None)
        volume_change = signal.get('volume_change_20d', None)
        sma5_pct = signal.get('price_vs_sma5_pct', None)
        alert_flags = signal.get('alert_flags', '')

        # Format values
        rsi_str = f"{rsi:.1f}" if pd.notna(rsi) else "-"
        volume_str = f"{volume_change:.2f}" if pd.notna(volume_change) else "-"
        sma5_str = f"{sma5_pct:.1f}%" if pd.notna(sma5_pct) else "-"

        # 結果の表示
        if pd.notna(next_open) and pd.notna(next_close):
            if win is True:
                result_icon = "✅"
                result_color = "#27ae60"
            elif win is False:
                result_icon = "❌"
                result_color = "#e74c3c"
            else:
                result_icon = "➖"
                result_color = "#95a5a6"

            result_str = f'{result_icon} {next_open:,.0f}→{next_close:,.0f} ({profit_100:+,.0f}円)'
        else:
            result_str = "-"
            result_color = "#666"

        # アラートを簡潔に表示
        if alert_flags:
            # 🚨の数を数える
            danger_count = alert_flags.count('🚨')
            warning_count = alert_flags.count('⚠️')
            if danger_count > 0:
                alert_display = f'🚨×{danger_count}'
                if warning_count > 0:
                    alert_display += f' ⚠️×{warning_count}'
                alert_color = '#e74c3c'
            else:
                alert_display = f'⚠️×{warning_count}'
                alert_color = '#f39c12'
        else:
            alert_display = '-'
            alert_color = '#666'

        html_content += f"""
                <tr style="{row_bg}">
                    <td>{date_str}</td>
                    <td style="white-space: nowrap;">{ticker}<br><small>{stock_name}</small></td>
                    <td>{badge_html}</td>
                    <td style="text-align: center;">{score_buy}/{score_sell}</td>
                    <td style="text-align: right;">{close:,.0f}円</td>
                    <td style="text-align: right;">{rsi_str}</td>
                    <td style="text-align: right;">{volume_str}倍</td>
                    <td style="text-align: right;">{sma5_str}</td>
                    <td style="color: {result_color}; font-weight: bold; white-space: nowrap;">{result_str}</td>
                    <td style="color: {alert_color}; text-align: center;" title="{alert_flags}">{alert_display}</td>
                </tr>
"""

    # HTML終了
    html_content += f"""
            </tbody>
        </table>

        <div style="text-align: right; color: #7f8c8d; font-size: 14px; margin-top: 30px;">
            生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""

    # ファイル保存
    OUTPUT_HTML.write_text(html_content, encoding='utf-8')
    print(f"✅ HTMLレポート生成完了: {OUTPUT_HTML}")
    print(f"   2025年10月・11月シグナル数: {len(signals_2025):,}件")
    print(f"   ファイルサイズ: {OUTPUT_HTML.stat().st_size / 1024:.1f} KB")

if __name__ == '__main__':
    generate_html_report()
