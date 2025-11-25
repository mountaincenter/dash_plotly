#!/usr/bin/env python3
"""
v2.1.5バックテスト結果のHTML可視化
データドリブンスコアリング + 価格フィルタ（≥1,000円）
"""
from pathlib import Path
import pandas as pd
import numpy as np

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'improvement' / 'data'
OUTPUT_DIR = BASE_DIR / 'improvement'
RESULTS_FILE = DATA_DIR / 'v2_1_5_backtest_results.parquet'
OUTPUT_HTML = OUTPUT_DIR / 'v2_1_5_static_backtest_report.html'

def generate_html_report():
    """HTML レポート生成"""

    # データ読み込み
    df = pd.read_parquet(RESULTS_FILE)

    # 基本統計
    total_count = len(df)
    buy_count = (df['action'] == '買い').sum()
    sell_count = (df['action'] == '売り').sum()
    hold_count = (df['action'] == '静観').sum()
    filtered_count = df['price_filtered'].sum()

    buy_df = df[df['action'] == '買い']
    sell_df = df[df['action'] == '売り']

    # 買いシグナル統計
    buy_win_rate = (buy_df['win'] == True).sum() / len(buy_df) * 100 if len(buy_df) > 0 else 0
    buy_avg_profit = buy_df['profit_100'].mean() if len(buy_df) > 0 else 0
    buy_total_profit = buy_df['profit_100'].sum() if len(buy_df) > 0 else 0

    # 売りシグナル統計
    sell_win_rate = (sell_df['win'] == True).sum() / len(sell_df) * 100 if len(sell_df) > 0 else 0
    sell_avg_profit = sell_df['profit_100'].mean() if len(sell_df) > 0 else 0
    sell_total_profit = sell_df['profit_100'].sum() if len(sell_df) > 0 else 0

    # RSI範囲別統計（買い）
    rsi_ranges = [
        (0, 10, 'RSI 0-10'),
        (10, 20, 'RSI 10-20'),
        (20, 30, 'RSI 20-30'),
        (30, 40, 'RSI 30-40'),
        (40, 50, 'RSI 40-50'),
        (50, 60, 'RSI 50-60'),
        (60, 70, 'RSI 60-70'),
        (70, 80, 'RSI 70-80'),
        (80, 100, 'RSI 80-100')
    ]

    rsi_stats_html = ""
    for min_val, max_val, label in rsi_ranges:
        subset = buy_df[(buy_df['rsi_14d'] >= min_val) & (buy_df['rsi_14d'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['win'] == True).sum() / count * 100
            avg_profit = subset['profit_100'].mean()
            total_profit = subset['profit_100'].sum()

            bg_color = '#e8f5e9' if avg_profit > 500 else '#fff3e0' if avg_profit > 0 else '#ffebee'
            rsi_stats_html += f"""
            <tr style="background-color: {bg_color}">
                <td>{label}</td>
                <td>{count:,}件</td>
                <td>{win_rate:.2f}%</td>
                <td>{avg_profit:,.0f}円</td>
                <td>{total_profit:,.0f}円</td>
            </tr>
            """

    # 出来高範囲別統計（買い）
    volume_ranges = [
        (0, 0.5, '< 0.5倍'),
        (0.5, 0.8, '0.5-0.8倍'),
        (0.8, 1.0, '0.8-1.0倍'),
        (1.0, 1.2, '1.0-1.2倍'),
        (1.2, 1.5, '1.2-1.5倍'),
        (1.5, 2.0, '1.5-2.0倍'),
        (2.0, 3.0, '2.0-3.0倍'),
        (3.0, 100, '> 3.0倍')
    ]

    volume_stats_html = ""
    for min_val, max_val, label in volume_ranges:
        subset = buy_df[(buy_df['volume_change_20d'] >= min_val) & (buy_df['volume_change_20d'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['win'] == True).sum() / count * 100
            avg_profit = subset['profit_100'].mean()
            total_profit = subset['profit_100'].sum()

            bg_color = '#e8f5e9' if avg_profit > 500 else '#fff3e0' if avg_profit > 0 else '#ffebee'
            volume_stats_html += f"""
            <tr style="background-color: {bg_color}">
                <td>{label}</td>
                <td>{count:,}件</td>
                <td>{win_rate:.2f}%</td>
                <td>{avg_profit:,.0f}円</td>
                <td>{total_profit:,.0f}円</td>
            </tr>
            """

    # SMA5乖離率別統計（買い）
    sma5_ranges = [
        (-100, -10, '< -10%'),
        (-10, -5, '-10% ~ -5%'),
        (-5, -2, '-5% ~ -2%'),
        (-2, 0, '-2% ~ 0%'),
        (0, 2, '0% ~ 2%'),
        (2, 5, '2% ~ 5%'),
        (5, 10, '5% ~ 10%'),
        (10, 100, '> 10%')
    ]

    sma5_stats_html = ""
    for min_val, max_val, label in sma5_ranges:
        subset = buy_df[(buy_df['price_vs_sma5_pct'] >= min_val) & (buy_df['price_vs_sma5_pct'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['win'] == True).sum() / count * 100
            avg_profit = subset['profit_100'].mean()
            total_profit = subset['profit_100'].sum()

            bg_color = '#e8f5e9' if avg_profit > 1000 else '#fff3e0' if avg_profit > 0 else '#ffebee'
            sma5_stats_html += f"""
            <tr style="background-color: {bg_color}">
                <td>{label}</td>
                <td>{count:,}件</td>
                <td>{win_rate:.2f}%</td>
                <td>{avg_profit:,.0f}円</td>
                <td>{total_profit:,.0f}円</td>
            </tr>
            """

    # 価格帯別統計（買い）
    price_ranges = [
        (0, 500, '< 500円'),
        (500, 1000, '500-1,000円'),
        (1000, 2000, '1,000-2,000円'),
        (2000, 5000, '2,000-5,000円'),
        (5000, 10000, '5,000-10,000円'),
        (10000, 1000000, '≥ 10,000円')
    ]

    price_stats_html = ""
    for min_val, max_val, label in price_ranges:
        subset = buy_df[(buy_df['close'] >= min_val) & (buy_df['close'] < max_val)]
        if len(subset) > 0:
            count = len(subset)
            win_rate = (subset['win'] == True).sum() / count * 100
            avg_profit = subset['profit_100'].mean()
            total_profit = subset['profit_100'].sum()

            bg_color = '#e8f5e9' if avg_profit > 500 else '#fff3e0' if avg_profit > 0 else '#ffebee'
            price_stats_html += f"""
            <tr style="background-color: {bg_color}">
                <td>{label}</td>
                <td>{count:,}件</td>
                <td>{win_rate:.2f}%</td>
                <td>{avg_profit:,.0f}円</td>
                <td>{total_profit:,.0f}円</td>
            </tr>
            """

    # v2.1.4との比較データ（参考値）
    v214_buy_count = 5410
    v214_buy_win_rate = 50.59
    v214_buy_avg_profit = 707
    v214_buy_total_profit = 3825638

    comparison_html = f"""
    <div class="comparison-section">
        <h2>📊 v2.1.4 vs v2.1.5 比較</h2>
        <table>
            <tr>
                <th>指標</th>
                <th>v2.1.4（価格フィルタなし）</th>
                <th>v2.1.5（≥1,000円フィルタ）</th>
                <th>差分</th>
            </tr>
            <tr>
                <td>買いシグナル数</td>
                <td>{v214_buy_count:,}件</td>
                <td>{buy_count:,}件</td>
                <td style="color: orange">{buy_count - v214_buy_count:+,}件 ({(buy_count - v214_buy_count) / v214_buy_count * 100:+.1f}%)</td>
            </tr>
            <tr>
                <td>買い勝率</td>
                <td>{v214_buy_win_rate:.2f}%</td>
                <td>{buy_win_rate:.2f}%</td>
                <td style="color: green; font-weight: bold">{buy_win_rate - v214_buy_win_rate:+.2f}%</td>
            </tr>
            <tr>
                <td>買い平均利益</td>
                <td>{v214_buy_avg_profit:,.0f}円</td>
                <td>{buy_avg_profit:,.0f}円</td>
                <td style="color: green; font-weight: bold">{buy_avg_profit - v214_buy_avg_profit:+,.0f}円 ({(buy_avg_profit - v214_buy_avg_profit) / v214_buy_avg_profit * 100:+.1f}%)</td>
            </tr>
            <tr>
                <td>買い合計利益</td>
                <td>{v214_buy_total_profit:,.0f}円</td>
                <td>{buy_total_profit:,.0f}円</td>
                <td style="color: green">{buy_total_profit - v214_buy_total_profit:+,.0f}円</td>
            </tr>
            <tr>
                <td>除外された買いシグナル</td>
                <td>-</td>
                <td>{filtered_count:,}件</td>
                <td style="color: gray">1,000円未満で除外</td>
            </tr>
        </table>
        <div style="margin-top: 20px; padding: 15px; background-color: #e3f2fd; border-left: 4px solid #2196f3;">
            <strong>改善ポイント:</strong><br>
            • 価格フィルタにより勝率が <strong>+{buy_win_rate - v214_buy_win_rate:.2f}%</strong> 向上<br>
            • 平均利益が <strong>+{buy_avg_profit - v214_buy_avg_profit:.0f}円 (+{(buy_avg_profit - v214_buy_avg_profit) / v214_buy_avg_profit * 100:.1f}%)</strong> 改善<br>
            • シグナル数を24.5%削減しつつ、総利益はほぼ維持<br>
            • 低品質な1,326件のシグナルを除外し、精度向上を実現
        </div>
    </div>
    """

    # HTML生成
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>v2.1.5 バックテストレポート（価格フィルタ付き）</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #1976d2;
                border-bottom: 3px solid #1976d2;
                padding-bottom: 10px;
            }}
            h2 {{
                color: #333;
                margin-top: 30px;
                border-bottom: 2px solid #ddd;
                padding-bottom: 8px;
            }}
            .summary {{
                background-color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 20px;
            }}
            .stat-card {{
                background-color: white;
                padding: 15px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .stat-label {{
                color: #666;
                font-size: 0.9em;
                margin-bottom: 5px;
            }}
            .stat-value {{
                font-size: 1.8em;
                font-weight: bold;
                color: #1976d2;
            }}
            .buy-signal {{
                color: #2e7d32;
            }}
            .sell-signal {{
                color: #c62828;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #1976d2;
                color: white;
                font-weight: bold;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .comparison-section {{
                background-color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <h1>v2.1.5 静的バックテストレポート</h1>
        <div class="summary">
            <p><strong>分析期間:</strong> 2020-2025（5年間）</p>
            <p><strong>対象銘柄:</strong> 政策銘柄 + TOPIX_CORE30（56銘柄）</p>
            <p><strong>スコアリングロジック:</strong> データドリブン（実績ベース配点）+ 価格フィルタ（買い: ≥1,000円）</p>
            <p><strong>総判定数:</strong> {total_count:,}件</p>
        </div>

        {comparison_html}

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">買いシグナル</div>
                <div class="stat-value buy-signal">{buy_count:,}件</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">買い勝率</div>
                <div class="stat-value buy-signal">{buy_win_rate:.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">買い平均利益</div>
                <div class="stat-value buy-signal">{buy_avg_profit:,.0f}円</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">買い合計利益</div>
                <div class="stat-value buy-signal">{buy_total_profit:,.0f}円</div>
            </div>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">売りシグナル</div>
                <div class="stat-value sell-signal">{sell_count:,}件</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">売り勝率</div>
                <div class="stat-value sell-signal">{sell_win_rate:.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">売り平均利益</div>
                <div class="stat-value sell-signal">{sell_avg_profit:,.0f}円</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">売り合計利益</div>
                <div class="stat-value sell-signal">{sell_total_profit:,.0f}円</div>
            </div>
        </div>

        <h2>📈 RSI範囲別分析（買いシグナル）</h2>
        <table>
            <tr>
                <th>RSI範囲</th>
                <th>件数</th>
                <th>勝率</th>
                <th>平均利益</th>
                <th>合計利益</th>
            </tr>
            {rsi_stats_html}
        </table>

        <h2>📊 出来高変化率別分析（買いシグナル）</h2>
        <table>
            <tr>
                <th>出来高変化率</th>
                <th>件数</th>
                <th>勝率</th>
                <th>平均利益</th>
                <th>合計利益</th>
            </tr>
            {volume_stats_html}
        </table>

        <h2>📉 SMA5乖離率別分析（買いシグナル）</h2>
        <table>
            <tr>
                <th>SMA5乖離率</th>
                <th>件数</th>
                <th>勝率</th>
                <th>平均利益</th>
                <th>合計利益</th>
            </tr>
            {sma5_stats_html}
        </table>

        <h2>💰 価格帯別分析（買いシグナル）</h2>
        <table>
            <tr>
                <th>価格帯</th>
                <th>件数</th>
                <th>勝率</th>
                <th>平均利益</th>
                <th>合計利益</th>
            </tr>
            {price_stats_html}
        </table>

        <div style="margin-top: 30px; padding: 15px; background-color: #fff3e0; border-left: 4px solid #ff9800;">
            <strong>💡 キーインサイト:</strong><br>
            • <strong>価格フィルタ（≥1,000円）</strong>により買いシグナルの質が大幅向上<br>
            • 勝率: 50.59% → <strong>51.86% (+1.27%)</strong><br>
            • 平均利益: +707円 → <strong>+937円 (+32.5%)</strong><br>
            • シグナル数は24.5%減少したが、総利益はほぼ同等を維持<br>
            • <strong>500円未満・500-1,000円の低価格帯は損失傾向</strong>が明確化
        </div>
    </body>
    </html>
    """

    # ファイル保存
    OUTPUT_HTML.write_text(html_content, encoding='utf-8')
    print(f"✅ HTMLレポート生成完了: {OUTPUT_HTML}")
    print(f"   ファイルサイズ: {OUTPUT_HTML.stat().st_size / 1024:.1f} KB")

if __name__ == '__main__':
    generate_html_report()
