#!/usr/bin/env python3
"""
generate_v2_1_0_html_report.py

v2.0.3 と v2.1.0 の比較HTMLレポートを生成

入力: improvement/data/v2_1_0_comparison_results.parquet
出力: improvement/v2_1_0_comparison_report.html
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

# パス設定
IMPROVEMENT_DIR = ROOT / "improvement"
INPUT_FILE = IMPROVEMENT_DIR / "data" / "v2_1_0_comparison_results.parquet"
OUTPUT_FILE = IMPROVEMENT_DIR / "v2_1_0_comparison_report.html"


def generate_html_report(df: pd.DataFrame) -> str:
    """
    HTMLレポートを生成

    Args:
        df: v2_1_0_comparison_results.parquet のデータ

    Returns:
        HTML文字列
    """
    # 統計計算
    v2_0_3_counts = df['v2_0_3_action'].value_counts().to_dict()
    v2_1_0_counts = df['v2_1_0_action'].value_counts().to_dict()

    total_records = len(df)
    changed_records = df['action_changed'].sum()
    changed_pct = changed_records / total_records * 100

    # テクニカル指標の統計
    rsi_stats = df['rsi_14d'].describe()
    volume_stats = df['volume_change_20d'].describe()
    sma5_stats = df['price_vs_sma5_pct'].describe()

    # 変更パターンの集計
    change_patterns = {}
    for _, row in df[df['action_changed']].iterrows():
        pattern = f"{row['v2_0_3_action']} → {row['v2_1_0_action']}"
        change_patterns[pattern] = change_patterns.get(pattern, 0) + 1

    # v2.1.0 判定別の成績を計算
    buy_df = df[df['v2_1_0_action'] == '買い'].copy()
    hold_df = df[df['v2_1_0_action'] == '静観'].copy()
    sell_df = df[df['v2_1_0_action'] == '売り'].copy()

    # 買いの成績
    if len(buy_df) > 0:
        buy_df['buy_win'] = buy_df['daily_close'] > buy_df['buy_price']
        buy_df['buy_profit'] = (buy_df['daily_close'] - buy_df['buy_price']) * 100
        buy_wins = buy_df['buy_win'].sum()
        buy_total = len(buy_df)
        buy_win_rate = buy_wins / buy_total * 100 if buy_total > 0 else 0
        buy_avg_profit = buy_df['buy_profit'].mean()
        buy_total_profit = buy_df['buy_profit'].sum()
    else:
        buy_wins = buy_total = buy_win_rate = buy_avg_profit = buy_total_profit = 0

    # 静観の成績
    if len(hold_df) > 0:
        hold_df['hold_win'] = hold_df['daily_close'] > hold_df['buy_price']
        hold_df['hold_profit'] = (hold_df['daily_close'] - hold_df['buy_price']) * 100
        hold_wins = hold_df['hold_win'].sum()
        hold_total = len(hold_df)
        hold_win_rate = hold_wins / hold_total * 100 if hold_total > 0 else 0
        hold_avg_profit = hold_df['hold_profit'].mean()
        hold_total_profit = hold_df['hold_profit'].sum()
    else:
        hold_wins = hold_total = hold_win_rate = hold_avg_profit = hold_total_profit = 0

    # 売りの成績を2つに分ける
    # 1. v2.0.3でも売り → v2.1.0も売り（元々売り）
    sell_to_sell_df = df[(df['v2_0_3_action'] == '売り') & (df['v2_1_0_action'] == '売り')].copy()
    # 2. v2.0.3静観 → v2.1.0売り（新たに売りになった）
    hold_to_sell_df = df[(df['v2_0_3_action'] == '静観') & (df['v2_1_0_action'] == '売り')].copy()

    # v2.0.3の売り成績（売り→売り）
    if len(sell_to_sell_df) > 0:
        sell_to_sell_df['sell_win'] = sell_to_sell_df['buy_price'] > sell_to_sell_df['daily_close']
        sell_to_sell_df['sell_profit'] = (sell_to_sell_df['buy_price'] - sell_to_sell_df['daily_close']) * 100
        v203_sell_wins = sell_to_sell_df['sell_win'].sum()
        v203_sell_total = len(sell_to_sell_df)
        v203_sell_win_rate = v203_sell_wins / v203_sell_total * 100 if v203_sell_total > 0 else 0
        v203_sell_avg_profit = sell_to_sell_df['sell_profit'].mean()
        v203_sell_total_profit = sell_to_sell_df['sell_profit'].sum()
    else:
        v203_sell_wins = v203_sell_total = v203_sell_win_rate = v203_sell_avg_profit = v203_sell_total_profit = 0

    # 静観→売りの成績
    if len(hold_to_sell_df) > 0:
        hold_to_sell_df['sell_win'] = hold_to_sell_df['buy_price'] > hold_to_sell_df['daily_close']
        hold_to_sell_df['sell_profit'] = (hold_to_sell_df['buy_price'] - hold_to_sell_df['daily_close']) * 100
        hold_to_sell_wins = hold_to_sell_df['sell_win'].sum()
        hold_to_sell_total = len(hold_to_sell_df)
        hold_to_sell_win_rate = hold_to_sell_wins / hold_to_sell_total * 100 if hold_to_sell_total > 0 else 0
        hold_to_sell_avg_profit = hold_to_sell_df['sell_profit'].mean()
        hold_to_sell_total_profit = hold_to_sell_df['sell_profit'].sum()
    else:
        hold_to_sell_wins = hold_to_sell_total = hold_to_sell_win_rate = hold_to_sell_avg_profit = hold_to_sell_total_profit = 0

    # v2.1.0の売り全体の成績
    sell_df = df[df['v2_1_0_action'] == '売り'].copy()
    if len(sell_df) > 0:
        sell_df['sell_win'] = sell_df['buy_price'] > sell_df['daily_close']
        sell_df['sell_profit'] = (sell_df['buy_price'] - sell_df['daily_close']) * 100
        sell_wins = sell_df['sell_win'].sum()
        sell_total = len(sell_df)
        sell_win_rate = sell_wins / sell_total * 100 if sell_total > 0 else 0
        sell_avg_profit = sell_df['sell_profit'].mean()
        sell_total_profit = sell_df['sell_profit'].sum()

        # 日付別の勝率分析（売りのみ）
        sell_df['backtest_date_str'] = pd.to_datetime(sell_df['backtest_date']).dt.strftime('%Y-%m-%d')
        date_stats = sell_df.groupby('backtest_date_str').agg({
            'ticker': 'count',
            'sell_win': 'sum'
        }).reset_index()
        date_stats['win_rate'] = (date_stats['sell_win'] / date_stats['ticker'] * 100).round(2)
        date_stats = date_stats.sort_values('backtest_date_str', ascending=False)

        # 高勝率日と低勝率日を抽出
        top_dates = date_stats.nlargest(3, 'win_rate')
        bottom_dates = date_stats.nsmallest(3, 'win_rate')
    else:
        sell_wins = sell_total = sell_win_rate = sell_avg_profit = sell_total_profit = 0
        date_stats = top_dates = bottom_dates = pd.DataFrame()

    # 日別の変更数を集計
    df_changed = df[df['action_changed']].copy()
    df_changed['backtest_date'] = pd.to_datetime(df_changed['backtest_date'])
    df_changed['date_str'] = df_changed['backtest_date'].dt.strftime('%Y-%m-%d')
    changes_by_date = df_changed.groupby('date_str').size().to_dict()

    # テーブル行を生成（新しい日付順、日別内はv2.0.3スコア良い順にソート）
    table_rows = []
    current_date = None

    for _, row in df.sort_values(['backtest_date', 'v2_0_3_score'], ascending=[False, False]).iterrows():
        date_str = pd.to_datetime(row['backtest_date']).strftime('%Y-%m-%d')

        # 日付セパレータ
        if date_str != current_date:
            current_date = date_str
            changed_today = changes_by_date.get(date_str, 0)
            table_rows.append(f'''
        <tr class="date-separator">
            <td colspan="18">{date_str} （変更: {changed_today}件）</td>
        </tr>''')

        # 変更フラグ
        changed_class = 'changed' if row['action_changed'] else ''

        # 各カラムの値
        ticker = row['ticker']
        stock_name = row.get('company_name', '')
        grok_rank = row['grok_rank']
        prev_2day_close = row.get('prev_2day_close', 0)
        prev_close = row.get('prev_day_close', 0)

        # 前々日→前日の変動
        prev_day_change = prev_close - prev_2day_close if prev_2day_close > 0 else 0
        prev_day_change_class = 'positive' if prev_day_change > 0 else 'negative' if prev_day_change < 0 else ''

        v2_0_3_score = row['v2_0_3_score']
        v2_0_3_action = row['v2_0_3_action']
        v2_1_0_score = row['v2_1_0_score']
        v2_1_0_action = row['v2_1_0_action']

        # スコア差分
        score_diff = row['score_diff']
        score_diff_str = f'+{score_diff}' if score_diff > 0 else str(score_diff)
        score_diff_class = 'positive' if score_diff > 0 else 'negative' if score_diff < 0 else ''

        # 始値・終値
        buy_price = row.get('buy_price', 0)
        daily_close = row.get('daily_close', 0)
        price_diff = daily_close - buy_price
        price_diff_str = f'+{price_diff:.0f}' if price_diff > 0 else f'{price_diff:.0f}'
        price_diff_class = 'positive' if price_diff > 0 else 'negative' if price_diff < 0 else ''

        # 100株あたりの利益計算（v2.1.0判定ベース）
        # 買い・静観: 終値 - 始値、売り: 始値 - 終値
        if v2_1_0_action == '売り':
            profit_100 = (buy_price - daily_close) * 100
        else:
            profit_100 = (daily_close - buy_price) * 100

        profit_str = f'+{profit_100:,.0f}' if profit_100 > 0 else f'{profit_100:,.0f}'
        profit_class = 'positive' if profit_100 > 0 else 'negative' if profit_100 < 0 else ''

        # 勝負引分判定
        if profit_100 > 0:
            result = '勝'
            result_class = 'positive'
        elif profit_100 < 0:
            result = '負'
            result_class = 'negative'
        else:
            result = '分'
            result_class = ''

        rsi = row['rsi_14d']
        vol_change = row['volume_change_20d']
        sma5_pct = row['price_vs_sma5_pct']

        table_rows.append(f'''
        <tr class="action-{v2_1_0_action} {changed_class}">
            <td>{ticker}</td>
            <td>{stock_name}</td>
            <td class="number">{grok_rank}</td>
            <td class="number">{prev_2day_close:,.0f}</td>
            <td class="number {prev_day_change_class}">{prev_close:,.0f}</td>
            <td class="number">{v2_0_3_score}</td>
            <td><span class="action-{v2_0_3_action}-badge action-badge">{v2_0_3_action}</span></td>
            <td class="number">{v2_1_0_score}</td>
            <td><span class="action-{v2_1_0_action}-badge action-badge">{v2_1_0_action}</span></td>
            <td class="number {score_diff_class}">{score_diff_str}</td>
            <td class="number">{buy_price:,.0f}</td>
            <td class="number">{daily_close:,.0f}</td>
            <td class="number {price_diff_class}">{price_diff_str}</td>
            <td class="number {result_class}" style="font-weight: bold;">{result}</td>
            <td class="number {profit_class}" style="font-weight: bold;">{profit_str}</td>
            <td class="number">{rsi:.1f}</td>
            <td class="number">{vol_change:.2f}</td>
            <td class="number">{sma5_pct:.1f}%</td>
        </tr>''')

    # 変更パターン詳細リスト用のテーブル行を生成
    change_pattern_list_rows = []
    current_date_pattern = None

    for _, row in df[df['action_changed']].sort_values(['backtest_date', 'v2_0_3_score'], ascending=[False, False]).iterrows():
        date_str = pd.to_datetime(row['backtest_date']).strftime('%Y-%m-%d')

        # 日付セパレータ
        if date_str != current_date_pattern:
            current_date_pattern = date_str
            change_pattern_list_rows.append(f'''
        <tr class="date-separator">
            <td colspan="18">{date_str}</td>
        </tr>''')

        # 各カラムの値
        ticker = row['ticker']
        stock_name = row.get('company_name', '')
        grok_rank = row['grok_rank']
        prev_2day_close = row.get('prev_2day_close', 0)
        prev_close = row.get('prev_day_close', 0)

        prev_day_change = prev_close - prev_2day_close if prev_2day_close > 0 else 0
        prev_day_change_class = 'positive' if prev_day_change > 0 else 'negative' if prev_day_change < 0 else ''

        v2_0_3_score = row['v2_0_3_score']
        v2_0_3_action = row['v2_0_3_action']
        v2_1_0_score = row['v2_1_0_score']
        v2_1_0_action = row['v2_1_0_action']

        score_diff = row['score_diff']
        score_diff_str = f'+{score_diff}' if score_diff > 0 else str(score_diff)
        score_diff_class = 'positive' if score_diff > 0 else 'negative' if score_diff < 0 else ''

        buy_price = row.get('buy_price', 0)
        daily_close = row.get('daily_close', 0)
        price_diff = daily_close - buy_price
        price_diff_str = f'+{price_diff:.0f}' if price_diff > 0 else f'{price_diff:.0f}'
        price_diff_class = 'positive' if price_diff > 0 else 'negative' if price_diff < 0 else ''

        # 100株あたりの利益計算
        if v2_1_0_action == '売り':
            profit_100 = (buy_price - daily_close) * 100
        else:
            profit_100 = (daily_close - buy_price) * 100

        profit_str = f'+{profit_100:,.0f}' if profit_100 > 0 else f'{profit_100:,.0f}'
        profit_class = 'positive' if profit_100 > 0 else 'negative' if profit_100 < 0 else ''

        if profit_100 > 0:
            result = '勝'
            result_class = 'positive'
        elif profit_100 < 0:
            result = '負'
            result_class = 'negative'
        else:
            result = '分'
            result_class = ''

        rsi = row['rsi_14d']
        vol_change = row['volume_change_20d']
        sma5_pct = row['price_vs_sma5_pct']

        change_pattern_list_rows.append(f'''
        <tr class="action-{v2_1_0_action} changed">
            <td>{ticker}</td>
            <td>{stock_name}</td>
            <td class="number">{grok_rank}</td>
            <td class="number">{prev_2day_close:,.0f}</td>
            <td class="number {prev_day_change_class}">{prev_close:,.0f}</td>
            <td class="number">{v2_0_3_score}</td>
            <td><span class="action-{v2_0_3_action}-badge action-badge">{v2_0_3_action}</span></td>
            <td class="number">{v2_1_0_score}</td>
            <td><span class="action-{v2_1_0_action}-badge action-badge">{v2_1_0_action}</span></td>
            <td class="number {score_diff_class}">{score_diff_str}</td>
            <td class="number">{buy_price:,.0f}</td>
            <td class="number">{daily_close:,.0f}</td>
            <td class="number {price_diff_class}">{price_diff_str}</td>
            <td class="number {result_class}" style="font-weight: bold;">{result}</td>
            <td class="number {profit_class}" style="font-weight: bold;">{profit_str}</td>
            <td class="number">{rsi:.1f}</td>
            <td class="number">{vol_change:.2f}</td>
            <td class="number">{sma5_pct:.1f}%</td>
        </tr>''')

    # 変更パターンのHTML（詳細サマリー：勝率・利益付き）
    pattern_rows = []
    for pattern, count in sorted(change_patterns.items(), key=lambda x: -x[1]):
        # パターンに該当するデータを抽出
        pattern_parts = pattern.split(' → ')
        before_action = pattern_parts[0]
        after_action = pattern_parts[1]

        pattern_df = df[(df['v2_0_3_action'] == before_action) & (df['v2_1_0_action'] == after_action)].copy()

        if len(pattern_df) == 0:
            continue

        # 変更前（v2.0.3）の勝率計算
        if before_action == '売り':
            pattern_df['before_win'] = pattern_df['buy_price'] > pattern_df['daily_close']
            pattern_df['before_profit'] = (pattern_df['buy_price'] - pattern_df['daily_close']) * 100
        else:  # 買い or 静観
            pattern_df['before_win'] = pattern_df['daily_close'] > pattern_df['buy_price']
            pattern_df['before_profit'] = (pattern_df['daily_close'] - pattern_df['buy_price']) * 100

        before_wins = pattern_df['before_win'].sum()
        before_total = len(pattern_df)
        before_win_rate = before_wins / before_total * 100 if before_total > 0 else 0
        before_avg_profit = pattern_df['before_profit'].mean()

        # 変更後（v2.1.0）の勝率計算
        if after_action == '売り':
            pattern_df['after_win'] = pattern_df['buy_price'] > pattern_df['daily_close']
            pattern_df['after_profit'] = (pattern_df['buy_price'] - pattern_df['daily_close']) * 100
        else:  # 買い or 静観
            pattern_df['after_win'] = pattern_df['daily_close'] > pattern_df['buy_price']
            pattern_df['after_profit'] = (pattern_df['daily_close'] - pattern_df['buy_price']) * 100

        after_wins = pattern_df['after_win'].sum()
        after_total = len(pattern_df)
        after_win_rate = after_wins / after_total * 100 if after_total > 0 else 0
        after_avg_profit = pattern_df['after_profit'].mean()

        # 色分け
        before_win_rate_class = 'positive' if before_win_rate >= 50 else 'negative' if before_win_rate < 50 else ''
        after_win_rate_class = 'positive' if after_win_rate >= 50 else 'negative' if after_win_rate < 50 else ''
        before_profit_class = 'positive' if before_avg_profit > 0 else 'negative' if before_avg_profit < 0 else ''
        after_profit_class = 'positive' if after_avg_profit > 0 else 'negative' if after_avg_profit < 0 else ''
        before_profit_str = f'+{before_avg_profit:,.0f}' if before_avg_profit > 0 else f'{before_avg_profit:,.0f}'
        after_profit_str = f'+{after_avg_profit:,.0f}' if after_avg_profit > 0 else f'{after_avg_profit:,.0f}'

        pattern_rows.append(
            f'<tr>'
            f'<td>{pattern}</td>'
            f'<td class="number">{count}</td>'
            f'<td class="number {before_win_rate_class}">{before_win_rate:.1f}%</td>'
            f'<td class="number {before_profit_class}">{before_profit_str}円</td>'
            f'<td class="number {after_win_rate_class}">{after_win_rate:.1f}%</td>'
            f'<td class="number {after_profit_class}">{after_profit_str}円</td>'
            f'</tr>'
        )

    pattern_rows = '\n'.join(pattern_rows)

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V2.1.0 比較レポート (v2.0.3 vs v2.1.0)</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 40px 20px;
    color: #333;
}}
.container {{
    max-width: 1800px;
    margin: 0 auto;
    background: white;
    border-radius: 16px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
    overflow: hidden;
}}
.header {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 40px;
    text-align: center;
}}
.header h1 {{
    font-size: 2.5em;
    margin-bottom: 10px;
    font-weight: 700;
}}
.header .subtitle {{
    font-size: 1.1em;
    opacity: 0.9;
}}
.summary-section {{
    padding: 40px;
    background: #f8f9fa;
}}
.summary-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 24px;
}}
.summary-card {{
    background: white;
    border-radius: 12px;
    padding: 28px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-left: 6px solid #667eea;
    transition: transform 0.2s;
}}
.summary-card:hover {{
    transform: translateY(-4px);
    box-shadow: 0 8px 24px rgba(0,0,0,0.12);
}}
.summary-card h3 {{
    font-size: 1.4em;
    margin-bottom: 20px;
    font-weight: 600;
    color: #667eea;
}}
.stat-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 12px 0;
    padding: 10px 0;
    border-bottom: 1px solid #f0f0f0;
}}
.stat-row:last-child {{ border-bottom: none; }}
.stat-label {{
    font-size: 1em;
    color: #666;
}}
.stat-value {{
    font-size: 1.5em;
    font-weight: 700;
    color: #333;
}}
.table-section {{
    padding: 40px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9em;
}}
thead {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    position: sticky;
    top: 0;
    z-index: 10;
}}
th {{
    padding: 16px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 0.95em;
    letter-spacing: 0.5px;
}}
th.number {{ text-align: right; }}
td {{
    padding: 14px 12px;
    border-bottom: 1px solid #e0e0e0;
}}
td.number {{
    text-align: right;
    font-family: "SF Mono", Monaco, Consolas, monospace;
    font-size: 0.95em;
}}
td.number.positive {{
    color: #27ae60;
    font-weight: 600;
}}
td.number.negative {{
    color: #e74c3c;
    font-weight: 600;
}}
tr.date-separator {{
    background: linear-gradient(to right, #667eea, #764ba2);
    color: white;
    font-weight: 700;
    font-size: 1.1em;
}}
tr.date-separator td {{
    padding: 16px 12px;
    border: none;
}}
tr.action-買い {{
    background: #ffdddd;
}}
tr.action-売り {{
    background: #cce5ff;
}}
tr.action-静観 {{
    background: #f5f5f5;
}}
tr.changed {{
    border-left: 4px solid #f39c12;
}}
tr:hover:not(.date-separator) {{
    background: #fff3cd !important;
}}
.action-badge {{
    display: inline-block;
    padding: 6px 14px;
    border-radius: 20px;
    font-weight: 600;
    font-size: 0.9em;
}}
.action-買い-badge {{
    background: #ff6b6b;
    color: white;
}}
.action-売り-badge {{
    background: #4dabf7;
    color: white;
}}
.action-静観-badge {{
    background: #adb5bd;
    color: white;
}}
.pattern-table {{
    margin-top: 20px;
}}
.pattern-table table {{
    font-size: 1em;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>V2.1.0 比較レポート</h1>
        <div class="subtitle">v2.0.3 vs v2.1.0 スコアリングロジック比較</div>
        <div class="subtitle">対象期間: 2025-11-04 ~ 2025-11-21 | 総レコード数: {total_records}</div>
    </div>

    <div class="summary-section">
        <div class="summary-grid">
            <div class="summary-card">
                <h3>📊 v2.0.3 判定結果</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{v2_0_3_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{v2_0_3_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{v2_0_3_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>🚀 v2.1.0 判定結果</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{v2_1_0_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{v2_1_0_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{v2_1_0_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>🔄 判定変更</h3>
                <div class="stat-row">
                    <span class="stat-label">変更数</span>
                    <span class="stat-value">{changed_records}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">変更率</span>
                    <span class="stat-value">{changed_pct:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">維持</span>
                    <span class="stat-value">{total_records - changed_records}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>📈 テクニカル指標統計</h3>
                <div class="stat-row">
                    <span class="stat-label">RSI平均</span>
                    <span class="stat-value">{rsi_stats['mean']:.1f}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">出来高変化平均</span>
                    <span class="stat-value">{volume_stats['mean']:.2f}x</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">SMA5乖離平均</span>
                    <span class="stat-value">{sma5_stats['mean']:.1f}%</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #e74c3c;">
                <h3>💰 v2.1.0「買い」成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{buy_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_win_rate > 50 else '#e74c3c'};">{buy_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{buy_avg_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_total_profit > 0 else '#e74c3c'};">{buy_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #95a5a6;">
                <h3>⏸️ v2.1.0「静観」成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{hold_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_win_rate > 50 else '#e74c3c'};">{hold_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_avg_profit > 0 else '#e74c3c'};">{hold_avg_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_total_profit > 0 else '#e74c3c'};">{hold_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #27ae60;">
                <h3>📉 v2.0.3「売り」成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数（売り→売り）</span>
                    <span class="stat-value">{v203_sell_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率（売りとして）</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_sell_win_rate > 50 else '#e74c3c'};">{v203_sell_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_sell_avg_profit > 0 else '#e74c3c'};">{v203_sell_avg_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_sell_total_profit > 0 else '#e74c3c'};">{v203_sell_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #3498db;">
                <h3>🔄 v2.1.0「静観→売り」成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数（静観→売り）</span>
                    <span class="stat-value">{hold_to_sell_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率（売りとして）</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_to_sell_win_rate > 50 else '#e74c3c'};">{hold_to_sell_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_to_sell_avg_profit > 0 else '#e74c3c'};">{hold_to_sell_avg_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_to_sell_total_profit > 0 else '#e74c3c'};">{hold_to_sell_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #f39c12;">
                <h3>📅 日別勝率分析（売りのみ）</h3>
                <div class="stat-row">
                    <span class="stat-label">高勝率Top3</span>
                    <span class="stat-value" style="font-size: 0.9em;">{'<br>'.join([f"{row['backtest_date_str']}: {row['win_rate']:.0f}% ({int(row['sell_win'])}/{int(row['ticker'])})" for _, row in top_dates.iterrows()]) if len(top_dates) > 0 else '-'}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">低勝率Top3</span>
                    <span class="stat-value" style="font-size: 0.9em; color: #e74c3c;">{'<br>'.join([f"{row['backtest_date_str']}: {row['win_rate']:.0f}% ({int(row['sell_win'])}/{int(row['ticker'])})" for _, row in bottom_dates.iterrows()]) if len(bottom_dates) > 0 else '-'}</span>
                </div>
            </div>
        </div>

        <div class="pattern-table">
            <h3 style="margin-bottom: 16px; color: #667eea;">変更パターン詳細</h3>
            <table>
                <thead>
                    <tr>
                        <th>変更パターン</th>
                        <th class="number">件数</th>
                        <th class="number">変更前勝率</th>
                        <th class="number">変更前100株利益</th>
                        <th class="number">変更後勝率</th>
                        <th class="number">変更後100株利益</th>
                    </tr>
                </thead>
                <tbody>
                    {pattern_rows}
                </tbody>
            </table>
        </div>
    </div>

    <div class="table-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">変更パターンリスト（{changed_records}件）</h2>
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th class="number">Grokランク</th>
                    <th class="number">前々日終値</th>
                    <th class="number">前日終値</th>
                    <th class="number">v2.0.3<br/>スコア</th>
                    <th>v2.0.3<br/>判定</th>
                    <th class="number">v2.1.0<br/>スコア</th>
                    <th>v2.1.0<br/>判定</th>
                    <th class="number">差分</th>
                    <th class="number">始値</th>
                    <th class="number">終値</th>
                    <th class="number">終値-始値</th>
                    <th class="number">勝負引分</th>
                    <th class="number">100株利益</th>
                    <th class="number">RSI</th>
                    <th class="number">出来高</th>
                    <th class="number">SMA5</th>
                </tr>
            </thead>
            <tbody>
                {''.join(change_pattern_list_rows)}
            </tbody>
        </table>
    </div>

    <div class="table-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">詳細比較テーブル</h2>
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th class="number">Grokランク</th>
                    <th class="number">前々日終値</th>
                    <th class="number">前日終値</th>
                    <th class="number">v2.0.3<br/>スコア</th>
                    <th>v2.0.3<br/>判定</th>
                    <th class="number">v2.1.0<br/>スコア</th>
                    <th>v2.1.0<br/>判定</th>
                    <th class="number">差分</th>
                    <th class="number">始値</th>
                    <th class="number">終値</th>
                    <th class="number">終値-始値</th>
                    <th class="number">勝負引分</th>
                    <th class="number">100株利益</th>
                    <th class="number">RSI</th>
                    <th class="number">出来高</th>
                    <th class="number">SMA5</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>
    </div>
</div>
</body>
</html>'''

    return html


def main() -> int:
    print("=" * 60)
    print("Generate v2.1.0 HTML Report")
    print("=" * 60)

    # [STEP 1] データ読み込み
    print("\n[STEP 1] Loading data...")

    if not INPUT_FILE.exists():
        print(f"  ✗ File not found: {INPUT_FILE}")
        return 1

    df = pd.read_parquet(INPUT_FILE)
    print(f"  ✓ Loaded: {len(df)} records, {len(df.columns)} columns")

    # [STEP 2] HTMLレポート生成
    print("\n[STEP 2] Generating HTML report...")
    html = generate_html_report(df)

    # [STEP 3] 保存
    print("\n[STEP 3] Saving...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  ✓ Saved: {OUTPUT_FILE}")

    print("\n✅ HTML report generated successfully!")
    print(f"\n📄 Open the report: file://{OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
