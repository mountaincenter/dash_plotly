#!/usr/bin/env python3
"""
generate_v2_0_3_vs_v2_1_comparison.py

v2.0.3 と v2.1 の比較レポートを生成

入力: improvement/data/complete_v2_0_3_comparison.parquet
出力: improvement/v2_0_3_vs_v2_1_comparison.html
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# パス設定
IMPROVEMENT_DIR = ROOT / "improvement"
INPUT_FILE = IMPROVEMENT_DIR / "data" / "v2_1_0_comparison_results.parquet"
OUTPUT_FILE = IMPROVEMENT_DIR / "v2_0_3_vs_v2_1_comparison.html"


def calculate_v2_1_score_and_action(row: pd.Series) -> tuple[int, str, list[str]]:
    """
    v2.1 のスコアリングロジックを適用（フルスコア再計算）

    v2.1 改善:
    - Grokランク配点強化（上位25%: +50, 下位25%: -30）
    - RSI < 30: +20、RSI > 70: -10
    - 出来高急増 > 2.0: +15
    - 5日線押し目（-2% to 0%）: +15
    - 前日変化率による補正
    - 価格帯強制判定（5,000-10,000円: 買い、10,000円以上: 売り）

    重要: v2.0.3が「売り」の場合は、v2.1スコアを計算するが判定は「売り」で固定

    Returns:
        (score, action, reasons)
    """
    score = 0
    reasons = []
    prev_close = row.get('prev_day_close')
    v2_0_3_action = row.get('v2_0_3_action')

    # 価格帯強制判定（v2.1でも維持）
    if pd.notna(prev_close):
        if 5000 <= prev_close < 10000:
            return (100, '買い', ['5,000-10,000円（強制買い）'])
        elif prev_close >= 10000:
            return (-100, '売り', ['10,000円以上（強制売り）'])

    # スコアベース判定（v2.1: 配点強化）
    # 1. Grokランク配点強化
    grok_rank = row.get('grok_rank', 999)
    total_stocks = row.get('total_stocks', 1)
    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    if relative_position <= 0.25:
        score += 50  # v2.0.3: 40 → v2.1: 50
        reasons.append(f'Grokランク上位25%（強化）')
    elif relative_position <= 0.50:
        score += 30  # v2.0.3: 20 → v2.1: 30
        reasons.append(f'Grokランク上位50%')
    elif relative_position <= 0.75:
        pass
    else:
        score -= 30  # v2.0.3: -10 → v2.1: -30
        reasons.append(f'Grokランク下位25%（減点強化）')

    # 2. RSI（新規）
    rsi_14d = row.get('rsi_14d')
    if pd.notna(rsi_14d):
        if rsi_14d < 30:
            score += 20
            reasons.append(f'RSI {rsi_14d:.1f}（売られすぎ）')
        elif rsi_14d > 70:
            score -= 10
            reasons.append(f'RSI {rsi_14d:.1f}（買われすぎ）')

    # 3. 出来高急増（新規）
    volume_change_20d = row.get('volume_change_20d')
    if pd.notna(volume_change_20d) and volume_change_20d > 2.0:
        score += 15
        reasons.append(f'出来高{volume_change_20d:.1f}倍（注目急増）')

    # 4. 5日線押し目（新規）
    price_vs_sma5_pct = row.get('price_vs_sma5_pct')
    if pd.notna(price_vs_sma5_pct) and -2.0 < price_vs_sma5_pct < 0:
        score += 15
        reasons.append(f'5日線押し目{price_vs_sma5_pct:.1f}%')

    # 5. 前日変化率
    if pd.notna(row.get('prev_day_change_pct')):
        change_pct = row['prev_day_change_pct']
        if change_pct < -5:
            score += 15
            reasons.append(f'前日-5%以上下落')
        elif change_pct > 10:
            score -= 10
            reasons.append(f'前日+10%以上急騰')

    # アクション判定
    if score >= 30:
        action = '買い'
    elif score <= -20:
        action = '売り'
    else:
        action = '静観'

    # 重要: v2.0.3が「売り」なら、v2.1も「売り」に固定
    if v2_0_3_action == '売り':
        action = '売り'
        reasons.append('（v2.0.3売り判定を保持）')

    return (score, action, reasons)


def generate_html_report(df: pd.DataFrame) -> str:
    """HTMLレポートを生成"""

    # 統計計算
    v2_0_3_counts = df['v2_0_3_action'].value_counts().to_dict()
    v2_1_counts = df['v2_1_action'].value_counts().to_dict()

    total_records = len(df)
    changed_records = df['action_changed'].sum()
    changed_pct = changed_records / total_records * 100

    # テクニカル指標の統計
    rsi_stats = df['rsi_14d'].describe()
    volume_stats = df['volume_change_20d'].describe()
    sma5_stats = df['price_vs_sma5_pct'].describe()

    # v2.0.3 買い成績
    v203_buy_df = df[df['v2_0_3_action'] == '買い'].copy()
    if len(v203_buy_df) > 0:
        v203_buy_df['buy_win'] = v203_buy_df['daily_close'] > v203_buy_df['buy_price']
        v203_buy_df['buy_profit'] = (v203_buy_df['daily_close'] - v203_buy_df['buy_price']) * 100
        v203_buy_wins = v203_buy_df['buy_win'].sum()
        v203_buy_total = len(v203_buy_df)
        v203_buy_win_rate = v203_buy_wins / v203_buy_total * 100
        v203_buy_total_profit = v203_buy_df['buy_profit'].sum()
    else:
        v203_buy_wins = v203_buy_total = v203_buy_win_rate = v203_buy_total_profit = 0

    # v2.0.3 売り成績
    v203_sell_df = df[df['v2_0_3_action'] == '売り'].copy()
    if len(v203_sell_df) > 0:
        v203_sell_df['sell_win'] = v203_sell_df['buy_price'] > v203_sell_df['daily_close']
        v203_sell_df['sell_profit'] = (v203_sell_df['buy_price'] - v203_sell_df['daily_close']) * 100
        v203_sell_wins = v203_sell_df['sell_win'].sum()
        v203_sell_total = len(v203_sell_df)
        v203_sell_win_rate = v203_sell_wins / v203_sell_total * 100
        v203_sell_total_profit = v203_sell_df['sell_profit'].sum()
    else:
        v203_sell_wins = v203_sell_total = v203_sell_win_rate = v203_sell_total_profit = 0

    # v2.0.3 総利益
    v203_total_profit = v203_buy_total_profit + v203_sell_total_profit

    # v2.1 成績
    v21_buy_df = df[df['v2_1_action'] == '買い'].copy()
    v21_sell_df = df[df['v2_1_action'] == '売り'].copy()

    # v2.1 買い成績
    if len(v21_buy_df) > 0:
        v21_buy_df['buy_win'] = v21_buy_df['daily_close'] > v21_buy_df['buy_price']
        v21_buy_df['buy_profit'] = (v21_buy_df['daily_close'] - v21_buy_df['buy_price']) * 100
        v21_buy_wins = v21_buy_df['buy_win'].sum()
        v21_buy_total = len(v21_buy_df)
        v21_buy_win_rate = v21_buy_wins / v21_buy_total * 100
        v21_buy_total_profit = v21_buy_df['buy_profit'].sum()
    else:
        v21_buy_wins = v21_buy_total = v21_buy_win_rate = v21_buy_total_profit = 0

    # v2.1 売り成績
    if len(v21_sell_df) > 0:
        v21_sell_df['sell_win'] = v21_sell_df['buy_price'] > v21_sell_df['daily_close']
        v21_sell_df['sell_profit'] = (v21_sell_df['buy_price'] - v21_sell_df['daily_close']) * 100
        v21_sell_wins = v21_sell_df['sell_win'].sum()
        v21_sell_total = len(v21_sell_df)
        v21_sell_win_rate = v21_sell_wins / v21_sell_total * 100
        v21_sell_total_profit = v21_sell_df['sell_profit'].sum()
    else:
        v21_sell_wins = v21_sell_total = v21_sell_win_rate = v21_sell_total_profit = 0

    # v2.1 売りの内訳分析
    # 売り→売り (v2.0.3でも売り)
    sell_to_sell_df = df[(df['v2_0_3_action'] == '売り') & (df['v2_1_action'] == '売り')].copy()
    if len(sell_to_sell_df) > 0:
        sell_to_sell_df['sell_win'] = sell_to_sell_df['buy_price'] > sell_to_sell_df['daily_close']
        sell_to_sell_df['sell_profit'] = (sell_to_sell_df['buy_price'] - sell_to_sell_df['daily_close']) * 100
        sell_to_sell_count = len(sell_to_sell_df)
        sell_to_sell_wins = sell_to_sell_df['sell_win'].sum()
        sell_to_sell_win_rate = sell_to_sell_wins / sell_to_sell_count * 100
        sell_to_sell_profit = sell_to_sell_df['sell_profit'].sum()
    else:
        sell_to_sell_count = sell_to_sell_wins = sell_to_sell_win_rate = sell_to_sell_profit = 0

    # 静観→売り (v2.1で追加)
    hold_to_sell_df = df[(df['v2_0_3_action'] == '静観') & (df['v2_1_action'] == '売り')].copy()
    if len(hold_to_sell_df) > 0:
        hold_to_sell_df['sell_win'] = hold_to_sell_df['buy_price'] > hold_to_sell_df['daily_close']
        hold_to_sell_df['sell_profit'] = (hold_to_sell_df['buy_price'] - hold_to_sell_df['daily_close']) * 100
        hold_to_sell_count = len(hold_to_sell_df)
        hold_to_sell_wins = hold_to_sell_df['sell_win'].sum()
        hold_to_sell_win_rate = hold_to_sell_wins / hold_to_sell_count * 100
        hold_to_sell_profit = hold_to_sell_df['sell_profit'].sum()
    else:
        hold_to_sell_count = hold_to_sell_wins = hold_to_sell_win_rate = hold_to_sell_profit = 0

    # 買い→売り (v2.1で追加)
    buy_to_sell_df = df[(df['v2_0_3_action'] == '買い') & (df['v2_1_action'] == '売り')].copy()
    if len(buy_to_sell_df) > 0:
        buy_to_sell_df['sell_win'] = buy_to_sell_df['buy_price'] > buy_to_sell_df['daily_close']
        buy_to_sell_df['sell_profit'] = (buy_to_sell_df['buy_price'] - buy_to_sell_df['daily_close']) * 100
        buy_to_sell_count = len(buy_to_sell_df)
        buy_to_sell_wins = buy_to_sell_df['sell_win'].sum()
        buy_to_sell_win_rate = buy_to_sell_wins / buy_to_sell_count * 100
        buy_to_sell_profit = buy_to_sell_df['sell_profit'].sum()
    else:
        buy_to_sell_count = buy_to_sell_wins = buy_to_sell_win_rate = buy_to_sell_profit = 0

    # v2.1 総利益
    v21_total_profit = v21_buy_total_profit + v21_sell_total_profit

    # 変更パターンの集計
    change_patterns = {}
    for _, row in df[df['action_changed']].iterrows():
        pattern = f"{row['v2_0_3_action']} → {row['v2_1_action']}"
        change_patterns[pattern] = change_patterns.get(pattern, 0) + 1

    # パターン別の勝率・利益計算
    pattern_rows = []
    for pattern, count in sorted(change_patterns.items(), key=lambda x: -x[1]):
        pattern_parts = pattern.split(' → ')
        before_action = pattern_parts[0]
        after_action = pattern_parts[1]

        pattern_df = df[(df['v2_0_3_action'] == before_action) & (df['v2_1_action'] == after_action)].copy()

        if len(pattern_df) == 0:
            continue

        # 変更前（v2.0.3）の勝率計算
        if before_action == '売り':
            pattern_df['before_win'] = pattern_df['buy_price'] > pattern_df['daily_close']
            pattern_df['before_profit'] = (pattern_df['buy_price'] - pattern_df['daily_close']) * 100
        else:
            pattern_df['before_win'] = pattern_df['daily_close'] > pattern_df['buy_price']
            pattern_df['before_profit'] = (pattern_df['daily_close'] - pattern_df['buy_price']) * 100

        before_wins = pattern_df['before_win'].sum()
        before_total = len(pattern_df)
        before_win_rate = before_wins / before_total * 100 if before_total > 0 else 0
        before_avg_profit = pattern_df['before_profit'].mean()

        # 変更後（v2.1）の勝率計算
        if after_action == '売り':
            pattern_df['after_win'] = pattern_df['buy_price'] > pattern_df['daily_close']
            pattern_df['after_profit'] = (pattern_df['buy_price'] - pattern_df['daily_close']) * 100
        else:
            pattern_df['after_win'] = pattern_df['daily_close'] > pattern_df['buy_price']
            pattern_df['after_profit'] = (pattern_df['daily_close'] - pattern_df['buy_price']) * 100

        after_wins = pattern_df['after_win'].sum()
        after_total = len(pattern_df)
        after_win_rate = after_wins / after_total * 100 if after_total > 0 else 0
        after_avg_profit = pattern_df['after_profit'].mean()

        # 色分け
        before_win_rate_class = 'positive' if before_win_rate >= 50 else 'negative'
        after_win_rate_class = 'positive' if after_win_rate >= 50 else 'negative'
        before_profit_class = 'positive' if before_avg_profit > 0 else 'negative'
        after_profit_class = 'positive' if after_avg_profit > 0 else 'negative'
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

    pattern_rows_html = '\n'.join(pattern_rows)

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>v2.0.3 vs v2.1 比較レポート</title>
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
.highlight {{
    background: #fff3cd;
    font-weight: bold;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>v2.0.3 vs v2.1 比較レポート</h1>
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
                <h3>🚀 v2.1 判定結果</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{v2_1_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{v2_1_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{v2_1_counts.get('静観', 0)}</span>
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

            <div class="summary-card" style="border-left-color: #3498db;">
                <h3>💰 v2.0.3 買い成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{v203_buy_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_buy_win_rate > 50 else '#e74c3c'};">{v203_buy_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_buy_total_profit > 0 else '#e74c3c'};">{v203_buy_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #9b59b6;">
                <h3>📉 v2.0.3 売り成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{v203_sell_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_sell_win_rate > 50 else '#e74c3c'};">{v203_sell_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if v203_sell_total_profit > 0 else '#e74c3c'};">{v203_sell_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #e74c3c;">
                <h3>💰 v2.1 買い成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{v21_buy_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if v21_buy_win_rate > 50 else '#e74c3c'};">{v21_buy_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if v21_buy_total_profit > 0 else '#e74c3c'};">{v21_buy_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #27ae60;">
                <h3>📉 v2.1 売り成績（合計）</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{v21_sell_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if v21_sell_win_rate > 50 else '#e74c3c'};">{v21_sell_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if v21_sell_total_profit > 0 else '#e74c3c'};">{v21_sell_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #16a085;">
                <h3>🔄 v2.0.3売り→v2.1売り</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{sell_to_sell_count}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_to_sell_win_rate > 50 else '#e74c3c'};">{sell_to_sell_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_to_sell_profit > 0 else '#e74c3c'};">{sell_to_sell_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #e67e22;">
                <h3>🔄 v2.0.3静観→v2.1売り</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{hold_to_sell_count}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_to_sell_win_rate > 50 else '#e74c3c'};">{hold_to_sell_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hold_to_sell_profit > 0 else '#e74c3c'};">{hold_to_sell_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #c0392b;">
                <h3>🔄 v2.0.3買い→v2.1売り</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{buy_to_sell_count}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_to_sell_win_rate > 50 else '#e74c3c'};">{buy_to_sell_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_to_sell_profit > 0 else '#e74c3c'};">{buy_to_sell_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card highlight" style="border-left-color: #f39c12;">
                <h3>🏆 総合比較</h3>
                <div class="stat-row">
                    <span class="stat-label">v2.0.3 総利益</span>
                    <span class="stat-value">{v203_total_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">v2.1 総利益</span>
                    <span class="stat-value">{v21_total_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">差分</span>
                    <span class="stat-value" style="color: {'#27ae60' if v21_total_profit > v203_total_profit else '#e74c3c'};">{v21_total_profit - v203_total_profit:+,.0f}円</span>
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
        </div>

        <div style="margin-top: 40px;">
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
                    {pattern_rows_html}
                </tbody>
            </table>
        </div>
    </div>
</div>
</body>
</html>'''

    return html


def main() -> int:
    print("=" * 60)
    print("v2.0.3 vs v2.1 Comparison Report Generator")
    print("=" * 60)

    # [STEP 1] データ読み込み
    print("\n[STEP 1] Loading data...")

    if not INPUT_FILE.exists():
        print(f"  ✗ File not found: {INPUT_FILE}")
        return 1

    df = pd.read_parquet(INPUT_FILE)
    print(f"  ✓ Loaded: {len(df)} records, {len(df.columns)} columns")

    # v2.0.3の結果を確認
    if 'v2_0_3_action' not in df.columns:
        print("  ✗ Error: v2_0_3_action column not found")
        return 1

    print(f"  ✓ v2.0.3 actions: {df['v2_0_3_action'].value_counts().to_dict()}")

    # [STEP 2] v2.1スコアを再計算（v2.0.3ベース + テクニカル補正）
    print("\n[STEP 2] Recalculating v2.1 scores (v2.0.3 base + technical overlay)...")

    v2_1_scores = []
    v2_1_actions = []
    v2_1_reasons = []

    for _, row in df.iterrows():
        score, action, reasons = calculate_v2_1_score_and_action(row)
        v2_1_scores.append(score)
        v2_1_actions.append(action)
        v2_1_reasons.append(reasons)

    df['v2_1_score'] = v2_1_scores
    df['v2_1_action'] = v2_1_actions
    df['v2_1_reasons'] = v2_1_reasons

    print(f"  ✓ v2.1 actions: {df['v2_1_action'].value_counts().to_dict()}")

    # [STEP 3] 差分分析
    print("\n[STEP 3] Analyzing differences...")

    df['action_changed'] = df['v2_0_3_action'] != df['v2_1_action']
    df['score_diff'] = df['v2_1_score'] - df['v2_0_3_score']

    changed_count = df['action_changed'].sum()
    print(f"  ✓ Action changed: {changed_count}/{len(df)} records ({changed_count/len(df)*100:.1f}%)")

    # [STEP 4] HTMLレポート生成
    print("\n[STEP 4] Generating HTML report...")
    html = generate_html_report(df)

    # [STEP 5] 保存
    print("\n[STEP 5] Saving...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  ✓ Saved: {OUTPUT_FILE}")

    print("\n✅ Comparison report generated successfully!")
    print(f"\n📄 Open: file://{OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
