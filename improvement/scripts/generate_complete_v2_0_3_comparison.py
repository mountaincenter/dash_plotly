#!/usr/bin/env python3
"""
generate_complete_v2_0_3_comparison.py

完全版v2.0.3ロジックを使用した比較レポート生成

入力: improvement/data/grok_analysis_merged_20251121_with_indicators.parquet
出力: improvement/data/complete_v2_0_3_comparison.parquet
      improvement/v2_0_3_complete_comparison_report.html
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from datetime import datetime

# generate_trading_recommendation_v2_0_3.pyから関数をインポート
# Note: スクリプトのパス設定が修正されたため、直接実行可能
import importlib.util
spec = importlib.util.spec_from_file_location(
    "v2_0_3_module",
    ROOT / "improvement" / "scripts" / "generate_trading_recommendation_v2_0_3.py"
)
v2_0_3_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v2_0_3_module)

calculate_rank_score_improved = v2_0_3_module.calculate_rank_score_improved
fetch_jquants_fundamentals = v2_0_3_module.fetch_jquants_fundamentals
fetch_prices_from_parquet = v2_0_3_module.fetch_prices_from_parquet
load_backtest_stats = v2_0_3_module.load_backtest_stats

IMPROVEMENT_DIR = ROOT / "improvement"
INPUT_FILE = IMPROVEMENT_DIR / "data" / "grok_analysis_merged_20251121_with_indicators.parquet"
OUTPUT_FILE = IMPROVEMENT_DIR / "data" / "complete_v2_0_3_comparison.parquet"
OUTPUT_HTML = IMPROVEMENT_DIR / "v2_0_3_complete_comparison_report.html"

# Note: The imported functions already have the correct paths defined
# We just need to ensure they use ROOT not IMPROVEMENT_DIR


def calculate_complete_v2_0_3(
    row: pd.Series,
    backtest_stats: dict,
    total_stocks: int
) -> tuple[str, int]:
    """
    完全版v2.0.3ロジック
    generate_trading_recommendation_v2_0_3.pyのdetermine_action_comprehensive_v2()を簡略化
    """
    ticker = row['ticker']
    grok_rank = row['grok_rank']
    prev_close = row['prev_day_close']

    # バックテスト勝率
    backtest_win_rate = backtest_stats.get('rank_win_rates', {}).get(grok_rank)
    backtest_win_rate_decimal = backtest_win_rate / 100 if backtest_win_rate else None

    # ランクスコア
    rank_score = calculate_rank_score_improved(grok_rank, total_stocks, backtest_win_rate_decimal)
    score = rank_score

    # ファンダメンタルズ取得
    fundamentals = fetch_jquants_fundamentals(ticker)

    if fundamentals:
        # ROE
        roe = fundamentals.get('roe', 0)
        if roe > 15:
            score += 20
        elif roe < 0:
            score -= 15

        # 営業利益成長率
        growth = fundamentals.get('operatingProfitGrowth')
        if growth:
            if growth > 50:
                score += 25
            elif growth < -30:
                score -= 20

    # 株価情報取得
    price_data = fetch_prices_from_parquet(ticker)

    if price_data:
        # 前日変化率
        daily_change = price_data.get('dailyChangePct', 0)
        if daily_change < -3:
            score += 15
        elif daily_change > 10:
            score -= 10

        # ATRボラティリティ
        atr_pct = price_data.get('atrPct')
        if atr_pct:
            if atr_pct < 3.0:
                score += 10
            elif atr_pct > 8.0:
                score -= 15

        # 移動平均との位置関係
        current_price = price_data.get('currentPrice')
        ma25 = price_data.get('ma25')
        if current_price and ma25:
            if current_price > ma25 * 1.05:
                score -= 10
            elif current_price < ma25 * 0.95:
                score += 10

        # 価格帯補正 (5000-8000円)
        if 5000 <= current_price <= 8000:
            score += 25

    # 行動決定
    if score >= 40:
        action = '買い'
    elif score >= 20:
        action = '買い'
    elif score <= -30:
        action = '売り'
    elif score <= -15:
        action = '売り'
    else:
        action = '静観'

    # 価格帯強制判定
    if pd.notna(prev_close):
        if 5000 <= prev_close < 10000:
            action = '買い'
        elif prev_close >= 10000:
            action = '売り'

    return (action, score)


def calculate_pipeline_v2_0_3(
    grok_rank: int,
    total_stocks: int,
    prev_close: float,
    prev_day_change_pct: float,
    atr_pct: float | None,
    backtest_win_rate: float | None
) -> tuple[str, int]:
    """pipeline版 v2.0.3 ロジック"""

    # 価格帯強制判定
    if pd.notna(prev_close):
        if 5000 <= prev_close < 10000:
            return ('買い', 100)
        elif prev_close >= 10000:
            return ('売り', -100)

    # スコア計算
    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    if relative_position <= 0.25:
        score = 40
    elif relative_position <= 0.50:
        score = 20
    elif relative_position <= 0.75:
        score = 0
    else:
        score = -10

    # バックテスト勝率調整
    if backtest_win_rate:
        bwr_decimal = backtest_win_rate / 100
        if bwr_decimal >= 0.70:
            score += 30
        elif bwr_decimal >= 0.60:
            score += 20
        elif bwr_decimal >= 0.50:
            score += 10
        elif bwr_decimal <= 0.30:
            score -= 20

    # 前日変化率
    if pd.notna(prev_day_change_pct):
        if prev_day_change_pct < -5:
            score += 15
        elif prev_day_change_pct > 10:
            score -= 10

    # ATR調整
    if atr_pct:
        if atr_pct < 3.0:
            score += 10
        elif atr_pct > 8.0:  # 先に高ボラをチェック
            score -= 15
        elif atr_pct > 5.0:  # 次に中ボラ
            score += 15

    # 閾値判定
    if score >= 40:
        action = '買い'
    elif score >= 20:
        action = '買い'
    elif score <= -30:
        action = '売り'
    elif score <= -15:
        action = '売り'
    else:
        action = '静観'

    return (action, score)


def generate_html_report(df: pd.DataFrame) -> str:
    """HTMLレポートを生成"""

    # 統計計算
    imp_counts = df['imp_action'].value_counts().to_dict()
    pipe_counts = df['pipe_action'].value_counts().to_dict()
    hybrid_counts = df['hybrid_action'].value_counts().to_dict()

    total_records = len(df)

    # 各戦略の成績を計算
    def calc_performance(df_subset, action_col):
        """買い・売り成績を計算"""
        buy_df = df_subset[df_subset[action_col] == '買い'].copy()
        sell_df = df_subset[df_subset[action_col] == '売り'].copy()

        # 買い成績
        if len(buy_df) > 0:
            buy_df['buy_win'] = buy_df['daily_close'] > buy_df['buy_price']
            buy_df['buy_profit'] = (buy_df['daily_close'] - buy_df['buy_price']) * 100
            buy_wins = buy_df['buy_win'].sum()
            buy_total = len(buy_df)
            buy_win_rate = buy_wins / buy_total * 100
            buy_total_profit = buy_df['buy_profit'].sum()
        else:
            buy_wins = buy_total = buy_win_rate = buy_total_profit = 0

        # 売り成績
        if len(sell_df) > 0:
            sell_df['sell_win'] = sell_df['buy_price'] > sell_df['daily_close']
            sell_df['sell_profit'] = (sell_df['buy_price'] - sell_df['daily_close']) * 100
            sell_wins = sell_df['sell_win'].sum()
            sell_total = len(sell_df)
            sell_win_rate = sell_wins / sell_total * 100
            sell_total_profit = sell_df['sell_profit'].sum()
        else:
            sell_wins = sell_total = sell_win_rate = sell_total_profit = 0

        # 総合
        total_wins = buy_wins + sell_wins
        total_count = buy_total + sell_total
        total_win_rate = total_wins / total_count * 100 if total_count > 0 else 0
        total_profit = buy_total_profit + sell_total_profit

        return {
            'buy': {'total': buy_total, 'wins': buy_wins, 'win_rate': buy_win_rate, 'profit': buy_total_profit},
            'sell': {'total': sell_total, 'wins': sell_wins, 'win_rate': sell_win_rate, 'profit': sell_total_profit},
            'total': {'count': total_count, 'wins': total_wins, 'win_rate': total_win_rate, 'profit': total_profit}
        }

    # 各戦略の成績を計算
    imp_perf = calc_performance(df, 'imp_action')
    pipe_perf = calc_performance(df, 'pipe_action')
    hybrid_perf = calc_performance(df, 'hybrid_action')

    # 後方互換性のため既存変数も設定
    buy_wins = hybrid_perf['buy']['wins']
    buy_total = hybrid_perf['buy']['total']
    buy_win_rate = hybrid_perf['buy']['win_rate']
    buy_total_profit = hybrid_perf['buy']['profit']
    sell_wins = hybrid_perf['sell']['wins']
    sell_total = hybrid_perf['sell']['total']
    sell_win_rate = hybrid_perf['sell']['win_rate']
    sell_total_profit = hybrid_perf['sell']['profit']

    # テーブル行を生成
    table_rows = []
    current_date = None

    for _, row in df.sort_values(['selection_date', 'hybrid_score'], ascending=[False, False]).iterrows():
        date_str = row['selection_date']

        # 日付セパレータ
        if date_str != current_date:
            current_date = date_str
            table_rows.append(f'''
        <tr class="date-separator">
            <td colspan="14">{date_str}</td>
        </tr>''')

        ticker = row['ticker']
        stock_name = row.get('stock_name', '')
        grok_rank = row['grok_rank']
        prev_close = row['prev_day_close']

        imp_score = row['imp_score']
        imp_action = row['imp_action']
        pipe_score = row['pipe_score']
        pipe_action = row['pipe_action']
        hybrid_score = row['hybrid_score']
        hybrid_action = row['hybrid_action']
        hybrid_source = row['hybrid_source']

        buy_price = row.get('buy_price', 0)
        daily_close = row.get('daily_close', 0)
        price_diff = daily_close - buy_price
        price_diff_str = f'+{price_diff:.0f}' if price_diff > 0 else f'{price_diff:.0f}'
        price_diff_class = 'positive' if price_diff > 0 else 'negative' if price_diff < 0 else ''

        # 100株あたりの利益計算
        if hybrid_action == '売り':
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

        table_rows.append(f'''
        <tr class="action-{hybrid_action}">
            <td>{ticker}</td>
            <td>{stock_name}</td>
            <td class="number">{grok_rank}</td>
            <td class="number">{prev_close:,.0f}</td>
            <td class="number">{imp_score}</td>
            <td><span class="action-{imp_action}-badge action-badge">{imp_action}</span></td>
            <td class="number">{pipe_score}</td>
            <td><span class="action-{pipe_action}-badge action-badge">{pipe_action}</span></td>
            <td class="number">{hybrid_score}</td>
            <td><span class="action-{hybrid_action}-badge action-badge">{hybrid_action}</span></td>
            <td class="number">{buy_price:,.0f}</td>
            <td class="number">{daily_close:,.0f}</td>
            <td class="number {result_class}" style="font-weight: bold;">{result}</td>
            <td class="number {profit_class}" style="font-weight: bold;">{profit_str}</td>
        </tr>''')

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>完全版 v2.0.3 Hybrid 比較レポート</title>
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
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>完全版 v2.0.3 Hybrid 比較レポート</h1>
        <div class="subtitle">Improvement v2.0.3 (完全版) vs Pipeline v2.0.3 vs Hybrid</div>
        <div class="subtitle">対象期間: 2025-11-04 ~ 2025-11-21 | 総レコード数: {total_records}</div>
    </div>

    <div class="summary-section">
        <div class="summary-grid">
            <div class="summary-card">
                <h3>📊 Improvement v2.0.3 (完全版)</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{imp_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{imp_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{imp_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>🚀 Pipeline v2.0.3</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{pipe_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{pipe_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{pipe_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #f39c12;">
                <h3>⚡ Hybrid (売り優先)</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{hybrid_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{hybrid_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{hybrid_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #e74c3c;">
                <h3>💰 Hybrid「買い」成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{buy_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_win_rate > 50 else '#e74c3c'};">{buy_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_total_profit > 0 else '#e74c3c'};">{buy_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #27ae60;">
                <h3>📉 Hybrid「売り」成績</h3>
                <div class="stat-row">
                    <span class="stat-label">対象銘柄数</span>
                    <span class="stat-value">{sell_total}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_win_rate > 50 else '#e74c3c'};">{sell_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_total_profit > 0 else '#e74c3c'};">{sell_total_profit:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #667eea;">
                <h3>🏆 Improvement 総合成績</h3>
                <div class="stat-row">
                    <span class="stat-label">買い勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if imp_perf['buy']['win_rate'] > 50 else '#e74c3c'};">{imp_perf['buy']['win_rate']:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">買い利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if imp_perf['buy']['profit'] > 0 else '#e74c3c'};">{imp_perf['buy']['profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if imp_perf['sell']['win_rate'] > 50 else '#e74c3c'};">{imp_perf['sell']['win_rate']:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if imp_perf['sell']['profit'] > 0 else '#e74c3c'};">{imp_perf['sell']['profit']:+,.0f}円</span>
                </div>
                <div class="stat-row" style="border-top: 2px solid #667eea; margin-top: 10px; padding-top: 10px;">
                    <span class="stat-label" style="font-weight: 700;">総合利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if imp_perf['total']['profit'] > 0 else '#e74c3c'}; font-size: 1.8em;">{imp_perf['total']['profit']:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #3498db;">
                <h3>📊 Pipeline 総合成績</h3>
                <div class="stat-row">
                    <span class="stat-label">買い勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if pipe_perf['buy']['win_rate'] > 50 else '#e74c3c'};">{pipe_perf['buy']['win_rate']:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">買い利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if pipe_perf['buy']['profit'] > 0 else '#e74c3c'};">{pipe_perf['buy']['profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if pipe_perf['sell']['win_rate'] > 50 else '#e74c3c'};">{pipe_perf['sell']['win_rate']:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if pipe_perf['sell']['profit'] > 0 else '#e74c3c'};">{pipe_perf['sell']['profit']:+,.0f}円</span>
                </div>
                <div class="stat-row" style="border-top: 2px solid #3498db; margin-top: 10px; padding-top: 10px;">
                    <span class="stat-label" style="font-weight: 700;">総合利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if pipe_perf['total']['profit'] > 0 else '#e74c3c'}; font-size: 1.8em;">{pipe_perf['total']['profit']:+,.0f}円</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #f39c12;">
                <h3>⚡ Hybrid 総合成績</h3>
                <div class="stat-row">
                    <span class="stat-label">買い勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if hybrid_perf['buy']['win_rate'] > 50 else '#e74c3c'};">{hybrid_perf['buy']['win_rate']:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">買い利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hybrid_perf['buy']['profit'] > 0 else '#e74c3c'};">{hybrid_perf['buy']['profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if hybrid_perf['sell']['win_rate'] > 50 else '#e74c3c'};">{hybrid_perf['sell']['win_rate']:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hybrid_perf['sell']['profit'] > 0 else '#e74c3c'};">{hybrid_perf['sell']['profit']:+,.0f}円</span>
                </div>
                <div class="stat-row" style="border-top: 2px solid #f39c12; margin-top: 10px; padding-top: 10px;">
                    <span class="stat-label" style="font-weight: 700;">総合利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if hybrid_perf['total']['profit'] > 0 else '#e74c3c'}; font-size: 1.8em;">{hybrid_perf['total']['profit']:+,.0f}円</span>
                </div>
            </div>
        </div>
    </div>

    <div class="table-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">詳細比較テーブル</h2>
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th class="number">Grokランク</th>
                    <th class="number">前日終値</th>
                    <th class="number">Imp<br/>スコア</th>
                    <th>Imp<br/>判定</th>
                    <th class="number">Pipe<br/>スコア</th>
                    <th>Pipe<br/>判定</th>
                    <th class="number">Hybrid<br/>スコア</th>
                    <th>Hybrid<br/>判定</th>
                    <th class="number">始値</th>
                    <th class="number">終値</th>
                    <th class="number">勝負引分</th>
                    <th class="number">100株利益</th>
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


def main():
    print("=" * 80)
    print("完全版v2.0.3 比較レポート生成")
    print("=" * 80)
    print()

    # データ読み込み
    print("[1/6] データ読み込み中...")
    df = pd.read_parquet(INPUT_FILE)
    print(f"  総レコード数: {len(df)}")
    print()

    # バックテスト統計
    print("[2/6] バックテスト統計読み込み中...")
    backtest_stats = load_backtest_stats()
    rank_win_rates = backtest_stats.get('rank_win_rates', {})
    print()

    # 各レコードに対して判定を適用
    print("[3/6] 完全版v2.0.3判定を適用中...")
    results = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        selection_date = row['selection_date']
        total_stocks = row.get('total_stocks', len(df[df['selection_date'] == selection_date]))

        print(f"  [{idx+1}/{len(df)}] {ticker} ({selection_date})...")

        # Improvement完全版v2.0.3
        imp_action, imp_score = calculate_complete_v2_0_3(row, backtest_stats, total_stocks)

        # Pipeline版v2.0.3
        grok_rank = row['grok_rank']
        prev_close = row['prev_day_close']
        prev_day_change_pct = row.get('prev_day_change_pct', 0)

        # ATRを計算（簡易版: daily_volatilityから推定）
        atr_pct = row.get('daily_volatility')

        backtest_win_rate = rank_win_rates.get(grok_rank)

        pipe_action, pipe_score = calculate_pipeline_v2_0_3(
            grok_rank, total_stocks, prev_close, prev_day_change_pct, atr_pct, backtest_win_rate
        )

        # ハイブリッド判定（売り優先）
        if imp_action == '売り':
            hybrid_action = '売り'
            hybrid_score = imp_score
            hybrid_source = 'improvement'
        elif pipe_action == '買い':
            hybrid_action = '買い'
            hybrid_score = pipe_score
            hybrid_source = 'pipeline'
        else:
            hybrid_action = '静観'
            hybrid_score = 0
            hybrid_source = 'both'

        results.append({
            'selection_date': selection_date,
            'ticker': ticker,
            'stock_name': row.get('stock_name', ''),
            'grok_rank': grok_rank,
            'prev_day_close': prev_close,
            'buy_price': row.get('buy_price'),
            'daily_close': row.get('daily_close'),
            'imp_action': imp_action,
            'imp_score': imp_score,
            'pipe_action': pipe_action,
            'pipe_score': pipe_score,
            'hybrid_action': hybrid_action,
            'hybrid_score': hybrid_score,
            'hybrid_source': hybrid_source,
        })

    result_df = pd.DataFrame(results)
    print()

    # 保存
    print("[4/6] 結果を保存中...")
    result_df.to_parquet(OUTPUT_FILE, index=False)
    print(f"  保存完了: {OUTPUT_FILE}")
    print()

    # HTMLレポート生成
    print("[5/6] HTMLレポート生成中...")
    html = generate_html_report(result_df)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  保存完了: {OUTPUT_HTML}")
    print()

    # サマリー
    print("[6/6] サマリー:")
    print(f"  Improvement版 - 買い: {len(result_df[result_df['imp_action']=='買い'])}, "
          f"売り: {len(result_df[result_df['imp_action']=='売り'])}, "
          f"静観: {len(result_df[result_df['imp_action']=='静観'])}")
    print(f"  Pipeline版    - 買い: {len(result_df[result_df['pipe_action']=='買い'])}, "
          f"売り: {len(result_df[result_df['pipe_action']=='売り'])}, "
          f"静観: {len(result_df[result_df['pipe_action']=='静観'])}")
    print(f"  Hybrid版      - 買い: {len(result_df[result_df['hybrid_action']=='買い'])}, "
          f"売り: {len(result_df[result_df['hybrid_action']=='売り'])}, "
          f"静観: {len(result_df[result_df['hybrid_action']=='静観'])}")
    print()
    print(f"📄 HTMLレポート: file://{OUTPUT_HTML}")
    print()
    print("完了!")


if __name__ == '__main__':
    main()
