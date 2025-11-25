#!/usr/bin/env python3
"""
simulate_stop_loss_sell_v2_1_0_2.py

v2.1.0.2の売りポジションに対して損切り水準を適用した場合のシミュレーション

損切り水準: +3%, +5%, +6%, +10%（株価上昇で損切り）
対象: 売り判定のみ

入力: improvement/data/v2_1_0_comparison_results.parquet
出力: improvement/stop_loss_sell_simulation_report.html
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
OUTPUT_FILE = IMPROVEMENT_DIR / "stop_loss_sell_simulation_report.html"

# 損切り水準（売りポジションなので上昇時に損切り）
STOP_LOSS_LEVELS = [3, 5, 6, 10]  # %


def apply_v2_1_0_1_strategy(row: pd.Series) -> str:
    """v2.1.0.1 ハイブリッド戦略を適用"""
    v2_0_3_action = row['v2_0_3_action']
    v2_1_0_action = row['v2_1_0_action']

    if v2_0_3_action == '買い' and v2_1_0_action == '静観':
        return '静観'
    elif v2_0_3_action == '静観' and v2_1_0_action == '売り':
        return '売り'
    else:
        return v2_0_3_action


def calculate_sell_with_stop_loss(df: pd.DataFrame, stop_loss_pct: float) -> dict:
    """
    売りポジションに損切り水準を適用した場合の成績を計算（日中Highベースで判定）

    Args:
        df: 売りポジションのデータフレーム
        stop_loss_pct: 損切り水準（%、正の値、株価上昇で損切り）

    Returns:
        成績の辞書
    """
    df_result = df.copy()

    # 損切り価格を計算（株価がこの価格を上回ったら損切り）
    df_result['stop_loss_price'] = df_result['buy_price'] * (1 + stop_loss_pct / 100)

    # 日中のHighが損切り価格を上回ったか判定
    df_result['stop_loss_triggered'] = df_result['high'] > df_result['stop_loss_price']

    # 実際の利益率（終値ベース、売りなので逆転）
    df_result['actual_profit_pct'] = (df_result['buy_price'] - df_result['daily_close']) / df_result['buy_price'] * 100
    df_result['actual_profit_100'] = (df_result['buy_price'] - df_result['daily_close']) * 100

    # 損切り適用後の利益率
    # 損切り発動: 損切り価格で決済（損失確定）
    # 損切り未発動: 終値で決済
    df_result['stop_loss_profit_pct'] = df_result.apply(
        lambda row: -stop_loss_pct if row['stop_loss_triggered'] else row['actual_profit_pct'],
        axis=1
    )
    df_result['stop_loss_profit_100'] = df_result.apply(
        lambda row: (row['buy_price'] - row['stop_loss_price']) * 100 if row['stop_loss_triggered'] else row['actual_profit_100'],
        axis=1
    )

    # 損切りによる利益差
    df_result['profit_diff_100'] = df_result['stop_loss_profit_100'] - df_result['actual_profit_100']

    # 機会損失（損切り発動したが、終値ではプラスだったケース）
    df_result['opportunity_loss'] = df_result.apply(
        lambda row: row['profit_diff_100'] if (row['stop_loss_triggered'] and row['actual_profit_100'] > 0) else 0,
        axis=1
    )

    # 勝ち負け判定（損切り適用後、売りなので利益>0で勝ち）
    df_result['win'] = df_result['stop_loss_profit_100'] > 0
    df_result['draw'] = df_result['stop_loss_profit_100'] == 0

    # 統計計算
    total = len(df_result)
    wins = df_result['win'].sum()
    draws = df_result['draw'].sum()
    losses = total - wins - draws
    win_rate = wins / (total - draws) * 100 if (total - draws) > 0 else 0

    total_profit = df_result['stop_loss_profit_100'].sum()
    avg_profit = df_result['stop_loss_profit_100'].mean()

    # 損切り発動統計
    stop_loss_count = df_result['stop_loss_triggered'].sum()
    stop_loss_rate = stop_loss_count / total * 100 if total > 0 else 0

    # 損切り発動時の平均損失軽減
    triggered_df = df_result[df_result['stop_loss_triggered']]
    avg_loss_reduction = triggered_df['profit_diff_100'].mean() if len(triggered_df) > 0 else 0

    # 機会損失統計
    opportunity_loss_cases = (df_result['opportunity_loss'] < 0).sum()
    opportunity_loss_rate = opportunity_loss_cases / total * 100 if total > 0 else 0
    total_opportunity_loss = df_result['opportunity_loss'].sum()
    avg_opportunity_loss = df_result[df_result['opportunity_loss'] < 0]['opportunity_loss'].mean() if opportunity_loss_cases > 0 else 0

    # 元の成績（損切りなし）
    original_total_profit = df_result['actual_profit_100'].sum()
    original_avg_profit = df_result['actual_profit_100'].mean()
    original_wins = (df_result['actual_profit_100'] > 0).sum()
    original_win_rate = original_wins / (total - (df_result['actual_profit_100'] == 0).sum()) * 100 if (total - (df_result['actual_profit_100'] == 0).sum()) > 0 else 0

    return {
        'total': total,
        'wins': wins,
        'draws': draws,
        'losses': losses,
        'win_rate': win_rate,
        'total_profit': total_profit,
        'avg_profit': avg_profit,
        'stop_loss_count': stop_loss_count,
        'stop_loss_rate': stop_loss_rate,
        'avg_loss_reduction': avg_loss_reduction,
        'opportunity_loss_cases': opportunity_loss_cases,
        'opportunity_loss_rate': opportunity_loss_rate,
        'total_opportunity_loss': total_opportunity_loss,
        'avg_opportunity_loss': avg_opportunity_loss,
        'original_total_profit': original_total_profit,
        'original_avg_profit': original_avg_profit,
        'original_wins': original_wins,
        'original_win_rate': original_win_rate,
        'profit_diff': total_profit - original_total_profit,
        'df': df_result
    }


def calculate_price_bracket_with_stop_loss(df: pd.DataFrame, stop_loss_pct: float) -> dict:
    """価格帯別の損切りシミュレーション"""
    brackets = {
        '1,000円未満': df[df['buy_price'] < 1000],
        '1,000-3,000円': df[(df['buy_price'] >= 1000) & (df['buy_price'] < 3000)],
        '3,000-5,000円': df[(df['buy_price'] >= 3000) & (df['buy_price'] < 5000)],
        '5,000-10,000円': df[(df['buy_price'] >= 5000) & (df['buy_price'] < 10000)]
    }

    results = {}
    for bracket_name, bracket_df in brackets.items():
        if len(bracket_df) > 0:
            results[bracket_name] = calculate_sell_with_stop_loss(bracket_df, stop_loss_pct)
        else:
            results[bracket_name] = None

    return results


def generate_html_report(df: pd.DataFrame) -> str:
    """HTMLレポートを生成"""
    # v2.1.0.1 判定を適用
    df['v2_1_0_1_action'] = df.apply(apply_v2_1_0_1_strategy, axis=1)

    # 売りポジションのみ抽出
    sell_df = df[df['v2_1_0_1_action'] == '売り'].copy()

    if len(sell_df) == 0:
        return "<html><body><h1>売りポジションがありません</h1></body></html>"

    # 各損切り水準でシミュレーション
    simulations = {}
    for stop_loss_pct in STOP_LOSS_LEVELS:
        simulations[stop_loss_pct] = {
            'overall': calculate_sell_with_stop_loss(sell_df, stop_loss_pct),
            'by_bracket': calculate_price_bracket_with_stop_loss(sell_df, stop_loss_pct)
        }

    # 損切りなしの成績（ベースライン）
    baseline = calculate_sell_with_stop_loss(sell_df, 100)  # 実質損切りなし

    # サマリーカードHTML生成
    summary_cards = []
    for stop_loss_pct in STOP_LOSS_LEVELS:
        result = simulations[stop_loss_pct]['overall']
        profit_diff = result['profit_diff']
        profit_diff_class = 'positive' if profit_diff > 0 else 'negative' if profit_diff < 0 else ''
        profit_diff_str = f'+{profit_diff:,.0f}' if profit_diff > 0 else f'{profit_diff:,.0f}'

        summary_cards.append(f'''
        <div class="summary-card">
            <h3>損切り +{stop_loss_pct}%</h3>
            <div class="stat-row">
                <span class="stat-label">勝率</span>
                <span class="stat-value {'positive' if result['win_rate'] >= 50 else 'negative'}">{result['win_rate']:.1f}%</span>
                <span class="stat-diff">({result['win_rate'] - baseline['win_rate']:+.1f}%)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">合計利益</span>
                <span class="stat-value {'positive' if result['total_profit'] > 0 else 'negative'}">{result['total_profit']:+,.0f}円</span>
                <span class="stat-diff {profit_diff_class}">({profit_diff_str}円)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">平均利益</span>
                <span class="stat-value {'positive' if result['avg_profit'] > 0 else 'negative'}">{result['avg_profit']:+,.0f}円</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">損切り発動率</span>
                <span class="stat-value">{result['stop_loss_rate']:.1f}%</span>
                <span class="stat-label">({result['stop_loss_count']}件)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">平均損失軽減</span>
                <span class="stat-value positive">{result['avg_loss_reduction']:+,.0f}円/件</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">機会損失発生率</span>
                <span class="stat-value negative">{result['opportunity_loss_rate']:.1f}%</span>
                <span class="stat-label">({result['opportunity_loss_cases']}件)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">機会損失合計</span>
                <span class="stat-value negative">{result['total_opportunity_loss']:,.0f}円</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">勝/負/分</span>
                <span class="stat-value">{result['wins']}/{result['losses']}/{result['draws']}</span>
            </div>
        </div>''')

    # 価格帯別比較テーブル
    bracket_comparison_rows = []
    bracket_names = ['1,000円未満', '1,000-3,000円', '3,000-5,000円', '5,000-10,000円']

    for bracket_name in bracket_names:
        # ベースライン（損切りなし）
        baseline_bracket = calculate_price_bracket_with_stop_loss(sell_df, 100)[bracket_name]
        if baseline_bracket is None:
            continue

        row_html = f'<tr><td class="bracket-name">{bracket_name}</td>'

        # 損切りなし
        row_html += f'''
        <td class="number">{baseline_bracket['total']}</td>
        <td class="number {'positive' if baseline_bracket['original_win_rate'] >= 50 else 'negative'}">{baseline_bracket['original_win_rate']:.1f}%</td>
        <td class="number {'positive' if baseline_bracket['original_total_profit'] > 0 else 'negative'}">{baseline_bracket['original_total_profit']:+,.0f}円</td>
        '''

        # 各損切り水準
        for stop_loss_pct in STOP_LOSS_LEVELS:
            result_bracket = simulations[stop_loss_pct]['by_bracket'][bracket_name]
            profit_diff = result_bracket['profit_diff']
            profit_diff_class = 'positive' if profit_diff > 0 else 'negative' if profit_diff < 0 else ''

            row_html += f'''
            <td class="number {'positive' if result_bracket['win_rate'] >= 50 else 'negative'}">{result_bracket['win_rate']:.1f}%<br><span class="stat-diff">({result_bracket['win_rate'] - baseline_bracket['original_win_rate']:+.1f}%)</span></td>
            <td class="number {'positive' if result_bracket['total_profit'] > 0 else 'negative'}">{result_bracket['total_profit']:+,.0f}円<br><span class="stat-diff {profit_diff_class}">({profit_diff:+,.0f}円)</span></td>
            '''

        row_html += '</tr>'
        bracket_comparison_rows.append(row_html)

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>売りポジション 損切りシミュレーション - V2.1.0.2</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
    padding: 40px 20px;
    color: #333;
}}
.container {{
    max-width: 1400px;
    margin: 0 auto;
    background: white;
    border-radius: 16px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
    overflow: hidden;
}}
.header {{
    background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
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
.baseline-box {{
    background: #f8f9fa;
    padding: 30px;
    margin: 30px;
    border-radius: 12px;
    border-left: 6px solid #27ae60;
}}
.baseline-box h2 {{
    color: #27ae60;
    margin-bottom: 15px;
}}
.baseline-stats {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 20px;
    margin-top: 15px;
}}
.baseline-stat {{
    background: white;
    padding: 15px;
    border-radius: 8px;
    text-align: center;
}}
.baseline-stat .label {{
    color: #666;
    font-size: 0.9em;
    margin-bottom: 5px;
}}
.baseline-stat .value {{
    font-size: 1.5em;
    font-weight: 700;
    color: #27ae60;
}}
.summary-section {{
    padding: 40px;
    background: #f8f9fa;
}}
.summary-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 24px;
    margin-bottom: 30px;
}}
.summary-card {{
    background: white;
    border-radius: 12px;
    padding: 28px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-left: 6px solid #e74c3c;
}}
.summary-card h3 {{
    font-size: 1.4em;
    margin-bottom: 20px;
    color: #e74c3c;
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
    color: #666;
    font-size: 0.95em;
}}
.stat-value {{
    font-weight: 600;
    font-size: 1.1em;
    color: #333;
}}
.stat-diff {{
    font-size: 0.85em;
    color: #999;
    margin-left: 8px;
}}
.positive {{ color: #27ae60 !important; }}
.negative {{ color: #e74c3c !important; }}
.table-section {{
    padding: 40px;
}}
.table-section h2 {{
    margin-bottom: 24px;
    color: #e74c3c;
    text-align: center;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    background: white;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
}}
thead {{
    background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
    color: white;
}}
th {{
    padding: 16px;
    text-align: left;
    font-weight: 600;
    font-size: 0.95em;
}}
th.number {{ text-align: right; }}
td {{
    padding: 12px 16px;
    border-bottom: 1px solid #f0f0f0;
}}
td.number {{ text-align: right; }}
td.bracket-name {{
    font-weight: 600;
    background: #f8f9fa;
}}
tr:hover {{
    background: #f8f9fa;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📉 売りポジション 損切りシミュレーション（日中High判定）</h1>
        <div class="subtitle">V2.1.0.2 売りポジション - 損切り水準別パフォーマンス比較</div>
        <div class="subtitle" style="margin-top: 10px;">対象: 売り判定 {len(sell_df)}銘柄 | 判定: 日中Highが損切り価格を上回ったら発動</div>
    </div>

    <div class="baseline-box">
        <h2>📈 ベースライン（損切りなし）</h2>
        <div class="baseline-stats">
            <div class="baseline-stat">
                <div class="label">勝率</div>
                <div class="value">{baseline['original_win_rate']:.1f}%</div>
            </div>
            <div class="baseline-stat">
                <div class="label">合計利益</div>
                <div class="value">{baseline['original_total_profit']:+,.0f}円</div>
            </div>
            <div class="baseline-stat">
                <div class="label">平均利益</div>
                <div class="value">{baseline['original_avg_profit']:+,.0f}円</div>
            </div>
            <div class="baseline-stat">
                <div class="label">勝/負/分</div>
                <div class="value">{baseline['original_wins']}/{baseline['total'] - baseline['original_wins'] - (sell_df['buy_price'] == sell_df['daily_close']).sum()}/{(sell_df['buy_price'] == sell_df['daily_close']).sum()}</div>
            </div>
        </div>
    </div>

    <div class="summary-section">
        <h2 style="margin-bottom: 20px; color: #e74c3c; text-align: center;">🎯 損切り水準別シミュレーション結果</h2>
        <div class="summary-grid">
            {''.join(summary_cards)}
        </div>
    </div>

    <div class="table-section">
        <h2>📊 価格帯別 損切りシミュレーション比較</h2>
        <table>
            <thead>
                <tr>
                    <th rowspan="2">価格帯</th>
                    <th colspan="3" style="text-align: center; border-right: 2px solid white;">損切りなし</th>
                    <th colspan="2" style="text-align: center; border-right: 2px solid white;">+3%</th>
                    <th colspan="2" style="text-align: center; border-right: 2px solid white;">+5%</th>
                    <th colspan="2" style="text-align: center; border-right: 2px solid white;">+6%</th>
                    <th colspan="2" style="text-align: center;">+10%</th>
                </tr>
                <tr>
                    <th class="number">銘柄数</th>
                    <th class="number">勝率</th>
                    <th class="number" style="border-right: 2px solid white;">合計利益</th>
                    <th class="number">勝率</th>
                    <th class="number" style="border-right: 2px solid white;">合計利益</th>
                    <th class="number">勝率</th>
                    <th class="number" style="border-right: 2px solid white;">合計利益</th>
                    <th class="number">勝率</th>
                    <th class="number" style="border-right: 2px solid white;">合計利益</th>
                    <th class="number">勝率</th>
                    <th class="number">合計利益</th>
                </tr>
            </thead>
            <tbody>
                {''.join(bracket_comparison_rows)}
            </tbody>
        </table>
    </div>
</div>

<div style="text-align: center; color: white; padding: 20px; font-size: 0.9em;">
    <p>売りポジション損切りシミュレーション: 損切り水準: +3%, +5%, +6%, +10%</p>
    <p style="margin-top: 10px;">判定方法: 日中Highが損切り価格（買値×1.0X）を上回った場合に損切り発動 | 機会損失: 損切り後に終値がプラスだったケース</p>
</div>

</body>
</html>'''

    return html


def main() -> int:
    print("=" * 60)
    print("Sell Position Stop Loss Simulation for V2.1.0.2")
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

    print("\n✅ Simulation report generated successfully!")
    print(f"\n📄 Open the report: file://{OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
