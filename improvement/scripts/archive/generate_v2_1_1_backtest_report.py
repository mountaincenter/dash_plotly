#!/usr/bin/env python3
"""
generate_v2_1_1_backtest_report.py

v2.1.1 バックテストレポート生成
v2.1.0.1ハイブリッド戦略 + 最適損切り戦略

最適損切り戦略:
- 買い（1,000円未満）: 損切りなし
- 買い（1,000-3,000円）: 損切り-5%
- 買い（3,000円以上）: 損切り-3%
- 売り（全価格帯）: 損切り+5%

表示項目:
- 購入価格、高値、安値、終値
- 損切り有無（発動したか）
- 100株利益（損切り適用後）
- 損切りによる影響（回避=プラス、機会損失=マイナス）

入力: improvement/data/v2_1_0_comparison_results.parquet
出力: improvement/v2_1_1_backtest_report.html
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
OUTPUT_FILE = IMPROVEMENT_DIR / "v2_1_1_backtest_report.html"


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


def get_optimal_stop_loss(action: str, buy_price: float) -> float | None:
    """
    最適損切り水準を返す

    Returns:
        損切り水準（%）、損切りなしの場合はNone
    """
    if action == '売り':
        return 5.0  # +5%
    elif action == '買い':
        if buy_price < 1000:
            return None  # 損切りなし
        elif buy_price < 3000:
            return -5.0  # -5%
        else:
            return -3.0  # -3%
    else:  # 静観
        return None


def apply_stop_loss(row: pd.Series) -> dict:
    """
    損切りを適用して結果を計算

    Returns:
        {
            'stop_loss_triggered': bool,
            'stop_loss_level': float | None,
            'profit_100_no_sl': float,
            'profit_100_with_sl': float,
            'profit_impact': float
        }
    """
    action = row['v2_1_1_action']
    buy_price = row['buy_price']
    high = row['high']
    low = row['low']
    daily_close = row['daily_close']

    stop_loss_level = get_optimal_stop_loss(action, buy_price)

    # 損切りなしの利益
    if action == '売り':
        profit_100_no_sl = (buy_price - daily_close) * 100
    else:  # 買いまたは静観
        profit_100_no_sl = (daily_close - buy_price) * 100

    # 損切りなしの場合
    if stop_loss_level is None:
        return {
            'stop_loss_triggered': False,
            'stop_loss_level': None,
            'profit_100_no_sl': profit_100_no_sl,
            'profit_100_with_sl': profit_100_no_sl,
            'profit_impact': 0
        }

    # 損切りありの場合
    if action == '売り':
        # 売りポジション: Highが損切り価格を上回ったら損切り
        stop_loss_price = buy_price * (1 + stop_loss_level / 100)
        stop_loss_triggered = high > stop_loss_price

        if stop_loss_triggered:
            profit_100_with_sl = (buy_price - stop_loss_price) * 100
        else:
            profit_100_with_sl = profit_100_no_sl

    else:  # 買いポジション
        # 買いポジション: Lowが損切り価格を下回ったら損切り
        stop_loss_price = buy_price * (1 + stop_loss_level / 100)
        stop_loss_triggered = low < stop_loss_price

        if stop_loss_triggered:
            profit_100_with_sl = (stop_loss_price - buy_price) * 100
        else:
            profit_100_with_sl = profit_100_no_sl

    profit_impact = profit_100_with_sl - profit_100_no_sl

    return {
        'stop_loss_triggered': stop_loss_triggered,
        'stop_loss_level': stop_loss_level,
        'profit_100_no_sl': profit_100_no_sl,
        'profit_100_with_sl': profit_100_with_sl,
        'profit_impact': profit_impact
    }


def generate_html_report(df: pd.DataFrame) -> str:
    """HTMLレポートを生成"""
    # v2.1.0.1 判定を適用
    df['v2_1_0_1_action'] = df.apply(apply_v2_1_0_1_strategy, axis=1)
    df['v2_1_1_action'] = df['v2_1_0_1_action']  # v2.1.1はv2.1.0.1と同じ判定

    # 損切りを適用
    stop_loss_results = df.apply(apply_stop_loss, axis=1)
    df['stop_loss_triggered'] = stop_loss_results.apply(lambda x: x['stop_loss_triggered'])
    df['stop_loss_level'] = stop_loss_results.apply(lambda x: x['stop_loss_level'])
    df['profit_100_no_sl'] = stop_loss_results.apply(lambda x: x['profit_100_no_sl'])
    df['profit_100_with_sl'] = stop_loss_results.apply(lambda x: x['profit_100_with_sl'])
    df['profit_impact'] = stop_loss_results.apply(lambda x: x['profit_impact'])

    # 判定別にデータを分割
    buy_df = df[df['v2_1_1_action'] == '買い'].copy()
    hold_df = df[df['v2_1_1_action'] == '静観'].copy()
    sell_df = df[df['v2_1_1_action'] == '売り'].copy()

    # 統計計算関数
    def calc_stats(df_action):
        if len(df_action) == 0:
            return {
                'total': 0, 'wins': 0, 'draws': 0, 'losses': 0,
                'win_rate': 0, 'total_profit': 0, 'avg_profit': 0,
                'stop_loss_count': 0, 'stop_loss_rate': 0,
                'total_impact': 0, 'avoided_loss': 0, 'opportunity_loss': 0
            }

        wins = (df_action['profit_100_with_sl'] > 0).sum()
        draws = (df_action['profit_100_with_sl'] == 0).sum()
        losses = len(df_action) - wins - draws
        win_rate = wins / (len(df_action) - draws) * 100 if (len(df_action) - draws) > 0 else 0

        return {
            'total': len(df_action),
            'wins': wins,
            'draws': draws,
            'losses': losses,
            'win_rate': win_rate,
            'total_profit': df_action['profit_100_with_sl'].sum(),
            'avg_profit': df_action['profit_100_with_sl'].mean(),
            'stop_loss_count': df_action['stop_loss_triggered'].sum(),
            'stop_loss_rate': df_action['stop_loss_triggered'].sum() / len(df_action) * 100,
            'total_impact': df_action['profit_impact'].sum(),
            'avoided_loss': df_action[df_action['profit_impact'] > 0]['profit_impact'].sum(),
            'opportunity_loss': df_action[df_action['profit_impact'] < 0]['profit_impact'].sum()
        }

    buy_stats = calc_stats(buy_df)
    hold_stats = calc_stats(hold_df)
    sell_stats = calc_stats(sell_df)

    # 全データを1つのテーブルに
    df_sorted = df.copy()
    df_sorted['backtest_date'] = pd.to_datetime(df_sorted['backtest_date'])
    df_sorted['action_order'] = df_sorted['v2_1_1_action'].map({'買い': 1, '静観': 2, '売り': 3})
    df_sorted = df_sorted.sort_values(['backtest_date', 'action_order'], ascending=[False, True])

    table_rows = []
    current_date = None

    for _, row in df_sorted.iterrows():
        date_str = row['backtest_date'].strftime('%Y-%m-%d')

        # 日付セパレータ
        if date_str != current_date:
            current_date = date_str
            table_rows.append(f'<tr class="date-separator"><td colspan="15">{date_str}</td></tr>')

        ticker = row['ticker']
        company = row.get('stock_name', '')
        action = row['v2_1_1_action']
        buy_price = row['buy_price']
        high = row['high']
        low = row['low']
        daily_close = row['daily_close']

        stop_loss_level = row['stop_loss_level']
        stop_loss_triggered = row['stop_loss_triggered']
        profit_100_with_sl = row['profit_100_with_sl']
        profit_impact = row['profit_impact']

        # 損切り表示
        if stop_loss_level is None:
            stop_loss_display = 'なし'
        else:
            if action == '売り':
                stop_loss_display = f'+{stop_loss_level:.0f}%'
            else:
                stop_loss_display = f'{stop_loss_level:.0f}%'

        # 損切り発動表示
        if stop_loss_triggered:
            triggered_badge = '<span class="badge-triggered">発動</span>'
        else:
            triggered_badge = '<span class="badge-not-triggered">-</span>'

        # 利益クラス
        profit_class = 'positive' if profit_100_with_sl > 0 else 'negative' if profit_100_with_sl < 0 else ''
        profit_str = f'+{profit_100_with_sl:,.0f}' if profit_100_with_sl > 0 else f'{profit_100_with_sl:,.0f}'

        # 損切り影響クラス
        if profit_impact > 0:
            impact_class = 'impact-positive'
            impact_label = '回避'
            impact_str = f'+{profit_impact:,.0f}'
        elif profit_impact < 0:
            impact_class = 'impact-negative'
            impact_label = '機会損失'
            impact_str = f'{profit_impact:,.0f}'
        else:
            impact_class = ''
            impact_label = '-'
            impact_str = '0'

        # 勝敗判定
        win_status = '勝' if profit_100_with_sl > 0 else '分' if profit_100_with_sl == 0 else '負'

        table_rows.append(f'''
        <tr class="action-{action}">
            <td>{date_str}</td>
            <td>{ticker}</td>
            <td>{company}</td>
            <td><span class="badge-{action}">{action}</span></td>
            <td class="number">{buy_price:,.0f}円</td>
            <td class="number">{high:,.0f}円</td>
            <td class="number">{low:,.0f}円</td>
            <td class="number">{daily_close:,.0f}円</td>
            <td class="number">{stop_loss_display}</td>
            <td class="center">{triggered_badge}</td>
            <td class="number {profit_class}">{profit_str}円</td>
            <td class="center {impact_class}">{impact_label}</td>
            <td class="number {impact_class}">{impact_str}円</td>
            <td class="result-{win_status}">{win_status}</td>
        </tr>''')

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V2.1.1 バックテストレポート（最適損切り戦略）</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 40px 20px;
    color: #333;
}}
.container {{
    max-width: 1600px;
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
.strategy-box {{
    background: #f8f9fa;
    padding: 30px;
    margin: 30px;
    border-radius: 12px;
    border-left: 6px solid #667eea;
}}
.strategy-box h2 {{
    color: #667eea;
    margin-bottom: 15px;
}}
.strategy-box ul {{
    list-style: none;
    padding-left: 0;
}}
.strategy-box li {{
    padding: 8px 0;
    border-bottom: 1px solid #e0e0e0;
}}
.strategy-box li:last-child {{
    border-bottom: none;
}}
.summary-section {{
    padding: 40px;
    background: #f8f9fa;
}}
.summary-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
    gap: 24px;
    margin-bottom: 30px;
}}
.summary-card {{
    background: white;
    border-radius: 12px;
    padding: 28px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-left: 6px solid #667eea;
}}
.summary-card h3 {{
    font-size: 1.4em;
    margin-bottom: 20px;
    color: #667eea;
}}
.stat-row {{
    display: flex;
    justify-content: space-between;
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
.positive {{ color: #27ae60 !important; }}
.negative {{ color: #e74c3c !important; }}
.impact-positive {{ color: #27ae60 !important; font-weight: 600; }}
.impact-negative {{ color: #e74c3c !important; font-weight: 600; }}
.table-section {{
    padding: 40px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    background: white;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
    font-size: 0.9em;
}}
thead {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
}}
th {{
    padding: 12px 8px;
    text-align: left;
    font-weight: 600;
    font-size: 0.9em;
}}
th.number {{ text-align: right; }}
th.center {{ text-align: center; }}
td {{
    padding: 10px 8px;
    border-bottom: 1px solid #f0f0f0;
}}
td.number {{ text-align: right; }}
td.center {{ text-align: center; }}
tr:hover {{
    background: #f8f9fa;
}}
.date-separator {{
    background: #e8eaf6 !important;
    font-weight: bold;
    text-align: center;
}}
.date-separator td {{
    padding: 8px !important;
}}
.action-買い {{
    background: rgba(52, 152, 219, 0.05);
}}
.action-静観 {{
    background: rgba(149, 165, 166, 0.05);
}}
.action-売り {{
    background: rgba(231, 76, 60, 0.05);
}}
.badge-買い {{
    background: #3498db;
    color: white;
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 0.85em;
    font-weight: 600;
}}
.badge-静観 {{
    background: #95a5a6;
    color: white;
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 0.85em;
    font-weight: 600;
}}
.badge-売り {{
    background: #e74c3c;
    color: white;
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 0.85em;
    font-weight: 600;
}}
.badge-triggered {{
    background: #e74c3c;
    color: white;
    padding: 2px 8px;
    border-radius: 8px;
    font-size: 0.8em;
    font-weight: 600;
}}
.badge-not-triggered {{
    color: #999;
    font-size: 0.8em;
}}
.result-勝 {{
    color: #27ae60;
    font-weight: bold;
    text-align: center;
}}
.result-負 {{
    color: #e74c3c;
    font-weight: bold;
    text-align: center;
}}
.result-分 {{
    color: #95a5a6;
    font-weight: bold;
    text-align: center;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 V2.1.1 バックテストレポート</h1>
        <div class="subtitle">ハイブリッド戦略（v2.1.0.1）+ 最適損切り戦略</div>
        <div class="subtitle" style="margin-top: 10px;">期間: 2025-11-04 ~ 2025-11-21 | 対象: Grok銘柄 {len(df)}件</div>
    </div>

    <div class="strategy-box">
        <h2>🎯 V2.1.1 戦略</h2>
        <ul>
            <li>✅ <strong>ハイブリッド判定</strong>: 買い→静観、静観→売りのみv2.1.0適用</li>
            <li>💎 <strong>買い（1,000円未満）</strong>: 損切りなし</li>
            <li>💰 <strong>買い（1,000-3,000円）</strong>: 損切り-5%（Low判定）</li>
            <li>💵 <strong>買い（3,000円以上）</strong>: 損切り-3%（Low判定）</li>
            <li>📉 <strong>売り（全価格帯）</strong>: 損切り+5%（High判定）</li>
        </ul>
    </div>

    <div class="summary-section">
        <h2 style="margin-bottom: 20px; color: #667eea; text-align: center;">📈 判定別成績</h2>
        <div class="summary-grid">
            <div class="summary-card" style="border-left-color: #3498db;">
                <h3>💎 買い判定 ({buy_stats['total']}銘柄)</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率 (引分除外)</span>
                    <span class="stat-value {'positive' if buy_stats['win_rate'] >= 50 else 'negative'}">{buy_stats['win_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value {'positive' if buy_stats['total_profit'] > 0 else 'negative'}">{buy_stats['total_profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value {'positive' if buy_stats['avg_profit'] > 0 else 'negative'}">{buy_stats['avg_profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">損切り発動率</span>
                    <span class="stat-value">{buy_stats['stop_loss_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">損失回避額</span>
                    <span class="stat-value positive">{buy_stats['avoided_loss']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">機会損失額</span>
                    <span class="stat-value negative">{buy_stats['opportunity_loss']:,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">損切り純影響</span>
                    <span class="stat-value {'positive' if buy_stats['total_impact'] > 0 else 'negative'}">{buy_stats['total_impact']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝/負/分</span>
                    <span class="stat-value">{buy_stats['wins']}/{buy_stats['losses']}/{buy_stats['draws']}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #95a5a6;">
                <h3>⏸️ 静観判定 ({hold_stats['total']}銘柄)</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率 (引分除外)</span>
                    <span class="stat-value {'positive' if hold_stats['win_rate'] >= 50 else 'negative'}">{hold_stats['win_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value {'positive' if hold_stats['total_profit'] > 0 else 'negative'}">{hold_stats['total_profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value {'positive' if hold_stats['avg_profit'] > 0 else 'negative'}">{hold_stats['avg_profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝/負/分</span>
                    <span class="stat-value">{hold_stats['wins']}/{hold_stats['losses']}/{hold_stats['draws']}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #e74c3c;">
                <h3>📉 売り判定 ({sell_stats['total']}銘柄)</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率 (引分除外)</span>
                    <span class="stat-value {'positive' if sell_stats['win_rate'] >= 50 else 'negative'}">{sell_stats['win_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value {'positive' if sell_stats['total_profit'] > 0 else 'negative'}">{sell_stats['total_profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value {'positive' if sell_stats['avg_profit'] > 0 else 'negative'}">{sell_stats['avg_profit']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">損切り発動率</span>
                    <span class="stat-value">{sell_stats['stop_loss_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">損失回避額</span>
                    <span class="stat-value positive">{sell_stats['avoided_loss']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">機会損失額</span>
                    <span class="stat-value negative">{sell_stats['opportunity_loss']:,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">損切り純影響</span>
                    <span class="stat-value {'positive' if sell_stats['total_impact'] > 0 else 'negative'}">{sell_stats['total_impact']:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝/負/分</span>
                    <span class="stat-value">{sell_stats['wins']}/{sell_stats['losses']}/{sell_stats['draws']}</span>
                </div>
            </div>
        </div>
    </div>

    <div class="table-section">
        <table>
            <thead>
                <tr>
                    <th>選定日</th>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>判定</th>
                    <th class="number">購入価格</th>
                    <th class="number">高値</th>
                    <th class="number">安値</th>
                    <th class="number">終値</th>
                    <th class="number">損切り設定</th>
                    <th class="center">損切り</th>
                    <th class="number">100株利益</th>
                    <th class="center">影響</th>
                    <th class="number">影響額</th>
                    <th class="center">結果</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>
    </div>
</div>

<div style="text-align: center; color: white; padding: 20px; font-size: 0.9em;">
    <p>V2.1.1バージョン: v2.1.0.1ハイブリッド戦略 + 最適損切り戦略（買い: 価格帯別、売り: +5%）</p>
    <p style="margin-top: 10px;">損切り判定: 買い=Low、売り=High | 影響: 回避=損失軽減、機会損失=利益逃失</p>
</div>

</body>
</html>'''

    return html


def main() -> int:
    print("=" * 60)
    print("Generate v2.1.1 Backtest Report")
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
