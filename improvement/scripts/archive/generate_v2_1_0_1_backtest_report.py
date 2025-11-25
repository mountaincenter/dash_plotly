#!/usr/bin/env python3
"""
generate_v2_1_0_1_backtest_report.py

v2.1.0.1 ハイブリッド戦略のバックテストレポート生成

戦略:
- v2.0.3をベースに、特定のパターンのみv2.1.0を適用
- 買い → 静観: リスク回避
- 静観 → 売り: 利益増大
- その他: v2.0.3のまま維持

入力: improvement/data/v2_1_0_comparison_results.parquet
出力: improvement/v2_1_0_1_backtest_report.html
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
OUTPUT_FILE = IMPROVEMENT_DIR / "v2_1_0_1_backtest_report.html"


def apply_v2_1_0_1_strategy(row: pd.Series) -> str:
    """
    v2.1.0.1 ハイブリッド戦略を適用

    戦略:
    - 買い → 静観: 静観に変更
    - 静観 → 売り: 売りに変更
    - その他: v2.0.3のまま
    """
    v2_0_3_action = row['v2_0_3_action']
    v2_1_0_action = row['v2_1_0_action']

    # 買い → 静観: リスク回避
    if v2_0_3_action == '買い' and v2_1_0_action == '静観':
        return '静観'

    # 静観 → 売り: 利益増大
    elif v2_0_3_action == '静観' and v2_1_0_action == '売り':
        return '売り'

    # その他: v2.0.3のまま
    else:
        return v2_0_3_action


def generate_html_report(df: pd.DataFrame) -> str:
    """
    HTMLレポートを生成（v2_0_3_backtest_report.html と同じ粒度）
    """
    # v2.1.0.1 判定を適用
    df['v2_1_0_1_action'] = df.apply(apply_v2_1_0_1_strategy, axis=1)

    # 判定別にデータを分割
    buy_df = df[df['v2_1_0_1_action'] == '買い'].copy()
    hold_df = df[df['v2_1_0_1_action'] == '静観'].copy()
    sell_df = df[df['v2_1_0_1_action'] == '売り'].copy()

    # 買いの成績計算
    if len(buy_df) > 0:
        buy_df['win'] = buy_df['daily_close'] > buy_df['buy_price']
        buy_df['profit'] = (buy_df['daily_close'] - buy_df['buy_price']) * 100
        buy_df_no_draw = buy_df[buy_df['daily_close'] != buy_df['buy_price']]

        buy_total = len(buy_df)
        buy_wins = buy_df['win'].sum()
        buy_draws = (buy_df['daily_close'] == buy_df['buy_price']).sum()
        buy_losses = buy_total - buy_wins - buy_draws
        buy_win_rate = buy_wins / len(buy_df_no_draw) * 100 if len(buy_df_no_draw) > 0 else 0
        buy_avg_profit_pct = ((buy_df['daily_close'] - buy_df['buy_price']) / buy_df['buy_price'] * 100).mean()
        buy_total_profit = buy_df['profit'].sum()
    else:
        buy_total = buy_wins = buy_draws = buy_losses = buy_win_rate = buy_avg_profit_pct = buy_total_profit = 0

    # 静観の成績計算
    if len(hold_df) > 0:
        hold_df['win'] = hold_df['daily_close'] > hold_df['buy_price']
        hold_df['profit'] = (hold_df['daily_close'] - hold_df['buy_price']) * 100
        hold_df_no_draw = hold_df[hold_df['daily_close'] != hold_df['buy_price']]

        hold_total = len(hold_df)
        hold_wins = hold_df['win'].sum()
        hold_draws = (hold_df['daily_close'] == hold_df['buy_price']).sum()
        hold_losses = hold_total - hold_wins - hold_draws
        hold_win_rate = hold_wins / len(hold_df_no_draw) * 100 if len(hold_df_no_draw) > 0 else 0
        hold_avg_profit_pct = ((hold_df['daily_close'] - hold_df['buy_price']) / hold_df['buy_price'] * 100).mean()
        hold_total_profit = hold_df['profit'].sum()
    else:
        hold_total = hold_wins = hold_draws = hold_losses = hold_win_rate = hold_avg_profit_pct = hold_total_profit = 0

    # 売りの成績計算
    if len(sell_df) > 0:
        sell_df['win'] = sell_df['buy_price'] > sell_df['daily_close']
        sell_df['profit'] = (sell_df['buy_price'] - sell_df['daily_close']) * 100
        sell_df_no_draw = sell_df[sell_df['daily_close'] != sell_df['buy_price']]

        sell_total = len(sell_df)
        sell_wins = sell_df['win'].sum()
        sell_draws = (sell_df['daily_close'] == sell_df['buy_price']).sum()
        sell_losses = sell_total - sell_wins - sell_draws
        sell_win_rate = sell_wins / len(sell_df_no_draw) * 100 if len(sell_df_no_draw) > 0 else 0
        sell_avg_profit_pct = ((sell_df['buy_price'] - sell_df['daily_close']) / sell_df['buy_price'] * 100).mean()
        sell_total_profit = sell_df['profit'].sum()
    else:
        sell_total = sell_wins = sell_draws = sell_losses = sell_win_rate = sell_avg_profit_pct = sell_total_profit = 0

    # 変更統計
    change_count = {
        'buy_to_hold': ((df['v2_0_3_action'] == '買い') & (df['v2_1_0_1_action'] == '静観')).sum(),
        'hold_to_sell': ((df['v2_0_3_action'] == '静観') & (df['v2_1_0_1_action'] == '売り')).sum()
    }
    total_changes = sum(change_count.values())

    # 全データを1つのテーブルに（v2_0_3と同じ構造）
    # 日付降順、日付内は判定順（買い→静観→売り）にソート
    df_sorted = df.copy()
    df_sorted['backtest_date'] = pd.to_datetime(df_sorted['backtest_date'])
    df_sorted['action_order'] = df_sorted['v2_1_0_1_action'].map({'買い': 1, '静観': 2, '売り': 3})
    df_sorted = df_sorted.sort_values(['backtest_date', 'action_order'], ascending=[False, True])

    table_rows = []
    current_date = None

    for _, row in df_sorted.iterrows():
        date_str = row['backtest_date'].strftime('%Y-%m-%d')

        # 日付セパレータ
        if date_str != current_date:
            current_date = date_str
            table_rows.append(f'<tr class="date-separator"><td colspan="12">{date_str}</td></tr>')

        ticker = row['ticker']
        company = row.get('company_name', '')
        grok_rank = row.get('grok_rank', 0)
        prev_close = row.get('prev_day_close', 0)
        v2_score = row.get('v2_0_3_score', 0)
        action = row['v2_1_0_1_action']
        buy_price = row['buy_price']
        daily_close = row['daily_close']

        # 利益率計算
        if action == '売り':
            profit_pct = (buy_price - daily_close) / buy_price * 100 if buy_price > 0 else 0
            profit_100 = (buy_price - daily_close) * 100
            win = buy_price > daily_close
        else:
            profit_pct = (daily_close - buy_price) / buy_price * 100 if buy_price > 0 else 0
            profit_100 = (daily_close - buy_price) * 100
            win = daily_close > buy_price

        win_status = '勝' if win else '分' if daily_close == buy_price else '負'

        profit_pct_class = 'positive' if profit_pct > 0 else 'negative' if profit_pct < 0 else ''
        profit_100_class = 'positive' if profit_100 > 0 else 'negative' if profit_100 < 0 else ''
        profit_pct_str = f'+{profit_pct:.2f}' if profit_pct > 0 else f'{profit_pct:.2f}'
        profit_100_str = f'+{profit_100:,.0f}' if profit_100 > 0 else f'{profit_100:,.0f}'

        table_rows.append(f'''
        <tr class="action-{action}">
            <td>{date_str}</td>
            <td>{ticker}</td>
            <td>{company}</td>
            <td class="number">{grok_rank}</td>
            <td class="number">{prev_close:,.0f}円</td>
            <td class="number">{v2_score}</td>
            <td><span class="badge-{action}">{action}</span></td>
            <td class="number">{buy_price:,.0f}円</td>
            <td class="number">{daily_close:,.0f}円</td>
            <td class="number {profit_pct_class}">{profit_pct_str}%</td>
            <td class="number {profit_100_class}">{profit_100_str}円</td>
            <td class="result-{win_status}">{win_status}</td>
        </tr>''')

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V2.1.0.1 バックテストレポート (ハイブリッド戦略)</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
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
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
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
.table-section {{
    padding: 40px;
}}
.table-section h2 {{
    margin-bottom: 24px;
    color: #667eea;
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
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
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
.summary-row {{
    background: #f8f9fa;
    padding: 16px;
    margin: 20px 0;
    border-radius: 8px;
    display: flex;
    justify-content: space-around;
    flex-wrap: wrap;
}}
.summary-row span {{
    margin: 8px 16px;
    font-weight: 600;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 V2.1.0.1 バックテストレポート</h1>
        <div class="subtitle">ハイブリッド戦略（v2.0.3 ベース + 選択的v2.1.0適用）</div>
        <div class="subtitle" style="margin-top: 10px;">期間: 2025-11-04 ~ 2025-11-21 | 対象: Grok銘柄 151件</div>
    </div>

    <div class="strategy-box">
        <h2>🎯 V2.1.0.1 戦略</h2>
        <ul>
            <li>✅ <strong>買い → 静観</strong>: リスク回避（{change_count['buy_to_hold']}件適用）</li>
            <li>✅ <strong>静観 → 売り</strong>: 利益増大（{change_count['hold_to_sell']}件適用）</li>
            <li>📌 <strong>その他</strong>: v2.0.3判定を維持（特に売り判定は高精度のため保持）</li>
            <li>📊 <strong>総変更数</strong>: {total_changes}件 / {len(df)}件 ({total_changes/len(df)*100:.1f}%)</li>
        </ul>
    </div>

    <div class="summary-section">
        <div class="summary-grid">
            <div class="summary-card" style="border-left-color: #3498db;">
                <h3>📈 買い判定 ({buy_total}銘柄)</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率 (引分除外)</span>
                    <span class="stat-value {'positive' if buy_win_rate >= 50 else 'negative'}">{buy_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益率</span>
                    <span class="stat-value {'positive' if buy_avg_profit_pct > 0 else 'negative'}">{buy_avg_profit_pct:+.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">100株利益合計</span>
                    <span class="stat-value {'positive' if buy_total_profit > 0 else 'negative'}">{buy_total_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝/負/分</span>
                    <span class="stat-value">{buy_wins}/{buy_losses}/{buy_draws}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #95a5a6;">
                <h3>⏸️ 静観判定 ({hold_total}銘柄)</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率 (引分除外)</span>
                    <span class="stat-value {'positive' if hold_win_rate >= 50 else 'negative'}">{hold_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益率</span>
                    <span class="stat-value {'positive' if hold_avg_profit_pct > 0 else 'negative'}">{hold_avg_profit_pct:+.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">100株利益合計</span>
                    <span class="stat-value {'positive' if hold_total_profit > 0 else 'negative'}">{hold_total_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝/負/分</span>
                    <span class="stat-value">{hold_wins}/{hold_losses}/{hold_draws}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #e74c3c;">
                <h3>📉 売り判定 ({sell_total}銘柄)</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率 (引分除外)</span>
                    <span class="stat-value {'positive' if sell_win_rate >= 50 else 'negative'}">{sell_win_rate:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">平均利益率</span>
                    <span class="stat-value {'positive' if sell_avg_profit_pct > 0 else 'negative'}">{sell_avg_profit_pct:+.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">100株利益合計</span>
                    <span class="stat-value {'positive' if sell_total_profit > 0 else 'negative'}">{sell_total_profit:+,.0f}円</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝/負/分</span>
                    <span class="stat-value">{sell_wins}/{sell_losses}/{sell_draws}</span>
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
                    <th class="number">順位</th>
                    <th class="number">前日終値</th>
                    <th class="number">V2スコア</th>
                    <th>判定</th>
                    <th class="number">購入価格</th>
                    <th class="number">終値</th>
                    <th class="number">利益率</th>
                    <th class="number">100株利益</th>
                    <th style="text-align:center">結果</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>
    </div>
</div>

<div style="text-align: center; color: white; padding: 20px; font-size: 0.9em;">
    <p>V2.1.0.1バージョン: v2.0.3ベース + 買い→静観（リスク回避）+ 静観→売り（利益増大）｜ 勝敗判定: 買い/静観=利益>0で勝ち、売り=利益<0で勝ち、±0は引分（勝率計算から除外）</p>
</div>

</body>
</html>'''

    return html


def main() -> int:
    print("=" * 60)
    print("Generate v2.1.0.1 Backtest Report")
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
