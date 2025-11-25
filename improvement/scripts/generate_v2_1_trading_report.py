#!/usr/bin/env python3
"""
generate_v2_1_trading_report.py

v2.1ロジックのトレーディングレポート生成（バックテストデータ使用）

入力: improvement/data/complete_v2_0_3_comparison.parquet (151銘柄)
出力: improvement/v2_1_trading_report.html

処理:
1. complete_v2_0_3_comparison.parquet を読み込み（v2.0.3判定済み）
2. 各銘柄にテクニカル指標を計算
3. v2.1ハイブリッド戦略を適用（買い→静観、静観→売り）
4. v2_0_3_trading_report.html と同じレイアウトでHTML生成
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

IMPROVEMENT_DIR = ROOT / "improvement"
INPUT_FILE = IMPROVEMENT_DIR / "data" / "complete_v2_0_3_comparison.parquet"
PRICES_FILE = ROOT / "data" / "parquet" / "prices_max_1d.parquet"
OUTPUT_HTML = IMPROVEMENT_DIR / "v2_1_trading_report.html"


def calculate_technical_indicators(ticker: str, prices_df: pd.DataFrame, target_date: str) -> dict:
    """
    prices_max_1d.parquetからテクニカル指標を計算

    Args:
        ticker: ティッカーコード
        prices_df: 全銘柄の株価データ
        target_date: 判断基準日（この日までのデータを使用）

    Returns:
        テクニカル指標の辞書
    """
    try:
        # ティッカーでフィルタ
        ticker_df = prices_df[prices_df['ticker'] == ticker].copy()

        if ticker_df.empty:
            return {}

        # 日付でソート
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        ticker_df = ticker_df.sort_values('date', ascending=False)

        # target_date以前のデータのみ使用
        target_dt = pd.to_datetime(target_date)
        ticker_df = ticker_df[ticker_df['date'] <= target_dt]

        if len(ticker_df) < 2:
            return {}

        # 最新30日分
        recent_df = ticker_df.head(30)

        # RSI計算（14日）
        rsi_14d = None
        if len(recent_df) >= 15:
            closes = recent_df['Close'].values[:15][::-1]  # 古い順に並べ替え
            deltas = np.diff(closes)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)

            avg_gain = np.mean(gains)
            avg_loss = np.mean(losses)

            if avg_loss != 0:
                rs = avg_gain / avg_loss
                rsi_14d = 100 - (100 / (1 + rs))

        # 出来高変化（20日平均比）
        volume_change_20d = None
        if len(recent_df) >= 21:
            latest_volume = recent_df.iloc[0]['Volume']
            avg_volume_20d = recent_df.iloc[1:21]['Volume'].mean()
            if avg_volume_20d > 0:
                volume_change_20d = latest_volume / avg_volume_20d

        # 5日線乖離率
        price_vs_sma5_pct = None
        if len(recent_df) >= 5:
            latest_close = recent_df.iloc[0]['Close']
            sma5 = recent_df.iloc[0:5]['Close'].mean()
            if sma5 > 0:
                price_vs_sma5_pct = ((latest_close - sma5) / sma5) * 100

        return {
            'rsi_14d': rsi_14d,
            'volume_change_20d': volume_change_20d,
            'price_vs_sma5_pct': price_vs_sma5_pct
        }

    except Exception as e:
        print(f"Warning: {ticker} のテクニカル指標計算失敗: {e}")
        return {}


def apply_v2_1_hybrid_strategy(v2_0_3_action: str, v2_0_3_score: int,
                                technical: dict) -> tuple[str, int]:
    """
    v2.1 ハイブリッド戦略を適用

    ルール:
    1. v2_0_3が「買い」の場合:
       - RSI > 70（買われすぎ）→「静観」
       - 出来高変化 < 1.5（注目不足）→「静観」
       - 5日線乖離 > +3%（割高）→「静観」

    2. v2_0_3が「静観」の場合:
       - RSI < 30（売られすぎ）AND 出来高変化 > 2.0 →「売り」
       - 5日線押し目（-2% < 乖離 < 0%）AND 出来高変化 > 2.0 →「売り」

    Args:
        v2_0_3_action: v2.0.3の判断
        v2_0_3_score: v2.0.3のスコア
        technical: テクニカル指標

    Returns:
        (最終判断, 最終スコア)
    """
    action = v2_0_3_action
    score = v2_0_3_score

    rsi = technical.get('rsi_14d')
    volume_change = technical.get('volume_change_20d')
    sma5_deviation = technical.get('price_vs_sma5_pct')

    # ルール1: 買い → 静観
    if v2_0_3_action == '買い':
        if rsi is not None and rsi > 70:
            action = '静観'
            score = 0
        elif volume_change is not None and volume_change < 1.5:
            action = '静観'
            score = 0
        elif sma5_deviation is not None and sma5_deviation > 3.0:
            action = '静観'
            score = 0

    # ルール2: 静観 → 売り
    elif v2_0_3_action == '静観':
        if (rsi is not None and rsi < 30 and
            volume_change is not None and volume_change > 2.0):
            action = '売り'
            score = -30
        elif (sma5_deviation is not None and -2.0 < sma5_deviation < 0 and
              volume_change is not None and volume_change > 2.0):
            action = '売り'
            score = -30

    return (action, score)


def generate_html_report(df: pd.DataFrame) -> str:
    """HTMLレポートを生成（v2_0_3_trading_report.html と同じレイアウト）"""

    # 統計計算
    action_counts = df['v2_1_action'].value_counts().to_dict()
    total_records = len(df)

    # 成績計算
    buy_df = df[df['v2_1_action'] == '買い'].copy()
    sell_df = df[df['v2_1_action'] == '売り'].copy()

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

    # テーブル行を生成
    table_rows = []
    current_date = None

    for _, row in df.sort_values(['selection_date', 'v2_1_score'], ascending=[False, False]).iterrows():
        date_str = row['selection_date']

        # 日付セパレータ
        if date_str != current_date:
            current_date = date_str
            table_rows.append(f'''
        <tr class="date-separator">
            <td colspan="10">{date_str}</td>
        </tr>''')

        ticker = row['ticker']
        stock_name = row.get('stock_name', '')
        grok_rank = row['grok_rank']
        prev_close = row['prev_day_close']
        score = row['v2_1_score']
        action = row['v2_1_action']
        buy_price = row.get('buy_price', 0)
        daily_close = row.get('daily_close', 0)
        price_diff = daily_close - buy_price
        price_diff_str = f'+{price_diff:.0f}' if price_diff > 0 else f'{price_diff:.0f}'
        price_diff_class = 'positive' if price_diff > 0 else 'negative' if price_diff < 0 else ''

        # 100株あたりの利益計算
        if action == '売り':
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
        <tr class="action-{action}">
            <td>{ticker}</td>
            <td>{stock_name}</td>
            <td class="number">{grok_rank}</td>
            <td class="number">{prev_close:,.0f}</td>
            <td class="number">{score:+d}</td>
            <td><span class="action-{action}-badge action-badge">{action}</span></td>
            <td class="number">{buy_price:,.0f}</td>
            <td class="number">{daily_close:,.0f}</td>
            <td class="number {result_class}" style="font-weight: bold;">{result}</td>
            <td class="number {profit_class}" style="font-weight: bold;">{profit_str}</td>
        </tr>''')

    table_html = '\n'.join(table_rows)

    # HTML生成（v2_0_3_trading_report.html と同じスタイル）
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>v2.1 トレーディングレポート</title>
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
.stat-value.profit {{
    font-size: 2em;
    color: #27ae60;
}}
.stat-value.loss {{
    font-size: 2em;
    color: #e74c3c;
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
    background: #e74c3c;
    color: white;
}}
.action-売り-badge {{
    background: #3498db;
    color: white;
}}
.action-静観-badge {{
    background: #95a5a6;
    color: white;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>v2.1 トレーディングレポート</h1>
        <div class="subtitle">v2.0.3 + テクニカル補正 | 2025-11-04 ~ 2025-11-21 | {total_records}銘柄</div>
    </div>

    <div class="summary-section">
        <div class="summary-grid">
            <!-- 判定数サマリー -->
            <div class="summary-card">
                <h3>📊 判定数サマリー</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{action_counts.get('買い', 0)}銘柄</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{action_counts.get('売り', 0)}銘柄</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{action_counts.get('静観', 0)}銘柄</span>
                </div>
            </div>

            <!-- 買い成績 -->
            <div class="summary-card">
                <h3>🔴 買い成績</h3>
                <div class="stat-row">
                    <span class="stat-label">勝敗</span>
                    <span class="stat-value">{buy_wins}勝 / {buy_total}戦</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value">{buy_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">利益（100株）</span>
                    <span class="stat-value {'profit' if buy_total_profit > 0 else 'loss'}">{buy_total_profit:+,.0f}円</span>
                </div>
            </div>

            <!-- 売り成績 -->
            <div class="summary-card">
                <h3>🔵 売り成績</h3>
                <div class="stat-row">
                    <span class="stat-label">勝敗</span>
                    <span class="stat-value">{sell_wins}勝 / {sell_total}戦</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value">{sell_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">利益（100株）</span>
                    <span class="stat-value {'profit' if sell_total_profit > 0 else 'loss'}">{sell_total_profit:+,.0f}円</span>
                </div>
            </div>

            <!-- 総合成績 -->
            <div class="summary-card">
                <h3>💰 総合成績</h3>
                <div class="stat-row">
                    <span class="stat-label">勝敗</span>
                    <span class="stat-value">{total_wins}勝 / {total_count}戦</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value">{total_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">利益（100株）</span>
                    <span class="stat-value {'profit' if total_profit > 0 else 'loss'}">{total_profit:+,.0f}円</span>
                </div>
            </div>
        </div>
    </div>

    <div class="table-section">
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th class="number">Grokランク</th>
                    <th class="number">前日終値</th>
                    <th class="number">スコア</th>
                    <th>判定</th>
                    <th class="number">買値</th>
                    <th class="number">終値</th>
                    <th class="number">勝負</th>
                    <th class="number">利益(100株)</th>
                </tr>
            </thead>
            <tbody>
{table_html}
            </tbody>
        </table>
    </div>
</div>
</body>
</html>'''

    return html


def main():
    print("=== v2.1 トレーディングレポート生成 ===\n")

    # データ読み込み
    print(f"入力ファイル: {INPUT_FILE}")
    df = pd.read_parquet(INPUT_FILE)
    print(f"  読み込み: {len(df)}レコード")

    # 株価データ読み込み
    print(f"\n株価データ読み込み: {PRICES_FILE}")
    prices_df = pd.read_parquet(PRICES_FILE)
    print(f"  読み込み: {len(prices_df)}レコード")

    # 各銘柄にv2.1ロジックを適用
    print("\nv2.1ロジック適用中...")
    results = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        selection_date = row['selection_date']
        v2_0_3_action = row['imp_action']
        v2_0_3_score = row['imp_score']

        if idx % 20 == 0:
            print(f"  処理中: {idx+1}/{len(df)}銘柄")

        # テクニカル指標を計算
        technical = calculate_technical_indicators(ticker, prices_df, selection_date)

        # v2.1ハイブリッド戦略を適用
        v2_1_action, v2_1_score = apply_v2_1_hybrid_strategy(
            v2_0_3_action, v2_0_3_score, technical
        )

        results.append({
            'selection_date': selection_date,
            'ticker': ticker,
            'stock_name': row.get('stock_name', ''),
            'grok_rank': row['grok_rank'],
            'prev_day_close': row['prev_day_close'],
            'buy_price': row['buy_price'],
            'daily_close': row['daily_close'],
            'v2_0_3_action': v2_0_3_action,
            'v2_0_3_score': v2_0_3_score,
            'v2_1_action': v2_1_action,
            'v2_1_score': v2_1_score
        })

    result_df = pd.DataFrame(results)
    print(f"  完了: {len(result_df)}銘柄")

    # 統計表示
    print("\n=== v2.1判定統計 ===")
    print(result_df['v2_1_action'].value_counts())

    # 変更があった銘柄数
    changed = (result_df['v2_0_3_action'] != result_df['v2_1_action']).sum()
    print(f"\nv2.0.3から変更: {changed}銘柄 ({changed/len(result_df)*100:.1f}%)")

    # HTML生成
    print("\nHTML生成中...")
    html = generate_html_report(result_df)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"HTML保存: {OUTPUT_HTML}")

    # 成績サマリー
    buy_df = result_df[result_df['v2_1_action'] == '買い']
    sell_df = result_df[result_df['v2_1_action'] == '売り']

    if len(buy_df) > 0:
        buy_profit = ((buy_df['daily_close'] - buy_df['buy_price']) * 100).sum()
        buy_wins = (buy_df['daily_close'] > buy_df['buy_price']).sum()
        buy_win_rate = buy_wins / len(buy_df) * 100
        print(f"\n【買い成績】")
        print(f"  利益: {buy_profit:+,.0f}円 ({buy_wins}/{len(buy_df)}勝, {buy_win_rate:.1f}%)")

    if len(sell_df) > 0:
        sell_profit = ((sell_df['buy_price'] - sell_df['daily_close']) * 100).sum()
        sell_wins = (sell_df['buy_price'] > sell_df['daily_close']).sum()
        sell_win_rate = sell_wins / len(sell_df) * 100
        print(f"\n【売り成績】")
        print(f"  利益: {sell_profit:+,.0f}円 ({sell_wins}/{len(sell_df)}勝, {sell_win_rate:.1f}%)")

    if len(buy_df) > 0 or len(sell_df) > 0:
        total_profit = (buy_profit if len(buy_df) > 0 else 0) + (sell_profit if len(sell_df) > 0 else 0)
        total_wins = (buy_wins if len(buy_df) > 0 else 0) + (sell_wins if len(sell_df) > 0 else 0)
        total_count = len(buy_df) + len(sell_df)
        total_win_rate = total_wins / total_count * 100
        print(f"\n【総合成績】")
        print(f"  利益: {total_profit:+,.0f}円 ({total_wins}/{total_count}勝, {total_win_rate:.1f}%)")

    print("\n完了!")


if __name__ == '__main__':
    main()
