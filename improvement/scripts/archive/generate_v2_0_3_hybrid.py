#!/usr/bin/env python3
"""
generate_v2_0_3_hybrid.py

v2.0.3 ハイブリッド戦略
- 買い判断: pipeline版 v2.0.3
- 売り判断: improvement版 v2.0.3
- 衝突時: 売り優先

入力: improvement/data/v2_1_0_comparison_results.parquet
出力: improvement/data/v2_0_3_hybrid_results.parquet
      improvement/v2_0_3_hybrid_report.html
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# パス設定
IMPROVEMENT_DIR = ROOT / "improvement"
INPUT_FILE = IMPROVEMENT_DIR / "data" / "v2_1_0_comparison_results.parquet"
OUTPUT_FILE = IMPROVEMENT_DIR / "data" / "v2_0_3_hybrid_results.parquet"
OUTPUT_HTML = IMPROVEMENT_DIR / "v2_0_3_hybrid_report.html"
PRICES_FILE = ROOT / "data" / "parquet" / "prices_max_1d.parquet"
BACKTEST_FILE = ROOT / "data" / "parquet" / "backtest" / "grok_analysis_merged.parquet"


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
    
    # スコアベース判定
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
        if prev_day_change_pct < -3:
            score += 15
        elif prev_day_change_pct > 10:
            score -= 10
    
    # ATR
    if atr_pct:
        if atr_pct < 3.0:
            score += 10
        elif atr_pct > 8.0:
            score -= 15
    
    # 判定
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


def main():
    print("=" * 80)
    print("v2.0.3 ハイブリッド戦略 - バックテスト")
    print("=" * 80)
    print()
    
    # 1. データ読み込み
    print("[1/4] データ読み込み中...")
    comp_df = pd.read_parquet(INPUT_FILE)
    prices_df = pd.read_parquet(PRICES_FILE)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    
    backtest_df = pd.read_parquet(BACKTEST_FILE)
    rank_stats = backtest_df.groupby('grok_rank').agg({'phase2_win': 'mean'}).round(3)
    rank_win_rates = {int(rank): rate * 100 for rank, rate in rank_stats['phase2_win'].items()}
    
    print(f"  読み込み完了: {len(comp_df)} レコード")
    print()
    
    # 2. ハイブリッド判定を適用
    print("[2/4] ハイブリッド判定を適用中...")
    results = []
    
    for _, row in comp_df.iterrows():
        ticker = row['ticker']
        selection_date = pd.to_datetime(row['selection_date'])
        grok_rank = int(row['grok_rank'])
        total_stocks = 7
        prev_close = row['prev_day_close']
        prev_day_change_pct = row.get('prev_day_change_pct', 0)
        
        # ATR計算
        ticker_prices = prices_df[prices_df['ticker'] == ticker].sort_values('date', ascending=False)
        ticker_prices_before = ticker_prices[ticker_prices['date'] <= selection_date].head(14)
        
        if len(ticker_prices_before) >= 14:
            atr = (ticker_prices_before['High'] - ticker_prices_before['Low']).mean()
            atr_pct = atr / prev_close * 100 if prev_close > 0 else None
        else:
            atr_pct = None
        
        # improvement版判定（元データから）
        imp_action = row['v2_0_3_action']
        imp_score = row['v2_0_3_score']
        
        # pipeline版判定
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
        
        # バックテスト結果
        phase2_win = row.get('phase2_win', 0)
        phase2_return = row.get('phase2_return_pct', 0)
        
        results.append({
            'selection_date': str(selection_date.date()),
            'ticker': ticker,
            'stock_name': row['stock_name'],
            'grok_rank': grok_rank,
            'prev_close': prev_close,
            'prev_day_change_pct': prev_day_change_pct,
            'atr_pct': atr_pct,
            'imp_action': imp_action,
            'imp_score': imp_score,
            'pipe_action': pipe_action,
            'pipe_score': pipe_score,
            'hybrid_action': hybrid_action,
            'hybrid_score': hybrid_score,
            'hybrid_source': hybrid_source,
            'phase2_win': phase2_win,
            'phase2_return': phase2_return,
            'sell_profit': -phase2_return if hybrid_action == '売り' else 0,
            'buy_profit': phase2_return if hybrid_action == '買い' else 0
        })
    
    result_df = pd.DataFrame(results)
    print(f"  完了: {len(result_df)} レコード処理")
    print()
    
    # 3. 結果を保存
    print("[3/4] 結果を保存中...")
    result_df.to_parquet(OUTPUT_FILE, index=False)
    print(f"  保存完了: {OUTPUT_FILE}")
    print()
    
    # 4. パフォーマンス集計
    print("[4/4] パフォーマンス集計...")

    # comparison_dfから価格データを取得（HTMLと同じデータソース）
    comparison_df = pd.read_parquet(IMPROVEMENT_DIR / "data" / "v2_1_0_comparison_results.parquet")

    # result_dfとマージ
    merged_for_summary = result_df.merge(
        comparison_df[['selection_date', 'ticker', 'buy_price', 'sell_price']],
        on=['selection_date', 'ticker'],
        how='left'
    )

    # 100株あたりの実利益を計算
    buy_trades = []
    sell_trades = []

    for _, row in merged_for_summary.iterrows():
        buy_price = row.get('buy_price', np.nan)
        sell_price = row.get('sell_price', np.nan)

        if pd.notna(buy_price) and pd.notna(sell_price):
            if row['hybrid_action'] == '買い':
                profit_yen = (sell_price - buy_price) * 100
                buy_trades.append(profit_yen)
            elif row['hybrid_action'] == '売り':
                profit_yen = (buy_price - sell_price) * 100
                sell_trades.append(profit_yen)

    buy_count = len([x for x in result_df['hybrid_action'] if x == '買い'])
    sell_count = len([x for x in result_df['hybrid_action'] if x == '売り'])
    hold_count = len([x for x in result_df['hybrid_action'] if x == '静観'])

    buy_win_count = len([x for x in buy_trades if x > 0])
    buy_win_rate = (buy_win_count / len(buy_trades) * 100) if buy_trades else 0
    buy_total_yen = sum(buy_trades) if buy_trades else 0
    buy_avg_yen = buy_total_yen / len(buy_trades) if buy_trades else 0

    sell_win_count = len([x for x in sell_trades if x > 0])
    sell_win_rate = (sell_win_count / len(sell_trades) * 100) if sell_trades else 0
    sell_total_yen = sum(sell_trades) if sell_trades else 0
    sell_avg_yen = sell_total_yen / len(sell_trades) if sell_trades else 0

    total_yen = buy_total_yen + sell_total_yen

    print()
    print("=" * 80)
    print("パフォーマンスサマリー（100株あたり）")
    print("=" * 80)
    print(f"買い推奨: {buy_count}銘柄 | 勝率: {buy_win_rate:.1f}% | 平均: ¥{buy_avg_yen:+,.0f} | 合計: ¥{buy_total_yen:+,.0f}")
    print(f"売り推奨: {sell_count}銘柄 | 勝率: {sell_win_rate:.1f}% | 平均: ¥{sell_avg_yen:+,.0f} | 合計: ¥{sell_total_yen:+,.0f}")
    print(f"静観: {hold_count}銘柄")
    print()
    print(f"総合利益: ¥{total_yen:+,.0f}")
    print("=" * 80)

    return result_df, {
        'buy_count': buy_count,
        'sell_count': sell_count,
        'hold_count': hold_count,
        'buy_win_rate': buy_win_rate,
        'buy_total_yen': buy_total_yen,
        'sell_win_rate': sell_win_rate,
        'sell_total_yen': sell_total_yen,
        'total_yen': total_yen
    }


def generate_html_report(result_df: pd.DataFrame, summary: dict) -> str:
    """HTML詳細レポート生成（既存フォーマットに準拠）"""

    # comparison_dfから追加データを取得
    comparison_df = pd.read_parquet(IMPROVEMENT_DIR / "data" / "v2_1_0_comparison_results.parquet")

    # マージして追加カラムを取得
    merged_df = result_df.merge(
        comparison_df[[
            'selection_date', 'ticker',
            'prev_2day_close', 'buy_price', 'sell_price',
            'rsi_14d', 'volume_change_20d', 'price_vs_sma5_pct'
        ]],
        on=['selection_date', 'ticker'],
        how='left'
    )

    # 日付降順、Grokランク昇順でソート
    merged_df['selection_date_dt'] = pd.to_datetime(merged_df['selection_date'])
    sorted_df = merged_df.sort_values(['selection_date_dt', 'grok_rank'], ascending=[False, True])

    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # サマリー統計計算
    total_records = len(merged_df)
    rsi_avg = merged_df['rsi_14d'].mean() if 'rsi_14d' in merged_df.columns else 0
    volume_avg = merged_df['volume_change_20d'].mean() if 'volume_change_20d' in merged_df.columns else 0
    sma5_avg = merged_df['price_vs_sma5_pct'].mean() if 'price_vs_sma5_pct' in merged_df.columns else 0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V2.0.3 ハイブリッド戦略レポート</title>
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
tr:hover {{
    background: #fff3cd;
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
        <h1>V2.0.3 ハイブリッド戦略レポート</h1>
        <div class="subtitle">Pipeline版(買い) + Improvement版(売り) | 衝突時: 売り優先</div>
        <div class="subtitle">総レコード数: {total_records} | 生成日時: {generated_time}</div>
    </div>

    <div class="summary-section">
        <div class="summary-grid">
            <div class="summary-card">
                <h3>📊 Improvement版 判定</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{len(result_df[result_df['imp_action'] == '買い'])}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{len(result_df[result_df['imp_action'] == '売り'])}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{len(result_df[result_df['imp_action'] == '静観'])}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>🚀 Pipeline版 判定</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{len(result_df[result_df['pipe_action'] == '買い'])}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{len(result_df[result_df['pipe_action'] == '売り'])}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{len(result_df[result_df['pipe_action'] == '静観'])}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #ff6b6b;">
                <h3>📊 買い戦略</h3>
                <div class="stat-row">
                    <span class="stat-label">件数</span>
                    <span class="stat-value">{summary['buy_count']}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value">{summary['buy_win_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計損益</span>
                    <span class="stat-value" style="color: {'#27ae60' if summary['buy_total_yen'] > 0 else '#e74c3c'}">¥{summary['buy_total_yen']:+,.0f}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #4dabf7;">
                <h3>📊 売り戦略</h3>
                <div class="stat-row">
                    <span class="stat-label">件数</span>
                    <span class="stat-value">{summary['sell_count']}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value">{summary['sell_win_rate']:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計損益</span>
                    <span class="stat-value" style="color: {'#27ae60' if summary['sell_total_yen'] > 0 else '#e74c3c'}">¥{summary['sell_total_yen']:+,.0f}</span>
                </div>
            </div>

            <div class="summary-card" style="border-left-color: #27ae60;">
                <h3>💰 総合</h3>
                <div class="stat-row">
                    <span class="stat-label">総取引</span>
                    <span class="stat-value">{summary['buy_count'] + summary['sell_count']}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{summary['hold_count']}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">総合損益</span>
                    <span class="stat-value" style="color: {'#27ae60' if summary['total_yen'] > 0 else '#e74c3c'}; font-size: 1.8em;">¥{summary['total_yen']:+,.0f}</span>
                </div>
            </div>
        </div>
    </div>

    <div class="table-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">詳細データ ({total_records}件)</h2>
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th class="number">Grokランク</th>
                    <th class="number">前々日終値</th>
                    <th class="number">前日終値</th>
                    <th class="number">Imp<br/>スコア</th>
                    <th>Imp<br/>判定</th>
                    <th class="number">Pipe<br/>スコア</th>
                    <th>Pipe<br/>判定</th>
                    <th class="number">Hybrid<br/>スコア</th>
                    <th>Hybrid<br/>判定</th>
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
"""

    current_date = None
    for _, row in sorted_df.iterrows():
        # 日付区切り行を挿入（日付別の損益集計付き）
        if current_date != row['selection_date']:
            current_date = row['selection_date']
            # その日の損益を計算
            day_data = sorted_df[sorted_df['selection_date'] == current_date]
            day_profit = 0
            for _, d in day_data.iterrows():
                bp = d.get('buy_price', np.nan)
                sp = d.get('sell_price', np.nan)
                if d['hybrid_action'] == '買い' and pd.notna(bp) and pd.notna(sp):
                    day_profit += (sp - bp) * 100
                elif d['hybrid_action'] == '売り' and pd.notna(bp) and pd.notna(sp):
                    day_profit += (bp - sp) * 100

            profit_class = 'positive' if day_profit > 0 else ('negative' if day_profit < 0 else '')
            html += f"""                <tr class="date-separator">
                    <td colspan="16">{current_date}</td>
                    <td colspan="3" class="number {profit_class}" style="font-weight: bold;">{day_profit:+,.0f}</td>
                </tr>
"""
        def action_badge(action):
            if action == '買い':
                return '<span class="action-badge action-買い-badge">買い</span>'
            elif action == '売り':
                return '<span class="action-badge action-売り-badge">売り</span>'
            else:
                return '<span class="action-badge action-静観-badge">静観</span>'

        # 勝負判定（実際の価格差で判定）
        buy_price = row.get('buy_price', np.nan)
        sell_price = row.get('sell_price', np.nan)

        if row['hybrid_action'] == '静観':
            win_text = '-'
            win_class = 'neutral'
        elif not pd.notna(buy_price) or not pd.notna(sell_price):
            win_text = '-'
            win_class = 'neutral'
        else:
            price_diff = sell_price - buy_price

            if price_diff == 0:
                win_text = '引分'
                win_class = 'neutral'
            elif row['hybrid_action'] == '売り':
                # 売り: 価格下落（price_diff < 0）が勝ち
                if price_diff < 0:
                    win_text = '勝'
                    win_class = 'positive'
                else:
                    win_text = '負'
                    win_class = 'negative'
            elif row['hybrid_action'] == '買い':
                # 買い: 価格上昇（price_diff > 0）が勝ち
                if price_diff > 0:
                    win_text = '勝'
                    win_class = 'positive'
                else:
                    win_text = '負'
                    win_class = 'negative'
            else:
                win_text = '-'
                win_class = 'neutral'

        # 100株利益: (終値 - 始値) × 100
        buy_price = row.get('buy_price', np.nan)
        sell_price = row.get('sell_price', np.nan)

        if row['hybrid_action'] == '買い' and pd.notna(buy_price) and pd.notna(sell_price):
            profit_100 = (sell_price - buy_price) * 100
        elif row['hybrid_action'] == '売り' and pd.notna(buy_price) and pd.notna(sell_price):
            profit_100 = (buy_price - sell_price) * 100
        else:
            profit_100 = 0

        profit_class = 'positive' if profit_100 > 0 else ('negative' if profit_100 < 0 else '')

        # 価格データ
        price_diff = sell_price - buy_price if pd.notna(buy_price) and pd.notna(sell_price) else np.nan
        price_diff_class = 'positive' if pd.notna(price_diff) and price_diff > 0 else ('negative' if pd.notna(price_diff) and price_diff < 0 else '')

        # テクニカル指標
        prev_2day_close = row.get('prev_2day_close', np.nan)
        rsi = row.get('rsi_14d', np.nan)
        volume_change = row.get('volume_change_20d', np.nan)
        sma5_deviation = row.get('price_vs_sma5_pct', np.nan)

        # 行のクラス（背景色）
        row_class = f"action-{row['hybrid_action']}"

        # 数値フォーマット
        prev_2day_text = f"{prev_2day_close:.0f}" if pd.notna(prev_2day_close) else "-"
        prev_close_text = f"{row['prev_close']:.0f}"
        prev_close_class = 'positive' if pd.notna(prev_2day_close) and row['prev_close'] > prev_2day_close else ('negative' if pd.notna(prev_2day_close) and row['prev_close'] < prev_2day_close else '')
        buy_price_text = f"{buy_price:.0f}" if pd.notna(buy_price) else "-"
        sell_price_text = f"{sell_price:.0f}" if pd.notna(sell_price) else "-"
        price_diff_text = f"{price_diff:+.0f}" if pd.notna(price_diff) else "-"
        profit_text = f"{profit_100:+,.0f}" if profit_100 != 0 else "-"
        rsi_text = f"{rsi:.1f}" if pd.notna(rsi) else "-"
        volume_text = f"{volume_change:.2f}" if pd.notna(volume_change) else "-"
        sma5_text = f"{sma5_deviation:+.1f}%" if pd.notna(sma5_deviation) else "-"

        html += f"""                <tr class="{row_class}">
                    <td>{row['ticker']}</td>
                    <td>{row['stock_name']}</td>
                    <td class="number">{row['grok_rank']}</td>
                    <td class="number">{prev_2day_text}</td>
                    <td class="number {prev_close_class}">{prev_close_text}</td>
                    <td class="number">{row['imp_score']:+d}</td>
                    <td>{action_badge(row['imp_action'])}</td>
                    <td class="number">{row['pipe_score']:+d}</td>
                    <td>{action_badge(row['pipe_action'])}</td>
                    <td class="number">{row['hybrid_score']:+d}</td>
                    <td>{action_badge(row['hybrid_action'])}</td>
                    <td class="number">{buy_price_text}</td>
                    <td class="number">{sell_price_text}</td>
                    <td class="number {price_diff_class}">{price_diff_text}</td>
                    <td class="number {profit_class if profit_100 != 0 else ''}" style="font-weight: bold;">{win_text}</td>
                    <td class="number {profit_class}" style="font-weight: bold;">{profit_text}</td>
                    <td class="number">{rsi_text}</td>
                    <td class="number">{volume_text}</td>
                    <td class="number">{sma5_text}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>
    </div>
</div>
</body>
</html>
"""

    return html


if __name__ == '__main__':
    result_df, summary = main()

    # HTML生成
    print()
    print("[5/5] HTML生成中...")
    html = generate_html_report(result_df, summary)
    OUTPUT_HTML.write_text(html, encoding='utf-8')
    print(f"  保存完了: {OUTPUT_HTML}")
    print()
