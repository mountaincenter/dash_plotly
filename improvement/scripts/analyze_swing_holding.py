#!/usr/bin/env python3
"""
analyze_swing_holding.py
GROK銘柄のデイスイング分析（1-5営業日保有）
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

def load_data():
    grok = pd.read_parquet(DATA_DIR / "grok_analysis_merged_v2_1.parquet")
    prices = pd.read_parquet(DATA_DIR / "prices_max_1d.parquet")
    prices['date'] = pd.to_datetime(prices['date']).dt.date
    return grok, prices

def get_future_prices(prices_df, ticker, start_date, days_list=[1,2,3,4,5]):
    ticker_prices = prices_df[prices_df['ticker'] == ticker].copy()
    ticker_prices = ticker_prices.sort_values('date')

    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date).date()
    elif hasattr(start_date, 'date'):
        start_date = start_date.date()

    dates = ticker_prices['date'].tolist()
    try:
        start_idx = dates.index(start_date)
    except ValueError:
        return {d: None for d in days_list}

    result = {}
    for days in days_list:
        target_idx = start_idx + days
        if target_idx < len(dates):
            target_date = dates[target_idx]
            close_price = ticker_prices[ticker_prices['date'] == target_date]['Close'].values
            result[days] = close_price[0] if len(close_price) > 0 else None
        else:
            result[days] = None
    return result

def calculate_swing_returns(grok_df, prices_df, action_col='v2_1_action'):
    results = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']
        buy_price = row['buy_price']
        sell_price = row.get('sell_price', row.get('daily_close'))
        action = row.get(action_col, '買い')

        if pd.isna(buy_price) or buy_price <= 0:
            continue

        stock_name = row.get('stock_name') or row.get('company_name') or ticker
        if pd.isna(stock_name) or stock_name == '':
            stock_name = ticker

        future_closes = get_future_prices(prices_df, ticker, backtest_date)

        record = {
            'backtest_date': row['backtest_date'],
            'ticker': ticker,
            'stock_name': stock_name,
            'action': action,
            'buy_price': buy_price,
        }

        # 当日損益（100株あたり円）
        if sell_price and not pd.isna(sell_price):
            if action == '売り':
                day0_profit = (buy_price - sell_price) * 100
            else:
                day0_profit = (sell_price - buy_price) * 100
            record['day0_profit'] = day0_profit
        else:
            record['day0_profit'] = None

        # 1-5日後の損益
        for days in [1, 2, 3, 4, 5]:
            close_price = future_closes.get(days)
            if close_price is not None and close_price > 0:
                if action == '売り':
                    profit = (buy_price - close_price) * 100
                else:
                    profit = (close_price - buy_price) * 100
                record[f'day{days}_profit'] = profit
            else:
                record[f'day{days}_profit'] = None

        results.append(record)

    return pd.DataFrame(results)

def generate_summary(df):
    summary_rows = []

    for action in ['買い', '静観', '売り', '全体']:
        if action == '全体':
            subset = df
        else:
            subset = df[df['action'] == action]

        if len(subset) == 0:
            continue

        row = {'action': action, 'count': len(subset)}

        for d in [0, 1, 2, 3, 4, 5]:
            col = f'day{d}_profit'
            profits = subset[col].dropna()
            if len(profits) > 0:
                row[f'day{d}_avg'] = profits.mean()
                row[f'day{d}_win'] = (profits > 0).mean() * 100
                row[f'day{d}_total'] = profits.sum()
            else:
                row[f'day{d}_avg'] = None
                row[f'day{d}_win'] = None
                row[f'day{d}_total'] = None

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)

def fmt_yen(val):
    if val is None or pd.isna(val):
        return "-"
    return f"{val:,.0f}円"

def fmt_pct(val):
    if val is None or pd.isna(val):
        return "-"
    return f"{val:.1f}%"

def generate_html_report(results_v203, results_v21, summary_v203, summary_v21, output_path):
    """ダークテーマHTML生成"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    css = """
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0a; color: #e0e0e0; padding: 20px; }
        .container { max-width: 1600px; margin: 0 auto; }
        h1 { color: #4ade80; margin-bottom: 10px; }
        h2 { color: #60a5fa; margin: 30px 0 15px; border-bottom: 1px solid #333; padding-bottom: 10px; }
        h3 { color: #fbbf24; margin: 20px 0 10px; }
        .subtitle { color: #888; margin-bottom: 30px; }
        .card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .card { background: #1a1a1a; border-radius: 12px; padding: 15px; border: 1px solid #333; }
        .card h4 { color: #888; font-size: 12px; margin-bottom: 5px; }
        .card .value { font-size: 24px; font-weight: bold; }
        .card .value.green { color: #4ade80; }
        .card .value.red { color: #f87171; }
        .card .value.blue { color: #60a5fa; }
        .card .detail { color: #888; font-size: 12px; margin-top: 5px; }
        table { width: 100%; border-collapse: collapse; margin: 15px 0; background: #1a1a1a; border-radius: 8px; overflow: hidden; }
        th { background: #252525; color: #888; font-weight: 500; text-align: right; padding: 10px 8px; font-size: 12px; }
        th:first-child, th:nth-child(2), th:nth-child(3) { text-align: left; }
        td { padding: 8px; border-top: 1px solid #2a2a2a; font-size: 13px; text-align: right; }
        td:first-child, td:nth-child(2), td:nth-child(3) { text-align: left; }
        tr:hover { background: #252525; }
        .positive { color: #4ade80; }
        .negative { color: #f87171; }
        .comparison-box { background: linear-gradient(135deg, #1a2e1a 0%, #1a1a2e 100%); border: 1px solid #4ade80; border-radius: 12px; padding: 25px; margin: 20px 0; }
        .comparison-grid { display: grid; grid-template-columns: 1fr auto 1fr; gap: 20px; align-items: center; }
        .comparison-item { text-align: center; }
        .comparison-item h4 { color: #888; margin-bottom: 10px; }
        .comparison-item .big { font-size: 32px; font-weight: bold; }
        .comparison-vs { font-size: 24px; color: #666; }
        .winner { background: rgba(74, 222, 128, 0.1); border-radius: 8px; padding: 15px; }
        .signal-box { border-radius: 8px; padding: 20px; margin: 15px 0; }
        .signal-buy { background: #1a2e1a; border: 1px solid #4ade80; }
        .signal-hold { background: #2e2e1a; border: 1px solid #fbbf24; }
        .signal-sell { background: #2e1a1a; border: 1px solid #f87171; }
        .signal-box h3 { margin-bottom: 15px; }
        .action-buy { background: rgba(74, 222, 128, 0.1); }
        .action-hold { background: rgba(251, 191, 36, 0.1); }
        .action-sell { background: rgba(248, 113, 113, 0.1); }
    """

    def make_summary_cards(summary_df, action):
        row = summary_df[summary_df['action'] == action]
        if len(row) == 0:
            return ""
        row = row.iloc[0]
        cards = ""
        for d in [0, 1, 3, 5]:
            label = '当日' if d == 0 else f'{d}日後'
            avg = row.get(f'day{d}_avg')
            win = row.get(f'day{d}_win')
            total = row.get(f'day{d}_total')
            avg_cls = 'green' if avg and avg > 0 else 'red' if avg and avg < 0 else ''
            cards += f"""
            <div class="card">
                <h4>{label}</h4>
                <div class="value {avg_cls}">{fmt_yen(avg)}</div>
                <div class="detail">勝率 {fmt_pct(win)} | 累計 {fmt_yen(total)}</div>
            </div>
            """
        return cards

    def make_detail_table(df):
        df_sorted = df.copy()
        df_sorted['action_order'] = df_sorted['action'].map({'買い': 0, '静観': 1, '売り': 2})
        df_sorted = df_sorted.sort_values(['backtest_date', 'action_order'], ascending=[False, True])

        rows = ""
        for _, row in df_sorted.iterrows():
            action = row['action']
            cls = 'action-buy' if action == '買い' else 'action-sell' if action == '売り' else 'action-hold'

            cols = f"<td>{row['backtest_date']}</td>"
            cols += f"<td>{row['ticker']}</td>"
            cols += f"<td>{row['stock_name']}</td>"
            cols += f"<td>{action}</td>"
            cols += f"<td>{row['buy_price']:,.0f}円</td>"

            for d in [0, 1, 2, 3, 4, 5]:
                profit = row.get(f'day{d}_profit')
                if profit is not None and not pd.isna(profit):
                    pcls = 'positive' if profit > 0 else 'negative'
                    cols += f"<td class='{pcls}'>{fmt_yen(profit)}</td>"
                else:
                    cols += "<td>-</td>"

            rows += f"<tr class='{cls}'>{cols}</tr>"

        return f"""
        <table>
            <tr>
                <th>日付</th><th>コード</th><th>銘柄名</th><th>判定</th><th>寄付</th>
                <th>当日</th><th>1日後</th><th>2日後</th><th>3日後</th><th>4日後</th><th>5日後</th>
            </tr>
            {rows}
        </table>
        """

    # v2.0.3 vs v2.1 比較（売りシグナル5日後）
    sell_v203 = summary_v203[summary_v203['action'] == '売り'].iloc[0] if len(summary_v203[summary_v203['action'] == '売り']) > 0 else None
    sell_v21 = summary_v21[summary_v21['action'] == '売り'].iloc[0] if len(summary_v21[summary_v21['action'] == '売り']) > 0 else None

    comparison_html = ""
    if sell_v203 is not None and sell_v21 is not None:
        v203_avg = sell_v203.get('day5_avg', 0) or 0
        v21_avg = sell_v21.get('day5_avg', 0) or 0
        v203_win = sell_v203.get('day5_win', 0) or 0
        v21_win = sell_v21.get('day5_win', 0) or 0
        v203_total = sell_v203.get('day5_total', 0) or 0
        v21_total = sell_v21.get('day5_total', 0) or 0

        winner_v203 = 'winner' if v203_avg > v21_avg else ''
        winner_v21 = 'winner' if v21_avg > v203_avg else ''

        comparison_html = f"""
        <div class="comparison-box">
            <h3 style="color:#f87171; text-align:center; margin-bottom:20px;">売りシグナル（空売り）5日後パフォーマンス比較</h3>
            <div class="comparison-grid">
                <div class="comparison-item {winner_v203}">
                    <h4>v2.0.3</h4>
                    <div class="big positive">{fmt_yen(v203_avg)}</div>
                    <div style="color:#888; margin-top:10px;">勝率 {v203_win:.1f}%</div>
                    <div style="color:#888;">累計 {fmt_yen(v203_total)}</div>
                </div>
                <div class="comparison-vs">vs</div>
                <div class="comparison-item {winner_v21}">
                    <h4>v2.1</h4>
                    <div class="big positive">{fmt_yen(v21_avg)}</div>
                    <div style="color:#888; margin-top:10px;">勝率 {v21_win:.1f}%</div>
                    <div style="color:#888;">累計 {fmt_yen(v21_total)}</div>
                </div>
            </div>
        </div>
        """

    # シグナル別セクション生成
    def make_signal_section(summary_df, results_df, version):
        sections = ""
        for action, box_cls, title_color in [('買い', 'signal-buy', '#4ade80'), ('静観', 'signal-hold', '#fbbf24'), ('売り', 'signal-sell', '#f87171')]:
            row = summary_df[summary_df['action'] == action]
            if len(row) == 0:
                continue
            row = row.iloc[0]
            subset = results_df[results_df['action'] == action]

            sections += f"""
            <div class="signal-box {box_cls}">
                <h3 style="color:{title_color};">{action}シグナル（{int(row['count'])}件）</h3>
                <div class="card-grid">
                    {make_summary_cards(summary_df, action)}
                </div>
            </div>
            """
        return sections

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GROK銘柄 デイスイング分析</title>
    <style>{css}</style>
</head>
<body>
    <div class="container">
        <h1>GROK銘柄 デイスイング分析</h1>
        <p class="subtitle">寄付→n日後大引け | 買い/静観=ロング、売り=空売り | 100株あたり損益 | 生成: {now}</p>

        {comparison_html}

        <h2>v2.0.3 シグナル分析</h2>
        <p class="subtitle">データ期間: {results_v203['backtest_date'].min()} ~ {results_v203['backtest_date'].max()} | {len(results_v203)}件</p>
        {make_signal_section(summary_v203, results_v203, 'v2.0.3')}

        <h2>v2.1 シグナル分析</h2>
        <p class="subtitle">データ期間: {results_v21['backtest_date'].min()} ~ {results_v21['backtest_date'].max()} | {len(results_v21)}件</p>
        {make_signal_section(summary_v21, results_v21, 'v2.1')}

        <h2>v2.0.3 銘柄別詳細</h2>
        {make_detail_table(results_v203)}

        <h2>v2.1 銘柄別詳細</h2>
        {make_detail_table(results_v21)}
    </div>
</body>
</html>
"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    return output_path

def main():
    print("=" * 60)
    print("GROK銘柄 デイスイング分析（v2.0.3 / v2.1）")
    print("=" * 60)

    grok, prices = load_data()
    print(f"GROK銘柄: {len(grok)}件")
    print(f"価格データ: {len(prices)}件")

    # v2.0.3分析
    print("\n--- v2.0.3 分析 ---")
    results_v203 = calculate_swing_returns(grok, prices, action_col='v2_0_3_action')
    summary_v203 = generate_summary(results_v203)

    # v2.1分析
    print("--- v2.1 分析 ---")
    results_v21 = calculate_swing_returns(grok, prices, action_col='v2_1_action')
    summary_v21 = generate_summary(results_v21)

    # サマリー表示
    print("\n=== v2.0.3 ===")
    print(summary_v203[['action', 'count', 'day0_avg', 'day0_win', 'day5_avg', 'day5_win']].to_string(index=False))
    print("\n=== v2.1 ===")
    print(summary_v21[['action', 'count', 'day0_avg', 'day0_win', 'day5_avg', 'day5_win']].to_string(index=False))

    # HTML保存
    html_path = DATA_DIR / "grok_swing_analysis.html"
    generate_html_report(results_v203, results_v21, summary_v203, summary_v21, html_path)
    print(f"\n✅ HTML: {html_path}")

    # CSV保存
    results_v203.to_csv(DATA_DIR / "grok_swing_v2_0_3.csv", index=False)
    results_v21.to_csv(DATA_DIR / "grok_swing_v2_1.csv", index=False)
    print("✅ CSV保存完了")

    return results_v203, results_v21

if __name__ == "__main__":
    main()
