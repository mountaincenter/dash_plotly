#!/usr/bin/env python3
"""
ショートIFO戦略 バックテスト HTML生成
- 前場（9:00-10:00）と後場（12:30-15:00）のショートIFO
- 空売り可能銘柄のみ対象
"""

import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from pathlib import Path

# パス設定
IMPROVEMENT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = IMPROVEMENT_DIR / "data"
YFINANCE_DIR = IMPROVEMENT_DIR / "yfinance" / "data"
OUTPUT_DIR = IMPROVEMENT_DIR / "output"
BACKTEST_DIR = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest"

# 戦略パラメータ
STOP_LOSS_YEN = 1  # 損切り: +1円
TAKE_PROFIT_PCTS = [0.5, 0.8, 1.0, 1.5]  # 利確%
SHARES = 100  # 取引株数


def load_data():
    """データ読み込み"""
    print("データ読み込み中...")

    # Grok銘柄アーカイブ
    grok = pd.read_parquet(BACKTEST_DIR / "grok_trending_archive.parquet")
    print(f"  Grok銘柄: {len(grok)}件")

    # 5分足データ
    prices_5m = pd.read_parquet(YFINANCE_DIR / "prices_60d_5m.parquet")
    print(f"  5分足データ: {len(prices_5m)}件")

    return grok, prices_5m


def get_shortable_tickers(grok):
    """空売り可能銘柄を取得（is_shortableカラムを使用）"""
    grok_shortable = grok[grok['is_shortable'] == True].copy()
    print(f"  空売り可能: {len(grok_shortable)}/{len(grok)}件")
    return grok_shortable


def run_short_ifo_backtest(grok, prices_5m, session='morning'):
    """ショートIFOバックテスト実行"""
    results = []

    if session == 'morning':
        entry_time = time(9, 0)
        exit_time = time(10, 0)
    else:  # afternoon
        entry_time = time(12, 30)
        exit_time = time(15, 0)

    # 日付・時刻列の前処理
    if 'Datetime' in prices_5m.columns:
        prices_5m = prices_5m.copy()
        prices_5m['datetime'] = pd.to_datetime(prices_5m['Datetime'])
        prices_5m['date'] = prices_5m['datetime'].dt.date
        prices_5m['time'] = prices_5m['datetime'].dt.time

    for _, row in grok.iterrows():
        ticker = row['ticker']
        # backtest_dateカラムを使用
        backtest_date = pd.to_datetime(row['backtest_date']).date()
        stock_name = row.get('stock_name', ticker)

        # 該当銘柄の5分足データ
        ticker_data = prices_5m[prices_5m['ticker'] == ticker].copy()
        if ticker_data.empty:
            continue

        # 該当日のデータ
        day_data = ticker_data[ticker_data['date'] == backtest_date].copy()
        if day_data.empty:
            continue

        day_data = day_data.sort_values('datetime')

        # エントリー時刻のデータ
        entry_data = day_data[day_data['time'] == entry_time]
        if entry_data.empty:
            # 最も近い時刻を探す
            entry_data = day_data[day_data['time'] >= entry_time].head(1)
            if entry_data.empty:
                continue

        entry_price = entry_data.iloc[0]['Open']
        entry_datetime = entry_data.iloc[0]['datetime']

        # エントリー後のデータ
        post_entry = day_data[day_data['datetime'] > entry_datetime]
        post_entry = post_entry[post_entry['time'] <= exit_time]

        if post_entry.empty:
            continue

        # 各利確%でシミュレーション
        for tp_pct in TAKE_PROFIT_PCTS:
            stop_loss_price = entry_price + STOP_LOSS_YEN
            take_profit_price = entry_price * (1 - tp_pct / 100)

            exit_price = None
            exit_reason = 'timeout'
            exit_dt = None

            for _, bar in post_entry.iterrows():
                # 損切りチェック（高値が損切りラインを超えた）
                if bar['High'] >= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_reason = 'stop_loss'
                    exit_dt = bar['datetime']
                    break
                # 利確チェック（安値が利確ラインを下回った）
                if bar['Low'] <= take_profit_price:
                    exit_price = take_profit_price
                    exit_reason = 'take_profit'
                    exit_dt = bar['datetime']
                    break

            # タイムアウト決済
            if exit_price is None:
                exit_bar = post_entry.iloc[-1]
                exit_price = exit_bar['Close']
                exit_dt = exit_bar['datetime']

            # 損益計算（ショートなので entry - exit）
            pnl_pct = (entry_price - exit_price) / entry_price * 100
            pnl_amount = (entry_price - exit_price) * SHARES

            results.append({
                'date': backtest_date,
                'ticker': ticker,
                'stock_name': stock_name,
                'session': session,
                'take_profit_pct': tp_pct,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'exit_reason': exit_reason,
                'pnl_pct': pnl_pct,
                'pnl_amount': pnl_amount,
            })

    return pd.DataFrame(results)


def generate_html(results_morning, results_afternoon, total_grok, shortable_count):
    """HTMLレポート生成"""

    # 利確1.5%のデータで集計
    m_15 = results_morning[results_morning['take_profit_pct'] == 1.5]
    a_15 = results_afternoon[results_afternoon['take_profit_pct'] == 1.5]

    total_pnl = m_15['pnl_amount'].sum() + a_15['pnl_amount'].sum()
    morning_pnl = m_15['pnl_amount'].sum()
    afternoon_pnl = a_15['pnl_amount'].sum()

    # 利確%別サマリー
    def calc_summary(df, session):
        summaries = []
        for tp in TAKE_PROFIT_PCTS:
            subset = df[df['take_profit_pct'] == tp]
            total = len(subset)
            wins = len(subset[subset['pnl_amount'] > 0])
            pnl = subset['pnl_amount'].sum()
            summaries.append({
                'tp': tp,
                'total': total,
                'wins': wins,
                'win_rate': wins / total * 100 if total > 0 else 0,
                'pnl': pnl,
            })
        return summaries

    morning_summary = calc_summary(results_morning, 'morning')
    afternoon_summary = calc_summary(results_afternoon, 'afternoon')

    # 日付別集計（利確1.5%）
    dates = sorted(set(m_15['date'].unique()) | set(a_15['date'].unique()), reverse=True)

    # 期間
    if dates:
        period_start = min(dates)
        period_end = max(dates)
    else:
        period_start = period_end = '-'

    # 利確%別カード生成
    def make_summary_cards(summaries):
        cards = ""
        for s in summaries:
            pnl_class = 'positive' if s['pnl'] >= 0 else 'negative'
            cards += f"""
                <div class="card">
                    <div class="card-title">利確 {s['tp']}%</div>
                    <div class="card-value {pnl_class}">{s['pnl']:+,.0f}円</div>
                    <div>勝率: {s['win_rate']:.1f}% ({s['wins']}/{s['total']})</div>
                </div>
            """
        return cards

    # 日付別セクション生成
    date_sections = ""
    for d in dates:
        m_day = m_15[m_15['date'] == d]
        a_day = a_15[a_15['date'] == d]
        day_pnl = m_day['pnl_amount'].sum() + a_day['pnl_amount'].sum()
        m_pnl = m_day['pnl_amount'].sum()
        a_pnl = a_day['pnl_amount'].sum()
        count = len(m_day['ticker'].unique())

        pnl_class = 'positive' if day_pnl >= 0 else 'negative'

        # 前場テーブル
        morning_rows = ""
        for _, row in m_day.iterrows():
            r_class = 'positive' if row['pnl_amount'] >= 0 else 'negative'
            reason_badge = {
                'take_profit': '<span class="badge badge-profit">利確</span>',
                'stop_loss': '<span class="badge badge-loss">損切</span>',
                'timeout': '<span class="badge badge-timeout">時間</span>',
            }.get(row['exit_reason'], '')
            morning_rows += f"""
                <tr>
                    <td>{row['ticker']}</td>
                    <td>{str(row['stock_name'])[:8]}</td>
                    <td class="number">{row['entry_price']:,.0f}</td>
                    <td class="number">{row['exit_price']:,.0f}</td>
                    <td class="number {r_class}">{row['pnl_amount']:+,.0f}円</td>
                    <td>{reason_badge}</td>
                </tr>
            """

        # 後場テーブル
        afternoon_rows = ""
        for _, row in a_day.iterrows():
            r_class = 'positive' if row['pnl_amount'] >= 0 else 'negative'
            reason_badge = {
                'take_profit': '<span class="badge badge-profit">利確</span>',
                'stop_loss': '<span class="badge badge-loss">損切</span>',
                'timeout': '<span class="badge badge-timeout">時間</span>',
            }.get(row['exit_reason'], '')
            afternoon_rows += f"""
                <tr>
                    <td>{row['ticker']}</td>
                    <td>{str(row['stock_name'])[:8]}</td>
                    <td class="number">{row['entry_price']:,.0f}</td>
                    <td class="number">{row['exit_price']:,.0f}</td>
                    <td class="number {r_class}">{row['pnl_amount']:+,.0f}円</td>
                    <td>{reason_badge}</td>
                </tr>
            """

        date_sections += f"""
    <div class="date-section">
        <div class="date-header">
            <span><strong>{d}</strong> - {count}銘柄</span>
            <span>合計(1.5%): <span class="{pnl_class}">{day_pnl:+,.0f}円</span> (前場{m_pnl:+,.0f} / 後場{a_pnl:+,.0f})</span>
        </div>
        <div class="date-content">
            <div class="session-grid">
                <div class="session-box">
                    <h4>前場（9:00-10:00）</h4>
                    <table>
                        <tr><th>コード</th><th>銘柄</th><th>IN</th><th>OUT</th><th>損益</th><th>理由</th></tr>
                        {morning_rows if morning_rows else '<tr><td colspan="6" style="color:var(--text-secondary);">データなし</td></tr>'}
                    </table>
                </div>
                <div class="session-box afternoon">
                    <h4>後場（12:30-15:00）</h4>
                    <table>
                        <tr><th>コード</th><th>銘柄</th><th>IN</th><th>OUT</th><th>損益</th><th>理由</th></tr>
                        {afternoon_rows if afternoon_rows else '<tr><td colspan="6" style="color:var(--text-secondary);">データなし</td></tr>'}
                    </table>
                </div>
            </div>
        </div>
    </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ショートIFO戦略 バックテスト（前場+後場）</title>
    <style>
        :root {{
            --bg-primary: #1a1a2e;
            --bg-secondary: #16213e;
            --bg-card: #0f3460;
            --text-primary: #e0e0e0;
            --text-secondary: #a0a0a0;
            --border-color: #2a4a6a;
            --positive: #4ade80;
            --negative: #f87171;
            --accent-blue: #60a5fa;
            --accent-orange: #fb923c;
            --accent-purple: #a78bfa;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Consolas', 'Monaco', 'Menlo', monospace;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ margin-bottom: 10px; color: var(--accent-blue); }}
        h2 {{ margin: 30px 0 15px; color: var(--accent-purple); border-bottom: 1px solid var(--border-color); padding-bottom: 5px; }}
        h3 {{ margin: 20px 0 10px; color: var(--text-secondary); }}
        .meta {{ color: var(--text-secondary); margin-bottom: 20px; }}
        .warning {{ background: rgba(251, 191, 36, 0.15); border: 1px solid #fbbf24; padding: 10px; border-radius: 5px; margin-bottom: 20px; color: #fbbf24; }}
        .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }}
        .summary-section {{ background: var(--bg-secondary); border-radius: 8px; padding: 15px; border: 1px solid var(--border-color); }}
        .summary-section h3 {{ margin-top: 0; color: var(--accent-blue); }}
        .summary-section.afternoon h3 {{ color: var(--accent-orange); }}
        .summary-cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }}
        .card {{ background: var(--bg-card); border-radius: 8px; padding: 12px; border: 1px solid var(--border-color); }}
        .card-title {{ font-size: 0.85em; color: var(--text-secondary); }}
        .card-value {{ font-size: 1.4em; font-weight: bold; }}
        .card-value.positive {{ color: var(--positive); }}
        .card-value.negative {{ color: var(--negative); }}
        .total-box {{ background: linear-gradient(135deg, #065f46, #0f3460); color: white; padding: 20px; border-radius: 8px; text-align: center; margin-bottom: 30px; border: 1px solid var(--positive); }}
        .total-box .label {{ font-size: 1em; opacity: 0.9; }}
        .total-box .value {{ font-size: 2.5em; font-weight: bold; color: var(--positive); }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 0.85em; }}
        th, td {{ padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--border-color); }}
        th {{ background: var(--bg-card); font-weight: 600; color: var(--text-secondary); }}
        tr:hover {{ background: rgba(96, 165, 250, 0.1); }}
        .number {{ text-align: right; }}
        .positive {{ color: var(--positive); }}
        .negative {{ color: var(--negative); }}
        .date-section {{ margin-bottom: 30px; }}
        .date-header {{ background: var(--bg-card); color: var(--text-primary); padding: 10px 15px; border-radius: 5px 5px 0 0; display: flex; justify-content: space-between; border: 1px solid var(--border-color); }}
        .date-header .positive {{ background: rgba(74, 222, 128, 0.2); padding: 2px 8px; border-radius: 4px; }}
        .date-header .negative {{ background: rgba(248, 113, 113, 0.2); padding: 2px 8px; border-radius: 4px; }}
        .date-content {{ border: 1px solid var(--border-color); border-top: none; padding: 15px; background: var(--bg-secondary); }}
        .session-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        .session-box {{ }}
        .session-box h4 {{ color: var(--accent-blue); margin-bottom: 10px; padding: 5px; background: rgba(96, 165, 250, 0.15); border-radius: 3px; }}
        .session-box.afternoon h4 {{ color: var(--accent-orange); background: rgba(251, 146, 60, 0.15); }}
        .badge {{ display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 0.75em; }}
        .badge-profit {{ background: rgba(74, 222, 128, 0.15); color: var(--positive); }}
        .badge-loss {{ background: rgba(248, 113, 113, 0.15); color: var(--negative); }}
        .badge-timeout {{ background: rgba(160, 160, 160, 0.15); color: var(--text-secondary); }}
        .strategy-box {{ background: var(--bg-secondary); border: 1px solid var(--accent-blue); padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .strategy-box ul {{ margin-left: 20px; color: var(--text-secondary); }}
    </style>
</head>
<body>
<div class="container">
    <h1>ショートIFO戦略 バックテスト（前場+後場）</h1>
    <p class="meta">生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 対象期間: {period_start} - {period_end}</p>

    <div class="warning">
        <strong>空売り制限:</strong> 全{total_grok}件中、空売り可能銘柄 <strong>{shortable_count}</strong> のみ分析
    </div>

    <div class="total-box">
        <div class="label">前場+後場 合計（利確1.5%）</div>
        <div class="value">{total_pnl:+,.0f}円</div>
        <div style="margin-top:10px; font-size:0.9em;">前場: {morning_pnl:+,.0f}円 / 後場: {afternoon_pnl:+,.0f}円</div>
    </div>

    <div class="strategy-box">
        <h3>戦略パラメータ</h3>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 10px;">
            <div>
                <strong>前場（9:00-10:00）</strong>
                <ul>
                    <li>エントリー: 9:00 寄付きショート</li>
                    <li>損切り: +1円（-100円/100株）</li>
                    <li>利確: -1.5%</li>
                    <li>タイムアウト: 10:00 成行決済</li>
                </ul>
            </div>
            <div>
                <strong>後場（12:30-15:00）</strong>
                <ul>
                    <li>エントリー: 12:30 寄付きショート</li>
                    <li>損切り: +1円（-100円/100株）</li>
                    <li>利確: -1.5%</li>
                    <li>タイムアウト: 大引け 成行決済</li>
                </ul>
            </div>
        </div>
    </div>

    <h2>利確%別パフォーマンス</h2>
    <div class="summary-grid">
        <div class="summary-section">
            <h3>前場（9:00-10:00）</h3>
            <div class="summary-cards">
                {make_summary_cards(morning_summary)}
            </div>
        </div>
        <div class="summary-section afternoon">
            <h3>後場（12:30-15:00）</h3>
            <div class="summary-cards">
                {make_summary_cards(afternoon_summary)}
            </div>
        </div>
    </div>

    <h2>日付別詳細</h2>

    {date_sections}

</div>
</body>
</html>"""

    return html


def main():
    print("=" * 60)
    print("ショートIFO戦略 バックテスト")
    print("=" * 60)

    # データ読み込み
    grok, prices_5m = load_data()
    total_grok = len(grok)

    # 空売り可能銘柄のみ
    grok_shortable = get_shortable_tickers(grok)
    shortable_count = len(grok_shortable)

    # バックテスト実行
    print("\n前場バックテスト...")
    results_morning = run_short_ifo_backtest(grok_shortable, prices_5m, session='morning')
    print(f"  結果: {len(results_morning)}件")

    print("\n後場バックテスト...")
    results_afternoon = run_short_ifo_backtest(grok_shortable, prices_5m, session='afternoon')
    print(f"  結果: {len(results_afternoon)}件")

    # HTML生成
    html = generate_html(results_morning, results_afternoon, total_grok, shortable_count)

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "grok_9am_short_ifo.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ HTML保存: {output_path}")

    # サマリー表示
    m_15 = results_morning[results_morning['take_profit_pct'] == 1.5]
    a_15 = results_afternoon[results_afternoon['take_profit_pct'] == 1.5]
    print(f"\n合計損益（利確1.5%）: {m_15['pnl_amount'].sum() + a_15['pnl_amount'].sum():+,.0f}円")
    print(f"  前場: {m_15['pnl_amount'].sum():+,.0f}円")
    print(f"  後場: {a_15['pnl_amount'].sum():+,.0f}円")


if __name__ == "__main__":
    main()
