#!/usr/bin/env python3
"""
静観→売りに変わった銘柄の成績分析をHTMLレポートに追加
"""
import pandas as pd
from pathlib import Path

def calculate_result(row):
    """勝敗判定: 始値で信用売り、終値で買い戻し"""
    buy_price = row['buy_price']
    sell_price = row['sell_price']

    if buy_price > sell_price:
        return '勝'
    elif buy_price < sell_price:
        return '負'
    else:
        return '引分'

def calculate_profit(row):
    """100株あたりの利益: (始値 - 終値) × 100"""
    return (row['buy_price'] - row['sell_price']) * 100

def format_price(price):
    """株価フォーマット: #,###"""
    return f"{price:,.0f}"

def format_percent(value):
    """パーセントフォーマット: #.##%"""
    return f"{value:.2f}%"

def format_profit(profit):
    """利益フォーマット: 符号付き、色付き"""
    if profit > 0:
        return f'<span style="color: green;">+{format_price(profit)}</span>'
    elif profit < 0:
        return f'<span style="color: red;">{format_price(profit)}</span>'
    else:
        return format_price(profit)

def format_change_pct(row):
    """変動率フォーマット: ±#.##%、色付き"""
    change_pct = ((row['sell_price'] - row['buy_price']) / row['buy_price']) * 100
    if change_pct > 0:
        return f'<span style="color: red;">+{change_pct:.2f}%</span>'
    elif change_pct < 0:
        return f'<span style="color: green;">{change_pct:.2f}%</span>'
    else:
        return '0.00%'

def generate_hold_to_sell_section(df):
    """静観→売り銘柄の分析セクションを生成"""
    # 静観→売り の銘柄を抽出
    hold_to_sell = df[(df['v2_0_3_action'] == '静観') & (df['v2_1_0_action'] == '売り')].copy()

    if len(hold_to_sell) == 0:
        return ""

    # 勝敗と利益を計算
    hold_to_sell['result'] = hold_to_sell.apply(calculate_result, axis=1)
    hold_to_sell['profit_100'] = hold_to_sell.apply(calculate_profit, axis=1)

    # 統計計算
    total_count = len(hold_to_sell)
    win_count = (hold_to_sell['result'] == '勝').sum()
    lose_count = (hold_to_sell['result'] == '負').sum()
    draw_count = (hold_to_sell['result'] == '引分').sum()
    win_rate = (win_count / total_count) * 100 if total_count > 0 else 0
    avg_profit = hold_to_sell['profit_100'].mean()
    total_profit = hold_to_sell['profit_100'].sum()

    # サマリーHTML（既存のsummary-cardスタイルを使用）
    summary_html = f"""
    <div class="summary-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">📊 静観→売り 銘柄の成績</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>対象銘柄数</h3>
                <div class="stat-row">
                    <span class="stat-label">銘柄数</span>
                    <span class="stat-value">{total_count}</span>
                </div>
            </div>
            <div class="summary-card">
                <h3>勝率（売りとして）</h3>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if win_rate >= 50 else '#e74c3c'};">{win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝</span>
                    <span class="stat-value" style="font-size: 1.2em; color: #27ae60;">{win_count}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">負</span>
                    <span class="stat-value" style="font-size: 1.2em; color: #e74c3c;">{lose_count}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">引分</span>
                    <span class="stat-value" style="font-size: 1.2em; color: #adb5bd;">{draw_count}</span>
                </div>
            </div>
            <div class="summary-card">
                <h3>100株あたり平均利益</h3>
                <div class="stat-row">
                    <span class="stat-label">平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if avg_profit > 0 else '#e74c3c'};">{'+'if avg_profit > 0 else ''}{format_price(avg_profit)}</span>
                </div>
            </div>
            <div class="summary-card">
                <h3>合計利益</h3>
                <div class="stat-row">
                    <span class="stat-label">100株×{total_count}銘柄</span>
                    <span class="stat-value" style="color: {'#27ae60' if total_profit > 0 else '#e74c3c'};">{'+'if total_profit > 0 else ''}{format_price(total_profit)}</span>
                </div>
            </div>
        </div>
    </div>
    """

    # 日別テーブルHTML（詳細比較テーブルと同じレイアウト）
    # 日付降順でソート
    hold_to_sell_sorted = hold_to_sell.sort_values('backtest_date', ascending=False)

    # 日付ごとにグループ化
    table_rows = []
    current_date = None

    for _, row in hold_to_sell_sorted.iterrows():
        row_date = row['backtest_date'].strftime('%Y-%m-%d')

        # 日付が変わったら日付セパレータを挿入
        if current_date != row_date:
            current_date = row_date
            date_count = len(hold_to_sell_sorted[hold_to_sell_sorted['backtest_date'] == row['backtest_date']])
            table_rows.append(f"""
        <tr class="date-separator">
            <td colspan="12">{row_date} （{date_count}件）</td>
        </tr>""")

        # 前々日→前日の変動
        prev_2day_close = row.get('prev_2day_close', 0)
        prev_day_close = row.get('prev_day_close', 0)
        prev_day_change = prev_day_close - prev_2day_close if prev_2day_close > 0 else 0
        prev_day_change_class = 'positive' if prev_day_change > 0 else ('negative' if prev_day_change < 0 else '')

        # 変動率を計算
        change_pct = ((row['sell_price'] - row['buy_price']) / row['buy_price']) * 100
        change_class = 'positive' if change_pct > 0 else ('negative' if change_pct < 0 else '')
        change_sign = '+' if change_pct > 0 else ''

        # 利益のクラス
        profit_class = 'positive' if row['profit_100'] > 0 else ('negative' if row['profit_100'] < 0 else '')
        profit_sign = '+' if row['profit_100'] > 0 else ''

        # 結果の色
        result_badge_class = {
            '勝': 'positive',
            '負': 'negative',
            '引分': ''
        }.get(row['result'], '')

        table_rows.append(f"""
        <tr class="action-売り">
            <td>{row['ticker']}</td>
            <td>{row['stock_name']}</td>
            <td><span class="action-badge action-静観-badge">静観</span></td>
            <td><span class="action-badge action-売り-badge">売り</span></td>
            <td class="number">{format_price(prev_2day_close)}</td>
            <td class="number {prev_day_change_class}">{format_price(prev_day_close)}</td>
            <td class="number">{format_price(row['buy_price'])}</td>
            <td class="number">{format_price(row['sell_price'])}</td>
            <td class="number {change_class}">{change_sign}{change_pct:.2f}%</td>
            <td class="number {profit_class}">{profit_sign}{format_price(row['profit_100'])}</td>
            <td class="number {result_badge_class}" style="font-weight: 600;">{row['result']}</td>
        </tr>""")

    table_html = f"""
    <h2 style="margin-bottom: 24px; color: #667eea;">📋 日別詳細（静観→売り）</h2>
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>v2.0.3</th>
                    <th>v2.1.0</th>
                    <th class="number">前々日終値</th>
                    <th class="number">前日終値</th>
                    <th class="number">始値</th>
                    <th class="number">終値</th>
                    <th class="number">変動率</th>
                    <th class="number">100株利益</th>
                    <th class="number">結果</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>
    """

    return summary_html + table_html

def main():
    # パス設定
    base_dir = Path(__file__).parent.parent.parent
    data_file = base_dir / 'improvement' / 'data' / 'v2_1_0_comparison_results.parquet'
    html_file = base_dir / 'improvement' / 'v2_1_0_comparison_report.html'

    # データ読み込み
    print(f"データ読み込み: {data_file}")
    df = pd.read_parquet(data_file)

    # 分析セクション生成
    print("静観→売り銘柄の分析を生成中...")
    hold_to_sell_section = generate_hold_to_sell_section(df)

    # HTML読み込み
    print(f"HTML読み込み: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # 既存のセクションを削除（もしあれば）- 複数回実行して全て削除
    while '📊 静観→売り 銘柄の成績' in html_content:
        # パターン1: 新しいスタイル（summary-section）
        start_marker_new = '<div class="summary-section">\n        <h2 style="margin-bottom: 24px; color: #667eea;">📊 静観→売り 銘柄の成績</h2>'
        start_idx = html_content.find(start_marker_new)

        if start_idx != -1:
            # 詳細比較テーブルの見出しまでを削除
            end_marker = '\n    <h2 style="margin-bottom: 24px; color: #667eea;">詳細比較テーブル</h2>'
            end_idx = html_content.find(end_marker, start_idx)
            if end_idx != -1:
                html_content = html_content[:start_idx] + html_content[end_idx + 1:]
                print("既存の静観→売りセクション（新スタイル）を削除しました")
                continue

        # パターン2: 古いスタイル（inline style）
        start_marker_old = '<div style="margin: 30px 0; padding: 20px; background-color: #f8f9fa; border-left: 4px solid #007bff; border-radius: 4px;">\n        <h3 style="margin-top: 0; color: #007bff;">📊 静観→売り 銘柄の成績</h3>'
        start_idx = html_content.find(start_marker_old)

        if start_idx != -1:
            # </table>まで探して削除（テーブルの終わりまで）
            # 2つのセクション（サマリーとテーブル）があるので、2つ目の</table>を探す
            temp_idx = start_idx
            table_count = 0
            while table_count < 2:
                temp_idx = html_content.find('</table>', temp_idx)
                if temp_idx == -1:
                    break
                temp_idx += len('</table>')
                table_count += 1

            if temp_idx != -1:
                # さらに終了divを探す
                end_idx = html_content.find('</div>', temp_idx)
                if end_idx != -1:
                    end_idx += len('</div>\n    ')
                    html_content = html_content[:start_idx] + html_content[end_idx:]
                    print("既存の静観→売りセクション（旧スタイル）を削除しました")
                    continue

        # どちらのパターンも見つからなかった場合
        print("⚠️ 警告: 静観→売りセクションが見つかりましたが、削除できませんでした")
        break

    # 詳細比較テーブルの直前に挿入
    # "詳細比較テーブル" の見出しを探す
    insert_marker = '<h2 style="margin-bottom: 24px; color: #667eea;">詳細比較テーブル</h2>'
    insert_idx = html_content.find(insert_marker)

    if insert_idx == -1:
        print("❌ エラー: 詳細比較テーブルの見出しが見つかりません")
        return

    # セクションを挿入（見出しの直前に挿入）
    # インデントを調整
    html_content = html_content[:insert_idx] + hold_to_sell_section + '\n    ' + html_content[insert_idx:]

    # HTML保存
    print(f"HTML保存: {html_file}")
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print("✅ 完了: 静観→売り銘柄の分析をHTMLに追加しました")

    # 統計を表示
    hold_to_sell = df[(df['v2_0_3_action'] == '静観') & (df['v2_1_0_action'] == '売り')].copy()
    hold_to_sell['result'] = hold_to_sell.apply(calculate_result, axis=1)
    hold_to_sell['profit_100'] = hold_to_sell.apply(calculate_profit, axis=1)

    total_count = len(hold_to_sell)
    win_count = (hold_to_sell['result'] == '勝').sum()
    lose_count = (hold_to_sell['result'] == '負').sum()
    draw_count = (hold_to_sell['result'] == '引分').sum()
    win_rate = (win_count / total_count) * 100 if total_count > 0 else 0
    avg_profit = hold_to_sell['profit_100'].mean()

    print(f"\n📊 統計サマリー:")
    print(f"  対象銘柄数: {total_count}")
    print(f"  勝率: {win_rate:.2f}% (勝: {win_count}, 負: {lose_count}, 引分: {draw_count})")
    print(f"  100株あたり平均利益: {avg_profit:,.0f}")

if __name__ == '__main__':
    main()
