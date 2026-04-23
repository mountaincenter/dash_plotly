#!/usr/bin/env python3
"""
grok_trending_archive.parquet から full_analysis_list.html を生成するスクリプト

使用方法:
    python scripts/generate_full_analysis_html.py

データソース:
    - ローカル: data/parquet/backtest/grok_trending_archive.parquet
    - S3: s3://stock-api-data/parquet/backtest/grok_trending_archive.parquet

戦略ルール（バックテスト結果に基づく）:
    【金曜日】
    - いちにち信用 × ATR7%+ × RSI<70 → ショート (+204,400円, 勝率72%)
    - 制度信用 × RSI70+ → ロング (+19,800円, 勝率75%)

    【エントリータイミング】
    - 寄り付き（9:00）でエントリー
    - 前場引け（11:30）または後場引け（15:00）で決済

    【レジーム転換警告 (2026-01-13以降)】
    - 1/13を境にショート優位→ロング優位に転換の兆候
    - 火曜ショートが崩壊（勝率63%→32%）
    - 直近2週間で従来ルールの効果が激減
    - 原因: 選挙ラリー開始（1/23解散期待）、TSMC好決算など外部材料
    - 推奨: データ収集に徹する（ルール崩壊期）
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import sys
import jpholiday  # 祝日判定用

# 定数
WEEKDAY_NAMES = ['月', '火', '水', '木', '金', '土', '日']

# レジーム転換日（この日以降は警告を表示）
REGIME_CHANGE_DATE = pd.Timestamp('2026-01-13')


def is_trading_day(date: datetime) -> bool:
    """営業日かどうかを判定（土日祝を除く）"""
    if date.weekday() >= 5:  # 土日
        return False
    try:
        if jpholiday.is_holiday(date):
            return False
    except:
        pass
    return True


def get_next_trading_day(from_date: datetime = None) -> datetime:
    """次の営業日を取得"""
    if from_date is None:
        from_date = datetime.now()

    # 現在時刻が15:00以降なら翌日から探す
    if from_date.hour >= 15:
        from_date = from_date + timedelta(days=1)

    # 営業日を探す
    while not is_trading_day(from_date):
        from_date = from_date + timedelta(days=1)

    return from_date


def get_today_or_next_trading_day() -> tuple:
    """
    今日が営業日なら今日、そうでなければ次の営業日を返す

    Returns:
        tuple: (日付, 「今日」か「次の営業日」かのラベル)
    """
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if is_trading_day(today) and now.hour < 15:
        return (today, '今日')
    else:
        next_day = get_next_trading_day(now)
        return (next_day, '次の営業日')


def get_strategy_recommendation(row: pd.Series) -> dict:
    """
    利益が出る戦略を判定

    Returns:
        dict with:
            - action: 'SHORT', 'LONG', 'SKIP'
            - reason: 判定理由
            - confidence: 信頼度 ('HIGH', 'MEDIUM', 'LOW')
            - credit_type: 'いちにち', '制度', None
    """
    rsi = row.get('rsi9', 0)
    atr = row.get('atr14_pct', 0)
    rsi = 0 if pd.isna(rsi) else rsi
    atr = 0 if pd.isna(atr) else atr

    weekday = row.get('weekday', -1)
    if pd.isna(weekday):
        weekday = -1
    weekday = int(weekday)

    day_trade = row.get('day_trade', False)
    is_shortable = row.get('is_shortable', False)
    margin_code = row.get('margin_code', '')
    ng = row.get('ng', False)

    # NG銘柄はスキップ
    if ng:
        return {'action': 'SKIP', 'reason': 'NG銘柄', 'confidence': None, 'credit_type': None}

    # 金曜日の戦略
    if weekday == 4:  # 金曜
        # いちにち信用 × ATR7%+ × RSI<70 → ショート
        if day_trade and atr >= 7.0 and rsi < 70:
            return {
                'action': 'SHORT',
                'reason': f'ATR{atr:.1f}%≥7 & RSI{int(rsi)}<70',
                'confidence': 'HIGH',
                'credit_type': 'いちにち'
            }

        # 制度信用 × RSI70+ → ロング
        if margin_code == '2' and is_shortable and rsi >= 70:
            return {
                'action': 'LONG',
                'reason': f'RSI{int(rsi)}≥70',
                'confidence': 'HIGH',
                'credit_type': '制度'
            }

        # 金曜日でRSI70以上はショート禁止
        if rsi >= 70:
            return {
                'action': 'SKIP',
                'reason': f'RSI{int(rsi)}≥70 ショート非推奨',
                'confidence': None,
                'credit_type': None
            }

    # デフォルト: ATR7%+でショート
    if day_trade and atr >= 7.0 and rsi < 70:
        return {
            'action': 'SHORT',
            'reason': f'ATR{atr:.1f}%≥7 & RSI{int(rsi)}<70',
            'confidence': 'MEDIUM',
            'credit_type': 'いちにち'
        }

    return {'action': 'SKIP', 'reason': '条件未達', 'confidence': None, 'credit_type': None}


def load_archive() -> pd.DataFrame:
    """アーカイブを読み込み（ローカル優先、なければS3から）"""
    local_path = Path('data/parquet/backtest/grok_trending_archive.parquet')

    if local_path.exists():
        print(f"📂 ローカルから読み込み: {local_path}")
        return pd.read_parquet(local_path)

    # S3から取得
    print("📥 S3からダウンロード中...")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run([
        'aws', 's3', 'cp',
        's3://stock-api-data/parquet/backtest/grok_trending_archive.parquet',
        str(local_path)
    ], capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ S3ダウンロード失敗: {result.stderr}")
        sys.exit(1)

    return pd.read_parquet(local_path)


def analyze_strategies(df: pd.DataFrame) -> dict:
    """曜日別・戦略別の損益を分析"""
    df = df.copy()
    df['short_profit'] = (df['buy_price'] - df['daily_close']) * 100
    df['long_profit'] = (df['daily_close'] - df['buy_price']) * 100

    if 'weekday' not in df.columns:
        df['weekday'] = pd.to_datetime(df['backtest_date']).dt.dayofweek

    results = {}

    for weekday in range(5):  # 月〜金
        weekday_df = df[df['weekday'] == weekday]
        if len(weekday_df) == 0:
            continue

        results[weekday] = {
            'total': len(weekday_df),
            'strategies': []
        }

        # いちにち信用 × ATR7%+ × RSI<70 → ショート
        day_trade = weekday_df[weekday_df['day_trade'] == True]
        cond1 = day_trade[(day_trade['atr14_pct'] >= 7) & (day_trade['rsi9'] < 70)]
        if len(cond1) > 0:
            profit = cond1['short_profit'].sum()
            win_rate = (cond1['short_profit'] > 0).mean() * 100
            results[weekday]['strategies'].append({
                'name': 'いちにち×ATR7%+×RSI<70→ショート',
                'profit': profit,
                'win_rate': win_rate,
                'count': len(cond1)
            })

        # 制度信用 × RSI70+ → ロング
        seido = weekday_df[(weekday_df['margin_code'] == '2') & (weekday_df['is_shortable'] == True)]
        cond2 = seido[seido['rsi9'] >= 70]
        if len(cond2) > 0:
            profit = cond2['long_profit'].sum()
            win_rate = (cond2['long_profit'] > 0).mean() * 100
            results[weekday]['strategies'].append({
                'name': '制度×RSI70+→ロング',
                'profit': profit,
                'win_rate': win_rate,
                'count': len(cond2)
            })

    return results


def generate_html(df: pd.DataFrame, output_path: Path) -> None:
    """HTMLを生成"""

    # weekdayがない場合は計算
    if 'weekday' not in df.columns:
        df['weekday'] = pd.to_datetime(df['backtest_date']).dt.dayofweek

    # 日付でソート（新しい順）
    df = df.sort_values('backtest_date', ascending=False).copy()

    # 損益計算
    df['short_profit'] = (df['buy_price'] - df['daily_close']).fillna(0) * 100
    df['long_profit'] = (df['daily_close'] - df['buy_price']).fillna(0) * 100

    # 戦略分析
    strategy_analysis = analyze_strategies(df)

    # 次の取引日を計算（今日が営業日なら今日、そうでなければ次の営業日）
    target_date, target_label = get_today_or_next_trading_day()
    target_weekday = target_date.weekday()
    target_str = target_date.strftime('%Y-%m-%d')
    target_weekday_name = WEEKDAY_NAMES[target_weekday]

    # レジーム転換中かどうか
    is_regime_change_period = datetime.now() >= REGIME_CHANGE_DATE.to_pydatetime()

    # 統計計算
    total_count = len(df)

    # HTML生成開始
    html_parts = ['''<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Grok銘柄 利益戦略分析</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; padding: 24px; line-height: 1.5; }
.container { max-width: 1400px; margin: 0 auto; }
h1 { font-size: 24px; font-weight: 600; margin-bottom: 8px; color: #e6edf3; }
h2 { font-size: 18px; font-weight: 600; margin: 32px 0 12px; color: #e6edf3; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
h3 { font-size: 15px; font-weight: 600; margin: 24px 0 12px; color: #e6edf3; }
.subtitle { color: #7d8590; font-size: 12px; margin-bottom: 24px; }

.strategy-box { background: linear-gradient(135deg, #1a3a2a 0%, #0d1117 100%); border: 2px solid #238636; border-radius: 12px; padding: 24px; margin-bottom: 24px; }
.strategy-box.caution { background: linear-gradient(135deg, #3d2a1a 0%, #0d1117 100%); border-color: #d29922; }
.strategy-title { font-size: 20px; font-weight: 700; color: #3fb950; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
.strategy-title.caution { color: #d29922; }
.strategy-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }
.strategy-item { background: #21262d; border-radius: 8px; padding: 16px; }
.strategy-label { font-size: 12px; color: #7d8590; margin-bottom: 4px; }
.strategy-value { font-size: 18px; font-weight: 600; color: #e6edf3; }
.strategy-value.action-short { color: #f85149; }
.strategy-value.action-long { color: #3fb950; }
.strategy-rule { margin-top: 16px; padding: 12px; background: #161b22; border-radius: 6px; font-size: 13px; }
.strategy-rule code { background: #30363d; padding: 2px 6px; border-radius: 4px; font-family: monospace; }

.summary { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 24px; }
.summary-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
.summary-item { text-align: center; padding: 12px; background: #21262d; border-radius: 6px; }
.summary-value { font-size: 20px; font-weight: 600; color: #e6edf3; }
.summary-value.pos { color: #3fb950; }
.summary-value.neg { color: #f85149; }
.summary-label { font-size: 11px; color: #7d8590; margin-top: 4px; }

table { width: 100%; border-collapse: collapse; font-size: 13px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; margin-bottom: 8px; }
th { text-align: left; padding: 10px 12px; background: #21262d; border-bottom: 1px solid #30363d; color: #7d8590; font-weight: 500; font-size: 12px; }
th.r { text-align: right; }
td { padding: 10px 12px; border-bottom: 1px solid #21262d; color: #e6edf3; }
td.r { text-align: right; font-variant-numeric: tabular-nums; }
tr:hover { background: #1c2128; }

.pos { color: #3fb950; }
.neg { color: #f85149; }
.win { color: #3fb950; font-size: 16px; }
.lose { color: #f85149; font-size: 16px; }

.action-short { background: #3d1f1f !important; }
.action-long { background: #1a3a2a !important; }
.action-skip { opacity: 0.5; }

.chip { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 11px; margin: 2px; font-weight: 500; background: #21262d; color: #7d8590; border: 1px solid #30363d; }
.chip-short { background: #da3633; color: #fff; border-color: #da3633; }
.chip-long { background: #238636; color: #fff; border-color: #238636; }
.chip-high { background: #238636; color: #fff; }
.chip-medium { background: #d29922; color: #fff; }

.ticker { font-weight: 600; color: #e6edf3; }
.stock-name { font-size: 11px; color: #7d8590; }
.generated { font-size: 11px; color: #7d8590; margin-top: 24px; padding-top: 16px; border-top: 1px solid #30363d; }
</style>
</head><body>
<div class="container">
<h1>💰 Grok銘柄 利益戦略分析</h1>
<p class="subtitle">バックテスト結果に基づく利益が出る戦略</p>
''']

    # 明日の方針セクション
    if tomorrow_weekday == 4:  # 金曜日
        html_parts.append(f'''
<div class="strategy-box">
<div class="strategy-title">📅 明日の方針 ({tomorrow_str} {tomorrow_weekday_name}曜日)</div>
<div class="strategy-grid">
<div class="strategy-item">
    <div class="strategy-label">戦略①</div>
    <div class="strategy-value action-short">ショート</div>
    <div class="strategy-rule">
        <strong>いちにち信用</strong><br>
        条件: <code>ATR ≥ 7%</code> かつ <code>RSI &lt; 70</code><br>
        実績: <span class="pos">+204,400円</span> (勝率72%, 32件)
    </div>
</div>
<div class="strategy-item">
    <div class="strategy-label">戦略②</div>
    <div class="strategy-value action-long">ロング</div>
    <div class="strategy-rule">
        <strong>制度信用</strong><br>
        条件: <code>RSI ≥ 70</code><br>
        実績: <span class="pos">+19,800円</span> (勝率75%, 8件)
    </div>
</div>
</div>
<div class="strategy-rule" style="margin-top: 16px; background: #da3633; color: #fff;">
    ⚠️ 注意: <strong>RSI ≥ 70 の銘柄はショート禁止！</strong> 金曜RSI70+ショートは大負け (-324,150円)
</div>
</div>
''')
    else:
        html_parts.append(f'''
<div class="strategy-box">
<div class="strategy-title">📅 明日の方針 ({tomorrow_str} {tomorrow_weekday_name}曜日)</div>
<div class="strategy-grid">
<div class="strategy-item">
    <div class="strategy-label">推奨戦略</div>
    <div class="strategy-value action-short">ショート</div>
    <div class="strategy-rule">
        <strong>いちにち信用</strong><br>
        条件: <code>ATR ≥ 7%</code> かつ <code>RSI &lt; 70</code>
    </div>
</div>
</div>
</div>
''')

    # 曜日別サマリー
    html_parts.append('''
<h2>📊 曜日別戦略パフォーマンス</h2>
<div class="summary">
<div class="summary-grid">
''')

    for weekday in range(5):
        weekday_name = WEEKDAY_NAMES[weekday]
        if weekday in strategy_analysis:
            data = strategy_analysis[weekday]
            total_profit = sum(s['profit'] for s in data['strategies'])
            profit_class = 'pos' if total_profit >= 0 else 'neg'
            html_parts.append(f'''
<div class="summary-item">
    <div class="summary-value {profit_class}">{total_profit:+,.0f}円</div>
    <div class="summary-label">{weekday_name}曜日 ({data['total']}件)</div>
</div>
''')
        else:
            html_parts.append(f'''
<div class="summary-item">
    <div class="summary-value">-</div>
    <div class="summary-label">{weekday_name}曜日</div>
</div>
''')

    html_parts.append('''
</div>
</div>
''')

    # 日別テーブル
    for date_val, group in df.groupby('backtest_date', sort=False):
        # 日付フォーマット
        if isinstance(date_val, str):
            date = date_val[:10]
        else:
            date = pd.Timestamp(date_val).strftime('%Y-%m-%d')

        weekday_idx = group['weekday'].iloc[0] if 'weekday' in group.columns else -1
        if pd.isna(weekday_idx):
            weekday_idx = -1
        weekday_str = WEEKDAY_NAMES[int(weekday_idx)] if 0 <= weekday_idx <= 6 else '?'

        # 戦略別の損益を計算
        group_copy = group.copy()
        strategy_results = []
        for _, row in group_copy.iterrows():
            rec = get_strategy_recommendation(row)
            if rec['action'] == 'SHORT':
                profit = (row['buy_price'] - row['daily_close']) * 100
            elif rec['action'] == 'LONG':
                profit = (row['daily_close'] - row['buy_price']) * 100
            else:
                profit = 0
            strategy_results.append({'action': rec['action'], 'profit': profit})

        daily_profit = sum(r['profit'] for r in strategy_results if r['action'] != 'SKIP')
        daily_count = sum(1 for r in strategy_results if r['action'] != 'SKIP')
        daily_wins = sum(1 for r in strategy_results if r['action'] != 'SKIP' and r['profit'] > 0)
        daily_win_rate = (daily_wins / daily_count * 100) if daily_count > 0 else 0

        daily_profit_class = 'pos' if daily_profit >= 0 else 'neg'
        daily_winrate_class = 'pos' if daily_win_rate >= 50 else 'neg'

        html_parts.append(f'''
<h3>{date} ({weekday_str}) - {daily_count}/{len(group)}銘柄エントリー | <span class="{daily_profit_class}">{daily_profit:+,.0f}円</span> | <span class="{daily_winrate_class}">勝率{daily_win_rate:.0f}%</span></h3>
<table>
<tr>
<th>結果</th>
<th>銘柄</th>
<th>アクション</th>
<th>信用区分</th>
<th>条件</th>
<th class="r">損益</th>
</tr>
''')

        for _, row in group.iterrows():
            rec = get_strategy_recommendation(row)
            action = rec['action']
            reason = rec['reason']
            confidence = rec['confidence']
            credit_type = rec['credit_type'] or '-'

            if action == 'SHORT':
                profit = (row['buy_price'] - row['daily_close']) * 100
                action_chip = '<span class="chip chip-short">ショート</span>'
                row_class = 'action-short'
            elif action == 'LONG':
                profit = (row['daily_close'] - row['buy_price']) * 100
                action_chip = '<span class="chip chip-long">ロング</span>'
                row_class = 'action-long'
            else:
                profit = 0
                action_chip = '<span class="chip">スキップ</span>'
                row_class = 'action-skip'

            profit_class = 'pos' if profit > 0 else 'neg' if profit < 0 else ''
            profit_str = f'{profit:+,.0f}円' if action != 'SKIP' else '-'

            win_lose = '⭕' if profit > 0 else '❌' if profit < 0 else '➖'
            win_class = 'win' if profit > 0 else 'lose' if profit < 0 else ''

            confidence_chip = ''
            if confidence == 'HIGH':
                confidence_chip = '<span class="chip chip-high">HIGH</span>'
            elif confidence == 'MEDIUM':
                confidence_chip = '<span class="chip chip-medium">MED</span>'

            html_parts.append(f'''
<tr class="{row_class}">
<td class="{win_class}">{win_lose}</td>
<td><span class="ticker">{row.get('ticker', '')}</span><br><span class="stock-name">{row.get('stock_name', '')}</span></td>
<td>{action_chip} {confidence_chip}</td>
<td>{credit_type}</td>
<td>{reason}</td>
<td class="r {profit_class}">{profit_str}</td>
</tr>
''')

        html_parts.append('</table>\n')

    # フッター
    html_parts.append(f'''
<p class="generated">Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 総レコード数: {total_count}</p>
</div>
</body></html>
''')

    # 書き出し
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(''.join(html_parts), encoding='utf-8')
    print(f"✅ HTML生成完了: {output_path}")


def main():
    # データ読み込み
    df = load_archive()
    print(f"📊 読み込み完了: {len(df)}行, {len(df.columns)}列")

    # HTML生成
    output_path = Path('improvement/output/full_analysis_list.html')
    generate_html(df, output_path)

if __name__ == '__main__':
    main()
