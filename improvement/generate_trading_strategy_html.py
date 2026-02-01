"""
取引戦略分析HTML生成スクリプト

grok_trending_archive.parquet から取引戦略HTMLを生成

【戦略ルール（バックテスト結果に基づく）】
    【金曜日】
    - いちにち信用 × ATR7%+ × RSI9<70 → ショート (+204,400円, 勝率72%)
    - 制度信用 × RSI9≥70 → ロング (+19,800円, 勝率75%)

    【エントリータイミング】
    - エントリー: 9:00 寄り付き
    - 決済: 15:00 大引け

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

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

WEEKDAY_NAMES = ['月', '火', '水', '木', '金', '土', '日']
REGIME_CHANGE_DATE = pd.Timestamp('2026-01-13')


def is_trading_day(date: datetime) -> bool:
    """営業日かどうかを判定（土日を除く）"""
    if date.weekday() >= 5:
        return False
    return True


def get_target_trading_day() -> tuple:
    """
    取引対象日を取得
    - 今日が営業日かつ15:00前 → 今日
    - それ以外 → 次の営業日
    """
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if is_trading_day(today) and now.hour < 15:
        return (today, '今日')
    else:
        next_day = today + timedelta(days=1)
        while not is_trading_day(next_day):
            next_day = next_day + timedelta(days=1)
        return (next_day, '次の営業日')


def load_archive() -> pd.DataFrame:
    """アーカイブを読み込み"""
    path = DATA_DIR / "backtest" / "grok_trending_archive.parquet"
    if path.exists():
        return pd.read_parquet(path)
    raise FileNotFoundError(f"アーカイブが見つかりません: {path}")


def get_strategy_recommendation(row: pd.Series) -> dict:
    """利益が出る戦略を判定"""
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

    if ng:
        return {'action': 'SKIP', 'reason': 'NG銘柄', 'confidence': None, 'credit_type': None}

    # 金曜日の戦略
    if weekday == 4:
        if day_trade and atr >= 7.0 and rsi < 70:
            return {
                'action': 'SHORT',
                'reason': f'ATR{atr:.1f}%≥7 & RSI9={int(rsi)}<70',
                'confidence': 'HIGH',
                'credit_type': 'いちにち'
            }
        if margin_code == '2' and is_shortable and rsi >= 70:
            return {
                'action': 'LONG',
                'reason': f'RSI9={int(rsi)}≥70',
                'confidence': 'HIGH',
                'credit_type': '制度'
            }
        if rsi >= 70:
            return {
                'action': 'SKIP',
                'reason': f'RSI9={int(rsi)}≥70 ショート禁止',
                'confidence': None,
                'credit_type': None
            }

    # 水曜日はショート不利
    if weekday == 2:
        return {'action': 'SKIP', 'reason': '水曜ショート不利', 'confidence': None, 'credit_type': None}

    # デフォルト: ATR7%+でショート
    if day_trade and atr >= 7.0 and rsi < 70:
        return {
            'action': 'SHORT',
            'reason': f'ATR{atr:.1f}%≥7 & RSI9={int(rsi)}<70',
            'confidence': 'MEDIUM',
            'credit_type': 'いちにち'
        }

    return {'action': 'SKIP', 'reason': '条件未達', 'confidence': None, 'credit_type': None}


def analyze_strategies(df: pd.DataFrame) -> dict:
    """曜日別・戦略別の損益を分析"""
    df = df.copy()
    df['short_profit'] = (df['buy_price'] - df['daily_close']) * 100
    df['long_profit'] = (df['daily_close'] - df['buy_price']) * 100

    if 'weekday' not in df.columns:
        df['weekday'] = pd.to_datetime(df['backtest_date']).dt.dayofweek

    results = {}

    for weekday in range(5):
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
                'name': 'いちにち×ATR7%+×RSI9<70→ショート',
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
                'name': '制度×RSI9≥70→ロング',
                'profit': profit,
                'win_rate': win_rate,
                'count': len(cond2)
            })

    return results


def analyze_recent_performance(df: pd.DataFrame) -> dict:
    """直近2週間のパフォーマンスを分析"""
    df = df.copy()
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])
    df['short_profit'] = (df['buy_price'] - df['daily_close']) * 100

    # 直近2週間
    cutoff = df['backtest_date'].max() - timedelta(days=14)
    recent = df[df['backtest_date'] >= cutoff]
    older = df[df['backtest_date'] < cutoff]

    if 'weekday' not in df.columns:
        df['weekday'] = df['backtest_date'].dt.dayofweek
        recent['weekday'] = recent['backtest_date'].dt.dayofweek
        older['weekday'] = older['backtest_date'].dt.dayofweek

    # 火曜日の比較
    recent_tue = recent[recent['weekday'] == 1]
    older_tue = older[older['weekday'] == 1]

    return {
        'recent_tue_count': len(recent_tue),
        'recent_tue_win_rate': (recent_tue['short_profit'] > 0).mean() * 100 if len(recent_tue) > 0 else 0,
        'recent_tue_profit': recent_tue['short_profit'].sum() if len(recent_tue) > 0 else 0,
        'older_tue_count': len(older_tue),
        'older_tue_win_rate': (older_tue['short_profit'] > 0).mean() * 100 if len(older_tue) > 0 else 0,
        'older_tue_profit': older_tue['short_profit'].sum() if len(older_tue) > 0 else 0,
        'recent_total_profit': recent['short_profit'].sum(),
        'older_total_profit': older['short_profit'].sum(),
    }


def generate_html(df: pd.DataFrame, output_path: Path) -> None:
    """HTMLを生成"""

    if 'weekday' not in df.columns:
        df['weekday'] = pd.to_datetime(df['backtest_date']).dt.dayofweek

    df = df.sort_values('backtest_date', ascending=False).copy()
    df['short_profit'] = (df['buy_price'] - df['daily_close']).fillna(0) * 100
    df['long_profit'] = (df['daily_close'] - df['buy_price']).fillna(0) * 100

    strategy_analysis = analyze_strategies(df)
    recent_perf = analyze_recent_performance(df)

    target_date, target_label = get_target_trading_day()
    target_weekday = target_date.weekday()
    target_str = target_date.strftime('%Y-%m-%d')
    target_weekday_name = WEEKDAY_NAMES[target_weekday]

    is_regime_change = datetime.now() >= REGIME_CHANGE_DATE.to_pydatetime()

    total_count = len(df)

    html_parts = ['''<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Grok銘柄 取引戦略分析</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; padding: 24px; line-height: 1.5; }
.container { max-width: 1400px; margin: 0 auto; }
h1 { font-size: 24px; font-weight: 600; margin-bottom: 8px; color: #e6edf3; }
h2 { font-size: 18px; font-weight: 600; margin: 32px 0 12px; color: #e6edf3; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
h3 { font-size: 15px; font-weight: 600; margin: 24px 0 12px; color: #e6edf3; }
.subtitle { color: #7d8590; font-size: 12px; margin-bottom: 24px; }

.alert-box { background: linear-gradient(135deg, #5c2d2d 0%, #0d1117 100%); border: 2px solid #f85149; border-radius: 12px; padding: 24px; margin-bottom: 24px; }
.alert-title { font-size: 20px; font-weight: 700; color: #f85149; margin-bottom: 16px; }

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
<h1>💰 Grok銘柄 取引戦略分析</h1>
<p class="subtitle">バックテスト結果に基づく利益戦略 | RSI9（楽天MarketSpeed準拠）</p>
''']

    # レジーム転換警告
    if is_regime_change:
        html_parts.append(f'''
<div class="alert-box">
<div class="alert-title">🚨 レジーム転換警告 (2026-01-13以降)</div>
<div class="strategy-rule" style="font-size: 14px; background: #3d1f1f;">
    <strong>⚠️ ショート優位 → ロング優位 に転換の兆候</strong><br><br>
    <table style="width: auto; background: transparent; border: none;">
        <tr><td style="border: none; padding: 4px 16px 4px 0;">火曜ショート（以前）:</td><td style="border: none;" class="pos">勝率{recent_perf['older_tue_win_rate']:.0f}%, {recent_perf['older_tue_profit']:+,.0f}円 ({recent_perf['older_tue_count']}件)</td></tr>
        <tr><td style="border: none; padding: 4px 16px 4px 0;">火曜ショート（直近2週間）:</td><td style="border: none;" class="neg">勝率{recent_perf['recent_tue_win_rate']:.0f}%, {recent_perf['recent_tue_profit']:+,.0f}円 ({recent_perf['recent_tue_count']}件)</td></tr>
    </table>
    <br>
    <strong>原因候補:</strong>
    <ul style="margin-left: 20px; line-height: 1.8;">
        <li>選挙ラリー開始（1/23解散に向けた先回り買い）</li>
        <li>TSMC好決算など外部材料</li>
        <li>市場センチメントの変化</li>
    </ul>
    <br>
    <div style="background: #d29922; color: #000; padding: 12px; border-radius: 6px; font-weight: 700;">
        📊 推奨: データ収集に徹する（ルール崩壊期）<br>
        従来戦略での積極的なエントリーは控え、新しいパターンを観察
    </div>
</div>
</div>
''')

    # 取引方針セクション
    if target_weekday == 4:  # 金曜日
        html_parts.append(f'''
<div class="strategy-box">
<div class="strategy-title">📅 {target_label}の方針 ({target_str} {target_weekday_name}曜日)</div>
<div class="strategy-grid">
<div class="strategy-item">
    <div class="strategy-label">戦略①</div>
    <div class="strategy-value action-short">ショート</div>
    <div class="strategy-rule">
        <strong>いちにち信用</strong><br>
        条件: <code>ATR ≥ 7%</code> かつ <code>RSI9 &lt; 70</code><br>
        実績: <span class="pos">+204,400円</span> (勝率72%, 32件)
    </div>
</div>
<div class="strategy-item">
    <div class="strategy-label">戦略②</div>
    <div class="strategy-value action-long">ロング</div>
    <div class="strategy-rule">
        <strong>制度信用</strong><br>
        条件: <code>RSI9 ≥ 70</code><br>
        実績: <span class="pos">+19,800円</span> (勝率75%, 8件)
    </div>
</div>
</div>
<div class="strategy-rule" style="margin-top: 16px; background: #21262d;">
    <strong>📍 エントリータイミング</strong><br>
    ・エントリー: <code>9:00 寄り付き</code><br>
    ・決済: <code>15:00 大引け</code>
</div>
<div class="strategy-rule" style="margin-top: 12px; background: #da3633; color: #fff;">
    ⚠️ <strong>RSI9 ≥ 70 の銘柄はショート禁止！</strong> 金曜RSI70+ショートは大負け (-324,150円)
</div>
</div>
''')
    elif target_weekday == 2:  # 水曜日
        html_parts.append(f'''
<div class="strategy-box caution">
<div class="strategy-title caution">📅 {target_label}の方針 ({target_str} {target_weekday_name}曜日)</div>
<div class="strategy-rule" style="font-size: 16px;">
    ⚠️ <strong>水曜日はショート不利</strong><br>
    エントリーを控えるか、ロング戦略を検討してください。
</div>
<div class="strategy-rule" style="margin-top: 16px; background: #21262d;">
    <strong>📍 エントリータイミング（エントリーする場合）</strong><br>
    ・エントリー: <code>9:00 寄り付き</code><br>
    ・決済: <code>15:00 大引け</code>
</div>
</div>
''')
    elif target_weekday == 1:  # 火曜日
        html_parts.append(f'''
<div class="strategy-box caution">
<div class="strategy-title caution">📅 {target_label}の方針 ({target_str} {target_weekday_name}曜日)</div>
<div class="strategy-rule" style="font-size: 14px; background: #3d2a1a;">
    ⚠️ <strong>火曜ショートは現在崩壊中</strong><br>
    <ul style="margin-left: 20px; line-height: 1.8;">
        <li>以前: 勝率{recent_perf['older_tue_win_rate']:.0f}%, {recent_perf['older_tue_profit']:+,.0f}円</li>
        <li>直近2週間: 勝率{recent_perf['recent_tue_win_rate']:.0f}%, {recent_perf['recent_tue_profit']:+,.0f}円</li>
    </ul>
    レジーム転換が確認されるまでエントリーは慎重に。
</div>
<div class="strategy-rule" style="margin-top: 16px; background: #21262d;">
    <strong>📍 エントリータイミング（エントリーする場合）</strong><br>
    ・エントリー: <code>9:00 寄り付き</code><br>
    ・決済: <code>15:00 大引け</code>
</div>
</div>
''')
    else:
        html_parts.append(f'''
<div class="strategy-box">
<div class="strategy-title">📅 {target_label}の方針 ({target_str} {target_weekday_name}曜日)</div>
<div class="strategy-grid">
<div class="strategy-item">
    <div class="strategy-label">推奨戦略</div>
    <div class="strategy-value action-short">ショート</div>
    <div class="strategy-rule">
        <strong>いちにち信用</strong><br>
        条件: <code>ATR ≥ 7%</code> かつ <code>RSI9 &lt; 70</code>
    </div>
</div>
</div>
<div class="strategy-rule" style="margin-top: 16px; background: #21262d;">
    <strong>📍 エントリータイミング</strong><br>
    ・エントリー: <code>9:00 寄り付き</code><br>
    ・決済: <code>15:00 大引け</code>
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
        if isinstance(date_val, str):
            date = date_val[:10]
        else:
            date = pd.Timestamp(date_val).strftime('%Y-%m-%d')

        weekday_idx = group['weekday'].iloc[0] if 'weekday' in group.columns else -1
        if pd.isna(weekday_idx):
            weekday_idx = -1
        weekday_str = WEEKDAY_NAMES[int(weekday_idx)] if 0 <= weekday_idx <= 6 else '?'

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
<th class="r">RSI9</th>
<th class="r">ATR%</th>
<th class="r">買値</th>
<th class="r">終値</th>
<th>アクション</th>
<th>信用</th>
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

            # 個別銘柄データ
            rsi9_val = row.get('rsi9', 0)
            rsi9_str = f'{rsi9_val:.1f}' if pd.notna(rsi9_val) else '-'
            rsi9_class = 'neg' if pd.notna(rsi9_val) and rsi9_val >= 70 else ''

            atr_val = row.get('atr14_pct', 0)
            atr_str = f'{atr_val:.1f}%' if pd.notna(atr_val) else '-'
            atr_class = 'pos' if pd.notna(atr_val) and atr_val >= 7 else ''

            buy_price = row.get('buy_price', 0)
            buy_str = f'{buy_price:,.0f}' if pd.notna(buy_price) else '-'

            close_price = row.get('daily_close', 0)
            close_str = f'{close_price:,.0f}' if pd.notna(close_price) else '-'

            html_parts.append(f'''
<tr class="{row_class}">
<td class="{win_class}">{win_lose}</td>
<td><span class="ticker">{row.get('ticker', '')}</span><br><span class="stock-name">{row.get('stock_name', '')}</span></td>
<td class="r {rsi9_class}">{rsi9_str}</td>
<td class="r {atr_class}">{atr_str}</td>
<td class="r">{buy_str}</td>
<td class="r">{close_str}</td>
<td>{action_chip} {confidence_chip}</td>
<td>{credit_type}</td>
<td class="r {profit_class}">{profit_str}</td>
</tr>
''')

        html_parts.append('</table>\n')

    html_parts.append(f'''
<p class="generated">Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 総レコード数: {total_count} | RSI9（楽天MarketSpeed準拠）</p>
</div>
</body></html>
''')

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(''.join(html_parts), encoding='utf-8')
    print(f"✅ HTML生成完了: {output_path}")


def main():
    print("=" * 60)
    print("取引戦略分析HTML生成")
    print("=" * 60)

    df = load_archive()
    print(f"📊 読み込み完了: {len(df)}行")

    output_path = OUTPUT_DIR / "trading_strategy_analysis.html"
    generate_html(df, output_path)

    print(f"\n📂 出力: {output_path}")


if __name__ == '__main__':
    main()
