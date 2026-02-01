#!/usr/bin/env python3
"""
grok_trending_archive.parquet から full_analysis_list.html を生成するスクリプト

使用方法:
    python scripts/generate_full_analysis_html.py

データソース:
    - ローカル: data/parquet/backtest/grok_trending_archive.parquet
    - S3: s3://stock-api-data/parquet/backtest/grok_trending_archive.parquet
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import subprocess
import sys

# 定数
WEEKDAY_NAMES = ['月', '火', '水', '木', '金', '土', '日']
PRICE_RANGES = [
    (0, 500, '～500'),
    (500, 1000, '500～1000'),
    (1000, 2000, '1000～2000'),
    (2000, 3000, '2000～3000'),
    (3000, 5000, '3000～5000'),
    (5000, float('inf'), '5000～'),
]

def get_price_range(price: float) -> str:
    """価格帯を取得"""
    if pd.isna(price):
        return '不明'
    for low, high, label in PRICE_RANGES:
        if low <= price < high:
            return label
    return '5000～'

def is_strong_short(row: pd.Series) -> bool:
    """強ショート判定（RSI70以上 or ATR7%以上 or 出来高4倍以上）"""
    rsi = row.get('rsi9', 0) or 0
    atr = row.get('atr14_pct', 0) or 0
    vol = row.get('vol_ratio', 0) or 0
    return rsi >= 70 or atr >= 7.0 or vol >= 4.0

def is_short_ng(row: pd.Series) -> bool:
    """ショート禁止判定"""
    return not row.get('shortable', True) or row.get('ng', False)

def get_badges(row: pd.Series) -> list:
    """判定バッジを取得"""
    badges = []
    rsi = row.get('rsi9', 0) or 0
    atr = row.get('atr14_pct', 0) or 0
    vol = row.get('vol_ratio', 0) or 0
    weekday = row.get('weekday', -1)

    if rsi >= 80:
        badges.append(('badge-good', f'RSI{int(rsi)}'))
    if atr >= 7.0:
        badges.append(('badge-good', f'ATR{atr:.1f}%'))
    if vol >= 4.0:
        badges.append(('badge-good', f'出来高{vol:.1f}x'))
    if 0 <= weekday <= 4:
        badges.append(('badge-warn', f'{WEEKDAY_NAMES[weekday]}曜'))

    return badges

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


def load_today() -> pd.DataFrame | None:
    """今日の銘柄を読み込み"""
    local_path = Path('data/parquet/grok_trending.parquet')
    if local_path.exists():
        return pd.read_parquet(local_path)
    return None


def is_shortable(row: pd.Series) -> bool:
    """空売り可否判定（制度信用 OR いちにち信用で株数>0）"""
    seido = row.get('shortable', False)
    day_trade = row.get('day_trade', False)
    day_shares = row.get('day_trade_available_shares', 0) or 0
    return seido or (day_trade and day_shares > 0)


def calc_vol_ratio(ticker: str, period: int = 10) -> float:
    """yfinanceから出来高倍率を計算"""
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period='30d')
        if len(hist) >= period + 1:
            today_vol = hist['Volume'].iloc[-1]
            avg_vol = hist['Volume'].iloc[-(period+1):-1].mean()
            return today_vol / avg_vol if avg_vol > 0 else 0
    except:
        pass
    return 0


def judge_entry(row: pd.Series, vol_ratio_10d: float = 0, vol_ratio_5d: float = 0) -> tuple[str, int, list[str], float]:
    """エントリー判定（金曜日基準: ATR<7%→LONG, ATR≥7%→SHORT）"""
    rsi = row.get('rsi9', 0) or 0
    atr = row.get('atr14_pct', 0) or 0
    can_short = is_shortable(row)
    day_shares = int(row.get('day_trade_available_shares', 0) or 0)

    reasons = []

    if atr < 7:
        # LONG条件: 10日平均で出来高判定
        vol_ratio = vol_ratio_10d
        reasons.append(f'ATR{atr:.1f}%')
        reasons.append(f'出来高{vol_ratio:.1f}x(10日)')

        if vol_ratio >= 2 and rsi < 95:
            action = 'LONG'
        elif rsi >= 95:
            action = '様子見'
            reasons.append(f'RSI{rsi:.0f}過熱')
        elif vol_ratio >= 1.5:
            action = '検討'
        else:
            action = '様子見'
    else:
        # SHORT条件: 5日平均で出来高判定
        vol_ratio = vol_ratio_5d
        reasons.append(f'ATR{atr:.1f}%≥7')
        reasons.append(f'出来高{vol_ratio:.1f}x(5日)')

        if vol_ratio >= 2 and can_short:
            action = 'SHORT'
            reasons.append(f'株数{day_shares:,}')
        elif vol_ratio >= 2 and not can_short:
            action = '様子見'
            reasons.append('空売不可')
        else:
            action = '様子見'

    return action, 0, reasons, vol_ratio


def generate_today_section(df_today: pd.DataFrame) -> str:
    """今日の判定セクションHTML生成"""
    if df_today is None or len(df_today) == 0:
        return ''

    # 出来高倍率をyfinanceから取得
    vol_cache = {}
    for _, row in df_today.iterrows():
        ticker = row['ticker']
        vol_cache[ticker] = {
            '5d': calc_vol_ratio(ticker, 5),
            '10d': calc_vol_ratio(ticker, 10),
        }

    results = []
    for _, row in df_today.iterrows():
        ticker = row['ticker']
        vol_10d = vol_cache.get(ticker, {}).get('10d', 0)
        vol_5d = vol_cache.get(ticker, {}).get('5d', 0)

        action, score, reasons, vol_ratio = judge_entry(row, vol_10d, vol_5d)
        results.append({
            'ticker': ticker.replace('.T', ''),
            'name': str(row.get('stock_name', ''))[:12],
            'rsi': row.get('rsi9', 0) or 0,
            'atr': row.get('atr14_pct', 0) or 0,
            'vol': vol_ratio,
            'can_short': '○' if is_shortable(row) else '×',
            'day_shares': int(row.get('day_trade_available_shares', 0) or 0),
            'action': action,
            'score': score,
            'reasons': ' / '.join(reasons),
        })

    # ソート: LONG/SHORT → 検討 → 様子見
    order = {'LONG': 0, 'SHORT': 0, '検討': 1, '様子見': 2}
    results.sort(key=lambda x: (order.get(x['action'], 9), x['action'] != 'LONG'))

    long_count = sum(1 for r in results if r['action'] == 'LONG')
    short_count = sum(1 for r in results if r['action'] == 'SHORT')
    watch_count = sum(1 for r in results if r['action'] == '様子見')

    html = f'''
<div class="card">
<div class="card-header">
    <span class="card-title">今日の判定（{len(results)}銘柄）</span>
    <div class="card-stats">
        <span class="pos">LONG {long_count}</span>
        <span class="neg">SHORT {short_count}</span>
        <span style="color:#a1a7b4">様子見 {watch_count}</span>
    </div>
</div>
<p style="font-size:12px; color:#f5a623; margin-bottom:8px; font-weight:600;">
⚠️ 寄付エントリー禁止！前場の動きを見て11:30頃にエントリー（寄付ロングは-86,910円、11:30エントリーは+164,160円）
</p>
<p style="font-size:12px; color:#a1a7b4; margin-bottom:12px;">
ATR&lt;7%→LONG(10日平均出来高2x以上, 勝率61.5%) / ATR≥7%→SHORT(5日平均出来高2x以上, 勝率71.4%)
</p>
<table>
<tr>
<th>銘柄</th>
<th class="r">RSI9</th>
<th class="r">ATR%</th>
<th class="r">出来高</th>
<th>空売</th>
<th class="r">株数</th>
<th>判定</th>
<th>理由</th>
</tr>
'''

    for r in results:
        action_class = 'pos' if r['action'] == 'LONG' else 'neg' if r['action'] == 'SHORT' else ''
        rsi_class = 'neg' if r['rsi'] >= 95 else ''
        atr_class = 'neg' if r['atr'] >= 7 else 'pos'
        vol_class = 'pos' if r['vol'] >= 2 else ''
        vol_str = f"{r['vol']:.1f}x" if r['vol'] > 0 else '-'
        shares_str = f"{r['day_shares']:,}" if r['day_shares'] > 0 else '-'

        html += f'''
<tr>
<td><span style="font-weight:600">{r['ticker']}</span> <span style="color:#8b949e">{r['name']}</span></td>
<td class="r {rsi_class}">{r['rsi']:.0f}</td>
<td class="r {atr_class}">{r['atr']:.1f}%</td>
<td class="r {vol_class}">{vol_str}</td>
<td>{r['can_short']}</td>
<td class="r">{shares_str}</td>
<td class="{action_class}" style="font-weight:600">{r['action']}</td>
<td style="font-size:11px; color:#a1a7b4">{r['reasons']}</td>
</tr>
'''

    html += '</table>\n</div>\n'
    return html

def generate_html(df: pd.DataFrame, output_path: Path, df_today: pd.DataFrame = None) -> None:
    """HTMLを生成"""

    # 日付でソート（新しい順）
    df = df.sort_values('selection_date', ascending=False).copy()

    # 統計計算
    total_count = len(df)
    strong_short_mask = df.apply(is_strong_short, axis=1)
    short_ng_mask = df.apply(is_short_ng, axis=1)
    strong_short_count = strong_short_mask.sum()
    short_ng_count = short_ng_mask.sum()

    # ショート損益（寄付エントリー、大引け精算）
    df['short_profit'] = (df['buy_price'] - df['daily_close']).fillna(0) * 100

    # 強ショートの勝率と累計損益
    strong_short_df = df[strong_short_mask]
    if len(strong_short_df) > 0:
        strong_short_win_rate = (strong_short_df['short_profit'] > 0).mean() * 100
        strong_short_total_profit = strong_short_df['short_profit'].sum()
    else:
        strong_short_win_rate = 0
        strong_short_total_profit = 0

    # HTML生成開始
    html_parts = ['''<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>Grok銘柄分析リスト</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: "Helvetica Neue", "SF Pro Text", "Inter", "Roboto", ui-sans-serif, "Yu Gothic", "Hiragino Kaku Gothic ProN", system-ui, sans-serif;
  background: #131722;
  color: #e4e6eb;
  padding: 16px;
  line-height: 1.8;
  letter-spacing: 0.02em;
  font-feature-settings: "tnum" 1, "lnum" 1;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1400px; margin: 0 auto; }
h1 { font-size: 20px; font-weight: 700; margin-bottom: 4px; }
.subtitle { color: #a1a7b4; font-size: 13px; margin-bottom: 20px; }
.summary { background: #1e222d; border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; padding: 20px; margin-bottom: 20px; }
.summary-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
.summary-item { text-align: center; padding: 16px 12px; background: #2a2e39; border-radius: 8px; }
.summary-value { font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; }
.summary-label { font-size: 12px; color: #a1a7b4; margin-top: 4px; }
.card { background: #1e222d; border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; padding: 16px; margin-bottom: 12px; }
.card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid rgba(255,255,255,0.1); }
.card-title { font-size: 15px; font-weight: 600; }
.card-stats { display: flex; gap: 20px; font-size: 14px; font-weight: 600; font-variant-numeric: tabular-nums; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 8px 12px; color: #787d8a; font-weight: 500; font-size: 12px; border-bottom: 1px solid rgba(255,255,255,0.1); }
th.r { text-align: right; }
td { padding: 8px 12px; border-bottom: 1px solid rgba(255,255,255,0.05); }
td.r { text-align: right; font-variant-numeric: tabular-nums; }
tr:hover { background: rgba(255,255,255,0.03); }
.pos { color: #2ecc94; }
.neg { color: #f06563; }
.strong-short { background: rgba(46,204,148,0.1) !important; }
.short-ng { background: rgba(240,101,99,0.1) !important; opacity: 0.5; }
.badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 600; }
.badge-good { background: rgba(46,204,148,0.2); color: #2ecc94; }
.badge-warn { background: rgba(245,166,35,0.2); color: #f5a623; }
.generated { font-size: 12px; color: #787d8a; margin-top: 20px; text-align: center; }
</style>
</head><body>
<div class="container">
<h1>Grok銘柄分析リスト</h1>
<p class="subtitle">RSI9 | ⚠️寄付エントリー禁止 → 前場引け(11:30)で判断してエントリー</p>
''']

    # サマリー
    profit_class = 'pos' if strong_short_total_profit >= 0 else 'neg'
    winrate_class = 'pos' if strong_short_win_rate >= 50 else 'neg'
    html_parts.append(f'''
<div class="summary">
<div class="summary-grid">
<div class="summary-item">
    <div class="summary-value">{total_count}</div>
    <div class="summary-label">総件数</div>
</div>
<div class="summary-item">
    <div class="summary-value pos">{strong_short_count}</div>
    <div class="summary-label">強ショート</div>
</div>
<div class="summary-item">
    <div class="summary-value neg">{short_ng_count}</div>
    <div class="summary-label">ショート禁止</div>
</div>
<div class="summary-item">
    <div class="summary-value {winrate_class}">{strong_short_win_rate:.0f}%</div>
    <div class="summary-label">強ショート勝率</div>
</div>
<div class="summary-item">
    <div class="summary-value {profit_class}">{strong_short_total_profit:+,.0f}円</div>
    <div class="summary-label">累計損益</div>
</div>
</div>
</div>
''')

    # 今日の判定セクション
    if df_today is not None and len(df_today) > 0:
        html_parts.append(generate_today_section(df_today))

    # 日別テーブル
    for date, group in df.groupby('selection_date', sort=False):
        # 日付を文字列に変換（00:00:00を除去）
        date_str = str(date)[:10] if len(str(date)) > 10 else str(date)
        weekday_idx = group['weekday'].iloc[0] if 'weekday' in group.columns else -1
        weekday_str = WEEKDAY_NAMES[int(weekday_idx)] if 0 <= weekday_idx <= 6 else '?'

        daily_short_profit = group['short_profit'].sum()
        daily_win_rate = (group['short_profit'] > 0).mean() * 100 if len(group) > 0 else 0
        daily_profit_class = 'pos' if daily_short_profit >= 0 else 'neg'
        daily_winrate_class = 'pos' if daily_win_rate >= 50 else 'neg'

        html_parts.append(f'''
<div class="card">
<div class="card-header">
    <span class="card-title">{date_str} ({weekday_str}) - {len(group)}件</span>
    <div class="card-stats">
        <span class="{daily_profit_class}">{daily_short_profit:+,.0f}円</span>
        <span class="{daily_winrate_class}">勝率{daily_win_rate:.0f}%</span>
    </div>
</div>
<table>
<tr>
<th>銘柄</th>
<th>価格帯</th>
<th class="r">RSI9</th>
<th class="r">ATR%</th>
<th class="r">出来高</th>
<th class="r">先物</th>
<th class="r">損益</th>
<th>判定</th>
</tr>
''')

        for _, row in group.iterrows():
            # 行クラス
            row_class = ''
            if is_short_ng(row):
                row_class = 'short-ng'
            elif is_strong_short(row):
                row_class = 'strong-short'

            # 価格帯
            price_range = get_price_range(row.get('prev_close', 0))

            # RSI, ATR, vol_ratio
            rsi = row.get('rsi9', 0)
            rsi_str = f'{rsi:.0f}' if pd.notna(rsi) else '-'
            rsi_class = 'pos' if pd.notna(rsi) and rsi >= 70 else ''

            atr = row.get('atr14_pct', 0)
            atr_str = f'{atr:.1f}%' if pd.notna(atr) else '-'
            atr_class = 'pos' if pd.notna(atr) and atr >= 7.0 else ''

            vol = row.get('vol_ratio', 0)
            vol_str = f'{vol:.1f}x' if pd.notna(vol) else '-'
            vol_class = 'pos' if pd.notna(vol) and vol >= 4.0 else ''

            # 先物変動
            futures = row.get('futures_change_pct', 0) or 0
            futures_class = 'neg' if futures > 0 else 'pos' if futures < 0 else ''
            futures_str = f'{futures:+.1f}%'

            # ショート損益
            short_profit = row.get('short_profit', 0)
            profit_class = 'pos' if short_profit > 0 else 'neg' if short_profit < 0 else ''
            profit_str = f'{short_profit:+,.0f}円'

            # バッジ
            badges = get_badges(row)
            badges_html = ' '.join([f'<span class="badge {cls}">{text}</span>' for cls, text in badges])

            ticker = row.get('ticker', '').replace('.T', '')
            stock_name = row.get('stock_name', '')
            html_parts.append(f'''
<tr class="{row_class}">
<td><span style="font-weight:600">{ticker}</span> <span style="color:#8b949e">{stock_name}</span></td>
<td>{price_range}</td>
<td class="r {rsi_class}">{rsi_str}</td>
<td class="r {atr_class}">{atr_str}</td>
<td class="r {vol_class}">{vol_str}</td>
<td class="r {futures_class}">{futures_str}</td>
<td class="r {profit_class}">{profit_str}</td>
<td>{badges_html}</td>
</tr>
''')

        html_parts.append('</table>\n</div>\n')

    # フッター
    html_parts.append(f'''
<p class="generated">Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | RSI9 | grok_trending_archive.parquet</p>
</div>
</body></html>
''')

    # 書き出し
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(''.join(html_parts), encoding='utf-8')
    print(f"✅ HTML生成完了: {output_path}")
    print(f"   総銘柄数: {total_count}")
    print(f"   強ショート: {strong_short_count} ({strong_short_win_rate:.0f}%)")
    print(f"   累計損益: {strong_short_total_profit:+,.0f}円")

def main():
    # データ読み込み
    df = load_archive()
    print(f"📊 アーカイブ読み込み完了: {len(df)}行, {len(df.columns)}列")

    # 今日の銘柄を読み込み
    df_today = load_today()
    if df_today is not None:
        print(f"📊 今日の銘柄読み込み完了: {len(df_today)}行")
    else:
        print("⚠️ 今日の銘柄なし")

    # HTML生成
    output_path = Path('improvement/output/full_analysis_list.html')
    generate_html(df, output_path, df_today)

if __name__ == '__main__':
    main()

