#!/usr/bin/env python3
"""
vol_ratio × rsi9 分析（ストップ高/安フラグ付き）
"""

import pandas as pd
import sys
sys.path.insert(0, '/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly')
from scripts.lib.price_limit import calc_price_limit


def calc_stop_flags(prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    ストップ高/安フラグを計算

    Args:
        prices_df: grok_prices_max_1d.parquet (date, Open, High, Low, Close, Volume, ticker)

    Returns:
        DataFrame with (date, ticker, prev_close, prev_prev_close, is_stop_high, is_stop_low)
    """
    prices_df = prices_df.sort_values(['ticker', 'date'])

    # 前日終値と前々日終値を計算
    prices_df['prev_close'] = prices_df.groupby('ticker')['Close'].shift(1)
    prices_df['prev_prev_close'] = prices_df.groupby('ticker')['Close'].shift(2)

    # 制限値幅を計算（前々日終値ベース）
    prices_df['price_limit'] = prices_df['prev_prev_close'].apply(
        lambda x: calc_price_limit(x) if pd.notna(x) else None
    )

    # ストップ高/安判定
    # 前日終値が前々日終値 + 制限値幅 以上 → ストップ高
    # 前日終値が前々日終値 - 制限値幅 以下 → ストップ安
    prices_df['is_stop_high'] = (
        prices_df['prev_close'] >= prices_df['prev_prev_close'] + prices_df['price_limit']
    )
    prices_df['is_stop_low'] = (
        prices_df['prev_close'] <= prices_df['prev_prev_close'] - prices_df['price_limit']
    )

    return prices_df[['date', 'ticker', 'prev_close', 'prev_prev_close', 'price_limit', 'is_stop_high', 'is_stop_low']]


def main():
    # データ読み込み
    archive_path = '/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/parquet/backtest/grok_trending_archive.parquet'
    prices_path = '/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/improvement/grok_prices_max_1d.parquet'

    df_archive = pd.read_parquet(archive_path)
    df_prices = pd.read_parquet(prices_path)

    # 日付型を統一（文字列から日付部分のみ抽出）
    df_prices['date'] = df_prices['date'].astype(str).str[:10]
    df_prices['date'] = pd.to_datetime(df_prices['date'])
    df_archive['backtest_date'] = df_archive['backtest_date'].astype(str).str[:10]
    df_archive['backtest_date'] = pd.to_datetime(df_archive['backtest_date'])

    # ストップ高/安フラグ計算（各dateがストップ高/安だったか）
    df_stop = calc_stop_flags(df_prices)
    df_stop['date'] = pd.to_datetime(df_stop['date']).dt.tz_localize(None)

    # 営業日リストを取得（prices_dfのdate一覧）
    business_days = sorted(df_prices['date'].unique())

    # backtest_dateの前営業日（選定日）を計算
    def get_prev_business_day(target_date, business_days):
        """target_dateの前営業日を返す"""
        for bd in reversed(business_days):
            if bd < target_date:
                return bd
        return None

    df_archive['selection_date'] = df_archive['backtest_date'].apply(
        lambda x: get_prev_business_day(x, business_days)
    )

    # アーカイブとマージ（selection_date = date で選定日のストップ高/安を取得）
    df_merged = df_archive.merge(
        df_stop[['date', 'ticker', 'is_stop_high', 'is_stop_low']],
        left_on=['selection_date', 'ticker'],
        right_on=['date', 'ticker'],
        how='left'
    )

    # 曜日・価格帯・信用区分を追加
    WEEKDAY_NAMES = ['月曜日', '火曜日', '水曜日', '木曜日', '金曜日']
    PRICE_RANGES = [
        {'label': '~1,000円', 'min': 0, 'max': 1000},
        {'label': '1,000~3,000円', 'min': 1000, 'max': 3000},
        {'label': '3,000~5,000円', 'min': 3000, 'max': 5000},
        {'label': '5,000~10,000円', 'min': 5000, 'max': 10000},
        {'label': '10,000円~', 'min': 10000, 'max': float('inf')},
    ]

    def get_price_range(price):
        if pd.isna(price):
            return ''
        for pr in PRICE_RANGES:
            if pr['min'] <= price < pr['max']:
                return pr['label']
        return PRICE_RANGES[-1]['label']

    df_merged['weekday'] = df_merged['backtest_date'].dt.weekday
    df_merged['weekday_name'] = df_merged['weekday'].apply(lambda x: WEEKDAY_NAMES[x] if x < 5 else '')
    df_merged['price_range'] = df_merged['buy_price'].apply(get_price_range)
    df_merged['margin_type'] = df_merged.apply(
        lambda r: '制度信用' if r.get('shortable') == True else 'いちにち信用', axis=1
    )

    # 寄付ストップ高/安の判定（前日終値ベース）
    # 始値=終値=前日終値±値幅制限 → 取引不成立
    df_merged['price_limit_prev'] = df_merged['prev_close'].apply(
        lambda x: calc_price_limit(x) if pd.notna(x) else None
    )
    df_merged['is_opening_stop_high'] = (
        (df_merged['buy_price'] == df_merged['daily_close']) &
        (df_merged['buy_price'] == df_merged['prev_close'] + df_merged['price_limit_prev'])
    )
    df_merged['is_opening_stop_low'] = (
        (df_merged['buy_price'] == df_merged['daily_close']) &
        (df_merged['buy_price'] == df_merged['prev_close'] - df_merged['price_limit_prev'])
    )

    # 必要なカラムを選択（4区分の損益を含む）
    df_view = df_merged[[
        'backtest_date', 'ticker', 'stock_name',
        'prev_close', 'buy_price', 'daily_close',
        'profit_per_100_shares_morning_early',
        'profit_per_100_shares_phase1',
        'profit_per_100_shares_afternoon_early',
        'profit_per_100_shares_phase2',
        'vol_ratio', 'rsi9',
        'is_stop_high', 'is_stop_low',
        'is_opening_stop_high', 'is_opening_stop_low',
        'weekday', 'weekday_name', 'price_range', 'margin_type'
    ]].copy()

    df_view.columns = ['日付', 'ティッカー', '銘柄名', '前日終値', '始値', '終値',
                       '損益_前前L', '損益_前引L', '損益_後前L', '損益_大引L',
                       'vol_ratio', 'rsi9', 'ストップ高', 'ストップ安',
                       '寄付S高', '寄付S安',
                       '曜日番号', '曜日', '価格帯', '信用区分']

    # ショート損益（4区分）
    df_view['損益_前前S'] = -df_view['損益_前前L'].fillna(0)
    df_view['損益_前引S'] = -df_view['損益_前引L'].fillna(0)
    df_view['損益_後前S'] = -df_view['損益_後前L'].fillna(0)
    df_view['損益_大引S'] = -df_view['損益_大引L'].fillna(0)

    # 後方互換のため
    df_view['損益_ショート'] = df_view['損益_大引S']

    # 日付でソート（降順）
    df_view = df_view.sort_values(['日付', 'ティッカー'], ascending=[False, True])

    # HTML生成
    def style_vol_ratio(val):
        if pd.isna(val):
            return ''
        if val < 1:
            return 'color: #ff6b6b;'
        elif val >= 2:
            return 'color: #51cf66;'
        return ''

    def style_rsi9(val):
        if pd.isna(val):
            return ''
        if val <= 30:
            return 'color: #ff6b6b;'
        elif val >= 70:
            return 'color: #51cf66;'
        return ''

    def style_profit(val):
        if pd.isna(val):
            return ''
        if val > 0:
            return 'color: #51cf66;'
        elif val < 0:
            return 'color: #ff6b6b;'
        return ''

    def style_stop(val):
        if val == True:
            return 'color: #ffd43b;'  # 黄色
        return ''

    # 統計
    total_count = len(df_view)
    stop_high_count = df_view['ストップ高'].sum()
    stop_low_count = df_view['ストップ安'].sum()
    opening_stop_high_count = df_view['寄付S高'].sum()
    opening_stop_low_count = df_view['寄付S安'].sum()

    # 寄付ストップ高/安フラグ（取引不成立）
    df_view['取引不成立'] = df_view['寄付S高'] | df_view['寄付S安']

    # ストップ高銘柄の統計（取引不成立を除外）
    df_stop_high = df_view[df_view['ストップ高'] == True].copy()
    df_stop_high_tradable = df_stop_high[df_stop_high['取引不成立'] != True]
    stop_high_win = (df_stop_high_tradable['損益_ショート'] > 0).sum()
    stop_high_lose = (df_stop_high_tradable['損益_ショート'] < 0).sum()
    stop_high_even = (df_stop_high_tradable['損益_ショート'] == 0).sum()
    stop_high_untradable = (df_stop_high['取引不成立'] == True).sum()
    stop_high_winrate = stop_high_win / len(df_stop_high_tradable) * 100 if len(df_stop_high_tradable) > 0 else 0
    stop_high_total_pnl = df_stop_high['損益_ショート'].sum()
    stop_high_avg_pnl = df_stop_high_tradable['損益_ショート'].mean() if len(df_stop_high_tradable) > 0 else 0

    # 勝率計算用ヘルパー（取引不成立を除外）
    def calc_winrate(df):
        """取引不成立を除外した勝率を計算"""
        tradable = df[df['取引不成立'] != True]
        if len(tradable) == 0:
            return 0
        return (tradable['損益_ショート'] > 0).mean() * 100

    # 全体 vs ストップ高 の曜日別・価格帯別・信用区分別集計
    def calc_comparison_stats(df_all, df_stop_high):
        """全体とストップ高を比較する統計を計算"""
        results = []
        for wd in range(5):
            wd_name = WEEKDAY_NAMES[wd]
            wd_all = df_all[df_all['曜日番号'] == wd]
            wd_sh = df_stop_high[df_stop_high['曜日番号'] == wd]

            # 制度信用
            seido_all = wd_all[wd_all['信用区分'] == '制度信用']
            seido_sh = wd_sh[wd_sh['信用区分'] == '制度信用']
            seido_ex = seido_all[seido_all['ストップ高'] != True]  # 除ストップ高

            seido_data = {
                'weekday': wd_name,
                'margin_type': '制度信用',
                'all_count': len(seido_all),
                'all_pnl': seido_all['損益_ショート'].sum(),
                'all_winrate': calc_winrate(seido_all),
                'sh_count': len(seido_sh),
                'sh_pnl': seido_sh['損益_ショート'].sum(),
                'ex_count': len(seido_ex),
                'ex_pnl': seido_ex['損益_ショート'].sum(),
                'ex_winrate': calc_winrate(seido_ex),
                'price_ranges': []
            }
            for pr in PRICE_RANGES:
                pr_all = seido_all[seido_all['価格帯'] == pr['label']]
                pr_sh = seido_sh[seido_sh['価格帯'] == pr['label']]
                pr_ex = pr_all[pr_all['ストップ高'] != True]
                seido_data['price_ranges'].append({
                    'label': pr['label'],
                    'all_count': len(pr_all),
                    'all_pnl': pr_all['損益_ショート'].sum(),
                    'all_winrate': calc_winrate(pr_all),
                    'sh_count': len(pr_sh),
                    'sh_pnl': pr_sh['損益_ショート'].sum(),
                    'ex_count': len(pr_ex),
                    'ex_pnl': pr_ex['損益_ショート'].sum(),
                    'ex_winrate': calc_winrate(pr_ex),
                })

            # いちにち信用
            ichi_all = wd_all[wd_all['信用区分'] == 'いちにち信用']
            ichi_sh = wd_sh[wd_sh['信用区分'] == 'いちにち信用']
            ichi_ex = ichi_all[ichi_all['ストップ高'] != True]

            ichi_data = {
                'weekday': wd_name,
                'margin_type': 'いちにち信用',
                'all_count': len(ichi_all),
                'all_pnl': ichi_all['損益_ショート'].sum(),
                'all_winrate': calc_winrate(ichi_all),
                'sh_count': len(ichi_sh),
                'sh_pnl': ichi_sh['損益_ショート'].sum(),
                'ex_count': len(ichi_ex),
                'ex_pnl': ichi_ex['損益_ショート'].sum(),
                'ex_winrate': calc_winrate(ichi_ex),
                'price_ranges': []
            }
            for pr in PRICE_RANGES:
                pr_all = ichi_all[ichi_all['価格帯'] == pr['label']]
                pr_sh = ichi_sh[ichi_sh['価格帯'] == pr['label']]
                pr_ex = pr_all[pr_all['ストップ高'] != True]
                ichi_data['price_ranges'].append({
                    'label': pr['label'],
                    'all_count': len(pr_all),
                    'all_pnl': pr_all['損益_ショート'].sum(),
                    'all_winrate': calc_winrate(pr_all),
                    'sh_count': len(pr_sh),
                    'sh_pnl': pr_sh['損益_ショート'].sum(),
                    'ex_count': len(pr_ex),
                    'ex_pnl': pr_ex['損益_ショート'].sum(),
                    'ex_winrate': calc_winrate(pr_ex),
                })

            results.append({'seido': seido_data, 'ichinichi': ichi_data})
        return results

    comparison_stats = calc_comparison_stats(df_view, df_stop_high)

    # ストップ安銘柄の統計（取引不成立を除外）
    df_stop_low = df_view[df_view['ストップ安'] == True].copy()
    df_stop_low_tradable = df_stop_low[df_stop_low['取引不成立'] != True]
    stop_low_win = (df_stop_low_tradable['損益_ショート'] > 0).sum()
    stop_low_lose = (df_stop_low_tradable['損益_ショート'] < 0).sum()
    stop_low_even = (df_stop_low_tradable['損益_ショート'] == 0).sum()
    stop_low_untradable = (df_stop_low['取引不成立'] == True).sum()
    stop_low_winrate = stop_low_win / len(df_stop_low_tradable) * 100 if len(df_stop_low_tradable) > 0 else 0
    stop_low_total_pnl = df_stop_low['損益_ショート'].sum()
    stop_low_avg_pnl = df_stop_low_tradable['損益_ショート'].mean() if len(df_stop_low_tradable) > 0 else 0

    # RSI 10区切り集計（ショート4区分）- 制度/いちにち別
    rsi_bands_seido = []
    rsi_bands_ichi = []
    for rsi_min in range(0, 100, 10):
        rsi_max = rsi_min + 10
        band_df = df_view[(df_view['rsi9'] >= rsi_min) & (df_view['rsi9'] < rsi_max)]
        seido_df = band_df[band_df['信用区分'] == '制度信用']
        ichi_df = band_df[band_df['信用区分'] == 'いちにち信用']
        rsi_bands_seido.append({
            'label': f'{rsi_min}-{rsi_max}',
            'count': len(seido_df),
            's_前前': seido_df['損益_前前S'].sum(),
            's_前引': seido_df['損益_前引S'].sum(),
            's_後前': seido_df['損益_後前S'].sum(),
            's_大引': seido_df['損益_大引S'].sum(),
        })
        rsi_bands_ichi.append({
            'label': f'{rsi_min}-{rsi_max}',
            'count': len(ichi_df),
            's_前前': ichi_df['損益_前前S'].sum(),
            's_前引': ichi_df['損益_前引S'].sum(),
            's_後前': ichi_df['損益_後前S'].sum(),
            's_大引': ichi_df['損益_大引S'].sum(),
        })

    html = f'''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>vol_ratio × rsi9 分析（ストップ高/安フラグ付き）</title>
<style>
body {{
    background-color: #1a1a2e;
    color: #eee;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    padding: 20px;
}}
h1 {{
    color: #fff;
    margin-bottom: 20px;
}}
table {{
    border-collapse: collapse;
    width: 100%;
    font-size: 13px;
}}
th, td {{
    padding: 8px 12px;
    text-align: right;
    border-bottom: 1px solid #333;
}}
th {{
    background-color: #252540;
    color: #aaa;
    font-weight: 600;
}}
td:nth-child(1), td:nth-child(2), td:nth-child(3) {{
    text-align: left;
}}
tr:hover {{
    background-color: #252540;
}}
.summary {{
    margin-bottom: 20px;
    padding: 15px;
    background-color: #252540;
    border-radius: 8px;
}}
.legend {{
    margin-bottom: 20px;
    padding: 15px;
    background-color: #252540;
    border-radius: 8px;
    font-size: 12px;
}}
.legend span {{
    margin-right: 20px;
}}
.cards {{
    display: flex;
    gap: 20px;
    margin-bottom: 20px;
}}
.card {{
    flex: 1;
    padding: 20px;
    background-color: #252540;
    border-radius: 8px;
}}
.card h3 {{
    margin: 0 0 15px 0;
    font-size: 16px;
}}
.card.stop-high h3 {{
    color: #ffd43b;
}}
.card.stop-low h3 {{
    color: #74c0fc;
}}
.card-stat {{
    display: flex;
    justify-content: space-between;
    margin-bottom: 8px;
    font-size: 14px;
}}
.card-stat .label {{
    color: #888;
}}
.card-stat .value {{
    font-weight: 600;
}}
.card-stat .value.positive {{
    color: #51cf66;
}}
.card-stat .value.negative {{
    color: #ff6b6b;
}}
.section {{
    margin-bottom: 30px;
}}
.section h2 {{
    font-size: 18px;
    margin-bottom: 15px;
    padding-bottom: 8px;
    border-bottom: 1px solid #333;
}}
.section h2.stop-high {{
    color: #ffd43b;
}}
.section h2.stop-low {{
    color: #74c0fc;
}}
table.mini {{
    font-size: 12px;
    margin-bottom: 20px;
}}
table.mini th, table.mini td {{
    padding: 6px 10px;
}}
</style>
</head>
<body>
<h1>vol_ratio × rsi9 分析（ストップ高/安フラグ付き）</h1>
<div class="summary">
    <p>データ期間: {df_view['日付'].min().strftime('%Y-%m-%d')} 〜 {df_view['日付'].max().strftime('%Y-%m-%d')}</p>
    <p>総件数: {total_count}件 / ストップ高: {int(stop_high_count)}件 / ストップ安: {int(stop_low_count)}件</p>
    <p>寄付ストップ高: {int(opening_stop_high_count)}件 / 寄付ストップ安: {int(opening_stop_low_count)}件 <span style="color:#888;">（取引不成立・勝率計算から除外）</span></p>
</div>
<div class="legend">
    <span><b>vol_ratio:</b> <span style="color:#ff6b6b;">1倍未満=赤</span> / <span style="color:#51cf66;">2倍以上=緑</span></span>
    <span><b>rsi9:</b> <span style="color:#ff6b6b;">30以下=赤</span> / <span style="color:#51cf66;">70以上=緑</span></span>
    <span><b>ストップ高/安:</b> <span style="color:#ffd43b;">黄色=該当</span></span>
</div>

<!-- RSI帯別ショート4区分 -->
<div class="section">
<h2>RSI帯別 ショート損益（4区分）</h2>
<div style="display:flex;gap:20px;">
    <div style="flex:1;background:#252540;padding:15px;border-radius:8px;">
        <h4 style="color:#74c0fc;margin:0 0 10px 0;">制度信用</h4>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th style="text-align:left;">RSI</th>
                <th style="text-align:right;">件</th>
                <th style="text-align:right;">前前</th>
                <th style="text-align:right;">前引</th>
                <th style="text-align:right;">後前</th>
                <th style="text-align:right;">大引</th>
            </tr>
'''

    # 色判定ヘルパー（単純: 正=緑、負=赤）
    def pnl_color(v):
        return '#51cf66' if v > 0 else '#ff6b6b' if v < 0 else '#888'

    # 4区分色判定（analysis準拠）
    WHITE = '#eee'
    GREEN = '#51cf66'
    RED = '#ff6b6b'

    def get_quadrant_colors(v1, v2, v3, v4):
        values = [v1, v2, v3, v4]
        positives = [v for v in values if v > 0]
        negatives = [v for v in values if v < 0]

        # 全部プラス → 最大値を緑
        if len(positives) == 4:
            max_val = max(values)
            return [GREEN if v == max_val else WHITE for v in values]

        # 全部マイナス → 最小値（最も損失大）を赤
        if len(negatives) == 4:
            min_val = min(values)
            return [RED if v == min_val else WHITE for v in values]

        # 全部ゼロ
        if len(positives) == 0 and len(negatives) == 0:
            return [WHITE, WHITE, WHITE, WHITE]

        # 混在: 最大プラス→緑、最小マイナス→赤、他は白
        max_pos = max(positives) if positives else None
        min_neg = min(negatives) if negatives else None
        return [
            GREEN if (v > 0 and v == max_pos) else RED if (v < 0 and v == min_neg) else WHITE
            for v in values
        ]

    for b in rsi_bands_seido:
        c = get_quadrant_colors(b['s_前前'], b['s_前引'], b['s_後前'], b['s_大引'])
        html += f'''            <tr>
                <td style="text-align:left;">{b['label']}</td>
                <td style="text-align:right;">{b['count']}</td>
                <td style="text-align:right;color:{c[0]};">{b['s_前前']:+,.0f}</td>
                <td style="text-align:right;color:{c[1]};">{b['s_前引']:+,.0f}</td>
                <td style="text-align:right;color:{c[2]};">{b['s_後前']:+,.0f}</td>
                <td style="text-align:right;color:{c[3]};">{b['s_大引']:+,.0f}</td>
            </tr>
'''

    html += '''        </table>
    </div>
    <div style="flex:1;background:#252540;padding:15px;border-radius:8px;">
        <h4 style="color:#ffd43b;margin:0 0 10px 0;">いちにち信用</h4>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th style="text-align:left;">RSI</th>
                <th style="text-align:right;">件</th>
                <th style="text-align:right;">前前</th>
                <th style="text-align:right;">前引</th>
                <th style="text-align:right;">後前</th>
                <th style="text-align:right;">大引</th>
            </tr>
'''

    for b in rsi_bands_ichi:
        c = get_quadrant_colors(b['s_前前'], b['s_前引'], b['s_後前'], b['s_大引'])
        html += f'''            <tr>
                <td style="text-align:left;">{b['label']}</td>
                <td style="text-align:right;">{b['count']}</td>
                <td style="text-align:right;color:{c[0]};">{b['s_前前']:+,.0f}</td>
                <td style="text-align:right;color:{c[1]};">{b['s_前引']:+,.0f}</td>
                <td style="text-align:right;color:{c[2]};">{b['s_後前']:+,.0f}</td>
                <td style="text-align:right;color:{c[3]};">{b['s_大引']:+,.0f}</td>
            </tr>
'''

    html += f'''        </table>
    </div>
</div>
</div>

<!-- サマリーカード -->
<div class="cards">
    <div class="card stop-high">
        <h3>ストップ高銘柄（選定日）</h3>
        <div class="card-stat">
            <span class="label">件数</span>
            <span class="value">{int(stop_high_count)}件</span>
        </div>
        <div class="card-stat">
            <span class="label">勝敗</span>
            <span class="value">{stop_high_win}勝 {stop_high_lose}敗 <span style="color:#666;text-decoration:line-through;">{int(stop_high_untradable)}不成立</span></span>
        </div>
        <div class="card-stat">
            <span class="label">勝率</span>
            <span class="value">{stop_high_winrate:.1f}%</span>
        </div>
        <div class="card-stat">
            <span class="label">合計損益</span>
            <span class="value {'positive' if stop_high_total_pnl > 0 else 'negative' if stop_high_total_pnl < 0 else ''}">{stop_high_total_pnl:+,.0f}円</span>
        </div>
        <div class="card-stat">
            <span class="label">平均損益</span>
            <span class="value {'positive' if stop_high_avg_pnl > 0 else 'negative' if stop_high_avg_pnl < 0 else ''}">{stop_high_avg_pnl:+,.0f}円</span>
        </div>
    </div>
    <div class="card stop-low">
        <h3>ストップ安銘柄（選定日）</h3>
        <div class="card-stat">
            <span class="label">件数</span>
            <span class="value">{int(stop_low_count)}件</span>
        </div>
        <div class="card-stat">
            <span class="label">勝敗</span>
            <span class="value">{stop_low_win}勝 {stop_low_lose}敗 <span style="color:#666;text-decoration:line-through;">{int(stop_low_untradable)}不成立</span></span>
        </div>
        <div class="card-stat">
            <span class="label">勝率</span>
            <span class="value">{stop_low_winrate:.1f}%</span>
        </div>
        <div class="card-stat">
            <span class="label">合計損益</span>
            <span class="value {'positive' if stop_low_total_pnl > 0 else 'negative' if stop_low_total_pnl < 0 else ''}">{stop_low_total_pnl:+,.0f}円</span>
        </div>
        <div class="card-stat">
            <span class="label">平均損益</span>
            <span class="value {'positive' if stop_low_avg_pnl > 0 else 'negative' if stop_low_avg_pnl < 0 else ''}">{stop_low_avg_pnl:+,.0f}円</span>
        </div>
    </div>
</div>

<!-- 全体 vs ストップ高 比較: 曜日別価格帯別信用区分別 -->
<div class="section">
<h2 class="stop-high">全体 vs ストップ高 比較（曜日別 × 価格帯別 × 信用区分別）</h2>
<p style="font-size:12px;color:#888;margin-bottom:15px;">※ (1)全体データ → S高の件数・損益 → 除S高の損益・勝率 を横並びで比較。ストップ高の影響度がわかる。</p>
'''

    for wd_stats in comparison_stats:
        seido = wd_stats['seido']
        ichinichi = wd_stats['ichinichi']
        wd_name = seido['weekday']

        html += f'''
<div style="margin-bottom:25px;">
    <h3 style="font-size:14px;margin-bottom:10px;color:#fff;">{wd_name}</h3>

    <!-- 制度信用 -->
    <div style="background:#252540;padding:15px;border-radius:8px;margin-bottom:10px;">
        <div style="font-size:13px;font-weight:600;margin-bottom:10px;">
            制度信用 <span style="color:#888;">{seido['all_count']}件</span>
            <span style="margin-left:20px;">全体: <span style="{'color:#51cf66' if seido['all_pnl'] > 0 else 'color:#ff6b6b' if seido['all_pnl'] < 0 else ''}">{seido['all_pnl']:+,.0f}</span></span>
            <span style="margin-left:10px;color:#ffd43b;">S高: {seido['sh_count']}件 / {seido['sh_pnl']:+,.0f}</span>
            <span style="margin-left:10px;">除S高: <span style="{'color:#51cf66' if seido['ex_pnl'] > 0 else 'color:#ff6b6b' if seido['ex_pnl'] < 0 else ''}">{seido['ex_pnl']:+,.0f}</span></span>
        </div>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th rowspan="2" style="vertical-align:middle;">価格帯</th>
                <th colspan="3" style="text-align:center;border-right:1px solid #444;">全体(1)</th>
                <th colspan="2" style="text-align:center;color:#ffd43b;border-right:1px solid #444;">S高</th>
                <th colspan="3" style="text-align:center;">除S高</th>
            </tr>
            <tr>
                <th>件</th><th>損益</th><th style="border-right:1px solid #444;">%</th>
                <th>件</th><th style="border-right:1px solid #444;">損益</th>
                <th>件</th><th>損益</th><th>%</th>
            </tr>
'''
        for pr in seido['price_ranges']:
            all_pnl_c = 'color:#51cf66' if pr['all_pnl'] > 0 else 'color:#ff6b6b' if pr['all_pnl'] < 0 else ''
            all_wr_c = 'color:#51cf66' if pr['all_winrate'] > 50 else 'color:#ff6b6b' if pr['all_winrate'] < 50 else ''
            sh_pnl_c = 'color:#ffd43b'
            ex_pnl_c = 'color:#51cf66' if pr['ex_pnl'] > 0 else 'color:#ff6b6b' if pr['ex_pnl'] < 0 else ''
            ex_wr_c = 'color:#51cf66' if pr['ex_winrate'] > 50 else 'color:#ff6b6b' if pr['ex_winrate'] < 50 else ''
            html += f'''            <tr>
                <td style="text-align:left;">{pr['label']}</td>
                <td>{pr['all_count']}</td>
                <td style="{all_pnl_c}">{pr['all_pnl']:+,.0f}</td>
                <td style="{all_wr_c};border-right:1px solid #444;">{pr['all_winrate']:.0f}%</td>
                <td style="{sh_pnl_c}">{pr['sh_count']}</td>
                <td style="{sh_pnl_c};border-right:1px solid #444;">{pr['sh_pnl']:+,.0f}</td>
                <td>{pr['ex_count']}</td>
                <td style="{ex_pnl_c}">{pr['ex_pnl']:+,.0f}</td>
                <td style="{ex_wr_c}">{pr['ex_winrate']:.0f}%</td>
            </tr>
'''
        html += '''        </table>
    </div>

    <!-- いちにち信用 -->
    <div style="background:#252540;padding:15px;border-radius:8px;">
'''
        html += f'''        <div style="font-size:13px;font-weight:600;margin-bottom:10px;">
            いちにち信用 <span style="color:#888;">{ichinichi['all_count']}件</span>
            <span style="margin-left:20px;">全体: <span style="{'color:#51cf66' if ichinichi['all_pnl'] > 0 else 'color:#ff6b6b' if ichinichi['all_pnl'] < 0 else ''}">{ichinichi['all_pnl']:+,.0f}</span></span>
            <span style="margin-left:10px;color:#ffd43b;">S高: {ichinichi['sh_count']}件 / {ichinichi['sh_pnl']:+,.0f}</span>
            <span style="margin-left:10px;">除S高: <span style="{'color:#51cf66' if ichinichi['ex_pnl'] > 0 else 'color:#ff6b6b' if ichinichi['ex_pnl'] < 0 else ''}">{ichinichi['ex_pnl']:+,.0f}</span></span>
        </div>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th rowspan="2" style="vertical-align:middle;">価格帯</th>
                <th colspan="3" style="text-align:center;border-right:1px solid #444;">全体(1)</th>
                <th colspan="2" style="text-align:center;color:#ffd43b;border-right:1px solid #444;">S高</th>
                <th colspan="3" style="text-align:center;">除S高</th>
            </tr>
            <tr>
                <th>件</th><th>損益</th><th style="border-right:1px solid #444;">%</th>
                <th>件</th><th style="border-right:1px solid #444;">損益</th>
                <th>件</th><th>損益</th><th>%</th>
            </tr>
'''
        for pr in ichinichi['price_ranges']:
            all_pnl_c = 'color:#51cf66' if pr['all_pnl'] > 0 else 'color:#ff6b6b' if pr['all_pnl'] < 0 else ''
            all_wr_c = 'color:#51cf66' if pr['all_winrate'] > 50 else 'color:#ff6b6b' if pr['all_winrate'] < 50 else ''
            sh_pnl_c = 'color:#ffd43b'
            ex_pnl_c = 'color:#51cf66' if pr['ex_pnl'] > 0 else 'color:#ff6b6b' if pr['ex_pnl'] < 0 else ''
            ex_wr_c = 'color:#51cf66' if pr['ex_winrate'] > 50 else 'color:#ff6b6b' if pr['ex_winrate'] < 50 else ''
            html += f'''            <tr>
                <td style="text-align:left;">{pr['label']}</td>
                <td>{pr['all_count']}</td>
                <td style="{all_pnl_c}">{pr['all_pnl']:+,.0f}</td>
                <td style="{all_wr_c};border-right:1px solid #444;">{pr['all_winrate']:.0f}%</td>
                <td style="{sh_pnl_c}">{pr['sh_count']}</td>
                <td style="{sh_pnl_c};border-right:1px solid #444;">{pr['sh_pnl']:+,.0f}</td>
                <td>{pr['ex_count']}</td>
                <td style="{ex_pnl_c}">{pr['ex_pnl']:+,.0f}</td>
                <td style="{ex_wr_c}">{pr['ex_winrate']:.0f}%</td>
            </tr>
'''
        html += '''        </table>
    </div>
</div>
'''

    html += '''</div>
'''

    # ストップ高テーブル生成
    if len(df_stop_high) > 0:
        html += '''<div class="section">
<h2 class="stop-high">ストップ高銘柄一覧</h2>
<p style="font-size:12px;color:#888;margin-bottom:10px;"><span style="text-decoration:line-through;color:#666;">消し込み線</span> = 寄付ストップ高（取引不成立）</p>
<table class="mini">
<tr>
    <th>日付</th>
    <th>ティッカー</th>
    <th>銘柄名</th>
    <th>始値</th>
    <th>終値</th>
    <th>損益(ショート)</th>
    <th>vol_ratio</th>
    <th>rsi9</th>
</tr>
'''
        for _, row in df_stop_high.iterrows():
            is_untradable = row['取引不成立'] == True
            strike_style = 'text-decoration:line-through;color:#666;' if is_untradable else ''
            profit_style = style_profit(row['損益_ショート']) if not is_untradable else strike_style
            vol_style = style_vol_ratio(row['vol_ratio']) if not is_untradable else strike_style
            rsi_style = style_rsi9(row['rsi9']) if not is_untradable else strike_style
            date_str = row['日付'].strftime('%Y-%m-%d') if pd.notna(row['日付']) else '-'
            buy_price = f"{row['始値']:,.0f}" if pd.notna(row['始値']) else '-'
            daily_close = f"{row['終値']:,.0f}" if pd.notna(row['終値']) else '-'
            profit = f"{row['損益_ショート']:+,.0f}" if pd.notna(row['損益_ショート']) else '-'
            vol = f"{row['vol_ratio']:.2f}" if pd.notna(row['vol_ratio']) else '-'
            rsi = f"{row['rsi9']:.1f}" if pd.notna(row['rsi9']) else '-'
            html += f'''<tr style="{strike_style}">
    <td>{date_str}</td>
    <td>{row['ティッカー']}</td>
    <td>{row['銘柄名']}</td>
    <td>{buy_price}</td>
    <td>{daily_close}</td>
    <td style="{profit_style}">{profit}</td>
    <td style="{vol_style}">{vol}</td>
    <td style="{rsi_style}">{rsi}</td>
</tr>
'''
        html += '''</table>
</div>
'''

    # ストップ安テーブル生成
    if len(df_stop_low) > 0:
        html += '''<div class="section">
<h2 class="stop-low">ストップ安銘柄一覧</h2>
<p style="font-size:12px;color:#888;margin-bottom:10px;"><span style="text-decoration:line-through;color:#666;">消し込み線</span> = 寄付ストップ安（取引不成立）</p>
<table class="mini">
<tr>
    <th>日付</th>
    <th>ティッカー</th>
    <th>銘柄名</th>
    <th>始値</th>
    <th>終値</th>
    <th>損益(ショート)</th>
    <th>vol_ratio</th>
    <th>rsi9</th>
</tr>
'''
        for _, row in df_stop_low.iterrows():
            is_untradable = row['取引不成立'] == True
            strike_style = 'text-decoration:line-through;color:#666;' if is_untradable else ''
            profit_style = style_profit(row['損益_ショート']) if not is_untradable else strike_style
            vol_style = style_vol_ratio(row['vol_ratio']) if not is_untradable else strike_style
            rsi_style = style_rsi9(row['rsi9']) if not is_untradable else strike_style
            date_str = row['日付'].strftime('%Y-%m-%d') if pd.notna(row['日付']) else '-'
            buy_price = f"{row['始値']:,.0f}" if pd.notna(row['始値']) else '-'
            daily_close = f"{row['終値']:,.0f}" if pd.notna(row['終値']) else '-'
            profit = f"{row['損益_ショート']:+,.0f}" if pd.notna(row['損益_ショート']) else '-'
            vol = f"{row['vol_ratio']:.2f}" if pd.notna(row['vol_ratio']) else '-'
            rsi = f"{row['rsi9']:.1f}" if pd.notna(row['rsi9']) else '-'
            html += f'''<tr style="{strike_style}">
    <td>{date_str}</td>
    <td>{row['ティッカー']}</td>
    <td>{row['銘柄名']}</td>
    <td>{buy_price}</td>
    <td>{daily_close}</td>
    <td style="{profit_style}">{profit}</td>
    <td style="{vol_style}">{vol}</td>
    <td style="{rsi_style}">{rsi}</td>
</tr>
'''
        html += '''</table>
</div>
'''

    # RSI分析セクション（除S高安）
    df_ex_stop = df_view[(df_view['ストップ高'] != True) & (df_view['ストップ安'] != True)].copy()
    df_ex_stop['margin_type'] = df_ex_stop['信用区分']

    # RSI帯別集計
    bins = [0, 30, 40, 50, 60, 70, 80, 90, 100]

    def calc_rsi_stats(data, bins):
        stats = []
        for i in range(len(bins)-1):
            mask = (data['rsi9'] >= bins[i]) & (data['rsi9'] < bins[i+1])
            subset = data[mask]
            tradable = subset[subset['取引不成立'] != True]
            if len(subset) > 0:
                pnl = subset['損益_ショート'].sum()
                winrate = (tradable['損益_ショート'] > 0).mean() * 100 if len(tradable) > 0 else 0
            else:
                pnl = 0
                winrate = 0
            stats.append({
                'label': f'{bins[i]}-{bins[i+1]}',
                'count': len(subset),
                'pnl': pnl,
                'winrate': winrate
            })
        return stats

    df_seido = df_ex_stop[df_ex_stop['信用区分'] == '制度信用']
    df_ichi = df_ex_stop[df_ex_stop['信用区分'] == 'いちにち信用']

    seido_stats = calc_rsi_stats(df_seido, bins)
    ichi_stats = calc_rsi_stats(df_ichi, bins)

    seido_total_pnl = df_seido['損益_ショート'].sum()
    seido_total_count = len(df_seido)
    seido_tradable = df_seido[df_seido['取引不成立'] != True]
    seido_total_winrate = (seido_tradable['損益_ショート'] > 0).mean() * 100 if len(seido_tradable) > 0 else 0

    ichi_total_pnl = df_ichi['損益_ショート'].sum()
    ichi_total_count = len(df_ichi)
    ichi_tradable = df_ichi[df_ichi['取引不成立'] != True]
    ichi_total_winrate = (ichi_tradable['損益_ショート'] > 0).mean() * 100 if len(ichi_tradable) > 0 else 0

    # RSI70以上（制度）、RSI90以上（いちにち）
    df_seido_70plus = df_seido[df_seido['rsi9'] >= 70].sort_values(['rsi9', '日付'], ascending=[False, False])
    df_ichi_90plus = df_ichi[df_ichi['rsi9'] >= 90].sort_values(['rsi9', '日付'], ascending=[False, False])

    seido_70plus_tradable = df_seido_70plus[df_seido_70plus['取引不成立'] != True]
    seido_70plus_pnl = df_seido_70plus['損益_ショート'].sum()
    seido_70plus_winrate = (seido_70plus_tradable['損益_ショート'] > 0).mean() * 100 if len(seido_70plus_tradable) > 0 else 0

    ichi_90plus_tradable = df_ichi_90plus[df_ichi_90plus['取引不成立'] != True]
    ichi_90plus_pnl = df_ichi_90plus['損益_ショート'].sum()
    ichi_90plus_winrate = (ichi_90plus_tradable['損益_ショート'] > 0).mean() * 100 if len(ichi_90plus_tradable) > 0 else 0

    html += f'''
<!-- RSI分析セクション -->
<div class="section">
<h2 style="color:#74c0fc;">RSI帯別分析（除S高安・曜日別戦略）</h2>
<p style="font-size:12px;color:#888;margin-bottom:15px;">※ ストップ高/安を除いた{len(df_ex_stop)}件を対象。閾値候補: 制度信用RSI70以上、いちにち信用RSI90以上</p>

<div class="cards">
    <div class="card" style="border-left:3px solid #74c0fc;">
        <h3 style="color:#74c0fc;">制度信用（{seido_total_count}件）</h3>
        <div class="card-stat">
            <span class="label">合計損益</span>
            <span class="value {'positive' if seido_total_pnl > 0 else 'negative' if seido_total_pnl < 0 else ''}">{seido_total_pnl:+,.0f}円</span>
        </div>
        <div class="card-stat">
            <span class="label">勝率</span>
            <span class="value">{seido_total_winrate:.1f}%</span>
        </div>
        <div class="card-stat" style="margin-top:10px;padding-top:10px;border-top:1px solid #444;">
            <span class="label" style="color:#ff6b6b;">RSI70以上（{len(df_seido_70plus)}件）</span>
            <span class="value negative">{seido_70plus_pnl:+,.0f}円 / {seido_70plus_winrate:.1f}%</span>
        </div>
    </div>
    <div class="card" style="border-left:3px solid #ffd43b;">
        <h3 style="color:#ffd43b;">いちにち信用（{ichi_total_count}件）</h3>
        <div class="card-stat">
            <span class="label">合計損益</span>
            <span class="value {'positive' if ichi_total_pnl > 0 else 'negative' if ichi_total_pnl < 0 else ''}">{ichi_total_pnl:+,.0f}円</span>
        </div>
        <div class="card-stat">
            <span class="label">勝率</span>
            <span class="value">{ichi_total_winrate:.1f}%</span>
        </div>
        <div class="card-stat" style="margin-top:10px;padding-top:10px;border-top:1px solid #444;">
            <span class="label" style="color:#ff6b6b;">RSI90以上（{len(df_ichi_90plus)}件）</span>
            <span class="value negative">{ichi_90plus_pnl:+,.0f}円 / {ichi_90plus_winrate:.1f}%</span>
        </div>
    </div>
</div>

<!-- RSI帯別テーブル -->
<div style="display:flex;gap:20px;">
    <div style="flex:1;background:#252540;padding:15px;border-radius:8px;">
        <h4 style="color:#74c0fc;margin:0 0 10px 0;">制度信用 RSI帯別</h4>
        <table class="mini">
            <tr><th>RSI帯</th><th>件数</th><th>損益</th><th>勝率</th></tr>
'''
    for s in seido_stats:
        pnl_c = 'color:#51cf66' if s['pnl'] > 0 else 'color:#ff6b6b' if s['pnl'] < 0 else ''
        wr_c = 'color:#51cf66' if s['winrate'] > 50 else 'color:#ff6b6b' if s['winrate'] < 50 else ''
        bg = 'background:#3a2a2a;' if s['label'] in ['70-80', '80-90', '90-100'] else ''
        html += f'            <tr style="{bg}"><td>{s["label"]}</td><td>{s["count"]}</td><td style="{pnl_c}">{s["pnl"]:+,.0f}</td><td style="{wr_c}">{s["winrate"]:.0f}%</td></tr>\n'
    html += f'''        </table>
    </div>
    <div style="flex:1;background:#252540;padding:15px;border-radius:8px;">
        <h4 style="color:#ffd43b;margin:0 0 10px 0;">いちにち信用 RSI帯別</h4>
        <table class="mini">
            <tr><th>RSI帯</th><th>件数</th><th>損益</th><th>勝率</th></tr>
'''
    for s in ichi_stats:
        pnl_c = 'color:#51cf66' if s['pnl'] > 0 else 'color:#ff6b6b' if s['pnl'] < 0 else ''
        wr_c = 'color:#51cf66' if s['winrate'] > 50 else 'color:#ff6b6b' if s['winrate'] < 50 else ''
        bg = 'background:#3a2a2a;' if s['label'] in ['90-100'] else ''
        html += f'            <tr style="{bg}"><td>{s["label"]}</td><td>{s["count"]}</td><td style="{pnl_c}">{s["pnl"]:+,.0f}</td><td style="{wr_c}">{s["winrate"]:.0f}%</td></tr>\n'
    html += '''        </table>
    </div>
</div>
</div>
'''

    # 高RSI銘柄の曜日別戦略分析
    # ロング損益を追加
    df_seido_70plus['損益_ロング'] = -df_seido_70plus['損益_ショート']
    df_ichi_90plus['損益_ロング'] = -df_ichi_90plus['損益_ショート']

    # 曜日別集計（制度信用RSI70+）
    wd_map_idx = {0: '月', 1: '火', 2: '水', 3: '木', 4: '金'}
    seido_70_wd_stats = []
    for wd in range(5):
        subset = df_seido_70plus[df_seido_70plus['曜日番号'] == wd]
        tradable = subset[subset['取引不成立'] != True]
        if len(subset) > 0:
            seido_70_wd_stats.append({
                'weekday': wd_map_idx[wd],
                'count': len(subset),
                's_前前': subset['損益_前前S'].sum(),
                's_前引': subset['損益_前引S'].sum(),
                's_後前': subset['損益_後前S'].sum(),
                's_大引': subset['損益_大引S'].sum(),
            })
        else:
            seido_70_wd_stats.append({
                'weekday': wd_map_idx[wd], 'count': 0,
                's_前前': 0, 's_前引': 0, 's_後前': 0, 's_大引': 0
            })

    # 曜日別集計（いちにち信用RSI90+）
    ichi_90_wd_stats = []
    for wd in range(5):
        subset = df_ichi_90plus[df_ichi_90plus['曜日番号'] == wd]
        tradable = subset[subset['取引不成立'] != True]
        if len(subset) > 0:
            ichi_90_wd_stats.append({
                'weekday': wd_map_idx[wd],
                'count': len(subset),
                's_前前': subset['損益_前前S'].sum(),
                's_前引': subset['損益_前引S'].sum(),
                's_後前': subset['損益_後前S'].sum(),
                's_大引': subset['損益_大引S'].sum(),
            })
        else:
            ichi_90_wd_stats.append({
                'weekday': wd_map_idx[wd], 'count': 0,
                's_前前': 0, 's_前引': 0, 's_後前': 0, 's_大引': 0
            })

    # 戦略サマリー計算
    # 基本戦略: 月火木金=ショート、水=ロング
    # 高RSI例外: RSI70+/90+は基本ロング、ただし水曜の制度RSI70+はショート
    seido_70_long_total = df_seido_70plus['損益_ロング'].sum()
    seido_70_short_total = df_seido_70plus['損益_ショート'].sum()
    ichi_90_long_total = df_ichi_90plus['損益_ロング'].sum()
    ichi_90_short_total = df_ichi_90plus['損益_ショート'].sum()

    # 4区分ショート合計
    seido_70_s_前前 = df_seido_70plus['損益_前前S'].sum()
    seido_70_s_前引 = df_seido_70plus['損益_前引S'].sum()
    seido_70_s_後前 = df_seido_70plus['損益_後前S'].sum()
    seido_70_s_大引 = df_seido_70plus['損益_大引S'].sum()
    ichi_90_s_前前 = df_ichi_90plus['損益_前前S'].sum()
    ichi_90_s_前引 = df_ichi_90plus['損益_前引S'].sum()
    ichi_90_s_後前 = df_ichi_90plus['損益_後前S'].sum()
    ichi_90_s_大引 = df_ichi_90plus['損益_大引S'].sum()

    html += f'''
<!-- 高RSI戦略分析 -->
<div class="section">
<h2 style="color:#51cf66;">高RSI銘柄 戦略分析（ロング vs ショート）</h2>
<p style="font-size:12px;color:#888;margin-bottom:15px;">※ 高RSI銘柄はショートで損失 → ロングに切り替えると利益に。ただし水曜日の制度信用RSI70+は例外</p>

<!-- 戦略サマリーカード -->
<div class="cards" style="margin-bottom:20px;">
    <div class="card" style="border-left:3px solid #51cf66;">
        <h3 style="color:#51cf66;">戦略推奨</h3>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th style="text-align:left;">条件</th>
                <th>月火木金</th>
                <th>水曜日</th>
            </tr>
            <tr>
                <td style="text-align:left;color:#888;">基本戦略</td>
                <td style="color:#ff6b6b;">ショート</td>
                <td style="color:#51cf66;">ロング</td>
            </tr>
            <tr style="background:#2a3a2a;">
                <td style="text-align:left;">制度信用 RSI70+</td>
                <td style="color:#51cf66;font-weight:bold;">ロング</td>
                <td style="color:#ff6b6b;font-weight:bold;">ショート（例外）</td>
            </tr>
            <tr style="background:#2a3a2a;">
                <td style="text-align:left;">いちにち RSI90+</td>
                <td style="color:#51cf66;font-weight:bold;">ロング</td>
                <td style="color:#51cf66;font-weight:bold;">ロング（一致）</td>
            </tr>
        </table>
    </div>
    <div class="card" style="border-left:3px solid #74c0fc;">
        <h3 style="color:#74c0fc;">損益比較（全体）</h3>
        <div class="card-stat">
            <span class="label">制度RSI70+</span>
            <span class="value">S: <span class="negative">{seido_70_short_total:+,.0f}</span> / L: <span class="positive">{seido_70_long_total:+,.0f}</span></span>
        </div>
        <div class="card-stat">
            <span class="label">いちにちRSI90+</span>
            <span class="value">S: <span class="negative">{ichi_90_short_total:+,.0f}</span> / L: <span class="positive">{ichi_90_long_total:+,.0f}</span></span>
        </div>
    </div>
</div>

'''

    html += f'''<!-- ショート4区分サマリー -->
<div class="cards" style="margin-bottom:20px;">
    <div class="card" style="border-left:3px solid #ff6b6b;">
        <h3 style="color:#ff6b6b;">制度RSI70+ ショート4区分</h3>
        <table class="mini" style="width:100%;border-collapse:collapse;">
            <tr style="border-bottom:2px solid #444;">
                <th style="text-align:center;padding:8px;">前前</th>
                <th style="text-align:center;padding:8px;">前引</th>
                <th style="text-align:center;padding:8px;">後前</th>
                <th style="text-align:center;padding:8px;">大引</th>
            </tr>
            <tr>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(seido_70_s_前前)};">{seido_70_s_前前:+,.0f}</td>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(seido_70_s_前引)};">{seido_70_s_前引:+,.0f}</td>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(seido_70_s_後前)};">{seido_70_s_後前:+,.0f}</td>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(seido_70_s_大引)};">{seido_70_s_大引:+,.0f}</td>
            </tr>
        </table>
    </div>
    <div class="card" style="border-left:3px solid #ffd43b;">
        <h3 style="color:#ffd43b;">いちにちRSI90+ ショート4区分</h3>
        <table class="mini" style="width:100%;border-collapse:collapse;">
            <tr style="border-bottom:2px solid #444;">
                <th style="text-align:center;padding:8px;">前前</th>
                <th style="text-align:center;padding:8px;">前引</th>
                <th style="text-align:center;padding:8px;">後前</th>
                <th style="text-align:center;padding:8px;">大引</th>
            </tr>
            <tr>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(ichi_90_s_前前)};">{ichi_90_s_前前:+,.0f}</td>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(ichi_90_s_前引)};">{ichi_90_s_前引:+,.0f}</td>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(ichi_90_s_後前)};">{ichi_90_s_後前:+,.0f}</td>
                <td style="text-align:right;padding:8px;font-weight:bold;color:{pnl_color(ichi_90_s_大引)};">{ichi_90_s_大引:+,.0f}</td>
            </tr>
        </table>
    </div>
</div>

<!-- 曜日別詳細テーブル -->
<div style="display:flex;gap:20px;">
    <div style="flex:1;background:#252540;padding:15px;border-radius:8px;">
        <h4 style="color:#74c0fc;margin:0 0 10px 0;">制度信用 RSI70+ 曜日別</h4>
        <div style="display:flex;gap:15px;margin-bottom:10px;padding:8px;background:#1a1a2e;border-radius:4px;font-size:12px;">
            <span style="color:#888;">ショート合計:</span>
            <span>前前 <span style="color:{pnl_color(seido_70_s_前前)};font-weight:bold;">{seido_70_s_前前:+,.0f}</span></span>
            <span>前引 <span style="color:{pnl_color(seido_70_s_前引)};font-weight:bold;">{seido_70_s_前引:+,.0f}</span></span>
            <span>後前 <span style="color:{pnl_color(seido_70_s_後前)};font-weight:bold;">{seido_70_s_後前:+,.0f}</span></span>
            <span>大引 <span style="color:{pnl_color(seido_70_s_大引)};font-weight:bold;">{seido_70_s_大引:+,.0f}</span></span>
        </div>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th>曜日</th><th>件</th>
                <th style="text-align:right;">前前</th>
                <th style="text-align:right;">前引</th>
                <th style="text-align:right;">後前</th>
                <th style="text-align:right;">大引</th>
            </tr>
'''
    for s in seido_70_wd_stats:
        html += f'            <tr><td>{s["weekday"]}</td><td style="text-align:right;">{s["count"]}</td><td style="text-align:right;color:{pnl_color(s["s_前前"])};">{s["s_前前"]:+,.0f}</td><td style="text-align:right;color:{pnl_color(s["s_前引"])};">{s["s_前引"]:+,.0f}</td><td style="text-align:right;color:{pnl_color(s["s_後前"])};">{s["s_後前"]:+,.0f}</td><td style="text-align:right;color:{pnl_color(s["s_大引"])};">{s["s_大引"]:+,.0f}</td></tr>\n'

    html += f'''        </table>
    </div>
    <div style="flex:1;background:#252540;padding:15px;border-radius:8px;">
        <h4 style="color:#ffd43b;margin:0 0 10px 0;">いちにち信用 RSI90+ 曜日別</h4>
        <div style="display:flex;gap:15px;margin-bottom:10px;padding:8px;background:#1a1a2e;border-radius:4px;font-size:12px;">
            <span style="color:#888;">ショート合計:</span>
            <span>前前 <span style="color:{pnl_color(ichi_90_s_前前)};font-weight:bold;">{ichi_90_s_前前:+,.0f}</span></span>
            <span>前引 <span style="color:{pnl_color(ichi_90_s_前引)};font-weight:bold;">{ichi_90_s_前引:+,.0f}</span></span>
            <span>後前 <span style="color:{pnl_color(ichi_90_s_後前)};font-weight:bold;">{ichi_90_s_後前:+,.0f}</span></span>
            <span>大引 <span style="color:{pnl_color(ichi_90_s_大引)};font-weight:bold;">{ichi_90_s_大引:+,.0f}</span></span>
        </div>
        <table class="mini" style="width:100%;">
            <tr style="border-bottom:2px solid #444;">
                <th>曜日</th><th>件</th>
                <th style="text-align:right;">前前</th>
                <th style="text-align:right;">前引</th>
                <th style="text-align:right;">後前</th>
                <th style="text-align:right;">大引</th>
            </tr>
'''
    for s in ichi_90_wd_stats:
        html += f'            <tr><td>{s["weekday"]}</td><td style="text-align:right;">{s["count"]}</td><td style="text-align:right;color:{pnl_color(s["s_前前"])};">{s["s_前前"]:+,.0f}</td><td style="text-align:right;color:{pnl_color(s["s_前引"])};">{s["s_前引"]:+,.0f}</td><td style="text-align:right;color:{pnl_color(s["s_後前"])};">{s["s_後前"]:+,.0f}</td><td style="text-align:right;color:{pnl_color(s["s_大引"])};">{s["s_大引"]:+,.0f}</td></tr>\n'

    html += '''        </table>
    </div>
</div>
</div>
'''

    # 高RSI銘柄 曜日別リスト（曜日 > 信用区分 > 銘柄）ショート4区分
    html += '''
<div class="section">
<h2 style="color:#51cf66;">高RSI銘柄 曜日別一覧（ショート4区分）</h2>
<p style="font-size:12px;color:#888;margin-bottom:15px;">※ 制度信用RSI70+、いちにち信用RSI90+を抽出。前前=前場前半、前引=前場引け、後前=後場前半、大引=大引け</p>
'''

    for wd in range(5):
        wd_name = wd_map_idx[wd]
        wd_seido = df_seido_70plus[df_seido_70plus['曜日番号'] == wd].sort_values('日付', ascending=False)
        wd_ichi = df_ichi_90plus[df_ichi_90plus['曜日番号'] == wd].sort_values('日付', ascending=False)

        html += f'''
<div style="background:#252540;padding:15px;border-radius:8px;margin-bottom:15px;">
    <h3 style="color:#fff;margin:0 0 15px 0;font-size:16px;border-bottom:1px solid #444;padding-bottom:8px;">{wd_name}曜日</h3>

    <div style="margin-bottom:15px;">
        <div style="font-size:13px;font-weight:600;margin-bottom:8px;color:#74c0fc;">
            制度信用 RSI70+ <span style="color:#888;">({len(wd_seido)}件)</span>
        </div>
'''
        if len(wd_seido) > 0:
            html += '        <table class="mini" style="width:100%;"><tr><th>日付</th><th>RSI</th><th>銘柄</th><th>前前</th><th>前引</th><th>後前</th><th>大引</th></tr>\n'
            for _, row in wd_seido.iterrows():
                is_untradable = row['取引不成立'] == True
                strike_style = 'text-decoration:line-through;color:#666;' if is_untradable else ''
                date_str = row['日付'].strftime('%m/%d') if pd.notna(row['日付']) else '-'
                rsi = f"{row['rsi9']:.1f}" if pd.notna(row['rsi9']) else '-'
                p1 = row['損益_前前S']
                p2 = row['損益_前引S']
                p3 = row['損益_後前S']
                p4 = row['損益_大引S']
                def pnl_style(v, strike):
                    if strike: return strike
                    return 'color:#51cf66' if v > 0 else 'color:#ff6b6b' if v < 0 else ''
                html += f'        <tr style="{strike_style}"><td>{date_str}</td><td>{rsi}</td><td style="text-align:left;">{row["ティッカー"]} {row["銘柄名"]}</td><td style="{pnl_style(p1, strike_style)}">{p1:+,.0f}</td><td style="{pnl_style(p2, strike_style)}">{p2:+,.0f}</td><td style="{pnl_style(p3, strike_style)}">{p3:+,.0f}</td><td style="{pnl_style(p4, strike_style)}">{p4:+,.0f}</td></tr>\n'
            html += '        </table>\n'
        else:
            html += '        <p style="color:#666;font-size:12px;">該当なし</p>\n'

        html += f'''    </div>

    <div>
        <div style="font-size:13px;font-weight:600;margin-bottom:8px;color:#ffd43b;">
            いちにち信用 RSI90+ <span style="color:#888;">({len(wd_ichi)}件)</span>
        </div>
'''
        if len(wd_ichi) > 0:
            html += '        <table class="mini" style="width:100%;"><tr><th>日付</th><th>RSI</th><th>銘柄</th><th>前前</th><th>前引</th><th>後前</th><th>大引</th></tr>\n'
            for _, row in wd_ichi.iterrows():
                is_untradable = row['取引不成立'] == True
                strike_style = 'text-decoration:line-through;color:#666;' if is_untradable else ''
                date_str = row['日付'].strftime('%m/%d') if pd.notna(row['日付']) else '-'
                rsi = f"{row['rsi9']:.1f}" if pd.notna(row['rsi9']) else '-'
                p1 = row['損益_前前S']
                p2 = row['損益_前引S']
                p3 = row['損益_後前S']
                p4 = row['損益_大引S']
                def pnl_style(v, strike):
                    if strike: return strike
                    return 'color:#51cf66' if v > 0 else 'color:#ff6b6b' if v < 0 else ''
                html += f'        <tr style="{strike_style}"><td>{date_str}</td><td>{rsi}</td><td style="text-align:left;">{row["ティッカー"]} {row["銘柄名"]}</td><td style="{pnl_style(p1, strike_style)}">{p1:+,.0f}</td><td style="{pnl_style(p2, strike_style)}">{p2:+,.0f}</td><td style="{pnl_style(p3, strike_style)}">{p3:+,.0f}</td><td style="{pnl_style(p4, strike_style)}">{p4:+,.0f}</td></tr>\n'
            html += '        </table>\n'
        else:
            html += '        <p style="color:#666;font-size:12px;">該当なし</p>\n'

        html += '''    </div>
</div>
'''

    html += '</div>\n'

    # 全体テーブル
    html += '''<div class="section">
<h2>全銘柄一覧</h2>
<p style="font-size:12px;color:#888;margin-bottom:10px;"><span style="text-decoration:line-through;color:#666;">消し込み線</span> = 寄付ストップ高/安（取引不成立）</p>
<table>
<tr>
    <th>日付</th>
    <th>ティッカー</th>
    <th>銘柄名</th>
    <th>前日終値</th>
    <th>始値</th>
    <th>終値</th>
    <th>損益(ショート)</th>
    <th>vol_ratio</th>
    <th>rsi9</th>
    <th>S高</th>
    <th>S安</th>
</tr>
'''

    for _, row in df_view.iterrows():
        is_untradable = row['取引不成立'] == True
        strike_style = 'text-decoration:line-through;color:#666;' if is_untradable else ''
        vol_style = style_vol_ratio(row['vol_ratio']) if not is_untradable else strike_style
        rsi_style = style_rsi9(row['rsi9']) if not is_untradable else strike_style
        profit_style = style_profit(row['損益_ショート']) if not is_untradable else strike_style
        stop_high_style = style_stop(row['ストップ高']) if not is_untradable else strike_style
        stop_low_style = style_stop(row['ストップ安']) if not is_untradable else strike_style

        date_str = row['日付'].strftime('%Y-%m-%d') if pd.notna(row['日付']) else '-'
        prev_close = f"{row['前日終値']:,.0f}" if pd.notna(row['前日終値']) else '-'
        buy_price = f"{row['始値']:,.0f}" if pd.notna(row['始値']) else '-'
        daily_close = f"{row['終値']:,.0f}" if pd.notna(row['終値']) else '-'
        profit = f"{row['損益_ショート']:+,.0f}" if pd.notna(row['損益_ショート']) else '-'
        vol = f"{row['vol_ratio']:.2f}" if pd.notna(row['vol_ratio']) else '-'
        rsi = f"{row['rsi9']:.1f}" if pd.notna(row['rsi9']) else '-'
        stop_high = '●' if row['ストップ高'] == True else ''
        stop_low = '●' if row['ストップ安'] == True else ''

        html += f'''<tr style="{strike_style}">
    <td>{date_str}</td>
    <td>{row['ティッカー']}</td>
    <td>{row['銘柄名']}</td>
    <td>{prev_close}</td>
    <td>{buy_price}</td>
    <td>{daily_close}</td>
    <td style="{profit_style}">{profit}</td>
    <td style="{vol_style}">{vol}</td>
    <td style="{rsi_style}">{rsi}</td>
    <td style="{stop_high_style}">{stop_high}</td>
    <td style="{stop_low_style}">{stop_low}</td>
</tr>
'''

    html += '''</table>
</div>
</body>
</html>'''

    # 保存
    output_path = '/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/improvement/output/vol_ratio_rsi9_stop_analysis.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'HTMLファイルを出力しました: {output_path}')
    print(f'件数: {total_count}件')
    print(f'ストップ高: {stop_high_count}件 / ストップ安: {stop_low_count}件')

    return output_path


if __name__ == '__main__':
    output_path = main()
    import subprocess
    subprocess.run(['open', output_path])
