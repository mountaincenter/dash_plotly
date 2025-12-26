#!/usr/bin/env python3
"""
Grok空売り日別分析HTMLを生成するスクリプト
P1: 株数カラムといちにち信用のtab切り替え（全数/除0株）
"""

import pandas as pd
from datetime import datetime
from pathlib import Path

# パス設定
IMPROVEMENT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = IMPROVEMENT_DIR / "output"

# 曜日マッピング
WEEKDAY_NAMES = ['月曜日', '火曜日', '水曜日', '木曜日', '金曜日']

# 価格帯
PRICE_RANGES = [
    ('~1,000円', 0, 1000),
    ('1,000~3,000円', 1000, 3000),
    ('3,000~5,000円', 3000, 5000),
    ('5,000~10,000円', 5000, 10000),
    ('10,000円~', 10000, float('inf')),
]


def compare_class(p1, p2):
    """前場/大引けの比較色クラスを返す"""
    if p1 >= 0 and p2 >= 0:
        if p1 > p2:
            return 'pos', ''
        elif p2 > p1:
            return '', 'pos'
        else:
            return '', ''
    elif p1 < 0 and p2 < 0:
        if abs(p1) > abs(p2):
            return 'neg', ''
        elif abs(p2) > abs(p1):
            return '', 'neg'
        else:
            return '', ''
    else:
        return ('pos' if p1 > 0 else 'neg'), ('pos' if p2 > 0 else 'neg')


def winrate_class(rate):
    """勝率の色クラス"""
    if rate > 50:
        return 'pos'
    elif rate < 50:
        return 'neg'
    return ''


def format_profit(val):
    """損益フォーマット"""
    if val >= 0:
        return f"+{int(val):,}"
    return f"{int(val):,}"


def format_shares(val):
    """株数フォーマット"""
    if pd.isna(val):
        return '-'
    return f"{int(val):,}"


def calc_period_stats(df, mode='short'):
    """期間別の統計を計算"""
    if len(df) == 0:
        return {
            'count': 0,
            'seido_count': 0,
            'ichinichi_count': 0,
            'p1': 0,
            'p2': 0,
            'win1': 0,
            'win2': 0,
        }

    # margin_type_baseがある場合はそれを使用、なければmargin_typeを使用
    type_col = 'margin_type_base' if 'margin_type_base' in df.columns else 'margin_type'
    seido = df[df[type_col] == '制度信用']
    ichinichi = df[df[type_col] == 'いちにち信用']

    p1_col = f'{mode}_p1'
    p2_col = f'{mode}_p2'
    win1_col = f'{mode}_win1'
    win2_col = f'{mode}_win2'

    return {
        'count': len(df),
        'seido_count': len(seido),
        'ichinichi_count': len(ichinichi),
        'p1': df[p1_col].sum(),
        'p2': df[p2_col].sum(),
        'win1': df[win1_col].mean() * 100 if len(df) > 0 else 0,
        'win2': df[win2_col].mean() * 100 if len(df) > 0 else 0,
    }


def calc_period_stats_ex0(df, mode='short'):
    """期間別の統計を計算（除0株）"""
    if len(df) == 0:
        return {
            'count': 0,
            'seido_count': 0,
            'ichinichi_count': 0,
            'p1': 0,
            'p2': 0,
            'win1': 0,
            'win2': 0,
        }

    # margin_type_baseがある場合はそれを使用、なければmargin_typeを使用
    type_col = 'margin_type_base' if 'margin_type_base' in df.columns else 'margin_type'
    seido = df[df[type_col] == '制度信用']
    ichinichi = df[df[type_col] == 'いちにち信用']
    ichinichi_ex0 = ichinichi[
        (ichinichi['day_trade_available_shares'].isna()) |
        (ichinichi['day_trade_available_shares'] > 0)
    ]
    df_ex0 = pd.concat([seido, ichinichi_ex0])

    p1_col = f'{mode}_p1'
    p2_col = f'{mode}_p2'
    win1_col = f'{mode}_win1'
    win2_col = f'{mode}_win2'

    return {
        'count': len(df_ex0),
        'seido_count': len(seido),
        'ichinichi_count': len(ichinichi_ex0),
        'p1': df_ex0[p1_col].sum(),
        'p2': df_ex0[p2_col].sum(),
        'win1': df_ex0[win1_col].mean() * 100 if len(df_ex0) > 0 else 0,
        'win2': df_ex0[win2_col].mean() * 100 if len(df_ex0) > 0 else 0,
    }


def generate_html(df):
    """HTML生成"""

    # 信用区分を分類（3種類）
    # 制度信用: shortable=True
    # いちにち信用: day_trade=True and shortable=False and 株数>0 or NaN
    # いちにち信用(0株): day_trade=True and shortable=False and 株数=0
    def get_margin_type(r):
        if r.get('shortable', False):
            return '制度信用'
        # いちにち信用の場合、株数で分類
        shares = r.get('day_trade_available_shares')
        if pd.notna(shares) and shares == 0:
            return 'いちにち信用(0株)'
        return 'いちにち信用'

    df['margin_type'] = df.apply(get_margin_type, axis=1)
    # 集計用の基本区分（制度/いちにち）
    df['margin_type_base'] = df.apply(
        lambda r: '制度信用' if r.get('shortable', False) else 'いちにち信用',
        axis=1
    )

    # 曜日追加
    df['selection_date'] = pd.to_datetime(df['selection_date'])
    df['weekday'] = df['selection_date'].dt.weekday

    # 価格帯追加
    def get_price_range(price):
        for label, low, high in PRICE_RANGES:
            if low <= price < high:
                return label
        return '10,000円~'
    df['price_range'] = df['buy_price'].apply(get_price_range)

    # 空売り損益（符号反転）
    df['short_p1'] = -df['profit_per_100_shares_phase1']
    df['short_p2'] = -df['profit_per_100_shares_phase2']
    df['short_win1'] = ~df['phase1_win'].astype(bool)
    df['short_win2'] = ~df['phase2_win'].astype(bool)

    # ロング損益（そのまま）
    df['long_p1'] = df['profit_per_100_shares_phase1']
    df['long_p2'] = df['profit_per_100_shares_phase2']
    df['long_win1'] = df['phase1_win'].astype(bool)
    df['long_win2'] = df['phase2_win'].astype(bool)

    # 期間別データを計算
    max_date = df['selection_date'].max()

    # 日別 = 直近1日
    df_daily = df[df['selection_date'] == max_date]
    # 週別 = 直近5営業日
    df_weekly = df.nlargest(5 * 20, 'selection_date')  # 十分な件数を取得
    recent_5days = df['selection_date'].drop_duplicates().nlargest(5)
    df_weekly = df[df['selection_date'].isin(recent_5days)]
    # 月別 = 最新日が所属する月
    max_year_month = max_date.to_period('M')
    df_monthly = df[df['selection_date'].dt.to_period('M') == max_year_month]

    # 期間別統計（ショート）
    stats_short = {
        'daily': calc_period_stats(df_daily, 'short'),
        'weekly': calc_period_stats(df_weekly, 'short'),
        'monthly': calc_period_stats(df_monthly, 'short'),
        'all': calc_period_stats(df, 'short'),
        'daily_ex0': calc_period_stats_ex0(df_daily, 'short'),
        'weekly_ex0': calc_period_stats_ex0(df_weekly, 'short'),
        'monthly_ex0': calc_period_stats_ex0(df_monthly, 'short'),
        'all_ex0': calc_period_stats_ex0(df, 'short'),
    }

    # 期間別統計（ロング）
    stats_long = {
        'daily': calc_period_stats(df_daily, 'long'),
        'weekly': calc_period_stats(df_weekly, 'long'),
        'monthly': calc_period_stats(df_monthly, 'long'),
        'all': calc_period_stats(df, 'long'),
        'daily_ex0': calc_period_stats_ex0(df_daily, 'long'),
        'weekly_ex0': calc_period_stats_ex0(df_weekly, 'long'),
        'monthly_ex0': calc_period_stats_ex0(df_monthly, 'long'),
        'all_ex0': calc_period_stats_ex0(df, 'long'),
    }

    # 後方互換用
    stats_daily = stats_short['daily']
    stats_weekly = stats_short['weekly']
    stats_monthly = stats_short['monthly']
    stats_all = stats_short['all']
    stats_daily_ex0 = stats_short['daily_ex0']
    stats_weekly_ex0 = stats_short['weekly_ex0']
    stats_monthly_ex0 = stats_short['monthly_ex0']
    stats_all_ex0 = stats_short['all_ex0']

    # 全体集計（後方互換用）
    total_count = len(df)
    seido_count = len(df[df['margin_type_base'] == '制度信用'])
    ichinichi_count = len(df[df['margin_type_base'] == 'いちにち信用'])

    total_p1 = df['short_p1'].sum()
    total_p2 = df['short_p2'].sum()
    total_win1 = df['short_win1'].mean() * 100
    total_win2 = df['short_win2'].mean() * 100

    p1_class, p2_class = compare_class(total_p1, total_p2)

    # 除0株の全体集計（いちにち信用で0株を除外）
    df_ichinichi = df[df['margin_type_base'] == 'いちにち信用']
    df_ichinichi_ex0 = df_ichinichi[
        (df_ichinichi['day_trade_available_shares'].isna()) |
        (df_ichinichi['day_trade_available_shares'] > 0)
    ]
    df_seido = df[df['margin_type_base'] == '制度信用']
    df_ex0 = pd.concat([df_seido, df_ichinichi_ex0])

    ex0_total_count = len(df_ex0)
    ex0_seido_count = len(df_seido)
    ex0_ichinichi_count = len(df_ichinichi_ex0)
    ex0_p1 = df_ex0['short_p1'].sum()
    ex0_p2 = df_ex0['short_p2'].sum()
    ex0_win1 = df_ex0['short_win1'].mean() * 100
    ex0_win2 = df_ex0['short_win2'].mean() * 100
    ex0_p1_class, ex0_p2_class = compare_class(ex0_p1, ex0_p2)

    # HTML構築
    html_parts = []

    # ヘッダー
    html_parts.append(f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grok日別分析</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; padding: 24px; line-height: 1.5; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ font-size: 20px; font-weight: 600; margin-bottom: 4px; color: #e6edf3; }}
        h2 {{ font-size: 15px; font-weight: 600; margin: 32px 0 12px; color: #e6edf3; }}
        .subtitle {{ color: #7d8590; font-size: 12px; margin-bottom: 24px; }}

        .header-row {{ display: flex; align-items: center; gap: 16px; margin-bottom: 8px; }}
        .mode-tabs {{ display: flex; gap: 8px; }}
        .mode-tab {{ background: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 6px 20px; font-size: 14px; font-weight: 600; color: #7d8590; cursor: pointer; }}
        .mode-tab.active {{ background: #388bfd; border-color: #388bfd; color: #fff; }}
        .mode-tab:hover:not(.active) {{ background: #30363d; }}

        .top-cards {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 32px; }}
        .top-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; }}
        .top-card-header {{ display: flex; align-items: center; margin-bottom: 8px; }}
        .top-card .label {{ font-size: 12px; color: #7d8590; }}
        .top-card .top-tab-container {{ display: flex; gap: 4px; margin-left: auto; }}
        .top-card .top-tab-btn {{ background: #21262d; border: 1px solid #30363d; border-radius: 4px; padding: 2px 8px; font-size: 10px; color: #7d8590; cursor: pointer; }}
        .top-card .top-tab-btn.active {{ background: #388bfd; border-color: #388bfd; color: #fff; }}
        .top-card .top-tab-btn:hover:not(.active) {{ background: #30363d; }}

        .period-tabs {{ display: flex; gap: 8px; margin-bottom: 16px; }}
        .period-tab {{ background: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 6px 16px; font-size: 13px; color: #7d8590; cursor: pointer; }}
        .period-tab.active {{ background: #388bfd; border-color: #388bfd; color: #fff; }}
        .period-tab:hover:not(.active) {{ background: #30363d; }}
        .top-card .val {{ font-size: 28px; font-weight: 600; color: #e6edf3; font-variant-numeric: tabular-nums; text-align: right; }}
        .top-card .val.pos {{ color: #3fb950; }}
        .top-card .val.neg {{ color: #f85149; }}
        .top-card .sub {{ font-size: 12px; color: #7d8590; margin-top: 4px; text-align: right; }}
        .top-card .sub.pos {{ color: #3fb950; }}
        .top-card .sub.neg {{ color: #f85149; }}

        .card-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }}
        .card-header {{ font-weight: 600; font-size: 14px; color: #e6edf3; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}
        .card-count {{ font-size: 12px; color: #7d8590; font-weight: 400; }}
        .card-summary-row {{ display: flex; justify-content: flex-end; gap: 24px; margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #30363d; }}
        .sum-block {{ text-align: right; }}
        .sum-label {{ font-size: 11px; color: #7d8590; }}
        .sum-val {{ font-size: 18px; font-weight: 600; font-variant-numeric: tabular-nums; }}
        .sum-val.pos {{ color: #3fb950; }}
        .sum-val.neg {{ color: #f85149; }}

        .tab-container {{ display: flex; gap: 4px; margin-left: auto; }}
        .tab-btn {{ background: #21262d; border: 1px solid #30363d; border-radius: 4px; padding: 2px 8px; font-size: 11px; color: #7d8590; cursor: pointer; }}
        .tab-btn.active {{ background: #388bfd; border-color: #388bfd; color: #fff; }}
        .tab-btn:hover:not(.active) {{ background: #30363d; }}

        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid #30363d; color: #7d8590; font-weight: 500; }}
        th.r {{ text-align: right; }}
        td {{ padding: 8px 10px; border-bottom: 1px solid #21262d; color: #e6edf3; }}
        td.r {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .pos {{ color: #3fb950; }}
        .neg {{ color: #f85149; }}

        .daily-detail {{ margin: 8px 0; background: #161b22; border: 1px solid #30363d; border-radius: 6px; }}
        .daily-detail summary {{ padding: 12px 16px; cursor: pointer; display: flex; align-items: center; gap: 16px; font-size: 14px; }}
        .daily-detail summary:hover {{ background: #1c2128; }}
        .daily-detail .date {{ font-weight: 600; color: #e6edf3; }}
        .daily-detail .count {{ font-size: 13px; color: #7d8590; }}
        .daily-detail .summary-vals {{ margin-left: auto; font-size: 14px; font-variant-numeric: tabular-nums; }}
        .daily-detail .summary-vals .sum-label {{ color: #7d8590; margin-right: 4px; margin-left: 16px; }}
        .daily-detail .summary-vals .ex0-val {{ margin-left: 4px; font-size: 12px; }}
        .daily-detail table {{ margin: 0; }}
        .daily-detail[open] summary {{ border-bottom: 1px solid #30363d; }}

        h2.daily-header {{ margin-top: 40px; margin-bottom: 0; }}

        .detail-section {{ margin-top: 40px; }}
        .detail-header {{ display: flex; align-items: center; gap: 16px; margin-bottom: 16px; }}
        .detail-header h2 {{ margin: 0; }}
        .detail-tabs {{ display: flex; gap: 8px; }}
        .detail-tab {{ background: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 6px 16px; font-size: 13px; color: #7d8590; cursor: pointer; }}
        .detail-tab.active {{ background: #388bfd; border-color: #388bfd; color: #fff; }}
        .detail-tab:hover:not(.active) {{ background: #30363d; }}
        .detail-content {{ }}

        .hidden {{ display: none !important; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header-row">
            <h1>Grok日別分析</h1>
            <div class="mode-tabs">
                <button class="mode-tab active" onclick="switchMode('short')">ショート</button>
                <button class="mode-tab" onclick="switchMode('long')">ロング</button>
            </div>
        </div>
        <p class="subtitle">生成: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

        <div class="period-tabs">
            <button class="period-tab" onclick="switchPeriod('daily')">日別</button>
            <button class="period-tab" onclick="switchPeriod('weekly')">週別</button>
            <button class="period-tab" onclick="switchPeriod('monthly')">月別</button>
            <button class="period-tab active" onclick="switchPeriod('all')">全期間</button>
        </div>

        <div class="top-cards">
            <div class="top-card" id="top_count">
                <div class="top-card-header">
                    <div class="label">総件数</div>
                    <div class="top-tab-container">
                        <button class="top-tab-btn active" onclick="switchTopTab('all')">全数</button>
                        <button class="top-tab-btn" onclick="switchTopTab('ex0')">除0株</button>
                    </div>
                </div>
                <div class="val" id="top_count_val">{total_count}</div>
                <div class="sub" id="top_count_sub">制度{seido_count} / いちにち{ichinichi_count}</div>
            </div>
            <div class="top-card" id="top_p1">
                <div class="top-card-header">
                    <div class="label">前場引け</div>
                    <div class="top-tab-container">
                        <button class="top-tab-btn active" onclick="switchTopTab('all')">全数</button>
                        <button class="top-tab-btn" onclick="switchTopTab('ex0')">除0株</button>
                    </div>
                </div>
                <div class="val {p1_class}" id="top_p1_val">{format_profit(total_p1)}</div>
                <div class="sub {winrate_class(total_win1)}" id="top_p1_sub">勝率 {total_win1:.0f}%</div>
            </div>
            <div class="top-card" id="top_p2">
                <div class="top-card-header">
                    <div class="label">大引け</div>
                    <div class="top-tab-container">
                        <button class="top-tab-btn active" onclick="switchTopTab('all')">全数</button>
                        <button class="top-tab-btn" onclick="switchTopTab('ex0')">除0株</button>
                    </div>
                </div>
                <div class="val {p2_class}" id="top_p2_val">{format_profit(total_p2)}</div>
                <div class="sub {winrate_class(total_win2)}" id="top_p2_sub">勝率 {total_win2:.0f}%</div>
            </div>
        </div>
''')

    # 曜日別カードと詳細セクションをショート/ロング両方で生成
    for trade_mode in ['short', 'long']:
        p1_col = f'{trade_mode}_p1'
        p2_col = f'{trade_mode}_p2'
        win1_col = f'{trade_mode}_win1'
        win2_col = f'{trade_mode}_win2'

        hidden_class = '' if trade_mode == 'short' else ' hidden'
        html_parts.append(f'''
        <div id="content_{trade_mode}" class="trade-mode-content{hidden_class}">''')

        # 曜日別カード
        for weekday in range(5):
            weekday_name = WEEKDAY_NAMES[weekday]
            df_weekday = df[df['weekday'] == weekday]

            if len(df_weekday) == 0:
                continue

            html_parts.append(f'''
        <h2>{weekday_name}</h2>
        <div class="card-row">''')

            # 制度信用カード
            df_seido = df_weekday[df_weekday['margin_type_base'] == '制度信用']
            if len(df_seido) > 0:
                seido_p1 = df_seido[p1_col].sum()
                seido_p2 = df_seido[p2_col].sum()
            s_p1_class, s_p2_class = compare_class(seido_p1, seido_p2)

            html_parts.append(f'''
        <div class="card">
            <div class="card-header">制度信用 <span class="card-count">{len(df_seido)}件</span></div>
            <div class="card-summary-row">
                <div class="sum-block">
                    <div class="sum-label">前場</div>
                    <div class="sum-val {s_p1_class}">{format_profit(seido_p1)}</div>
                </div>
                <div class="sum-block">
                    <div class="sum-label">大引</div>
                    <div class="sum-val {s_p2_class}">{format_profit(seido_p2)}</div>
                </div>
            </div>
            <table>
        <tr><th class="r">価格帯</th><th class="r">件</th><th class="r">前場損益</th><th class="r">前場勝率</th><th class="r">大引損益</th><th class="r">大引勝率</th></tr>''')

            for label, low, high in PRICE_RANGES:
                df_range = df_seido[(df_seido['buy_price'] >= low) & (df_seido['buy_price'] < high)]
                if len(df_range) == 0:
                    continue

                rp1 = df_range[p1_col].sum()
                rp2 = df_range[p2_col].sum()
                rw1 = df_range[win1_col].mean() * 100
                rw2 = df_range[win2_col].mean() * 100
                rp1_class, rp2_class = compare_class(rp1, rp2)

                html_parts.append(f'''
        <tr>
            <td class="r">{label}</td>
            <td class="r">{len(df_range)}</td>
            <td class="r {rp1_class}">{format_profit(rp1)}</td>
            <td class="r {winrate_class(rw1)}">{rw1:.0f}%</td>
            <td class="r {rp2_class}">{format_profit(rp2)}</td>
            <td class="r {winrate_class(rw2)}">{rw2:.0f}%</td>
        </tr>''')

            html_parts.append('''
    </table>
        </div>''')
        else:
            html_parts.append('''
        <div class="card">
            <div class="card-header">制度信用 <span class="card-count">0件</span></div>
        </div>''')

        # いちにち信用カード（タブ付き）
        df_ichinichi_wd = df_weekday[df_weekday['margin_type_base'] == 'いちにち信用']
        df_ichinichi_wd_ex0 = df_ichinichi_wd[
            (df_ichinichi_wd['day_trade_available_shares'].isna()) |
            (df_ichinichi_wd['day_trade_available_shares'] > 0)
        ]

        if len(df_ichinichi_wd) > 0:
            # 全数の集計
            all_p1 = df_ichinichi_wd[p1_col].sum()
            all_p2 = df_ichinichi_wd[p2_col].sum()
            all_p1_class, all_p2_class = compare_class(all_p1, all_p2)

            # 除0株の集計（曜日別）
            wd_ex0_p1 = df_ichinichi_wd_ex0[p1_col].sum()
            wd_ex0_p2 = df_ichinichi_wd_ex0[p2_col].sum()
            wd_ex0_p1_class, wd_ex0_p2_class = compare_class(wd_ex0_p1, wd_ex0_p2)

            card_id = f"ichinichi_{trade_mode}_{weekday}"

            html_parts.append(f'''
        <div class="card" id="{card_id}">
            <div class="card-header">
                いちにち信用
                <span class="card-count" id="{card_id}_count">{len(df_ichinichi_wd)}件</span>
                <div class="tab-container">
                    <button class="tab-btn active" onclick="switchTab('{card_id}', 'all')">全数</button>
                    <button class="tab-btn" onclick="switchTab('{card_id}', 'ex0')">除0株</button>
                </div>
            </div>
            <div class="card-summary-row" id="{card_id}_summary_all">
                <div class="sum-block">
                    <div class="sum-label">前場</div>
                    <div class="sum-val {all_p1_class}">{format_profit(all_p1)}</div>
                </div>
                <div class="sum-block">
                    <div class="sum-label">大引</div>
                    <div class="sum-val {all_p2_class}">{format_profit(all_p2)}</div>
                </div>
            </div>
            <div class="card-summary-row hidden" id="{card_id}_summary_ex0">
                <div class="sum-block">
                    <div class="sum-label">前場</div>
                    <div class="sum-val {wd_ex0_p1_class}">{format_profit(wd_ex0_p1)}</div>
                </div>
                <div class="sum-block">
                    <div class="sum-label">大引</div>
                    <div class="sum-val {wd_ex0_p2_class}">{format_profit(wd_ex0_p2)}</div>
                </div>
            </div>
            <table id="{card_id}_table_all">
        <tr><th class="r">価格帯</th><th class="r">件</th><th class="r">株数</th><th class="r">前場損益</th><th class="r">前場勝率</th><th class="r">大引損益</th><th class="r">大引勝率</th></tr>''')

            # 全数テーブル
            for label, low, high in PRICE_RANGES:
                df_range = df_ichinichi_wd[(df_ichinichi_wd['buy_price'] >= low) & (df_ichinichi_wd['buy_price'] < high)]
                if len(df_range) == 0:
                    continue

                rp1 = df_range[p1_col].sum()
                rp2 = df_range[p2_col].sum()
                rw1 = df_range[win1_col].mean() * 100
                rw2 = df_range[win2_col].mean() * 100
                rp1_class, rp2_class = compare_class(rp1, rp2)

                # 株数: 有効データの合計
                shares_data = df_range['day_trade_available_shares'].dropna()
                shares_str = format_shares(shares_data.sum()) if len(shares_data) > 0 else '-'

                html_parts.append(f'''
        <tr>
            <td class="r">{label}</td>
            <td class="r">{len(df_range)}</td>
            <td class="r">{shares_str}</td>
            <td class="r {rp1_class}">{format_profit(rp1)}</td>
            <td class="r {winrate_class(rw1)}">{rw1:.0f}%</td>
            <td class="r {rp2_class}">{format_profit(rp2)}</td>
            <td class="r {winrate_class(rw2)}">{rw2:.0f}%</td>
        </tr>''')

            html_parts.append(f'''
    </table>
            <table class="hidden" id="{card_id}_table_ex0">
        <tr><th class="r">価格帯</th><th class="r">件</th><th class="r">株数</th><th class="r">前場損益</th><th class="r">前場勝率</th><th class="r">大引損益</th><th class="r">大引勝率</th></tr>''')

            # 除0株テーブル
            for label, low, high in PRICE_RANGES:
                df_range = df_ichinichi_wd_ex0[(df_ichinichi_wd_ex0['buy_price'] >= low) & (df_ichinichi_wd_ex0['buy_price'] < high)]
                if len(df_range) == 0:
                    continue

                rp1 = df_range[p1_col].sum()
                rp2 = df_range[p2_col].sum()
                rw1 = df_range[win1_col].mean() * 100 if len(df_range) > 0 else 0
                rw2 = df_range[win2_col].mean() * 100 if len(df_range) > 0 else 0
                rp1_class, rp2_class = compare_class(rp1, rp2)

                shares_data = df_range['day_trade_available_shares'].dropna()
                shares_str = format_shares(shares_data.sum()) if len(shares_data) > 0 else '-'

                html_parts.append(f'''
        <tr>
            <td class="r">{label}</td>
            <td class="r">{len(df_range)}</td>
            <td class="r">{shares_str}</td>
            <td class="r {rp1_class}">{format_profit(rp1)}</td>
            <td class="r {winrate_class(rw1)}">{rw1:.0f}%</td>
            <td class="r {rp2_class}">{format_profit(rp2)}</td>
            <td class="r {winrate_class(rw2)}">{rw2:.0f}%</td>
        </tr>''')

            html_parts.append(f'''
    </table>
        </div>''')
        else:
            html_parts.append('''
        <div class="card">
            <div class="card-header">いちにち信用 <span class="card-count">0件</span></div>
        </div>''')

        html_parts.append('''
        </div>''')

        # 詳細セクション（日別/週別/月別タブ付き）- trade_modeループ内
        html_parts.append(f'''
        <div class="detail-section">
            <div class="detail-header">
                <h2 class="daily-header">詳細（除0株損益）</h2>
                <div class="detail-tabs">
                    <button class="detail-tab active" onclick="switchDetailTab('{trade_mode}', 'daily')">日別</button>
                    <button class="detail-tab" onclick="switchDetailTab('{trade_mode}', 'weekly')">週別</button>
                    <button class="detail-tab" onclick="switchDetailTab('{trade_mode}', 'monthly')">月別</button>
                </div>
            </div>
        </div>''')

        # 週・月のキーを追加（初回のみ）
        if 'week_key' not in df.columns:
            df['week_key'] = df['selection_date'].dt.strftime('%Y/W%V')
            df['month_key'] = df['selection_date'].dt.strftime('%Y/%m')

        # 日別詳細
        html_parts.append(f'''
        <div id="detail_{trade_mode}_daily" class="detail-content">''')

        for date in sorted(df['selection_date'].unique(), reverse=True):
            df_date = df[df['selection_date'] == date]
            date_p1 = df_date[p1_col].sum()
            date_p2 = df_date[p2_col].sum()
            df_date_ex0 = df_date[df_date['margin_type'] != 'いちにち信用(0株)']
            date_p1_ex0 = df_date_ex0[p1_col].sum()
            date_p2_ex0 = df_date_ex0[p2_col].sum()
            dp1_class, dp2_class = compare_class(date_p1, date_p2)
            dp1_ex0_class, dp2_ex0_class = compare_class(date_p1_ex0, date_p2_ex0)
            date_str = pd.to_datetime(date).strftime('%Y-%m-%d')

            html_parts.append(f'''
        <details class="daily-detail">
            <summary>
                <span class="date">{date_str}</span>
                <span class="count">{len(df_date)}件</span>
                <span class="summary-vals">
                    <span class="sum-label">前場</span><span class="{dp1_class}">{format_profit(date_p1)}</span><span class="ex0-val {dp1_ex0_class}">({format_profit(date_p1_ex0)})</span>
                    <span class="sum-label">大引</span><span class="{dp2_class}">{format_profit(date_p2)}</span><span class="ex0-val {dp2_ex0_class}">({format_profit(date_p2_ex0)})</span>
                </span>
            </summary>
            <table>
                <tr><th>銘柄</th><th>区分</th><th class="r">買値</th><th class="r">株数</th><th class="r">前場損益</th><th class="r">大引損益</th></tr>''')

            for _, row in df_date.iterrows():
                rp1 = row[p1_col]
                rp2 = row[p2_col]
                rp1_class, rp2_class = compare_class(rp1, rp2)
                shares_str = format_shares(row.get('day_trade_available_shares'))

                html_parts.append(f'''
                <tr>
                    <td>{row['ticker']} {row.get('stock_name', '')}</td>
                    <td>{row['margin_type']}</td>
                    <td class="r">{row['buy_price']:,.0f}</td>
                    <td class="r">{shares_str}</td>
                    <td class="r {rp1_class}">{format_profit(rp1)}</td>
                    <td class="r {rp2_class}">{format_profit(rp2)}</td>
                </tr>''')

            html_parts.append('''
            </table>
        </details>''')

        html_parts.append('''
        </div>''')

        # 週別詳細
        html_parts.append(f'''
        <div id="detail_{trade_mode}_weekly" class="detail-content hidden">''')

        for week_key in sorted(df['week_key'].unique(), reverse=True):
            df_week = df[df['week_key'] == week_key]
            week_p1 = df_week[p1_col].sum()
            week_p2 = df_week[p2_col].sum()
            df_week_ex0 = df_week[df_week['margin_type'] != 'いちにち信用(0株)']
            week_p1_ex0 = df_week_ex0[p1_col].sum()
            week_p2_ex0 = df_week_ex0[p2_col].sum()
            wp1_class, wp2_class = compare_class(week_p1, week_p2)
            wp1_ex0_class, wp2_ex0_class = compare_class(week_p1_ex0, week_p2_ex0)

            html_parts.append(f'''
        <details class="daily-detail">
            <summary>
                <span class="date">{week_key}</span>
                <span class="count">{len(df_week)}件</span>
                <span class="summary-vals">
                    <span class="sum-label">前場</span><span class="{wp1_class}">{format_profit(week_p1)}</span><span class="ex0-val {wp1_ex0_class}">({format_profit(week_p1_ex0)})</span>
                    <span class="sum-label">大引</span><span class="{wp2_class}">{format_profit(week_p2)}</span><span class="ex0-val {wp2_ex0_class}">({format_profit(week_p2_ex0)})</span>
                </span>
            </summary>
            <table>
                <tr><th>銘柄</th><th>日付</th><th>区分</th><th class="r">買値</th><th class="r">株数</th><th class="r">前場損益</th><th class="r">大引損益</th></tr>''')

            for _, row in df_week.sort_values('selection_date', ascending=False).iterrows():
                rp1 = row[p1_col]
                rp2 = row[p2_col]
                rp1_class, rp2_class = compare_class(rp1, rp2)
                shares_str = format_shares(row.get('day_trade_available_shares'))
                row_date = pd.to_datetime(row['selection_date']).strftime('%m-%d')

                html_parts.append(f'''
                <tr>
                    <td>{row['ticker']} {row.get('stock_name', '')}</td>
                    <td>{row_date}</td>
                    <td>{row['margin_type']}</td>
                    <td class="r">{row['buy_price']:,.0f}</td>
                    <td class="r">{shares_str}</td>
                    <td class="r {rp1_class}">{format_profit(rp1)}</td>
                    <td class="r {rp2_class}">{format_profit(rp2)}</td>
                </tr>''')

            html_parts.append('''
            </table>
        </details>''')

        html_parts.append('''
        </div>''')

        # 月別詳細
        html_parts.append(f'''
        <div id="detail_{trade_mode}_monthly" class="detail-content hidden">''')

        for month_key in sorted(df['month_key'].unique(), reverse=True):
            df_month = df[df['month_key'] == month_key]
            month_p1 = df_month[p1_col].sum()
            month_p2 = df_month[p2_col].sum()
            df_month_ex0 = df_month[df_month['margin_type'] != 'いちにち信用(0株)']
            month_p1_ex0 = df_month_ex0[p1_col].sum()
            month_p2_ex0 = df_month_ex0[p2_col].sum()
            mp1_class, mp2_class = compare_class(month_p1, month_p2)
            mp1_ex0_class, mp2_ex0_class = compare_class(month_p1_ex0, month_p2_ex0)

            html_parts.append(f'''
        <details class="daily-detail">
            <summary>
                <span class="date">{month_key}</span>
                <span class="count">{len(df_month)}件</span>
                <span class="summary-vals">
                    <span class="sum-label">前場</span><span class="{mp1_class}">{format_profit(month_p1)}</span><span class="ex0-val {mp1_ex0_class}">({format_profit(month_p1_ex0)})</span>
                    <span class="sum-label">大引</span><span class="{mp2_class}">{format_profit(month_p2)}</span><span class="ex0-val {mp2_ex0_class}">({format_profit(month_p2_ex0)})</span>
                </span>
            </summary>
            <table>
                <tr><th>銘柄</th><th>日付</th><th>区分</th><th class="r">買値</th><th class="r">株数</th><th class="r">前場損益</th><th class="r">大引損益</th></tr>''')

            for _, row in df_month.sort_values('selection_date', ascending=False).iterrows():
                rp1 = row[p1_col]
                rp2 = row[p2_col]
                rp1_class, rp2_class = compare_class(rp1, rp2)
                shares_str = format_shares(row.get('day_trade_available_shares'))
                row_date = pd.to_datetime(row['selection_date']).strftime('%m-%d')

                html_parts.append(f'''
                <tr>
                    <td>{row['ticker']} {row.get('stock_name', '')}</td>
                    <td>{row_date}</td>
                    <td>{row['margin_type']}</td>
                    <td class="r">{row['buy_price']:,.0f}</td>
                    <td class="r">{shares_str}</td>
                    <td class="r {rp1_class}">{format_profit(rp1)}</td>
                    <td class="r {rp2_class}">{format_profit(rp2)}</td>
                </tr>''')

            html_parts.append('''
            </table>
        </details>''')

        html_parts.append('''
        </div>''')

        # trade_modeコンテンツの閉じタグ
        html_parts.append('''
        </div>''')

    # 期間別統計データを生成するヘルパー関数
    def make_period_data(stats, stats_ex0):
        p1_class, p2_class = compare_class(stats['p1'], stats['p2'])
        ex0_p1_class, ex0_p2_class = compare_class(stats_ex0['p1'], stats_ex0['p2'])
        return f'''{{
            all: {{
                count: {stats['count']},
                countSub: '制度{stats['seido_count']} / いちにち{stats['ichinichi_count']}',
                p1: '{format_profit(stats['p1'])}',
                p1Class: '{p1_class}',
                p1Win: '勝率 {stats['win1']:.0f}%',
                p1WinClass: '{winrate_class(stats['win1'])}',
                p2: '{format_profit(stats['p2'])}',
                p2Class: '{p2_class}',
                p2Win: '勝率 {stats['win2']:.0f}%',
                p2WinClass: '{winrate_class(stats['win2'])}'
            }},
            ex0: {{
                count: {stats_ex0['count']},
                countSub: '制度{stats_ex0['seido_count']} / いちにち{stats_ex0['ichinichi_count']}',
                p1: '{format_profit(stats_ex0['p1'])}',
                p1Class: '{ex0_p1_class}',
                p1Win: '勝率 {stats_ex0['win1']:.0f}%',
                p1WinClass: '{winrate_class(stats_ex0['win1'])}',
                p2: '{format_profit(stats_ex0['p2'])}',
                p2Class: '{ex0_p2_class}',
                p2Win: '勝率 {stats_ex0['win2']:.0f}%',
                p2WinClass: '{winrate_class(stats_ex0['win2'])}'
            }}
        }}'''

    # JavaScript（トップカード用のデータを埋め込み）
    html_parts.append(f'''
    </div>
    <script>
    // ショート/ロングデータ
    const modeData = {{
        short: {{
            daily: {make_period_data(stats_short['daily'], stats_short['daily_ex0'])},
            weekly: {make_period_data(stats_short['weekly'], stats_short['weekly_ex0'])},
            monthly: {make_period_data(stats_short['monthly'], stats_short['monthly_ex0'])},
            all: {make_period_data(stats_short['all'], stats_short['all_ex0'])}
        }},
        long: {{
            daily: {make_period_data(stats_long['daily'], stats_long['daily_ex0'])},
            weekly: {make_period_data(stats_long['weekly'], stats_long['weekly_ex0'])},
            monthly: {make_period_data(stats_long['monthly'], stats_long['monthly_ex0'])},
            all: {make_period_data(stats_long['all'], stats_long['all_ex0'])}
        }}
    }};

    // 現在の状態
    let currentTradeMode = 'short';  // 'short' or 'long'
    let currentPeriod = 'all';
    let currentFilterMode = 'all';  // 'all' or 'ex0'

    function switchMode(mode) {{
        currentTradeMode = mode;

        // モードタブのアクティブ状態切り替え
        document.querySelectorAll('.mode-tab').forEach(tab => {{
            if ((mode === 'short' && tab.textContent === 'ショート') ||
                (mode === 'long' && tab.textContent === 'ロング')) {{
                tab.classList.add('active');
            }} else {{
                tab.classList.remove('active');
            }}
        }});

        // トップカードの値を更新
        updateTopCards();
    }}

    function switchPeriod(period) {{
        currentPeriod = period;

        // 期間タブのアクティブ状態切り替え
        document.querySelectorAll('.period-tab').forEach(tab => {{
            const tabPeriod = tab.textContent === '日別' ? 'daily' :
                             tab.textContent === '週別' ? 'weekly' :
                             tab.textContent === '月別' ? 'monthly' : 'all';
            if (tabPeriod === period) {{
                tab.classList.add('active');
            }} else {{
                tab.classList.remove('active');
            }}
        }});

        // トップカードの値を更新
        updateTopCards();
    }}

    function switchTopTab(mode) {{
        currentFilterMode = mode;

        // 全てのトップタブを更新
        document.querySelectorAll('.top-tab-btn').forEach(btn => {{
            if ((mode === 'all' && btn.textContent === '全数') ||
                (mode === 'ex0' && btn.textContent === '除0株')) {{
                btn.classList.add('active');
            }} else {{
                btn.classList.remove('active');
            }}
        }});

        // トップカードの値を更新
        updateTopCards();
    }}

    function updateTopCards() {{
        const data = modeData[currentTradeMode][currentPeriod][currentFilterMode];

        // 総件数
        document.getElementById('top_count_val').textContent = data.count;
        document.getElementById('top_count_sub').textContent = data.countSub;

        // 前場引け
        const p1Val = document.getElementById('top_p1_val');
        p1Val.textContent = data.p1;
        p1Val.className = 'val ' + data.p1Class;
        const p1Sub = document.getElementById('top_p1_sub');
        p1Sub.textContent = data.p1Win;
        p1Sub.className = 'sub ' + data.p1WinClass;

        // 大引け
        const p2Val = document.getElementById('top_p2_val');
        p2Val.textContent = data.p2;
        p2Val.className = 'val ' + data.p2Class;
        const p2Sub = document.getElementById('top_p2_sub');
        p2Sub.textContent = data.p2Win;
        p2Sub.className = 'sub ' + data.p2WinClass;
    }}

    function switchTab(cardId, mode) {{
        const card = document.getElementById(cardId);
        const tabs = card.querySelectorAll('.tab-btn');

        // タブのアクティブ状態切り替え
        tabs.forEach(tab => {{
            if ((mode === 'all' && tab.textContent === '全数') ||
                (mode === 'ex0' && tab.textContent === '除0株')) {{
                tab.classList.add('active');
            }} else {{
                tab.classList.remove('active');
            }}
        }});

        // サマリーとテーブルの表示切り替え
        const summaryAll = document.getElementById(cardId + '_summary_all');
        const summaryEx0 = document.getElementById(cardId + '_summary_ex0');
        const tableAll = document.getElementById(cardId + '_table_all');
        const tableEx0 = document.getElementById(cardId + '_table_ex0');

        if (mode === 'all') {{
            summaryAll.classList.remove('hidden');
            summaryEx0.classList.add('hidden');
            tableAll.classList.remove('hidden');
            tableEx0.classList.add('hidden');
        }} else {{
            summaryAll.classList.add('hidden');
            summaryEx0.classList.remove('hidden');
            tableAll.classList.add('hidden');
            tableEx0.classList.remove('hidden');
        }}
    }}

    function switchDetailTab(tab) {{
        // タブのアクティブ状態切り替え
        document.querySelectorAll('.detail-tab').forEach(btn => {{
            const btnTab = btn.textContent === '日別' ? 'daily' :
                          btn.textContent === '週別' ? 'weekly' : 'monthly';
            if (btnTab === tab) {{
                btn.classList.add('active');
            }} else {{
                btn.classList.remove('active');
            }}
        }});

        // コンテンツの表示切り替え
        document.getElementById('detail_daily').classList.toggle('hidden', tab !== 'daily');
        document.getElementById('detail_weekly').classList.toggle('hidden', tab !== 'weekly');
        document.getElementById('detail_monthly').classList.toggle('hidden', tab !== 'monthly');
    }}
    </script>
</body>
</html>''')

    return ''.join(html_parts)


def main():
    print("=" * 60)
    print("Grok空売り日別分析HTML生成")
    print("=" * 60)

    # data/parquet/backtest/grok_trending_archive.parquet を使用
    archive_path = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
    df = pd.read_parquet(archive_path)
    print(f"  ✅ 読み込み完了: {len(df)} 件")

    # バックテストデータのみ（buy_priceがあるもの）
    df = df[df['buy_price'].notna()]
    print(f"  ✅ バックテストデータ: {len(df)} 件")

    # 2025-11-04以降のみ（それ以前はデータ品質の問題あり）
    df = df[df['selection_date'] >= '2025-11-04']
    print(f"  ✅ 2025-11-04以降: {len(df)} 件")

    # 制度信用 or いちにち信用のみ（それ以外は除外）
    df = df[(df['shortable'] == True) | ((df['day_trade'] == True) & (df['shortable'] == False))]
    print(f"  ✅ 制度+いちにち: {len(df)} 件")

    # HTML生成
    print()
    print("📊 HTML生成中...")
    html = generate_html(df)

    # 保存
    output_path = OUTPUT_DIR / "grok_day_trade_daily.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  ✅ 保存完了: {output_path}")
    print()
    print("=" * 60)
    print("完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
