"""
RSI/ATR閾値除外後の曜日別損益分析HTML生成

除外条件:
- 制度信用: RSI >= 70 または ATR >= 8%
- いちにち信用: RSI >= 90 または ATR >= 9%
"""
import pandas as pd
from pathlib import Path

WEEKDAY_NAMES = ['月曜日', '火曜日', '水曜日', '木曜日', '金曜日']

def load_archive():
    path = Path(__file__).parent.parent / 'data' / 'parquet' / 'backtest' / 'grok_trending_archive.parquet'
    df = pd.read_parquet(path)

    # buy_priceがあるもののみ（analysisと同じ）
    df = df[df['buy_price'].notna()].copy()

    # 2025-11-04以降（analysisと同じ）
    if 'selection_date' in df.columns:
        df['date'] = pd.to_datetime(df['selection_date'])
    elif 'backtest_date' in df.columns:
        df['date'] = pd.to_datetime(df['backtest_date'])
    df = df[df['date'] >= '2025-11-04']

    # 制度信用 or いちにち信用のみ（analysisと同じ）
    df = df[(df['shortable'] == True) | ((df['day_trade'] == True) & (df['shortable'] == False))]

    # 極端相場除外（analysisと同じ: is_extreme_market == False のみ）
    df = df[df['is_extreme_market'] == False]

    # 曜日
    df['weekday'] = df['date'].dt.weekday

    return df


def calc_stats(target_df: pd.DataFrame) -> dict:
    """損益統計を計算（ショート戦略: 符号反転）"""
    if len(target_df) == 0:
        return {'count': 0, 'me': 0, 'p1': 0, 'ae': 0, 'p2': 0}
    return {
        'count': len(target_df),
        'me': int(-target_df['profit_per_100_shares_morning_early'].sum()),
        'p1': int(-target_df['profit_per_100_shares_phase1'].sum()),
        'ae': int(-target_df['profit_per_100_shares_afternoon_early'].sum()),
        'p2': int(-target_df['profit_per_100_shares_phase2'].sum()),
    }


def calc_weekday_stats(df: pd.DataFrame) -> list:
    """曜日別統計を計算（全パターン）"""
    results = []

    for wd in range(5):
        wd_df = df[df['weekday'] == wd]

        # 制度信用
        seido_df = wd_df[wd_df['shortable'] == True]
        seido_rsi_hit = seido_df[(seido_df['rsi9'] >= 70) & ~(seido_df['atr14_pct'] >= 8)]  # RSIのみ該当
        seido_atr_hit = seido_df[~(seido_df['rsi9'] >= 70) & (seido_df['atr14_pct'] >= 8)]  # ATRのみ該当
        seido_both_hit = seido_df[(seido_df['rsi9'] >= 70) & (seido_df['atr14_pct'] >= 8)]  # 両方該当
        seido_rsi_atr = seido_df[~((seido_df['rsi9'] >= 70) | (seido_df['atr14_pct'] >= 8))]  # 除外後

        # いちにち信用
        ichi_df = wd_df[(wd_df['shortable'] == False) & (wd_df['day_trade'] == True)]
        ichi_rsi_hit = ichi_df[(ichi_df['rsi9'] >= 90) & ~(ichi_df['atr14_pct'] >= 9)]  # RSIのみ該当
        ichi_atr_hit = ichi_df[~(ichi_df['rsi9'] >= 90) & (ichi_df['atr14_pct'] >= 9)]  # ATRのみ該当
        ichi_both_hit = ichi_df[(ichi_df['rsi9'] >= 90) & (ichi_df['atr14_pct'] >= 9)]  # 両方該当
        ichi_rsi_atr = ichi_df[~((ichi_df['rsi9'] >= 90) | (ichi_df['atr14_pct'] >= 9))]  # 除外後

        # いちにち信用（除0）: 株数0を除外、NaNはそのまま
        ichi0_df = ichi_df[~(ichi_df['day_trade_available_shares'] == 0)]
        ichi0_rsi_hit = ichi0_df[(ichi0_df['rsi9'] >= 90) & ~(ichi0_df['atr14_pct'] >= 9)]  # RSIのみ該当
        ichi0_atr_hit = ichi0_df[~(ichi0_df['rsi9'] >= 90) & (ichi0_df['atr14_pct'] >= 9)]  # ATRのみ該当
        ichi0_both_hit = ichi0_df[(ichi0_df['rsi9'] >= 90) & (ichi0_df['atr14_pct'] >= 9)]  # 両方該当
        ichi0_rsi_atr = ichi0_df[~((ichi0_df['rsi9'] >= 90) | (ichi0_df['atr14_pct'] >= 9))]

        # 統計計算
        seido_all = calc_stats(seido_df)
        seido_rsi = calc_stats(seido_rsi_hit)
        seido_atr = calc_stats(seido_atr_hit)
        seido_both = calc_stats(seido_both_hit)
        seido_excluded = calc_stats(seido_rsi_atr)
        seido_diff = {
            'count': seido_all['count'] - seido_excluded['count'],
            'me': seido_all['me'] - seido_excluded['me'],
            'p1': seido_all['p1'] - seido_excluded['p1'],
            'ae': seido_all['ae'] - seido_excluded['ae'],
            'p2': seido_all['p2'] - seido_excluded['p2'],
        }

        ichi_all = calc_stats(ichi_df)
        ichi_rsi = calc_stats(ichi_rsi_hit)
        ichi_atr = calc_stats(ichi_atr_hit)
        ichi_both = calc_stats(ichi_both_hit)
        ichi_excluded = calc_stats(ichi_rsi_atr)
        ichi_diff = {
            'count': ichi_all['count'] - ichi_excluded['count'],
            'me': ichi_all['me'] - ichi_excluded['me'],
            'p1': ichi_all['p1'] - ichi_excluded['p1'],
            'ae': ichi_all['ae'] - ichi_excluded['ae'],
            'p2': ichi_all['p2'] - ichi_excluded['p2'],
        }

        # いちにち（除0）
        ichi0_all = calc_stats(ichi0_df)
        ichi0_rsi = calc_stats(ichi0_rsi_hit)
        ichi0_atr = calc_stats(ichi0_atr_hit)
        ichi0_both = calc_stats(ichi0_both_hit)
        ichi0_excluded = calc_stats(ichi0_rsi_atr)
        ichi0_diff = {
            'count': ichi0_all['count'] - ichi0_excluded['count'],
            'me': ichi0_all['me'] - ichi0_excluded['me'],
            'p1': ichi0_all['p1'] - ichi0_excluded['p1'],
            'ae': ichi0_all['ae'] - ichi0_excluded['ae'],
            'p2': ichi0_all['p2'] - ichi0_excluded['p2'],
        }

        results.append({
            'weekday': WEEKDAY_NAMES[wd],
            'seido': {
                'all': seido_all,
                'rsi': seido_rsi,
                'atr': seido_atr,
                'both': seido_both,
                'excluded': seido_excluded,
                'diff': seido_diff,
            },
            'ichinichi': {
                'all': ichi_all,
                'rsi': ichi_rsi,
                'atr': ichi_atr,
                'both': ichi_both,
                'excluded': ichi_excluded,
                'diff': ichi_diff,
            },
            'ichinichi0': {
                'all': ichi0_all,
                'rsi': ichi0_rsi,
                'atr': ichi0_atr,
                'both': ichi0_both,
                'excluded': ichi0_excluded,
                'diff': ichi0_diff,
            }
        })

    return results


def format_profit(val: int) -> str:
    """損益をフォーマット"""
    if val > 0:
        return f'<span class="positive">+{val:,}</span>'
    elif val < 0:
        return f'<span class="negative">{val:,}</span>'
    else:
        return f'<span class="zero">0</span>'


def sum_stats(weekday_stats: list, credit_type: str, stat_type: str) -> dict:
    """全曜日の合計を計算"""
    return {
        'count': sum(w[credit_type][stat_type]['count'] for w in weekday_stats),
        'me': sum(w[credit_type][stat_type]['me'] for w in weekday_stats),
        'p1': sum(w[credit_type][stat_type]['p1'] for w in weekday_stats),
        'ae': sum(w[credit_type][stat_type]['ae'] for w in weekday_stats),
        'p2': sum(w[credit_type][stat_type]['p2'] for w in weekday_stats),
    }


def generate_stat_row(label: str, stats: dict, label_class: str = "") -> str:
    """統計行のHTMLを生成"""
    label_style = f' class="{label_class}"' if label_class else ''
    return f'''
        <tr>
            <td{label_style}>{label}</td>
            <td class="num">{stats['count']}</td>
            <td class="num">{format_profit(stats['me'])}</td>
            <td class="num">{format_profit(stats['p1'])}</td>
            <td class="num">{format_profit(stats['ae'])}</td>
            <td class="num">{format_profit(stats['p2'])}</td>
        </tr>'''


def add_stats(a: dict, b: dict) -> dict:
    """2つのstatsを加算"""
    return {
        'count': a['count'] + b['count'],
        'me': a['me'] + b['me'],
        'p1': a['p1'] + b['p1'],
        'ae': a['ae'] + b['ae'],
        'p2': a['p2'] + b['p2'],
    }


def generate_html(weekday_stats: list) -> str:
    """HTMLを生成"""

    # 全体集計
    totals = {
        'seido': {
            'all': sum_stats(weekday_stats, 'seido', 'all'),
            'rsi': sum_stats(weekday_stats, 'seido', 'rsi'),
            'atr': sum_stats(weekday_stats, 'seido', 'atr'),
            'both': sum_stats(weekday_stats, 'seido', 'both'),
            'excluded': sum_stats(weekday_stats, 'seido', 'excluded'),
            'diff': sum_stats(weekday_stats, 'seido', 'diff'),
        },
        'ichinichi': {
            'all': sum_stats(weekday_stats, 'ichinichi', 'all'),
            'rsi': sum_stats(weekday_stats, 'ichinichi', 'rsi'),
            'atr': sum_stats(weekday_stats, 'ichinichi', 'atr'),
            'both': sum_stats(weekday_stats, 'ichinichi', 'both'),
            'excluded': sum_stats(weekday_stats, 'ichinichi', 'excluded'),
            'diff': sum_stats(weekday_stats, 'ichinichi', 'diff'),
        },
        'ichinichi0': {
            'all': sum_stats(weekday_stats, 'ichinichi0', 'all'),
            'rsi': sum_stats(weekday_stats, 'ichinichi0', 'rsi'),
            'atr': sum_stats(weekday_stats, 'ichinichi0', 'atr'),
            'both': sum_stats(weekday_stats, 'ichinichi0', 'both'),
            'excluded': sum_stats(weekday_stats, 'ichinichi0', 'excluded'),
            'diff': sum_stats(weekday_stats, 'ichinichi0', 'diff'),
        }
    }

    # 制度+いちにち合計（積み上げ）
    combined = {
        'all': add_stats(totals['seido']['all'], totals['ichinichi']['all']),
        'excluded': add_stats(totals['seido']['excluded'], totals['ichinichi']['excluded']),
    }
    combined_rsi = add_stats(totals['seido']['rsi'], totals['ichinichi']['rsi'])
    combined_atr = add_stats(totals['seido']['atr'], totals['ichinichi']['atr'])
    combined_both = add_stats(totals['seido']['both'], totals['ichinichi']['both'])

    # 除0版
    combined_ex0 = {
        'all': add_stats(totals['seido']['all'], totals['ichinichi0']['all']),
        'excluded': add_stats(totals['seido']['excluded'], totals['ichinichi0']['excluded']),
    }
    combined_ex0_rsi = combined_rsi  # RSIのみは除0なし（通常版と同じ）
    combined_ex0_atr = add_stats(totals['seido']['atr'], totals['ichinichi0']['atr'])
    combined_ex0_both = add_stats(totals['seido']['both'], totals['ichinichi0']['both'])

    html = '''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>RSI/ATR除外 曜日別損益分析</title>
<style>
body {
    background-color: #1a1a2e;
    color: #eee;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    padding: 20px;
    max-width: 1600px;
    margin: 0 auto;
}
h1 {
    color: #fff;
    margin-bottom: 10px;
}
h2 {
    color: #fff;
    margin-top: 40px;
    margin-bottom: 15px;
    font-size: 18px;
}
.subtitle {
    color: #888;
    font-size: 14px;
    margin-bottom: 30px;
}
.legend {
    background-color: #252540;
    padding: 15px;
    border-radius: 8px;
    margin-bottom: 25px;
    font-size: 13px;
}
.legend-title {
    font-weight: 600;
    margin-bottom: 8px;
}
.legend-item {
    margin-bottom: 4px;
    color: #aaa;
}
.tables-container {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 20px;
    margin-bottom: 30px;
}
.table-section {
    background: linear-gradient(135deg, #252540 0%, #1e1e35 100%);
    border-radius: 12px;
    border: 1px solid #333;
    padding: 20px;
}
.table-section h3 {
    margin: 0 0 15px 0;
    font-size: 16px;
}
.table-section.seido h3 {
    color: #74c0fc;
}
.table-section.ichinichi h3 {
    color: #ffd43b;
}
.table-section.ichinichi0 h3 {
    color: #ff922b;
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}
th, td {
    padding: 8px 10px;
    text-align: left;
    border-bottom: 1px solid #333;
}
th {
    color: #888;
    font-weight: 500;
    font-size: 12px;
}
td.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}
.row-all { background-color: rgba(255,255,255,0.03); }
.row-rsi { }
.row-atr { }
.row-both-hit { background-color: rgba(177, 98, 252, 0.1); }
.row-excluded { background-color: rgba(81, 207, 102, 0.1); }
.row-diff { background-color: rgba(255, 107, 107, 0.1); }
.label-all { color: #fff; }
.label-rsi { color: #74c0fc; }
.label-atr { color: #ffa94d; }
.label-both-hit { color: #b162fc; }
.label-excluded { color: #51cf66; font-weight: 600; }
.label-diff { color: #ff6b6b; }
.positive { color: #51cf66; }
.negative { color: #ff6b6b; }
.zero { color: #666; }
.weekday-section {
    margin-bottom: 30px;
}
.weekday-title {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 15px;
    padding-bottom: 8px;
    border-bottom: 1px solid #333;
}
.summary-table-container {
    background: linear-gradient(135deg, #252540 0%, #1e1e35 100%);
    border-radius: 12px;
    border: 1px solid #444;
    padding: 20px;
    margin-bottom: 20px;
    max-width: 600px;
}
.summary-table-container.ex0 {
    border: 1px solid #ff922b;
}
.ex0-label {
    color: #ff922b;
    font-size: 12px;
    margin-bottom: 10px;
}
.summary-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
}
.summary-table th {
    color: #888;
    font-weight: 500;
    font-size: 12px;
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid #444;
}
.summary-table td {
    padding: 10px 12px;
    border-bottom: 1px solid #333;
}
.summary-table td.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}
</style>
</head>
<body>
<h1>RSI/ATR除外 曜日別損益分析</h1>
<div class="subtitle">ショート戦略 - 各条件での損益比較</div>

<div class="legend">
    <div class="legend-title">除外条件</div>
    <div class="legend-item">制度信用: RSI ≥ 70 または ATR ≥ 8%</div>
    <div class="legend-item">いちにち信用: RSI ≥ 90 または ATR ≥ 9%</div>
</div>

<h2>サマリー（制度+いちにち合計）</h2>
<div class="summary-table-container">
    <table class="summary-table">
        <thead>
            <tr>
                <th>条件</th>
                <th>件数</th>
                <th>10:25</th>
                <th>前引</th>
                <th>14:45</th>
                <th>大引</th>
            </tr>
        </thead>
        <tbody>
            <tr class="row-all">
                <td class="label-all">全体</td>
                <td class="num">''' + str(combined['all']['count']) + '''</td>
                <td class="num">''' + format_profit(combined['all']['me']) + '''</td>
                <td class="num">''' + format_profit(combined['all']['p1']) + '''</td>
                <td class="num">''' + format_profit(combined['all']['ae']) + '''</td>
                <td class="num">''' + format_profit(combined['all']['p2']) + '''</td>
            </tr>
            <tr class="row-rsi">
                <td class="label-rsi">RSIのみ</td>
                <td class="num">''' + str(combined_rsi['count']) + '''</td>
                <td class="num">''' + format_profit(combined_rsi['me']) + '''</td>
                <td class="num">''' + format_profit(combined_rsi['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_rsi['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_rsi['p2']) + '''</td>
            </tr>
            <tr class="row-atr">
                <td class="label-atr">ATRのみ</td>
                <td class="num">''' + str(combined_atr['count']) + '''</td>
                <td class="num">''' + format_profit(combined_atr['me']) + '''</td>
                <td class="num">''' + format_profit(combined_atr['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_atr['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_atr['p2']) + '''</td>
            </tr>
            <tr class="row-both-hit">
                <td class="label-both-hit">両方</td>
                <td class="num">''' + str(combined_both['count']) + '''</td>
                <td class="num">''' + format_profit(combined_both['me']) + '''</td>
                <td class="num">''' + format_profit(combined_both['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_both['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_both['p2']) + '''</td>
            </tr>
            <tr class="row-excluded">
                <td class="label-excluded">除外後</td>
                <td class="num">''' + str(combined['excluded']['count']) + '''</td>
                <td class="num">''' + format_profit(combined['excluded']['me']) + '''</td>
                <td class="num">''' + format_profit(combined['excluded']['p1']) + '''</td>
                <td class="num">''' + format_profit(combined['excluded']['ae']) + '''</td>
                <td class="num">''' + format_profit(combined['excluded']['p2']) + '''</td>
            </tr>
        </tbody>
    </table>
</div>
<div class="summary-table-container ex0">
    <div class="ex0-label">除0版（RSIのみは除0なし）</div>
    <table class="summary-table">
        <thead>
            <tr>
                <th>条件</th>
                <th>件数</th>
                <th>10:25</th>
                <th>前引</th>
                <th>14:45</th>
                <th>大引</th>
            </tr>
        </thead>
        <tbody>
            <tr class="row-all">
                <td class="label-all">全体</td>
                <td class="num">''' + str(combined_ex0['all']['count']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['all']['me']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['all']['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['all']['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['all']['p2']) + '''</td>
            </tr>
            <tr class="row-rsi">
                <td class="label-rsi">RSIのみ</td>
                <td class="num">''' + str(combined_ex0_rsi['count']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_rsi['me']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_rsi['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_rsi['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_rsi['p2']) + '''</td>
            </tr>
            <tr class="row-atr">
                <td class="label-atr">ATRのみ</td>
                <td class="num">''' + str(combined_ex0_atr['count']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_atr['me']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_atr['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_atr['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_atr['p2']) + '''</td>
            </tr>
            <tr class="row-both-hit">
                <td class="label-both-hit">両方</td>
                <td class="num">''' + str(combined_ex0_both['count']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_both['me']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_both['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_both['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0_both['p2']) + '''</td>
            </tr>
            <tr class="row-excluded">
                <td class="label-excluded">除外後</td>
                <td class="num">''' + str(combined_ex0['excluded']['count']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['excluded']['me']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['excluded']['p1']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['excluded']['ae']) + '''</td>
                <td class="num">''' + format_profit(combined_ex0['excluded']['p2']) + '''</td>
            </tr>
        </tbody>
    </table>
</div>

<h2>全体合計</h2>
<div class="tables-container">
    <div class="table-section seido">
        <h3>制度信用</h3>
        <table>
            <thead>
                <tr>
                    <th>条件</th>
                    <th>件数</th>
                    <th>10:25</th>
                    <th>前引</th>
                    <th>14:45</th>
                    <th>大引</th>
                </tr>
            </thead>
            <tbody>'''

    # 制度信用合計
    html += generate_stat_row('全データ', totals['seido']['all'], 'label-all')
    html += generate_stat_row('RSIのみ', totals['seido']['rsi'], 'label-rsi')
    html += generate_stat_row('ATRのみ', totals['seido']['atr'], 'label-atr')
    html += generate_stat_row('両方', totals['seido']['both'], 'label-both-hit')
    html += generate_stat_row('除外後', totals['seido']['excluded'], 'label-excluded')
    html += generate_stat_row('差額', totals['seido']['diff'], 'label-diff')

    html += '''
            </tbody>
        </table>
    </div>
    <div class="table-section ichinichi">
        <h3>いちにち信用</h3>
        <table>
            <thead>
                <tr>
                    <th>条件</th>
                    <th>件数</th>
                    <th>10:25</th>
                    <th>前引</th>
                    <th>14:45</th>
                    <th>大引</th>
                </tr>
            </thead>
            <tbody>'''

    # いちにち信用合計
    html += generate_stat_row('全データ', totals['ichinichi']['all'], 'label-all')
    html += generate_stat_row('RSIのみ', totals['ichinichi']['rsi'], 'label-rsi')
    html += generate_stat_row('ATRのみ', totals['ichinichi']['atr'], 'label-atr')
    html += generate_stat_row('両方', totals['ichinichi']['both'], 'label-both-hit')
    html += generate_stat_row('除外後', totals['ichinichi']['excluded'], 'label-excluded')
    html += generate_stat_row('差額', totals['ichinichi']['diff'], 'label-diff')

    html += '''
            </tbody>
        </table>
    </div>
    <div class="table-section ichinichi0">
        <h3>いちにち信用（除0）</h3>
        <table>
            <thead>
                <tr>
                    <th>条件</th>
                    <th>件数</th>
                    <th>10:25</th>
                    <th>前引</th>
                    <th>14:45</th>
                    <th>大引</th>
                </tr>
            </thead>
            <tbody>'''

    # いちにち信用（除0）合計
    html += generate_stat_row('全データ', totals['ichinichi0']['all'], 'label-all')
    html += generate_stat_row('RSIのみ', totals['ichinichi0']['rsi'], 'label-rsi')
    html += generate_stat_row('ATRのみ', totals['ichinichi0']['atr'], 'label-atr')
    html += generate_stat_row('両方', totals['ichinichi0']['both'], 'label-both-hit')
    html += generate_stat_row('除外後', totals['ichinichi0']['excluded'], 'label-excluded')
    html += generate_stat_row('差額', totals['ichinichi0']['diff'], 'label-diff')

    html += '''
            </tbody>
        </table>
    </div>
</div>
'''

    # 曜日別
    for w in weekday_stats:
        html += f'''
<div class="weekday-section">
    <div class="weekday-title">{w['weekday']}</div>
    <div class="tables-container">
        <div class="table-section seido">
            <h3>制度信用</h3>
            <table>
                <thead>
                    <tr>
                        <th>条件</th>
                        <th>件数</th>
                        <th>10:25</th>
                        <th>前引</th>
                        <th>14:45</th>
                        <th>大引</th>
                    </tr>
                </thead>
                <tbody>'''
        html += generate_stat_row('全データ', w['seido']['all'], 'label-all')
        html += generate_stat_row('RSIのみ', w['seido']['rsi'], 'label-rsi')
        html += generate_stat_row('ATRのみ', w['seido']['atr'], 'label-atr')
        html += generate_stat_row('両方', w['seido']['both'], 'label-both-hit')
        html += generate_stat_row('除外後', w['seido']['excluded'], 'label-excluded')
        html += generate_stat_row('差額', w['seido']['diff'], 'label-diff')

        html += f'''
                </tbody>
            </table>
        </div>
        <div class="table-section ichinichi">
            <h3>いちにち信用</h3>
            <table>
                <thead>
                    <tr>
                        <th>条件</th>
                        <th>件数</th>
                        <th>10:25</th>
                        <th>前引</th>
                        <th>14:45</th>
                        <th>大引</th>
                    </tr>
                </thead>
                <tbody>'''
        html += generate_stat_row('全データ', w['ichinichi']['all'], 'label-all')
        html += generate_stat_row('RSIのみ', w['ichinichi']['rsi'], 'label-rsi')
        html += generate_stat_row('ATRのみ', w['ichinichi']['atr'], 'label-atr')
        html += generate_stat_row('両方', w['ichinichi']['both'], 'label-both-hit')
        html += generate_stat_row('除外後', w['ichinichi']['excluded'], 'label-excluded')
        html += generate_stat_row('差額', w['ichinichi']['diff'], 'label-diff')

        html += f'''
                </tbody>
            </table>
        </div>
        <div class="table-section ichinichi0">
            <h3>いちにち信用（除0）</h3>
            <table>
                <thead>
                    <tr>
                        <th>条件</th>
                        <th>件数</th>
                        <th>10:25</th>
                        <th>前引</th>
                        <th>14:45</th>
                        <th>大引</th>
                    </tr>
                </thead>
                <tbody>'''
        html += generate_stat_row('全データ', w['ichinichi0']['all'], 'label-all')
        html += generate_stat_row('RSIのみ', w['ichinichi0']['rsi'], 'label-rsi')
        html += generate_stat_row('ATRのみ', w['ichinichi0']['atr'], 'label-atr')
        html += generate_stat_row('両方', w['ichinichi0']['both'], 'label-both-hit')
        html += generate_stat_row('除外後', w['ichinichi0']['excluded'], 'label-excluded')
        html += generate_stat_row('差額', w['ichinichi0']['diff'], 'label-diff')

        html += '''
                </tbody>
            </table>
        </div>
    </div>
</div>
'''

    html += '''
</body>
</html>
'''
    return html


def main():
    df = load_archive()
    print(f"データ読み込み: {len(df)}件")

    weekday_stats = calc_weekday_stats(df)

    html = generate_html(weekday_stats)

    output_path = Path(__file__).parent / 'output' / 'excluded_weekday_analysis.html'
    output_path.write_text(html, encoding='utf-8')
    print(f"HTML生成完了: {output_path}")


if __name__ == '__main__':
    main()
