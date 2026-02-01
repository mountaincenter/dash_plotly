"""
曜日 × RSI/ATR条件 × 価格帯 の損益分析HTML生成

全ショート戦略で以下の4セグメントを価格帯別に分析:
1. RSIのみ該当
2. ATRのみ該当
3. 両方該当
4. 除外後（どちらも非該当）

analysisページと同じ粒度・スタイリング
"""
import pandas as pd
from pathlib import Path

WEEKDAY_NAMES = ['月曜日', '火曜日', '水曜日', '木曜日', '金曜日']
PRICE_RANGES = [
    ('~1,000円', 0, 1000),
    ('1,000~3,000円', 1000, 3000),
    ('3,000~5,000円', 3000, 5000),
    ('5,000~10,000円', 5000, 10000),
    ('10,000円~', 10000, float('inf')),
]

SEIDO_RSI = 70
SEIDO_ATR = 8
ICHI_RSI = 90
ICHI_ATR = 9


def get_price_range(price: float) -> str:
    if pd.isna(price):
        return '不明'
    for label, low, high in PRICE_RANGES:
        if low <= price < high:
            return label
    return '不明'


def load_archive():
    path = Path(__file__).parent.parent / 'data' / 'parquet' / 'backtest' / 'grok_trending_archive.parquet'
    df = pd.read_parquet(path)
    df = df[df['buy_price'].notna()].copy()

    if 'selection_date' in df.columns:
        df['date'] = pd.to_datetime(df['selection_date'])
    elif 'backtest_date' in df.columns:
        df['date'] = pd.to_datetime(df['backtest_date'])
    df = df[df['date'] >= '2025-11-04']

    df = df[(df['shortable'] == True) | ((df['day_trade'] == True) & (df['shortable'] == False))]
    df = df[df['is_extreme_market'] == False]

    df['weekday'] = df['date'].dt.weekday
    df['price_range'] = df['prev_close'].apply(get_price_range)

    df['is_seido'] = df['shortable'] == True
    df['rsi_hit'] = ((df['is_seido']) & (df['rsi9'] >= SEIDO_RSI)) | ((~df['is_seido']) & (df['rsi9'] >= ICHI_RSI))
    df['atr_hit'] = ((df['is_seido']) & (df['atr14_pct'] >= SEIDO_ATR)) | ((~df['is_seido']) & (df['atr14_pct'] >= ICHI_ATR))

    df['segment'] = 'excluded'
    df.loc[df['rsi_hit'] & ~df['atr_hit'], 'segment'] = 'rsi_only'
    df.loc[~df['rsi_hit'] & df['atr_hit'], 'segment'] = 'atr_only'
    df.loc[df['rsi_hit'] & df['atr_hit'], 'segment'] = 'both'

    return df


def calc_stats(target_df: pd.DataFrame) -> dict:
    """損益統計を計算（ショート戦略: 符号反転）+ 勝率"""
    if len(target_df) == 0:
        return {'count': 0, 'me': 0, 'p1': 0, 'ae': 0, 'p2': 0,
                'win_me': 0, 'win_p1': 0, 'win_ae': 0, 'win_p2': 0}

    # ショートなので符号反転後にプラスなら勝ち
    me_vals = -target_df['profit_per_100_shares_morning_early']
    p1_vals = -target_df['profit_per_100_shares_phase1']
    ae_vals = -target_df['profit_per_100_shares_afternoon_early']
    p2_vals = -target_df['profit_per_100_shares_phase2']

    n = len(target_df)
    return {
        'count': n,
        'me': int(me_vals.sum()),
        'p1': int(p1_vals.sum()),
        'ae': int(ae_vals.sum()),
        'p2': int(p2_vals.sum()),
        'win_me': round((me_vals > 0).sum() / n * 100) if n > 0 else 0,
        'win_p1': round((p1_vals > 0).sum() / n * 100) if n > 0 else 0,
        'win_ae': round((ae_vals > 0).sum() / n * 100) if n > 0 else 0,
        'win_p2': round((p2_vals > 0).sum() / n * 100) if n > 0 else 0,
    }


def profit_class(val: int) -> str:
    if val > 0:
        return 'text-emerald-400'
    elif val < 0:
        return 'text-rose-400'
    return 'text-foreground'


def winrate_class(rate: int) -> str:
    if rate > 50:
        return 'text-emerald-400'
    elif rate < 50:
        return 'text-rose-400'
    return 'text-foreground'


def format_profit(val: int) -> str:
    sign = '+' if val >= 0 else ''
    return f'{sign}{val:,}'


def get_quadrant_classes(me: int, p1: int, ae: int, p2: int) -> tuple:
    """4区分の色分け"""
    values = [me, p1, ae, p2]
    positives = [v for v in values if v > 0]
    negatives = [v for v in values if v < 0]

    WHITE = 'text-foreground'
    GREEN = 'text-emerald-400'
    RED = 'text-rose-400'

    if len(positives) == 4:
        max_val = max(values)
        return tuple(GREEN if v == max_val else WHITE for v in values)

    if len(negatives) == 4:
        min_val = min(values)
        return tuple(RED if v == min_val else WHITE for v in values)

    if len(positives) == 0 and len(negatives) == 0:
        return (WHITE, WHITE, WHITE, WHITE)

    max_positive = max(positives) if positives else None
    min_negative = min(negatives) if negatives else None

    result = []
    for v in values:
        if v > 0 and v == max_positive:
            result.append(GREEN)
        elif v < 0 and v == min_negative:
            result.append(RED)
        else:
            result.append(WHITE)
    return tuple(result)


def generate_weekday_card(df: pd.DataFrame, weekday: int, segment_label: str) -> str:
    """曜日別カードを生成（analysisと同じ形式）"""
    wd_df = df[df['weekday'] == weekday]
    total = calc_stats(wd_df)
    me_c, p1_c, ae_c, p2_c = get_quadrant_classes(total['me'], total['p1'], total['ae'], total['p2'])

    html = f'''
                <div class="relative overflow-hidden rounded-xl border border-border/40 bg-gradient-to-br from-card/50 via-card/80 to-card/50 p-4 shadow-lg shadow-black/5 backdrop-blur-xl">
                    <div class="absolute inset-0 bg-gradient-to-br from-white/[0.02] via-transparent to-transparent pointer-events-none"></div>
                    <div class="relative">
                        <div class="flex items-center gap-2 mb-3">
                            <span class="font-semibold text-lg text-foreground">{WEEKDAY_NAMES[weekday]}</span>
                            <span class="text-muted-foreground text-base">{total['count']}件</span>
                        </div>
                        <div class="flex justify-end gap-5 mb-3 pb-3 border-b border-border/30">
                            <div class="text-right">
                                <div class="text-muted-foreground text-sm">10:25</div>
                                <div class="text-xl font-bold tabular-nums {me_c}">{format_profit(total['me'])}</div>
                            </div>
                            <div class="text-right">
                                <div class="text-muted-foreground text-sm">前場引け</div>
                                <div class="text-xl font-bold tabular-nums {p1_c}">{format_profit(total['p1'])}</div>
                            </div>
                            <div class="text-right">
                                <div class="text-muted-foreground text-sm">14:45</div>
                                <div class="text-xl font-bold tabular-nums {ae_c}">{format_profit(total['ae'])}</div>
                            </div>
                            <div class="text-right">
                                <div class="text-muted-foreground text-sm whitespace-nowrap">大引け(15:30)</div>
                                <div class="text-xl font-bold tabular-nums {p2_c}">{format_profit(total['p2'])}</div>
                            </div>
                        </div>
                        <div class="overflow-x-auto">
                            <table class="w-full text-sm">
                                <thead>
                                    <tr class="text-muted-foreground text-sm border-b border-border/30">
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">価格帯</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">件</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">10:25</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">%</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">前場引</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">%</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">14:45</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">%</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">大引</th>
                                        <th class="text-right px-2 py-2.5 font-medium whitespace-nowrap">%</th>
                                    </tr>
                                </thead>
                                <tbody>
'''

    for pr_label, _, _ in PRICE_RANGES:
        pr_df = wd_df[wd_df['price_range'] == pr_label]
        pr = calc_stats(pr_df)
        pr_me_c, pr_p1_c, pr_ae_c, pr_p2_c = get_quadrant_classes(pr['me'], pr['p1'], pr['ae'], pr['p2'])

        html += f'''
                                    <tr class="border-b border-border/20">
                                        <td class="text-right px-2 py-2.5 tabular-nums text-foreground whitespace-nowrap">{pr_label}</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums text-foreground">{pr['count']}</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {pr_me_c}">{format_profit(pr['me'])}</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_me'])}">{pr['win_me']}%</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {pr_p1_c}">{format_profit(pr['p1'])}</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_p1'])}">{pr['win_p1']}%</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {pr_ae_c}">{format_profit(pr['ae'])}</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_ae'])}">{pr['win_ae']}%</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {pr_p2_c}">{format_profit(pr['p2'])}</td>
                                        <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_p2'])}">{pr['win_p2']}%</td>
                                    </tr>
'''

    html += '''
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
'''
    return html


def generate_segment_section(df: pd.DataFrame, segment: str, segment_label: str, color: str) -> str:
    """セグメント別セクションを生成"""
    seg_df = df[df['segment'] == segment]
    total = calc_stats(seg_df)

    html = f'''
        <div class="mb-8">
            <div class="flex items-center gap-2 mb-4">
                <h2 class="text-lg font-semibold" style="color: {color};">{segment_label}</h2>
                <span class="text-muted-foreground text-sm">({total['count']}件)</span>
                <span class="text-sm tabular-nums ml-4" style="color: {color};">
                    10:25: {format_profit(total['me'])} / 前引: {format_profit(total['p1'])} / 14:45: {format_profit(total['ae'])} / 大引: {format_profit(total['p2'])}
                </span>
            </div>
            <div class="grid grid-cols-1 gap-4">
'''

    for wd in range(5):
        html += generate_weekday_card(seg_df, wd, segment_label)

    html += '''
            </div>
        </div>
'''
    return html


def generate_html(df: pd.DataFrame) -> str:
    total_all = calc_stats(df)
    total_rsi = calc_stats(df[df['segment'] == 'rsi_only'])
    total_atr = calc_stats(df[df['segment'] == 'atr_only'])
    total_both = calc_stats(df[df['segment'] == 'both'])
    total_excl = calc_stats(df[df['segment'] == 'excluded'])

    html = '''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>曜日×RSI/ATR×価格帯 損益分析</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
tailwind.config = {
    theme: {
        extend: {
            colors: {
                background: '#0a0a0f',
                foreground: '#fafafa',
                card: '#18181b',
                'card-foreground': '#fafafa',
                muted: '#27272a',
                'muted-foreground': '#a1a1aa',
                border: '#27272a',
            }
        }
    }
}
</script>
<style>
body {
    background-color: #0a0a0f;
    color: #fafafa;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
.tabular-nums { font-variant-numeric: tabular-nums; }
</style>
</head>
<body>
    <main class="relative min-h-screen">
        <div class="fixed inset-0 -z-10 overflow-hidden">
            <div class="absolute inset-0 bg-gradient-to-br from-background via-background to-muted/20"></div>
            <div class="absolute -top-1/2 -right-1/4 w-[800px] h-[800px] rounded-full bg-gradient-to-br from-primary/8 via-primary/3 to-transparent blur-3xl"></div>
            <div class="absolute -bottom-1/3 -left-1/4 w-[600px] h-[600px] rounded-full bg-gradient-to-tr from-accent/10 via-accent/4 to-transparent blur-3xl"></div>
        </div>

        <div class="max-w-7xl mx-auto px-4 py-4">
            <header class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3 mb-4 pb-3 border-b border-border/30">
                <div>
                    <h1 class="text-xl font-bold text-foreground">曜日×RSI/ATR×価格帯 分析</h1>
                    <p class="text-muted-foreground text-sm">ショート戦略 - 極端相場除外</p>
                </div>
            </header>

            <div class="bg-card/30 rounded-lg border border-border/40 p-4 mb-6">
                <div class="text-sm text-muted-foreground mb-2">条件定義</div>
                <div class="text-xs text-muted-foreground space-y-1">
                    <div>制度信用: RSI ≥ 70, ATR ≥ 8%</div>
                    <div>いちにち信用: RSI ≥ 90, ATR ≥ 9%</div>
                </div>
            </div>

            <div class="relative overflow-hidden rounded-xl border border-border/40 bg-gradient-to-br from-card/50 via-card/80 to-card/50 p-4 shadow-lg mb-6">
                <h2 class="text-base font-semibold text-foreground mb-3">全体サマリー</h2>
                <table class="w-full text-sm max-w-3xl">
                    <thead>
                        <tr class="text-muted-foreground border-b border-border/30">
                            <th class="text-left px-2 py-2 font-medium">セグメント</th>
                            <th class="text-right px-2 py-2 font-medium">件</th>
                            <th class="text-right px-2 py-2 font-medium">10:25</th>
                            <th class="text-right px-2 py-2 font-medium">前引</th>
                            <th class="text-right px-2 py-2 font-medium">14:45</th>
                            <th class="text-right px-2 py-2 font-medium">大引</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr class="border-b border-border/20">
                            <td class="px-2 py-2 text-sky-400">RSIのみ</td>
                            <td class="text-right px-2 py-2 tabular-nums">''' + str(total_rsi['count']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_rsi['me']) + '''">''' + format_profit(total_rsi['me']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_rsi['p1']) + '''">''' + format_profit(total_rsi['p1']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_rsi['ae']) + '''">''' + format_profit(total_rsi['ae']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_rsi['p2']) + '''">''' + format_profit(total_rsi['p2']) + '''</td>
                        </tr>
                        <tr class="border-b border-border/20">
                            <td class="px-2 py-2 text-amber-400">ATRのみ</td>
                            <td class="text-right px-2 py-2 tabular-nums">''' + str(total_atr['count']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_atr['me']) + '''">''' + format_profit(total_atr['me']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_atr['p1']) + '''">''' + format_profit(total_atr['p1']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_atr['ae']) + '''">''' + format_profit(total_atr['ae']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_atr['p2']) + '''">''' + format_profit(total_atr['p2']) + '''</td>
                        </tr>
                        <tr class="border-b border-border/20">
                            <td class="px-2 py-2 text-purple-400">両方該当</td>
                            <td class="text-right px-2 py-2 tabular-nums">''' + str(total_both['count']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_both['me']) + '''">''' + format_profit(total_both['me']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_both['p1']) + '''">''' + format_profit(total_both['p1']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_both['ae']) + '''">''' + format_profit(total_both['ae']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_both['p2']) + '''">''' + format_profit(total_both['p2']) + '''</td>
                        </tr>
                        <tr class="border-b border-border/20">
                            <td class="px-2 py-2 text-emerald-400">除外後</td>
                            <td class="text-right px-2 py-2 tabular-nums">''' + str(total_excl['count']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_excl['me']) + '''">''' + format_profit(total_excl['me']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_excl['p1']) + '''">''' + format_profit(total_excl['p1']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_excl['ae']) + '''">''' + format_profit(total_excl['ae']) + '''</td>
                            <td class="text-right px-2 py-2 tabular-nums ''' + profit_class(total_excl['p2']) + '''">''' + format_profit(total_excl['p2']) + '''</td>
                        </tr>
                    </tbody>
                </table>
            </div>
'''

    html += generate_segment_section(df, 'rsi_only', 'RSIのみ該当', '#38bdf8')
    html += generate_segment_section(df, 'atr_only', 'ATRのみ該当', '#fbbf24')
    html += generate_segment_section(df, 'both', '両方該当（RSI & ATR）', '#a78bfa')
    html += generate_segment_section(df, 'excluded', '除外後（どちらも非該当）', '#34d399')

    html += '''
        </div>
    </main>
</body>
</html>
'''
    return html


def main():
    df = load_archive()
    print(f"データ読み込み: {len(df)}件")
    print(f"RSIのみ: {len(df[df['segment'] == 'rsi_only'])}件")
    print(f"ATRのみ: {len(df[df['segment'] == 'atr_only'])}件")
    print(f"両方: {len(df[df['segment'] == 'both'])}件")
    print(f"除外後: {len(df[df['segment'] == 'excluded'])}件")

    html = generate_html(df)

    output_path = Path(__file__).parent / 'output' / 'weekday_rsi_atr_price_analysis.html'
    output_path.write_text(html, encoding='utf-8')
    print(f"HTML生成完了: {output_path}")


if __name__ == '__main__':
    main()
