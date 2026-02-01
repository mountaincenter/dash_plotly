"""
Analysis + RSI/ATR 統合モックアップ生成
4つの案を実データで作成
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

SEGMENTS = [
    ('rsi_only', 'RSIのみ', '#38bdf8', 'sky'),
    ('atr_only', 'ATRのみ', '#fbbf24', 'amber'),
    ('both', '両方', '#a78bfa', 'purple'),
    ('excluded', '除外後', '#34d399', 'emerald'),
]


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
    df['is_ichi'] = ~df['is_seido']
    df['rsi_hit'] = ((df['is_seido']) & (df['rsi9'] >= SEIDO_RSI)) | ((df['is_ichi']) & (df['rsi9'] >= ICHI_RSI))
    df['atr_hit'] = ((df['is_seido']) & (df['atr14_pct'] >= SEIDO_ATR)) | ((df['is_ichi']) & (df['atr14_pct'] >= ICHI_ATR))

    df['segment'] = 'excluded'
    df.loc[df['rsi_hit'] & ~df['atr_hit'], 'segment'] = 'rsi_only'
    df.loc[~df['rsi_hit'] & df['atr_hit'], 'segment'] = 'atr_only'
    df.loc[df['rsi_hit'] & df['atr_hit'], 'segment'] = 'both'

    return df


def calc_stats(target_df: pd.DataFrame) -> dict:
    if len(target_df) == 0:
        return {'count': 0, 'me': 0, 'p1': 0, 'ae': 0, 'p2': 0,
                'win_me': 0, 'win_p1': 0, 'win_ae': 0, 'win_p2': 0}

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


def format_profit(val: int) -> str:
    if val == 0:
        return '-'
    sign = '+' if val >= 0 else ''
    return f'{sign}{val:,}'


def html_head(title: str) -> str:
    return f'''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>{title}</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>
tailwind.config = {{
    theme: {{
        extend: {{
            colors: {{
                background: '#0a0a0f',
                foreground: '#fafafa',
                card: '#18181b',
                'card-foreground': '#fafafa',
                muted: '#27272a',
                'muted-foreground': '#a1a1aa',
                border: '#27272a',
            }}
        }}
    }}
}}
</script>
<style>
body {{
    background-color: #0a0a0f;
    color: #fafafa;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}}
.tabular-nums {{ font-variant-numeric: tabular-nums; }}
.tab-active {{ background: rgba(39, 39, 42, 0.8); border-bottom: 2px solid #fafafa; }}
.tab-inactive {{ background: transparent; border-bottom: 2px solid transparent; }}
.tab-inactive:hover {{ background: rgba(39, 39, 42, 0.4); }}
details summary {{ cursor: pointer; }}
details summary::-webkit-details-marker {{ display: none; }}
</style>
</head>
<body>
    <main class="relative min-h-screen">
        <div class="fixed inset-0 -z-10 overflow-hidden">
            <div class="absolute inset-0 bg-gradient-to-br from-background via-background to-muted/20"></div>
        </div>
        <div class="max-w-7xl mx-auto px-4 py-4">
'''


def html_foot() -> str:
    return '''
        </div>
    </main>
</body>
</html>
'''


# ==================== 案1: タブ切替方式 ====================
def generate_mockup1(df: pd.DataFrame) -> str:
    html = html_head('案1: タブ切替方式')
    html += '''
            <header class="mb-4 pb-3 border-b border-border/30">
                <h1 class="text-xl font-bold text-foreground">案1: タブ切替方式</h1>
                <p class="text-muted-foreground text-sm">上部タブでセグメント切替、各セグメント内はanalysisと同じレイアウト</p>
            </header>

            <!-- タブ -->
            <div class="flex gap-1 mb-6 border-b border-border/30">
                <button class="tab-active px-4 py-2 text-sm text-foreground rounded-t-lg">RSIのみ</button>
                <button class="tab-inactive px-4 py-2 text-sm text-muted-foreground rounded-t-lg">ATRのみ</button>
                <button class="tab-inactive px-4 py-2 text-sm text-muted-foreground rounded-t-lg">両方</button>
                <button class="tab-inactive px-4 py-2 text-sm text-muted-foreground rounded-t-lg">除外後</button>
            </div>

            <p class="text-xs text-muted-foreground mb-4">※実装時はJSでタブ切替。ここではRSIのみを表示</p>

            <div class="grid grid-cols-1 gap-4">
'''

    seg_df = df[df['segment'] == 'rsi_only']
    for wd in range(5):
        wd_df = seg_df[seg_df['weekday'] == wd]
        total = calc_stats(wd_df)

        html += f'''
                <div class="relative overflow-hidden rounded-xl border border-border/40 bg-gradient-to-br from-card/50 via-card/80 to-card/50 p-4">
                    <div class="flex items-center gap-2 mb-3">
                        <span class="font-semibold text-lg text-foreground">{WEEKDAY_NAMES[wd]}</span>
                        <span class="text-muted-foreground">{total['count']}件</span>
                    </div>
                    <div class="flex justify-end gap-5 mb-3 pb-3 border-b border-border/30">
                        <div class="text-right">
                            <div class="text-muted-foreground text-sm">10:25</div>
                            <div class="text-xl font-bold tabular-nums {profit_class(total['me'])}">{format_profit(total['me'])}</div>
                        </div>
                        <div class="text-right">
                            <div class="text-muted-foreground text-sm">前場引</div>
                            <div class="text-xl font-bold tabular-nums {profit_class(total['p1'])}">{format_profit(total['p1'])}</div>
                        </div>
                        <div class="text-right">
                            <div class="text-muted-foreground text-sm">14:45</div>
                            <div class="text-xl font-bold tabular-nums {profit_class(total['ae'])}">{format_profit(total['ae'])}</div>
                        </div>
                        <div class="text-right">
                            <div class="text-muted-foreground text-sm">大引</div>
                            <div class="text-xl font-bold tabular-nums {profit_class(total['p2'])}">{format_profit(total['p2'])}</div>
                        </div>
                    </div>
                    <table class="w-full text-sm">
                        <thead>
                            <tr class="text-muted-foreground border-b border-border/30">
                                <th class="text-left px-2 py-2">価格帯</th>
                                <th class="text-right px-2 py-2">件</th>
                                <th class="text-right px-2 py-2">10:25</th>
                                <th class="text-right px-2 py-2">%</th>
                                <th class="text-right px-2 py-2">大引</th>
                                <th class="text-right px-2 py-2">%</th>
                            </tr>
                        </thead>
                        <tbody>
'''
        for pr_label, _, _ in PRICE_RANGES:
            pr_df = wd_df[wd_df['price_range'] == pr_label]
            pr = calc_stats(pr_df)
            html += f'''
                            <tr class="border-b border-border/20">
                                <td class="px-2 py-2">{pr_label}</td>
                                <td class="text-right px-2 py-2 tabular-nums">{pr['count']}</td>
                                <td class="text-right px-2 py-2 tabular-nums {profit_class(pr['me'])}">{format_profit(pr['me'])}</td>
                                <td class="text-right px-2 py-2 tabular-nums">{pr['win_me']}%</td>
                                <td class="text-right px-2 py-2 tabular-nums {profit_class(pr['p2'])}">{format_profit(pr['p2'])}</td>
                                <td class="text-right px-2 py-2 tabular-nums">{pr['win_p2']}%</td>
                            </tr>
'''
        html += '''
                        </tbody>
                    </table>
                </div>
'''

    html += '''
            </div>
'''
    html += html_foot()
    return html


# ==================== 案2: マトリクス表示 ====================
def generate_mockup2(df: pd.DataFrame) -> str:
    html = html_head('案2: マトリクス表示')
    html += '''
            <header class="mb-4 pb-3 border-b border-border/30">
                <h1 class="text-xl font-bold text-foreground">案2: マトリクス表示</h1>
                <p class="text-muted-foreground text-sm">1行に全セグメントのP&Lを横並び表示。一覧性重視。</p>
            </header>

            <div class="grid grid-cols-1 gap-6">
'''

    for wd in range(5):
        wd_df = df[df['weekday'] == wd]
        total = calc_stats(wd_df)

        html += f'''
                <div class="relative overflow-hidden rounded-xl border border-border/40 bg-gradient-to-br from-card/50 via-card/80 to-card/50 p-4">
                    <div class="flex items-center gap-2 mb-4">
                        <span class="font-semibold text-lg text-foreground">{WEEKDAY_NAMES[wd]}</span>
                        <span class="text-muted-foreground">{total['count']}件</span>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead>
                                <tr class="text-muted-foreground border-b border-border/30">
                                    <th class="text-left px-2 py-2 font-medium">価格帯</th>
                                    <th class="text-center px-3 py-2 font-medium text-sky-400" colspan="2">RSIのみ</th>
                                    <th class="text-center px-3 py-2 font-medium text-amber-400" colspan="2">ATRのみ</th>
                                    <th class="text-center px-3 py-2 font-medium text-purple-400" colspan="2">両方</th>
                                    <th class="text-center px-3 py-2 font-medium text-emerald-400" colspan="2">除外後</th>
                                </tr>
                                <tr class="text-muted-foreground text-xs border-b border-border/20">
                                    <th></th>
                                    <th class="text-right px-2 py-1">件</th>
                                    <th class="text-right px-2 py-1">大引</th>
                                    <th class="text-right px-2 py-1">件</th>
                                    <th class="text-right px-2 py-1">大引</th>
                                    <th class="text-right px-2 py-1">件</th>
                                    <th class="text-right px-2 py-1">大引</th>
                                    <th class="text-right px-2 py-1">件</th>
                                    <th class="text-right px-2 py-1">大引</th>
                                </tr>
                            </thead>
                            <tbody>
'''
        for pr_label, _, _ in PRICE_RANGES:
            pr_df = wd_df[wd_df['price_range'] == pr_label]
            rsi = calc_stats(pr_df[pr_df['segment'] == 'rsi_only'])
            atr = calc_stats(pr_df[pr_df['segment'] == 'atr_only'])
            both = calc_stats(pr_df[pr_df['segment'] == 'both'])
            excl = calc_stats(pr_df[pr_df['segment'] == 'excluded'])

            html += f'''
                                <tr class="border-b border-border/20">
                                    <td class="px-2 py-2 whitespace-nowrap">{pr_label}</td>
                                    <td class="text-right px-2 py-2 tabular-nums text-sky-400/70">{rsi['count'] or '-'}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(rsi['p2'])}">{format_profit(rsi['p2'])}</td>
                                    <td class="text-right px-2 py-2 tabular-nums text-amber-400/70">{atr['count'] or '-'}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(atr['p2'])}">{format_profit(atr['p2'])}</td>
                                    <td class="text-right px-2 py-2 tabular-nums text-purple-400/70">{both['count'] or '-'}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(both['p2'])}">{format_profit(both['p2'])}</td>
                                    <td class="text-right px-2 py-2 tabular-nums text-emerald-400/70">{excl['count'] or '-'}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(excl['p2'])}">{format_profit(excl['p2'])}</td>
                                </tr>
'''
        html += '''
                            </tbody>
                        </table>
                    </div>
                </div>
'''

    html += '''
            </div>
'''
    html += html_foot()
    return html


# ==================== 案3: サマリー + 展開式 ====================
def generate_mockup3(df: pd.DataFrame) -> str:
    html = html_head('案3: サマリー + 展開式')
    html += '''
            <header class="mb-4 pb-3 border-b border-border/30">
                <h1 class="text-xl font-bold text-foreground">案3: サマリー + 展開式</h1>
                <p class="text-muted-foreground text-sm">通常は除外後のP&Lのみ表示。クリックでRSI/ATR詳細が展開。</p>
            </header>

            <div class="grid grid-cols-1 gap-4">
'''

    for wd in range(5):
        wd_df = df[df['weekday'] == wd]
        excl_total = calc_stats(wd_df[wd_df['segment'] == 'excluded'])
        rsi_total = calc_stats(wd_df[wd_df['segment'] == 'rsi_only'])
        atr_total = calc_stats(wd_df[wd_df['segment'] == 'atr_only'])
        both_total = calc_stats(wd_df[wd_df['segment'] == 'both'])

        html += f'''
                <details class="relative overflow-hidden rounded-xl border border-border/40 bg-gradient-to-br from-card/50 via-card/80 to-card/50" open>
                    <summary class="p-4 flex items-center justify-between">
                        <div class="flex items-center gap-4">
                            <span class="font-semibold text-lg text-foreground">{WEEKDAY_NAMES[wd]}</span>
                            <span class="text-muted-foreground">{excl_total['count']}件（除外後）</span>
                            <span class="tabular-nums {profit_class(excl_total['p2'])}">{format_profit(excl_total['p2'])}</span>
                        </div>
                        <span class="text-muted-foreground text-sm">▼ RSI/ATR詳細</span>
                    </summary>
                    <div class="px-4 pb-4 border-t border-border/30 pt-3">
                        <div class="grid grid-cols-3 gap-3 mb-4">
                            <div class="bg-sky-500/10 rounded-lg p-3 border border-sky-500/30">
                                <div class="text-sky-400 text-sm font-medium mb-1">RSIのみ ({rsi_total['count']}件)</div>
                                <div class="text-lg font-bold tabular-nums {profit_class(rsi_total['p2'])}">{format_profit(rsi_total['p2'])}</div>
                                <div class="text-xs text-muted-foreground mt-1">→ ロング推奨</div>
                            </div>
                            <div class="bg-amber-500/10 rounded-lg p-3 border border-amber-500/30">
                                <div class="text-amber-400 text-sm font-medium mb-1">ATRのみ ({atr_total['count']}件)</div>
                                <div class="text-lg font-bold tabular-nums {profit_class(atr_total['p2'])}">{format_profit(atr_total['p2'])}</div>
                                <div class="text-xs text-muted-foreground mt-1">→ ショート有効</div>
                            </div>
                            <div class="bg-purple-500/10 rounded-lg p-3 border border-purple-500/30">
                                <div class="text-purple-400 text-sm font-medium mb-1">両方 ({both_total['count']}件)</div>
                                <div class="text-lg font-bold tabular-nums {profit_class(both_total['p2'])}">{format_profit(both_total['p2'])}</div>
                                <div class="text-xs text-muted-foreground mt-1">→ 要分析</div>
                            </div>
                        </div>
                        <table class="w-full text-sm">
                            <thead>
                                <tr class="text-muted-foreground border-b border-border/30 text-xs">
                                    <th class="text-left px-2 py-2">価格帯</th>
                                    <th class="text-right px-2 py-2 text-emerald-400">除外後</th>
                                    <th class="text-right px-2 py-2 text-sky-400">RSI</th>
                                    <th class="text-right px-2 py-2 text-amber-400">ATR</th>
                                    <th class="text-right px-2 py-2 text-purple-400">両方</th>
                                </tr>
                            </thead>
                            <tbody>
'''
        for pr_label, _, _ in PRICE_RANGES:
            pr_df = wd_df[wd_df['price_range'] == pr_label]
            rsi = calc_stats(pr_df[pr_df['segment'] == 'rsi_only'])
            atr = calc_stats(pr_df[pr_df['segment'] == 'atr_only'])
            both = calc_stats(pr_df[pr_df['segment'] == 'both'])
            excl = calc_stats(pr_df[pr_df['segment'] == 'excluded'])

            html += f'''
                                <tr class="border-b border-border/20">
                                    <td class="px-2 py-2">{pr_label}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(excl['p2'])}">{excl['count']}件 {format_profit(excl['p2'])}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(rsi['p2'])}">{rsi['count']}件 {format_profit(rsi['p2'])}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(atr['p2'])}">{atr['count']}件 {format_profit(atr['p2'])}</td>
                                    <td class="text-right px-2 py-2 tabular-nums {profit_class(both['p2'])}">{both['count']}件 {format_profit(both['p2'])}</td>
                                </tr>
'''
        html += '''
                            </tbody>
                        </table>
                    </div>
                </details>
'''

    html += '''
            </div>
'''
    html += html_foot()
    return html


# ==================== 案4: 2カラムレイアウト ====================
def generate_mockup4(df: pd.DataFrame) -> str:
    html = html_head('案4: 2カラムレイアウト')
    html += '''
            <header class="mb-4 pb-3 border-b border-border/30">
                <h1 class="text-xl font-bold text-foreground">案4: 2カラムレイアウト</h1>
                <p class="text-muted-foreground text-sm">左に「除外後」メイン、右に「RSI/ATR詳細」を常時並列表示。</p>
            </header>

            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <!-- 左カラム: 除外後（メイン） -->
                <div>
                    <h2 class="text-lg font-semibold text-emerald-400 mb-4">除外後（メイン）</h2>
                    <div class="space-y-4">
'''

    excl_df = df[df['segment'] == 'excluded']
    for wd in range(5):
        wd_df = excl_df[excl_df['weekday'] == wd]
        total = calc_stats(wd_df)

        html += f'''
                        <div class="rounded-lg border border-border/40 bg-card/50 p-3">
                            <div class="flex items-center justify-between mb-2">
                                <span class="font-medium text-foreground">{WEEKDAY_NAMES[wd]}</span>
                                <span class="text-muted-foreground text-sm">{total['count']}件</span>
                            </div>
                            <table class="w-full text-xs">
                                <tbody>
'''
        for pr_label, _, _ in PRICE_RANGES:
            pr_df = wd_df[wd_df['price_range'] == pr_label]
            pr = calc_stats(pr_df)
            html += f'''
                                    <tr class="border-b border-border/20">
                                        <td class="py-1">{pr_label}</td>
                                        <td class="text-right py-1 tabular-nums">{pr['count']}</td>
                                        <td class="text-right py-1 tabular-nums {profit_class(pr['p2'])}">{format_profit(pr['p2'])}</td>
                                    </tr>
'''
        html += '''
                                </tbody>
                            </table>
                        </div>
'''

    html += '''
                    </div>
                </div>

                <!-- 右カラム: RSI/ATR詳細 -->
                <div>
                    <h2 class="text-lg font-semibold text-foreground mb-4">RSI/ATR 詳細</h2>
                    <div class="space-y-4">
'''

    # RSIのみ
    html += '''
                        <div class="rounded-lg border border-sky-500/40 bg-sky-500/5 p-3">
                            <h3 class="text-sky-400 font-medium mb-2">RSIのみ → ロング推奨</h3>
                            <table class="w-full text-xs">
                                <thead>
                                    <tr class="text-muted-foreground border-b border-border/30">
                                        <th class="text-left py-1">曜日</th>
                                        <th class="text-right py-1">件</th>
                                        <th class="text-right py-1">大引</th>
                                    </tr>
                                </thead>
                                <tbody>
'''
    rsi_df = df[df['segment'] == 'rsi_only']
    for wd in range(5):
        wd_stats = calc_stats(rsi_df[rsi_df['weekday'] == wd])
        html += f'''
                                    <tr class="border-b border-border/20">
                                        <td class="py-1">{WEEKDAY_NAMES[wd]}</td>
                                        <td class="text-right py-1 tabular-nums">{wd_stats['count']}</td>
                                        <td class="text-right py-1 tabular-nums {profit_class(wd_stats['p2'])}">{format_profit(wd_stats['p2'])}</td>
                                    </tr>
'''
    html += '''
                                </tbody>
                            </table>
                        </div>
'''

    # ATRのみ
    html += '''
                        <div class="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3">
                            <h3 class="text-amber-400 font-medium mb-2">ATRのみ → ショート有効</h3>
                            <table class="w-full text-xs">
                                <thead>
                                    <tr class="text-muted-foreground border-b border-border/30">
                                        <th class="text-left py-1">曜日</th>
                                        <th class="text-right py-1">件</th>
                                        <th class="text-right py-1">大引</th>
                                    </tr>
                                </thead>
                                <tbody>
'''
    atr_df = df[df['segment'] == 'atr_only']
    for wd in range(5):
        wd_stats = calc_stats(atr_df[atr_df['weekday'] == wd])
        html += f'''
                                    <tr class="border-b border-border/20">
                                        <td class="py-1">{WEEKDAY_NAMES[wd]}</td>
                                        <td class="text-right py-1 tabular-nums">{wd_stats['count']}</td>
                                        <td class="text-right py-1 tabular-nums {profit_class(wd_stats['p2'])}">{format_profit(wd_stats['p2'])}</td>
                                    </tr>
'''
    html += '''
                                </tbody>
                            </table>
                        </div>
'''

    # 両方
    html += '''
                        <div class="rounded-lg border border-purple-500/40 bg-purple-500/5 p-3">
                            <h3 class="text-purple-400 font-medium mb-2">両方 → 要分析</h3>
                            <table class="w-full text-xs">
                                <thead>
                                    <tr class="text-muted-foreground border-b border-border/30">
                                        <th class="text-left py-1">曜日</th>
                                        <th class="text-right py-1">件</th>
                                        <th class="text-right py-1">大引</th>
                                    </tr>
                                </thead>
                                <tbody>
'''
    both_df = df[df['segment'] == 'both']
    for wd in range(5):
        wd_stats = calc_stats(both_df[both_df['weekday'] == wd])
        html += f'''
                                    <tr class="border-b border-border/20">
                                        <td class="py-1">{WEEKDAY_NAMES[wd]}</td>
                                        <td class="text-right py-1 tabular-nums">{wd_stats['count']}</td>
                                        <td class="text-right py-1 tabular-nums {profit_class(wd_stats['p2'])}">{format_profit(wd_stats['p2'])}</td>
                                    </tr>
'''
    html += '''
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
'''
    html += html_foot()
    return html


def main():
    df = load_archive()
    print(f"データ読み込み: {len(df)}件")

    output_dir = Path(__file__).parent / 'output'

    # 案1
    html1 = generate_mockup1(df)
    (output_dir / 'mockup1_tab.html').write_text(html1, encoding='utf-8')
    print("案1: mockup1_tab.html 生成完了")

    # 案2
    html2 = generate_mockup2(df)
    (output_dir / 'mockup2_matrix.html').write_text(html2, encoding='utf-8')
    print("案2: mockup2_matrix.html 生成完了")

    # 案3
    html3 = generate_mockup3(df)
    (output_dir / 'mockup3_expand.html').write_text(html3, encoding='utf-8')
    print("案3: mockup3_expand.html 生成完了")

    # 案4
    html4 = generate_mockup4(df)
    (output_dir / 'mockup4_twocol.html').write_text(html4, encoding='utf-8')
    print("案4: mockup4_twocol.html 生成完了")


if __name__ == '__main__':
    main()
