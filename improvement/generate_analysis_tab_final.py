"""
Analysis + RSI/ATR タブ切替版（最終版）

タブ: 通常 | RSI | ATR | 両方 | 全て
デフォルト: 通常
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

TABS = [
    ('excluded', '通常', '#34d399', 'emerald'),
    ('rsi_only', 'RSI', '#38bdf8', 'sky'),
    ('atr_only', 'ATR', '#fbbf24', 'amber'),
    ('both', '両方', '#a78bfa', 'purple'),
    ('all', '全て', '#fafafa', 'foreground'),
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


def winrate_class(rate: int) -> str:
    if rate > 50:
        return 'text-emerald-400'
    elif rate < 50:
        return 'text-rose-400'
    return 'text-foreground'


def format_profit(val: int) -> str:
    if val == 0:
        return '-'
    sign = '+' if val >= 0 else ''
    return f'{sign}{val:,}'


def get_quadrant_classes(me: int, p1: int, ae: int, p2: int) -> tuple:
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


def generate_weekday_card(df: pd.DataFrame, weekday: int) -> str:
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


def generate_tab_content(df: pd.DataFrame, segment: str) -> str:
    if segment == 'all':
        seg_df = df
    else:
        seg_df = df[df['segment'] == segment]

    html = ''
    for wd in range(5):
        html += generate_weekday_card(seg_df, wd)
    return html


def generate_html(df: pd.DataFrame) -> str:
    # 各タブのコンテンツを生成
    tab_contents = {}
    for seg_id, _, _, _ in TABS:
        tab_contents[seg_id] = generate_tab_content(df, seg_id)

    # 各セグメントの件数を取得
    counts = {
        'excluded': len(df[df['segment'] == 'excluded']),
        'rsi_only': len(df[df['segment'] == 'rsi_only']),
        'atr_only': len(df[df['segment'] == 'atr_only']),
        'both': len(df[df['segment'] == 'both']),
        'all': len(df),
    }

    html = '''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Analysis - RSI/ATR タブ切替</title>
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
.tab-btn {
    transition: all 0.15s ease;
}
.tab-btn.active {
    border-bottom-width: 2px;
}
.tab-content {
    display: none;
}
.tab-content.active {
    display: block;
}
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
                    <h1 class="text-xl font-bold text-foreground">曜日×価格帯 分析</h1>
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

            <!-- タブ -->
            <div class="flex gap-1 mb-6 border-b border-border/30">
'''

    # タブボタン
    for i, (seg_id, label, color, tw_color) in enumerate(TABS):
        active_class = 'active border-emerald-400 text-foreground' if i == 0 else 'border-transparent text-muted-foreground hover:text-foreground'
        html += f'''
                <button class="tab-btn {active_class} px-4 py-2 text-sm font-medium" data-tab="{seg_id}" style="--tab-color: {color};">
                    {label} <span class="text-xs opacity-70">({counts[seg_id]})</span>
                </button>
'''

    html += '''
            </div>

            <!-- タブコンテンツ -->
'''

    # 各タブのコンテンツ
    for i, (seg_id, label, color, tw_color) in enumerate(TABS):
        active_class = 'active' if i == 0 else ''
        html += f'''
            <div id="tab-{seg_id}" class="tab-content {active_class} grid grid-cols-1 gap-4">
{tab_contents[seg_id]}
            </div>
'''

    html += '''
        </div>
    </main>

    <script>
    document.addEventListener('DOMContentLoaded', function() {
        const tabs = document.querySelectorAll('.tab-btn');
        const contents = document.querySelectorAll('.tab-content');

        tabs.forEach(tab => {
            tab.addEventListener('click', function() {
                const targetId = this.dataset.tab;

                // タブのアクティブ状態を更新
                tabs.forEach(t => {
                    t.classList.remove('active', 'border-emerald-400', 'text-foreground');
                    t.classList.add('border-transparent', 'text-muted-foreground');
                });
                this.classList.add('active', 'border-emerald-400', 'text-foreground');
                this.classList.remove('border-transparent', 'text-muted-foreground');

                // コンテンツの表示を切替
                contents.forEach(c => {
                    c.classList.remove('active');
                });
                document.getElementById('tab-' + targetId).classList.add('active');
            });
        });
    });
    </script>
</body>
</html>
'''
    return html


def main():
    df = load_archive()
    print(f"データ読み込み: {len(df)}件")
    print(f"通常: {len(df[df['segment'] == 'excluded'])}件")
    print(f"RSI: {len(df[df['segment'] == 'rsi_only'])}件")
    print(f"ATR: {len(df[df['segment'] == 'atr_only'])}件")
    print(f"両方: {len(df[df['segment'] == 'both'])}件")

    html = generate_html(df)

    output_path = Path(__file__).parent / 'output' / 'analysis_tab_final.html'
    output_path.write_text(html, encoding='utf-8')
    print(f"HTML生成完了: {output_path}")


if __name__ == '__main__':
    main()
