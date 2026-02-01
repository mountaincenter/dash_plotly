"""
Analysis + RSI/ATR 曜日内タブ切替版

各曜日カードの中にタブを配置
曜日名の横に [通常] [RSI] [ATR] [両方] [全て] タブ
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
    ('excluded', '通常'),
    ('rsi_only', 'RSI'),
    ('atr_only', 'ATR'),
    ('both', '両方'),
    ('all', '全て'),
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
    df['is_ichi0'] = df['is_ichi'] & (df['profit_per_100_shares_phase2'] != 0)

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


def generate_margin_section(df: pd.DataFrame, margin_type: str, margin_label: str, wd: int, seg: str) -> str:
    """信用種別セクションを生成"""
    if margin_type == 'seido':
        type_df = df[df['is_seido']]
    elif margin_type == 'ichi':
        type_df = df[df['is_ichi']]
    else:  # ichi0
        type_df = df[df['is_ichi0']]

    total = calc_stats(type_df)
    if total['count'] == 0:
        return ''

    html = f'''
                            <div class="mb-6">
                                <div class="flex items-center gap-2 mb-2">
                                    <span class="text-sm font-medium text-muted-foreground">{margin_label}</span>
                                    <span class="text-sm text-muted-foreground">{total['count']}件</span>
                                </div>
                                <div class="flex justify-end gap-5 mb-3 pb-3 border-b border-border/30">
                                    <div class="text-right">
                                        <div class="text-muted-foreground text-sm">10:25</div>
                                        <div class="text-xl font-bold tabular-nums {profit_class(total['me'])}">{format_profit(total['me'])}</div>
                                    </div>
                                    <div class="text-right">
                                        <div class="text-muted-foreground text-sm">前場引け</div>
                                        <div class="text-xl font-bold tabular-nums {profit_class(total['p1'])}">{format_profit(total['p1'])}</div>
                                    </div>
                                    <div class="text-right">
                                        <div class="text-muted-foreground text-sm">14:45</div>
                                        <div class="text-xl font-bold tabular-nums {profit_class(total['ae'])}">{format_profit(total['ae'])}</div>
                                    </div>
                                    <div class="text-right">
                                        <div class="text-muted-foreground text-sm whitespace-nowrap">大引け(15:30)</div>
                                        <div class="text-xl font-bold tabular-nums {profit_class(total['p2'])}">{format_profit(total['p2'])}</div>
                                    </div>
                                </div>
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
        pr_df = type_df[type_df['price_range'] == pr_label]
        pr = calc_stats(pr_df)

        html += f'''
                                        <tr class="border-b border-border/20">
                                            <td class="text-right px-2 py-2.5 tabular-nums text-foreground whitespace-nowrap">{pr_label}</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums text-foreground">{pr['count']}</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {profit_class(pr['me'])}">{format_profit(pr['me'])}</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_me'])}">{pr['win_me']}%</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {profit_class(pr['p1'])}">{format_profit(pr['p1'])}</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_p1'])}">{pr['win_p1']}%</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {profit_class(pr['ae'])}">{format_profit(pr['ae'])}</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_ae'])}">{pr['win_ae']}%</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums whitespace-nowrap {profit_class(pr['p2'])}">{format_profit(pr['p2'])}</td>
                                            <td class="text-right px-2 py-2.5 tabular-nums {winrate_class(pr['win_p2'])}">{pr['win_p2']}%</td>
                                        </tr>
'''

    html += '''
                                    </tbody>
                                </table>
                            </div>
'''
    return html


def generate_tab_content(df: pd.DataFrame, wd: int, seg: str) -> str:
    """タブコンテンツを生成"""
    wd_df = df[df['weekday'] == wd]
    if seg != 'all':
        wd_df = wd_df[wd_df['segment'] == seg]

    html = ''
    html += generate_margin_section(wd_df, 'seido', '制度信用', wd, seg)
    html += generate_margin_section(wd_df, 'ichi', 'いちにち信用', wd, seg)

    return html


def generate_weekday_card(df: pd.DataFrame, weekday: int) -> str:
    """曜日カードを生成（タブ付き）"""
    wd_df = df[df['weekday'] == weekday]

    # 各セグメントの件数
    counts = {
        'excluded': len(wd_df[wd_df['segment'] == 'excluded']),
        'rsi_only': len(wd_df[wd_df['segment'] == 'rsi_only']),
        'atr_only': len(wd_df[wd_df['segment'] == 'atr_only']),
        'both': len(wd_df[wd_df['segment'] == 'both']),
        'all': len(wd_df),
    }

    html = f'''
                <div class="relative overflow-hidden rounded-xl border border-border/40 bg-gradient-to-br from-card/50 via-card/80 to-card/50 p-4 shadow-lg shadow-black/5 backdrop-blur-xl">
                    <div class="absolute inset-0 bg-gradient-to-br from-white/[0.02] via-transparent to-transparent pointer-events-none"></div>
                    <div class="relative">
                        <!-- 曜日名 + タブ -->
                        <div class="flex items-center gap-4 mb-4 pb-3 border-b border-border/30">
                            <span class="font-semibold text-lg text-foreground">{WEEKDAY_NAMES[weekday]}</span>
                            <div class="flex gap-1">
'''

    # タブボタン
    for i, (seg_id, label) in enumerate(TABS):
        active = 'active bg-muted/50 text-foreground' if i == 0 else 'text-muted-foreground hover:text-foreground hover:bg-muted/30'
        html += f'''
                                <button class="wd-tab-btn {active} px-2 py-1 text-xs rounded" data-weekday="{weekday}" data-seg="{seg_id}">
                                    {label}({counts[seg_id]})
                                </button>
'''

    html += '''
                            </div>
                        </div>

                        <!-- タブコンテンツ -->
'''

    # 各セグメントのコンテンツ
    for i, (seg_id, label) in enumerate(TABS):
        active = 'active' if i == 0 else ''
        html += f'''
                        <div class="wd-tab-content wd{weekday}-{seg_id} {active}">
{generate_tab_content(df, weekday, seg_id)}
                        </div>
'''

    html += '''
                    </div>
                </div>
'''
    return html


def generate_html(df: pd.DataFrame) -> str:
    html = '''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Analysis - 曜日内RSI/ATRタブ</title>
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
.wd-tab-content {
    display: none;
}
.wd-tab-content.active {
    display: block;
}
.wd-tab-btn {
    transition: all 0.15s ease;
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
                    <p class="text-muted-foreground text-sm">ショート戦略 - 極端相場除外 - 曜日内RSI/ATRタブ</p>
                </div>
            </header>

            <div class="bg-card/30 rounded-lg border border-border/40 p-4 mb-6">
                <div class="text-sm text-muted-foreground mb-2">条件定義</div>
                <div class="text-xs text-muted-foreground space-y-1">
                    <div>制度信用: RSI ≥ 70, ATR ≥ 8%</div>
                    <div>いちにち信用: RSI ≥ 90, ATR ≥ 9%</div>
                </div>
                <div class="text-xs text-muted-foreground mt-2">
                    <span class="text-foreground">タブ:</span> 通常=RSI/ATR非該当 | RSI=RSIのみ該当 | ATR=ATRのみ該当 | 両方=両方該当 | 全て=フィルタなし
                </div>
            </div>

            <div class="grid grid-cols-1 gap-4">
'''

    for wd in range(5):
        html += generate_weekday_card(df, wd)

    html += '''
            </div>
        </div>
    </main>

    <script>
    document.addEventListener('DOMContentLoaded', function() {
        const tabBtns = document.querySelectorAll('.wd-tab-btn');

        tabBtns.forEach(btn => {
            btn.addEventListener('click', function() {
                const weekday = this.dataset.weekday;
                const seg = this.dataset.seg;

                // 同じ曜日のタブボタンのスタイルをリセット
                document.querySelectorAll(`.wd-tab-btn[data-weekday="${weekday}"]`).forEach(b => {
                    b.classList.remove('active', 'bg-muted/50', 'text-foreground');
                    b.classList.add('text-muted-foreground');
                });

                // クリックしたボタンをアクティブに
                this.classList.add('active', 'bg-muted/50', 'text-foreground');
                this.classList.remove('text-muted-foreground');

                // コンテンツの表示切替
                document.querySelectorAll(`.wd-tab-content`).forEach(c => {
                    if (c.classList.contains(`wd${weekday}-${seg}`)) {
                        c.classList.add('active');
                    } else if (c.className.includes(`wd${weekday}-`)) {
                        c.classList.remove('active');
                    }
                });
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

    html = generate_html(df)

    output_path = Path(__file__).parent / 'output' / 'analysis_weekday_tab.html'
    output_path.write_text(html, encoding='utf-8')
    print(f"HTML生成完了: {output_path}")


if __name__ == '__main__':
    main()
