#!/usr/bin/env python3
"""
空売り候補（出来高急騰 × 常習犯）HTML生成スクリプト

データソース:
- surge_candidates_5m.parquet: 5分足データ（出来高計算用）
- morning_peak_watchlist.parquet: 高値崩れ常習犯リスト
- margin_code_master.parquet: 信用取引区分

出力:
- short_candidates_tomorrow.html

使用方法:
  python3 generate_short_candidates_html.py [--date YYYY-MM-DD]

  --date: 基準日を指定（省略時は5分足データの最新日）
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# データファイル
SURGE_5M_PATH = DATA_DIR / "surge_candidates_5m.parquet"
WATCHLIST_5M_PATH = DATA_DIR / "watchlist_5m_latest.parquet"  # 常習犯のみの5分足
WATCHLIST_PATH = DATA_DIR / "morning_peak_watchlist.parquet"
MARGIN_PATH = DATA_DIR / "margin_code_master.parquet"
OUTPUT_PATH = OUTPUT_DIR / "short_candidates_tomorrow.html"

# 閾値
VOL_SURGE_MIN = 2.0  # 出来高急騰 最低倍率
VOL_SURGE_STRONG = 5.0  # 強シグナル
VOL_SURGE_EXTREME = 10.0  # 極大

# 価格帯別の出来高第1四分位（目安）
# 実際には動的に計算するが、簡易版として固定値を使用
VOLUME_Q1_BY_PRICE = {
    'under500': 21800,
    '500_1000': 30000,
    '1000_2000': 25000,
    '2000_5000': 20000,
    'over5000': 15000,
}


def load_data():
    """データ読み込み"""
    print("データ読み込み中...")

    # 5分足データ（常習犯のみの最新ファイルがあれば優先）
    if WATCHLIST_5M_PATH.exists():
        df_5m = pd.read_parquet(WATCHLIST_5M_PATH)
        print(f"  [使用] {WATCHLIST_5M_PATH.name}")
    else:
        df_5m = pd.read_parquet(SURGE_5M_PATH)
        print(f"  [使用] {SURGE_5M_PATH.name}")
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime'])
    df_5m['date'] = df_5m['Datetime'].dt.date
    print(f"  5分足: {len(df_5m):,}行, {df_5m['ticker'].nunique()}銘柄")
    print(f"  期間: {df_5m['date'].min()} 〜 {df_5m['date'].max()}")

    # 常習犯リスト
    df_watchlist = pd.read_parquet(WATCHLIST_PATH)
    print(f"  常習犯リスト: {len(df_watchlist)}銘柄")

    # 信用取引区分
    df_margin = pd.read_parquet(MARGIN_PATH)
    print(f"  信用区分マスタ: {len(df_margin)}件")

    return df_5m, df_watchlist, df_margin


def calc_volume_surge(df_5m, target_date):
    """出来高急騰率を計算"""
    print(f"\n出来高急騰率計算（基準日: {target_date}）...")

    # 日次出来高を集計
    daily_vol = df_5m.groupby(['ticker', 'date'])['Volume'].sum().reset_index()
    daily_vol.columns = ['ticker', 'date', 'daily_volume']

    # 日付でソート
    daily_vol = daily_vol.sort_values(['ticker', 'date'])

    # 過去20日の移動平均を計算
    daily_vol['vol_ma20'] = daily_vol.groupby('ticker')['daily_volume'].transform(
        lambda x: x.rolling(20, min_periods=10).mean().shift(1)
    )

    # 出来高急騰率
    daily_vol['vol_surge'] = daily_vol['daily_volume'] / daily_vol['vol_ma20']

    # 基準日のデータを抽出
    target = daily_vol[daily_vol['date'] == target_date].copy()
    target = target.dropna(subset=['vol_surge'])

    print(f"  基準日データ: {len(target)}銘柄")
    print(f"  急騰2倍以上: {len(target[target['vol_surge'] >= VOL_SURGE_MIN])}銘柄")

    return target


def get_price_band(price):
    """価格帯を判定"""
    if price < 500:
        return '500円未満', 'under500'
    elif price < 1000:
        return '500-1000円', '500_1000'
    elif price < 2000:
        return '1000-2000円', '1000_2000'
    elif price < 5000:
        return '2000-5000円', '2000_5000'
    else:
        return '5000円以上', 'over5000'


def merge_data(vol_surge_df, watchlist_df, margin_df, df_5m, target_date):
    """データをマージ"""
    print("\nデータマージ中...")

    # 出来高急騰（2倍以上）かつ常習犯リスト
    surge = vol_surge_df[vol_surge_df['vol_surge'] >= VOL_SURGE_MIN].copy()

    # 常習犯リストとマージ
    merged = surge.merge(watchlist_df, on='ticker', how='inner')
    print(f"  急騰 × 常習犯: {len(merged)}銘柄")

    # 終値を取得（基準日の終値）
    close_prices = df_5m[df_5m['date'] == target_date].groupby('ticker').agg({
        'Close': 'last'
    }).reset_index()
    close_prices.columns = ['ticker', 'latest_close_5m']

    merged = merged.merge(close_prices, on='ticker', how='left')
    # 終値はwatchlistのlatest_closeを優先、なければ5分足から
    merged['price'] = merged['latest_close'].fillna(merged['latest_close_5m'])

    # 価格帯
    merged['price_band'], merged['price_band_key'] = zip(*merged['price'].apply(get_price_band))

    # 出来高フィルタ（価格帯別第1四分位との比較）
    def check_volume(row):
        threshold = VOLUME_Q1_BY_PRICE.get(row['price_band_key'], 20000)
        return row['daily_volume'] > threshold

    merged['vol_ok'] = merged.apply(check_volume, axis=1)

    # 信用区分をマージ
    # margin_dfのカラム確認
    if 'ticker' in margin_df.columns or 'code' in margin_df.columns:
        margin_col = 'ticker' if 'ticker' in margin_df.columns else 'code'
        # tickerを.T付きで正規化
        if margin_col == 'code':
            margin_df = margin_df.copy()
            margin_df['ticker'] = margin_df['code'].astype(str) + '.T'

        merged = merged.merge(
            margin_df[['ticker', 'margin_code_name']].drop_duplicates(),
            on='ticker',
            how='left'
        )
    else:
        merged['margin_code_name'] = '不明'

    # 貸借銘柄かどうか
    merged['is_taishaku'] = merged['margin_code_name'].str.contains('貸借', na=False)

    print(f"  貸借銘柄: {merged['is_taishaku'].sum()}銘柄")
    print(f"  信用銘柄: {(~merged['is_taishaku']).sum()}銘柄")

    return merged


def generate_html(df, target_date):
    """HTML生成"""
    print("\nHTML生成中...")

    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    # 貸借銘柄と信用銘柄に分割
    taishaku = df[df['is_taishaku']].sort_values('vol_surge', ascending=False)
    shinyo = df[~df['is_taishaku']].sort_values('vol_surge', ascending=False)

    def vol_surge_class(v):
        if v >= VOL_SURGE_EXTREME:
            return 'vol-extreme', '極大'
        elif v >= VOL_SURGE_STRONG:
            return 'vol-high', '強'
        else:
            return 'vol-mid', '注目'

    def count_class(c):
        if c >= 10:
            return 'count-high'
        elif c >= 5:
            return 'count-mid'
        return 'count-low'

    def make_rows(subset, is_priority_section=False):
        rows = []
        # 1000-2000円帯を優先表示
        priority = subset[subset['price_band_key'] == '1000_2000'].copy()
        others = subset[subset['price_band_key'] != '1000_2000'].copy()

        for df_part, is_priority in [(priority, True), (others, False)]:
            for _, row in df_part.iterrows():
                ticker = row['ticker']
                code = ticker.replace('.T', '')
                name = str(row.get('stock_name', ticker))[:20]
                price = row.get('price', 0)
                vol_surge = row.get('vol_surge', 0)
                count = int(row.get('morning_peak_count', 0))
                avg_drop = row.get('avg_drop', 0)
                vol_ok = row.get('vol_ok', False)
                price_band = row.get('price_band', '-')

                vol_class, vol_label = vol_surge_class(vol_surge)
                cnt_class = count_class(count)
                vol_mark = '✓' if vol_ok else '△'
                vol_style = 'vol-ok' if vol_ok else 'vol-ng'
                row_class = 'priority-row' if is_priority else ''

                rows.append(f"""
        <tr class="{row_class}">
            <td><a href="https://finance.yahoo.co.jp/quote/{ticker}" target="_blank">{ticker}</a></td>
            <td class="name">{name}</td>
            <td class="number">{price:,.0f}円</td>
            <td class="number {vol_class}"><span class="badge {vol_class}">{vol_label}</span> {vol_surge:.1f}倍</td>
            <td class="number {cnt_class}">{count}回</td>
            <td class="number">{avg_drop:.1f}%</td>
            <td class="{vol_style}">{vol_mark}</td>
            <td>{price_band}</td>
        </tr>
""")
        return ''.join(rows)

    taishaku_rows = make_rows(taishaku)
    shinyo_rows = make_rows(shinyo)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>空売り候補（出来高急騰 × 常習犯）</title>
    <style>
        :root {{
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --border-color: #30363d;
            --accent-red: #f85149;
            --accent-orange: #d29922;
            --accent-green: #3fb950;
            --accent-blue: #58a6ff;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ font-size: 1.8rem; margin-bottom: 10px; }}
        .subtitle {{ color: var(--text-secondary); margin-bottom: 20px; }}
        .logic-box {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
        }}
        .logic-box h2 {{ font-size: 1.1rem; color: var(--accent-blue); margin-bottom: 15px; }}
        .logic-formula {{
            background: var(--bg-tertiary);
            padding: 15px;
            border-radius: 6px;
            font-family: monospace;
            margin-bottom: 15px;
        }}
        .logic-criteria {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
        }}
        .criteria-item {{
            background: var(--bg-tertiary);
            padding: 10px 15px;
            border-radius: 6px;
            border-left: 3px solid var(--accent-blue);
        }}
        .criteria-item.highlight {{ border-left-color: var(--accent-green); background: rgba(63, 185, 80, 0.1); }}
        .exclusion-note {{
            background: rgba(248, 81, 73, 0.1);
            border: 1px solid var(--accent-red);
            border-radius: 6px;
            padding: 10px 15px;
            margin-top: 15px;
            font-size: 0.9rem;
        }}
        .section {{ margin-bottom: 40px; }}
        .section-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 15px; }}
        .section-title {{ font-size: 1.3rem; }}
        .badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        .badge.vol-extreme {{ background: var(--accent-red); color: white; }}
        .badge.vol-high {{ background: var(--accent-orange); color: black; }}
        .badge.vol-mid {{ background: var(--accent-blue); color: white; }}
        .count-badge {{
            background: var(--bg-tertiary);
            color: var(--text-secondary);
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.9rem;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--bg-secondary);
            border-radius: 8px;
            overflow: hidden;
        }}
        th, td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid var(--border-color); }}
        th {{
            background: var(--bg-tertiary);
            font-weight: 600;
            color: var(--text-secondary);
            font-size: 0.85rem;
            text-transform: uppercase;
        }}
        tr:hover {{ background: var(--bg-tertiary); }}
        .priority-row {{ background: rgba(63, 185, 80, 0.05); }}
        .priority-row:hover {{ background: rgba(63, 185, 80, 0.1); }}
        .number {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .name {{ max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
        .vol-extreme {{ color: var(--accent-red); font-weight: 600; }}
        .vol-high {{ color: var(--accent-orange); font-weight: 600; }}
        .vol-mid {{ color: var(--accent-blue); }}
        .count-high {{ color: var(--accent-red); }}
        .count-mid {{ color: var(--accent-orange); }}
        .count-low {{ color: var(--text-primary); }}
        .vol-ok {{ color: var(--accent-green); }}
        .vol-ng {{ color: var(--text-secondary); }}
        a {{ color: var(--accent-blue); text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .subsection-title {{ font-size: 1rem; color: var(--text-secondary); margin-bottom: 10px; }}
        .priority-label {{
            display: inline-block;
            background: var(--accent-green);
            color: black;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-left: 10px;
        }}
        .footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
            color: var(--text-secondary);
            font-size: 0.85rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>空売り候補（出来高急騰 × 常習犯）</h1>
        <p class="subtitle">生成日時: {now} | 基準日: {target_date}</p>

        <div class="logic-box">
            <h2>選定ロジック（short_strategy.md準拠）</h2>
            <div class="logic-formula">
                <strong>出来高急騰率</strong> = 当日出来高 ÷ 過去20日平均出来高<br>
                <strong>抽出条件</strong>: 出来高急騰率 ≥ 2倍 AND 高値崩れ常習犯（3回以上）
            </div>
            <div class="logic-criteria">
                <div class="criteria-item">
                    <strong>2倍以上</strong><br>
                    <span style="color: var(--accent-blue);">→ 注目</span>
                </div>
                <div class="criteria-item">
                    <strong>5倍以上</strong><br>
                    <span style="color: var(--accent-orange);">→ 強いシグナル</span>
                </div>
                <div class="criteria-item">
                    <strong>10倍以上</strong><br>
                    <span style="color: var(--accent-red);">→ 極大（要警戒）</span>
                </div>
                <div class="criteria-item highlight">
                    <strong>1000-2000円帯</strong><br>
                    <span style="color: var(--accent-green);">→ 勝率64.7%（最優先）</span>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-header">
                <h2 class="section-title">貸借銘柄（制度信用可）</h2>
                <span class="count-badge">{len(taishaku)}銘柄</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>コード</th>
                        <th>銘柄名</th>
                        <th>株価</th>
                        <th>出来高急騰</th>
                        <th>常習回数</th>
                        <th>平均下落</th>
                        <th>出来高</th>
                        <th>価格帯</th>
                    </tr>
                </thead>
                <tbody>
                    {taishaku_rows}
                </tbody>
            </table>
        </div>

        <div class="section">
            <div class="section-header">
                <h2 class="section-title">信用銘柄（いちにち信用のみ）</h2>
                <span class="count-badge">{len(shinyo)}銘柄</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>コード</th>
                        <th>銘柄名</th>
                        <th>株価</th>
                        <th>出来高急騰</th>
                        <th>常習回数</th>
                        <th>平均下落</th>
                        <th>出来高</th>
                        <th>価格帯</th>
                    </tr>
                </thead>
                <tbody>
                    {shinyo_rows}
                </tbody>
            </table>
        </div>

        <div class="footer">
            <p><strong>データソース:</strong> surge_candidates_5m.parquet → morning_peak_watchlist.parquet</p>
            <p><strong>常習犯定義:</strong> 日中高値 → 終値-5%以上が3回以上</p>
            <p><strong>出来高フィルタ:</strong> ✓=価格帯別第1四分位超、△=第1四分位以下（流動性注意）</p>
        </div>
    </div>
</body>
</html>"""

    return html


def main():
    parser = argparse.ArgumentParser(description='空売り候補HTML生成')
    parser.add_argument('--date', type=str, help='基準日 (YYYY-MM-DD)')
    args = parser.parse_args()

    print("=" * 60)
    print("空売り候補（出来高急騰 × 常習犯）HTML生成")
    print("=" * 60)

    # データ読み込み
    df_5m, df_watchlist, df_margin = load_data()

    # 基準日決定
    if args.date:
        from datetime import date
        target_date = date.fromisoformat(args.date)
    else:
        target_date = df_5m['date'].max()

    print(f"\n基準日: {target_date}")

    # 出来高急騰率計算
    vol_surge_df = calc_volume_surge(df_5m, target_date)

    if vol_surge_df.empty:
        print(f"[ERROR] 基準日 {target_date} のデータがありません")
        return 1

    # データマージ
    merged = merge_data(vol_surge_df, df_watchlist, df_margin, df_5m, target_date)

    if merged.empty:
        print("[WARN] 条件に合致する銘柄がありません")
        return 1

    # HTML生成
    html = generate_html(merged, target_date)

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding='utf-8')

    print(f"\n✅ 保存完了: {OUTPUT_PATH}")
    print(f"   貸借銘柄: {merged['is_taishaku'].sum()}件")
    print(f"   信用銘柄: {(~merged['is_taishaku']).sum()}件")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
