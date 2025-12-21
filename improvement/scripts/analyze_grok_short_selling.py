#!/usr/bin/env python3
"""
GROK銘柄とセクター空売り比率の相関分析
"""

import pandas as pd
import numpy as np
from datetime import datetime
from scipy import stats
from pathlib import Path

# パス設定
IMPROVEMENT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = IMPROVEMENT_DIR / "data"
ARCHIVE_DIR = IMPROVEMENT_DIR / "archive" / "data"

# J-Quantsセクターコード対応表
JQUANTS_SECTOR_CODES = {
    50: '水産・農林業',
    1050: '鉱業',
    2050: '建設業',
    3050: '食料品',
    3100: '繊維製品',
    3150: 'パルプ・紙',
    3200: '化学',
    3250: '医薬品',
    3300: '石油・石炭製品',
    3350: 'ゴム製品',
    3400: 'ガラス・土石製品',
    3450: '鉄鋼',
    3500: '非鉄金属',
    3550: '金属製品',
    3600: '機械',
    3650: '電気機器',
    3700: '輸送用機器',
    3750: '精密機器',
    3800: 'その他製品',
    4050: '電気・ガス業',
    5050: '陸運業',
    5100: '海運業',
    5150: '空運業',
    5200: '倉庫・運輸関連業',
    5250: '情報・通信業',
    6050: '卸売業',
    6100: '小売業',
    7050: '銀行業',
    7100: '証券、商品先物取引業',
    7150: '保険業',
    7200: 'その他金融業',
    8050: '不動産業',
    9050: 'サービス業',
}

# セクター名 → コード（逆引き）
SECTOR_NAME_TO_CODE = {v: k for k, v in JQUANTS_SECTOR_CODES.items()}
# 表記揺れ対応
SECTOR_NAME_TO_CODE['証券・商品先物取引業'] = 7100
SECTOR_NAME_TO_CODE['情報･通信業'] = 5250


def normalize_sector(s):
    """セクター名の正規化"""
    if pd.isna(s):
        return s
    return s.replace('･', '・').replace('　', ' ').strip()


def load_data():
    """データ読み込み"""
    print("データ読み込み中...")

    grok = pd.read_parquet(DATA_DIR / "grok_trending_archive.parquet")
    meta = pd.read_parquet(DATA_DIR / "meta_jquants.parquet")
    short_selling = pd.read_csv(ARCHIVE_DIR / "short_selling_10y.csv")

    print(f"  GROK銘柄: {len(grok)}件")
    print(f"  銘柄マスタ: {len(meta)}件")
    print(f"  空売りデータ: {len(short_selling)}件")

    return grok, meta, short_selling


def merge_data(grok, meta, short_selling):
    """データ結合"""
    # GROK銘柄にセクター情報を付与
    grok = grok.merge(meta[['ticker', 'sectors']], on='ticker', how='left')

    # セクター名を正規化
    grok['sectors'] = grok['sectors'].apply(normalize_sector)

    # セクター名 → セクターコード
    grok['Sector33Code'] = grok['sectors'].map(SECTOR_NAME_TO_CODE)

    print(f"  セクターコード付与: {grok['Sector33Code'].notna().sum()}/{len(grok)}件")

    # 空売りデータの日付を変換
    short_selling['Date'] = pd.to_datetime(short_selling['Date'])
    grok['selection_date'] = pd.to_datetime(grok['selection_date'])

    # GROK銘柄に空売り比率を付与
    grok = grok.merge(
        short_selling[['Date', 'Sector33Code', 'short_ratio']],
        left_on=['selection_date', 'Sector33Code'],
        right_on=['Date', 'Sector33Code'],
        how='left'
    )

    print(f"  空売り比率付与: {grok['short_ratio'].notna().sum()}/{len(grok)}件")

    return grok


def analyze(grok):
    """相関分析"""
    results = {}

    valid_data = grok[grok['short_ratio'].notna() & grok['phase1_return'].notna()]
    results['valid_count'] = len(valid_data)
    results['total_count'] = len(grok)

    if len(valid_data) < 5:
        print("有効データが不足しています")
        return results, valid_data

    # 相関分析
    corr_phase1, pval_phase1 = stats.pearsonr(valid_data['short_ratio'], valid_data['phase1_return'])
    corr_phase2, pval_phase2 = stats.pearsonr(valid_data['short_ratio'], valid_data['phase2_return'])

    results['corr_phase1'] = corr_phase1
    results['pval_phase1'] = pval_phase1
    results['corr_phase2'] = corr_phase2
    results['pval_phase2'] = pval_phase2

    # セクター別集計
    sector_stats = valid_data.groupby('sectors').agg({
        'phase1_return': ['count', 'mean'],
        'phase2_return': 'mean',
        'phase1_win': 'mean',
        'phase2_win': 'mean',
        'short_ratio': 'mean'
    }).round(4)
    sector_stats.columns = ['件数', 'Phase1平均', 'Phase2平均', 'Phase1勝率', 'Phase2勝率', '空売り比率']
    sector_stats = sector_stats.sort_values('件数', ascending=False)
    results['sector_stats'] = sector_stats

    # 空売り比率クオンタイル分析
    try:
        valid_data = valid_data.copy()
        valid_data['short_quintile'] = pd.qcut(
            valid_data['short_ratio'], q=5,
            labels=['Q1(低)', 'Q2', 'Q3', 'Q4', 'Q5(高)'],
            duplicates='drop'
        )
        quintile_stats = valid_data.groupby('short_quintile', observed=True).agg({
            'phase1_return': ['count', 'mean'],
            'phase2_return': 'mean',
            'phase1_win': 'mean',
            'phase2_win': 'mean',
            'short_ratio': ['min', 'max']
        }).round(4)
        results['quintile_stats'] = quintile_stats
    except Exception as e:
        print(f"クオンタイル分析エラー: {e}")
        # 代替: 中央値で2分割
        median = valid_data['short_ratio'].median()
        valid_data['short_group'] = valid_data['short_ratio'].apply(
            lambda x: '高空売り' if x >= median else '低空売り'
        )
        group_stats = valid_data.groupby('short_group').agg({
            'phase1_return': ['count', 'mean'],
            'phase2_return': 'mean',
            'phase1_win': 'mean',
            'phase2_win': 'mean',
            'short_ratio': ['min', 'max', 'mean']
        }).round(4)
        results['group_stats'] = group_stats

    return results, valid_data


def generate_html(results, valid_data):
    """HTMLレポート生成"""

    corr_phase1 = results.get('corr_phase1', 0)
    corr_phase2 = results.get('corr_phase2', 0)
    pval_phase1 = results.get('pval_phase1', 1)
    pval_phase2 = results.get('pval_phase2', 1)

    # 相関の強さの解釈
    def interpret_corr(r):
        r = abs(r)
        if r < 0.1:
            return "ほぼなし"
        elif r < 0.3:
            return "弱い"
        elif r < 0.5:
            return "中程度"
        elif r < 0.7:
            return "強い"
        else:
            return "非常に強い"

    # セクター別テーブル
    sector_rows = ""
    if 'sector_stats' in results:
        for sector, row in results['sector_stats'].iterrows():
            phase1_class = 'positive' if row['Phase1平均'] > 0 else 'negative'
            phase2_class = 'positive' if row['Phase2平均'] > 0 else 'negative'
            sector_rows += f"""
            <tr>
                <td>{sector}</td>
                <td>{int(row['件数'])}</td>
                <td class="{phase1_class}">{row['Phase1平均']*100:+.2f}%</td>
                <td>{row['Phase1勝率']*100:.1f}%</td>
                <td class="{phase2_class}">{row['Phase2平均']*100:+.2f}%</td>
                <td>{row['Phase2勝率']*100:.1f}%</td>
                <td>{row['空売り比率']:.1f}%</td>
            </tr>"""

    # クオンタイル/グループ別テーブル
    group_rows = ""
    if 'quintile_stats' in results:
        for quintile, row in results['quintile_stats'].iterrows():
            phase1_mean = row[('phase1_return', 'mean')]
            phase2_mean = row[('phase2_return', 'mean')]
            phase1_class = 'positive' if phase1_mean > 0 else 'negative'
            phase2_class = 'positive' if phase2_mean > 0 else 'negative'
            group_rows += f"""
            <tr>
                <td>{quintile}</td>
                <td>{int(row[('phase1_return', 'count')])}</td>
                <td>{row[('short_ratio', 'min')]:.1f}% - {row[('short_ratio', 'max')]:.1f}%</td>
                <td class="{phase1_class}">{phase1_mean*100:+.2f}%</td>
                <td>{row[('phase1_win', 'mean')]*100:.1f}%</td>
                <td class="{phase2_class}">{phase2_mean*100:+.2f}%</td>
                <td>{row[('phase2_win', 'mean')]*100:.1f}%</td>
            </tr>"""
    elif 'group_stats' in results:
        for group, row in results['group_stats'].iterrows():
            phase1_mean = row[('phase1_return', 'mean')]
            phase2_mean = row[('phase2_return', 'mean')]
            phase1_class = 'positive' if phase1_mean > 0 else 'negative'
            phase2_class = 'positive' if phase2_mean > 0 else 'negative'
            group_rows += f"""
            <tr>
                <td>{group}</td>
                <td>{int(row[('phase1_return', 'count')])}</td>
                <td>{row[('short_ratio', 'min')]:.1f}% - {row[('short_ratio', 'max')]:.1f}%</td>
                <td class="{phase1_class}">{phase1_mean*100:+.2f}%</td>
                <td>{row[('phase1_win', 'mean')]*100:.1f}%</td>
                <td class="{phase2_class}">{phase2_mean*100:+.2f}%</td>
                <td>{row[('phase2_win', 'mean')]*100:.1f}%</td>
            </tr>"""

    # 銘柄別詳細テーブル（上位20件）
    detail_rows = ""
    if len(valid_data) > 0:
        top_data = valid_data.nlargest(20, 'short_ratio')
        for _, row in top_data.iterrows():
            phase1_class = 'positive' if row['phase1_return'] > 0 else 'negative'
            phase2_class = 'positive' if row['phase2_return'] > 0 else 'negative'
            detail_rows += f"""
            <tr>
                <td>{row['selection_date'].strftime('%Y-%m-%d') if pd.notna(row['selection_date']) else '-'}</td>
                <td>{row['ticker']}</td>
                <td>{row.get('stock_name', '-')}</td>
                <td>{row.get('sectors', '-')}</td>
                <td>{row['short_ratio']:.1f}%</td>
                <td class="{phase1_class}">{row['phase1_return']*100:+.2f}%</td>
                <td class="{phase2_class}">{row['phase2_return']*100:+.2f}%</td>
            </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GROK銘柄 × セクター空売り相関分析</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0a; color: #e0e0e0; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #4ade80; margin-bottom: 10px; }}
        h2 {{ color: #60a5fa; margin: 30px 0 15px; border-bottom: 1px solid #333; padding-bottom: 10px; }}
        .subtitle {{ color: #888; margin-bottom: 30px; }}
        .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin: 20px 0; }}
        .card {{ background: #1a1a1a; border-radius: 12px; padding: 20px; border: 1px solid #333; }}
        .card h3 {{ color: #888; font-size: 14px; margin-bottom: 8px; }}
        .card .value {{ font-size: 32px; font-weight: bold; }}
        .card .value.green {{ color: #4ade80; }}
        .card .value.blue {{ color: #60a5fa; }}
        .card .value.yellow {{ color: #fbbf24; }}
        .card .value.red {{ color: #f87171; }}
        .card .detail {{ color: #888; font-size: 14px; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; background: #1a1a1a; border-radius: 8px; overflow: hidden; }}
        th {{ background: #252525; color: #888; font-weight: 500; text-align: left; padding: 12px; font-size: 13px; }}
        td {{ padding: 12px; border-top: 1px solid #2a2a2a; font-size: 14px; }}
        tr:hover {{ background: #252525; }}
        .positive {{ color: #4ade80; }}
        .negative {{ color: #f87171; }}
        .summary-box {{ background: linear-gradient(135deg, #1a2e1a 0%, #1a1a2e 100%); border: 1px solid #4ade80; border-radius: 12px; padding: 25px; margin: 20px 0; }}
        .correlation-indicator {{ display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; margin-left: 10px; }}
        .corr-positive {{ background: rgba(74, 222, 128, 0.2); color: #4ade80; }}
        .corr-negative {{ background: rgba(248, 113, 113, 0.2); color: #f87171; }}
        .corr-neutral {{ background: rgba(136, 136, 136, 0.2); color: #888; }}
        .insight-box {{ background: #1a1a2e; border: 1px solid #60a5fa; border-radius: 8px; padding: 20px; margin: 20px 0; }}
        .insight-box h3 {{ color: #60a5fa; margin-bottom: 15px; }}
        .insight-box ul {{ list-style: none; }}
        .insight-box li {{ padding: 8px 0; color: #a0a0a0; }}
        .insight-box li::before {{ content: "→ "; color: #60a5fa; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>GROK銘柄 × セクター空売り相関分析</h1>
        <p class="subtitle">分析期間: {valid_data['selection_date'].min().strftime('%Y-%m-%d') if len(valid_data) > 0 else '-'} ~ {valid_data['selection_date'].max().strftime('%Y-%m-%d') if len(valid_data) > 0 else '-'} | 生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

        <h2>サマリー</h2>
        <div class="card-grid">
            <div class="card">
                <h3>分析対象</h3>
                <div class="value blue">{results['valid_count']}</div>
                <div class="detail">全{results['total_count']}件中、空売りデータ紐付け成功</div>
            </div>
            <div class="card">
                <h3>空売り比率 vs Phase1リターン</h3>
                <div class="value {'green' if corr_phase1 > 0 else 'red'}">{corr_phase1:+.3f}</div>
                <div class="detail">相関: {interpret_corr(corr_phase1)} (p={pval_phase1:.4f})</div>
            </div>
            <div class="card">
                <h3>空売り比率 vs Phase2リターン</h3>
                <div class="value {'green' if corr_phase2 > 0 else 'red'}">{corr_phase2:+.3f}</div>
                <div class="detail">相関: {interpret_corr(corr_phase2)} (p={pval_phase2:.4f})</div>
            </div>
        </div>

        <div class="insight-box">
            <h3>分析インサイト</h3>
            <ul>
                <li>Phase1（前場引け）と空売り比率の相関: <strong>{interpret_corr(corr_phase1)}</strong> {'（正の相関: 空売り多いセクターほど上昇傾向）' if corr_phase1 > 0.1 else '（負の相関: 空売り多いセクターほど下落傾向）' if corr_phase1 < -0.1 else '（相関なし）'}</li>
                <li>Phase2（大引け）と空売り比率の相関: <strong>{interpret_corr(corr_phase2)}</strong> {'（正の相関: 空売り多いセクターほど上昇傾向）' if corr_phase2 > 0.1 else '（負の相関: 空売り多いセクターほど下落傾向）' if corr_phase2 < -0.1 else '（相関なし）'}</li>
                <li>{'空売り比率が高いセクターの銘柄は、ショートカバーによる上昇が期待できる可能性' if corr_phase1 > 0.15 or corr_phase2 > 0.15 else '空売り比率とリターンに明確な関係は見られない'}</li>
            </ul>
        </div>

        <h2>空売り比率レンジ別パフォーマンス</h2>
        <table>
            <tr>
                <th>空売りレンジ</th>
                <th>件数</th>
                <th>空売り比率</th>
                <th>Phase1リターン</th>
                <th>Phase1勝率</th>
                <th>Phase2リターン</th>
                <th>Phase2勝率</th>
            </tr>
            {group_rows}
        </table>

        <h2>セクター別パフォーマンス</h2>
        <table>
            <tr>
                <th>セクター</th>
                <th>件数</th>
                <th>Phase1リターン</th>
                <th>Phase1勝率</th>
                <th>Phase2リターン</th>
                <th>Phase2勝率</th>
                <th>空売り比率</th>
            </tr>
            {sector_rows}
        </table>

        <h2>銘柄詳細（空売り比率上位20件）</h2>
        <table>
            <tr>
                <th>選定日</th>
                <th>ティッカー</th>
                <th>銘柄名</th>
                <th>セクター</th>
                <th>空売り比率</th>
                <th>Phase1</th>
                <th>Phase2</th>
            </tr>
            {detail_rows}
        </table>
    </div>
</body>
</html>"""

    return html


def main():
    print("=" * 60)
    print("GROK銘柄 × セクター空売り相関分析")
    print("=" * 60)

    # データ読み込み
    grok, meta, short_selling = load_data()

    # データ結合
    grok = merge_data(grok, meta, short_selling)

    # 分析
    results, valid_data = analyze(grok)

    if results['valid_count'] == 0:
        print("有効データがありません")
        return

    # HTML生成
    html = generate_html(results, valid_data)

    # 保存
    output_path = DATA_DIR / "grok_short_selling_analysis.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ HTMLレポート保存: {output_path}")

    # CSVも保存
    csv_path = DATA_DIR / "grok_short_selling_analysis.csv"
    valid_data.to_csv(csv_path, index=False)
    print(f"✅ CSVデータ保存: {csv_path}")


if __name__ == "__main__":
    main()
