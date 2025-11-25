#!/usr/bin/env python3
"""
元のサマリーカード（v2.0.3/v2.1.0判定結果など）を復元
"""
from pathlib import Path
import pandas as pd

def main():
    # パス設定
    base_dir = Path(__file__).parent.parent.parent
    data_file = base_dir / 'improvement' / 'data' / 'v2_1_0_comparison_results.parquet'
    html_file = base_dir / 'improvement' / 'v2_1_0_comparison_report.html'

    # データ読み込み
    print(f"データ読み込み: {data_file}")
    df = pd.read_parquet(data_file)

    # 統計計算
    v2_0_3_counts = df['v2_0_3_action'].value_counts().to_dict()
    v2_1_0_counts = df['v2_1_0_action'].value_counts().to_dict()

    total_records = len(df)
    changed_records = df['action_changed'].sum()
    changed_pct = changed_records / total_records * 100

    # テクニカル指標の統計
    rsi_stats = df['rsi_14d'].describe()
    volume_stats = df['volume_change_20d'].describe()
    sma5_stats = df['price_vs_sma5_pct'].describe()

    # 変更パターンの集計
    change_patterns = {}
    for _, row in df[df['action_changed']].iterrows():
        pattern = f"{row['v2_0_3_action']} → {row['v2_1_0_action']}"
        change_patterns[pattern] = change_patterns.get(pattern, 0) + 1

    # 変更パターンのHTML
    pattern_rows = '\n'.join([
        f'                    <tr><td>{pattern}</td><td class="number">{count}</td></tr>'
        for pattern, count in sorted(change_patterns.items(), key=lambda x: -x[1])
    ])

    # 元のサマリーセクションHTML
    original_summary = f'''    <div class="summary-section">
        <div class="summary-grid">
            <div class="summary-card">
                <h3>📊 v2.0.3 判定結果</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{v2_0_3_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{v2_0_3_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{v2_0_3_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>🚀 v2.1.0 判定結果</h3>
                <div class="stat-row">
                    <span class="stat-label">買い</span>
                    <span class="stat-value">{v2_1_0_counts.get('買い', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">売り</span>
                    <span class="stat-value">{v2_1_0_counts.get('売り', 0)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">静観</span>
                    <span class="stat-value">{v2_1_0_counts.get('静観', 0)}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>🔄 判定変更</h3>
                <div class="stat-row">
                    <span class="stat-label">変更数</span>
                    <span class="stat-value">{changed_records}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">変更率</span>
                    <span class="stat-value">{changed_pct:.1f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">維持</span>
                    <span class="stat-value">{total_records - changed_records}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>📈 テクニカル指標統計</h3>
                <div class="stat-row">
                    <span class="stat-label">RSI平均</span>
                    <span class="stat-value">{rsi_stats['mean']:.1f}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">出来高変化平均</span>
                    <span class="stat-value">{volume_stats['mean']:.2f}x</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">SMA5乖離平均</span>
                    <span class="stat-value">{sma5_stats['mean']:.1f}%</span>
                </div>
            </div>
        </div>

        <div class="pattern-table">
            <h3 style="margin-bottom: 16px; color: #667eea;">変更パターン詳細</h3>
            <table>
                <thead>
                    <tr>
                        <th>変更パターン</th>
                        <th class="number">件数</th>
                    </tr>
                </thead>
                <tbody>
{pattern_rows}
                </tbody>
            </table>
        </div>
    </div>

'''

    # HTML読み込み
    print(f"HTML読み込み: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # ヘッダーの後、最初のsummary-sectionの前に挿入
    insert_marker = '\n    <div class="summary-section">'
    insert_idx = html_content.find(insert_marker)

    if insert_idx == -1:
        print("❌ エラー: 挿入位置が見つかりません")
        return

    # 元のサマリーセクションを挿入
    html_content = html_content[:insert_idx] + '\n' + original_summary + html_content[insert_idx:]

    # HTML保存
    print(f"HTML保存: {html_file}")
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print("✅ 完了: 元のサマリーカードを復元しました")
    print(f"\n📊 統計:")
    print(f"  v2.0.3: 買い={v2_0_3_counts.get('買い', 0)}, 売り={v2_0_3_counts.get('売り', 0)}, 静観={v2_0_3_counts.get('静観', 0)}")
    print(f"  v2.1.0: 買い={v2_1_0_counts.get('買い', 0)}, 売り={v2_1_0_counts.get('売り', 0)}, 静観={v2_1_0_counts.get('静観', 0)}")
    print(f"  変更: {changed_records}件 ({changed_pct:.1f}%)")

if __name__ == '__main__':
    main()
