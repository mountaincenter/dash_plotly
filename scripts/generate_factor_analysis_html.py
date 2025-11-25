#!/usr/bin/env python3
"""
売買タイミング要因別分析結果をHTMLファイルにまとめる
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'test_output'
DATA_DIR = ROOT / 'data' / 'parquet'

# 入力ファイル
TIMING_ANALYSIS = OUTPUT_DIR / 'timing_analysis_results.parquet'
GROK_ANALYSIS = DATA_DIR / 'backtest' / 'grok_analysis_merged.parquet'

# 出力ファイル
HTML_OUTPUT = OUTPUT_DIR / 'timing_factor_analysis_report.html'


def create_factor_section(factor_name, analysis_results):
    """要因別分析セクションのHTMLを生成"""
    html = f"""
        <div class="factor-section">
            <h2 class="factor-title">📊 {factor_name}別の分析</h2>
            <div class="factor-grid">
"""

    for category, data in analysis_results.items():
        total = data['total']
        morning_better = data['morning_better']
        day_better = data['day_better']
        morning_pct = data['morning_pct']
        day_pct = data['day_pct']
        avg_morning = data['avg_morning']
        avg_day = data['avg_day']
        win_rate_morning = data['win_rate_morning']
        win_rate_day = data['win_rate_day']

        # 有利なタイミングを判定
        if morning_pct > day_pct:
            better = "前場不成"
            better_class = "better-morning"
        elif day_pct > morning_pct:
            better = "大引不成"
            better_class = "better-day"
        else:
            better = "同等"
            better_class = "better-equal"

        html += f"""
                <div class="factor-card">
                    <h3 class="category-name">{category} <span class="count">({total}件)</span></h3>

                    <div class="metric-row">
                        <div class="metric-label">有利なタイミング</div>
                        <div class="timing-comparison">
                            <div class="timing-bar">
                                <div class="timing-label">前場不成</div>
                                <div class="progress-container">
                                    <div class="progress-bar-small morning" style="width: {morning_pct}%">
                                        <span class="progress-text">{morning_better}件 ({morning_pct:.0f}%)</span>
                                    </div>
                                </div>
                            </div>
                            <div class="timing-bar">
                                <div class="timing-label">大引不成</div>
                                <div class="progress-container">
                                    <div class="progress-bar-small day" style="width: {day_pct}%">
                                        <span class="progress-text">{day_better}件 ({day_pct:.0f}%)</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="metric-row">
                        <div class="metric-label">平均利益率</div>
                        <div class="metric-values">
                            <div class="metric-value {'positive' if avg_morning > 0 else 'negative'}">
                                前場: {avg_morning:+.2f}%
                            </div>
                            <div class="metric-value {'positive' if avg_day > 0 else 'negative'}">
                                大引: {avg_day:+.2f}%
                            </div>
                        </div>
                    </div>

                    <div class="metric-row">
                        <div class="metric-label">勝率</div>
                        <div class="metric-values">
                            <div class="metric-value neutral">前場: {win_rate_morning:.0f}%</div>
                            <div class="metric-value neutral">大引: {win_rate_day:.0f}%</div>
                        </div>
                    </div>

                    <div class="conclusion {better_class}">
                        ✓ {better}
                    </div>
                </div>
"""

    html += """
            </div>
        </div>
"""
    return html


def analyze_by_factor(df, factor_column):
    """要因別に分析してデータを返す"""
    results = {}

    if factor_column not in df.columns:
        return results

    for factor_value in sorted(df[factor_column].dropna().unique()):
        subset = df[df[factor_column] == factor_value]

        if len(subset) == 0:
            continue

        morning_better = (subset['better_profit_timing'] == 'morning_close').sum()
        day_better = (subset['better_profit_timing'] == 'day_close').sum()
        total = len(subset)

        morning_pct = (morning_better / total * 100) if total > 0 else 0
        day_pct = (day_better / total * 100) if total > 0 else 0

        avg_morning = subset['profit_morning_pct'].mean()
        avg_day = subset['profit_day_close_pct'].mean()

        win_rate_morning = (subset['is_win_morning'].sum() / total * 100) if total > 0 else 0
        win_rate_day = (subset['is_win_day_close'].sum() / total * 100) if total > 0 else 0

        results[str(factor_value)] = {
            'total': total,
            'morning_better': morning_better,
            'day_better': day_better,
            'morning_pct': morning_pct,
            'day_pct': day_pct,
            'avg_morning': avg_morning,
            'avg_day': avg_day,
            'win_rate_morning': win_rate_morning,
            'win_rate_day': win_rate_day,
        }

    return results


def generate_html_report():
    """HTMLレポートを生成"""

    # データ読み込み
    df = pd.read_parquet(TIMING_ANALYSIS)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    # grok_analysis_merged.parquetから追加情報を取得
    grok_df = pd.read_parquet(GROK_ANALYSIS)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    # 2025-11-14のデータのみ
    df = df[df['backtest_date'] == '2025-11-14'].copy()
    grok_df = grok_df[grok_df['backtest_date'] == '2025-11-14'].copy()

    # マージ
    merge_cols = ['ticker', 'backtest_date', 'daily_volatility', 'prev_day_volume', 'market_cap']
    available_cols = ['ticker', 'backtest_date']
    for col in merge_cols[2:]:
        if col in grok_df.columns:
            available_cols.append(col)

    df = df.merge(grok_df[available_cols], on=['ticker', 'backtest_date'], how='left')

    # カテゴリ分類
    if 'daily_volatility' in df.columns:
        df['volatility_level'] = pd.qcut(df['daily_volatility'], q=3,
                                          labels=['低ボラ', '中ボラ', '高ボラ'],
                                          duplicates='drop')

    if 'market_cap' in df.columns:
        df['market_cap_level'] = pd.qcut(df['market_cap'], q=3,
                                          labels=['小型株', '中型株', '大型株'],
                                          duplicates='drop')

    df['price_level'] = pd.qcut(df['buy_price'], q=3,
                                 labels=['低価格帯', '中価格帯', '高価格帯'],
                                 duplicates='drop')

    if 'prev_day_volume' in df.columns:
        df['volume_level'] = pd.qcut(df['prev_day_volume'], q=3,
                                      labels=['低出来高', '中出来高', '高出来高'],
                                      duplicates='drop')

    if 'recommendation_score' in df.columns:
        df_with_score = df[df['recommendation_score'].notna()].copy()
        score_bins = [-np.inf, 20, 50, np.inf]
        score_labels = ['低スコア(-∞〜20)', '中スコア(20〜50)', '高スコア(50〜)']
        df_with_score['score_level'] = pd.cut(df_with_score['recommendation_score'],
                                               bins=score_bins, labels=score_labels)
        # 元のdfにマージバック
        df = df.merge(df_with_score[['ticker', 'score_level']], on='ticker', how='left')

    # 各要因別に分析
    analyses = {
        'ボラティリティ': ('volatility_level', analyze_by_factor(df, 'volatility_level')),
        '時価総額': ('market_cap_level', analyze_by_factor(df, 'market_cap_level')),
        '株価水準': ('price_level', analyze_by_factor(df, 'price_level')),
        '売買推奨': ('recommendation_action', analyze_by_factor(df, 'recommendation_action')),
        'スコア': ('score_level', analyze_by_factor(df, 'score_level')),
        '出来高': ('volume_level', analyze_by_factor(df, 'volume_level')),
    }

    # HTML生成開始
    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>売買タイミング要因別分析レポート</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #e2e8f0;
            padding: 2rem;
            line-height: 1.6;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
        }

        h1 {
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
            color: #60a5fa;
            text-align: center;
        }

        .subtitle {
            text-align: center;
            color: #94a3b8;
            margin-bottom: 3rem;
            font-size: 1.1rem;
        }

        .summary {
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 3rem;
        }

        .summary h2 {
            font-size: 1.5rem;
            margin-bottom: 1.5rem;
            color: #60a5fa;
        }

        .key-findings {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1rem;
        }

        .finding {
            background: rgba(15, 23, 42, 0.6);
            border-left: 4px solid #3b82f6;
            padding: 1rem;
            border-radius: 4px;
        }

        .finding-title {
            font-weight: 600;
            color: #60a5fa;
            margin-bottom: 0.5rem;
        }

        .finding-content {
            color: #cbd5e1;
            font-size: 0.9rem;
        }

        .factor-section {
            margin-bottom: 3rem;
        }

        .factor-title {
            font-size: 1.8rem;
            color: #60a5fa;
            margin-bottom: 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #334155;
        }

        .factor-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 1.5rem;
        }

        .factor-card {
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 1.5rem;
        }

        .category-name {
            font-size: 1.3rem;
            color: #e2e8f0;
            margin-bottom: 1rem;
        }

        .count {
            font-size: 0.9rem;
            color: #94a3b8;
            font-weight: normal;
        }

        .metric-row {
            margin-bottom: 1.5rem;
        }

        .metric-label {
            font-size: 0.85rem;
            color: #94a3b8;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .timing-comparison {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        .timing-bar {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .timing-label {
            min-width: 80px;
            font-size: 0.9rem;
            color: #cbd5e1;
        }

        .progress-container {
            flex: 1;
            background: rgba(15, 23, 42, 0.6);
            border-radius: 8px;
            height: 28px;
            overflow: hidden;
        }

        .progress-bar-small {
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 0.5rem;
            transition: width 0.3s ease;
        }

        .progress-bar-small.morning {
            background: linear-gradient(90deg, #fbbf24, #fcd34d);
        }

        .progress-bar-small.day {
            background: linear-gradient(90deg, #3b82f6, #60a5fa);
        }

        .progress-text {
            font-size: 0.85rem;
            font-weight: 600;
            color: #0f172a;
        }

        .metric-values {
            display: flex;
            gap: 1rem;
        }

        .metric-value {
            flex: 1;
            padding: 0.5rem;
            background: rgba(15, 23, 42, 0.4);
            border-radius: 4px;
            text-align: center;
            font-size: 0.9rem;
        }

        .metric-value.positive {
            color: #34d399;
        }

        .metric-value.negative {
            color: #f87171;
        }

        .metric-value.neutral {
            color: #60a5fa;
        }

        .conclusion {
            margin-top: 1rem;
            padding: 0.75rem;
            border-radius: 8px;
            text-align: center;
            font-weight: 600;
            font-size: 1.1rem;
        }

        .conclusion.better-morning {
            background: rgba(251, 191, 36, 0.2);
            color: #fbbf24;
            border: 2px solid #fbbf24;
        }

        .conclusion.better-day {
            background: rgba(59, 130, 246, 0.2);
            color: #60a5fa;
            border: 2px solid #60a5fa;
        }

        .conclusion.better-equal {
            background: rgba(100, 116, 139, 0.2);
            color: #94a3b8;
            border: 2px solid #64748b;
        }

        .footer {
            text-align: center;
            color: #64748b;
            margin-top: 3rem;
            padding-top: 2rem;
            border-top: 1px solid #334155;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 売買タイミング要因別分析レポート</h1>
        <p class="subtitle">規模・状況によるタイミング最適化の違い（2025-11-14）</p>

        <div class="summary">
            <h2>🔍 主要な発見</h2>
            <div class="key-findings">
                <div class="finding">
                    <div class="finding-title">📈 ボラティリティで異なる</div>
                    <div class="finding-content">
                        低ボラ: 前場不成が有利<br>
                        高ボラ・中ボラ: 大引不成が有利
                    </div>
                </div>
                <div class="finding">
                    <div class="finding-title">🎯 スコア別戦略</div>
                    <div class="finding-content">
                        高スコア(50〜): 大引不成が有利<br>
                        中スコア(20-50): 前場不成が有利
                    </div>
                </div>
                <div class="finding">
                    <div class="finding-title">📊 売買推奨別</div>
                    <div class="finding-content">
                        買い推奨: 大引不成が有利<br>
                        静観: 前場不成が有利
                    </div>
                </div>
                <div class="finding">
                    <div class="finding-title">💼 時価総額別</div>
                    <div class="finding-content">
                        小型株: 大引不成が有利<br>
                        大型株: 同等（前場の勝率高）
                    </div>
                </div>
            </div>
        </div>
"""

    # 各要因別のセクションを追加
    for factor_name, (column_name, analysis_data) in analyses.items():
        if analysis_data:
            html += create_factor_section(factor_name, analysis_data)

    # フッター
    html += f"""
        <div class="footer">
            <p>分析期間: 2025-11-14</p>
            <p>分析件数: {len(df)}件</p>
            <p>生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>
"""

    # HTMLファイルを保存
    with open(HTML_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ HTMLレポートを生成しました: {HTML_OUTPUT}")
    print(f"   ファイルサイズ: {HTML_OUTPUT.stat().st_size / 1024:.1f} KB")


if __name__ == '__main__':
    generate_html_report()
