#!/usr/bin/env python3
"""
売買タイミング分析結果をHTMLファイルにまとめる
"""
import pandas as pd
import numpy as np
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'test_output'
DATA_DIR = ROOT / 'data' / 'parquet'

# 入力ファイル
TIMING_ANALYSIS = OUTPUT_DIR / 'timing_analysis_results.parquet'
META_JQUANTS = DATA_DIR / 'meta_jquants.parquet'
GROK_ANALYSIS = DATA_DIR / 'backtest' / 'grok_analysis_merged.parquet'

# 出力ファイル
HTML_OUTPUT = OUTPUT_DIR / 'timing_analysis_report.html'


def analyze_by_factor(df, factor_column, timing_type='profit'):
    """要因別に分析してデータを返す

    Args:
        df: データフレーム
        factor_column: 要因のカラム名
        timing_type: 'profit' (利確タイミング) or 'loss' (損切りタイミング)
    """
    results = {}

    if factor_column not in df.columns:
        return results

    timing_column = 'better_profit_timing' if timing_type == 'profit' else 'better_loss_timing'

    for factor_value in sorted(df[factor_column].dropna().unique()):
        subset = df[df[factor_column] == factor_value]

        if len(subset) == 0:
            continue

        morning_better = (subset[timing_column] == 'morning_close').sum()
        day_better = (subset[timing_column] == 'day_close').sum()
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


def create_summary_table_html(all_factor_results, timing_type='profit'):
    """要因別分析のサマリーテーブルを生成"""
    if timing_type == 'profit':
        title = "📊 利確タイミング - 要因別サマリー"
        subtitle = "どの状況で前場/大引が有利か一覧"
    else:
        title = "🛡️ 損切りタイミング - 要因別サマリー"
        subtitle = "どの状況で前場/大引の損失が小さいか一覧"

    html = f"""
        <div class="comparison">
            <h2>{title}</h2>
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">{subtitle}</p>

            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; background: rgba(15, 23, 42, 0.6); border-radius: 8px;">
                    <thead>
                        <tr style="border-bottom: 2px solid #334155;">
                            <th style="padding: 1rem; text-align: left; color: #60a5fa; min-width: 120px;">要因</th>
                            <th style="padding: 1rem; text-align: left; color: #60a5fa; min-width: 150px;">カテゴリ</th>
                            <th style="padding: 1rem; text-align: center; color: #60a5fa; min-width: 80px;">件数</th>
                            <th style="padding: 1rem; text-align: center; color: #60a5fa; min-width: 120px;">有利なタイミング</th>
                            <th style="padding: 1rem; text-align: center; color: #60a5fa; min-width: 100px;">前場</th>
                            <th style="padding: 1rem; text-align: center; color: #60a5fa; min-width: 100px;">大引</th>
                            <th style="padding: 1rem; text-align: left; color: #60a5fa; min-width: 200px;">結論</th>
                        </tr>
                    </thead>
                    <tbody>
"""

    row_count = 0
    for factor_name, analysis_results in all_factor_results.items():
        for idx, (category, data) in enumerate(analysis_results.items()):
            morning_pct = data['morning_pct']
            day_pct = data['day_pct']

            # 有利なタイミングを判定
            if morning_pct > day_pct:
                better = "前場"
                better_color = "#fbbf24"
                better_bg = "rgba(251, 191, 36, 0.2)"
            elif day_pct > morning_pct:
                better = "大引"
                better_color = "#60a5fa"
                better_bg = "rgba(96, 165, 250, 0.2)"
            else:
                better = "同等"
                better_color = "#94a3b8"
                better_bg = "rgba(100, 116, 139, 0.2)"

            # 行の背景色を交互に
            row_bg = "rgba(30, 41, 59, 0.4)" if row_count % 2 == 0 else "rgba(15, 23, 42, 0.4)"

            html += f"""
                        <tr style="border-bottom: 1px solid #334155; background: {row_bg};">
                            <td style="padding: 0.75rem; color: #e2e8f0; font-weight: 600;">{factor_name if idx == 0 else ''}</td>
                            <td style="padding: 0.75rem; color: #cbd5e1;">{category}</td>
                            <td style="padding: 0.75rem; text-align: center; color: #94a3b8;">{data['total']}</td>
                            <td style="padding: 0.75rem; text-align: center;">
                                <span style="background: {better_bg}; color: {better_color}; padding: 0.25rem 0.75rem; border-radius: 12px; font-weight: 600; border: 1px solid {better_color};">
                                    {better}
                                </span>
                            </td>
                            <td style="padding: 0.75rem; text-align: center; color: {'#34d399' if data['avg_morning'] > 0 else '#f87171'};">
                                {data['avg_morning']:+.2f}%
                            </td>
                            <td style="padding: 0.75rem; text-align: center; color: {'#34d399' if data['avg_day'] > 0 else '#f87171'};">
                                {data['avg_day']:+.2f}%
                            </td>
                            <td style="padding: 0.75rem; color: #cbd5e1; font-size: 0.85rem;">
                                前場 {data['morning_better']}件 ({morning_pct:.0f}%) / 大引 {data['day_better']}件 ({day_pct:.0f}%)
                            </td>
                        </tr>
"""
            row_count += 1

    html += """
                    </tbody>
                </table>
            </div>
        </div>
"""
    return html


def create_factor_section_html(factor_name, analysis_results, timing_type='profit'):
    """要因別分析セクションのHTMLを生成

    Args:
        factor_name: 要因名
        analysis_results: 分析結果
        timing_type: 'profit' (利確) or 'loss' (損切り)
    """
    if not analysis_results:
        return ""

    if timing_type == 'profit':
        title = f"📊 {factor_name}別の分析（利確タイミング）"
        subtitle = "利益を確定するタイミング：前場 vs 大引"
    else:
        title = f"🛡️ {factor_name}別の分析（損切りタイミング）"
        subtitle = "損失を最小化するタイミング：前場 vs 大引"

    html = f"""
        <!-- {factor_name}別分析 ({timing_type}) -->
        <div class="comparison">
            <h2>{title}</h2>
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">{subtitle}</p>
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
        if timing_type == 'profit':
            timing_label = "利益が大きいタイミング"
            if morning_pct > day_pct:
                better = "前場不成が有利"
                better_color = "#fbbf24"
            elif day_pct > morning_pct:
                better = "大引不成が有利"
                better_color = "#60a5fa"
            else:
                better = "同等"
                better_color = "#94a3b8"
        else:  # loss
            timing_label = "損失が小さいタイミング"
            if morning_pct > day_pct:
                better = "前場損切りが有利（損失を抑える）"
                better_color = "#fbbf24"
            elif day_pct > morning_pct:
                better = "大引精算が有利（損失を抑える）"
                better_color = "#60a5fa"
            else:
                better = "同等"
                better_color = "#94a3b8"

        html += f"""
            <div style="background: rgba(15, 23, 42, 0.6); padding: 1.5rem; border-radius: 8px; margin-bottom: 1.5rem;">
                <h3 style="color: #e2e8f0; margin-bottom: 1rem;">{category} <span style="color: #94a3b8; font-size: 0.9rem;">({total}件)</span></h3>

                <div style="margin-bottom: 1rem;">
                    <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 0.5rem;">{timing_label}</div>
                    <div class="comparison-row" style="margin-bottom: 0.5rem;">
                        <span class="comparison-label">前場不成（11:30）</span>
                        <span class="comparison-value" style="color: #fbbf24;">{morning_better}件 ({morning_pct:.1f}%)</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {morning_pct}%; background: linear-gradient(90deg, #fbbf24, #fcd34d);">
                            {morning_pct:.1f}%
                        </div>
                    </div>

                    <div class="comparison-row" style="margin-bottom: 0.5rem; margin-top: 0.5rem;">
                        <span class="comparison-label">大引不成（15:30）</span>
                        <span class="comparison-value" style="color: #60a5fa;">{day_better}件 ({day_pct:.1f}%)</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {day_pct}%;">
                            {day_pct:.1f}%
                        </div>
                    </div>
                </div>

                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem;">
                    <div>
                        <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 0.5rem;">平均利益率</div>
                        <div style="color: {'#34d399' if avg_morning > 0 else '#f87171'}; font-weight: 600;">前場: {avg_morning:+.2f}%</div>
                        <div style="color: {'#34d399' if avg_day > 0 else '#f87171'}; font-weight: 600;">大引: {avg_day:+.2f}%</div>
                    </div>
                    <div>
                        <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 0.5rem;">勝率</div>
                        <div style="color: #60a5fa; font-weight: 600;">前場: {win_rate_morning:.0f}%</div>
                        <div style="color: #60a5fa; font-weight: 600;">大引: {win_rate_day:.0f}%</div>
                    </div>
                </div>

                <div style="padding: 0.75rem; background: rgba(59, 130, 246, 0.1); border-left: 4px solid {better_color}; border-radius: 4px;">
                    <strong style="color: {better_color};">結論:</strong>
                    <span style="color: #e2e8f0;">{better}</span>
                </div>
            </div>
"""

    html += """
        </div>
"""
    return html


def generate_html_report():
    """HTMLレポートを生成"""

    # データ読み込み
    df = pd.read_parquet(TIMING_ANALYSIS)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    # 2025-11-14のデータのみにフィルタ
    df = df[df['backtest_date'] == '2025-11-14'].copy()

    # meta_jquants.parquetから企業名を取得
    meta_df = pd.read_parquet(META_JQUANTS)
    ticker_to_name = dict(zip(meta_df['ticker'], meta_df['stock_name']))

    # 企業名をマージ
    df['company_name'] = df['ticker'].map(ticker_to_name).fillna('N/A')

    # grok_analysis_merged.parquetから追加情報を取得（要因分析用）
    grok_df = pd.read_parquet(GROK_ANALYSIS)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])
    grok_df = grok_df[grok_df['backtest_date'] == '2025-11-14'].copy()

    # マージ
    merge_cols = ['ticker', 'backtest_date', 'daily_volatility', 'prev_day_volume', 'market_cap']
    available_cols = ['ticker', 'backtest_date']
    for col in merge_cols[2:]:
        if col in grok_df.columns:
            available_cols.append(col)

    df = df.merge(grok_df[available_cols], on=['ticker', 'backtest_date'], how='left')

    # 要因分析用のカテゴリ分類
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
        if len(df_with_score) > 0:
            score_bins = [-np.inf, 20, 50, np.inf]
            score_labels = ['低スコア(-∞〜20)', '中スコア(20〜50)', '高スコア(50〜)']
            df_with_score['score_level'] = pd.cut(df_with_score['recommendation_score'],
                                                   bins=score_bins, labels=score_labels)
            # 元のdfにマージバック
            df = df.merge(df_with_score[['ticker', 'score_level']], on='ticker', how='left')

    # サマリー統計
    total_count = len(df)
    morning_better_count = (df['better_profit_timing'] == 'morning_close').sum()
    day_close_better_count = (df['better_profit_timing'] == 'day_close').sum()

    avg_profit_morning = df['profit_morning_pct'].mean()
    avg_profit_day_close = df['profit_day_close_pct'].mean()

    morning_win_rate = df['is_win_morning'].sum() / total_count * 100
    day_close_win_rate = df['is_win_day_close'].sum() / total_count * 100

    morning_better_loss_count = (df['better_loss_timing'] == 'morning_close').sum()
    day_close_better_loss_count = (df['better_loss_timing'] == 'day_close').sum()

    # HTML生成
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>売買タイミング最適化分析レポート</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #e2e8f0;
            padding: 2rem;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        h1 {{
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
            color: #60a5fa;
            text-align: center;
        }}

        .subtitle {{
            text-align: center;
            color: #94a3b8;
            margin-bottom: 2rem;
            font-size: 1.1rem;
        }}

        .summary {{
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
            backdrop-filter: blur(10px);
        }}

        .summary h2 {{
            font-size: 1.5rem;
            margin-bottom: 1rem;
            color: #60a5fa;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-top: 1.5rem;
        }}

        .stat-card {{
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 1.5rem;
        }}

        .stat-label {{
            font-size: 0.9rem;
            color: #94a3b8;
            margin-bottom: 0.5rem;
        }}

        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
        }}

        .stat-value.positive {{
            color: #34d399;
        }}

        .stat-value.negative {{
            color: #f87171;
        }}

        .stat-value.neutral {{
            color: #60a5fa;
        }}

        .comparison {{
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
        }}

        .comparison h2 {{
            font-size: 1.5rem;
            margin-bottom: 1.5rem;
            color: #60a5fa;
        }}

        .comparison-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem;
            background: rgba(15, 23, 42, 0.4);
            border-radius: 8px;
            margin-bottom: 1rem;
        }}

        .comparison-label {{
            font-size: 1.1rem;
            font-weight: 600;
        }}

        .comparison-value {{
            font-size: 1.3rem;
            font-weight: bold;
        }}

        .progress-bar {{
            width: 100%;
            height: 30px;
            background: rgba(15, 23, 42, 0.6);
            border-radius: 15px;
            overflow: hidden;
            margin: 1rem 0;
            position: relative;
        }}

        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #3b82f6, #60a5fa);
            transition: width 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 1rem;
            font-size: 0.9rem;
            font-weight: bold;
        }}

        .table-container {{
            background: rgba(30, 41, 59, 0.8);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
            overflow-x: auto;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}

        thead {{
            background: rgba(15, 23, 42, 0.8);
        }}

        th {{
            padding: 1rem;
            text-align: left;
            font-weight: 600;
            color: #60a5fa;
            border-bottom: 2px solid #334155;
        }}

        td {{
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #334155;
        }}

        td.number {{
            text-align: right;
        }}

        tr:hover {{
            background: rgba(51, 65, 85, 0.3);
        }}

        .positive {{
            color: #34d399;
        }}

        .negative {{
            color: #f87171;
        }}

        .badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.85rem;
            font-weight: 600;
        }}

        .badge-morning {{
            background: rgba(251, 191, 36, 0.2);
            color: #fbbf24;
            border: 1px solid #fbbf24;
        }}

        .badge-day {{
            background: rgba(96, 165, 250, 0.2);
            color: #60a5fa;
            border: 1px solid #60a5fa;
        }}

        .footer {{
            text-align: center;
            color: #64748b;
            margin-top: 3rem;
            padding-top: 2rem;
            border-top: 1px solid #334155;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 売買タイミング最適化分析レポート</h1>
        <p class="subtitle">利確・損切りタイミングの比較分析（2025-11-14）</p>

        <!-- サマリー統計 -->
        <div class="summary">
            <h2>📈 サマリー統計</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">分析銘柄数</div>
                    <div class="stat-value neutral">{total_count}件</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">平均利益率（前場不成）</div>
                    <div class="stat-value {'positive' if avg_profit_morning > 0 else 'negative'}">{avg_profit_morning:.2f}%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">平均利益率（大引不成）</div>
                    <div class="stat-value {'positive' if avg_profit_day_close > 0 else 'negative'}">{avg_profit_day_close:.2f}%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">勝率（前場不成）</div>
                    <div class="stat-value neutral">{morning_win_rate:.1f}%</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">勝率（大引不成）</div>
                    <div class="stat-value neutral">{day_close_win_rate:.1f}%</div>
                </div>
            </div>
        </div>

        <!-- 利確タイミング比較 -->
        <div class="comparison">
            <h2>💰 利確タイミング比較</h2>
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">どちらのタイミングで売却した方が利益が大きかったか</p>

            <div class="comparison-row">
                <span class="comparison-label">前場不成（11:30）</span>
                <span class="comparison-value" style="color: #fbbf24;">{morning_better_count}件 ({morning_better_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {morning_better_count/total_count*100}%; background: linear-gradient(90deg, #fbbf24, #fcd34d);">
                    {morning_better_count/total_count*100:.1f}%
                </div>
            </div>

            <div class="comparison-row">
                <span class="comparison-label">大引不成（15:30）</span>
                <span class="comparison-value" style="color: #60a5fa;">{day_close_better_count}件 ({day_close_better_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {day_close_better_count/total_count*100}%;">
                    {day_close_better_count/total_count*100:.1f}%
                </div>
            </div>

            <div style="margin-top: 2rem; padding: 1rem; background: rgba(59, 130, 246, 0.1); border-left: 4px solid #3b82f6; border-radius: 4px;">
                <strong style="color: #60a5fa;">結論:</strong>
                <span style="color: #e2e8f0;">
                    大引不成（15:30）の方が有利なケースが多い（{day_close_better_count/total_count*100:.1f}%）
                </span>
            </div>
        </div>

        <!-- 損切りタイミング比較 -->
        <div class="comparison">
            <h2>🛡️ 損切りタイミング比較</h2>
            <p style="color: #94a3b8; margin-bottom: 1.5rem;">どちらのタイミングで損切りした方が損失が小さかったか</p>

            <div class="comparison-row">
                <span class="comparison-label">前場損切り（11:30）</span>
                <span class="comparison-value" style="color: #fbbf24;">{morning_better_loss_count}件 ({morning_better_loss_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {morning_better_loss_count/total_count*100}%; background: linear-gradient(90deg, #fbbf24, #fcd34d);">
                    {morning_better_loss_count/total_count*100:.1f}%
                </div>
            </div>

            <div class="comparison-row">
                <span class="comparison-label">大引精算（15:30）</span>
                <span class="comparison-value" style="color: #60a5fa;">{day_close_better_loss_count}件 ({day_close_better_loss_count/total_count*100:.1f}%)</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {day_close_better_loss_count/total_count*100}%;">
                    {day_close_better_loss_count/total_count*100:.1f}%
                </div>
            </div>

            <div style="margin-top: 2rem; padding: 1rem; background: rgba(59, 130, 246, 0.1); border-left: 4px solid #3b82f6; border-radius: 4px;">
                <strong style="color: #60a5fa;">結論:</strong>
                <span style="color: #e2e8f0;">
                    大引精算（15:30）の方が損失が小さいケースが多い（{day_close_better_loss_count/total_count*100:.1f}%）
                </span>
            </div>
        </div>

        <!-- 詳細データテーブル -->
        <div class="table-container">
            <h2 style="color: #60a5fa; margin-bottom: 1.5rem;">📋 銘柄別詳細データ</h2>
            <table>
                <thead>
                    <tr>
                        <th>日付</th>
                        <th>銘柄コード</th>
                        <th>企業名</th>
                        <th>売買推奨</th>
                        <th>前場利益(100株)</th>
                        <th>大引利益(100株)</th>
                        <th>買値</th>
                        <th>前場終値</th>
                        <th>前場利益率</th>
                        <th>大引値</th>
                        <th>大引利益率</th>
                        <th>有利なタイミング</th>
                        <th>最高値</th>
                        <th>最安値</th>
                    </tr>
                </thead>
                <tbody>
"""

    # テーブル行を生成（スコア順）
    for _, row in df.sort_values('recommendation_score', ascending=False, na_position='last').iterrows():
        better_timing_badge = f'<span class="badge badge-morning">前場</span>' if row['better_profit_timing'] == 'morning_close' else f'<span class="badge badge-day">大引</span>'
        profit_morning_class = 'positive' if row['profit_morning_pct'] > 0 else 'negative'
        profit_day_class = 'positive' if row['profit_day_close_pct'] > 0 else 'negative'

        # 売買推奨のバッジ
        rec_action = row.get('recommendation_action')
        if rec_action == 'buy':
            rec_badge = '<span class="badge" style="background: rgba(34, 197, 94, 0.2); color: #22c55e; border: 1px solid #22c55e;">買い</span>'
        elif rec_action == 'sell':
            rec_badge = '<span class="badge" style="background: rgba(239, 68, 68, 0.2); color: #ef4444; border: 1px solid #ef4444;">売り</span>'
        elif rec_action == 'hold':
            rec_badge = '<span class="badge" style="background: rgba(251, 191, 36, 0.2); color: #fbbf24; border: 1px solid #fbbf24;">静観</span>'
        else:
            rec_badge = '<span class="badge" style="background: rgba(100, 116, 139, 0.2); color: #64748b; border: 1px solid #64748b;">N/A</span>'

        # 100株あたりの利益計算
        profit_morning_100 = row['profit_morning'] * 100
        profit_day_100 = row['profit_day_close'] * 100
        profit_morning_100_class = 'positive' if profit_morning_100 > 0 else 'negative'
        profit_day_100_class = 'positive' if profit_day_100 > 0 else 'negative'

        html += f"""                    <tr>
                        <td>{row['backtest_date'].strftime('%Y-%m-%d')}</td>
                        <td><strong>{row['ticker']}</strong></td>
                        <td>{row['company_name']}</td>
                        <td>{rec_badge}</td>
                        <td class="number {profit_morning_100_class}">{profit_morning_100:+,.0f}</td>
                        <td class="number {profit_day_100_class}">{profit_day_100:+,.0f}</td>
                        <td class="number">{row['buy_price']:,.0f}</td>
                        <td class="number">{row['morning_close_price']:,.0f}</td>
                        <td class="number {profit_morning_class}">{row['profit_morning_pct']:+.2f}%</td>
                        <td class="number">{row['day_close_price']:,.0f}</td>
                        <td class="number {profit_day_class}">{row['profit_day_close_pct']:+.2f}%</td>
                        <td>{better_timing_badge}</td>
                        <td class="number positive">{row['day_high']:,.0f} ({row['max_gain_pct']:+.2f}%)</td>
                        <td class="number negative">{row['day_low']:,.0f} ({row['max_loss_pct']:+.2f}%)</td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>
"""

    # 要因別分析セクションを追加
    factor_analyses = [
        ('ボラティリティ', 'volatility_level'),
        ('時価総額', 'market_cap_level'),
        ('株価水準', 'price_level'),
        ('売買推奨', 'recommendation_action'),
        ('スコア', 'score_level'),
        ('出来高', 'volume_level'),
    ]

    # 利確タイミング分析 - サマリーテーブル形式
    profit_results = {}
    for factor_name, factor_column in factor_analyses:
        analysis_results = analyze_by_factor(df, factor_column, timing_type='profit')
        if analysis_results:
            profit_results[factor_name] = analysis_results

    if profit_results:
        html += create_summary_table_html(profit_results, timing_type='profit')

    # 損切りタイミング分析 - サマリーテーブル形式
    loss_results = {}
    for factor_name, factor_column in factor_analyses:
        analysis_results = analyze_by_factor(df, factor_column, timing_type='loss')
        if analysis_results:
            loss_results[factor_name] = analysis_results

    if loss_results:
        html += create_summary_table_html(loss_results, timing_type='loss')

    html += """
        <div class="footer">
            <p>分析期間: 2025-11-14</p>
            <p>データソース: Yahoo Finance (5分足データ)</p>
            <p>生成日時: """ + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
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
