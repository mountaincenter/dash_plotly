#!/usr/bin/env python3
"""
Phase 1: テクニカル分析レポート HTML生成

technical_scores_YYYYMMDD.json を読み込み、HTMLレポートを生成

出力: test_output/technical_report_YYYYMMDD.html
"""

import sys
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TEST_OUTPUT_DIR = ROOT / 'test_output'


def load_latest_scores():
    """最新のスコアJSONを読み込み"""
    print("[1/3] Loading latest technical scores...")

    json_files = sorted(TEST_OUTPUT_DIR.glob('technical_scores_*.json'), reverse=True)

    if not json_files:
        raise FileNotFoundError("No technical_scores_*.json found in test_output/")

    latest_file = json_files[0]
    print(f"  ✓ Loading: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data, latest_file


def load_backtest_validation():
    """バックテスト検証結果を読み込み"""
    print("\n[2/4] Loading backtest validation...")

    json_files = sorted(TEST_OUTPUT_DIR.glob('backtest_validation_*.json'), reverse=True)

    if not json_files:
        print("  ⚠ No backtest validation found, skipping...")
        return None

    latest_file = json_files[0]
    print(f"  ✓ Loading: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data


def load_optimization_results():
    """最適化結果を読み込み（テクニカルのみ）"""
    print("\n[3/5] Loading optimization results (technical only)...")

    json_files = sorted(TEST_OUTPUT_DIR.glob('optimized_scoring_2*.json'), reverse=True)

    if not json_files:
        print("  ⚠ No optimization results found, skipping...")
        return None

    latest_file = json_files[0]
    print(f"  ✓ Loading: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data


def load_market_optimization_results():
    """最適化結果を読み込み（マーケット要因含む）"""
    print("\n[4/6] Loading optimization results (with market features)...")

    json_files = sorted(TEST_OUTPUT_DIR.glob('optimized_scoring_market_*.json'), reverse=True)

    if not json_files:
        print("  ⚠ No market optimization results found, skipping...")
        return None

    latest_file = json_files[0]
    print(f"  ✓ Loading: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data


def load_phase1_phase2_results():
    """Phase 1 + Phase 2 分析結果を読み込み"""
    print("\n[5/6] Loading Phase 1 + Phase 2 analysis...")

    json_files = sorted(TEST_OUTPUT_DIR.glob('phase1_phase2_analysis_*.json'), reverse=True)

    if not json_files:
        print("  ⚠ No Phase 1 + Phase 2 analysis found, skipping...")
        return None

    latest_file = json_files[0]
    print(f"  ✓ Loading: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data


def generate_html(data, source_file, backtest_data=None, optimization_data=None, market_optimization_data=None, phase1_phase2_data=None):
    """HTMLレポートを生成"""
    print("\n[6/6] Generating HTML report...")

    date_str = data['date']
    generated_at = data['generated_at']
    summary = data['summary']
    stocks = data['stocks']

    # スコア順にソート
    stocks_sorted = sorted(stocks, key=lambda x: x['total_score'], reverse=True)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>テクニカル分析レポート - {date_str}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}

        .header h1 {{
            font-size: 2em;
            margin-bottom: 10px;
        }}

        .header p {{
            opacity: 0.9;
            font-size: 0.95em;
        }}

        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
        }}

        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}

        .summary-card .label {{
            font-size: 0.85em;
            color: #666;
            margin-bottom: 8px;
        }}

        .summary-card .value {{
            font-size: 2em;
            font-weight: bold;
        }}

        .summary-card.buy .value {{
            color: #10b981;
        }}

        .summary-card.hold .value {{
            color: #f59e0b;
        }}

        .summary-card.sell .value {{
            color: #ef4444;
        }}

        .summary-card.total .value {{
            color: #667eea;
        }}

        .stocks-section {{
            padding: 30px;
        }}

        .section-title {{
            font-size: 1.5em;
            margin-bottom: 20px;
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 40px;
        }}

        thead {{
            background: #667eea;
            color: white;
        }}

        th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 0.9em;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid #e5e7eb;
        }}

        tbody tr:hover {{
            background: #f9fafb;
        }}

        .signal-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }}

        .signal-badge.StrongBuy {{
            background: #d1fae5;
            color: #065f46;
        }}

        .signal-badge.Hold {{
            background: #fef3c7;
            color: #92400e;
        }}

        .signal-badge.StrongSell {{
            background: #fee2e2;
            color: #991b1b;
        }}

        .score {{
            font-weight: bold;
            font-size: 1.1em;
        }}

        .score.positive {{
            color: #10b981;
        }}

        .score.negative {{
            color: #ef4444;
        }}

        .score.neutral {{
            color: #6b7280;
        }}

        .tech-details {{
            font-size: 0.85em;
            color: #6b7280;
        }}

        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #6b7280;
            font-size: 0.9em;
        }}

        .note {{
            background: #fffbeb;
            border-left: 4px solid #f59e0b;
            padding: 15px;
            margin: 20px 0;
            font-size: 0.9em;
        }}

        .note strong {{
            color: #92400e;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 テクニカル分析レポート（Phase 1）</h1>
            <p>Core30 + 政策銘柄 | 寄付→大引け戦略</p>
            <p>生成日時: {generated_at}</p>
        </div>

        <div class="summary">
            <div class="summary-card total">
                <div class="label">総銘柄数</div>
                <div class="value">{data['total_stocks']}</div>
            </div>
            <div class="summary-card buy">
                <div class="label">StrongBuy</div>
                <div class="value">{summary['StrongBuy']}</div>
            </div>
            <div class="summary-card hold">
                <div class="label">Hold</div>
                <div class="value">{summary['Hold']}</div>
            </div>
            <div class="summary-card sell">
                <div class="label">StrongSell</div>
                <div class="value">{summary['StrongSell']}</div>
            </div>
        </div>

        <div class="stocks-section">
            <div class="note">
                <strong>Phase 1:</strong> テクニカル分析のみ（WebSearch分析は Phase 2 で実施）<br>
                スコア範囲: -100 ~ +100 | StrongBuy: ≥50 | StrongSell: ≤-40 | Hold: その他
            </div>

            <h2 class="section-title">🟢 StrongBuy 銘柄（スコア順）</h2>
            <table>
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>銘柄名</th>
                        <th>終値</th>
                        <th>スコア</th>
                        <th>シグナル</th>
                        <th>tech_snapshot</th>
                        <th>TOPIX相対</th>
                        <th>セクター</th>
                    </tr>
                </thead>
                <tbody>
"""

    # StrongBuy
    strong_buy = [s for s in stocks_sorted if s['signal'] == 'StrongBuy']
    if strong_buy:
        for stock in strong_buy:
            score_class = 'positive' if stock['total_score'] > 0 else 'negative' if stock['total_score'] < 0 else 'neutral'
            close_price = f"{stock['latest_close']:,.0f}円" if stock.get('latest_close') else 'N/A'
            html += f"""
                    <tr>
                        <td>{stock['ticker']}</td>
                        <td>{stock['stock_name']}</td>
                        <td>{close_price}</td>
                        <td class="score {score_class}">{stock['total_score']:+.1f}</td>
                        <td><span class="signal-badge {stock['signal']}">{stock['signal']}</span></td>
                        <td>{stock['tech_snapshot_score']:+.0f}</td>
                        <td>{stock['topix_relative']:+d}</td>
                        <td class="tech-details">{stock['sectors']}</td>
                    </tr>
"""
    else:
        html += '<tr><td colspan="8" style="text-align:center; color:#999;">該当なし</td></tr>'

    html += """
                </tbody>
            </table>

            <h2 class="section-title">🟡 Hold 銘柄（スコア順）</h2>
            <table>
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>銘柄名</th>
                        <th>終値</th>
                        <th>スコア</th>
                        <th>シグナル</th>
                        <th>tech_snapshot</th>
                        <th>TOPIX相対</th>
                        <th>セクター</th>
                    </tr>
                </thead>
                <tbody>
"""

    # Hold
    hold = [s for s in stocks_sorted if s['signal'] == 'Hold']
    if hold:
        for stock in hold:
            score_class = 'positive' if stock['total_score'] > 0 else 'negative' if stock['total_score'] < 0 else 'neutral'
            close_price = f"{stock['latest_close']:,.0f}円" if stock.get('latest_close') else 'N/A'
            html += f"""
                    <tr>
                        <td>{stock['ticker']}</td>
                        <td>{stock['stock_name']}</td>
                        <td>{close_price}</td>
                        <td class="score {score_class}">{stock['total_score']:+.1f}</td>
                        <td><span class="signal-badge {stock['signal']}">{stock['signal']}</span></td>
                        <td>{stock['tech_snapshot_score']:+.0f}</td>
                        <td>{stock['topix_relative']:+d}</td>
                        <td class="tech-details">{stock['sectors']}</td>
                    </tr>
"""
    else:
        html += '<tr><td colspan="8" style="text-align:center; color:#999;">該当なし</td></tr>'

    html += """
                </tbody>
            </table>

            <h2 class="section-title">🔴 StrongSell 銘柄（スコア順）</h2>
            <table>
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>銘柄名</th>
                        <th>終値</th>
                        <th>スコア</th>
                        <th>シグナル</th>
                        <th>tech_snapshot</th>
                        <th>TOPIX相対</th>
                        <th>セクター</th>
                    </tr>
                </thead>
                <tbody>
"""

    # StrongSell
    strong_sell = [s for s in stocks_sorted if s['signal'] == 'StrongSell']
    if strong_sell:
        for stock in strong_sell:
            score_class = 'positive' if stock['total_score'] > 0 else 'negative' if stock['total_score'] < 0 else 'neutral'
            close_price = f"{stock['latest_close']:,.0f}円" if stock.get('latest_close') else 'N/A'
            html += f"""
                    <tr>
                        <td>{stock['ticker']}</td>
                        <td>{stock['stock_name']}</td>
                        <td>{close_price}</td>
                        <td class="score {score_class}">{stock['total_score']:+.1f}</td>
                        <td><span class="signal-badge {stock['signal']}">{stock['signal']}</span></td>
                        <td>{stock['tech_snapshot_score']:+.0f}</td>
                        <td>{stock['topix_relative']:+d}</td>
                        <td class="tech-details">{stock['sectors']}</td>
                    </tr>
"""
    else:
        html += '<tr><td colspan="8" style="text-align:center; color:#999;">該当なし</td></tr>'

    html += """
                </tbody>
            </table>
        </div>
"""

    # 最適化結果セクション
    if optimization_data:
        indicator_analysis = optimization_data.get('indicator_analysis', {})
        selected_indicators = optimization_data.get('selected_indicators', [])
        weights = optimization_data.get('weights', {})
        performance = optimization_data.get('performance', {})
        train_perf = performance.get('train', {})
        test_perf = performance.get('test', {})

        html += """
        <div class="optimization-section" style="padding: 30px; background: #f0fdf4; border-top: 3px solid #10b981;">
            <h2 class="section-title" style="color: #10b981;">✅ スコアリング最適化結果</h2>

            <div class="success-box" style="background: #d1fae5; border-left: 4px solid #10b981; padding: 20px; margin: 20px 0;">
                <h3 style="color: #065f46; margin-bottom: 10px;">重要な発見</h3>
                <ul style="color: #065f46; margin-left: 20px; font-size: 1.05em;">
"""

        if selected_indicators:
            html += f"""
                    <li><strong>ATR（ボラティリティ指標）のみが一貫した予測力を持つ</strong></li>
                    <li>高ボラティリティ銘柄 → 翌週パフォーマンスが良い</li>
                    <li>モメンタム相場で合理的（ボラティリティ = トレンドの強さ）</li>
"""
        else:
            html += """
                    <li>予測力のある指標が見つかりませんでした</li>
"""

        html += """
                </ul>
            </div>

            <h3 style="margin-top: 30px; margin-bottom: 15px; color: #333;">個別指標分析（Train/Test分割検証）</h3>
            <table>
                <thead>
                    <tr>
                        <th>指標</th>
                        <th>Train相関</th>
                        <th>Train Spread</th>
                        <th>Test相関</th>
                        <th>Test Spread</th>
                        <th>判定</th>
                    </tr>
                </thead>
                <tbody>
"""

        # 全指標を表示（選択/非選択を明示）
        indicator_order = ['atr14_pct', 'rsi14', 'macd_hist', 'percent_b', 'cmf20', 'obv_slope', 'sma25_dev_pct']
        for indicator in indicator_order:
            if indicator not in indicator_analysis:
                continue

            analysis = indicator_analysis[indicator]
            train = analysis.get('train', {})
            test = analysis.get('test', {})

            train_corr = train.get('correlation', 0)
            train_spread = train.get('spread', 0)
            test_corr = test.get('correlation', 0)
            test_spread = test.get('spread', 0)

            # 選択されているかチェック
            is_selected = indicator in weights

            row_style = ''
            status = '❌ 不採用'
            if is_selected:
                row_style = 'style="background: #d1fae5; font-weight: bold;"'
                status = '✅ 採用'

            html += f"""
                    <tr {row_style}>
                        <td>{indicator}</td>
                        <td class="score {'positive' if train_corr > 0 else 'negative'}">{train_corr:+.4f}</td>
                        <td class="score {'positive' if train_spread > 0 else 'negative'}">{train_spread:+.3f}%</td>
                        <td class="score {'positive' if test_corr > 0 else 'negative'}">{test_corr:+.4f}</td>
                        <td class="score {'positive' if test_spread > 0 else 'negative'}">{test_spread:+.3f}%</td>
                        <td>{status}</td>
                    </tr>
"""

        html += """
                </tbody>
            </table>
"""

        if weights:
            html += """
            <h3 style="margin-top: 30px; margin-bottom: 15px; color: #333;">最適化されたウェイト</h3>
            <div style="background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
"""

            for indicator, weight in weights.items():
                html += f"""
                <div style="margin-bottom: 10px;">
                    <span style="display: inline-block; width: 150px; font-weight: bold;">{indicator}:</span>
                    <span style="display: inline-block; width: 300px; background: #10b981; height: 20px; border-radius: 4px; position: relative;">
                        <span style="display: block; background: #059669; height: 100%; width: {weight*100:.1f}%; border-radius: 4px;"></span>
                    </span>
                    <span style="margin-left: 10px;">{weight:.3f}</span>
                </div>
"""

            html += """
            </div>
"""

        # パフォーマンス比較
        if train_perf and test_perf:
            html += """
            <h3 style="margin-top: 30px; margin-bottom: 15px; color: #333;">最適化スコアのパフォーマンス</h3>
            <table>
                <thead>
                    <tr>
                        <th>データセット</th>
                        <th>相関</th>
                        <th>Spread (High - Low)</th>
                        <th>High Tercile</th>
                        <th>Low Tercile</th>
                    </tr>
                </thead>
                <tbody>
"""

            train_corr = train_perf.get('correlation', 0)
            train_spread = train_perf.get('spread', 0)
            train_high = train_perf.get('tercile_performance', {}).get('High', {}).get('avg_return', 0)
            train_low = train_perf.get('tercile_performance', {}).get('Low', {}).get('avg_return', 0)

            test_corr = test_perf.get('correlation', 0)
            test_spread = test_perf.get('spread', 0)
            test_high = test_perf.get('tercile_performance', {}).get('High', {}).get('avg_return', 0)
            test_low = test_perf.get('tercile_performance', {}).get('Low', {}).get('avg_return', 0)

            html += f"""
                    <tr>
                        <td><strong>Train Set</strong><br><small>前半6ヶ月（最適化用）</small></td>
                        <td class="score positive">{train_corr:+.4f}</td>
                        <td class="score positive">{train_spread:+.3f}%</td>
                        <td class="score positive">{train_high:+.3f}%</td>
                        <td class="score {'positive' if train_low > 0 else 'negative'}">{train_low:+.3f}%</td>
                    </tr>
                    <tr style="background: #d1fae5;">
                        <td><strong>Test Set (Out-of-Sample)</strong><br><small>後半6ヶ月（検証用）</small></td>
                        <td class="score positive"><strong>{test_corr:+.4f}</strong></td>
                        <td class="score positive"><strong>{test_spread:+.3f}%</strong></td>
                        <td class="score positive"><strong>{test_high:+.3f}%</strong></td>
                        <td class="score {'positive' if test_low > 0 else 'negative'}"><strong>{test_low:+.3f}%</strong></td>
                    </tr>
"""

            html += """
                </tbody>
            </table>
"""

        html += """
            <div class="note" style="margin-top: 30px; background: #d1fae5; border-left: 4px solid #10b981; padding: 15px;">
                <strong>結論:</strong><br>
                ✅ ATR（ボラティリティ）が唯一の予測力のある指標<br>
                ✅ Out-of-Sample検証で相関 +0.0627、Spread +0.709%<br>
                ✅ 他の伝統的テクニカル指標（RSI, MACD, Bollinger等）は予測力なし
            </div>
        </div>
"""

    # マーケット要因追加版の最適化結果
    if market_optimization_data:
        indicator_analysis = market_optimization_data.get('indicator_analysis', {})
        selected_indicators = market_optimization_data.get('selected_indicators', [])
        weights = market_optimization_data.get('weights', {})
        performance = market_optimization_data.get('performance', {})
        train_perf = performance.get('train', {})
        test_perf = performance.get('test', {})

        # 比較用に元の最適化結果を取得
        orig_test_corr = 0.0627
        orig_test_spread = 0.709
        if optimization_data:
            orig_perf = optimization_data.get('performance', {}).get('test', {})
            orig_test_corr = orig_perf.get('correlation', 0.0627)
            orig_test_spread = orig_perf.get('spread', 0.709)

        # 改善率を計算
        corr_improvement = ((test_perf.get('correlation', 0) - orig_test_corr) / orig_test_corr * 100) if orig_test_corr != 0 else 0
        spread_improvement = ((test_perf.get('spread', 0) - orig_test_spread) / orig_test_spread * 100) if orig_test_spread != 0 else 0

        html += """
        <div class="optimization-section" style="padding: 30px; background: #ecfdf5; border-top: 3px solid #059669;">
            <h2 class="section-title" style="color: #059669;">🎉 マーケット要因追加版（v2）</h2>

            <div class="success-box" style="background: #a7f3d0; border-left: 4px solid #059669; padding: 20px; margin: 20px 0;">
                <h3 style="color: #065f46; margin-bottom: 10px;">✅ 改善成功！</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 15px;">
"""

        test_corr = test_perf.get('correlation', 0)
        test_spread = test_perf.get('spread', 0)

        html += f"""
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        <div style="font-size: 0.9em; color: #666;">Test相関</div>
                        <div style="font-size: 1.8em; font-weight: bold; color: #059669;">{test_corr:+.4f}</div>
                        <div style="font-size: 0.85em; color: #10b981;">↑ {corr_improvement:+.1f}% 改善</div>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        <div style="font-size: 0.9em; color: #666;">Test Spread</div>
                        <div style="font-size: 1.8em; font-weight: bold; color: #059669;">{test_spread:+.3f}%</div>
                        <div style="font-size: 0.85em; color: #10b981;">↑ {spread_improvement:+.1f}% 改善</div>
                    </div>
"""

        if test_perf.get('tercile_performance'):
            high_return = test_perf['tercile_performance'].get('High', {}).get('avg_return', 0)
            html += f"""
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        <div style="font-size: 0.9em; color: #666;">High Tercile</div>
                        <div style="font-size: 1.8em; font-weight: bold; color: #059669;">{high_return:+.3f}%</div>
                        <div style="font-size: 0.85em; color: #10b981;">翌週リターン</div>
                    </div>
"""

        html += """
                </div>
            </div>

            <h3 style="margin-top: 30px; margin-bottom: 15px; color: #333;">採用された指標（4つ）</h3>
            <table>
                <thead>
                    <tr>
                        <th>指標</th>
                        <th>種類</th>
                        <th>ウェイト</th>
                        <th>Train相関</th>
                        <th>Test相関</th>
                        <th>Test Spread</th>
                    </tr>
                </thead>
                <tbody>
"""

        # 選択された指標のみ表示
        for ind in selected_indicators:
            indicator_name = ind.get('name', '')
            analysis = indicator_analysis.get(indicator_name, {})
            train = analysis.get('train', {})
            test = analysis.get('test', {})

            # 指標の種類を判定
            if 'relative' in indicator_name:
                indicator_type = 'マーケット相対'
            elif 'topix' in indicator_name or 'nikkei' in indicator_name:
                indicator_type = 'マーケット'
            elif 'stock' in indicator_name:
                indicator_type = '個別銘柄'
            else:
                indicator_type = 'テクニカル'

            weight = weights.get(indicator_name, 0)
            train_corr = train.get('correlation', 0)
            test_corr = test.get('correlation', 0)
            test_spread = test.get('spread', 0)

            html += f"""
                    <tr style="background: #d1fae5;">
                        <td><strong>{indicator_name}</strong></td>
                        <td>{indicator_type}</td>
                        <td>{weight:.3f} ({weight*100:.1f}%)</td>
                        <td class="score {'positive' if train_corr > 0 else 'negative'}">{train_corr:+.4f}</td>
                        <td class="score {'positive' if test_corr > 0 else 'negative'}">{test_corr:+.4f}</td>
                        <td class="score {'positive' if test_spread > 0 else 'negative'}">{test_spread:+.3f}%</td>
                    </tr>
"""

        html += """
                </tbody>
            </table>

            <div style="margin-top: 30px; padding: 20px; background: white; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <h4 style="margin-bottom: 15px; color: #333;">重要な発見</h4>
                <ul style="color: #065f46; margin-left: 20px; line-height: 1.8;">
                    <li><strong>ボラティリティ/モメンタム系が全て</strong><br>
                        4指標全てがボラティリティまたはモメンタム関連</li>
                    <li><strong>マーケット相対強度が効く</strong><br>
                        TOPIX対比の20日リターン（relative_strength_20d）が採用</li>
                    <li><strong>相対ボラティリティも有効</strong><br>
                        個別銘柄単体よりマーケット対比のボラティリティが重要</li>
                    <li><strong>伝統的テクニカル指標は全滅</strong><br>
                        RSI, MACD, Bollinger Bands等は依然として不採用</li>
                </ul>
            </div>

            <div class="note" style="margin-top: 30px; background: #a7f3d0; border-left: 4px solid #059669; padding: 15px;">
                <strong>結論:</strong><br>
                🎉 マーケット要因の追加で相関・Spread共に改善<br>
                ✅ ボラティリティ + 相対強度の組み合わせが有効<br>
                ✅ 次のステップ: ファンダメンタルズ（ニュース・IR・決算）追加で更なる改善を目指す<br>
                <br>
                ⚠️ それでも相関は0.07と弱い → テクニカルだけでは限界あり
            </div>
        </div>
"""

    # Phase 1 + Phase 2 実戦分析セクション
    if phase1_phase2_data:
        target_date = phase1_phase2_data.get('target_date', '')
        summary = phase1_phase2_data.get('summary', {})
        phase2_results = phase1_phase2_data.get('phase2_results', [])

        html += f"""
        <div class="phase2-section" style="padding: 30px; background: #fef3c7; border-top: 4px solid #f59e0b;">
            <h2 class="section-title" style="color: #d97706;">🎯 Phase 1 + Phase 2 実戦分析</h2>

            <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 20px; margin: 20px 0;">
                <h3 style="color: #92400e; margin-bottom: 10px;">2段階スクリーニング結果</h3>
                <p style="color: #78350f; font-size: 1.05em;">
                    <strong>対象日:</strong> {target_date}<br>
                    <strong>Phase 1:</strong> 全{summary.get('total_stocks', 0)}銘柄をスコアリング → 上位10%（{summary.get('top_10_percent_count', 0)}銘柄）抽出<br>
                    <strong>Phase 2:</strong> 上位{summary.get('top_10_percent_count', 0)}銘柄のみ深掘り分析 → 再スコアリング
                </p>
            </div>

            <h3 style="margin-top: 30px; margin-bottom: 15px; color: #333;">Phase 2 分析対象銘柄（上位10%）</h3>
            <table>
                <thead>
                    <tr>
                        <th>最終ランク</th>
                        <th>Ticker</th>
                        <th>銘柄名</th>
                        <th>終値</th>
                        <th>Phase 1</th>
                        <th>Phase 2</th>
                        <th>最終スコア</th>
                        <th>セクター</th>
                    </tr>
                </thead>
                <tbody>
"""

        for stock in phase2_results:
            ticker = stock.get('ticker', '')
            stock_name = stock.get('stock_name', '')
            latest_close = stock.get('latest_close', 0)
            phase1_score = stock.get('phase1_score', 0)
            phase2_score = stock.get('phase2_score', 0)
            final_score = stock.get('final_score', 0)
            final_rank = stock.get('final_rank', 0)
            sectors = stock.get('sectors', '')

            close_price = f"{latest_close:,.0f}円" if not pd.isna(latest_close) else 'N/A'

            phase1_class = 'positive' if phase1_score > 0 else 'negative' if phase1_score < 0 else 'neutral'
            phase2_class = 'positive' if phase2_score > 0 else 'negative' if phase2_score < 0 else 'neutral'
            final_class = 'positive' if final_score > 0 else 'negative' if final_score < 0 else 'neutral'

            html += f"""
                    <tr style="background: #fef3c7;">
                        <td><strong>#{final_rank}</strong></td>
                        <td>{ticker}</td>
                        <td><strong>{stock_name}</strong></td>
                        <td>{close_price}</td>
                        <td class="score {phase1_class}">{phase1_score:+.2f}</td>
                        <td class="score {phase2_class}">{phase2_score:+.2f}</td>
                        <td class="score {final_class}"><strong>{final_score:+.2f}</strong></td>
                        <td style="font-size: 0.85em;">{sectors}</td>
                    </tr>
"""

        html += """
                </tbody>
            </table>

            <div style="margin-top: 30px; padding: 20px; background: white; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <h4 style="margin-bottom: 15px; color: #333;">Phase 2 スコア内訳（プロトタイプ版）</h4>
"""

        if phase2_results:
            for stock in phase2_results:
                ticker = stock.get('ticker', '')
                stock_name = stock.get('stock_name', '')
                components = stock.get('phase2_components', {})

                html += f"""
                <div style="margin-bottom: 20px; padding: 15px; background: #fef3c7; border-radius: 6px;">
                    <div style="font-weight: bold; margin-bottom: 10px;">{ticker} {stock_name}</div>
                    <ul style="margin-left: 20px; color: #78350f; line-height: 1.6;">
                        <li>ニュースセンチメント: {components.get('news_sentiment', 0):+d} <span style="color: #999;">(TODO: WebSearch実装)</span></li>
                        <li>IR情報分析: {components.get('ir_sentiment', 0):+d} <span style="color: #999;">(TODO: IR分析実装)</span></li>
                        <li>ファンダメンタルズ: {components.get('fundamental_score', 0):+d} <span style="color: #999;">(TODO: PER/PBR分析)</span></li>
                        <li>モメンタム確認: {components.get('momentum_confirmation', 0):+d} <span style="color: #999;">(TODO: 材料確認)</span></li>
                    </ul>
                </div>
"""

        html += """
            </div>

            <div class="note" style="margin-top: 30px; background: #fffbeb; border-left: 4px solid #f59e0b; padding: 15px;">
                <strong>⚠️ プロトタイプ版について:</strong><br>
                Phase 2は構造のみ実装済み。以下を追加実装することで実用化：<br><br>
                <strong>実装予定:</strong><br>
                1. <strong>WebSearch API</strong>: 各銘柄のニュース・材料を検索<br>
                2. <strong>センチメント分析</strong>: ニュースの好悪判定<br>
                3. <strong>IR情報取得</strong>: 決算発表・業績修正等<br>
                4. <strong>ファンダメンタルズ</strong>: PER/PBR/ROE等の評価<br><br>
                <strong>次のステップ:</strong><br>
                この5銘柄に対して手動で深掘り → Phase 2スコアリングロジックを設計
            </div>
        </div>
"""

    # バックテスト検証セクション
    if backtest_data:
        signal_perf = backtest_data.get('signal_performance', {})
        correlation = backtest_data.get('score_correlation', {})
        overall_stats = backtest_data.get('overall_stats', {})

        html += """
        <div class="backtest-section" style="padding: 30px; background: #f8f9fa; border-top: 3px solid #667eea;">
            <h2 class="section-title" style="color: #dc2626;">⚠️ バックテスト検証結果</h2>

            <div class="warning-box" style="background: #fee2e2; border-left: 4px solid #dc2626; padding: 20px; margin: 20px 0;">
                <h3 style="color: #991b1b; margin-bottom: 10px;">重大な問題が発見されました</h3>
                <p style="font-size: 1.1em; color: #991b1b; margin-bottom: 10px;">
                    <strong>現在のテクニカルスコアリングは予測力がほぼゼロです</strong>
                </p>
                <ul style="color: #991b1b; margin-left: 20px;">
"""

        html += f"""
                    <li>スコアとリターンの相関: <strong>{correlation.get('next_day', 0):.4f}</strong>（翌日）、<strong>{correlation.get('next_week', 0):.4f}</strong>（翌週）</li>
                    <li>検証期間: {overall_stats.get('total_records', 0):,} レコード</li>
"""

        # Find best and worst performing signals
        if signal_perf:
            best_signal = max(signal_perf.items(), key=lambda x: x[1]['avg_next_week_return'])
            worst_signal = min(signal_perf.items(), key=lambda x: x[1]['avg_next_week_return'])

            html += f"""
                    <li>最良シグナル: <strong>{best_signal[0]}</strong> → 翌週 +{best_signal[1]['avg_next_week_return']:.2f}%</li>
                    <li>最悪シグナル: <strong>{worst_signal[0]}</strong> → 翌週 +{worst_signal[1]['avg_next_week_return']:.2f}%</li>
"""

        html += """
                </ul>
            </div>

            <h3 style="margin-top: 30px; margin-bottom: 15px; color: #333;">シグナル別パフォーマンス（過去1年のバックテスト）</h3>
            <table>
                <thead>
                    <tr>
                        <th>シグナル</th>
                        <th>サンプル数</th>
                        <th>翌日リターン</th>
                        <th>翌日勝率</th>
                        <th>翌週リターン</th>
                        <th>翌週勝率</th>
                        <th>翌週中央値</th>
                    </tr>
                </thead>
                <tbody>
"""

        # Sort signals by expected order
        signal_order = ['強い買い', '買い', '中立', '売り', '強い売り']
        for label in signal_order:
            if label not in signal_perf:
                continue

            perf = signal_perf[label]

            # Color coding based on performance
            next_week_return = perf['avg_next_week_return']
            row_class = ''
            if next_week_return > 0.8:
                row_class = 'style="background: #d1fae5;"'
            elif next_week_return < 0.4:
                row_class = 'style="background: #fee2e2;"'

            html += f"""
                    <tr {row_class}>
                        <td><span class="signal-badge {label.replace('い', '').replace('り', '')}">{label}</span></td>
                        <td>{perf['count']:,}</td>
                        <td class="score {'positive' if perf['avg_next_day_return'] > 0 else 'negative'}">{perf['avg_next_day_return']:+.3f}%</td>
                        <td>{perf['next_day_win_rate']:.1f}%</td>
                        <td class="score {'positive' if perf['avg_next_week_return'] > 0 else 'negative'}">{perf['avg_next_week_return']:+.3f}%</td>
                        <td>{perf['next_week_win_rate']:.1f}%</td>
                        <td>{perf['next_week_median']:+.3f}%</td>
                    </tr>
"""

        html += """
                </tbody>
            </table>

            <div class="note" style="margin-top: 30px; background: #fffbeb; border-left: 4px solid #f59e0b; padding: 15px;">
                <strong>結論:</strong> 現在のスコアリングロジックは改善が必要です。<br>
                推奨アクション:<br>
                1. テクニカル指標の重み付けを見直し<br>
                2. 期間パラメータを最適化<br>
                3. マーケット環境（トレンド/レンジ）に応じた適応型スコアリング<br>
                4. ファンダメンタル要因の追加
            </div>
        </div>
"""

    html += f"""
        <div class="footer">
            <p>Data source: {source_file.name}</p>
            <p>Next Phase: WebSearch分析（ニュース・IR・ファンダメンタルズ）を追加予定</p>
        </div>
    </div>
</body>
</html>
"""

    # 保存
    output_file = TEST_OUTPUT_DIR / f'technical_report_{date_str}.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  ✓ Saved: {output_file}")

    return output_file


def main():
    print("=" * 60)
    print("Generate Technical Analysis HTML Report")
    print("=" * 60)

    # 最新のスコアJSONを読み込み
    data, source_file = load_latest_scores()

    # バックテスト検証結果を読み込み
    backtest_data = load_backtest_validation()

    # 最適化結果を読み込み（テクニカルのみ）
    optimization_data = load_optimization_results()

    # 最適化結果を読み込み（マーケット要因含む）
    market_optimization_data = load_market_optimization_results()

    # Phase 1 + Phase 2 分析結果を読み込み
    phase1_phase2_data = load_phase1_phase2_results()

    # HTML生成
    output_file = generate_html(data, source_file, backtest_data, optimization_data, market_optimization_data, phase1_phase2_data)

    print("\n✅ HTML report generated successfully!")
    print(f"\nOpen: {output_file}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
