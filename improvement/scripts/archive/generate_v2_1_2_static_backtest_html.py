#!/usr/bin/env python3
"""
v2.1.2静的バックテスト結果のHTML生成（精鋭化版）
対象: 政策銘柄+CORE30（2020-2025）全価格帯
ロジック: strong_buy（25-29点）、strong_sell（-19~-15点）のみ
出力: improvement/v2_1_2_backtest_report.html
"""
from pathlib import Path
import pandas as pd
import numpy as np

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = BASE_DIR / 'improvement' / 'data' / 'v2_1_2_backtest_results.parquet'
OUTPUT_FILE = BASE_DIR / 'improvement' / 'v2_1_2_static_backtest_report.html'

def format_price(price):
    """株価フォーマット: #,###"""
    return f"{price:,.0f}"

def format_percent(value):
    """パーセントフォーマット: #.##%"""
    return f"{value:.2f}%"

def generate_overall_stats(df):
    """全体統計を生成"""
    total_count = len(df)

    # 買いシグナル統計
    buy_df = df[df['action'] == 'strong_buy']
    buy_count = len(buy_df)
    buy_win_count = (buy_df['win'] == True).sum()
    buy_win_rate = (buy_win_count / buy_count * 100) if buy_count > 0 else 0
    buy_avg_profit = buy_df['profit_100'].mean() if buy_count > 0 else 0
    buy_total_profit = buy_df['profit_100'].sum() if buy_count > 0 else 0

    # 売りシグナル統計
    sell_df = df[df['action'] == 'strong_sell']
    sell_count = len(sell_df)
    sell_win_count = (sell_df['win'] == True).sum()
    sell_win_rate = (sell_win_count / sell_count * 100) if sell_count > 0 else 0
    sell_avg_profit = sell_df['profit_100'].mean() if sell_count > 0 else 0
    sell_total_profit = sell_df['profit_100'].sum() if sell_count > 0 else 0

    # 静観
    hold_count = (df['action'] == 'hold').sum()

    # 日平均シグナル数（総取引日数で割る）
    total_trading_days = df['date'].nunique()
    avg_buy_per_day = buy_count / total_trading_days if total_trading_days > 0 else 0
    avg_sell_per_day = sell_count / total_trading_days if total_trading_days > 0 else 0
    avg_total_per_day = (buy_count + sell_count) / total_trading_days if total_trading_days > 0 else 0

    html = f"""
    <div class="summary-section">
        <div class="summary-grid">
            <div class="summary-card">
                <h3>📊 全体統計</h3>
                <div class="stat-row">
                    <span class="stat-label">総判定数</span>
                    <span class="stat-value">{total_count:,}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">strong_buy</span>
                    <span class="stat-value">{buy_count:,}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">strong_sell</span>
                    <span class="stat-value">{sell_count:,}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">hold</span>
                    <span class="stat-value">{hold_count:,}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">総取引日数</span>
                    <span class="stat-value">{total_trading_days:,}日</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">1日平均シグナル数</span>
                    <span class="stat-value">{avg_total_per_day:.1f}件</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>📈 strong_buyシグナル成績</h3>
                <div class="stat-row">
                    <span class="stat-label">判定数</span>
                    <span class="stat-value">{buy_count:,}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">1日平均</span>
                    <span class="stat-value">{avg_buy_per_day:.1f}件</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_win_rate >= 50 else '#e74c3c'};">{buy_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">100株平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{'+'if buy_avg_profit > 0 else ''}{format_price(buy_avg_profit)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if buy_total_profit > 0 else '#e74c3c'};">{'+'if buy_total_profit > 0 else ''}{format_price(buy_total_profit)}</span>
                </div>
            </div>

            <div class="summary-card">
                <h3>📉 strong_sellシグナル成績</h3>
                <div class="stat-row">
                    <span class="stat-label">判定数</span>
                    <span class="stat-value">{sell_count:,}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">1日平均</span>
                    <span class="stat-value">{avg_sell_per_day:.1f}件</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">勝率</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_win_rate >= 50 else '#e74c3c'};">{sell_win_rate:.2f}%</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">100株平均利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_avg_profit > 0 else '#e74c3c'};">{'+'if sell_avg_profit > 0 else ''}{format_price(sell_avg_profit)}</span>
                </div>
                <div class="stat-row">
                    <span class="stat-label">合計利益</span>
                    <span class="stat-value" style="color: {'#27ae60' if sell_total_profit > 0 else '#e74c3c'};">{'+'if sell_total_profit > 0 else ''}{format_price(sell_total_profit)}</span>
                </div>
            </div>
        </div>
    </div>
"""
    return html

def generate_rsi_analysis(df):
    """RSI範囲別の分析"""
    buy_df = df[df['action'] == 'strong_buy'].copy()
    sell_df = df[df['action'] == 'strong_sell'].copy()

    # RSI範囲定義
    rsi_ranges = [
        (0, 30, '< 30 (売られすぎ)'),
        (30, 40, '30-40'),
        (40, 60, '40-60 (中立)'),
        (60, 70, '60-70'),
        (70, 100, '> 70 (買われすぎ)')
    ]

    buy_rows = []
    sell_rows = []

    for min_val, max_val, label in rsi_ranges:
        # 買いシグナル
        buy_range = buy_df[(buy_df['rsi_14d'] >= min_val) & (buy_df['rsi_14d'] < max_val)]
        buy_count = len(buy_range)
        buy_win_rate = (buy_range['win'] == True).sum() / buy_count * 100 if buy_count > 0 else 0
        buy_avg_profit = buy_range['profit_100'].mean() if buy_count > 0 else 0

        buy_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{buy_count:,}</td>
            <td class="number" style="color: {'#27ae60' if buy_win_rate >= 50 else '#e74c3c'};">{buy_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{'+'if buy_avg_profit > 0 else ''}{format_price(buy_avg_profit)}</td>
        </tr>""")

        # 売りシグナル
        sell_range = sell_df[(sell_df['rsi_14d'] >= min_val) & (sell_df['rsi_14d'] < max_val)]
        sell_count = len(sell_range)
        sell_win_rate = (sell_range['win'] == True).sum() / sell_count * 100 if sell_count > 0 else 0
        sell_avg_profit = sell_range['profit_100'].mean() if sell_count > 0 else 0

        sell_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{sell_count:,}</td>
            <td class="number" style="color: {'#27ae60' if sell_win_rate >= 50 else '#e74c3c'};">{sell_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if sell_avg_profit > 0 else '#e74c3c'};">{'+'if sell_avg_profit > 0 else ''}{format_price(sell_avg_profit)}</td>
        </tr>""")

    html = f"""
    <div class="summary-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">📊 RSI範囲別成績</h2>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px;">
            <div>
                <h3 style="margin-bottom: 16px; color: #27ae60;">strong_buyシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>RSI範囲</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(buy_rows)}
                    </tbody>
                </table>
            </div>
            <div>
                <h3 style="margin-bottom: 16px; color: #e74c3c;">strong_sellシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>RSI範囲</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(sell_rows)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
"""
    return html

def generate_volume_analysis(df):
    """出来高変化率別の分析"""
    buy_df = df[df['action'] == 'strong_buy'].copy()
    sell_df = df[df['action'] == 'strong_sell'].copy()

    # 出来高変化率範囲定義
    volume_ranges = [
        (0, 0.5, '< 0.5x (極小)'),
        (0.5, 1.0, '0.5-1.0x (低調)'),
        (1.0, 1.5, '1.0-1.5x (平常)'),
        (1.5, 2.0, '1.5-2.0x (やや活発)'),
        (2.0, 100, '> 2.0x (急増)')
    ]

    buy_rows = []
    sell_rows = []

    for min_val, max_val, label in volume_ranges:
        # 買いシグナル
        buy_range = buy_df[(buy_df['volume_change_20d'] >= min_val) & (buy_df['volume_change_20d'] < max_val)]
        buy_count = len(buy_range)
        buy_win_rate = (buy_range['win'] == True).sum() / buy_count * 100 if buy_count > 0 else 0
        buy_avg_profit = buy_range['profit_100'].mean() if buy_count > 0 else 0

        buy_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{buy_count:,}</td>
            <td class="number" style="color: {'#27ae60' if buy_win_rate >= 50 else '#e74c3c'};">{buy_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{'+'if buy_avg_profit > 0 else ''}{format_price(buy_avg_profit)}</td>
        </tr>""")

        # 売りシグナル
        sell_range = sell_df[(sell_df['volume_change_20d'] >= min_val) & (sell_df['volume_change_20d'] < max_val)]
        sell_count = len(sell_range)
        sell_win_rate = (sell_range['win'] == True).sum() / sell_count * 100 if sell_count > 0 else 0
        sell_avg_profit = sell_range['profit_100'].mean() if sell_count > 0 else 0

        sell_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{sell_count:,}</td>
            <td class="number" style="color: {'#27ae60' if sell_win_rate >= 50 else '#e74c3c'};">{sell_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if sell_avg_profit > 0 else '#e74c3c'};">{'+'if sell_avg_profit > 0 else ''}{format_price(sell_avg_profit)}</td>
        </tr>""")

    html = f"""
    <div class="summary-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">📊 出来高変化率別成績</h2>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px;">
            <div>
                <h3 style="margin-bottom: 16px; color: #27ae60;">strong_buyシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>出来高変化率</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(buy_rows)}
                    </tbody>
                </table>
            </div>
            <div>
                <h3 style="margin-bottom: 16px; color: #e74c3c;">strong_sellシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>出来高変化率</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(sell_rows)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
"""
    return html

def generate_sma5_analysis(df):
    """SMA5乖離率別の分析"""
    buy_df = df[df['action'] == 'strong_buy'].copy()
    sell_df = df[df['action'] == 'strong_sell'].copy()

    # SMA5乖離率範囲定義
    sma5_ranges = [
        (-100, -5, '< -5% (大幅下落)'),
        (-5, -2, '-5% ~ -2% (下落)'),
        (-2, 0, '-2% ~ 0% (押し目)'),
        (0, 2, '0% ~ 2% (微上昇)'),
        (2, 5, '2% ~ 5% (上昇)'),
        (5, 100, '> 5% (大幅上昇)')
    ]

    buy_rows = []
    sell_rows = []

    for min_val, max_val, label in sma5_ranges:
        # 買いシグナル
        buy_range = buy_df[(buy_df['price_vs_sma5_pct'] >= min_val) & (buy_df['price_vs_sma5_pct'] < max_val)]
        buy_count = len(buy_range)
        buy_win_rate = (buy_range['win'] == True).sum() / buy_count * 100 if buy_count > 0 else 0
        buy_avg_profit = buy_range['profit_100'].mean() if buy_count > 0 else 0

        buy_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{buy_count:,}</td>
            <td class="number" style="color: {'#27ae60' if buy_win_rate >= 50 else '#e74c3c'};">{buy_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{'+'if buy_avg_profit > 0 else ''}{format_price(buy_avg_profit)}</td>
        </tr>""")

        # 売りシグナル
        sell_range = sell_df[(sell_df['price_vs_sma5_pct'] >= min_val) & (sell_df['price_vs_sma5_pct'] < max_val)]
        sell_count = len(sell_range)
        sell_win_rate = (sell_range['win'] == True).sum() / sell_count * 100 if sell_count > 0 else 0
        sell_avg_profit = sell_range['profit_100'].mean() if sell_count > 0 else 0

        sell_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{sell_count:,}</td>
            <td class="number" style="color: {'#27ae60' if sell_win_rate >= 50 else '#e74c3c'};">{sell_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if sell_avg_profit > 0 else '#e74c3c'};">{'+'if sell_avg_profit > 0 else ''}{format_price(sell_avg_profit)}</td>
        </tr>""")

    html = f"""
    <div class="summary-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">📊 SMA5乖離率別成績</h2>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px;">
            <div>
                <h3 style="margin-bottom: 16px; color: #27ae60;">strong_buyシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>SMA5乖離率</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(buy_rows)}
                    </tbody>
                </table>
            </div>
            <div>
                <h3 style="margin-bottom: 16px; color: #e74c3c;">strong_sellシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>SMA5乖離率</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(sell_rows)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
"""
    return html

def generate_price_range_analysis(df):
    """株価レンジ別の分析"""
    buy_df = df[df['action'] == 'strong_buy'].copy()
    sell_df = df[df['action'] == 'strong_sell'].copy()

    # 株価レンジ定義
    price_ranges = [
        (0, 500, '< 500円'),
        (500, 1000, '500-1,000円'),
        (1000, 2000, '1,000-2,000円'),
        (2000, 3000, '2,000-3,000円'),
        (3000, 5000, '3,000-5,000円'),
        (5000, 10000, '5,000-10,000円'),
        (10000, 1000000, '10,000円以上')
    ]

    buy_rows = []
    sell_rows = []

    for min_val, max_val, label in price_ranges:
        # 買いシグナル
        buy_range = buy_df[(buy_df['prev_close'] >= min_val) & (buy_df['prev_close'] < max_val)]
        buy_count = len(buy_range)
        buy_win_rate = (buy_range['win'] == True).sum() / buy_count * 100 if buy_count > 0 else 0
        buy_avg_profit = buy_range['profit_100'].mean() if buy_count > 0 else 0

        buy_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{buy_count:,}</td>
            <td class="number" style="color: {'#27ae60' if buy_win_rate >= 50 else '#e74c3c'};">{buy_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{'+'if buy_avg_profit > 0 else ''}{format_price(buy_avg_profit)}</td>
        </tr>""")

        # 売りシグナル
        sell_range = sell_df[(sell_df['prev_close'] >= min_val) & (sell_df['prev_close'] < max_val)]
        sell_count = len(sell_range)
        sell_win_rate = (sell_range['win'] == True).sum() / sell_count * 100 if sell_count > 0 else 0
        sell_avg_profit = sell_range['profit_100'].mean() if sell_count > 0 else 0

        sell_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{sell_count:,}</td>
            <td class="number" style="color: {'#27ae60' if sell_win_rate >= 50 else '#e74c3c'};">{sell_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if sell_avg_profit > 0 else '#e74c3c'};">{'+'if sell_avg_profit > 0 else ''}{format_price(sell_avg_profit)}</td>
        </tr>""")

    html = f"""
    <div class="summary-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">📊 株価レンジ別成績</h2>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px;">
            <div>
                <h3 style="margin-bottom: 16px; color: #27ae60;">strong_buyシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>株価レンジ</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(buy_rows)}
                    </tbody>
                </table>
            </div>
            <div>
                <h3 style="margin-bottom: 16px; color: #e74c3c;">strong_sellシグナル</h3>
                <table>
                    <thead>
                        <tr>
                            <th>株価レンジ</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(sell_rows)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
"""
    return html

def generate_score_range_analysis(df):
    """スコア範囲別の分析（精鋭化版）"""
    buy_df = df[df['action'] == 'strong_buy'].copy()
    sell_df = df[df['action'] == 'strong_sell'].copy()

    # スコア範囲定義（精鋭化版）
    buy_score_ranges = [
        (25, 26, '25点'),
        (26, 27, '26点'),
        (27, 28, '27点'),
        (28, 29, '28点'),
        (29, 30, '29点')
    ]

    sell_score_ranges = [
        (-19, -18, '-19点'),
        (-18, -17, '-18点'),
        (-17, -16, '-17点'),
        (-16, -15, '-16点'),
        (-15, -14, '-15点')
    ]

    buy_rows = []
    sell_rows = []

    # 買いシグナル（25-29点）
    for min_val, max_val, label in buy_score_ranges:
        buy_range = buy_df[(buy_df['score'] >= min_val) & (buy_df['score'] < max_val)]
        buy_count = len(buy_range)
        buy_win_rate = (buy_range['win'] == True).sum() / buy_count * 100 if buy_count > 0 else 0
        buy_avg_profit = buy_range['profit_100'].mean() if buy_count > 0 else 0

        buy_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{buy_count:,}</td>
            <td class="number" style="color: {'#27ae60' if buy_win_rate >= 50 else '#e74c3c'};">{buy_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if buy_avg_profit > 0 else '#e74c3c'};">{'+'if buy_avg_profit > 0 else ''}{format_price(buy_avg_profit)}</td>
        </tr>""")

    # 売りシグナル（-19~-15点）
    for min_val, max_val, label in sell_score_ranges:
        sell_range = sell_df[(sell_df['score'] >= min_val) & (sell_df['score'] < max_val)]
        sell_count = len(sell_range)
        sell_win_rate = (sell_range['win'] == True).sum() / sell_count * 100 if sell_count > 0 else 0
        sell_avg_profit = sell_range['profit_100'].mean() if sell_count > 0 else 0

        sell_rows.append(f"""
        <tr>
            <td>{label}</td>
            <td class="number">{sell_count:,}</td>
            <td class="number" style="color: {'#27ae60' if sell_win_rate >= 50 else '#e74c3c'};">{sell_win_rate:.2f}%</td>
            <td class="number" style="color: {'#27ae60' if sell_avg_profit > 0 else '#e74c3c'};">{'+'if sell_avg_profit > 0 else ''}{format_price(sell_avg_profit)}</td>
        </tr>""")

    html = f"""
    <div class="summary-section">
        <h2 style="margin-bottom: 24px; color: #667eea;">📊 スコア別成績（精鋭化版）</h2>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px;">
            <div>
                <h3 style="margin-bottom: 16px; color: #27ae60;">strong_buy (25-29点)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>スコア</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(buy_rows)}
                    </tbody>
                </table>
            </div>
            <div>
                <h3 style="margin-bottom: 16px; color: #e74c3c;">strong_sell (-19~-15点)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>スコア</th>
                            <th class="number">判定数</th>
                            <th class="number">勝率</th>
                            <th class="number">100株平均利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(sell_rows)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
"""
    return html

def main():
    print("=" * 60)
    print("v2.1.2バックテストHTMLレポート生成（精鋭化版）")
    print("=" * 60)

    # データ読み込み
    print(f"\n[STEP 1] データ読み込み: {DATA_FILE}")
    df = pd.read_parquet(DATA_FILE)
    print(f"  総レコード数: {len(df):,}")

    # HTML生成
    print("\n[STEP 2] HTML生成中...")

    overall_stats_html = generate_overall_stats(df)
    rsi_analysis_html = generate_rsi_analysis(df)
    volume_analysis_html = generate_volume_analysis(df)
    sma5_analysis_html = generate_sma5_analysis(df)
    price_range_html = generate_price_range_analysis(df)
    score_range_html = generate_score_range_analysis(df)

    # 完全なHTML
    html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>v2.1.2 バックテスト分析レポート</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #2c3e50;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 40px 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }}

        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 60px 40px;
            text-align: center;
        }}

        header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 700;
        }}

        header p {{
            font-size: 1.1em;
            opacity: 0.95;
        }}

        main {{
            padding: 40px;
        }}

        .summary-section {{
            margin-bottom: 50px;
        }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }}

        .summary-card {{
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}

        .summary-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 35px rgba(0, 0, 0, 0.15);
        }}

        .summary-card h3 {{
            font-size: 1.3em;
            margin-bottom: 20px;
            color: #667eea;
            font-weight: 600;
        }}

        .stat-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            padding: 8px 0;
            border-bottom: 1px solid rgba(0, 0, 0, 0.05);
        }}

        .stat-row:last-child {{
            border-bottom: none;
        }}

        .stat-label {{
            font-size: 0.95em;
            color: #555;
            font-weight: 500;
        }}

        .stat-value {{
            font-size: 1.4em;
            font-weight: 700;
            color: #2c3e50;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.08);
        }}

        thead {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }}

        th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
            font-size: 0.95em;
            letter-spacing: 0.5px;
        }}

        th.number {{
            text-align: right;
        }}

        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #ecf0f1;
        }}

        td.number {{
            text-align: right;
            font-family: 'Monaco', 'Menlo', monospace;
        }}

        tbody tr:hover {{
            background-color: #f8f9fa;
        }}

        tbody tr:last-child td {{
            border-bottom: none;
        }}

        .positive {{
            color: #27ae60;
        }}

        .negative {{
            color: #e74c3c;
        }}

        h2 {{
            color: #2c3e50;
            font-size: 1.8em;
            margin-bottom: 20px;
            font-weight: 600;
        }}

        h3 {{
            color: #34495e;
            font-size: 1.3em;
            margin-bottom: 15px;
            font-weight: 600;
        }}

        footer {{
            background: #2c3e50;
            color: white;
            text-align: center;
            padding: 30px;
            font-size: 0.9em;
        }}

        .date-separator {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: 600;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 v2.1.2 静的バックテスト分析レポート（精鋭化版）</h1>
            <p>政策銘柄 + TOPIX_CORE30 | 2020-2025 全価格帯 | strong_buy（25-29点）・strong_sell（-19~-15点）のみ</p>
        </header>

        <main>
            {overall_stats_html}
            {rsi_analysis_html}
            {volume_analysis_html}
            {sma5_analysis_html}
            {price_range_html}
            {score_range_html}
        </main>

        <footer>
            <p>Generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </footer>
    </div>
</body>
</html>
"""

    # HTML保存
    print(f"\n[STEP 3] HTML保存: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"\n✅ 完了: {OUTPUT_FILE}")
    print(f"  ファイルサイズ: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
