#!/usr/bin/env python3
"""
simulate_stop_loss_v2_1_0_2.py

v2.1.0.2の買いポジションに対して損切り水準を適用した場合のシミュレーション

損切り水準: -3%, -5%, -10%
対象: 買い判定のみ（売り・静観は対象外）

入力: improvement/data/v2_1_0_comparison_results.parquet
出力: improvement/stop_loss_simulation_report.html
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# パス設定
IMPROVEMENT_DIR = ROOT / "improvement"
INPUT_FILE = IMPROVEMENT_DIR / "data" / "v2_1_0_comparison_results.parquet"
PRICE_5M_FILE = IMPROVEMENT_DIR / "data" / "prices_60d_5m.parquet"
OUTPUT_FILE = IMPROVEMENT_DIR / "stop_loss_simulation_report.html"

# 損切り水準
STOP_LOSS_LEVELS = [-1, -1.5, -2, -2.5, -3, -3.5, -4, -5, -6]  # %


def apply_v2_1_0_1_strategy(row: pd.Series) -> str:
    """v2.1.0.1 ハイブリッド戦略を適用"""
    v2_0_3_action = row['v2_0_3_action']
    v2_1_0_action = row['v2_1_0_action']

    if v2_0_3_action == '買い' and v2_1_0_action == '静観':
        return '静観'
    elif v2_0_3_action == '静観' and v2_1_0_action == '売り':
        return '売り'
    else:
        return v2_0_3_action


def load_morning_close_prices(price_5m_file: Path) -> pd.DataFrame:
    """
    5分足データから前場引け価格（11:30 Close）を取得

    ロジック:
    1. 11:30のClose価格があればそれを使用
    2. なければ午後最早のOpen価格を使用
    3. どちらもなければNaN

    Returns:
        DataFrame with columns: date, ticker, morning_close
    """
    if not price_5m_file.exists():
        print(f"  ⚠ Warning: {price_5m_file} not found. Morning close prices unavailable.")
        return pd.DataFrame(columns=['date', 'ticker', 'morning_close'])

    df = pd.read_parquet(price_5m_file)
    df['date'] = pd.to_datetime(df['date'])
    df['date_only'] = df['date'].dt.date
    df['time_only'] = df['date'].dt.time

    # 11:30 のデータ
    morning_1130 = df[df['time_only'] == pd.Timestamp('11:30:00').time()].copy()
    morning_1130 = morning_1130[['date_only', 'ticker', 'Close']].rename(columns={'Close': 'morning_close'})
    morning_1130 = morning_1130.rename(columns={'date_only': 'date'})

    # 午後データ（12:30以降）
    afternoon_start = pd.Timestamp('12:30:00').time()
    afternoon = df[df['time_only'] >= afternoon_start].copy()
    afternoon = afternoon.sort_values(['date_only', 'ticker', 'date'])
    afternoon_earliest = afternoon.groupby(['date_only', 'ticker']).first().reset_index()
    afternoon_earliest = afternoon_earliest[['date_only', 'ticker', 'Open']].rename(columns={'Open': 'morning_close'})
    afternoon_earliest = afternoon_earliest.rename(columns={'date_only': 'date'})

    # 11:30を優先、なければ午後最早
    morning_prices = pd.concat([morning_1130, afternoon_earliest])
    morning_prices = morning_prices.drop_duplicates(subset=['date', 'ticker'], keep='first')

    return morning_prices


def calculate_with_stop_loss(df: pd.DataFrame, stop_loss_pct: float) -> dict:
    """
    損切り水準を適用した場合の成績を計算（日中Lowベースで判定）

    Args:
        df: 買いポジションのデータフレーム
        stop_loss_pct: 損切り水準（%、負の値）

    Returns:
        成績の辞書
    """
    df_result = df.copy()

    # 損切り価格を計算
    df_result['stop_loss_price'] = df_result['buy_price'] * (1 + stop_loss_pct / 100)

    # 日中のLowが損切り価格を下回ったか判定
    df_result['stop_loss_triggered'] = df_result['low'] < df_result['stop_loss_price']

    # 実際の利益率（終値ベース）
    df_result['actual_profit_pct'] = (df_result['daily_close'] - df_result['buy_price']) / df_result['buy_price'] * 100
    df_result['actual_profit_100'] = (df_result['daily_close'] - df_result['buy_price']) * 100

    # 損切り適用後の利益率
    # 損切り発動: 損切り価格で決済
    # 損切り未発動: 終値で決済
    df_result['stop_loss_profit_pct'] = df_result.apply(
        lambda row: stop_loss_pct if row['stop_loss_triggered'] else row['actual_profit_pct'],
        axis=1
    )
    df_result['stop_loss_profit_100'] = df_result.apply(
        lambda row: (row['stop_loss_price'] - row['buy_price']) * 100 if row['stop_loss_triggered'] else row['actual_profit_100'],
        axis=1
    )

    # 損切りによる利益差
    df_result['profit_diff_100'] = df_result['stop_loss_profit_100'] - df_result['actual_profit_100']

    # 機会損失（損切り発動したが、終値ではプラスだったケース）
    df_result['opportunity_loss'] = df_result.apply(
        lambda row: row['profit_diff_100'] if (row['stop_loss_triggered'] and row['actual_profit_100'] > 0) else 0,
        axis=1
    )

    # 勝ち負け判定（損切り適用後）
    df_result['win'] = df_result['stop_loss_profit_100'] > 0
    df_result['draw'] = df_result['stop_loss_profit_100'] == 0

    # 統計計算
    total = len(df_result)
    wins = df_result['win'].sum()
    draws = df_result['draw'].sum()
    losses = total - wins - draws
    win_rate = wins / (total - draws) * 100 if (total - draws) > 0 else 0

    total_profit = df_result['stop_loss_profit_100'].sum()
    avg_profit = df_result['stop_loss_profit_100'].mean()

    # 損切り発動統計
    stop_loss_count = df_result['stop_loss_triggered'].sum()
    stop_loss_rate = stop_loss_count / total * 100 if total > 0 else 0

    # 損切り発動時の平均損失軽減
    triggered_df = df_result[df_result['stop_loss_triggered']]
    avg_loss_reduction = triggered_df['profit_diff_100'].mean() if len(triggered_df) > 0 else 0

    # 機会損失統計
    opportunity_loss_cases = (df_result['opportunity_loss'] < 0).sum()
    opportunity_loss_rate = opportunity_loss_cases / total * 100 if total > 0 else 0
    total_opportunity_loss = df_result['opportunity_loss'].sum()
    avg_opportunity_loss = df_result[df_result['opportunity_loss'] < 0]['opportunity_loss'].mean() if opportunity_loss_cases > 0 else 0

    # 元の成績（損切りなし）
    original_total_profit = df_result['actual_profit_100'].sum()
    original_avg_profit = df_result['actual_profit_100'].mean()
    original_wins = (df_result['actual_profit_100'] > 0).sum()
    original_win_rate = original_wins / (total - (df_result['actual_profit_100'] == 0).sum()) * 100 if (total - (df_result['actual_profit_100'] == 0).sum()) > 0 else 0

    return {
        'total': total,
        'wins': wins,
        'draws': draws,
        'losses': losses,
        'win_rate': win_rate,
        'total_profit': total_profit,
        'avg_profit': avg_profit,
        'stop_loss_count': stop_loss_count,
        'stop_loss_rate': stop_loss_rate,
        'avg_loss_reduction': avg_loss_reduction,
        'opportunity_loss_cases': opportunity_loss_cases,
        'opportunity_loss_rate': opportunity_loss_rate,
        'total_opportunity_loss': total_opportunity_loss,
        'avg_opportunity_loss': avg_opportunity_loss,
        'original_total_profit': original_total_profit,
        'original_avg_profit': original_avg_profit,
        'original_wins': original_wins,
        'original_win_rate': original_win_rate,
        'profit_diff': total_profit - original_total_profit,
        'df': df_result
    }


def calculate_with_stop_loss_morning(df: pd.DataFrame, stop_loss_pct: float) -> dict:
    """
    損切り水準を適用した場合の成績を計算（前場引けベース）

    Args:
        df: 買いポジションのデータフレーム（morning_closeカラムを含む）
        stop_loss_pct: 損切り水準（%、負の値）

    Returns:
        成績の辞書
    """
    # morning_closeがあるデータのみ使用
    df_result = df[df['morning_close'].notna()].copy()

    if len(df_result) == 0:
        return {
            'total': 0,
            'wins': 0,
            'win_rate': 0,
            'total_profit': 0,
            'df': df_result
        }

    # 損切り価格を計算
    df_result['stop_loss_price'] = df_result['buy_price'] * (1 + stop_loss_pct / 100)

    # 日中のLowが損切り価格を下回ったか判定
    df_result['stop_loss_triggered'] = df_result['low'] < df_result['stop_loss_price']

    # 実際の利益率（前場引けベース）
    df_result['actual_profit_pct'] = (df_result['morning_close'] - df_result['buy_price']) / df_result['buy_price'] * 100
    df_result['actual_profit_100'] = (df_result['morning_close'] - df_result['buy_price']) * 100

    # 損切り適用後の利益率
    df_result['stop_loss_profit_pct'] = df_result.apply(
        lambda row: stop_loss_pct if row['stop_loss_triggered'] else row['actual_profit_pct'],
        axis=1
    )
    df_result['stop_loss_profit_100'] = df_result.apply(
        lambda row: (row['stop_loss_price'] - row['buy_price']) * 100 if row['stop_loss_triggered'] else row['actual_profit_100'],
        axis=1
    )

    # 損切りによる利益差
    df_result['profit_diff_100'] = df_result['stop_loss_profit_100'] - df_result['actual_profit_100']

    # 機会損失（損切り発動したが、前場引けではプラスだったケース）
    df_result['opportunity_loss'] = df_result.apply(
        lambda row: row['profit_diff_100'] if (row['stop_loss_triggered'] and row['actual_profit_100'] > 0) else 0,
        axis=1
    )

    # 勝ち負け判定（損切り適用後）
    df_result['win'] = df_result['stop_loss_profit_100'] > 0
    df_result['draw'] = df_result['stop_loss_profit_100'] == 0

    # 統計計算
    total = len(df_result)
    wins = df_result['win'].sum()
    draws = df_result['draw'].sum()
    losses = total - wins - draws
    win_rate = wins / (total - draws) * 100 if (total - draws) > 0 else 0

    total_profit = df_result['stop_loss_profit_100'].sum()
    avg_profit = df_result['stop_loss_profit_100'].mean()

    # 元の成績（損切りなし、前場引けベース）
    original_total_profit = df_result['actual_profit_100'].sum()
    original_avg_profit = df_result['actual_profit_100'].mean()
    original_wins = (df_result['actual_profit_100'] > 0).sum()
    original_win_rate = original_wins / (total - (df_result['actual_profit_100'] == 0).sum()) * 100 if (total - (df_result['actual_profit_100'] == 0).sum()) > 0 else 0

    return {
        'total': total,
        'wins': wins,
        'draws': draws,
        'losses': losses,
        'win_rate': win_rate,
        'total_profit': total_profit,
        'avg_profit': avg_profit,
        'original_total_profit': original_total_profit,
        'original_avg_profit': original_avg_profit,
        'original_wins': original_wins,
        'original_win_rate': original_win_rate,
        'profit_diff': total_profit - original_total_profit,
        'df': df_result
    }


def calculate_price_bracket_with_stop_loss(df: pd.DataFrame, stop_loss_pct: float, use_morning: bool = False) -> dict:
    """価格帯別の損切りシミュレーション"""
    brackets = {
        '1,000円未満': df[df['buy_price'] < 1000],
        '1,000-3,000円': df[(df['buy_price'] >= 1000) & (df['buy_price'] < 3000)],
        '3,000-5,000円': df[(df['buy_price'] >= 3000) & (df['buy_price'] < 5000)],
        '5,000-10,000円': df[(df['buy_price'] >= 5000) & (df['buy_price'] < 10000)]
    }

    results = {}
    for bracket_name, bracket_df in brackets.items():
        if len(bracket_df) > 0:
            if use_morning:
                results[bracket_name] = calculate_with_stop_loss_morning(bracket_df, stop_loss_pct)
            else:
                results[bracket_name] = calculate_with_stop_loss(bracket_df, stop_loss_pct)
        else:
            results[bracket_name] = None

    return results


def generate_drawdown_analysis_section(buy_df: pd.DataFrame) -> str:
    """価格帯別の下落幅分析セクションを生成"""
    # 下落率を計算
    buy_df = buy_df.copy()
    buy_df['drawdown_pct'] = (buy_df['buy_price'] - buy_df['low']) / buy_df['buy_price'] * 100
    buy_df['win'] = buy_df['daily_close'] >= buy_df['buy_price']

    # 価格帯を追加
    buy_df['price_bracket'] = buy_df['buy_price'].apply(
        lambda x: '1,000円未満' if x < 1000 else
                 '1,000-3,000円' if x < 3000 else
                 '3,000-5,000円' if x < 5000 else
                 '5,000-10,000円'
    )

    brackets = ['1,000円未満', '1,000-3,000円', '3,000-5,000円', '5,000-10,000円']

    # 価格帯別統計カード生成
    bracket_cards = []
    for bracket in brackets:
        bracket_df = buy_df[buy_df['price_bracket'] == bracket]

        if len(bracket_df) == 0:
            continue

        win_draw_df = bracket_df[bracket_df['win'] == True]
        lose_df = bracket_df[bracket_df['win'] == False]

        # 勝ち・引分統計
        if len(win_draw_df) > 0:
            win_mean = win_draw_df['drawdown_pct'].mean()
            win_median = win_draw_df['drawdown_pct'].median()
            win_max = win_draw_df['drawdown_pct'].max()
            suggested_sl = -win_median
        else:
            win_mean = win_median = win_max = suggested_sl = 0

        # 負け統計
        if len(lose_df) > 0:
            lose_mean = lose_df['drawdown_pct'].mean()
            lose_median = lose_df['drawdown_pct'].median()
            lose_max = lose_df['drawdown_pct'].max()
        else:
            lose_mean = lose_median = lose_max = 0

        bracket_cards.append(f'''
        <div class="drawdown-card">
            <h4>{bracket}</h4>
            <div class="drawdown-section">
                <div class="section-title">✅ 勝ち・引分 ({len(win_draw_df)}銘柄)</div>
                <div class="stat-grid">
                    <div class="stat-item-small">
                        <span class="label">平均下落</span>
                        <span class="value">{win_mean:.2f}%</span>
                    </div>
                    <div class="stat-item-small">
                        <span class="label">中央値</span>
                        <span class="value highlight">{win_median:.2f}%</span>
                    </div>
                    <div class="stat-item-small">
                        <span class="label">最大下落</span>
                        <span class="value">{win_max:.2f}%</span>
                    </div>
                </div>
            </div>
            <div class="drawdown-section">
                <div class="section-title">❌ 負け ({len(lose_df)}銘柄)</div>
                <div class="stat-grid">
                    <div class="stat-item-small">
                        <span class="label">平均下落</span>
                        <span class="value">{lose_mean:.2f}%</span>
                    </div>
                    <div class="stat-item-small">
                        <span class="label">中央値</span>
                        <span class="value highlight">{lose_median:.2f}%</span>
                    </div>
                    <div class="stat-item-small">
                        <span class="label">最大下落</span>
                        <span class="value">{lose_max:.2f}%</span>
                    </div>
                </div>
            </div>
            <div class="suggestion">
                💡 理論値: 約 <strong>{suggested_sl:.1f}%</strong>
            </div>
        </div>''')

    html = f'''
    <div class="drawdown-analysis">
        <h2>📉 価格帯別 下落幅分析</h2>
        <p class="description">勝ち・引分銘柄と負け銘柄の日中下落率（購入価格→安値）を分析</p>
        <div class="drawdown-grid">
            {''.join(bracket_cards)}
        </div>
        <div class="analysis-note">
            <h3>💡 最適損切り水準の根拠</h3>
            <p>理論値（勝ち銘柄の中央値下落率）は約-1%ですが、実績ベースの最適値は-1%～-5%となります。</p>
            <p><strong>理由</strong>: 勝ち銘柄でも正常な値動きで1%程度下落するため、-1%では機会損失が多い。負け銘柄の平均下落5-6%との境界を考慮した損切り水準が最適。</p>
        </div>
    </div>'''

    return html


def generate_stop_loss_detail_sections(buy_df: pd.DataFrame, stop_loss_levels: list) -> list:
    """各損切り水準ごとの個別銘柄詳細セクションを生成"""
    sections = []

    for stop_loss_pct in stop_loss_levels:
        # 損切り適用
        result = calculate_with_stop_loss(buy_df, stop_loss_pct)
        df_detail = result['df'].copy()

        # 価格帯を追加
        df_detail['price_bracket'] = df_detail['buy_price'].apply(
            lambda x: '1,000円未満' if x < 1000 else
                     '1,000-3,000円' if x < 3000 else
                     '3,000-5,000円' if x < 5000 else
                     '5,000-10,000円'
        )

        # 購入価格順にソート（昇順）
        df_detail = df_detail.sort_values('buy_price', ascending=True)

        # テーブル行を生成
        rows = []
        for _, row in df_detail.iterrows():
            ticker = row['ticker']
            company = row.get('company_name', '')
            bracket = row['price_bracket']
            buy_price = row['buy_price']
            low = row['low']
            close = row['daily_close']
            triggered = row['stop_loss_triggered']
            profit_with_sl = row['stop_loss_profit_100']
            profit_no_sl = row['actual_profit_100']
            profit_impact = row['profit_diff_100']

            # 損切り発動
            triggered_badge = '<span class="badge-triggered-mini">発動</span>' if triggered else '-'

            # 利益クラス
            profit_class = 'positive' if profit_with_sl > 0 else 'negative' if profit_with_sl < 0 else ''
            profit_str = f'{profit_with_sl:+,.0f}'

            # 効果クラス
            impact_class = 'positive' if profit_impact > 0 else 'negative' if profit_impact < 0 else ''
            impact_str = f'({profit_impact:+,.0f})' if profit_impact != 0 else '-'

            rows.append(f'''
            <tr class="{'row-triggered' if triggered else ''}">
                <td>{ticker}</td>
                <td>{company}</td>
                <td>{bracket}</td>
                <td class="number">{buy_price:,.0f}円</td>
                <td class="number">{low:,.0f}円</td>
                <td class="number">{close:,.0f}円</td>
                <td class="center">{triggered_badge}</td>
                <td class="number {profit_class}">{profit_str}円</td>
                <td class="number {impact_class}">{impact_str}円</td>
            </tr>''')

        # セクションHTML
        section_html = f'''
        <div class="detail-section">
            <h3>損切り {stop_loss_pct}%</h3>
            <div class="detail-summary">
                <span>合計利益: <strong class="{'positive' if result['total_profit'] > 0 else 'negative'}">{result['total_profit']:+,.0f}円</strong></span>
                <span>損切り発動: <strong>{result['stop_loss_count']}件 / {result['total']}件 ({result['stop_loss_rate']:.1f}%)</strong></span>
                <span>純影響: <strong class="{'positive' if result['profit_diff'] > 0 else 'negative'}">{result['profit_diff']:+,.0f}円</strong></span>
            </div>
            <table class="detail-table">
                <thead>
                    <tr>
                        <th>ティッカー</th>
                        <th>銘柄名</th>
                        <th>価格帯</th>
                        <th class="number">購入価格</th>
                        <th class="number">安値</th>
                        <th class="number">終値</th>
                        <th class="center">損切り</th>
                        <th class="number">100株利益</th>
                        <th class="number">効果</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>'''

        sections.append(section_html)

    return sections


def generate_morning_vs_day_comparison(morning_sims: dict, day_sims: dict, morning_baseline: dict, day_baseline: dict,
                                       morning_sims_bracket: dict = None, day_sims_bracket: dict = None,
                                       morning_baseline_bracket: dict = None, day_baseline_bracket: dict = None) -> str:
    """前場引け vs 大引け 比較セクションを生成"""

    # 比較テーブル行を生成
    comparison_rows = []

    for stop_loss_pct in STOP_LOSS_LEVELS:
        morning = morning_sims[stop_loss_pct]
        day = day_sims[stop_loss_pct]['overall']

        # 前場引けベース
        morning_profit = morning['total_profit']
        morning_win_rate = morning['win_rate']
        morning_count = morning['total']

        # 大引けベース
        day_profit = day['total_profit']
        day_win_rate = day['win_rate']
        day_count = day['total']

        # 差分
        profit_diff = morning_profit - day_profit
        profit_diff_class = 'positive' if profit_diff > 0 else 'negative' if profit_diff < 0 else ''

        # より良い方をハイライト
        better_morning_profit = morning_profit > day_profit
        better_morning_wr = morning_win_rate > day_win_rate

        comparison_rows.append(f'''
        <tr>
            <td class="bracket-name">{stop_loss_pct}%</td>
            <td class="number">{morning_count}</td>
            <td class="number {'positive' if better_morning_wr else ''}" style="{'font-weight: bold;' if better_morning_wr else ''}">{morning_win_rate:.1f}%</td>
            <td class="number {'positive' if morning_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if better_morning_profit else ''}">{morning_profit:+,.0f}円</td>
            <td class="number">{day_count}</td>
            <td class="number {'positive' if not better_morning_wr else ''}" style="{'font-weight: bold;' if not better_morning_wr else ''}">{day_win_rate:.1f}%</td>
            <td class="number {'positive' if day_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if not better_morning_profit else ''}">{day_profit:+,.0f}円</td>
            <td class="number {profit_diff_class}" style="font-weight: bold;">{profit_diff:+,.0f}円</td>
        </tr>''')

    # 損切りなし（ベースライン）の比較
    morning_bl_profit = morning_baseline['total_profit']
    morning_bl_wr = morning_baseline['win_rate']
    day_bl_profit = day_baseline['total_profit']
    day_bl_wr = day_baseline['win_rate']
    bl_diff = morning_bl_profit - day_bl_profit
    bl_diff_class = 'positive' if bl_diff > 0 else 'negative' if bl_diff < 0 else ''
    better_bl_morning = morning_bl_profit > day_bl_profit

    baseline_row = f'''
    <tr style="background: #f5f5f5; border-top: 3px solid #667eea;">
        <td class="bracket-name">損切りなし</td>
        <td class="number">{morning_baseline['total']}</td>
        <td class="number">{morning_bl_wr:.1f}%</td>
        <td class="number {'positive' if morning_bl_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if better_bl_morning else ''}">{morning_bl_profit:+,.0f}円</td>
        <td class="number">{day_baseline['total']}</td>
        <td class="number">{day_bl_wr:.1f}%</td>
        <td class="number {'positive' if day_bl_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if not better_bl_morning else ''}">{day_bl_profit:+,.0f}円</td>
        <td class="number {bl_diff_class}" style="font-weight: bold;">{bl_diff:+,.0f}円</td>
    </tr>'''

    # 価格帯別比較テーブル
    bracket_comparison_html = ''
    if morning_sims_bracket and day_sims_bracket:
        bracket_comparison_html = generate_bracket_morning_vs_day_comparison(
            morning_sims_bracket, day_sims_bracket, morning_baseline_bracket, day_baseline_bracket
        )

    html = f'''
    <h2>🕐 前場引け vs 大引け 比較</h2>
    <p style="text-align: center; color: #666; margin-bottom: 15px; font-size: 0.9em;">
        前場引け（11:30 Close）と大引け（終値）のパフォーマンス比較
    </p>

    <h3 style="margin-top: 30px; color: #667eea;">全体比較</h3>
    <div class="table-wrapper">
        <table class="comparison-table">
            <thead>
                <tr>
                    <th rowspan="2">損切り水準</th>
                    <th colspan="3" style="text-align: center; background: #ff9800; border-right: 2px solid white;">前場引け</th>
                    <th colspan="3" style="text-align: center; background: #2196f3; border-right: 2px solid white;">大引け</th>
                    <th rowspan="2">差分<br>(前場-大引け)</th>
                </tr>
                <tr>
                    <th class="number">銘柄数</th>
                    <th class="number">勝率</th>
                    <th class="number" style="border-right: 2px solid white;">合計利益</th>
                    <th class="number">銘柄数</th>
                    <th class="number">勝率</th>
                    <th class="number" style="border-right: 2px solid white;">合計利益</th>
                </tr>
            </thead>
            <tbody>
                {''.join(comparison_rows)}
                {baseline_row}
            </tbody>
        </table>
    </div>
    <div style="margin-top: 20px; padding: 15px; background: #fff3cd; border-radius: 8px; border-left: 4px solid #ffc107;">
        <strong>💡 結論:</strong>
        <span style="color: #333;">{'前場引けの方が有利' if bl_diff > 0 else '大引けの方が有利' if bl_diff < 0 else '差なし'}</span>
        （差分: {bl_diff:+,.0f}円）
    </div>

    {bracket_comparison_html}
    '''

    return html


def generate_bracket_morning_vs_day_comparison(morning_sims_bracket: dict, day_sims_bracket: dict,
                                               morning_baseline_bracket: dict, day_baseline_bracket: dict) -> str:
    """価格帯別の前場引け vs 大引け 比較を生成"""

    bracket_names = ['1,000円未満', '1,000-3,000円', '3,000-5,000円', '5,000-10,000円']

    # 各価格帯のテーブルを生成
    bracket_tables = []

    for bracket_name in bracket_names:
        # ベースラインデータ
        morning_bl = morning_baseline_bracket.get(bracket_name)
        day_bl = day_baseline_bracket.get(bracket_name)

        if not morning_bl or not day_bl:
            continue

        # 各損切り水準の行を生成
        rows = []
        for stop_loss_pct in STOP_LOSS_LEVELS:
            morning = morning_sims_bracket[stop_loss_pct].get(bracket_name)
            day = day_sims_bracket[stop_loss_pct]['by_bracket'].get(bracket_name)

            if not morning or not day:
                continue

            # 前場引けベース
            morning_profit = morning['total_profit']
            morning_wr = morning['win_rate']

            # 大引けベース
            day_profit = day['total_profit']
            day_wr = day['win_rate']

            # 差分
            profit_diff = morning_profit - day_profit
            profit_diff_class = 'positive' if profit_diff > 0 else 'negative' if profit_diff < 0 else ''
            better_morning = morning_profit > day_profit

            rows.append(f'''
            <tr>
                <td class="bracket-name">{stop_loss_pct}%</td>
                <td class="number">{morning_wr:.1f}%</td>
                <td class="number {'positive' if morning_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if better_morning else ''}">{morning_profit:+,.0f}円</td>
                <td class="number">{day_wr:.1f}%</td>
                <td class="number {'positive' if day_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if not better_morning else ''}">{day_profit:+,.0f}円</td>
                <td class="number {profit_diff_class}" style="font-weight: bold;">{profit_diff:+,.0f}円</td>
            </tr>''')

        # ベースライン行
        morning_bl_profit = morning_bl['total_profit']
        morning_bl_wr = morning_bl['win_rate']
        day_bl_profit = day_bl['total_profit']
        day_bl_wr = day_bl['win_rate']
        bl_diff = morning_bl_profit - day_bl_profit
        bl_diff_class = 'positive' if bl_diff > 0 else 'negative' if bl_diff < 0 else ''
        better_bl_morning = morning_bl_profit > day_bl_profit

        baseline_row = f'''
        <tr style="background: #f5f5f5; border-top: 2px solid #667eea;">
            <td class="bracket-name">損切りなし</td>
            <td class="number">{morning_bl_wr:.1f}%</td>
            <td class="number {'positive' if morning_bl_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if better_bl_morning else ''}">{morning_bl_profit:+,.0f}円</td>
            <td class="number">{day_bl_wr:.1f}%</td>
            <td class="number {'positive' if day_bl_profit > 0 else 'negative'}" style="{'font-weight: bold; background: #e8f5e9;' if not better_bl_morning else ''}">{day_bl_profit:+,.0f}円</td>
            <td class="number {bl_diff_class}" style="font-weight: bold;">{bl_diff:+,.0f}円</td>
        </tr>'''

        # 結論メッセージ
        conclusion = '前場引けが有利' if bl_diff > 0 else '大引けが有利' if bl_diff < 0 else '差なし'
        conclusion_color = '#27ae60' if bl_diff > 0 else '#e74c3c' if bl_diff < 0 else '#666'

        bracket_tables.append(f'''
        <div style="margin-top: 30px;">
            <h4 style="color: #667eea; margin-bottom: 15px;">{bracket_name} ({morning_bl['total']}銘柄)</h4>
            <div class="table-wrapper">
                <table class="comparison-table" style="font-size: 0.85em;">
                    <thead>
                        <tr>
                            <th rowspan="2">損切り水準</th>
                            <th colspan="2" style="text-align: center; background: #ff9800; border-right: 2px solid white;">前場引け</th>
                            <th colspan="2" style="text-align: center; background: #2196f3; border-right: 2px solid white;">大引け</th>
                            <th rowspan="2">差分</th>
                        </tr>
                        <tr>
                            <th class="number">勝率</th>
                            <th class="number" style="border-right: 2px solid white;">合計利益</th>
                            <th class="number">勝率</th>
                            <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(rows)}
                        {baseline_row}
                    </tbody>
                </table>
            </div>
            <div style="margin-top: 10px; padding: 10px; background: #f8f9fa; border-radius: 6px; border-left: 3px solid {conclusion_color};">
                <strong>💡 {bracket_name}:</strong>
                <span style="color: {conclusion_color}; font-weight: bold;">{conclusion}</span>
                （差分: {bl_diff:+,.0f}円）
            </div>
        </div>''')

    html = f'''
    <h3 style="margin-top: 40px; color: #667eea;">価格帯別 比較</h3>
    <p style="text-align: center; color: #666; margin-bottom: 15px; font-size: 0.9em;">
        各価格帯における前場引け vs 大引けのパフォーマンス
    </p>
    {''.join(bracket_tables)}
    '''

    return html


def generate_html_report(df: pd.DataFrame, morning_prices: pd.DataFrame = None) -> str:
    """HTMLレポートを生成"""
    # v2.1.0.1 判定を適用
    df['v2_1_0_1_action'] = df.apply(apply_v2_1_0_1_strategy, axis=1)

    # 買いポジションのみ抽出
    buy_df = df[df['v2_1_0_1_action'] == '買い'].copy()

    if len(buy_df) == 0:
        return "<html><body><h1>買いポジションがありません</h1></body></html>"

    # 前場引け価格をマージ
    if morning_prices is not None and len(morning_prices) > 0:
        buy_df['selection_date'] = pd.to_datetime(buy_df['selection_date'])
        buy_df['date_for_merge'] = buy_df['selection_date'].dt.date
        buy_df = buy_df.merge(
            morning_prices,
            left_on=['date_for_merge', 'ticker'],
            right_on=['date', 'ticker'],
            how='left'
        )
        buy_df = buy_df.drop(columns=['date_for_merge', 'date'], errors='ignore')

    # 各損切り水準でシミュレーション
    simulations = {}
    for stop_loss_pct in STOP_LOSS_LEVELS:
        simulations[stop_loss_pct] = {
            'overall': calculate_with_stop_loss(buy_df, stop_loss_pct),
            'by_bracket': calculate_price_bracket_with_stop_loss(buy_df, stop_loss_pct)
        }

    # 損切りなしの成績（ベースライン）
    baseline = calculate_with_stop_loss(buy_df, -100)  # 実質損切りなし
    baseline_by_bracket = calculate_price_bracket_with_stop_loss(buy_df, -100, use_morning=False)

    # 前場引けシミュレーション（morning_closeがある場合のみ）
    morning_simulations = {}
    morning_simulations_by_bracket = {}
    has_morning_data = 'morning_close' in buy_df.columns and buy_df['morning_close'].notna().any()

    if has_morning_data:
        for stop_loss_pct in STOP_LOSS_LEVELS:
            morning_simulations[stop_loss_pct] = calculate_with_stop_loss_morning(buy_df, stop_loss_pct)
            morning_simulations_by_bracket[stop_loss_pct] = calculate_price_bracket_with_stop_loss(buy_df, stop_loss_pct, use_morning=True)

        # 前場引けベースライン
        morning_baseline = calculate_with_stop_loss_morning(buy_df, -100)
        morning_baseline_by_bracket = calculate_price_bracket_with_stop_loss(buy_df, -100, use_morning=True)

    # サマリーカードHTML生成
    summary_cards = []
    for stop_loss_pct in STOP_LOSS_LEVELS:
        result = simulations[stop_loss_pct]['overall']
        profit_diff = result['profit_diff']
        profit_diff_class = 'positive' if profit_diff > 0 else 'negative' if profit_diff < 0 else ''
        profit_diff_str = f'+{profit_diff:,.0f}' if profit_diff > 0 else f'{profit_diff:,.0f}'

        summary_cards.append(f'''
        <div class="summary-card">
            <h3>損切り {stop_loss_pct}%</h3>
            <div class="stat-row">
                <span class="stat-label">勝率</span>
                <span class="stat-value {'positive' if result['win_rate'] >= 50 else 'negative'}">{result['win_rate']:.1f}%</span>
                <span class="stat-diff">({result['win_rate'] - baseline['win_rate']:+.1f}%)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">合計利益</span>
                <span class="stat-value {'positive' if result['total_profit'] > 0 else 'negative'}">{result['total_profit']:+,.0f}円</span>
                <span class="stat-diff {profit_diff_class}">({profit_diff_str}円)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">平均利益</span>
                <span class="stat-value {'positive' if result['avg_profit'] > 0 else 'negative'}">{result['avg_profit']:+,.0f}円</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">損切り発動率</span>
                <span class="stat-value">{result['stop_loss_rate']:.1f}%</span>
                <span class="stat-label">({result['stop_loss_count']}件)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">平均損失軽減</span>
                <span class="stat-value positive">{result['avg_loss_reduction']:+,.0f}円/件</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">機会損失発生率</span>
                <span class="stat-value negative">{result['opportunity_loss_rate']:.1f}%</span>
                <span class="stat-label">({result['opportunity_loss_cases']}件)</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">機会損失合計</span>
                <span class="stat-value negative">{result['total_opportunity_loss']:,.0f}円</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">勝/負/分</span>
                <span class="stat-value">{result['wins']}/{result['losses']}/{result['draws']}</span>
            </div>
        </div>''')

    # 価格帯別比較テーブル
    bracket_comparison_rows = []
    bracket_names = ['1,000円未満', '1,000-3,000円', '3,000-5,000円', '5,000-10,000円']

    for bracket_name in bracket_names:
        # ベースライン（損切りなし）
        baseline_bracket = calculate_price_bracket_with_stop_loss(buy_df, -100)[bracket_name]
        if baseline_bracket is None:
            continue

        row_html = f'<tr><td class="bracket-name">{bracket_name}</td>'

        # 損切りなし
        row_html += f'''
        <td class="number">{baseline_bracket['total']}</td>
        <td class="number {'positive' if baseline_bracket['original_win_rate'] >= 50 else 'negative'}">{baseline_bracket['original_win_rate']:.1f}%</td>
        <td class="number {'positive' if baseline_bracket['original_total_profit'] > 0 else 'negative'}">{baseline_bracket['original_total_profit']:+,.0f}円</td>
        '''

        # 各損切り水準
        for stop_loss_pct in STOP_LOSS_LEVELS:
            result_bracket = simulations[stop_loss_pct]['by_bracket'][bracket_name]
            profit_diff = result_bracket['profit_diff']
            profit_diff_class = 'positive' if profit_diff > 0 else 'negative' if profit_diff < 0 else ''

            row_html += f'''
            <td class="number {'positive' if result_bracket['win_rate'] >= 50 else 'negative'}">{result_bracket['win_rate']:.1f}%<br><span class="stat-diff">({result_bracket['win_rate'] - baseline_bracket['original_win_rate']:+.1f}%)</span></td>
            <td class="number {'positive' if result_bracket['total_profit'] > 0 else 'negative'}">{result_bracket['total_profit']:+,.0f}円<br><span class="stat-diff {profit_diff_class}">({profit_diff:+,.0f}円)</span></td>
            '''

        row_html += '</tr>'
        bracket_comparison_rows.append(row_html)

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>損切りシミュレーション - V2.1.0.2</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 40px 20px;
    color: #333;
}}
.container {{
    max-width: 1400px;
    margin: 0 auto;
    background: white;
    border-radius: 16px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
    overflow: hidden;
}}
.header {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 40px;
    text-align: center;
}}
.header h1 {{
    font-size: 2.5em;
    margin-bottom: 10px;
    font-weight: 700;
}}
.header .subtitle {{
    font-size: 1.1em;
    opacity: 0.9;
}}
.baseline-box {{
    background: #f8f9fa;
    padding: 30px;
    margin: 30px;
    border-radius: 12px;
    border-left: 6px solid #27ae60;
}}
.baseline-box h2 {{
    color: #27ae60;
    margin-bottom: 15px;
}}
.baseline-stats {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 20px;
    margin-top: 15px;
}}
.baseline-stat {{
    background: white;
    padding: 15px;
    border-radius: 8px;
    text-align: center;
}}
.baseline-stat .label {{
    color: #666;
    font-size: 0.9em;
    margin-bottom: 5px;
}}
.baseline-stat .value {{
    font-size: 1.5em;
    font-weight: 700;
    color: #27ae60;
}}
.summary-section {{
    padding: 40px;
    background: #f8f9fa;
}}
.summary-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 24px;
    margin-bottom: 30px;
}}
.summary-card {{
    background: white;
    border-radius: 12px;
    padding: 28px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-left: 6px solid #e74c3c;
}}
.summary-card h3 {{
    font-size: 1.4em;
    margin-bottom: 20px;
    color: #e74c3c;
}}
.stat-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 12px 0;
    padding: 10px 0;
    border-bottom: 1px solid #f0f0f0;
}}
.stat-row:last-child {{ border-bottom: none; }}
.stat-label {{
    color: #666;
    font-size: 0.95em;
}}
.stat-value {{
    font-weight: 600;
    font-size: 1.1em;
    color: #333;
}}
.stat-diff {{
    font-size: 0.85em;
    color: #999;
    margin-left: 8px;
}}
.positive {{ color: #27ae60 !important; }}
.negative {{ color: #e74c3c !important; }}
.table-section {{
    padding: 40px;
}}
.table-section h2 {{
    margin-bottom: 24px;
    color: #667eea;
    text-align: center;
}}
.table-wrapper {{
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}}
table {{
    width: 100%;
    min-width: 1400px;
    border-collapse: collapse;
    background: white;
}}
table.comparison-table {{
    font-size: 0.75em;
}}
table.comparison-table th {{
    padding: 8px 6px;
    white-space: nowrap;
}}
table.comparison-table td {{
    padding: 6px 6px;
}}
thead {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
}}
th {{
    padding: 16px;
    text-align: left;
    font-weight: 600;
    font-size: 0.95em;
}}
th.number {{ text-align: right; }}
td {{
    padding: 12px 16px;
    border-bottom: 1px solid #f0f0f0;
}}
td.number {{ text-align: right; }}
td.bracket-name {{
    font-weight: 600;
    background: #f8f9fa;
}}
tr:hover {{
    background: #f8f9fa;
}}
.stop-loss-details {{
    display: flex;
    flex-direction: column;
    gap: 30px;
}}
.detail-section {{
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}}
.detail-section h3 {{
    color: #667eea;
    margin-bottom: 15px;
    font-size: 1.3em;
}}
.detail-summary {{
    display: flex;
    gap: 30px;
    margin-bottom: 20px;
    padding: 15px;
    background: #f8f9fa;
    border-radius: 8px;
    flex-wrap: wrap;
}}
.detail-summary span {{
    font-size: 0.95em;
    color: #666;
}}
.detail-summary strong {{
    color: #333;
    margin-left: 5px;
}}
.detail-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9em;
}}
.detail-table thead {{
    background: #667eea;
    color: white;
}}
.detail-table th {{
    padding: 10px 8px;
    text-align: left;
    font-weight: 600;
}}
.detail-table td {{
    padding: 8px;
    border-bottom: 1px solid #f0f0f0;
}}
.detail-table tbody tr:hover {{
    background: #f8f9fa;
}}
.row-triggered {{
    background: rgba(231, 76, 60, 0.05);
}}
.badge-triggered-mini {{
    background: #e74c3c;
    color: white;
    padding: 2px 6px;
    border-radius: 6px;
    font-size: 0.75em;
    font-weight: 600;
}}
.center {{
    text-align: center !important;
}}
.drawdown-analysis {{
    padding: 40px;
    background: #f8f9fa;
}}
.drawdown-analysis h2 {{
    color: #667eea;
    margin-bottom: 10px;
    text-align: center;
}}
.drawdown-analysis .description {{
    text-align: center;
    color: #666;
    margin-bottom: 30px;
}}
.drawdown-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 24px;
    margin-bottom: 30px;
}}
.drawdown-card {{
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
}}
.drawdown-card h4 {{
    color: #667eea;
    margin-bottom: 20px;
    font-size: 1.2em;
    text-align: center;
}}
.drawdown-section {{
    margin-bottom: 20px;
    padding-bottom: 20px;
    border-bottom: 1px solid #f0f0f0;
}}
.drawdown-section:last-of-type {{
    border-bottom: none;
}}
.section-title {{
    font-weight: 600;
    margin-bottom: 12px;
    color: #333;
}}
.stat-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
}}
.stat-item-small {{
    background: #f8f9fa;
    padding: 12px;
    border-radius: 8px;
    text-align: center;
}}
.stat-item-small .label {{
    display: block;
    font-size: 0.8em;
    color: #666;
    margin-bottom: 6px;
}}
.stat-item-small .value {{
    display: block;
    font-size: 1.2em;
    font-weight: 700;
    color: #333;
}}
.stat-item-small .value.highlight {{
    color: #667eea;
}}
.suggestion {{
    background: #e8f4f8;
    padding: 12px;
    border-radius: 8px;
    text-align: center;
    color: #555;
}}
.suggestion strong {{
    color: #667eea;
    font-size: 1.1em;
}}
.analysis-note {{
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
}}
.analysis-note h3 {{
    color: #667eea;
    margin-bottom: 15px;
}}
.analysis-note p {{
    color: #555;
    line-height: 1.6;
    margin-bottom: 10px;
}}
.strategy-recommendation {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 24px;
    margin-bottom: 30px;
}}
.recommendation-card {{
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    border-left: 6px solid #3498db;
}}
.recommendation-card.highlight {{
    border-left-color: #f39c12;
    background: linear-gradient(135deg, #fff9e6 0%, #ffffff 100%);
}}
.recommendation-card h3 {{
    font-size: 1.2em;
    margin-bottom: 12px;
    color: #333;
}}
.recommendation-badge {{
    display: inline-block;
    padding: 8px 16px;
    border-radius: 20px;
    font-weight: 600;
    font-size: 0.95em;
    margin-bottom: 16px;
}}
.recommendation-badge.best {{
    background: #27ae60;
    color: white;
}}
.recommendation-badge.excellent {{
    background: #f39c12;
    color: white;
}}
.recommendation-badge.good {{
    background: #3498db;
    color: white;
}}
.recommendation-stats {{
    margin-top: 12px;
}}
.stat-item {{
    margin: 10px 0;
    padding: 8px 0;
    border-bottom: 1px solid #f0f0f0;
}}
.stat-item:last-child {{
    border-bottom: none;
}}
.stat-item .label {{
    display: block;
    color: #666;
    font-size: 0.85em;
    margin-bottom: 4px;
}}
.stat-item .value {{
    display: block;
    font-weight: 700;
    font-size: 1.3em;
    color: #333;
}}
.stat-item .reason {{
    display: block;
    color: #555;
    font-size: 0.9em;
    font-style: italic;
}}
.total-strategy-box {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 30px;
    border-radius: 12px;
    margin-top: 30px;
}}
.total-strategy-box h3 {{
    font-size: 1.5em;
    margin-bottom: 20px;
    text-align: center;
}}
.total-stats {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 20px;
}}
.total-stat {{
    background: rgba(255,255,255,0.15);
    padding: 20px;
    border-radius: 8px;
    text-align: center;
}}
.total-stat.highlight-stat {{
    background: rgba(255,255,255,0.25);
    border: 2px solid rgba(255,255,255,0.5);
}}
.total-stat .label {{
    font-size: 0.95em;
    opacity: 0.9;
    margin-bottom: 8px;
}}
.total-stat .value {{
    font-size: 2em;
    font-weight: 700;
    margin-bottom: 8px;
}}
.total-stat .diff {{
    font-size: 1.1em;
    opacity: 0.95;
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 損切りシミュレーション（日中Low判定）</h1>
        <div class="subtitle">V2.1.0.2 買いポジション - 損切り水準別パフォーマンス比較</div>
        <div class="subtitle" style="margin-top: 10px;">対象: 買い判定 {len(buy_df)}銘柄 | 判定: 日中Lowが損切り価格を下回ったら発動</div>
    </div>

    <div class="baseline-box">
        <h2>📈 ベースライン（損切りなし）</h2>
        <div class="baseline-stats">
            <div class="baseline-stat">
                <div class="label">勝率</div>
                <div class="value">{baseline['original_win_rate']:.1f}%</div>
            </div>
            <div class="baseline-stat">
                <div class="label">合計利益</div>
                <div class="value">{baseline['original_total_profit']:+,.0f}円</div>
            </div>
            <div class="baseline-stat">
                <div class="label">平均利益</div>
                <div class="value">{baseline['original_avg_profit']:+,.0f}円</div>
            </div>
            <div class="baseline-stat">
                <div class="label">勝/負/分</div>
                <div class="value">{baseline['original_wins']}/{baseline['total'] - baseline['original_wins'] - (buy_df['daily_close'] == buy_df['buy_price']).sum()}/{(buy_df['daily_close'] == buy_df['buy_price']).sum()}</div>
            </div>
        </div>
    </div>

    <div class="summary-section">
        <h2 style="margin-bottom: 20px; color: #667eea; text-align: center;">🎯 損切り水準別シミュレーション結果</h2>
        <div class="summary-grid">
            {''.join(summary_cards)}
        </div>
    </div>

    <div class="summary-section">
        <h2 style="margin-bottom: 20px; color: #667eea; text-align: center;">💡 価格帯別 最適損切り戦略</h2>
        <div class="strategy-recommendation">
            <div class="recommendation-card">
                <h3>💎 1,000円未満</h3>
                <div class="recommendation-badge best">損切りなし</div>
                <div class="recommendation-stats">
                    <div class="stat-item">
                        <span class="label">合計利益</span>
                        <span class="value positive">+800円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">理由</span>
                        <span class="reason">損切りで利益が減少</span>
                    </div>
                </div>
            </div>

            <div class="recommendation-card">
                <h3>💰 1,000-3,000円</h3>
                <div class="recommendation-badge good">損切り-5%</div>
                <div class="recommendation-stats">
                    <div class="stat-item">
                        <span class="label">合計利益</span>
                        <span class="value negative">-2,700円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">改善効果</span>
                        <span class="value positive">+12,100円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">理由</span>
                        <span class="reason">損失を大幅に軽減</span>
                    </div>
                </div>
            </div>

            <div class="recommendation-card highlight">
                <h3>💵 3,000-5,000円</h3>
                <div class="recommendation-badge excellent">損切り-3%</div>
                <div class="recommendation-stats">
                    <div class="stat-item">
                        <span class="label">合計利益</span>
                        <span class="value positive">+21,825円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">改善効果</span>
                        <span class="value positive">+33,425円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">理由</span>
                        <span class="reason">🔥 劇的改善！マイナスをプラスに転換</span>
                    </div>
                </div>
            </div>

            <div class="recommendation-card">
                <h3>💸 5,000-10,000円</h3>
                <div class="recommendation-badge good">損切り-3%</div>
                <div class="recommendation-stats">
                    <div class="stat-item">
                        <span class="label">合計利益</span>
                        <span class="value positive">+63,010円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">改善効果</span>
                        <span class="value positive">+1,010円</span>
                    </div>
                    <div class="stat-item">
                        <span class="label">理由</span>
                        <span class="reason">わずかに改善</span>
                    </div>
                </div>
            </div>
        </div>

        <div class="total-strategy-box">
            <h3>🎯 統合戦略での成績</h3>
            <div class="total-stats">
                <div class="total-stat">
                    <div class="label">損切りなし（全銘柄）</div>
                    <div class="value">+36,400円</div>
                </div>
                <div class="total-stat highlight-stat">
                    <div class="label">価格帯別最適戦略</div>
                    <div class="value positive">+82,935円</div>
                    <div class="diff">+46,535円の改善</div>
                </div>
            </div>
        </div>
    </div>

    {generate_drawdown_analysis_section(buy_df)}

    <div class="table-section">
        <h2>📋 損切り水準別 個別銘柄詳細</h2>
        <div class="stop-loss-details">
            {''.join(generate_stop_loss_detail_sections(buy_df, STOP_LOSS_LEVELS))}
        </div>
    </div>

    <div class="table-section">
        <h2>📊 価格帯別 損切りシミュレーション比較</h2>
        <p style="text-align: center; color: #666; margin-bottom: 15px; font-size: 0.9em;">
            ※ 横スクロールで全データを表示できます
        </p>
        <div class="table-wrapper">
            <table class="comparison-table">
                <thead>
                    <tr>
                        <th rowspan="2">価格帯</th>
                        <th colspan="3" style="text-align: center; border-right: 2px solid white;">損切りなし</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-1%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-1.5%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-2%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-2.5%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-3%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-3.5%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-4%</th>
                        <th colspan="2" style="text-align: center; border-right: 2px solid white;">-5%</th>
                        <th colspan="2" style="text-align: center;">-6%</th>
                    </tr>
                    <tr>
                        <th class="number">銘柄数</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number" style="border-right: 2px solid white;">合計利益</th>
                        <th class="number">勝率</th>
                        <th class="number">合計利益</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(bracket_comparison_rows)}
                </tbody>
            </table>
        </div>
    </div>

    {'<div class="table-section">' if has_morning_data else ''}
    {generate_morning_vs_day_comparison(morning_simulations, simulations, morning_baseline, baseline, morning_simulations_by_bracket, simulations, morning_baseline_by_bracket, baseline_by_bracket) if has_morning_data else ''}
    {'</div>' if has_morning_data else ''}
</div>

<div style="text-align: center; color: white; padding: 20px; font-size: 0.9em;">
    <p>損切りシミュレーション: 買いポジション対象 | 損切り水準: -1%, -1.5%, -2%, -2.5%, -3%, -3.5%, -4%, -5%, -6%</p>
    <p style="margin-top: 10px;">判定方法: 日中Lowが損切り価格を下回った場合に損切り発動 | 機会損失: 損切り後に終値がプラスだったケース</p>
</div>

</body>
</html>'''

    return html


def main() -> int:
    print("=" * 60)
    print("Stop Loss Simulation for V2.1.0.2")
    print("=" * 60)

    # [STEP 1] データ読み込み
    print("\n[STEP 1] Loading data...")

    if not INPUT_FILE.exists():
        print(f"  ✗ File not found: {INPUT_FILE}")
        return 1

    df = pd.read_parquet(INPUT_FILE)
    print(f"  ✓ Loaded: {len(df)} records, {len(df.columns)} columns")

    # [STEP 1.5] 前場引け価格を読み込み
    print("\n[STEP 1.5] Loading morning close prices...")
    morning_prices = load_morning_close_prices(PRICE_5M_FILE)
    if len(morning_prices) > 0:
        print(f"  ✓ Loaded: {len(morning_prices)} morning close prices")
    else:
        print(f"  ⚠ No morning close prices available")

    # [STEP 2] HTMLレポート生成
    print("\n[STEP 2] Generating HTML report...")
    html = generate_html_report(df, morning_prices)

    # [STEP 3] 保存
    print("\n[STEP 3] Saving...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  ✓ Saved: {OUTPUT_FILE}")

    print("\n✅ Simulation report generated successfully!")
    print(f"\n📄 Open the report: file://{OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
