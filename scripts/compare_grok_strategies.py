#!/usr/bin/env python3
"""
compare_grok_strategies.py
Grok銘柄選定の3パターンを比較検証

パターンA: 26時のみ選定（10銘柄→Top5）
パターンB: 16時+26時選定（20銘柄→Top5）
パターンC: 16時Top5+26時Top5（固定10銘柄）

実行方法:
    python3 scripts/compare_grok_strategies.py --days 5
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import argparse
from datetime import datetime, timedelta


def calculate_selection_score(row):
    """選定時点でのスコアを計算"""
    score = row.get('sentiment_score', 0.5) * 100
    policy_bonus = {'High': 30, 'Med': 20, 'Low': 10}
    score += policy_bonus.get(row.get('policy_link', 'Low'), 10)
    if row.get('has_mention', False):
        score += 50
    return score


def load_backtest_data():
    """最新のバックテスト結果を読み込み"""
    backtest_dir = ROOT / "data/parquet/backtest_results"

    if not backtest_dir.exists():
        raise FileNotFoundError("No backtest results found")

    latest_result = sorted(backtest_dir.glob("*/summary.csv"))
    if not latest_result:
        raise FileNotFoundError("No summary.csv found in backtest results")

    latest_result_path = latest_result[-1]
    print(f"[INFO] Loading: {latest_result_path}")

    df = pd.read_csv(latest_result_path)

    # 選定スコアを計算
    df['selection_score'] = df.apply(calculate_selection_score, axis=1)

    return df


def simulate_pattern_a(df):
    """
    パターンA: 26時のみ選定（10銘柄→Top5）

    仮定: 全銘柄が26時選定と仮定し、各日のTop5のみを抽出
    """
    results = []

    for date in df['target_date'].unique():
        df_date = df[df['target_date'] == date].copy()

        # スコア上位5銘柄
        top5 = df_date.nlargest(5, 'selection_score')
        results.append(top5)

    df_pattern_a = pd.concat(results, ignore_index=True)

    # 統計計算
    total_trades = len(df_pattern_a)
    morning_wins = (df_pattern_a['morning_change_pct'] > 0).sum()
    morning_win_rate = morning_wins / total_trades * 100 if total_trades > 0 else 0
    morning_avg_return = df_pattern_a['morning_change_pct'].mean()
    morning_total_return = df_pattern_a['morning_change_pct'].sum()

    daily_wins = (df_pattern_a['daily_change_pct'] > 0).sum()
    daily_win_rate = daily_wins / total_trades * 100 if total_trades > 0 else 0
    daily_avg_return = df_pattern_a['daily_change_pct'].mean()
    daily_total_return = df_pattern_a['daily_change_pct'].sum()

    return {
        'pattern': 'A: 26時のみ選定（10銘柄→Top5）',
        'total_trades': total_trades,
        'morning_win_rate': morning_win_rate,
        'morning_avg_return': morning_avg_return,
        'morning_total_return': morning_total_return,
        'daily_win_rate': daily_win_rate,
        'daily_avg_return': daily_avg_return,
        'daily_total_return': daily_total_return,
        'df': df_pattern_a
    }


def simulate_pattern_b(df):
    """
    パターンB: 16時+26時選定（20銘柄→Top5）

    仮定: 各日10銘柄ずつ2回選定（合計20銘柄）し、スコア上位5銘柄を抽出
    現実には16時と26時で異なる銘柄を選定するが、
    ここでは既存データから各日20銘柄あると仮定してTop5を抽出
    """
    results = []

    for date in df['target_date'].unique():
        df_date = df[df['target_date'] == date].copy()

        # 20銘柄のうちTop5（実際には全データからTop5）
        # NOTE: 実データでは16時+26時で合計20銘柄になるが、
        # 現在は区別できないので全データからTop5を取る（パターンAと同じ）
        top5 = df_date.nlargest(5, 'selection_score')
        results.append(top5)

    df_pattern_b = pd.concat(results, ignore_index=True)

    # 統計計算
    total_trades = len(df_pattern_b)
    morning_wins = (df_pattern_b['morning_change_pct'] > 0).sum()
    morning_win_rate = morning_wins / total_trades * 100 if total_trades > 0 else 0
    morning_avg_return = df_pattern_b['morning_change_pct'].mean()
    morning_total_return = df_pattern_b['morning_change_pct'].sum()

    daily_wins = (df_pattern_b['daily_change_pct'] > 0).sum()
    daily_win_rate = daily_wins / total_trades * 100 if total_trades > 0 else 0
    daily_avg_return = df_pattern_b['daily_change_pct'].mean()
    daily_total_return = df_pattern_b['daily_change_pct'].sum()

    return {
        'pattern': 'B: 16時+26時選定（20銘柄→Top5）',
        'total_trades': total_trades,
        'morning_win_rate': morning_win_rate,
        'morning_avg_return': morning_avg_return,
        'morning_total_return': morning_total_return,
        'daily_win_rate': daily_win_rate,
        'daily_avg_return': daily_avg_return,
        'daily_total_return': daily_total_return,
        'df': df_pattern_b,
        'note': '※現在のデータでは16時/26時の区別がないため、パターンAと同じ結果'
    }


def simulate_pattern_c(df):
    """
    パターンC: 16時Top5+26時Top5（固定10銘柄）

    仮定: 各日のTop10をそのまま使用
    （実際には16時Top5 + 26時Top5 = 10銘柄固定）
    """
    results = []

    for date in df['target_date'].unique():
        df_date = df[df['target_date'] == date].copy()

        # Top10
        top10 = df_date.nlargest(10, 'selection_score')
        results.append(top10)

    df_pattern_c = pd.concat(results, ignore_index=True)

    # 統計計算
    total_trades = len(df_pattern_c)
    morning_wins = (df_pattern_c['morning_change_pct'] > 0).sum()
    morning_win_rate = morning_wins / total_trades * 100 if total_trades > 0 else 0
    morning_avg_return = df_pattern_c['morning_change_pct'].mean()
    morning_total_return = df_pattern_c['morning_change_pct'].sum()

    daily_wins = (df_pattern_c['daily_change_pct'] > 0).sum()
    daily_win_rate = daily_wins / total_trades * 100 if total_trades > 0 else 0
    daily_avg_return = df_pattern_c['daily_change_pct'].mean()
    daily_total_return = df_pattern_c['daily_change_pct'].sum()

    return {
        'pattern': 'C: 16時Top5+26時Top5（固定10銘柄）',
        'total_trades': total_trades,
        'morning_win_rate': morning_win_rate,
        'morning_avg_return': morning_avg_return,
        'morning_total_return': morning_total_return,
        'daily_win_rate': daily_win_rate,
        'daily_avg_return': daily_avg_return,
        'daily_total_return': daily_total_return,
        'df': df_pattern_c
    }


def print_comparison(results):
    """結果を比較表示"""
    print("\n" + "=" * 100)
    print("Grok銘柄選定パターン比較")
    print("=" * 100)

    # テーブルヘッダー
    print(f"\n{'パターン':<40} | {'取引数':>6} | {'前場勝率':>8} | {'前場平均':>8} | {'前場累計':>8} | {'終日勝率':>8} | {'終日平均':>8} | {'終日累計':>8}")
    print("-" * 140)

    for result in results:
        pattern = result['pattern']
        total = result['total_trades']
        m_win = result['morning_win_rate']
        m_avg = result['morning_avg_return']
        m_total = result['morning_total_return']
        d_win = result['daily_win_rate']
        d_avg = result['daily_avg_return']
        d_total = result['daily_total_return']

        print(f"{pattern:<40} | {total:>6} | {m_win:>7.1f}% | {m_avg:>7.2f}% | {m_total:>7.2f}% | {d_win:>7.1f}% | {d_avg:>7.2f}% | {d_total:>7.2f}%")

        if 'note' in result:
            print(f"  → {result['note']}")

    print("=" * 100)

    # 推奨パターンを表示
    print("\n【推奨パターン】")

    # 前場戦略で最も良いパターンを選ぶ
    best_morning = max(results, key=lambda x: x['morning_win_rate'] * x['morning_avg_return'])
    print(f"✅ 前場戦略: {best_morning['pattern']}")
    print(f"   勝率: {best_morning['morning_win_rate']:.1f}%, 平均リターン: {best_morning['morning_avg_return']:.2f}%")

    # 終日戦略で最も良いパターンを選ぶ
    best_daily = max(results, key=lambda x: x['daily_win_rate'] * x['daily_avg_return'])
    print(f"\n✅ 終日戦略: {best_daily['pattern']}")
    print(f"   勝率: {best_daily['daily_win_rate']:.1f}%, 平均リターン: {best_daily['daily_avg_return']:.2f}%")

    print("\n" + "=" * 100)


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description="Compare Grok selection patterns")
    parser.add_argument("--days", type=int, default=5, help="Number of days to analyze")
    args = parser.parse_args()

    print("=" * 100)
    print("Grok銘柄選定パターン比較スクリプト")
    print("=" * 100)

    # データ読み込み
    df = load_backtest_data()
    print(f"[OK] Loaded {len(df)} records")
    print(f"[INFO] Date range: {df['target_date'].min()} to {df['target_date'].max()}")
    print(f"[INFO] Unique dates: {df['target_date'].nunique()}")

    # 3パターンをシミュレーション
    pattern_a = simulate_pattern_a(df)
    pattern_b = simulate_pattern_b(df)
    pattern_c = simulate_pattern_c(df)

    # 比較結果を表示
    print_comparison([pattern_a, pattern_b, pattern_c])

    # 詳細CSVを保存
    output_dir = ROOT / "data/parquet/strategy_comparison"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    pattern_a['df'].to_csv(output_dir / f"pattern_a_{timestamp}.csv", index=False)
    pattern_b['df'].to_csv(output_dir / f"pattern_b_{timestamp}.csv", index=False)
    pattern_c['df'].to_csv(output_dir / f"pattern_c_{timestamp}.csv", index=False)

    print(f"\n[OK] Detailed results saved to: {output_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
