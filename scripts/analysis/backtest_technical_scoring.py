#!/usr/bin/env python3
"""
テクニカルスコアリングのバックテスト検証

tech_snapshot_history.parquet を使用して：
1. 各日の各銘柄のスコアを取得
2. 翌日・翌週のリターンを計算
3. スコアとリターンの相関を分析
4. シグナル別のパフォーマンスを評価

出力: test_output/backtest_validation_YYYYMMDD.json
"""

import sys
from pathlib import Path
from datetime import datetime
import json

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from tqdm import tqdm

PARQUET_DIR = ROOT / 'data' / 'parquet'
TEST_OUTPUT_DIR = ROOT / 'test_output'


def load_data():
    """データ読み込み"""
    print("[1/5] Loading data...")

    # 履歴テクニカルデータ
    tech_history = pd.read_parquet(PARQUET_DIR / 'tech_snapshot_history.parquet')

    # 価格データ
    prices = pd.read_parquet(PARQUET_DIR / 'prices_max_1d.parquet')
    prices['date'] = pd.to_datetime(prices['date'])

    print(f"  ✓ tech_history: {len(tech_history):,} records")
    print(f"  ✓ prices: {len(prices):,} records")

    return tech_history, prices


def calculate_future_returns(prices):
    """各日の翌日・翌週リターンを計算"""
    print("\n[2/5] Calculating future returns...")

    returns_data = []

    for ticker in tqdm(prices['ticker'].unique(), desc="Processing tickers"):
        ticker_data = prices[prices['ticker'] == ticker].sort_values('date').copy()

        # 翌日リターン
        ticker_data['next_day_return'] = ticker_data['Close'].pct_change().shift(-1) * 100

        # 翌週リターン (5営業日後)
        ticker_data['next_week_return'] = (ticker_data['Close'].shift(-5) / ticker_data['Close'] - 1) * 100

        returns_data.append(ticker_data[['ticker', 'date', 'Close', 'next_day_return', 'next_week_return']])

    returns_df = pd.concat(returns_data, ignore_index=True)
    print(f"  ✓ Calculated returns for {len(returns_df):,} records")

    return returns_df


def merge_tech_and_returns(tech_history, returns_df):
    """テクニカルスコアとリターンをマージ"""
    print("\n[3/5] Merging technical scores with returns...")

    # tech_history から必要なデータを抽出
    tech_scores = []

    for idx, row in tech_history.iterrows():
        overall = row['overall']
        tech_scores.append({
            'ticker': row['ticker'],
            'date': pd.to_datetime(row['date']),
            'tech_label': overall['label'],
            'tech_score': overall['score']
        })

    tech_df = pd.DataFrame(tech_scores)

    # マージ
    merged = pd.merge(
        tech_df,
        returns_df,
        on=['ticker', 'date'],
        how='inner'
    )

    # NaNを除外
    merged = merged.dropna(subset=['next_day_return', 'next_week_return'])

    print(f"  ✓ Merged: {len(merged):,} records")

    return merged


def analyze_performance(merged):
    """シグナル別パフォーマンスを分析"""
    print("\n[4/5] Analyzing signal performance...")

    results = {
        'overall_stats': {},
        'signal_performance': {},
        'score_correlation': {}
    }

    # 全体統計
    results['overall_stats'] = {
        'total_records': len(merged),
        'avg_next_day_return': float(merged['next_day_return'].mean()),
        'avg_next_week_return': float(merged['next_week_return'].mean()),
        'score_range': {
            'min': int(merged['tech_score'].min()),
            'max': int(merged['tech_score'].max()),
            'mean': float(merged['tech_score'].mean())
        }
    }

    # シグナル別パフォーマンス
    for label in ['強い買い', '買い', '中立', '売り', '強い売り']:
        signal_data = merged[merged['tech_label'] == label]

        if len(signal_data) == 0:
            continue

        results['signal_performance'][label] = {
            'count': len(signal_data),
            'avg_next_day_return': float(signal_data['next_day_return'].mean()),
            'avg_next_week_return': float(signal_data['next_week_return'].mean()),
            'next_day_win_rate': float((signal_data['next_day_return'] > 0).mean() * 100),
            'next_week_win_rate': float((signal_data['next_week_return'] > 0).mean() * 100),
            'next_day_median': float(signal_data['next_day_return'].median()),
            'next_week_median': float(signal_data['next_week_return'].median())
        }

    # スコアとリターンの相関
    results['score_correlation'] = {
        'next_day': float(merged['tech_score'].corr(merged['next_day_return'])),
        'next_week': float(merged['tech_score'].corr(merged['next_week_return']))
    }

    # スコア範囲別の平均リターン
    merged['score_bin'] = pd.cut(
        merged['tech_score'],
        bins=[-20, -10, -5, 0, 5, 10, 20],
        labels=['<-10', '-10~-5', '-5~0', '0~5', '5~10', '>10']
    )

    score_bins = {}
    for bin_label in merged['score_bin'].unique():
        if pd.isna(bin_label):
            continue
        bin_data = merged[merged['score_bin'] == bin_label]
        score_bins[str(bin_label)] = {
            'count': len(bin_data),
            'avg_next_day_return': float(bin_data['next_day_return'].mean()),
            'avg_next_week_return': float(bin_data['next_week_return'].mean())
        }

    results['score_bins'] = score_bins

    # サマリー出力
    print("\n  📊 Signal Performance Summary:")
    for label, perf in results['signal_performance'].items():
        print(f"    {label}:")
        print(f"      Count: {perf['count']}")
        print(f"      Next Day: {perf['avg_next_day_return']:+.2f}% (勝率 {perf['next_day_win_rate']:.1f}%)")
        print(f"      Next Week: {perf['avg_next_week_return']:+.2f}% (勝率 {perf['next_week_win_rate']:.1f}%)")

    print(f"\n  📈 Score Correlation:")
    print(f"    Next Day: {results['score_correlation']['next_day']:.4f}")
    print(f"    Next Week: {results['score_correlation']['next_week']:.4f}")

    return results


def save_results(results):
    """結果を保存"""
    print("\n[5/5] Saving results...")

    today = datetime.now().strftime('%Y%m%d')
    output_file = TEST_OUTPUT_DIR / f'backtest_validation_{today}.json'

    output = {
        'generated_at': datetime.now().isoformat(),
        'date': today,
        **results
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ Saved: {output_file}")

    return output_file


def main():
    print("=" * 60)
    print("Backtest: Technical Scoring Validation")
    print("=" * 60)

    # データ読み込み
    tech_history, prices = load_data()

    # 将来リターン計算
    returns_df = calculate_future_returns(prices)

    # マージ
    merged = merge_tech_and_returns(tech_history, returns_df)

    # パフォーマンス分析
    results = analyze_performance(merged)

    # 保存
    output_file = save_results(results)

    print("\n✅ Backtest validation completed!")
    print(f"\nNext: Generate HTML report with backtest results")

    return 0


if __name__ == '__main__':
    sys.exit(main())
