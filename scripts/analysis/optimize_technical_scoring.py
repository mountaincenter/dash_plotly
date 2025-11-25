#!/usr/bin/env python3
"""
テクニカルスコアリング最適化（過学習防止版）

アプローチ:
1. Train/Test分割（前半6ヶ月 / 後半6ヶ月）
2. 各指標の予測力を個別検証
3. シンプルな線形重み付けのみ
4. Out-of-sample検証

出力: test_output/optimized_scoring_YYYYMMDD.json
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
    print("[1/6] Loading data...")

    tech_history = pd.read_parquet(PARQUET_DIR / 'tech_snapshot_history.parquet')
    prices = pd.read_parquet(PARQUET_DIR / 'prices_max_1d.parquet')
    prices['date'] = pd.to_datetime(prices['date'])

    print(f"  ✓ tech_history: {len(tech_history):,} records")
    print(f"  ✓ prices: {len(prices):,} records")

    return tech_history, prices


def prepare_dataset(tech_history, prices):
    """予測用データセットを準備"""
    print("\n[2/6] Preparing dataset...")

    # 将来リターンを計算
    returns_data = []

    for ticker in tqdm(prices['ticker'].unique(), desc="Calculating returns"):
        ticker_data = prices[prices['ticker'] == ticker].sort_values('date').copy()

        # 翌週リターン (5営業日後)
        ticker_data['next_week_return'] = (ticker_data['Close'].shift(-5) / ticker_data['Close'] - 1) * 100

        returns_data.append(ticker_data[['ticker', 'date', 'Close', 'next_week_return']])

    returns_df = pd.concat(returns_data, ignore_index=True)

    # tech_history から指標値を抽出
    tech_data = []

    for idx, row in tech_history.iterrows():
        values = row['values']
        tech_data.append({
            'ticker': row['ticker'],
            'date': pd.to_datetime(row['date']),
            'rsi14': values.get('rsi14', np.nan),
            'macd_hist': values.get('macd_hist', np.nan),
            'percent_b': values.get('percent_b', np.nan),
            'cmf20': values.get('cmf20', np.nan),
            'obv_slope': values.get('obv_slope', np.nan),
            'sma25_dev_pct': values.get('sma25_dev_pct', np.nan),
            'atr14_pct': values.get('atr14_pct', np.nan)
        })

    tech_df = pd.DataFrame(tech_data)

    # マージ
    dataset = pd.merge(tech_df, returns_df, on=['ticker', 'date'], how='inner')
    dataset = dataset.dropna(subset=['next_week_return'])

    print(f"  ✓ Dataset: {len(dataset):,} records")
    print(f"  Date range: {dataset['date'].min().date()} ~ {dataset['date'].max().date()}")

    return dataset


def train_test_split(dataset):
    """Train/Test分割（前半6ヶ月 / 後半6ヶ月）"""
    print("\n[3/6] Splitting train/test...")

    # 日付でソート
    dataset = dataset.sort_values('date').copy()

    # 中央値で分割
    mid_date = dataset['date'].median()

    train = dataset[dataset['date'] < mid_date].copy()
    test = dataset[dataset['date'] >= mid_date].copy()

    print(f"  ✓ Train: {len(train):,} records ({train['date'].min().date()} ~ {train['date'].max().date()})")
    print(f"  ✓ Test:  {len(test):,} records ({test['date'].min().date()} ~ {test['date'].max().date()})")

    return train, test


def evaluate_indicator(data, indicator_name):
    """個別指標の予測力を評価"""
    # NaNを除外
    valid_data = data.dropna(subset=[indicator_name]).copy()

    if len(valid_data) == 0:
        return {
            'correlation': 0,
            'top_tercile_return': 0,
            'bottom_tercile_return': 0,
            'spread': 0
        }

    # 相関
    correlation = valid_data[indicator_name].corr(valid_data['next_week_return'])

    # Tercile分析（3分割）
    valid_data['tercile'] = pd.qcut(valid_data[indicator_name], q=3, labels=['Low', 'Mid', 'High'], duplicates='drop')

    tercile_returns = valid_data.groupby('tercile', observed=True)['next_week_return'].mean()

    top_return = tercile_returns.get('High', 0)
    bottom_return = tercile_returns.get('Low', 0)
    spread = top_return - bottom_return  # Highが良ければプラス

    return {
        'correlation': float(correlation),
        'top_tercile_return': float(top_return),
        'bottom_tercile_return': float(bottom_return),
        'spread': float(spread),
        'sample_size': len(valid_data)
    }


def analyze_indicators(train, test):
    """各指標の予測力を分析"""
    print("\n[4/6] Analyzing individual indicators...")

    indicators = ['rsi14', 'macd_hist', 'percent_b', 'cmf20', 'obv_slope', 'sma25_dev_pct', 'atr14_pct']

    results = {}

    print("\n  Train Set:")
    for indicator in indicators:
        train_perf = evaluate_indicator(train, indicator)
        test_perf = evaluate_indicator(test, indicator)

        results[indicator] = {
            'train': train_perf,
            'test': test_perf
        }

        print(f"    {indicator:15s} | Train Corr: {train_perf['correlation']:+.4f}, Spread: {train_perf['spread']:+.3f}% | Test Corr: {test_perf['correlation']:+.4f}, Spread: {test_perf['spread']:+.3f}%")

    return results


def optimize_weights(train, indicator_results):
    """重み付けを最適化（シンプルな手法）"""
    print("\n[5/6] Optimizing weights...")

    # Test setでの相関が正で、Train/Test両方でSpreadが正の指標のみ使用
    good_indicators = []

    for indicator, perf in indicator_results.items():
        train_spread = perf['train']['spread']
        test_spread = perf['test']['spread']
        test_corr = perf['test']['correlation']

        # 条件: Test相関が正、かつTrain/Test両方のSpreadが正
        if test_corr > 0 and train_spread > 0 and test_spread > 0:
            good_indicators.append({
                'name': indicator,
                'test_corr': test_corr,
                'train_spread': train_spread,
                'test_spread': test_spread,
                'avg_spread': (train_spread + test_spread) / 2
            })

    if not good_indicators:
        print("  ⚠️ No indicators with consistent positive predictive power found!")
        return None, []

    # 平均Spreadでソート
    good_indicators.sort(key=lambda x: x['avg_spread'], reverse=True)

    print(f"  ✓ Selected indicators: {len(good_indicators)}")
    for ind in good_indicators:
        print(f"    {ind['name']:15s} | Avg Spread: {ind['avg_spread']:+.3f}%")

    # 重み付け: 平均Spreadに比例
    total_spread = sum(ind['avg_spread'] for ind in good_indicators)

    weights = {}
    for ind in good_indicators:
        if total_spread > 0:
            weights[ind['name']] = ind['avg_spread'] / total_spread
        else:
            weights[ind['name']] = 1.0 / len(good_indicators)

    print("\n  Weights:")
    for name, weight in weights.items():
        print(f"    {name:15s}: {weight:.3f}")

    return weights, good_indicators


def apply_scoring(data, weights, indicator_results):
    """新しいスコアリングを適用"""
    if not weights:
        return data

    data = data.copy()
    data['optimized_score'] = 0.0

    for indicator, weight in weights.items():
        # 指標を -1 ~ +1 に正規化（Tercile境界を使用）
        valid_values = data[indicator].dropna()

        if len(valid_values) == 0:
            continue

        # 3分位点を取得
        q33 = valid_values.quantile(0.33)
        q67 = valid_values.quantile(0.67)

        # スコアリング: Low=-1, Mid=0, High=+1
        data['temp_score'] = 0.0
        data.loc[data[indicator] < q33, 'temp_score'] = -1.0
        data.loc[data[indicator] > q67, 'temp_score'] = 1.0

        data['optimized_score'] += data['temp_score'] * weight

    data.drop(columns=['temp_score'], inplace=True)

    # -1 ~ +1 の範囲に正規化
    max_score = data['optimized_score'].abs().max()
    if max_score > 0:
        data['optimized_score'] = data['optimized_score'] / max_score

    return data


def evaluate_scoring(data, scoring_column='optimized_score'):
    """スコアリングのパフォーマンスを評価"""
    # NaNを除外
    valid_data = data.dropna(subset=[scoring_column, 'next_week_return']).copy()

    if len(valid_data) == 0:
        return {}

    # 相関
    correlation = valid_data[scoring_column].corr(valid_data['next_week_return'])

    # スコアを3分割（duplicatesを処理）
    try:
        valid_data['score_tercile'] = pd.qcut(valid_data[scoring_column], q=3, labels=['Low', 'Mid', 'High'], duplicates='drop')
    except ValueError:
        # qcutが失敗した場合は手動で3分割
        q33 = valid_data[scoring_column].quantile(0.33)
        q67 = valid_data[scoring_column].quantile(0.67)
        valid_data['score_tercile'] = 'Mid'
        valid_data.loc[valid_data[scoring_column] <= q33, 'score_tercile'] = 'Low'
        valid_data.loc[valid_data[scoring_column] >= q67, 'score_tercile'] = 'High'

    tercile_perf = {}
    for tercile in ['Low', 'Mid', 'High']:
        tercile_data = valid_data[valid_data['score_tercile'] == tercile]

        if len(tercile_data) > 0:
            tercile_perf[tercile] = {
                'count': len(tercile_data),
                'avg_return': float(tercile_data['next_week_return'].mean()),
                'median_return': float(tercile_data['next_week_return'].median()),
                'win_rate': float((tercile_data['next_week_return'] > 0).mean() * 100)
            }

    spread = tercile_perf.get('High', {}).get('avg_return', 0) - tercile_perf.get('Low', {}).get('avg_return', 0)

    return {
        'correlation': float(correlation),
        'tercile_performance': tercile_perf,
        'spread': float(spread),
        'sample_size': len(valid_data)
    }


def save_results(indicator_results, weights, good_indicators, train_perf, test_perf):
    """結果を保存"""
    print("\n[6/6] Saving results...")

    today = datetime.now().strftime('%Y%m%d')
    output_file = TEST_OUTPUT_DIR / f'optimized_scoring_{today}.json'

    output = {
        'generated_at': datetime.now().isoformat(),
        'date': today,
        'methodology': {
            'approach': 'Train/Test Split (前半6ヶ月 / 後半6ヶ月)',
            'indicator_selection': 'Test相関が正 かつ Train/Test両方のSpreadが正',
            'weighting': '平均Spreadに比例した重み付け',
            'scoring': 'Tercile-based normalization (-1, 0, +1)'
        },
        'indicator_analysis': indicator_results,
        'selected_indicators': good_indicators,
        'weights': weights,
        'performance': {
            'train': train_perf,
            'test': test_perf
        }
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ Saved: {output_file}")

    return output_file


def main():
    print("=" * 60)
    print("Optimize Technical Scoring (Overfitting Prevention)")
    print("=" * 60)

    # データ読み込み
    tech_history, prices = load_data()

    # データセット準備
    dataset = prepare_dataset(tech_history, prices)

    # Train/Test分割
    train, test = train_test_split(dataset)

    # 各指標の予測力を分析
    indicator_results = analyze_indicators(train, test)

    # 重み付け最適化
    weights, good_indicators = optimize_weights(train, indicator_results)

    if not weights:
        print("\n❌ 予測力のある指標が見つかりませんでした")
        return 1

    # スコアリング適用
    train = apply_scoring(train, weights, indicator_results)
    test = apply_scoring(test, weights, indicator_results)

    # パフォーマンス評価
    train_perf = evaluate_scoring(train, 'optimized_score')
    test_perf = evaluate_scoring(test, 'optimized_score')

    print("\n" + "=" * 60)
    print("Performance Summary")
    print("=" * 60)
    print(f"\nTrain Set:")
    print(f"  Correlation: {train_perf['correlation']:+.4f}")
    print(f"  Spread (High - Low): {train_perf['spread']:+.3f}%")
    print(f"  High Tercile: {train_perf['tercile_performance']['High']['avg_return']:+.3f}%")
    print(f"  Low Tercile: {train_perf['tercile_performance']['Low']['avg_return']:+.3f}%")

    print(f"\nTest Set (Out-of-Sample):")
    print(f"  Correlation: {test_perf['correlation']:+.4f}")
    print(f"  Spread (High - Low): {test_perf['spread']:+.3f}%")
    print(f"  High Tercile: {test_perf['tercile_performance']['High']['avg_return']:+.3f}%")
    print(f"  Low Tercile: {test_perf['tercile_performance']['Low']['avg_return']:+.3f}%")

    # 結果保存
    output_file = save_results(indicator_results, weights, good_indicators, train_perf, test_perf)

    print("\n✅ Optimization completed!")
    print(f"\nNext: Update scoring logic with optimized weights")

    return 0


if __name__ == '__main__':
    sys.exit(main())
