#!/usr/bin/env python3
"""
Phase 1 + Phase 2 分析（2段階スクリーニング）

Phase 1: 全銘柄をテクニカル + マーケット要因でスコアリング → 上位10%抽出
Phase 2: 上位10%のみ深掘り（WebSearch・ファンダメンタルズ）→ 再スコアリング

出力: test_output/phase1_phase2_analysis_YYYYMMDD.json
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

PARQUET_DIR = ROOT / 'data' / 'parquet'
TEST_OUTPUT_DIR = ROOT / 'test_output'


def load_latest_data():
    """最新データを読み込み"""
    print("[1/5] Loading latest data...")

    all_stocks = pd.read_parquet(PARQUET_DIR / 'all_stocks.parquet')
    tech_snapshot = pd.read_parquet(PARQUET_DIR / 'tech_snapshot_1d.parquet')
    prices = pd.read_parquet(PARQUET_DIR / 'prices_max_1d.parquet')
    prices['date'] = pd.to_datetime(prices['date'])

    # 最新日を取得
    latest_date = prices['date'].max()

    print(f"  ✓ Latest date: {latest_date.date()}")
    print(f"  ✓ Loaded {len(all_stocks)} stocks")

    return all_stocks, tech_snapshot, prices, latest_date


def extract_target_stocks(all_stocks):
    """Core30/政策銘柄を抽出"""
    print("\n[2/5] Extracting target stocks...")

    def has_category(cats, target):
        if cats is None:
            return False
        if isinstance(cats, (list, np.ndarray)):
            return target in list(cats)
        return False

    core30 = all_stocks[all_stocks['categories'].apply(lambda x: has_category(x, 'TOPIX_CORE30'))]
    policy = all_stocks[all_stocks['categories'].apply(lambda x: has_category(x, '政策銘柄'))]

    all_target = set(core30['ticker']) | set(policy['ticker'])
    target_stocks = all_stocks[all_stocks['ticker'].isin(all_target)].copy()

    print(f"  ✓ Target stocks: {len(target_stocks)}")

    return target_stocks


def calculate_phase1_score(target_stocks, tech_snapshot, prices):
    """Phase 1スコアを計算（テクニカル + マーケット要因の最適化版）"""
    print("\n[3/5] Calculating Phase 1 scores...")

    # 最適化された重み（optimize_scoring_with_market.py の結果）
    weights = {
        'stock_volatility_20d': 0.346,
        'atr14_pct': 0.336,
        'relative_volatility': 0.175,
        'relative_strength_20d': 0.143
    }

    results = []

    # 最新日の価格データ
    latest_date = prices['date'].max()
    latest_prices = prices[prices['date'] == latest_date]

    for idx, row in target_stocks.iterrows():
        ticker = row['ticker']
        stock_name = row['stock_name']

        # tech_snapshot から指標取得
        tech_row = tech_snapshot[tech_snapshot['ticker'] == ticker]

        if len(tech_row) == 0:
            continue

        tech_row = tech_row.iloc[0]
        values = tech_row.get('values', {})

        # ATR
        atr14_pct = values.get('atr14_pct', np.nan)

        # 個別銘柄の20日ボラティリティと20日リターンを計算
        stock_data = prices[prices['ticker'] == ticker].sort_values('date').tail(21)

        if len(stock_data) >= 20:
            stock_volatility_20d = stock_data['Close'].pct_change().std() * 100
            stock_return_20d = (stock_data.iloc[-1]['Close'] - stock_data.iloc[-20]['Close']) / stock_data.iloc[-20]['Close'] * 100
        else:
            stock_volatility_20d = np.nan
            stock_return_20d = np.nan

        # TOPIX-Primeとの相対（簡易版：今回は固定値として扱う）
        # 本来はTOPIXのボラティリティと比較が必要だが、プロトタイプなので簡略化
        relative_volatility = 0  # TODO: 実装
        relative_strength_20d = 0  # TODO: 実装

        # Phase 1スコア計算（簡易版）
        phase1_score = 0.0
        feature_count = 0

        if not pd.isna(atr14_pct):
            # ATRを正規化（簡易版：0-10の範囲と仮定）
            normalized_atr = (atr14_pct - 2) / 8 * 2 - 1  # -1 ~ +1 に正規化
            phase1_score += normalized_atr * weights['atr14_pct']
            feature_count += 1

        if not pd.isna(stock_volatility_20d):
            # ボラティリティを正規化（簡易版：0-5の範囲と仮定）
            normalized_vol = (stock_volatility_20d - 1.5) / 3.5 * 2 - 1
            phase1_score += normalized_vol * weights['stock_volatility_20d']
            feature_count += 1

        # スコアを -100 ~ +100 にスケール
        if feature_count > 0:
            phase1_score = phase1_score * 100
        else:
            phase1_score = 0

        # 最新価格取得
        latest_close = latest_prices[latest_prices['ticker'] == ticker]['Close'].values
        latest_close = latest_close[0] if len(latest_close) > 0 else np.nan

        results.append({
            'ticker': ticker,
            'stock_name': stock_name,
            'categories': list(row['categories']) if isinstance(row['categories'], np.ndarray) else row['categories'],
            'sectors': row['sectors'],
            'latest_close': latest_close,
            'phase1_score': round(phase1_score, 2),
            'atr14_pct': atr14_pct,
            'stock_volatility_20d': stock_volatility_20d
        })

    # スコアでソート
    results.sort(key=lambda x: x['phase1_score'], reverse=True)

    # ランク付け
    for rank, item in enumerate(results, 1):
        item['phase1_rank'] = rank

    print(f"  ✓ Calculated scores for {len(results)} stocks")

    return results


def filter_top_10_percent(phase1_results):
    """Phase 1上位10%を抽出"""
    print("\n[4/5] Filtering top 10%...")

    total_count = len(phase1_results)
    top_10_count = max(1, int(total_count * 0.1))

    top_10_percent = phase1_results[:top_10_count]

    print(f"  ✓ Total stocks: {total_count}")
    print(f"  ✓ Top 10%: {top_10_count} stocks")
    print(f"\n  Top stocks:")
    for stock in top_10_percent:
        print(f"    #{stock['phase1_rank']:2d} {stock['ticker']:8s} {stock['stock_name']:20s} Score: {stock['phase1_score']:+7.2f}")

    return top_10_percent


def run_phase2_analysis(top_stocks):
    """Phase 2分析（深掘り）- プロトタイプ版"""
    print("\n[5/5] Running Phase 2 deep analysis...")

    phase2_results = []

    for stock in top_stocks:
        ticker = stock['ticker']
        stock_name = stock['stock_name']

        print(f"\n  Analyzing {ticker} {stock_name}...")

        # Phase 2スコア要素（プロトタイプ：今は手動/ダミー値）
        phase2_score_components = {
            'news_sentiment': 0,  # TODO: WebSearch実装
            'ir_sentiment': 0,    # TODO: IR情報分析
            'fundamental_score': 0,  # TODO: PER/PBR/決算分析
            'momentum_confirmation': 0  # TODO: 材料の有無
        }

        # Phase 2総合スコア（-100 ~ +100）
        phase2_score = sum(phase2_score_components.values())

        # 最終スコア（Phase 1 40% + Phase 2 60%）
        final_score = stock['phase1_score'] * 0.4 + phase2_score * 0.6

        phase2_results.append({
            **stock,
            'phase2_executed': True,
            'phase2_components': phase2_score_components,
            'phase2_score': round(phase2_score, 2),
            'final_score': round(final_score, 2),
            'phase2_note': 'Phase 2はプロトタイプ版（WebSearch未実装）'
        })

        print(f"    Phase 1: {stock['phase1_score']:+7.2f}")
        print(f"    Phase 2: {phase2_score:+7.2f} (プロトタイプ)")
        print(f"    Final:   {final_score:+7.2f}")

    # 最終スコアでソート
    phase2_results.sort(key=lambda x: x['final_score'], reverse=True)

    # 最終ランク付け
    for rank, item in enumerate(phase2_results, 1):
        item['final_rank'] = rank

    return phase2_results


def save_results(phase1_results, top_10_percent, phase2_results, latest_date):
    """結果を保存"""
    print("\n[6/6] Saving results...")

    today = datetime.now().strftime('%Y%m%d')
    output_file = TEST_OUTPUT_DIR / f'phase1_phase2_analysis_{today}.json'

    output = {
        'generated_at': datetime.now().isoformat(),
        'analysis_date': latest_date.strftime('%Y-%m-%d'),
        'target_date': '2025-11-18',
        'methodology': {
            'phase1': 'テクニカル + マーケット要因（最適化スコアリング）',
            'phase1_threshold': 'Top 10%',
            'phase2': 'WebSearch + ファンダメンタルズ分析（プロトタイプ版）',
            'final_score': 'Phase1 × 0.4 + Phase2 × 0.6'
        },
        'summary': {
            'total_stocks': len(phase1_results),
            'top_10_percent_count': len(top_10_percent),
            'phase2_analyzed_count': len(phase2_results)
        },
        'phase1_all_stocks': phase1_results,
        'top_10_percent': top_10_percent,
        'phase2_results': phase2_results
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ Saved: {output_file}")

    return output_file


def main():
    print("=" * 60)
    print("Phase 1 + Phase 2 Analysis (2-Stage Screening)")
    print("Target: 2025-11-18")
    print("=" * 60)

    # データ読み込み
    all_stocks, tech_snapshot, prices, latest_date = load_latest_data()

    # Core30/政策銘柄抽出
    target_stocks = extract_target_stocks(all_stocks)

    # Phase 1: スコア計算
    phase1_results = calculate_phase1_score(target_stocks, tech_snapshot, prices)

    # 上位10%抽出
    top_10_percent = filter_top_10_percent(phase1_results)

    # Phase 2: 深掘り分析
    phase2_results = run_phase2_analysis(top_10_percent)

    # 結果保存
    output_file = save_results(phase1_results, top_10_percent, phase2_results, latest_date)

    print("\n✅ Analysis completed!")
    print(f"\n📊 Results:")
    print(f"  Total stocks analyzed: {len(phase1_results)}")
    print(f"  Top 10% stocks: {len(top_10_percent)}")
    print(f"  Phase 2 deep dive: {len(phase2_results)}")
    print(f"\n📁 Output: {output_file}")
    print(f"\n⚠️  Note: Phase 2 is prototype (WebSearch not implemented yet)")

    return 0


if __name__ == '__main__':
    sys.exit(main())
