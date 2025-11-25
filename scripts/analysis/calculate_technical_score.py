#!/usr/bin/env python3
"""
Phase 1: テクニカル分析スコア計算（WebSearch除く）

Core30/政策銘柄（56銘柄）に対して：
1. tech_snapshot_1d.parquet から既計算の指標スコアを取得
2. TOPIX-Prime との相対強度を計算
3. セクター指数との相対強度を計算
4. テクニカルスコアを算出（-100 ~ +100）

出力: test_output/technical_scores_YYYYMMDD.json
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

# パス設定
PARQUET_DIR = ROOT / 'data' / 'parquet'
TEST_OUTPUT_DIR = ROOT / 'test_output'
TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data():
    """必要なデータを読み込み"""
    print("[1/5] Loading data...")

    # all_stocks.parquet（Core30/政策銘柄抽出用）
    all_stocks = pd.read_parquet(PARQUET_DIR / 'all_stocks.parquet')

    # tech_snapshot_1d.parquet（テクニカル指標）
    tech_snapshot = pd.read_parquet(PARQUET_DIR / 'tech_snapshot_1d.parquet')

    # prices_max_1d.parquet（株価データ）
    prices = pd.read_parquet(PARQUET_DIR / 'prices_max_1d.parquet')
    prices['date'] = pd.to_datetime(prices['date'])

    # topix_prices_max_1d.parquet（TOPIX-Prime）
    topix = pd.read_parquet(PARQUET_DIR / 'topix_prices_max_1d.parquet')
    topix['date'] = pd.to_datetime(topix['date'])

    # sectors_prices_max_1d.parquet（33業種別指数）
    sectors = pd.read_parquet(PARQUET_DIR / 'sectors_prices_max_1d.parquet')
    sectors['date'] = pd.to_datetime(sectors['date'])

    print(f"  ✓ all_stocks: {len(all_stocks)} stocks")
    print(f"  ✓ tech_snapshot: {len(tech_snapshot)} stocks")
    print(f"  ✓ prices: {len(prices):,} records")
    print(f"  ✓ topix: {len(topix):,} records")
    print(f"  ✓ sectors: {len(sectors):,} records")

    return all_stocks, tech_snapshot, prices, topix, sectors


def extract_target_stocks(all_stocks):
    """Core30/政策銘柄を抽出"""
    print("\n[2/5] Extracting Core30 and policy stocks...")

    def has_category(cats, target):
        if cats is None:
            return False
        if isinstance(cats, (list, np.ndarray)):
            return target in list(cats)
        return False

    core30 = all_stocks[all_stocks['categories'].apply(lambda x: has_category(x, 'TOPIX_CORE30'))]
    policy = all_stocks[all_stocks['categories'].apply(lambda x: has_category(x, '政策銘柄'))]

    # 重複除去
    all_target = set(core30['ticker']) | set(policy['ticker'])
    target_stocks = all_stocks[all_stocks['ticker'].isin(all_target)].copy()

    print(f"  ✓ TOPIX Core30: {len(core30)} stocks")
    print(f"  ✓ 政策銘柄: {len(policy)} stocks")
    print(f"  ✓ Total (unique): {len(target_stocks)} stocks")

    return target_stocks


def calculate_tech_snapshot_score(tech_snapshot, ticker):
    """tech_snapshot から指標スコアを計算"""
    row = tech_snapshot[tech_snapshot['ticker'] == ticker]

    if len(row) == 0:
        return 0, {}

    votes = row.iloc[0]['votes']

    # 各指標のスコアを取得（-2 ~ +2）
    scores = {
        'ma': votes.get('ma', {}).get('score', 0),
        'macd_hist': votes.get('macd_hist', {}).get('score', 0),
        'rsi14': votes.get('rsi14', {}).get('score', 0),
        'percent_b': votes.get('percent_b', {}).get('score', 0),
        'ichimoku': votes.get('ichimoku', {}).get('score', 0),
        'cmf20': votes.get('cmf20', {}).get('score', 0),
        'obv_slope': votes.get('obv_slope', {}).get('score', 0),
        'donchian': votes.get('donchian', {}).get('score', 0),
        'roc12': votes.get('roc12', {}).get('score', 0),
        'sma25_dev_pct': votes.get('sma25_dev_pct', {}).get('score', 0)
    }

    # 合計スコアを -100 ~ +100 に正規化
    total = sum(scores.values())  # -20 ~ +20
    normalized_score = total * 5  # -100 ~ +100

    return normalized_score, scores


def calculate_relative_strength(prices, topix, ticker, sector_code=None):
    """TOPIX-Prime / セクター との相対強度を計算"""
    # 最新5日間の騰落率を計算
    latest_date = prices['date'].max()
    start_date = latest_date - pd.Timedelta(days=7)  # 営業日5日分を確保

    # 銘柄の騰落率
    stock_data = prices[(prices['ticker'] == ticker) & (prices['date'] >= start_date)].sort_values('date')
    if len(stock_data) < 2:
        return 0, 0, None, None

    latest_close = stock_data.iloc[-1]['Close']
    latest_date_actual = stock_data.iloc[-1]['date']
    stock_return = (stock_data.iloc[-1]['Close'] - stock_data.iloc[0]['Close']) / stock_data.iloc[0]['Close'] * 100

    # TOPIX-Prime の騰落率
    topix_prime = topix[topix['ticker'] == '0500'].copy()  # TOPIX-Prime
    topix_data = topix_prime[(topix_prime['date'] >= start_date)].sort_values('date')
    if len(topix_data) < 2:
        topix_relative = 0
    else:
        topix_return = (topix_data.iloc[-1]['close'] - topix_data.iloc[0]['close']) / topix_data.iloc[0]['close'] * 100
        diff = stock_return - topix_return

        # スコアリング（-10 ~ +10）
        if diff >= 3:
            topix_relative = 10
        elif diff >= 1:
            topix_relative = 5
        elif diff >= -1:
            topix_relative = 0
        elif diff >= -3:
            topix_relative = -5
        else:
            topix_relative = -10

    # セクター相対強度は今回はスキップ（セクターコードのマッピングが必要）
    sector_relative = 0

    return topix_relative, sector_relative, latest_close, latest_date_actual


def calculate_scores(target_stocks, tech_snapshot, prices, topix, sectors):
    """全銘柄のスコアを計算"""
    print("\n[3/5] Calculating technical scores...")

    results = []

    for idx, row in target_stocks.iterrows():
        ticker = row['ticker']
        stock_name = row['stock_name']

        # 1. tech_snapshot スコア
        tech_score, tech_details = calculate_tech_snapshot_score(tech_snapshot, ticker)

        # 2. 相対強度と直近終値
        topix_relative, sector_relative, latest_close, latest_date = calculate_relative_strength(prices, topix, ticker)

        # 3. 総合テクニカルスコア
        total_score = tech_score + topix_relative + sector_relative

        # -100 ~ +100 に制限
        total_score = max(-100, min(100, total_score))

        # 判定
        if total_score >= 50:
            signal = 'StrongBuy'
        elif total_score <= -40:
            signal = 'StrongSell'
        else:
            signal = 'Hold'

        results.append({
            'ticker': ticker,
            'stock_name': stock_name,
            'categories': list(row['categories']) if isinstance(row['categories'], np.ndarray) else row['categories'],
            'sectors': row['sectors'],
            'series': row['series'],
            'latest_close': latest_close,
            'latest_date': latest_date.strftime('%Y-%m-%d') if latest_date else None,
            'tech_snapshot_score': tech_score,
            'tech_details': tech_details,
            'topix_relative': topix_relative,
            'sector_relative': sector_relative,
            'total_score': round(total_score, 2),
            'signal': signal
        })

        print(f"  {ticker} {stock_name}: {total_score:.1f} ({signal})")

    return results


def save_results(results):
    """結果をJSONで保存"""
    print("\n[4/5] Saving results...")

    today = datetime.now().strftime('%Y%m%d')
    output_file = TEST_OUTPUT_DIR / f'technical_scores_{today}.json'

    output = {
        'generated_at': datetime.now().isoformat(),
        'date': today,
        'total_stocks': len(results),
        'summary': {
            'StrongBuy': sum(1 for r in results if r['signal'] == 'StrongBuy'),
            'Hold': sum(1 for r in results if r['signal'] == 'Hold'),
            'StrongSell': sum(1 for r in results if r['signal'] == 'StrongSell')
        },
        'stocks': results
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✓ Saved: {output_file}")
    print(f"\n  Summary:")
    print(f"    StrongBuy:  {output['summary']['StrongBuy']}")
    print(f"    Hold:       {output['summary']['Hold']}")
    print(f"    StrongSell: {output['summary']['StrongSell']}")

    return output_file


def main():
    print("=" * 60)
    print("Phase 1: Technical Score Calculation")
    print("=" * 60)

    # データ読み込み
    all_stocks, tech_snapshot, prices, topix, sectors = load_data()

    # Core30/政策銘柄抽出
    target_stocks = extract_target_stocks(all_stocks)

    # スコア計算
    results = calculate_scores(target_stocks, tech_snapshot, prices, topix, sectors)

    # 結果保存
    output_file = save_results(results)

    print("\n[5/5] Done!")
    print(f"\nNext: Generate HTML report from {output_file}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
