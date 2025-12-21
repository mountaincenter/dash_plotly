"""
高値崩れ常習犯リスト生成スクリプト v2

データソース: surge_candidates_5m.parquet
条件: 日中高値 → 終値 -5%以上（前場/後場問わず）

出力:
- morning_peak_watchlist.parquet (3回以上)
- morning_peak_1_2_times.parquet (1-2回)

追加カラム:
- am_peak_count: 前場高値の回数
- pm_peak_count: 後場高値の回数
- am_peak_ratio: 前場高値率（%）
"""

import pandas as pd
from collections import defaultdict


def count_morning_peak_patterns(df_5m: pd.DataFrame) -> dict:
    """全銘柄の高値崩れパターン回数をカウント（前場/後場の内訳付き）"""

    df_5m = df_5m.copy()
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime']).dt.tz_localize(None)
    df_5m['date'] = df_5m['Datetime'].dt.date
    df_5m['hour'] = df_5m['Datetime'].dt.hour

    pattern_counts = defaultdict(lambda: {
        'count': 0,
        'am_peak_count': 0,
        'pm_peak_count': 0,
        'crashes': []
    })

    tickers = df_5m['ticker'].unique()
    total = len(tickers)

    for i, ticker in enumerate(tickers):
        if (i + 1) % 500 == 0:
            print(f"  処理中: {i+1}/{total}")

        ticker_data = df_5m[df_5m['ticker'] == ticker]

        for date, day_data in ticker_data.groupby('date'):
            if len(day_data) < 10:
                continue

            day_data = day_data.sort_values('Datetime')
            am_data = day_data[day_data['hour'] < 12]

            if len(am_data) == 0:
                continue

            day_high = day_data['High'].max()
            am_high = am_data['High'].max()
            close = day_data.iloc[-1]['Close']
            high_to_close = (close - day_high) / day_high * 100

            # 条件: 日中高値 → 終値 -5%以上（前場/後場問わず）
            if high_to_close <= -5:
                pattern_counts[ticker]['count'] += 1
                pattern_counts[ticker]['crashes'].append(high_to_close)

                # 前場高値 vs 後場高値を記録
                if day_high == am_high:
                    pattern_counts[ticker]['am_peak_count'] += 1
                else:
                    pattern_counts[ticker]['pm_peak_count'] += 1

    return pattern_counts


def main():
    print("=== 高値崩れ常習犯リスト生成 v2 ===\n")

    # データ読み込み
    print("1. データ読み込み...")
    df_5m = pd.read_parquet('data/surge_candidates_5m.parquet')
    df_meta = pd.read_parquet('data/meta_jquants.parquet')
    print(f"   5分足データ: {len(df_5m):,} 行, {df_5m['ticker'].nunique()} 銘柄")

    # パターンカウント
    print("\n2. パターンカウント...")
    pattern_counts = count_morning_peak_patterns(df_5m)
    print(f"   パターン発生銘柄: {len(pattern_counts)}")

    # 最新株価取得
    print("\n3. 最新株価取得...")
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime']).dt.tz_localize(None)
    latest_prices = df_5m.sort_values('Datetime').groupby('ticker').last()[['Close']].reset_index()
    latest_prices.columns = ['ticker', 'latest_close']

    # DataFrameに変換
    results = []
    for ticker, data in pattern_counts.items():
        avg_crash = sum(data['crashes']) / len(data['crashes']) if data['crashes'] else None
        total = data['count']
        am_count = data['am_peak_count']
        pm_count = data['pm_peak_count']
        am_ratio = (am_count / total * 100) if total > 0 else 0

        results.append({
            'ticker': ticker,
            'morning_peak_count': total,
            'am_peak_count': am_count,
            'pm_peak_count': pm_count,
            'am_peak_ratio': round(am_ratio, 1),
            'avg_drop': avg_crash
        })

    df_results = pd.DataFrame(results)
    df_results = df_results.merge(df_meta[['ticker', 'stock_name', 'market']], on='ticker', how='left')
    df_results = df_results.merge(latest_prices, on='ticker', how='left')
    df_results = df_results.sort_values('morning_peak_count', ascending=False)

    # 分割保存
    print("\n4. 保存...")

    # 3回以上 (常習犯)
    df_3plus = df_results[df_results['morning_peak_count'] >= 3].copy()
    df_3plus.to_parquet('data/morning_peak_watchlist.parquet', index=False)
    print(f"   常習犯 (3回以上): {len(df_3plus)} 銘柄 -> morning_peak_watchlist.parquet")

    # 1-2回
    df_12 = df_results[df_results['morning_peak_count'].isin([1, 2])].copy()
    df_12 = df_12.rename(columns={'morning_peak_count': 'pattern_count', 'avg_drop': 'avg_crash', 'latest_close': 'price'})
    df_12.to_parquet('data/morning_peak_1_2_times.parquet', index=False)
    print(f"   1-2回組: {len(df_12)} 銘柄 -> morning_peak_1_2_times.parquet")

    # 検証: kudan確認
    print("\n5. 検証...")
    kudan = df_results[df_results['ticker'] == '4425.T']
    if len(kudan) > 0:
        k = kudan.iloc[0]
        print(f"   kudan(4425.T): {k['morning_peak_count']}回（前場{k['am_peak_count']}回 / 後場{k['pm_peak_count']}回）")
        print(f"   前場高値率: {k['am_peak_ratio']:.0f}%, 平均崩れ{k['avg_drop']:.1f}%")
        if k['morning_peak_count'] >= 3:
            print("   -> 常習犯リストに含まれる OK")
        else:
            print("   -> 常習犯リストに含まれない")
    else:
        print("   kudan(4425.T): データなし")

    # サマリー
    print("\n=== サマリー ===")
    print(f"総銘柄数: {df_5m['ticker'].nunique()}")
    print(f"パターン発生銘柄: {len(df_results)}")
    print(f"  - 3回以上: {len(df_3plus)}")
    print(f"  - 1-2回: {len(df_12)}")
    print(f"  - 0回: {df_5m['ticker'].nunique() - len(df_results)}")

    # 前場/後場傾向サマリー
    if len(df_3plus) > 0:
        am_dominant = df_3plus[df_3plus['am_peak_ratio'] >= 70]
        pm_dominant = df_3plus[df_3plus['am_peak_ratio'] <= 30]
        mixed = df_3plus[(df_3plus['am_peak_ratio'] > 30) & (df_3plus['am_peak_ratio'] < 70)]
        print(f"\n=== 高値傾向（3回以上） ===")
        print(f"  前場型（AM率70%以上）: {len(am_dominant)} 銘柄 -> 前場ショートOK")
        print(f"  後場型（AM率30%以下）: {len(pm_dominant)} 銘柄 -> 後場まで待て")
        print(f"  混合型（30-70%）: {len(mixed)} 銘柄 -> 慎重に")


if __name__ == '__main__':
    main()
