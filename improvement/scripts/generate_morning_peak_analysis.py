#!/usr/bin/env python3
"""
morning_peak_analysis_full.parquet 生成スクリプト

surge_candidates_5m.parquet から日次集計分析データを生成
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


def generate_daily_analysis(df_5m: pd.DataFrame) -> pd.DataFrame:
    """5分足データから日次分析データを生成"""

    df_5m = df_5m.copy()
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime']).dt.tz_localize(None)
    df_5m['date'] = df_5m['Datetime'].dt.date
    df_5m['hour'] = df_5m['Datetime'].dt.hour

    results = []
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

            # 基本OHLCV
            open_price = day_data.iloc[0]['Open']
            high_price = day_data['High'].max()
            low_price = day_data['Low'].min()
            close_price = day_data.iloc[-1]['Close']
            volume = day_data['Volume'].sum()

            # 前場高値
            am_high = am_data['High'].max() if len(am_data) > 0 else high_price

            # 各種指標
            high_from_open_pct = (high_price - open_price) / open_price * 100
            close_from_high_pct = (close_price - high_price) / high_price * 100

            # 高値崩れ判定（日中高値 → 終値 -5%以上）
            is_morning_peak = close_from_high_pct <= -5

            results.append({
                'ticker': ticker,
                'date': date,
                'Open': open_price,
                'High': high_price,
                'Low': low_price,
                'Close': close_price,
                'Volume': volume,
                'am_high': am_high,
                'prev_close': None,  # 後で計算
                'gap_up_pct': None,  # 後で計算
                'high_from_open_pct': high_from_open_pct,
                'close_from_high_pct': close_from_high_pct,
                'daily_return_pct': None,  # 後で計算
                'is_morning_peak': is_morning_peak
            })

    df_result = pd.DataFrame(results)
    df_result = df_result.sort_values(['ticker', 'date']).reset_index(drop=True)

    # 前日終値、ギャップ、日次リターンを計算
    print("  前日比指標を計算中...")
    df_result['prev_close'] = df_result.groupby('ticker')['Close'].shift(1)
    df_result['gap_up_pct'] = (df_result['Open'] - df_result['prev_close']) / df_result['prev_close'] * 100
    df_result['daily_return_pct'] = (df_result['Close'] - df_result['prev_close']) / df_result['prev_close'] * 100

    return df_result


def main():
    print("=== morning_peak_analysis_full.parquet 生成 ===\n")

    # データ読み込み
    print("1. データ読み込み...")
    input_path = DATA_DIR / "surge_candidates_5m.parquet"
    df_5m = pd.read_parquet(input_path)
    print(f"   5分足データ: {len(df_5m):,} 行, {df_5m['ticker'].nunique()} 銘柄")

    # 日次分析生成
    print("\n2. 日次分析データ生成...")
    df_analysis = generate_daily_analysis(df_5m)

    # 保存
    print("\n3. 保存...")
    output_path = DATA_DIR / "morning_peak_analysis_full.parquet"
    df_analysis.to_parquet(output_path, index=False)

    # サマリー
    print(f"\n=== 完了 ===")
    print(f"出力: {output_path}")
    print(f"行数: {len(df_analysis):,}")
    print(f"銘柄数: {df_analysis['ticker'].nunique()}")
    print(f"期間: {df_analysis['date'].min()} 〜 {df_analysis['date'].max()}")
    print(f"高値崩れ日数: {df_analysis['is_morning_peak'].sum():,}")


if __name__ == '__main__':
    main()
