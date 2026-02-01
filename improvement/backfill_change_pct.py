#!/usr/bin/env python3
"""
archiveにchange_pctをbackfill

grok_prices_max_1d.parquetを使って、selection_date時点のchange_pctを計算
"""

import pandas as pd


def main():
    # データ読み込み
    archive_path = 'data/parquet/backtest/grok_trending_archive.parquet'
    prices_path = 'improvement/grok_prices_max_1d.parquet'

    df_archive = pd.read_parquet(archive_path)
    df_prices = pd.read_parquet(prices_path)

    print(f"Archive: {len(df_archive)} records")
    print(f"Prices: {len(df_prices)} records, {df_prices['ticker'].nunique()} tickers")

    # 日付型を統一（文字列の日付部分だけ取り出す）
    df_prices['date'] = pd.to_datetime(df_prices['date'].astype(str).str[:10])
    df_archive['selection_date'] = pd.to_datetime(df_archive['selection_date'].astype(str).str[:10])
    df_archive['backtest_date'] = pd.to_datetime(df_archive['backtest_date'].astype(str).str[:10])

    # 価格データをソート
    df_prices = df_prices.sort_values(['ticker', 'date'])

    # 前日終値を計算
    df_prices['prev_close'] = df_prices.groupby('ticker')['Close'].shift(1)

    # change_pctを計算
    df_prices['change_pct_calc'] = (
        (df_prices['Close'] - df_prices['prev_close']) / df_prices['prev_close'] * 100
    ).round(2)

    # selection_dateでマージするためのマップ作成
    # selection_date = backtest_dateの前営業日なので、selection_dateの終値とその前日終値でchange_pctを計算
    price_map = df_prices.set_index(['ticker', 'date'])['change_pct_calc'].to_dict()

    # archiveにchange_pctを追加
    updated_count = 0
    for idx, row in df_archive.iterrows():
        ticker = row['ticker']
        selection_date = row['selection_date']

        key = (ticker, selection_date)
        if key in price_map and pd.notna(price_map[key]):
            df_archive.at[idx, 'change_pct'] = price_map[key]
            updated_count += 1

    # 結果確認
    print(f"\n=== 結果 ===")
    print(f"Updated: {updated_count} / {len(df_archive)}")
    print(f"change_pct non-null: {df_archive['change_pct'].notna().sum()}")

    print(f"\n=== change_pct統計 ===")
    print(df_archive['change_pct'].describe())

    # 保存
    df_archive.to_parquet(archive_path, index=False)
    print(f"\n[DONE] Saved to {archive_path}")


if __name__ == "__main__":
    main()
