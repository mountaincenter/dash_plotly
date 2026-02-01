#!/usr/bin/env python3
"""
archiveのchange_pctを修正
backtest_date - 1営業日の変化率を取得
"""

import pandas as pd


def main():
    archive_path = 'data/parquet/backtest/grok_trending_archive.parquet'
    prices_path = 'improvement/grok_prices_max_1d.parquet'

    df_archive = pd.read_parquet(archive_path)
    df_prices = pd.read_parquet(prices_path)

    print(f"Archive: {len(df_archive)} records")
    print(f"Prices: {len(df_prices)} records")

    # 日付型を統一
    df_prices['date'] = pd.to_datetime(df_prices['date'].astype(str).str[:10])
    df_archive['backtest_date'] = pd.to_datetime(df_archive['backtest_date'].astype(str).str[:10])

    # 営業日リスト
    business_days = sorted(df_prices['date'].unique())
    bd_set = set(business_days)

    # 前営業日を計算する関数
    def get_prev_business_day(target_date):
        for bd in reversed(business_days):
            if bd < target_date:
                return bd
        return None

    # 価格データをソート
    df_prices = df_prices.sort_values(['ticker', 'date'])

    # 前日終値を計算
    df_prices['prev_close'] = df_prices.groupby('ticker')['Close'].shift(1)

    # change_pctを計算
    df_prices['change_pct_calc'] = (
        (df_prices['Close'] - df_prices['prev_close']) / df_prices['prev_close'] * 100
    ).round(2)

    # マップ作成
    price_map = df_prices.set_index(['ticker', 'date'])['change_pct_calc'].to_dict()

    # archiveにchange_pctを更新（backtest_date - 1営業日）
    updated_count = 0
    for idx, row in df_archive.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']

        # backtest_dateの前営業日を取得
        prev_bd = get_prev_business_day(backtest_date)
        if prev_bd is None:
            continue

        # 正しいselection_dateを設定
        df_archive.at[idx, 'selection_date'] = prev_bd

        key = (ticker, prev_bd)
        if key in price_map and pd.notna(price_map[key]):
            df_archive.at[idx, 'change_pct'] = price_map[key]
            updated_count += 1

    print(f"\n=== 結果 ===")
    print(f"Updated: {updated_count} / {len(df_archive)}")
    print(f"change_pct non-null: {df_archive['change_pct'].notna().sum()}")

    # 確認
    print(f"\n=== サンプル確認 ===")
    sample = df_archive.iloc[0]
    print(f"ticker: {sample['ticker']}")
    print(f"selection_date: {sample['selection_date']}")
    print(f"backtest_date: {sample['backtest_date']}")
    print(f"change_pct: {sample['change_pct']}")

    # 保存
    df_archive.to_parquet(archive_path, index=False)
    print(f"\n[DONE] Saved to {archive_path}")


if __name__ == "__main__":
    main()
