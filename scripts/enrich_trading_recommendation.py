#!/usr/bin/env python3
"""
[DEPRECATED] このスクリプトは不要になりました
grok_analysis_merged.parquet に統合されたため、このエンリッチメント処理は不要です

旧機能:
- trading_recommendation_history.parquet のデータを補完
- all_stocks.parquet から正しい銘柄名を取得
- prices_max_1d.parquet から前日終値を取得
- ATR% を正しく計算 (ATR絶対値 / 前日終値 × 100)
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "parquet"


def enrich_trading_recommendation(
    history_file: Path = None,
    all_stocks_file: Path = None,
    prices_file: Path = None
):
    """
    [DEPRECATED] grok_analysis_merged.parquet に統合されたため不要
    """
    # デフォルト値設定
    if history_file is None:
        history_file = DATA_DIR / "backtest" / "grok_analysis_merged.parquet"
    if all_stocks_file is None:
        all_stocks_file = DATA_DIR / "all_stocks.parquet"
    if prices_file is None:
        prices_file = DATA_DIR / "prices_max_1d.parquet"

    print(f"[INFO] Loading history file: {history_file}")
    if not history_file.exists():
        print(f"[ERROR] History file not found: {history_file}")
        sys.exit(1)

    history_df = pd.read_parquet(history_file)
    print(f"[INFO] Loaded {len(history_df)} records")

    # 最新日付のデータのみ処理
    history_df['recommendation_date'] = pd.to_datetime(history_df['recommendation_date'])
    latest_date = history_df['recommendation_date'].max()
    latest_mask = history_df['recommendation_date'] == latest_date

    print(f"[INFO] Latest date: {latest_date.date()}, {latest_mask.sum()} records")

    # 1. 銘柄名を all_stocks.parquet から更新
    print(f"[INFO] Loading stock names from: {all_stocks_file}")
    if all_stocks_file.exists():
        all_stocks = pd.read_parquet(all_stocks_file)
        stock_name_map = dict(zip(all_stocks['ticker'], all_stocks['stock_name']))

        updated_count = 0
        for idx in history_df[latest_mask].index:
            ticker = history_df.at[idx, 'ticker']
            if ticker in stock_name_map:
                old_name = history_df.at[idx, 'stock_name']
                new_name = stock_name_map[ticker]
                if old_name != new_name:
                    history_df.at[idx, 'stock_name'] = new_name
                    updated_count += 1
                    print(f"  {ticker}: {old_name} -> {new_name}")

        print(f"[INFO] Updated {updated_count} stock names")
    else:
        print(f"[WARNING] all_stocks.parquet not found, skipping stock name update")

    # 2. 前日終値を prices_max_1d.parquet から取得し、ATR% を再計算
    print(f"[INFO] Loading prices from: {prices_file}")
    if prices_file.exists():
        prices_df = pd.read_parquet(prices_file)
        prices_df['date'] = pd.to_datetime(prices_df['date'])

        updated_count = 0
        for idx in history_df[latest_mask].index:
            ticker = history_df.at[idx, 'ticker']

            # 該当銘柄の価格データを取得（日付降順）
            ticker_prices = prices_df[prices_df['ticker'] == ticker].sort_values('date', ascending=False)

            if len(ticker_prices) >= 2:
                # 最新の終値（今日）= ticker_prices.iloc[0]['Close']
                # 前日終値 = ticker_prices.iloc[1]['Close']
                prev_close = float(ticker_prices.iloc[1]['Close'])

                # 元のATR値を取得
                atr_value = float(history_df.at[idx, 'atr_value'])

                # ATR値が既にパーセントかどうかを判定
                # 通常、ATR%は0.1%〜20%程度なので、100以上なら絶対値と判定
                if atr_value >= 100:
                    # ATR絶対値 → ATR%に変換
                    atr_pct = (atr_value / prev_close) * 100
                    print(f"  {ticker}: prevClose={prev_close:.0f}円, ATR絶対値={atr_value:.2f} → ATR%={atr_pct:.2f}%")
                else:
                    # 既にパーセント形式
                    atr_pct = atr_value
                    print(f"  {ticker}: prevClose={prev_close:.0f}円, ATR%={atr_pct:.2f}% (already percentage)")

                # 更新
                history_df.at[idx, 'prev_close'] = prev_close
                history_df.at[idx, 'atr_value'] = atr_pct

                updated_count += 1
            else:
                print(f"  [WARNING] {ticker}: Not enough price data (found {len(ticker_prices)} days)")

        print(f"[INFO] Updated {updated_count} records with prev_close and ATR%")
    else:
        print(f"[WARNING] prices_max_1d.parquet not found, skipping prev_close/ATR% update")

    # 3. 保存
    print(f"[INFO] Saving enriched data to: {history_file}")
    history_df.to_parquet(history_file, index=False)
    print(f"[SUCCESS] Enrichment completed")


if __name__ == "__main__":
    # コマンドライン引数から取得
    if len(sys.argv) >= 2:
        history_file = Path(sys.argv[1])
    else:
        history_file = None

    if len(sys.argv) >= 3:
        all_stocks_file = Path(sys.argv[2])
    else:
        all_stocks_file = None

    if len(sys.argv) >= 4:
        prices_file = Path(sys.argv[3])
    else:
        prices_file = None

    enrich_trading_recommendation(history_file, all_stocks_file, prices_file)
