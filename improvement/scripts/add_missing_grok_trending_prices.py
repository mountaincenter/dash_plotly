#!/usr/bin/env python3
"""
grok_trending.parquetにあるがprices_{period}_{interval}.parquetにない銘柄を追加

yfinanceから不足銘柄のデータを取得し、既存のpricesファイルに追加
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
import time

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'

# 不足銘柄を特定
print("=== 不足銘柄の特定 ===")
grok_df = pd.read_parquet(DATA_DIR / 'grok_trending.parquet')
grok_tickers = set(grok_df['ticker'].unique())

prices_1d = pd.read_parquet(DATA_DIR / 'prices_max_1d.parquet')
prices_tickers = set(prices_1d['ticker'].unique())

missing_tickers = sorted(grok_tickers - prices_tickers)
print(f"grok_trending: {len(grok_tickers)}銘柄")
print(f"prices既存: {len(prices_tickers)}銘柄")
print(f"不足: {len(missing_tickers)}銘柄")
print(f"  → {missing_tickers}")

if not missing_tickers:
    print("\n全銘柄データ済み。終了します。")
    exit(0)

# 1. prices_max_1d.parquet に追加（日足）
print("\n" + "="*60)
print("日足データ取得中（prices_max_1d.parquet）...")
print("="*60)

new_data_1d = []
success_count = 0
fail_count = 0

for i, ticker in enumerate(missing_tickers, 1):
    print(f"[{i}/{len(missing_tickers)}] {ticker}... ", end='', flush=True)

    try:
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period='max', interval='1d')

        if hist.empty:
            print("❌ データなし")
            fail_count += 1
            continue

        # データ整形
        hist_df = hist.reset_index()
        hist_df['ticker'] = ticker
        hist_df = hist_df.rename(columns={'Date': 'date'})

        # カラムを揃える
        hist_df = hist_df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']]

        new_data_1d.append(hist_df)
        print(f"✅ {len(hist_df)}件")
        success_count += 1

        # API制限対策
        time.sleep(0.1)

    except Exception as e:
        print(f"❌ エラー: {e}")
        fail_count += 1

# 既存データに追加
if new_data_1d:
    new_df_1d = pd.concat(new_data_1d, ignore_index=True)
    # 日付をdatetime型に変換（タイムゾーン削除）
    new_df_1d['date'] = pd.to_datetime(new_df_1d['date']).dt.tz_localize(None)

    # 既存データと結合
    combined_1d = pd.concat([prices_1d, new_df_1d], ignore_index=True)
    combined_1d = combined_1d.sort_values(['ticker', 'date']).reset_index(drop=True)

    output_path = DATA_DIR / 'prices_max_1d.parquet'
    combined_1d.to_parquet(output_path, index=False)
    print(f"\n✅ 保存: {output_path}")
    print(f"   追加: {len(new_df_1d):,}件")
    print(f"   総件数: {len(combined_1d):,}件")
    print(f"   銘柄数: {combined_1d['ticker'].nunique()}銘柄（+{success_count}）")

# 2. prices_60d_5m.parquet に追加（5分足）
print("\n" + "="*60)
print("5分足データ取得中（prices_60d_5m.parquet）...")
print("="*60)

prices_5m = pd.read_parquet(DATA_DIR / 'prices_60d_5m.parquet')

new_data_5m = []
success_count_5m = 0
fail_count_5m = 0

for i, ticker in enumerate(missing_tickers, 1):
    print(f"[{i}/{len(missing_tickers)}] {ticker}... ", end='', flush=True)

    try:
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period='60d', interval='5m')

        if hist.empty:
            print("❌ データなし")
            fail_count_5m += 1
            continue

        # データ整形
        hist_df = hist.reset_index()
        hist_df['ticker'] = ticker
        hist_df = hist_df.rename(columns={'Datetime': 'date'})

        # カラムを揃える
        hist_df = hist_df[['date', 'Open', 'High', 'Low', 'Close', 'Volume', 'ticker']]

        new_data_5m.append(hist_df)
        print(f"✅ {len(hist_df)}件")
        success_count_5m += 1

        # API制限対策
        time.sleep(0.1)

    except Exception as e:
        print(f"❌ エラー: {e}")
        fail_count_5m += 1

# 既存データに追加
if new_data_5m:
    new_df_5m = pd.concat(new_data_5m, ignore_index=True)
    # 日付をdatetime型に変換（タイムゾーン削除）
    new_df_5m['date'] = pd.to_datetime(new_df_5m['date']).dt.tz_localize(None)

    # 既存データと結合
    combined_5m = pd.concat([prices_5m, new_df_5m], ignore_index=True)
    combined_5m = combined_5m.sort_values(['ticker', 'date']).reset_index(drop=True)

    output_path = DATA_DIR / 'prices_60d_5m.parquet'
    combined_5m.to_parquet(output_path, index=False)
    print(f"\n✅ 保存: {output_path}")
    print(f"   追加: {len(new_df_5m):,}件")
    print(f"   総件数: {len(combined_5m):,}件")
    print(f"   銘柄数: {combined_5m['ticker'].nunique()}銘柄（+{success_count_5m}）")

print("\n" + "="*60)
print("完了: grok_trending.parquetの不足銘柄をpricesファイルに追加しました")
print("="*60)
