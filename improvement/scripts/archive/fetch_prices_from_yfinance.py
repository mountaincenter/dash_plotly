#!/usr/bin/env python3
"""
improvement用のpricesデータをyfinanceから取得

grok_trending_archive.parquetの銘柄に対して
yfinanceから直接株価データを取得し、
data/parquetと同じ形式で保存
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import time

# パス設定
BASE_DIR = Path(__file__).parent.parent
IMPROVEMENT_DATA_DIR = BASE_DIR / 'data'

# 対象銘柄を取得
grok_archive = pd.read_parquet(IMPROVEMENT_DATA_DIR / 'grok_trending_archive.parquet')
target_tickers = sorted(grok_archive['ticker'].unique())

print(f"対象銘柄数: {len(target_tickers)}銘柄")
print(f"取得期間: 過去730日分（日足）、60日分（5分足）")

# 1. prices_max_1d.parquet 作成（日足）
print("\n" + "="*60)
print("日足データ取得中（prices_max_1d.parquet）...")
print("="*60)

all_data_1d = []
success_count = 0
fail_count = 0

for i, ticker in enumerate(target_tickers, 1):
    print(f"[{i}/{len(target_tickers)}] {ticker}... ", end='', flush=True)

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

        all_data_1d.append(hist_df)
        print(f"✅ {len(hist_df)}件")
        success_count += 1

        # API制限対策
        time.sleep(0.1)

    except Exception as e:
        print(f"❌ エラー: {e}")
        fail_count += 1

# 結合して保存
if all_data_1d:
    df_1d = pd.concat(all_data_1d, ignore_index=True)
    # 日付をdatetime型に変換（タイムゾーン削除）
    df_1d['date'] = pd.to_datetime(df_1d['date']).dt.tz_localize(None)
    # ソート
    df_1d = df_1d.sort_values(['ticker', 'date']).reset_index(drop=True)

    output_path = IMPROVEMENT_DATA_DIR / 'prices_max_1d.parquet'
    df_1d.to_parquet(output_path, index=False)
    print(f"\n✅ 保存: {output_path}")
    print(f"   総件数: {len(df_1d):,}件")
    print(f"   銘柄数: {df_1d['ticker'].nunique()}銘柄")
    print(f"   成功: {success_count}銘柄、失敗: {fail_count}銘柄")

# 2. prices_60d_5m.parquet 作成（5分足）
print("\n" + "="*60)
print("5分足データ取得中（prices_60d_5m.parquet）...")
print("="*60)

all_data_5m = []
success_count_5m = 0
fail_count_5m = 0

for i, ticker in enumerate(target_tickers, 1):
    print(f"[{i}/{len(target_tickers)}] {ticker}... ", end='', flush=True)

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

        all_data_5m.append(hist_df)
        print(f"✅ {len(hist_df)}件")
        success_count_5m += 1

        # API制限対策
        time.sleep(0.1)

    except Exception as e:
        print(f"❌ エラー: {e}")
        fail_count_5m += 1

# 結合して保存
if all_data_5m:
    df_5m = pd.concat(all_data_5m, ignore_index=True)
    # 日付をdatetime型に変換（タイムゾーン削除）
    df_5m['date'] = pd.to_datetime(df_5m['date']).dt.tz_localize(None)
    # ソート
    df_5m = df_5m.sort_values(['ticker', 'date']).reset_index(drop=True)

    output_path = IMPROVEMENT_DATA_DIR / 'prices_60d_5m.parquet'
    df_5m.to_parquet(output_path, index=False)
    print(f"\n✅ 保存: {output_path}")
    print(f"   総件数: {len(df_5m):,}件")
    print(f"   銘柄数: {df_5m['ticker'].nunique()}銘柄")
    print(f"   成功: {success_count_5m}銘柄、失敗: {fail_count_5m}銘柄")

print("\n" + "="*60)
print("完了: improvement/dataに必要なpricesファイルを作成しました")
print("="*60)
