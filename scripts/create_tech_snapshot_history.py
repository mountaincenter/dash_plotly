#!/usr/bin/env python3
"""
tech_snapshot_history.parquet 作成

prices_max_1d.parquetから過去1年分（245営業日）のテクニカル指標を計算
各日付・各銘柄のtech_snapshotを作成してバックテスト用データとして保存

出力: data/parquet/tech_snapshot_history.parquet
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PARQUET_DIR = ROOT / 'data' / 'parquet'


def calculate_rsi(series, period=14):
    """RSI計算"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if len(rsi) > 0 else np.nan


def calculate_macd(series, fast=12, slow=26, signal=9):
    """MACD計算"""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return hist.iloc[-1] if len(hist) > 0 else np.nan


def calculate_bollinger_bands(series, period=20, std=2):
    """ボリンジャーバンド %B計算"""
    sma = series.rolling(window=period).mean()
    rolling_std = series.rolling(window=period).std()

    upper = sma + (rolling_std * std)
    lower = sma - (rolling_std * std)

    latest_price = series.iloc[-1]
    latest_upper = upper.iloc[-1]
    latest_lower = lower.iloc[-1]

    if pd.notna(latest_upper) and pd.notna(latest_lower) and latest_upper != latest_lower:
        percent_b = (latest_price - latest_lower) / (latest_upper - latest_lower)
        return percent_b
    return np.nan


def calculate_atr(high, low, close, period=14):
    """ATR %計算"""
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()

    latest_atr = atr.iloc[-1]
    latest_close = close.iloc[-1]

    if pd.notna(latest_atr) and pd.notna(latest_close) and latest_close > 0:
        return (latest_atr / latest_close) * 100
    return np.nan


def calculate_cmf(high, low, close, volume, period=20):
    """Chaikin Money Flow計算"""
    mf_multiplier = ((close - low) - (high - close)) / (high - low)
    mf_multiplier = mf_multiplier.fillna(0)
    mf_volume = mf_multiplier * volume

    cmf = mf_volume.rolling(window=period).sum() / volume.rolling(window=period).sum()
    return cmf.iloc[-1] if len(cmf) > 0 else np.nan


def calculate_obv_slope(close, volume, period=20):
    """OBV傾き計算"""
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()

    if len(obv) < period:
        return np.nan

    recent_obv = obv.iloc[-period:]
    slope = recent_obv.iloc[-1] - recent_obv.iloc[0]
    return slope


def calculate_sma_deviation(close, period=25):
    """移動平均線乖離率計算"""
    sma = close.rolling(window=period).mean()
    latest_close = close.iloc[-1]
    latest_sma = sma.iloc[-1]

    if pd.notna(latest_sma) and latest_sma > 0:
        return ((latest_close - latest_sma) / latest_sma) * 100
    return np.nan


def calculate_tech_indicators(stock_data):
    """1銘柄の全テクニカル指標を計算"""
    close = stock_data['Close']
    high = stock_data['High']
    low = stock_data['Low']
    volume = stock_data['Volume']

    indicators = {
        'atr14_pct': calculate_atr(high, low, close, period=14),
        'rsi14': calculate_rsi(close, period=14),
        'macd_hist': calculate_macd(close),
        'percent_b': calculate_bollinger_bands(close),
        'cmf20': calculate_cmf(high, low, close, volume, period=20),
        'obv_slope': calculate_obv_slope(close, volume, period=20),
        'sma25_dev_pct': calculate_sma_deviation(close, period=25),
    }

    return indicators


def score_indicator(value, indicator_name):
    """各指標をスコアリング（-2 ~ +2）"""
    if pd.isna(value):
        return {'label': '中立', 'score': 0}

    if indicator_name == 'rsi14':
        if value < 30:
            return {'label': '買い', 'score': 1}
        elif value > 70:
            return {'label': '売り', 'score': -1}
        else:
            return {'label': '中立', 'score': 0}

    elif indicator_name == 'macd_hist':
        if value > 0:
            return {'label': '買い', 'score': 1}
        elif value < 0:
            return {'label': '売り', 'score': -1}
        else:
            return {'label': '中立', 'score': 0}

    elif indicator_name == 'percent_b':
        if value < 0.2:
            return {'label': '買い', 'score': 1}
        elif value > 0.8:
            return {'label': '売り', 'score': -1}
        else:
            return {'label': '中立', 'score': 0}

    elif indicator_name == 'cmf20':
        if value > 0.1:
            return {'label': '買い', 'score': 1}
        elif value < -0.1:
            return {'label': '売り', 'score': -1}
        else:
            return {'label': '中立', 'score': 0}

    elif indicator_name == 'obv_slope':
        if value > 0:
            return {'label': '買い', 'score': 1}
        elif value < 0:
            return {'label': '売り', 'score': -1}
        else:
            return {'label': '中立', 'score': 0}

    elif indicator_name == 'sma25_dev_pct':
        if value > 5:
            return {'label': '売り', 'score': -1}
        elif value < -5:
            return {'label': '買い', 'score': 1}
        else:
            return {'label': '中立', 'score': 0}

    return {'label': '中立', 'score': 0}


def main():
    print("=" * 60)
    print("Create tech_snapshot_history.parquet")
    print("Period: Last 1 year (245 trading days)")
    print("=" * 60)

    # データ読み込み
    print("\n[1/4] Loading prices_max_1d.parquet...")
    prices = pd.read_parquet(PARQUET_DIR / 'prices_max_1d.parquet')
    prices['date'] = pd.to_datetime(prices['date'])

    # 直近1年のデータに絞る
    latest_date = prices['date'].max()
    one_year_ago = latest_date - timedelta(days=365)

    print(f"  Latest date: {latest_date.date()}")
    print(f"  Start date: {one_year_ago.date()}")

    # 対象日付を取得（営業日のみ）
    trading_days = sorted(prices[prices['date'] >= one_year_ago]['date'].unique())
    tickers = sorted(prices['ticker'].unique())

    print(f"  Trading days: {len(trading_days)}")
    print(f"  Tickers: {len(tickers)}")
    print(f"  Total calculations: {len(trading_days) * len(tickers):,}")

    # テクニカル指標を計算
    print("\n[2/4] Calculating technical indicators...")

    results = []

    # 各日付ごとに処理
    for target_date in tqdm(trading_days, desc="Processing dates"):
        # 対象日までのデータ
        historical = prices[prices['date'] <= target_date]

        # 各銘柄ごとに計算
        for ticker in tickers:
            ticker_data = historical[historical['ticker'] == ticker].sort_values('date')

            # 最低50日分のデータが必要
            if len(ticker_data) < 50:
                continue

            # テクニカル指標計算
            indicators = calculate_tech_indicators(ticker_data)

            # スコアリング
            votes = {}
            for name, value in indicators.items():
                votes[name] = score_indicator(value, name)

            # 総合スコア
            total_score = sum(v['score'] for v in votes.values())
            if total_score >= 2:
                overall_label = '強い買い'
            elif total_score >= 1:
                overall_label = '買い'
            elif total_score <= -2:
                overall_label = '強い売り'
            elif total_score <= -1:
                overall_label = '売り'
            else:
                overall_label = '中立'

            results.append({
                'ticker': ticker,
                'date': target_date,
                'values': indicators,
                'votes': votes,
                'overall': {
                    'label': overall_label,
                    'score': total_score
                }
            })

    print(f"\n  Calculated: {len(results):,} records")

    # DataFrame化
    print("\n[3/4] Creating DataFrame...")
    df = pd.DataFrame(results)

    print(f"  Records: {len(df):,}")
    print(f"  Date range: {df['date'].min().date()} ~ {df['date'].max().date()}")
    print(f"  Tickers: {df['ticker'].nunique()}")

    # 保存
    print("\n[4/4] Saving to parquet...")
    output_file = PARQUET_DIR / 'tech_snapshot_history.parquet'
    df.to_parquet(output_file, index=False)

    file_size = output_file.stat().st_size / 1024 / 1024
    print(f"  ✓ Saved: {output_file}")
    print(f"  File size: {file_size:.2f} MB")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Records: {len(df):,}")
    print(f"Period: {df['date'].min().date()} ~ {df['date'].max().date()}")
    print(f"Trading days: {df['date'].nunique()}")
    print(f"Tickers: {df['ticker'].nunique()}")
    print(f"File: {output_file}")
    print("=" * 60)

    print("\n✅ tech_snapshot_history.parquet created successfully!")

    return 0


if __name__ == '__main__':
    sys.exit(main())
