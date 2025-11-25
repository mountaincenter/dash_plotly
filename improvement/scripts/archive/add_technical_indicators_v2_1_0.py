#!/usr/bin/env python3
"""
add_technical_indicators_v2_1_0.py

grok_analysis_merged_20251121.parquet に v2.1 用のテクニカル指標カラムを追加

追加するカラム:
- rsi_14d: 14日RSI
- volume_change_20d: Volume / SMA20
- sma_5d: 5日移動平均
- price_vs_sma5_pct: (Price - SMA5) / SMA5 * 100

データソース: improvement/data/prices_max_1d.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# パス設定
IMPROVEMENT_DATA_DIR = ROOT / "improvement" / "data"
GROK_ANALYSIS_FILE = IMPROVEMENT_DATA_DIR / "grok_analysis_merged_20251121.parquet"
PRICES_FILE = IMPROVEMENT_DATA_DIR / "prices_max_1d.parquet"
OUTPUT_FILE = IMPROVEMENT_DATA_DIR / "grok_analysis_merged_20251121_with_indicators.parquet"


def calculate_rsi(prices: pd.Series, period: int = 14) -> float:
    """
    RSI (Relative Strength Index) を計算

    Args:
        prices: 価格のSeriesデータ (新しい順)
        period: 計算期間（デフォルト14日）

    Returns:
        RSI値（0-100）、計算不可の場合はNaN
    """
    if len(prices) < period + 1:
        return np.nan

    # 古い順にソート
    prices = prices.sort_index()

    # 価格変動を計算
    delta = prices.diff()

    # 上昇と下降を分離
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # 平均上昇/下降を計算
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    # RSを計算
    rs = avg_gain / avg_loss

    # RSIを計算
    rsi = 100 - (100 / (1 + rs))

    # 最新のRSI値を返す
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else np.nan


def calculate_sma(prices: pd.Series, period: int) -> float:
    """
    単純移動平均（SMA）を計算

    Args:
        prices: 価格のSeriesデータ
        period: 計算期間

    Returns:
        SMA値、計算不可の場合はNaN
    """
    if len(prices) < period:
        return np.nan

    return prices.head(period).mean()


def calculate_volume_change(volumes: pd.Series, period: int = 20) -> float:
    """
    出来高変化率を計算（最新出来高 / SMA20）

    Args:
        volumes: 出来高のSeriesデータ (新しい順)
        period: 計算期間（デフォルト20日）

    Returns:
        出来高変化率、計算不可の場合はNaN
    """
    if len(volumes) < period:
        return np.nan

    latest_volume = volumes.iloc[0]
    sma_volume = volumes.head(period).mean()

    if sma_volume == 0:
        return np.nan

    return latest_volume / sma_volume


def add_technical_indicators(grok_df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    grok_analysis_merged_20251121.parquet に テクニカル指標を追加

    Args:
        grok_df: grok_analysis_merged_20251121.parquet のデータ
        prices_df: prices_max_1d.parquet のデータ

    Returns:
        テクニカル指標が追加されたDataFrame
    """
    # 新しいカラムを初期化
    grok_df['rsi_14d'] = np.nan
    grok_df['volume_change_20d'] = np.nan
    grok_df['sma_5d'] = np.nan
    grok_df['price_vs_sma5_pct'] = np.nan

    print(f"[INFO] Processing {len(grok_df)} records...")

    # 各レコードに対して指標を計算
    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']

        # 該当銘柄のprice dataを取得
        ticker_prices = prices_df[prices_df['ticker'] == ticker].copy()

        if ticker_prices.empty:
            print(f"  [WARN] No price data for {ticker}")
            continue

        # dateカラムをdatetimeに変換
        ticker_prices['date'] = pd.to_datetime(ticker_prices['date'])

        # backtest_date以前のデータのみ使用（未来データを含めない）
        backtest_dt = pd.to_datetime(backtest_date)
        ticker_prices = ticker_prices[ticker_prices['date'] <= backtest_dt]

        # 日付でソート（新しい順）
        ticker_prices = ticker_prices.sort_values('date', ascending=False)

        if len(ticker_prices) < 2:
            print(f"  [WARN] Insufficient data for {ticker} on {backtest_date}")
            continue

        # 最新価格
        latest_close = ticker_prices.iloc[0]['Close']

        # RSI 14日計算
        rsi_14d = calculate_rsi(ticker_prices['Close'], period=14)

        # 出来高変化率計算
        volume_change_20d = calculate_volume_change(ticker_prices['Volume'], period=20)

        # SMA 5日計算
        sma_5d = calculate_sma(ticker_prices['Close'], period=5)

        # Price vs SMA5計算
        if not pd.isna(sma_5d) and sma_5d != 0:
            price_vs_sma5_pct = ((latest_close - sma_5d) / sma_5d) * 100
        else:
            price_vs_sma5_pct = np.nan

        # データフレームに格納
        grok_df.at[idx, 'rsi_14d'] = rsi_14d
        grok_df.at[idx, 'volume_change_20d'] = volume_change_20d
        grok_df.at[idx, 'sma_5d'] = sma_5d
        grok_df.at[idx, 'price_vs_sma5_pct'] = price_vs_sma5_pct

    return grok_df


def main() -> int:
    print("=" * 60)
    print("Add Technical Indicators to grok_analysis_merged_20251121")
    print("=" * 60)

    # [STEP 1] データ読み込み
    print("\n[STEP 1] Loading data...")

    if not GROK_ANALYSIS_FILE.exists():
        print(f"  ✗ File not found: {GROK_ANALYSIS_FILE}")
        return 1

    if not PRICES_FILE.exists():
        print(f"  ✗ File not found: {PRICES_FILE}")
        return 1

    grok_df = pd.read_parquet(GROK_ANALYSIS_FILE)
    prices_df = pd.read_parquet(PRICES_FILE)

    print(f"  ✓ Loaded grok_analysis: {len(grok_df)} records, {len(grok_df.columns)} columns")
    print(f"  ✓ Loaded prices: {len(prices_df)} records, {prices_df['ticker'].nunique()} tickers")

    # [STEP 2] テクニカル指標計算
    print("\n[STEP 2] Calculating technical indicators...")
    grok_df_with_indicators = add_technical_indicators(grok_df, prices_df)

    # [STEP 3] 統計情報
    print("\n[STEP 3] Statistics...")
    print(f"  rsi_14d: {grok_df_with_indicators['rsi_14d'].notna().sum()}/{len(grok_df_with_indicators)} valid")
    print(f"  volume_change_20d: {grok_df_with_indicators['volume_change_20d'].notna().sum()}/{len(grok_df_with_indicators)} valid")
    print(f"  sma_5d: {grok_df_with_indicators['sma_5d'].notna().sum()}/{len(grok_df_with_indicators)} valid")
    print(f"  price_vs_sma5_pct: {grok_df_with_indicators['price_vs_sma5_pct'].notna().sum()}/{len(grok_df_with_indicators)} valid")

    # [STEP 4] 保存
    print("\n[STEP 4] Saving...")
    grok_df_with_indicators.to_parquet(OUTPUT_FILE, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {OUTPUT_FILE}")
    print(f"  Columns: {len(grok_df_with_indicators.columns)} ({len(grok_df.columns)} → {len(grok_df_with_indicators.columns)})")

    print("\n✅ Technical indicators added successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
