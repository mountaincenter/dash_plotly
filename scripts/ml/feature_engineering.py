#!/usr/bin/env python3
"""
feature_engineering.py
grok_trending_archive + 日足データから特徴量を作成
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from typing import Optional
from common_cfg.paths import PARQUET_DIR

ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
PRICES_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """データ読み込み"""
    print("[INFO] Loading data...")

    archive = pd.read_parquet(ARCHIVE_PATH)
    print(f"  Archive: {len(archive)} rows")

    prices = pd.read_parquet(PRICES_PATH)
    prices['date'] = pd.to_datetime(prices['date'])
    print(f"  Prices: {len(prices):,} rows, {prices['ticker'].nunique()} tickers")

    return archive, prices


def calc_price_features(
    ticker: str,
    target_date: pd.Timestamp,
    prices_df: pd.DataFrame,
    lookback_days: int = 60
) -> dict:
    """
    指定銘柄・日付の価格ベース特徴量を計算

    Args:
        ticker: 銘柄コード
        target_date: 対象日付（この日より前のデータのみ使用）
        prices_df: 日足データ（全銘柄）
        lookback_days: 遡る日数

    Returns:
        特徴量辞書
    """
    # 対象銘柄のデータを抽出（target_dateより前のみ）
    ticker_prices = prices_df[
        (prices_df['ticker'] == ticker) &
        (prices_df['date'] < target_date)
    ].sort_values('date').tail(lookback_days)

    features = {}

    if len(ticker_prices) < 5:
        # データ不足時はNaN
        return {
            'volatility_5d': np.nan,
            'volatility_10d': np.nan,
            'volatility_20d': np.nan,
            'ma5_deviation': np.nan,
            'ma25_deviation': np.nan,
            'prev_day_return': np.nan,
            'return_5d': np.nan,
            'return_10d': np.nan,
            'volume_ratio_5d': np.nan,
            'price_range_5d': np.nan,
        }

    closes = ticker_prices['Close'].values
    volumes = ticker_prices['Volume'].values
    highs = ticker_prices['High'].values
    lows = ticker_prices['Low'].values

    # ボラティリティ（日次リターンの標準偏差）
    returns = np.diff(closes) / closes[:-1]
    features['volatility_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan
    features['volatility_10d'] = np.std(returns[-10:]) * 100 if len(returns) >= 10 else np.nan
    features['volatility_20d'] = np.std(returns[-20:]) * 100 if len(returns) >= 20 else np.nan

    # 移動平均乖離率
    ma5 = np.mean(closes[-5:])
    ma25 = np.mean(closes[-25:]) if len(closes) >= 25 else np.nan
    prev_close = closes[-1]

    features['ma5_deviation'] = (prev_close - ma5) / ma5 * 100
    features['ma25_deviation'] = (prev_close - ma25) / ma25 * 100 if not np.isnan(ma25) else np.nan

    # 前日リターン
    if len(closes) >= 2:
        features['prev_day_return'] = (closes[-1] - closes[-2]) / closes[-2] * 100
    else:
        features['prev_day_return'] = np.nan

    # N日リターン
    features['return_5d'] = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else np.nan
    features['return_10d'] = (closes[-1] - closes[-10]) / closes[-10] * 100 if len(closes) >= 10 else np.nan

    # 出来高比（直近5日平均との比較）
    if len(volumes) >= 5:
        avg_vol_5d = np.mean(volumes[-5:])
        features['volume_ratio_5d'] = volumes[-1] / avg_vol_5d if avg_vol_5d > 0 else np.nan
    else:
        features['volume_ratio_5d'] = np.nan

    # 前日終値（archiveに既にあるのでスキップ）
    # features['prev_close'] = prev_close

    # 5日間の価格レンジ（%）
    if len(highs) >= 5 and len(lows) >= 5:
        high_5d = np.max(highs[-5:])
        low_5d = np.min(lows[-5:])
        features['price_range_5d'] = (high_5d - low_5d) / low_5d * 100 if low_5d > 0 else np.nan
    else:
        features['price_range_5d'] = np.nan

    return features


def create_features(archive_df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    archive全行に対して特徴量を追加

    Args:
        archive_df: grok_trending_archive
        prices_df: grok_prices_max_1d

    Returns:
        特徴量追加済みDataFrame
    """
    print("\n[INFO] Creating features...")

    # backtest_dateをdatetimeに変換
    archive_df = archive_df.copy()
    archive_df['backtest_date'] = pd.to_datetime(archive_df['backtest_date'])

    feature_records = []
    total = len(archive_df)

    for idx, row in archive_df.iterrows():
        ticker = row['ticker']
        target_date = row['backtest_date']

        # 価格ベース特徴量
        price_features = calc_price_features(ticker, target_date, prices_df)
        feature_records.append(price_features)

        # 進捗表示
        if (idx + 1) % 100 == 0 or idx == total - 1:
            print(f"  Progress: {idx + 1}/{total} ({(idx + 1) / total * 100:.1f}%)")

    # 特徴量をDataFrameに変換して結合
    features_df = pd.DataFrame(feature_records)
    result_df = pd.concat([archive_df.reset_index(drop=True), features_df], axis=1)

    print(f"\n✓ Created {len(features_df.columns)} new features")
    return result_df


def get_feature_columns() -> list[str]:
    """特徴量カラム一覧を返す"""
    # 既存特徴量（archiveにあるもの）
    existing_features = [
        'grok_rank', 'selection_score', 'buy_price', 'market_cap',
        'atr14_pct', 'vol_ratio', 'rsi9', 'weekday',
        'nikkei_change_pct', 'futures_change_pct',
        'shortable', 'day_trade'
    ]

    # 新規作成した特徴量
    new_features = [
        'volatility_5d', 'volatility_10d', 'volatility_20d',
        'ma5_deviation', 'ma25_deviation',
        'prev_day_return', 'return_5d', 'return_10d',
        'volume_ratio_5d', 'price_range_5d'
    ]

    return existing_features + new_features


def main():
    """メイン処理"""
    print("=" * 60)
    print("Feature Engineering for ML Prediction")
    print("=" * 60)

    # データ読み込み
    archive, prices = load_data()

    # 特徴量作成
    df_with_features = create_features(archive, prices)

    # 保存
    output_path = PARQUET_DIR / "ml" / "archive_with_features.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_with_features.to_parquet(output_path, index=False)

    print(f"\n✓ Saved: {output_path}")
    print(f"  - Rows: {len(df_with_features)}")
    print(f"  - Columns: {len(df_with_features.columns)}")

    # 特徴量のサマリー
    feature_cols = get_feature_columns()
    print(f"\n[Feature Summary]")
    for col in feature_cols:
        if col in df_with_features.columns:
            non_null = df_with_features[col].notna().sum()
            print(f"  {col}: {non_null}/{len(df_with_features)} ({non_null/len(df_with_features)*100:.1f}%)")


if __name__ == "__main__":
    main()
