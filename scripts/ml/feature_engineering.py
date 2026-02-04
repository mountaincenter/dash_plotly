#!/usr/bin/env python3
"""
feature_engineering.py
grok_trending_archive + 日足データ + 市場データから特徴量を作成

=== データソース ===
- grok_trending_archive.parquet: 学習データ（目的変数 phase2_win 含む）
- grok_prices_max_1d.parquet: 銘柄個別の日足（yfinanceから取得済み）
- index_prices_max_1d.parquet: 日経225(^N225), TOPIX ETF(1306.T)
- futures_prices_max_1d.parquet: 日経先物(NKD=F)
- currency_prices_max_1d.parquet: ドル円(JPY=X)

=== 目的変数の定義 ===
- phase2_win = True: 終値 > 始値（株価上昇 = ロング利益）
- phase2_win = False: 終値 < 始値（株価下落 = ショート利益）

※ ショート戦略では phase2_win=False を狙う
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

# 市場データパス
INDEX_PRICES_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
FUTURES_PRICES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"
CURRENCY_PRICES_PATH = PARQUET_DIR / "currency_prices_max_1d.parquet"

# 市場指標のticker
MARKET_TICKERS = {
    'nikkei': '^N225',
    'topix': '1306.T',
    'futures': 'NKD=F',
    'usdjpy': 'JPY=X',
}


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    """データ読み込み"""
    print("[INFO] Loading data...")

    archive = pd.read_parquet(ARCHIVE_PATH)
    print(f"  Archive: {len(archive)} rows")

    prices = pd.read_parquet(PRICES_PATH)
    prices['date'] = pd.to_datetime(prices['date'])
    print(f"  Prices: {len(prices):,} rows, {prices['ticker'].nunique()} tickers")

    # 市場データ読み込み
    market_data = load_market_data()

    return archive, prices, market_data


def load_market_data() -> dict[str, pd.DataFrame]:
    """市場データ（日経、TOPIX、先物、為替）を読み込み"""
    print("[INFO] Loading market data...")

    market_data = {}

    # Index (日経225, TOPIX ETF)
    if INDEX_PRICES_PATH.exists():
        idx_df = pd.read_parquet(INDEX_PRICES_PATH)
        idx_df['date'] = pd.to_datetime(idx_df['date'])

        for key, ticker in [('nikkei', '^N225'), ('topix', '1306.T')]:
            df = idx_df[idx_df['ticker'] == ticker].copy()
            df = df.sort_values('date').reset_index(drop=True)
            market_data[key] = df
            print(f"  {key} ({ticker}): {len(df):,} rows")

    # Futures (日経先物)
    if FUTURES_PRICES_PATH.exists():
        fut_df = pd.read_parquet(FUTURES_PRICES_PATH)
        fut_df['date'] = pd.to_datetime(fut_df['date'])
        df = fut_df[fut_df['ticker'] == 'NKD=F'].copy()
        df = df.sort_values('date').reset_index(drop=True)
        market_data['futures'] = df
        print(f"  futures (NKD=F): {len(df):,} rows")

    # Currency (ドル円)
    if CURRENCY_PRICES_PATH.exists():
        cur_df = pd.read_parquet(CURRENCY_PRICES_PATH)
        cur_df['date'] = pd.to_datetime(cur_df['date'])
        df = cur_df[cur_df['ticker'] == 'JPY=X'].copy()
        df = df.sort_values('date').reset_index(drop=True)
        market_data['usdjpy'] = df
        print(f"  usdjpy (JPY=X): {len(df):,} rows")

    return market_data


def calc_market_features(
    target_date: pd.Timestamp,
    market_data: dict[str, pd.DataFrame],
) -> dict:
    """
    市場指標の特徴量を計算

    Args:
        target_date: 対象日付（この日より前のデータのみ使用）
        market_data: 市場データ辞書

    Returns:
        特徴量辞書
    """
    features = {}

    for key in ['nikkei', 'topix', 'futures', 'usdjpy']:
        if key not in market_data:
            # データなしの場合はNaN
            features[f'{key}_vol_5d'] = np.nan
            features[f'{key}_ret_5d'] = np.nan
            features[f'{key}_ma5_dev'] = np.nan
            continue

        df = market_data[key]
        df_past = df[df['date'] < target_date].tail(30)

        if len(df_past) < 5:
            features[f'{key}_vol_5d'] = np.nan
            features[f'{key}_ret_5d'] = np.nan
            features[f'{key}_ma5_dev'] = np.nan
            continue

        closes = df_past['Close'].values

        # 5日ボラティリティ
        returns = np.diff(closes) / closes[:-1]
        features[f'{key}_vol_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan

        # 5日リターン
        features[f'{key}_ret_5d'] = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else np.nan

        # MA5乖離率
        ma5 = np.mean(closes[-5:])
        features[f'{key}_ma5_dev'] = (closes[-1] - ma5) / ma5 * 100

    return features


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


def create_features(
    archive_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    market_data: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    archive全行に対して特徴量を追加

    Args:
        archive_df: grok_trending_archive
        prices_df: grok_prices_max_1d
        market_data: 市場データ辞書

    Returns:
        特徴量追加済みDataFrame
    """
    print("\n[INFO] Creating features...")

    # backtest_dateをdatetimeに変換
    archive_df = archive_df.copy()
    archive_df['backtest_date'] = pd.to_datetime(archive_df['backtest_date'])

    # 市場特徴量を日付ごとにキャッシュ
    market_features_cache = {}

    feature_records = []
    total = len(archive_df)

    for idx, row in archive_df.iterrows():
        ticker = row['ticker']
        target_date = row['backtest_date']

        # 価格ベース特徴量（銘柄個別）
        price_features = calc_price_features(ticker, target_date, prices_df)

        # 市場特徴量（日付ごとにキャッシュ）
        date_key = target_date.strftime('%Y-%m-%d')
        if date_key not in market_features_cache:
            market_features_cache[date_key] = calc_market_features(target_date, market_data)
        market_features = market_features_cache[date_key]

        # 結合
        all_features = {**price_features, **market_features}
        feature_records.append(all_features)

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

    # 新規作成した特徴量（銘柄個別）
    price_features = [
        'volatility_5d', 'volatility_10d', 'volatility_20d',
        'ma5_deviation', 'ma25_deviation',
        'prev_day_return', 'return_5d', 'return_10d',
        'volume_ratio_5d', 'price_range_5d'
    ]

    # 市場特徴量（日経、TOPIX、先物、ドル円）
    market_features = []
    for key in ['nikkei', 'topix', 'futures', 'usdjpy']:
        market_features.extend([
            f'{key}_vol_5d',
            f'{key}_ret_5d',
            f'{key}_ma5_dev',
        ])

    return existing_features + price_features + market_features


def main():
    """メイン処理"""
    print("=" * 60)
    print("Feature Engineering for ML Prediction")
    print("=" * 60)

    # データ読み込み
    archive, prices, market_data = load_data()

    # 特徴量作成
    df_with_features = create_features(archive, prices, market_data)

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
