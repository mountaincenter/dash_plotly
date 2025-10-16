#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scalping_lists.py
- Generate scalping watchlists from existing price data
- Output: scalping_entry.parquet, scalping_active.parquet
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR, MASTER_META_PARQUET
from common_cfg.s3cfg import DATA_BUCKET, PARQUET_PREFIX, AWS_REGION, AWS_PROFILE
from common_cfg.s3io import maybe_upload_files_s3

load_dotenv_cascade()

# ==== Paths ====
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
SCALPING_ENTRY_PATH = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE_PATH = PARQUET_DIR / "scalping_active.parquet"


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate technical indicators for scalping selection"""
    df = df.sort_values(['ticker', 'date']).copy()

    # Previous close
    df['prevClose'] = df.groupby('ticker')['Close'].shift(1)

    # Change %
    df['change_pct'] = ((df['Close'] - df['prevClose']) / df['prevClose'] * 100).round(2)

    # True Range and ATR(14)
    hl = df['High'] - df['Low']
    hp = (df['High'] - df['prevClose']).abs()
    lp = (df['Low'] - df['prevClose']).abs()
    df['tr'] = pd.concat([hl, hp, lp], axis=1).max(axis=1)

    df['atr14'] = (
        df.groupby('ticker', group_keys=False)['tr']
        .apply(lambda s: s.ewm(span=14, adjust=False).mean())
    )

    df['atr14_pct'] = (df['atr14'] / df['Close'] * 100.0).round(2)

    # Moving Averages
    df['ma5'] = df.groupby('ticker', group_keys=False)['Close'].transform(
        lambda x: x.rolling(window=5, min_periods=1).mean()
    )
    df['ma25'] = df.groupby('ticker', group_keys=False)['Close'].transform(
        lambda x: x.rolling(window=25, min_periods=1).mean()
    )

    # RSI(14)
    def calculate_rsi(series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    df['rsi14'] = df.groupby('ticker', group_keys=False)['Close'].transform(
        lambda x: calculate_rsi(x, 14)
    ).round(2)

    # Volume MA(10) and ratio
    if 'Volume' in df.columns:
        df['vol_ma10'] = df.groupby('ticker', group_keys=False)['Volume'].transform(
            lambda x: x.rolling(window=10, min_periods=1).mean()
        )
        df['vol_ratio'] = (df['Volume'] / df['vol_ma10'] * 100).round(2)
    else:
        df['Volume'] = np.nan
        df['vol_ma10'] = np.nan
        df['vol_ratio'] = np.nan

    return df


def generate_entry_list(df_latest: pd.DataFrame, meta_df: pd.DataFrame) -> pd.DataFrame:
    """Generate entry scalping list (beginner-friendly)"""
    print("[INFO] Generating entry list...")

    df_entry = df_latest[
        (df_latest['Close'] >= 100) &
        (df_latest['Close'] <= 1500) &
        (df_latest['Volume'] * df_latest['Close'] >= 100_000_000) &
        (df_latest['atr14_pct'] >= 1.0) &
        (df_latest['atr14_pct'] <= 3.5) &
        (df_latest['change_pct'] >= -3.0) &
        (df_latest['change_pct'] <= 3.0)
    ].copy()

    if df_entry.empty:
        print("[WARN] No stocks met entry criteria")
        return pd.DataFrame()

    # Calculate score
    df_entry['score'] = 50.0  # Base score

    # Price appropriateness (300-800 is ideal)
    df_entry['score'] += df_entry['Close'].apply(
        lambda p: 30 if 300 <= p <= 800 else 15
    )

    # Volume stability
    df_entry['score'] += df_entry['vol_ratio'].apply(
        lambda v: 25 if 90 <= v <= 130 else 10
    )

    # Tags
    def get_tags_entry(row):
        tags = []
        if not pd.isna(row['ma5']) and not pd.isna(row['ma25']) and row['ma5'] > row['ma25']:
            tags.append('trend')
        if not pd.isna(row['rsi14']):
            if row['rsi14'] < 40:
                tags.append('oversold')
            elif row['rsi14'] > 60:
                tags.append('overbought')
        if not pd.isna(row['vol_ratio']) and 95 <= row['vol_ratio'] <= 120:
            tags.append('stable_volume')
        return tags

    df_entry['tags'] = df_entry.apply(get_tags_entry, axis=1)

    # Key signal
    df_entry['key_signal'] = df_entry.apply(
        lambda r: f"¥{r['Close']:.0f} | {r['change_pct']:+.1f}% | Vol {r['vol_ratio']:.0f}% | ATR {r['atr14_pct']:.1f}%",
        axis=1
    )

    # Merge meta
    df_entry = df_entry.merge(
        meta_df[['ticker', 'stock_name', 'market', 'sectors']],
        on='ticker',
        how='left'
    )

    # Sort by score and select top 20
    df_entry = df_entry.sort_values('score', ascending=False).drop_duplicates(subset=['ticker'], keep='first').head(20)

    # Select columns
    entry_cols = [
        'ticker', 'stock_name', 'market', 'sectors', 'date',
        'Close', 'change_pct', 'Volume', 'vol_ratio',
        'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
    ]
    df_entry = df_entry[[c for c in entry_cols if c in df_entry.columns]]

    print(f"[OK] Entry list: {len(df_entry)} stocks")
    return df_entry


def generate_active_list(df_latest: pd.DataFrame, meta_df: pd.DataFrame, entry_tickers: set) -> pd.DataFrame:
    """Generate active scalping list (challenging)"""
    print("[INFO] Generating active list...")

    df_active = df_latest[
        ~df_latest['ticker'].isin(entry_tickers) &
        (df_latest['Close'] >= 100) &
        (df_latest['Close'] <= 3000) &
        ((df_latest['Volume'] * df_latest['Close'] >= 50_000_000) | (df_latest['vol_ratio'] >= 150)) &
        (df_latest['atr14_pct'] >= 2.5) &
        (df_latest['change_pct'].abs() >= 2.0)
    ].copy()

    if df_active.empty:
        print("[WARN] No stocks met active criteria")
        return pd.DataFrame()

    # Calculate score
    df_active['score'] = 50.0  # Base score

    # Change score (bigger is better)
    df_active['score'] += df_active['change_pct'].apply(
        lambda c: min(35, abs(c) / 7.0 * 35)
    )

    # Volume surge score
    df_active['score'] += df_active['vol_ratio'].apply(
        lambda v: 30 if v >= 200 else max(0, v / 150 * 30)
    )

    # Tags
    def get_tags_active(row):
        tags = []
        if not pd.isna(row['ma5']) and not pd.isna(row['ma25']) and row['ma5'] > row['ma25']:
            tags.append('trend')
        if not pd.isna(row['rsi14']):
            if row['rsi14'] < 30:
                tags.append('oversold')
            elif row['rsi14'] > 70:
                tags.append('overbought')
        if not pd.isna(row['vol_ratio']) and row['vol_ratio'] >= 200:
            tags.append('volume_surge')
        return tags

    df_active['tags'] = df_active.apply(get_tags_active, axis=1)

    # Key signal
    df_active['key_signal'] = df_active.apply(
        lambda r: f"¥{r['Close']:.0f} | {r['change_pct']:+.1f}% | Vol {r['vol_ratio']:.0f}% | ATR {r['atr14_pct']:.1f}%",
        axis=1
    )

    # Merge meta
    df_active = df_active.merge(
        meta_df[['ticker', 'stock_name', 'market', 'sectors']],
        on='ticker',
        how='left'
    )

    # Sort by score and select top 20
    df_active = df_active.sort_values('score', ascending=False).drop_duplicates(subset=['ticker'], keep='first').head(20)

    # Select columns
    active_cols = [
        'ticker', 'stock_name', 'market', 'sectors', 'date',
        'Close', 'change_pct', 'Volume', 'vol_ratio',
        'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
    ]
    df_active = df_active[[c for c in active_cols if c in df_active.columns]]

    print(f"[OK] Active list: {len(df_active)} stocks")
    return df_active


def main() -> int:
    print("[INFO] Loading price data...")
    if not PRICES_1D_PATH.exists():
        raise FileNotFoundError(f"Price data not found: {PRICES_1D_PATH}")

    df = pd.read_parquet(PRICES_1D_PATH, engine="pyarrow")
    print(f"[INFO] Loaded {len(df)} rows")

    # Load meta
    meta_df = pd.read_parquet(MASTER_META_PARQUET, engine="pyarrow")

    # Calculate technical indicators
    print("[INFO] Calculating technical indicators...")
    df = calculate_technical_indicators(df)
    print("[INFO] Technical indicators calculated")

    # Extract latest data
    latest_date = df['date'].max()
    df_latest = df[df['date'] == latest_date].copy()
    print(f"[INFO] Latest date: {latest_date}, {len(df_latest)} stocks")

    # Generate entry list
    df_entry = generate_entry_list(df_latest, meta_df)

    # Generate active list (exclude entry tickers)
    entry_tickers = set(df_entry['ticker'].tolist()) if not df_entry.empty else set()
    df_active = generate_active_list(df_latest, meta_df, entry_tickers)

    # Save parquet files
    print("[INFO] Saving parquet files...")
    df_entry.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
    print(f"[OK] Saved: {SCALPING_ENTRY_PATH}")

    df_active.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)
    print(f"[OK] Saved: {SCALPING_ACTIVE_PATH}")

    # Upload to S3
    files_to_upload = [SCALPING_ENTRY_PATH, SCALPING_ACTIVE_PATH]
    maybe_upload_files_s3(
        files_to_upload,
        bucket=DATA_BUCKET,
        prefix=PARQUET_PREFIX,
        aws_region=AWS_REGION,
        aws_profile=AWS_PROFILE,
        dry_run=False
    )
    print(f"[OK] S3 upload done (count={len(files_to_upload)})")

    print("\n=== Summary ===")
    print(f"Entry list: {len(df_entry)} stocks")
    print(f"Active list: {len(df_active)} stocks")
    print("[DONE] Scalping lists generated successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
