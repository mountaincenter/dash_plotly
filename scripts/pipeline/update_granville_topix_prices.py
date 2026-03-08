#!/usr/bin/env python3
"""
update_granville_topix_prices.py
TOPIX 1,660銘柄の日次差分価格更新

1. prices_topix.parquet の最終日付を取得
2. yfinance period="5d" で直近分を50銘柄バッチで取得
3. クリーニング適用（Volume=0除外、NaN除外、異常リターン検出）
4. 既存parquetに追記 → data/parquet/granville/prices_topix.parquet
5. S3にアップロード

初回: prices_cleaned_topix_v3.parquet → prices_topix.parquet にコピー
"""
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
OUTPUT_PATH = GRANVILLE_DIR / "prices_topix.parquet"
S3_KEY = "granville/prices_topix.parquet"

# 初期データソース（検証時に構築済み）
SEED_PATH = ROOT / "strategy_verification" / "data" / "processed" / "prices_cleaned_topix_v3.parquet"

BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2
EXTREME_RET_THRESHOLD = 50.0  # 日次リターン ±50% 超は異常


def fetch_batch(tickers: list[str], period: str = "5d") -> pd.DataFrame:
    """yfinance batch download → 統一スキーマに変換"""
    if not tickers:
        return pd.DataFrame()

    raw = yf.download(
        tickers,
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True,
        progress=False,
    )

    frames: list[pd.DataFrame] = []
    if len(tickers) == 1:
        t = tickers[0]
        df = raw[["Open", "High", "Low", "Close", "Volume"]].copy()
        df["ticker"] = t
        df = df.reset_index()
        df.columns = [c if c not in ("Date", "Price") else "date" for c in df.columns]
        if "date" not in df.columns:
            df = df.rename(columns={df.columns[0]: "date"})
        df = df.dropna(subset=["Close"])
        frames.append(df)
    else:
        for t in tickers:
            try:
                if t not in raw.columns.get_level_values(0):
                    continue
                df = raw[t][["Open", "High", "Low", "Close", "Volume"]].copy()
                df["ticker"] = t
                df = df.reset_index()
                df.columns = [c if c not in ("Date", "Price") else "date" for c in df.columns]
                if "date" not in df.columns:
                    df = df.rename(columns={df.columns[0]: "date"})
                df = df.dropna(subset=["Close"])
                frames.append(df)
            except Exception as e:
                print(f"  SKIP {t}: {e}")

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"]).dt.tz_localize(None)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        result[col] = result[col].astype(float)
    return result[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]]


def clean_new_data(df: pd.DataFrame) -> pd.DataFrame:
    """日次差分データのクリーニング（13_expand_universe.py準拠）"""
    n0 = len(df)

    # Volume=0 除外
    df = df[df["Volume"] > 0].copy()

    # NaN/Inf 除外
    price_cols = ["Open", "High", "Low", "Close"]
    mask_nan = df[price_cols].isna().any(axis=1) | np.isinf(df[price_cols].values).any(axis=1)
    df = df[~mask_nan].copy()

    # 負の価格除外
    mask_neg = (df[price_cols] < 0).any(axis=1)
    df = df[~mask_neg].copy()

    removed = n0 - len(df)
    if removed > 0:
        print(f"  Cleaned: {removed} rows removed ({n0} → {len(df)})")

    return df


def detect_extreme_returns(existing: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """既存データの末尾と新データを結合して異常リターンを検出・除外"""
    if new_data.empty:
        return new_data

    # 各ticker の既存データ最終行を取得（prev_close計算用）
    tickers_new = new_data["ticker"].unique()
    last_rows = (
        existing[existing["ticker"].isin(tickers_new)]
        .sort_values(["ticker", "date"])
        .groupby("ticker")
        .tail(1)[["ticker", "Close"]]
        .rename(columns={"Close": "prev_close_seed"})
    )

    new_sorted = new_data.sort_values(["ticker", "date"]).copy()
    new_sorted["prev_close"] = new_sorted.groupby("ticker")["Close"].shift(1)

    # 最初の行には既存データの最終Closeを使う
    new_sorted = new_sorted.merge(last_rows, on="ticker", how="left")
    mask_first = new_sorted["prev_close"].isna()
    new_sorted.loc[mask_first, "prev_close"] = new_sorted.loc[mask_first, "prev_close_seed"]
    new_sorted.drop(columns=["prev_close_seed"], inplace=True)

    new_sorted["daily_ret"] = np.where(
        new_sorted["prev_close"] > 0,
        (new_sorted["Close"] / new_sorted["prev_close"] - 1) * 100,
        np.nan,
    )

    extreme = new_sorted["daily_ret"].abs() > EXTREME_RET_THRESHOLD
    n_extreme = int(extreme.sum())
    if n_extreme > 0:
        print(f"  ⚠️ Extreme returns detected: {n_extreme} rows (removed)")
        for _, row in new_sorted[extreme].iterrows():
            print(f"    {row['ticker']} {row['date'].date()} ret={row['daily_ret']:.1f}%")

    result = new_sorted[~extreme][["date", "Open", "High", "Low", "Close", "Volume", "ticker"]].copy()
    return result


def main() -> int:
    print("=" * 60)
    print("Update Granville TOPIX Prices")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)

    # 初回: seed parquet からコピー
    if not OUTPUT_PATH.exists():
        if not SEED_PATH.exists():
            print(f"[ERROR] Seed data not found: {SEED_PATH}")
            print("  Run strategy_verification/scripts/13_expand_universe.py first")
            return 1
        print(f"[INIT] Copying seed data: {SEED_PATH.name}")
        seed = pd.read_parquet(SEED_PATH)
        seed["date"] = pd.to_datetime(seed["date"])
        # segment列があれば除外（prices_topixには不要）
        keep_cols = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
        seed = seed[[c for c in keep_cols if c in seed.columns]]
        seed.to_parquet(OUTPUT_PATH, index=False)
        print(f"  {len(seed):,} rows, {seed['ticker'].nunique()} tickers → {OUTPUT_PATH.name}")

    # 既存データ読み込み
    print("\n[1] Loading existing data...")
    existing = pd.read_parquet(OUTPUT_PATH)
    existing["date"] = pd.to_datetime(existing["date"])
    last_date = existing["date"].max()
    tickers = sorted(existing["ticker"].unique().tolist())
    print(f"  {len(existing):,} rows, {len(tickers)} tickers, last date: {last_date.date()}")

    # 最終日が今日なら更新不要
    today = pd.Timestamp.now().normalize()
    if last_date >= today:
        print(f"[INFO] Already up to date (last_date={last_date.date()} >= today={today.date()})")
        return 0

    # yfinanceで差分取得
    print(f"\n[2] Fetching updates via yfinance (period=5d)...")
    total = len(tickers)
    all_new: list[pd.DataFrame] = []

    for i in range(0, total, BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1}/{(total + BATCH_SIZE - 1) // BATCH_SIZE} "
              f"({len(batch)} tickers)")
        df = fetch_batch(batch, period="5d")
        if not df.empty:
            all_new.append(df)
        if i + BATCH_SIZE < total:
            time.sleep(SLEEP_BETWEEN_BATCHES)

    if not all_new:
        print("[WARN] No new data fetched")
        return 0

    new_data = pd.concat(all_new, ignore_index=True)
    print(f"  Fetched: {len(new_data):,} rows")

    # 既存データとの重複排除（last_dateより後のみ）
    new_data = new_data[new_data["date"] > last_date].copy()
    print(f"  After dedup (> {last_date.date()}): {len(new_data):,} rows")

    if new_data.empty:
        print("[INFO] No new data after dedup")
        return 0

    # クリーニング
    print("\n[3] Cleaning...")
    new_data = clean_new_data(new_data)
    new_data = detect_extreme_returns(existing, new_data)

    if new_data.empty:
        print("[INFO] No data remaining after cleaning")
        return 0

    # 追記
    print(f"\n[4] Appending {len(new_data):,} rows...")
    merged = pd.concat([existing, new_data], ignore_index=True)
    merged = merged.sort_values(["ticker", "date"]).reset_index(drop=True)
    merged.to_parquet(OUTPUT_PATH, index=False)

    new_last = merged["date"].max()
    print(f"  Total: {len(merged):,} rows, last date: {new_last.date()}")

    # S3アップロード
    print("\n[5] Uploading to S3...")
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, OUTPUT_PATH, S3_KEY)
        else:
            print("  [INFO] S3 bucket not configured, skipping upload")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"Updated: {last_date.date()} → {new_last.date()}")
    print(f"New rows: {len(new_data):,}")
    print(f"Total: {len(merged):,} rows, {merged['ticker'].nunique()} tickers")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
