#!/usr/bin/env python3
"""
update_granville_prices.py
TOPIX 1,660銘柄の日足データを差分更新（yfinance period="5d"）

prices_max_1d.parquet の最終日付以降のデータを取得し追記する。
GHA pipeline (18:00 JST) で実行。
"""
from __future__ import annotations

import sys
import time
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

_SPLIT_RATIOS: dict[str, float] = {}

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
PRICES_PATH = GRANVILLE_DIR / "prices_topix.parquet"
BATCH_SIZE = 50
SLEEP_BETWEEN = 2


def load_existing() -> pd.DataFrame:
    """既存の prices_topix.parquet を読み込み"""
    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)
    if not PRICES_PATH.exists():
        # S3フォールバック
        try:
            cfg = load_s3_config()
            if cfg and cfg.bucket:
                from common_cfg.s3io import download_file
                download_file(cfg, "granville/prices_topix.parquet", PRICES_PATH)
        except Exception:
            pass
    if not PRICES_PATH.exists():
        print(f"[ERROR] {PRICES_PATH} not found")
        sys.exit(1)
    df = pd.read_parquet(PRICES_PATH)
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_batch(tickers: list[str], period: str = "5d") -> pd.DataFrame:
    """yfinance でバッチ取得"""
    yf_tickers = [t.replace("_", ".") for t in tickers]
    try:
        df = yf.download(yf_tickers, period=period, interval="1d", progress=False, threads=True)
    except Exception as e:
        print(f"  [WARN] Batch failed: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    rows = []
    if isinstance(df.columns, pd.MultiIndex):
        for i, t_yf in enumerate(yf_tickers):
            t_orig = tickers[i]
            try:
                sub = df.xs(t_yf, level=1, axis=1).copy()
                sub = sub.dropna(subset=["Close"])
                if sub.empty:
                    continue
                sub["ticker"] = t_orig
                sub["date"] = sub.index
                sub = sub.reset_index(drop=True)
                rows.append(sub[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]])
            except Exception:
                pass
    else:
        df = df.dropna(subset=["Close"])
        if not df.empty:
            df["ticker"] = tickers[0]
            df["date"] = df.index
            df = df.reset_index(drop=True)
            rows.append(df[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]])

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _calculate_split_ratios(existing: pd.DataFrame, new_df: pd.DataFrame) -> dict[str, float]:
    """最新データと既存データを比較し、終値比率を返す"""
    if existing.empty or new_df.empty:
        return {}

    existing_last = (
        existing.sort_values("date")
        .dropna(subset=["Close"])
        .groupby("ticker")["Close"]
        .last()
    )
    new_first = (
        new_df.sort_values("date")
        .dropna(subset=["Close"])
        .groupby("ticker")["Close"]
        .first()
    )

    ratios: dict[str, float] = {}
    common = existing_last.index.intersection(new_first.index)
    for ticker in common:
        old_close = existing_last[ticker]
        new_close = new_first[ticker]
        if pd.isna(old_close) or pd.isna(new_close) or old_close == 0:
            continue
        ratios[ticker] = float(new_close) / float(old_close)
    return ratios


def detect_splits(existing: pd.DataFrame, new_df: pd.DataFrame) -> list[str]:
    """終値ギャップから株式分割の可能性がある銘柄を検出"""
    global _SPLIT_RATIOS
    _SPLIT_RATIOS = _calculate_split_ratios(existing, new_df)
    return [t for t, ratio in _SPLIT_RATIOS.items() if ratio < 0.6 or ratio > 1.8]


def refetch_full_history(tickers: list[str]) -> pd.DataFrame:
    """株式分割が疑われる銘柄を最大期間で再取得"""
    if not tickers:
        return pd.DataFrame()

    all_rows: list[pd.DataFrame] = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        df = fetch_batch(batch, period="max")
        if not df.empty:
            all_rows.append(df)
        time.sleep(SLEEP_BETWEEN)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def main() -> int:
    print("=" * 60)
    print("Update Granville Prices (TOPIX daily differential)")
    print("=" * 60)

    print("\n[1/3] Loading existing prices...")
    existing = load_existing()
    tickers = existing["ticker"].unique().tolist()
    last_date = existing["date"].max()
    print(f"  {len(tickers)} tickers, last date: {last_date.date()}")

    print(f"\n[2/3] Fetching 5d data for {len(tickers)} tickers...")
    all_new = []
    failed = 0
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        df = fetch_batch(batch)
        if not df.empty:
            all_new.append(df)
        else:
            failed += len(batch)
        if i > 0 and i % (BATCH_SIZE * 10) == 0:
            print(f"  {min(i + BATCH_SIZE, len(tickers))}/{len(tickers)} processed")
        time.sleep(SLEEP_BETWEEN)

    if not all_new:
        print("[ERROR] No data fetched")
        return 1

    new_df = pd.concat(all_new, ignore_index=True)
    new_df["date"] = pd.to_datetime(new_df["date"]).dt.tz_localize(None)

    # per-tickerの最終日以降のみ追加（J-Quants置換で銘柄ごとに最終日が異なる場合に対応）
    ticker_last = existing.groupby("ticker")["date"].max()
    new_df = new_df[new_df.apply(lambda r: r["date"] > ticker_last.get(r["ticker"], last_date), axis=1)]
    new_dates = sorted(new_df["date"].unique())
    print(f"  New rows: {len(new_df)}")
    print(f"  New dates: {[d.strftime('%Y-%m-%d') for d in pd.to_datetime(new_dates)]}")

    if new_df.empty:
        print("[INFO] Already up to date. No new data.")
        return 0

    # クリーニング: Volume=0 かつ OHLC全同値の行は除外（休場データ）
    before = len(new_df)
    new_df = new_df[~(
        (new_df["Volume"] == 0) &
        (new_df["Open"] == new_df["Close"]) &
        (new_df["High"] == new_df["Close"]) &
        (new_df["Low"] == new_df["Close"])
    )]
    if len(new_df) < before:
        print(f"  Cleaned: {before - len(new_df)} zero-volume rows removed")

    split_tickers = detect_splits(existing, new_df)
    ratios = _SPLIT_RATIOS
    if split_tickers:
        for ticker in split_tickers:
            ratio = ratios.get(ticker, float("nan"))
            print(f"[SPLIT] Detected split for {ticker}: ratio={ratio:.3f}, re-fetching full history...")
        refreshed = refetch_full_history(split_tickers)
        if refreshed.empty:
            print("  [WARN] Split refetch returned no data; keeping existing rows")
        else:
            refreshed["date"] = pd.to_datetime(refreshed["date"]).dt.tz_localize(None)
            # 実際に取得できた銘柄だけ差し替え（取得失敗銘柄の既存データは保持）
            fetched_tickers = set(refreshed["ticker"].unique())
            missing = set(split_tickers) - fetched_tickers
            if missing:
                print(f"  [WARN] Refetch failed for {len(missing)} tickers, keeping existing: {sorted(missing)[:5]}")
            existing = existing[~existing["ticker"].isin(fetched_tickers)].reset_index(drop=True)
            new_df = new_df[~new_df["ticker"].isin(fetched_tickers)].reset_index(drop=True)
            existing = pd.concat([existing, refreshed], ignore_index=True)

    print(f"\n[3/3] Merging and saving...")
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    combined.to_parquet(PRICES_PATH, index=False)
    print(f"  Saved: {PRICES_PATH.name}")
    print(f"  {combined['ticker'].nunique()} tickers, {len(combined):,} rows")
    print(f"  Date range: {combined['date'].min().date()} ~ {combined['date'].max().date()}")

    # S3アップロード
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, PRICES_PATH, "granville/prices_topix.parquet")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    if failed > 0:
        print(f"\n  [WARN] {failed} tickers failed to fetch")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
