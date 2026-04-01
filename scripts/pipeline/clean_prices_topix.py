#!/usr/bin/env python3
"""
clean_prices_topix.py
prices_topix.parquetのyfinance分割未調整データをJ-Quantsで修正

1. 異常値検出（|daily_ret| > 40% or Close < 0）
2. 対象銘柄のJ-Quants調整済み価格を取得
3. J-Quantsデータで置換
4. 修正後のparquetを保存
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"
BACKUP_PATH = PARQUET_DIR / "granville" / "prices_topix_backup.parquet"


def detect_anomalous_tickers(ps: pd.DataFrame) -> set[str]:
    """異常値のある銘柄を検出"""
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = ps.groupby("ticker")
    ps["prev_close"] = g["Close"].shift(1)
    ps["daily_ret"] = (ps["Close"] - ps["prev_close"]) / ps["prev_close"] * 100

    # |daily_ret| > 40%
    anom = ps[ps["daily_ret"].abs() > 40]["ticker"].unique().tolist()

    # 負の価格
    neg = ps[ps["Close"] < 0]["ticker"].unique().tolist()

    # Close=0のデータ
    zero = ps[ps["Close"] == 0]["ticker"].unique().tolist()

    all_bad = set(anom + neg + zero)
    print(f"  Anomalous (|ret|>40%): {len(anom)} tickers")
    print(f"  Negative prices: {len(neg)} tickers")
    print(f"  Zero prices: {len(zero)} tickers")
    print(f"  Total unique: {len(all_bad)} tickers")
    return all_bad


def ticker_to_jquants_code(ticker: str) -> str:
    """1514.T → 15140（J-Quants 5桁コード）"""
    base = ticker.replace(".T", "")
    # 4桁の場合は末尾に0を追加（チェックデジット）
    if len(base) == 4:
        return base + "0"
    return base


def fetch_jquants_prices(tickers: list[str], fetcher: JQuantsFetcher) -> pd.DataFrame:
    """J-Quantsから調整済み価格を取得"""
    all_frames = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        code = ticker_to_jquants_code(ticker)
        if i % 10 == 0 or i == total:
            print(f"  [{i}/{total}] {ticker} (code={code})")

        try:
            df = fetcher.get_prices_daily(code=code)
            if df.empty:
                print(f"    [SKIP] No data for {ticker}")
                continue

            # AdjC (AdjustmentClose) 等を使用
            required = ["Date", "AdjustmentOpen", "AdjustmentHigh",
                        "AdjustmentLow", "AdjustmentClose", "AdjustmentVolume"]
            # v2短縮名が既にマッピングされている
            available = [c for c in required if c in df.columns]
            if "AdjustmentClose" not in df.columns:
                print(f"    [WARN] No AdjC for {ticker}, columns: {df.columns.tolist()}")
                continue

            out = pd.DataFrame({
                "date": pd.to_datetime(df["Date"]),
                "Open": pd.to_numeric(df["AdjustmentOpen"], errors="coerce"),
                "High": pd.to_numeric(df["AdjustmentHigh"], errors="coerce"),
                "Low": pd.to_numeric(df["AdjustmentLow"], errors="coerce"),
                "Close": pd.to_numeric(df["AdjustmentClose"], errors="coerce"),
                "Volume": pd.to_numeric(df.get("AdjustmentVolume", df.get("Volume", 0)), errors="coerce"),
                "ticker": ticker,
            })
            out = out.dropna(subset=["Close"])
            all_frames.append(out)
            time.sleep(0.5)  # レート制限

        except Exception as e:
            print(f"    [ERROR] {ticker}: {e}")
            time.sleep(1.0)
            continue

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


def main() -> int:
    print("=" * 60)
    print("Clean prices_topix.parquet (J-Quants correction)")
    print("=" * 60)

    # 1. 既存データ読み込み
    print("\n[1/5] Loading existing prices...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")
    print(f"  Date range: {ps['date'].min().date()} ~ {ps['date'].max().date()}")

    # 2. 異常値検出
    print("\n[2/5] Detecting anomalies...")
    bad_tickers = detect_anomalous_tickers(ps)

    if not bad_tickers:
        print("[INFO] No anomalies found. Nothing to fix.")
        return 0

    # 3. バックアップ
    print(f"\n[3/5] Backing up to {BACKUP_PATH.name}...")
    ps.to_parquet(BACKUP_PATH, index=False)
    print(f"  Saved backup: {BACKUP_PATH}")

    # 4. J-Quantsからクリーンデータ取得
    print(f"\n[4/5] Fetching J-Quants data for {len(bad_tickers)} tickers...")
    fetcher = JQuantsFetcher()
    jq_data = fetch_jquants_prices(sorted(bad_tickers), fetcher)
    print(f"  J-Quants rows: {len(jq_data):,}")

    if jq_data.empty:
        print("[ERROR] No J-Quants data fetched")
        return 1

    jq_tickers = set(jq_data["ticker"].unique())
    print(f"  J-Quants tickers: {len(jq_tickers)}")

    # 5. データ置換
    print("\n[5/5] Replacing data...")
    # 対象銘柄のyfinanceデータを除去し、J-Quantsデータで置換
    ps_clean = ps[~ps["ticker"].isin(jq_tickers)].copy()
    ps_clean = pd.concat([ps_clean, jq_data], ignore_index=True)
    ps_clean = ps_clean.sort_values(["ticker", "date"]).reset_index(drop=True)

    # J-Quantsで取得できなかった銘柄は元データを維持（ただし負の価格は除去）
    not_fixed = bad_tickers - jq_tickers
    if not_fixed:
        print(f"  [WARN] {len(not_fixed)} tickers not in J-Quants: {sorted(not_fixed)[:10]}...")
        # 負の価格を除去
        before = len(ps_clean)
        ps_clean = ps_clean[ps_clean["Close"] > 0]
        removed = before - len(ps_clean)
        if removed:
            print(f"  Removed {removed} rows with negative/zero prices")

    # 重複排除
    ps_clean = ps_clean.drop_duplicates(subset=["date", "ticker"], keep="last")

    ps_clean.to_parquet(PRICES_PATH, index=False)
    print(f"\n  Saved: {PRICES_PATH.name}")
    print(f"  {ps_clean['ticker'].nunique()} tickers, {len(ps_clean):,} rows")
    print(f"  Date range: {ps_clean['date'].min().date()} ~ {ps_clean['date'].max().date()}")

    # 修正後の異常値チェック
    print("\n--- Post-fix anomaly check ---")
    remaining = detect_anomalous_tickers(ps_clean)
    if remaining:
        ps_tmp = ps_clean.sort_values(["ticker", "date"]).reset_index(drop=True)
        g = ps_tmp.groupby("ticker")
        ps_tmp["prev_close"] = g["Close"].shift(1)
        ps_tmp["daily_ret"] = (ps_tmp["Close"] - ps_tmp["prev_close"]) / ps_tmp["prev_close"] * 100
        # バックテスト期間の残存異常値
        cutoff = pd.Timestamp("2024-03-01")
        recent_anom = ps_tmp[(ps_tmp["date"] >= cutoff) & (ps_tmp["daily_ret"].abs() > 40)]
        if not recent_anom.empty:
            print(f"\n  Remaining anomalies in backtest window:")
            for _, r in recent_anom.sort_values("date").iterrows():
                print(f"    {r['ticker']} {r['date'].date()} ret={r['daily_ret']:.1f}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
