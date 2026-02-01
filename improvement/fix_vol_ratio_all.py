#!/usr/bin/env python3
"""
fix_vol_ratio_all.py
全てのgrok_trendingファイルのvol_ratioを修正

データソース: improvement/grok_prices_max_1d.parquet (469 stocks)
"""

import os
from pathlib import Path
import pandas as pd
from glob import glob

ROOT = Path(__file__).resolve().parent.parent
PRICES_PATH = ROOT / "improvement" / "grok_prices_max_1d.parquet"


def load_prices() -> pd.DataFrame:
    """価格データを読み込み、日付を正規化"""
    df = pd.read_parquet(PRICES_PATH)
    # 日付をYYYY-MM-DD文字列に変換（タイムゾーン除去）
    df["date_str"] = df["date"].astype(str).str[:10]
    df = df.sort_values(["ticker", "date_str"])
    return df


def calc_vol_ratio(ticker_df: pd.DataFrame, target_date_str: str) -> float | None:
    """
    vol_ratioを計算（当日出来高 / 10日移動平均）
    target_date_str: YYYY-MM-DD形式
    """
    # target_date以前のデータを取得
    ticker_df = ticker_df[ticker_df["date_str"] <= target_date_str].copy()
    if len(ticker_df) < 10:
        return None

    ticker_df = ticker_df.sort_values("date_str")

    # 直近10日を取得
    recent_df = ticker_df.tail(10)
    if len(recent_df) < 10:
        return None

    latest_vol = recent_df["Volume"].iloc[-1]
    vol_ma10 = recent_df["Volume"].mean()

    if vol_ma10 == 0 or pd.isna(vol_ma10) or pd.isna(latest_vol):
        return None

    return round(latest_vol / vol_ma10, 2)


def get_date_from_row(row: pd.Series, columns: list) -> str | None:
    """行から日付を取得（YYYY-MM-DD形式で返す）"""
    # 優先順: selection_date > date
    for col in ["selection_date", "date"]:
        if col in columns:
            val = row.get(col)
            if pd.notna(val):
                # datetime/Timestamp/stringどれでも対応
                return str(val)[:10]
    return None


def process_file(filepath: Path, prices_df: pd.DataFrame) -> tuple[int, int]:
    """
    1ファイルを処理
    Returns: (updated_count, total_count)
    """
    df = pd.read_parquet(filepath)
    if df.empty:
        return 0, 0

    columns = df.columns.tolist()

    # vol_ratioカラムがなければ追加
    if "vol_ratio" not in columns:
        df["vol_ratio"] = None

    updated = 0
    for idx, row in df.iterrows():
        ticker = row.get("ticker")
        if not ticker:
            continue

        # 日付取得
        date_str = get_date_from_row(row, columns)
        if not date_str:
            continue

        # 価格データ取得
        ticker_prices = prices_df[prices_df["ticker"] == ticker]
        if ticker_prices.empty:
            continue

        # vol_ratio計算
        vol_ratio = calc_vol_ratio(ticker_prices, date_str)
        if vol_ratio is not None:
            df.at[idx, "vol_ratio"] = vol_ratio
            updated += 1

    # 保存
    df.to_parquet(filepath, index=False)
    return updated, len(df)


def main():
    print("=" * 60)
    print("Fix vol_ratio for all grok_trending files")
    print("=" * 60)

    # 価格データ読み込み
    print(f"\nLoading prices from: {PRICES_PATH}")
    prices_df = load_prices()
    print(f"Loaded: {len(prices_df)} rows, {prices_df['ticker'].nunique()} tickers")
    print(f"Date range: {prices_df['date_str'].min()} to {prices_df['date_str'].max()}")

    # 対象ファイル一覧
    targets = [
        ROOT / "data" / "parquet" / "grok_trending.parquet",
        ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet",
    ]

    # backtest/grok_trending_YYYYMMDD.parquet
    backtest_files = glob(str(ROOT / "data" / "parquet" / "backtest" / "grok_trending_2*.parquet"))
    targets.extend([Path(f) for f in backtest_files])

    # archive/backtest/grok_trending_YYYYMMDD.parquet
    archive_files = glob(str(ROOT / "data" / "parquet" / "archive" / "backtest" / "grok_trending_2*.parquet"))
    targets.extend([Path(f) for f in archive_files])

    # 重複除去
    targets = list(set(targets))
    targets = [f for f in targets if f.exists()]

    print(f"\nTarget files: {len(targets)}")

    total_updated = 0
    total_records = 0

    for filepath in sorted(targets):
        updated, total = process_file(filepath, prices_df)
        total_updated += updated
        total_records += total
        rel_path = filepath.relative_to(ROOT)
        print(f"  {rel_path}: {updated}/{total}")

    print("=" * 60)
    print(f"Done! Updated {total_updated}/{total_records} records across {len(targets)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
