#!/usr/bin/env python3
"""
enrich_grok_trending_prices.py
grok_trending.parquetにprices_max_1d.parquetから価格データをマージ

fetch_pricesの後に実行することで、最新の価格データを反映する
"""

import os
import sys
from pathlib import Path
import pandas as pd
import boto3
from io import BytesIO

ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = ROOT / "data" / "parquet"
GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
PRICES_PATH = PARQUET_DIR / "prices_max_1d.parquet"


def load_grok_trending() -> pd.DataFrame:
    """grok_trending.parquetを読み込み"""
    if GROK_TRENDING_PATH.exists():
        return pd.read_parquet(GROK_TRENDING_PATH)

    # S3から取得
    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    key = "parquet/grok_trending.parquet"

    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))


def load_prices() -> pd.DataFrame:
    """prices_max_1d.parquetを読み込み"""
    if PRICES_PATH.exists():
        return pd.read_parquet(PRICES_PATH)

    # S3から取得
    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    key = "parquet/prices_max_1d.parquet"

    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))


def calc_atr14(ticker_df: pd.DataFrame) -> float | None:
    """ATR14%を計算"""
    if len(ticker_df) < 14:
        return None
    ticker_df = ticker_df.sort_values("date")
    ticker_df["prev_close"] = ticker_df["Close"].shift(1)
    ticker_df["tr1"] = ticker_df["High"] - ticker_df["Low"]
    ticker_df["tr2"] = (ticker_df["High"] - ticker_df["prev_close"]).abs()
    ticker_df["tr3"] = (ticker_df["Low"] - ticker_df["prev_close"]).abs()
    ticker_df["tr"] = ticker_df[["tr1", "tr2", "tr3"]].max(axis=1)
    atr14 = ticker_df["tr"].tail(14).mean()
    latest_close = ticker_df["Close"].iloc[-1]
    if latest_close and latest_close > 0:
        return round(atr14 / latest_close * 100, 2)
    return None


def enrich_prices(df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """価格データをマージ"""
    if df.empty:
        return df

    # 最新日付のデータ
    latest_date = prices_df["date"].max()
    latest_prices = prices_df[prices_df["date"] == latest_date].copy()
    print(f"[INFO] Latest price date: {latest_date}, {len(latest_prices)} stocks")

    # 前日データ取得（change_pct計算用）
    prev_date = prices_df[prices_df["date"] < latest_date]["date"].max()
    prev_prices = prices_df[prices_df["date"] == prev_date][["ticker", "Close"]].copy()
    prev_prices.columns = ["ticker", "prev_close"]

    # change_pct計算
    latest_prices = latest_prices.merge(prev_prices, on="ticker", how="left")
    latest_prices["change_pct"] = (
        (latest_prices["Close"] - latest_prices["prev_close"])
        / latest_prices["prev_close"] * 100
    ).round(2)

    # ATR14%を計算
    tickers_in_grok = df["ticker"].unique()
    atr_data = {}
    for ticker in tickers_in_grok:
        ticker_prices = prices_df[prices_df["ticker"] == ticker].copy()
        if len(ticker_prices) >= 14:
            atr_data[ticker] = calc_atr14(ticker_prices)

    # マージ用マップ
    price_map = latest_prices.set_index("ticker")[["Close", "change_pct", "Volume"]].to_dict("index")

    # マージ
    df = df.copy()
    for idx, row in df.iterrows():
        ticker = row["ticker"]
        if ticker in price_map:
            p = price_map[ticker]
            df.at[idx, "Close"] = p.get("Close")
            df.at[idx, "change_pct"] = p.get("change_pct")
            df.at[idx, "Volume"] = p.get("Volume")
        if ticker in atr_data:
            df.at[idx, "atr14_pct"] = atr_data[ticker]

    # サマリー
    has_close = df["Close"].notna().sum()
    has_change = df["change_pct"].notna().sum()
    has_atr = df["atr14_pct"].notna().sum()
    print(f"[INFO] Price data enriched:")
    print(f"       Close: {has_close}/{len(df)}")
    print(f"       change_pct: {has_change}/{len(df)}")
    print(f"       atr14_pct: {has_atr}/{len(df)}")

    return df


def save_grok_trending(df: pd.DataFrame) -> None:
    """ローカルとS3に保存"""
    # ローカル保存
    df.to_parquet(GROK_TRENDING_PATH, index=False)
    print(f"[OK] Saved locally: {GROK_TRENDING_PATH}")

    # S3保存
    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    key = "parquet/grok_trending.parquet"

    s3_client = boto3.client("s3")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"[OK] Uploaded to S3: s3://{bucket}/{key}")


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("Enrich grok_trending.parquet with price data")
    print("=" * 60)

    try:
        # 読み込み
        df = load_grok_trending()
        print(f"[INFO] Loaded grok_trending: {len(df)} stocks")

        prices_df = load_prices()
        print(f"[INFO] Loaded prices_max_1d: {len(prices_df)} rows")

        # 価格マージ
        df = enrich_prices(df, prices_df)

        # 保存
        save_grok_trending(df)

        print("=" * 60)
        print("Done!")
        return 0

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
