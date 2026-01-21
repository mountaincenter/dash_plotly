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
INDEX_PRICES_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
FUTURES_PRICES_PATH = PARQUET_DIR / "futures_prices_60d_5m.parquet"


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


def calc_rsi9(ticker_df: pd.DataFrame) -> float | None:
    """RSI9を計算（SMA方式、期間9）"""
    import numpy as np

    if len(ticker_df) < 10:
        return None
    ticker_df = ticker_df.sort_values("date")
    close = ticker_df["Close"].values
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    # SMA方式（期間9）
    avg_gain = pd.Series(gain).tail(9).mean()
    avg_loss = pd.Series(loss).tail(9).mean()
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 1)


def calc_vol_ratio(ticker_df: pd.DataFrame) -> float | None:
    """出来高倍率を計算（当日出来高 / 10日移動平均）"""
    if len(ticker_df) < 10:
        return None
    ticker_df = ticker_df.sort_values("date")
    latest_vol = ticker_df["Volume"].iloc[-1]
    # 10日移動平均（当日を含む直近10日）
    vol_ma10 = ticker_df["Volume"].tail(10).mean()
    if vol_ma10 == 0 or pd.isna(vol_ma10):
        return None
    return round(latest_vol / vol_ma10, 2)


def calc_extreme_market_info() -> dict:
    """
    極端相場情報を計算

    Returns:
        dict: nikkei_change_pct, futures_change_pct, is_extreme_market, extreme_market_reason,
              nikkei_close, nikkei_prev_close, nikkei_date, futures_price, futures_date
    """
    result = {
        "nikkei_change_pct": None,
        "futures_change_pct": None,
        "is_extreme_market": False,
        "extreme_market_reason": None,
        "nikkei_close": None,
        "nikkei_prev_close": None,
        "nikkei_date": None,
        "futures_price": None,
        "futures_date": None,
    }

    # 日経225データ読み込み
    if not INDEX_PRICES_PATH.exists():
        print("[WARN] index_prices_max_1d.parquet not found")
        return result

    index_df = pd.read_parquet(INDEX_PRICES_PATH)
    nikkei_df = index_df[index_df["ticker"] == "^N225"].copy()

    if len(nikkei_df) < 2:
        print("[WARN] Not enough Nikkei data")
        return result

    nikkei_df = nikkei_df.sort_values("date")
    latest_nikkei = nikkei_df["Close"].iloc[-1]
    prev_nikkei = nikkei_df["Close"].iloc[-2]
    latest_nikkei_date = nikkei_df["date"].iloc[-1]

    # 日経データ格納
    result["nikkei_close"] = round(float(latest_nikkei), 2)
    result["nikkei_prev_close"] = round(float(prev_nikkei), 2)
    result["nikkei_date"] = str(latest_nikkei_date)[:10]

    # nikkei_change_pct: 日経の前日比
    if prev_nikkei and prev_nikkei > 0:
        result["nikkei_change_pct"] = round(
            (latest_nikkei - prev_nikkei) / prev_nikkei * 100, 6
        )

    # 先物データ読み込み
    if not FUTURES_PRICES_PATH.exists():
        print("[WARN] futures_prices_60d_5m.parquet not found")
        return result

    futures_df = pd.read_parquet(FUTURES_PRICES_PATH)
    if futures_df.empty or len(futures_df) < 1:
        print("[WARN] futures_prices_60d_5m.parquet is empty")
        return result

    futures_df = futures_df.sort_values("date")
    latest_futures = futures_df["Close"].iloc[-1]
    latest_futures_time = futures_df["date"].iloc[-1]

    # 先物データ格納
    result["futures_price"] = round(float(latest_futures), 2)
    result["futures_date"] = str(latest_futures_time)[:10]

    # futures_change_pct: 先物 vs 日経終値
    if latest_nikkei and latest_nikkei > 0:
        result["futures_change_pct"] = round(
            (latest_futures - latest_nikkei) / latest_nikkei * 100, 6
        )

    # is_extreme_market: |futures_change_pct| >= 3%
    if result["futures_change_pct"] is not None:
        if abs(result["futures_change_pct"]) >= 3.0:
            result["is_extreme_market"] = True
            direction = "上昇" if result["futures_change_pct"] > 0 else "下落"
            result["extreme_market_reason"] = f"先物{direction}{abs(result['futures_change_pct']):.2f}%"

    print(f"[INFO] Extreme market calculation:")
    print(f"       Nikkei: {prev_nikkei:.2f} -> {latest_nikkei:.2f} ({result['nikkei_change_pct']:.4f}%)")
    print(f"       Futures: {latest_futures:.2f} @ {latest_futures_time}")
    print(f"       futures_change_pct: {result['futures_change_pct']:.4f}%")
    print(f"       is_extreme_market: {result['is_extreme_market']}")

    return result


def enrich_prices(df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """価格データをマージ"""
    if df.empty:
        return df

    # 最新日付のデータ
    latest_date = prices_df["date"].max()
    latest_prices = prices_df[prices_df["date"] == latest_date].copy()
    print(f"[INFO] Latest price date: {latest_date}, {len(latest_prices)} stocks")

    # 前日データ取得（price_diff計算用）
    prev_date = prices_df[prices_df["date"] < latest_date]["date"].max()
    prev_prices = prices_df[prices_df["date"] == prev_date][["ticker", "Close"]].copy()
    prev_prices.columns = ["ticker", "prev_close"]

    # price_diff計算（前日差の実額）
    latest_prices = latest_prices.merge(prev_prices, on="ticker", how="left")
    latest_prices["price_diff"] = (
        latest_prices["Close"] - latest_prices["prev_close"]
    ).round(0)

    # ATR14%を計算
    tickers_in_grok = df["ticker"].unique()
    atr_data = {}
    for ticker in tickers_in_grok:
        ticker_prices = prices_df[prices_df["ticker"] == ticker].copy()
        if len(ticker_prices) >= 14:
            atr_data[ticker] = calc_atr14(ticker_prices)

    # rsi9 を計算
    rsi_data = {}
    for ticker in tickers_in_grok:
        ticker_prices = prices_df[prices_df["ticker"] == ticker].copy()
        if len(ticker_prices) >= 10:
            rsi_data[ticker] = calc_rsi9(ticker_prices)

    # vol_ratio を計算（10日移動平均）
    vol_data = {}
    for ticker in tickers_in_grok:
        ticker_prices = prices_df[prices_df["ticker"] == ticker].copy()
        if len(ticker_prices) >= 10:
            vol_data[ticker] = calc_vol_ratio(ticker_prices)

    # マージ用マップ
    price_map = latest_prices.set_index("ticker")[["Close", "price_diff", "Volume"]].to_dict("index")

    # マージ
    df = df.copy()
    for idx, row in df.iterrows():
        ticker = row["ticker"]
        if ticker in price_map:
            p = price_map[ticker]
            df.at[idx, "Close"] = p.get("Close")
            df.at[idx, "price_diff"] = p.get("price_diff")
            df.at[idx, "Volume"] = p.get("Volume")
        if ticker in atr_data:
            df.at[idx, "atr14_pct"] = atr_data[ticker]
        if ticker in rsi_data:
            df.at[idx, "rsi9"] = rsi_data[ticker]
        if ticker in vol_data:
            df.at[idx, "vol_ratio"] = vol_data[ticker]

    # 極端相場情報を計算・追加（全行同一値）
    extreme_info = calc_extreme_market_info()
    df["nikkei_change_pct"] = extreme_info["nikkei_change_pct"]
    df["futures_change_pct"] = extreme_info["futures_change_pct"]
    df["is_extreme_market"] = extreme_info["is_extreme_market"]
    df["extreme_market_reason"] = extreme_info["extreme_market_reason"]
    df["nikkei_close"] = extreme_info["nikkei_close"]
    df["nikkei_prev_close"] = extreme_info["nikkei_prev_close"]
    df["nikkei_date"] = extreme_info["nikkei_date"]
    df["futures_price"] = extreme_info["futures_price"]
    df["futures_date"] = extreme_info["futures_date"]

    # サマリー
    has_close = df["Close"].notna().sum()
    has_change = df["price_diff"].notna().sum()
    has_atr = df["atr14_pct"].notna().sum()
    has_rsi = df["rsi9"].notna().sum()
    has_vol = df["vol_ratio"].notna().sum()
    print(f"[INFO] Price data enriched:")
    print(f"       Close: {has_close}/{len(df)}")
    print(f"       price_diff: {has_change}/{len(df)}")
    print(f"       atr14_pct: {has_atr}/{len(df)}")
    print(f"       rsi9: {has_rsi}/{len(df)}")
    print(f"       vol_ratio: {has_vol}/{len(df)}")
    print(f"       nikkei_change_pct: {extreme_info['nikkei_change_pct']}")
    print(f"       futures_change_pct: {extreme_info['futures_change_pct']}")
    print(f"       is_extreme_market: {extreme_info['is_extreme_market']}")

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
