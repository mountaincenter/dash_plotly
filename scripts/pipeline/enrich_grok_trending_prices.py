#!/usr/bin/env python3
"""
enrich_grok_trending_prices.py
grok_trending.parquetにprices_max_1d.parquetから価格データをマージ

fetch_pricesの後に実行することで、最新の価格データを反映する
"""

import os
import sys
from pathlib import Path
from typing import List
import pandas as pd
import boto3
from io import BytesIO

from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers

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

    nikkei_df = nikkei_df.dropna(subset=["Close"]).sort_values("date")

    if len(nikkei_df) < 2:
        print("[WARN] Not enough Nikkei data after dropna")
        return result

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
    futures_df = futures_df[futures_df["ticker"] == "NKD=F"]
    if futures_df.empty or len(futures_df) < 1:
        print("[WARN] NKD=F data not found in futures_prices_60d_5m.parquet")
        return result

    futures_df = futures_df.dropna(subset=["Close"]).sort_values("date")
    latest_futures = futures_df["Close"].iloc[-1]
    latest_futures_time = futures_df["date"].iloc[-1]

    # 先物データ格納
    result["futures_price"] = round(float(latest_futures), 2)
    result["futures_date"] = str(latest_futures_time)

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
    nikkei_chg = result['nikkei_change_pct']
    futures_chg = result['futures_change_pct']
    nikkei_chg_str = f"{nikkei_chg:.4f}%" if nikkei_chg is not None else "N/A"
    futures_chg_str = f"{futures_chg:.4f}%" if futures_chg is not None else "N/A"
    print(f"       Nikkei: {prev_nikkei:.2f} -> {latest_nikkei:.2f} ({nikkei_chg_str})")
    print(f"       Futures: {latest_futures:.2f} @ {latest_futures_time}")
    print(f"       futures_change_pct: {futures_chg_str}")
    print(f"       is_extreme_market: {result['is_extreme_market']}")

    return result


def _rows_from_jquants(
    missing_tickers: List[str],
    target_date: pd.Timestamp,
) -> pd.DataFrame:
    """Fetch target-date daily bars from J-Quants for missing tickers."""

    try:
        from scripts.lib.jquants_fetcher import JQuantsFetcher
    except Exception as e:
        print(f"  ⚠ J-Quants fetcher unavailable: {e}")
        return pd.DataFrame()

    codes = [t.replace(".T", "") for t in missing_tickers]
    target = target_date.strftime("%Y-%m-%d")
    print(f"[INFO] Backfilling {len(codes)} ticker(s) from J-Quants for {target}")

    try:
        fetched_df = JQuantsFetcher().get_prices_daily_batch(
            codes,
            from_date=target,
            to_date=target,
            batch_delay=0.0,
        )
    except Exception as e:
        print(f"  ⚠ J-Quants backfill failed: {e}")
        return pd.DataFrame()

    if fetched_df.empty or "Close" not in fetched_df.columns:
        print("  ⚠ J-Quants returned no usable rows")
        return pd.DataFrame()

    rows = fetched_df.dropna(subset=["Close"]).copy()
    if rows.empty:
        print("  ⚠ J-Quants rows had no confirmed Close")
        return pd.DataFrame()

    rows["ticker"] = rows["Code"].astype(str).str[:4] + ".T"
    rows["date"] = pd.to_datetime(rows["Date"])
    return rows[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]]


def _backfill_missing_latest_prices(
    prices_df: pd.DataFrame,
    target_date: pd.Timestamp,
    missing_tickers: List[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch daily bars for missing tickers and update prices_df."""

    if not missing_tickers:
        return prices_df, pd.DataFrame()

    target_rows = _rows_from_jquants(missing_tickers, target_date)

    if target_rows.empty:
        print(f"[INFO] Backfilling {len(missing_tickers)} ticker(s) from yfinance (1d)")
        fetched_df = fetch_prices_for_tickers(missing_tickers, period="10d", interval="1d")

        if fetched_df.empty:
            print("  ✗ yfinance returned no rows for missing tickers")
            return prices_df, pd.DataFrame()

        fetched_df = fetched_df.copy()
        fetched_df["date"] = pd.to_datetime(fetched_df["date"])
        fetched_df = fetched_df.dropna(subset=["Close"])
        target_rows = fetched_df[fetched_df["date"] <= target_date].copy()

        if target_rows.empty:
            print("  ✗ No yfinance rows with confirmed Close at or before target date")
            return prices_df, pd.DataFrame()

        target_rows = (
            target_rows.sort_values("date")
            .groupby("ticker", as_index=False)
            .tail(1)
        )
        print("  ⚠ Using latest yfinance row at or before target date per ticker")

    updated_prices = prices_df.copy()
    appended_rows = []
    for _, row in target_rows.iterrows():
        mask = (updated_prices["ticker"] == row["ticker"]) & (updated_prices["date"] == row["date"])
        values = {col: row.get(col) for col in ["Open", "High", "Low", "Close", "Volume"]}
        if mask.any():
            for col, val in values.items():
                updated_prices.loc[mask, col] = val
        else:
            appended_rows.append({
                "date": row["date"],
                "Open": values["Open"],
                "High": values["High"],
                "Low": values["Low"],
                "Close": values["Close"],
                "Volume": values["Volume"],
                "ticker": row["ticker"],
            })

    if appended_rows:
        updated_prices = pd.concat([updated_prices, pd.DataFrame(appended_rows)], ignore_index=True)

    return updated_prices, target_rows


def _infer_price_asof_date(df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.Timestamp:
    """Resolve the trading day whose Close should enrich the selected Grok list."""

    if "price_asof_date" in df.columns:
        asof = pd.to_datetime(df["price_asof_date"], errors="coerce").dropna()
        if not asof.empty:
            return pd.Timestamp(asof.max()).normalize()

    if "date" in df.columns:
        selected = pd.to_datetime(df["date"], errors="coerce").dropna()
        if not selected.empty:
            return (pd.Timestamp(selected.max()).normalize() - pd.tseries.offsets.BDay(1)).normalize()

    return pd.Timestamp(prices_df["date"].max()).normalize()


def _latest_valid_prices(
    prices_df: pd.DataFrame,
    tickers: List[str],
    price_asof_date: pd.Timestamp,
) -> pd.DataFrame:
    """Get each ticker's latest confirmed Close at or before price_asof_date."""

    subset = prices_df[
        prices_df["ticker"].isin(tickers)
        & (prices_df["date"] <= price_asof_date)
    ].dropna(subset=["Close"]).copy()

    if subset.empty:
        return subset

    return (
        subset.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def _attach_prev_close(prices_df: pd.DataFrame, latest_prices: pd.DataFrame) -> pd.DataFrame:
    """Attach previous confirmed Close per ticker relative to that ticker's source date."""

    prev_rows = []
    for _, row in latest_prices[["ticker", "date"]].iterrows():
        hist = prices_df[
            (prices_df["ticker"] == row["ticker"])
            & (prices_df["date"] < row["date"])
        ].dropna(subset=["Close"]).sort_values("date")
        if hist.empty:
            continue
        prev = hist.iloc[-1]
        prev_rows.append({"ticker": row["ticker"], "prev_close": prev["Close"]})

    if not prev_rows:
        latest_prices["prev_close"] = pd.NA
        return latest_prices

    return latest_prices.merge(pd.DataFrame(prev_rows), on="ticker", how="left")


def save_prices(prices_df: pd.DataFrame) -> None:
    """Persist repaired prices_max_1d locally and to S3."""

    prices_df.to_parquet(PRICES_PATH, index=False)
    print(f"[OK] Saved repaired prices locally: {PRICES_PATH}")

    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    key = "parquet/prices_max_1d.parquet"
    s3_client = boto3.client("s3")
    buffer = BytesIO()
    prices_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
        CacheControl="max-age=60",
        ServerSideEncryption="AES256",
    )
    print(f"[OK] Uploaded repaired prices to S3: s3://{bucket}/{key}")


def enrich_prices(df: pd.DataFrame, prices_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    """価格データをマージ"""
    if df.empty:
        return df, prices_df, False

    prices_df = prices_df.copy()
    prices_df["date"] = pd.to_datetime(prices_df["date"])
    tickers_in_grok = df["ticker"].unique().tolist()
    price_asof_date = _infer_price_asof_date(df, prices_df)

    latest_prices = _latest_valid_prices(prices_df, tickers_in_grok, price_asof_date)
    print(
        f"[INFO] Price as-of date: {price_asof_date.date()}, "
        f"valid rows: {len(latest_prices)}/{len(tickers_in_grok)}"
    )

    valid_tickers = set(latest_prices["ticker"].unique()) if not latest_prices.empty else set()
    missing_tickers = sorted(set(tickers_in_grok) - valid_tickers)
    stale_tickers = []
    if not latest_prices.empty:
        source_dates = latest_prices.set_index("ticker")["date"].to_dict()
        stale_tickers = sorted(
            ticker for ticker in tickers_in_grok
            if ticker in source_dates and pd.Timestamp(source_dates[ticker]).normalize() < price_asof_date
        )
        if stale_tickers:
            print(
                f"[INFO] {len(stale_tickers)} ticker(s) only have confirmed Close before "
                f"{price_asof_date.date()}; attempting target-date repair"
            )
    prices_updated = False

    backfill_tickers = sorted(set(missing_tickers) | set(stale_tickers))
    if backfill_tickers:
        prices_df, recovered_rows = _backfill_missing_latest_prices(prices_df, price_asof_date, backfill_tickers)
        if not recovered_rows.empty:
            prices_updated = True
            latest_prices = _latest_valid_prices(prices_df, tickers_in_grok, price_asof_date)
            still_missing = set(tickers_in_grok) - set(latest_prices["ticker"].unique())
            recovered_count = len(set(recovered_rows["ticker"].unique()) - still_missing)
            print(f"[INFO] Backfilled confirmed Close for {recovered_count} ticker(s)")
            if still_missing:
                print(f"[WARN] Still missing Close for: {sorted(still_missing)}")
        else:
            print("[WARN] Failed to backfill missing tickers")

    if latest_prices.empty:
        raise RuntimeError("No confirmed Close rows were available for Grok tickers")

    # price_diff計算（前日差の実額）
    latest_prices = _attach_prev_close(prices_df, latest_prices)
    latest_prices["price_diff"] = (
        latest_prices["Close"] - latest_prices["prev_close"]
    ).round(0)
    latest_prices["price_source_date"] = latest_prices["date"].dt.strftime("%Y-%m-%d")

    # ATR14%を計算
    atr_data = {}
    for ticker in tickers_in_grok:
        source_date = latest_prices.loc[latest_prices["ticker"] == ticker, "date"]
        if source_date.empty:
            continue
        ticker_prices = prices_df[
            (prices_df["ticker"] == ticker)
            & (prices_df["date"] <= source_date.iloc[0])
        ].dropna(subset=["Close"]).copy()
        if len(ticker_prices) >= 14:
            atr_data[ticker] = calc_atr14(ticker_prices)

    # rsi9 を計算
    rsi_data = {}
    for ticker in tickers_in_grok:
        source_date = latest_prices.loc[latest_prices["ticker"] == ticker, "date"]
        if source_date.empty:
            continue
        ticker_prices = prices_df[
            (prices_df["ticker"] == ticker)
            & (prices_df["date"] <= source_date.iloc[0])
        ].dropna(subset=["Close"]).copy()
        if len(ticker_prices) >= 10:
            rsi_data[ticker] = calc_rsi9(ticker_prices)

    # vol_ratio を計算（10日移動平均）
    vol_data = {}
    for ticker in tickers_in_grok:
        source_date = latest_prices.loc[latest_prices["ticker"] == ticker, "date"]
        if source_date.empty:
            continue
        ticker_prices = prices_df[
            (prices_df["ticker"] == ticker)
            & (prices_df["date"] <= source_date.iloc[0])
        ].dropna(subset=["Close"]).copy()
        if len(ticker_prices) >= 10:
            vol_data[ticker] = calc_vol_ratio(ticker_prices)

    # マージ用マップ
    price_map = latest_prices.set_index("ticker")[["Close", "price_diff", "Volume", "price_source_date"]].to_dict("index")

    # マージ
    df = df.copy()
    for idx, row in df.iterrows():
        ticker = row["ticker"]
        if ticker in price_map:
            p = price_map[ticker]
            df.at[idx, "Close"] = p.get("Close")
            df.at[idx, "price_diff"] = p.get("price_diff")
            df.at[idx, "Volume"] = p.get("Volume")
            df.at[idx, "price_source_date"] = p.get("price_source_date")
            df.at[idx, "price_asof_date"] = price_asof_date.strftime("%Y-%m-%d")
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

    min_close_required = int(os.getenv("GROK_PRICE_MIN_CLOSE", str(len(df))))
    if has_close < min_close_required:
        raise RuntimeError(
            f"Close coverage {has_close}/{len(df)} is below required {min_close_required}; "
            "refusing to publish incomplete grok_trending.parquet"
        )

    # SHORT推奨フラグ: grade廃止済み、暫定False
    df["short_recommended"] = False

    # reason カテゴリ自動分類
    if "reason" in df.columns:
        df["reason_category"] = df["reason"].fillna("").apply(_classify_reason)
        cats = df["reason_category"].value_counts().to_dict()
        print(f"       reason_category: {cats}")

    return df, prices_df, prices_updated


def _classify_reason(reason: str) -> str:
    """reason テキストから材料タイプを判定"""
    r = reason.lower()
    has_s_high = any(k in r for k in ["ストップ高", "s高"])
    has_ir = any(k in r for k in ["決算", "増益", "減益", "業績", "上方修正", "下方修正", "適時開示", "ir"])
    has_momentum = any(k in r for k in ["値上がり率", "急騰", "急伸", "連続", "継続"])

    if has_s_high and has_ir:
        return "S高+決算IR"
    if has_momentum:
        return "急騰/連続上昇"
    if has_s_high:
        return "S高"
    if has_ir:
        return "決算IR"
    return "その他"


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
        df, prices_df, prices_updated = enrich_prices(df, prices_df)
        if prices_updated:
            save_prices(prices_df)

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
