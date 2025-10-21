# -*- coding: utf-8 -*-
"""
scalping.py
- Scalping watchlist API
- Returns pre-generated scalping lists merged with latest price/perf data
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from fastapi import APIRouter, Query

from ..utils import (
    read_prices_1d_df,
    normalize_prices,
    load_all_stocks,
)

router = APIRouter()

# Parquet file paths
PARQUET_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "parquet"
SCALPING_ENTRY_PATH = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE_PATH = PARQUET_DIR / "scalping_active.parquet"
GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"


def _add_volatility_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add volatility columns (prevClose, tr, atr14, etc.)"""
    if df is None or df.empty:
        return df

    g = df.sort_values(["ticker", "date"]).copy()
    g["prevClose"] = g.groupby("ticker")["Close"].shift(1)

    # True Range
    hl = g["High"] - g["Low"]
    hp = (g["High"] - g["prevClose"]).abs()
    lp = (g["Low"] - g["prevClose"]).abs()
    g["tr"] = pd.concat([hl, hp, lp], axis=1).max(axis=1)

    # ATR(14)
    g["atr14"] = (
        g.groupby("ticker", group_keys=False)["tr"]
        .apply(lambda s: s.ewm(span=14, adjust=False).mean())
    )

    # % notation
    with pd.option_context("mode.use_inf_as_na", True):
        g["tr_pct"] = (g["tr"] / g["prevClose"] * 100.0).where(g["prevClose"] > 0)
        g["atr14_pct"] = (g["atr14"] / g["Close"] * 100.0).where(g["Close"] > 0)

    return g


def _calculate_perf(df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    """Calculate performance returns for given tickers"""
    if df is None or df.empty:
        return pd.DataFrame()

    # Filter by tickers
    df = df[df["ticker"].isin(tickers)].copy()

    windows = ["5d", "1mo", "3mo", "ytd", "1y", "3y", "5y", "all"]
    days_map = {
        "5d": 7,
        "1mo": 30,
        "3mo": 90,
        "1y": 365,
        "3y": 365 * 3,
        "5y": 365 * 5,
    }

    def pct_return(last_close: float, base_close: Optional[float]) -> Optional[float]:
        if (
            last_close is None
            or base_close is None
            or pd.isna(last_close)
            or pd.isna(base_close)
            or base_close == 0
        ):
            return None
        return float((float(last_close) / float(base_close) - 1.0) * 100.0)

    records = []
    for ticker, grp in df.sort_values(["ticker", "date"]).groupby("ticker"):
        g = grp[["date", "Close"]].dropna(subset=["Close"])
        if g.empty:
            continue
        last_row = g.iloc[-1]
        last_date = last_row["date"]
        last_close = float(last_row["Close"])

        def base_close_before_or_on(target_dt: pd.Timestamp) -> Optional[float]:
            sel = g[g["date"] <= target_dt]
            if sel.empty:
                return None
            return float(sel.iloc[-1]["Close"])

        row = {"ticker": ticker}
        for w in windows:
            if w == "ytd":
                start_of_year = pd.Timestamp(year=last_date.year, month=1, day=1)
                base = base_close_before_or_on(start_of_year)
                row[f"r_{w}"] = pct_return(last_close, base)
            elif w == "all":
                base = float(g.iloc[0]["Close"])
                row[f"r_{w}"] = pct_return(last_close, base)
            else:
                days = days_map.get(w)
                if not days:
                    row[f"r_{w}"] = None
                else:
                    target = last_date - pd.Timedelta(days=days)
                    base = base_close_before_or_on(target)
                    row[f"r_{w}"] = pct_return(last_close, base)
        records.append(row)

    return pd.DataFrame(records) if records else pd.DataFrame()


def _read_scalping_list(file_path: Path) -> pd.DataFrame:
    """Read pre-generated scalping list from parquet file"""
    if not file_path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(file_path, engine="pyarrow")
        return df if not df.empty else pd.DataFrame()
    except Exception as e:
        print(f"Error reading scalping list from {file_path}: {e}")
        return pd.DataFrame()


def _merge_with_latest_data(scalping_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Merge scalping list with latest price/perf data"""
    if scalping_df.empty:
        return []

    # Get tickers from scalping list
    tickers = scalping_df["ticker"].unique().tolist()

    # Load latest price data
    price_df = read_prices_1d_df()
    if price_df is None:
        return []

    price_df = normalize_prices(price_df)
    if price_df is None or price_df.empty:
        return []

    # Add volatility columns
    price_df = _add_volatility_columns(price_df)

    # Volume MA10
    if "Volume" in price_df.columns:
        price_df["vol_ma10"] = (
            price_df.sort_values(["ticker", "date"])
            .groupby("ticker")["Volume"]
            .rolling(window=10, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    else:
        price_df["vol_ma10"] = pd.NA

    # Get latest snapshot for each ticker
    latest_price = (
        price_df[price_df["ticker"].isin(tickers)]
        .sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
    )
    latest_price["diff"] = latest_price["Close"] - latest_price["prevClose"]
    latest_price["pct_diff"] = (
        (latest_price["diff"] / latest_price["prevClose"] * 100)
        .where(latest_price["prevClose"] > 0)
    )

    # Calculate performance
    perf_df = _calculate_perf(price_df, tickers)

    # Load meta from all_stocks.parquet
    meta = load_all_stocks()
    if meta:
        meta_df = pd.DataFrame(meta)
    else:
        meta_df = pd.DataFrame()

    # Merge all data
    result = scalping_df.copy()

    # Merge with latest price data
    if not latest_price.empty:
        latest_price = latest_price.rename(columns={"Close": "close", "Volume": "volume"})
        merge_cols = ["ticker", "date", "close", "prevClose", "diff", "pct_diff",
                      "volume", "vol_ma10", "tr", "tr_pct", "atr14", "atr14_pct"]
        merge_cols = [c for c in merge_cols if c in latest_price.columns]
        result = result.merge(
            latest_price[merge_cols],
            on="ticker",
            how="left",
            suffixes=("_old", "")
        )

    # Merge with perf data
    if not perf_df.empty:
        result = result.merge(perf_df, on="ticker", how="left")

    # Merge with meta (if not already present)
    if not meta_df.empty and "stock_name" not in result.columns:
        meta_cols = ["ticker", "code", "stock_name", "market", "sectors"]
        meta_cols = [c for c in meta_cols if c in meta_df.columns]
        result = result.merge(
            meta_df[meta_cols],
            on="ticker",
            how="left",
            suffixes=("", "_meta")
        )

    # Convert to JSON records
    def _none(v):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (int, float)):
            return float(v)
        return v

    records = []
    for _, row in result.iterrows():
        # Handle tags
        tags_raw = row.get('tags', [])
        if isinstance(tags_raw, (list, tuple)):
            tags = list(tags_raw)
        elif tags_raw is None or (isinstance(tags_raw, float) and pd.isna(tags_raw)):
            tags = []
        else:
            tags = []

        record = {
            'ticker': str(row['ticker']),
            'code': str(row.get('code', row['ticker'].replace('.T', ''))),
            'stock_name': row.get('stock_name'),
            'market': row.get('market'),
            'sectors': row.get('sectors'),
            'date': row.get('date_y', row.get('date')),
            'close': _none(row.get('close')),
            'prevClose': _none(row.get('prevClose')),
            'diff': _none(row.get('diff')),
            'pct_diff': _none(row.get('pct_diff')),
            'volume': _none(row.get('volume')),
            'vol_ma10': _none(row.get('vol_ma10')),
            'tr': _none(row.get('tr')),
            'tr_pct': _none(row.get('tr_pct')),
            'atr14': _none(row.get('atr14')),
            'atr14_pct': _none(row.get('atr14_pct')),
            'r_5d': _none(row.get('r_5d')),
            'r_1mo': _none(row.get('r_1mo')),
            'r_3mo': _none(row.get('r_3mo')),
            'r_ytd': _none(row.get('r_ytd')),
            'r_1y': _none(row.get('r_1y')),
            'r_3y': _none(row.get('r_3y')),
            'r_5y': _none(row.get('r_5y')),
            'r_all': _none(row.get('r_all')),
        }

        # Format date
        if hasattr(record['date'], 'strftime'):
            record['date'] = record['date'].strftime('%Y-%m-%d')
        elif record['date']:
            record['date'] = str(record['date'])

        records.append(record)

    return records


@router.get("/entry", summary="Get entry scalping watchlist")
def get_scalping_entry(
    limit: int = Query(default=25, ge=1, le=50, description="最大銘柄数")
) -> List[Dict[str, Any]]:
    """
    エントリースキャルピングリスト（王道）
    - 大型株・主力株中心
    - 適度なボラティリティ
    - 予測しやすい値動き

    事前生成されたparquetファイルから読み込み、最新のprice/perfデータとマージ
    """
    scalping_df = _read_scalping_list(SCALPING_ENTRY_PATH)
    records = _merge_with_latest_data(scalping_df)
    return records[:limit] if records else []


@router.get("/active", summary="Get active scalping watchlist")
def get_scalping_active(
    limit: int = Query(default=25, ge=1, le=50, description="最大銘柄数")
) -> List[Dict[str, Any]]:
    """
    アクティブスキャルピングリスト
    - 高ボラティリティ
    - 大きな値動き
    - ハイリスク・ハイリターン

    事前生成されたparquetファイルから読み込み、最新のprice/perfデータとマージ
    """
    scalping_df = _read_scalping_list(SCALPING_ACTIVE_PATH)
    records = _merge_with_latest_data(scalping_df)
    return records[:limit] if records else []


@router.get("/grok", summary="Get Grok AI trending watchlist")
def get_grok_trending(
    limit: int = Query(default=25, ge=1, le=50, description="最大銘柄数")
) -> List[Dict[str, Any]]:
    """
    Grok AI銘柄選定リスト
    - xAI Grok APIで選定された「尖った銘柄」
    - 株クラ（X）でバズっている銘柄
    - IR発表・ニュース材料あり
    - 翌営業日のデイスキャルピング狙い

    事前生成されたparquetファイルから読み込み、最新のprice/perfデータとマージ
    """
    grok_df = _read_scalping_list(GROK_TRENDING_PATH)

    # Grok特有のカラム（reason, selected_time）を含めて返す
    if grok_df.empty:
        return []

    records = _merge_with_latest_data(grok_df)

    # reasonカラムを追加（Grok銘柄の選定理由）
    if not grok_df.empty and 'reason' in grok_df.columns:
        reason_map = dict(zip(grok_df['ticker'], grok_df['reason']))
        for record in records:
            record['reason'] = reason_map.get(record['ticker'])

    # selected_timeカラムを追加（16:00 or 26:00）
    if not grok_df.empty and 'selected_time' in grok_df.columns:
        selected_time_map = dict(zip(grok_df['ticker'], grok_df['selected_time']))
        for record in records:
            record['selected_time'] = selected_time_map.get(record['ticker'])

    return records[:limit] if records else []
