"""
開発用: 日中分析API

5分足データを使用した高値安値時間帯分析
- GET /dev/intraday-analysis: 高値安値テーブル + 正規化価格データ
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

router = APIRouter()

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
PRICES_5M_PATH = BASE_DIR / "data" / "parquet" / "prices_60d_5m.parquet"
PRICES_1D_PATH = BASE_DIR / "data" / "parquet" / "prices_max_1d.parquet"
INDEX_5M_PATH = BASE_DIR / "data" / "parquet" / "index_prices_60d_5m.parquet"

# 曜日名
WEEKDAY_NAMES = ["月", "火", "水", "木", "金", "土", "日"]


def load_5m_data(ticker: str) -> pd.DataFrame:
    """個別銘柄の5分足データを読み込み"""
    if not PRICES_5M_PATH.exists():
        raise HTTPException(status_code=500, detail="5分足データが見つかりません")

    df = pd.read_parquet(PRICES_5M_PATH)
    df = df[df["ticker"] == ticker].copy()

    if len(df) == 0:
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} のデータが見つかりません")

    return df.dropna(subset=["Close"])


def load_1d_data(ticker: str) -> pd.DataFrame:
    """個別銘柄の日足データを読み込み"""
    if not PRICES_1D_PATH.exists():
        raise HTTPException(status_code=500, detail="日足データが見つかりません")

    df = pd.read_parquet(PRICES_1D_PATH)
    df = df[df["ticker"] == ticker].copy()

    return df


def load_index_5m_data(index_ticker: str) -> pd.DataFrame:
    """指数の5分足データを読み込み"""
    if not INDEX_5M_PATH.exists():
        return pd.DataFrame()

    df = pd.read_parquet(INDEX_5M_PATH)
    df = df[df["ticker"] == index_ticker].copy()

    return df.dropna(subset=["Close"])


def calc_intraday_table(df_5m: pd.DataFrame, df_1d: pd.DataFrame) -> list:
    """高値安値時間帯テーブルを計算"""
    # 日付リスト（新しい順）
    dates = sorted(df_1d["date"].unique(), reverse=True)

    result = []
    prev_close = None

    # 古い順にループして前日終値を取得
    for date in reversed(dates):
        day_1d = df_1d[df_1d["date"] == date]
        if len(day_1d) == 0:
            continue

        row = day_1d.iloc[0]

        # 5分足データ
        day_5m = df_5m[df_5m["date"].dt.date == pd.Timestamp(date).date()]

        if len(day_5m) == 0:
            prev_close = row["Close"]
            continue

        # 高値・安値の時間
        high_idx = day_5m["High"].idxmax()
        low_idx = day_5m["Low"].idxmin()
        high_time = day_5m.loc[high_idx, "date"].strftime("%H:%M")
        low_time = day_5m.loc[low_idx, "date"].strftime("%H:%M")

        # 前場終値（12:00より前の最後のClose）
        am_data = day_5m[day_5m["date"].dt.hour < 12]
        am_close = am_data.sort_values("date").iloc[-1]["Close"] if len(am_data) > 0 else None

        # 曜日
        weekday = pd.Timestamp(date).weekday()
        weekday_name = WEEKDAY_NAMES[weekday] if weekday < 7 else ""

        # PnL計算（ロングベース）
        open_price = row["Open"]
        close_price = row["Close"]
        am_pnl = int(am_close - open_price) if am_close is not None else None
        day_pnl = int(close_price - open_price)

        result.append({
            "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
            "dayOfWeek": weekday_name,
            "prevClose": int(prev_close) if prev_close is not None else None,
            "open": int(open_price),
            "high": int(row["High"]),
            "highTime": high_time,
            "low": int(row["Low"]),
            "lowTime": low_time,
            "amClose": int(am_close) if am_close is not None else None,
            "amPnl": am_pnl,
            "close": int(close_price),
            "dayPnl": day_pnl,
            "volatility": int(row["High"] - row["Low"]),
        })

        prev_close = row["Close"]

    # 新しい順に反転
    return list(reversed(result))


def calc_normalized_prices(df_5m: pd.DataFrame, df_1d: pd.DataFrame, date_str: str) -> list:
    """指定日の前日終値=100の正規化価格を計算"""
    target_date = pd.Timestamp(date_str).date()

    # 前日終値を取得
    df_1d_sorted = df_1d.sort_values("date")
    prev_dates = df_1d_sorted[df_1d_sorted["date"].dt.date < target_date]

    if len(prev_dates) == 0:
        return []

    prev_close = prev_dates.iloc[-1]["Close"]

    # 当日の5分足
    day_5m = df_5m[df_5m["date"].dt.date == target_date].copy()

    if len(day_5m) == 0:
        return []

    # 正規化
    day_5m = day_5m.sort_values("date")

    return [
        {
            "time": row["date"].strftime("%H:%M"),
            "value": round((row["Close"] / prev_close) * 100, 2),
        }
        for _, row in day_5m.iterrows()
    ]


def calc_summary(table: list) -> dict:
    """サマリー統計を計算"""
    if len(table) == 0:
        return {}

    # 高値が前場の割合
    high_am_count = sum(1 for row in table if row["highTime"] and row["highTime"] < "12:30")
    # 安値が前場の割合
    low_am_count = sum(1 for row in table if row["lowTime"] and row["lowTime"] < "12:30")

    # ロング勝率（日中PnL > 0）
    long_wins = sum(1 for row in table if row["dayPnl"] is not None and row["dayPnl"] > 0)

    # 前場ロング勝率（前場PnL > 0）
    am_wins = sum(1 for row in table if row["amPnl"] is not None and row["amPnl"] > 0)
    am_count = sum(1 for row in table if row["amPnl"] is not None)

    total = len(table)

    return {
        "highAmPct": round(high_am_count / total * 100, 1) if total > 0 else 0,
        "lowAmPct": round(low_am_count / total * 100, 1) if total > 0 else 0,
        "longWinRate": round(long_wins / total * 100, 1) if total > 0 else 0,
        "amLongWinRate": round(am_wins / am_count * 100, 1) if am_count > 0 else 0,
        "totalDays": total,
    }


@router.get("/dev/intraday-analysis")
async def get_intraday_analysis(
    ticker: str = Query(..., description="ティッカー（例: 7729.T）"),
    date: Optional[str] = Query(None, description="正規化価格の基準日（YYYY-MM-DD）。省略時は直近営業日"),
):
    """
    高値安値時間帯分析

    Returns:
    - table: 高値安値テーブル（60営業日分、新しい順）
    - normalizedPrices: 前日終値=100の正規化価格（指定日のみ）
    - summary: サマリー統計
    """
    # 個別銘柄データ
    df_5m = load_5m_data(ticker)
    df_1d = load_1d_data(ticker)

    if len(df_1d) == 0:
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} の日足データが見つかりません")

    # テーブル計算
    table = calc_intraday_table(df_5m, df_1d)

    # 基準日（デフォルトは直近営業日）
    if date is None:
        date = table[0]["date"] if len(table) > 0 else None

    # 正規化価格
    normalized_ticker = calc_normalized_prices(df_5m, df_1d, date) if date else []

    # 日経平均
    df_nikkei_5m = load_index_5m_data("^N225")
    df_nikkei_1d = pd.read_parquet(BASE_DIR / "data" / "parquet" / "index_prices_max_1d.parquet") if (BASE_DIR / "data" / "parquet" / "index_prices_max_1d.parquet").exists() else pd.DataFrame()
    df_nikkei_1d = df_nikkei_1d[df_nikkei_1d["ticker"] == "^N225"] if len(df_nikkei_1d) > 0 else pd.DataFrame()
    normalized_nikkei = calc_normalized_prices(df_nikkei_5m, df_nikkei_1d, date) if date and len(df_nikkei_5m) > 0 and len(df_nikkei_1d) > 0 else []

    # TOPIX (ETF)
    df_topix_5m = load_index_5m_data("1489.T")
    df_topix_1d = pd.read_parquet(BASE_DIR / "data" / "parquet" / "index_prices_max_1d.parquet") if (BASE_DIR / "data" / "parquet" / "index_prices_max_1d.parquet").exists() else pd.DataFrame()
    df_topix_1d = df_topix_1d[df_topix_1d["ticker"] == "1489.T"] if len(df_topix_1d) > 0 else pd.DataFrame()
    normalized_topix = calc_normalized_prices(df_topix_5m, df_topix_1d, date) if date and len(df_topix_5m) > 0 and len(df_topix_1d) > 0 else []

    # サマリー
    summary = calc_summary(table)

    return JSONResponse(content={
        "table": table,
        "normalizedPrices": {
            "date": date,
            "ticker": normalized_ticker,
            "nikkei": normalized_nikkei,
            "topix": normalized_topix,
        },
        "summary": summary,
        "meta": {
            "generatedAt": datetime.now().isoformat(),
            "ticker": ticker,
        },
    })
