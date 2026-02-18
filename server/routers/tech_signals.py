"""テクニカルシグナル API

GET /tech/signals        — 個別銘柄のシグナル検出
GET /tech/signals/scan   — 全銘柄一括スキャン
"""
from __future__ import annotations

import json
import math
import time as _time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..utils import (
    load_all_stocks,
    read_prices_df,
    normalize_prices,
)
from ..services.granville import compute_ma_series, detect_granville_signals
from ..services.macd_signals import compute_macd, compute_rsi, detect_macd_signals
from ..services.entry_optimizer import detect_optimal_entry

router = APIRouter()

# ─── キャッシュ（5分TTL）─────────────────────
_scan_cache: Dict[str, Any] = {}
_SCAN_CACHE_TTL = 300  # 5分


# ─── 個別銘柄 ─────────────────────────────────

@router.get("/tech/signals", summary="個別銘柄のテクニカルシグナル検出")
def tech_signals(
    ticker: str = Query(..., description="ティッカー（例: 7203.T）"),
    date: Optional[str] = Query(default=None, description="日付 YYYY-MM-DD（省略時=最新営業日）"),
    interval: str = Query(default="5m", description="時間足（1m, 5m）"),
    ma_period: int = Query(default=25, ge=5, le=200, description="MA期間"),
):
    ticker = ticker.strip()
    if not ticker:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    result = _compute_signals_for_ticker(ticker, date, interval, ma_period)
    if result is None:
        return JSONResponse(content={"error": f"データが見つかりません: {ticker}"}, status_code=404)

    return JSONResponse(content=_sanitize(result))


# ─── 全銘柄スキャン ────────────────────────────

@router.get("/tech/signals/scan", summary="全銘柄テクニカルシグナル一括スキャン")
def tech_signals_scan(
    date: Optional[str] = Query(default=None, description="日付 YYYY-MM-DD（省略時=最新営業日）"),
    tag: Optional[str] = Query(default=None, description="銘柄タグ（例: grok）"),
):
    cache_key = f"{date or 'latest'}:{tag or 'all'}"
    now = _time.time()

    # キャッシュチェック
    if cache_key in _scan_cache:
        cached_at, cached_data = _scan_cache[cache_key]
        if now - cached_at < _SCAN_CACHE_TTL:
            return cached_data

    # 銘柄一覧取得
    stocks = load_all_stocks(tag=tag) if tag else load_all_stocks()
    if not stocks:
        return {"date": date, "results": []}

    # ★ データ読み込み + 正規化を1回だけ実行
    df_all = read_prices_df("60d", "5m")
    if df_all is None or df_all.empty:
        return {"date": date, "results": []}
    df_all = normalize_prices(df_all)
    if df_all.empty:
        return {"date": date, "results": []}

    results: List[Dict[str, Any]] = []
    resolved_date = date

    for stock in stocks:
        tk = stock.get("ticker")
        if not tk:
            continue

        sig_result = _compute_signals_for_ticker_from_df(df_all, tk, date, 25)
        if sig_result is None:
            continue

        if resolved_date is None:
            resolved_date = sig_result.get("meta", {}).get("date")

        signals = sig_result.get("signals", [])
        optimal = sig_result.get("optimal_entry")

        signal_count = len(signals) + (1 if optimal else 0)

        buy_2 = next((s for s in signals if s["type"] == "buy_2"), None)
        sell_2 = next((s for s in signals if s["type"] == "sell_2"), None)

        results.append({
            "ticker": tk,
            "stock_name": stock.get("stock_name", ""),
            "signals": {
                "buy_2": buy_2,
                "sell_2": sell_2,
                "optimal_entry": optimal,
            },
            "signal_count": signal_count,
        })

    results.sort(key=lambda r: -r["signal_count"])

    response = _sanitize({"date": resolved_date or "", "results": results})

    _scan_cache[cache_key] = (now, response)

    return JSONResponse(content=response)


# ─── 内部ロジック ──────────────────────────────

def _compute_signals_for_ticker_from_df(
    df_all: pd.DataFrame,
    ticker: str,
    date: Optional[str],
    ma_period: int,
) -> Optional[Dict[str, Any]]:
    """事前にロード済みの全銘柄DataFrameからシグナル計算（scan用）"""
    df_ticker = df_all[df_all["ticker"] == ticker].copy()
    if df_ticker.empty:
        return None

    df_ticker = df_ticker.sort_values("date").reset_index(drop=True)

    if date:
        target_date = pd.to_datetime(date).tz_localize(None).normalize()
    else:
        target_date = df_ticker["date"].dt.normalize().max()

    target_date_str = target_date.strftime("%Y-%m-%d")

    df_for_calc = df_ticker.copy()
    day_mask = df_for_calc["date"].dt.normalize() == target_date
    if not day_mask.any():
        return None

    granville_signals = detect_granville_signals(df_for_calc, ma_period=ma_period)
    granville_signals = [s for s in granville_signals if s["time"].startswith(target_date_str)]

    macd_result = detect_macd_signals(df_for_calc)
    macd_signals = [s for s in macd_result["signals"] if s["time"].startswith(target_date_str)]

    rsi_series = compute_rsi(df_for_calc["Close"], period=9)

    df_day = df_for_calc[day_mask].copy().reset_index(drop=True)
    rsi_day = rsi_series.iloc[day_mask.values].reset_index(drop=True) if len(rsi_series) > 0 else pd.Series(dtype=float)

    macd_data = compute_macd(df_for_calc["Close"])
    ml_day = macd_data["macd_line"].iloc[day_mask.values].reset_index(drop=True) if len(macd_data["macd_line"]) > 0 else pd.Series(dtype=float)
    sl_day = macd_data["signal_line"].iloc[day_mask.values].reset_index(drop=True) if len(macd_data["signal_line"]) > 0 else pd.Series(dtype=float)
    hist_day = macd_data["histogram"].iloc[day_mask.values].reset_index(drop=True) if len(macd_data["histogram"]) > 0 else pd.Series(dtype=float)

    optimal_entry = detect_optimal_entry(df_day, rsi_day, ml_day, sl_day, hist_day)

    all_signals = granville_signals + macd_signals

    return {
        "signals": all_signals,
        "optimal_entry": optimal_entry,
        "meta": {
            "ticker": ticker,
            "date": target_date_str,
            "interval": "5m",
            "ma_period": ma_period,
        },
    }


def _compute_signals_for_ticker(
    ticker: str,
    date: Optional[str],
    interval: str,
    ma_period: int,
) -> Optional[Dict[str, Any]]:
    """個別銘柄のシグナル計算。

    Returns:
        結果辞書 or None（データなし）
    """
    # period 自動マッピング
    period_map = {"1m": "5d", "5m": "60d"}
    period = period_map.get(interval.lower(), "60d")

    df_all = read_prices_df(period, interval)
    if df_all is None or df_all.empty:
        return None

    df_all = normalize_prices(df_all)
    if df_all.empty:
        return None

    df_ticker = df_all[df_all["ticker"] == ticker].copy()
    if df_ticker.empty:
        return None

    df_ticker = df_ticker.sort_values("date").reset_index(drop=True)

    # 日付フィルタ: 当日 + MA ウォームアップ用の前日以前データ
    if date:
        target_date = pd.to_datetime(date).tz_localize(None).normalize()
    else:
        # 最新営業日
        target_date = df_ticker["date"].dt.normalize().max()

    target_date_str = target_date.strftime("%Y-%m-%d")

    # MA ウォームアップ: 当日以前のデータも含める（全期間を使ってMA計算）
    # 表示は当日分のみ
    df_for_calc = df_ticker.copy()

    # 当日データのインデックス範囲
    day_mask = df_for_calc["date"].dt.normalize() == target_date
    if not day_mask.any():
        return None

    day_start_idx = day_mask.idxmax()

    # ─── シグナル検出（全期間データで計算）───────
    # グランビル
    granville_signals = detect_granville_signals(df_for_calc, ma_period=ma_period)
    # 当日分のみフィルタ
    granville_signals = [
        s for s in granville_signals
        if s["time"].startswith(target_date_str)
    ]

    # MA系列（当日分のみ）
    ma_series = compute_ma_series(df_for_calc, ma_period=ma_period)
    ma_series = [m for m in ma_series if m["time"].startswith(target_date_str)]

    # MACD（全期間で計算し当日分を返す）
    macd_result = detect_macd_signals(df_for_calc)
    macd_signals = [
        s for s in macd_result["signals"]
        if s["time"].startswith(target_date_str)
    ]
    macd_line = [m for m in macd_result["macd_line"] if m["time"].startswith(target_date_str)]
    signal_line = [m for m in macd_result["signal_line"] if m["time"].startswith(target_date_str)]
    histogram = [m for m in macd_result["histogram"] if m["time"].startswith(target_date_str)]

    # RSI（全期間で計算し当日分を返す）
    rsi_series = compute_rsi(df_for_calc["Close"], period=9)

    # 寄付エントリー最適化（当日データのみ）
    df_day = df_for_calc[day_mask].copy().reset_index(drop=True)
    rsi_day = rsi_series.iloc[day_mask.values].reset_index(drop=True) if len(rsi_series) > 0 else pd.Series(dtype=float)

    # MACD系列を当日分だけ切り出し
    macd_data = compute_macd(df_for_calc["Close"])
    ml_day = macd_data["macd_line"].iloc[day_mask.values].reset_index(drop=True) if len(macd_data["macd_line"]) > 0 else pd.Series(dtype=float)
    sl_day = macd_data["signal_line"].iloc[day_mask.values].reset_index(drop=True) if len(macd_data["signal_line"]) > 0 else pd.Series(dtype=float)
    hist_day = macd_data["histogram"].iloc[day_mask.values].reset_index(drop=True) if len(macd_data["histogram"]) > 0 else pd.Series(dtype=float)

    optimal_entry = detect_optimal_entry(df_day, rsi_day, ml_day, sl_day, hist_day)

    # 全シグナル統合
    all_signals = granville_signals + macd_signals

    # ローソク足データ（当日分のみ）
    candles = _df_to_candles(df_day)

    return {
        "candles": candles,
        "ma": ma_series,
        "macd": {
            "macd_line": macd_line,
            "signal_line": signal_line,
            "histogram": histogram,
        },
        "signals": all_signals,
        "optimal_entry": optimal_entry,
        "meta": {
            "ticker": ticker,
            "date": target_date_str,
            "interval": interval,
            "ma_period": ma_period,
        },
    }


def _sanitize(obj: Any) -> Any:
    """再帰的に NaN / Infinity を None に変換（JSON互換）"""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def _df_to_candles(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """DataFrame → ローソク足レコード"""
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        ts = pd.Timestamp(row["date"])
        time_str = ts.strftime("%Y-%m-%d %H:%M") if ts.hour != 0 or ts.minute != 0 else ts.strftime("%Y-%m-%d")
        records.append({
            "time": time_str,
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
            "volume": int(row["Volume"]) if pd.notna(row.get("Volume")) else 0,
        })
    return records
