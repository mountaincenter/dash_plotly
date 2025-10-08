from __future__ import annotations

from typing import Any, Dict, List, Optional
import json
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ...utils import (
    read_prices_1d_df,
    read_prices_df,
    normalize_prices,
)

router = APIRouter()

def _add_volatility_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    前提: normalize_prices() 済みの縦持ち DataFrame
         必須列: ["ticker","date","Open","High","Low","Close"]（Volume は任意）
    追加: prevClose, tr, tr_pct, atr14, atr14_pct
    """
    if df is None or df.empty:
        return df

    g = df.sort_values(["ticker", "date"]).copy()
    g["prevClose"] = g.groupby("ticker")["Close"].shift(1)

    # --- True Range (TR) ---
    hl = g["High"] - g["Low"]
    hp = (g["High"] - g["prevClose"]).abs()
    lp = (g["Low"] - g["prevClose"]).abs()
    g["tr"] = pd.concat([hl, hp, lp], axis=1).max(axis=1)

    # --- ATR(14): EMA(TR, span=14) ---
    g["atr14"] = (
        g.groupby("ticker", group_keys=False)["tr"]
        .apply(lambda s: s.ewm(span=14, adjust=False).mean())
    )

    # --- %表記 ---
    with pd.option_context("mode.use_inf_as_na", True):
        g["tr_pct"] = (g["tr"] / g["prevClose"] * 100.0).where(g["prevClose"] > 0)
        g["atr14_pct"] = (g["atr14"] / g["Close"] * 100.0).where(g["Close"] > 0)

    return g


def to_json_records(df: pd.DataFrame, *, include_time: bool = False) -> List[Dict]:
    """
    DataFrame → JSON-ready list[dict]
    include_time=True の場合、datetimeを 'YYYY-MM-DDTHH:MM:SS' で出力
    """
    g = df.copy()
    g = g.sort_values(["ticker", "date"]).reset_index(drop=True)

    if include_time:
        g["date"] = g["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        g["date"] = g["date"].dt.strftime("%Y-%m-%d")

    g = g.where(pd.notna(g), None)
    return json.loads(g.to_json(orient="records"))


@router.get("/prices/max/1d")
def core30_prices_max_1d():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []
    # 既存仕様維持（ここではボラ列は付けない）
    return to_json_records(out)


@router.get("/prices")
def core30_prices(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    interval: str = Query(default="1d", description="時間足（例: 1d, 5m, 15m, 1h, 1wk, 1mo）"),
    end: Optional[str] = Query(default=None, description="終了日（YYYY-MM-DD または YYYY-MM-DDTHH:mm:ss）"),
    start: Optional[str] = Query(default=None, description="開始日（YYYY-MM-DD または YYYY-MM-DDTHH:mm:ss）"),
):
    """periodを自動マッピングし、日付指定は00:00〜23:59:59に自動補完"""
    ticker = (ticker or "").strip()
    if not ticker:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    # --- period 自動マッピング ---
    period_map = {
        "5m": "60d",
        "15m": "60d",
        "1h": "730d",
        "1d": "max",
        "1wk": "max",
        "1mo": "max",
    }
    interval_lc = interval.lower()
    period = period_map.get(interval_lc)
    if period is None:
        return JSONResponse(content={"error": f"unsupported interval: {interval}"}, status_code=400)

    # --- 日付パラメータ処理 ---
    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    try:
        if end:
            end_dt = pd.to_datetime(end).tz_localize(None)
            if end_dt.hour == 0 and end_dt.minute == 0 and end_dt.second == 0:
                end_dt = end_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        else:
            end_dt = today + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    except Exception:
        return JSONResponse(content={"error": "invalid end"}, status_code=400)

    try:
        if start:
            start_dt = pd.to_datetime(start).tz_localize(None)
        else:
            start_dt = end_dt - pd.Timedelta(days=365)
    except Exception:
        return JSONResponse(content={"error": "invalid start"}, status_code=400)

    if start_dt > end_dt:
        return JSONResponse(content={"error": "start must be <= end"}, status_code=400)

    # --- データ取得 ---
    df = read_prices_df(period, interval)
    if df is None:
        return JSONResponse(content={"error": f"No data found for interval={interval}"}, status_code=404)
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    # --- ボラティリティ付与 + 期間抽出 ---
    g = _add_volatility_columns(out)
    sel = g[
        (g["ticker"] == ticker)
        & (g["date"] >= start_dt)
        & (g["date"] <= end_dt)
    ]
    if sel.empty:
        return []

    # --- 出力：分足・時間足のみ時刻付き ---
    need_time = interval.lower() in {"5m", "15m", "1h"}
    return to_json_records(sel, include_time=need_time)


@router.get("/prices/1d")
def core30_prices_1d(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    end: Optional[str] = Query(default=None, description="終了日（YYYY-MM-DD）"),
    start: Optional[str] = Query(default=None, description="開始日（YYYY-MM-DD）"),
):
    """既存エンドポイント（互換性維持）"""
    return core30_prices(ticker=ticker, interval="1d", end=end, start=start)


@router.get("/prices/snapshot/last2")
def core30_prices_snapshot_last2():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    # --- ボラ列を付与（prevClose, tr, atr14 など） ---
    g = _add_volatility_columns(out)

    # 既存の出来高10MAも維持
    g = g.sort_values(["ticker", "date"]).copy()
    if "Volume" in g.columns:
        g["vol_ma10"] = (
            g.groupby("ticker")["Volume"]
            .rolling(window=10, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    else:
        g["Volume"] = pd.NA
        g["vol_ma10"] = pd.NA

    snap = g.groupby("ticker", as_index=False).tail(1).copy()
    snap["diff"] = snap["Close"] - snap["prevClose"]
    snap["date"] = snap["date"].dt.strftime("%Y-%m-%d")

    def _none(v: Any):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (int, float)):
            return float(v)
        return v

    records: List[Dict[str, Any]] = []
    for _, r in snap.iterrows():
        records.append({
            "ticker": str(r["ticker"]),
            "date": r["date"],
            "close": _none(r["Close"]),
            "prevClose": _none(r["prevClose"]),
            "diff": _none(r["diff"]),
            "volume": _none(r["Volume"]),
            "vol_ma10": _none(r["vol_ma10"]),
            "tr": _none(r.get("tr")),
            "tr_pct": _none(r.get("tr_pct")),
            "atr14": _none(r.get("atr14")),
            "atr14_pct": _none(r.get("atr14_pct")),
        })
    return records
