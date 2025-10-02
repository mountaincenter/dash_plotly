from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ...utils import (
    read_prices_1d_df,
    normalize_prices,
    to_json_records,
)

router = APIRouter()

@router.get("/prices/max/1d")
def core30_prices_max_1d():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []
    return to_json_records(out)

@router.get("/prices/1d")
def core30_prices_1d(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    end: Optional[str] = Query(default=None, description="終了日（YYYY-MM-DD）"),
    start: Optional[str] = Query(default=None, description="開始日（YYYY-MM-DD）"),
):
    ticker = (ticker or "").strip()
    if not ticker:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    try:
        end_dt = pd.to_datetime(end).tz_localize(None) if end else today
    except Exception:
        return JSONResponse(content={"error": "invalid end"}, status_code=400)
    try:
        start_dt = pd.to_datetime(start).tz_localize(None) if start else end_dt - pd.Timedelta(days=365)
    except Exception:
        return JSONResponse(content={"error": "invalid start"}, status_code=400)
    if start_dt > end_dt:
        return JSONResponse(content={"error": "start must be <= end"}, status_code=400)

    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    sel = out[
        (out["ticker"] == ticker) &
        (out["date"] >= start_dt) &
        (out["date"] <= end_dt)
    ]
    if sel.empty:
        return []
    return to_json_records(sel)

@router.get("/prices/snapshot/last2")
def core30_prices_snapshot_last2():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    g = out.sort_values(["ticker", "date"]).copy()
    g["prevClose"] = g.groupby("ticker")["Close"].shift(1)

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
        })
    return records
