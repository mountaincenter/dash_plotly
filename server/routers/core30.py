from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

# 👇 utils は server 配下なので相対 import
from ..utils import (
    load_core30_meta,
    read_prices_1d_df,
    normalize_prices,
    to_json_records,
)

router = APIRouter()

@router.get("/meta")
def core30_meta():
    data = load_core30_meta()
    return data if data is not None else []

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

@router.get("/perf/returns")
def core30_perf_returns(
    windows: Optional[str] = Query(default=None, description="例: 5d,1mo,3mo,ytd,1y,5y,all"),
):
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    win_param = (windows or "").strip()
    default_wins = ["5d", "1mo", "3mo", "ytd", "1y", "5y", "all"]
    wins = [w.strip() for w in win_param.split(",") if w.strip()] or default_wins

    days_map = {"5d": 7, "1w": 7, "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "3y": 365*3, "5y": 365*5}

    def pct_return(last_close, base_close):
        if last_close is None or base_close is None or pd.isna(last_close) or pd.isna(base_close) or base_close == 0:
            return None
        return float((float(last_close) / float(base_close) - 1.0) * 100.0)

    records: List[Dict[str, Any]] = []
    for tkr, g in out.sort_values(["ticker", "date"]).groupby("ticker", as_index=False):
        g = g[["date", "Close"]].dropna(subset=["Close"])
        if g.empty:
            continue
        last_row = g.iloc[-1]
        last_date = last_row["date"]
        last_close = float(last_row["Close"])

        def base_close_before_or_on(target_dt: pd.Timestamp):
            sel = g[g["date"] <= target_dt]
            if sel.empty:
                return None
            return float(sel.iloc[-1]["Close"])

        row: Dict[str, Any] = {"ticker": tkr, "date": last_date.strftime("%Y-%m-%d")}
        for w in wins:
            key = f"r_{w}"
            if w == "ytd":
                start_of_year = pd.Timestamp(year=last_date.year, month=1, day=1)
                base = base_close_before_or_on(start_of_year)
                row[key] = pct_return(last_close, base)
            elif w == "all":
                base = float(g.iloc[0]["Close"])
                row[key] = pct_return(last_close, base)
            else:
                days = days_map.get(w)
                if not days:
                    row[key] = None
                else:
                    target = last_date - pd.Timedelta(days=days)
                    base = base_close_before_or_on(target)
                    row[key] = pct_return(last_close, base)
        records.append(row)
    return records
