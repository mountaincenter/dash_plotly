from __future__ import annotations

import json
from typing import Optional
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, FileResponse

# 👇 utils は相対 import
from ..utils import (
    safe_demo_path,
    filter_bb_payload,
    parse_date_param,
    normalize_prices,
    to_json_records,
)

router = APIRouter()

@router.get("/demo/json/{fname:path}")
def demo_json_file(fname: str):
    if not fname.lower().endswith(".json"):
        fname = fname + ".json"
    p = safe_demo_path(fname, {".json"})
    if not p:
        return JSONResponse(content={"error": "not found"}, status_code=404)
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/demo/ichimoku/3350T")
def demo_ichimoku_3350t():
    return demo_json_file("ichimoku_3350T_demo.json")

@router.get("/demo/bb/3350T")
def demo_bb_3350t(
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
):
    p = safe_demo_path("bb_3350T_demo.json", {".json"})
    if not p:
        return JSONResponse(content={"error": "not found"}, status_code=404)
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

    start_dt = parse_date_param(start)
    end_dt = parse_date_param(end)
    if start_dt and end_dt and start_dt > end_dt:
        return JSONResponse(content={"error": "start must be <= end"}, status_code=400)

    return filter_bb_payload(data, start_dt, end_dt)

@router.get("/demo/bb30/3350T")
def demo_bb30_3350t():
    return demo_json_file("bb_3350T_demo_30d.json")

@router.get("/demo/dow-tod/3350T")
def demo_dow_tod_3350t():
    return demo_json_file("dow_tod_onecell_3350T.json")

@router.get("/demo/parquet/{fname:path}")
def demo_parquet_download(fname: str):
    if not fname.lower().endswith(".parquet"):
        fname = fname + ".parquet"
    p = safe_demo_path(fname, {".parquet"})
    if not p:
        return JSONResponse(content={"error": "not found"}, status_code=404)
    return FileResponse(path=str(p), media_type="application/octet-stream", filename=p.name)

@router.get("/demo/prices/max/1d/{code}")
def demo_prices_max_1d_json(
    code: str,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
):
    code = code.upper().replace(".T", "T")
    fname = f"prices_max_1d_{code}_demo.parquet"
    p = safe_demo_path(fname, {".parquet"})
    if not p:
        return []
    try:
        df = pd.read_parquet(str(p), engine="pyarrow")
        df = normalize_prices(df)
        if df is None or df.empty:
            return []
        end_dt = pd.to_datetime(end).tz_localize(None) if end else None
        start_dt = pd.to_datetime(start).tz_localize(None) if start else None
        if start_dt is not None:
            df = df[df["date"] >= start_dt]
        if end_dt is not None:
            df = df[df["date"] <= end_dt]
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str)
        return to_json_records(df)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
