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
    to_json_records,  # 既存利用箇所のため残置（価格APIでは未使用）
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


# ====== 価格データ（period/interval/コード可変・*_demo フォールバック無し） ======

def _load_and_respond_prices(
    *,
    period: str,
    interval: str,
    code: str,
    start: Optional[str],
    end: Optional[str],
):
    """
    demo_data から parquet を読み出して JSON で返却。
    *_demo へのフォールバックは行わず、
    prices_{period}_{interval}_{CODE}.parquet のみを対象とする。
    見つからない場合は空配列 [] を返す。
    """
    code = code.upper().replace(".T", "T")
    name = f"prices_{period}_{interval}_{code}.parquet"

    p = safe_demo_path(name, {".parquet"})
    if not p:
        return []  # 見つからない場合は空配列（既存の挙動に合わせる）

    try:
        df = pd.read_parquet(str(p), engine="pyarrow")
        df = normalize_prices(df)
        if df is None or df.empty:
            return []

        # --- フィルタ（日付の上限は“日付のみ”指定なら翌日0時未満で絞る）---
        start_dt = parse_date_param(start) if start else None
        end_dt = parse_date_param(end) if end else None

        if start_dt is not None:
            df = df[df["date"] >= start_dt]

        if end_dt is not None:
            is_date_only = end is not None and len(end.strip()) <= 10 and "T" not in end and ":" not in end
            if is_date_only:
                end_exclusive = end_dt + pd.Timedelta(days=1)
                df = df[df["date"] < end_exclusive]
            else:
                df = df[df["date"] <= end_dt]
        # --------------------------------------------------------------------

        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str)

        # ---- 出力整形（intraday は ISO 日時、daily は日付のみ）----
        # 厳密昇順 & time 重複排除（LWC の time 一意制約に対応）
        df = df.sort_values("date").dropna(subset=["date"])

        il = (interval or "").strip().lower()
        # 1d / Nd 判定（末尾 d を daily とみなす）
        is_daily = il == "1d" or il.endswith("d")

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")

        if is_daily:
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        else:
            df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        cols = [c for c in ["date", "Open", "High", "Low", "Close", "Volume", "ticker"] if c in df.columns]
        return df[cols].to_dict(orient="records")
        # ---- 出力整形 ここまで ----

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/demo/prices/{period}/{interval}/{code}")
def demo_prices_generic(
    period: str,
    interval: str,
    code: str,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
):
    """
    例:
      /demo/prices/60d/5m/3350T
      /demo/prices/60d/15m/3350T
      /demo/prices/max/1h/3350T
      /demo/prices/max/1d/3350T
    """
    return _load_and_respond_prices(
        period=period, interval=interval, code=code, start=start, end=end
    )


# 既存の固定パスも維持（*_demo へはフォールバックしない）
@router.get("/demo/prices/max/1d/{code}")
def demo_prices_max_1d_json(
    code: str,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
):
    return _load_and_respond_prices(
        period="max", interval="1d", code=code, start=start, end=end
    )
