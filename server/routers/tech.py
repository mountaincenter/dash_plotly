from __future__ import annotations

from typing import Any, Dict, List
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..utils import read_tech_snapshot_df

router = APIRouter()

@router.get("/tech/decision/snapshot", summary="Get precomputed technical decision snapshot for all tickers")
def tech_decision_snapshot() -> List[Dict[str, Any]]:
    """事前計算された全銘柄のテクニカル指標スナップショットを返す"""
    df = read_tech_snapshot_df()
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/tech/decision", summary="Get technical decision snapshot for a ticker")
def tech_decision(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
) -> Dict[str, Any]:
    """指定された銘柄のテクニカル指標スナップショットを返す"""
    t = (ticker or "").strip()
    if not t:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    df = read_tech_snapshot_df()
    if df is None or df.empty:
        return JSONResponse(content={"error": "snapshot data not found"}, status_code=404)

    record_df = df[df["ticker"] == t]
    if record_df.empty:
        return JSONResponse(content={"error": f"ticker '{t}' not found in snapshot"}, status_code=404)

    return record_df.to_dict(orient="records")[0]
