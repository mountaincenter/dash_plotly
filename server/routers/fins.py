# server/routers/fins.py
"""
J-Quants財務データエンドポイント
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi_cache.decorator import cache

router = APIRouter(prefix="/fins")


def get_jquants_client():
    """JQuantsClientのインスタンスを取得"""
    scripts_path = Path(__file__).resolve().parents[2] / "scripts"
    if str(scripts_path) not in sys.path:
        sys.path.insert(0, str(scripts_path))

    from lib.jquants_client import JQuantsClient
    return JQuantsClient()


@router.get("/summary/{ticker}")
@cache(expire=3600)
async def get_financial_summary(ticker: str) -> dict[str, Any]:
    """銘柄の財務サマリーを取得"""
    code = ticker.replace(".T", "").strip()

    try:
        client = get_jquants_client()
        response = client.request("/fins/summary", params={"code": code})
        data = response.get("data", [])

        if not data:
            raise HTTPException(status_code=404, detail=f"Financial data not found for {ticker}")

        # 最新データを取得（配列の最後）
        latest = data[-1]

        def to_oku(val: Any) -> float | None:
            """円単位を億円に変換"""
            if val is None or val == "":
                return None
            try:
                return round(float(val) / 100_000_000, 1)
            except (ValueError, TypeError):
                return None

        def to_float(val: Any) -> float | None:
            if val is None or val == "":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        return {
            "ticker": ticker,
            "fiscalPeriod": latest.get("CurPerType"),
            "periodEnd": latest.get("CurPerEn"),
            "disclosureDate": latest.get("DiscDate"),
            "sales": to_oku(latest.get("Sales")),
            "operatingProfit": to_oku(latest.get("OP")),
            "ordinaryProfit": to_oku(latest.get("OdP")),
            "netProfit": to_oku(latest.get("NP")),
            "eps": to_float(latest.get("EPS")),
            "totalAssets": to_oku(latest.get("TA")),
            "equity": to_oku(latest.get("Eq")),
            "equityRatio": to_float(latest.get("EqAR")),
            "bps": to_float(latest.get("BPS")),
            "sharesOutstanding": to_float(latest.get("ShOutFY")),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch financial data: {str(e)}")
