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
        response = client.request("/fins/summary", params={"LocalCode": code})
        summaries = response.get("summary", [])

        if not summaries:
            raise HTTPException(status_code=404, detail=f"Financial data not found for {ticker}")

        latest = summaries[0]

        def to_oku(val: Any) -> float | None:
            if val is None:
                return None
            try:
                return round(float(val) / 100, 1)
            except (ValueError, TypeError):
                return None

        return {
            "ticker": ticker,
            "fiscalYear": latest.get("FiscalYear"),
            "fiscalQuarter": latest.get("FiscalQuarter"),
            "disclosureDate": latest.get("DisclosedDate"),
            "sales": to_oku(latest.get("NetSales")),
            "operatingProfit": to_oku(latest.get("OperatingProfit")),
            "ordinaryProfit": to_oku(latest.get("OrdinaryProfit")),
            "netProfit": to_oku(latest.get("Profit")),
            "eps": latest.get("EarningsPerShare"),
            "totalAssets": to_oku(latest.get("TotalAssets")),
            "equity": to_oku(latest.get("Equity")),
            "equityRatio": latest.get("EquityToAssetRatio"),
            "bps": latest.get("BookValuePerShare"),
            "sharesOutstanding": latest.get("NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch financial data: {str(e)}")
