from __future__ import annotations

from fastapi import APIRouter

from .core30.tech_v2 import (
    core30_tech_decision_snapshot,
    core30_tech_decision,
)

router = APIRouter()

router.add_api_route(
    "/tech/decision/snapshot",
    core30_tech_decision_snapshot,
    methods=["GET"],
    summary="Get precomputed technical decision snapshot for all tickers",
)

router.add_api_route(
    "/tech/decision",
    core30_tech_decision,
    methods=["GET"],
    summary="Get technical decision snapshot for a ticker",
)
