from __future__ import annotations

from fastapi import APIRouter

from .core30.prices import (
    core30_prices_max_1d,
    core30_prices,
    core30_prices_1d,
    core30_prices_snapshot_last2,
)

router = APIRouter()

router.add_api_route(
    "/prices/max/1d",
    core30_prices_max_1d,
    methods=["GET"],
    summary="Get price history (max span, 1d interval)",
)

router.add_api_route(
    "/prices",
    core30_prices,
    methods=["GET"],
    summary="Get price series with interval selection",
)

router.add_api_route(
    "/prices/1d",
    core30_prices_1d,
    methods=["GET"],
    summary="Get daily price series (compat)",
)

router.add_api_route(
    "/prices/snapshot/last2",
    core30_prices_snapshot_last2,
    methods=["GET"],
    summary="Get latest snapshot with technical indicators",
)
