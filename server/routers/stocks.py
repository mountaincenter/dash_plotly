from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from ..utils import load_master_meta

router = APIRouter()


@router.get("")
def list_stocks(tag: Optional[str] = Query(default=None, description="Filter by primary tag (e.g., takaichi, TOPIX_CORE30)")):
    data = load_master_meta(tag=tag)
    return data if data is not None else []
