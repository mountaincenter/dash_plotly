from __future__ import annotations

from fastapi import APIRouter
from ...utils import load_core30_meta

router = APIRouter()

@router.get("/meta")
def core30_meta():
    data = load_core30_meta()
    return data if data is not None else []
