# server/routers/core30/__init__.py
from fastapi import APIRouter
from .meta import router as meta_router
from .prices import router as prices_router
from .perf import router as perf_router
from .tech_v2 import router as tech_v2_router   # v2のみ

router = APIRouter()
router.include_router(meta_router)
router.include_router(prices_router)
router.include_router(perf_router)
router.include_router(tech_v2_router)  # /core30/tech/decision, .../snapshot