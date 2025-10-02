from fastapi import APIRouter

# サブモジュールの各 router を取り込んで集約
from .meta import router as meta_router
from .prices import router as prices_router
from .perf import router as perf_router
from .tech import router as tech_router

router = APIRouter()
router.include_router(meta_router)
router.include_router(prices_router)
router.include_router(perf_router)
router.include_router(tech_router)
