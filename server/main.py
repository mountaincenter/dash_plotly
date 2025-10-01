# server/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.routers.core30 import router as core30_router
from server.routers.health import router as health_router
from server.routers.demo import router as demo_router

app = FastAPI(title="Core30 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://192.168.0.20:3000",  # ← PCのLAN IPに置き換え
        "https://stock-frontend-sigma.vercel.app",
        "https://ymnk.jp",
        "https://www.ymnk.jp",
    ],
    allow_origin_regex=r"^https://([a-z0-9-]+\.)?vercel\.app$",
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(core30_router, prefix="/core30", tags=["core30"])
app.include_router(demo_router)
