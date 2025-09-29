# server/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ← ディレクトリに合わせて "routers" を使う（絶対import）
from server.routers.core30 import router as core30_router
from server.routers.health import router as health_router
from server.routers.demo import router as demo_router  # 使わないなら後でコメント可

app = FastAPI(title="Core30 API", version="1.0.0")

# Next.js dev からのCORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ルータ登録（フロント互換のパス）
app.include_router(health_router)                                   # /health
app.include_router(core30_router, prefix="/core30", tags=["core30"]) # /core30/...
app.include_router(demo_router)                                      # /demo/...
