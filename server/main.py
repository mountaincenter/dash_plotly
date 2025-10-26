# server/main.py
from fastapi import FastAPI, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from server.routers.health import router as health_router
from server.routers.stocks import router as stocks_router
from server.routers.prices import router as prices_router
from server.routers.tech import router as tech_router
from server.routers.scalping import router as scalping_router
from server.routers.grok import router as grok_router

import os

app = FastAPI(title="Market Data API", version="1.0.0")

# === 圧縮（1KB以上を自動gzip） ===
app.add_middleware(GZipMiddleware, minimum_size=1024)

# === CORS（既存そのまま） ===
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

# === 起動時の軽量ウォームアップ（S3接続の確立を早め、初回TTFBスパイクを抑制） ===
@app.on_event("startup")
async def _warmup():
    try:
        import boto3  # 依存は既存前提
        region = os.getenv("AWS_REGION") or "ap-northeast-1"
        endpoint = os.getenv("AWS_ENDPOINT_URL")  # 任意
        session = boto3.Session()
        s3 = session.client("s3", region_name=region, endpoint_url=endpoint)
        # 失敗してもサービス起動は続行（ネットワーク事情で失敗する可能性を考慮）
        _ = s3.list_buckets()
    except Exception:
        pass

# === キャッシュ系ヘッダの付与（/prices, /tech, /stocks, /demo のGETのみ） ===
# 既にハンドラ側で Cache-Control/ETag を設定している場合はそれを尊重
@app.middleware("http")
async def _cache_headers(request: Request, call_next):
    response: Response = await call_next(request)
    p = request.url.path or ""
    if request.method == "GET" and (
        p.startswith("/stocks")
        or p.startswith("/prices")
        or p.startswith("/tech")
        or p.startswith("/scalping")
    ):
        # 既存ヘッダが無ければ付与（stale-while-revalidateで中間キャッシュを活用）
        if "cache-control" not in {k.lower() for k in response.headers.keys()}:
            response.headers["Cache-Control"] = "public, max-age=300, stale-while-revalidate=60"
        # 変更しない: ETag/Last-Modified は各エンドポイント側で付与していればそのまま使用
    return response

# === ルーター登録 ===
app.include_router(health_router)
app.include_router(stocks_router, prefix="/stocks", tags=["stocks"])
app.include_router(prices_router, tags=["prices"])
app.include_router(tech_router, tags=["tech"])
app.include_router(scalping_router, prefix="/scalping", tags=["scalping"])
app.include_router(grok_router, tags=["grok"])
