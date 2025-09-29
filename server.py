# server.py （FastAPI 互換ラッパ）
from server.main import app  # FastAPI インスタンスを公開

if __name__ == "__main__":
    import os
    import uvicorn
    uvicorn.run(
        "server.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )