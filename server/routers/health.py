from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def health():
    return {
        "ok": True,
        "version": "2025-10-22-fix-grok-perf",  # デバッグ用バージョン
    }