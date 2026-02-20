# server/routers/dev_reports.py
"""
レポート一覧 + 表示 + ダウンロード API
GET  /api/dev/reports                      - レポート一覧
GET  /api/dev/reports/{filename}/view      - HTML 表示
GET  /api/dev/reports/{filename}/download  - HTML ダウンロード
POST /api/dev/reports/refresh              - キャッシュリフレッシュ
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, HTMLResponse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

# ローカルパス
REPORTS_LOCAL_DIR = ROOT / "improvement" / "output"
MANIFEST_LOCAL = REPORTS_LOCAL_DIR / "reports_manifest.json"

# S3 設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
REPORTS_PREFIX = "reports/"

# キャッシュ（stock_results と同パターン）
_manifest_cache: Optional[dict] = None
_cache_timestamp: Optional[datetime] = None
CACHE_TTL_SECONDS = 60


def _clear_cache():
    global _manifest_cache, _cache_timestamp
    _manifest_cache = None
    _cache_timestamp = None


def _s3_client():
    import boto3

    if os.environ.get("AWS_PROFILE") == "":
        del os.environ["AWS_PROFILE"]

    kwargs: dict = {"region_name": AWS_REGION}
    return boto3.client("s3", **kwargs)


def _resolve_html_name(filename: str) -> str:
    return filename.replace(".pdf", ".html") if filename.endswith(".pdf") else filename


def _load_manifest() -> dict:
    """
    manifest を読み込み。
    ローカルファイルを最優先（開発環境）、なければS3（本番環境）。
    """
    global _manifest_cache, _cache_timestamp

    # キャッシュチェック
    if _manifest_cache is not None and _cache_timestamp is not None:
        if (datetime.now() - _cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
            return _manifest_cache

    # ローカルファイルを最優先
    if MANIFEST_LOCAL.exists():
        try:
            manifest = json.loads(MANIFEST_LOCAL.read_text(encoding="utf-8"))
            _manifest_cache = manifest
            _cache_timestamp = datetime.now()
            return manifest
        except Exception as e:
            print(f"[ERROR] local manifest read failed: {e}")

    # S3 から読み込み
    try:
        s3 = _s3_client()
        resp = s3.get_object(
            Bucket=S3_BUCKET, Key=f"{REPORTS_PREFIX}manifest.json"
        )
        manifest = json.loads(resp["Body"].read().decode("utf-8"))
        _manifest_cache = manifest
        _cache_timestamp = datetime.now()
        return manifest
    except Exception as e:
        print(f"[ERROR] S3 manifest read failed: {e}")

    return {"reports": []}


@router.get("/api/dev/reports")
def list_reports():
    """レポート一覧を返す。"""
    return _load_manifest()


@router.get("/api/dev/reports/{filename}/view")
def view_report(filename: str):
    """HTML レポートを返す。ローカル優先、なければS3。"""
    html_name = _resolve_html_name(filename)

    # ローカル
    local_path = REPORTS_LOCAL_DIR / html_name
    if local_path.exists():
        return HTMLResponse(
            content=local_path.read_text(encoding="utf-8"),
            media_type="text/html",
        )

    # S3
    try:
        s3 = _s3_client()
        resp = s3.get_object(
            Bucket=S3_BUCKET, Key=f"{REPORTS_PREFIX}{html_name}"
        )
        html = resp["Body"].read().decode("utf-8")
        return HTMLResponse(content=html, media_type="text/html")
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"HTML not found: {exc}")


@router.get("/api/dev/reports/{filename}/download")
def download_report(filename: str):
    """HTML をダウンロード。ローカル優先、なければ presigned URL。"""
    html_name = _resolve_html_name(filename)
    if not html_name.endswith(".html"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    # ローカル
    local_path = REPORTS_LOCAL_DIR / html_name
    if local_path.exists():
        return FileResponse(
            path=str(local_path),
            media_type="text/html; charset=utf-8",
            filename=html_name,
        )

    # S3 presigned URL
    try:
        s3 = _s3_client()
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": f"{REPORTS_PREFIX}{html_name}"},
            ExpiresIn=3600,
        )
        return {"url": url}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"download failed: {exc}")


@router.post("/api/dev/reports/refresh")
async def refresh_reports():
    """
    キャッシュをクリアして最新データを再読み込み。
    S3アップロード後に呼び出すことでリアルタイム反映。
    """
    _clear_cache()
    manifest = _load_manifest()
    count = len(manifest.get("reports", []))
    return {
        "status": "success",
        "message": "Cache refreshed",
        "count": count,
        "updated_at": datetime.now().isoformat(),
    }
