# server/routers/dev_reports.py
"""
レポート一覧 + 表示 + ダウンロード API
GET /api/dev/reports                      - レポート一覧
GET /api/dev/reports/{filename}/view      - HTML 表示
GET /api/dev/reports/{filename}/download  - HTML ダウンロード
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, HTMLResponse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

router = APIRouter()

# ローカルパス
REPORTS_LOCAL_DIR = ROOT / "improvement" / "output"
MANIFEST_LOCAL = REPORTS_LOCAL_DIR / "reports_manifest.json"

# S3 設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
REPORTS_PREFIX = "reports/"


def _s3_client():
    import boto3

    if os.environ.get("AWS_PROFILE") == "":
        del os.environ["AWS_PROFILE"]

    kwargs: dict = {"region_name": AWS_REGION}
    if AWS_ENDPOINT_URL:
        kwargs["endpoint_url"] = AWS_ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def _resolve_html_name(filename: str) -> str:
    """filename が .pdf でも .html でも HTML ファイル名を返す。"""
    return filename.replace(".pdf", ".html") if filename.endswith(".pdf") else filename


@router.get("/api/dev/reports")
def list_reports():
    """レポート一覧を返す。ローカル優先、なければS3。"""
    # ローカル
    if MANIFEST_LOCAL.exists():
        try:
            manifest = json.loads(MANIFEST_LOCAL.read_text(encoding="utf-8"))
            return manifest
        except Exception as e:
            print(f"[WARN] local manifest read failed: {e}")

    # S3 フォールバック
    try:
        s3 = _s3_client()
        resp = s3.get_object(
            Bucket=S3_BUCKET, Key=f"{REPORTS_PREFIX}manifest.json"
        )
        manifest = json.loads(resp["Body"].read().decode("utf-8"))
        return manifest
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"manifest read failed: {exc}")


@router.get("/api/dev/reports/{filename}/view")
def view_report(filename: str):
    """HTML レポートを返す。"""
    html_name = _resolve_html_name(filename)
    local_path = REPORTS_LOCAL_DIR / html_name
    if local_path.exists():
        return HTMLResponse(
            content=local_path.read_text(encoding="utf-8"),
            media_type="text/html",
        )

    # S3 フォールバック
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

    # S3 フォールバック
    try:
        s3 = _s3_client()
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": f"{REPORTS_PREFIX}{html_name}"},
            ExpiresIn=3600,
        )
        return {"url": url}
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"download failed: {exc}"
        )
