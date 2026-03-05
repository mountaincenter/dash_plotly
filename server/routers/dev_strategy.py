# server/routers/dev_strategy.py
"""
戦略検証レポート一覧 + 表示 API
GET  /api/dev/strategy/granville            - レポート一覧
GET  /api/dev/strategy/granville/{fn}/view  - HTML 表示
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

router = APIRouter()

# ローカルのchaptersディレクトリ
CHAPTERS_DIR = ROOT / "strategy_verification" / "chapters"
_IS_LOCAL = CHAPTERS_DIR.exists()

# S3 設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
S3_PREFIX = "reports/strategy/granville/"


def _s3_client():
    import boto3

    if os.environ.get("AWS_PROFILE") == "":
        del os.environ["AWS_PROFILE"]
    return boto3.client("s3", region_name=AWS_REGION)


def _extract_title_from_html(html: str, fallback: str) -> str:
    m = re.search(r"<title>(.+?)</title>", html, re.IGNORECASE)
    return m.group(1).strip() if m else fallback


def _chapter_sort_key(filename: str) -> str:
    """ch01, ch02, ... の順にソート"""
    m = re.match(r"ch(\d+)", filename)
    return m.group(1) if m else filename


@router.get("/api/dev/strategy/granville")
def list_strategy_reports():
    if _IS_LOCAL:
        return {"reports": _list_local()}
    return {"reports": _list_s3()}


def _list_local() -> list[dict]:
    reports = []
    if not CHAPTERS_DIR.exists():
        return reports
    for chapter_dir in sorted(CHAPTERS_DIR.iterdir()):
        if not chapter_dir.is_dir():
            continue
        for f in chapter_dir.glob("*.html"):
            reports.append({
                "filename": f"{chapter_dir.name}/{f.name}",
                "chapter": chapter_dir.name,
                "title": _extract_title_from_html(
                    f.read_text(encoding="utf-8", errors="ignore"), f.name
                ),
                "size_bytes": f.stat().st_size,
            })
    reports.sort(key=lambda r: _chapter_sort_key(r["chapter"]))
    return reports


def _list_s3() -> list[dict]:
    reports = []
    try:
        s3 = _s3_client()
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            name = key.removeprefix(S3_PREFIX)
            if not name.endswith(".html") or "/" not in name:
                continue
            chapter = name.split("/")[0]
            title = name
            try:
                head = s3.get_object(
                    Bucket=S3_BUCKET, Key=key, Range="bytes=0-4095"
                )
                title = _extract_title_from_html(
                    head["Body"].read().decode("utf-8", errors="ignore"), name
                )
            except Exception:
                pass
            reports.append({
                "filename": name,
                "chapter": chapter,
                "title": title,
                "size_bytes": obj["Size"],
            })
        reports.sort(key=lambda r: _chapter_sort_key(r["chapter"]))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"S3 list failed: {exc}")
    return reports


@router.get("/api/dev/strategy/granville/{path:path}/view")
def view_strategy_report(path: str):
    if not path.endswith(".html"):
        raise HTTPException(status_code=400, detail="Invalid path")

    if _IS_LOCAL:
        local_path = CHAPTERS_DIR / path
        if not local_path.exists():
            raise HTTPException(status_code=404, detail=f"Not found: {path}")
        return HTMLResponse(
            content=local_path.read_text(encoding="utf-8"),
            media_type="text/html",
        )

    try:
        s3 = _s3_client()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}{path}")
        html = resp["Body"].read().decode("utf-8")
        return HTMLResponse(content=html, media_type="text/html")
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Not found: {exc}")
