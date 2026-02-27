# server/routers/dev_reports.py
"""
レポート一覧 + 表示 + ダウンロード API
GET  /api/dev/reports                      - レポート一覧
GET  /api/dev/reports/{filename}/view      - HTML 表示
GET  /api/dev/reports/{filename}/download  - HTML ダウンロード
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, HTMLResponse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import REPORTS_DIR

router = APIRouter()

# 環境判定: ローカルにディレクトリがあれば開発、なければ本番（S3）
_IS_LOCAL = REPORTS_DIR.exists()

# S3 設定（本番用）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
REPORTS_PREFIX = "reports/"


def _s3_client():
    import boto3

    if os.environ.get("AWS_PROFILE") == "":
        del os.environ["AWS_PROFILE"]

    return boto3.client("s3", region_name=AWS_REGION)


def _extract_date(filename: str) -> str:
    """ファイル名から YYYY-MM-DD を抽出。月次も最終営業日YYYYMMDD形式。"""
    m = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def _extract_title_from_html(html: str, filename: str) -> str:
    """HTML の <title> からタイトルを取得。"""
    m = re.search(r"<title>(.+?)</title>", html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return filename


def _resolve_html_name(filename: str) -> str:
    return filename.replace(".pdf", ".html") if filename.endswith(".pdf") else filename


@router.get("/api/dev/reports")
def list_reports():
    """レポート一覧。開発=ローカル、本番=S3。完全分離。"""
    if _IS_LOCAL:
        return {"reports": _list_local()}
    return {"reports": _list_s3()}


def _list_local() -> list[dict]:
    reports = []
    for f in REPORTS_DIR.glob("market_analysis*.html"):
        reports.append({
            "filename": f.name,
            "date": _extract_date(f.name),
            "title": _extract_title_from_html(
                f.read_text(encoding="utf-8", errors="ignore"), f.name
            ),
            "size_bytes": f.stat().st_size,
        })
    reports.sort(key=lambda r: r.get("date", ""), reverse=True)
    return reports


def _list_s3() -> list[dict]:
    reports = []
    try:
        s3 = _s3_client()
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=REPORTS_PREFIX)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            name = key.removeprefix(REPORTS_PREFIX)
            if not name.endswith(".html"):
                continue
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
                "date": _extract_date(name),
                "title": title,
                "size_bytes": obj["Size"],
            })
        reports.sort(key=lambda r: r.get("date", ""), reverse=True)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"S3 list failed: {exc}")
    return reports


@router.get("/api/dev/reports/{filename}/view")
def view_report(filename: str):
    """HTML レポートを返す。開発=ローカル、本番=S3。"""
    html_name = _resolve_html_name(filename)

    if _IS_LOCAL:
        local_path = REPORTS_DIR / html_name
        if not local_path.exists():
            raise HTTPException(status_code=404, detail=f"Not found: {html_name}")
        return HTMLResponse(
            content=local_path.read_text(encoding="utf-8"),
            media_type="text/html",
        )

    try:
        s3 = _s3_client()
        resp = s3.get_object(
            Bucket=S3_BUCKET, Key=f"{REPORTS_PREFIX}{html_name}"
        )
        html = resp["Body"].read().decode("utf-8")
        return HTMLResponse(content=html, media_type="text/html")
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Not found: {exc}")


@router.get("/api/dev/reports/{filename}/download")
def download_report(filename: str):
    """HTML をダウンロード。開発=FileResponse、本番=presigned URL。"""
    html_name = _resolve_html_name(filename)
    if not html_name.endswith(".html"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    if _IS_LOCAL:
        local_path = REPORTS_DIR / html_name
        if not local_path.exists():
            raise HTTPException(status_code=404, detail=f"Not found: {html_name}")
        return FileResponse(
            path=str(local_path),
            media_type="text/html; charset=utf-8",
            filename=html_name,
        )

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
