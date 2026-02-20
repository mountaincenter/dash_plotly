#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML マーケットレポートを manifest に登録し S3 にアップロードする。

Usage:
    python scripts/convert_market_report_pdf.py improvement/output/market_analysis_20260219.html
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.s3cfg import S3Config, load_s3_config
from common_cfg.s3io import _init_s3_client


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
REPORTS_PREFIX = "reports/"
MANIFEST_KEY = "manifest.json"


# ---------------------------------------------------------------------------
# S3 設定
# ---------------------------------------------------------------------------
def _reports_cfg() -> S3Config:
    base = load_s3_config()
    return S3Config(
        bucket=base.bucket,
        prefix=REPORTS_PREFIX,
        region=base.region,
        profile=base.profile,
        endpoint_url=base.endpoint_url,
    )


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------
def _extract_date(filename: str) -> str:
    """ファイル名から YYYY-MM-DD を抽出。"""
    m = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return datetime.now().strftime("%Y-%m-%d")


def _extract_title(html_path: Path, date_str: str) -> str:
    """HTML の <title> からタイトルを取得。なければデフォルト。"""
    try:
        text = html_path.read_text(encoding="utf-8")
        m = re.search(r"<title>(.+?)</title>", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return f"マーケット振り返り {date_str}"


def _build_manifest_entry(html_path: Path) -> dict:
    """manifest 用のエントリを作成する。"""
    date_str = _extract_date(html_path.name)
    title = _extract_title(html_path, date_str)
    return {
        "filename": html_path.name,
        "date": date_str,
        "title": title,
        "size_bytes": html_path.stat().st_size,
        "uploaded_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", ""),
    }


def _update_manifest(existing: dict, entry: dict) -> dict:
    """既存 manifest にエントリを追加/更新して返す。"""
    reports: list[dict] = [
        r for r in existing.get("reports", [])
        if r.get("filename") != entry["filename"]
    ]
    reports.append(entry)
    reports.sort(key=lambda r: r.get("date", ""), reverse=True)
    return {"reports": reports}


# ---------------------------------------------------------------------------
# ローカル manifest 更新
# ---------------------------------------------------------------------------
def save_local_manifest(html_path: Path) -> None:
    """ローカルの reports_manifest.json を更新する。"""
    manifest_path = html_path.parent / "reports_manifest.json"

    existing: dict = {"reports": []}
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    entry = _build_manifest_entry(html_path)
    manifest = _update_manifest(existing, entry)

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[OK] local manifest updated: {manifest_path}")


# ---------------------------------------------------------------------------
# S3 アップロード
# ---------------------------------------------------------------------------
def upload_to_s3(html_path: Path) -> None:
    """HTML を S3 にアップロードし manifest.json を更新する。"""
    cfg = _reports_cfg()
    if not cfg.bucket:
        print("[WARN] S3 bucket が未設定のためアップロードをスキップしました。")
        return

    s3 = _init_s3_client(cfg)
    if s3 is None:
        return

    # HTML アップロード
    html_key = f"{cfg.prefix}{html_path.name}"
    try:
        s3.upload_file(str(html_path), cfg.bucket, html_key, ExtraArgs={
            "ContentType": "text/html; charset=utf-8",
            "CacheControl": "max-age=86400",
            "ServerSideEncryption": "AES256",
        })
        print(f"[OK] uploaded: s3://{cfg.bucket}/{html_key}")
    except Exception as e:
        print(f"[ERROR] upload failed: {e}", file=sys.stderr)
        return

    # S3 manifest.json 読み込み → 更新 → 再アップロード
    manifest_key = f"{cfg.prefix}{MANIFEST_KEY}"
    existing: dict = {"reports": []}
    try:
        resp = s3.get_object(Bucket=cfg.bucket, Key=manifest_key)
        existing = json.loads(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        pass
    except Exception:
        pass

    entry = _build_manifest_entry(html_path)
    manifest = _update_manifest(existing, entry)

    try:
        s3.put_object(
            Bucket=cfg.bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="max-age=60",
            ServerSideEncryption="AES256",
        )
        print(f"[OK] manifest updated: s3://{cfg.bucket}/{manifest_key}")
    except Exception as e:
        print(f"[WARN] manifest update failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <html_file>", file=sys.stderr)
        sys.exit(1)

    html_path = Path(sys.argv[1]).resolve()
    if not html_path.exists():
        print(f"[ERROR] ファイルが見つかりません: {html_path}", file=sys.stderr)
        sys.exit(1)

    save_local_manifest(html_path)
    upload_to_s3(html_path)

    # App Runner キャッシュリフレッシュ
    import urllib.request
    import urllib.error

    API_URL = "https://muuq3bv2n2.ap-northeast-1.awsapprunner.com/api/dev/reports/refresh"
    try:
        req = urllib.request.Request(API_URL, method="POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            result = response.read().decode("utf-8")
            print(f"[OK] キャッシュリフレッシュ完了: {result}")
    except (urllib.error.URLError, Exception) as e:
        print(f"[WARNING] キャッシュリフレッシュ失敗: {e}")


if __name__ == "__main__":
    main()
