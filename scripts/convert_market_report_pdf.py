#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML マーケットレポートを S3 にアップロードする。

Usage:
    python scripts/convert_market_report_pdf.py improvement/output/market_analysis_20260219.html
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import shutil

from common_cfg.paths import REPORTS_DIR
from common_cfg.s3cfg import S3Config, load_s3_config
from common_cfg.s3io import _init_s3_client

REPORTS_PREFIX = "reports/"


def _reports_cfg() -> S3Config:
    base = load_s3_config()
    return S3Config(
        bucket=base.bucket,
        prefix=REPORTS_PREFIX,
        region=base.region,
        profile=base.profile,
        endpoint_url=base.endpoint_url,
    )


def upload_to_s3(html_path: Path) -> None:
    """HTML を S3 にアップロードする。"""
    cfg = _reports_cfg()
    if not cfg.bucket:
        print("[WARN] S3 bucket が未設定のためアップロードをスキップしました。")
        return

    s3 = _init_s3_client(cfg)
    if s3 is None:
        return

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


def copy_to_data_reports(html_path: Path) -> Path:
    """HTML を data/reports/ にコピーする。"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = REPORTS_DIR / html_path.name
    shutil.copy2(str(html_path), str(dest))
    print(f"[OK] copied: {dest}")
    return dest


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <html_file>", file=sys.stderr)
        sys.exit(1)

    html_path = Path(sys.argv[1]).resolve()
    if not html_path.exists():
        print(f"[ERROR] ファイルが見つかりません: {html_path}", file=sys.stderr)
        sys.exit(1)

    copy_to_data_reports(html_path)
    upload_to_s3(html_path)


if __name__ == "__main__":
    main()
