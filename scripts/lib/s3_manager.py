#!/usr/bin/env python3
"""
S3アップロード管理モジュール
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.s3cfg import S3Config, load_s3_config
from common_cfg.s3io import upload_files


def upload_to_s3(files: List[Path]) -> bool:
    """
    ファイルをS3にアップロード

    Args:
        files: アップロードするファイルのリスト

    Returns:
        成功時True、失敗時False
    """
    try:
        cfg = load_s3_config()
        bucket = cfg.bucket or "dash-plotly"
        prefix = cfg.prefix or "parquet/"

        s3_cfg = S3Config(
            bucket=bucket,
            prefix=prefix,
            region=cfg.region,
            profile=cfg.profile,
            endpoint_url=cfg.endpoint_url,
        )

        if not s3_cfg.bucket:
            print("[WARN] S3 bucket not configured; upload skipped.")
            return False

        upload_files(s3_cfg, files)
        print(f"[OK] Uploaded {len(files)} files to S3")
        return True

    except Exception as e:
        print(f"[ERROR] S3 upload failed: {e}")
        return False
