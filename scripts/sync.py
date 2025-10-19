#!/usr/bin/env python3
"""
S3同期コマンド

使い方:
  python scripts/sync.py                    # 全ファイル同期（S3 → ローカル）
  python scripts/sync.py --dry-run          # 確認のみ（ダウンロードしない）
  python scripts/sync.py meta.parquet       # 特定ファイルのみ同期
  python scripts/sync.py meta.parquet prices_max_1d.parquet  # 複数指定
"""

from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# scripts/sync/download_from_s3.py のmain()を実行
from scripts.sync.download_from_s3 import main

if __name__ == "__main__":
    raise SystemExit(main())
