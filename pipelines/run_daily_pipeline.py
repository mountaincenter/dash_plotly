#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_pipeline.py
- 1) analyze/csv_to_parquet_topixweight.py（CSV変更時のみ）
- 2) analyze/fetch_core30_yf.ipynb
- 3) analyze/anomaly.ipynb
- 各処理の manifest/S3 は PIPELINE_NO_* で抑止し、最後に manifest を一括生成→S3へ
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import argparse
import json
import os
import shlex
import subprocess
import sys
from typing import List, Optional

# ---- 共有ユーティリティ ----
from common_cfg.paths import (
    PARQUET_DIR,
    CORE30_ANOMALY_PARQUET as OUT_ANOMALY,
    CORE30_META_PARQUET as OUT_CORE30_META,
    CORE30_PRICES_PARQUET as OUT_CORE30_1D,
    TOPIX_WEIGHT_PARQUET as OUT_TOPIX,
    MANIFEST_JSON as MANIFEST_PATH,
)
from common_cfg.manifest import sha256_of, write_manifest_atomic
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_files

ROOT = Path(".").resolve()
CSV_DIR = ROOT / "data" / "csv"
INPUT_TOPIX_CSV = CSV_DIR / "topixweight_j.csv"

ANALYZE_DIR    = ROOT / "analyze"
TOPIX_PY       = ANALYZE_DIR / "csv_to_parquet_topixweight.py"
CORE30_IPYNB   = ANALYZE_DIR / "fetch_core30_yf.ipynb"
ANOMALY_IPYNB  = ANALYZE_DIR / "anomaly.ipynb"

STATE_DIR     = PARQUET_DIR / "_state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
CSV_HASH_FILE = STATE_DIR / "topixweight_j.csv.sha256"


# 先頭付近の既存 import の下にある _run_cmd をこの実装に差し替え
def _run_cmd(cmd, cwd: Optional[Path] = None, extra_env: Optional[dict] = None) -> None:
    env = os.environ.copy()
    # ★ ここがポイント：プロジェクトルートを PYTHONPATH に先頭追加
    root = str(ROOT)
    current_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{root}{os.pathsep}{current_pp}" if current_pp else root

    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})

    print(f"[CMD] {shlex.join(cmd)}")
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, env=env)


def _run_py_script(script: Path, extra_env: Optional[dict] = None) -> None:
    if not script.exists():
        raise FileNotFoundError(f"not found: {script}")
    _run_cmd([sys.executable, str(script)], extra_env=extra_env)


def _run_notebook(nb_path: Path, extra_env: Optional[dict] = None) -> None:
    if not nb_path.exists():
        raise FileNotFoundError(f"not found: {nb_path}")
    executed = nb_path.with_name(nb_path.stem + "_executed.ipynb")
    _run_cmd([
        "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute", str(nb_path),
        "--output", str(executed.name),
        "--ExecutePreprocessor.timeout=0"
    ], cwd=nb_path.parent, extra_env=extra_env)


def _should_run_topix(force: bool) -> bool:
    if force:
        return True
    if not INPUT_TOPIX_CSV.exists():
        print("[WARN] 入力CSVが見つからないため topixweight 変換はスキップします。")
        return False
    new_hash = sha256_of(INPUT_TOPIX_CSV)
    old_hash = CSV_HASH_FILE.read_text().strip() if CSV_HASH_FILE.exists() else ""
    return new_hash != old_hash


def _record_csv_hash():
    if INPUT_TOPIX_CSV.exists():
        CSV_HASH_FILE.write_text(sha256_of(INPUT_TOPIX_CSV))


def _gather_manifest_items() -> list[dict]:
    targets = [OUT_ANOMALY, OUT_CORE30_META, OUT_CORE30_1D, OUT_TOPIX]
    items = []
    for p in targets:
        if p.exists():
            stat = p.stat()
            items.append({
                "key": p.name,
                "bytes": stat.st_size,
                "sha256": sha256_of(p),
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
        else:
            print(f"[INFO] missing (skip manifest item): {p.name}")
    if not items:
        raise RuntimeError("成果物が見つかりません。manifest を生成できません。")
    return items


def _maybe_upload_to_s3(files: list[Path], *, dry_run: bool) -> None:
    cfg = load_s3_config()
    if dry_run:
        for p in files:
            print(f"[PUT] s3://{cfg.bucket}/{cfg.prefix}{p.name} (dry-run)")
        return
    upload_files(cfg, files)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-topix", action="store_true")
    ap.add_argument("--skip-core30", action="store_true")
    ap.add_argument("--skip-anomaly", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # パイプライン実行中は子処理の manifest/S3 を抑止
    pipeline_env = {"PIPELINE_NO_MANIFEST": "1", "PIPELINE_NO_S3": "1"}

    # 1) topixweight（CSV変更時のみ）
    try:
        if _should_run_topix(args.force_topix):
            print("[STEP] run csv_to_parquet_topixweight.py")
            _run_py_script(TOPIX_PY, extra_env=pipeline_env)
            _record_csv_hash()
        else:
            print("[STEP] skip topixweight (no CSV change)")
    except Exception as e:
        print(f"[ERROR] topixweight 変換に失敗: {e}")
        return 1

    # 2) fetch_core30
    if not args.skip_core30:
        try:
            print("[STEP] run fetch_core30_yf.ipynb")
            _run_notebook(CORE30_IPYNB, extra_env=pipeline_env)
        except Exception as e:
            print(f"[ERROR] fetch_core30 実行に失敗: {e}")
            return 1
    else:
        print("[STEP] skip fetch_core30_yf.ipynb")

    # 3) anomaly
    if not args.skip_anomaly:
        try:
            print("[STEP] run anomaly.ipynb")
            _run_notebook(ANOMALY_IPYNB, extra_env=pipeline_env)
        except Exception as e:
            print(f"[ERROR] anomaly 実行に失敗: {e}")
            return 1
    else:
        print("[STEP] skip anomaly.ipynb")

    # 4) manifest を一括生成
    try:
        print("[STEP] write manifest.json (aggregate)")
        items = _gather_manifest_items()
        write_manifest_atomic(items, MANIFEST_PATH)
        print(f"[OK] manifest updated: {MANIFEST_PATH}")
    except Exception as e:
        print(f"[ERROR] manifest 作成に失敗: {e}")
        return 1

    # 5) S3（manifest + 成果物）
    try:
        print("[STEP] upload to S3 (aggregate)")
        files = [MANIFEST_PATH] + [p for p in [OUT_ANOMALY, OUT_CORE30_META, OUT_CORE30_1D, OUT_TOPIX] if p.exists()]
        _maybe_upload_to_s3(files, dry_run=args.dry_run)
    except Exception as e:
        print(f"[ERROR] S3 アップロードに失敗: {e}")
        return 1

    print("[DONE] pipeline completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
