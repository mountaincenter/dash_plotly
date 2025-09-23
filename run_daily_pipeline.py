#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
目的:
- 既存の 3 つの処理を順次実行
  1) csv_to_parquet_topixweight.py（CSV 変更があった場合のみ）
  2) fetch_core30_yf.ipynb
  3) anomaly.ipynb
- パイプライン実行時は各処理の manifest/S3 を抑止（環境変数で制御）
- 最後に ./data/parquet の所定成果物から manifest.json を一括生成
- 生成物と manifest.json を S3 にアップロード（環境変数があれば）

使い方:
  python run_daily_pipeline.py
  python run_daily_pipeline.py --force-topix
  python run_daily_pipeline.py --dry-run
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
from typing import List, Optional

# --- .env 読み込み（任意） ---
try:
    from dotenv import load_dotenv
    for _p in (Path(".env.s3"), Path(".env")):
        if _p.exists():
            load_dotenv(dotenv_path=_p, override=False)
except Exception:
    pass

ROOT            = Path(".").resolve()
PARQUET_DIR     = ROOT / "data" / "parquet"
CSV_DIR         = ROOT / "data" / "csv"

TOPIX_PY        = ROOT / "csv_to_parquet_topixweight.py"
CORE30_IPYNB    = ROOT / "fetch_core30_yf.ipynb"
ANOMALY_IPYNB   = ROOT / "anomaly.ipynb"

OUT_TOPIX       = PARQUET_DIR / "topixweight_j.parquet"
OUT_CORE30_META = PARQUET_DIR / "core30_meta.parquet"
OUT_CORE30_1D   = PARQUET_DIR / "core30_prices_1y_1d.parquet"
OUT_ANOMALY     = PARQUET_DIR / "core30_anomaly.parquet"
MANIFEST_PATH   = PARQUET_DIR / "manifest.json"

INPUT_TOPIX_CSV = CSV_DIR / "topixweight_j.csv"

DATA_BUCKET     = os.getenv("DATA_BUCKET")           # 例: dash-plotly
PARQUET_PREFIX  = os.getenv("PARQUET_PREFIX", "parquet/")
AWS_REGION      = os.getenv("AWS_REGION")
AWS_PROFILE     = os.getenv("AWS_PROFILE")

STATE_DIR       = PARQUET_DIR / "_state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
CSV_HASH_FILE   = STATE_DIR / "topixweight_j.csv.sha256"

def _sha256_of(path: Path, chunk: int = 1024 * 1024) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

def _run_cmd(cmd: List[str], cwd: Optional[Path] = None, extra_env: Optional[dict] = None) -> None:
    env = os.environ.copy()
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
    new_hash = _sha256_of(INPUT_TOPIX_CSV)
    old_hash = CSV_HASH_FILE.read_text().strip() if CSV_HASH_FILE.exists() else ""
    return new_hash != old_hash

def _record_csv_hash():
    if INPUT_TOPIX_CSV.exists():
        CSV_HASH_FILE.write_text(_sha256_of(INPUT_TOPIX_CSV))

def _write_manifest_atomic(items: list[dict], path: Path) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "items": sorted(items, key=lambda d: d["key"]),
        "note": "Auto-generated. Do not edit by hand."
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def _gather_manifest_items() -> list[dict]:
    targets = [OUT_ANOMALY, OUT_CORE30_META, OUT_CORE30_1D, OUT_TOPIX]
    items = []
    for p in targets:
        if p.exists():
            stat = p.stat()
            items.append({
                "key": p.name,
                "bytes": stat.st_size,
                "sha256": _sha256_of(p),
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
        else:
            print(f"[INFO] missing (skip manifest item): {p.name}")
    if not items:
        raise RuntimeError("成果物が見つかりません。manifest を生成できません。")
    return items

def _maybe_upload_to_s3(files: list[Path], *, dry_run: bool) -> None:
    if not DATA_BUCKET:
        print("[INFO] DATA_BUCKET 未設定のため S3 アップロードはスキップします。")
        return
    try:
        import boto3
        session_kwargs = {}
        if AWS_PROFILE:
            session_kwargs["profile_name"] = AWS_PROFILE
        session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
        s3 = session.client("s3", region_name=AWS_REGION) if AWS_REGION else session.client("s3")
    except Exception as e:
        print(f"[WARN] boto3 初期化に失敗: {e}  S3アップロードをスキップします。")
        return

    for p in files:
        key = f"{PARQUET_PREFIX}{p.name}"
        print(f"[PUT] s3://{DATA_BUCKET}/{key}")
        if dry_run:
            continue
        try:
            extra = {
                "ServerSideEncryption": "AES256",
                "CacheControl": "max-age=60",
                "ContentType": "application/octet-stream",
            }
            s3.upload_file(str(p), DATA_BUCKET, key, ExtraArgs=extra)
        except Exception as e:
            print(f"[WARN] upload failed: {p} -> s3://{DATA_BUCKET}/{key}: {e}")

def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--force-topix", action="store_true")
    ap.add_argument("--skip-core30", action="store_true")
    ap.add_argument("--skip-anomaly", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # パイプライン実行時は各処理の manifest/S3 を抑止
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
        _write_manifest_atomic(items, MANIFEST_PATH)
        print(f"[OK] manifest updated: {MANIFEST_PATH}")
    except Exception as e:
        print(f"[ERROR] manifest 作成に失敗: {e}")
        return 1

    # 5) S3 にアップロード（manifest + 成果物）
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
