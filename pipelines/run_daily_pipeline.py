#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_pipeline.py
- 1) analyze/create_master_meta.py（統合メタデータ生成）
- 2) analyze/fetch_core30_yf.ipynb
- 3) テクニカル指標のスナップショットを事前計算
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

import pandas as pd

# ---- 共有ユーティリティ ----
from common_cfg.paths import (
    PARQUET_DIR,
    MASTER_META_PARQUET as OUT_MASTER_META,
    TECH_SNAPSHOT_PARQUET as OUT_TECH_SNAPSHOT,
    MANIFEST_JSON as MANIFEST_PATH,
    PRICE_SPECS,
    price_parquet,
)
from common_cfg.manifest import sha256_of, write_manifest_atomic
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_files
from common_cfg.env import load_dotenv_cascade

# ★ 追加：.env.s3 → .env の順で読み込み（空の上書きに注意）
load_dotenv_cascade()

# ---- パイプライン内でのみインポート（PYTHONPATH設定後） ----
# server 以下のモジュールは _run_cmd で PYTHONPATH を設定した後に import する

ROOT = Path(".").resolve()
ANALYZE_DIR    = ROOT / "analyze"
CORE30_IPYNB   = ANALYZE_DIR / "fetch_core30_yf.ipynb"
MASTER_META_PY = ANALYZE_DIR / "create_master_meta.py"

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


def _run_tech_analysis_snapshot() -> None:
    """テクニカル指標のスナップショットを計算して保存する"""
    print("[STEP] run technical analysis snapshot generation")
    # PYTHONPATH設定済みの環境で import
    from server.utils import read_prices_1d_df, normalize_prices
    from server.services.tech_utils_v2 import evaluate_latest_snapshot

    df = read_prices_1d_df()
    if df is None or df.empty:
        print("[WARN] prices data not found, skipping tech analysis.")
        return

    out = normalize_prices(df)
    if out is None or out.empty:
        print("[WARN] normalized prices are empty, skipping tech analysis.")
        return

    res = []
    for _, grp in out.sort_values(["ticker", "date"]).groupby("ticker", sort=False):
        grp = grp.dropna(subset=["Close"]).copy()
        if grp.empty:
            continue
        grp = grp.set_index("date")
        try:
            res.append(evaluate_latest_snapshot(grp))
        except Exception as e:
            ticker = grp["ticker"].iloc[0]
            print(f"[WARN] Failed to evaluate snapshot for {ticker}: {e}")

    if not res:
        print("[WARN] No snapshot data generated.")
        return

    snapshot_df = pd.DataFrame(res)
    # ネストした辞書はJSON文字列に変換して保存
    for col in ["values", "votes", "overall"]:
        if col in snapshot_df.columns:
            snapshot_df[col] = snapshot_df[col].apply(json.dumps)

    snapshot_df.to_parquet(OUT_TECH_SNAPSHOT, engine="pyarrow", index=False)
    print(f"[OK] tech snapshot saved: {OUT_TECH_SNAPSHOT}")


def _gather_manifest_items() -> list[dict]:
    # Core30の全ファイルを収集（メタ + 複数のpricesファイル）
    targets = [OUT_TECH_SNAPSHOT, OUT_MASTER_META]
    for period, interval in PRICE_SPECS:
        targets.append(price_parquet(period, interval))

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
    if not cfg.bucket:
        print("[INFO] S3 bucket not configured, skipping upload.")
        return
    if dry_run:
        for p in files:
            print(f"[PUT] s3://{cfg.bucket}/{cfg.prefix}{p.name} (dry-run)")
        return
    upload_files(cfg, files)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-core30", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # パイプライン実行中は子処理の manifest/S3 を抑止
    pipeline_env = {"PIPELINE_NO_MANIFEST": "1", "PIPELINE_NO_S3": "1"}

    # 1) master meta (always regenerate to keep tags in sync)
    try:
        print("[STEP] run create_master_meta.py")
        _run_py_script(MASTER_META_PY, extra_env=pipeline_env)
    except Exception as e:
        print(f"[ERROR] master meta 生成に失敗: {e}")
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

    # 3) テクニカル指標スナップショット計算 ★追加ステップ
    try:
        _run_tech_analysis_snapshot()
    except Exception as e:
        print(f"[ERROR] テクニカル分析スナップショットの生成に失敗: {e}")
        # エラー詳細を出力
        import traceback
        traceback.print_exc()
        return 1

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
        files = [MANIFEST_PATH, OUT_TECH_SNAPSHOT, OUT_MASTER_META]
        for period, interval in PRICE_SPECS:
            p = price_parquet(period, interval)
            if p.exists():
                files.append(p)
        _maybe_upload_to_s3(files, dry_run=args.dry_run)
    except Exception as e:
        print(f"[ERROR] S3 アップロードに失敗: {e}")
        return 1

    print("[DONE] pipeline completed.")
    return 0


if __name__ == "__main__":
    # PYTHONPATH を設定して server 以下のモジュールを import できるようにする
    sys.path.insert(0, str(ROOT))
    main()
