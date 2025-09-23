# -*- coding: utf-8 -*-
"""
common_cfg.manifest: manifest.json 読み書きユーティリティ（分析用）
"""
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib
from typing import List, Dict


def sha256_of(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(chunk_size), b""):
            h.update(b)
    return h.hexdigest()


def write_manifest_atomic(items: List[Dict], path: Path) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "items": sorted(items, key=lambda d: d["key"]),
        "note": "Auto-generated. Do not edit by hand."
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_manifest_items(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        items = obj.get("items", [])
        return items if isinstance(items, list) else []
    except Exception:
        return []


def upsert_manifest_item(items: list[dict], key: str, file_path: Path) -> list[dict]:
    stat = file_path.stat()
    newitem = {
        "key": key,
        "bytes": stat.st_size,
        "sha256": sha256_of(file_path),
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }
    out, found = [], False
    for it in items:
        if isinstance(it, dict) and it.get("key") == key:
            out.append(newitem)
            found = True
        else:
            out.append(it)
    if not found:
        out.append(newitem)
    out.sort(key=lambda d: d.get("key", ""))
    return out

# 互換エイリアス（既存の呼び出しに合わせるため）
write_manifest = write_manifest_atomic