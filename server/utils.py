# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import re
import unicodedata
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd

# ---- 既存パス（あれば） ----
try:
    from common_cfg.paths import OUT_META  # data/parquet/core30_meta.parquet
    META_PATH = Path(str(OUT_META)).resolve()
except Exception:
    META_PATH = Path(__file__).resolve().parent.parent / "data" / "parquet" / "core30_meta.parquet"

try:
    from common_cfg.paths import OUT_PRICES
    PRICES_1D_PATH = Path(str(OUT_PRICES)).resolve()
    if "max_1d" not in PRICES_1D_PATH.name:
        raise Exception("OUT_PRICES is not max_1d file.")
except Exception:
    PRICES_1D_PATH = Path(__file__).resolve().parent.parent / "data" / "parquet" / "core30_prices_max_1d.parquet"

DEMO_DIR = Path(__file__).resolve().parent.parent / "demo_data"


# ----------------------------------------------------------------------
# Werkzeug の secure_filename 代替（依存排除のため簡易実装）
# - パス区切り（/ \）は "_" に
# - Unicode 正規化（NFKC）
# - 許可文字: 英数, ドット, アンダースコア, ハイフン
# - 先頭のドットは除去（隠しファイル防止）
# - 空文字になった場合は "file"
# ----------------------------------------------------------------------
_ALLOWED = re.compile(r"[^A-Za-z0-9._-]")

def _secure_filename(filename: str) -> str:
    if not isinstance(filename, str):
        filename = str(filename or "")
    # パス区切りをアンダースコアに
    filename = filename.replace("/", "_").replace("\\", "_")
    # Unicode 正規化（全角→半角など）
    filename = unicodedata.normalize("NFKC", filename)
    # 許可外の文字を _
    filename = _ALLOWED.sub("_", filename)
    # 先頭のドットは除去（.., .env 等を避ける）
    filename = filename.lstrip(".")
    # 連続する _ を圧縮
    filename = re.sub(r"_+", "_", filename)
    # 先頭・末尾の空白/_/ドットを削る
    filename = filename.strip(" ._")
    if not filename:
        filename = "file"
    # 長さ制限（任意・保守的に）
    return filename[:255]


def to_ticker(code: str) -> str:
    s = str(code).strip()
    return s if s.endswith(".T") else f"{s}.T"


def load_core30_meta() -> List[Dict]:
    if not META_PATH.exists():
        return []
    try:
        df = pd.read_parquet(str(META_PATH), engine="pyarrow")
    except Exception:
        return []
    need = {"code", "stock_name"}
    if not need.issubset(df.columns):
        return []
    out = df[["code", "stock_name"]].drop_duplicates(subset=["code"]).copy()
    if "ticker" not in out.columns:
        out["ticker"] = out["code"].astype(str).map(to_ticker)
    out["code"] = out["code"].astype("string")
    out["stock_name"] = out["stock_name"].astype("string")
    out["ticker"] = out["ticker"].astype("string")
    out = out.sort_values("code", key=lambda s: s.astype(str)).reset_index(drop=True)
    return out.to_dict(orient="records")


def read_prices_1d_df() -> Optional[pd.DataFrame]:
    if not PRICES_1D_PATH.exists():
        return None
    try:
        return pd.read_parquet(str(PRICES_1D_PATH), engine="pyarrow")
    except Exception:
        return None


def normalize_prices(df: pd.DataFrame) -> pd.DataFrame:
    need = {"date", "Open", "High", "Low", "Close", "ticker"}
    if not need.issubset(df.columns):
        return pd.DataFrame()
    keep = ["date", "Open", "High", "Low", "Close", "ticker"]
    if "Volume" in df.columns:
        keep.append("Volume")
    out = df[keep].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    out = out[out["date"].notna()].copy()
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out["ticker"] = out["ticker"].astype("string")
    return out


def to_json_records(df: pd.DataFrame) -> List[Dict]:
    g = df.copy()
    g["date"] = g["date"].dt.strftime("%Y-%m-%d")
    g = g.sort_values(["ticker", "date"]).reset_index(drop=True)
    return g.to_dict(orient="records")


def safe_demo_path(fname: str, allow_ext: set[str]) -> Optional[Path]:
    if not fname:
        return None
    sf = _secure_filename(fname)
    p = (DEMO_DIR / sf)
    try:
        # 解決後にディレクトリトラバーサルを防止
        p = p.resolve()
        if not str(p).startswith(str(DEMO_DIR.resolve())):
            return None
    except Exception:
        return None
    if p.suffix.lower() not in allow_ext:
        return None
    if not p.exists():
        return None
    return p


def parse_date_param(v: Optional[str]) -> Optional[pd.Timestamp]:
    if not v:
        return None
    try:
        return pd.to_datetime(v).tz_localize(None)
    except Exception:
        return None


def slice_xy_by_date(x: list[str], y: list, start_dt: Optional[pd.Timestamp], end_dt: Optional[pd.Timestamp]) -> Tuple[list, list]:
    if not isinstance(x, list) or not isinstance(y, list) or len(x) != len(y):
        return x, y
    if start_dt is None and end_dt is None:
        return x, y

    def in_range(s: str) -> bool:
        try:
            d = pd.to_datetime(s).tz_localize(None)
        except Exception:
            return False
        if start_dt is not None and d < start_dt:
            return False
        if end_dt is not None and d > end_dt:
            return False
        return True

    idxs = [i for i, s in enumerate(x) if in_range(s)]
    if not idxs:
        return [], []
    i0, i1 = idxs[0], idxs[-1] + 1
    return x[i0:i1], y[i0:i1]


def filter_bb_payload(data: dict, start_dt: Optional[pd.Timestamp], end_dt: Optional[pd.Timestamp]) -> dict:
    if not isinstance(data, dict) or "series" not in data:
        return data
    series = data.get("series", {})
    out = {"meta": data.get("meta", {}), "series": {}}
    for key in ["close", "ma", "upper", "lower", "bandwidth"]:
        obj = series.get(key)
        if isinstance(obj, dict) and "x" in obj and "y" in obj:
            sx, sy = slice_xy_by_date(obj.get("x", []), obj.get("y", []), start_dt, end_dt)
            out["series"][key] = {"x": sx, "y": sy}
        elif obj is not None:
            out["series"][key] = obj
    if "meta" in out and isinstance(out["meta"], dict):
        out["meta"]["filtered_by"] = {
            "start": start_dt.strftime("%Y-%m-%d") if start_dt else None,
            "end": end_dt.strftime("%Y-%m-%d") if end_dt else None,
        }
    return out
