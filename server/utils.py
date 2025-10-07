# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import re
import unicodedata
from functools import cache
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd

# ==============================
# 環境変数（S3設定）
# ==============================
def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else None

_S3_BUCKET = _get_env("DATA_BUCKET")
_S3_META_KEY = _get_env("CORE30_META_KEY")            # 例: parquet/core30_meta.parquet
_S3_PRICES_1D_KEY = _get_env("CORE30_PRICES_KEY")     # 例: parquet/core30_prices_max_1d.parquet
_S3_PREFIX = _get_env("PARQUET_PREFIX") or "parquet"  # S3 prefix (default: parquet)
_AWS_REGION = _get_env("AWS_REGION")
_AWS_PROFILE = _get_env("AWS_PROFILE")
_AWS_ENDPOINT = _get_env("AWS_ENDPOINT_URL")          # 任意（LocalStack/MinIO等）

# ---- 既存ローカルパス（フォールバック用） ----
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
PARQUET_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet"

# ==============================
# S3 / Local 読み込みヘルパ
# ==============================
def _read_parquet_local(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(str(path), engine="pyarrow")
    except Exception:
        return None

def _read_parquet_s3(bucket: Optional[str], key: Optional[str]) -> Optional[pd.DataFrame]:
    if not bucket or not key:
        return None
    try:
        import boto3
        from io import BytesIO

        session_kwargs = {}
        if _AWS_PROFILE:
            session_kwargs["profile_name"] = _AWS_PROFILE
        session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()

        client_kwargs = {}
        if _AWS_REGION:
            client_kwargs["region_name"] = _AWS_REGION
        if _AWS_ENDPOINT:
            client_kwargs["endpoint_url"] = _AWS_ENDPOINT

        s3 = session.client("s3", **client_kwargs)
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_parquet(BytesIO(data), engine="pyarrow")
    except Exception:
        return None

# ==============================
# 既存ユーティリティ
# ==============================
_ALLOWED = re.compile(r"[^A-Za-z0-9._-]")

def _secure_filename(filename: str) -> str:
    if not isinstance(filename, str):
        filename = str(filename or "")
    filename = filename.replace("/", "_").replace("\\", "_")
    filename = unicodedata.normalize("NFKC", filename)
    filename = _ALLOWED.sub("_", filename)
    filename = filename.lstrip(".")
    filename = re.sub(r"_+", "_", filename)
    filename = filename.strip(" ._")
    if not filename:
        filename = "file"
    return filename[:255]

def to_ticker(code: str) -> str:
    s = str(code).strip()
    return s if s.endswith(".T") else f"{s}.T"

# ==============================
# 公開API向けローダ
# ==============================
@cache
def load_core30_meta() -> List[Dict]:
    # まずS3
    df = _read_parquet_s3(_S3_BUCKET, _S3_META_KEY)
    # 取れなかった/空ならローカルへ
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = _read_parquet_local(META_PATH)

    if df is None or df.empty:
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

@cache
def read_prices_1d_df() -> Optional[pd.DataFrame]:
    df = _read_parquet_s3(_S3_BUCKET, _S3_PRICES_1D_KEY)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = _read_parquet_local(PRICES_1D_PATH)
    return df

@cache
def read_prices_df(period: str, interval: str) -> Optional[pd.DataFrame]:
    """
    指定した period と interval に対応する Parquet を読み込む。
    ファイル名: core30_prices_{period}_{interval}.parquet
    S3 → ローカル の順で試行。
    """
    filename = f"core30_prices_{period}_{interval}.parquet"
    s3_key = f"{_S3_PREFIX}/{filename}"

    df = _read_parquet_s3(_S3_BUCKET, s3_key)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        local_path = PARQUET_DIR / filename
        df = _read_parquet_local(local_path)

    return df

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

# ==============================
# demo 用ユーティリティ（既存のまま）
# ==============================
def safe_demo_path(fname: str, allow_ext: set[str]) -> Optional[Path]:
    if not fname:
        return None
    sf = _secure_filename(fname)
    p = (DEMO_DIR / sf)
    try:
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
