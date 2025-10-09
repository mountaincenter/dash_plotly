# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import re
import time
import unicodedata
from functools import cache
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

# ==============================
# 環境変数（S3設定）
# ==============================
def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else None

_S3_BUCKET = _get_env("DATA_BUCKET")
_S3_PREFIX_RAW = _get_env("PARQUET_PREFIX")
_S3_PREFIX = (_S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet")
_AWS_REGION = _get_env("AWS_REGION")
_AWS_PROFILE = _get_env("AWS_PROFILE")
_AWS_ENDPOINT = _get_env("AWS_ENDPOINT_URL")

# ---- 既存ローカルパス（フォールバック用） ----
PARQUET_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet"
DEMO_DIR = Path(__file__).resolve().parent.parent / "demo_data"

MASTER_META_PATH = PARQUET_DIR / "meta.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
TECH_SNAPSHOT_PATH = PARQUET_DIR / "tech_snapshot_1d.parquet"

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return default

_DATA_CACHE_SECONDS = max(0, _env_int("DATA_CACHE_SECONDS", 300))

def _s3_key(default_name: str) -> str:
    return f"{_S3_PREFIX}/{default_name}" if _S3_PREFIX else default_name

def _env_s3_key(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = _get_env(name)
        if value:
            return value.lstrip("/")
    if default:
        return _s3_key(default)
    return None

_S3_MASTER_META_KEY = _env_s3_key("MASTER_META_KEY", "META_KEY", "CORE30_META_KEY", default=MASTER_META_PATH.name)
_S3_PRICES_1D_KEY = _env_s3_key("PRICES_MAX_1D_KEY", "PRICES_1D_KEY", "CORE30_PRICES_KEY", default=PRICES_1D_PATH.name)
_S3_TECH_SNAPSHOT_KEY = _env_s3_key("TECH_SNAPSHOT_KEY", "CORE30_TECH_SNAPSHOT_KEY", default=TECH_SNAPSHOT_PATH.name)

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
    except Exception as e:
        print(f"!!! S3 READ ERROR: Failed to read s3://{bucket}/{key}. Error: {e}")
        return None

# ==============================
# DataFrame キャッシュ
# ==============================
class _DataFrameCache:
    def __init__(self, ttl_seconds: int):
        self._ttl_seconds = max(0, ttl_seconds)
        self._store: Dict[Tuple, Tuple[Optional[float], float, Optional[pd.DataFrame]]] = {}

    def get(
        self,
        key: Tuple,
        loader: Callable[[], Optional[pd.DataFrame]],
        *,
        local_path: Optional[Path] = None,
    ) -> Optional[pd.DataFrame]:
        if self._ttl_seconds == 0:
            return loader()

        now = time.time()
        mtime: Optional[float] = None
        if local_path is not None:
            try:
                mtime = local_path.stat().st_mtime
            except FileNotFoundError:
                mtime = None

        cached = self._store.get(key)
        if cached:
            cached_mtime, expires_at, df = cached
            if (mtime is None or mtime == cached_mtime) and now < expires_at:
                return df

        df = loader()
        if df is None:
            # Avoid caching failures so next call can retry immediately.
            self._store.pop(key, None)
            return None

        expires_at = now + self._ttl_seconds
        self._store[key] = (mtime, expires_at, df)
        return df

_df_cache = _DataFrameCache(_DATA_CACHE_SECONDS)

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
def _resolve_tag(tag: Optional[str]) -> Optional[str]:
    if not tag:
        return None
    tag_norm = str(tag).strip()
    if not tag_norm:
        return None
    lut = {
        "core30": "TOPIX_CORE30",
        "topix": "TOPIX_CORE30",
        "topix_core30": "TOPIX_CORE30",
        "topixcore30": "TOPIX_CORE30",
        "takaichi": "高市銘柄",
        "takaichi_stock": "高市銘柄",
        "高市": "高市銘柄",
        "高市銘柄": "高市銘柄",
        "topix_core30_upper": "TOPIX_CORE30",
    }
    key = tag_norm.lower()
    return lut.get(key, tag_norm)


@cache
def load_master_meta(tag: Optional[str] = None) -> List[Dict]:
    df = _read_parquet_local(MASTER_META_PATH)
    if (df is None or df.empty) and _S3_BUCKET and _S3_MASTER_META_KEY:
        df = _read_parquet_s3(_S3_BUCKET, _S3_MASTER_META_KEY)
    if df is None or df.empty:
        return []

    cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "tag1", "tag2", "tag3"]
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[cols].copy()
    resolved_tag = _resolve_tag(tag)
    if resolved_tag:
        df = df[df["tag1"].astype("string").str.lower() == str(resolved_tag).lower()]
    df = df.sort_values(["tag1", "code"], kind="mergesort")
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")

def read_prices_1d_df() -> Optional[pd.DataFrame]:
    def _load() -> Optional[pd.DataFrame]:
        df = _read_parquet_local(PRICES_1D_PATH)
        if (df is None or df.empty) and _S3_BUCKET and _S3_PRICES_1D_KEY:
            df = _read_parquet_s3(_S3_BUCKET, _S3_PRICES_1D_KEY)
        return df

    return _df_cache.get(("prices_1d",), _load, local_path=PRICES_1D_PATH)

def read_prices_df(period: str, interval: str) -> Optional[pd.DataFrame]:
    filename = f"prices_{period}_{interval}.parquet"
    local_path = PARQUET_DIR / filename
    def _load() -> Optional[pd.DataFrame]:
        df = _read_parquet_local(local_path)
        if (df is None or df.empty) and _S3_BUCKET:
            s3_key = _s3_key(filename)
            df = _read_parquet_s3(_S3_BUCKET, s3_key)
        return df

    return _df_cache.get(("prices", period, interval), _load, local_path=local_path)

def read_tech_snapshot_df() -> Optional[pd.DataFrame]:
    """事前計算されたテクニカル指標スナップショットを読み込む"""
    def _load() -> Optional[pd.DataFrame]:
        df = _read_parquet_local(TECH_SNAPSHOT_PATH)
        if (df is None or df.empty) and _S3_BUCKET and _S3_TECH_SNAPSHOT_KEY:
            df = _read_parquet_s3(_S3_BUCKET, _S3_TECH_SNAPSHOT_KEY)

        if df is None or df.empty:
            return df

        # JSON文字列の列を辞書に変換
        for col in ["values", "votes", "overall"]:
            if col in df.columns and isinstance(df[col].iloc[0], str):
                df[col] = df[col].apply(json.loads)
        return df

    return _df_cache.get(("tech_snapshot",), _load, local_path=TECH_SNAPSHOT_PATH)

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
