# server/routers/dev_pairs.py
"""
ペアトレードAPI
/api/dev/pairs/* — シグナル・ステータス
"""
from fastapi import APIRouter
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Optional
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

PAIRS_DIR = PARQUET_DIR / "pairs"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", os.getenv("DATA_BUCKET", "stock-api-data"))
_S3_PREFIX_RAW = os.getenv("PARQUET_PREFIX", "parquet")
S3_PREFIX = _S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# キャッシュ
_cache: dict[str, tuple[datetime, object]] = {}
CACHE_TTL = 120


def _cached(key: str) -> Optional[object]:
    if key in _cache:
        ts, data = _cache[key]
        if (datetime.now() - ts).total_seconds() < CACHE_TTL:
            return data
    return None


def _set_cache(key: str, data: object) -> None:
    _cache[key] = (datetime.now(), data)


def _latest_file(directory: Path, prefix: str) -> Optional[Path]:
    files = sorted(directory.glob(f"{prefix}_*.parquet"))
    return files[-1] if files else None


def _load_latest(directory: Path, prefix: str, s3_prefix: str) -> pd.DataFrame:
    cache_key = f"{s3_prefix}/{prefix}"
    cached = _cached(cache_key)
    if cached is not None:
        return cached

    path = _latest_file(directory, prefix)
    if path is None:
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            resp = s3.list_objects_v2(
                Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/{s3_prefix}/{prefix}_",
            )
            if "Contents" in resp:
                keys = sorted([o["Key"] for o in resp["Contents"]])
                if keys:
                    local = directory / Path(keys[-1]).name
                    directory.mkdir(parents=True, exist_ok=True)
                    s3.download_file(S3_BUCKET, keys[-1], str(local))
                    path = local
        except Exception:
            pass

    if path is None:
        return pd.DataFrame()

    df = pd.read_parquet(path)
    _set_cache(cache_key, df)
    return df


def _safe_float(v, decimals: int = 1) -> float:
    try:
        return round(float(v), decimals)
    except (ValueError, TypeError):
        return 0.0


def _safe_int(v) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


@router.get("/api/dev/pairs/signals")
async def get_pairs_signals():
    """全ペアのシグナル（z-score, 閾値, 直近成績）"""
    df = _load_latest(PAIRS_DIR, "pairs_signals", "pairs")

    if df.empty:
        return {"pairs": [], "hot": [], "signal_date": None, "total": 0, "hot_count": 0}

    signal_date = None
    if "signal_date" in df.columns:
        signal_date = pd.to_datetime(df["signal_date"]).max().strftime("%Y-%m-%d")

    pairs = []
    entry = []
    for _, r in df.iterrows():
        pair_date = ""
        if "signal_date" in r.index and pd.notna(r.get("signal_date")):
            pair_date = pd.to_datetime(r["signal_date"]).strftime("%Y-%m-%d")
        item = {
            "tk1": r.get("tk1", ""),
            "tk2": r.get("tk2", ""),
            "name1": r.get("name1", ""),
            "name2": r.get("name2", ""),
            "c1": _safe_float(r.get("c1", 0)),
            "c2": _safe_float(r.get("c2", 0)),
            "z_latest": _safe_float(r.get("z_latest", 0), 3),
            "z_abs": _safe_float(r.get("z_abs", abs(r.get("z_latest", 0))), 3),
            "tk1_upper": _safe_float(r.get("tk1_upper", 0)),
            "tk1_lower": _safe_float(r.get("tk1_lower", 0)),
            "mu": _safe_float(r.get("mu", 0), 6),
            "sigma": _safe_float(r.get("sigma", 0), 6),
            "lookback": _safe_int(r.get("lookback", 20)),
            "shares1": _safe_int(r.get("shares1", 100)),
            "shares2": _safe_int(r.get("shares2", 100)),
            "notional1": _safe_int(r.get("notional1", 0)),
            "notional2": _safe_int(r.get("notional2", 0)),
            "imbalance_pct": _safe_float(r.get("imbalance_pct", 0), 1),
            "full_pf": _safe_float(r.get("full_pf", 0), 2),
            "full_n": _safe_int(r.get("full_n", 0)),
            "half_life": _safe_float(r.get("half_life", 0), 1),
            "is_entry": bool(r.get("is_entry", r.get("is_hot", False))),
            "direction": r.get("direction", ""),
            "signal_date": pair_date,
        }
        pairs.append(item)
        if item["is_entry"]:
            entry.append(item)

    return {
        "pairs": pairs,
        "entry": entry,
        "signal_date": signal_date,
        "total": len(pairs),
        "entry_count": len(entry),
    }


@router.get("/api/dev/pairs/status")
async def get_pairs_status():
    """ペアトレード ステータスサマリー"""
    df = _load_latest(PAIRS_DIR, "pairs_signals", "pairs")

    if df.empty:
        return {
            "signal_date": None,
            "total_pairs": 0,
            "hot_count": 0,
            "hot_pairs": [],
        }

    signal_date = None
    if "signal_date" in df.columns:
        signal_date = pd.to_datetime(df["signal_date"]).max().strftime("%Y-%m-%d")

    entry_col = "is_entry" if "is_entry" in df.columns else "is_hot"
    entry_df = df[df[entry_col] == True] if entry_col in df.columns else pd.DataFrame()
    entry_pairs = []
    for _, r in entry_df.iterrows():
        entry_pairs.append({
            "pair": f"{r.get('name1', r.get('tk1', ''))} / {r.get('name2', r.get('tk2', ''))}",
            "z": _safe_float(r.get("z_latest", 0), 2),
            "direction": r.get("direction", ""),
            "full_pf": _safe_float(r.get("full_pf", 0), 2),
            "shares1": _safe_int(r.get("shares1", 100)),
            "shares2": _safe_int(r.get("shares2", 100)),
        })

    return {
        "signal_date": signal_date,
        "total_pairs": len(df),
        "entry_count": len(entry_pairs),
        "entry_pairs": entry_pairs,
    }


@router.post("/api/dev/pairs/refresh")
async def refresh_pairs_cache():
    """キャッシュクリア+S3から最新ファイルをダウンロード"""
    _cache.clear()

    refreshed = []
    try:
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        resp = s3.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/pairs/pairs_signals_",
        )
        if "Contents" in resp:
            keys = sorted([o["Key"] for o in resp["Contents"]])
            if keys:
                local = PAIRS_DIR / Path(keys[-1]).name
                PAIRS_DIR.mkdir(parents=True, exist_ok=True)
                s3.download_file(S3_BUCKET, keys[-1], str(local))
                refreshed.append(Path(keys[-1]).name)
    except Exception as e:
        print(f"[pairs/refresh] S3 download failed: {e}")

    return {
        "status": "success",
        "message": "Pairs cache refreshed",
        "refreshed_files": refreshed,
        "updated_at": datetime.now().isoformat(),
    }
