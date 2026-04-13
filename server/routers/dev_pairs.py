# server/routers/dev_pairs.py
"""
ペアトレードAPI
/api/dev/pairs/* — シグナル・ステータス
"""
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pathlib import Path
import numpy as np
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
_IS_LOCAL = PARQUET_DIR.exists()

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

    if _IS_LOCAL:
        path = _latest_file(directory, prefix)
        if path is not None:
            df = pd.read_parquet(path)
        else:
            df = pd.DataFrame()
    else:
        import boto3
        import io
        s3 = boto3.client("s3", region_name=AWS_REGION)
        resp = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{S3_PREFIX}/{s3_prefix}/{prefix}_",
        )
        keys = sorted([o["Key"] for o in resp.get("Contents", [])])
        if keys:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=keys[-1])
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        else:
            df = pd.DataFrame()

    if not df.empty:
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
        return {"pairs": [], "entry": [], "signal_date": None, "total": 0, "entry_count": 0}

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
            "entry_count": 0,
            "entry_pairs": [],
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
    """キャッシュクリア（サーバーは次回リクエストでS3から再読込）"""
    _cache.clear()
    return {
        "status": "success",
        "message": "Cache cleared, next request will reload from S3" if not _IS_LOCAL else "Cache cleared",
        "updated_at": datetime.now().isoformat(),
    }


# --- ペアチャート用 ---

# V2_PAIRS: (tk1, tk2, lookback, pf, n, half_life) — パイプラインと同一定義
try:
    from scripts.pipeline.generate_pairs_signals import V2_PAIRS
except Exception:
    V2_PAIRS = []

_V2_LOOKUP: dict[tuple[str, str], tuple[int, float, int, float]] = {
    (tk1, tk2): (lb, pf, n, hl) for tk1, tk2, lb, pf, n, hl in V2_PAIRS
}


PRICES_TOPIX = PARQUET_DIR / "granville" / "prices_topix.parquet"


def _load_prices_for_chart(tk1: str, tk2: str, days: int) -> pd.DataFrame:
    """チャート用に2銘柄の日足Closeを取得（prices_topix優先→prices_max_1d フォールバック）"""
    cache_key = "prices_topix_all"
    df = _cached(cache_key)
    if df is None:
        if PRICES_TOPIX.exists():
            df = pd.read_parquet(PRICES_TOPIX)
        if df is None or df.empty:
            from server.utils import read_prices_df, normalize_prices
            df = read_prices_df("max", "1d")
            if df is not None and not df.empty:
                df = normalize_prices(df)
        if df is not None and not df.empty:
            _set_cache(cache_key, df)
    if df is None or df.empty:
        return pd.DataFrame()

    tk_col = "ticker" if "ticker" in df.columns else "Ticker"
    df = df[df[tk_col].isin([tk1, tk2])].copy()
    if df.empty:
        return df

    date_col = "date" if "date" in df.columns else "Date"
    close_col = "Close" if "Close" in df.columns else "close"
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values([tk_col, date_col])

    all_dates = sorted(df[date_col].unique())
    if len(all_dates) > days:
        cutoff = all_dates[-days]
        df = df[df[date_col] >= cutoff]

    return df[[tk_col, date_col, close_col]].rename(
        columns={tk_col: "ticker", date_col: "date", close_col: "Close"}
    )


@router.get("/api/dev/pairs/chart")
async def get_pair_chart(
    tk1: str = Query(..., description="銘柄1ティッカー (例: 8801.T)"),
    tk2: str = Query(..., description="銘柄2ティッカー (例: 8830.T)"),
    days: int = Query(500, description="取得日数"),
):
    """ペアチャート用データ（正規化価格 + ローリングz-score時系列）"""
    cache_key = f"pair_chart/{tk1}/{tk2}/{days}"
    cached = _cached(cache_key)
    if cached is not None:
        return cached

    pair_info = _V2_LOOKUP.get((tk1, tk2)) or _V2_LOOKUP.get((tk2, tk1))
    if pair_info is None:
        return JSONResponse(status_code=404, content={"detail": f"Pair {tk1}/{tk2} not found in V2_PAIRS"})

    if (tk1, tk2) not in _V2_LOOKUP:
        tk1, tk2 = tk2, tk1
    lookback, full_pf, full_n, half_life = _V2_LOOKUP[(tk1, tk2)]

    name1, name2 = tk1, tk2
    tk1_upper, tk1_lower = 0.0, 0.0
    sig_df = _load_latest(PAIRS_DIR, "pairs_signals", "pairs")
    if not sig_df.empty:
        match = sig_df[(sig_df["tk1"] == tk1) & (sig_df["tk2"] == tk2)]
        if not match.empty:
            row = match.iloc[0]
            name1 = str(row.get("name1", tk1))
            name2 = str(row.get("name2", tk2))
            tk1_upper = _safe_float(row.get("tk1_upper", 0))
            tk1_lower = _safe_float(row.get("tk1_lower", 0))

    df = _load_prices_for_chart(tk1, tk2, days + lookback)
    if df.empty:
        return JSONResponse(status_code=404, content={"detail": "Price data not found"})

    d1 = df[df["ticker"] == tk1].set_index("date").sort_index()
    d2 = df[df["ticker"] == tk2].set_index("date").sort_index()
    common = d1.index.intersection(d2.index)
    if len(common) < lookback + 5:
        return JSONResponse(status_code=404, content={"detail": f"Insufficient data: {len(common)} days (need {lookback + 5})"})

    d1 = d1.loc[common]
    d2 = d2.loc[common]

    c1 = d1["Close"].values.astype(float)
    c2 = d2["Close"].values.astype(float)
    dates = [d.strftime("%Y-%m-%d") for d in common]

    start_idx = lookback
    base1 = c1[start_idx]
    base2 = c2[start_idx]
    norm1 = (c1[start_idx:] / base1 * 100).round(2).tolist()
    norm2 = (c2[start_idx:] / base2 * 100).round(2).tolist()

    spread = np.log(c1 / c2)
    z_scores = []
    rolling_hl = []
    hl_window = max(lookback, 60)
    for i in range(start_idx, len(spread)):
        window = spread[i - lookback + 1: i + 1]
        mu = window.mean()
        sigma = window.std()
        if sigma < 1e-8:
            z_scores.append(0.0)
        else:
            z_scores.append(round(float((spread[i] - mu) / sigma), 4))

        if i >= hl_window:
            hl_spread = spread[i - hl_window + 1: i + 1]
            y = hl_spread[1:]
            x = hl_spread[:-1]
            x_mean = x.mean()
            denom = ((x - x_mean) ** 2).sum()
            if denom > 1e-12:
                beta = float(((x - x_mean) * (y - y.mean())).sum() / denom)
                if 0 < beta < 1:
                    hl_val = round(-np.log(2) / np.log(beta), 1)
                    rolling_hl.append(min(hl_val, 999.0))
                else:
                    rolling_hl.append(None)
            else:
                rolling_hl.append(None)
        else:
            rolling_hl.append(None)

    out_dates = dates[start_idx:]

    z_latest = z_scores[-1] if z_scores else 0.0
    direction = "short_tk1" if z_latest > 0 else "long_tk1"

    series = []
    for i in range(len(out_dates)):
        series.append({
            "date": out_dates[i],
            "norm1": norm1[i],
            "norm2": norm2[i],
            "z": z_scores[i],
            "hl": rolling_hl[i],
        })

    c1_last = round(float(c1[-1]), 1)
    c2_last = round(float(c2[-1]), 1)
    c1_prev = round(float(c1[-2]), 1) if len(c1) >= 2 else c1_last
    c2_prev = round(float(c2[-2]), 1) if len(c2) >= 2 else c2_last
    chg1 = round(c1_last - c1_prev, 1)
    chg2 = round(c2_last - c2_prev, 1)
    chg1_pct = round((c1_last / c1_prev - 1) * 100, 2) if c1_prev else 0.0
    chg2_pct = round((c2_last / c2_prev - 1) * 100, 2) if c2_prev else 0.0

    result = {
        "tk1": tk1, "tk2": tk2,
        "name1": name1, "name2": name2,
        "c1": c1_last, "c2": c2_last,
        "chg1": chg1, "chg2": chg2,
        "chg1_pct": chg1_pct, "chg2_pct": chg2_pct,
        "lookback": lookback, "full_pf": full_pf, "full_n": full_n,
        "half_life": half_life,
        "z_latest": round(z_latest, 3),
        "direction": direction,
        "tk1_upper": tk1_upper,
        "tk1_lower": tk1_lower,
        "series": series,
    }

    _set_cache(cache_key, result)
    return result
