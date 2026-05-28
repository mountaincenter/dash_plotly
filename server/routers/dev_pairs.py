# server/routers/dev_pairs.py
"""
ペアトレードAPI
/api/dev/pairs/* — シグナル・ステータス
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pathlib import Path
import io
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, Optional
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from ..utils import read_prices_1d_df

router = APIRouter()

PAIRS_DIR = PARQUET_DIR / "pairs"
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"
ANALYSIS_DIR = ROOT / "data" / "analysis"
PAIR_HEALTH_STATE_PATH = ANALYSIS_DIR / "pair_health_state.json"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", os.getenv("DATA_BUCKET", "stock-api-data"))
_S3_PREFIX_RAW = os.getenv("PARQUET_PREFIX", "parquet")
S3_PREFIX = _S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# キャッシュ
_cache: dict[str, tuple[datetime, object]] = {}
CACHE_TTL = 120


class PairHealthUpdate(BaseModel):
    pair: str
    state: str
    reason: str = ""
    recheck_after: str | None = None


class PairHealthStateRequest(BaseModel):
    updates: list[PairHealthUpdate] = Field(default_factory=list)
    note: str = ""


def _cached(key: str) -> Optional[object]:
    if key in _cache:
        ts, data = _cache[key]
        if (datetime.now() - ts).total_seconds() < CACHE_TTL:
            return data
    return None


def _set_cache(key: str, data: object) -> None:
    _cache[key] = (datetime.now(), data)


def _s3_client():
    import boto3
    return boto3.client("s3", region_name=AWS_REGION)


def _latest_file(directory: Path, prefix: str) -> Optional[Path]:
    files = sorted(directory.glob(f"{prefix}_*.parquet"))
    return files[-1] if files else None


_IS_LOCAL = PARQUET_DIR.exists()


def _load_from_s3(prefix: str, s3_prefix: str) -> pd.DataFrame:
    """S3から最新parquetを直接読み込み（サーバー用）"""
    import boto3
    s3 = boto3.client("s3", region_name=AWS_REGION)
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/{s3_prefix}/{prefix}_",
    )
    if "Contents" not in resp:
        return pd.DataFrame()
    keys = sorted([o["Key"] for o in resp["Contents"]])
    if not keys:
        return pd.DataFrame()
    import io
    obj = s3.get_object(Bucket=S3_BUCKET, Key=keys[-1])
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _load_from_local(directory: Path, prefix: str) -> pd.DataFrame:
    """ローカルファイルから読み込み（開発用）"""
    path = _latest_file(directory, prefix)
    if path is None:
        return pd.DataFrame()
    return pd.read_parquet(path)


def _load_latest(directory: Path, prefix: str, s3_prefix: str) -> pd.DataFrame:
    cache_key = f"{s3_prefix}/{prefix}"
    cached = _cached(cache_key)
    if cached is not None:
        return cached

    if _IS_LOCAL:
        df = _load_from_local(directory, prefix)
    else:
        try:
            df = _load_from_s3(prefix, s3_prefix)
        except Exception:
            df = pd.DataFrame()

    if not df.empty:
        _set_cache(cache_key, df)
    return df


def _s3_download(filename: str, dest: Path) -> None:
    """S3 の top-level parquet を dest にダウンロード"""
    try:
        s3 = _s3_client()
        key = f"{S3_PREFIX}/{filename}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(S3_BUCKET, key, str(dest))
    except Exception:
        pass


def _read_analysis_text(filename: str) -> str | None:
    path = ANALYSIS_DIR / filename
    if _IS_LOCAL and path.exists():
        return path.read_text(encoding="utf-8")
    try:
        obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=f"analysis/{filename}")
        return obj["Body"].read().decode("utf-8")
    except Exception:
        if path.exists():
            return path.read_text(encoding="utf-8")
    return None


def _read_analysis_bytes(filename: str) -> bytes | None:
    path = ANALYSIS_DIR / filename
    if _IS_LOCAL and path.exists():
        return path.read_bytes()
    try:
        obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=f"analysis/{filename}")
        return obj["Body"].read()
    except Exception:
        if path.exists():
            return path.read_bytes()
    return None


def _load_analysis_json(filename: str) -> dict[str, Any]:
    text = _read_analysis_text(filename)
    if not text:
        return {"generated_at": None, "count": 0, "rows": []}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {"generated_at": None, "count": 0, "rows": []}
    return data if isinstance(data, dict) else {"generated_at": None, "count": 0, "rows": []}


def _load_pair_health_state() -> dict[str, Any]:
    text = _read_analysis_text("pair_health_state.json")
    if text:
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                data.setdefault("pairs", {})
                return data
        except json.JSONDecodeError:
            pass
    return {
        "generated_at": datetime.now().date().isoformat(),
        "policy": {},
        "pairs": {},
    }


def _write_pair_health_state(state: dict[str, Any]) -> None:
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    PAIR_HEALTH_STATE_PATH.write_text(payload + "\n", encoding="utf-8")
    if S3_BUCKET:
        _s3_client().put_object(
            Bucket=S3_BUCKET,
            Key="analysis/pair_health_state.json",
            Body=(payload + "\n").encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )


def _pair_health_for(pair_health: dict[str, Any], tk1: str, tk2: str) -> dict[str, str]:
    pairs = pair_health.get("pairs", {})
    item = {}
    if isinstance(pairs, dict):
        item = pairs.get(f"{tk1}/{tk2}") or pairs.get(f"{tk2}/{tk1}") or {}
    if not isinstance(item, dict):
        item = {}
    return {
        "pair_health_state": str(item.get("state", "ACTIVE")).upper(),
        "pair_health_reason": str(item.get("reason", "")),
        "pair_health_reviewed_at": str(item.get("reviewed_at", "")),
        "pair_health_recheck_after": str(item.get("recheck_after", "")),
    }


def _json_safe(v: object) -> object:
    if isinstance(v, float) and np.isnan(v):
        return None
    if pd.isna(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    return v


def _load_unified(filename: str, cache_key: str) -> pd.DataFrame:
    cached = _cached(cache_key)
    if cached is not None:
        return cached
    path = PARQUET_DIR / filename
    if not _IS_LOCAL or not path.exists():
        _s3_download(filename, path)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    _set_cache(cache_key, df)
    return df


def _load_unified_signals(strategy: str) -> pd.DataFrame:
    """signals.parquet から指定 strategy の行を抽出。strategy 列が無い旧形式は空を返す"""
    df = _load_unified("signals.parquet", "unified_signals")
    if df.empty:
        return df
    if "strategy" not in df.columns:
        return df.iloc[0:0].copy()
    return df[df["strategy"] == strategy].copy()


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


def _value_or_default(v, default):
    if pd.isna(v):
        return default
    return v


def _pair_validity_from_full_n(full_n: int) -> dict[str, object]:
    if full_n >= 150:
        return {
            "pair_validity": "principle_strong",
            "pair_validity_label": "原則・厚め",
            "pair_validity_rank": 1,
            "pair_validity_reason": "walk-forward実測N>=150。サンプル厚めの原則候補。",
        }
    if full_n >= 120:
        return {
            "pair_validity": "principle",
            "pair_validity_label": "原則",
            "pair_validity_rank": 2,
            "pair_validity_reason": "walk-forward実測N>=120。原則候補。",
        }
    return {
        "pair_validity": "exception_caution",
        "pair_validity_label": "例外注意",
        "pair_validity_rank": 3,
        "pair_validity_reason": "walk-forward実測N<120。機会は残すがサイズ縮小または見送り候補。",
    }


def _next_business_day_label(signal_date: str) -> tuple[str, str]:
    if not signal_date:
        return "", ""
    try:
        trade_date = pd.Timestamp(signal_date) + pd.tseries.offsets.BDay(1)
    except Exception:
        return "", ""
    weekday = ["月", "火", "水", "木", "金", "土", "日"][trade_date.weekday()]
    return trade_date.strftime("%Y-%m-%d"), weekday


def _pair_operational_labels(item: dict[str, Any]) -> dict[str, str]:
    """Practical operation labels.

    These are not hard-entry rules. They expose the post-diagnostic operation
    policy on the screen while preserving the existing pair signal logic.
    """
    reasons: list[str] = []
    health_state = str(item.get("pair_health_state", "ACTIVE")).upper()
    risk_ok = bool(item.get("risk_ok", True))
    validity_rank = _safe_int(item.get("pair_validity_rank", 3))
    z_abs = _safe_float(item.get("z_abs", 0), 3)
    ret1_spread_abs = _safe_float(item.get("ret1_spread_abs", 0), 5)
    imbalance_pct = _safe_float(item.get("imbalance_pct", 0), 1)
    trade_weekday = str(item.get("trade_weekday", ""))

    if health_state != "ACTIVE":
        reasons.append(f"health={health_state}")
    if not risk_ok:
        reasons.append("risk除外")
    if z_abs >= 5:
        reasons.append("|z|過大")
    if validity_rank >= 3:
        reasons.append("例外注意")
    if ret1_spread_abs >= 0.04:
        reasons.append("ret1差注意")
    if imbalance_pct >= 15:
        reasons.append("不均衡注意")

    if health_state != "ACTIVE" or not risk_ok:
        return {
            "risk_label": "blocked",
            "operation_label": "見送り",
            "operation_reason": " / ".join(reasons) or "healthまたはriskで除外",
            "top3_permission": "不可",
        }

    risk_count = len(reasons)
    risk_label = "clean" if risk_count == 0 else "watch" if risk_count == 1 else "high_risk"

    if trade_weekday in {"月", "木"}:
        return {
            "risk_label": risk_label,
            "operation_label": "Top1限定",
            "operation_reason": "月木は損益が薄くDDが出やすいためTop3抑制" + (f" / {' / '.join(reasons)}" if reasons else ""),
            "top3_permission": "抑制",
        }
    if trade_weekday in {"火", "水", "金"} and risk_label == "clean":
        return {
            "risk_label": risk_label,
            "operation_label": "Top3可",
            "operation_reason": "火水金かつrisk label clean",
            "top3_permission": "可",
        }
    return {
        "risk_label": risk_label,
        "operation_label": "Top1限定",
        "operation_reason": "risk labelありのためTop1まで" + (f" / {' / '.join(reasons)}" if reasons else ""),
        "top3_permission": "不可",
    }


@router.get("/api/dev/pairs/signals")
async def get_pairs_signals():
    """全ペアのシグナル（z-score, 閾値, 直近成績）"""
    df = _load_unified_signals("pairs")

    if df.empty:
        return {"pairs": [], "hot": [], "signal_date": None, "total": 0, "hot_count": 0}

    signal_date = None
    if "signal_date" in df.columns:
        signal_date = pd.to_datetime(df["signal_date"]).max().strftime("%Y-%m-%d")

    # tk -> sector lookup (for frontend-side dedup of top3 display)
    # meta_jquants.parquet は全上場銘柄 (3754) を網羅。all_stocks.parquet (116) は半導体ユニバース限定
    sector_map: dict[str, str] = {}
    stocks_df = _load_unified("meta_jquants.parquet", "unified_meta_jquants")
    if not stocks_df.empty and "ticker" in stocks_df.columns and "sectors" in stocks_df.columns:
        sector_map = dict(zip(stocks_df["ticker"].astype(str), stocks_df["sectors"].astype(str)))

    pair_health = _load_pair_health_state()
    pairs = []
    entry = []
    for _, r in df.iterrows():
        pair_date = ""
        if "signal_date" in r.index and pd.notna(r.get("signal_date")):
            pair_date = pd.to_datetime(r["signal_date"]).strftime("%Y-%m-%d")
        trade_date, trade_weekday = _next_business_day_label(pair_date)
        tk1 = r.get("tk1", "")
        tk2 = r.get("tk2", "")
        health = _pair_health_for(pair_health, str(tk1), str(tk2))
        full_n = _safe_int(r.get("full_n", 0))
        validity = _pair_validity_from_full_n(full_n)
        item = {
            "tk1": tk1,
            "tk2": tk2,
            "sector1": sector_map.get(str(tk1), ""),
            "sector2": sector_map.get(str(tk2), ""),
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
            "ret1_tk1": _safe_float(r.get("ret1_tk1", 0), 5),
            "ret1_tk2": _safe_float(r.get("ret1_tk2", 0), 5),
            "ret1_spread_abs": _safe_float(r.get("ret1_spread_abs", 0), 5),
            "earnings_near": bool(r.get("earnings_near", False)),
            "risk_ok": bool(r.get("risk_ok", True)),
            "risk_model": _value_or_default(r.get("risk_model"), ""),
            "risk_skip_reason": _value_or_default(r.get("risk_skip_reason"), ""),
            "full_pf": _safe_float(r.get("full_pf", 0), 2),
            "full_n": full_n,
            "revert_1d": _safe_float(r.get("revert_1d", r.get("half_life", 0)), 1),
            "pair_validity": _value_or_default(r.get("pair_validity"), validity["pair_validity"]),
            "pair_validity_label": _value_or_default(r.get("pair_validity_label"), validity["pair_validity_label"]),
            "pair_validity_rank": _safe_int(_value_or_default(r.get("pair_validity_rank"), validity["pair_validity_rank"])),
            "pair_validity_reason": _value_or_default(r.get("pair_validity_reason"), validity["pair_validity_reason"]),
            "pair_health_state": health["pair_health_state"],
            "pair_health_reason": health["pair_health_reason"],
            "pair_health_reviewed_at": health["pair_health_reviewed_at"],
            "pair_health_recheck_after": health["pair_health_recheck_after"],
            "is_entry": bool(r.get("is_entry", r.get("is_hot", False))),
            "direction": r.get("direction", ""),
            "signal_date": pair_date,
            "trade_date": trade_date,
            "trade_weekday": trade_weekday,
        }
        item.update(_pair_operational_labels(item))
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
    df = _load_unified_signals("pairs")

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
    pair_health = _load_pair_health_state()
    for _, r in entry_df.iterrows():
        full_n = _safe_int(r.get("full_n", 0))
        validity = _pair_validity_from_full_n(full_n)
        health = _pair_health_for(pair_health, str(r.get("tk1", "")), str(r.get("tk2", "")))
        entry_pairs.append({
            "pair": f"{r.get('name1', r.get('tk1', ''))} / {r.get('name2', r.get('tk2', ''))}",
            "z": _safe_float(r.get("z_latest", 0), 2),
            "direction": r.get("direction", ""),
            "full_pf": _safe_float(r.get("full_pf", 0), 2),
            "full_n": full_n,
            "pair_validity": _value_or_default(r.get("pair_validity"), validity["pair_validity"]),
            "pair_validity_label": _value_or_default(r.get("pair_validity_label"), validity["pair_validity_label"]),
            "pair_validity_rank": _safe_int(_value_or_default(r.get("pair_validity_rank"), validity["pair_validity_rank"])),
            "pair_validity_reason": _value_or_default(r.get("pair_validity_reason"), validity["pair_validity_reason"]),
            "pair_health_state": health["pair_health_state"],
            "pair_health_reason": health["pair_health_reason"],
            "pair_health_recheck_after": health["pair_health_recheck_after"],
            "shares1": _safe_int(r.get("shares1", 100)),
            "shares2": _safe_int(r.get("shares2", 100)),
        })

    return {
        "signal_date": signal_date,
        "total_pairs": len(df),
        "entry_count": len(entry_pairs),
        "entry_pairs": entry_pairs,
    }


@router.get("/api/dev/pairs/health")
async def get_pair_health():
    """Pair health report and current manual state."""
    state = _load_pair_health_state()
    summary_bytes = _read_analysis_bytes("pair_health_check_summary_latest.csv")
    summary_rows: list[dict[str, object]] = []
    if summary_bytes:
        try:
            df = pd.read_csv(io.BytesIO(summary_bytes))
            summary_rows = [
                {str(k): _json_safe(v) for k, v in row.items()}
                for row in df.to_dict(orient="records")
            ]
        except Exception:
            summary_rows = []

    stop = _load_analysis_json("pair_health_stop_candidates_latest.json")
    suspended_to_watch = _load_analysis_json("pair_health_suspended_to_watch_candidates_latest.json")
    restore = _load_analysis_json("pair_health_restore_candidates_latest.json")
    return {
        "state": state,
        "summary": summary_rows,
        "candidates": {
            "stop": stop,
            "suspended_to_watch": suspended_to_watch,
            "restore": restore,
        },
        "counts": {
            "summary": len(summary_rows),
            "stop": len(stop.get("rows", [])) if isinstance(stop, dict) else 0,
            "suspended_to_watch": len(suspended_to_watch.get("rows", [])) if isinstance(suspended_to_watch, dict) else 0,
            "restore": len(restore.get("rows", [])) if isinstance(restore, dict) else 0,
            "suspended": len([p for p in state.get("pairs", {}).values() if isinstance(p, dict) and p.get("state") == "SUSPENDED"]),
            "watch": len([p for p in state.get("pairs", {}).values() if isinstance(p, dict) and p.get("state") == "WATCH"]),
        },
    }


@router.post("/api/dev/pairs/health/state")
async def update_pair_health_state(req: PairHealthStateRequest):
    """Manual pair health state update. Writes local JSON and uploads to S3."""
    allowed = {"ACTIVE", "WATCH", "SUSPENDED"}
    if not req.updates:
        raise HTTPException(status_code=400, detail="updates is empty")

    state = _load_pair_health_state()
    pairs = state.setdefault("pairs", {})
    if not isinstance(pairs, dict):
        pairs = {}
        state["pairs"] = pairs

    today = datetime.now().date()
    default_recheck = (today + timedelta(days=30)).isoformat()
    updated = []
    for update in req.updates:
        next_state = update.state.upper()
        if next_state not in allowed:
            raise HTTPException(status_code=400, detail=f"invalid state: {update.state}")
        pair = update.pair.strip()
        if "/" not in pair:
            raise HTTPException(status_code=400, detail=f"invalid pair: {pair}")
        reason_parts = [p for p in [update.reason.strip(), req.note.strip()] if p]
        prev = pairs.get(pair, {}) if isinstance(pairs.get(pair, {}), dict) else {}
        if next_state == "ACTIVE":
            if pair in pairs:
                pairs.pop(pair, None)
        else:
            pairs[pair] = {
                "state": next_state,
                "reason": " / ".join(reason_parts) or str(prev.get("reason", "manual update")),
                "reviewed_at": today.isoformat(),
                "recheck_after": update.recheck_after or default_recheck,
            }
        updated.append({"pair": pair, "state": next_state})

    state["generated_at"] = today.isoformat()
    _write_pair_health_state(state)
    _cache.clear()
    return {
        "status": "success",
        "updated": updated,
        "updated_at": datetime.now().isoformat(),
    }


@router.post("/api/dev/pairs/refresh")
async def refresh_pairs_cache():
    """キャッシュクリア（サーバーは次回リクエストでS3から再読込）"""
    _cache.clear()
    return {
        "status": "success",
        "message": "Cache cleared" if _IS_LOCAL else "Cache cleared, next request will reload from S3",
        "updated_at": datetime.now().isoformat(),
    }


# --- ペアチャート用 ---

# V2_PAIRS: (tk1, tk2, lookback, pf, n, revert_1d) — パイプラインと同一定義
try:
    from scripts.pipeline.generate_pairs_signals import V2_PAIRS
except ImportError:
    V2_PAIRS = []

_V2_LOOKUP: dict[tuple[str, str], tuple[int, float, int, float]] = {
    (tk1, tk2): (lb, pf, n, r1d) for tk1, tk2, lb, pf, n, r1d in V2_PAIRS
}


def _load_prices_for_chart(tk1: str, tk2: str, days: int) -> pd.DataFrame:
    """チャート用に2銘柄の日足Closeを取得

    signals.parquet → all_stocks.parquet → prices_max_1d.parquet の流れで
    pair銘柄は prices_max_1d に含まれる。read_prices_1d_df は
    ローカル→S3フォールバック＋キャッシュ済み。
    """
    df = read_prices_1d_df()
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

    # ペア情報取得
    pair_info = _V2_LOOKUP.get((tk1, tk2)) or _V2_LOOKUP.get((tk2, tk1))
    if pair_info is None:
        return JSONResponse(status_code=404, content={"detail": f"Pair {tk1}/{tk2} not found in V2_PAIRS"})

    # tk1/tk2の順序をV2_PAIRSに合わせる
    if (tk1, tk2) not in _V2_LOOKUP:
        tk1, tk2 = tk2, tk1
    lookback, full_pf, full_n, revert_1d = _V2_LOOKUP[(tk1, tk2)]

    # 銘柄名・閾値 — signals.parquet (pairs strategy) から取得
    name1, name2 = tk1, tk2
    tk1_upper, tk1_lower = 0.0, 0.0
    sig_df = _load_unified_signals("pairs")
    if not sig_df.empty:
        match = sig_df[(sig_df["tk1"] == tk1) & (sig_df["tk2"] == tk2)]
        if not match.empty:
            row = match.iloc[0]
            name1 = str(row.get("name1", tk1))
            name2 = str(row.get("name2", tk2))
            tk1_upper = _safe_float(row.get("tk1_upper", 0))
            tk1_lower = _safe_float(row.get("tk1_lower", 0))

    # 価格データ取得
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

    # 正規化価格 (返却範囲の先頭 = 100)
    # lookback分の余裕を取ったので、返却はlookback以降
    start_idx = lookback
    base1 = c1[start_idx]
    base2 = c2[start_idx]
    norm1 = (c1[start_idx:] / base1 * 100).round(2).tolist()
    norm2 = (c2[start_idx:] / base2 * 100).round(2).tolist()

    # ローリングz-score + ローリング半減期
    spread = np.log(c1 / c2)
    z_scores = []
    rolling_hl = []
    hl_window = max(lookback, 60)  # 半減期計算は最低60日のウィンドウ
    for i in range(start_idx, len(spread)):
        window = spread[i - lookback + 1: i + 1]
        mu = window.mean()
        sigma = window.std()
        if sigma < 1e-8:
            z_scores.append(0.0)
        else:
            z_scores.append(round(float((spread[i] - mu) / sigma), 4))

        # ローリング半減期: AR(1)係数からhalf_life = -log(2)/log(beta)
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
                    rolling_hl.append(min(hl_val, 999.0))  # 上限キャップ
                else:
                    rolling_hl.append(None)
            else:
                rolling_hl.append(None)
        else:
            rolling_hl.append(None)

    out_dates = dates[start_idx:]

    # 最新z-scoreからdirection判定
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

    # 終値・前日比
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
        "revert_1d": revert_1d,
        "z_latest": round(z_latest, 3),
        "direction": direction,
        "tk1_upper": tk1_upper,
        "tk1_lower": tk1_lower,
        "series": series,
    }

    _set_cache(cache_key, result)
    return result
