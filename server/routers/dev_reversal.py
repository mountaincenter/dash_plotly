# server/routers/dev_reversal.py
"""
逆張り戦略統合API（B4 + 大陰線）
/api/dev/reversal/* — 統合シグナル・ステータス・ポジション
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

GRANVILLE_DIR = PARQUET_DIR / "granville"
REVERSAL_DIR = PARQUET_DIR / "reversal"

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


def _s3_download(s3_key: str, local_path: Path) -> bool:
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.download_file(S3_BUCKET, f"{S3_PREFIX}/{s3_key}", str(local_path))
        return True
    except Exception as e:
        print(f"[S3] download failed: {S3_BUCKET}/{S3_PREFIX}/{s3_key} → {e}")
        return False


def _latest_file(directory: Path, prefix: str) -> Optional[Path]:
    """指定ディレクトリから最新の日付ファイルを取得"""
    files = sorted(directory.glob(f"{prefix}_*.parquet"))
    return files[-1] if files else None


def _load_latest(directory: Path, prefix: str, s3_prefix: str) -> pd.DataFrame:
    """最新ファイルを読み込み（キャッシュ+S3フォールバック）"""
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


def _safe_int(v) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def _safe_float(v, decimals: int = 1) -> float:
    try:
        return round(float(v), decimals)
    except (ValueError, TypeError):
        return 0.0


def _get_vi() -> Optional[float]:
    """最新の日経VI値を取得"""
    vi_path = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
    if vi_path.exists():
        try:
            vi_df = pd.read_parquet(vi_path)
            vi_df["date"] = pd.to_datetime(vi_df["date"])
            vi_df = vi_df.sort_values("date")
            return float(vi_df["close"].iloc[-1])
        except Exception:
            pass
    return None


# ==========================================
# エンドポイント
# ==========================================

@router.post("/api/dev/reversal/refresh")
async def refresh_cache():
    """キャッシュクリア+S3再ダウンロード"""
    _cache.clear()

    refreshed = []
    for f in ["hold_stocks.parquet", "credit_status.parquet",
              "nikkei_vi_max_1d.parquet", "index_prices_max_1d.parquet",
              "futures_prices_max_1d.parquet"]:
        local = PARQUET_DIR / f
        if _s3_download(f, local):
            refreshed.append(f)

    return {
        "status": "success",
        "message": "Cache refreshed",
        "refreshed_files": refreshed,
        "updated_at": datetime.now().isoformat(),
    }


@router.get("/api/dev/reversal/signals")
async def get_signals():
    """B4 + 大陰線の統合シグナル（1レスポンスで両方返す）"""
    # 大陰線シグナル
    bearish_df = _load_latest(REVERSAL_DIR, "bearish_signals", "reversal")
    bearish_signals = []
    bearish_date = None

    if not bearish_df.empty:
        if "signal_date" in bearish_df.columns:
            bearish_date = pd.to_datetime(bearish_df["signal_date"].iloc[0]).strftime("%Y-%m-%d")
        for _, r in bearish_df.iterrows():
            bearish_signals.append({
                "ticker": r.get("ticker", ""),
                "stock_name": r.get("stock_name", ""),
                "sector": r.get("sector", ""),
                "strategy": "bearish",
                "close": _safe_float(r.get("close", 0)),
                "open": _safe_float(r.get("open", 0)),
                "body_pct": _safe_float(r.get("body_pct", 0), 2),
                "sma20": _safe_float(r.get("sma20", 0), 2),
                "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
                "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
                "prev_close": _safe_float(r.get("prev_close", 0)),
                "vi": _safe_float(r.get("vi", 0), 1),
            })

    # B4シグナル（granvilleから取得）
    b4_df = _load_latest(GRANVILLE_DIR, "signals", "granville")
    b4_signals = []
    b4_date = None

    if not b4_df.empty:
        if "signal_date" in b4_df.columns:
            b4_date = pd.to_datetime(b4_df["signal_date"].iloc[0]).strftime("%Y-%m-%d")
        b4_only = b4_df[b4_df["rule"] == "B4"] if "rule" in b4_df.columns else pd.DataFrame()
        for _, r in b4_only.iterrows():
            b4_signals.append({
                "ticker": r.get("ticker", ""),
                "stock_name": r.get("stock_name", ""),
                "sector": r.get("sector", ""),
                "strategy": "B4",
                "close": _safe_float(r.get("close", 0)),
                "open": _safe_float(r.get("open", 0)),
                "body_pct": 0.0,
                "sma20": _safe_float(r.get("sma20", 0), 2),
                "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
                "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
                "prev_close": _safe_float(r.get("prev_close", 0)),
                "vi": 0.0,
            })

    return {
        "bearish": bearish_signals,
        "b4": b4_signals,
        "bearish_count": len(bearish_signals),
        "b4_count": len(b4_signals),
        "bearish_date": bearish_date,
        "b4_date": b4_date,
    }


@router.get("/api/dev/reversal/status")
async def get_status():
    """統合ステータス（VI、信用余力、両戦略件数）"""
    vi = _get_vi()

    # 大陰線シグナル数
    bearish_df = _load_latest(REVERSAL_DIR, "bearish_signals", "reversal")
    bearish_count = len(bearish_df)
    bearish_date = None
    if not bearish_df.empty and "signal_date" in bearish_df.columns:
        bearish_date = pd.to_datetime(bearish_df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    # B4シグナル数
    b4_df = _load_latest(GRANVILLE_DIR, "signals", "granville")
    b4_count = 0
    b4_date = None
    if not b4_df.empty:
        if "signal_date" in b4_df.columns:
            b4_date = pd.to_datetime(b4_df["signal_date"].iloc[0]).strftime("%Y-%m-%d")
        if "rule" in b4_df.columns:
            b4_count = int((b4_df["rule"] == "B4").sum())

    # 信用余力
    cash_margin = 0
    cs_path = PARQUET_DIR / "credit_status.parquet"
    if cs_path.exists():
        try:
            cs = pd.read_parquet(cs_path)
            row = cs[cs["asset"].str.contains("信用", na=False)]
            if not row.empty:
                cash_margin = int(row["value"].iloc[0])
        except Exception:
            pass
    if cash_margin == 0:
        cash_margin = 4_650_000

    # ポジション数
    hold_path = PARQUET_DIR / "hold_stocks.parquet"
    position_count = 0
    if hold_path.exists():
        try:
            position_count = len(pd.read_parquet(hold_path))
        except Exception:
            pass

    # 大陰線ポジション数
    bearish_pos = _load_latest(REVERSAL_DIR, "bearish_positions", "reversal")
    bearish_open = 0
    bearish_exit = 0
    if not bearish_pos.empty and "status" in bearish_pos.columns:
        bearish_open = int((bearish_pos["status"] == "open").sum())
        bearish_exit = int((bearish_pos["status"] == "exit").sum())

    return {
        "vi": _safe_float(vi, 1) if vi else None,
        "vi_signal": "green" if vi and vi >= 20 else "red" if vi else "unknown",
        "cash_margin": cash_margin,
        "position_count": position_count,
        "bearish_count": bearish_count,
        "bearish_date": bearish_date,
        "bearish_open": bearish_open,
        "bearish_exit": bearish_exit,
        "b4_count": b4_count,
        "b4_date": b4_date,
        "signal_date": bearish_date or b4_date,
    }


@router.get("/api/dev/reversal/positions")
async def get_positions():
    """B4 + 大陰線の統合ポジション"""
    # 大陰線ポジション
    bearish_pos = _load_latest(REVERSAL_DIR, "bearish_positions", "reversal")
    bearish_rows = []
    if not bearish_pos.empty:
        for _, r in bearish_pos.iterrows():
            bearish_rows.append({
                "ticker": r.get("ticker", ""),
                "stock_name": r.get("stock_name", ""),
                "strategy": "bearish",
                "entry_date": pd.to_datetime(r["entry_date"]).strftime("%Y-%m-%d") if "entry_date" in r else "",
                "entry_price": _safe_float(r.get("entry_price", 0)),
                "current_price": _safe_float(r.get("current_price", 0)),
                "sma20": _safe_float(r.get("sma20", 0)),
                "pct": _safe_float(r.get("pct", 0), 2),
                "pnl": _safe_int(r.get("pnl", 0)),
                "hold_days": _safe_int(r.get("hold_days", 0)),
                "max_hold": _safe_int(r.get("max_hold", 0)),
                "status": r.get("status", ""),
                "exit_type": r.get("exit_type", ""),
            })

    # B4ポジション（granvilleから）
    b4_pos = _load_latest(GRANVILLE_DIR, "positions", "granville")
    b4_rows = []
    if not b4_pos.empty:
        b4_only = b4_pos[b4_pos["rule"] == "B4"] if "rule" in b4_pos.columns else pd.DataFrame()
        for _, r in b4_only.iterrows():
            b4_rows.append({
                "ticker": r.get("ticker", ""),
                "stock_name": r.get("stock_name", ""),
                "strategy": "B4",
                "entry_date": pd.to_datetime(r["entry_date"]).strftime("%Y-%m-%d") if "entry_date" in r else "",
                "entry_price": _safe_float(r.get("entry_price", 0)),
                "current_price": _safe_float(r.get("current_price", 0)),
                "sma20": 0.0,
                "pct": _safe_float(r.get("pct", 0), 2),
                "pnl": _safe_int(r.get("pnl", 0)),
                "hold_days": _safe_int(r.get("hold_days", 0)),
                "max_hold": _safe_int(r.get("max_hold", 0)),
                "status": r.get("status", ""),
                "exit_type": r.get("exit_type", ""),
            })

    # 統合（大陰線 + B4）
    all_positions = bearish_rows + b4_rows

    return {
        "positions": [p for p in all_positions if p["status"] == "open"],
        "exits": [p for p in all_positions if p["status"] == "exit"],
        "bearish_count": len(bearish_rows),
        "b4_count": len(b4_rows),
    }
