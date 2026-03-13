# server/routers/dev_granville.py
"""
グランビル戦略 API（B1-B4, TOPIX 1,660銘柄）
/api/dev/granville/* - 推奨銘柄・シグナル・ポジション・ステータス・統計
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
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
META_FALLBACK = PARQUET_DIR / "meta.parquet"
CSV_DIR = ROOT / "data" / "csv"

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


def _latest_file(prefix: str) -> Optional[Path]:
    """granvilleディレクトリから最新の日付ファイルを取得"""
    files = sorted(GRANVILLE_DIR.glob(f"{prefix}_*.parquet"))
    return files[-1] if files else None


def _load_latest(prefix: str) -> pd.DataFrame:
    """最新ファイルを読み込み（キャッシュ付き）"""
    cached = _cached(prefix)
    if cached is not None:
        return cached

    path = _latest_file(prefix)
    if path is None:
        # S3フォールバック
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            resp = s3.list_objects_v2(
                Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/granville/{prefix}_",
            )
            if "Contents" in resp:
                keys = sorted([o["Key"] for o in resp["Contents"]])
                if keys:
                    local = GRANVILLE_DIR / Path(keys[-1]).name
                    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)
                    s3.download_file(S3_BUCKET, keys[-1], str(local))
                    path = local
        except Exception:
            pass

    if path is None:
        return pd.DataFrame()

    df = pd.read_parquet(path)
    _set_cache(prefix, df)
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


# ==========================================
# エンドポイント
# ==========================================

@router.get("/api/dev/granville/recommendations")
async def get_recommendations():
    """当日推奨銘柄（B4>B1>B3>B2、到着順、15%証拠金上限済み）"""
    df = _load_latest("recommendations")
    if df.empty:
        return {"recommendations": [], "count": 0, "date": None}

    date_str = None
    if "signal_date" in df.columns:
        date_str = pd.to_datetime(df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    recs = []
    for _, r in df.iterrows():
        rec = {
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "rule": r.get("rule", ""),
            "rank_score": _safe_float(r.get("rank_score", 0)),
            "close": _safe_float(r.get("close", 0)),
            "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
            "prev_close": _safe_float(r.get("prev_close", 0)),
            "sma20": _safe_float(r.get("sma20", 0), 2),
            "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
            "atr10_pct": _safe_float(r.get("atr10_pct", 0), 2),
            "vol_ratio": _safe_float(r.get("vol_ratio", 0), 2),
            "expected_profit": _safe_int(r.get("expected_profit", 0)),
            "margin": _safe_int(r.get("margin", 0)),
            "margin_pct": _safe_float(r.get("margin_pct", 0)),
            "max_hold": _safe_int(r.get("max_hold", 0)),
        }
        recs.append(rec)

    total_margin = sum(r["margin"] for r in recs)

    return {
        "recommendations": recs,
        "count": len(recs),
        "total_margin": total_margin,
        "date": date_str,
    }


@router.get("/api/dev/granville/signals")
async def get_signals():
    """当日全シグナル（フィルター前）"""
    df = _load_latest("signals")
    if df.empty:
        return {"signals": [], "count": 0, "signal_date": None}

    date_str = None
    if "signal_date" in df.columns:
        date_str = pd.to_datetime(df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    signals = []
    for _, r in df.iterrows():
        sig = {
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "rule": r.get("rule", ""),
            "close": _safe_float(r.get("close", 0)),
            "sma20": _safe_float(r.get("sma20", 0), 2),
            "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
            "sma20_slope": _safe_float(r.get("sma20_slope", 0), 4),
            "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
            "prev_close": _safe_float(r.get("prev_close", 0)),
        }
        signals.append(sig)

    return {
        "signals": signals,
        "count": len(signals),
        "signal_date": date_str,
    }


@router.get("/api/dev/granville/positions")
async def get_positions():
    """保有ポジション + Exit候補"""
    df = _load_latest("positions")
    if df.empty:
        return {"positions": [], "exits": [], "as_of": None}

    as_of = None
    if "as_of" in df.columns:
        as_of = pd.to_datetime(df["as_of"].iloc[0]).strftime("%Y-%m-%d")

    # stock_nameがなければメタから補完
    if "stock_name" not in df.columns:
        for p in [META_PATH, META_FALLBACK]:
            if p.exists():
                meta = pd.read_parquet(p, columns=["ticker", "stock_name"])
                name_map = dict(zip(meta["ticker"], meta["stock_name"]))
                df["stock_name"] = df["ticker"].map(name_map).fillna("")
                break
        else:
            df["stock_name"] = ""

    positions = []
    exits = []
    for _, r in df.iterrows():
        cp = _safe_float(r.get("current_price", 0))
        high_20d = _safe_float(r.get("high_20d", 0))
        atr10 = _safe_float(r.get("atr10", 0))
        entry = {
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "rule": r.get("rule", ""),
            "entry_date": pd.to_datetime(r["entry_date"]).strftime("%Y-%m-%d") if "entry_date" in r else "",
            "entry_price": _safe_float(r.get("entry_price", 0)),
            "current_price": cp,
            "high_20d": high_20d,
            "atr10": atr10,
            "gap_to_high": _safe_int(round(high_20d - cp)) if high_20d > 0 else 0,
            "unrealized_pct": _safe_float(r.get("pct", 0), 2),
            "unrealized_yen": _safe_int(r.get("pnl", 0)),
            "hold_days": _safe_int(r.get("hold_days", 0)),
            "max_hold": _safe_int(r.get("max_hold", 0)),
            "remaining_days": max(0, _safe_int(r.get("max_hold", 0)) - _safe_int(r.get("hold_days", 0))),
            "exit_type": r.get("exit_type", ""),
        }
        if r.get("status") == "exit":
            exits.append(entry)
        else:
            positions.append(entry)

    return {"positions": positions, "exits": exits, "as_of": as_of}


@router.get("/api/dev/granville/status")
async def get_status():
    """証拠金残、ポジション数、シグナル数"""
    # §7: デフォルト465万
    available_margin = 4_650_000
    credit_csv = CSV_DIR / "credit_capacity.csv"
    if credit_csv.exists():
        try:
            cc = pd.read_csv(credit_csv)
            if not cc.empty:
                available_margin = float(cc.iloc[-1]["available_margin"])
        except Exception:
            pass

    # シグナル数
    signals_df = _load_latest("signals")
    signal_count = len(signals_df)
    signal_date = None
    if not signals_df.empty and "signal_date" in signals_df.columns:
        signal_date = pd.to_datetime(signals_df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    # ポジション数
    pos_df = _load_latest("positions")
    open_count = 0
    exit_count = 0
    if not pos_df.empty and "status" in pos_df.columns:
        open_count = int((pos_df["status"] == "open").sum())
        exit_count = int((pos_df["status"] == "exit").sum())

    # 推奨数
    rec_df = _load_latest("recommendations")
    rec_count = len(rec_df)
    total_margin_used = 0
    if not rec_df.empty and "margin" in rec_df.columns:
        total_margin_used = int(rec_df["margin"].sum())

    # ルール別内訳
    rule_breakdown = {}
    for rule in ["B4", "B1", "B3", "B2"]:
        if not signals_df.empty and "rule" in signals_df.columns:
            rule_breakdown[rule] = int((signals_df["rule"] == rule).sum())
        else:
            rule_breakdown[rule] = 0

    return {
        "available_margin": int(available_margin),
        "total_margin_used": total_margin_used,
        "signal_count": signal_count,
        "signal_date": signal_date,
        "open_positions": open_count,
        "exit_candidates": exit_count,
        "recommendation_count": rec_count,
        "rule_breakdown": rule_breakdown,
    }


@router.get("/api/dev/granville/stats")
async def get_stats():
    """ルール別勝率、月別PnL（B1-B4バックテストアーカイブから）"""
    archive_path = PARQUET_DIR / "backtest" / "granville_b1b4_archive.parquet"
    if not archive_path.exists():
        # S3フォールバック
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            s3_key = f"{S3_PREFIX}/backtest/granville_b1b4_archive.parquet"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(S3_BUCKET, s3_key, str(archive_path))
        except Exception:
            return {"by_rule": {}, "monthly": [], "total_trades": 0}

    if not archive_path.exists():
        return {"by_rule": {}, "monthly": [], "total_trades": 0}

    cached = _cached("stats_archive")
    if cached is not None:
        df = cached
    else:
        df = pd.read_parquet(archive_path)
        _set_cache("stats_archive", df)

    if df.empty:
        return {"by_rule": {}, "monthly": [], "total_trades": 0}

    df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
    df["pnl_yen"] = pd.to_numeric(df["pnl_yen"], errors="coerce").fillna(0).astype(int)
    for col in ["entry_date", "exit_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    # B1-B4 ルール別統計
    by_rule = {}
    for rule in ["B4", "B1", "B3", "B2"]:
        gdf = df[df["rule"] == rule]
        n = len(gdf)
        if n == 0:
            by_rule[rule] = {"count": 0, "win_rate": 0.0, "total_pnl": 0, "avg_pnl": 0, "avg_pct": 0.0, "exit_20d_high": 0, "exit_max_hold": 0}
            continue
        wins = int((gdf["ret_pct"] > 0).sum())
        h20 = int((gdf["exit_type"] == "20d_high").sum()) if "exit_type" in gdf.columns else 0
        mh = int((gdf["exit_type"] == "max_hold").sum()) if "exit_type" in gdf.columns else 0
        by_rule[rule] = {
            "count": n,
            "win_rate": _safe_float(wins / n * 100),
            "total_pnl": _safe_int(gdf["pnl_yen"].sum()),
            "avg_pnl": _safe_int(gdf["pnl_yen"].mean()),
            "avg_pct": _safe_float(gdf["ret_pct"].mean(), 2),
            "exit_20d_high": h20,
            "exit_max_hold": mh,
        }

    # 月別統計
    monthly = []
    if "entry_date" in df.columns:
        df["month"] = df["entry_date"].dt.strftime("%Y-%m")
        for month, mdf in df.groupby("month"):
            n = len(mdf)
            wins = int((mdf["ret_pct"] > 0).sum())
            monthly.append({
                "month": month,
                "count": n,
                "pnl": _safe_int(mdf["pnl_yen"].sum()),
                "win_rate": _safe_float(wins / n * 100),
            })
        monthly.sort(key=lambda x: x["month"], reverse=True)

    return {
        "by_rule": by_rule,
        "monthly": monthly[:24],
        "total_trades": len(df),
    }
