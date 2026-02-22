# server/routers/dev_granville.py
"""
グランビルIFDロング戦略 API
/api/dev/granville/* - シグナル・バックテスト・ステータス
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

SIGNALS_FILE = PARQUET_DIR / "granville_ifd_signals.parquet"
ARCHIVE_FILE = PARQUET_DIR / "backtest" / "granville_ifd_archive.parquet"
INDEX_FILE = PARQUET_DIR / "index_prices_max_1d.parquet"
MACRO_DIR = ROOT / "improvement" / "data" / "macro"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# キャッシュ
_signals_cache: Optional[pd.DataFrame] = None
_archive_cache: Optional[pd.DataFrame] = None
_cache_ts: Optional[datetime] = None
CACHE_TTL = 60


def _is_fresh() -> bool:
    return _cache_ts is not None and (datetime.now() - _cache_ts).total_seconds() < CACHE_TTL


def _load_parquet(local: Path, s3_key: str) -> pd.DataFrame:
    """ローカル優先 → S3フォールバック"""
    if local.exists():
        return pd.read_parquet(local)
    try:
        s3_url = f"s3://{S3_BUCKET}/{S3_PREFIX}{s3_key}"
        return pd.read_parquet(s3_url, storage_options={"client_kwargs": {"region_name": AWS_REGION}})
    except Exception:
        return pd.DataFrame()


def load_signals() -> pd.DataFrame:
    global _signals_cache, _cache_ts
    if _signals_cache is not None and _is_fresh():
        return _signals_cache
    df = _load_parquet(SIGNALS_FILE, "granville_ifd_signals.parquet")
    if not df.empty and "signal_date" in df.columns:
        df["signal_date"] = pd.to_datetime(df["signal_date"])
    _signals_cache = df
    _cache_ts = datetime.now()
    return df


def load_archive() -> pd.DataFrame:
    global _archive_cache, _cache_ts
    if _archive_cache is not None and _is_fresh():
        return _archive_cache
    df = _load_parquet(ARCHIVE_FILE, "backtest/granville_ifd_archive.parquet")
    if not df.empty:
        for col in ["signal_date", "entry_date", "exit_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        if "ret_pct" in df.columns:
            df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
        if "pnl_yen" in df.columns:
            df["pnl_yen"] = pd.to_numeric(df["pnl_yen"], errors="coerce").fillna(0).astype(int)
    _archive_cache = df
    _cache_ts = datetime.now()
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


@router.get("/api/dev/granville/signals")
async def get_signals():
    """今日のシグナル（翌日エントリー候補）"""
    df = load_signals()
    if df.empty:
        return {"signals": [], "count": 0, "signal_date": None}

    latest_date = df["signal_date"].max()
    rows = df[df["signal_date"] == latest_date]

    signals = []
    for _, r in rows.iterrows():
        signals.append({
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "signal_type": r.get("signal_type", ""),
            "close": _safe_float(r.get("close", 0), 1),
            "sma20": _safe_float(r.get("sma20", 0), 2),
            "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
            "sl_price": _safe_float(r.get("sl_price", 0), 1),
        })

    return {
        "signals": signals,
        "count": len(signals),
        "signal_date": latest_date.strftime("%Y-%m-%d"),
    }


@router.get("/api/dev/granville/summary")
async def get_summary():
    """バックテスト全体統計 + 月別"""
    df = load_archive()
    if df.empty:
        return {"overall": {}, "monthly": [], "count": 0}

    # 全体統計
    n = len(df)
    wins = df["ret_pct"] > 0
    losses = df["ret_pct"] <= 0
    w_pnl = df.loc[wins, "pnl_yen"].sum()
    l_pnl = abs(df.loc[losses, "pnl_yen"].sum())
    pf = round(w_pnl / l_pnl, 2) if l_pnl > 0 else 999
    sl_count = (df["exit_type"] == "SL").sum() if "exit_type" in df.columns else 0

    overall = {
        "count": n,
        "total_pnl": _safe_int(df["pnl_yen"].sum()),
        "win_rate": _safe_float(wins.mean() * 100),
        "pf": pf,
        "avg_ret": _safe_float(df["ret_pct"].mean(), 3),
        "sl_count": _safe_int(sl_count),
        "sl_rate": _safe_float(sl_count / n * 100) if n > 0 else 0,
    }

    # 月別統計
    df["month"] = df["entry_date"].dt.strftime("%Y-%m")
    monthly = []
    for month, mdf in df.groupby("month"):
        m_n = len(mdf)
        m_wins = mdf["ret_pct"] > 0
        m_losses = mdf["ret_pct"] <= 0
        m_w = mdf.loc[m_wins, "pnl_yen"].sum()
        m_l = abs(mdf.loc[m_losses, "pnl_yen"].sum())
        m_pf = round(m_w / m_l, 2) if m_l > 0 else 999
        m_sl = (mdf["exit_type"] == "SL").sum() if "exit_type" in mdf.columns else 0
        m_uptrend = mdf["market_uptrend"].mean() * 100 if "market_uptrend" in mdf.columns else 0

        monthly.append({
            "month": month,
            "count": m_n,
            "pnl": _safe_int(mdf["pnl_yen"].sum()),
            "win_rate": _safe_float(m_wins.mean() * 100),
            "pf": m_pf,
            "sl_count": _safe_int(m_sl),
            "uptrend_pct": _safe_float(m_uptrend),
        })

    monthly.sort(key=lambda x: x["month"], reverse=True)

    return {
        "overall": overall,
        "monthly": monthly,
        "count": n,
    }


@router.get("/api/dev/granville/trades")
async def get_trades(view: str = "daily"):
    """トレード詳細 (daily / monthly / by-stock)"""
    df = load_archive()
    if df.empty:
        return {"view": view, "results": []}

    if view == "monthly":
        group_col = df["entry_date"].dt.strftime("%Y-%m")
    elif view == "weekly":
        group_col = df["entry_date"].dt.strftime("%Y/W%W")
    elif view == "by-stock":
        group_col = df["ticker"]
    else:
        group_col = df["entry_date"].dt.strftime("%Y-%m-%d")

    results = []
    for key, gdf in df.groupby(group_col):
        trades = []
        for _, r in gdf.iterrows():
            trades.append({
                "signal_date": r["signal_date"].strftime("%Y-%m-%d") if pd.notna(r.get("signal_date")) else "",
                "entry_date": r["entry_date"].strftime("%Y-%m-%d") if pd.notna(r.get("entry_date")) else "",
                "exit_date": r["exit_date"].strftime("%Y-%m-%d") if pd.notna(r.get("exit_date")) else "",
                "ticker": r.get("ticker", ""),
                "stock_name": r.get("stock_name", ""),
                "sector": r.get("sector", ""),
                "signal_type": r.get("signal_type", ""),
                "entry_price": _safe_float(r.get("entry_price", 0)),
                "exit_price": _safe_float(r.get("exit_price", 0)),
                "ret_pct": _safe_float(r.get("ret_pct", 0), 3),
                "pnl_yen": _safe_int(r.get("pnl_yen", 0)),
                "exit_type": r.get("exit_type", ""),
            })

        trades.sort(key=lambda x: x["entry_date"], reverse=True)

        g_pnl = gdf["pnl_yen"].sum()
        g_wins = (gdf["ret_pct"] > 0).sum()

        if view == "by-stock":
            label = f"{key} {gdf.iloc[0].get('stock_name', '')}"
        else:
            label = str(key)

        results.append({
            "key": label,
            "count": len(gdf),
            "pnl": _safe_int(g_pnl),
            "win_count": _safe_int(g_wins),
            "trades": trades,
        })

    results.sort(key=lambda x: x["key"], reverse=True)

    return {"view": view, "results": results}


@router.get("/api/dev/granville/status")
async def get_status():
    """現在のフィルター状態（uptrend/CI/N225 vs SMA20）"""
    result = {
        "market_uptrend": None,
        "ci_expand": None,
        "nk225_close": None,
        "nk225_sma20": None,
        "nk225_diff_pct": None,
        "ci_latest": None,
        "ci_chg3m": None,
        "as_of": None,
    }

    # N225
    try:
        idx = _load_parquet(INDEX_FILE, "index_prices_max_1d.parquet")
        if not idx.empty:
            nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
            nk["date"] = pd.to_datetime(nk["date"])
            nk = nk.sort_values("date")
            nk["sma20"] = nk["Close"].rolling(20).mean()
            latest = nk.dropna(subset=["sma20"]).iloc[-1]
            result["nk225_close"] = _safe_float(latest["Close"], 2)
            result["nk225_sma20"] = _safe_float(latest["sma20"], 2)
            result["market_uptrend"] = bool(latest["Close"] > latest["sma20"])
            result["nk225_diff_pct"] = _safe_float(
                (latest["Close"] - latest["sma20"]) / latest["sma20"] * 100, 2
            )
            result["as_of"] = latest["date"].strftime("%Y-%m-%d")
    except Exception:
        pass

    # CI（ローカル優先 → S3フォールバック）
    try:
        ci_path = MACRO_DIR / "estat_ci_index.parquet"
        ci = _load_parquet(ci_path, "macro/estat_ci_index.parquet")
        ci = ci[["date", "leading"]].sort_values("date")
        ci["chg3m"] = ci["leading"].diff(3)
        latest_ci = ci.dropna(subset=["chg3m"]).iloc[-1]
        result["ci_latest"] = _safe_float(latest_ci["leading"], 1)
        result["ci_chg3m"] = _safe_float(latest_ci["chg3m"], 2)
        result["ci_expand"] = bool(latest_ci["chg3m"] > 0)
    except Exception:
        pass

    return result
