"""
Calendar Trades API
/api/dev/calendar — SQ-4 + 1306ETF四半期末
"""
from __future__ import annotations

import io
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
ETF_1306_PATH = PARQUET_DIR / "etf_1306_prices.parquet"
QE_JSON_PATH = ROOT / "data" / "analysis" / "quarter_end_effect.json"

S3_BUCKET = os.getenv("S3_BUCKET", os.getenv("DATA_BUCKET", "stock-api-data"))
_S3_PREFIX_RAW = os.getenv("PARQUET_PREFIX", "parquet")
S3_PREFIX = _S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
_IS_LOCAL = PARQUET_DIR.exists()

_cache: dict[str, tuple[datetime, object]] = {}
CACHE_TTL = 300


def _cached(key: str) -> Optional[object]:
    if key in _cache:
        ts, data = _cache[key]
        if (datetime.now() - ts).total_seconds() < CACHE_TTL:
            return data
    return None


def _set_cache(key: str, data: object) -> None:
    _cache[key] = (datetime.now(), data)


def _read_parquet_by_env(filename: str) -> pd.DataFrame:
    """環境フラグに応じてparquetを完全分離で読み込む（フォールバック禁止）。"""
    if _IS_LOCAL:
        return pd.read_parquet(PARQUET_DIR / filename)

    import boto3

    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/{filename}")
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _read_json_by_env(s3_key: str, local_path: Path) -> dict:
    """環境フラグに応じてJSONを完全分離で読み込む（フォールバック禁止）。"""
    if _IS_LOCAL:
        with open(local_path, encoding="utf-8") as f:
            return json.load(f)

    import boto3

    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _s3_download(s3_key: str, local_path: Path) -> bool:
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.download_file(S3_BUCKET, f"{S3_PREFIX}/{s3_key}", str(local_path))
        return True
    except Exception as e:
        print(f"[S3] download failed: {S3_PREFIX}/{s3_key} → {e}")
        return False


def _load_calendar() -> pd.DataFrame:
    cached = _cached("calendar")
    if cached is not None:
        return cached
    df = _read_parquet_by_env("calendar.parquet")
    df["date"] = pd.to_datetime(df["date"])
    _set_cache("calendar", df)
    return df


def _load_1306_prices() -> pd.DataFrame:
    cached = _cached("1306_prices")
    if cached is not None:
        return cached
    df = _read_parquet_by_env("etf_1306_prices.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    _set_cache("1306_prices", df)
    return df


def _load_qe_json() -> dict:
    cached = _cached("qe_json")
    if cached is not None:
        return cached
    data = _read_json_by_env(f"{S3_PREFIX}/quarter_end_effect.json", QE_JSON_PATH)
    _set_cache("qe_json", data)
    return data


def _enrich_trades(trades: list[dict], prices: pd.DataFrame) -> list[dict]:
    """tradesにentry_price, exit_price, pnl_100を追加"""
    price_map = {}
    for _, row in prices.iterrows():
        d = row["date"].strftime("%Y-%m-%d")
        price_map[d] = row["Close"]

    enriched = []
    for t in trades:
        entry_p = price_map.get(t["entry_date"])
        exit_p = price_map.get(t["exit_date"])
        pnl_100 = round((exit_p - entry_p) * 100, 0) if entry_p is not None and exit_p is not None else None
        enriched.append({
            **t,
            "entry_price": round(entry_p, 1) if entry_p is not None else None,
            "exit_price": round(exit_p, 1) if exit_p is not None else None,
            "pnl_100": int(pnl_100) if pnl_100 is not None else None,
        })
    return enriched


def _calc_year_summary(trades: list[dict]) -> list[dict]:
    """年ごとのサマリーを計算"""
    from collections import defaultdict
    by_year: dict[int, list[dict]] = defaultdict(list)
    for t in trades:
        by_year[t["year"]].append(t)

    summaries = []
    for year in sorted(by_year.keys()):
        yr_trades = sorted(by_year[year], key=lambda t: t["entry_date"])
        n = len(yr_trades)
        rets = [t["ret_pct"] for t in yr_trades]
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]

        pnl_100_list = [t["pnl_100"] for t in yr_trades if t.get("pnl_100") is not None]
        total_pnl_100 = sum(pnl_100_list) if pnl_100_list else None
        total_ret = sum(rets)
        pf = round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) != 0 else None

        cum = 0.0
        peak = 0.0
        max_dd = 0.0
        for r in rets:
            cum += r
            peak = max(peak, cum)
            dd = cum - peak
            max_dd = min(max_dd, dd)

        summaries.append({
            "year": year,
            "n": n,
            "wins": len(wins),
            "wr": round(len(wins) / n * 100, 1) if n else 0,
            "total_ret": round(total_ret, 3),
            "pnl_100": total_pnl_100,
            "pf": pf,
            "max_dd": round(max_dd, 3),
        })
    return summaries


def _build_flags(row) -> list[str]:
    """カレンダー行からイベントフラグ文字列リストを生成"""
    flags = []
    if row.get("sq4_entry"):
        flags.append("SQ-4 買い")
    if row.get("sq3_exit"):
        flags.append("SQ-4 決済")
    if row.get("sq_day"):
        flags.append("SQ日")

    qe_remain = int(row["qe_remain"]) if pd.notna(row.get("qe_remain")) else None
    if qe_remain is not None:
        q = f"{row['date'].month // 3}Q"
        if row.get("qe_1306_buy") and qe_remain == 4:
            flags.append(f"{q}-4 買い")
        if row.get("qe_1306_sell") and qe_remain == 3:
            flags.append(f"{q}-4 決済")
        if row.get("qe_1306_buy") and qe_remain == 3:
            flags.append(f"{q}-3 買い")
        if row.get("qe_1306_sell") and qe_remain == 2:
            flags.append(f"{q}-3 決済")
        if qe_remain == 1:
            flags.append(f"{q}末")

    return flags


@router.get("/api/dev/calendar")
async def get_calendar_data():
    cal = _load_calendar()
    prices = _load_1306_prices()
    qe_data = _load_qe_json()

    today_str = date.today().isoformat()
    today_data: dict = {"flags": []}
    upcoming = []
    if not cal.empty:
        today_row = cal[cal["date"] == today_str]
        if not today_row.empty:
            today_data = {"flags": _build_flags(today_row.iloc[0])}

        future = cal[cal["date"] > today_str].head(60)
        for _, row in future.iterrows():
            flags = _build_flags(row)
            if flags:
                upcoming.append({
                    "date": row["date"].strftime("%Y-%m-%d"),
                    "flags": flags,
                })

    # --- 1306 latest price ---
    etf_latest = {}
    if not prices.empty:
        latest = prices.iloc[-1]
        prev = prices.iloc[-2] if len(prices) > 1 else None
        etf_latest = {
            "date": latest["date"].strftime("%Y-%m-%d"),
            "close": round(float(latest["Close"]), 1),
            "prev_close": round(float(prev["Close"]), 1) if prev is not None else None,
            "change": round(float(latest["Close"] - prev["Close"]), 1) if prev is not None else None,
            "change_pct": round(float((latest["Close"] / prev["Close"] - 1) * 100), 2) if prev is not None else None,
        }

    # --- 1306 trade performance ---
    trades_raw = qe_data.get("trades", [])
    trades = _enrich_trades(trades_raw, prices)
    year_summary = _calc_year_summary(trades)
    default_stats = {"total": 0, "wins": 0, "losses": 0, "wr": 0, "avg": 0, "median": 0, "max": 0, "min": 0, "pf": 0, "total_ret": 0}
    stats = {**default_stats, **qe_data.get("stats", {})}

    total_pnl_100 = sum(t["pnl_100"] for t in trades if t.get("pnl_100") is not None)

    return {
        "today": today_data,
        "upcoming": upcoming,
        "etf_latest": etf_latest,
        "etf1306": {
            "stats": {
                **stats,
                "pnl_100": total_pnl_100,
            },
            "year_summary": year_summary,
            "trades": trades,
        },
    }


@router.post("/api/dev/calendar/refresh")
async def refresh_cache():
    _cache.clear()
    refreshed = []
    for f in ["calendar.parquet", "etf_1306_prices.parquet"]:
        local = PARQUET_DIR / f
        if _s3_download(f, local):
            refreshed.append(f)
    return {"status": "success", "refreshed_files": refreshed}
