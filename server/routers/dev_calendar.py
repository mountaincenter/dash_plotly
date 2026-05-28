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
SQ4_JSON_PATH = ROOT / "data" / "analysis" / "sq4_trades.json"
SQ_PLUS1_JSON_PATH = ROOT / "data" / "analysis" / "sq_plus1_trades.json"
WEEKDAY_EDGE_JSON_PATH = ROOT / "data" / "analysis" / "weekday_edge_trades.json"

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
    data = _read_json_by_env("analysis/quarter_end_effect.json", QE_JSON_PATH)
    _set_cache("qe_json", data)
    return data


def _load_sq4_json() -> dict:
    cached = _cached("sq4_json")
    if cached is not None:
        return cached
    data = _read_json_by_env("analysis/sq4_trades.json", SQ4_JSON_PATH)
    _set_cache("sq4_json", data)
    return data


def _load_sq_plus1_json() -> dict:
    cached = _cached("sq_plus1_json")
    if cached is not None:
        return cached
    data = _read_json_by_env("analysis/sq_plus1_trades.json", SQ_PLUS1_JSON_PATH)
    _set_cache("sq_plus1_json", data)
    return data


def _load_weekday_edge_json() -> dict:
    cached = _cached("weekday_edge_json")
    if cached is not None:
        return cached
    data = _read_json_by_env("analysis/weekday_edge_trades.json", WEEKDAY_EDGE_JSON_PATH)
    _set_cache("weekday_edge_json", data)
    return data


def _load_cme_latest() -> dict:
    """CME NKD=F 直近終値を取得"""
    cached = _cached("cme_latest")
    if cached is not None:
        return cached
    try:
        df = _read_parquet_by_env("futures_prices_max_1d.parquet")
        cme = df[df["ticker"] == "NKD=F"][["date", "Close"]].copy()
        cme["date"] = pd.to_datetime(cme["date"])
        cme = cme.dropna(subset=["Close"]).sort_values("date").reset_index(drop=True)
        if len(cme) < 2:
            return {}
        latest = cme.iloc[-1]
        prev = cme.iloc[-2]
        result = {
            "date": latest["date"].strftime("%Y-%m-%d"),
            "close": int(round(latest["Close"])),
            "prev_close": int(round(prev["Close"])),
            "change": int(round(latest["Close"] - prev["Close"])),
            "change_pct": round((latest["Close"] / prev["Close"] - 1) * 100, 2),
        }
        _set_cache("cme_latest", result)
        return result
    except Exception:
        return {}


def _next_trading_date() -> str | None:
    """weekday_edge_trades.jsonのpipeline確定値を優先、なければcalendar.parquetでフォールバック"""
    try:
        we = _load_weekday_edge_json()
        ntd = we.get("next_trading_date")
        if ntd:
            return ntd
    except Exception:
        pass
    try:
        cal = _load_calendar()
        today = pd.Timestamp(date.today())
        future = cal[cal["date"] > today].sort_values("date")
        if future.empty:
            return None
        return future.iloc[0]["date"].strftime("%Y-%m-%d")
    except Exception:
        return None


def _load_latest_prices() -> dict[str, dict]:
    """prices_topix500_oc から銘柄ごとの最新終値・前日比を取得"""
    cached = _cached("latest_prices")
    if cached is not None:
        return cached
    try:
        df = _read_parquet_by_env("prices_topix500_oc.parquet")
        df.columns = ["date", "code", "adj_open", "adj_close"]
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["code", "date"])
        result: dict[str, dict] = {}
        for code, grp in df.groupby("code"):
            grp = grp.dropna(subset=["adj_close"]).tail(2)
            if len(grp) < 1:
                continue
            latest = grp.iloc[-1]
            prev_close = grp.iloc[-2]["adj_close"] if len(grp) >= 2 else None
            change = round(float(latest["adj_close"] - prev_close), 1) if prev_close else None
            change_pct = round(float((latest["adj_close"] / prev_close - 1) * 100), 2) if prev_close else None
            result[str(code)] = {
                "prev_close": round(float(latest["adj_close"]), 1),
                "prev_day_ret": change_pct,
                "prev_day_change": change,
            }
        _set_cache("latest_prices", result)
        return result
    except Exception:
        return {}


def _load_sp500_latest() -> dict:
    """S&P500 直近終値を取得"""
    cached = _cached("sp500_latest")
    if cached is not None:
        return cached
    try:
        df = _read_parquet_by_env("index_prices_max_1d.parquet")
        sp = df[df["ticker"] == "^GSPC"][["date", "Close"]].copy()
        sp["date"] = pd.to_datetime(sp["date"])
        sp = sp.dropna(subset=["Close"]).sort_values("date").reset_index(drop=True)
        if len(sp) < 2:
            return {}
        latest = sp.iloc[-1]
        prev = sp.iloc[-2]
        result = {
            "date": latest["date"].strftime("%Y-%m-%d"),
            "close": round(float(latest["Close"]), 2),
            "prev_close": round(float(prev["Close"]), 2),
            "change": round(float(latest["Close"] - prev["Close"]), 2),
            "change_pct": round((latest["Close"] / prev["Close"] - 1) * 100, 2),
        }
        _set_cache("sp500_latest", result)
        return result
    except Exception:
        return {}


def _calc_1306_max_dd(trades: list[dict]) -> dict:
    """1306トレードからMaxDD計算"""
    if not trades:
        return {"amount": 0, "pct": 0.0}
    cum = 0
    peak = 0
    max_dd_amount = 0
    cum_ret = 0.0
    peak_ret = 0.0
    max_dd_pct = 0.0
    for t in trades:
        pnl = t.get("pnl_1000") or 0
        cum += pnl
        peak = max(peak, cum)
        max_dd_amount = min(max_dd_amount, cum - peak)

        cum_ret += t.get("ret_pct", 0)
        peak_ret = max(peak_ret, cum_ret)
        max_dd_pct = min(max_dd_pct, cum_ret - peak_ret)

    return {"amount": int(max_dd_amount), "pct": round(max_dd_pct, 3)}


def _enrich_trades(trades: list[dict], prices: pd.DataFrame) -> list[dict]:
    """tradesにentry_price, exit_price, pnl_1000(1000株)を追加"""
    price_map = {}
    for _, row in prices.iterrows():
        d = row["date"].strftime("%Y-%m-%d")
        price_map[d] = row["Close"]

    enriched = []
    for t in trades:
        entry_p = price_map.get(t["entry_date"])
        exit_p = price_map.get(t["exit_date"])
        pnl_1000 = round((exit_p - entry_p) * 1000, 0) if entry_p is not None and exit_p is not None else None
        enriched.append({
            **t,
            "entry_price": round(entry_p, 1) if entry_p is not None else None,
            "exit_price": round(exit_p, 1) if exit_p is not None else None,
            "pnl_1000": int(pnl_1000) if pnl_1000 is not None else None,
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

        pnl_1000_list = [t["pnl_1000"] for t in yr_trades if t.get("pnl_1000") is not None]
        total_pnl_1000 = sum(pnl_1000_list) if pnl_1000_list else None
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
            "pnl_1000": total_pnl_1000,
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
    if row.get("sq_plus1_short"):
        flags.append("SQ+1 売り")

    if row.get("b4_etf_signal"):
        count = int(row["b4_etf_signal_count"]) if pd.notna(row.get("b4_etf_signal_count")) else 0
        if row.get("b4_etf_signal_panic"):
            flags.append(f"B4 ETF 発火 強 {count}")
        elif row.get("b4_etf_signal_strong"):
            flags.append(f"B4 ETF 発火 {count}")
        else:
            flags.append(f"B4 ETF 発火 {count}")
    if row.get("b4_etf_buy_no_overlap"):
        flags.append("B4 ETF 買い")
    if row.get("b4_etf_sell_10d_no_overlap"):
        flags.append("B4 ETF 決済")

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
    errors: list[str] = []

    try:
        cal = _load_calendar()
    except Exception as e:
        cal = pd.DataFrame()
        errors.append(f"calendar: {e}")

    try:
        prices = _load_1306_prices()
    except Exception as e:
        prices = pd.DataFrame()
        errors.append(f"etf_1306_prices: {e}")

    try:
        qe_data = _load_qe_json()
    except Exception as e:
        qe_data = {}
        errors.append(f"quarter_end_effect: {e}")

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
    trades = _enrich_trades(trades_raw, prices) if not prices.empty else trades_raw
    year_summary = _calc_year_summary(trades) if trades else []
    default_stats = {"total": 0, "wins": 0, "losses": 0, "wr": 0, "avg": 0, "median": 0, "max": 0, "min": 0, "pf": 0, "total_ret": 0}
    stats = {**default_stats, **qe_data.get("stats", {})}

    total_pnl_1000 = sum(t.get("pnl_1000", 0) for t in trades)

    # --- 1306 MaxDD ---
    etf_max_dd = _calc_1306_max_dd(trades)

    # --- CME latest ---
    cme_latest = _load_cme_latest()

    # --- S&P500 latest ---
    sp500_latest = _load_sp500_latest()

    # --- SQ-4 trades ---
    try:
        sq4_data = _load_sq4_json()
    except Exception as e:
        sq4_data = {}
        errors.append(f"sq4_trades: {e}")

    # --- SQ+1 trades ---
    try:
        sq_plus1_data = _load_sq_plus1_json()
    except Exception as e:
        sq_plus1_data = {}
        errors.append(f"sq_plus1_trades: {e}")

    # --- Weekday Edge trades ---
    try:
        weekday_edge_data = _load_weekday_edge_json()
    except Exception as e:
        weekday_edge_data = {}
        errors.append(f"weekday_edge: {e}")

    # weekday_edge next_entries に最新終値を付与
    try:
        latest_prices = _load_latest_prices()
    except Exception as e:
        latest_prices = {}
        errors.append(f"latest_prices: {e}")
    we_entries = weekday_edge_data.get("next_entries", [])
    for entry in we_entries:
        code = entry.get("code", "")
        price_info = latest_prices.get(code, {})
        entry["prev_close"] = price_info.get("prev_close")
        entry["prev_day_ret"] = price_info.get("prev_day_ret")

    if errors:
        print(f"[WARN] /api/dev/calendar partial errors: {errors}")

    return {
        "today": today_data,
        "upcoming": upcoming,
        "etf_latest": etf_latest,
        "cme_latest": cme_latest,
        "sp500_latest": sp500_latest,
        "next_trading_date": _next_trading_date(),
        "etf1306": {
            "stats": {
                **stats,
                "pnl_1000": total_pnl_1000,
            },
            "max_dd": etf_max_dd,
            "year_summary": year_summary,
            "trades": trades,
        },
        "sq4": {
            "stats": sq4_data.get("stats", {}),
            "stats_cme_down": sq4_data.get("stats_cme_down", {}),
            "stats_cme_up": sq4_data.get("stats_cme_up", {}),
            "max_dd": sq4_data.get("max_dd", {}),
            "max_dd_cme_down": sq4_data.get("max_dd_cme_down", {}),
            "next_sq4": sq4_data.get("next_sq4"),
            "candidates": sq4_data.get("candidates", {}),
            "monthly": sq4_data.get("monthly", []),
        },
        "sq_plus1": {
            "stats": sq_plus1_data.get("stats", {}),
            "stats_cme_down": sq_plus1_data.get("stats_cme_down", {}),
            "stats_cme_up": sq_plus1_data.get("stats_cme_up", {}),
            "max_dd": sq_plus1_data.get("max_dd", {}),
            "max_dd_cme_down": sq_plus1_data.get("max_dd_cme_down", {}),
            "next_sq_plus1": sq_plus1_data.get("next_sq_plus1"),
            "monthly": sq_plus1_data.get("monthly", []),
        },
        "weekday_edge": {
            "params": weekday_edge_data.get("params", {}),
            "next_market_date": weekday_edge_data.get("next_market_date"),
            "next_trading_date": weekday_edge_data.get("next_trading_date"),
            "is_weekday_entry_day": weekday_edge_data.get("is_weekday_entry_day"),
            "weekday_no_entry_reason": weekday_edge_data.get("weekday_no_entry_reason"),
            "next_weekday_signal_date": weekday_edge_data.get("next_weekday_signal_date"),
            "stats_filtered": weekday_edge_data.get("stats_filtered", {}),
            "stats_all": weekday_edge_data.get("stats_all", {}),
            "max_dd_filtered": weekday_edge_data.get("max_dd_filtered", {}),
            "yearly": weekday_edge_data.get("yearly", []),
            "stock_stats": weekday_edge_data.get("stock_stats", []),
            "next_entries": weekday_edge_data.get("next_entries", []),
            "weekly": weekday_edge_data.get("weekly", []),
        },
        "_errors": errors if errors else None,
    }


@router.post("/api/dev/calendar/refresh")
async def refresh_cache():
    _cache.clear()
    refreshed = []
    for f in ["calendar.parquet", "etf_1306_prices.parquet", "index_prices_max_1d.parquet"]:
        local = PARQUET_DIR / f
        if _s3_download(f, local):
            refreshed.append(f)
    # sq4_trades.json is in analysis/ not parquet/
    sq4_local = SQ4_JSON_PATH
    try:
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        sq4_local.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(S3_BUCKET, "analysis/sq4_trades.json", str(sq4_local))
        refreshed.append("sq4_trades.json")
    except Exception:
        pass
    for json_name, local_path in [
        ("weekday_edge_trades.json", WEEKDAY_EDGE_JSON_PATH),
        ("sq_plus1_trades.json", SQ_PLUS1_JSON_PATH),
    ]:
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(S3_BUCKET, f"analysis/{json_name}", str(local_path))
            refreshed.append(json_name)
        except Exception:
            pass
    return {"status": "success", "refreshed_files": refreshed}
