"""Market-flow prototype API from trading-value Top150 history."""
from __future__ import annotations

import math
import os
import json
import threading
import time
from datetime import datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, Query

ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = ROOT / "data" / "parquet"
HISTORY_PATH = PARQUET_DIR / "trading_value_top_history.parquet"
INDEX_PRICES_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
TOPIX_PRICES_PATH = PARQUET_DIR / "topix_prices_max_1d.parquet"
MARKET_BASKET_TURNOVER_PATH = PARQUET_DIR / "market_basket_turnover.parquet"
MARKET_FLOW_200A_FORWARD_PATH = PARQUET_DIR / "market_flow_200a_forward.parquet"
MARKET_FLOW_200A_STATUS_PATH = PARQUET_DIR / "market_flow_200a_phase_status.json"

S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET")
S3_PREFIX = (os.getenv("PARQUET_PREFIX") or "parquet/").strip("/")
AWS_REGION = os.getenv("AWS_REGION") or "ap-northeast-1"
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
APP_ENV = (
    os.getenv("APP_ENV")
    or os.getenv("ENVIRONMENT")
    or os.getenv("STAGE")
    or "local"
).strip().lower()
USE_S3_DATA = APP_ENV in {"production", "prod"}
DATA_SOURCE_MODE = "s3" if USE_S3_DATA else "local"
PAYLOAD_CACHE_TTL_SECONDS = max(
    1,
    int(os.getenv("MARKET_FLOW_CACHE_TTL_SECONDS") or "60"),
)

router = APIRouter()

_CACHE_LOCK = threading.RLock()
_S3_CONTENT_CACHE: dict[str, dict[str, Any]] = {}
_PAYLOAD_CACHE: dict[tuple[int, int], dict[str, Any]] = {}
_PAYLOAD_SOURCE_PATHS = (
    HISTORY_PATH,
    INDEX_PRICES_PATH,
    TOPIX_PRICES_PATH,
    MARKET_BASKET_TURNOVER_PATH,
    MARKET_FLOW_200A_FORWARD_PATH,
    MARKET_FLOW_200A_STATUS_PATH,
)

SEMICON_CODES = {
    "1802", "1925", "1963", "1979", "2802", "285A", "3436", "4004",
    "4062", "4063", "4186", "5801", "5802", "5803", "5805", "6146",
    "6367", "6368", "6501", "6503", "6504", "6525", "6526", "6645",
    "6723", "6762", "6857", "6920", "6963", "6971", "6976", "6981",
    "7735", "7911", "7912", "8035", "8088", "8801", "8802",
}

THEME_BUCKET_TICKERS = {
    "kioxia": {"285A.T"},
    "semicon_main": {
        "3436.T", "4004.T", "4062.T", "4063.T", "4186.T", "6146.T",
        "6315.T", "6525.T", "6526.T", "6723.T", "6857.T", "6920.T",
        "6963.T", "7735.T", "8035.T", "6055.T",
    },
    "dc_cable_optical": {"5801.T", "5802.T", "5803.T", "5985.T"},
    "ai_power_heavy": {
        "6501.T", "6503.T", "7011.T", "7012.T", "7013.T", "9501.T",
        "6367.T", "9984.T",
    },
    "electronics_parts": {
        "4078.T", "4092.T", "4100.T", "5331.T", "5332.T", "5333.T",
        "5344.T", "5367.T", "6479.T", "6666.T", "6762.T", "6779.T",
        "6787.T", "6962.T", "6971.T", "6976.T", "6981.T",
    },
    "robotics_factory_auto": {
        "6273.T", "6301.T", "6324.T", "6361.T", "6506.T", "6594.T",
        "6861.T", "6954.T", "7245.T",
    },
    "semicon_etf": {"200A.T", "213A.T", "2243.T", "2644.T", "346A.T"},
    "index_bull": {"1306.T", "1321.T", "1458.T", "1570.T", "1579.T"},
    "index_inverse": {"1357.T", "1360.T"},
}

THEME_BUCKET_BY_TICKER = {
    ticker: bucket
    for bucket, tickers in THEME_BUCKET_TICKERS.items()
    for ticker in tickers
}

THEME_BUCKET_LABELS = {
    "kioxia": "キオクシア",
    "semicon_main": "半導体中核 exキオクシア",
    "dc_cable_optical": "電線・光通信",
    "ai_power_heavy": "AI電力・重電",
    "electronics_parts": "MLCC・電子部品",
    "robotics_factory_auto": "ロボット・FA",
    "semicon_etf": "半導体ETF",
    "index_bull": "指数・レバETF",
    "index_inverse": "インバースETF",
    "index_other": "その他ETF",
    "other": "その他",
}

THEME_BUCKET_ORDER = [
    "kioxia",
    "semicon_main",
    "dc_cable_optical",
    "electronics_parts",
    "ai_power_heavy",
    "robotics_factory_auto",
    "semicon_etf",
    "index_bull",
    "index_inverse",
    "index_other",
    "other",
]

SEMICON_CORE_BUCKETS = {"kioxia", "semicon_main"}
PERIPHERAL_BUCKETS = {
    "dc_cable_optical",
    "electronics_parts",
    "ai_power_heavy",
    "robotics_factory_auto",
}
ETF_CONFIRMATION_BUCKETS = {"semicon_etf", "index_bull", "index_inverse", "index_other"}
ETF_TICKERS = {
    ticker
    for bucket, tickers in THEME_BUCKET_TICKERS.items()
    if bucket in ETF_CONFIRMATION_BUCKETS
    for ticker in tickers
}
ETF_DISPLAY_META = {
    "1306.T": {"stock_name": "TOPIX連動ETF", "sectors": "指数ETF"},
    "1321.T": {"stock_name": "日経225連動ETF", "sectors": "指数ETF"},
    "1458.T": {"stock_name": "楽天日経レバETF", "sectors": "指数・レバETF"},
    "1570.T": {"stock_name": "日経平均レバレッジETF", "sectors": "指数・レバETF"},
    "1579.T": {"stock_name": "日経平均ブル2倍ETF", "sectors": "指数・レバETF"},
    "1357.T": {"stock_name": "日経平均ダブルインバースETF", "sectors": "インバースETF"},
    "1360.T": {"stock_name": "日経平均ベア2倍ETF", "sectors": "インバースETF"},
    "200A.T": {"stock_name": "日経半導体株ETF", "sectors": "半導体ETF"},
    "213A.T": {"stock_name": "日経半導体株ETF", "sectors": "半導体ETF"},
    "2243.T": {"stock_name": "半導体関連ETF", "sectors": "半導体ETF"},
    "2644.T": {"stock_name": "半導体関連-日本株ETF", "sectors": "半導体ETF"},
    "346A.T": {"stock_name": "半導体ETF", "sectors": "半導体ETF"},
}


def _s3_key(name: str) -> str:
    return f"{S3_PREFIX}/{name}" if S3_PREFIX else name


def _source_label(path: Path) -> str:
    if USE_S3_DATA:
        bucket = S3_BUCKET or "<missing-bucket>"
        return f"s3://{bucket}/{_s3_key(path.name)}"
    return str(path)


@lru_cache(maxsize=1)
def _s3_client():
    import boto3

    client_kwargs: dict[str, str] = {"region_name": AWS_REGION}
    if AWS_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = AWS_ENDPOINT_URL
    return boto3.client("s3", **client_kwargs)


def _s3_version_token(head: dict[str, Any]) -> str:
    last_modified = head.get("LastModified")
    if hasattr(last_modified, "isoformat"):
        last_modified = last_modified.isoformat()
    return "|".join(
        [
            str(head.get("ETag") or ""),
            str(head.get("VersionId") or ""),
            str(head.get("ContentLength") or ""),
            str(last_modified or ""),
        ]
    )


def _read_s3_cached(path: Path, *, kind: str) -> Any:
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET or DATA_BUCKET is required in production")
    key = _s3_key(path.name)
    client = _s3_client()
    head = client.head_object(Bucket=S3_BUCKET, Key=key)
    token = _s3_version_token(head)
    cache_key = f"{kind}:{key}"
    with _CACHE_LOCK:
        cached = _S3_CONTENT_CACHE.get(cache_key)
        if cached and cached.get("token") == token:
            return cached.get("value")

    obj = client.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"].read()
    if kind == "parquet":
        value = pd.read_parquet(BytesIO(body))
    elif kind == "json":
        value = json.loads(body.decode("utf-8"))
    else:
        raise ValueError(f"Unsupported S3 cache kind: {kind}")

    with _CACHE_LOCK:
        _S3_CONTENT_CACHE[cache_key] = {"token": token, "value": value}
    return value


def _source_token(path: Path) -> str:
    if not USE_S3_DATA:
        if not path.exists():
            return "missing"
        stat = path.stat()
        return f"local:{stat.st_size}:{stat.st_mtime_ns}"

    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET or DATA_BUCKET is required in production")
    key = _s3_key(path.name)
    try:
        head = _s3_client().head_object(Bucket=S3_BUCKET, Key=key)
    except Exception as exc:
        response = getattr(exc, "response", {})
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        code = str(error.get("Code") or "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return "missing"
        raise
    return f"s3:{_s3_version_token(head)}"


def _payload_source_fingerprint() -> tuple[str, ...] | None:
    try:
        return tuple(_source_token(path) for path in _PAYLOAD_SOURCE_PATHS)
    except Exception as exc:
        print(f"[WARN] failed to refresh market-flow source fingerprint: {exc}")
        return None


def _read_history() -> pd.DataFrame | None:
    return _read_optional_parquet(HISTORY_PATH)


def _read_optional_json(path: Path) -> dict[str, Any] | None:
    if not USE_S3_DATA:
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else None
        except Exception as exc:
            print(f"[WARN] failed to read local {path.name}: {exc}")
            return None

    try:
        payload = _read_s3_cached(path, kind="json")
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        print(f"[WARN] failed to read S3 {path.name}: {exc}")
        return None


def _read_optional_parquet(path: Path) -> pd.DataFrame | None:
    if not USE_S3_DATA:
        if not path.exists():
            return None
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            print(f"[WARN] failed to read local {path.name}: {exc}")
            return None

    try:
        value = _read_s3_cached(path, kind="parquet")
        return value if isinstance(value, pd.DataFrame) else None
    except Exception as exc:
        print(f"[WARN] failed to read S3 {path.name}: {exc}")
        return None


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _int_or_none(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    if isinstance(value, tuple):
        return [_clean(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item"):
        return _clean(value.item())
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value


def _records(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> list[dict[str, Any]]:
    if df.empty:
        return []
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = None
    if limit is not None:
        out = out.head(limit)
    return [_clean(row) for row in out[columns].to_dict(orient="records")]


def _execution_program() -> dict[str, Any]:
    status = _read_optional_json(MARKET_FLOW_200A_STATUS_PATH)
    if status is None:
        return {
            "available": False,
            "reason": "market_flow_200a_phase_status.json not found",
        }

    forward = _read_optional_parquet(MARKET_FLOW_200A_FORWARD_PATH)
    if forward is None or forward.empty:
        return {
            "available": True,
            "phase_status": _clean(status),
            "latest_route_date": None,
            "latest_routes": [],
            "recent_primary": [],
        }

    out = forward.copy()
    if "trade_date" not in out.columns:
        return {
            "available": True,
            "phase_status": _clean(status),
            "latest_route_date": None,
            "latest_routes": [],
            "recent_primary": [],
            "warning": "forward parquet has no trade_date column",
        }
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["trade_date"]).copy()
    sort_columns = [column for column in ["trade_date", "priority"] if column in out.columns]
    if sort_columns:
        out = out.sort_values(sort_columns, kind="mergesort")

    latest_date = str(out["trade_date"].max()) if not out.empty else None
    latest_routes = out[out["trade_date"].eq(latest_date)].copy() if latest_date else pd.DataFrame()
    primary = out[out.get("primary_selected", False).fillna(False).astype(bool)].copy() \
        if "primary_selected" in out.columns else pd.DataFrame()

    columns = [
        "trade_date", "rule_id", "priority", "side", "signal_time", "exit_time",
        "signal_available", "triggered", "primary_selected", "execution_status",
        "entry_time", "entry_price", "exit_price", "net_return_bps",
        "shadow_pnl_yen_30", "etf_200a_ret_from_open_pct",
        "etf_200a_above_vwap", "etf_2644_ret_from_open_pct",
        "etf_2644_above_vwap", "semicon_weighted_open_return_pct",
        "semicon_above_vwap_rate_pct", "semicon_active_count",
    ]
    return {
        "available": True,
        "phase_status": _clean(status),
        "latest_route_date": latest_date,
        "latest_routes": _records(latest_routes, columns),
        "recent_primary": _records(primary.tail(10), columns),
    }


def _missing_text(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    return series.isna() | text.isin({"", "None", "nan", "NaN", "UNKNOWN"})


def _apply_known_etf_metadata(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ["stock_name", "market", "sectors", "series", "topixnewindexseries"]:
        if col not in out.columns:
            out[col] = None
    tickers = out["ticker"].astype(str)
    codes = out["code"].astype(str)
    for ticker, meta in ETF_DISPLAY_META.items():
        code = ticker.removesuffix(".T")
        mask = tickers.eq(ticker) | codes.eq(code)
        if not bool(mask.any()):
            continue
        stock_name_text = out["stock_name"].astype(str).str.strip()
        missing_name = _missing_text(out["stock_name"]) | stock_name_text.eq(code) | stock_name_text.eq(ticker)
        out.loc[mask & missing_name, "stock_name"] = meta["stock_name"]
        out.loc[mask & _missing_text(out["market"]), "market"] = "ETF"
        out.loc[mask & _missing_text(out["sectors"]), "sectors"] = meta["sectors"]
        out.loc[mask & _missing_text(out["series"]), "series"] = "ETF"
        out.loc[mask & _missing_text(out["topixnewindexseries"]), "topixnewindexseries"] = "ETF"
    return out


def _rank_band(rank: Any) -> str:
    value = _int_or_none(rank)
    if value is None:
        return "unknown"
    if value <= 30:
        return "top1_30"
    if value <= 100:
        return "top31_100"
    return "top101_150"


def _is_etf(row: pd.Series) -> bool:
    ticker = str(row.get("ticker") or "").strip()
    code = _code_from_ticker(ticker or row.get("code"))
    if ticker in ETF_TICKERS or f"{code}.T" in ETF_TICKERS:
        return True
    text = " ".join(
        str(row.get(col) or "")
        for col in ["market", "sectors", "stock_name", "topixnewindexseries"]
    ).upper()
    return (
        "ETF" in text
        or "ＥＴＦ" in text
        or "投信" in text
        or "上場投資信託" in text
        or "上場投信" in text
    )


def _theme_bucket(row: pd.Series) -> str:
    ticker = str(row.get("ticker") or "").strip()
    bucket = THEME_BUCKET_BY_TICKER.get(ticker)
    if bucket:
        return bucket
    if bool(row.get("is_etf")):
        return "index_other"
    return "other"


def _code_from_ticker(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".T"):
        text = text[:-2]
    return text


def _prepare(df: pd.DataFrame, top_n: int, days: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    required = {"date", "ticker", "rank", "trading_value"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"trading_value_top_history.parquet missing columns: {missing}")

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out[out["date"].notna()].copy()
    out["rank"] = pd.to_numeric(out["rank"], errors="coerce")
    out = out[out["rank"].between(1, top_n)].copy()
    out["trading_value"] = pd.to_numeric(out["trading_value"], errors="coerce")
    out["trading_value_billion"] = pd.to_numeric(
        out.get("trading_value_billion", out["trading_value"] / 1_000_000_000.0),
        errors="coerce",
    )
    out["open_to_close_pct"] = pd.to_numeric(out.get("open_to_close_pct"), errors="coerce")
    out["price_diff"] = pd.to_numeric(out.get("price_diff"), errors="coerce")
    out["code"] = out.get("code", out["ticker"]).map(_code_from_ticker)
    if "stock_name" not in out.columns:
        out["stock_name"] = out["code"]
    out["stock_name"] = out["stock_name"].fillna(out["code"])
    if "sectors" not in out.columns:
        out["sectors"] = "UNKNOWN"
    out["sectors"] = out["sectors"].fillna("UNKNOWN")
    if "market" not in out.columns:
        out["market"] = ""
    out["market"] = out["market"].fillna("")
    out["rank_band"] = out["rank"].map(_rank_band)
    out["is_semiconductor"] = out["code"].astype(str).isin(SEMICON_CODES)
    out["is_etf"] = out.apply(_is_etf, axis=1)
    out["theme_bucket"] = out.apply(_theme_bucket, axis=1)
    out["theme_label"] = out["theme_bucket"].map(THEME_BUCKET_LABELS).fillna(out["theme_bucket"])
    out = _apply_known_etf_metadata(out)
    out["is_semicon_main"] = out["theme_bucket"].eq("semicon_main")
    out["is_theme_peripheral"] = out["theme_bucket"].isin(PERIPHERAL_BUCKETS)
    out = out.sort_values(["date", "rank"], kind="mergesort")

    dates = sorted(out["date"].dropna().unique().tolist())
    selected_dates = dates[-max(days, 2):]
    recent = out[out["date"].isin(selected_dates)].copy()
    return out, recent, selected_dates


def _weighted_avg(values: pd.Series, weights: pd.Series) -> float | None:
    valid = values.notna() & weights.notna() & weights.gt(0)
    if not bool(valid.any()):
        return None
    return float((values[valid] * weights[valid]).sum() / weights[valid].sum())


def _consecutive_date_count(date_values: set[str], dates: list[str]) -> int:
    count = 0
    for date in reversed(dates):
        if date not in date_values:
            break
        count += 1
    return count


def _entity_persistence(source: pd.DataFrame, group_col: str, dates: list[str]) -> dict[str, dict[str, int]]:
    if source.empty or group_col not in source.columns:
        return {}
    daily_rows = []
    for (date, key), group in source.groupby(["date", group_col], dropna=False):
        daily_rows.append({
            "date": str(date),
            "key": str(key or "UNKNOWN"),
            "avg_open_to_close_pct": _weighted_avg(group["open_to_close_pct"], group["trading_value_billion"]),
        })
    if not daily_rows:
        return {}

    daily = pd.DataFrame(daily_rows)
    result: dict[str, dict[str, int]] = {}
    for key, group in daily.groupby("key", dropna=False):
        active_dates = set(group["date"].astype(str).tolist())
        positive_dates = set(group[group["avg_open_to_close_pct"].gt(0)]["date"].astype(str).tolist())
        active_streak = _consecutive_date_count(active_dates, dates)
        positive_streak = _consecutive_date_count(positive_dates, dates)
        result[str(key or "UNKNOWN")] = {
            "active_days": int(len(active_dates)),
            "active_streak_days": int(active_streak),
            "positive_days": int(len(positive_dates)),
            "positive_streak_days": int(positive_streak),
            "persistence_score": int(active_streak * 2 + positive_streak + min(len(active_dates), 5)),
        }
    return result


def _with_rank_context(latest: pd.DataFrame, previous: pd.DataFrame, recent: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    out = latest.copy()
    prev_rank = previous.set_index("ticker")["rank"] if not previous.empty else pd.Series(dtype=float)
    out["prev_rank"] = out["ticker"].map(prev_rank)
    out["rank_change"] = out["prev_rank"] - out["rank"]
    out["is_new_top150"] = out["prev_rank"].isna()
    days_count = recent.groupby("ticker")["date"].nunique()
    out["days_in_top150"] = out["ticker"].map(days_count).fillna(0).astype(int)
    active_dates = recent.groupby("ticker")["date"].agg(lambda values: set(values.astype(str)))
    positive_dates = (
        recent[recent["open_to_close_pct"].gt(0)]
        .groupby("ticker")["date"]
        .agg(lambda values: set(values.astype(str)))
    )
    out["consecutive_days_in_top150"] = [
        _consecutive_date_count(active_dates.get(ticker, set()), dates)
        for ticker in out["ticker"].astype(str)
    ]
    out["positive_streak_days"] = [
        _consecutive_date_count(positive_dates.get(ticker, set()), dates)
        for ticker in out["ticker"].astype(str)
    ]
    out["persistence_score"] = (
        out["consecutive_days_in_top150"].fillna(0) * 2
        + out["positive_streak_days"].fillna(0)
        + out["days_in_top150"].fillna(0).clip(upper=5)
    ).astype(int)
    return out


def _sector_daily(latest: pd.DataFrame, recent: pd.DataFrame, dates: list[str]) -> list[dict[str, Any]]:
    source = latest[~latest["is_etf"]].copy()
    persistence = _entity_persistence(recent[~recent["is_etf"]].copy(), "sectors", dates)
    total = float(source["trading_value_billion"].sum())
    rows = []
    for sector, group in source.groupby("sectors", dropna=False):
        sector_text = str(sector or "UNKNOWN")
        group = group.sort_values("rank", kind="mergesort")
        turnover = float(group["trading_value_billion"].sum())
        non_semicon = group[~group["is_semiconductor"]]
        persist = persistence.get(sector_text, {})
        rows.append({
            "sector": sector_text,
            "count": int(len(group)),
            "turnover_bil": turnover,
            "turnover_share_pct": turnover / total * 100.0 if total else None,
            "non_semiconductor_turnover_bil": float(non_semicon["trading_value_billion"].sum()),
            "avg_open_to_close_pct": _weighted_avg(group["open_to_close_pct"], group["trading_value_billion"]),
            "up_count": int(group["open_to_close_pct"].gt(0).sum()),
            "top_code": str(group.iloc[0].get("code") or ""),
            "top_ticker": str(group.iloc[0].get("ticker") or ""),
            "top_name": str(group.iloc[0].get("stock_name") or ""),
            "top_rank": _int_or_none(group.iloc[0].get("rank")),
            "top1_30_count": int(group["rank_band"].eq("top1_30").sum()),
            "top31_100_count": int(group["rank_band"].eq("top31_100").sum()),
            "top101_150_count": int(group["rank_band"].eq("top101_150").sum()),
            "semiconductor_count": int(group["is_semiconductor"].sum()),
            "active_days": persist.get("active_days", 0),
            "active_streak_days": persist.get("active_streak_days", 0),
            "positive_days": persist.get("positive_days", 0),
            "positive_streak_days": persist.get("positive_streak_days", 0),
            "persistence_score": persist.get("persistence_score", 0),
        })
    return sorted(rows, key=lambda row: row["turnover_bil"], reverse=True)


def _sector_weekly(recent: pd.DataFrame, latest_date: str) -> list[dict[str, Any]]:
    source = recent[~recent["is_etf"]].copy()
    if source.empty:
        return []
    daily = (
        source.groupby(["date", "sectors"], as_index=False)
        .agg(
            turnover_bil=("trading_value_billion", "sum"),
            count=("ticker", "nunique"),
            avg_open_to_close_pct=("open_to_close_pct", "mean"),
        )
    )
    rows = []
    for sector, group in daily.groupby("sectors", dropna=False):
        latest = group[group["date"].eq(latest_date)]
        latest_turnover = float(latest["turnover_bil"].iloc[0]) if not latest.empty else 0.0
        avg_turnover = float(group["turnover_bil"].mean()) if not group.empty else 0.0
        rows.append({
            "sector": str(sector or "UNKNOWN"),
            "active_days": int(group["date"].nunique()),
            "avg_daily_turnover_bil": avg_turnover,
            "latest_turnover_bil": latest_turnover,
            "latest_vs_avg": latest_turnover / avg_turnover if avg_turnover else None,
            "latest_count": int(latest["count"].iloc[0]) if not latest.empty else 0,
            "avg_count": float(group["count"].mean()) if not group.empty else None,
        })
    return sorted(rows, key=lambda row: (row["latest_vs_avg"] or 0, row["latest_turnover_bil"]), reverse=True)


def _sector_radar(
    latest: pd.DataFrame,
    history: pd.DataFrame,
    latest_date: str,
) -> dict[str, Any]:
    latest_non_etf = latest[~latest["is_etf"]].copy()
    latest_source = latest_non_etf[
        latest_non_etf["sectors"].astype(str).ne("UNKNOWN")
        & latest_non_etf["sectors"].astype(str).str.strip().ne("")
    ].copy()
    history_source = history[
        ~history["is_etf"]
        & history["date"].le(latest_date)
        & history["sectors"].astype(str).ne("UNKNOWN")
        & history["sectors"].astype(str).str.strip().ne("")
    ].copy()
    if latest_source.empty or history_source.empty:
        return {
            "available": False,
            "reason": "sector data is not available",
            "rows": [],
        }

    history_dates = sorted(history_source["date"].dropna().unique().tolist())
    prior_dates = [date for date in history_dates if date < latest_date]
    prior_5_dates = prior_dates[-5:]
    prior_20_dates = prior_dates[-20:]
    persistence_dates = history_dates[-20:]
    persistence_source = history_source[history_source["date"].isin(persistence_dates)]
    persistence = _entity_persistence(persistence_source, "sectors", persistence_dates)
    daily_turnover = (
        history_source.groupby(["date", "sectors"], as_index=False)["trading_value_billion"]
        .sum()
    )
    latest_total = float(latest_source["trading_value_billion"].sum())
    non_etf_total = float(latest_non_etf["trading_value_billion"].sum())
    unclassified = latest_non_etf[
        latest_non_etf["sectors"].astype(str).eq("UNKNOWN")
        | latest_non_etf["sectors"].astype(str).str.strip().eq("")
    ]

    def _window_average(sector: str, dates: list[str]) -> float | None:
        if not dates:
            return None
        values = daily_turnover[
            daily_turnover["date"].isin(dates)
            & daily_turnover["sectors"].astype(str).eq(sector)
        ]["trading_value_billion"]
        return float(values.sum()) / len(dates)

    rows: list[dict[str, Any]] = []
    for sector, group in latest_source.groupby("sectors", dropna=False):
        sector_text = str(sector or "UNKNOWN")
        group = group.sort_values("rank", kind="mergesort")
        turnover = float(group["trading_value_billion"].sum())
        avg_5d = _window_average(sector_text, prior_5_dates)
        avg_20d = _window_average(sector_text, prior_20_dates)
        persist = persistence.get(sector_text, {})
        top_stocks = [
            {
                "ticker": str(row.get("ticker") or ""),
                "code": str(row.get("code") or ""),
                "name": str(row.get("stock_name") or ""),
                "rank": _int_or_none(row.get("rank")),
                "turnover_bil": _number(row.get("trading_value_billion")),
                "open_to_close_pct": _number(row.get("open_to_close_pct")),
            }
            for row in group.head(3).to_dict(orient="records")
        ]
        rows.append({
            "sector": sector_text,
            "count": int(len(group)),
            "turnover_bil": turnover,
            "turnover_share_pct": turnover / latest_total * 100.0 if latest_total else None,
            "avg_open_to_close_pct": _weighted_avg(
                group["open_to_close_pct"],
                group["trading_value_billion"],
            ),
            "up_rate_pct": _up_rate(group),
            "avg_prior_5d_turnover_bil": avg_5d,
            "avg_prior_20d_turnover_bil": avg_20d,
            "latest_vs_prior_5d": turnover / avg_5d if avg_5d else None,
            "latest_vs_prior_20d": turnover / avg_20d if avg_20d else None,
            "new_count": int(group["is_new_top150"].fillna(False).sum()),
            "rank_up_count": int(group["rank_change"].fillna(0).gt(0).sum()),
            "positive_streak_days": int(persist.get("positive_streak_days", 0)),
            "active_streak_days": int(persist.get("active_streak_days", 0)),
            "top_stocks": top_stocks,
            "top_names": [stock["name"] or stock["code"] for stock in top_stocks],
        })

    turnover_values = [float(row["turnover_bil"]) for row in rows]
    sector_count = len(turnover_values)
    for row in rows:
        turnover = float(row["turnover_bil"])
        turnover_percentile = (
            sum(value <= turnover for value in turnover_values) / sector_count * 100.0
            if sector_count
            else 0.0
        )
        ratio_5d = _number(row.get("latest_vs_prior_5d"))
        ratio_20d = _number(row.get("latest_vs_prior_20d"))
        ratio_5d_score = _clamp(((ratio_5d if ratio_5d is not None else 1.0) - 0.5) * 100.0)
        ratio_20d_score = _clamp(((ratio_20d if ratio_20d is not None else 1.0) - 0.5) * 100.0)
        count = max(int(row["count"]), 1)
        movement_score = _clamp(
            (int(row["new_count"]) + int(row["rank_up_count"])) / count * 100.0
        )
        persistence_score = _clamp(int(row["positive_streak_days"]) * 20.0)
        attention_score = _clamp(
            turnover_percentile * 0.30
            + ratio_5d_score * 0.25
            + ratio_20d_score * 0.15
            + movement_score * 0.15
            + persistence_score * 0.15
        )

        oc = _number(row.get("avg_open_to_close_pct")) or 0.0
        up_rate = _number(row.get("up_rate_pct"))
        oc_signal = _clamp(oc * 25.0, -100.0, 100.0)
        up_signal = _clamp(((up_rate if up_rate is not None else 50.0) - 50.0) * 2.0, -100.0, 100.0)
        direction_score = _clamp(
            oc_signal * 0.60 + up_signal * 0.40,
            -100.0,
            100.0,
        )
        flow_score = _clamp(
            direction_score * (0.55 + attention_score / 100.0 * 0.45),
            -100.0,
            100.0,
        )

        positive_streak = int(row["positive_streak_days"])
        change_count = int(row["new_count"]) + int(row["rank_up_count"])
        if direction_score <= -30.0 and attention_score >= 45.0:
            status = "risk"
            status_label = "リスク"
        elif direction_score < -10.0 and (
            (ratio_5d is not None and ratio_5d < 0.90)
            or positive_streak == 0
        ):
            status = "fading"
            status_label = "失速"
        elif (
            direction_score >= 35.0
            and attention_score >= 50.0
            and positive_streak >= 2
            and oc >= 0.50
        ):
            status = "leader"
            status_label = "主役"
        elif (
            direction_score >= 25.0
            and oc >= 0.50
            and (
                (ratio_5d is not None and ratio_5d >= 1.15)
                or change_count >= 2
            )
        ):
            status = "emerging"
            status_label = "浮上"
        elif direction_score >= 10.0 and attention_score >= 35.0 and oc >= 0.25:
            status = "watch"
            status_label = "監視"
        else:
            status = "neutral"
            status_label = "中立"

        evidence = [
            f"OC {oc:+.2f}%",
            f"上昇率 {(up_rate if up_rate is not None else 0.0):.1f}%",
        ]
        if ratio_5d is not None:
            evidence.append(f"5日比 {ratio_5d:.2f}x")
        if positive_streak:
            evidence.append(f"陽線 {positive_streak}日")

        row.update({
            "turnover_percentile": turnover_percentile,
            "attention_score": attention_score,
            "direction_score": direction_score,
            "flow_score": flow_score,
            "status": status,
            "status_label": status_label,
            "evidence": evidence,
        })

    status_order = {
        "leader": 0,
        "emerging": 1,
        "risk": 2,
        "watch": 3,
        "fading": 4,
        "neutral": 5,
    }
    rows.sort(
        key=lambda row: (
            status_order.get(str(row["status"]), 9),
            -abs(float(row["flow_score"])),
            -float(row["attention_score"]),
        )
    )
    counts = {
        status: sum(row["status"] == status for row in rows)
        for status in status_order
    }
    return {
        "available": True,
        "baseline": {
            "prior_5d_dates": prior_5_dates,
            "prior_20d_dates": prior_20_dates,
        },
        "coverage": {
            "classified_turnover_pct": (
                latest_total / non_etf_total * 100.0
                if non_etf_total
                else None
            ),
            "unclassified_count": int(len(unclassified)),
            "unclassified_turnover_bil": float(unclassified["trading_value_billion"].sum()),
        },
        "status_counts": counts,
        "rows": rows,
    }


def _bucket_sort_key(bucket: str) -> tuple[int, str]:
    try:
        return THEME_BUCKET_ORDER.index(bucket), bucket
    except ValueError:
        return len(THEME_BUCKET_ORDER), bucket


def _up_rate(group: pd.DataFrame) -> float | None:
    if group.empty:
        return None
    return float(group["open_to_close_pct"].gt(0).mean() * 100.0)


def _top_names(group: pd.DataFrame, limit: int = 3) -> list[str]:
    if group.empty:
        return []
    return [
        str(name)
        for name in group.sort_values("rank", kind="mergesort")["stock_name"].head(limit).fillna("").tolist()
        if str(name)
    ]


def _bucket_daily(latest: pd.DataFrame, recent: pd.DataFrame, dates: list[str]) -> list[dict[str, Any]]:
    if latest.empty:
        return []
    persistence = _entity_persistence(recent, "theme_bucket", dates)
    total = float(latest["trading_value_billion"].sum())
    rows = []
    for bucket, group in latest.groupby("theme_bucket", dropna=False):
        bucket_text = str(bucket or "other")
        turnover = float(group["trading_value_billion"].sum())
        persist = persistence.get(bucket_text, {})
        rows.append({
            "bucket": bucket_text,
            "label": THEME_BUCKET_LABELS.get(bucket_text, bucket_text),
            "count": int(len(group)),
            "turnover_bil": turnover,
            "turnover_share_pct": turnover / total * 100.0 if total else None,
            "avg_open_to_close_pct": _weighted_avg(group["open_to_close_pct"], group["trading_value_billion"]),
            "up_rate_pct": _up_rate(group),
            "top1_30_count": int(group["rank_band"].eq("top1_30").sum()),
            "top31_150_count": int(group["rank"].between(31, 150).sum()),
            "new_count": int(group.get("is_new_top150", pd.Series(dtype=bool)).fillna(False).sum()),
            "rank_up_count": int(group.get("rank_change", pd.Series(dtype=float)).fillna(0).gt(0).sum()),
            "top_names": _top_names(group),
            "active_days": persist.get("active_days", 0),
            "active_streak_days": persist.get("active_streak_days", 0),
            "positive_days": persist.get("positive_days", 0),
            "positive_streak_days": persist.get("positive_streak_days", 0),
            "persistence_score": persist.get("persistence_score", 0),
        })
    return sorted(rows, key=lambda row: _bucket_sort_key(str(row["bucket"])))


def _bucket_weekly(recent: pd.DataFrame, latest_date: str) -> list[dict[str, Any]]:
    if recent.empty:
        return []
    daily_rows = []
    for (date, bucket), group in recent.groupby(["date", "theme_bucket"], dropna=False):
        daily_rows.append({
            "date": str(date),
            "bucket": str(bucket or "other"),
            "turnover_bil": float(group["trading_value_billion"].sum()),
            "avg_open_to_close_pct": _weighted_avg(group["open_to_close_pct"], group["trading_value_billion"]),
            "count": int(len(group)),
            "up_rate_pct": _up_rate(group),
        })
    daily = pd.DataFrame(daily_rows)
    if daily.empty:
        return []
    all_dates = sorted(daily["date"].astype(str).unique().tolist())
    baseline_dates = [date for date in all_dates if date < latest_date][-5:]
    rows = []
    for bucket, group in daily.groupby("bucket", dropna=False):
        latest = group[group["date"].eq(latest_date)]
        latest_turnover = float(latest["turnover_bil"].iloc[0]) if not latest.empty else 0.0
        avg_turnover = float(group["turnover_bil"].mean()) if not group.empty else 0.0
        baseline = group[group["date"].isin(baseline_dates)].set_index("date")["turnover_bil"] if baseline_dates else pd.Series(dtype=float)
        baseline_values = [float(baseline.get(date, 0.0)) for date in baseline_dates]
        avg_5d_turnover = float(sum(baseline_values) / len(baseline_values)) if baseline_values else None
        rows.append({
            "bucket": str(bucket or "other"),
            "label": THEME_BUCKET_LABELS.get(str(bucket or "other"), str(bucket or "other")),
            "active_days": int(group["date"].nunique()),
            "avg_daily_turnover_bil": avg_turnover,
            "avg_5d_turnover_bil": avg_5d_turnover,
            "baseline_days": int(len(baseline_dates)),
            "latest_turnover_bil": latest_turnover,
            "latest_vs_avg": latest_turnover / avg_turnover if avg_turnover else None,
            "latest_vs_5d_avg": latest_turnover / avg_5d_turnover if avg_5d_turnover else None,
            "latest_count": int(latest["count"].iloc[0]) if not latest.empty else 0,
            "avg_open_to_close_pct": _weighted_avg(group["avg_open_to_close_pct"], group["turnover_bil"]),
            "avg_up_rate_pct": float(group["up_rate_pct"].mean()) if not group.empty else None,
        })
    return sorted(rows, key=lambda row: _bucket_sort_key(str(row["bucket"])))


def _bucket_snapshots(recent: pd.DataFrame, dates: list[str], display_days: int = 5) -> list[dict[str, Any]]:
    if recent.empty or not dates:
        return []
    snapshot_dates = dates[-display_days:]
    rows: list[dict[str, Any]] = []
    for snapshot_date in snapshot_dates:
        day = recent[recent["date"].eq(snapshot_date)].copy()
        if day.empty:
            continue
        total = float(day["trading_value_billion"].sum())
        baseline_dates = [date for date in dates if date < snapshot_date][-5:]
        baseline_source = recent[recent["date"].isin(baseline_dates)].copy()
        persistence = _entity_persistence(recent[recent["date"].le(snapshot_date)].copy(), "theme_bucket", [date for date in dates if date <= snapshot_date])

        baseline_by_bucket: dict[str, pd.Series] = {}
        if not baseline_source.empty:
            baseline_daily = (
                baseline_source
                .groupby(["date", "theme_bucket"], dropna=False)["trading_value_billion"]
                .sum()
                .reset_index()
            )
            for bucket, group in baseline_daily.groupby("theme_bucket", dropna=False):
                baseline_by_bucket[str(bucket or "other")] = group.set_index("date")["trading_value_billion"]

        for bucket, group in day.groupby("theme_bucket", dropna=False):
            bucket_text = str(bucket or "other")
            turnover = float(group["trading_value_billion"].sum())
            baseline = baseline_by_bucket.get(bucket_text, pd.Series(dtype=float))
            baseline_values = [float(baseline.get(date, 0.0)) for date in baseline_dates]
            avg_5d_turnover = float(sum(baseline_values) / len(baseline_values)) if baseline_values else None
            persist = persistence.get(bucket_text, {})
            rows.append({
                "date": snapshot_date,
                "bucket": bucket_text,
                "label": THEME_BUCKET_LABELS.get(bucket_text, bucket_text),
                "count": int(len(group)),
                "turnover_bil": turnover,
                "turnover_share_pct": turnover / total * 100.0 if total else None,
                "avg_open_to_close_pct": _weighted_avg(group["open_to_close_pct"], group["trading_value_billion"]),
                "up_rate_pct": _up_rate(group),
                "top1_30_count": int(group["rank_band"].eq("top1_30").sum()),
                "top31_150_count": int(group["rank"].between(31, 150).sum()),
                "top_names": _top_names(group),
                "avg_5d_turnover_bil": avg_5d_turnover,
                "baseline_days": int(len(baseline_dates)),
                "latest_turnover_bil": turnover,
                "latest_vs_5d_avg": turnover / avg_5d_turnover if avg_5d_turnover else None,
                "latest_count": int(len(group)),
                "active_days": persist.get("active_days", 0),
                "active_streak_days": persist.get("active_streak_days", 0),
                "positive_days": persist.get("positive_days", 0),
                "positive_streak_days": persist.get("positive_streak_days", 0),
                "persistence_score": persist.get("persistence_score", 0),
            })
    return sorted(rows, key=lambda row: (str(row["date"]), _bucket_sort_key(str(row["bucket"]))))


def _normalize_index_frame(
    frame: pd.DataFrame,
    *,
    ticker: str | None = None,
    ticker_col: str = "ticker",
    date_col: str = "date",
    open_col: str = "Open",
    close_col: str = "Close",
    volume_col: str | None = "Volume",
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    source = frame.copy()
    if ticker is not None and ticker_col in source.columns:
        source = source[source[ticker_col].astype(str).eq(ticker)].copy()
    if source.empty or date_col not in source.columns or open_col not in source.columns or close_col not in source.columns:
        return pd.DataFrame()
    out = pd.DataFrame({
        "date": pd.to_datetime(source[date_col], errors="coerce"),
        "open": pd.to_numeric(source[open_col], errors="coerce"),
        "close": pd.to_numeric(source[close_col], errors="coerce"),
    })
    if volume_col and volume_col in source.columns:
        out["volume"] = pd.to_numeric(source[volume_col], errors="coerce")
    else:
        out["volume"] = None
    out = out.dropna(subset=["date", "open", "close"]).sort_values("date").copy()
    if out.empty:
        return out
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["turnover_bil"] = out["close"] * pd.to_numeric(out["volume"], errors="coerce") / 1_000_000_000
    return out


def _market_basket_flow_frame(basket_key: str) -> pd.DataFrame:
    source = _read_optional_parquet(MARKET_BASKET_TURNOVER_PATH)
    if source is None:
        return pd.DataFrame()
    required = {"date", "basket_key", "turnover_bil"}
    if source.empty or not required.issubset(source.columns):
        return pd.DataFrame()
    rows = source[source["basket_key"].astype(str).eq(basket_key)].copy()
    if rows.empty:
        return pd.DataFrame()
    out = pd.DataFrame({
        "date": pd.to_datetime(rows["date"], errors="coerce"),
        "turnover_bil": pd.to_numeric(rows["turnover_bil"], errors="coerce"),
    })
    out = out.dropna(subset=["date", "turnover_bil"]).sort_values("date").copy()
    if out.empty:
        return out
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def _benchmark_snapshot_rows(
    price_source: pd.DataFrame,
    flow_source: pd.DataFrame,
    *,
    key: str,
    label: str,
    dates: list[str],
    price_ticker: str,
    flow_ticker: str,
    note: str,
) -> list[dict[str, Any]]:
    if price_source.empty:
        return []
    flow_by_date = flow_source.set_index("date") if not flow_source.empty else pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for date in dates[-5:]:
        price_row = price_source[price_source["date"].eq(date)]
        if price_row.empty:
            continue
        price = price_row.iloc[-1]
        open_price = _number(price.get("open"))
        close_price = _number(price.get("close"))
        oc = (close_price / open_price - 1.0) * 100.0 if open_price and close_price else None

        turnover = None
        avg_5d_turnover = None
        latest_vs_5d = None
        if not flow_by_date.empty and date in flow_by_date.index:
            flow_row = flow_by_date.loc[date]
            if isinstance(flow_row, pd.DataFrame):
                flow_row = flow_row.iloc[-1]
            turnover = _number(flow_row.get("turnover_bil"))
            baseline = flow_source[flow_source["date"].lt(date)].tail(5)
            baseline_turnovers = [
                _number(value)
                for value in baseline["turnover_bil"].tolist()
                if _number(value) is not None and (_number(value) or 0) > 0
            ]
            avg_5d_turnover = float(sum(baseline_turnovers) / len(baseline_turnovers)) if baseline_turnovers else None
            latest_vs_5d = turnover / avg_5d_turnover if turnover and avg_5d_turnover else None

        rows.append({
            "key": key,
            "label": label,
            "date": date,
            "oc_pct": oc,
            "turnover_bil": turnover,
            "avg_5d_turnover_bil": avg_5d_turnover,
            "latest_vs_5d_avg": latest_vs_5d,
            "tv5d_delta_pct": (latest_vs_5d - 1.0) * 100.0 if latest_vs_5d is not None else None,
            "price_ticker": price_ticker,
            "flow_ticker": flow_ticker,
            "note": note,
        })
    return rows


def _market_benchmark_snapshots(dates: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not dates:
        return {}
    index_prices = _read_optional_parquet(INDEX_PRICES_PATH)
    if index_prices is None:
        return {}

    topix_prices = pd.DataFrame()
    topix_raw = _read_optional_parquet(TOPIX_PRICES_PATH)
    if topix_raw is not None:
        topix_prices = _normalize_index_frame(
            topix_raw,
            ticker="0000",
            open_col="open",
            close_col="close",
            volume_col=None,
        )

    n225_price = _normalize_index_frame(index_prices, ticker="^N225")
    n225_basket_flow = _market_basket_flow_frame("n225")
    topix500_basket_flow = _market_basket_flow_frame("topix500")
    n225_flow = n225_basket_flow if not n225_basket_flow.empty else _normalize_index_frame(index_prices, ticker="1570.T")
    topix_flow = topix500_basket_flow if not topix500_basket_flow.empty else _normalize_index_frame(index_prices, ticker="1306.T")
    n225_flow_ticker = "N225 basket" if not n225_basket_flow.empty else "1570.T"
    topix_flow_ticker = "TOPIX500 basket" if not topix500_basket_flow.empty else "1306.T"
    n225_note = "TV5dΔはN225構成銘柄Va合計" if not n225_basket_flow.empty else "TV5dΔは1570 ETF売買代金proxy"
    topix_note = "TV5dΔはTOPIX500構成銘柄Va合計" if not topix500_basket_flow.empty else "TV5dΔは1306 ETF売買代金proxy"
    if topix_prices.empty:
        topix_prices = topix_flow

    return {
        "n225": _benchmark_snapshot_rows(
            n225_price,
            n225_flow,
            key="n225",
            label="N225",
            dates=dates,
            price_ticker="^N225",
            flow_ticker=n225_flow_ticker,
            note=n225_note,
        ),
        "topix": _benchmark_snapshot_rows(
            topix_prices,
            topix_flow,
            key="topix",
            label="TOPIX500",
            dates=dates,
            price_ticker="TOPIX",
            flow_ticker=topix_flow_ticker,
            note=topix_note,
        ),
    }


def _bucket_metric(rows: list[dict[str, Any]], bucket: str, key: str) -> float | None:
    for row in rows:
        if row.get("bucket") == bucket:
            return _number(row.get(key))
    return None


def _bucket_turnover(rows: list[dict[str, Any]], buckets: set[str]) -> float:
    return float(sum(_number(row.get("turnover_bil")) or 0.0 for row in rows if row.get("bucket") in buckets))


def _bucket_weighted_oc(rows: list[dict[str, Any]], buckets: set[str]) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for row in rows:
        if row.get("bucket") not in buckets:
            continue
        turnover = _number(row.get("turnover_bil"))
        oc = _number(row.get("avg_open_to_close_pct"))
        if turnover is None or oc is None:
            continue
        numerator += turnover * oc
        denominator += turnover
    return numerator / denominator if denominator else None


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _bucket_row(rows: list[dict[str, Any]], bucket: str) -> dict[str, Any]:
    return next((row for row in rows if row.get("bucket") == bucket), {})


def _ticker_row(latest: pd.DataFrame, ticker: str) -> dict[str, Any] | None:
    if latest.empty or "ticker" not in latest.columns:
        return None
    rows = latest[latest["ticker"].astype(str).eq(ticker)].sort_values("rank", kind="mergesort")
    if rows.empty:
        return None
    return rows.iloc[0].to_dict()


def _fmt_pct_value(value: Any, digits: int = 1) -> str:
    number = _number(value)
    return "-" if number is None else f"{number:+.{digits}f}%"


def _fmt_share_value(value: Any, digits: int = 1) -> str:
    number = _number(value)
    return "-" if number is None else f"{number:.{digits}f}%"


def _fmt_bil_value(value: Any, digits: int = 0) -> str:
    number = _number(value)
    return "-" if number is None else f"{number:,.{digits}f}億"


def _point_metrics(row: dict[str, Any] | None, previous_row: dict[str, Any] | None = None) -> dict[str, Any]:
    if not row:
        return {"in_top150": False}
    open_value = _number(row.get("Open"))
    close_value = _number(row.get("Close"))
    previous_close = _number(previous_row.get("Close")) if previous_row else None
    return {
        "in_top150": True,
        "rank": int(row["rank"]) if _number(row.get("rank")) is not None else None,
        "ticker": row.get("ticker"),
        "code": row.get("code"),
        "name": row.get("stock_name"),
        "turnover_bil": _number(row.get("trading_value_billion")),
        "open_to_close_pct": _number(row.get("open_to_close_pct")),
        "close_to_close_pct": (close_value / previous_close - 1.0) * 100.0 if close_value and previous_close else None,
        "gap_pct": (open_value / previous_close - 1.0) * 100.0 if open_value and previous_close else None,
        "rank_band": row.get("rank_band"),
    }


def _stock_metrics(row: pd.Series) -> dict[str, Any]:
    return {
        "rank": int(row["rank"]) if _number(row.get("rank")) is not None else None,
        "rank_change": _number(row.get("rank_change")),
        "ticker": row.get("ticker"),
        "code": row.get("code"),
        "name": row.get("stock_name"),
        "turnover_bil": _number(row.get("trading_value_billion")),
        "open_to_close_pct": _number(row.get("open_to_close_pct")),
        "rank_band": row.get("rank_band"),
    }


def _bucket_stock_rows(
    latest: pd.DataFrame,
    bucket: str,
    *,
    limit: int = 5,
    positive_only: bool = False,
) -> list[dict[str, Any]]:
    if latest.empty or "theme_bucket" not in latest.columns:
        return []
    source = latest[latest["theme_bucket"].astype(str).eq(bucket)].copy()
    if positive_only:
        source = source[pd.to_numeric(source["open_to_close_pct"], errors="coerce").gt(0)]
    if source.empty:
        return []
    return [
        _stock_metrics(row)
        for _, row in source.sort_values(
            ["rank", "trading_value_billion"],
            ascending=[True, False],
            kind="mergesort",
        ).head(limit).iterrows()
    ]


def _bucket_positive_leaders(latest: pd.DataFrame, bucket: str, *, limit: int = 5) -> list[dict[str, Any]]:
    if latest.empty or "theme_bucket" not in latest.columns:
        return []
    source = latest[latest["theme_bucket"].astype(str).eq(bucket)].copy()
    source["open_to_close_pct"] = pd.to_numeric(source["open_to_close_pct"], errors="coerce")
    source = source[source["open_to_close_pct"].gt(0)].copy()
    if source.empty:
        return []
    return [
        _stock_metrics(row)
        for _, row in source.sort_values(
            ["open_to_close_pct", "trading_value_billion"],
            ascending=[False, False],
            kind="mergesort",
        ).head(limit).iterrows()
    ]


def _bucket_weak_laggards(latest: pd.DataFrame, bucket: str, *, limit: int = 3) -> list[dict[str, Any]]:
    if latest.empty or "theme_bucket" not in latest.columns:
        return []
    source = latest[latest["theme_bucket"].astype(str).eq(bucket)].copy()
    source["open_to_close_pct"] = pd.to_numeric(source["open_to_close_pct"], errors="coerce")
    source = source[source["open_to_close_pct"].lt(0)].copy()
    if source.empty:
        return []
    return [
        _stock_metrics(row)
        for _, row in source.sort_values(
            ["open_to_close_pct", "trading_value_billion"],
            ascending=[True, False],
            kind="mergesort",
        ).head(limit).iterrows()
    ]


def _flow_alert(
    *,
    key: str,
    severity: str,
    title: str,
    scope: str,
    evidence: list[str],
    metrics: dict[str, Any] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "severity": severity,
        "title": title,
        "scope": scope,
        "evidence": evidence,
        "metrics": metrics or {},
        "note": note,
    }


def _stock_names(stocks: list[dict[str, Any]], *, limit: int = 4, with_pct: bool = False) -> list[str]:
    names: list[str] = []
    for stock in stocks[:limit]:
        name = str(stock.get("name") or stock.get("ticker") or "").strip()
        if not name:
            continue
        if with_pct:
            names.append(f"{name}({_fmt_pct_value(stock.get('open_to_close_pct'))})")
        else:
            names.append(name)
    return names


def _flow_analysis(
    latest: pd.DataFrame,
    previous: pd.DataFrame,
    bucket_daily: list[dict[str, Any]],
    bucket_weekly: list[dict[str, Any]],
    market_regime: dict[str, Any],
    market_temperature: dict[str, Any],
) -> dict[str, Any]:
    kioxia = _bucket_row(bucket_daily, "kioxia")
    semicon_main = _bucket_row(bucket_daily, "semicon_main")
    mlcc = _bucket_row(bucket_daily, "electronics_parts")
    cable = _bucket_row(bucket_daily, "dc_cable_optical")
    ai_power = _bucket_row(bucket_daily, "ai_power_heavy")
    robotics = _bucket_row(bucket_daily, "robotics_factory_auto")
    other = _bucket_row(bucket_daily, "other")

    weekly = {str(row.get("bucket")): row for row in bucket_weekly}
    mlcc_weekly = weekly.get("electronics_parts", {})
    semicon_main_weekly = weekly.get("semicon_main", {})

    row_200a = _ticker_row(latest, "200A.T")
    row_2644 = _ticker_row(latest, "2644.T")
    row_2243 = _ticker_row(latest, "2243.T")
    row_1570 = _ticker_row(latest, "1570.T")
    row_kioxia = _ticker_row(latest, "285A.T")
    prev_200a = _ticker_row(previous, "200A.T")
    prev_2644 = _ticker_row(previous, "2644.T")
    prev_2243 = _ticker_row(previous, "2243.T")
    prev_1570 = _ticker_row(previous, "1570.T")
    prev_kioxia = _ticker_row(previous, "285A.T")

    metrics_200a = _point_metrics(row_200a, prev_200a)
    metrics_2644 = _point_metrics(row_2644, prev_2644)
    metrics_2243 = _point_metrics(row_2243, prev_2243)
    metrics_1570 = _point_metrics(row_1570, prev_1570)
    metrics_kioxia = _point_metrics(row_kioxia, prev_kioxia)
    mlcc_top_stocks = _bucket_stock_rows(latest, "electronics_parts", limit=5)
    mlcc_positive_leaders = _bucket_positive_leaders(latest, "electronics_parts", limit=5)
    mlcc_weak_laggards = _bucket_weak_laggards(latest, "electronics_parts", limit=3)
    semicon_top_stocks = _bucket_stock_rows(latest, "semicon_main", limit=5)
    semicon_positive_leaders = _bucket_positive_leaders(latest, "semicon_main", limit=5)
    semicon_weak_laggards = _bucket_weak_laggards(latest, "semicon_main", limit=3)

    kioxia_share = _number(kioxia.get("turnover_share_pct")) or 0.0
    kioxia_oc = _number(kioxia.get("avg_open_to_close_pct")) or 0.0
    mlcc_share = _number(mlcc.get("turnover_share_pct")) or 0.0
    mlcc_oc = _number(mlcc.get("avg_open_to_close_pct")) or 0.0
    mlcc_up = _number(mlcc.get("up_rate_pct"))
    mlcc_vs_5d = _number(mlcc_weekly.get("latest_vs_5d_avg"))
    semicon_main_oc = _number(semicon_main.get("avg_open_to_close_pct")) or 0.0
    semicon_main_up = _number(semicon_main.get("up_rate_pct"))
    semicon_main_vs_5d = _number(semicon_main_weekly.get("latest_vs_5d_avg"))
    cable_oc = _number(cable.get("avg_open_to_close_pct")) or 0.0
    ai_power_oc = _number(ai_power.get("avg_open_to_close_pct")) or 0.0
    other_oc = _number(other.get("avg_open_to_close_pct")) or 0.0
    other_up = _number(other.get("up_rate_pct"))
    oc_200a = _number(metrics_200a.get("open_to_close_pct"))
    oc_2644 = _number(metrics_2644.get("open_to_close_pct"))
    oc_1570 = _number(metrics_1570.get("open_to_close_pct"))
    kioxia_close_to_close = _number(metrics_kioxia.get("close_to_close_pct"))
    kioxia_gap = _number(metrics_kioxia.get("gap_pct"))

    kioxia_sell = kioxia_share >= 20.0 and kioxia_oc <= -1.0
    kioxia_breakdown = (
        kioxia_share >= 25.0
        and (
            (kioxia_close_to_close is not None and kioxia_close_to_close <= -10.0)
            or (
                kioxia_gap is not None
                and kioxia_gap <= -7.0
                and kioxia_oc <= -2.0
            )
        )
    )
    mlcc_probe = mlcc_share >= 5.0 and mlcc_oc >= 3.0 and (mlcc_up is None or mlcc_up >= 50.0)
    mlcc_daily_bid = mlcc_share >= 8.0 and mlcc_oc >= 1.0 and (mlcc_up is None or mlcc_up >= 50.0)
    mlcc_weekly_confirmed = mlcc_vs_5d is not None and mlcc_vs_5d >= 1.2
    semicon_main_repair = semicon_main_oc >= 0.25 and (semicon_main_up is None or semicon_main_up >= 50.0)
    semicon_main_weekly_confirmed = semicon_main_vs_5d is not None and semicon_main_vs_5d >= 1.05
    etf_divergence = (
        oc_200a is not None
        and oc_2644 is not None
        and oc_200a <= -1.0
        and oc_2644 >= -0.5
        and oc_2644 >= oc_200a + 1.5
    )
    index_drag = oc_1570 is not None and oc_1570 <= -1.5

    point_alerts: list[dict[str, Any]] = []
    if kioxia_sell:
        point_alerts.append(_flow_alert(
            key="kioxia_sell_pressure",
            severity="high",
            title="キオクシア急落" if kioxia_breakdown else "キオクシア安",
            scope="point",
            evidence=[
                f"構成比 {_fmt_share_value(kioxia_share)}",
                f"前日比 {_fmt_pct_value(kioxia_close_to_close)}",
                f"Gap {_fmt_pct_value(kioxia_gap)}",
                f"OC {_fmt_pct_value(kioxia_oc)}",
                f"売買代金 {_fmt_bil_value(kioxia.get('turnover_bil'))}",
            ],
            metrics=metrics_kioxia,
        ))
    if metrics_200a.get("in_top150"):
        point_alerts.append(_flow_alert(
            key="etf_200a_headline",
            severity="warn" if oc_200a is not None and oc_200a < 0 else "info",
            title="200A 軟調",
            scope="point",
            evidence=[
                f"順位 {metrics_200a.get('rank')}",
                f"OC {_fmt_pct_value(oc_200a)}",
                f"売買代金 {_fmt_bil_value(metrics_200a.get('turnover_bil'))}",
            ],
            metrics=metrics_200a,
            note="半導体ETFの確認用。個別・bucketの代替にしない。",
        ))
    if metrics_2644.get("in_top150"):
        point_alerts.append(_flow_alert(
            key="etf_2644_related",
            severity="info" if oc_2644 is None or oc_2644 >= 0 else "warn",
            title="2644 小幅耐性",
            scope="point",
            evidence=[
                f"順位 {metrics_2644.get('rank')}",
                f"OC {_fmt_pct_value(oc_2644)}",
                f"売買代金 {_fmt_bil_value(metrics_2644.get('turnover_bil'))}",
            ],
            metrics=metrics_2644,
            note="200Aとの差を見る補助指標。",
        ))
    if not metrics_2243.get("in_top150"):
        point_alerts.append(_flow_alert(
            key="etf_2243_low_liquidity",
            severity="info",
            title="2243 Top150外",
            scope="point",
            evidence=["売買代金Top150外", "主監視ではなく補助"],
            metrics=metrics_2243,
        ))
    if index_drag:
        point_alerts.append(_flow_alert(
            key="index_beta_drag",
            severity="medium",
            title="1570 軟調",
            scope="point",
            evidence=[
                f"順位 {metrics_1570.get('rank')}",
                f"OC {_fmt_pct_value(oc_1570)}",
                f"売買代金 {_fmt_bil_value(metrics_1570.get('turnover_bil'))}",
            ],
            metrics=metrics_1570,
        ))

    line_alerts: list[dict[str, Any]] = []
    if kioxia_sell and mlcc_daily_bid:
        line_alerts.append(_flow_alert(
            key="memory_to_mlcc_shift",
            severity="high" if mlcc_weekly_confirmed else "medium",
            title="キオクシア安・MLCC高",
            scope="line",
            evidence=[
                f"キオクシア OC {_fmt_pct_value(kioxia_oc)}",
                f"MLCC OC {_fmt_pct_value(mlcc_oc)}",
                f"MLCC TV5d {mlcc_vs_5d:.2f}x" if mlcc_vs_5d is not None else "MLCC TV5d -",
            ],
            metrics={
                "kioxia_share_pct": kioxia_share,
                "kioxia_open_to_close_pct": kioxia_oc,
                "mlcc_share_pct": mlcc_share,
                "mlcc_open_to_close_pct": mlcc_oc,
                "mlcc_latest_vs_5d_avg": mlcc_vs_5d,
            },
        ))
    elif kioxia_sell and mlcc_probe:
        line_alerts.append(_flow_alert(
            key="mlcc_probe",
            severity="medium",
            title="キオクシア安・MLCC打診",
            scope="line",
            evidence=[
                f"キオクシア OC {_fmt_pct_value(kioxia_oc)}",
                f"MLCC OC {_fmt_pct_value(mlcc_oc)}",
                f"MLCC構成比 {_fmt_share_value(mlcc_share)}",
            ],
            metrics={
                "kioxia_share_pct": kioxia_share,
                "kioxia_open_to_close_pct": kioxia_oc,
                "mlcc_share_pct": mlcc_share,
                "mlcc_open_to_close_pct": mlcc_oc,
                "mlcc_latest_vs_5d_avg": mlcc_vs_5d,
            },
            note="端緒。5日確認までは待つ。",
        ))
    if etf_divergence:
        line_alerts.append(_flow_alert(
            key="etf_200a_2644_divergence",
            severity="medium",
            title="200A < 2644",
            scope="line",
            evidence=[
                f"200A OC {_fmt_pct_value(oc_200a)}",
                f"2644 OC {_fmt_pct_value(oc_2644)}",
            ],
            metrics={
                "etf_200a_open_to_close_pct": oc_200a,
                "etf_2644_open_to_close_pct": oc_2644,
            },
            note="半導体内の濃淡を確認。",
        ))
    if semicon_main_repair:
        line_alerts.append(_flow_alert(
            key="semicon_ex_kioxia_repair",
            severity="medium" if semicon_main_weekly_confirmed else "info",
            title="中核exキオクシア 改善",
            scope="line",
            evidence=[
                f"OC {_fmt_pct_value(semicon_main_oc)}",
                f"Up率 {_fmt_share_value(semicon_main_up)}",
                f"TV5d {semicon_main_vs_5d:.2f}x" if semicon_main_vs_5d is not None else "TV5d -",
            ],
            metrics={
                "semicon_ex_open_to_close_pct": semicon_main_oc,
                "semicon_ex_up_rate_pct": semicon_main_up,
                "semicon_ex_latest_vs_5d_avg": semicon_main_vs_5d,
            },
        ))

    surface_alerts: list[dict[str, Any]] = []
    if mlcc_daily_bid:
        surface_alerts.append(_flow_alert(
            key="mlcc_bucket_bid",
            severity="high" if mlcc_weekly_confirmed else "medium",
            title="MLCC・電子部品 優位",
            scope="surface",
            evidence=[
                f"構成比 {_fmt_share_value(mlcc_share)}",
                f"OC {_fmt_pct_value(mlcc_oc)}",
                f"Up率 {_fmt_share_value(mlcc_up)}",
                f"TV5d {mlcc_vs_5d:.2f}x" if mlcc_vs_5d is not None else "TV5d -",
            ],
            metrics=mlcc,
        ))
    elif mlcc_probe:
        surface_alerts.append(_flow_alert(
            key="mlcc_bucket_probe",
            severity="medium",
            title="MLCC・電子部品 打診",
            scope="surface",
            evidence=[
                f"構成比 {_fmt_share_value(mlcc_share)}",
                f"OC {_fmt_pct_value(mlcc_oc)}",
                f"Up率 {_fmt_share_value(mlcc_up)}",
            ],
            metrics=mlcc,
        ))
    if semicon_main_repair:
        surface_alerts.append(_flow_alert(
            key="equipment_bucket_repair",
            severity="medium",
            title="装置・大型 改善確認",
            scope="surface",
            evidence=[
                f"OC {_fmt_pct_value(semicon_main_oc)}",
                f"Up率 {_fmt_share_value(semicon_main_up)}",
            ],
            metrics=semicon_main,
        ))

    not_observed: list[dict[str, Any]] = []
    if other_oc < 1.0 or (other_up is not None and other_up < 55.0):
        not_observed.append(_flow_alert(
            key="non_semicon_rotation_not_confirmed",
            severity="info",
            title="半導体外ローテ未成立",
            scope="not_observed",
            evidence=[
                f"その他 OC {_fmt_pct_value(other_oc)}",
                f"その他Up率 {_fmt_share_value(other_up)}",
            ],
            metrics=other,
        ))
    if cable_oc < 0.0 and ai_power_oc < 0.0:
        not_observed.append(_flow_alert(
            key="peripheral_contagion_not_broad",
            severity="info",
            title="電線・AI電力 波及なし",
            scope="not_observed",
            evidence=[
                f"電線 OC {_fmt_pct_value(cable_oc)}",
                f"AI電力 OC {_fmt_pct_value(ai_power_oc)}",
            ],
            metrics={
                "dc_cable_optical": cable,
                "ai_power_heavy": ai_power,
                "robotics_factory_auto": robotics,
            },
        ))

    trade_lens: list[dict[str, Any]] = []
    if kioxia_breakdown:
        trade_lens.append({
            "key": "semicon_long_wait",
            "priority": "high",
            "title": "半導体ロングは戻り確認",
            "evidence": [
                f"キオクシア前日比 {_fmt_pct_value(kioxia_close_to_close)}",
                "MLCC・装置の確認はリセット",
            ],
            "watch": ["キオクシアの下げ止まり", "MLCC・装置のVWAP上復帰", "AI電力・重電の持続"],
            "avoid": ["キオクシアの早い逆張り", "半導体ETFの一括ロング"],
            "next_day_checks": ["キオクシアがVWAP上に戻るか", "200A/2644が同時に沈まないか", "MLCCの売買代金が再拡大するか"],
        })
    elif kioxia_sell and mlcc_daily_bid:
        trade_lens.append({
            "key": "individual_selection_preferred",
            "priority": "high",
            "title": "個別選別を優先",
            "evidence": [
                "200A軟調、MLCC優位",
                "半導体内の濃淡を確認",
            ],
            "watch": ["太陽誘電", "村田製作所", "TDK", "東京エレクトロン", "イビデン"],
            "avoid": ["キオクシアの上値追い", "半導体ETFの一括ロング"],
            "next_day_checks": ["MLCC・装置がVWAP上に残るか", "200Aと2644の乖離が続くか", "キオクシアの売り圧が低下するか"],
        })
    else:
        trade_lens.append({
            "key": "wait_for_confirmation",
            "priority": "medium",
            "title": "日次端緒の継続確認",
            "evidence": ["bucket間の優位がまだ不十分"],
            "watch": ["売買代金増加とOCプラスが揃うbucket"],
            "avoid": ["根拠の薄いETF一括エントリー"],
            "next_day_checks": ["同じbucketが2営業日以上続くか"],
        })

    hypothesis_checks = [
        {
            "key": "daily",
            "label": "日次",
            "status": "成立" if kioxia_sell and mlcc_daily_bid else "打診" if kioxia_sell and mlcc_probe else "確認中",
            "evidence": [
                f"キオクシア OC {_fmt_pct_value(kioxia_oc)}",
                f"MLCC OC {_fmt_pct_value(mlcc_oc)}",
                f"200A OC {_fmt_pct_value(oc_200a)}",
                f"2644 OC {_fmt_pct_value(oc_2644)}",
            ],
        },
        {
            "key": "five_day",
            "label": "5日確認",
            "status": "確認" if mlcc_weekly_confirmed else "未確認",
            "evidence": [
                f"MLCC TV5d {mlcc_vs_5d:.2f}x" if mlcc_vs_5d is not None else "MLCC TV5d -",
                f"中核ex TV5d {semicon_main_vs_5d:.2f}x" if semicon_main_vs_5d is not None else "中核ex TV5d -",
            ],
        },
        {
            "key": "risk",
            "label": "リスク",
            "status": "高い" if kioxia_breakdown else "残る" if index_drag or kioxia_sell else "低い",
            "evidence": [
                f"キオクシア前日比 {_fmt_pct_value(kioxia_close_to_close)}",
                f"1570 OC {_fmt_pct_value(oc_1570)}",
                f"その他 OC {_fmt_pct_value(other_oc)}",
                f"電線 OC {_fmt_pct_value(cable_oc)}",
            ],
        },
    ]

    focus_groups = []
    if mlcc_daily_bid or mlcc_probe:
        focus_groups.append({
            "key": "mlcc",
            "label": "MLCC・電子部品",
            "status": "優位" if mlcc_daily_bid else "打診",
            "evidence": [
                f"構成比 {_fmt_share_value(mlcc_share)}",
                f"OC {_fmt_pct_value(mlcc_oc)}",
                f"TV5d {mlcc_vs_5d:.2f}x" if mlcc_vs_5d is not None else "TV5d -",
            ],
            "top_stocks": mlcc_top_stocks,
            "positive_leaders": mlcc_positive_leaders,
            "weak_laggards": mlcc_weak_laggards,
        })
    if semicon_main_repair:
        focus_groups.append({
            "key": "semicon_main_ex_kioxia",
            "label": "中核exキオクシア",
            "status": "改善確認",
            "evidence": [
                f"OC {_fmt_pct_value(semicon_main_oc)}",
                f"Up率 {_fmt_share_value(semicon_main_up)}",
                f"TV5d {semicon_main_vs_5d:.2f}x" if semicon_main_vs_5d is not None else "TV5d -",
            ],
            "top_stocks": semicon_top_stocks,
            "positive_leaders": semicon_positive_leaders,
            "weak_laggards": semicon_weak_laggards,
        })

    signal_score = 45.0
    signal_score += 8.0 if kioxia_sell else 0.0
    signal_score -= 10.0 if kioxia_breakdown else 0.0
    signal_score += 5.0 if mlcc_probe and not mlcc_daily_bid else 0.0
    signal_score += 10.0 if mlcc_daily_bid else 0.0
    signal_score += 6.0 if mlcc_weekly_confirmed else 0.0
    signal_score += 4.0 if etf_divergence else 0.0
    signal_score += 3.0 if semicon_main_repair else 0.0
    signal_score -= 5.0 if index_drag else 0.0
    signal_score = _clamp(signal_score)

    if kioxia_breakdown:
        state_label = "キオクシア急落"
        state_summary = f"キオクシアは前日比{_fmt_pct_value(kioxia_close_to_close)}、寄り後もOC{_fmt_pct_value(kioxia_oc)}。半導体内循環はリセット気味で、ロングは戻り確認優先。"
    elif kioxia_sell and mlcc_daily_bid:
        state_label = "7月初動・半導体内選別"
        state_summary = "キオクシア安、MLCC高。200Aは軟調、2644は小幅耐性。半導体全体ではなく内側の選別。"
    elif kioxia_sell and mlcc_probe:
        state_label = "キオクシア安・MLCC打診"
        state_summary = "キオクシア安は継続。MLCCは反発したが、構成比と5日確認はまだ不足。"
    elif kioxia_sell:
        state_label = "メモリー売り圧"
        state_summary = "キオクシアの売り圧が強く、半導体内循環はまだ確認不足。"
    elif mlcc_daily_bid or semicon_main_repair:
        state_label = "半導体内改善確認"
        state_summary = "MLCC・装置側の改善を確認中。継続性は週次で見る。"
    else:
        state_label = str(market_regime.get("primary") or "中立・確認待ち")
        state_summary = str(market_temperature.get("action") or "強いbucketの継続を確認。")

    check_text = " / ".join(f"{row['label']}={row['status']}" for row in hypothesis_checks)
    not_observed_titles = [str(row.get("title")) for row in not_observed if row.get("title")]
    trade = trade_lens[0] if trade_lens else {}
    focus_lines: list[str] = []
    for group in focus_groups:
        strong = " / ".join(_stock_names(group.get("positive_leaders", []), limit=4, with_pct=True)) or "-"
        weak = " / ".join(_stock_names(group.get("weak_laggards", []), limit=3, with_pct=True)) or "-"
        focus_lines.append(f"{group['label']}({group['status']}): 強い {strong} / 弱い {weak}")

    report_bullets = [
        state_summary,
        f"確認: {check_text}",
    ]
    if focus_lines:
        report_bullets.extend(focus_lines)
    if not_observed_titles:
        report_bullets.append("未成立: " + " / ".join(not_observed_titles))
    if trade:
        report_bullets.append(f"方針: {trade.get('title')}")

    report_body_parts = [
        f"{state_label}。",
        state_summary,
        f"確認は{check_text}。",
    ]
    if not_observed_titles:
        report_body_parts.append("未成立は" + "、".join(not_observed_titles) + "。")
    if trade:
        report_body_parts.append(f"方針は{trade.get('title')}。")

    report_summary = {
        "title": "Top150資金フロー",
        "headline": state_label,
        "body": "".join(report_body_parts),
        "bullets": report_bullets,
        "watch": trade.get("watch", []),
        "avoid": trade.get("avoid", []),
        "next_day_checks": trade.get("next_day_checks", []),
        "evidence_label": "J-Quants / trading_value_top_history.parquet",
    }

    return {
        "version": 1,
        "market_state": {
            "label": state_label,
            "summary": state_summary,
            "stance": market_temperature.get("stance"),
            "signal_score": signal_score,
            "basis": "point_line_surface",
        },
        "point_alerts": point_alerts,
        "line_alerts": line_alerts,
        "surface_alerts": surface_alerts,
        "not_observed": not_observed,
        "trade_lens": trade_lens,
        "hypothesis_checks": hypothesis_checks,
        "focus_groups": focus_groups,
        "report_summary": report_summary,
    }


def _market_regime(latest: pd.DataFrame, previous: pd.DataFrame, bucket_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = float(latest["trading_value_billion"].sum())
    all_oc = _weighted_avg(latest["open_to_close_pct"], latest["trading_value_billion"])
    all_up = _up_rate(latest)
    semicon_turnover = _bucket_turnover(bucket_rows, SEMICON_CORE_BUCKETS)
    semicon_share = semicon_turnover / total * 100.0 if total else 0.0
    semicon_oc = _bucket_weighted_oc(bucket_rows, SEMICON_CORE_BUCKETS) or 0.0
    kioxia_share = _bucket_metric(bucket_rows, "kioxia", "turnover_share_pct") or 0.0
    kioxia_oc = _bucket_metric(bucket_rows, "kioxia", "avg_open_to_close_pct") or 0.0
    metrics_kioxia = _point_metrics(_ticker_row(latest, "285A.T"), _ticker_row(previous, "285A.T"))
    kioxia_close_to_close = _number(metrics_kioxia.get("close_to_close_pct"))
    kioxia_gap = _number(metrics_kioxia.get("gap_pct"))
    semicon_ex_share = _bucket_metric(bucket_rows, "semicon_main", "turnover_share_pct") or 0.0
    semicon_ex_oc = _bucket_metric(bucket_rows, "semicon_main", "avg_open_to_close_pct") or 0.0
    semicon_ex_up = _bucket_metric(bucket_rows, "semicon_main", "up_rate_pct")
    mlcc_share = _bucket_metric(bucket_rows, "electronics_parts", "turnover_share_pct") or 0.0
    mlcc_oc = _bucket_metric(bucket_rows, "electronics_parts", "avg_open_to_close_pct") or 0.0
    mlcc_up = _bucket_metric(bucket_rows, "electronics_parts", "up_rate_pct")
    other_share = _bucket_metric(bucket_rows, "other", "turnover_share_pct") or 0.0
    other_oc = _bucket_metric(bucket_rows, "other", "avg_open_to_close_pct") or 0.0
    peripheral_turnover = _bucket_turnover(bucket_rows, PERIPHERAL_BUCKETS)
    peripheral_oc = _bucket_weighted_oc(bucket_rows, PERIPHERAL_BUCKETS)
    peripheral_share = peripheral_turnover / total * 100.0 if total else None

    kioxia_sell_pressure = kioxia_share >= 20.0 and kioxia_oc <= -1.0
    kioxia_breakdown = (
        kioxia_share >= 25.0
        and (
            (kioxia_close_to_close is not None and kioxia_close_to_close <= -10.0)
            or (kioxia_gap is not None and kioxia_gap <= -7.0 and kioxia_oc <= -2.0)
        )
    )
    mlcc_selective_bid = mlcc_share >= 8.0 and mlcc_oc >= 1.0 and (mlcc_up is None or mlcc_up >= 50.0)
    semicon_ex_bid = semicon_ex_share >= 12.0 and semicon_ex_oc >= 0.25 and (semicon_ex_up is None or semicon_ex_up >= 50.0)
    semicon_ex_sell = semicon_ex_share >= 12.0 and semicon_ex_oc <= -1.0
    broad_risk_off = all_oc is not None and all_oc < -2.0 and all_up is not None and all_up < 35.0

    secondary: list[str] = []
    if kioxia_breakdown:
        secondary.append("Kioxia Breakdown")
    if kioxia_sell_pressure:
        secondary.append("Kioxia Sell Pressure")
    if mlcc_selective_bid:
        secondary.append("MLCC Selective Bid")
    if semicon_ex_bid:
        secondary.append("Semicon Ex-Kioxia Confirmation")
    if peripheral_oc is not None and semicon_oc < -1.0 and peripheral_oc < -1.0:
        secondary.append("Peripheral Contagion")
    if other_share >= 18.0 and other_oc >= 0.5:
        secondary.append("Relative Strength Outside Semicon")
    if peripheral_oc is not None and semicon_oc > 1.0 and peripheral_oc > 0.5:
        secondary.append("Peripheral Confirmation")
    if broad_risk_off:
        secondary.append("Broad Risk-Off")

    if kioxia_breakdown:
        primary = "Kioxia Breakdown"
    elif broad_risk_off:
        primary = "Broad Risk-Off"
    elif kioxia_sell_pressure and mlcc_selective_bid:
        primary = "Kioxia Sell / MLCC Selective Bid"
    elif kioxia_sell_pressure and semicon_ex_bid:
        primary = "Kioxia Sell / Semicon Selective Bid"
    elif semicon_share >= 30.0 and kioxia_sell_pressure and semicon_ex_sell:
        primary = "Semicon Core Risk-Off"
    elif semicon_ex_share >= 15.0 and semicon_ex_oc >= 1.0 and (kioxia_share < 20.0 or kioxia_oc >= 0.0):
        primary = "Semicon Core Risk-On"
    elif other_share >= 20.0 and other_oc >= 1.0:
        primary = "Relative Strength Outside Semicon"
    else:
        primary = "Mixed / Neutral"

    return {
        "primary": primary,
        "secondary": secondary,
        "semicon_main_share_pct": semicon_share,
        "semicon_main_open_to_close_pct": semicon_oc,
        "semicon_core_share_pct": semicon_share,
        "semicon_core_open_to_close_pct": semicon_oc,
        "kioxia_share_pct": kioxia_share,
        "kioxia_open_to_close_pct": kioxia_oc,
        "kioxia_close_to_close_pct": kioxia_close_to_close,
        "kioxia_gap_pct": kioxia_gap,
        "kioxia_breakdown": kioxia_breakdown,
        "semicon_ex_kioxia_share_pct": semicon_ex_share,
        "semicon_ex_kioxia_open_to_close_pct": semicon_ex_oc,
        "mlcc_share_pct": mlcc_share,
        "mlcc_open_to_close_pct": mlcc_oc,
        "peripheral_share_pct": peripheral_share,
        "peripheral_open_to_close_pct": peripheral_oc,
        "other_share_pct": other_share,
        "other_open_to_close_pct": other_oc,
        "all_open_to_close_pct": all_oc,
        "all_up_rate_pct": all_up,
    }


def _index_metric(ticker: str, label: str) -> dict[str, Any] | None:
    prices = _read_optional_parquet(INDEX_PRICES_PATH)
    if prices is None:
        return None
    if prices.empty or "ticker" not in prices.columns or "Close" not in prices.columns:
        return None

    source = prices[prices["ticker"].astype(str).eq(ticker)].copy()
    if source.empty:
        return None
    source["date"] = pd.to_datetime(source["date"], errors="coerce")
    source["Close"] = pd.to_numeric(source["Close"], errors="coerce")
    source = source.dropna(subset=["date", "Close"]).sort_values("date").reset_index(drop=True)
    if source.empty:
        return None

    close = source["Close"]
    latest = source.iloc[-1]
    sma20 = float(close.tail(20).mean()) if len(close) >= 20 else None
    prev = float(close.iloc[-2]) if len(close) >= 2 else None
    prev5 = float(close.iloc[-6]) if len(close) >= 6 else None
    prev20 = float(close.iloc[-21]) if len(close) >= 21 else None
    latest_close = float(latest["Close"])

    return {
        "ticker": ticker,
        "label": label,
        "date": latest["date"].strftime("%Y-%m-%d"),
        "close": latest_close,
        "ret1_pct": (latest_close / prev - 1.0) * 100.0 if prev else None,
        "ret5_pct": (latest_close / prev5 - 1.0) * 100.0 if prev5 else None,
        "ret20_pct": (latest_close / prev20 - 1.0) * 100.0 if prev20 else None,
        "vs_sma20_pct": (latest_close / sma20 - 1.0) * 100.0 if sma20 else None,
        "above_sma20": bool(latest_close >= sma20) if sma20 else None,
    }


def _score_index_metric(metric: dict[str, Any] | None) -> float | None:
    if not metric:
        return None
    score = 50.0
    ret5 = _number(metric.get("ret5_pct"))
    ret20 = _number(metric.get("ret20_pct"))
    vs20 = _number(metric.get("vs_sma20_pct"))
    ret1 = _number(metric.get("ret1_pct"))
    if ret5 is not None:
        score += 10 if ret5 > 0 else -10
        if abs(ret5) >= 3:
            score += 5 if ret5 > 0 else -5
    if ret20 is not None:
        score += 10 if ret20 > 0 else -10
    if vs20 is not None:
        score += 10 if vs20 > 0 else -10
    if ret1 is not None:
        score += 4 if ret1 > 0 else -4
    return _clamp(score)


def _signed_tone(score: float) -> str:
    if score >= 20.0:
        return "good"
    if score <= -20.0:
        return "bad"
    if score < 0:
        return "warn"
    return "neutral"


def _market_direction(
    latest: pd.DataFrame,
    sector_radar: dict[str, Any],
) -> dict[str, Any]:
    top30 = latest[latest["rank"].le(30)].copy()
    top150_up = _up_rate(latest)
    top30_up = _up_rate(top30)
    top150_oc = _weighted_avg(latest["open_to_close_pct"], latest["trading_value_billion"])
    top30_oc = _weighted_avg(top30["open_to_close_pct"], top30["trading_value_billion"])

    n225 = _index_metric("^N225", "日経平均")
    topix_etf = _index_metric("1306.T", "TOPIX ETF")
    index_values = [
        (score - 50.0) * 2.0
        for score in [_score_index_metric(n225), _score_index_metric(topix_etf)]
        if score is not None
    ]
    index_direction = (
        float(sum(index_values) / len(index_values))
        if index_values
        else 0.0
    )

    top150_up_signal = _clamp(
        ((top150_up if top150_up is not None else 50.0) - 50.0) * 2.0,
        -100.0,
        100.0,
    )
    top30_up_signal = _clamp(
        ((top30_up if top30_up is not None else 50.0) - 50.0) * 2.0,
        -100.0,
        100.0,
    )
    top150_oc_signal = _clamp(
        (top150_oc if top150_oc is not None else 0.0) * 25.0,
        -100.0,
        100.0,
    )
    top30_oc_signal = _clamp(
        (top30_oc if top30_oc is not None else 0.0) * 25.0,
        -100.0,
        100.0,
    )
    breadth_direction = _clamp(
        top150_up_signal * 0.35
        + top30_up_signal * 0.15
        + top150_oc_signal * 0.30
        + top30_oc_signal * 0.20,
        -100.0,
        100.0,
    )

    sector_rows = sector_radar.get("rows") if sector_radar.get("available") else []
    sector_rows = sector_rows if isinstance(sector_rows, list) else []
    sector_positive_rate = (
        sum((_number(row.get("avg_open_to_close_pct")) or 0.0) > 0 for row in sector_rows)
        / len(sector_rows)
        * 100.0
        if sector_rows
        else 50.0
    )
    sector_equal_signal = _clamp(
        (sector_positive_rate - 50.0) * 2.0,
        -100.0,
        100.0,
    )
    non_etf = latest[
        ~latest["is_etf"]
        & latest["sectors"].astype(str).ne("UNKNOWN")
        & latest["sectors"].astype(str).str.strip().ne("")
    ].copy()
    non_etf_oc = _weighted_avg(
        non_etf["open_to_close_pct"],
        non_etf["trading_value_billion"],
    )
    sector_turnover_signal = _clamp(
        (non_etf_oc if non_etf_oc is not None else 0.0) * 25.0,
        -100.0,
        100.0,
    )
    sector_direction = _clamp(
        sector_equal_signal * 0.55 + sector_turnover_signal * 0.45,
        -100.0,
        100.0,
    )

    score = _clamp(
        index_direction * 0.30
        + breadth_direction * 0.35
        + sector_direction * 0.35,
        -100.0,
        100.0,
    )
    if score >= 55.0:
        label = "強気"
    elif score >= 20.0:
        label = "やや強気"
    elif score <= -55.0:
        label = "弱気"
    elif score <= -20.0:
        label = "やや弱気"
    else:
        label = "中立"

    components = [
        {
            "key": "index",
            "label": "指数",
            "score": index_direction,
            "weight": 0.30,
            "metrics": [
                {"label": "N225 1日", "value": _number(n225.get("ret1_pct")) if n225 else None, "format": "pct"},
                {"label": "N225 5日", "value": _number(n225.get("ret5_pct")) if n225 else None, "format": "pct"},
                {"label": "TOPIX 1日", "value": _number(topix_etf.get("ret1_pct")) if topix_etf else None, "format": "pct"},
                {"label": "TOPIX 5日", "value": _number(topix_etf.get("ret5_pct")) if topix_etf else None, "format": "pct"},
            ],
        },
        {
            "key": "breadth",
            "label": "銘柄の広がり",
            "score": breadth_direction,
            "weight": 0.35,
            "metrics": [
                {"label": "Top150上昇率", "value": top150_up, "format": "pct1"},
                {"label": "Top30上昇率", "value": top30_up, "format": "pct1"},
                {"label": "Top150 OC", "value": top150_oc, "format": "pct"},
                {"label": "Top30 OC", "value": top30_oc, "format": "pct"},
            ],
        },
        {
            "key": "sector",
            "label": "セクターの広がり",
            "score": sector_direction,
            "weight": 0.35,
            "metrics": [
                {"label": "上昇セクター率", "value": sector_positive_rate, "format": "pct1"},
                {"label": "全業種OC", "value": non_etf_oc, "format": "pct"},
                {"label": "業種数", "value": float(len(sector_rows)), "format": "number"},
            ],
        },
    ]
    summary = (
        f"指数 {index_direction:+.0f} / 銘柄 {breadth_direction:+.0f} / "
        f"業種 {sector_direction:+.0f}"
    )
    return {
        "score": score,
        "label": label,
        "tone": _signed_tone(score),
        "summary": summary,
        "components": components,
        "breadth": {
            "top150_up_rate_pct": top150_up,
            "top30_up_rate_pct": top30_up,
            "top150_open_to_close_pct": top150_oc,
            "top30_open_to_close_pct": top30_oc,
            "positive_sector_rate_pct": sector_positive_rate,
            "non_etf_open_to_close_pct": non_etf_oc,
        },
        "index_metrics": {
            "n225": n225,
            "topix_etf": topix_etf,
        },
        "method": "index 30% + breadth 35% + equal/turnover sector breadth 35%",
        "sector_coverage": sector_radar.get("coverage"),
    }


def _execution_permission(
    latest: pd.DataFrame,
    market_direction: dict[str, Any],
) -> dict[str, Any]:
    direction_score = _number(market_direction.get("score")) or 0.0
    component_scores = [
        _number(component.get("score"))
        for component in market_direction.get("components", [])
        if isinstance(component, dict)
    ]
    component_scores = [score for score in component_scores if score is not None]
    if component_scores:
        mean_score = sum(component_scores) / len(component_scores)
        spread = math.sqrt(
            sum((score - mean_score) ** 2 for score in component_scores)
            / len(component_scores)
        )
        spread_agreement = _clamp(100.0 - spread * 1.2)
        if direction_score > 10.0:
            same_side = sum(score > 0 for score in component_scores)
        elif direction_score < -10.0:
            same_side = sum(score < 0 for score in component_scores)
        else:
            same_side = sum(abs(score) <= 20.0 for score in component_scores)
        side_agreement = same_side / len(component_scores) * 100.0
        agreement_score = spread_agreement * 0.55 + side_agreement * 0.45
    else:
        spread = None
        agreement_score = 0.0

    breadth = market_direction.get("breadth") or {}
    top150_up = _number(breadth.get("top150_up_rate_pct"))
    positive_sector_rate = _number(breadth.get("positive_sector_rate_pct"))
    breadth_conviction = sum([
        abs((top150_up if top150_up is not None else 50.0) - 50.0) * 2.0,
        abs((positive_sector_rate if positive_sector_rate is not None else 50.0) - 50.0) * 2.0,
    ]) / 2.0
    direction_clarity = _clamp(abs(direction_score) * 1.5)

    total = float(latest["trading_value_billion"].sum())
    sorted_latest = latest.sort_values("rank", kind="mergesort")
    top1_share = (
        float(sorted_latest.head(1)["trading_value_billion"].sum()) / total * 100.0
        if total
        else 0.0
    )
    top10_share = (
        float(sorted_latest.head(10)["trading_value_billion"].sum()) / total * 100.0
        if total
        else 0.0
    )
    extreme_move_rate = (
        float(latest["open_to_close_pct"].abs().ge(5.0).mean() * 100.0)
        if not latest.empty
        else 0.0
    )
    concentration_risk = 0.0
    concentration_risk += _clamp((top1_share - 10.0) * 2.0, 0.0, 45.0)
    concentration_risk += _clamp((top10_share - 45.0) * 1.2, 0.0, 30.0)
    concentration_risk += _clamp(extreme_move_rate * 2.0, 0.0, 25.0)
    concentration_risk = _clamp(concentration_risk)
    safety_score = 100.0 - concentration_risk

    score = _clamp(
        direction_clarity * 0.35
        + agreement_score * 0.25
        + breadth_conviction * 0.20
        + safety_score * 0.20
    )
    caps: list[str] = []
    if abs(direction_score) < 20.0:
        score = min(score, 39.0)
        caps.append("市場方向が中立域")
    if agreement_score < 45.0:
        score = min(score, 44.0)
        caps.append("指数・銘柄・業種が不一致")
    if concentration_risk >= 60.0:
        score = min(score, 44.0)
        caps.append("売買代金の集中または急変が大きい")

    if score >= 70.0:
        label = "積極"
        action = "方向に沿った執行可"
    elif score >= 45.0:
        label = "小さく可"
        action = "小ロットで確認後に拡大"
    else:
        label = "静観"
        action = "方向と広がりの一致を待つ"

    return {
        "score": score,
        "label": label,
        "tone": "good" if score >= 70.0 else "warn" if score >= 45.0 else "neutral",
        "action": action,
        "caps": caps,
        "components": [
            {"key": "clarity", "label": "方向明瞭度", "score": direction_clarity},
            {"key": "agreement", "label": "指標一致度", "score": agreement_score},
            {"key": "breadth", "label": "広がり確信度", "score": breadth_conviction},
            {"key": "safety", "label": "集中安全度", "score": safety_score},
        ],
        "metrics": {
            "component_dispersion": spread,
            "top1_share_pct": top1_share,
            "top10_share_pct": top10_share,
            "extreme_move_rate_pct": extreme_move_rate,
            "concentration_risk": concentration_risk,
        },
        "method": "direction clarity 35% + agreement 25% + breadth conviction 20% + concentration safety 20%",
    }


def _semiconductor_monitor(
    bucket_rows: list[dict[str, Any]],
    market_regime: dict[str, Any],
) -> dict[str, Any]:
    core_share = _number(market_regime.get("semicon_core_share_pct")) or 0.0
    core_oc = _number(market_regime.get("semicon_core_open_to_close_pct")) or 0.0
    kioxia_share = _bucket_metric(bucket_rows, "kioxia", "turnover_share_pct") or 0.0
    kioxia_oc = _bucket_metric(bucket_rows, "kioxia", "avg_open_to_close_pct") or 0.0
    ex_kioxia_oc = _number(market_regime.get("semicon_ex_kioxia_open_to_close_pct")) or 0.0
    etf_oc = _bucket_weighted_oc(bucket_rows, {"semicon_etf"})

    if core_share >= 30.0 and core_oc <= -1.0:
        state = "risk_source"
        label = "大商いの売り圧"
        tone = "bad"
    elif core_share >= 30.0 and core_oc >= 1.0:
        state = "leadership"
        label = "主役継続"
        tone = "good"
    elif kioxia_oc * ex_kioxia_oc < 0:
        state = "divergence"
        label = "内部選別"
        tone = "warn"
    else:
        state = "contested"
        label = "方向未確定"
        tone = "neutral"

    bucket_keys = [
        "kioxia",
        "semicon_main",
        "electronics_parts",
        "dc_cable_optical",
        "semicon_etf",
    ]
    groups = [
        {
            "key": key,
            "label": THEME_BUCKET_LABELS.get(key, key),
            "turnover_bil": _bucket_metric(bucket_rows, key, "turnover_bil"),
            "turnover_share_pct": _bucket_metric(bucket_rows, key, "turnover_share_pct"),
            "open_to_close_pct": _bucket_metric(bucket_rows, key, "avg_open_to_close_pct"),
            "up_rate_pct": _bucket_metric(bucket_rows, key, "up_rate_pct"),
            "positive_streak_days": _bucket_metric(bucket_rows, key, "positive_streak_days"),
        }
        for key in bucket_keys
    ]
    return {
        "state": state,
        "label": label,
        "tone": tone,
        "summary": (
            f"中核Share {core_share:.1f}% / 中核OC {core_oc:+.2f}% / "
            f"ETF OC {(etf_oc if etf_oc is not None else 0.0):+.2f}%"
        ),
        "metrics": {
            "core_share_pct": core_share,
            "core_open_to_close_pct": core_oc,
            "kioxia_share_pct": kioxia_share,
            "kioxia_open_to_close_pct": kioxia_oc,
            "ex_kioxia_open_to_close_pct": ex_kioxia_oc,
            "semicon_etf_open_to_close_pct": etf_oc,
        },
        "groups": groups,
    }


def _temperature_tone(score: float, risk_pressure: float = 0.0) -> str:
    if risk_pressure >= 70:
        return "bad"
    if score >= 62:
        return "good"
    if score <= 40:
        return "bad"
    if score <= 50:
        return "warn"
    return "neutral"


def _market_temperature(latest: pd.DataFrame, bucket_rows: list[dict[str, Any]], market_regime: dict[str, Any]) -> dict[str, Any]:
    total = float(latest["trading_value_billion"].sum())
    top30 = latest[latest["rank"].le(30)].copy()
    top10 = latest[latest["rank"].le(10)].copy()

    top150_up = _up_rate(latest)
    top30_up = _up_rate(top30)
    top150_oc = _weighted_avg(latest["open_to_close_pct"], latest["trading_value_billion"])
    top30_oc = _weighted_avg(top30["open_to_close_pct"], top30["trading_value_billion"])
    top1_share = float(latest.sort_values("rank").head(1)["trading_value_billion"].sum()) / total * 100.0 if total else None
    top10_share = float(top10["trading_value_billion"].sum()) / total * 100.0 if total else None
    inverse_share = sum(
        (_number(row.get("turnover_share_pct")) or 0.0)
        for row in bucket_rows
        if row.get("bucket") == "index_inverse"
    )

    n225 = _index_metric("^N225", "日経平均")
    topix_etf = _index_metric("1306.T", "TOPIX ETF")
    leveraged = _index_metric("1570.T", "日経レバ")
    sp500 = _index_metric("^GSPC", "S&P500")

    index_scores = [score for score in [_score_index_metric(n225), _score_index_metric(topix_etf)] if score is not None]
    index_score = float(sum(index_scores) / len(index_scores)) if index_scores else 50.0

    breadth_score = 50.0
    if top150_up is not None:
        breadth_score += (top150_up - 50.0) * 0.7
    if top30_up is not None:
        breadth_score += (top30_up - 50.0) * 0.3
    if top150_oc is not None:
        breadth_score += max(-12.0, min(12.0, top150_oc * 4.0))
    if top30_oc is not None:
        breadth_score += max(-10.0, min(10.0, top30_oc * 3.0))
    breadth_score = _clamp(breadth_score)

    semicon_oc = _number(market_regime.get("semicon_core_open_to_close_pct")) or 0.0
    semicon_share = _number(market_regime.get("semicon_core_share_pct")) or 0.0
    semicon_ex_share = _number(market_regime.get("semicon_ex_kioxia_share_pct")) or 0.0
    semicon_ex_oc = _number(market_regime.get("semicon_ex_kioxia_open_to_close_pct")) or 0.0
    other_oc = _number(market_regime.get("other_open_to_close_pct")) or 0.0
    other_share = _number(market_regime.get("other_share_pct")) or 0.0
    other_bucket = next((row for row in bucket_rows if row.get("bucket") == "other"), {})
    other_up = _number(other_bucket.get("up_rate_pct"))
    kioxia_share = _bucket_metric(bucket_rows, "kioxia", "turnover_share_pct") or 0.0
    kioxia_oc = _bucket_metric(bucket_rows, "kioxia", "avg_open_to_close_pct") or 0.0
    kioxia_close_to_close = _number(market_regime.get("kioxia_close_to_close_pct"))
    kioxia_breakdown = bool(market_regime.get("kioxia_breakdown"))
    mlcc_share = _bucket_metric(bucket_rows, "electronics_parts", "turnover_share_pct") or 0.0
    mlcc_oc = _bucket_metric(bucket_rows, "electronics_parts", "avg_open_to_close_pct") or 0.0
    mlcc_up = _bucket_metric(bucket_rows, "electronics_parts", "up_rate_pct")

    kioxia_sell_pressure = kioxia_share >= 20.0 and kioxia_oc <= -1.0
    mlcc_selective_bid = mlcc_share >= 8.0 and mlcc_oc >= 1.0 and (mlcc_up is None or mlcc_up >= 50.0)
    semicon_ex_improving = semicon_ex_share >= 12.0 and semicon_ex_oc >= 0.25
    semicon_core_broad_sell = kioxia_sell_pressure and semicon_ex_oc <= -1.0

    flow_score = 50.0
    flow_score += 12 if mlcc_selective_bid else 0
    flow_score += 6 if semicon_ex_improving else 0
    flow_score += 10 if other_oc >= 0.5 else 0
    flow_score += 6 if other_share >= 20.0 else 0
    flow_score += 6 if other_up is not None and other_up >= 55.0 else 0
    flow_score -= 12 if semicon_core_broad_sell and semicon_share >= 30.0 else 0
    flow_score -= 6 if kioxia_sell_pressure and not (mlcc_selective_bid or semicon_ex_improving) else 0
    flow_score -= 10 if kioxia_share >= 30.0 else 0
    flow_score -= 8 if kioxia_breakdown else 0
    flow_score = _clamp(flow_score)

    risk_pressure = 0.0
    risk_pressure += 30.0 if kioxia_share >= 30.0 else 15.0 if kioxia_share >= 20.0 else 0.0
    risk_pressure += 18.0 if kioxia_breakdown else 0.0
    risk_pressure += 18.0 if top1_share is not None and top1_share >= 30.0 else 8.0 if top1_share is not None and top1_share >= 20.0 else 0.0
    risk_pressure += 14.0 if top10_share is not None and top10_share >= 65.0 else 0.0
    risk_pressure += 18.0 if top30_oc is not None and top30_oc <= -2.0 else 8.0 if top30_oc is not None and top30_oc < 0 else 0.0
    risk_pressure += 10.0 if inverse_share >= 1.0 else 0.0
    risk_pressure = _clamp(risk_pressure)

    score = _clamp(index_score * 0.28 + breadth_score * 0.34 + flow_score * 0.28 + (100.0 - risk_pressure) * 0.10)

    semicon_risk_off = semicon_share >= 30.0 and semicon_core_broad_sell
    other_selective_bid = other_share >= 20.0 and other_oc >= 0.0 and (other_up is None or other_up >= 50.0)
    selective_bid = mlcc_selective_bid or semicon_ex_improving or other_selective_bid
    broad_risk_off = top150_oc is not None and top150_oc <= -2.0 and top150_up is not None and top150_up < 40.0
    month_end_noise = latest["date"].max()[8:10] in {"29", "30", "31"}

    if kioxia_breakdown:
        label = "キオクシア急落"
        stance = "守り優先"
        action = "半導体ロングは戻り確認。キオクシアのVWAP上復帰と売買代金の落ち着きを待つ。"
    elif broad_risk_off:
        label = "全面リスクオフ"
        stance = "守り優先"
        action = "ロングは薄く、VWAP下の大型主導は戻り売り目線。"
    elif kioxia_sell_pressure and mlcc_selective_bid:
        label = "MLCC選別買い優位"
        stance = "短期選別"
        action = "キオクシアは外し、MLCC・電子部品のVWAP上維持を確認。"
    elif kioxia_sell_pressure and semicon_ex_improving:
        label = "中核exキオクシア改善"
        stance = "短期選別"
        action = "キオクシアを分け、半導体中核のVWAP上維持を確認。"
    elif semicon_risk_off and selective_bid:
        label = "半導体売り / 選別買い"
        stance = "短期選別"
        action = "半導体ロングは抑え、非半導体でVWAP上に残る銘柄だけ短く見る。"
    elif semicon_risk_off:
        label = "半導体リスクオフ"
        stance = "慎重"
        action = "キオクシアと半導体大型の売り圧が落ちるまで攻めすぎない。"
    elif score >= 62:
        label = "選別リスクオン"
        stance = "攻め可"
        action = "売買代金とOCが揃うbucketを優先し、VWAP上を主戦場にする。"
    elif score <= 42:
        label = "リスクオフ注意"
        stance = "縮小"
        action = "方向が割れるため新規は短く、引け前の無理な追随は避ける。"
    else:
        label = "中立・方向確認"
        stance = "静観強め"
        action = "強い候補は監視、スイングは継続確認後に回す。"

    warnings: list[str] = []
    if kioxia_breakdown:
        warnings.append(f"キオクシア前日比 {kioxia_close_to_close:+.1f}%" if kioxia_close_to_close is not None else "キオクシア急落")
    elif kioxia_sell_pressure:
        warnings.append(f"キオクシア売り圧 {kioxia_share:.1f}%")
    elif kioxia_share >= 30.0:
        warnings.append(f"キオクシア集中 {kioxia_share:.1f}%")
    if top30_oc is not None and top30_oc < 0:
        warnings.append(f"Top30 OC {top30_oc:+.2f}%")
    if month_end_noise:
        warnings.append("月末需給ノイズ")
    if inverse_share >= 1.0:
        warnings.append(f"インバースETF浮上 {inverse_share:.1f}%")

    components = [
        {
            "key": "index",
            "label": "指数温度",
            "score": index_score,
            "tone": _temperature_tone(index_score),
            "headline": f"N225 5日 {(_number(n225.get('ret5_pct')) if n225 else None):+.2f}%" if n225 and _number(n225.get("ret5_pct")) is not None else "指数データ不足",
            "metrics": [
                {"label": "N225 5日", "value": _number(n225.get("ret5_pct")) if n225 else None, "format": "pct"},
                {"label": "N225 20日", "value": _number(n225.get("ret20_pct")) if n225 else None, "format": "pct"},
                {"label": "TOPIX 5日", "value": _number(topix_etf.get("ret5_pct")) if topix_etf else None, "format": "pct"},
                {"label": "TOPIX 20日", "value": _number(topix_etf.get("ret20_pct")) if topix_etf else None, "format": "pct"},
            ],
        },
        {
            "key": "breadth",
            "label": "内部温度",
            "score": breadth_score,
            "tone": _temperature_tone(breadth_score),
            "headline": f"Top150 Up率 {top150_up:.1f}%" if top150_up is not None else "内部データ不足",
            "metrics": [
                {"label": "Top150 Up率", "value": top150_up, "format": "pct1"},
                {"label": "Top30 Up率", "value": top30_up, "format": "pct1"},
                {"label": "Top150 OC", "value": top150_oc, "format": "pct"},
                {"label": "Top30 OC", "value": top30_oc, "format": "pct"},
            ],
        },
        {
            "key": "flow",
            "label": "フロー温度",
            "score": flow_score,
            "tone": _temperature_tone(flow_score),
            "headline": f"その他 OC {other_oc:+.2f}%",
            "metrics": [
                {"label": "半導体中核OC", "value": semicon_oc, "format": "pct"},
                {"label": "その他OC", "value": other_oc, "format": "pct"},
                {"label": "その他Up率", "value": other_up, "format": "pct1"},
                {"label": "その他Share", "value": other_share, "format": "pct1"},
            ],
        },
        {
            "key": "risk",
            "label": "左尾温度",
            "score": 100.0 - risk_pressure,
            "tone": _temperature_tone(100.0 - risk_pressure, risk_pressure),
            "headline": f"集中リスク {risk_pressure:.0f}/100",
            "metrics": [
                {"label": "Top1 Share", "value": top1_share, "format": "pct1"},
                {"label": "Top10 Share", "value": top10_share, "format": "pct1"},
                {"label": "キオクシア", "value": kioxia_share, "format": "pct1"},
                {"label": "キオクシア前日比", "value": kioxia_close_to_close, "format": "pct"},
                {"label": "インバース", "value": inverse_share, "format": "pct1"},
            ],
        },
    ]

    return {
        "label": label,
        "score": score,
        "tone": _temperature_tone(score, risk_pressure),
        "stance": stance,
        "action": action,
        "warnings": warnings,
        "components": components,
        "index_metrics": {
            "n225": n225,
            "topix_etf": topix_etf,
            "leveraged": leveraged,
            "sp500": sp500,
        },
        "risk_pressure": risk_pressure,
    }


def _risk_sources(latest: pd.DataFrame) -> pd.DataFrame:
    out = latest[
        latest["open_to_close_pct"].lt(0)
        & (
            latest["rank"].le(30)
            | latest["theme_bucket"].isin({"kioxia", "semicon_main", "dc_cable_optical", "electronics_parts", "semicon_etf", "index_bull"})
            | latest["trading_value_billion"].ge(100)
        )
    ].copy()
    return out.sort_values(["trading_value_billion", "rank"], ascending=[False, True], kind="mergesort")


def _other_leads(latest: pd.DataFrame, recent: pd.DataFrame) -> list[dict[str, Any]]:
    source = latest[latest["theme_bucket"].eq("other") & ~latest["is_etf"]].copy()
    if source.empty:
        return []
    total = float(latest["trading_value_billion"].sum())
    recent_other = recent[recent["theme_bucket"].eq("other") & ~recent["is_etf"]].copy()
    active_days = recent_other.groupby("sectors")["date"].nunique() if not recent_other.empty else pd.Series(dtype=int)
    dates = sorted(recent["date"].dropna().unique().tolist())
    persistence = _entity_persistence(recent_other, "sectors", dates)
    rows = []
    for sector, group in source.groupby("sectors", dropna=False):
        sector_text = str(sector or "UNKNOWN")
        turnover = float(group["trading_value_billion"].sum())
        share = turnover / total * 100.0 if total else 0.0
        rank_up_count = int(group.get("rank_change", pd.Series(dtype=float)).fillna(0).gt(0).sum())
        new_count = int(group.get("is_new_top150", pd.Series(dtype=bool)).fillna(False).sum())
        oc = _weighted_avg(group["open_to_close_pct"], group["trading_value_billion"])
        up_rate = _up_rate(group)
        days = int(active_days.get(sector, 0)) if len(active_days) else 0
        persist = persistence.get(sector_text, {})
        promotion_score = 0
        promotion_score += 2 if share >= 3.0 else 0
        promotion_score += 2 if oc is not None and oc >= 1.0 else 0
        promotion_score += 1 if up_rate is not None and up_rate >= 60.0 else 0
        promotion_score += 1 if days >= 2 else 0
        promotion_score += 1 if (persist.get("active_streak_days") or 0) >= 2 else 0
        promotion_score += 1 if (persist.get("positive_streak_days") or 0) >= 2 else 0
        promotion_score += 1 if rank_up_count + new_count >= 2 else 0
        rows.append({
            "sector": sector_text,
            "turnover_bil": turnover,
            "turnover_share_pct": share if total else None,
            "avg_open_to_close_pct": oc,
            "up_rate_pct": up_rate,
            "count": int(len(group)),
            "active_days": days,
            "active_streak_days": persist.get("active_streak_days", 0),
            "positive_days": persist.get("positive_days", 0),
            "positive_streak_days": persist.get("positive_streak_days", 0),
            "persistence_score": persist.get("persistence_score", 0),
            "rank_up_count": rank_up_count,
            "new_count": new_count,
            "promotion_score": promotion_score,
            "top_names": _top_names(group),
        })
    return sorted(rows, key=lambda row: (row["promotion_score"], row["turnover_bil"]), reverse=True)


def _dropped(previous: pd.DataFrame, latest: pd.DataFrame) -> pd.DataFrame:
    if previous.empty:
        return pd.DataFrame()
    current = set(latest["ticker"].astype(str).tolist())
    out = previous[~previous["ticker"].astype(str).isin(current)].copy()
    out = out[~out["is_etf"] & ~out["is_semiconductor"]].copy()
    return out.sort_values(["rank", "trading_value_billion"], ascending=[True, False], kind="mergesort")


def _flow_leads(latest: pd.DataFrame) -> pd.DataFrame:
    out = latest[
        ~latest["is_etf"]
        & ~latest["is_semiconductor"]
        & latest["rank"].ge(31)
    ].copy()
    if out.empty:
        return out
    out["flow_trigger"] = "watch"
    out.loc[out["is_new_top150"], "flow_trigger"] = "new"
    out.loc[out["rank_change"].ge(20), "flow_trigger"] = "rank_up"
    out.loc[out["days_in_top150"].le(2) & out["rank_change"].ge(10), "flow_trigger"] = "early_rank_up"
    out["_trigger_score"] = 0
    out.loc[out["flow_trigger"].eq("new"), "_trigger_score"] = 20
    out.loc[out["flow_trigger"].eq("rank_up"), "_trigger_score"] = 30
    out.loc[out["flow_trigger"].eq("early_rank_up"), "_trigger_score"] = 40
    out["_rank_change_sort"] = out["rank_change"].fillna(0)
    return out.sort_values(
        ["_trigger_score", "_rank_change_sort", "trading_value_billion", "rank"],
        ascending=[False, False, False, True],
        kind="mergesort",
    )


def _sustained_stocks(latest: pd.DataFrame) -> pd.DataFrame:
    out = latest[
        ~latest["is_etf"]
        & latest["consecutive_days_in_top150"].ge(2)
    ].copy()
    if out.empty:
        return out
    return out.sort_values(
        ["persistence_score", "positive_streak_days", "trading_value_billion", "rank"],
        ascending=[False, False, False, True],
        kind="mergesort",
    )


def _build_payload(days: int, top_n: int) -> dict[str, Any]:
    raw = _read_history()
    if raw is None or raw.empty:
        return {
            "available": False,
            "reason": "trading_value_top_history.parquet not found",
            "source": _source_label(HISTORY_PATH),
            "source_environment": APP_ENV,
            "source_data_mode": DATA_SOURCE_MODE,
        }

    all_history, recent, dates = _prepare(raw, top_n=top_n, days=days)
    if recent.empty or not dates:
        return {
            "available": False,
            "reason": "history has no usable rows",
            "source": _source_label(HISTORY_PATH),
            "source_environment": APP_ENV,
            "source_data_mode": DATA_SOURCE_MODE,
        }

    latest_date = dates[-1]
    previous_date = dates[-2] if len(dates) >= 2 else None
    latest = recent[recent["date"].eq(latest_date)].copy()
    previous = recent[recent["date"].eq(previous_date)].copy() if previous_date else pd.DataFrame()
    latest = _with_rank_context(latest, previous, recent, dates)

    total_turnover = float(latest["trading_value_billion"].sum())
    semicon = latest[latest["is_semiconductor"]]
    non_semicon_tail = latest[
        ~latest["is_semiconductor"]
        & ~latest["is_etf"]
        & latest["rank"].ge(31)
    ]
    leads = _flow_leads(latest)
    bucket_daily = _bucket_daily(latest, recent, dates)
    # Keep ETF summary aligned with the bucket taxonomy used by the composition view.
    etf_turnover = _bucket_turnover(bucket_daily, ETF_CONFIRMATION_BUCKETS)
    etf_open_to_close = _bucket_weighted_oc(bucket_daily, ETF_CONFIRMATION_BUCKETS)
    bucket_weekly = _bucket_weekly(recent, latest_date)
    bucket_snapshots = _bucket_snapshots(recent, dates, display_days=5)
    market_benchmark_snapshots = _market_benchmark_snapshots(dates)
    market_regime = _market_regime(latest, previous, bucket_daily)
    sector_radar = _sector_radar(latest, all_history, latest_date)
    market_direction = _market_direction(latest, sector_radar)
    execution_permission = _execution_permission(latest, market_direction)
    semiconductor_monitor = _semiconductor_monitor(bucket_daily, market_regime)
    market_temperature = _market_temperature(latest, bucket_daily, market_regime)
    flow_analysis = _flow_analysis(
        latest, previous, bucket_daily, bucket_weekly, market_regime, market_temperature
    )
    risk_sources = _risk_sources(latest)
    other_leads = _other_leads(latest, recent)
    promotion_candidates = [
        row
        for row in other_leads
        if (_number(row.get("promotion_score")) or 0) >= 4
    ]
    new_entries = latest[
        latest["is_new_top150"]
        & ~latest["is_etf"]
        & ~latest["is_semiconductor"]
    ].sort_values(["rank", "trading_value_billion"], ascending=[True, False], kind="mergesort")
    dropped = _dropped(previous, latest)
    rank_movers = latest[
        latest["rank_change"].notna()
        & latest["rank_change"].gt(0)
        & ~latest["is_etf"]
    ].sort_values(["rank_change", "trading_value_billion"], ascending=[False, False], kind="mergesort")
    sustained_stocks = _sustained_stocks(latest)
    sustained_buckets = sorted(
        [
            row for row in bucket_daily
            if (_number(row.get("active_streak_days")) or 0) >= 2
        ],
        key=lambda row: (
            _number(row.get("persistence_score")) or 0,
            _number(row.get("positive_streak_days")) or 0,
            _number(row.get("turnover_bil")) or 0,
        ),
        reverse=True,
    )
    sustained_other_sectors = [
        row for row in other_leads
        if (_number(row.get("active_streak_days")) or 0) >= 2
    ]

    row_cols = [
        "date", "rank", "prev_rank", "rank_change", "rank_band", "days_in_top150",
        "consecutive_days_in_top150", "positive_streak_days", "persistence_score",
        "code", "ticker", "stock_name", "market", "sectors", "trading_value_billion",
        "open_to_close_pct", "price_diff", "is_new_top150", "is_semiconductor",
        "is_etf", "theme_bucket", "theme_label", "is_semicon_main", "is_theme_peripheral",
        "flow_trigger",
    ]
    top_cols = [
        "date", "rank", "prev_rank", "rank_change", "rank_band", "days_in_top150",
        "consecutive_days_in_top150", "positive_streak_days", "persistence_score",
        "code", "ticker", "stock_name", "market", "sectors", "trading_value_billion",
        "open_to_close_pct", "price_diff", "is_new_top150", "is_semiconductor", "is_etf",
        "theme_bucket", "theme_label", "is_semicon_main", "is_theme_peripheral",
    ]

    return _clean({
        "available": True,
        "generated_at": datetime.now().isoformat(),
        "source": _source_label(HISTORY_PATH),
        "source_environment": APP_ENV,
        "source_data_mode": DATA_SOURCE_MODE,
        "latest_date": latest_date,
        "previous_date": previous_date,
        "history_rows": int(len(all_history)),
        "recent_dates": dates,
        "summary": {
            "latest_total_turnover_bil": total_turnover,
            "semiconductor_turnover_bil": float(semicon["trading_value_billion"].sum()),
            "semiconductor_share_pct": float(semicon["trading_value_billion"].sum()) / total_turnover * 100.0 if total_turnover else None,
            "semiconductor_open_to_close_pct": _weighted_avg(semicon["open_to_close_pct"], semicon["trading_value_billion"]),
            "etf_turnover_bil": etf_turnover,
            "etf_share_pct": etf_turnover / total_turnover * 100.0 if total_turnover else None,
            "etf_open_to_close_pct": etf_open_to_close,
            "non_semiconductor_top31_150_turnover_bil": float(non_semicon_tail["trading_value_billion"].sum()),
            "non_semiconductor_top31_150_count": int(len(non_semicon_tail)),
            "new_non_semiconductor_count": int(len(new_entries)),
            "dropped_non_semiconductor_count": int(len(dropped)),
            "kioxia_turnover_bil": _bucket_metric(bucket_daily, "kioxia", "turnover_bil"),
            "kioxia_share_pct": _bucket_metric(bucket_daily, "kioxia", "turnover_share_pct"),
            "kioxia_open_to_close_pct": _bucket_metric(bucket_daily, "kioxia", "avg_open_to_close_pct"),
            "semicon_core_turnover_bil": _bucket_turnover(bucket_daily, SEMICON_CORE_BUCKETS),
            "semicon_core_share_pct": market_regime.get("semicon_core_share_pct"),
            "semicon_core_open_to_close_pct": market_regime.get("semicon_core_open_to_close_pct"),
            "semicon_main_turnover_bil": _bucket_metric(bucket_daily, "semicon_main", "turnover_bil"),
            "semicon_main_share_pct": _bucket_metric(bucket_daily, "semicon_main", "turnover_share_pct"),
            "semicon_main_open_to_close_pct": _bucket_metric(bucket_daily, "semicon_main", "avg_open_to_close_pct"),
            "theme_peripheral_turnover_bil": _bucket_turnover(bucket_daily, PERIPHERAL_BUCKETS),
            "theme_peripheral_share_pct": market_regime.get("peripheral_share_pct"),
            "theme_peripheral_open_to_close_pct": market_regime.get("peripheral_open_to_close_pct"),
            "other_turnover_bil": _bucket_metric(bucket_daily, "other", "turnover_bil"),
            "other_share_pct": _bucket_metric(bucket_daily, "other", "turnover_share_pct"),
            "other_open_to_close_pct": _bucket_metric(bucket_daily, "other", "avg_open_to_close_pct"),
        },
        "market_regime": market_regime,
        "market_direction": market_direction,
        "execution_permission": execution_permission,
        "sector_radar": sector_radar,
        "semiconductor_monitor": semiconductor_monitor,
        "market_temperature": market_temperature,
        "execution_program": _execution_program(),
        "flow_analysis": flow_analysis,
        "bucket_daily": bucket_daily,
        "bucket_weekly": bucket_weekly,
        "bucket_snapshot_dates": dates[-5:],
        "bucket_snapshots": _clean(bucket_snapshots),
        "market_benchmark_snapshots": _clean(market_benchmark_snapshots),
        "risk_sources": _records(risk_sources, row_cols, limit=30),
        "other_leads": _clean(other_leads[:30]),
        "promotion_candidates": _clean(promotion_candidates[:12]),
        "sustained_buckets": _clean(sustained_buckets[:12]),
        "sustained_other_sectors": _clean(sustained_other_sectors[:12]),
        "sustained_stocks": _records(sustained_stocks, row_cols, limit=30),
        "sector_daily": _sector_daily(latest, recent, dates),
        "sector_weekly": _sector_weekly(recent, latest_date),
        "flow_leads": _records(leads, row_cols, limit=30),
        "rank_movers": _records(rank_movers, row_cols, limit=30),
        "new_entries": _records(new_entries, row_cols, limit=30),
        "dropped": _records(dropped, top_cols, limit=30),
        "top150": _records(latest.sort_values("rank", kind="mergesort"), top_cols, limit=top_n),
    })


@router.get("/api/dev/market-flow")
async def get_market_flow(
    days: int = Query(default=20, ge=2, le=120),
    top_n: int = Query(default=150, ge=30, le=150),
):
    try:
        cache_key = (int(days), int(top_n))
        now = time.monotonic()
        with _CACHE_LOCK:
            cached = _PAYLOAD_CACHE.get(cache_key)
            if cached and float(cached["expires_at"]) > now:
                return cached["payload"]

        fingerprint = _payload_source_fingerprint()
        with _CACHE_LOCK:
            cached = _PAYLOAD_CACHE.get(cache_key)
            if cached and (
                fingerprint is None or cached.get("fingerprint") == fingerprint
            ):
                cached["expires_at"] = now + PAYLOAD_CACHE_TTL_SECONDS
                return cached["payload"]

        payload = _build_payload(days=days, top_n=top_n)
        if fingerprint is None:
            fingerprint = _payload_source_fingerprint()
        with _CACHE_LOCK:
            _PAYLOAD_CACHE[cache_key] = {
                "expires_at": now + PAYLOAD_CACHE_TTL_SECONDS,
                "fingerprint": fingerprint,
                "payload": payload,
            }
        return payload
    except Exception as exc:
        return {
            "available": False,
            "reason": str(exc),
            "source": _source_label(HISTORY_PATH),
            "source_environment": APP_ENV,
            "source_data_mode": DATA_SOURCE_MODE,
        }
