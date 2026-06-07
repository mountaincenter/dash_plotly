from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException

router = APIRouter()

ROOT = Path(__file__).resolve().parents[2]
HOLDINGS_PARQUET = ROOT / "data" / "parquet" / "hold_stocks.parquet"
RESULTS_PARQUET = ROOT / "data" / "parquet" / "stock_results.parquet"
DAILY_PARQUET = ROOT / "data" / "parquet" / "prices_max_1d.parquet"
INTRADAY_PARQUET = ROOT / "data" / "parquet" / "prices_60d_5m.parquet"
PLAYBOOK_DIR = ROOT / "data" / "analysis" / "hedge_playbooks"

S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET") or "stock-api-data"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
APP_ENV = (os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or os.getenv("STAGE") or "local").strip().lower()
USE_S3_DATA = APP_ENV in {"production", "prod"}
S3_PREFIX = (os.getenv("PARQUET_PREFIX") or "parquet").strip("/")
ANALYSIS_PREFIX = (os.getenv("ANALYSIS_PREFIX") or "analysis").strip("/")

WATCHLIST = {
    "6323.T": {
        "code": "6323",
        "name": "ローツェ",
        "role": "守りのヘッジ管理。ショート損失の左尾を消すことを優先。",
        "stance": "4100/4002/3959 の維持・割れでヘッジ判断。裸ロングは下げ止まり確認後だけ。",
        "rules": [
            "4240超で強くVWAP上なら、ショート左尾対策としてヘッジ維持を優先。",
            "4100割れ・VWAP下維持ならヘッジを急がず、ショート回復を待つ。",
            "4002/3959付近は欲張り過ぎず、ショート損失縮小の候補。",
            "ヘッジ後は損失固定として扱い、明確な方向が出るまで細かく外さない。",
        ],
        "manual_levels": [
            ("直近天井", 4590, "risk"),
            ("6/4終値", 4424, "risk"),
            ("6/3高値", 4314, "risk"),
            ("6/5安値", 4002, "support"),
            ("6/3始値", 4039, "support"),
            ("6/3安値", 3959, "support"),
            ("6/2安値", 3616, "support"),
        ],
    },
    "6055.T": {
        "code": "6055",
        "name": "ジャパンマテリアル",
        "role": "ローツェより短期リバを見やすい候補。ただし2155割れ戻せずならロング禁止。",
        "stance": "2180-2200で止まりVWAPを回復するなら短期ロング余地。割れ戻せずならショート回復待ち。",
        "rules": [
            "2180-2200で下げ止まり、VWAP回復なら短期リバ候補。",
            "2155割れから戻せない場合はロング禁止。",
            "2266-2268は終値/VWAP近辺。ここを回復できるかで地合いを判断。",
            "ショートとヘッジを混在させる場合は、損失固定後に方向が出るまで触らない。",
        ],
        "manual_levels": [
            ("直近高値", 2425, "risk"),
            ("6/5高値", 2320, "risk"),
            ("6/5安値", 2155, "support"),
            ("2000近辺", 2000, "support"),
            ("6/2安値", 1956, "support"),
        ],
    },
}


def _num(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).replace(",", "").replace("%", "").strip()
    if text in {"", "-", "nan", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _date_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y-%m-%d")
    return str(value)


def _s3_key(filename: str) -> str:
    return f"{S3_PREFIX}/{filename}" if S3_PREFIX else filename


def _analysis_s3_key(relative_key: str) -> str:
    relative_key = relative_key.lstrip("/")
    return f"{ANALYSIS_PREFIX}/{relative_key}" if ANALYSIS_PREFIX else relative_key


def _source_label(path: Path) -> str:
    if USE_S3_DATA:
        return f"s3://{S3_BUCKET}/{_s3_key(path.name)}"
    return str(path.relative_to(ROOT))


def _s3_client():
    if not S3_BUCKET:
        raise HTTPException(status_code=500, detail="S3 bucket is not configured")
    import boto3

    client_kwargs: dict[str, Any] = {"region_name": AWS_REGION}
    if AWS_ENDPOINT:
        client_kwargs["endpoint_url"] = AWS_ENDPOINT
    return boto3.client("s3", **client_kwargs)


def _read_s3_parquet_required(filename: str) -> pd.DataFrame:
    try:
        s3 = _s3_client()
        key = _s3_key(filename)
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Required S3 parquet read failed: s3://{S3_BUCKET}/{_s3_key(filename)}: {exc}",
        ) from exc


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_data_parquet(path: Path) -> pd.DataFrame:
    if USE_S3_DATA:
        return _read_s3_parquet_required(path.name)
    return _read_parquet(path)


def _load_local_playbook() -> tuple[dict[str, Any] | None, str | None]:
    files = sorted(PLAYBOOK_DIR.glob("*.json"))
    if not files:
        return None, None
    path = files[-1]
    return json.loads(path.read_text(encoding="utf-8")), str(path.relative_to(ROOT))


def _load_s3_playbook() -> tuple[dict[str, Any] | None, str | None]:
    prefix = _analysis_s3_key("hedge_playbooks/")
    try:
        s3 = _s3_client()
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        keys = sorted(
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj.get("Key", "").endswith(".json")
        )
        if not keys:
            return None, None
        key = keys[-1]
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        return payload, f"s3://{S3_BUCKET}/{key}"
    except Exception as exc:
        return None, f"s3://{S3_BUCKET}/{prefix} read_error={exc}"


def _load_latest_playbook() -> tuple[dict[str, Any] | None, str | None]:
    if USE_S3_DATA:
        return _load_s3_playbook()
    return _load_local_playbook()


def _price_rows_from_df(df: pd.DataFrame, ticker: str, rows: int) -> list[dict[str, Any]]:
    if "ticker" not in df.columns:
        return []
    df = df[df["ticker"].astype(str) == ticker].copy()
    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").tail(rows)
    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        dt = row["date"]
        out.append(
            {
                "date": dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
                "Open": _num(row.get("Open")),
                "High": _num(row.get("High")),
                "Low": _num(row.get("Low")),
                "Close": _num(row.get("Close")),
                "Volume": _num(row.get("Volume")),
            }
        )
    return out


def _ma(rows: list[dict[str, Any]], window: int) -> float | None:
    values = [r.get("Close") for r in rows if r.get("Close") is not None]
    if len(values) < window:
        return None
    return round(sum(values[-window:]) / window, 2)


def _levels(meta: dict[str, Any], daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    levels: list[dict[str, Any]] = [
        {"label": label, "price": price, "kind": kind}
        for label, price, kind in meta["manual_levels"]
    ]
    for window in (5, 25, 75):
        value = _ma(daily_rows, window)
        if value is not None:
            levels.append({"label": f"{window}日線", "price": value, "kind": "ma"})
    latest = daily_rows[-1] if daily_rows else None
    if latest and latest.get("Close") is not None:
        levels.append({"label": "終値", "price": latest["Close"], "kind": "neutral"})
    seen_prices: set[int] = set()
    unique: list[dict[str, Any]] = []
    for item in levels:
        price = _num(item.get("price"))
        if price is None:
            continue
        price_key = int(round(price))
        if price_key in seen_prices:
            continue
        seen_prices.add(price_key)
        unique.append({**item, "price": price})
    return unique


def _holding_rows(holdings: pd.DataFrame, code: str) -> list[dict[str, Any]]:
    if holdings.empty or "ticker" not in holdings.columns:
        return []
    df = holdings[holdings["ticker"].astype(str).str.replace(".T", "", regex=False) == code].copy()
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        qty = _num(row.get("quantity")) or 0
        side = str(row.get("direction") or "")
        cost_total = _num(row.get("cost_total"))
        entry_price = cost_total / qty if cost_total is not None and qty else None
        rows.append(
            {
                "ticker": f"{code}.T",
                "stock_name": str(row.get("stock_name") or ""),
                "direction": side,
                "margin_type": str(row.get("margin_type") or ""),
                "deadline": str(row.get("deadline") or ""),
                "quantity": qty,
                "entry_price": entry_price,
                "current_price": _num(row.get("current_price")),
                "jnx_price": _num(row.get("jnx_price")),
                "market_value": _num(row.get("market_value")),
                "unrealized_pnl": _num(row.get("unrealized_pnl")) or 0,
                "unrealized_pct": _num(row.get("unrealized_pct")),
                "expiry_date": _date_text(row.get("expiry_date")),
            }
        )
    return rows


def _trade_rows(results: pd.DataFrame, code: str) -> tuple[list[dict[str, Any]], float]:
    if results.empty or "コード" not in results.columns:
        return [], 0.0
    df = results[results["コード"].astype(str).str.replace(".0", "", regex=False) == code].copy()
    if df.empty:
        return [], 0.0
    df["_date"] = pd.to_datetime(df["約定日"], errors="coerce")
    df["_pnl"] = df["実現損益"].map(_num).fillna(0)
    total = float(df["_pnl"].sum())
    df = df.sort_values("_date", ascending=False).head(12)
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rows.append(
            {
                "trade_date": _date_text(row.get("約定日")),
                "side": str(row.get("売買") or ""),
                "margin_type": str(row.get("信用区分") or ""),
                "realized_pnl": _num(row.get("実現損益")) or 0,
                "price": _num(row.get("平均単価")),
                "quantity": _num(row.get("数量")),
                "avg_cost": _num(row.get("平均取得価額")),
                "entry_date": _date_text(row.get("取得日")),
            }
        )
    return rows, total


def _position_summary(holdings: list[dict[str, Any]], realized_total: float) -> dict[str, Any]:
    long_qty = sum(h["quantity"] for h in holdings if h["direction"] == "買建")
    short_qty = sum(h["quantity"] for h in holdings if h["direction"] == "売建")
    long_unrealized = sum(h["unrealized_pnl"] for h in holdings if h["direction"] == "買建")
    short_unrealized = sum(h["unrealized_pnl"] for h in holdings if h["direction"] == "売建")
    net_unrealized = long_unrealized + short_unrealized
    return {
        "long_qty": long_qty,
        "short_qty": short_qty,
        "long_unrealized": long_unrealized,
        "short_unrealized": short_unrealized,
        "net_unrealized": net_unrealized,
        "realized_total": realized_total,
        "total_pnl": realized_total + net_unrealized,
        "hedged": long_qty > 0 and short_qty > 0,
    }


@router.get("/api/dev/hedge/positions")
async def get_hedge_positions() -> dict[str, Any]:
    holdings_df = _read_data_parquet(HOLDINGS_PARQUET)
    results_df = _read_data_parquet(RESULTS_PARQUET)
    daily_df = _read_data_parquet(DAILY_PARQUET)
    intraday_df = _read_data_parquet(INTRADAY_PARQUET)
    playbook, playbook_source = _load_latest_playbook()
    positions: list[dict[str, Any]] = []

    portfolio_unrealized = 0.0
    portfolio_realized = 0.0
    for ticker, meta in WATCHLIST.items():
        code = meta["code"]
        daily_rows = _price_rows_from_df(daily_df, ticker, 180)
        intraday_rows = _price_rows_from_df(intraday_df, ticker, 96)
        holdings = _holding_rows(holdings_df, code)
        latest_trades, realized_total = _trade_rows(results_df, code)
        summary = _position_summary(holdings, realized_total)
        portfolio_unrealized += summary["net_unrealized"]
        portfolio_realized += realized_total

        positions.append(
            {
                "ticker": ticker,
                "code": code,
                "name": meta["name"],
                "role": meta["role"],
                "stance": meta["stance"],
                "rules": meta["rules"],
                "summary": summary,
                "holdings": holdings,
                "latest_trades": latest_trades,
                "levels": _levels(meta, daily_rows),
                "daily_rows": daily_rows,
                "intraday_rows": intraday_rows,
            }
        )

    return {
        "as_of": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
        "source": {
            "environment": APP_ENV,
            "mode": "s3_required" if USE_S3_DATA else "local_parquet",
            "s3_bucket": S3_BUCKET if USE_S3_DATA else None,
            "s3_prefix": S3_PREFIX if USE_S3_DATA else None,
            "holdings": _source_label(HOLDINGS_PARQUET),
            "results": _source_label(RESULTS_PARQUET),
            "daily": _source_label(DAILY_PARQUET),
            "intraday": _source_label(INTRADAY_PARQUET),
            "playbook": playbook_source,
        },
        "portfolio": {
            "watch_count": len(positions),
            "unrealized_pnl": portfolio_unrealized,
            "realized_pnl": portfolio_realized,
            "total_pnl": portfolio_unrealized + portfolio_realized,
        },
        "rules": [
            "ヘッジは損失固定の道具。外す前に、VWAP・節目・指数の3点で方向を確認する。",
            "真水は別枠。ヘッジの損失を取り返す目的では入れない。",
            "急騰実業テーマ株は、参加条件より撤退条件を先に決める。",
        ],
        "playbook": playbook,
        "positions": positions,
    }
