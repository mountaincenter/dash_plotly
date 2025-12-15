from __future__ import annotations

from typing import Any, Dict, List, Optional
import json
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..utils import (
    read_prices_1d_df,
    read_prices_df,
    normalize_prices,
    load_all_stocks,
)

router = APIRouter()


def _add_volatility_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    前提: normalize_prices() 済みの縦持ち DataFrame
         必須列: ["ticker","date","Open","High","Low","Close"]（Volume は任意）
    追加: prevClose, tr, tr_pct, atr14, atr14_pct
    """
    if df is None or df.empty:
        return df

    g = df.sort_values(["ticker", "date"]).copy()
    g["prevClose"] = g.groupby("ticker")["Close"].shift(1)

    # --- True Range (TR) ---
    hl = g["High"] - g["Low"]
    hp = (g["High"] - g["prevClose"]).abs()
    lp = (g["Low"] - g["prevClose"]).abs()
    g["tr"] = pd.concat([hl, hp, lp], axis=1).max(axis=1)

    # --- ATR(14): EMA(TR, span=14) ---
    g["atr14"] = (
        g.groupby("ticker", group_keys=False)["tr"]
        .apply(lambda s: s.ewm(span=14, adjust=False).mean())
    )

    # --- %表記 ---
    g["tr_pct"] = (g["tr"] / g["prevClose"] * 100.0).where(g["prevClose"] > 0)
    g["atr14_pct"] = (g["atr14"] / g["Close"] * 100.0).where(g["Close"] > 0)

    # Convert inf to NaN (replace deprecated use_inf_as_na)
    g["tr_pct"] = g["tr_pct"].replace([float('inf'), float('-inf')], pd.NA)
    g["atr14_pct"] = g["atr14_pct"].replace([float('inf'), float('-inf')], pd.NA)

    return g


def _to_json_records(df: pd.DataFrame, *, include_time: bool = False) -> List[Dict]:
    """
    DataFrame → JSON-ready list[dict]
    include_time=True の場合、datetimeを 'YYYY-MM-DDTHH:MM:SS' で出力
    """
    g = df.copy()
    g = g.sort_values(["ticker", "date"]).reset_index(drop=True)

    if include_time:
        g["date"] = g["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        g["date"] = g["date"].dt.strftime("%Y-%m-%d")

    g = g.where(pd.notna(g), None)
    return json.loads(g.to_json(orient="records"))


@router.get("/prices/max/1d", summary="Get price history (max span, 1d interval)")
def prices_max_1d():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []
    # 既存仕様維持（ここではボラ列は付けない）
    return _to_json_records(out)


@router.get("/prices", summary="Get price series with interval selection")
def prices(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    interval: str = Query(default="1d", description="時間足（例: 1d, 5m, 15m, 1h, 1wk, 1mo）"),
    end: Optional[str] = Query(default=None, description="終了日（YYYY-MM-DD または YYYY-MM-DDTHH:mm:ss）"),
    start: Optional[str] = Query(default=None, description="開始日（YYYY-MM-DD または YYYY-MM-DDTHH:mm:ss）"),
):
    """periodを自動マッピングし、日付指定は00:00〜23:59:59に自動補完"""
    ticker = (ticker or "").strip()
    if not ticker:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    # --- period 自動マッピング ---
    period_map = {
        "1m": "5d",
        "5m": "60d",
        "15m": "60d",
        "1h": "730d",
        "1d": "max",
        "1wk": "max",
        "1mo": "max",
    }
    interval_lc = interval.lower()
    period = period_map.get(interval_lc)
    if period is None:
        return JSONResponse(content={"error": f"unsupported interval: {interval}"}, status_code=400)

    # --- 日付パラメータ処理 ---
    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    try:
        if end:
            end_dt = pd.to_datetime(end).tz_localize(None)
            if end_dt.hour == 0 and end_dt.minute == 0 and end_dt.second == 0:
                end_dt = end_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        else:
            end_dt = today + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    except Exception:
        return JSONResponse(content={"error": "invalid end"}, status_code=400)

    try:
        if start:
            start_dt = pd.to_datetime(start).tz_localize(None)
        else:
            start_dt = end_dt - pd.Timedelta(days=365)
    except Exception:
        return JSONResponse(content={"error": "invalid start"}, status_code=400)

    if start_dt > end_dt:
        return JSONResponse(content={"error": "start must be <= end"}, status_code=400)

    # --- データ取得 ---
    df = read_prices_df(period, interval)
    if df is None:
        return JSONResponse(content={"error": f"No data found for interval={interval}"}, status_code=404)
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    # --- ボラティリティ付与 + 期間抽出 ---
    g = _add_volatility_columns(out)
    sel = g[
        (g["ticker"] == ticker)
        & (g["date"] >= start_dt)
        & (g["date"] <= end_dt)
    ]
    if sel.empty:
        return []

    # --- 出力：分足・時間足のみ時刻付き ---
    need_time = interval.lower() in {"1m", "5m", "15m", "1h"}
    return _to_json_records(sel, include_time=need_time)


@router.get("/prices/1d", summary="Get daily price series (compat)")
def prices_1d(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    end: Optional[str] = Query(default=None, description="終了日（YYYY-MM-DD）"),
    start: Optional[str] = Query(default=None, description="開始日（YYYY-MM-DD）"),
):
    """既存エンドポイント（互換性維持）"""
    return prices(ticker=ticker, interval="1d", end=end, start=start)


@router.get("/prices/snapshot/last2", summary="Get latest snapshot with technical indicators")
def prices_snapshot_last2():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    # --- ボラ列を付与（prevClose, tr, atr14 など） ---
    g = _add_volatility_columns(out)

    # 既存の出来高10MAも維持
    g = g.sort_values(["ticker", "date"]).copy()
    if "Volume" in g.columns:
        g["vol_ma10"] = (
            g.groupby("ticker")["Volume"]
            .rolling(window=10, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    else:
        g["Volume"] = pd.NA
        g["vol_ma10"] = pd.NA

    snap = g.groupby("ticker", as_index=False).tail(1).copy()
    snap["diff"] = snap["Close"] - snap["prevClose"]
    snap["date"] = snap["date"].dt.strftime("%Y-%m-%d")

    def _none(v: Any):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (int, float)):
            return float(v)
        return v

    records: List[Dict[str, Any]] = []
    for _, r in snap.iterrows():
        records.append({
            "ticker": str(r["ticker"]),
            "date": r["date"],
            "close": _none(r["Close"]),
            "prevClose": _none(r["prevClose"]),
            "diff": _none(r["diff"]),
            "volume": _none(r["Volume"]),
            "vol_ma10": _none(r["vol_ma10"]),
            "tr": _none(r.get("tr")),
            "tr_pct": _none(r.get("tr_pct")),
            "atr14": _none(r.get("atr14")),
            "atr14_pct": _none(r.get("atr14_pct")),
        })
    return records


@router.get("/perf/returns", summary="Get rolling returns for tickers")
def perf_returns(
    windows: str = Query(
        default="1d,5d,1mo,3mo,6mo,ytd,1y,3y,5y,all",
        description="Comma separated list of windows (e.g. 1d,5d,1mo)",
    ),
    tag: Optional[str] = Query(
        default=None,
        description="Filter tickers by primary tag (e.g. TOPIX_CORE30, 高市銘柄)",
    ),
) -> List[Dict[str, Any]]:
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    allowed_tickers: Optional[set[str]] = None
    if tag:
        meta = load_all_stocks(tag=tag) or []
        allowed_tickers = {
            str(item.get("ticker")).strip()
            for item in meta
            if item.get("ticker")
        }
        if allowed_tickers:
            out = out[out["ticker"].isin(allowed_tickers)].copy()
        else:
            return []

    wins = [w.strip() for w in (windows or "").split(",") if w.strip()]
    if not wins:
        wins = ["1d", "5d", "1mo", "3mo", "6mo", "ytd", "1y", "3y", "5y", "all"]

    days_map = {
        "5d": 7,
        "1w": 7,
        "1mo": 30,
        "3mo": 90,
        "6mo": 180,
        "1y": 365,
        "3y": 365 * 3,
        "5y": 365 * 5,
    }

    def pct_return(last_close: float, base_close: Optional[float]) -> Optional[float]:
        if (
            last_close is None
            or base_close is None
            or pd.isna(last_close)
            or pd.isna(base_close)
            or base_close == 0
        ):
            return None
        return float((float(last_close) / float(base_close) - 1.0) * 100.0)

    records: List[Dict[str, Any]] = []
    for ticker, grp in out.sort_values(["ticker", "date"]).groupby("ticker", as_index=False):
        g = grp[["date", "Close"]].dropna(subset=["Close"])
        if g.empty:
            continue
        last_row = g.iloc[-1]
        last_date = last_row["date"]
        last_close = float(last_row["Close"])

        def base_close_before_or_on(target_dt: pd.Timestamp) -> Optional[float]:
            sel = g[g["date"] <= target_dt]
            if sel.empty:
                return None
            return float(sel.iloc[-1]["Close"])

        row: Dict[str, Any] = {"ticker": ticker, "date": last_date.strftime("%Y-%m-%d")}
        for w in wins:
            key = f"r_{w}"
            if w == "1d":
                base = float(g.iloc[-2]["Close"]) if len(g) >= 2 else None
                row[key] = pct_return(last_close, base)
                continue
            elif w == "ytd":
                start_of_year = pd.Timestamp(year=last_date.year, month=1, day=1)
                base = base_close_before_or_on(start_of_year)
                row[key] = pct_return(last_close, base)
            elif w == "all":
                base = float(g.iloc[0]["Close"])
                row[key] = pct_return(last_close, base)
            else:
                days = days_map.get(w)
                if not days:
                    row[key] = None
                else:
                    target = last_date - pd.Timedelta(days=days)
                    base = base_close_before_or_on(target)
                    row[key] = pct_return(last_close, base)
        records.append(row)

    if allowed_tickers is not None:
        records = [r for r in records if r["ticker"] in allowed_tickers]

    return records
