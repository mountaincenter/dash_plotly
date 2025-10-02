from __future__ import annotations

from typing import Dict, List, Optional
import pandas as pd
from fastapi import APIRouter, Query

from ...utils import (
    read_prices_1d_df,
    normalize_prices,
)

router = APIRouter()

@router.get("/perf/returns")
def core30_perf_returns(
    windows: Optional[str] = Query(default=None, description="例: 5d,1mo,3mo,ytd,1y,3y,5y,all"),
):
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    win_param = (windows or "").strip()
    default_wins = ["5d", "1mo", "3mo", "ytd", "1y", "3y","5y", "all"]
    wins = [w.strip() for w in win_param.split(",") if w.strip()] or default_wins

    days_map = {"5d": 7, "1w": 7, "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "3y": 365*3, "5y": 365*5}

    def pct_return(last_close, base_close):
        if last_close is None or base_close is None or pd.isna(last_close) or pd.isna(base_close) or base_close == 0:
            return None
        return float((float(last_close) / float(base_close) - 1.0) * 100.0)

    records: List[Dict[str, any]] = []
    for tkr, g in out.sort_values(["ticker", "date"]).groupby("ticker", as_index=False):
        g = g[["date", "Close"]].dropna(subset=["Close"])
        if g.empty:
            continue
        last_row = g.iloc[-1]
        last_date = last_row["date"]
        last_close = float(last_row["Close"])

        def base_close_before_or_on(target_dt: pd.Timestamp):
            sel = g[g["date"] <= target_dt]
            if sel.empty:
                return None
            return float(sel.iloc[-1]["Close"])

        row: Dict[str, any] = {"ticker": tkr, "date": last_date.strftime("%Y-%m-%d")}
        for w in wins:
            key = f"r_{w}"
            if w == "ytd":
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
    return records
