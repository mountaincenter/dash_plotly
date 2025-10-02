from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

# 👇 utils は server 配下なので相対 import
from ..utils import (
    load_core30_meta,
    read_prices_1d_df,
    normalize_prices,
    to_json_records,
)

router = APIRouter()

@router.get("/meta")
def core30_meta():
    data = load_core30_meta()
    return data if data is not None else []

@router.get("/prices/max/1d")
def core30_prices_max_1d():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []
    return to_json_records(out)

@router.get("/prices/1d")
def core30_prices_1d(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    end: Optional[str] = Query(default=None, description="終了日（YYYY-MM-DD）"),
    start: Optional[str] = Query(default=None, description="開始日（YYYY-MM-DD）"),
):
    ticker = (ticker or "").strip()
    if not ticker:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    try:
        end_dt = pd.to_datetime(end).tz_localize(None) if end else today
    except Exception:
        return JSONResponse(content={"error": "invalid end"}, status_code=400)
    try:
        start_dt = pd.to_datetime(start).tz_localize(None) if start else end_dt - pd.Timedelta(days=365)
    except Exception:
        return JSONResponse(content={"error": "invalid start"}, status_code=400)
    if start_dt > end_dt:
        return JSONResponse(content={"error": "start must be <= end"}, status_code=400)

    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    sel = out[
        (out["ticker"] == ticker) &
        (out["date"] >= start_dt) &
        (out["date"] <= end_dt)
    ]
    if sel.empty:
        return []
    return to_json_records(sel)

@router.get("/prices/snapshot/last2")
def core30_prices_snapshot_last2():
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    g = out.sort_values(["ticker", "date"]).copy()
    g["prevClose"] = g.groupby("ticker")["Close"].shift(1)

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
        })
    return records

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

    records: List[Dict[str, Any]] = []
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

        row: Dict[str, Any] = {"ticker": tkr, "date": last_date.strftime("%Y-%m-%d")}
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


# ============== ここから：テクニカル（KPI4 + 評価4） ==============

def _safe_float(v: Any) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def _sma(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window=window, min_periods=window).mean()

def _rsi14(close: pd.Series, period: int = 14) -> pd.Series:
    diff = close.diff()
    gain = diff.clip(lower=0.0)
    loss = (-diff).clip(lower=0.0)
    avg_gain = _ema(gain, period)
    avg_loss = _ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def _macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9) -> pd.Series:
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd = ema_fast - ema_slow
    signal = _ema(macd, sig)
    hist = macd - signal
    return hist

def _bb_percent_b(close: pd.Series, window: int = 20, k: float = 2.0) -> pd.Series:
    ma = close.rolling(window=window, min_periods=window).mean()
    sd = close.rolling(window=window, min_periods=window).std(ddof=0)
    upper = ma + k * sd
    lower = ma - k * sd
    rng = (upper - lower).replace(0, np.nan)
    pb = (close - lower) / rng
    return pb

def _sma_dev_pct(close: pd.Series, window: int = 25) -> pd.Series:
    sma = close.rolling(window=window, min_periods=window).mean()
    dev = (close / sma - 1.0) * 100.0
    return dev

# ---------- 5段階ラベル ----------
def _label_from_score(score: int) -> str:
    if score >= 2:
        return "強い買い"
    if score == 1:
        return "買い"
    if score == 0:
        return "中立"
    if score == -1:
        return "売り"
    return "強い売り"

# ---------- Tech（RSI/%b/MACDHist/乖離） ----------
def _score_tech_row(row: pd.Series) -> int:
    score = 0

    rsi = _safe_float(row.get("rsi14"))
    if rsi is not None:
        if rsi < 30:
            score += 1
        elif rsi > 70:
            score -= 1

    pb = _safe_float(row.get("bb_percent_b"))
    if pb is not None:
        if pb < 0.05:
            score += 1
        elif pb > 0.95:
            score -= 1

    mh = _safe_float(row.get("macd_hist"))
    if mh is not None:
        eps = 1e-2
        if mh > eps:
            score += 1
        elif mh < -eps:
            score -= 1

    dev = _safe_float(row.get("sma25_dev_pct"))
    if dev is not None:
        if dev > 2.0:
            score += 1
        elif dev < -2.0:
            score -= 1

    if score >= 3:
        return 2
    if score >= 1:
        return 1
    if score <= -3:
        return -2
    if score <= -1:
        return -1
    return 0

# ---------- 移動平均（位置 + 直近クロス） ----------
def _score_ma_series(close: pd.Series) -> pd.Series:
    sma25 = _sma(close, 25)
    sma75 = _sma(close, 75)
    sma200 = _sma(close, 200)

    above_n = (close > sma25).astype(int) + (close > sma75).astype(int) + (close > sma200).astype(int)
    pos_score = above_n.copy()
    pos_score[above_n == 3] = 2
    pos_score[above_n == 2] = 1
    pos_score[above_n == 1] = -1
    pos_score[above_n == 0] = -2

    spread = sma25 - sma75
    prev = spread.shift(1)

    recent_window = 5
    gc_flag = (
        ((prev <= 0) & (spread > 0))
        .rolling(recent_window, min_periods=1)
        .max()
        .astype(bool)
    )
    dc_flag = (
        ((prev >= 0) & (spread < 0))
        .rolling(recent_window, min_periods=1)
        .max()
        .astype(bool)
    )
    cross_score = gc_flag.astype(int) - dc_flag.astype(int)

    score = (pos_score + cross_score).clip(-2, 2)
    return score

# ---------- 一目均衡表（9,26,52） ----------
def _ichimoku_components(close: pd.Series) -> Dict[str, pd.Series]:
    period_t = 9
    period_k = 26
    period_s = 52

    high_n = close.rolling(period_t, min_periods=period_t).max()
    low_n = close.rolling(period_t, min_periods=period_t).min()
    tenkan = (high_n + low_n) / 2.0

    high_k = close.rolling(period_k, min_periods=period_k).max()
    low_k = close.rolling(period_k, min_periods=period_k).min()
    kijun = (high_k + low_k) / 2.0

    span_a = (tenkan + kijun) / 2.0
    high_s = close.rolling(period_s, min_periods=period_s).max()
    low_s = close.rolling(period_s, min_periods=period_s).min()
    span_b = (high_s + low_s) / 2.0

    chikou = close.shift(-period_k)

    return {"tenkan": tenkan, "kijun": kijun, "span_a": span_a, "span_b": span_b, "chikou": chikou}

def _score_ichimoku(close: pd.Series) -> pd.Series:
    comp = _ichimoku_components(close)
    tenkan, kijun = comp["tenkan"], comp["kijun"]
    span_a, span_b = comp["span_a"], comp["span_b"]
    chikou = comp["chikou"]

    cloud_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
    cloud_bot = pd.concat([span_a, span_b], axis=1).min(axis=1)

    price_vs_cloud = pd.Series(0, index=close.index, dtype=float)
    price_vs_cloud[close > cloud_top] = 2
    price_vs_cloud[(close <= cloud_top) & (close >= cloud_bot)] = 0
    price_vs_cloud[close < cloud_bot] = -2

    tenkan_vs_kijun = pd.Series(0, index=close.index, dtype=float)
    tenkan_vs_kijun[tenkan > kijun] = 1
    tenkan_vs_kijun[tenkan < kijun] = -1

    lag_cmp = pd.Series(0, index=close.index, dtype=float)
    valid = comp["chikou"].notna() & close.notna()
    lag_cmp[valid & (comp["chikou"] > close)] = 1
    lag_cmp[valid & (comp["chikou"] < close)] = -1

    raw = price_vs_cloud + tenkan_vs_kijun + lag_cmp  # -4..+4
    score = pd.Series(0, index=close.index, dtype=int)
    score[raw >= 3] = 2
    score[(raw >= 1) & (raw <= 2)] = 1
    score[(raw <= -1) & (raw >= -2)] = -1
    score[raw <= -3] = -2
    return score

# ---------- meta マージ ----------
def _join_meta(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ticker に code / stock_name をマージ。無ければ素通し。"""
    try:
        meta = load_core30_meta()
        if not meta:
            return records
        mdf = pd.DataFrame(meta)

        if "ticker" not in mdf.columns and "code" in mdf.columns:
            def _to_ticker(x: Any) -> str:
                s = str(x).strip()
                return s if s.endswith(".T") else f"{s}.T"
            mdf["ticker"] = mdf["code"].map(_to_ticker)

        need = {"ticker", "code", "stock_name"}
        if not need.issubset(mdf.columns):
            return records

        mdf = mdf[list(need)].drop_duplicates("ticker")
        rdf = pd.DataFrame.from_records(records)
        out = rdf.merge(mdf, on="ticker", how="left")
        return out.to_dict(orient="records")
    except Exception:
        return records


# ====== 一覧用スナップショット（KPI4 + 評価4） ======
@router.get("/tech/snapshot")
def core30_tech_snapshot():
    """
    一覧用の直近スナップショット:
      - KPI: rsi14, macd_hist, bb_percent_b, sma25_dev_pct
      - 評価: tech_rating, ma_rating, ichimoku_rating, overall_rating（日本語）
    """
    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    g = out.sort_values(["ticker", "date"]).dropna(subset=["Close"]).copy()

    def _calc_grp(x: pd.DataFrame) -> pd.DataFrame:
        c = x["Close"]
        rsi = _rsi14(c)
        mh  = _macd_hist(c)
        pb  = _bb_percent_b(c)
        dev = _sma_dev_pct(c)
        ma_score   = _score_ma_series(c)
        ichi_score = _score_ichimoku(c)

        res = pd.DataFrame({
            "date": x["date"].values,
            "ticker": x["ticker"].values,
            "rsi14": rsi.values,
            "macd_hist": mh.values,
            "bb_percent_b": pb.values,
            "sma25_dev_pct": dev.values,
            "ma_score": ma_score.values,
            "ichi_score": ichi_score.values,
        })
        res["tech_score"] = res.apply(_score_tech_row, axis=1)
        overall = (res["tech_score"] + res["ma_score"] + res["ichi_score"]) / 3.0
        res["overall_score"] = np.rint(overall).astype(int).clip(-2, 2)
        return res

    # ★ FutureWarning 回避
    feat = g.groupby("ticker", as_index=False, group_keys=False).apply(
        _calc_grp, include_groups=False
    )
    if feat.empty:
        return []

    snap = feat.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).tail(1).copy()
    snap["date"] = snap["date"].dt.strftime("%Y-%m-%d")

    recs: List[Dict[str, Any]] = []
    for _, r in snap.iterrows():
        recs.append({
            "ticker": str(r["ticker"]),
            "date": r["date"],
            "rsi14": _safe_float(r["rsi14"]),
            "macd_hist": _safe_float(r["macd_hist"]),
            "bb_percent_b": _safe_float(r["bb_percent_b"]),
            "sma25_dev_pct": _safe_float(r["sma25_dev_pct"]),
            "tech_rating": _label_from_score(int(r["tech_score"])) if pd.notna(r["tech_score"]) else "中立",
            "ma_rating": _label_from_score(int(r["ma_score"])) if pd.notna(r["ma_score"]) else "中立",
            "ichimoku_rating": _label_from_score(int(r["ichi_score"])) if pd.notna(r["ichi_score"]) else "中立",
            "overall_rating": _label_from_score(int(r["overall_score"])) if pd.notna(r["overall_score"]) else "中立",
        })

    return _join_meta(recs)

# ====== 銘柄別ディテール（KPI 時系列） ======
@router.get("/tech/detail")
def core30_tech_detail(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    n: int = Query(default=60, ge=10, le=400, description="返す本数（直近）"),
):
    """
    直近 N 本の KPI: rsi14 / macd_hist / bb_percent_b / sma25_dev_pct
    """
    t = (ticker or "").strip()
    if not t:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    df = read_prices_1d_df()
    if df is None:
        return []
    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    g = out[out["ticker"] == t].sort_values("date").dropna(subset=["Close"]).copy()
    if g.empty:
        return []

    c = g["Close"]
    feat = pd.DataFrame({
        "date": g["date"],
        "ticker": g["ticker"],
        "rsi14": _rsi14(c),
        "macd_hist": _macd_hist(c),
        "bb_percent_b": _bb_percent_b(c),
        "sma25_dev_pct": _sma_dev_pct(c),
    }).dropna(subset=["rsi14","macd_hist","bb_percent_b","sma25_dev_pct"], how="all")

    if feat.empty:
        return []

    tail = feat.tail(int(n)).copy()
    tail["date"] = tail["date"].dt.strftime("%Y-%m-%d")

    recs: List[Dict[str, Any]] = []
    for _, r in tail.iterrows():
        recs.append({
            "ticker": str(r["ticker"]),
            "date": r["date"],
            "rsi14": _safe_float(r["rsi14"]),
            "macd_hist": _safe_float(r["macd_hist"]),
            "bb_percent_b": _safe_float(r["bb_percent_b"]),
            "sma25_dev_pct": _safe_float(r["sma25_dev_pct"]),
        })
    return recs

# ====== 後方互換（既存モバイル名） ======
@router.get("/tech/mobile/core")
def core30_tech_mobile_core_alias():
    return core30_tech_snapshot()

@router.get("/tech/mobile/detail")
def core30_tech_mobile_detail_alias(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    n: int = Query(default=60, ge=10, le=400, description="返す本数（直近）"),
):
    return core30_tech_detail(ticker=ticker, n=n)
