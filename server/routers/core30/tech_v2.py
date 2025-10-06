from __future__ import annotations
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

# ==== 既存ユーティリティ ====
from ...utils import (
    normalize_prices,
    read_prices_1d_df,
)

# interval 汎用リーダーがあれば使う（無ければ 1d のみ）
try:
    from ...utils import read_prices_df as _read_by_interval  # type: ignore
except Exception:
    _read_by_interval = None  # type: ignore

router = APIRouter()

# ================= 共通ユーティリティ =================

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

def _label_from_int(score: int) -> str:
    if score >= 2:  return "強い買い"
    if score == 1:  return "買い"
    if score == 0:  return "中立"
    if score == -1: return "売り"
    return "強い売り"

# ================= 指標（値） =================

def _rsi14(close: pd.Series, period: int = 14) -> pd.Series:
    diff = close.diff()
    gain = diff.clip(lower=0.0)
    loss = (-diff).clip(lower=0.0)
    avg_gain = _ema(gain, period)
    avg_loss = _ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))

def _macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9) -> pd.Series:
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd = ema_fast - ema_slow
    signal = _ema(macd, sig)
    return macd - signal

def _bb_percent_b(close: pd.Series, window: int = 20, k: float = 2.0) -> pd.Series:
    ma = close.rolling(window=window, min_periods=window).mean()
    sd = close.rolling(window=window, min_periods=window).std(ddof=0)
    upper = ma + k * sd
    lower = ma - k * sd
    rng = (upper - lower).replace(0, np.nan)
    return (close - lower) / rng

def _sma_dev_pct(close: pd.Series, window: int = 25) -> pd.Series:
    base = close.rolling(window=window, min_periods=window).mean()
    return (close / base - 1.0) * 100.0

def _roc(close: pd.Series, window: int = 12) -> pd.Series:
    return (close / close.shift(window) - 1.0) * 100.0

def _donchian_dist(close: pd.Series, window: int = 20) -> pd.DataFrame:
    hh = close.rolling(window, min_periods=window).max()
    ll = close.rolling(window, min_periods=window).min()
    return pd.DataFrame({"hh": hh, "ll": ll, "dist_up": close - hh, "dist_dn": close - ll})

def _true_range(high: pd.Series, low: pd.Series, prev_close: pd.Series) -> pd.Series:
    hl = high - low
    hp = (high - prev_close).abs()
    lp = (low - prev_close).abs()
    return pd.concat([hl, hp, lp], axis=1).max(axis=1)

def _atr(close: pd.Series, high: pd.Series, low: pd.Series, span: int = 14) -> pd.Series:
    prev = close.shift(1)
    tr = _true_range(high, low, prev)
    return tr.ewm(span=span, adjust=False).mean()

def _rv(close: pd.Series, window: int = 20) -> pd.Series:
    ret = close.pct_change()
    # FutureWarning 回避: inf を明示的に NaN に
    std = ret.rolling(window, min_periods=window).std(ddof=0)
    return std.replace([np.inf, -np.inf], np.nan) * 100.0

def _efficiency_ratio(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff().abs()
    num = (close - close.shift(window)).abs()
    den = delta.rolling(window, min_periods=window).sum()
    er = (num / den)
    return er.replace([np.inf, -np.inf], np.nan)

def _obv_slope(close: pd.Series, vol: pd.Series, lookback: int = 5) -> pd.Series:
    dir_ = close.diff().fillna(0.0)
    obv = (np.sign(dir_) * vol.fillna(0.0)).cumsum()
    return obv.diff().rolling(lookback, min_periods=lookback).mean()

def _volume_z(vol: pd.Series, window: int = 20) -> pd.Series:
    ma = vol.rolling(window, min_periods=window).mean()
    sd = vol.rolling(window, min_periods=window).std(ddof=0)
    z = (vol - ma) / sd
    return z.replace([np.inf, -np.inf], np.nan)

def _cmf(close: pd.Series, high: pd.Series, low: pd.Series, vol: pd.Series, window: int = 20) -> pd.Series:
    mfm = ((close - low) - (high - close)) / (high - low).replace(0, np.nan)
    mfv = mfm * vol
    num = mfv.rolling(window, min_periods=window).sum()
    den = vol.rolling(window, min_periods=window).sum()
    cmf = num / den
    return cmf.replace([np.inf, -np.inf], np.nan)

# ================= 既存: MA / 一目のスコア（-2..+2） =================

def _score_ma_series(close: pd.Series) -> pd.Series:
    s25 = _sma(close, 25)
    s75 = _sma(close, 75)
    s200 = _sma(close, 200)

    above_n = (close > s25).astype(int) + (close > s75).astype(int) + (close > s200).astype(int)
    pos = above_n.copy()
    pos[above_n == 3] = 2
    pos[above_n == 2] = 1
    pos[above_n == 1] = -1
    pos[above_n == 0] = -2

    spread = s25 - s75
    prev = spread.shift(1)

    recent = 5
    gc = (((prev <= 0) & (spread > 0)).rolling(recent, min_periods=1).max().astype(bool))
    dc = (((prev >= 0) & (spread < 0)).rolling(recent, min_periods=1).max().astype(bool))
    cross = gc.astype(int) - dc.astype(int)

    return (pos + cross).clip(-2, 2)

def _ichimoku_components(close: pd.Series) -> Dict[str, pd.Series]:
    pt, pk, ps = 9, 26, 52
    high_t = close.rolling(pt, min_periods=pt).max()
    low_t  = close.rolling(pt, min_periods=pt).min()
    tenkan = (high_t + low_t) / 2.0

    high_k = close.rolling(pk, min_periods=pk).max()
    low_k  = close.rolling(pk, min_periods=pk).min()
    kijun = (high_k + low_k) / 2.0

    span_a = (tenkan + kijun) / 2.0
    high_s = close.rolling(ps, min_periods=ps).max()
    low_s  = close.rolling(ps, min_periods=ps).min()
    span_b = (high_s + low_s) / 2.0

    chikou = close.shift(-pk)
    return {"tenkan": tenkan, "kijun": kijun, "span_a": span_a, "span_b": span_b, "chikou": chikou}

def _score_ichimoku(close: pd.Series) -> pd.Series:
    comp = _ichimoku_components(close)
    tenkan, kijun = comp["tenkan"], comp["kijun"]
    span_a, span_b = comp["span_a"], comp["span_b"]

    cloud_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
    cloud_bot = pd.concat([span_a, span_b], axis=1).min(axis=1)

    price_vs_cloud = pd.Series(0, index=close.index, dtype=float)
    price_vs_cloud[close > cloud_top] = 2
    price_vs_cloud[(close <= cloud_top) & (close >= cloud_bot)] = 0
    price_vs_cloud[close < cloud_bot] = -2

    tenkan_vs_kijun = pd.Series(0, index=close.index, dtype=float)
    tenkan_vs_kijun[tenkan > kijun] = 1
    tenkan_vs_kijun[tenkan < kijun] = -1

    lag = comp["chikou"]
    lag_cmp = pd.Series(0, index=close.index, dtype=float)
    valid = lag.notna() & close.notna()
    lag_cmp[valid & (lag > close)] = 1
    lag_cmp[valid & (lag < close)] = -1

    raw = price_vs_cloud + tenkan_vs_kijun + lag_cmp
    out = pd.Series(0, index=close.index, dtype=int)
    out[raw >= 3] = 2
    out[(raw >= 1) & (raw <= 2)] = 1
    out[(raw <= -1) & (raw >= -2)] = -1
    out[raw <= -3] = -2
    return out

# ================= スコアリング（-2..+2） =================

THRESH = {
    "rsi14": {"buy2": 20,  "buy1": 30,  "sell1": 70,  "sell2": 80},
    "macd_hist": {"eps": 1e-2},
    "percent_b": {"buy2": 0.02, "buy1": 0.05, "sell1": 0.95, "sell2": 0.98},
    "sma25_dev_pct": {"buy1": -2.0, "sell1": 2.0, "buy2": -5.0, "sell2": 5.0},
    "roc12": {"buy1": 2.0, "sell1": -2.0, "buy2": 5.0, "sell2": -5.0},
    "donchian": {"tol": 0.0},
    "obv_slope": {"eps": 0.0},
    "cmf20": {"buy1": 0.05, "sell1": -0.05, "buy2": 0.20, "sell2": -0.20},
}

def _score_rsi(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["rsi14"]
    if v <= t["buy2"]: return 2
    if v <  t["buy1"]: return 1
    if v >= t["sell2"]: return -2
    if v >  t["sell1"]: return -1
    return 0

def _score_macd_hist(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    e = THRESH["macd_hist"]["eps"]
    return 1 if v > e else (-1 if v < -e else 0)

def _score_percent_b(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["percent_b"]
    if v <= t["buy2"]: return 2
    if v <  t["buy1"]: return 1
    if v >= t["sell2"]: return -2
    if v >  t["sell1"]: return -1
    return 0

def _score_sma25_dev(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["sma25_dev_pct"]
    if v <= t["buy2"]: return 2
    if v <  t["buy1"]: return 1
    if v >= t["sell2"]: return -2
    if v >  t["sell1"]: return -1
    return 0

def _score_roc12(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["roc12"]
    if v >= t["buy2"]: return 2
    if v >  t["buy1"]: return 1
    if v <= t["sell2"]: return -2
    if v <  t["sell1"]: return -1
    return 0

def _score_donchian(dist_up: float | None, dist_dn: float | None) -> int:
    tol = THRESH["donchian"]["tol"]
    up_ok = (dist_up is not None and np.isfinite(dist_up) and dist_up > tol)
    dn_ok = (dist_dn is not None and np.isfinite(dist_dn) and dist_dn < -tol)
    if up_ok and not dn_ok: return 1
    if dn_ok and not up_ok: return -1
    return 0

def _score_obv_slope(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    e = THRESH["obv_slope"]["eps"]
    return 1 if v > e else (-1 if v < -e else 0)

def _score_cmf(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["cmf20"]
    if v >= t["buy2"]: return 2
    if v >  t["buy1"]: return 1
    if v <= t["sell2"]: return -2
    if v <  t["sell1"]: return -1
    return 0

# ================= 読み出し（interval対応） =================

def _read_prices(interval: str) -> Optional[pd.DataFrame]:
    iv = (interval or "1d").strip().lower()
    if _read_by_interval is not None:
        try:
            df = _read_by_interval(iv)  # type: ignore
            if df is not None:
                return df
        except Exception:
            pass
    if iv == "1d":
        return read_prices_1d_df()
    return None

# ================= 集約評価 =================

def _evaluate_latest(x: pd.DataFrame) -> Dict[str, Any]:
    """
    前提: x は単一 ticker の DataFrame（index: date 昇順）
    必須列: Close / High / Low （Volume は任意）
    """
    # index は date
    last_ts = x.index[-1]                      # ← 修正：date は index から取得
    date_str = pd.Timestamp(last_ts).strftime("%Y-%m-%d")

    c = x["Close"]; h = x["High"]; l = x["Low"]
    v = x["Volume"] if "Volume" in x.columns else pd.Series(index=x.index, dtype=float)

    # 値の算出
    rsi    = _rsi14(c)
    mh     = _macd_hist(c)
    pb     = _bb_percent_b(c)
    dev    = _sma_dev_pct(c)
    roc12  = _roc(c, 12)
    dch    = _donchian_dist(c, 20)
    atr14  = _atr(c, h, l, 14)
    atr_pct = (atr14 / c * 100.0).replace([np.inf, -np.inf], np.nan)
    rv20   = _rv(c, 20)
    er14   = _efficiency_ratio(c, 14)
    obv_k  = _obv_slope(c, v, 5)
    cmf20  = _cmf(c, h, l, v, 20)
    volz   = _volume_z(v, 20) if "Volume" in x.columns else pd.Series(index=x.index, dtype=float)

    # 既存スコア
    ma_score   = _score_ma_series(c)
    ichi_score = _score_ichimoku(c)

    # 方向投票
    votes: Dict[str, int] = {
        "rsi14":         _score_rsi(_safe_float(rsi.loc[last_ts])),
        "macd_hist":     _score_macd_hist(_safe_float(mh.loc[last_ts])),
        "percent_b":     _score_percent_b(_safe_float(pb.loc[last_ts])),
        "sma25_dev_pct": _score_sma25_dev(_safe_float(dev.loc[last_ts])),
        "roc12":         _score_roc12(_safe_float(roc12.loc[last_ts])),
        "donchian":      _score_donchian(_safe_float(dch["dist_up"].loc[last_ts]), _safe_float(dch["dist_dn"].loc[last_ts])),
        "obv_slope":     _score_obv_slope(_safe_float(obv_k.loc[last_ts])),
        "cmf20":         _score_cmf(_safe_float(cmf20.loc[last_ts])),
    }
    v_ma   = int(_safe_float(ma_score.loc[last_ts]) or 0)
    v_ichi = int(_safe_float(ichi_score.loc[last_ts]) or 0)

    vote_vals = list(votes.values()) + [v_ma, v_ichi]
    # NaN考慮の安全平均 → 丸め → クリップ
    mean = np.nanmean([float(s) for s in vote_vals]) if vote_vals else 0.0
    overall = int(np.clip(np.rint(mean), -2, 2))

    out: Dict[str, Any] = {
        "ticker": str(x["ticker"].iloc[-1]),
        "date":   date_str,                   # ← 修正：index から整形した日付
        "values": {
            "rsi14": _safe_float(rsi.loc[last_ts]),
            "macd_hist": _safe_float(mh.loc[last_ts]),
            "percent_b": _safe_float(pb.loc[last_ts]),
            "sma25_dev_pct": _safe_float(dev.loc[last_ts]),
            "roc12": _safe_float(roc12.loc[last_ts]),
            "donchian_dist_up": _safe_float(dch["dist_up"].loc[last_ts]),
            "donchian_dist_dn": _safe_float(dch["dist_dn"].loc[last_ts]),
            "atr14_pct": _safe_float(atr_pct.loc[last_ts]),
            "rv20": _safe_float(rv20.loc[last_ts]),
            "er14": _safe_float(er14.loc[last_ts]),
            "obv_slope": _safe_float(obv_k.loc[last_ts]),
            "cmf20": _safe_float(cmf20.loc[last_ts]),
            "vol_z20": _safe_float(volz.loc[last_ts]),
        },
        "votes": {
            **{k: {"score": int(v), "label": _label_from_int(int(v))} for k, v in votes.items()},
            "ma": {"score": v_ma, "label": _label_from_int(v_ma)},
            "ichimoku": {"score": v_ichi, "label": _label_from_int(v_ichi)},
        },
        "overall": {"score": overall, "label": _label_from_int(overall)},
    }
    return out

# ================= エンドポイント =================

@router.get("/tech/decision")
def core30_tech_decision(
    ticker: str = Query(default="", description="必須: ティッカー（例: 7203.T）"),
    interval: str = Query(default="1d", description="1d | 1wk | 1mo"),
):
    t = (ticker or "").strip()
    if not t:
        return JSONResponse(content={"error": "ticker is required"}, status_code=400)

    df = _read_prices(interval)
    if df is None:
        return JSONResponse(content={"error": f"interval '{interval}' is not supported on server"}, status_code=400)

    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    g = out[out["ticker"] == t].sort_values("date").dropna(subset=["Close"]).copy()
    if g.empty:
        return []

    # index を date に（以後は index から参照）
    g = g.set_index("date")
    return _evaluate_latest(g)

@router.get("/tech/decision/snapshot")
def core30_tech_decision_snapshot(
    interval: str = Query(default="1d", description="1d | 1wk | 1mo"),
):
    df = _read_prices(interval)
    if df is None:
        return JSONResponse(content={"error": f"interval '{interval}' is not supported on server"}, status_code=400)

    out = normalize_prices(df)
    if out is None or out.empty:
        return []

    res: List[Dict[str, Any]] = []
    for _, grp in out.sort_values(["ticker", "date"]).groupby("ticker", sort=False):
        grp = grp.dropna(subset=["Close"]).copy()
        if grp.empty:
            continue
        grp = grp.set_index("date")
        res.append(_evaluate_latest(grp))
    return res
