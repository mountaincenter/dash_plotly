from __future__ import annotations

from typing import Any, Dict, Optional
import numpy as np
import pandas as pd

# ========= 共通ユーティリティ =========

def safe_float(v: Any) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None

def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def sma(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window=window, min_periods=window).mean()

# ========= 既存 KPI（値） =========

def rsi14(close: pd.Series, period: int = 14) -> pd.Series:
    diff = close.diff()
    gain = diff.clip(lower=0.0)
    loss = (-diff).clip(lower=0.0)
    avg_gain = ema(gain, period)
    avg_loss = ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))

def macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9) -> pd.Series:
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd = ema_fast - ema_slow
    signal = ema(macd, sig)
    return macd - signal

def bb_percent_b(close: pd.Series, window: int = 20, k: float = 2.0) -> pd.Series:
    ma = close.rolling(window=window, min_periods=window).mean()
    sd = close.rolling(window=window, min_periods=window).std(ddof=0)
    upper = ma + k * sd
    lower = ma - k * sd
    rng = (upper - lower).replace(0, np.nan)
    return (close - lower) / rng

def sma_dev_pct(close: pd.Series, window: int = 25) -> pd.Series:
    base = close.rolling(window=window, min_periods=window).mean()
    return (close / base - 1.0) * 100.0

# ========= 追加 KPI（値） =========

def roc(close: pd.Series, window: int = 12) -> pd.Series:
    return (close / close.shift(window) - 1.0) * 100.0

def donchian_dist(close: pd.Series, window: int = 20) -> pd.DataFrame:
    hh = close.rolling(window, min_periods=window).max()
    ll = close.rolling(window, min_periods=window).min()
    return pd.DataFrame({"hh": hh, "ll": ll, "dist_up": close - hh, "dist_dn": close - ll})

def _true_range(high: pd.Series, low: pd.Series, prev_close: pd.Series) -> pd.Series:
    hl = high - low
    hp = (high - prev_close).abs()
    lp = (low - prev_close).abs()
    return pd.concat([hl, hp, lp], axis=1).max(axis=1)

def atr(close: pd.Series, high: pd.Series, low: pd.Series, span: int = 14) -> pd.Series:
    prev = close.shift(1)
    tr = _true_range(high, low, prev)
    return tr.ewm(span=span, adjust=False).mean()

def rv(close: pd.Series, window: int = 20) -> pd.Series:
    ret = close.pct_change()
    return ret.rolling(window, min_periods=window).std(ddof=0) * 100.0

def efficiency_ratio(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff().abs()
    num = (close - close.shift(window)).abs()
    den = delta.rolling(window, min_periods=window).sum()
    with pd.option_context("mode.use_inf_as_na", True):
        er = (num / den).where(den > 0)
    return er

def obv_slope(close: pd.Series, vol: pd.Series, lookback: int = 5) -> pd.Series:
    dir_ = close.diff().fillna(0.0)
    obv = (np.sign(dir_) * vol.fillna(0.0)).cumsum()
    return obv.diff().rolling(lookback, min_periods=lookback).mean()

def volume_z(vol: pd.Series, window: int = 20) -> pd.Series:
    ma = vol.rolling(window, min_periods=window).mean()
    sd = vol.rolling(window, min_periods=window).std(ddof=0)
    with pd.option_context("mode.use_inf_as_na", True):
        return ((vol - ma) / sd).where(sd > 0)

def cmf(close: pd.Series, high: pd.Series, low: pd.Series, vol: pd.Series, window: int = 20) -> pd.Series:
    mfm = ((close - low) - (high - close)) / (high - low).replace(0, np.nan)
    mfv = mfm * vol
    return (mfv.rolling(window, min_periods=window).sum() /
            vol.rolling(window, min_periods=window).sum())

# ========= MA / Ichimoku（既存同等のロジック） =========

def score_ma_series(close: pd.Series) -> pd.Series:
    sma25 = sma(close, 25)
    sma75 = sma(close, 75)
    sma200 = sma(close, 200)

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
    return (pos_score + cross_score).clip(-2, 2)

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

def score_ichimoku(close: pd.Series) -> pd.Series:
    comp = _ichimoku_components(close)
    span_a, span_b = comp["span_a"], comp["span_b"]
    cloud_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
    cloud_bot = pd.concat([span_a, span_b], axis=1).min(axis=1)
    tenkan, kijun = comp["tenkan"], comp["kijun"]

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

    raw = price_vs_cloud + tenkan_vs_kijun + lag_cmp
    score = pd.Series(0, index=close.index, dtype=int)
    score[raw >= 3] = 2
    score[(raw >= 1) & (raw <= 2)] = 1
    score[(raw <= -1) & (raw >= -2)] = -1
    score[raw <= -3] = -2
    return score

# ========= ラベル/スコア（v2 しきい値） =========

def label_from_score(score: int) -> str:
    if score >= 2:  return "強い買い"
    if score == 1:  return "買い"
    if score == 0:  return "中立"
    if score == -1: return "売り"
    return "強い売り"

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

def score_rsi(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["rsi14"]
    if v <= t["buy2"]: return 2
    if v <  t["buy1"]: return 1
    if v >= t["sell2"]: return -2
    if v >  t["sell1"]: return -1
    return 0

def score_macd_hist(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    e = THRESH["macd_hist"]["eps"]
    return 1 if v > e else (-1 if v < -e else 0)

def score_percent_b(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["percent_b"]
    if v <= t["buy2"]: return 2
    if v <  t["buy1"]: return 1
    if v >= t["sell2"]: return -2
    if v >  t["sell1"]: return -1
    return 0

def score_sma25_dev(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["sma25_dev_pct"]
    if v <= t["buy2"]: return 2
    if v <  t["buy1"]: return 1
    if v >= t["sell2"]: return -2
    if v >  t["sell1"]: return -1
    return 0

def score_roc12(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["roc12"]
    if v >= t["buy2"]: return 2
    if v >  t["buy1"]: return 1
    if v <= t["sell2"]: return -2
    if v <  t["sell1"]: return -1
    return 0

def score_donchian(dist_up: float | None, dist_dn: float | None) -> int:
    tol = THRESH["donchian"]["tol"]
    up_ok = (dist_up is not None and np.isfinite(dist_up) and dist_up > tol)
    dn_ok = (dist_dn is not None and np.isfinite(dist_dn) and dist_dn < -tol)
    if up_ok and not dn_ok: return 1
    if dn_ok and not up_ok: return -1
    return 0

def score_obv_slope(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    e = THRESH["obv_slope"]["eps"]
    return 1 if v > e else (-1 if v < -e else 0)

def score_cmf(v: float | None) -> int:
    if v is None or not np.isfinite(v): return 0
    t = THRESH["cmf20"]
    if v >= t["buy2"]: return 2
    if v >  t["buy1"]: return 1
    if v <= t["sell2"]: return -2
    if v <  t["sell1"]: return -1
    return 0
