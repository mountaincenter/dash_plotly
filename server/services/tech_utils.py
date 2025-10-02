from __future__ import annotations
from typing import Any, Dict, Optional
import numpy as np
import pandas as pd

# ============== 共通ユーティリティ ==============
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

# ============== KPI計算 ==============
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
    m = close.rolling(window=window, min_periods=window).mean()
    return (close / m - 1.0) * 100.0

# ============== ラベル/スコア ==============
def label_from_score(score: int) -> str:
    if score >= 2: return "強い買い"
    if score == 1: return "買い"
    if score == 0: return "中立"
    if score == -1: return "売り"
    return "強い売り"

def score_tech_row(row: pd.Series) -> int:
    from .tech_utils import safe_float  # 循環防止のためローカル参照でもOK
    score = 0

    rsi = safe_float(row.get("rsi14"))
    if rsi is not None:
        if rsi < 30: score += 1
        elif rsi > 70: score -= 1

    pb = safe_float(row.get("bb_percent_b"))
    if pb is not None:
        if pb < 0.05: score += 1
        elif pb > 0.95: score -= 1

    mh = safe_float(row.get("macd_hist"))
    if mh is not None:
        eps = 1e-2
        if mh > eps: score += 1
        elif mh < -eps: score -= 1

    dev = safe_float(row.get("sma25_dev_pct"))
    if dev is not None:
        if dev > 2.0: score += 1
        elif dev < -2.0: score -= 1

    if score >= 3: return 2
    if score >= 1: return 1
    if score <= -3: return -2
    if score <= -1: return -1
    return 0

def score_ma_series(close: pd.Series) -> pd.Series:
    s25 = sma(close, 25)
    s75 = sma(close, 75)
    s200 = sma(close, 200)

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

def ichimoku_components(close: pd.Series) -> Dict[str, pd.Series]:
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

def score_ichimoku(close: pd.Series) -> pd.Series:
    comp = ichimoku_components(close)
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
    valid = chikou.notna() & close.notna()
    lag_cmp[valid & (chikou > close)] = 1
    lag_cmp[valid & (chikou < close)] = -1

    raw = price_vs_cloud + tenkan_vs_kijun + lag_cmp
    out = pd.Series(0, index=close.index, dtype=int)
    out[raw >= 3] = 2
    out[(raw >= 1) & (raw <= 2)] = 1
    out[(raw <= -1) & (raw >= -2)] = -1
    out[raw <= -3] = -2
    return out
