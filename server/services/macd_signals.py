"""MACD計算 + シグナル検出

楽天証券 MARKETSPEED 準拠:
  - EMA_fast: span=5
  - EMA_slow: span=20
  - MACD = EMA_fast - EMA_slow
  - Signal = SMA(MACD, 9)  ← EMAではなくSMA
  - Histogram = MACD - Signal

RSI (EMAベース Wilder's smoothing):
  - 期間: 9

シグナル:
  - macd_golden: MACD line が Signal line を上抜け
  - macd_dead:   MACD line が Signal line を下抜け
  - macd_hist_shrink: abs(hist) が3本連続縮小
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from server.services.tech_utils_v2 import ema, sma


# ─── MACD ────────────────────────────────────

def compute_macd(
    close: pd.Series,
    fast: int = 5,
    slow: int = 20,
    signal_period: int = 9,
) -> Dict[str, pd.Series]:
    """楽天MARKETSPEED互換のMACD計算。

    Returns:
        {"macd_line": Series, "signal_line": Series, "histogram": Series}
    """
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = sma(macd_line, signal_period)  # SMA（楽天式）
    histogram = macd_line - signal_line
    return {
        "macd_line": macd_line,
        "signal_line": signal_line,
        "histogram": histogram,
    }


# ─── RSI (Wilder's EMA, period=9) ───────────

def compute_rsi(close: pd.Series, period: int = 9) -> pd.Series:
    """楽天MARKETSPEED互換のRSI（EMAベース）。"""
    diff = close.diff()
    gain = diff.clip(lower=0.0)
    loss = (-diff).clip(lower=0.0)
    avg_gain = ema(gain, period)
    avg_loss = ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


# ─── MACD シグナル検出 ──────────────────────

MACD_SIGNAL_LABELS: Dict[str, Dict[str, str]] = {
    "macd_golden":      {"label": "MACD GC",         "side": "buy"},
    "macd_dead":        {"label": "MACD DC",         "side": "sell"},
    "macd_hist_shrink": {"label": "ヒストグラム縮小", "side": "neutral"},
}


def detect_macd_signals(
    df: pd.DataFrame,
    fast: int = 5,
    slow: int = 20,
    signal_period: int = 9,
) -> Dict[str, Any]:
    """MACD関連データ + シグナルを返す。

    Args:
        df: OHLCV DataFrame（date, Close 必須）

    Returns:
        {
          "macd_line": [{"time": str, "value": float}, ...],
          "signal_line": [...],
          "histogram": [...],
          "signals": [{"time": str, "type": str, ...}, ...],
        }
    """
    if df.empty or len(df) < slow + signal_period:
        return {
            "macd_line": [], "signal_line": [], "histogram": [],
            "signals": [],
        }

    close = df["Close"]
    dates = df["date"].values

    macd_data = compute_macd(close, fast, slow, signal_period)
    ml = macd_data["macd_line"].values.astype(float)
    sl = macd_data["signal_line"].values.astype(float)
    hist = macd_data["histogram"].values.astype(float)

    # 系列データ
    macd_line_out = _series_to_records(dates, ml)
    signal_line_out = _series_to_records(dates, sl)
    histogram_out = _series_to_records(dates, hist)

    # シグナル検出
    signals: List[Dict[str, Any]] = []

    for i in range(1, len(ml)):
        if np.isnan(ml[i]) or np.isnan(sl[i]) or np.isnan(ml[i - 1]) or np.isnan(sl[i - 1]):
            continue

        time_str = _format_time(dates[i])
        price_raw = close.iloc[i]
        if np.isnan(price_raw):
            continue
        price = float(price_raw)

        # macd_golden: MACD が Signal を上抜け
        if ml[i - 1] <= sl[i - 1] and ml[i] > sl[i]:
            signals.append({
                "time": time_str,
                "type": "macd_golden",
                "label": MACD_SIGNAL_LABELS["macd_golden"]["label"],
                "side": "buy",
                "price": round(price, 2),
                "description": "MACD lineがSignal lineを上抜け",
            })

        # macd_dead: MACD が Signal を下抜け
        if ml[i - 1] >= sl[i - 1] and ml[i] < sl[i]:
            signals.append({
                "time": time_str,
                "type": "macd_dead",
                "label": MACD_SIGNAL_LABELS["macd_dead"]["label"],
                "side": "sell",
                "price": round(price, 2),
                "description": "MACD lineがSignal lineを下抜け",
            })

        # macd_hist_shrink: abs(hist) が3本連続縮小
        if i >= 3:
            abs_h = [abs(hist[j]) if not np.isnan(hist[j]) else np.nan for j in range(i - 3, i + 1)]
            if all(not np.isnan(v) for v in abs_h):
                if abs_h[1] < abs_h[0] and abs_h[2] < abs_h[1] and abs_h[3] < abs_h[2]:
                    signals.append({
                        "time": time_str,
                        "type": "macd_hist_shrink",
                        "label": MACD_SIGNAL_LABELS["macd_hist_shrink"]["label"],
                        "side": "neutral",
                        "price": round(price, 2),
                        "description": f"ヒストグラムが3本連続縮小（{abs_h[0]:.2f}→{abs_h[3]:.2f}）",
                    })

    return {
        "macd_line": macd_line_out,
        "signal_line": signal_line_out,
        "histogram": histogram_out,
        "signals": signals,
    }


# ─── ヘルパー ──────────────────────────────────

def _series_to_records(
    dates: np.ndarray, values: np.ndarray
) -> List[Dict[str, Any]]:
    """(dates, values) → [{"time": str, "value": float}, ...]"""
    out: List[Dict[str, Any]] = []
    for i in range(len(values)):
        if not np.isnan(values[i]):
            out.append({
                "time": _format_time(dates[i]),
                "value": round(float(values[i]), 4),
            })
    return out


def _format_time(t: Any) -> str:
    if isinstance(t, str):
        return t
    ts = pd.Timestamp(t)
    return ts.strftime("%Y-%m-%d %H:%M")
