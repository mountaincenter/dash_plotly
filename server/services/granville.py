"""グランビルの法則 8シグナル検出

MA（デフォルト25期間）と価格の関係から機械的にシグナルを検出する。

買い:
  buy_1 (GC突破)       MA上昇中 + 価格がMAを下→上抜け
  buy_2 (押し目買い)    MA上昇中 + MA付近まで下落後反発
  buy_3 (MA接触反発)    MA上昇中 + 安値がMA付近タッチ後上昇
  buy_4 (乖離反発)      MA下降中 + 大幅下方乖離 + 反発

売り:
  sell_1 (DC割込)       MA下降中 + 価格がMAを上→下抜け
  sell_2 (戻り売り)     MA下降中 + MA付近まで上昇後反落
  sell_3 (MA接触反落)   MA下降中 + 高値がMA付近タッチ後下落
  sell_4 (乖離反落)     MA上昇中 + 大幅上方乖離 + 反落
"""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from server.services.tech_utils_v2 import atr, sma


# ─── ラベル定義 ─────────────────────────────────
SIGNAL_LABELS: Dict[str, Dict[str, str]] = {
    "buy_1":  {"label": "GC突破",      "side": "buy"},
    "buy_2":  {"label": "押し目買い",   "side": "buy"},
    "buy_3":  {"label": "MA接触反発",   "side": "buy"},
    "buy_4":  {"label": "乖離反発",     "side": "buy"},
    "sell_1": {"label": "DC割込",       "side": "sell"},
    "sell_2": {"label": "戻り売り",     "side": "sell"},
    "sell_3": {"label": "MA接触反落",   "side": "sell"},
    "sell_4": {"label": "乖離反落",     "side": "sell"},
}


def detect_granville_signals(
    df: pd.DataFrame,
    ma_period: int = 25,
    slope_lookback: int = 3,
    proximity_atr_mult: float = 0.5,
    deviation_threshold_pct: float = 1.5,
) -> List[Dict[str, Any]]:
    """グランビルの法則に基づくシグナルを検出する。

    Args:
        df: OHLCV DataFrame。必須列: date, Open, High, Low, Close
        ma_period: 移動平均期間
        slope_lookback: MA傾き判定に使う直近バー数
        proximity_atr_mult: MA「付近」と判定するATR倍率
        deviation_threshold_pct: 「大幅乖離」と判定する%閾値

    Returns:
        シグナル辞書のリスト。各要素:
        {"time": str, "type": str, "label": str, "side": str,
         "price": float, "description": str}
    """
    if df.empty or len(df) < ma_period + slope_lookback + 2:
        return []

    close = df["Close"].values.astype(float)
    high = df["High"].values.astype(float)
    low = df["Low"].values.astype(float)
    dates = df["date"].values

    # MA / ATR を pandas で計算後 numpy に変換
    ma_s = sma(pd.Series(close), ma_period)
    atr_s = atr(pd.Series(close), pd.Series(high), pd.Series(low), span=14)
    ma = ma_s.values.astype(float)
    atr_vals = atr_s.values.astype(float)

    signals: List[Dict[str, Any]] = []

    for i in range(ma_period + slope_lookback, len(close)):
        if np.isnan(ma[i]) or np.isnan(atr_vals[i]):
            continue

        # MA の傾き判定
        ma_slope = ma[i] - ma[i - slope_lookback]
        ma_rising = ma_slope > 0
        ma_falling = ma_slope < 0

        # 近接判定
        prox = atr_vals[i] * proximity_atr_mult
        # 乖離率
        dev_pct = (close[i] - ma[i]) / ma[i] * 100.0 if ma[i] != 0 else 0.0

        time_str = _format_time(dates[i])
        price = float(close[i])

        # ─── 買いシグナル ────────────────────
        # buy_1: GC突破 — MA上昇中 + 前バーがMA以下 → 当バーがMA以上
        if ma_rising and close[i - 1] <= ma[i - 1] and close[i] > ma[i]:
            signals.append(_make_signal(
                "buy_1", time_str, price,
                f"MA{ma_period}上昇中、終値がMAを上抜け"
            ))

        # buy_2: 押し目買い — MA上昇中 + MA付近まで下落後反発
        if ma_rising and abs(low[i - 1] - ma[i - 1]) < prox and close[i] > close[i - 1]:
            # 前バーで下落接近 → 当バーで反発
            if close[i - 1] < close[i - 2] if i >= 2 else False:
                signals.append(_make_signal(
                    "buy_2", time_str, price,
                    f"MA{ma_period}上昇中、MA付近で反発"
                ))

        # buy_3: MA接触反発 — MA上昇中 + 安値がMA付近タッチ後上昇
        if ma_rising and abs(low[i] - ma[i]) < prox and close[i] > ma[i]:
            if close[i] > close[i - 1]:
                signals.append(_make_signal(
                    "buy_3", time_str, price,
                    f"MA{ma_period}上昇中、安値がMA付近タッチ後上昇"
                ))

        # buy_4: 乖離反発 — MA下降中 + 大幅下方乖離 + 反発
        if ma_falling and dev_pct < -deviation_threshold_pct:
            if close[i] > close[i - 1]:
                signals.append(_make_signal(
                    "buy_4", time_str, price,
                    f"MA{ma_period}下降中、{dev_pct:.1f}%下方乖離から反発"
                ))

        # ─── 売りシグナル ────────────────────
        # sell_1: DC割込 — MA下降中 + 前バーがMA以上 → 当バーがMA以下
        if ma_falling and close[i - 1] >= ma[i - 1] and close[i] < ma[i]:
            signals.append(_make_signal(
                "sell_1", time_str, price,
                f"MA{ma_period}下降中、終値がMAを下抜け"
            ))

        # sell_2: 戻り売り — MA下降中 + MA付近まで上昇後反落
        if ma_falling and abs(high[i - 1] - ma[i - 1]) < prox and close[i] < close[i - 1]:
            if close[i - 1] > close[i - 2] if i >= 2 else False:
                signals.append(_make_signal(
                    "sell_2", time_str, price,
                    f"MA{ma_period}下降中、MA付近で反落"
                ))

        # sell_3: MA接触反落 — MA下降中 + 高値がMA付近タッチ後下落
        if ma_falling and abs(high[i] - ma[i]) < prox and close[i] < ma[i]:
            if close[i] < close[i - 1]:
                signals.append(_make_signal(
                    "sell_3", time_str, price,
                    f"MA{ma_period}下降中、高値がMA付近タッチ後下落"
                ))

        # sell_4: 乖離反落 — MA上昇中 + 大幅上方乖離 + 反落
        if ma_rising and dev_pct > deviation_threshold_pct:
            if close[i] < close[i - 1]:
                signals.append(_make_signal(
                    "sell_4", time_str, price,
                    f"MA{ma_period}上昇中、{dev_pct:.1f}%上方乖離から反落"
                ))

    return signals


def compute_ma_series(df: pd.DataFrame, ma_period: int = 25) -> List[Dict[str, Any]]:
    """MA系列を返す（チャート表示用）。

    Returns:
        [{"time": str, "value": float}, ...]
    """
    if df.empty or len(df) < ma_period:
        return []

    close = df["Close"]
    ma_s = sma(close, ma_period)
    dates = df["date"].values

    result: List[Dict[str, Any]] = []
    for i in range(len(ma_s)):
        v = ma_s.iloc[i]
        if not np.isnan(v):
            result.append({
                "time": _format_time(dates[i]),
                "value": round(float(v), 2),
            })
    return result


# ─── ヘルパー ──────────────────────────────────

def _format_time(t: Any) -> str:
    """numpy datetime64 / pd.Timestamp / str → 文字列"""
    if isinstance(t, str):
        return t
    ts = pd.Timestamp(t)
    return ts.strftime("%Y-%m-%d %H:%M")


def _make_signal(
    sig_type: str, time_str: str, price: float, description: str
) -> Dict[str, Any]:
    meta = SIGNAL_LABELS[sig_type]
    return {
        "time": time_str,
        "type": sig_type,
        "label": meta["label"],
        "side": meta["side"],
        "price": round(price, 2),
        "description": description,
    }
