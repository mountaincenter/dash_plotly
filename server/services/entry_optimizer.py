"""寄付エントリー最適化検出

寄付後30分（9:00〜9:30）の値動きを走査し、テクニカル根拠があるエントリーポイントを検出。

検出条件:
  - RSI 70超→60以下に下落 (ショート向き反落開始)
  - RSI 30未満→40以上に上昇 (ロング向き反発開始)
  - MACDヒストグラムの符号転換
  - MACDヒストグラム3本連続縮小→クロス

複数根拠が重なるポイントを優先スコアリングし、最も根拠が多い1点を返す。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


def detect_optimal_entry(
    df: pd.DataFrame,
    rsi_series: pd.Series,
    macd_line: pd.Series,
    signal_line: pd.Series,
    histogram: pd.Series,
) -> Optional[Dict[str, Any]]:
    """寄付エントリー最適化ポイントを検出する。

    Args:
        df: 当日の5分足 DataFrame（date, OHLCV 列）
        rsi_series: RSI(9) の Series（df と同じインデックス）
        macd_line: MACD line の Series
        signal_line: Signal line の Series
        histogram: Histogram の Series

    Returns:
        検出できた場合:
        {"time": str, "price": float, "side": "long"|"short",
         "reasons": [str], "score": int}
        検出できなかった場合: None
    """
    if df.empty or len(df) < 4:
        return None

    dates = df["date"].values
    close = df["Close"].values.astype(float)
    rsi = rsi_series.values.astype(float) if len(rsi_series) > 0 else np.array([])
    hist = histogram.values.astype(float) if len(histogram) > 0 else np.array([])
    ml = macd_line.values.astype(float) if len(macd_line) > 0 else np.array([])
    sl = signal_line.values.astype(float) if len(signal_line) > 0 else np.array([])

    # 寄付後30分のインデックス範囲を特定
    opening_indices = _get_opening_range_indices(dates)
    if not opening_indices:
        return None

    # 各バーごとに根拠を収集
    candidates: List[Dict[str, Any]] = []

    for i in opening_indices:
        if i < 1 or i >= len(close):
            continue
        if np.isnan(close[i]):
            continue

        reasons: List[str] = []
        side_votes: Dict[str, int] = {"long": 0, "short": 0}

        time_str = _format_time(dates[i])
        price = float(close[i])

        # --- RSI 条件 ---
        if len(rsi) > i and len(rsi) > i - 1:
            rsi_now = rsi[i]
            rsi_prev = rsi[i - 1]

            if not (np.isnan(rsi_now) or np.isnan(rsi_prev)):
                # RSI 70超→60以下 (ショート)
                if rsi_prev > 70 and rsi_now <= 60:
                    reasons.append(f"RSI {rsi_prev:.0f}\u2192{rsi_now:.0f} 反落")
                    side_votes["short"] += 1

                # RSI 30未満→40以上 (ロング)
                if rsi_prev < 30 and rsi_now >= 40:
                    reasons.append(f"RSI {rsi_prev:.0f}\u2192{rsi_now:.0f} 反発")
                    side_votes["long"] += 1

        # --- MACD ヒストグラム符号転換 ---
        if len(hist) > i and len(hist) > i - 1:
            h_now = hist[i]
            h_prev = hist[i - 1]

            if not (np.isnan(h_now) or np.isnan(h_prev)):
                if h_prev <= 0 < h_now:
                    reasons.append("MACD hist マイナス\u2192プラス転換")
                    side_votes["long"] += 1
                elif h_prev >= 0 > h_now:
                    reasons.append("MACD hist プラス\u2192マイナス転換")
                    side_votes["short"] += 1

        # --- MACD ヒストグラム3本連続縮小 ---
        if len(hist) > i and i >= 3:
            abs_h = [abs(hist[j]) if not np.isnan(hist[j]) else np.nan
                     for j in range(i - 3, i + 1)]
            if all(not np.isnan(v) for v in abs_h):
                if abs_h[1] < abs_h[0] and abs_h[2] < abs_h[1] and abs_h[3] < abs_h[2]:
                    reasons.append(f"MACD hist 縮小3本")
                    # 方向は hist の符号で判定
                    if hist[i] > 0:
                        side_votes["short"] += 1  # 縮小=勢い低下
                    else:
                        side_votes["long"] += 1

        # --- MACD GC/DC ---
        if len(ml) > i and len(sl) > i and i >= 1:
            if not (np.isnan(ml[i]) or np.isnan(sl[i]) or np.isnan(ml[i-1]) or np.isnan(sl[i-1])):
                if ml[i-1] <= sl[i-1] and ml[i] > sl[i]:
                    reasons.append("MACD GC")
                    side_votes["long"] += 1
                elif ml[i-1] >= sl[i-1] and ml[i] < sl[i]:
                    reasons.append("MACD DC")
                    side_votes["short"] += 1

        if reasons:
            # side を投票で決定
            side = "long" if side_votes["long"] >= side_votes["short"] else "short"
            candidates.append({
                "time": time_str,
                "price": round(price, 2),
                "side": side,
                "reasons": reasons,
                "score": len(reasons),
            })

    if not candidates:
        return None

    # 最もスコアが高い候補を返す（同点なら最も早い時刻）
    candidates.sort(key=lambda c: (-c["score"], c["time"]))
    return candidates[0]


# ─── ヘルパー ──────────────────────────────────

def _get_opening_range_indices(dates: np.ndarray) -> List[int]:
    """寄付後30分（09:00〜09:30 JST）に該当するインデックスを返す。"""
    indices: List[int] = []
    for i, d in enumerate(dates):
        ts = pd.Timestamp(d)
        # 時刻成分がない場合（日足）はスキップ
        if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
            if "T" not in str(d) and " " not in str(d):
                continue
        # 09:00〜09:30
        if ts.hour == 9 and ts.minute <= 30:
            indices.append(i)
    return indices


def _format_time(t: Any) -> str:
    if isinstance(t, str):
        return t
    ts = pd.Timestamp(t)
    return ts.strftime("%Y-%m-%d %H:%M")
