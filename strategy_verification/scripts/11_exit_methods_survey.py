#!/usr/bin/env python3
"""
11_exit_methods_survey.py
=========================
Chapter 5-2: 出口戦略の体系的検証

歴代投資家が提唱した20の出口手法を体系的に検証し、
B1-B4各シグナルに最適な出口戦略を理論的根拠を持って結論づける。

入力:
  - strategy_verification/data/processed/trades_cleaned.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet

出力:
  - strategy_verification/chapters/05-2_exit_methods_survey/report.html
"""
from __future__ import annotations

import json
import time
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "05-2_exit_methods_survey"

RULES = ["B1", "B2", "B3", "B4"]
MAX_HOLD = 60
PROPOSED_SLS: dict[str, float] = {"B1": 3.0, "B2": 3.0, "B3": 2.5, "B4": 999.0}

# Ch5結果（ベースライン引用用）
CH5_BEST: dict[str, tuple[str, float]] = {
    "B1": ("fixed_60d", 60),
    "B2": ("min_hold_30d", 30),
    "B3": ("fixed_60d", 60),
    "B4": ("fixed_13d", 13),
}


# ============================================================
# テクニカル指標計算
# ============================================================

def _rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
    """RSI(period)を計算。最初のperiod日はNaN。"""
    n = len(closes)
    rsi = np.full(n, np.nan)
    if n < period + 1:
        return rsi
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains[:period].mean()
    avg_loss = losses[:period].mean()
    if avg_loss == 0:
        rsi[period] = 100.0
    else:
        rsi[period] = 100.0 - 100.0 / (1 + avg_gain / avg_loss)
    for i in range(period, n - 1):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi[i + 1] = 100.0
        else:
            rsi[i + 1] = 100.0 - 100.0 / (1 + avg_gain / avg_loss)
    return rsi


def _stochastic(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                k_period: int = 14, d_period: int = 3) -> tuple[np.ndarray, np.ndarray]:
    """%K, %D を計算。"""
    n = len(closes)
    pct_k = np.full(n, np.nan)
    for i in range(k_period - 1, n):
        hh = highs[i - k_period + 1:i + 1].max()
        ll = lows[i - k_period + 1:i + 1].min()
        if hh == ll:
            pct_k[i] = 50.0
        else:
            pct_k[i] = (closes[i] - ll) / (hh - ll) * 100
    pct_d = np.full(n, np.nan)
    for i in range(k_period - 1 + d_period - 1, n):
        pct_d[i] = np.nanmean(pct_k[i - d_period + 1:i + 1])
    return pct_k, pct_d


def _macd(closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9
           ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """MACD line, signal line, histogram。"""
    n = len(closes)

    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        out = np.full(n, np.nan)
        if n < period:
            return out
        out[period - 1] = data[:period].mean()
        k = 2.0 / (period + 1)
        for i in range(period, n):
            out[i] = data[i] * k + out[i - 1] * (1 - k)
        return out

    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = ema_fast - ema_slow
    sig_line = np.full(n, np.nan)
    start = slow - 1
    valid = macd_line[start:]
    if len(valid) >= signal:
        sig_line[start + signal - 1] = np.nanmean(valid[:signal])
        k = 2.0 / (signal + 1)
        for i in range(start + signal, n):
            if not np.isnan(macd_line[i]) and not np.isnan(sig_line[i - 1]):
                sig_line[i] = macd_line[i] * k + sig_line[i - 1] * (1 - k)
    hist = macd_line - sig_line
    return macd_line, sig_line, hist


def _bollinger(closes: np.ndarray, period: int = 20, num_std: float = 2.0
                ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """SMA, Upper, Lower band。"""
    n = len(closes)
    sma = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    for i in range(period - 1, n):
        window = closes[i - period + 1:i + 1]
        m = window.mean()
        s = window.std(ddof=0)
        sma[i] = m
        upper[i] = m + num_std * s
        lower[i] = m - num_std * s
    return sma, upper, lower


def _atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
         period: int = 14) -> np.ndarray:
    """ATR(period)。"""
    n = len(closes)
    atr = np.full(n, np.nan)
    if n < 2:
        return atr
    tr = np.zeros(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
    if n >= period:
        atr[period - 1] = tr[:period].mean()
        for i in range(period, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def _parabolic_sar(highs: np.ndarray, lows: np.ndarray,
                    af_init: float = 0.02, af_max: float = 0.2) -> np.ndarray:
    """Parabolic SAR。LONGポジション前提でSAR値を返す。"""
    n = len(highs)
    sar = np.full(n, np.nan)
    if n < 2:
        return sar
    is_long = True
    af = af_init
    ep = highs[0]
    sar[0] = lows[0]

    for i in range(1, n):
        prev_sar = sar[i - 1] if not np.isnan(sar[i - 1]) else lows[i - 1]
        sar_val = prev_sar + af * (ep - prev_sar)

        if is_long:
            sar_val = min(sar_val, lows[i - 1])
            if i >= 2 and not np.isnan(lows[i - 2]):
                sar_val = min(sar_val, lows[i - 2])
            if lows[i] < sar_val:
                is_long = False
                sar_val = ep
                ep = lows[i]
                af = af_init
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + af_init, af_max)
        else:
            sar_val = max(sar_val, highs[i - 1])
            if i >= 2 and not np.isnan(highs[i - 2]):
                sar_val = max(sar_val, highs[i - 2])
            if highs[i] > sar_val:
                is_long = True
                sar_val = ep
                ep = highs[i]
                af = af_init
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + af_init, af_max)

        sar[i] = sar_val
    return sar


def _donchian_low(lows: np.ndarray, period: int) -> np.ndarray:
    """N日ドンチャン安値チャネル。"""
    n = len(lows)
    out = np.full(n, np.nan)
    for i in range(period - 1, n):
        out[i] = lows[i - period + 1:i + 1].min()
    return out


def _donchian_high(highs: np.ndarray, period: int) -> np.ndarray:
    """N日ドンチャン高値チャネル。"""
    n = len(highs)
    out = np.full(n, np.nan)
    for i in range(period - 1, n):
        out[i] = highs[i - period + 1:i + 1].max()
    return out


def _sma(closes: np.ndarray, period: int) -> np.ndarray:
    """Simple Moving Average。"""
    n = len(closes)
    out = np.full(n, np.nan)
    for i in range(period - 1, n):
        out[i] = closes[i - period + 1:i + 1].mean()
    return out


# ============================================================
# Price Lookup v2 — テクニカル指標付き
# ============================================================

def build_price_lookup_v2(prices: pd.DataFrame) -> dict:
    """ticker → numpy配列辞書 + テクニカル指標。"""
    lookup = {}
    for ticker, grp in prices.groupby("ticker"):
        grp = grp.sort_values("date").dropna(subset=["Close"])
        if len(grp) == 0:
            continue
        dates = grp["date"].values
        opens = grp["Open"].values.astype(np.float64)
        highs = grp["High"].values.astype(np.float64)
        lows = grp["Low"].values.astype(np.float64)
        closes = grp["Close"].values.astype(np.float64)

        atr14 = _atr(highs, lows, closes, 14)
        sma20 = _sma(closes, 20)

        lookup[ticker] = {
            "dates": dates,
            "opens": opens,
            "highs": highs,
            "lows": lows,
            "closes": closes,
            "rsi14": _rsi(closes, 14),
            "stoch_k": _stochastic(highs, lows, closes, 14, 3)[0],
            "stoch_d": _stochastic(highs, lows, closes, 14, 3)[1],
            "macd_hist": _macd(closes, 12, 26, 9)[2],
            "bb_upper": _bollinger(closes, 20, 2.0)[1],
            "atr14": atr14,
            "atr14_ma20": _sma(atr14, 20),
            "sar": _parabolic_sar(highs, lows, 0.02, 0.2),
            "donchian_low_10": _donchian_low(lows, 10),
            "donchian_low_20": _donchian_low(lows, 20),
            "donchian_low_40": _donchian_low(lows, 40),
            "donchian_high_20": _donchian_high(highs, 20),
            "donchian_high_60": _donchian_high(highs, 60),
            "sma20": sma20,
        }
    return lookup


# ============================================================
# Simulation Engine v2 — 全手法対応
# ============================================================

def simulate_trade_v2(
    pl: dict,
    entry_date: np.datetime64,
    entry_price: float,
    method: str,
    param: float,
    sl_pct: float = 999.0,
) -> dict | None:
    """
    単一トレードを再シミュレーション。

    method / param の組み合わせ:
      Baseline:
        "current"         — 既存ret_pct使用（呼ばない）
        "ch5_fixed_N"     — Ch5結論の固定N日 (param=N)
        "ch5_min_hold_N"  — Ch5結論の最低保有N日 (param=N)
      A. Target:
        "target_pct"      — 固定%利確 (param=target%)
        "target_r"        — R倍数利確 (param=R倍数, R=SL幅)
        "target_atr"      — ATR目標 (param=ATR倍数)
      B. Trailing:
        "trail_pct"       — 固定%トレーリング (param=X%)
        "chandelier"      — Chandelier Exit (param=ATR倍数)
        "donchian"        — ドンチャン安値割れ (param=N日)
        "sar"             — パラボリックSAR (param=不使用)
      C. MA:
        "sma20_break"     — SMA20割れ (param=不使用)
        "dead_cross"      — SMA5<SMA20デッドクロス (param=不使用)
        "ma_envelope"     — MAエンベロープ到達 (param=乖離%)
      D. Time:
        "fixed_N"         — 固定N日保有 (param=N)
        "time_stop"       — N日間含み損なら強制exit (param=N)
        "weekday_month"   — 金曜大引け=1, 月末大引け=2 (param=1or2)
      E. Oscillator:
        "rsi_70"          — RSI(14)>70 (param=不使用)
        "stoch_cross"     — %K>80&%K<%D (param=不使用)
        "macd_hist_rev"   — MACDヒストグラム正→負 (param=不使用)
      F. Volatility:
        "bb_upper"        — ボリンジャー+2σタッチ (param=不使用)
        "atr_spike"       — ATR>20日平均×倍率 (param=倍率)
      G. Structure:
        "n_day_high"      — N日高値タッチ (param=N)
        "fib_161"         — フィボナッチ161.8% (param=不使用)
      H. Hybrid:
        "trail_max60"     — Trail(param%) + 最大60日
        "partial_profit"  — 半分+10%利確、残りTrail(param%)
        "target_then_trail" — +10%後Trail(param%)開始
    """
    dates = pl["dates"]
    opens = pl["opens"]
    highs = pl["highs"]
    lows = pl["lows"]
    closes = pl["closes"]

    entry_mask = dates == entry_date
    if not entry_mask.any():
        return None
    entry_idx = int(np.where(entry_mask)[0][0])

    sl_price = entry_price * (1 - sl_pct / 100) if sl_pct < 900 else 0.0
    max_high = entry_price
    exit_price = None
    exit_day = 0
    exit_reason = ""

    # method-specific state
    partial_exited = False  # H2用
    trailing_active = False  # H3用
    trail_high = entry_price  # H3用
    prev_macd_hist = None  # E3用

    for d in range(MAX_HOLD):
        ci = entry_idx + d
        if ci >= len(dates):
            break

        c_open = opens[ci]
        c_high = highs[ci]
        c_low = lows[ci]
        c_close = closes[ci]

        # d=0: エントリー日。SLのみチェック
        if d == 0:
            if sl_pct < 900 and c_low <= sl_price:
                exit_price = sl_price
                exit_day = d
                exit_reason = "SL"
                break
            max_high = max(max_high, c_high)
            # MACD hist初期化
            if method == "macd_hist_rev" and not np.isnan(pl["macd_hist"][ci]):
                prev_macd_hist = pl["macd_hist"][ci]
            continue

        # SLチェック（全手法共通）
        if sl_pct < 900 and c_low <= sl_price:
            exit_price = sl_price
            exit_day = d
            exit_reason = "SL"
            break

        max_high = max(max_high, c_high)

        # --- A. Target型 ---
        if method == "target_pct":
            target = entry_price * (1 + param / 100)
            if c_high >= target:
                exit_price = target
                exit_day = d
                exit_reason = f"target_{param}pct"
                break

        elif method == "target_r":
            if sl_pct < 900:
                r_value = entry_price * sl_pct / 100
                target = entry_price + r_value * param
                if c_high >= target:
                    exit_price = target
                    exit_day = d
                    exit_reason = f"target_{param}R"
                    break

        elif method == "target_atr":
            atr_at_entry = pl["atr14"][entry_idx]
            if not np.isnan(atr_at_entry):
                target = entry_price + atr_at_entry * param
                if c_high >= target:
                    exit_price = target
                    exit_day = d
                    exit_reason = f"target_{param}ATR"
                    break

        # --- B. Trailing型 ---
        elif method == "trail_pct":
            trail_trigger = max_high * (1 - param / 100)
            if c_close <= trail_trigger and d >= 1:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = f"trail_{param}pct"
                break

        elif method == "chandelier":
            atr_val = pl["atr14"][ci]
            if not np.isnan(atr_val):
                chandelier_stop = max_high - param * atr_val
                if c_close <= chandelier_stop:
                    if ci + 1 < len(dates):
                        exit_price = opens[ci + 1]
                        exit_day = d + 1
                    else:
                        exit_price = c_close
                        exit_day = d
                    exit_reason = f"chandelier_{param}ATR"
                    break

        elif method == "donchian":
            n = int(param)
            key = f"donchian_low_{n}"
            if key in pl:
                don_low = pl[key][ci]
            else:
                # fallback: 動的計算
                start = max(0, ci - n + 1)
                don_low = lows[start:ci + 1].min()
            if not np.isnan(don_low) and c_close < don_low:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = f"donchian_{n}d"
                break

        elif method == "sar":
            sar_val = pl["sar"][ci]
            if not np.isnan(sar_val) and c_close < sar_val:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = "sar"
                break

        # --- C. MA型 ---
        elif method == "sma20_break":
            sma_val = pl["sma20"][ci]
            if not np.isnan(sma_val) and c_close < sma_val:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = "sma20_break"
                break

        elif method == "dead_cross":
            # SMA5 < SMA20 デッドクロス
            if ci >= 4:
                sma5_now = closes[ci - 4:ci + 1].mean()
                sma5_prev = closes[ci - 5:ci].mean() if ci >= 5 else sma5_now + 1
                sma20_now = pl["sma20"][ci]
                sma20_prev = pl["sma20"][ci - 1] if ci >= 1 and not np.isnan(pl["sma20"][ci - 1]) else np.nan
                if (not np.isnan(sma20_now) and not np.isnan(sma20_prev)
                        and sma5_prev >= sma20_prev and sma5_now < sma20_now):
                    if ci + 1 < len(dates):
                        exit_price = opens[ci + 1]
                        exit_day = d + 1
                    else:
                        exit_price = c_close
                        exit_day = d
                    exit_reason = "dead_cross"
                    break

        elif method == "ma_envelope":
            sma_val = pl["sma20"][ci]
            if not np.isnan(sma_val):
                envelope_upper = sma_val * (1 + param / 100)
                if c_high >= envelope_upper:
                    exit_price = envelope_upper
                    exit_day = d
                    exit_reason = f"envelope_{param}pct"
                    break

        # --- D. Time型 ---
        elif method == "fixed_N":
            n = int(param)
            if d == n:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = f"fixed_{n}d"
                break

        elif method == "time_stop":
            n = int(param)
            if d >= n and c_close < entry_price:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = f"time_stop_{n}d"
                break

        elif method == "weekday_month":
            trade_date = pd.Timestamp(dates[ci])
            if param == 1:  # 金曜大引け
                if trade_date.weekday() == 4:  # Friday
                    exit_price = c_close
                    exit_day = d
                    exit_reason = "friday_close"
                    break
            elif param == 2:  # 月末大引け
                next_ci = ci + 1
                if next_ci < len(dates):
                    next_date = pd.Timestamp(dates[next_ci])
                    if next_date.month != trade_date.month:
                        exit_price = c_close
                        exit_day = d
                        exit_reason = "month_end"
                        break
                else:
                    exit_price = c_close
                    exit_day = d
                    exit_reason = "month_end"
                    break

        # --- E. Oscillator型 ---
        elif method == "rsi_70":
            rsi_val = pl["rsi14"][ci]
            if not np.isnan(rsi_val) and rsi_val > 70:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = "rsi_70"
                break

        elif method == "stoch_cross":
            k_val = pl["stoch_k"][ci]
            d_val = pl["stoch_d"][ci]
            if not np.isnan(k_val) and not np.isnan(d_val):
                if k_val > 80 and k_val < d_val:
                    if ci + 1 < len(dates):
                        exit_price = opens[ci + 1]
                        exit_day = d + 1
                    else:
                        exit_price = c_close
                        exit_day = d
                    exit_reason = "stoch_cross"
                    break

        elif method == "macd_hist_rev":
            hist_val = pl["macd_hist"][ci]
            if not np.isnan(hist_val):
                if prev_macd_hist is not None and prev_macd_hist > 0 and hist_val <= 0:
                    if ci + 1 < len(dates):
                        exit_price = opens[ci + 1]
                        exit_day = d + 1
                    else:
                        exit_price = c_close
                        exit_day = d
                    exit_reason = "macd_hist_rev"
                    break
                prev_macd_hist = hist_val

        # --- F. Volatility型 ---
        elif method == "bb_upper":
            bb_val = pl["bb_upper"][ci]
            if not np.isnan(bb_val) and c_high >= bb_val:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = "bb_upper"
                break

        elif method == "atr_spike":
            atr_val = pl["atr14"][ci]
            atr_ma = pl["atr14_ma20"][ci]
            if not np.isnan(atr_val) and not np.isnan(atr_ma) and atr_ma > 0:
                if atr_val > atr_ma * param:
                    if ci + 1 < len(dates):
                        exit_price = opens[ci + 1]
                        exit_day = d + 1
                    else:
                        exit_price = c_close
                        exit_day = d
                    exit_reason = f"atr_spike_{param}x"
                    break

        # --- G. Structure型 ---
        elif method == "n_day_high":
            n = int(param)
            key = f"donchian_high_{n}" if n in (20, 60) else None
            if key and key in pl:
                hi = pl[key][ci]
            else:
                start = max(0, ci - n + 1)
                hi = highs[start:ci + 1].max()
            if not np.isnan(hi) and c_high >= hi and d >= 1:
                exit_price = hi
                exit_day = d
                exit_reason = f"high_{n}d"
                break

        elif method == "fib_161":
            # 直近安値 = entry日前20日間の最安値
            lookback_start = max(0, entry_idx - 20)
            recent_low = lows[lookback_start:entry_idx + 1].min()
            swing = entry_price - recent_low
            if swing > 0:
                fib_target = entry_price + swing * 0.618  # 161.8% = entry + 61.8% of swing
                if c_high >= fib_target:
                    exit_price = fib_target
                    exit_day = d
                    exit_reason = "fib_161"
                    break

        # --- H. Hybrid型 ---
        elif method == "trail_max60":
            # Trail(param%) + 最大60日（MAX_HOLDと同じなので実質Trail）
            trail_trigger = max_high * (1 - param / 100)
            if c_close <= trail_trigger and d >= 1:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                else:
                    exit_price = c_close
                    exit_day = d
                exit_reason = f"trail_max60_{param}pct"
                break

        elif method == "partial_profit":
            # 半分を+10%利確、残りTrail(param%)
            target_10 = entry_price * 1.10
            if not partial_exited and c_high >= target_10:
                partial_exited = True
                # 半分利確分のPnLは後で計算
            if partial_exited:
                trail_trigger = max_high * (1 - param / 100)
                if c_close <= trail_trigger:
                    if ci + 1 < len(dates):
                        trail_exit = opens[ci + 1]
                        trail_day = d + 1
                    else:
                        trail_exit = c_close
                        trail_day = d
                    # 加重平均: 半分+10%, 半分trail_exit
                    exit_price = (target_10 + trail_exit) / 2
                    exit_day = trail_day
                    exit_reason = f"partial_{param}pct"
                    break

        elif method == "target_then_trail":
            # +10%到達後にTrail(param%)開始
            target_10 = entry_price * 1.10
            if not trailing_active and c_high >= target_10:
                trailing_active = True
                trail_high = c_high
            if trailing_active:
                trail_high = max(trail_high, c_high)
                trail_trigger = trail_high * (1 - param / 100)
                if c_close <= trail_trigger:
                    if ci + 1 < len(dates):
                        exit_price = opens[ci + 1]
                        exit_day = d + 1
                    else:
                        exit_price = c_close
                        exit_day = d
                    exit_reason = f"tgt_trail_{param}pct"
                    break

    # MAX_HOLD到達
    if exit_price is None:
        ci = min(entry_idx + MAX_HOLD - 1, len(dates) - 1)
        if ci + 1 < len(dates):
            exit_price = opens[ci + 1]
            exit_day = MAX_HOLD
        else:
            exit_price = closes[ci]
            exit_day = MAX_HOLD - 1
        exit_reason = "expire"

    ret_pct = (exit_price / entry_price - 1) * 100
    pnl = entry_price * 100 * ret_pct / 100  # 100株

    return {
        "ret_pct": round(ret_pct, 3),
        "pnl": round(pnl, 2),
        "hold_days": exit_day,
        "exit_reason": exit_reason,
    }


def simulate_all_v2(
    trades: pd.DataFrame,
    price_lookup: dict,
    method: str,
    param: float,
    sl_pct: float = 999.0,
) -> dict:
    """全トレードをシミュレーションし統計量を返す。"""
    results = []
    for _, row in trades.iterrows():
        ticker = row["ticker"]
        if ticker not in price_lookup:
            continue
        entry_dt = (row["entry_date"].to_numpy().astype("datetime64[ns]")
                    if hasattr(row["entry_date"], "to_numpy")
                    else np.datetime64(row["entry_date"]))
        r = simulate_trade_v2(
            price_lookup[ticker], entry_dt,
            float(row["entry_price"]), method, param, sl_pct,
        )
        if r is not None:
            results.append(r)

    if not results:
        return {"n": 0, "wr": 0, "pf": 0, "pnl_m": 0, "avg_ret": 0, "avg_hold": 0}

    rets = np.array([r["ret_pct"] for r in results])
    pnls = np.array([r["pnl"] for r in results])
    holds = np.array([r["hold_days"] for r in results])
    wins = rets > 0
    gw = rets[wins].sum()
    gl = abs(rets[~wins].sum())

    return {
        "n": len(results),
        "wr": round(wins.mean() * 100, 1),
        "pf": round(gw / gl if gl > 0 else 999, 2),
        "pnl_m": round(pnls.sum() / 10000, 1),
        "avg_ret": round(rets.mean(), 3),
        "avg_hold": round(holds.mean(), 1),
    }


def simulate_all_v2_yearly(
    trades: pd.DataFrame,
    price_lookup: dict,
    method: str,
    param: float,
    sl_pct: float = 999.0,
) -> dict[int, dict]:
    """年別統計量を返す。"""
    trades = trades.copy()
    trades["_year"] = pd.to_datetime(trades["entry_date"]).dt.year
    yearly = {}
    for year, grp in trades.groupby("_year"):
        yearly[int(year)] = simulate_all_v2(grp, price_lookup, method, param, sl_pct)
    return yearly


# ============================================================
# HTML Helpers（09と同じデザインシステム）
# ============================================================

def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return (f'<div class="stat-card {cls}"><div class="label">{label}</div>'
            f'<div class="value">{value}</div>{sub_html}</div>')


def _table_html(headers: list[str], rows: list[list], highlight_col: int | None = None) -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    best_idx = -1
    if highlight_col is not None:
        vals = []
        for r in rows:
            try:
                raw = (str(r[highlight_col]).replace("万", "").replace(",", "")
                       .replace("+", "").replace("<b>", "").replace("</b>", ""))
                vals.append(float(raw))
            except (ValueError, IndexError):
                vals.append(-9999)
        if vals:
            best_idx = vals.index(max(vals))
    trs = []
    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _insight_box(text: str) -> str:
    return f'<div class="insight-box">{text}</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{title}</h2>{content}</section>'


def _plotly_bar(div_id: str, labels: list[str], values: list[float],
                title: str = "", yaxis_title: str = "", height: int = 300) -> str:
    colors = ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in values]
    traces = json.dumps([{"x": labels, "y": values, "type": "bar",
                          "marker": {"color": colors}}])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 50, "l": 60, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "yaxis": {"title": yaxis_title},
    })
    return (f'<div id="{div_id}" style="height:{height}px"></div>\n'
            f'<script>Plotly.newPlot("{div_id}",{traces},{layout},{{responsive:true}})</script>')


def _plotly_heatmap(div_id: str, z: list[list[float]], x_labels: list[str],
                    y_labels: list[str], title: str = "", height: int = 500) -> str:
    # z値に基づくテキスト注釈
    text = [[f"{v:+.0f}" for v in row] for row in z]
    traces = json.dumps([{
        "z": z, "x": x_labels, "y": y_labels,
        "type": "heatmap", "colorscale": "RdYlGn",
        "text": text, "texttemplate": "%{text}",
        "textfont": {"size": 10},
        "hovertemplate": "%{y} × %{x}: %{z:+.0f}万<extra></extra>",
    }])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 50, "b": 80, "l": 180, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "xaxis": {"title": "Rule", "side": "bottom"},
        "yaxis": {"autorange": "reversed"},
    })
    return (f'<div id="{div_id}" style="height:{height}px"></div>\n'
            f'<script>Plotly.newPlot("{div_id}",{traces},{layout},{{responsive:true}})</script>')


# ============================================================
# 手法定義
# ============================================================

# (method_id, display_name, method_key, param, category)
def _build_method_configs() -> list[tuple[str, str, str, float, str]]:
    """全手法リストを返す。"""
    configs = []

    # A. Target
    for pct in [5, 10, 15, 20, 25]:
        configs.append((f"A1_{pct}", f"固定{pct}%利確", "target_pct", pct, "A"))
    for r in [2, 3, 5]:
        configs.append((f"A2_{r}R", f"R倍数{r}R", "target_r", r, "A"))
    for m in [2, 3, 5]:
        configs.append((f"A3_{m}ATR", f"ATR目標{m}倍", "target_atr", m, "A"))

    # B. Trailing
    for pct in [5, 10, 15, 20]:
        configs.append((f"B1_{pct}", f"Trail{pct}%", "trail_pct", pct, "B"))
    for m in [2, 3]:
        configs.append((f"B2_{m}ATR", f"Chandelier{m}ATR", "chandelier", m, "B"))
    for n in [10, 20, 40]:
        configs.append((f"B3_{n}d", f"Donchian{n}d", "donchian", n, "B"))
    configs.append(("B4_SAR", "ParabolicSAR", "sar", 0, "B"))

    # C. MA
    configs.append(("C1_SMA20", "SMA20割れ", "sma20_break", 0, "C"))
    configs.append(("C2_DC", "デッドクロス", "dead_cross", 0, "C"))
    for pct in [5, 10, 15]:
        configs.append((f"C3_{pct}", f"Envelope{pct}%", "ma_envelope", pct, "C"))

    # D. Time
    for n in [5, 10, 20, 30, 45, 60]:
        configs.append((f"D1_{n}d", f"固定{n}日", "fixed_N", n, "D"))
    for n in [5, 10, 20, 30]:
        configs.append((f"D2_{n}d", f"TimeStop{n}d", "time_stop", n, "D"))
    configs.append(("D3_fri", "金曜大引け", "weekday_month", 1, "D"))
    configs.append(("D3_eom", "月末大引け", "weekday_month", 2, "D"))

    # E. Oscillator
    configs.append(("E1_RSI", "RSI>70", "rsi_70", 0, "E"))
    configs.append(("E2_STOCH", "Stoch%K>80&<%D", "stoch_cross", 0, "E"))
    configs.append(("E3_MACD", "MACDhist反転", "macd_hist_rev", 0, "E"))

    # F. Volatility
    configs.append(("F1_BB", "BB+2σ", "bb_upper", 0, "F"))
    configs.append(("F2_ATR2x", "ATR急拡大2x", "atr_spike", 2, "F"))

    # G. Structure
    for n in [20, 60]:
        configs.append((f"G1_{n}d", f"{n}日高値", "n_day_high", n, "G"))
    configs.append(("G2_FIB", "Fib161.8%", "fib_161", 0, "G"))

    # H. Hybrid
    configs.append(("H1_T10M60", "Trail10%+Max60", "trail_max60", 10, "H"))
    configs.append(("H2_PART10", "半分+10%+Trail10%", "partial_profit", 10, "H"))
    configs.append(("H3_TGT_TR", "+10%後Trail5%", "target_then_trail", 5, "H"))

    return configs


CATEGORY_NAMES = {
    "A": "A. 固定目標型（Target）",
    "B": "B. トレーリング型（Trailing）",
    "C": "C. 移動平均型（MA）",
    "D": "D. 時間型（Time）",
    "E": "E. オシレーター型（Oscillator）",
    "F": "F. ボラティリティ型（Volatility）",
    "G": "G. 構造型（Structure）",
    "H": "H. 複合型（Hybrid）",
}


# ============================================================
# Main
# ============================================================

def main() -> None:
    t0 = time.time()
    print("=" * 60)
    print("Ch5-2: Exit Methods Survey — 20手法体系的検証")
    print("=" * 60)

    # ---- Load data ----
    print("\n[1/5] Loading data...")
    trades = pd.read_parquet(PROCESSED / "trades_cleaned.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned.parquet")
    long = trades[trades["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")
    print(f"  Price records: {len(prices):,}")

    # ---- Build price lookup v2 ----
    print("[2/5] Building price lookup with indicators...")
    t1 = time.time()
    price_lookup = build_price_lookup_v2(prices)
    print(f"  Tickers: {len(price_lookup)} ({time.time()-t1:.1f}s)")

    # ---- Baseline ----
    print("[3/5] Computing baselines...")
    baseline: dict[str, dict] = {}
    for rule in RULES:
        sub = long[long["rule"] == rule]
        ret = sub["ret_pct"].values
        pnl_total = (sub["entry_price"] * 100 * ret / 100).sum() / 10000
        wins = ret > 0
        gw = ret[wins].sum()
        gl = abs(ret[~wins].sum())
        baseline[rule] = {
            "n": len(sub),
            "wr": round(wins.mean() * 100, 1),
            "pf": round(gw / gl if gl > 0 else 999, 2),
            "pnl_m": round(pnl_total, 1),
            "avg_hold": round(sub["hold_days"].mean(), 1),
        }

    # Ch5 best results
    ch5_best: dict[str, dict] = {}
    for rule in RULES:
        sub = long[long["rule"] == rule]
        sl = PROPOSED_SLS[rule]
        mode, param = CH5_BEST[rule]
        if mode.startswith("fixed"):
            ch5_best[rule] = simulate_all_v2(sub, price_lookup, "fixed_N", param, sl)
        elif mode.startswith("min_hold"):
            ch5_best[rule] = simulate_all_v2(sub, price_lookup, "time_stop", param, sl)
        else:
            ch5_best[rule] = baseline[rule]

    # ---- Run all methods ----
    print("[4/5] Running all exit methods...")
    configs = _build_method_configs()
    total_configs = len(configs) * len(RULES)
    print(f"  {len(configs)} methods × {len(RULES)} rules = {total_configs} combinations")

    # results[method_id][rule] = stats_dict
    all_results: dict[str, dict[str, dict]] = {}
    done = 0
    for mid, display, method_key, param, cat in configs:
        all_results[mid] = {}
        for rule in RULES:
            sub = long[long["rule"] == rule]
            sl = PROPOSED_SLS[rule]
            all_results[mid][rule] = simulate_all_v2(sub, price_lookup, method_key, param, sl)
            done += 1
        if done % 40 == 0 or done == total_configs:
            elapsed = time.time() - t0
            print(f"  {done}/{total_configs} ({elapsed:.0f}s)")

    # ---- Build report ----
    print("[5/5] Building HTML report...")
    sections_html = []

    # ========== Section 0: Baseline + Ch5 summary ==========
    s0 = ""
    cards = []
    for rule in RULES:
        b = baseline[rule]
        cards.append(_stat_card(
            f"{rule} 現行", f'{b["pnl_m"]:+,.0f}万',
            f'N={b["n"]:,} / WR={b["wr"]}% / PF={b["pf"]:.2f}',
            "pos" if b["pnl_m"] > 0 else "neg",
        ))
    s0 += f'<div class="card-grid">{" ".join(cards)}</div>'

    ch5_cards = []
    for rule in RULES:
        c = ch5_best[rule]
        mode_name, param_val = CH5_BEST[rule]
        ch5_cards.append(_stat_card(
            f"{rule} Ch5提案", f'{c["pnl_m"]:+,.0f}万',
            f'{mode_name} / WR={c["wr"]}% / PF={c["pf"]:.2f}',
            "pos" if c["pnl_m"] > baseline[rule]["pnl_m"] else "warn",
        ))
    s0 += f'<div class="card-grid">{" ".join(ch5_cards)}</div>'
    s0 += _insight_box(
        "上段: 現行signal exit、下段: Ch5提案（B1/B3=fixed60d, B2=min_hold30d, B4=fixed13d）。"
        "ここから20手法を体系的に検証し、最善の出口を理論的に結論づける。"
    )
    sections_html.append(_section("0. ベースライン", s0))

    # ========== Section 1: Heatmap summary ==========
    s1 = ""
    # method_id → display name mapping
    mid_to_display = {mid: disp for mid, disp, _, _, _ in configs}

    # Build heatmap: rows=methods, cols=rules
    heatmap_z = []
    heatmap_y = []
    for mid, display, _, _, _ in configs:
        row = [all_results[mid][rule]["pnl_m"] for rule in RULES]
        heatmap_z.append(row)
        heatmap_y.append(display)

    s1 += _plotly_heatmap("heatmap_all", heatmap_z, RULES, heatmap_y,
                          title="全手法×ルール PnLヒートマップ（万円）", height=1200)
    s1 += _insight_box("緑=プラス、赤=マイナス。各セルの数値は12年間累積PnL（万円）。")
    sections_html.append(_section("1. 全手法サマリー（ヒートマップ）", s1))

    # ========== Sections 2-9: Category details ==========
    section_num = 2
    for cat_key in ["A", "B", "C", "D", "E", "F", "G", "H"]:
        cat_name = CATEGORY_NAMES[cat_key]
        cat_configs = [(mid, disp, mk, p, c) for mid, disp, mk, p, c in configs if c == cat_key]

        s = ""
        for rule in RULES:
            rows_data = []
            sl = PROPOSED_SLS[rule]
            bl_pnl = baseline[rule]["pnl_m"]

            for mid, display, _, _, _ in cat_configs:
                r = all_results[mid][rule]
                delta = r["pnl_m"] - bl_pnl
                rows_data.append([
                    display,
                    f'{r["n"]:,}',
                    f'{r["wr"]}%',
                    f'{r["pf"]:.2f}',
                    f'{r["pnl_m"]:+,.0f}万',
                    f'{r["avg_hold"]:.0f}d',
                    f'{delta:+,.0f}万',
                ])

            s += f"<h3>{rule}（SL=-{sl}%）</h3>"
            s += _table_html(
                ["手法", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"],
                rows_data, highlight_col=4,
            )

        # Chart: best param per rule
        labels = [d for _, d, _, _, _ in cat_configs]
        for rule in RULES:
            pnls = [all_results[mid][rule]["pnl_m"] for mid, _, _, _, _ in cat_configs]
            s += _plotly_bar(
                f"bar_{cat_key}_{rule}", labels, pnls,
                title=f"{rule}: {cat_name} PnL比較", yaxis_title="PnL(万)",
            )

        sections_html.append(_section(f"{section_num}. {cat_name}", s))
        section_num += 1

    # ========== Section 10: Ranking + Robustness ==========
    s10 = ""

    # Grand ranking: sum of PnL across all rules
    ranking = []
    for mid, display, mk, param, cat in configs:
        total_pnl = sum(all_results[mid][rule]["pnl_m"] for rule in RULES)
        avg_pf = np.mean([all_results[mid][rule]["pf"] for rule in RULES])
        avg_wr = np.mean([all_results[mid][rule]["wr"] for rule in RULES])
        ranking.append((mid, display, cat, total_pnl, avg_pf, avg_wr))

    ranking.sort(key=lambda x: x[3], reverse=True)

    # Top 20 table
    top_rows = []
    for i, (mid, display, cat, total_pnl, avg_pf, avg_wr) in enumerate(ranking[:20]):
        per_rule = " / ".join(f'{all_results[mid][r]["pnl_m"]:+.0f}' for r in RULES)
        top_rows.append([
            f"#{i+1}", display, CATEGORY_NAMES[cat].split(".")[0],
            f'{total_pnl:+,.0f}万', f'{avg_pf:.2f}', f'{avg_wr:.1f}%',
            per_rule,
        ])
    s10 += "<h3>総合PnLランキング Top20</h3>"
    s10 += _table_html(
        ["Rank", "手法", "カテゴリ", "総PnL", "平均PF", "平均WR", "B1/B2/B3/B4"],
        top_rows, highlight_col=3,
    )

    # Bottom 5
    bottom_rows = []
    for mid, display, cat, total_pnl, avg_pf, avg_wr in ranking[-5:]:
        bottom_rows.append([
            display, CATEGORY_NAMES[cat].split(".")[0],
            f'{total_pnl:+,.0f}万', f'{avg_pf:.2f}', f'{avg_wr:.1f}%',
        ])
    s10 += "<h3>ワースト5</h3>"
    s10 += _table_html(["手法", "カテゴリ", "総PnL", "平均PF", "平均WR"], bottom_rows)

    # Per-rule best
    s10 += "<h3>ルール別ベスト手法</h3>"
    per_rule_rows = []
    for rule in RULES:
        best_mid = max(configs, key=lambda c: all_results[c[0]][rule]["pnl_m"])
        mid = best_mid[0]
        r = all_results[mid][rule]
        delta = r["pnl_m"] - baseline[rule]["pnl_m"]
        ch5_delta = r["pnl_m"] - ch5_best[rule]["pnl_m"]
        per_rule_rows.append([
            rule, best_mid[1],
            f'{r["pnl_m"]:+,.0f}万',
            f'{r["wr"]}%', f'{r["pf"]:.2f}',
            f'{delta:+,.0f}万',
            f'{ch5_delta:+,.0f}万',
        ])
    s10 += _table_html(
        ["Rule", "ベスト手法", "PnL", "WR", "PF", "vs現行", "vsCh5"],
        per_rule_rows, highlight_col=2,
    )

    # Robustness: top3の年別安定性
    top3_mids = [ranking[i][0] for i in range(min(3, len(ranking)))]
    s10 += "<h3>Top3手法の年別安定性</h3>"
    for mid in top3_mids:
        disp = mid_to_display[mid]
        # find method_key and param
        mk, pm, sl_rule = "", 0.0, 999.0
        for _mid, _disp, _mk, _pm, _cat in configs:
            if _mid == mid:
                mk, pm = _mk, _pm
                break

        s10 += f"<h4>{disp}</h4>"
        for rule in RULES:
            sub = long[long["rule"] == rule]
            sl = PROPOSED_SLS[rule]
            yearly = simulate_all_v2_yearly(sub, price_lookup, mk, pm, sl)
            if not yearly:
                continue
            years = sorted(yearly.keys())
            y_rows = []
            win_years = 0
            for y in years:
                ys = yearly[y]
                if ys["pnl_m"] > 0:
                    win_years += 1
                y_rows.append([
                    str(y), f'{ys["n"]:,}', f'{ys["wr"]}%',
                    f'{ys["pf"]:.2f}', f'{ys["pnl_m"]:+,.0f}万',
                ])
            s10 += f"<p><b>{rule}</b>: 年勝率 {win_years}/{len(years)}</p>"
            s10 += _table_html(["年", "N", "WR", "PF", "PnL"], y_rows, highlight_col=4)

    sections_html.append(_section("10. ランキング＋ロバストネス", s10))

    # ========== Section 11: Conclusion ==========
    s11 = ""

    # Optimal combination
    s11 += "<h3>最終推奨: ルール別最適出口</h3>"
    final_rows = []
    total_proposed = 0.0
    total_baseline = 0.0
    total_ch5 = 0.0
    for rule in RULES:
        best_cfg = max(configs, key=lambda c: all_results[c[0]][rule]["pnl_m"])
        mid = best_cfg[0]
        r = all_results[mid][rule]
        bl = baseline[rule]["pnl_m"]
        ch5 = ch5_best[rule]["pnl_m"]
        total_proposed += r["pnl_m"]
        total_baseline += bl
        total_ch5 += ch5
        final_rows.append([
            rule, best_cfg[1],
            f'{r["pnl_m"]:+,.0f}万',
            f'{r["wr"]}% / PF={r["pf"]:.2f}',
            f'{r["pnl_m"] - bl:+,.0f}万',
            f'{r["pnl_m"] - ch5:+,.0f}万',
        ])
    final_rows.append([
        "<b>合計</b>", "",
        f'<b>{total_proposed:+,.0f}万</b>', "",
        f'<b>{total_proposed - total_baseline:+,.0f}万</b>',
        f'<b>{total_proposed - total_ch5:+,.0f}万</b>',
    ])
    s11 += _table_html(
        ["Rule", "推奨出口", "PnL", "WR/PF", "vs現行", "vsCh5"],
        final_rows, highlight_col=2,
    )

    # Category-level insights
    cat_totals = {}
    for cat_key in CATEGORY_NAMES:
        cat_mids = [mid for mid, _, _, _, c in configs if c == cat_key]
        cat_pnls = []
        for mid in cat_mids:
            cat_pnls.append(sum(all_results[mid][r]["pnl_m"] for r in RULES))
        cat_totals[cat_key] = {
            "best": max(cat_pnls) if cat_pnls else 0,
            "avg": np.mean(cat_pnls) if cat_pnls else 0,
        }

    cat_rank = sorted(cat_totals.items(), key=lambda x: x[1]["best"], reverse=True)
    cat_rows = []
    for cat_key, stats in cat_rank:
        cat_rows.append([
            CATEGORY_NAMES[cat_key],
            f'{stats["best"]:+,.0f}万',
            f'{stats["avg"]:+,.0f}万',
        ])
    s11 += "<h3>カテゴリ別パフォーマンス</h3>"
    s11 += _table_html(["カテゴリ", "ベスト手法PnL", "平均PnL"], cat_rows, highlight_col=1)

    # 理論的解釈
    s11 += _insight_box(
        "<b>理論的解釈:</b><br>"
        "・固定目標型（Target）は利益の上限を自ら設定するため、トレンドフォロー戦略と本質的に矛盾する<br>"
        "・トレーリング型はMFE捕捉率を直接改善するが、パラメータ感度が高い<br>"
        "・時間型（固定保有）はシンプルかつロバストで、パラメータ感度が低い<br>"
        "・オシレーター型は逆張り指標であり、順張り戦略の出口には不向き<br>"
        "・ボラティリティ型は市場環境に適応的だが、トレンド初期での早期撤退リスクがある<br>"
        "・複合型はパラメータが増え過剰適合リスクが高まる一方、理論的には最適に近づける"
    )

    sections_html.append(_section("11. 結論と理論的解釈", s11))

    # ========== Assemble HTML ==========
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch5-2 Exit Methods Survey — Granville Strategy Verification</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
:root {{
  --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
  --text: #e2e8f0; --muted: #8892a8; --primary: #60a5fa;
  --pos: #34d399; --neg: #f87171; --warn: #fbbf24;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.6; padding: 20px; max-width: 1400px; margin: 0 auto;
}}
h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
h2 {{ font-size: 1.1rem; color: var(--primary); margin: 24px 0 12px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
h3 {{ font-size: 0.95rem; color: var(--muted); margin: 16px 0 8px; }}
h4 {{ font-size: 0.85rem; color: var(--muted); margin: 12px 0 6px; }}
p {{ margin: 6px 0; font-size: 0.82rem; }}
section {{ margin-bottom: 24px; }}
.meta {{ font-size: 0.75rem; color: var(--muted); margin-bottom: 16px; }}
.card-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 12px 0; }}
.stat-card {{
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 12px; text-align: center;
}}
.stat-card .label {{ font-size: 0.75rem; color: var(--muted); }}
.stat-card .value {{ font-size: 1.3rem; font-weight: 700; margin: 4px 0; }}
.stat-card .sub {{ font-size: 0.7rem; color: var(--muted); }}
.card-pos .value {{ color: var(--pos); }}
.card-neg .value {{ color: var(--neg); }}
.card-warn .value {{ color: var(--warn); }}
table {{
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
  margin: 10px 0; background: var(--card);
}}
th, td {{ padding: 6px 10px; border: 1px solid var(--border); text-align: right; }}
th {{ background: #1e2130; color: var(--primary); font-weight: 600; text-align: center; }}
td:first-child {{ text-align: left; font-weight: 500; }}
.best-row {{ background: rgba(96, 165, 250, 0.12); }}
.insight-box {{
  background: rgba(96, 165, 250, 0.08); border-left: 3px solid var(--primary);
  padding: 10px 14px; margin: 12px 0; font-size: 0.82rem;
  border-radius: 0 6px 6px 0; line-height: 1.7;
}}
@media (max-width: 768px) {{
  .card-grid {{ grid-template-columns: repeat(2, 1fr); }}
  table {{ font-size: 0.7rem; }}
  th, td {{ padding: 4px 6px; }}
}}
</style>
</head>
<body>
<h1>Chapter 5-2: Exit Methods Survey — 出口戦略の体系的検証</h1>
<div class="meta">Generated: {now} | Data: {len(long):,} LONG trades × {len(configs)} methods = {total_configs:,} combinations | Runtime: {time.time()-t0:.0f}s</div>
{body}
</body>
</html>"""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "report.html"
    out.write_text(html, encoding="utf-8")
    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"[OK] Report saved: {out}")
    print(f"  Size: {out.stat().st_size / 1024:.0f} KB")
    print(f"  Runtime: {elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    main()
