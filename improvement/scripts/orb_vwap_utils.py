"""
ORB + VWAP バックテスト ユーティリティ関数
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import Tuple, Optional


def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """
    VWAPを計算する

    Parameters:
        df: OHLCV データフレーム（High, Low, Close, Volume カラム必須）

    Returns:
        VWAP の Series
    """
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    cumulative_tp_vol = (typical_price * df['Volume']).cumsum()
    cumulative_vol = df['Volume'].cumsum()

    # ゼロ除算回避
    vwap = cumulative_tp_vol / cumulative_vol.replace(0, np.nan)
    return vwap


def calculate_opening_range(
    df: pd.DataFrame,
    or_start: time = time(9, 0),
    or_end: time = time(9, 30)
) -> Tuple[Optional[float], Optional[float]]:
    """
    Opening Range (OR) の High/Low を計算する

    Parameters:
        df: 5分足データ（datetime インデックス、High/Low カラム必須）
        or_start: OR開始時刻（デフォルト 9:00）
        or_end: OR終了時刻（デフォルト 9:30）

    Returns:
        (or_high, or_low) のタプル。データ不足時は (None, None)
    """
    # datetime列がインデックスの場合
    if isinstance(df.index, pd.DatetimeIndex):
        times = df.index.time
    elif 'time' in df.columns:
        times = df['time']
    elif 'datetime' in df.columns:
        times = pd.to_datetime(df['datetime']).dt.time
    elif 'date' in df.columns:
        times = pd.to_datetime(df['date']).dt.time
    else:
        return None, None

    # OR期間のデータをフィルタ
    mask = (times >= or_start) & (times < or_end)
    or_data = df[mask]

    if or_data.empty:
        return None, None

    or_high = or_data['High'].max()
    or_low = or_data['Low'].min()

    return or_high, or_low


def get_entry_signal(
    price: float,
    or_high: float,
    or_low: float,
    vwap: float
) -> str:
    """
    エントリーシグナルを判定する

    Parameters:
        price: 現在価格
        or_high: OR High
        or_low: OR Low
        vwap: 現在のVWAP

    Returns:
        "BUY", "SELL", または "NO_SIGNAL"
    """
    # 買いシグナル: OR High 上抜け かつ VWAP より上
    if price > or_high and price > vwap:
        return "BUY"

    # 売りシグナル: OR Low 下抜け かつ VWAP より下
    if price < or_low and price < vwap:
        return "SELL"

    return "NO_SIGNAL"


def check_exit_condition(
    signal: str,
    current_price: float,
    entry_price: float,
    vwap_at_entry: float,
    or_range: float
) -> Tuple[bool, str]:
    """
    決済条件をチェックする

    Parameters:
        signal: "BUY" or "SELL"
        current_price: 現在価格
        entry_price: エントリー価格
        vwap_at_entry: エントリー時のVWAP
        or_range: OR Range (OR High - OR Low)

    Returns:
        (should_exit, reason) のタプル
        reason: "STOP_LOSS", "TAKE_PROFIT", または ""
    """
    if signal == "BUY":
        # 損切り: VWAPを下回る
        if current_price <= vwap_at_entry:
            return True, "STOP_LOSS"

        # 利確: エントリー価格 + OR Range
        take_profit_price = entry_price + or_range
        if current_price >= take_profit_price:
            return True, "TAKE_PROFIT"

    elif signal == "SELL":
        # 損切り: VWAPを上回る
        if current_price >= vwap_at_entry:
            return True, "STOP_LOSS"

        # 利確: エントリー価格 - OR Range
        take_profit_price = entry_price - or_range
        if current_price <= take_profit_price:
            return True, "TAKE_PROFIT"

    return False, ""


def calculate_pnl(
    signal: str,
    entry_price: float,
    exit_price: float,
    shares: int = 100
) -> Tuple[float, float]:
    """
    損益を計算する

    Parameters:
        signal: "BUY" or "SELL"
        entry_price: エントリー価格
        exit_price: 決済価格
        shares: 株数（デフォルト100株）

    Returns:
        (pnl_pct, pnl_amount) のタプル
    """
    if signal == "BUY":
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        pnl_amount = (exit_price - entry_price) * shares
    elif signal == "SELL":
        pnl_pct = ((entry_price - exit_price) / entry_price) * 100
        pnl_amount = (entry_price - exit_price) * shares
    else:
        pnl_pct = 0.0
        pnl_amount = 0.0

    return pnl_pct, pnl_amount


def filter_trading_hours(
    df: pd.DataFrame,
    entry_start: time = time(9, 30),
    entry_end: time = time(14, 30),
    market_close: time = time(15, 30)
) -> pd.DataFrame:
    """
    取引時間内のデータのみフィルタする

    Parameters:
        df: 5分足データ
        entry_start: エントリー開始時刻
        entry_end: エントリー終了時刻
        market_close: 大引け時刻

    Returns:
        フィルタされたデータフレーム
    """
    if isinstance(df.index, pd.DatetimeIndex):
        times = df.index.time
    elif 'datetime' in df.columns:
        times = pd.to_datetime(df['datetime']).dt.time
    else:
        return df

    # 前場・後場の取引時間（11:30-12:30は昼休み）
    morning_session = (times >= entry_start) & (times <= time(11, 30))
    afternoon_session = (times >= time(12, 30)) & (times <= market_close)

    mask = morning_session | afternoon_session
    return df[mask]


def get_next_trading_day(date_str: str, trading_dates: list) -> Optional[str]:
    """
    指定日の次の営業日を取得する

    Parameters:
        date_str: 基準日（YYYY-MM-DD）
        trading_dates: 営業日リスト

    Returns:
        次の営業日（YYYY-MM-DD）。見つからない場合は None
    """
    trading_dates_sorted = sorted(trading_dates)

    for td in trading_dates_sorted:
        if td > date_str:
            return td

    return None
