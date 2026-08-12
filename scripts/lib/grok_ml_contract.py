"""Shared, point-in-time-safe feature contract for the Grok ML package."""

from __future__ import annotations


FEATURE_COLUMNS = (
    "market_cap",
    "atr14_pct",
    "vol_ratio",
    "rsi9",
    "nikkei_change_pct",
    "futures_change_pct",
    "volatility_5d",
    "ma5_deviation",
    "ma25_deviation",
    "prev_day_return",
    "volume_ratio_5d",
    "price_range_5d",
    "nikkei_vol_5d",
    "nikkei_ret_5d",
    "topix_vol_5d",
    "topix_ret_5d",
    "futures_ret_5d",
    "usdjpy_vol_5d",
    "usdjpy_ret_5d",
    "prev_close_position",
    "prev_candle",
    "macd_hist",
    "bb_pctb",
    "vol_trend",
)

MARKET_CAP_SOURCE = "jquants_eq_daily_mktcap_d_minus_1"
PRICE_HISTORY_SOURCE = "grok_prices_yfinance_preferred_jquants_fallback"
FEATURE_CONTRACT = "selection_time_v2_no_target_open"
