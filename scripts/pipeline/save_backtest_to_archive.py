#!/usr/bin/env python3
"""
save_backtest_to_archive.py
Grok trending銘柄のバックテスト結果をアーカイブに保存

実行方法:
    # パイプライン実行（16時更新 - GitHub Actions）
    python3 scripts/pipeline/save_backtest_to_archive.py

機能:
    - grok_trending.parquet を読み込み
    - 対象日のJ-Quants 1分足を取得済みファイルから読み込み
    - J-Quants日足とOHLCVを照合し、不整合時は日次保存全体を中止
    - バックテスト結果を計算（Phase1, Phase2, Phase3）
    - 前場・全日の高値・安値・最大上昇率・最大下落率を計算
    - grok_trending_YYYYMMDD.parquet として保存
    - grok_trending_archive.parquet に追加
    - S3にアップロード

出力:
    - data/parquet/backtest/grok_trending_YYYYMMDD.parquet
    - data/parquet/backtest/grok_trending_archive.parquet
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, time
from tempfile import TemporaryDirectory
from typing import Optional, Tuple, Any
import traceback

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file, download_file
from common_cfg.s3cfg import load_s3_config
from scripts.lib.grok_jquants_backtest import (
    JQuantsBacktestDataError,
    MARKET_CAP_PROVENANCE_COLUMNS,
    SEGMENT_TARGETS,
    assert_archive_history_unchanged,
    assert_archive_target_rows_preserved,
    build_derived_backtest_rows,
    calculate_segment_pnl,
    executable_exit,
    has_trade_after_entry,
    merge_archive_date,
    normalize_daily_prices,
    normalize_minute_bars,
    session_last_close,
    validate_daily_alignment,
    validate_selection_asof,
    validate_selection_market_cap,
    validate_target_daily_corporate_actions,
)
from scripts.lib.jquants_daily_fields import (
    DAILY_TRADE_STATUS_NO_MARKET_TRADE,
    DAILY_TRADE_STATUS_TRADED,
    JQ_DAILY_TRADE_STATUS,
)
from scripts.lib.protected_archive_s3 import (
    download_verified_archive,
    file_sha256,
    publish_guarded_archive,
    publish_guarded_manifest_entry,
    write_publish_state,
)
from scripts.pipeline.add_market_cap_to_grok_trending import (
    attach_official_market_cap_asof,
)

# パス定義
BACKTEST_DIR = PARQUET_DIR / "backtest"
BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
GROK_TRENDING_PATH = BACKTEST_DIR / "grok_trending_temp.parquet"
BACKTEST_ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"
ARCHIVE_PUBLISH_STATE_PATH = BACKTEST_DIR / "grok_trending_archive.publish.json"
FUTURES_PATH = PARQUET_DIR / "futures_prices_60d_5m.parquet"
JQUANTS_MINUTE_WATCH_PATH = PARQUET_DIR / "jquants_minute_watch.parquet"
JQUANTS_DAILY_PATH = PARQUET_DIR / "prices_max_1d.parquet"
JQUANTS_DAILY_FEATURES_PATH = (
    PARQUET_DIR / "jquants" / "watch_daily_features.parquet"
)
TRADING_CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"

# J-Quants入力のキャッシュ
_jquants_minute_df: Optional[pd.DataFrame] = None
_jquants_daily_df: Optional[pd.DataFrame] = None

# 極端相場の閾値（±3%）
EXTREME_MARKET_THRESHOLD = 3.0

# 取引制限データのパス（複数の候補をチェック）
ROOT = Path(__file__).resolve().parents[2]
MARGIN_CODE_MASTER_PATHS = [
    ROOT / "data" / "parquet" / "margin_code_master.parquet",
    ROOT / "improvement" / "data" / "margin_code_master.parquet",
]
JSF_RESTRICTION_PATHS = [
    ROOT / "data" / "parquet" / "jsf_seigenichiran.csv",
    ROOT / "improvement" / "data" / "jsf_seigenichiran.csv",
]

# 取引制限データ（グローバルキャッシュ）
_margin_code_map: Optional[dict] = None
_margin_name_map: Optional[dict] = None
_jsf_stop_codes: Optional[set] = None

# デイトレードリスト（グローバルキャッシュ）
_day_trade_list: Optional[pd.DataFrame] = None

def load_trading_restrictions() -> Tuple[dict, dict, set]:
    """
    取引制限データを読み込み（シングルトン）

    Returns:
        Tuple of (margin_code_map, margin_name_map, jsf_stop_codes)

    Raises:
        FileNotFoundError: 必須ファイルが見つからない場合
    """
    global _margin_code_map, _margin_name_map, _jsf_stop_codes

    # MarginCodeマスター（複数パスをチェック）
    if _margin_code_map is None:
        margin_path = None
        for path in MARGIN_CODE_MASTER_PATHS:
            if path.exists():
                margin_path = path
                break

        if margin_path:
            margin_df = pd.read_parquet(margin_path)
            _margin_code_map = dict(zip(margin_df['ticker'], margin_df['margin_code']))
            _margin_name_map = dict(zip(margin_df['ticker'], margin_df['margin_code_name']))
            print(f"[INFO] MarginCode loaded: {len(_margin_code_map)} stocks from {margin_path.name}")
        else:
            raise FileNotFoundError(
                f"[ERROR] MarginCode master not found. "
                f"Checked paths: {[str(p) for p in MARGIN_CODE_MASTER_PATHS]}. "
                f"Please ensure margin_code_master.parquet is available on S3 or locally."
            )

    # 日証金制限データ（複数パスをチェック）
    if _jsf_stop_codes is None:
        jsf_path = None
        for path in JSF_RESTRICTION_PATHS:
            if path.exists():
                jsf_path = path
                break

        if jsf_path:
            try:
                jsf = pd.read_csv(jsf_path, skiprows=4)
                _jsf_stop_codes = set(jsf[jsf['実施措置'] == '申込停止']['銘柄コード'].astype(str))
                print(f"[INFO] JSF restrictions loaded: {len(_jsf_stop_codes)} stocks from {jsf_path.name}")
            except Exception as e:
                raise RuntimeError(f"[ERROR] Failed to parse JSF CSV: {e}")
        else:
            raise FileNotFoundError(
                f"[ERROR] JSF restriction file not found. "
                f"Checked paths: {[str(p) for p in JSF_RESTRICTION_PATHS]}. "
                f"Please ensure jsf_seigenichiran.csv is available on S3 or locally."
            )

    return _margin_code_map, _margin_name_map, _jsf_stop_codes


def load_day_trade_list() -> pd.DataFrame:
    """
    grok_day_trade_list.parquet を読み込み（シングルトン）

    Returns:
        pd.DataFrame: デイトレードリスト
    """
    global _day_trade_list

    if _day_trade_list is not None:
        return _day_trade_list

    # S3からダウンロード
    cfg = load_s3_config()
    if cfg:
        local_path = PARQUET_DIR / "grok_day_trade_list.parquet"
        s3_key = "grok_day_trade_list.parquet"
        if download_file(cfg, s3_key, local_path):
            _day_trade_list = pd.read_parquet(local_path)
            print(f"[INFO] Day trade list loaded: {len(_day_trade_list)} stocks from S3")
            return _day_trade_list

    # ローカルファイルをチェック
    local_path = PARQUET_DIR / "grok_day_trade_list.parquet"
    if local_path.exists():
        _day_trade_list = pd.read_parquet(local_path)
        print(f"[INFO] Day trade list loaded: {len(_day_trade_list)} stocks from local")
        return _day_trade_list

    print("[WARN] grok_day_trade_list.parquet not found")
    _day_trade_list = pd.DataFrame()
    return _day_trade_list


def get_day_trade_info(ticker: str) -> dict:
    """
    銘柄のデイトレード情報を取得

    Args:
        ticker: 銘柄コード (例: "7203.T")

    Returns:
        dict with shortable, day_trade, ng, day_trade_available_shares
    """
    df = load_day_trade_list()

    if df.empty:
        return {
            'shortable': False,
            'day_trade': True,
            'ng': False,
            'day_trade_available_shares': None,
        }

    match = df[df['ticker'] == ticker]
    if match.empty:
        return {
            'shortable': False,
            'day_trade': True,
            'ng': False,
            'day_trade_available_shares': None,
        }

    row = match.iloc[0]
    shares = row.get('day_trade_available_shares')
    if pd.isna(shares):
        shares = None
    else:
        shares = int(shares)

    return {
        'shortable': bool(row.get('shortable', False)),
        'day_trade': bool(row.get('day_trade', True)),
        'ng': bool(row.get('ng', False)),
        'day_trade_available_shares': shares,
    }


def get_trading_restriction_info(ticker: str) -> dict:
    """
    銘柄の取引制限情報を取得

    Args:
        ticker: 銘柄コード (例: "7203.T")

    Returns:
        dict with margin_code, margin_code_name, jsf_restricted, is_shortable
    """
    margin_code_map, margin_name_map, jsf_stop_codes = load_trading_restrictions()

    code = ticker.replace('.T', '')
    margin_code = margin_code_map.get(ticker, '2')  # デフォルトは貸借
    margin_code_name = margin_name_map.get(ticker, '貸借')
    jsf_restricted = code in jsf_stop_codes
    is_shortable = (margin_code == '2') and (not jsf_restricted)

    return {
        'margin_code': margin_code,
        'margin_code_name': margin_code_name,
        'jsf_restricted': jsf_restricted,
        'is_shortable': is_shortable,
    }


def load_jquants_minute() -> pd.DataFrame:
    """Load the pipeline's J-Quants watch-universe minute file once."""
    global _jquants_minute_df
    if _jquants_minute_df is not None:
        return _jquants_minute_df
    if not JQUANTS_MINUTE_WATCH_PATH.exists():
        raise FileNotFoundError(
            f"J-Quants minute file not found: {JQUANTS_MINUTE_WATCH_PATH}"
        )
    _jquants_minute_df = pd.read_parquet(JQUANTS_MINUTE_WATCH_PATH)
    print(
        f"[INFO] J-Quants minute watch loaded: "
        f"{len(_jquants_minute_df)} records"
    )
    return _jquants_minute_df


def load_jquants_daily() -> pd.DataFrame:
    """Load and normalize the pipeline's J-Quants daily file once."""
    global _jquants_daily_df
    if _jquants_daily_df is not None:
        return _jquants_daily_df
    if not JQUANTS_DAILY_PATH.exists():
        raise FileNotFoundError(f"J-Quants daily file not found: {JQUANTS_DAILY_PATH}")
    _jquants_daily_df = normalize_daily_prices(pd.read_parquet(JQUANTS_DAILY_PATH))
    print(f"[INFO] J-Quants daily prices loaded: {len(_jquants_daily_df)} records")
    return _jquants_daily_df


def fetch_intraday_data(ticker: str, target_date: datetime) -> pd.DataFrame:
    """Return strictly validated J-Quants one-minute bars for one ticker-day."""
    bars = normalize_minute_bars(load_jquants_minute(), ticker, target_date)
    print(
        f"[DEBUG] {ticker}: got {len(bars)} J-Quants 1m records "
        f"for {target_date.date()}"
    )
    return bars


def validate_batch_coverage(
    grok: pd.DataFrame,
    target_date: datetime,
    daily_features: pd.DataFrame,
) -> set[str]:
    """Validate traded inputs and return explicit official no-market tickers."""
    if "ticker" not in grok.columns:
        raise JQuantsBacktestDataError("grok_trending.parquet has no ticker column")
    selected = grok["ticker"].astype(str)
    duplicates = sorted(selected[selected.duplicated()].unique().tolist())
    if duplicates:
        raise JQuantsBacktestDataError(
            f"grok_trending.parquet contains duplicate tickers: {duplicates}"
        )

    day = target_date.date()
    minute = load_jquants_minute()
    minute_datetimes = pd.to_datetime(minute["datetime"], errors="coerce")
    minute_tickers = set(
        minute.loc[minute_datetimes.dt.date.eq(day), "ticker"].astype(str)
    )

    daily = load_jquants_daily()
    daily_tickers = set(
        daily.loc[daily["date"].dt.date.eq(day), "ticker"].astype(str)
    )
    selected_tickers = set(selected)
    feature_dates = pd.to_datetime(
        daily_features["trading_date"], errors="coerce"
    ).dt.date
    feature_rows = daily_features.loc[
        feature_dates.eq(day)
        & daily_features["ticker"].astype(str).isin(selected_tickers),
        ["ticker", JQ_DAILY_TRADE_STATUS],
    ].copy()
    feature_rows["ticker"] = feature_rows["ticker"].astype(str)
    if feature_rows["ticker"].duplicated().any():
        raise JQuantsBacktestDataError(
            "J-Quants target-day sidecar has duplicate trade-status keys"
        )
    status_by_ticker = dict(
        zip(feature_rows["ticker"], feature_rows[JQ_DAILY_TRADE_STATUS])
    )
    missing_status = sorted(selected_tickers - set(status_by_ticker))
    invalid_status = {
        ticker: status
        for ticker, status in status_by_ticker.items()
        if status
        not in {
            DAILY_TRADE_STATUS_TRADED,
            DAILY_TRADE_STATUS_NO_MARKET_TRADE,
        }
    }
    if missing_status or invalid_status:
        raise JQuantsBacktestDataError(
            "J-Quants target-day trade status is incomplete or invalid: "
            f"missing={missing_status}, invalid={invalid_status}"
        )

    no_market_tickers = {
        ticker
        for ticker, status in status_by_ticker.items()
        if status == DAILY_TRADE_STATUS_NO_MARKET_TRADE
    }
    traded_tickers = selected_tickers - no_market_tickers
    missing_minute = sorted(traded_tickers - minute_tickers)
    missing_daily = sorted(traded_tickers - daily_tickers)
    unexpected_no_market_minute = sorted(no_market_tickers & minute_tickers)
    unexpected_no_market_daily = sorted(no_market_tickers & daily_tickers)
    if (
        missing_minute
        or missing_daily
        or unexpected_no_market_minute
        or unexpected_no_market_daily
    ):
        raise JQuantsBacktestDataError(
            "Incomplete J-Quants batch coverage; archive append refused. "
            f"missing_minute={missing_minute}, missing_daily={missing_daily}, "
            f"no_market_with_minute={unexpected_no_market_minute}, "
            f"no_market_with_daily_price={unexpected_no_market_daily}"
        )

    print(
        f"[OK] J-Quants preflight coverage: traded={len(traded_tickers)}, "
        f"official_no_market_trade={len(no_market_tickers)}, "
        f"total={len(selected_tickers)} for {day}"
    )
    return no_market_tickers


def validate_or_attach_market_cap_provenance(
    grok: pd.DataFrame,
    target_date: datetime,
    daily_features: pd.DataFrame,
    trading_calendar: pd.DataFrame,
) -> pd.DataFrame:
    """Validate current provenance or safely upgrade one fully legacy artifact."""
    proof_columns = MARKET_CAP_PROVENANCE_COLUMNS
    present = proof_columns & set(grok.columns)
    if present == proof_columns:
        validate_selection_market_cap(grok, target_date, trading_calendar)
        return grok
    if present:
        missing = sorted(proof_columns - present)
        raise JQuantsBacktestDataError(
            "Selection has partially populated market-cap provenance; "
            f"missing={missing}"
        )
    if "market_cap" not in grok.columns:
        raise JQuantsBacktestDataError(
            "Legacy selection has neither market_cap nor provenance"
        )

    existing_cap = pd.to_numeric(grok["market_cap"], errors="coerce")
    enriched = attach_official_market_cap_asof(
        grok,
        daily_features,
        trading_calendar,
    )
    official_cap = pd.to_numeric(enriched["market_cap"], errors="coerce")
    null_mismatch = existing_cap.isna().ne(official_cap.isna())
    comparable = existing_cap.notna() & official_cap.notna()
    value_mismatch = comparable & ~pd.Series(
        np.isclose(
            existing_cap.astype(float),
            official_cap.astype(float),
            rtol=0.0,
            atol=0.5,
        ),
        index=grok.index,
    )
    if null_mismatch.any() or value_mismatch.any():
        raise JQuantsBacktestDataError(
            "Legacy selection market_cap differs from re-fetched official D-1 data"
        )
    validate_selection_market_cap(enriched, target_date, trading_calendar)
    print(
        "[OK] Fully legacy market-cap provenance upgraded in memory after exact "
        "official D-1 value comparison"
    )
    return enriched


def calculate_morning_metrics(
    df: pd.DataFrame,
    open_price: float
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    前場（9:00-11:30）のメトリクスを計算

    Args:
        df: 5分足データ
        open_price: 始値

    Returns:
        Tuple of (morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct)
    """
    if df.empty or open_price is None or open_price == 0:
        return None, None, None, None

    try:
        # 前場の時間帯でフィルタ (9:00-11:30)
        morning_data = df.between_time("09:00", "11:30")

        if morning_data.empty:
            return None, None, None, None

        morning_high = morning_data['High'].max()
        morning_low = morning_data['Low'].min()

        if pd.isna(morning_high) or pd.isna(morning_low):
            return None, None, None, None

        morning_max_gain_pct = ((morning_high - open_price) / open_price * 100)
        morning_max_drawdown_pct = ((morning_low - open_price) / open_price * 100)

        return morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct

    except Exception as e:
        print(f"[WARN] Failed to calculate morning metrics: {e}")
        return None, None, None, None


def calculate_daily_metrics(
    df: pd.DataFrame,
    open_price: float
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    全日（9:00-15:30）のメトリクスを計算

    Args:
        df: 5分足データ
        open_price: 始値

    Returns:
        Tuple of (high, low, daily_max_gain_pct, daily_max_drawdown_pct)
    """
    if df.empty or open_price is None or open_price == 0:
        return None, None, None, None

    try:
        high = df['High'].max()
        low = df['Low'].min()

        if pd.isna(high) or pd.isna(low):
            return None, None, None, None

        daily_max_gain_pct = ((high - open_price) / open_price * 100)
        daily_max_drawdown_pct = ((low - open_price) / open_price * 100)

        return high, low, daily_max_gain_pct, daily_max_drawdown_pct

    except Exception as e:
        print(f"[WARN] Failed to calculate daily metrics: {e}")
        return None, None, None, None


def calculate_segment_prices(
    minute_bars: pd.DataFrame,
    buy_price: float,
    daily_close: Optional[float] = None
) -> dict:
    """Calculate the 11 canonical J-Quants execution segments."""
    segments = calculate_segment_pnl(minute_bars, buy_price)
    if daily_close is not None:
        expected_close_pnl = (float(buy_price) - float(daily_close)) * 100.0
        if not abs(float(segments["seg_1530"]) - expected_close_pnl) <= 0.01:
            raise JQuantsBacktestDataError(
                "seg_1530 differs from the validated J-Quants daily close"
            )
    return segments


def calculate_phase3_return(
    df_5min: pd.DataFrame,
    open_price: float,
    profit_threshold: float,
    loss_threshold: float
) -> Tuple[Optional[float], Optional[bool], Optional[str]]:
    """
    Phase3（利確損切戦略）のリターンを計算

    Args:
        df_5min: 5分足データ
        open_price: 始値
        profit_threshold: 利確閾値（例: 0.03 = 3%）
        loss_threshold: 損切閾値（例: -0.03 = -3%）

    Returns:
        Tuple of (return, win, exit_reason)
    """
    if (
        df_5min.empty
        or open_price is None
        or open_price == 0
    ):
        return None, None, None

    try:
        # 時系列順にソート
        df_sorted = df_5min.sort_index()

        for idx, row in df_sorted.iterrows():
            high_price = row['High']
            low_price = row['Low']

            profit_hit = (open_price - low_price) / open_price >= profit_threshold
            loss_hit = (open_price - high_price) / open_price <= loss_threshold

            # A one-minute OHLC bar does not reveal which threshold was hit first.
            # Use the adverse outcome so the backtest never gains from that ambiguity.
            if profit_hit and loss_hit:
                return (
                    loss_threshold,
                    False,
                    "ambiguous_both_hit_stop_loss_conservative",
                )

            # ショートベース: 株価下落で利確（安値で判定）
            if profit_hit:
                phase_return = profit_threshold
                win = True
                exit_reason = f"profit_take_{profit_threshold*100}%"
                return phase_return, win, exit_reason

            # ショートベース: 株価上昇で損切（高値で判定）
            if loss_hit:
                phase_return = loss_threshold
                win = False
                exit_reason = f"stop_loss_{loss_threshold*100}%"
                return phase_return, win, exit_reason

        # 閾値に到達せず大引けまで保持
        close_price = df_sorted.iloc[-1]['Close']
        if pd.isna(close_price):
            return None, None, None
        phase_return = (open_price - close_price) / open_price
        win = phase_return > 0
        exit_reason = "hold_until_close"
        return phase_return, win, exit_reason

    except Exception as e:
        print(f"[WARN] Failed to calculate Phase3 return: {e}")
        return None, None, None


def fetch_backtest_data(ticker: str, backtest_date: datetime) -> dict:
    """Build one complete target-day row from official J-Quants inputs."""
    try:
        minute_bars = fetch_intraday_data(ticker, backtest_date)
        aggregate, prev_close = validate_daily_alignment(
            minute_bars,
            load_jquants_daily(),
            ticker,
            backtest_date,
        )

        buy_price = float(aggregate["Open"])
        daily_close = float(aggregate["Close"])
        high = float(aggregate["High"])
        low = float(aggregate["Low"])
        volume = float(aggregate["Volume"])
        value = float(aggregate["Value"])

        morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct = (
            calculate_morning_metrics(minute_bars, buy_price)
        )
        _, _, daily_max_gain_pct, daily_max_drawdown_pct = calculate_daily_metrics(
            minute_bars, buy_price
        )

        sell_price = session_last_close(minute_bars, "09:00", "11:30")
        if sell_price is None:
            phase1_return = None
            phase1_win = None
            profit_per_100_shares_phase1 = None
        else:
            phase1_return = (buy_price - sell_price) / buy_price
            phase1_win = phase1_return > 0
            profit_per_100_shares_phase1 = (buy_price - sell_price) * 100.0

        close_executable = has_trade_after_entry(minute_bars)
        phase2_return = (buy_price - daily_close) / buy_price
        phase2_win = phase2_return > 0
        profit_per_100_shares_phase2 = (buy_price - daily_close) * 100.0

        me_price = executable_exit(minute_bars, time(10, 25))["price"]
        profit_per_100_shares_morning_early = (
            (buy_price - me_price) * 100.0 if me_price is not None else None
        )
        ae_price = executable_exit(minute_bars, time(14, 45))["price"]
        profit_per_100_shares_afternoon_early = (
            (buy_price - ae_price) * 100.0 if ae_price is not None else None
        )

        phase3_results: dict[str, dict[str, Any]] = {}
        for threshold_pct in [1, 2, 3]:
            threshold = threshold_pct / 100.0
            phase_return, phase_win, exit_reason = calculate_phase3_return(
                minute_bars, buy_price, threshold, -threshold
            )
            phase3_results[f"phase3_{threshold_pct}pct"] = {
                "return": phase_return,
                "win": phase_win,
                "exit_reason": exit_reason,
                "profit_per_100_shares": (
                    phase_return * buy_price * 100.0
                    if phase_return is not None
                    else None
                ),
            }

        segment_prices = calculate_segment_prices(
            minute_bars, buy_price, daily_close
        )

        return {
            "prev_close": prev_close,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "daily_close": daily_close,
            "high": high,
            "low": low,
            "volume": volume,
            "Close": daily_close,
            "Volume": volume,
            "Value": value,
            "phase1_return": phase1_return,
            "phase1_win": phase1_win,
            "profit_per_100_shares_phase1": profit_per_100_shares_phase1,
            "phase2_return": phase2_return,
            "phase2_win": phase2_win,
            "profit_per_100_shares_phase2": profit_per_100_shares_phase2,
            "phase3_1pct_return": phase3_results["phase3_1pct"]["return"],
            "phase3_1pct_win": phase3_results["phase3_1pct"]["win"],
            "phase3_1pct_exit_reason": phase3_results["phase3_1pct"]["exit_reason"],
            "profit_per_100_shares_phase3_1pct": phase3_results["phase3_1pct"]["profit_per_100_shares"],
            "phase3_2pct_return": phase3_results["phase3_2pct"]["return"],
            "phase3_2pct_win": phase3_results["phase3_2pct"]["win"],
            "phase3_2pct_exit_reason": phase3_results["phase3_2pct"]["exit_reason"],
            "profit_per_100_shares_phase3_2pct": phase3_results["phase3_2pct"]["profit_per_100_shares"],
            "phase3_3pct_return": phase3_results["phase3_3pct"]["return"],
            "phase3_3pct_win": phase3_results["phase3_3pct"]["win"],
            "phase3_3pct_exit_reason": phase3_results["phase3_3pct"]["exit_reason"],
            "profit_per_100_shares_phase3_3pct": phase3_results["phase3_3pct"]["profit_per_100_shares"],
            "morning_high": morning_high,
            "morning_low": morning_low,
            "morning_max_gain_pct": morning_max_gain_pct,
            "morning_max_drawdown_pct": morning_max_drawdown_pct,
            "daily_max_gain_pct": daily_max_gain_pct,
            "daily_max_drawdown_pct": daily_max_drawdown_pct,
            "data_source": "jquants_1m",
            "phase1_mark_status": (
                "available"
                if sell_price is not None
                else "no_morning_price"
            ),
            "close_execution_status": (
                "executable" if close_executable else "mark_only_no_round_trip"
            ),
            "jquants_first_time": aggregate["first_time"],
            "jquants_last_time": aggregate["last_time"],
            "jquants_bar_count": aggregate["bar_count"],
            "jquants_price_validation": "minute_daily_ohlcv_match",
            "segment_definition": "first_executable_open_at_or_after_target_after_entry",
            "profit_per_100_shares_morning_early": profit_per_100_shares_morning_early,
            "profit_per_100_shares_afternoon_early": profit_per_100_shares_afternoon_early,
            **segment_prices,
        }
    except Exception as error:
        raise JQuantsBacktestDataError(
            f"Failed to build J-Quants backtest row for {ticker}: {error}"
        ) from error


def build_no_market_trade_backtest_data(
    ticker: str,
    backtest_date: datetime,
) -> dict[str, Any]:
    """Represent an official all-null daily row without inventing prices."""
    daily = load_jquants_daily()
    ticker_daily = daily[daily["ticker"].astype(str).eq(str(ticker))].copy()
    ticker_daily["date"] = pd.to_datetime(ticker_daily["date"], errors="coerce")
    prior = ticker_daily[
        ticker_daily["date"].dt.date.lt(backtest_date.date())
        & ticker_daily["Close"].notna()
    ].sort_values("date")
    prev_close = float(prior.iloc[-1]["Close"]) if not prior.empty else None

    result: dict[str, Any] = {
        "prev_close": prev_close,
        "buy_price": None,
        "sell_price": None,
        "daily_close": None,
        "high": None,
        "low": None,
        "volume": None,
        "Close": None,
        "Volume": None,
        "Value": None,
        "phase1_return": None,
        "phase1_win": None,
        "profit_per_100_shares_phase1": None,
        "phase2_return": None,
        "phase2_win": None,
        "profit_per_100_shares_phase2": None,
        "morning_high": None,
        "morning_low": None,
        "morning_max_gain_pct": None,
        "morning_max_drawdown_pct": None,
        "daily_max_gain_pct": None,
        "daily_max_drawdown_pct": None,
        "data_source": "jquants_no_market_trade",
        "phase1_mark_status": "no_market_trade",
        "close_execution_status": "no_market_trade",
        "jquants_first_time": None,
        "jquants_last_time": None,
        "jquants_bar_count": 0,
        "jquants_price_validation": "official_daily_no_market_trade_no_minute",
        "segment_definition": "no_market_trade",
        "profit_per_100_shares_morning_early": None,
        "profit_per_100_shares_afternoon_early": None,
    }
    for threshold_pct in [1, 2, 3]:
        prefix = f"phase3_{threshold_pct}pct"
        result[f"{prefix}_return"] = None
        result[f"{prefix}_win"] = None
        result[f"{prefix}_exit_reason"] = "no_market_trade"
        result[f"profit_per_100_shares_{prefix}"] = None
    result.update({column: None for column in SEGMENT_TARGETS})
    return result


def fetch_extreme_market_info(backtest_date: datetime) -> dict:
    """
    先物23:00時点の前日比を計算し、極端相場かどうかを判定

    Args:
        backtest_date: バックテスト日

    Returns:
        dict: {
            "futures_change_pct": float or None,
            "is_extreme_market": bool,
            "extreme_market_reason": str or None
        }
    """
    result = {
        "futures_change_pct": None,
        "is_extreme_market": False,
        "extreme_market_reason": None,
    }

    if not FUTURES_PATH.exists():
        print(f"[WARN] Futures file not found: {FUTURES_PATH}")
        return result

    try:
        df = pd.read_parquet(FUTURES_PATH)
        df["date"] = pd.to_datetime(df["date"])
        df["trade_date"] = df["date"].dt.date
        df["hour"] = df["date"].dt.hour
        df["minute"] = df["date"].dt.minute

        # 23:00付近のデータを抽出（22:55〜23:05）
        df_2300 = df[
            ((df["hour"] == 22) & (df["minute"] >= 55)) |
            ((df["hour"] == 23) & (df["minute"] <= 5))
        ]

        # 日付ごとに23:00に最も近いデータを取得
        prices = {}
        for trade_date, group in df_2300.groupby("trade_date"):
            group = group.copy()
            group["diff_to_2300"] = abs(group["hour"] * 60 + group["minute"] - 23 * 60)
            closest = group.loc[group["diff_to_2300"].idxmin()]
            prices[trade_date] = closest["Close"]

        # backtest_dateの前日と当日の23:00価格を取得
        backtest_date_obj = backtest_date.date()
        prev_date_obj = (backtest_date - timedelta(days=1)).date()

        # 前日を探す（土日祝を考慮して最大5日遡る）
        for i in range(5):
            check_date = (backtest_date - timedelta(days=i+1)).date()
            if check_date in prices:
                prev_date_obj = check_date
                break

        if prev_date_obj not in prices:
            print(f"[WARN] Previous day futures price not found for {backtest_date.date()}")
            return result

        # 当日の早朝価格（8:45-9:00）を取得
        df_morning = df[
            (df["trade_date"] == backtest_date_obj) &
            (df["hour"] == 8) & (df["minute"] >= 45)
        ]
        if df_morning.empty:
            df_morning = df[
                (df["trade_date"] == backtest_date_obj) &
                (df["hour"] == 9) & (df["minute"] <= 5)
            ]

        if df_morning.empty:
            print(f"[WARN] Morning futures price not found for {backtest_date.date()}")
            return result

        morning_price = df_morning.iloc[0]["Open"]
        if pd.isna(morning_price):
            print(f"[WARN] Morning futures Open is NaN for {backtest_date.date()}")
            return result
        prev_price = prices[prev_date_obj]

        # 変動率を計算
        change_pct = (morning_price - prev_price) / prev_price * 100
        result["futures_change_pct"] = round(change_pct, 2)

        # 極端相場の判定（±3%）
        if change_pct >= EXTREME_MARKET_THRESHOLD:
            result["is_extreme_market"] = True
            result["extreme_market_reason"] = "futures_3pct_up"
        elif change_pct <= -EXTREME_MARKET_THRESHOLD:
            result["is_extreme_market"] = True
            result["extreme_market_reason"] = "futures_3pct_down"

        print(f"[INFO] Futures change: {change_pct:+.2f}% (extreme: {result['is_extreme_market']})")
        return result

    except Exception as e:
        print(f"[WARN] Failed to fetch extreme market info: {e}")
        return result


def run_backtest() -> pd.DataFrame:
    """
    grok_trending.parquetのバックテストを実行

    Returns:
        pd.DataFrame: バックテスト結果
    """
    print("=" * 80)
    print("Grok Trending Backtest")
    print("=" * 80)

    # 1. S3からgrok_trending.parquetをダウンロード
    cfg = load_s3_config()
    if not cfg:
        print("[ERROR] S3 not configured")
        return pd.DataFrame()

    s3_key = "grok_trending.parquet"
    print(f"[INFO] Downloading from S3: {s3_key}")
    if not download_file(cfg, s3_key, GROK_TRENDING_PATH):
        print(f"[ERROR] Failed to download grok_trending.parquet from S3")
        return pd.DataFrame()

    df_grok = pd.read_parquet(GROK_TRENDING_PATH)
    print(f"[OK] Loaded {len(df_grok)} stocks from grok_trending.parquet")

    if df_grok.empty:
        print("[WARN] No stocks in grok_trending.parquet")
        return pd.DataFrame()

    # 2. dateは選定日ではなく、売買・検証の対象日
    target_date_value = df_grok['date'].iloc[0] if 'date' in df_grok.columns else None

    if target_date_value is None or pd.isna(target_date_value):
        print("[ERROR] 'date' column not found in grok_trending.parquet")
        return pd.DataFrame()

    target_dates = pd.to_datetime(df_grok["date"], errors="raise").dt.normalize()
    if target_dates.nunique() != 1:
        raise JQuantsBacktestDataError(
            "grok_trending.parquet contains more than one target date"
        )
    target_date = target_dates.iloc[0].to_pydatetime()
    print(f"[INFO] Target date: {target_date.date()}")

    backtest_date = target_date
    print(f"[INFO] Backtest date: {backtest_date.date()}")

    # 3. 当日の全選定銘柄について、保存前にJ-Quants入力を一括検査
    validate_selection_asof(df_grok, backtest_date)
    if not TRADING_CALENDAR_PATH.exists():
        raise FileNotFoundError(
            f"Trading calendar not found: {TRADING_CALENDAR_PATH}"
        )
    if not JQUANTS_DAILY_FEATURES_PATH.exists():
        raise FileNotFoundError(
            "J-Quants daily field sidecar not found: "
            f"{JQUANTS_DAILY_FEATURES_PATH}"
        )
    trading_calendar = pd.read_parquet(TRADING_CALENDAR_PATH)
    daily_features = pd.read_parquet(JQUANTS_DAILY_FEATURES_PATH)
    df_grok = validate_or_attach_market_cap_provenance(
        df_grok,
        backtest_date,
        daily_features,
        trading_calendar,
    )
    validate_target_daily_corporate_actions(
        df_grok,
        backtest_date,
        daily_features,
    )
    ex_rights_count = int(
        pd.to_numeric(
            daily_features.loc[
                pd.to_datetime(
                    daily_features["trading_date"], errors="coerce"
                ).dt.normalize().eq(pd.Timestamp(backtest_date).normalize())
                & daily_features["ticker"].astype(str).isin(
                    set(df_grok["ticker"].astype(str))
                ),
                "jq_ex_rights_type",
            ],
            errors="coerce",
        ).notna().sum()
    )
    print(
        "[OK] Official market-cap/corporate-action preflight: "
        f"D-1 MktCap={len(df_grok)}/{len(df_grok)}, "
        f"target-day ExRT events={ex_rights_count}"
    )
    no_market_tickers = validate_batch_coverage(
        df_grok,
        backtest_date,
        daily_features,
    )

    # 4. 各銘柄のバックテストを実行
    results = []
    failures: list[str] = []

    for idx, row in df_grok.iterrows():
        ticker = row['ticker']
        print(f"[{idx+1}/{len(df_grok)}] Processing {ticker}...", end=" ", flush=True)

        try:
            if ticker in no_market_tickers:
                backtest_data = build_no_market_trade_backtest_data(
                    ticker,
                    backtest_date,
                )
            else:
                backtest_data = fetch_backtest_data(ticker, backtest_date)
        except Exception as error:
            failures.append(f"{ticker}: {error}")
            print(f"FAILED ({error})")
            continue

        # 取引制限情報を取得
        trading_restrictions = get_trading_restriction_info(ticker)

        # デイトレード情報を取得（grok_day_trade_list.parquet）
        day_trade_info = get_day_trade_info(ticker)

        result = {
            # Preserve all selection-time fields used by current/future models.
            **row.to_dict(),
            # Historical column name retained; its value is the target date.
            "selection_date": target_date.strftime("%Y-%m-%d"),
            "backtest_date": backtest_date.strftime("%Y-%m-%d"),
            "ticker": ticker,
            "stock_name": row.get("stock_name", ""),
            "categories": row.get("tags", "").split(",")[0] if row.get("tags") else "",
            "reason": row.get("reason", ""),
            "grok_rank": row.get("grok_rank", idx + 1),
            "selection_score": row.get("selection_score", 0),
            "price_diff": row.get("price_diff"),
            **backtest_data,
            "prompt_version": row.get("prompt_version", "v1_1_web_search"),
            # 取引制限カラム（margin_code_master.parquet）
            "margin_code": trading_restrictions['margin_code'],
            "margin_code_name": trading_restrictions['margin_code_name'],
            "jsf_restricted": trading_restrictions['jsf_restricted'],
            "is_shortable": trading_restrictions['is_shortable'],
            # デイトレード情報（grok_day_trade_list.parquet）
            "shortable": day_trade_info['shortable'],
            "day_trade": day_trade_info['day_trade'],
            "ng": day_trade_info['ng'],
            "day_trade_available_shares": day_trade_info['day_trade_available_shares'],
            # 売り残・買い残（grok_trending.parquetから直接取得、日付×銘柄で管理）
            "margin_sell_balance": row.get("margin_sell_balance"),
            "margin_buy_balance": row.get("margin_buy_balance"),
            # 価格制限・成行コスト（grok_trending.parquetから）
            "price_limit": row.get("price_limit"),
            "limit_price_upper": row.get("limit_price_upper"),
            "max_cost_100": row.get("max_cost_100"),
            # 指標カラム（generate_grok_trending.pyで計算）
            "rsi9": row.get("rsi9"),
            "atr14_pct": row.get("atr14_pct"),
            "vol_ratio": row.get("vol_ratio"),
            "weekday": row.get("weekday"),
            # 極端相場情報（23:00選定時にgrok_trending.parquetで計算）
            "nikkei_change_pct": row.get("nikkei_change_pct"),
            "futures_change_pct": row.get("futures_change_pct"),
            "is_extreme_market": row.get("is_extreme_market"),
            "extreme_market_reason": row.get("extreme_market_reason"),
            # ML予測（23:00 pipeline で grok_trending.parquet に付与済み）
            # 日次本番予測はWFCV検証用のml_probと混ぜない
            "ml_prob_live": row.get("ml_prob_live", row.get("ml_prob", row.get("prob_up"))),
            "ml_prob_source": "live",
        }

        results.append(result)
        shortable_mark = "○" if trading_restrictions['is_shortable'] else "✗"
        phase1_text = (
            f"{backtest_data['phase1_return'] * 100:+.2f}%"
            if backtest_data["phase1_return"] is not None
            else "N/A"
        )
        phase2_text = (
            f"{backtest_data['phase2_return'] * 100:+.2f}%"
            if backtest_data["phase2_return"] is not None
            else "N/A"
        )
        print(
            f"OK (Phase1: {phase1_text}, "
            f"Phase2: {phase2_text}, "
            f"Short: {shortable_mark})"
        )

    if failures:
        details = "\n  - ".join(failures)
        raise JQuantsBacktestDataError(
            "One or more selected tickers failed; no derived rows will be saved:\n  - "
            + details
        )

    if not results:
        print("[WARN] No backtest results generated")
        return pd.DataFrame()

    df_results = pd.DataFrame(results)
    expected_tickers = set(df_grok["ticker"].astype(str))
    actual_tickers = set(df_results["ticker"].astype(str))
    if len(df_results) != len(df_grok) or actual_tickers != expected_tickers:
        raise JQuantsBacktestDataError(
            "Generated batch does not exactly match the selected ticker universe"
        )
    print(f"\n[OK] Generated backtest results for {len(df_results)} stocks")

    # 4. 統計を表示
    print("\n" + "=" * 80)
    print("Backtest Summary")
    print("=" * 80)
    print(f"Phase1 (前場引け売り):")
    print(f"  Win rate: {df_results['phase1_win'].mean()*100:.1f}%")
    print(f"  Avg return: {df_results['phase1_return'].mean()*100:+.2f}%")
    print(f"Phase2 (大引け売り):")
    print(f"  Win rate: {df_results['phase2_win'].mean()*100:.1f}%")
    print(f"  Avg return: {df_results['phase2_return'].mean()*100:+.2f}%")
    print(f"Phase3-3% (±3%利確損切):")
    phase3_3pct_win_rate = df_results['phase3_3pct_win'].mean() * 100 if df_results['phase3_3pct_win'].notna().any() else 0
    phase3_3pct_avg_return = df_results['phase3_3pct_return'].mean() * 100 if df_results['phase3_3pct_return'].notna().any() else 0
    print(f"  Win rate: {phase3_3pct_win_rate:.1f}%")
    print(f"  Avg return: {phase3_3pct_avg_return:+.2f}%")
    print("-" * 80)
    print(f"Trading Restrictions:")
    print(f"  Total stocks: {len(df_results)}")
    print(f"  Shortable (貸借+JSF OK): {df_results['is_shortable'].sum()}")
    print(f"  Margin only (信用): {len(df_results[df_results['margin_code'] == '1'])}")
    print(f"  JSF restricted: {df_results['jsf_restricted'].sum()}")
    print("=" * 80)

    return df_results


def assert_parquet_roundtrip_equal(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
) -> None:
    """Require identical shape, missing positions, and non-missing values."""
    left = expected.reset_index(drop=True)
    right = actual.reset_index(drop=True)
    if list(left.columns) != list(right.columns):
        raise AssertionError(
            f"column order changed: expected={list(left.columns)}, "
            f"actual={list(right.columns)}"
        )
    if left.shape != right.shape:
        raise AssertionError(
            f"shape changed: expected={left.shape}, actual={right.shape}"
        )

    left_missing = left.isna()
    right_missing = right.isna()
    pd.testing.assert_frame_equal(
        left_missing,
        right_missing,
        check_dtype=False,
        check_exact=True,
    )
    for column in left.columns:
        present = ~left_missing[column]
        if not present.any():
            continue
        pd.testing.assert_series_equal(
            left.loc[present, column].reset_index(drop=True),
            right.loc[present, column].reset_index(drop=True),
            check_dtype=False,
            check_exact=True,
            check_categorical=False,
        )


def save_to_archive(df: pd.DataFrame, backtest_date: str) -> None:
    """Validate, conditionally publish, then install one complete J-Quants day."""
    cfg = load_s3_config()
    if not cfg or not cfg.bucket:
        raise RuntimeError("S3 is not configured; canonical archive publish refused")

    if df.empty:
        raise JQuantsBacktestDataError("Cannot save an empty derived result batch")
    if df[["backtest_date", "ticker"]].duplicated().any():
        raise JQuantsBacktestDataError("Result batch has duplicate ticker-date keys")
    target = pd.Timestamp(backtest_date).strftime("%Y-%m-%d")
    result_dates = pd.to_datetime(df["backtest_date"], errors="raise").dt.strftime(
        "%Y-%m-%d"
    )
    if not result_dates.eq(target).all():
        raise JQuantsBacktestDataError("Result batch contains a non-target date")
    if not TRADING_CALENDAR_PATH.exists() or not JQUANTS_DAILY_FEATURES_PATH.exists():
        raise JQuantsBacktestDataError(
            "Calendar/daily sidecar disappeared before canonical archive publish"
        )
    trading_calendar = pd.read_parquet(TRADING_CALENDAR_PATH)
    daily_features = pd.read_parquet(JQUANTS_DAILY_FEATURES_PATH)
    validate_selection_market_cap(df, target, trading_calendar)
    validate_target_daily_corporate_actions(
        df,
        target,
        daily_features,
    )
    required_values = ["phase1_mark_status", "close_execution_status"]
    missing_required = [
        column
        for column in required_values
        if column not in df.columns or df[column].isna().any()
    ]
    if missing_required:
        raise JQuantsBacktestDataError(
            f"Result batch has missing canonical values: {missing_required}"
        )
    no_market = (
        df["phase1_mark_status"].eq("no_market_trade")
        & df["close_execution_status"].eq("no_market_trade")
    )
    traded = ~no_market
    if df.loc[traded, ["buy_price", "daily_close"]].isna().any().any():
        raise JQuantsBacktestDataError(
            "Traded result batch has missing buy/daily close values"
        )
    if not df.loc[traded, "data_source"].eq("jquants_1m").all():
        raise JQuantsBacktestDataError("Non-J-Quants traded result row detected")
    if not df.loc[no_market, "data_source"].eq(
        "jquants_no_market_trade"
    ).all():
        raise JQuantsBacktestDataError(
            "Official no-market row has an invalid data source"
        )

    phase1_columns = [
        "sell_price",
        "phase1_return",
        "phase1_win",
        "profit_per_100_shares_phase1",
    ]
    close_columns = [
        "phase2_return",
        "phase2_win",
        "profit_per_100_shares_phase2",
        "seg_1530",
    ]
    for threshold in ["1pct", "2pct", "3pct"]:
        close_columns.extend(
            [
                f"phase3_{threshold}_return",
                f"phase3_{threshold}_win",
                f"phase3_{threshold}_exit_reason",
                f"profit_per_100_shares_phase3_{threshold}",
            ]
        )

    phase1_exec = df["phase1_mark_status"].eq("available")
    phase1_no_trade = df["phase1_mark_status"].eq("no_morning_price")
    close_exec = df["close_execution_status"].eq("executable")
    close_no_trade = df["close_execution_status"].eq("mark_only_no_round_trip")
    if not (phase1_exec | phase1_no_trade | no_market).all():
        raise JQuantsBacktestDataError("Unknown Phase1 mark status")
    if not (close_exec | close_no_trade | no_market).all():
        raise JQuantsBacktestDataError("Unknown close execution status")
    if df.loc[phase1_exec, phase1_columns].isna().any().any() or df.loc[
        phase1_no_trade, phase1_columns
    ].notna().any().any():
        raise JQuantsBacktestDataError(
            "Phase1 values are inconsistent with execution status"
        )
    if df.loc[traded, close_columns].isna().any().any():
        raise JQuantsBacktestDataError(
            "Hypothetical close/Phase3 values must be present for every traded row"
        )
    no_market_null_columns = [
        "buy_price",
        "sell_price",
        "daily_close",
        "high",
        "low",
        "volume",
        "Close",
        "Volume",
        "Value",
        *phase1_columns,
        "phase2_return",
        "phase2_win",
        "profit_per_100_shares_phase2",
        *SEGMENT_TARGETS,
        "profit_per_100_shares_morning_early",
        "profit_per_100_shares_afternoon_early",
    ]
    for threshold in ["1pct", "2pct", "3pct"]:
        no_market_null_columns.extend(
            [
                f"phase3_{threshold}_return",
                f"phase3_{threshold}_win",
                f"profit_per_100_shares_phase3_{threshold}",
            ]
        )
    if df.loc[no_market, no_market_null_columns].notna().any().any():
        raise JQuantsBacktestDataError(
            "Official no-market row contains invented price or P&L values"
        )
    no_market_exit_columns = [
        f"phase3_{threshold}_exit_reason"
        for threshold in ["1pct", "2pct", "3pct"]
    ]
    if not df.loc[no_market, no_market_exit_columns].eq(
        "no_market_trade"
    ).all().all():
        raise JQuantsBacktestDataError(
            "Official no-market row lacks explicit Phase3 no-trade reasons"
        )
    if (
        pd.to_numeric(df.loc[no_market, "jquants_bar_count"], errors="coerce")
        .fillna(-1)
        .ne(0)
        .any()
    ):
        raise JQuantsBacktestDataError(
            "Official no-market row must have zero J-Quants bars"
        )

    with TemporaryDirectory(prefix="grok-archive-", dir=BACKTEST_DIR) as temp_dir:
        temp_root = Path(temp_dir)
        source_path = temp_root / "source.parquet"
        dated_candidate_path = temp_root / "derived-day.parquet"
        archive_candidate_path = temp_root / "candidate.parquet"
        readback_path = temp_root / "s3-readback.parquet"

        print("[INFO] Downloading checksum-pinned canonical archive from S3")
        source_state = download_verified_archive(cfg, source_path)
        source_archive = pd.read_parquet(source_path)

        derived_rows = build_derived_backtest_rows(
            source_archive,
            df,
            target,
            daily_features,
        )
        date_str = target.replace("-", "")
        dated_file = BACKTEST_DIR / f"grok_trending_{date_str}.parquet"
        derived_rows.to_parquet(dated_candidate_path, index=False)

        dated_reloaded = pd.read_parquet(dated_candidate_path)
        if len(dated_reloaded) != len(derived_rows):
            raise JQuantsBacktestDataError(
                "Derived daily row count changed after parquet serialization"
            )
        if dated_reloaded[["backtest_date", "ticker"]].duplicated().any():
            raise JQuantsBacktestDataError(
                "Derived daily artifact contains duplicate keys after serialization"
            )
        derived_dates = pd.to_datetime(
            dated_reloaded["backtest_date"], errors="raise"
        )
        if not derived_dates.dt.strftime("%Y-%m-%d").eq(target).all():
            raise JQuantsBacktestDataError(
                "Derived daily artifact contains a non-target date"
            )
        try:
            assert_parquet_roundtrip_equal(derived_rows, dated_reloaded)
        except AssertionError as error:
            raise JQuantsBacktestDataError(
                f"Derived daily artifact changed after serialization: {error}"
            ) from error

        canonical_rows = dated_reloaded.loc[:, source_archive.columns].copy()
        candidate = merge_archive_date(source_archive, canonical_rows, target)
        candidate.to_parquet(archive_candidate_path, index=False)

        archive_reloaded = pd.read_parquet(archive_candidate_path)
        if len(archive_reloaded) != len(candidate):
            raise JQuantsBacktestDataError(
                "Candidate archive row count changed after parquet serialization"
            )
        if archive_reloaded[["backtest_date", "ticker"]].duplicated().any():
            raise JQuantsBacktestDataError(
                "Candidate archive contains duplicate keys after serialization"
            )
        assert_archive_history_unchanged(source_archive, archive_reloaded, target)
        assert_archive_target_rows_preserved(
            canonical_rows,
            archive_reloaded,
            target,
        )
        candidate_dates = pd.to_datetime(
            archive_reloaded["backtest_date"], errors="raise"
        )
        expected_rows = len(source_archive) - int(
            pd.to_datetime(source_archive["backtest_date"], errors="raise")
            .dt.strftime("%Y-%m-%d")
            .eq(target)
            .sum()
        ) + len(canonical_rows)
        if len(archive_reloaded) != expected_rows:
            raise JQuantsBacktestDataError(
                "Candidate archive row count mismatch: "
                f"{len(archive_reloaded)} != {expected_rows}"
            )

        os.replace(dated_candidate_path, dated_file)
        print(f"[OK] Saved dated J-Quants day: {dated_file}")

        s3_key_dated = f"backtest/grok_trending_{date_str}.parquet"
        if not upload_file(cfg, dated_file, s3_key_dated):
            raise RuntimeError(f"Failed to upload dated artifact: {s3_key_dated}")
        if not download_file(cfg, s3_key_dated, readback_path):
            raise RuntimeError(f"Failed to read back dated artifact: {s3_key_dated}")
        if file_sha256(readback_path) != file_sha256(dated_file):
            raise RuntimeError(f"S3 readback checksum mismatch: {s3_key_dated}")

        publish_state = publish_guarded_archive(
            cfg,
            archive_candidate_path,
            source_state,
            backtest_date=target,
            row_count=len(archive_reloaded),
        )
        publish_state.update(
            {
                "date_min": candidate_dates.min().date().isoformat(),
                "date_max": candidate_dates.max().date().isoformat(),
                "unique_ticker_date_keys": int(
                    archive_reloaded[["ticker", "backtest_date"]]
                    .drop_duplicates()
                    .shape[0]
                ),
                "columns": archive_reloaded.columns.tolist(),
            }
        )
        manifest_state = publish_guarded_manifest_entry(
            cfg,
            source_state,
            publish_state,
            columns=archive_reloaded.columns.tolist(),
            date_min=publish_state["date_min"],
            date_max=publish_state["date_max"],
            unique_ticker_date_keys=publish_state["unique_ticker_date_keys"],
        )
        publish_state.update(manifest_state)
        write_publish_state(ARCHIVE_PUBLISH_STATE_PATH, publish_state)

        # Install locally only after S3 has accepted and verified the guarded write.
        os.replace(archive_candidate_path, BACKTEST_ARCHIVE_PATH)

    print(f"[OK] Guarded canonical archive publish: {BACKTEST_ARCHIVE_PATH}")
    print(f"     Total records: {len(archive_reloaded)}")
    print(
        f"     Date range: {candidate_dates.min().date()} "
        f"to {candidate_dates.max().date()}"
    )
    print(f"     S3 VersionId: {publish_state.get('s3_version_id')}")


def main() -> int:
    """メイン処理"""
    try:
        # 1. バックテスト実行
        df_results = run_backtest()

        if df_results.empty:
            print("[ERROR] No backtest results to save")
            return 1

        # 2. アーカイブに保存
        backtest_date = df_results['backtest_date'].iloc[0]
        save_to_archive(df_results, backtest_date)

        print("\n" + "=" * 80)
        print("✅ Backtest completed and archived successfully!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\n[ERROR] Backtest failed: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
