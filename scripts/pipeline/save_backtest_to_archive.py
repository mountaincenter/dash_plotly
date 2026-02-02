#!/usr/bin/env python3
"""
save_backtest_to_archive.py
Grok trending銘柄のバックテスト結果をアーカイブに保存

実行方法:
    # パイプライン実行（16時更新 - GitHub Actions）
    python3 scripts/pipeline/save_backtest_to_archive.py

機能:
    - grok_trending.parquet を読み込み
    - 翌営業日の株価データを取得（yfinance）
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
from pathlib import Path
from datetime import datetime, timedelta, time
from typing import Optional, Tuple, Any
import traceback
import time as time_module

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file, download_file
from common_cfg.s3cfg import load_s3_config
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher

# パス定義
BACKTEST_DIR = PARQUET_DIR / "backtest"
BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
GROK_TRENDING_PATH = BACKTEST_DIR / "grok_trending_temp.parquet"
BACKTEST_ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"
FUTURES_PATH = PARQUET_DIR / "futures_prices_60d_5m.parquet"

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

# J-Quants クライアント（グローバル）
_jquants_client: Optional[JQuantsClient] = None


def get_jquants_client() -> JQuantsClient:
    """J-Quantsクライアントを取得（シングルトン）"""
    global _jquants_client
    if _jquants_client is None:
        _jquants_client = JQuantsClient()
    return _jquants_client


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


def fetch_market_cap(ticker: str, close_price: float, date: datetime) -> Optional[float]:
    """
    J-Quants APIを使用して時価総額を取得

    Args:
        ticker: 銘柄コード (例: "7203.T")
        close_price: 終値
        date: 取得日

    Returns:
        時価総額（円）、または取得失敗時はNone
    """
    try:
        # ティッカーからコードを抽出（"7203.T" → "72030"）
        code = ticker.replace('.T', '').ljust(5, '0')

        client = get_jquants_client()

        # v2: /fins/summary から発行済株式数を取得（v1は/fins/statements）
        statements_response = client.request('/fins/summary', params={'code': code})

        if 'data' not in statements_response or not statements_response['data']:
            return None

        # 最新のデータを取得（v2: DiscDate、v1はDisclosedDate）
        statements = sorted(
            statements_response['data'],
            key=lambda x: x.get('DiscDate', ''),
            reverse=True
        )

        issued_shares = None
        for statement in statements:
            # v2: ShOutFY = 発行済株式数（期末）
            issued_shares = statement.get('ShOutFY')
            if issued_shares:
                issued_shares = float(issued_shares)  # 文字列からfloatに変換
                break

        if not issued_shares:
            return None

        # v2: /equities/bars/daily から調整係数を取得（v1は/prices/daily_quotes）
        date_str = date.strftime('%Y-%m-%d')
        quotes_response = client.request('/equities/bars/daily', params={'code': code, 'from': date_str, 'to': date_str})

        if 'data' not in quotes_response or not quotes_response['data']:
            return None

        adjustment_factor = float(quotes_response['data'][0].get('AdjustmentFactor', 1.0))

        # 3. 時価総額を計算
        # 時価総額 = 終値 × (発行済株式数 / 調整係数)
        market_cap = close_price * (issued_shares / adjustment_factor)

        return market_cap

    except Exception as e:
        print(f"[WARN] Failed to fetch market cap for {ticker}: {e}")
        return None


def fetch_intraday_data(ticker: str, date: datetime) -> Optional[pd.DataFrame]:
    """
    yfinanceを使用して5分足の株価データを取得

    Args:
        ticker: 銘柄コード (例: "9984.T")
        date: 取得する日付

    Returns:
        5分足データのDataFrame、または取得失敗時はNone
    """
    try:
        # 日付の前後2日分のデータを取得（余裕を持たせる）
        start_date = date - timedelta(days=2)
        end_date = date + timedelta(days=2)

        stock = yf.Ticker(ticker)
        df = stock.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="5m"
        )

        if df.empty:
            return None

        # タイムゾーンを除去（JST前提）
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # 指定日のデータのみを抽出
        target_date = pd.Timestamp(date.date())
        df_target = df[df.index.date == target_date.date()]

        return df_target if not df_target.empty else None

    except Exception as e:
        print(f"[WARN] Failed to fetch intraday data for {ticker} on {date.date()}: {e}")
        return None


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

        daily_max_gain_pct = ((high - open_price) / open_price * 100)
        daily_max_drawdown_pct = ((low - open_price) / open_price * 100)

        return high, low, daily_max_gain_pct, daily_max_drawdown_pct

    except Exception as e:
        print(f"[WARN] Failed to calculate daily metrics: {e}")
        return None, None, None, None


def get_price_at_time(
    df_5min: pd.DataFrame,
    start_time: str,
    end_time: str,
    fallback_start_time: Optional[str] = None
) -> Optional[float]:
    """
    指定時間帯の終値を取得。データがなければ次時間帯の最早Openを返す。

    Args:
        df_5min: 5分足データ
        start_time: 開始時刻 (例: "09:00", "12:30")
        end_time: 終了時刻 (例: "10:25", "13:55")
        fallback_start_time: フォールバック開始時刻 (例: "10:30", "14:00")

    Returns:
        価格、またはデータなしの場合None
    """
    if df_5min is None or df_5min.empty:
        return None

    try:
        # 指定時間帯のデータを取得
        slot_data = df_5min.between_time(start_time, end_time)

        if not slot_data.empty:
            return float(slot_data.iloc[-1]['Close'])

        # フォールバック: 次時間帯の最早Open
        if fallback_start_time:
            next_data = df_5min.between_time(fallback_start_time, "15:30")
            if not next_data.empty:
                return float(next_data.iloc[0]['Open'])

        return None

    except Exception as e:
        print(f"[WARN] Failed to get price at {start_time}-{end_time}: {e}")
        return None


# 11セグメント時刻定義
SEGMENT_TIMES = [
    ("seg_0930", "09:25", "09:30"),  # 09:30時点
    ("seg_1000", "09:55", "10:00"),  # 10:00時点
    ("seg_1030", "10:25", "10:30"),  # 10:30時点
    ("seg_1100", "10:55", "11:00"),  # 11:00時点
    ("seg_1130", "11:25", "11:30"),  # 11:30時点（前場引け）
    ("seg_1300", "12:55", "13:00"),  # 13:00時点（後場寄り）
    ("seg_1330", "13:25", "13:30"),  # 13:30時点
    ("seg_1400", "13:55", "14:00"),  # 14:00時点
    ("seg_1430", "14:25", "14:30"),  # 14:30時点
    ("seg_1500", "14:55", "15:00"),  # 15:00時点
    ("seg_1530", "15:25", "15:30"),  # 15:30時点（大引け）
]


def calculate_segment_prices(
    df_5min: pd.DataFrame,
    buy_price: float
) -> dict:
    """
    11セグメントの価格を計算

    Args:
        df_5min: 5分足データ
        buy_price: 始値（買値）

    Returns:
        dict: {seg_0930: 利益, seg_1000: 利益, ...}
    """
    segments = {}

    if df_5min is None or df_5min.empty or buy_price is None or buy_price == 0:
        for seg_name, _, _ in SEGMENT_TIMES:
            segments[seg_name] = None
        return segments

    for seg_name, start_time, end_time in SEGMENT_TIMES:
        try:
            # 指定時刻付近のデータを取得
            slot_data = df_5min.between_time(start_time, end_time)

            if not slot_data.empty:
                price = float(slot_data.iloc[-1]['Close'])
                # 100株あたりの利益（円）
                segments[seg_name] = (price - buy_price) * 100
            else:
                segments[seg_name] = None
        except Exception:
            segments[seg_name] = None

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
    if df_5min.empty or open_price is None or open_price == 0:
        return None, None, None

    try:
        # 時系列順にソート
        df_sorted = df_5min.sort_index()

        for idx, row in df_sorted.iterrows():
            high_price = row['High']
            low_price = row['Low']

            # 利確判定（高値で判定）
            if (high_price - open_price) / open_price >= profit_threshold:
                phase_return = profit_threshold
                win = True
                exit_reason = f"profit_take_{profit_threshold*100}%"
                return phase_return, win, exit_reason

            # 損切判定（安値で判定）
            if (low_price - open_price) / open_price <= loss_threshold:
                phase_return = loss_threshold
                win = False
                exit_reason = f"stop_loss_{loss_threshold*100}%"
                return phase_return, win, exit_reason

        # 閾値に到達せず大引けまで保持
        close_price = df_sorted.iloc[-1]['Close']
        phase_return = (close_price - open_price) / open_price
        win = phase_return > 0
        exit_reason = "hold_until_close"
        return phase_return, win, exit_reason

    except Exception as e:
        print(f"[WARN] Failed to calculate Phase3 return: {e}")
        return None, None, None


def fetch_backtest_data(ticker: str, backtest_date: datetime, prev_trading_day: str | None = None) -> Optional[dict]:
    """
    バックテスト用の株価データを取得

    Args:
        ticker: 銘柄コード (例: "6526.T")
        backtest_date: バックテスト日（翌営業日）
        prev_trading_day: 前営業日（YYYY-MM-DD形式）。Noneの場合はJ-Quantsから取得

    Returns:
        dict: バックテストデータ
    """
    try:
        # 前営業日が未指定の場合はJ-Quantsから取得
        if prev_trading_day is None:
            fetcher = JQuantsFetcher()
            prev_trading_day = fetcher.get_previous_trading_day(backtest_date.date())

        # 日次データを取得（前営業日を含める）
        stock = yf.Ticker(ticker)
        if prev_trading_day:
            start_date = prev_trading_day
        else:
            start_date = (backtest_date - timedelta(days=5)).strftime("%Y-%m-%d")
        end_date = (backtest_date + timedelta(days=2)).strftime("%Y-%m-%d")

        # リトライロジック（GitHub Actions環境でのAPI制限対策）
        hist_daily = pd.DataFrame()
        max_retries = 3
        for attempt in range(max_retries):
            try:
                hist_daily = stock.history(start=start_date, end=end_date, interval="1d")
                if not hist_daily.empty:
                    break
                if attempt < max_retries - 1:
                    wait_sec = (attempt + 1) * 2
                    print(f"[WARN] {ticker}: Empty response, retry {attempt + 1}/{max_retries} after {wait_sec}s")
                    time_module.sleep(wait_sec)
            except Exception as e:
                print(f"[WARN] {ticker}: yfinance error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time_module.sleep((attempt + 1) * 2)

        # デバッグログ: yfinanceの戻り値を確認
        print(f"[DEBUG] {ticker}: yfinance query start={start_date}, end={end_date}")
        print(f"[DEBUG] {ticker}: hist_daily.empty={hist_daily.empty}, shape={hist_daily.shape}")
        if not hist_daily.empty:
            print(f"[DEBUG] {ticker}: hist_daily.index={list(hist_daily.index)}")

        if hist_daily.empty:
            print(f"[DEBUG] {ticker}: FAIL - hist_daily is empty after {max_retries} retries")
            return None

        # インデックスをdate型に変換
        hist_daily.index = pd.to_datetime(hist_daily.index).date
        backtest_date_obj = backtest_date.date()

        print(f"[DEBUG] {ticker}: backtest_date_obj={backtest_date_obj}, index after conversion={list(hist_daily.index)}")

        if backtest_date_obj not in hist_daily.index:
            print(f"[DEBUG] {ticker}: FAIL - backtest_date_obj not in index")
            return None

        daily_row = hist_daily.loc[backtest_date_obj]

        # 前営業日終値を取得（J-Quantsカレンダーベース）
        prev_close = None
        if prev_trading_day:
            prev_date_obj = datetime.strptime(prev_trading_day, "%Y-%m-%d").date()
            if prev_date_obj in hist_daily.index:
                prev_close = float(hist_daily.loc[prev_date_obj]['Close'])

        buy_price = float(daily_row['Open'])
        sell_price = float(daily_row['Close'])  # Phase1用（前場引け値として近似）
        daily_close = float(daily_row['Close'])
        high = float(daily_row['High'])
        low = float(daily_row['Low'])
        volume = int(daily_row['Volume'])

        # 5分足データを取得
        df_5min = fetch_intraday_data(ticker, backtest_date)

        # 前場メトリクス計算
        morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct = calculate_morning_metrics(
            df_5min, buy_price
        ) if df_5min is not None else (None, None, None, None)

        # 全日メトリクス計算
        high_calc, low_calc, daily_max_gain_pct, daily_max_drawdown_pct = calculate_daily_metrics(
            df_5min, buy_price
        ) if df_5min is not None else (None, None, None, None)

        # 時価総額を取得
        market_cap = fetch_market_cap(ticker, daily_close, backtest_date)

        # Phase1: 前場引け売り（11:30売却）
        if df_5min is not None:
            morning_data = df_5min.between_time("09:00", "11:30")
            if not morning_data.empty:
                sell_price = float(morning_data.iloc[-1]['Close'])  # 11:30の終値
        phase1_return = (sell_price - buy_price) / buy_price
        phase1_win = phase1_return > 0
        profit_per_100_shares_phase1 = (sell_price - buy_price) * 100

        # Phase2: 大引け売り（15:30売却）
        phase2_return = (daily_close - buy_price) / buy_price
        phase2_win = phase2_return > 0
        profit_per_100_shares_phase2 = (daily_close - buy_price) * 100

        # 前場前半 (me): 10:25売却 (09:00-10:25)
        me_price = get_price_at_time(df_5min, "09:00", "10:25", "10:30")
        if me_price is not None and buy_price > 0:
            profit_per_100_shares_morning_early = (me_price - buy_price) * 100
        else:
            profit_per_100_shares_morning_early = None

        # 後場前半 (ae): 14:45売却 (12:30-14:45)
        ae_price = get_price_at_time(df_5min, "12:30", "14:45", "14:50")
        if ae_price is not None and buy_price > 0:
            profit_per_100_shares_afternoon_early = (ae_price - buy_price) * 100
        else:
            profit_per_100_shares_afternoon_early = None

        # Phase3: ±1%/2%/3% 利確損切戦略
        phase3_results = {}
        for threshold_pct in [1, 2, 3]:
            threshold = threshold_pct / 100
            if df_5min is not None:
                phase_return, phase_win, exit_reason = calculate_phase3_return(
                    df_5min, buy_price, threshold, -threshold
                )
            else:
                phase_return, phase_win, exit_reason = None, None, None

            phase3_results[f"phase3_{threshold_pct}pct"] = {
                "return": phase_return,
                "win": phase_win,
                "exit_reason": exit_reason,
                "profit_per_100_shares": (phase_return * buy_price * 100) if phase_return is not None else None
            }

        # 11セグメント価格を計算
        segment_prices = calculate_segment_prices(df_5min, buy_price)

        return {
            "prev_close": prev_close,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "daily_close": daily_close,
            "high": high,
            "low": low,
            "volume": volume,
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
            "market_cap": market_cap,
            "data_source": "5min" if df_5min is not None else "1d",
            "profit_per_100_shares_morning_early": profit_per_100_shares_morning_early,
            "profit_per_100_shares_afternoon_early": profit_per_100_shares_afternoon_early,
            # 11セグメント価格（100株あたり利益）
            **segment_prices,
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch backtest data for {ticker}: {e}")
        traceback.print_exc()
        return None


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

    # 2. 選定日と翌営業日を取得
    selection_date_str = df_grok['date'].iloc[0] if 'date' in df_grok.columns else None

    if not selection_date_str:
        print("[ERROR] 'date' column not found in grok_trending.parquet")
        return pd.DataFrame()

    selection_date = datetime.strptime(selection_date_str, "%Y-%m-%d")
    print(f"[INFO] Selection date: {selection_date.date()}")

    # grok_trending.parquetのdate列は既に翌営業日（next_trading_day）なのでそのまま使用
    backtest_date = selection_date
    print(f"[INFO] Backtest date: {backtest_date.date()}")

    # 3. 前営業日を取得（全銘柄共通なので1回だけ）
    fetcher = JQuantsFetcher()
    prev_trading_day = fetcher.get_previous_trading_day(backtest_date.date())
    print(f"[INFO] Previous trading day: {prev_trading_day}")

    # 4. 各銘柄のバックテストを実行
    results = []

    for idx, row in df_grok.iterrows():
        ticker = row['ticker']
        print(f"[{idx+1}/{len(df_grok)}] Processing {ticker}...", end=" ", flush=True)

        backtest_data = fetch_backtest_data(ticker, backtest_date, prev_trading_day)

        if backtest_data is None:
            print("SKIP (no data)")
            continue

        # 取引制限情報を取得
        trading_restrictions = get_trading_restriction_info(ticker)

        # デイトレード情報を取得（grok_day_trade_list.parquet）
        day_trade_info = get_day_trade_info(ticker)

        result = {
            "selection_date": selection_date.strftime("%Y-%m-%d"),
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
        }

        results.append(result)
        shortable_mark = "○" if trading_restrictions['is_shortable'] else "✗"
        print(f"OK (Phase1: {backtest_data['phase1_return']*100:+.2f}%, Phase2: {backtest_data['phase2_return']*100:+.2f}%, Short: {shortable_mark})")

    if not results:
        print("[WARN] No backtest results generated")
        return pd.DataFrame()

    df_results = pd.DataFrame(results)
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


def save_to_archive(df: pd.DataFrame, backtest_date: str) -> None:
    """
    バックテスト結果をアーカイブに保存

    Args:
        df: バックテスト結果
        backtest_date: バックテスト日 (YYYY-MM-DD)
    """
    # S3設定を読み込み
    cfg = load_s3_config()
    if not cfg:
        print("[WARN] S3 not configured, skipping archive")
        return

    # 1. 日付ごとのファイルとして保存
    date_str = backtest_date.replace("-", "")
    dated_file = BACKTEST_DIR / f"grok_trending_{date_str}.parquet"
    df.to_parquet(dated_file, index=False)
    print(f"[OK] Saved dated file: {dated_file}")

    # 2. S3から既存のアーカイブをダウンロード
    s3_archive_key = "backtest/grok_trending_archive.parquet"
    print(f"[INFO] Downloading existing archive from S3: {s3_archive_key}")
    archive_exists = download_file(cfg, s3_archive_key, BACKTEST_ARCHIVE_PATH)

    if archive_exists:
        df_archive = pd.read_parquet(BACKTEST_ARCHIVE_PATH)

        # カラム名統一: 旧カラム名 → 新カラム名 (COLUMN_RENAME_TASKS.md準拠)
        if 'company_name' in df_archive.columns:
            # stock_nameが空またはNoneの場合、company_nameの値を使用
            if 'stock_name' not in df_archive.columns:
                df_archive['stock_name'] = df_archive['company_name']
            else:
                df_archive['stock_name'] = df_archive['stock_name'].fillna(df_archive['company_name'])
                df_archive.loc[df_archive['stock_name'] == '', 'stock_name'] = df_archive.loc[df_archive['stock_name'] == '', 'company_name']
            df_archive = df_archive.drop(columns=['company_name'])
            print("[INFO] Migrated company_name → stock_name")

        if 'category' in df_archive.columns:
            # categoriesが空またはNoneの場合、categoryの値を使用
            if 'categories' not in df_archive.columns:
                df_archive['categories'] = df_archive['category']
            else:
                df_archive['categories'] = df_archive['categories'].fillna(df_archive['category'])
                df_archive.loc[df_archive['categories'] == '', 'categories'] = df_archive.loc[df_archive['categories'] == '', 'category']
            df_archive = df_archive.drop(columns=['category'])
            print("[INFO] Migrated category → categories")

        # 取引制限カラムがない場合は追加（既存アーカイブのバックフィル）
        if 'margin_code' not in df_archive.columns or 'jsf_restricted' not in df_archive.columns:
            print("[INFO] Backfilling trading restriction columns for existing archive...")
            margin_code_map, margin_name_map, jsf_stop_codes = load_trading_restrictions()
            for col in ['margin_code', 'margin_code_name', 'jsf_restricted', 'is_shortable']:
                if col not in df_archive.columns:
                    df_archive[col] = None
            for idx, row in df_archive.iterrows():
                ticker = row['ticker']
                code = ticker.replace('.T', '')
                df_archive.at[idx, 'margin_code'] = margin_code_map.get(ticker, '2')
                df_archive.at[idx, 'margin_code_name'] = margin_name_map.get(ticker, '貸借')
                df_archive.at[idx, 'jsf_restricted'] = code in jsf_stop_codes
                df_archive.at[idx, 'is_shortable'] = (margin_code_map.get(ticker, '2') == '2') and (code not in jsf_stop_codes)
            print(f"[INFO] Backfilled {len(df_archive)} archive records with trading restrictions")

        # 同じbacktest_dateのデータを削除（上書き）
        df_archive = df_archive[df_archive['backtest_date'] != backtest_date]

        # 型を統一（selection_date, backtest_dateを文字列に）
        for date_col in ['selection_date', 'backtest_date']:
            if date_col in df_archive.columns:
                df_archive[date_col] = df_archive[date_col].astype(str)
            if date_col in df.columns:
                df[date_col] = df[date_col].astype(str)

        df_merged = pd.concat([df_archive, df], ignore_index=True)
        print(f"[INFO] Merged with existing archive: {len(df_archive)} + {len(df)} = {len(df_merged)} records")
    else:
        df_merged = df
        print("[INFO] Creating new archive file")

    df_merged.to_parquet(BACKTEST_ARCHIVE_PATH, index=False)
    print(f"[OK] Saved archive: {BACKTEST_ARCHIVE_PATH}")
    print(f"     Total records: {len(df_merged)}")
    print(f"     Date range: {df_merged['backtest_date'].min()} to {df_merged['backtest_date'].max()}")

    # 3. S3にアップロード
    try:
        # 日付ごとのファイルをアップロード
        s3_key_dated = f"backtest/grok_trending_{date_str}.parquet"
        upload_file(cfg, dated_file, s3_key_dated)
        print(f"[OK] Uploaded to S3: {s3_key_dated}")

        # アーカイブファイルをアップロード
        s3_key_archive = "backtest/grok_trending_archive.parquet"
        upload_file(cfg, BACKTEST_ARCHIVE_PATH, s3_key_archive)
        print(f"[OK] Uploaded to S3: {s3_key_archive}")
    except Exception as e:
        print(f"[ERROR] Failed to upload to S3: {e}")


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
