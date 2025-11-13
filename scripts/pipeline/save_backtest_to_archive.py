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

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file, download_file
from common_cfg.s3cfg import load_s3_config
from scripts.lib.jquants_client import JQuantsClient

# パス定義
BACKTEST_DIR = PARQUET_DIR / "backtest"
BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
GROK_TRENDING_PATH = BACKTEST_DIR / "grok_trending_temp.parquet"
BACKTEST_ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"

# J-Quants クライアント（グローバル）
_jquants_client: Optional[JQuantsClient] = None


def get_jquants_client() -> JQuantsClient:
    """J-Quantsクライアントを取得（シングルトン）"""
    global _jquants_client
    if _jquants_client is None:
        _jquants_client = JQuantsClient()
    return _jquants_client


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

        # 1. 発行済株式数を取得（最新の決算データ）
        statements_response = client.request('/fins/statements', params={'code': code})

        if 'statements' not in statements_response or not statements_response['statements']:
            return None

        # 最新のデータを取得（日付順でソート）
        statements = sorted(
            statements_response['statements'],
            key=lambda x: x.get('DisclosedDate', ''),
            reverse=True
        )

        issued_shares = None
        for statement in statements:
            issued_shares = statement.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
            if issued_shares:
                issued_shares = float(issued_shares)  # 文字列からfloatに変換
                break

        if not issued_shares:
            return None

        # 2. 調整係数を取得
        date_str = date.strftime('%Y-%m-%d')
        quotes_response = client.request('/prices/daily_quotes', params={'code': code, 'date': date_str})

        if 'daily_quotes' not in quotes_response or not quotes_response['daily_quotes']:
            return None

        adjustment_factor = float(quotes_response['daily_quotes'][0].get('AdjustmentFactor', 1.0))

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


def fetch_backtest_data(ticker: str, backtest_date: datetime) -> Optional[dict]:
    """
    バックテスト用の株価データを取得

    Args:
        ticker: 銘柄コード (例: "6526.T")
        backtest_date: バックテスト日（翌営業日）

    Returns:
        dict: バックテストデータ
    """
    try:
        # 日次データを取得
        stock = yf.Ticker(ticker)
        start_date = (backtest_date - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (backtest_date + timedelta(days=2)).strftime("%Y-%m-%d")

        hist_daily = stock.history(start=start_date, end=end_date, interval="1d")

        if hist_daily.empty:
            return None

        # インデックスをdate型に変換
        hist_daily.index = pd.to_datetime(hist_daily.index).date
        backtest_date_obj = backtest_date.date()

        if backtest_date_obj not in hist_daily.index:
            return None

        daily_row = hist_daily.loc[backtest_date_obj]

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

        return {
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
            "data_source": "5min" if df_5min is not None else "1d"
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch backtest data for {ticker}: {e}")
        traceback.print_exc()
        return None


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

    # 3. 各銘柄のバックテストを実行
    results = []

    for idx, row in df_grok.iterrows():
        ticker = row['ticker']
        print(f"[{idx+1}/{len(df_grok)}] Processing {ticker}...", end=" ", flush=True)

        backtest_data = fetch_backtest_data(ticker, backtest_date)

        if backtest_data is None:
            print("SKIP (no data)")
            continue

        result = {
            "selection_date": selection_date.strftime("%Y-%m-%d"),
            "backtest_date": backtest_date.strftime("%Y-%m-%d"),
            "ticker": ticker,
            "company_name": row.get("stock_name", ""),
            "category": row.get("tags", "").split(",")[0] if row.get("tags") else "",
            "reason": row.get("reason", ""),
            "grok_rank": row.get("grok_rank", idx + 1),
            "selection_score": row.get("selection_score", 0),
            **backtest_data,
            "prompt_version": row.get("prompt_version", "v1_1_web_search")
        }

        results.append(result)
        print(f"OK (Phase1: {backtest_data['phase1_return']*100:+.2f}%, Phase2: {backtest_data['phase2_return']*100:+.2f}%)")

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
        # 同じbacktest_dateのデータを削除（上書き）
        df_archive = df_archive[df_archive['backtest_date'] != backtest_date]
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
