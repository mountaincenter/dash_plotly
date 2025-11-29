#!/usr/bin/env python3
"""
save_static_backtest.py
Static銘柄シグナルのバックテスト結果を保存

実行方法:
    python scripts/pipeline/save_static_backtest.py

出力:
    data/parquet/backtest/static_backtest.parquet

処理:
    1. 直近のstatic_signals.parquetを読み込み
    2. 各シグナルの5日後リターンを計算
    3. 結果をアーカイブに追加保存
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import logging

warnings.filterwarnings('ignore')

# パス設定
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file, download_file
from common_cfg.s3cfg import load_s3_config

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# パス設定
SIGNALS_FILE = PARQUET_DIR / "static_signals.parquet"
BACKTEST_DIR = PARQUET_DIR / "backtest"
BACKTEST_FILE = BACKTEST_DIR / "static_backtest.parquet"


def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    """株価データを取得"""
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data.empty:
            return None
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        return data.reset_index()
    except Exception as e:
        logger.warning(f"Failed to fetch {ticker}: {e}")
        return None


def calculate_returns(
    ticker: str,
    signal_date: str,
    prices_df: pd.DataFrame
) -> dict | None:
    """シグナル日からのリターンを計算"""
    signal_dt = pd.to_datetime(signal_date)

    # シグナル日以降のデータ
    future_prices = prices_df[prices_df['Date'] > signal_dt].sort_values('Date')

    if len(future_prices) < 6:
        return None

    # 翌日寄付でエントリー
    entry_row = future_prices.iloc[0]
    entry_price = float(entry_row['Open'])
    entry_date = entry_row['Date']

    result = {
        'entry_date': entry_date.strftime('%Y-%m-%d'),
        'entry_price': entry_price,
    }

    # 1日後
    if len(future_prices) >= 1:
        day1 = future_prices.iloc[0]
        exit_price = float(day1['Close'])
        result['exit_date_1d'] = day1['Date'].strftime('%Y-%m-%d')
        result['exit_price_1d'] = exit_price
        result['return_1d'] = (exit_price - entry_price) / entry_price * 100
        result['profit_100_1d'] = (exit_price - entry_price) * 100

    # 5日後
    if len(future_prices) >= 5:
        day5 = future_prices.iloc[4]
        exit_price = float(day5['Close'])
        result['exit_date_5d'] = day5['Date'].strftime('%Y-%m-%d')
        result['exit_price_5d'] = exit_price
        result['return_5d'] = (exit_price - entry_price) / entry_price * 100
        result['profit_100_5d'] = (exit_price - entry_price) * 100

    # 10日後
    if len(future_prices) >= 10:
        day10 = future_prices.iloc[9]
        exit_price = float(day10['Close'])
        result['exit_date_10d'] = day10['Date'].strftime('%Y-%m-%d')
        result['exit_price_10d'] = exit_price
        result['return_10d'] = (exit_price - entry_price) / entry_price * 100
        result['profit_100_10d'] = (exit_price - entry_price) * 100

    return result


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("Static Backtest Saver")
    logger.info("=" * 60)

    # S3設定を読み込み
    cfg = load_s3_config()

    # 1. シグナルファイル読み込み
    if not SIGNALS_FILE.exists():
        logger.error(f"Signals file not found: {SIGNALS_FILE}")
        return None

    signals_df = pd.read_parquet(SIGNALS_FILE)
    logger.info(f"Loaded {len(signals_df)} signals")

    # STRONG_BUY/BUYのみ
    buy_signals = signals_df[signals_df['signal'].isin(['STRONG_BUY', 'BUY'])].copy()
    logger.info(f"Buy signals: {len(buy_signals)}")

    if len(buy_signals) == 0:
        logger.info("No buy signals to backtest")
        return None

    # 2. 既存のバックテスト結果を読み込み（ローカルになければS3から）
    existing_df = None
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)

    if BACKTEST_FILE.exists():
        existing_df = pd.read_parquet(BACKTEST_FILE)
        logger.info(f"Existing backtest records (local): {len(existing_df)}")
    elif cfg and cfg.bucket:
        logger.info("Downloading existing backtest from S3...")
        if download_file(cfg, "backtest/static_backtest.parquet", BACKTEST_FILE):
            existing_df = pd.read_parquet(BACKTEST_FILE)
            logger.info(f"Existing backtest records (S3): {len(existing_df)}")

    # 3. 各シグナルのリターンを計算
    results = []

    for _, row in buy_signals.iterrows():
        ticker = row['ticker']
        signal_date = row['signal_date']

        # 既存データにあればスキップ
        if existing_df is not None:
            exists = (
                (existing_df['ticker'] == ticker) &
                (existing_df['signal_date'] == signal_date)
            ).any()
            if exists:
                continue

        # 株価データ取得
        start = (pd.to_datetime(signal_date) - timedelta(days=5)).strftime('%Y-%m-%d')
        end = (pd.to_datetime(signal_date) + timedelta(days=20)).strftime('%Y-%m-%d')

        prices_df = fetch_stock_data(ticker, start, end)
        if prices_df is None:
            continue

        # リターン計算
        returns = calculate_returns(ticker, signal_date, prices_df)
        if returns is None:
            continue

        # 結果を結合
        result = {
            'signal_date': signal_date,
            'ticker': ticker,
            'stock_name': row['stock_name'],
            'sector': row['sector'],
            'score': row['score'],
            'signal': row['signal'],
            'market_condition': row['market_condition'],
            'n225_vs_sma5': row['n225_vs_sma5'],
            'close': row['close'],
            'rsi_14d': row['rsi_14d'],
            'ma25_deviation': row['ma25_deviation'],
            'atr_pct': row['atr_pct'],
            'daily_change': row['daily_change'],
            **returns,
        }
        results.append(result)

    if not results:
        logger.info("No new results to add")
        return existing_df

    new_df = pd.DataFrame(results)
    logger.info(f"New backtest records: {len(new_df)}")

    # 4. 既存データとマージ
    if existing_df is not None:
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    # 重複削除
    final_df = final_df.drop_duplicates(subset=['ticker', 'signal_date'], keep='last')
    final_df = final_df.sort_values(['signal_date', 'ticker']).reset_index(drop=True)

    # 5. 保存
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(BACKTEST_FILE, index=False)

    logger.info("=" * 60)
    logger.info(f"Saved to: {BACKTEST_FILE}")
    logger.info(f"Total records: {len(final_df)}")

    # 6. S3にアップロード
    if cfg and cfg.bucket:
        logger.info("Uploading static_backtest.parquet to S3...")
        upload_file(cfg, BACKTEST_FILE, "backtest/static_backtest.parquet")

    # サマリー表示（1日 vs 5日比較）
    strong_buy = final_df[final_df['signal'] == 'STRONG_BUY']
    if len(strong_buy) > 0:
        logger.info("")
        logger.info("【STRONG_BUY パフォーマンス比較】")
        logger.info("-" * 50)
        logger.info(f"{'':12} {'件数':>6} {'勝率':>8} {'平均':>8} {'合計利益':>12}")
        logger.info("-" * 50)

        for days, col_ret, col_profit in [
            ('1日保有', 'return_1d', 'profit_100_1d'),
            ('5日保有', 'return_5d', 'profit_100_5d'),
        ]:
            if col_ret in strong_buy.columns:
                valid = strong_buy[strong_buy[col_ret].notna()]
                if len(valid) > 0:
                    count = len(valid)
                    win_rate = (valid[col_ret] > 0).mean() * 100
                    avg_ret = valid[col_ret].mean()
                    total = valid[col_profit].sum()
                    logger.info(f"{days:12} {count:>6} {win_rate:>7.1f}% {avg_ret:>+7.2f}% ¥{total:>11,.0f}")

        logger.info("-" * 50)

        # 5日 vs 1日の差分
        if 'return_1d' in strong_buy.columns and 'return_5d' in strong_buy.columns:
            valid = strong_buy[strong_buy['return_5d'].notna() & strong_buy['return_1d'].notna()]
            if len(valid) > 0:
                diff_ret = valid['return_5d'].mean() - valid['return_1d'].mean()
                diff_profit = valid['profit_100_5d'].sum() - valid['profit_100_1d'].sum()
                logger.info(f"{'5日-1日差分':12} {'':>6} {'':>8} {diff_ret:>+7.2f}% ¥{diff_profit:>+11,.0f}")

    logger.info("=" * 60)

    return final_df


if __name__ == "__main__":
    main()
