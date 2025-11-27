"""
政策銘柄バックテストをアーカイブに保存

Grokバックテストと同じ期間・粒度で政策銘柄のバックテストを実行し、
political_trending_archive.parquet として保存する。
"""

import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# パス設定
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'parquet'
META_PATH = DATA_DIR / 'meta.parquet'
GROK_ARCHIVE_PATH = DATA_DIR / 'backtest' / 'grok_trending_archive.parquet'
OUTPUT_PATH = DATA_DIR / 'backtest' / 'political_trending_archive.parquet'


def load_political_stocks():
    """政策銘柄を読み込む"""
    meta_df = pd.read_parquet(META_PATH)
    political_stocks = meta_df[
        meta_df['categories'].apply(lambda x: any('政策銘柄' == cat for cat in x) if x is not None else False)
    ].copy()

    logger.info(f"政策銘柄数: {len(political_stocks)}")
    return political_stocks


def get_backtest_dates():
    """Grokバックテストの日付リストを取得"""
    grok_archive = pd.read_parquet(GROK_ARCHIVE_PATH)
    grok_archive['selection_date'] = pd.to_datetime(grok_archive['selection_date'])

    selection_dates = sorted(grok_archive['selection_date'].unique())
    logger.info(f"バックテスト期間: {selection_dates[0].date()} 〜 {selection_dates[-1].date()}")
    logger.info(f"総日数: {len(selection_dates)}")

    return selection_dates


def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """株価データを取得（日足 & 5分足）"""
    try:
        # 日足
        daily_df = yf.download(ticker, start=start_date, end=end_date, interval='1d', progress=False)

        # MultiIndex対応: カラムをflatten
        if isinstance(daily_df.columns, pd.MultiIndex):
            daily_df.columns = daily_df.columns.droplevel(1)

        # 5分足（同日のみ）
        intraday_df = yf.download(ticker, start=start_date, end=end_date, interval='5m', progress=False)

        # MultiIndex対応
        if not intraday_df.empty and isinstance(intraday_df.columns, pd.MultiIndex):
            intraday_df.columns = intraday_df.columns.droplevel(1)

        if daily_df.empty:
            return None, None

        return daily_df, intraday_df if not intraday_df.empty else None

    except Exception as e:
        logger.warning(f"Error fetching {ticker}: {e}")
        return None, None


def calculate_backtest_stats(
    ticker: str,
    selection_date: pd.Timestamp,
    backtest_date: pd.Timestamp,
    daily_df: pd.DataFrame,
    intraday_df: Optional[pd.DataFrame],
    meta_row: pd.Series
) -> Optional[Dict[str, Any]]:
    """1銘柄のバックテスト統計を計算"""

    result = {
        'selection_date': selection_date.strftime('%Y-%m-%d'),
        'backtest_date': backtest_date.strftime('%Y-%m-%d'),
        'ticker': ticker,
        'stock_name': meta_row['stock_name'],
        'categories': '政策銘柄',
        'tags': ', '.join(meta_row['tags']) if isinstance(meta_row['tags'], (list, np.ndarray)) and len(meta_row['tags']) > 0 else '',
    }

    # backtest_date の日足データ
    target_day = daily_df[daily_df.index.date == backtest_date.date()]
    if target_day.empty:
        return None

    open_price = float(target_day['Open'].iloc[0])
    close_price = float(target_day['Close'].iloc[0])
    high_price = float(target_day['High'].iloc[0])
    low_price = float(target_day['Low'].iloc[0])
    volume = int(target_day['Volume'].iloc[0])

    result['buy_price'] = open_price
    result['sell_price'] = close_price
    result['daily_close'] = close_price
    result['high'] = high_price
    result['low'] = low_price
    result['volume'] = volume

    # Phase1: 前場引け（11:30）
    if intraday_df is not None and not intraday_df.empty:
        morning_data = intraday_df[
            (intraday_df.index.time >= pd.Timestamp('09:00').time()) &
            (intraday_df.index.time <= pd.Timestamp('11:30').time())
        ]
        if not morning_data.empty:
            morning_close = float(morning_data['Close'].iloc[-1])
            phase1_return = (morning_close - open_price) / open_price
            result['phase1_return'] = phase1_return
            result['phase1_win'] = bool(phase1_return > 0)
            result['profit_per_100_shares_phase1'] = (morning_close - open_price) * 100

            # morning max/min
            morning_high = float(morning_data['High'].max())
            morning_low = float(morning_data['Low'].min())
            result['morning_high'] = morning_high
            result['morning_low'] = morning_low
            result['morning_max_gain_pct'] = (morning_high - open_price) / open_price * 100
            result['morning_max_drawdown_pct'] = (morning_low - open_price) / open_price * 100
        else:
            result['phase1_return'] = None
            result['phase1_win'] = None
            result['profit_per_100_shares_phase1'] = None
            result['morning_high'] = None
            result['morning_low'] = None
            result['morning_max_gain_pct'] = None
            result['morning_max_drawdown_pct'] = None
    else:
        result['phase1_return'] = None
        result['phase1_win'] = None
        result['profit_per_100_shares_phase1'] = None
        result['morning_high'] = None
        result['morning_low'] = None
        result['morning_max_gain_pct'] = None
        result['morning_max_drawdown_pct'] = None

    # Phase2: 大引け（15:30）
    phase2_return = (close_price - open_price) / open_price
    result['phase2_return'] = phase2_return
    result['phase2_win'] = bool(phase2_return > 0)
    result['profit_per_100_shares_phase2'] = (close_price - open_price) * 100

    # daily max/min
    result['daily_max_gain_pct'] = (high_price - open_price) / open_price * 100
    result['daily_max_drawdown_pct'] = (low_price - open_price) / open_price * 100

    # Phase3: ±1%/2%/3%
    for pct in [1, 2, 3]:
        profit_threshold = open_price * (1 + pct/100)
        loss_threshold = open_price * (1 - pct/100)

        if intraday_df is not None and not intraday_df.empty:
            profit_hit = intraday_df[intraday_df['High'] >= profit_threshold]
            loss_hit = intraday_df[intraday_df['Low'] <= loss_threshold]

            if not profit_hit.empty and not loss_hit.empty:
                if profit_hit.index[0] < loss_hit.index[0]:
                    exit_price = profit_threshold
                    exit_reason = f'profit_take_{pct}.0%'
                else:
                    exit_price = loss_threshold
                    exit_reason = f'stop_loss_-{pct}.0%'
            elif not profit_hit.empty:
                exit_price = profit_threshold
                exit_reason = f'profit_take_{pct}.0%'
            elif not loss_hit.empty:
                exit_price = loss_threshold
                exit_reason = f'stop_loss_-{pct}.0%'
            else:
                exit_price = close_price
                exit_reason = 'eod_close'
        else:
            exit_price = close_price
            exit_reason = 'eod_close'

        phase_return = (exit_price - open_price) / open_price
        result[f'phase3_{pct}pct_return'] = float(phase_return)
        result[f'phase3_{pct}pct_win'] = bool(phase_return > 0)
        result[f'phase3_{pct}pct_exit_reason'] = exit_reason
        result[f'profit_per_100_shares_phase3_{pct}pct'] = (exit_price - open_price) * 100

    # prompt_version, data_source
    result['prompt_version'] = 'political_v1'
    result['data_source'] = '5min'

    return result


def run_backtest():
    """全期間でバックテスト実行"""
    political_stocks = load_political_stocks()
    backtest_dates = get_backtest_dates()

    all_results = []

    for date_idx, selection_date in enumerate(backtest_dates):
        backtest_date = selection_date
        logger.info(f"Processing {date_idx+1}/{len(backtest_dates)}: {backtest_date.date()}")

        # 株価データ取得用の日付範囲
        start_date = (backtest_date - timedelta(days=5)).strftime('%Y-%m-%d')
        end_date = (backtest_date + timedelta(days=1)).strftime('%Y-%m-%d')

        for idx, row in political_stocks.iterrows():
            ticker = row['ticker']

            # 株価データ取得
            daily_df, intraday_df = fetch_stock_data(ticker, start_date, end_date)
            if daily_df is None:
                continue

            # バックテスト計算
            result = calculate_backtest_stats(
                ticker, selection_date, backtest_date, daily_df, intraday_df, row
            )
            if result:
                all_results.append(result)

        logger.info(f"  {len(all_results)} results so far")

    # DataFrame化
    results_df = pd.DataFrame(all_results)
    logger.info(f"Total results: {len(results_df)}")

    # 保存
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_parquet(OUTPUT_PATH, index=False)
    logger.info(f"Saved to {OUTPUT_PATH}")

    return results_df


def main():
    logger.info("Starting political backtest...")
    results_df = run_backtest()

    # 統計表示
    logger.info("\n=== Summary ===")
    logger.info(f"Total trades: {len(results_df)}")
    logger.info(f"Period: {results_df['selection_date'].min()} - {results_df['selection_date'].max()}")
    logger.info(f"Unique tickers: {results_df['ticker'].nunique()}")

    # tags別集計
    tags_expanded = results_df[results_df['tags'] != ''].copy()
    tags_expanded['tags_list'] = tags_expanded['tags'].str.split(', ')
    tags_expanded = tags_expanded.explode('tags_list')

    tags_stats = tags_expanded.groupby('tags_list').agg({
        'ticker': 'count',
        'profit_per_100_shares_phase2': 'sum',
        'phase2_win': lambda x: (x.sum() / len(x) * 100) if len(x) > 0 else 0
    }).round(2)

    logger.info("\nTags performance:")
    logger.info(tags_stats)


if __name__ == '__main__':
    main()
