"""
Grok銘柄分析用基礎データ作成 (最新版)

日次ファイル grok_trending_*.parquet から基礎データを抽出し、
最新データまで含めた分析用parquetを作成する。

出力: test_output/grok_analysis_base_latest.parquet
"""

import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict
from glob import glob

# J-Quants クライアント
import sys
sys.path.append(str(Path(__file__).parent.parent))
from scripts.lib.jquants_client import JQuantsClient

# J-Quantsクライアント（グローバル）
_jquants_client: Optional[JQuantsClient] = None


def get_jquants_client() -> JQuantsClient:
    """J-Quantsクライアントを取得（シングルトン）"""
    global _jquants_client
    if _jquants_client is None:
        _jquants_client = JQuantsClient()
    return _jquants_client

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'parquet' / 'backtest'
OUTPUT_DIR = BASE_DIR / 'test_output'
OUTPUT_PATH = OUTPUT_DIR / 'grok_analysis_base_latest.parquet'


def get_latest_daily_files():
    """最新の日次ファイルを取得"""
    pattern = str(DATA_DIR / 'grok_trending_202511*.parquet')
    files = glob(pattern)

    # アーカイブファイルを除外
    files = [f for f in files if 'archive' not in f]

    # 日付でソート
    files.sort()

    return [Path(f) for f in files]


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
        logger.warning(f"Failed to fetch market cap for {ticker}: {e}")
        return None


def fetch_intraday_timing_data(ticker: str, date: datetime, buy_price: float, direction: str = 'buy') -> Dict[str, float]:
    """
    yfinanceの5分足からタイミング分析データを取得

    Args:
        ticker: 銘柄コード
        date: 対象日
        buy_price: 買値（寄り付き価格）
        direction: 'buy' or 'sell'

    Returns:
        dict: {
            'morning_volume': 前場出来高,
            'morning_close_price': 前場終値(11:30),
            'day_close_price': 大引値(15:30),
            'morning_high': 前場高値,
            'morning_low': 前場安値,
            'day_high': 日中高値,
            'day_low': 日中安値,
            'profit_morning': 前場利益（円）,
            'profit_day_close': 大引利益（円）,
            'profit_morning_pct': 前場利益率（%）,
            'profit_day_close_pct': 大引利益率（%）,
            'better_profit_timing': 'morning_close' or 'day_close',
            'better_loss_timing': 'morning_close' or 'day_close',
            'is_win_morning': 前場勝ち,
            'is_win_day_close': 大引勝ち
        }
    """
    result = {
        'morning_volume': np.nan,
        'morning_close_price': np.nan,
        'day_close_price': np.nan,
        'morning_high': np.nan,
        'morning_low': np.nan,
        'day_high': np.nan,
        'day_low': np.nan,
        'profit_morning': np.nan,
        'profit_day_close': np.nan,
        'profit_morning_pct': np.nan,
        'profit_day_close_pct': np.nan,
        'better_profit_timing': np.nan,
        'better_loss_timing': np.nan,
        'is_win_morning': False,
        'is_win_day_close': False,
    }

    try:
        start_date = date.strftime('%Y-%m-%d')
        end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')

        # 5分足取得
        df = yf.download(ticker, start=start_date, end=end_date, interval='5m', progress=False)

        if df.empty:
            return result

        # MultiIndex対応（価格データはMultiIndexで返される）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # タイムゾーン変換: UTC → JST
        if df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Tokyo')

        # 前場データ抽出(9:00-11:30 JST)
        morning_data = df.between_time('09:00', '11:30')
        # 全日データ(9:00-15:30 JST)
        full_day_data = df.between_time('09:00', '15:30')

        if morning_data.empty or full_day_data.empty:
            return result

        # 出来高
        result['morning_volume'] = float(morning_data['Volume'].sum())

        # 価格データ
        result['morning_close_price'] = float(morning_data['Close'].iloc[-1])
        result['day_close_price'] = float(full_day_data['Close'].iloc[-1])
        result['morning_high'] = float(morning_data['High'].max())
        result['morning_low'] = float(morning_data['Low'].min())
        result['day_high'] = float(full_day_data['High'].max())
        result['day_low'] = float(full_day_data['Low'].min())

        # 売買方向（買い=1, 売り=-1）
        dir_mult = -1 if direction == 'sell' else 1

        # 利益計算
        result['profit_morning'] = (result['morning_close_price'] - buy_price) * dir_mult
        result['profit_day_close'] = (result['day_close_price'] - buy_price) * dir_mult
        result['profit_morning_pct'] = (result['profit_morning'] / buy_price) * 100 if buy_price > 0 else 0
        result['profit_day_close_pct'] = (result['profit_day_close'] / buy_price) * 100 if buy_price > 0 else 0

        # 有利なタイミング判定
        result['better_profit_timing'] = 'morning_close' if result['profit_morning'] > result['profit_day_close'] else 'day_close'

        # 損切りタイミング（損失がより小さい方）
        loss_morning = result['morning_close_price'] - buy_price
        loss_day = result['day_close_price'] - buy_price
        result['better_loss_timing'] = 'morning_close' if loss_morning > loss_day else 'day_close'

        # 勝敗判定
        result['is_win_morning'] = result['profit_morning'] > 0
        result['is_win_day_close'] = result['profit_day_close'] > 0

        return result

    except Exception as e:
        logger.warning(f"Error fetching intraday timing data for {ticker} on {date.date()}: {e}")
        return result


def fetch_previous_days_data(ticker: str, date: datetime) -> Dict[str, float]:
    """
    yfinanceから前日・前々日の終値と出来高を取得

    Args:
        ticker: 銘柄コード (例: "7203.T")
        date: 基準日

    Returns:
        dict: {
            'prev_day_close': 前日終値,
            'prev_day_volume': 前日出来高,
            'prev_2day_close': 前々日終値,
            'prev_2day_volume': 前々日出来高
        }
    """
    result = {
        'prev_day_close': np.nan,
        'prev_day_volume': np.nan,
        'prev_2day_close': np.nan,
        'prev_2day_volume': np.nan
    }

    try:
        # 前々日から当日まで取得（営業日ベース）
        end_date = date
        start_date = date - timedelta(days=7)  # 余裕を持って7日前から取得

        # 日次データ取得
        df = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'),
                        end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
                        interval='1d', progress=False)

        if df.empty:
            return result

        # MultiIndex対応
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # タイムゾーンを除去してdate型に変換
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        df.index = pd.to_datetime(df.index).date

        # 基準日の前営業日を取得
        target_date = date.date()

        # 基準日より前のデータのみ抽出
        past_data = df[df.index < target_date]

        if len(past_data) == 0:
            return result

        # 前日データ（最新の1件）
        if len(past_data) >= 1:
            prev_day = past_data.iloc[-1]
            result['prev_day_close'] = float(prev_day['Close'])
            result['prev_day_volume'] = float(prev_day['Volume'])

        # 前々日データ（最新の2件目）
        if len(past_data) >= 2:
            prev_2day = past_data.iloc[-2]
            result['prev_2day_close'] = float(prev_2day['Close'])
            result['prev_2day_volume'] = float(prev_2day['Volume'])

    except Exception as e:
        logger.warning(f"Error fetching previous days data for {ticker} on {date.date()}: {e}")

    return result


def create_analysis_base():
    """基礎データ作成"""

    # 日次ファイルを取得
    daily_files = get_latest_daily_files()

    logger.info(f"Found {len(daily_files)} daily files:")
    for file_path in daily_files:
        logger.info(f"  {file_path.name}")

    # 日次ファイルを統合
    logger.info("\nLoading daily files...")
    dfs = []
    for file_path in daily_files:
        if file_path.exists():
            df = pd.read_parquet(file_path)
            dfs.append(df)
            logger.info(f"  {file_path.name}: {len(df)} records")
        else:
            logger.warning(f"  {file_path.name}: Not found")

    if not dfs:
        logger.error("No files found!")
        return None

    # 統合
    grok_df = pd.concat(dfs, ignore_index=True)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])
    logger.info(f"\nTotal records: {len(grok_df)}")
    logger.info(f"Date range: {grok_df['backtest_date'].min().date()} to {grok_df['backtest_date'].max().date()}")

    # morning_volume + 前日・前々日データ追加
    base_data = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        date = row['backtest_date']

        logger.info(f"Processing {idx+1}/{len(grok_df)}: {ticker} on {date.date()}")

        # 売買方向の決定（recommendation_actionから）
        rec_action = row.get('recommendation_action')
        rec_score = row.get('recommendation_score', 0)

        if rec_action == 'sell':
            direction = 'sell'
        elif rec_action == 'hold' and rec_score < 0:
            direction = 'sell'
        else:
            direction = 'buy'

        # 5分足からタイミング分析データ取得
        timing_data = fetch_intraday_timing_data(ticker, date, row['buy_price'], direction)

        # 時価総額取得（daily_closeを使用）
        market_cap = fetch_market_cap(ticker, row['daily_close'], date)

        # 前日・前々日データ取得
        prev_data = fetch_previous_days_data(ticker, date)

        # 基礎レコード作成（既存データ + timing_data + market_cap + 前日データ）
        base_record = row.to_dict()

        # タイミング分析データ追加
        for key, value in timing_data.items():
            base_record[key] = value

        base_record['market_cap'] = market_cap

        # 前日・前々日データ追加
        base_record['prev_day_close'] = prev_data['prev_day_close']
        base_record['prev_day_volume'] = prev_data['prev_day_volume']
        base_record['prev_2day_close'] = prev_data['prev_2day_close']
        base_record['prev_2day_volume'] = prev_data['prev_2day_volume']

        # ボラティリティ計算（morning_highとmorning_lowが既に存在）
        if not pd.isna(row.get('morning_high')) and not pd.isna(row.get('morning_low')) and row['buy_price'] > 0:
            base_record['morning_volatility'] = (
                (row['morning_high'] - row['morning_low']) / row['buy_price'] * 100
            )
        else:
            base_record['morning_volatility'] = np.nan

        if not pd.isna(row.get('high')) and not pd.isna(row.get('low')) and row['buy_price'] > 0:
            base_record['daily_volatility'] = (
                (row['high'] - row['low']) / row['buy_price'] * 100
            )
        else:
            base_record['daily_volatility'] = np.nan

        # 前日比計算
        if not pd.isna(prev_data['prev_day_close']) and prev_data['prev_day_close'] > 0:
            base_record['prev_day_change_pct'] = (
                (row['buy_price'] - prev_data['prev_day_close']) / prev_data['prev_day_close'] * 100
            )
        else:
            base_record['prev_day_change_pct'] = np.nan

        # 前日出来高比
        morning_volume = base_record.get('morning_volume', np.nan)
        if not pd.isna(prev_data['prev_day_volume']) and prev_data['prev_day_volume'] > 0 and not pd.isna(morning_volume):
            base_record['prev_day_volume_ratio'] = morning_volume / prev_data['prev_day_volume']
        else:
            base_record['prev_day_volume_ratio'] = np.nan

        base_data.append(base_record)

    # DataFrame化
    base_df = pd.DataFrame(base_data)

    logger.info(f"\nTotal records after processing: {len(base_df)}")

    # Phase1-3のreturnをパーセント形式に変換
    logger.info("Adding percentage columns...")
    base_df['phase1_return_pct'] = base_df['phase1_return'] * 100
    base_df['phase2_return_pct'] = base_df['phase2_return'] * 100
    base_df['phase3_1pct_return_pct'] = base_df['phase3_1pct_return'] * 100
    base_df['phase3_2pct_return_pct'] = base_df['phase3_2pct_return'] * 100
    base_df['phase3_3pct_return_pct'] = base_df['phase3_3pct_return'] * 100

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    base_df.to_parquet(OUTPUT_PATH, index=False)
    logger.info(f"\nSaved to {OUTPUT_PATH}")

    return base_df


def main():
    logger.info("Starting Grok analysis base data creation (latest version)...")
    base_df = create_analysis_base()

    if base_df is None:
        return

    # サマリー表示
    logger.info("\n=== Summary ===")
    logger.info(f"Total records: {len(base_df)}")
    logger.info(f"Unique tickers: {base_df['ticker'].nunique()}")
    logger.info(f"Date range: {base_df['backtest_date'].min().date()} to {base_df['backtest_date'].max().date()}")
    logger.info(f"Unique dates: {base_df['backtest_date'].nunique()}")

    if 'category' in base_df.columns:
        logger.info(f"\nCategory distribution:")
        logger.info(base_df['category'].value_counts().head(10))

    logger.info(f"\nmorning_volume stats:")
    logger.info(f"  Non-NaN: {base_df['morning_volume'].notna().sum()} / {len(base_df)}")
    logger.info(f"  Mean: {base_df['morning_volume'].mean():,.0f}")
    logger.info(f"  Median: {base_df['morning_volume'].median():,.0f}")

    logger.info(f"\nmarket_cap stats:")
    logger.info(f"  Non-NaN: {base_df['market_cap'].notna().sum()} / {len(base_df)}")
    if base_df['market_cap'].notna().sum() > 0:
        logger.info(f"  Mean: {base_df['market_cap'].mean() / 1e8:,.1f}億円")
        logger.info(f"  Median: {base_df['market_cap'].median() / 1e8:,.1f}億円")

    logger.info(f"\nprev_day_close stats:")
    logger.info(f"  Non-NaN: {base_df['prev_day_close'].notna().sum()} / {len(base_df)}")

    logger.info(f"\nprev_day_change_pct stats:")
    logger.info(f"  Non-NaN: {base_df['prev_day_change_pct'].notna().sum()} / {len(base_df)}")
    if base_df['prev_day_change_pct'].notna().sum() > 0:
        logger.info(f"  Mean: {base_df['prev_day_change_pct'].mean():.2f}%")
        logger.info(f"  Median: {base_df['prev_day_change_pct'].median():.2f}%")

    logger.info("\nSample data:")
    print(base_df[['ticker', 'company_name', 'backtest_date', 'category',
                    'prev_day_close', 'prev_day_change_pct', 'prev_day_volume_ratio']].head(5))


if __name__ == '__main__':
    main()
