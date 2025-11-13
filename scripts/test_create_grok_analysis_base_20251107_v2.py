"""
Grok銘柄分析用基礎データ作成 v2

日次ファイル grok_trending_20251104-07.parquet から基礎データを抽出し、
morning_volumeを追加して分析用parquetを作成する。

出力: test_output/test_grok_analysis_base_20251107_v2.parquet
"""

import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Optional

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
OUTPUT_PATH = OUTPUT_DIR / 'test_grok_analysis_base_20251107_v2.parquet'

# 日次ファイル
DAILY_FILES = [
    DATA_DIR / 'grok_trending_20251104.parquet',
    DATA_DIR / 'grok_trending_20251105.parquet',
    DATA_DIR / 'grok_trending_20251106.parquet',
    DATA_DIR / 'grok_trending_20251107.parquet',
]


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


def fetch_morning_volume(ticker: str, date: datetime) -> float:
    """
    yfinanceの5分足から前場(9:00-11:30)の出来高合計を取得

    Returns:
        float: 前場出来高合計、取得失敗時はNaN
    """
    try:
        start_date = date.strftime('%Y-%m-%d')
        end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')

        # 5分足取得
        df = yf.download(ticker, start=start_date, end=end_date, interval='5m', progress=False)

        if df.empty:
            return np.nan

        # MultiIndex対応（価格データはMultiIndexで返される）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # タイムゾーン変換: UTC → JST
        if df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Tokyo')

        # 前場データ抽出(9:00-11:30 JST)
        morning_data = df.between_time('09:00', '11:30')

        if morning_data.empty:
            return np.nan

        # 出来高合計
        morning_volume = float(morning_data['Volume'].sum())
        return morning_volume

    except Exception as e:
        logger.warning(f"Error fetching morning volume for {ticker} on {date.date()}: {e}")
        return np.nan


def create_analysis_base():
    """基礎データ作成"""

    # 日次ファイルを統合
    logger.info("Loading daily files...")
    dfs = []
    for file_path in DAILY_FILES:
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
    logger.info(f"Total records: {len(grok_df)}")

    # morning_volume追加
    base_data = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        date = row['backtest_date']

        logger.info(f"Processing {idx+1}/{len(grok_df)}: {ticker} on {date.date()}")

        # morning_volume取得
        morning_volume = fetch_morning_volume(ticker, date)

        # 時価総額取得（daily_closeを使用）
        market_cap = fetch_market_cap(ticker, row['daily_close'], date)

        # 基礎レコード作成（既存データ + morning_volume + market_cap）
        base_record = row.to_dict()
        base_record['morning_volume'] = morning_volume
        base_record['market_cap'] = market_cap

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

        base_data.append(base_record)

    # DataFrame化
    base_df = pd.DataFrame(base_data)

    logger.info(f"Total records: {len(base_df)}")

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
    logger.info(f"Saved to {OUTPUT_PATH}")

    return base_df


def main():
    logger.info("Starting Grok analysis base data creation (v2)...")
    base_df = create_analysis_base()

    if base_df is None:
        return

    # サマリー表示
    logger.info("\n=== Summary ===")
    logger.info(f"Total records: {len(base_df)}")
    logger.info(f"Unique tickers: {base_df['ticker'].nunique()}")
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

    logger.info("\nSample data:")
    print(base_df[['ticker', 'company_name', 'backtest_date', 'category', 'morning_volume', 'volume', 'market_cap']].head(5))


if __name__ == '__main__':
    main()
