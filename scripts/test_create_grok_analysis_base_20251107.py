"""
Grok銘柄分析用基礎データ作成

grok_trending_archive.parquetから基礎データを抽出し、
分析しやすい形式でparquetを作成する。

出力: test_output/test_grok_analysis_base_20251107.parquet
"""

import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'parquet'
GROK_ARCHIVE_PATH = DATA_DIR / 'backtest' / 'grok_trending_archive.parquet'
OUTPUT_DIR = BASE_DIR / 'test_output'
OUTPUT_PATH = OUTPUT_DIR / 'test_grok_analysis_base_20251107.parquet'


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

        # MultiIndex対応
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        # 前場データ抽出(9:00-11:30)
        # タイムゾーン問題を回避するためbetween_timeを使用
        try:
            morning_data = df.between_time('09:00', '11:30')
        except Exception:
            # タイムゾーン付きの場合はローカライズ解除
            df.index = df.index.tz_localize(None)
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

    # Grokアーカイブ読み込み
    logger.info(f"Loading {GROK_ARCHIVE_PATH}")
    grok_df = pd.read_parquet(GROK_ARCHIVE_PATH)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    logger.info(f"Total records: {len(grok_df)}")

    # 基礎データ作成
    base_data = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        date = row['backtest_date']

        logger.info(f"Processing {idx+1}/{len(grok_df)}: {ticker} on {date.date()}")

        # morning_volume取得
        morning_volume = fetch_morning_volume(ticker, date)

        # morning_close (phase1のsell_price)
        morning_close = row.get('sell_price', np.nan)  # phase1の売却価格がない場合
        if pd.isna(morning_close):
            # phase1_returnから逆算
            if not pd.isna(row.get('phase1_return')):
                morning_close = row['buy_price'] * (1 + row['phase1_return'])
            else:
                morning_close = np.nan

        # 基礎レコード作成
        base_record = {
            'code': ticker,
            'stock_name': row['company_name'],
            'date': date.strftime('%Y-%m-%d'),
            'open': row['buy_price'],
            'morning_close': morning_close,
            'close': row['daily_close'],
            'morning_high': row.get('morning_high', np.nan),
            'high': row['high'],
            'morning_low': row.get('morning_low', np.nan),
            'low': row['low'],
            'morning_volume': morning_volume,
            'volume': row['volume'],
            'category': row['category'],
            'reason': row['reason'],
        }

        # ボラティリティ計算
        if not pd.isna(row.get('morning_high')) and not pd.isna(row.get('morning_low')):
            base_record['morning_volatility'] = (
                (row['morning_high'] - row['morning_low']) / row['buy_price'] * 100
            )
        else:
            base_record['morning_volatility'] = np.nan

        base_record['daily_volatility'] = (
            (row['high'] - row['low']) / row['buy_price'] * 100
        )

        # tags分割（categoryから）
        # categoryが "株クラバズ+出来高急増" のような形式の場合、分割
        if '+' in row['category']:
            tags_list = [tag.strip() for tag in row['category'].split('+')]
        else:
            tags_list = [row['category']]

        # 各tagごとに1レコード作成
        for tag in tags_list:
            tag_record = base_record.copy()
            tag_record['tags'] = tag
            base_data.append(tag_record)

    # DataFrame化
    base_df = pd.DataFrame(base_data)

    logger.info(f"Total records after tag expansion: {len(base_df)}")

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    base_df.to_parquet(OUTPUT_PATH, index=False)
    logger.info(f"Saved to {OUTPUT_PATH}")

    return base_df


def main():
    logger.info("Starting Grok analysis base data creation...")
    base_df = create_analysis_base()

    # サマリー表示
    logger.info("\n=== Summary ===")
    logger.info(f"Total records: {len(base_df)}")
    logger.info(f"Unique tickers: {base_df['code'].nunique()}")
    logger.info(f"Unique dates: {base_df['date'].nunique()}")
    logger.info(f"Unique tags: {base_df['tags'].nunique()}")

    logger.info("\nTags distribution:")
    logger.info(base_df['tags'].value_counts())

    logger.info("\nSample data:")
    logger.info(base_df.head(3))


if __name__ == '__main__':
    main()
