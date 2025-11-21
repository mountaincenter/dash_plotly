"""
Grok銘柄分析用基礎データ作成 (最新版)

日次ファイル grok_trending_*.parquet から基礎データを抽出し、
最新データまで含めた分析用parquetを作成する。

出力: test_output/grok_analysis_base_latest.parquet
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict
from glob import glob
import json

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
PRICES_DIR = BASE_DIR / 'data' / 'parquet'
OUTPUT_PATH = DATA_DIR / 'grok_analysis_base_latest.parquet'

# Prices parquet paths
PRICES_1D = PRICES_DIR / 'prices_max_1d.parquet'
PRICES_5M = PRICES_DIR / 'prices_60d_5m.parquet'


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


def fetch_previous_days_data(ticker: str, date: datetime, prices_1d_df: pd.DataFrame) -> Dict[str, float]:
    """
    parquetから前日・前々日の終値と出来高を取得

    Args:
        ticker: 銘柄コード (例: "7203.T")
        date: 基準日
        prices_1d_df: 1日足データ

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
        # parquetから取得
        target_date = pd.to_datetime(date).date()

        # 基準日より前のデータのみ抽出
        past_data = prices_1d_df[
            (prices_1d_df['ticker'] == ticker) &
            (prices_1d_df['date'].dt.date < target_date)
        ].sort_values('date')

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


def apply_v2_0_3_logic(row, prev_day_close):
    """
    v2.0.3 価格帯ロジックを適用してv2_actionを決定

    Args:
        row: データ行
        prev_day_close: 前日終値

    Returns:
        dict: {
            'v2_score': スコア,
            'v2_action': '買い'/'売り'/'静観',
            'v2_confidence': '高'/'中'/'低',
            'v2_reasons_json': JSON文字列
        }
    """
    # Default v2_action from recommendation_action (if exists)
    rec_action = row.get('recommendation_action', 'hold')
    action_map = {'buy': '買い', 'sell': '売り', 'hold': '静観'}
    v2_action_default = action_map.get(rec_action, '静観')

    # v2_score
    v2_score = row.get('recommendation_score', 0)
    v2_confidence = row.get('recommendation_confidence', '中')

    # Apply v2.0.3 price-based forced positions
    v2_reasons = []
    if not pd.isna(prev_day_close):
        if 5000 <= prev_day_close < 10000:
            # Forced buy for 5,000-10,000円
            v2_action = '買い'
            v2_confidence = '高'
            v2_reasons.append({
                'type': 'price_5000_10000',
                'description': f'5,000-10,000円範囲（{prev_day_close:,.0f}円）→ロング戦略',
                'impact': 0
            })
        elif prev_day_close >= 10000:
            # Forced sell for >10,000円
            v2_action = '売り'
            v2_confidence = '高'
            v2_reasons.append({
                'type': 'price_over_10000',
                'description': f'10,000円超え（{prev_day_close:,.0f}円）→ショート戦略',
                'impact': 0
            })
        else:
            # Use default action for <5,000円
            v2_action = v2_action_default
            v2_reasons.append({
                'type': 'recommendation_based',
                'description': f'価格帯<5,000円: スコアベース判定',
                'impact': 0
            })
    else:
        # No prev_day_close data, use default
        v2_action = v2_action_default
        v2_reasons.append({
            'type': 'recommendation_based',
            'description': 'スコアベース判定（前日終値データなし）',
            'impact': 0
        })

    return {
        'v2_score': v2_score,
        'v2_action': v2_action,
        'v2_confidence': v2_confidence,
        'v2_reasons_json': json.dumps(v2_reasons, ensure_ascii=False)
    }


def create_analysis_base():
    """基礎データ作成 - 49カラムに統一"""

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

    # 49カラムに統一したデータを作成
    base_data = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        date = row['backtest_date']

        logger.info(f"Processing {idx+1}/{len(grok_df)}: {ticker} on {date.date()}")

        # 前日・前々日データ取得
        prev_data = fetch_previous_days_data(ticker, date)

        # v2.0.3ロジック適用
        v2_data = apply_v2_0_3_logic(row, prev_data['prev_day_close'])

        # 時価総額取得（daily_closeを使用）
        market_cap = fetch_market_cap(ticker, row['daily_close'], date)

        # ボラティリティ計算
        morning_volatility = np.nan
        if not pd.isna(row.get('morning_high')) and not pd.isna(row.get('morning_low')) and row['buy_price'] > 0:
            morning_volatility = (row['morning_high'] - row['morning_low']) / row['buy_price'] * 100

        daily_volatility = np.nan
        if not pd.isna(row.get('high')) and not pd.isna(row.get('low')) and row['buy_price'] > 0:
            daily_volatility = (row['high'] - row['low']) / row['buy_price'] * 100

        # 前日比計算
        prev_day_change_pct = np.nan
        if not pd.isna(prev_data['prev_day_close']) and prev_data['prev_day_close'] > 0:
            prev_day_change_pct = (row['buy_price'] - prev_data['prev_day_close']) / prev_data['prev_day_close'] * 100

        # 前日出来高比
        prev_day_volume_ratio = np.nan
        morning_volume = row.get('morning_volume')
        if not pd.isna(prev_data['prev_day_volume']) and prev_data['prev_day_volume'] > 0 and not pd.isna(morning_volume):
            prev_day_volume_ratio = morning_volume / prev_data['prev_day_volume']

        # 49カラムに統一したレコード作成
        base_record = {
            # 1-8: Basic info
            'selection_date': row.get('selection_date'),
            'backtest_date': date,
            'ticker': ticker,
            'company_name': row.get('company_name', ''),
            'category': row.get('category', ''),
            'reason': row.get('reason', ''),
            'grok_rank': row.get('grok_rank'),
            'selection_score': row.get('selection_score', 0.0),

            # 9-14: Price data
            'buy_price': row.get('buy_price'),
            'sell_price': row.get('sell_price'),
            'daily_close': row.get('daily_close'),
            'high': row.get('high'),
            'low': row.get('low'),
            'volume': row.get('volume'),

            # 15-17: Phase 1-2 results
            'phase1_win': row.get('phase1_win'),
            'phase2_win': row.get('phase2_win'),
            'profit_per_100_shares_phase2': row.get('profit_per_100_shares_phase2'),

            # 18-23: Phase 3 results
            'phase3_1pct_win': row.get('phase3_1pct_win'),
            'phase3_1pct_exit_reason': row.get('phase3_1pct_exit_reason'),
            'phase3_2pct_win': row.get('phase3_2pct_win'),
            'phase3_2pct_exit_reason': row.get('phase3_2pct_exit_reason'),
            'phase3_3pct_win': row.get('phase3_3pct_win'),
            'phase3_3pct_exit_reason': row.get('phase3_3pct_exit_reason'),

            # 24-25: Max gain/drawdown
            'daily_max_gain_pct': row.get('daily_max_gain_pct'),
            'daily_max_drawdown_pct': row.get('daily_max_drawdown_pct'),

            # 26-27: Metadata
            'data_source': row.get('data_source', 'legacy'),
            'prompt_version': 'v2_0_3',

            # 28-31: Market/timing data
            'market_cap': market_cap,
            'morning_volume': row.get('morning_volume'),
            'day_high': row.get('high'),
            'day_low': row.get('low'),

            # 32-35: Previous days data
            'prev_day_close': prev_data['prev_day_close'],
            'prev_day_volume': prev_data['prev_day_volume'],
            'prev_2day_close': prev_data['prev_2day_close'],
            'prev_2day_volume': prev_data['prev_2day_volume'],

            # 36-39: Volatility and ratios
            'morning_volatility': morning_volatility,
            'daily_volatility': daily_volatility,
            'prev_day_change_pct': prev_day_change_pct,
            'prev_day_volume_ratio': prev_day_volume_ratio,

            # 40-44: Phase returns (percentage)
            'phase1_return_pct': row.get('phase1_return', 0) * 100,
            'phase2_return_pct': row.get('phase2_return', 0) * 100,
            'phase3_1pct_return_pct': row.get('phase3_1pct_return', 0) * 100,
            'phase3_2pct_return_pct': row.get('phase3_2pct_return', 0) * 100,
            'phase3_3pct_return_pct': row.get('phase3_3pct_return', 0) * 100,

            # 45: Previous date
            'prev_date': (date - timedelta(days=1)).strftime('%Y-%m-%d'),

            # 46-49: v2 columns (v2.0.3 logic applied)
            'v2_score': v2_data['v2_score'],
            'v2_action': v2_data['v2_action'],
            'v2_confidence': v2_data['v2_confidence'],
            'v2_reasons_json': v2_data['v2_reasons_json'],
        }

        base_data.append(base_record)

    # DataFrame化
    base_df = pd.DataFrame(base_data)

    logger.info(f"\nTotal records after processing: {len(base_df)}")
    logger.info(f"Columns: {len(base_df.columns)}")

    # 保存
    DATA_DIR.mkdir(parents=True, exist_ok=True)
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
