#!/usr/bin/env python3
"""
trading_recommendation.json と deep_analysis_YYYY-MM-DD.json から
grok_analysis_merged.parquet に直接マージ

Usage:
  python3 scripts/pipeline/merge_json_to_grok_analysis.py
"""
import sys
from pathlib import Path
import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional

# J-Quants クライアント
sys.path.append(str(Path(__file__).resolve().parents[2]))
from scripts.lib.jquants_client import JQuantsClient

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]

# ファイルパス
TRADING_REC_JSON = ROOT / 'data/parquet/backtest/trading_recommendation.json'
DEEP_ANALYSIS_DIR = ROOT / 'data/parquet/backtest/analysis'
GROK_ANALYSIS_PARQUET = ROOT / 'data/parquet/backtest/grok_analysis_merged.parquet'
PRICES_1D = ROOT / 'data/parquet/prices_max_1d.parquet'
PRICES_5M = ROOT / 'data/parquet/prices_60d_5m.parquet'

# J-Quantsクライアント（グローバル）
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
                issued_shares = float(issued_shares)
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
        market_cap = close_price * (issued_shares / adjustment_factor)

        return market_cap

    except Exception as e:
        logger.warning(f"Failed to fetch market cap for {ticker}: {e}")
        return None


def fetch_previous_days_data(ticker: str, date: datetime, prices_1d_df: pd.DataFrame) -> dict:
    """
    前日・前々日の終値と出来高を取得（parquetから優先、なければyfinance）

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


def get_backtest_date_from_json(rec_data):
    """trading_recommendation.jsonから対象日を取得"""
    # technicalDataDate = selection_date = backtest_date（同日）
    tech_date = rec_data.get('dataSource', {}).get('technicalDataDate')
    if not tech_date:
        raise ValueError("technicalDataDate not found in trading_recommendation.json")

    tech_dt = datetime.strptime(tech_date, '%Y-%m-%d')
    backtest_dt = tech_dt  # +1しない！selection_dateと同じ日
    # selection_date と backtest_date は同じ
    return backtest_dt.strftime('%Y-%m-%d'), backtest_dt.strftime('%Y-%m-%d')


def calculate_backtest_metrics(ticker, backtest_date, prices_1d_df, prices_5m_df):
    """バックテスト指標を計算"""
    from datetime import time

    backtest_dt = pd.to_datetime(backtest_date)

    # 1日データ取得
    day_data = prices_1d_df[
        (prices_1d_df['ticker'] == ticker) &
        (prices_1d_df['date'].dt.date == backtest_dt.date())
    ]

    if day_data.empty:
        return None

    day_row = day_data.iloc[0]
    open_price = float(day_row['Open'])
    high_price = float(day_row['High'])
    low_price = float(day_row['Low'])
    close_price = float(day_row['Close'])
    volume = int(day_row['Volume'])

    # buy_price は backtest_date の Open
    buy_price = open_price

    # 前場データ取得
    morning_data = prices_5m_df[
        (prices_5m_df['ticker'] == ticker) &
        (prices_5m_df['date'].dt.date == backtest_dt.date()) &
        (prices_5m_df['date'].dt.time >= time(9, 0)) &
        (prices_5m_df['date'].dt.time <= time(11, 30))
    ]

    if not morning_data.empty:
        morning_high_val = morning_data['High'].max()
        morning_low_val = morning_data['Low'].min()
        morning_close_val = morning_data.iloc[-1]['Close']
        morning_volume_val = morning_data['Volume'].sum()

        # NaN チェック: NaN の場合は日次データにフォールバック
        morning_high = float(morning_high_val) if pd.notna(morning_high_val) else high_price
        morning_low = float(morning_low_val) if pd.notna(morning_low_val) else low_price
        morning_close = float(morning_close_val) if pd.notna(morning_close_val) else close_price
        morning_volume = int(morning_volume_val) if pd.notna(morning_volume_val) else (volume // 2)
    else:
        morning_high = high_price
        morning_low = low_price
        morning_close = close_price
        morning_volume = volume // 2

    # Phase 1: 前場終値
    phase1_return = morning_close - buy_price
    phase1_return_pct = (phase1_return / buy_price * 100) if buy_price > 0 else 0
    phase1_win = phase1_return > 0
    profit_per_100_shares_phase1 = phase1_return * 100

    # Phase 2: 大引け
    phase2_return = close_price - buy_price
    phase2_return_pct = (phase2_return / buy_price * 100) if buy_price > 0 else 0
    phase2_win = phase2_return > 0
    profit_per_100_shares_phase2 = phase2_return * 100

    # Phase 3: 損切りシミュレーション
    def calc_phase3(stop_pct):
        stop_price = buy_price * (1 - stop_pct / 100)
        if low_price <= stop_price:
            exit_price = stop_price
            exit_reason = "stop_loss"
        elif high_price >= buy_price * (1 + stop_pct / 100):
            exit_price = close_price
            exit_reason = "take_profit"
        else:
            exit_price = close_price
            exit_reason = "eod"

        ret = exit_price - buy_price
        ret_pct = (ret / buy_price * 100) if buy_price > 0 else 0
        return {
            'return': ret,
            'return_pct': ret_pct,
            'win': ret > 0,
            'exit_reason': exit_reason,
            'profit_per_100_shares': ret * 100
        }

    phase3_1pct = calc_phase3(1)
    phase3_2pct = calc_phase3(2)
    phase3_3pct = calc_phase3(3)

    # Max gain/drawdown
    morning_max_gain_pct = ((morning_high - buy_price) / buy_price * 100) if buy_price > 0 else 0
    morning_max_drawdown_pct = ((morning_low - buy_price) / buy_price * 100) if buy_price > 0 else 0
    daily_max_gain_pct = ((high_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
    daily_max_drawdown_pct = ((low_price - buy_price) / buy_price * 100) if buy_price > 0 else 0

    # Profit metrics
    profit_morning = morning_close - buy_price
    profit_day_close = close_price - buy_price
    profit_morning_pct = (profit_morning / buy_price * 100) if buy_price > 0 else 0
    profit_day_close_pct = (profit_day_close / buy_price * 100) if buy_price > 0 else 0

    # Win/loss判定
    is_win_morning = profit_morning > 0
    is_win_day_close = profit_day_close > 0

    # Better timing
    better_profit_timing = 'morning_close' if profit_morning > profit_day_close else 'day_close'
    better_loss_timing = 'morning_close' if profit_morning > profit_day_close else 'day_close'

    # Volatility (placeholder, will be calculated in main with prev_day data)
    morning_volatility = ((morning_high - morning_low) / buy_price * 100) if buy_price > 0 else 0
    daily_volatility = ((high_price - low_price) / buy_price * 100) if buy_price > 0 else 0

    return {
        'buy_price': buy_price,
        'sell_price': close_price,
        'daily_close': close_price,
        'high': high_price,
        'low': low_price,
        'volume': volume,

        # Morning data
        'morning_close_price': morning_close,
        'morning_volume': morning_volume,
        'morning_high': morning_high,
        'morning_low': morning_low,

        # Day data (aliases)
        'day_close_price': close_price,
        'day_high': high_price,
        'day_low': low_price,

        # Phase 1
        'phase1_return': phase1_return,
        'phase1_win': phase1_win,
        'profit_per_100_shares_phase1': profit_per_100_shares_phase1,
        'phase1_return_pct': phase1_return_pct,

        # Phase 2
        'phase2_return': phase2_return,
        'phase2_win': phase2_win,
        'profit_per_100_shares_phase2': profit_per_100_shares_phase2,
        'phase2_return_pct': phase2_return_pct,

        # Phase 3 - 1%
        'phase3_1pct_return': phase3_1pct['return'],
        'phase3_1pct_win': phase3_1pct['win'],
        'phase3_1pct_exit_reason': phase3_1pct['exit_reason'],
        'profit_per_100_shares_phase3_1pct': phase3_1pct['profit_per_100_shares'],
        'phase3_1pct_return_pct': phase3_1pct['return_pct'],

        # Phase 3 - 2%
        'phase3_2pct_return': phase3_2pct['return'],
        'phase3_2pct_win': phase3_2pct['win'],
        'phase3_2pct_exit_reason': phase3_2pct['exit_reason'],
        'profit_per_100_shares_phase3_2pct': phase3_2pct['profit_per_100_shares'],
        'phase3_2pct_return_pct': phase3_2pct['return_pct'],

        # Phase 3 - 3%
        'phase3_3pct_return': phase3_3pct['return'],
        'phase3_3pct_win': phase3_3pct['win'],
        'phase3_3pct_exit_reason': phase3_3pct['exit_reason'],
        'profit_per_100_shares_phase3_3pct': phase3_3pct['profit_per_100_shares'],
        'phase3_3pct_return_pct': phase3_3pct['return_pct'],

        # Max gain/drawdown
        'morning_max_gain_pct': morning_max_gain_pct,
        'morning_max_drawdown_pct': morning_max_drawdown_pct,
        'daily_max_gain_pct': daily_max_gain_pct,
        'daily_max_drawdown_pct': daily_max_drawdown_pct,

        # Profit metrics
        'profit_morning': profit_morning,
        'profit_day_close': profit_day_close,
        'profit_morning_pct': profit_morning_pct,
        'profit_day_close_pct': profit_day_close_pct,

        # Win/loss
        'is_win_morning': is_win_morning,
        'is_win_day_close': is_win_day_close,

        # Better timing
        'better_profit_timing': better_profit_timing,
        'better_loss_timing': better_loss_timing,

        # Volatility
        'morning_volatility': morning_volatility,
        'daily_volatility': daily_volatility,
    }


def main():
    print("=" * 60)
    print("Merge JSON files to grok_analysis_merged.parquet")
    print("=" * 60)

    # Step 1: Load trading_recommendation.json
    print("\n[Step 1] Loading trading_recommendation.json...")
    if not TRADING_REC_JSON.exists():
        print(f"❌ File not found: {TRADING_REC_JSON}")
        sys.exit(1)

    with open(TRADING_REC_JSON, 'r', encoding='utf-8') as f:
        rec_data = json.load(f)

    # DEBUG: Print keys to verify dataSource exists
    print(f"  DEBUG: JSON keys: {list(rec_data.keys())}")
    if 'dataSource' in rec_data:
        print(f"  DEBUG: dataSource keys: {list(rec_data['dataSource'].keys())}")
        print(f"  DEBUG: technicalDataDate: {rec_data['dataSource'].get('technicalDataDate')}")
    else:
        print(f"  DEBUG: dataSource NOT FOUND in JSON")

    backtest_date, selection_date = get_backtest_date_from_json(rec_data)
    print(f"  Selection date: {selection_date}")
    print(f"  Backtest date: {backtest_date}")
    print(f"  Stocks: {len(rec_data['stocks'])}")

    # Step 2: Skip deep_analysis (not used)
    print("\n[Step 2] Skipping deep_analysis (not used)...")

    # Step 3: Load grok_trending data for category, reason, selection_score
    print("\n[Step 3] Loading grok_trending data...")
    grok_trending_file = ROOT / 'data' / 'parquet' / 'backtest' / f'grok_trending_{backtest_date.replace("-", "")}.parquet'

    grok_trending_map = {}
    if grok_trending_file.exists():
        grok_trending_df = pd.read_parquet(grok_trending_file)
        for _, row in grok_trending_df.iterrows():
            grok_trending_map[row['ticker']] = {
                'category': row.get('category', ''),
                'reason': row.get('reason', ''),
                'selection_score': row.get('selection_score', 0.0)
            }
        print(f"  grok_trending_{backtest_date.replace('-', '')}.parquet: {len(grok_trending_df)} stocks")
    else:
        print(f"  ⚠️  grok_trending file not found: {grok_trending_file}")

    # Step 4: Load price data
    print("\n[Step 4] Loading price data...")
    prices_1d_df = pd.read_parquet(PRICES_1D)
    prices_1d_df['date'] = pd.to_datetime(prices_1d_df['date'])

    prices_5m_df = pd.read_parquet(PRICES_5M)
    prices_5m_df['date'] = pd.to_datetime(prices_5m_df['date'])

    print(f"  prices_max_1d.parquet: {len(prices_1d_df)} records")
    print(f"  prices_60d_5m.parquet: {len(prices_5m_df)} records")

    # Step 5: Build new records
    print("\n[Step 5] Building new records...")
    new_records = []
    backtest_dt = pd.to_datetime(backtest_date)

    for idx, stock in enumerate(rec_data['stocks'], 1):
        ticker = stock['ticker']
        stock_name = stock.get('company_name', stock.get('stockName', ''))  # v2.1 or v2.0.3
        grok_rank = stock.get('grok_rank', stock.get('grokRank', 0))  # v2.1 or v2.0.3

        # Handle both nested (v2.0.3) and flat (v2.1) schema
        if 'technicalData' in stock and 'recommendation' in stock:
            # v2.0.3 nested schema
            tech_data = stock['technicalData']
            rec = stock['recommendation']
        else:
            # v2.1 flat schema - create nested structure for compatibility
            tech_data = {
                'prevDayClose': stock.get('prev_day_close', 0),
                'prevDayChangePct': stock.get('prev_day_change_pct', 0),
                'atrPct': stock.get('atr_pct', 0),
            }
            rec = {
                'action': stock.get('v2_0_3_action', '静観'),
                'score': stock.get('v2_0_3_score', 0),
                'confidence': '中',
                'reasons': stock.get('v2_0_3_reasons', '').split(' / ') if isinstance(stock.get('v2_0_3_reasons'), str) else []
            }

        print(f"  Processing {idx}/{len(rec_data['stocks'])}: {ticker} {stock_name}")

        # Calculate backtest metrics
        metrics = calculate_backtest_metrics(
            ticker, backtest_date,
            prices_1d_df, prices_5m_df
        )

        if not metrics:
            print(f"    ⚠️  No price data for {backtest_date}, skipping")
            continue

        # Fetch market cap
        market_cap = fetch_market_cap(ticker, metrics['daily_close'], backtest_dt)

        # Fetch previous days data (relative to backtest_date)
        prev_data = fetch_previous_days_data(ticker, backtest_dt, prices_1d_df)

        # Get category, reason, selection_score from grok_trending
        trending_data = grok_trending_map.get(ticker, {})

        # === v2.0.3: Calculate v2_action with price-based forced positions ===
        prev_day_close_val = prev_data['prev_day_close']

        # Default v2_action from recommendation or deep analysis
        base_action = rec['action']  # 'buy', 'sell', 'hold'
        action_map = {'buy': '買い', 'sell': '売り', 'hold': '静観'}
        v2_action_default = action_map.get(base_action, '静観')

        # v2_score: Use score and confidence from trading_recommendation only
        v2_score = rec['score']
        # Convert English confidence to Japanese
        confidence_map = {'high': '高', 'medium': '中', 'low': '低'}
        v2_confidence = confidence_map.get(rec['confidence'], '中')

        # Apply v2.0.3 price-based forced positions
        v2_reasons = []
        if not pd.isna(prev_day_close_val):
            if 5000 <= prev_day_close_val < 10000:
                # Forced buy for 5,000-10,000円
                v2_action = '買い'
                v2_confidence = '高'
                v2_reasons.append({
                    'type': 'price_5000_10000',
                    'description': f'5,000-10,000円範囲（{prev_day_close_val:,.0f}円）→ロング戦略',
                    'impact': 0
                })
            elif prev_day_close_val >= 10000:
                # Forced sell for >10,000円
                v2_action = '売り'
                v2_confidence = '高'
                v2_reasons.append({
                    'type': 'price_over_10000',
                    'description': f'10,000円超え（{prev_day_close_val:,.0f}円）→ショート戦略',
                    'impact': 0
                })
            else:
                # Use default action for <5,000円 and use full reasons from trading_recommendation.json
                v2_action = v2_action_default
                v2_reasons = rec.get('reasons', [])
        else:
            # No prev_day_close data, use default and use full reasons from trading_recommendation.json
            v2_action = v2_action_default
            v2_reasons = rec.get('reasons', [])

        # Base record - 49 columns matching existing schema
        record = {
            # 1-8: Basic info
            'selection_date': selection_date,
            'backtest_date': backtest_dt,
            'ticker': ticker,
            'company_name': stock_name,
            'category': trending_data.get('category', ''),
            'reason': trending_data.get('reason', ''),
            'grok_rank': grok_rank,
            'selection_score': trending_data.get('selection_score', 0.0),

            # 9-14: Price data
            'buy_price': metrics['buy_price'],
            'sell_price': metrics['sell_price'],
            'daily_close': metrics['daily_close'],
            'high': metrics['high'],
            'low': metrics['low'],
            'volume': metrics['volume'],

            # 15-17: Phase 1-2 results
            'phase1_win': metrics['phase1_win'],
            'phase2_win': metrics['phase2_win'],
            'profit_per_100_shares_phase2': metrics['profit_per_100_shares_phase2'],

            # 18-23: Phase 3 results
            'phase3_1pct_win': metrics['phase3_1pct_win'],
            'phase3_1pct_exit_reason': metrics['phase3_1pct_exit_reason'],
            'phase3_2pct_win': metrics['phase3_2pct_win'],
            'phase3_2pct_exit_reason': metrics['phase3_2pct_exit_reason'],
            'phase3_3pct_win': metrics['phase3_3pct_win'],
            'phase3_3pct_exit_reason': metrics['phase3_3pct_exit_reason'],

            # 24-25: Max gain/drawdown
            'daily_max_gain_pct': metrics['daily_max_gain_pct'],
            'daily_max_drawdown_pct': metrics['daily_max_drawdown_pct'],

            # 26-27: Metadata
            'data_source': 'trading_recommendation',
            'prompt_version': 'v2_0_3',

            # 28-31: Market/timing data
            'market_cap': market_cap,
            'morning_volume': metrics.get('morning_volume'),
            'day_high': metrics['high'],
            'day_low': metrics['low'],

            # 32-35: Previous days data
            'prev_day_close': prev_data['prev_day_close'],
            'prev_day_volume': prev_data['prev_day_volume'],
            'prev_2day_close': prev_data['prev_2day_close'],
            'prev_2day_volume': prev_data['prev_2day_volume'],

            # 36-39: Volatility and ratios
            'morning_volatility': metrics.get('morning_volatility'),
            'daily_volatility': metrics.get('daily_volatility'),
            'prev_day_change_pct': np.nan,
            'prev_day_volume_ratio': np.nan,

            # 40-44: Phase returns (percentage)
            'phase1_return_pct': metrics['phase1_return_pct'],
            'phase2_return_pct': metrics['phase2_return_pct'],
            'phase3_1pct_return_pct': metrics['phase3_1pct_return_pct'],
            'phase3_2pct_return_pct': metrics['phase3_2pct_return_pct'],
            'phase3_3pct_return_pct': metrics['phase3_3pct_return_pct'],

            # 45: Previous date (for compatibility)
            'prev_date': (backtest_dt - timedelta(days=1)).strftime('%Y-%m-%d'),

            # 46-49: v2 columns (v2.0.3 logic applied)
            'v2_score': v2_score,
            'v2_action': v2_action,
            'v2_confidence': v2_confidence,
            'v2_reasons_json': json.dumps(v2_reasons, ensure_ascii=False),
        }

        # Calculate prev_day_change_pct
        buy_price = metrics['buy_price']
        if not pd.isna(prev_data['prev_day_close']) and prev_data['prev_day_close'] > 0:
            record['prev_day_change_pct'] = (
                (buy_price - prev_data['prev_day_close']) / prev_data['prev_day_close'] * 100
            )

        # Calculate prev_day_volume_ratio
        if not pd.isna(prev_data['prev_day_volume']) and prev_data['prev_day_volume'] > 0 and not pd.isna(metrics.get('morning_volume')):
            record['prev_day_volume_ratio'] = metrics['morning_volume'] / prev_data['prev_day_volume']

        new_records.append(record)

    new_df = pd.DataFrame(new_records)
    print(f"\n  Created {len(new_df)} records")

    # Step 5: Merge with existing data (UPSERT pattern)
    print("\n[Step 5] Merging with existing grok_analysis_merged.parquet...")

    if GROK_ANALYSIS_PARQUET.exists():
        existing_df = pd.read_parquet(GROK_ANALYSIS_PARQUET)
        existing_df['backtest_date'] = pd.to_datetime(existing_df['backtest_date'])

        old_count = len(existing_df)
        backtest_dt = pd.to_datetime(backtest_date)

        print(f"  Existing records: {old_count}")
        print(f"  Date range: {existing_df['backtest_date'].min().date()} to {existing_df['backtest_date'].max().date()}")

        # UPSERT: Remove all existing records for this backtest_date
        existing_df_filtered = existing_df[existing_df['backtest_date'] != backtest_dt]
        removed_for_date = old_count - len(existing_df_filtered)

        print(f"  Removed existing records for {backtest_date}: {removed_for_date}")
        print(f"  New records for {backtest_date}: {len(new_df)}")

        # Append new records for this date
        combined_df = pd.concat([existing_df_filtered, new_df], ignore_index=True)

        final_count = len(combined_df)
        net_change = final_count - old_count

        print(f"\n  Verification:")
        print(f"  - Old total: {old_count}")
        print(f"  - Removed for {backtest_date}: {removed_for_date}")
        print(f"  - Added for {backtest_date}: {len(new_df)}")
        print(f"  - New total: {final_count}")
        print(f"  - Net change: {net_change:+d}")
        print(f"  - Expected: {old_count} - {removed_for_date} + {len(new_df)} = {old_count - removed_for_date + len(new_df)}")

        if final_count == old_count - removed_for_date + len(new_df):
            print(f"  - Status: ✅ PASS (upsert successful)")
        else:
            print(f"  - Status: ❌ FAIL (record count mismatch)")
            sys.exit(1)
    else:
        combined_df = new_df
        print(f"  No existing data - creating new file")
        print(f"  New records: {len(new_df)}")

    combined_df = combined_df.sort_values('backtest_date').reset_index(drop=True)

    print(f"\n  Final: {len(combined_df)} records ({combined_df['backtest_date'].min().date()} to {combined_df['backtest_date'].max().date()})")

    # Step 6: Save
    print("\n[Step 6] Saving...")
    combined_df.to_parquet(GROK_ANALYSIS_PARQUET, index=False)
    print(f"✅ Saved to {GROK_ANALYSIS_PARQUET}")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(combined_df)}")
    print(f"Date range: {combined_df['backtest_date'].min().date()} to {combined_df['backtest_date'].max().date()}")
    print(f"New date added: {backtest_date}")
    print(f"New records: {len(new_df)}")
    print("\n✅ Merge completed successfully!")


if __name__ == '__main__':
    main()
