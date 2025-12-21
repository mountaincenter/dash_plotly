"""
ORB + VWAP バックテスト メインスクリプト
Version: 1.0.0

Usage:
    cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/improvement
    python scripts/orb_vwap_backtest.py
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from orb_vwap_utils import (
    calculate_vwap,
    calculate_opening_range,
    get_entry_signal,
    check_exit_condition,
    calculate_pnl,
    get_next_trading_day,
)

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "backtest"

# 取引時間設定
OR_START = time(9, 0)
OR_END = time(9, 30)
ENTRY_START = time(9, 30)
ENTRY_END = time(14, 30)
MARKET_CLOSE = time(15, 30)
LUNCH_START = time(11, 30)
LUNCH_END = time(12, 30)


def load_data() -> tuple:
    """データをロードする"""
    print("Loading data...")

    # Grok推奨銘柄（日付指定ファイルを優先）
    grok_dated_path = DATA_DIR / "backtest" / "grok_trending_20251127.parquet"
    grok_path = DATA_DIR / "grok_trending.parquet"

    if grok_dated_path.exists():
        grok_df = pd.read_parquet(grok_dated_path)
        print(f"  grok_trending_20251127: {len(grok_df)} records")
    elif grok_path.exists():
        grok_df = pd.read_parquet(grok_path)
        print(f"  grok_trending: {len(grok_df)} records")
    else:
        raise FileNotFoundError(f"Grok trending data not found")

    # 5分足データ（Grok銘柄専用ファイルを優先）
    prices_5m_grok_path = DATA_DIR / "prices_60d_5m_grok.parquet"
    prices_5m_path = DATA_DIR / "prices_60d_5m.parquet"

    if prices_5m_grok_path.exists():
        prices_5m = pd.read_parquet(prices_5m_grok_path)
        print(f"  prices_60d_5m_grok: {len(prices_5m)} records")
    elif prices_5m_path.exists():
        prices_5m = pd.read_parquet(prices_5m_path)
        print(f"  prices_60d_5m: {len(prices_5m)} records")
    else:
        raise FileNotFoundError(f"5-min prices not found")

    # 日足データ（比較用）
    prices_1d_path = DATA_DIR / "prices_max_1d.parquet"
    prices_1d = None
    if prices_1d_path.exists():
        prices_1d = pd.read_parquet(prices_1d_path)
        print(f"  prices_max_1d: {len(prices_1d)} records")

    return grok_df, prices_5m, prices_1d


def get_trading_dates(prices_5m: pd.DataFrame) -> List[str]:
    """5分足データから営業日リストを取得"""
    if 'date' in prices_5m.columns:
        dates = pd.to_datetime(prices_5m['date']).dt.date.unique()
    elif 'datetime' in prices_5m.columns:
        dates = pd.to_datetime(prices_5m['datetime']).dt.date.unique()
    else:
        dates = prices_5m.index.date.unique()

    return sorted([d.strftime('%Y-%m-%d') for d in dates])


def run_backtest_for_ticker_date(
    ticker: str,
    date_str: str,
    prices_5m: pd.DataFrame,
    grok_rank: int
) -> Optional[Dict[str, Any]]:
    """
    単一銘柄・単一日のバックテストを実行

    Parameters:
        ticker: 銘柄コード
        date_str: 取引日（YYYY-MM-DD）
        prices_5m: 5分足データ
        grok_rank: Grokランキング

    Returns:
        バックテスト結果の辞書。シグナルなしの場合も結果を返す
    """
    # 該当銘柄・日付のデータを抽出
    if 'ticker' in prices_5m.columns:
        ticker_data = prices_5m[prices_5m['ticker'] == ticker].copy()
    else:
        return None

    if ticker_data.empty:
        return None

    # datetime列の処理
    if 'date' in ticker_data.columns:
        ticker_data['datetime'] = pd.to_datetime(ticker_data['date'])
        ticker_data['date_str'] = ticker_data['datetime'].dt.strftime('%Y-%m-%d')
        ticker_data['time'] = ticker_data['datetime'].dt.time
    elif 'datetime' in ticker_data.columns:
        ticker_data['datetime'] = pd.to_datetime(ticker_data['datetime'])
        ticker_data['date_str'] = ticker_data['datetime'].dt.strftime('%Y-%m-%d')
        ticker_data['time'] = ticker_data['datetime'].dt.time
    else:
        return None

    # 指定日のデータ
    day_data = ticker_data[ticker_data['date_str'] == date_str].copy()
    if day_data.empty:
        return None

    day_data = day_data.sort_values('datetime').reset_index(drop=True)

    # Opening Range 計算
    or_high, or_low = calculate_opening_range(day_data, OR_START, OR_END)
    if or_high is None or or_low is None:
        return {
            'ticker': ticker,
            'date': date_str,
            'grok_rank': grok_rank,
            'signal': 'NO_SIGNAL',
            'entry_time': None,
            'entry_price': None,
            'exit_time': None,
            'exit_price': None,
            'exit_reason': 'NO_OR_DATA',
            'pnl_pct': 0.0,
            'pnl_amount': 0.0,
            'or_high': None,
            'or_low': None,
            'or_range_pct': None,
            'vwap_at_entry': None,
        }

    or_range = or_high - or_low
    or_range_pct = (or_range / or_low) * 100 if or_low > 0 else 0

    # VWAP計算（9:00からの累積）
    day_data['vwap'] = calculate_vwap(day_data)

    # エントリー探索（9:30以降、14:30まで）
    entry_time = None
    entry_price = None
    vwap_at_entry = None
    signal = 'NO_SIGNAL'

    for idx, row in day_data.iterrows():
        row_time = row['time']

        # エントリー時間外はスキップ
        if row_time < ENTRY_START or row_time > ENTRY_END:
            continue

        # 昼休みはスキップ
        if LUNCH_START <= row_time < LUNCH_END:
            continue

        current_price = row['Close']
        current_vwap = row['vwap']

        if pd.isna(current_vwap):
            continue

        # シグナル判定
        sig = get_entry_signal(current_price, or_high, or_low, current_vwap)

        if sig != 'NO_SIGNAL':
            signal = sig
            entry_time = row['datetime']
            entry_price = current_price
            vwap_at_entry = current_vwap
            break

    # シグナルなしの場合
    if signal == 'NO_SIGNAL':
        return {
            'ticker': ticker,
            'date': date_str,
            'grok_rank': grok_rank,
            'signal': 'NO_SIGNAL',
            'entry_time': None,
            'entry_price': None,
            'exit_time': None,
            'exit_price': None,
            'exit_reason': 'NO_SIGNAL',
            'pnl_pct': 0.0,
            'pnl_amount': 0.0,
            'or_high': or_high,
            'or_low': or_low,
            'or_range_pct': or_range_pct,
            'vwap_at_entry': None,
        }

    # 決済探索
    exit_time = None
    exit_price = None
    exit_reason = 'TIME_EXIT'

    entry_idx = day_data[day_data['datetime'] == entry_time].index[0]
    post_entry_data = day_data.loc[entry_idx + 1:]

    for idx, row in post_entry_data.iterrows():
        row_time = row['time']

        # 昼休みはスキップ
        if LUNCH_START <= row_time < LUNCH_END:
            continue

        current_price = row['Close']

        # 決済条件チェック
        should_exit, reason = check_exit_condition(
            signal, current_price, entry_price, vwap_at_entry, or_range
        )

        if should_exit:
            exit_time = row['datetime']
            exit_price = current_price
            exit_reason = reason
            break

        # 大引け強制決済（15:25以降の最終足）
        if row_time >= time(15, 25):
            exit_time = row['datetime']
            exit_price = current_price
            exit_reason = 'TIME_EXIT'
            break

    # 決済できなかった場合は最終の有効データで決済
    if exit_time is None:
        if not post_entry_data.empty:
            # NaN以外の最終行を探す
            valid_data = post_entry_data.dropna(subset=['Close'])
            if not valid_data.empty:
                last_row = valid_data.iloc[-1]
                exit_time = last_row['datetime']
                exit_price = last_row['Close']
                exit_reason = 'TIME_EXIT'
            else:
                # 有効データがない場合はエントリー価格で決済（ストップ高/安の可能性）
                exit_time = entry_time
                exit_price = entry_price
                exit_reason = 'LIMIT_HIT'
        else:
            # post_entry_dataが空の場合（エントリーが最終足）
            exit_time = entry_time
            exit_price = entry_price
            exit_reason = 'LIMIT_HIT'

    # 損益計算
    if entry_price and exit_price:
        pnl_pct, pnl_amount = calculate_pnl(signal, entry_price, exit_price)
    else:
        pnl_pct, pnl_amount = 0.0, 0.0

    return {
        'ticker': ticker,
        'date': date_str,
        'grok_rank': grok_rank,
        'signal': signal,
        'entry_time': entry_time,
        'entry_price': entry_price,
        'exit_time': exit_time,
        'exit_price': exit_price,
        'exit_reason': exit_reason,
        'pnl_pct': pnl_pct,
        'pnl_amount': pnl_amount,
        'or_high': or_high,
        'or_low': or_low,
        'or_range_pct': or_range_pct,
        'vwap_at_entry': vwap_at_entry,
    }


def run_backtest(grok_df: pd.DataFrame, prices_5m: pd.DataFrame) -> pd.DataFrame:
    """
    全銘柄・全日のバックテストを実行

    Parameters:
        grok_df: Grok推奨銘柄データ
        prices_5m: 5分足データ

    Returns:
        バックテスト結果のデータフレーム
    """
    results = []
    trading_dates = get_trading_dates(prices_5m)

    print(f"\nRunning backtest...")
    print(f"  Trading dates: {len(trading_dates)} days")
    print(f"  Date range: {trading_dates[0]} to {trading_dates[-1]}")

    # selection_date カラムの確認（'backtest_date' > 'selection_date' > 'date'）
    date_col = None
    if 'backtest_date' in grok_df.columns:
        date_col = 'backtest_date'
    elif 'selection_date' in grok_df.columns:
        date_col = 'selection_date'
    elif 'date' in grok_df.columns:
        date_col = 'date'

    if date_col is None:
        print("Warning: No date column found. Using all trading dates.")
        selection_dates = None
    else:
        selection_dates = grok_df[date_col].unique()
        print(f"  Using '{date_col}' column for backtest dates")

    # ticker カラム名の確認
    ticker_col = 'ticker' if 'ticker' in grok_df.columns else 'code'
    rank_col = 'grok_rank' if 'grok_rank' in grok_df.columns else 'rank'

    # 各銘柄・各日でバックテスト
    processed = 0
    total = len(grok_df)

    for idx, row in grok_df.iterrows():
        ticker = row[ticker_col]
        grok_rank = row.get(rank_col, 0)

        if selection_dates is not None and date_col is not None:
            target_date_raw = str(row.get(date_col, ''))[:10]  # YYYY-MM-DD形式
            # backtest_dateの場合はその日を対象、それ以外は翌営業日
            if date_col == 'backtest_date':
                target_date = target_date_raw if target_date_raw in trading_dates else None
            else:
                target_date = get_next_trading_day(target_date_raw, trading_dates)
        else:
            # selection_dateがない場合は全営業日を対象
            target_date = None

        if target_date:
            # 特定日のみバックテスト
            result = run_backtest_for_ticker_date(
                ticker, target_date, prices_5m, grok_rank
            )
            if result:
                results.append(result)
        else:
            # 全営業日でバックテスト
            for trade_date in trading_dates:
                result = run_backtest_for_ticker_date(
                    ticker, trade_date, prices_5m, grok_rank
                )
                if result:
                    results.append(result)

        processed += 1
        if processed % 10 == 0:
            print(f"  Processed {processed}/{total} stocks...")

    print(f"  Completed: {len(results)} trade records")

    return pd.DataFrame(results)


def generate_summary(results_df: pd.DataFrame, prices_1d: pd.DataFrame = None) -> Dict:
    """
    バックテスト結果のサマリーを生成

    Parameters:
        results_df: バックテスト結果
        prices_1d: 日足データ（現行戦略比較用）

    Returns:
        サマリー辞書
    """
    # トレードのあった結果のみ
    trades = results_df[results_df['signal'] != 'NO_SIGNAL']

    if trades.empty:
        return {
            'period': {
                'start': results_df['date'].min() if not results_df.empty else None,
                'end': results_df['date'].max() if not results_df.empty else None,
                'trading_days': results_df['date'].nunique() if not results_df.empty else 0,
            },
            'total_trades': 0,
            'buy_signals': 0,
            'sell_signals': 0,
            'no_signals': len(results_df),
            'win_rate': 0.0,
            'avg_return_pct': 0.0,
            'total_return_pct': 0.0,
            'max_gain_pct': 0.0,
            'max_loss_pct': 0.0,
            'exit_reasons': {},
            'comparison_vs_current': {},
        }

    # 勝敗判定
    wins = trades[trades['pnl_pct'] > 0]
    losses = trades[trades['pnl_pct'] < 0]

    summary = {
        'period': {
            'start': results_df['date'].min(),
            'end': results_df['date'].max(),
            'trading_days': results_df['date'].nunique(),
        },
        'total_trades': len(trades),
        'buy_signals': len(trades[trades['signal'] == 'BUY']),
        'sell_signals': len(trades[trades['signal'] == 'SELL']),
        'no_signals': len(results_df[results_df['signal'] == 'NO_SIGNAL']),
        'win_rate': len(wins) / len(trades) * 100 if len(trades) > 0 else 0,
        'avg_return_pct': trades['pnl_pct'].mean(),
        'total_return_pct': trades['pnl_pct'].sum(),
        'max_gain_pct': trades['pnl_pct'].max(),
        'max_loss_pct': trades['pnl_pct'].min(),
        'exit_reasons': trades['exit_reason'].value_counts().to_dict(),
        'by_grok_rank': {},
    }

    # Grokランク別の勝率
    for rank in sorted(trades['grok_rank'].unique()):
        rank_trades = trades[trades['grok_rank'] == rank]
        rank_wins = rank_trades[rank_trades['pnl_pct'] > 0]
        summary['by_grok_rank'][int(rank)] = {
            'trades': len(rank_trades),
            'win_rate': len(rank_wins) / len(rank_trades) * 100 if len(rank_trades) > 0 else 0,
            'avg_return': rank_trades['pnl_pct'].mean(),
        }

    # 現行戦略との比較（prices_1dがある場合）
    # TODO: 現行戦略（寄付買い→大引け売り）のリターンを計算して比較

    return summary


def main():
    """メイン処理"""
    print("=" * 60)
    print("ORB + VWAP Backtest")
    print("=" * 60)

    # データロード
    grok_df, prices_5m, prices_1d = load_data()

    # バックテスト実行
    results_df = run_backtest(grok_df, prices_5m)

    # 結果保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results_path = OUTPUT_DIR / "orb_vwap_backtest_results.parquet"
    results_df.to_parquet(results_path, index=False)
    print(f"\nResults saved: {results_path}")

    # サマリー生成
    summary = generate_summary(results_df, prices_1d)

    summary_path = OUTPUT_DIR / "orb_vwap_backtest_summary.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    print(f"Summary saved: {summary_path}")

    # サマリー表示
    print("\n" + "=" * 60)
    print("Backtest Summary")
    print("=" * 60)
    print(f"Period: {summary['period']['start']} to {summary['period']['end']}")
    print(f"Trading days: {summary['period']['trading_days']}")
    print(f"Total trades: {summary['total_trades']}")
    print(f"  - BUY signals: {summary['buy_signals']}")
    print(f"  - SELL signals: {summary['sell_signals']}")
    print(f"  - NO signals: {summary['no_signals']}")
    print(f"Win rate: {summary['win_rate']:.1f}%")
    print(f"Average return: {summary['avg_return_pct']:.2f}%")
    print(f"Total return: {summary['total_return_pct']:.2f}%")
    print(f"Max gain: {summary['max_gain_pct']:.2f}%")
    print(f"Max loss: {summary['max_loss_pct']:.2f}%")
    print(f"Exit reasons: {summary['exit_reasons']}")

    if summary.get('by_grok_rank'):
        print("\nBy Grok Rank:")
        for rank, stats in summary['by_grok_rank'].items():
            print(f"  Rank {rank}: {stats['trades']} trades, "
                  f"win rate {stats['win_rate']:.1f}%, "
                  f"avg return {stats['avg_return']:.2f}%")

    print("\n" + "=" * 60)
    print("Done!")


if __name__ == "__main__":
    main()
