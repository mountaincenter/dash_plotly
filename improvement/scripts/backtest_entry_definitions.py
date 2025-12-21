#!/usr/bin/env python3
"""
エントリー定義別バックテスト

複数の「高値形成」定義を比較し、最も有効なエントリー条件を特定する。
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"


def load_data():
    """5分足データと常習犯リストを読み込む"""
    df_5m = pd.read_parquet(DATA_DIR / "surge_candidates_5m.parquet")
    df_5m['Datetime'] = pd.to_datetime(df_5m['Datetime']).dt.tz_localize(None)
    df_5m['date'] = df_5m['Datetime'].dt.date
    df_5m['time'] = df_5m['Datetime'].dt.time

    df_watchlist = pd.read_parquet(DATA_DIR / "morning_peak_watchlist.parquet")

    return df_5m, df_watchlist


def calc_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """RSI計算"""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def backtest_pattern_a(day_data: pd.DataFrame, threshold: float = -2.0) -> dict:
    """
    パターンA: 日中高値から-X%下落した時点でエントリー
    イグジット: 大引け
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 10:
        return None

    open_price = day_data.iloc[0]['Open']

    # 高値を追跡しながらエントリーポイントを探す
    running_high = day_data.iloc[0]['High']
    entry_idx = None
    entry_price = None

    for i, row in day_data.iterrows():
        if row['High'] > running_high:
            running_high = row['High']

        # 高値からの下落率
        drop_from_high = (row['Low'] - running_high) / running_high * 100

        if drop_from_high <= threshold and entry_idx is None:
            entry_idx = i
            entry_price = running_high * (1 + threshold / 100)  # 閾値到達時の価格
            break

    if entry_idx is None:
        return None

    # イグジット: 大引け
    exit_price = day_data.iloc[-1]['Close']
    pnl_pct = (entry_price - exit_price) / entry_price * 100  # ショートなので逆

    return {
        'entry_time': day_data.iloc[entry_idx]['Datetime'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct,
        'high_at_entry': running_high
    }


def backtest_pattern_b(day_data: pd.DataFrame) -> dict:
    """
    パターンB: 前場高値をつけた後、後場寄りで前場高値を下回っていたらエントリー
    イグジット: 大引け
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 10:
        return None

    # 前場・後場を分離
    am_data = day_data[day_data['time'] < time(12, 0)]
    pm_data = day_data[day_data['time'] >= time(12, 30)]

    if len(am_data) < 5 or len(pm_data) < 5:
        return None

    am_high = am_data['High'].max()
    pm_open = pm_data.iloc[0]['Open']

    # 後場寄りが前場高値を下回っていたらエントリー
    if pm_open >= am_high:
        return None

    entry_price = pm_open
    exit_price = day_data.iloc[-1]['Close']
    pnl_pct = (entry_price - exit_price) / entry_price * 100

    return {
        'entry_time': pm_data.iloc[0]['Datetime'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct,
        'am_high': am_high
    }


def backtest_pattern_c(day_data: pd.DataFrame, consec_bars: int = 3) -> dict:
    """
    パターンC: 陰線N本連続でエントリー
    イグジット: 大引け
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 10:
        return None

    # 陰線判定
    day_data['is_bearish'] = day_data['Close'] < day_data['Open']

    # 連続陰線を探す
    entry_idx = None
    for i in range(consec_bars - 1, len(day_data)):
        if all(day_data.iloc[i - j]['is_bearish'] for j in range(consec_bars)):
            entry_idx = i
            break

    if entry_idx is None:
        return None

    entry_price = day_data.iloc[entry_idx]['Close']
    exit_price = day_data.iloc[-1]['Close']
    pnl_pct = (entry_price - exit_price) / entry_price * 100

    return {
        'entry_time': day_data.iloc[entry_idx]['Datetime'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct
    }


def backtest_pattern_d(day_data: pd.DataFrame, rsi_high: int = 70, rsi_low: int = 60) -> dict:
    """
    パターンD: RSIが70超→60割れでエントリー
    イグジット: 大引け
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 20:
        return None

    day_data['rsi'] = calc_rsi(day_data['Close'], 14)

    # RSIが70超えた後、60を割ったポイントを探す
    was_overbought = False
    entry_idx = None

    for i, row in day_data.iterrows():
        if pd.isna(row['rsi']):
            continue
        if row['rsi'] > rsi_high:
            was_overbought = True
        if was_overbought and row['rsi'] < rsi_low:
            entry_idx = i
            break

    if entry_idx is None:
        return None

    entry_price = day_data.iloc[entry_idx]['Close']
    exit_price = day_data.iloc[-1]['Close']
    pnl_pct = (entry_price - exit_price) / entry_price * 100

    return {
        'entry_time': day_data.iloc[entry_idx]['Datetime'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct
    }


def backtest_pattern_e(day_data: pd.DataFrame, vol_threshold: float = 2.0, drop_threshold: float = -1.0) -> dict:
    """
    パターンE: 出来高急増後、価格反転（高値から-1%）でエントリー
    イグジット: 大引け
    """
    day_data = day_data.sort_values('Datetime').reset_index(drop=True)

    if len(day_data) < 10:
        return None

    # 出来高の移動平均
    day_data['vol_ma'] = day_data['Volume'].rolling(5).mean()
    day_data['vol_ratio'] = day_data['Volume'] / day_data['vol_ma']

    # 出来高急増ポイントを探す
    vol_spike_idx = None
    for i, row in day_data.iterrows():
        if pd.notna(row['vol_ratio']) and row['vol_ratio'] >= vol_threshold:
            vol_spike_idx = i
            break

    if vol_spike_idx is None:
        return None

    # 出来高急増後の高値を追跡
    post_spike = day_data.iloc[vol_spike_idx:]
    running_high = post_spike.iloc[0]['High']
    entry_idx = None

    for i, row in post_spike.iterrows():
        if row['High'] > running_high:
            running_high = row['High']

        drop = (row['Low'] - running_high) / running_high * 100
        if drop <= drop_threshold:
            entry_idx = i
            break

    if entry_idx is None:
        return None

    entry_price = running_high * (1 + drop_threshold / 100)
    exit_price = day_data.iloc[-1]['Close']
    pnl_pct = (entry_price - exit_price) / entry_price * 100

    return {
        'entry_time': day_data.iloc[entry_idx]['Datetime'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct
    }


def run_backtest():
    """全パターンのバックテストを実行"""
    print("=== エントリー定義別バックテスト ===\n")

    print("1. データ読み込み...")
    df_5m, df_watchlist = load_data()

    # 常習犯リストの銘柄に絞る
    target_tickers = set(df_watchlist['ticker'].unique())
    df_5m = df_5m[df_5m['ticker'].isin(target_tickers)]

    print(f"   対象銘柄: {len(target_tickers)}")
    print(f"   5分足データ: {len(df_5m):,} 行")

    patterns = {
        'A: 高値から-2%': lambda d: backtest_pattern_a(d, -2.0),
        'A: 高値から-3%': lambda d: backtest_pattern_a(d, -3.0),
        'B: 後場寄り<前場高値': backtest_pattern_b,
        'C: 陰線3本連続': lambda d: backtest_pattern_c(d, 3),
        'D: RSI 70→60': backtest_pattern_d,
        'E: 出来高急増→反転': backtest_pattern_e,
    }

    results = {name: [] for name in patterns}

    print("\n2. バックテスト実行...")

    # 銘柄×日付でループ
    grouped = df_5m.groupby(['ticker', 'date'])
    total = len(grouped)

    for i, ((ticker, date), day_data) in enumerate(grouped):
        if (i + 1) % 5000 == 0:
            print(f"   処理中: {i+1:,}/{total:,}")

        for name, func in patterns.items():
            try:
                result = func(day_data)
                if result:
                    result['ticker'] = ticker
                    result['date'] = date
                    results[name].append(result)
            except Exception as e:
                pass

    print("\n3. 結果集計...")

    summary = []
    for name, trades in results.items():
        if not trades:
            continue

        df_trades = pd.DataFrame(trades)

        win_rate = (df_trades['pnl_pct'] > 0).mean() * 100
        avg_pnl = df_trades['pnl_pct'].mean()
        median_pnl = df_trades['pnl_pct'].median()
        total_trades = len(df_trades)

        # 期待値 = 平均損益
        # シャープ比的な指標 = 平均 / 標準偏差
        std_pnl = df_trades['pnl_pct'].std()
        sharpe_like = avg_pnl / std_pnl if std_pnl > 0 else 0

        summary.append({
            'パターン': name,
            'トレード数': total_trades,
            '勝率': f"{win_rate:.1f}%",
            '平均損益': f"{avg_pnl:.2f}%",
            '中央値': f"{median_pnl:.2f}%",
            'シャープ様': f"{sharpe_like:.3f}"
        })

        # 詳細を保存
        df_trades.to_parquet(
            DATA_DIR / f"backtest_{name.split(':')[0].strip()}.parquet",
            index=False
        )

    df_summary = pd.DataFrame(summary)
    print("\n" + "=" * 60)
    print("結果サマリー")
    print("=" * 60)
    print(df_summary.to_string(index=False))

    # サマリーを保存
    df_summary.to_csv(DATA_DIR / "backtest_entry_summary.csv", index=False)

    return df_summary


if __name__ == '__main__':
    run_backtest()
