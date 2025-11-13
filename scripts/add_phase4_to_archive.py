#!/usr/bin/env python3
"""
add_phase4_to_archive.py
既存アーカイブにPhase4カラムを追加

Phase4 = 非対称閾値（+2% -4%） + 11:30 TOPIX判断

ロジック:
1. 9:00寄付買い + 同時設定:
   - 利確: +2%指値売り（大引不成）
   - 損切: -4%逆指値売り（大引不成）

2. 11:30 TOPIX確認（追加判断）:
   - TOPIX下落 & 前場利益（+2%未到達だがプラス）→ 12:30成行売り（即利確）
   - それ以外 → そのまま保持

使い方:
    python3 scripts/add_phase4_to_archive.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf
from common_cfg.paths import PARQUET_DIR
from typing import Tuple, Optional

# パス定義
ARCHIVE_WITH_MARKET_FILE = PARQUET_DIR / "backtest" / "grok_trending_archive_with_market.parquet"
OUTPUT_FILE = PARQUET_DIR / "backtest" / "grok_trending_archive_with_phase4.parquet"

# Phase4の閾値（ヒートマップ分析結果のベスト戦略）
PHASE4_PROFIT_THRESHOLD = 0.02  # +2%
PHASE4_LOSS_THRESHOLD = -0.04   # -4%


def calculate_phase4_return(
    ticker: str,
    backtest_date: datetime,
    buy_price: float,
    daily_topix_return: float,
) -> Tuple[Optional[float], Optional[bool], Optional[str]]:
    """
    Phase4のリターンを計算

    Args:
        ticker: 銘柄コード
        backtest_date: バックテスト日
        buy_price: 寄付価格
        daily_topix_return: TOPIX日次リターン

    Returns:
        (profit_per_100, win, exit_reason)
    """
    # yfinanceで5分足データを取得
    try:
        stock = yf.Ticker(ticker)
        df_5min = stock.history(
            start=backtest_date,
            end=backtest_date + timedelta(days=1),
            interval="5m"
        )
    except Exception as e:
        print(f"[WARN] {ticker} {backtest_date}: yfinance取得失敗: {e}")
        return None, None, None

    if df_5min.empty:
        return None, None, None

    # カラム名を統一
    df_5min = df_5min.reset_index()
    df_5min.rename(columns={'Datetime': 'date'}, inplace=True)
    df_5min['date'] = pd.to_datetime(df_5min['date'])

    # 9:00-15:30のデータのみ
    df_5min = df_5min[
        (df_5min['date'].dt.time >= pd.Timestamp("09:00").time()) &
        (df_5min['date'].dt.time <= pd.Timestamp("15:30").time())
    ].sort_values('date')

    if df_5min.empty:
        return None, None, None

    # 11:30前のデータ（前場）
    df_morning = df_5min[df_5min['date'].dt.time <= pd.Timestamp("11:30").time()]

    # 12:30のデータ（後場寄付）
    df_afternoon_open = df_5min[
        (df_5min['date'].dt.time >= pd.Timestamp("12:30").time()) &
        (df_5min['date'].dt.time <= pd.Timestamp("12:35").time())
    ]

    # 閾値価格
    profit_price = buy_price * (1 + PHASE4_PROFIT_THRESHOLD)
    loss_price = buy_price * (1 + PHASE4_LOSS_THRESHOLD)

    # --- Phase 1: 9:00-11:30 前場 ---
    for idx, row in df_morning.iterrows():
        high = row['High']
        low = row['Low']
        timestamp = row['date']

        # 利確条件: +2%到達
        if high >= profit_price:
            exit_return = PHASE4_PROFIT_THRESHOLD
            profit_per_100 = exit_return * buy_price * 100
            return profit_per_100, True, f"profit_{timestamp.strftime('%H:%M')}"

        # 損切条件: -4%到達
        if low <= loss_price:
            exit_return = PHASE4_LOSS_THRESHOLD
            profit_per_100 = exit_return * buy_price * 100
            return profit_per_100, False, f"loss_{timestamp.strftime('%H:%M')}"

    # --- 11:30 TOPIX判断 ---
    topix_down = daily_topix_return < 0 if pd.notna(daily_topix_return) else False

    # 前場終値（11:30時点の価格）
    if not df_morning.empty:
        morning_close = df_morning.iloc[-1]['Close']
        morning_profit = (morning_close - buy_price) / buy_price

        # TOPIX下落 & 前場利益（+2%未到達だがプラス）→ 12:30即利確
        if topix_down and morning_profit > 0 and morning_profit < PHASE4_PROFIT_THRESHOLD:
            # 12:30で売却（後場寄付価格で決済）
            if not df_afternoon_open.empty:
                afternoon_open_price = df_afternoon_open.iloc[0]['Open']
                final_return = (afternoon_open_price - buy_price) / buy_price
                profit_per_100 = final_return * buy_price * 100
                win = final_return > 0
                return profit_per_100, win, "topix_down_morning_exit"

    # --- Phase 2: 12:30-15:30 後場（通常通り） ---
    df_afternoon = df_5min[df_5min['date'].dt.time > pd.Timestamp("11:30").time()]

    for idx, row in df_afternoon.iterrows():
        high = row['High']
        low = row['Low']
        timestamp = row['date']

        # 利確条件: +2%到達
        if high >= profit_price:
            exit_return = PHASE4_PROFIT_THRESHOLD
            profit_per_100 = exit_return * buy_price * 100
            return profit_per_100, True, f"profit_{timestamp.strftime('%H:%M')}"

        # 損切条件: -4%到達
        if low <= loss_price:
            exit_return = PHASE4_LOSS_THRESHOLD
            profit_per_100 = exit_return * buy_price * 100
            return profit_per_100, False, f"loss_{timestamp.strftime('%H:%M')}"

    # --- 大引け決済 ---
    if not df_5min.empty:
        close_price = df_5min.iloc[-1]['Close']
        final_return = (close_price - buy_price) / buy_price
        profit_per_100 = final_return * buy_price * 100
        win = final_return > 0
        return profit_per_100, win, "close"

    return None, None, None


def main():
    """Phase4カラム追加のメイン処理"""

    if not ARCHIVE_WITH_MARKET_FILE.exists():
        print(f"[ERROR] アーカイブが見つかりません: {ARCHIVE_WITH_MARKET_FILE}")
        return

    # アーカイブ読み込み
    df = pd.read_parquet(ARCHIVE_WITH_MARKET_FILE)
    print(f"[INFO] アーカイブ読み込み: {len(df)}銘柄")

    # Phase4カラムを初期化
    df['profit_per_100_shares_phase4'] = None
    df['phase4_win'] = None
    df['phase4_exit_reason'] = None

    # 各銘柄でPhase4を計算
    for idx, row in df.iterrows():
        ticker = row['ticker']
        backtest_date = pd.to_datetime(row['backtest_date']).date()
        buy_price = row['buy_price']
        daily_topix_return = row.get('daily_topix_return', None)

        print(f"[INFO] {idx+1}/{len(df)}: {ticker} {backtest_date}")

        profit_per_100, win, exit_reason = calculate_phase4_return(
            ticker, backtest_date, buy_price, daily_topix_return
        )

        if profit_per_100 is not None:
            df.at[idx, 'profit_per_100_shares_phase4'] = profit_per_100
            df.at[idx, 'phase4_win'] = win
            df.at[idx, 'phase4_exit_reason'] = exit_reason

    # 保存
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    print(f"[INFO] Phase4カラム追加完了: {OUTPUT_FILE}")

    # 統計表示
    phase4_df = df[df['profit_per_100_shares_phase4'].notna()]
    if not phase4_df.empty:
        total_profit = phase4_df['profit_per_100_shares_phase4'].sum()
        win_count = phase4_df['phase4_win'].sum()
        win_rate = (win_count / len(phase4_df)) * 100
        avg_profit = phase4_df['profit_per_100_shares_phase4'].mean()

        print("\n" + "=" * 80)
        print("Phase4 統計")
        print("=" * 80)
        print(f"対象銘柄: {len(phase4_df)}銘柄")
        print(f"累積利益: {total_profit:,.0f}円")
        print(f"勝率: {win_rate:.1f}%")
        print(f"平均利益: {avg_profit:,.0f}円")
        print()

        # 決済理由の内訳
        exit_reasons = phase4_df['phase4_exit_reason'].value_counts()
        print("決済理由内訳:")
        for reason, count in exit_reasons.items():
            print(f"  {reason}: {count}回")
        print()

        # Phase3との比較
        print("=" * 80)
        print("Phase3±3% との比較")
        print("=" * 80)
        phase3_total = df['profit_per_100_shares_phase3_3pct'].sum()
        phase3_win_rate = (df['phase3_3pct_win'].sum() / len(df)) * 100
        phase3_avg = df['profit_per_100_shares_phase3_3pct'].mean()

        print(f"Phase3±3%: 累積{phase3_total:,.0f}円, 勝率{phase3_win_rate:.1f}%, 平均{phase3_avg:,.0f}円")
        print(f"Phase4:    累積{total_profit:,.0f}円, 勝率{win_rate:.1f}%, 平均{avg_profit:,.0f}円")
        print(f"改善:      累積{total_profit - phase3_total:+,.0f}円, 勝率{win_rate - phase3_win_rate:+.1f}%, 平均{avg_profit - phase3_avg:+,.0f}円")
        print()


if __name__ == "__main__":
    main()
