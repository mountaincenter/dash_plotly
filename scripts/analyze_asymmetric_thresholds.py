#!/usr/bin/env python3
"""
analyze_asymmetric_thresholds.py
非対称な利確・損切閾値の分析

実行方法:
    python3 scripts/analyze_asymmetric_thresholds.py

分析内容:
    - 対称: ±1%, ±2%, ±3%
    - 非対称: +2% -1%, +3% -1%, +4% -2%, +5% -2%, +3% -2%, +2% -3%, など
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf
from common_cfg.paths import PARQUET_DIR
from typing import Tuple, Optional
from datetime import datetime, timedelta


def calculate_asymmetric_return(
    df_5min: pd.DataFrame,
    open_price: float,
    profit_threshold: float,
    loss_threshold: float,
) -> Tuple[Optional[float], Optional[bool], Optional[str]]:
    """
    非対称な利確・損切戦略のリターンを計算

    Args:
        df_5min: 5分足データ（9:00-15:30）
        open_price: 寄付価格
        profit_threshold: 利確閾値（例: 0.03 = +3%）
        loss_threshold: 損切閾値（例: -0.02 = -2%）

    Returns:
        (return, win, exit_reason)
    """
    if df_5min.empty or open_price == 0:
        return None, None, None

    profit_price = open_price * (1 + profit_threshold)
    loss_price = open_price * (1 + loss_threshold)

    # 9:00-15:30の5分足を時系列順にチェック
    for idx, row in df_5min.iterrows():
        high = row['High']
        low = row['Low']
        timestamp = row['date']

        # 利確条件: High が利確価格に到達
        if high >= profit_price:
            exit_return = profit_threshold
            return exit_return, True, f"profit_{timestamp.strftime('%H:%M')}"

        # 損切条件: Low が損切価格に到達
        if low <= loss_price:
            exit_return = loss_threshold
            return exit_return, False, f"loss_{timestamp.strftime('%H:%M')}"

    # どちらにも到達しなかった場合: 大引け（15:30）で決済
    if not df_5min.empty:
        close_price = df_5min.iloc[-1]['Close']
        final_return = (close_price - open_price) / open_price
        win = final_return > 0
        return final_return, win, "close"

    return None, None, None


def main():
    """非対称閾値分析のメイン処理"""

    # 既存のアーカイブから銘柄リストと日付を取得
    archive_path = PARQUET_DIR / "backtest" / "grok_trending_archive_with_market.parquet"
    if not archive_path.exists():
        print(f"[ERROR] アーカイブが見つかりません: {archive_path}")
        return

    df_archive = pd.read_parquet(archive_path)
    print(f"[INFO] アーカイブ読み込み: {len(df_archive)}銘柄")

    # テストする閾値の組み合わせ
    threshold_combinations = [
        # 対称
        (0.01, -0.01, "±1%"),
        (0.02, -0.02, "±2%"),
        (0.03, -0.03, "±3%"),
        # 非対称（利確 > 損切）
        (0.02, -0.01, "+2% -1%"),
        (0.03, -0.01, "+3% -1%"),
        (0.03, -0.02, "+3% -2%"),
        (0.04, -0.02, "+4% -2%"),
        (0.05, -0.02, "+5% -2%"),
        (0.04, -0.03, "+4% -3%"),
        # 非対称（利確 < 損切）
        (0.01, -0.02, "+1% -2%"),
        (0.02, -0.03, "+2% -3%"),
        (0.02, -0.04, "+2% -4%"),
    ]

    results = []

    for profit_pct, loss_pct, label in threshold_combinations:
        print(f"\n[INFO] 計算中: {label}")

        total_profit = 0
        win_count = 0
        total_count = 0
        exit_reasons = {"profit": 0, "loss": 0, "close": 0}

        for idx, row in df_archive.iterrows():
            ticker = row['ticker']
            backtest_date = pd.to_datetime(row['backtest_date']).date()
            buy_price = row['buy_price']

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
                continue

            if df_5min.empty:
                continue

            # カラム名を統一
            df_5min = df_5min.reset_index()
            df_5min.rename(columns={'Datetime': 'date'}, inplace=True)
            df_5min['date'] = pd.to_datetime(df_5min['date'])

            # 9:00-15:30のデータのみ
            df_5min = df_5min[
                (df_5min['date'].dt.time >= pd.Timestamp("09:00").time()) &
                (df_5min['date'].dt.time <= pd.Timestamp("15:30").time())
            ].sort_values('date')

            # リターン計算
            phase_return, phase_win, exit_reason = calculate_asymmetric_return(
                df_5min, buy_price, profit_pct, loss_pct
            )

            if phase_return is not None:
                profit_per_100 = phase_return * buy_price * 100
                total_profit += profit_per_100
                if phase_win:
                    win_count += 1
                total_count += 1

                # 決済理由を集計
                if exit_reason:
                    if "profit" in exit_reason:
                        exit_reasons["profit"] += 1
                    elif "loss" in exit_reason:
                        exit_reasons["loss"] += 1
                    elif "close" in exit_reason:
                        exit_reasons["close"] += 1

        if total_count > 0:
            win_rate = (win_count / total_count) * 100
            avg_profit = total_profit / total_count
            results.append({
                "label": label,
                "profit_pct": profit_pct * 100,
                "loss_pct": loss_pct * 100,
                "total_profit": total_profit,
                "win_rate": win_rate,
                "avg_profit": avg_profit,
                "count": total_count,
                "exit_profit": exit_reasons["profit"],
                "exit_loss": exit_reasons["loss"],
                "exit_close": exit_reasons["close"],
            })

    # 結果表示
    print("\n" + "=" * 100)
    print("非対称閾値分析結果")
    print("=" * 100)
    print()

    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('total_profit', ascending=False)

    print(f"{'戦略':<15} {'累積利益':>12} {'勝率':>8} {'平均利益':>10} {'利確':>6} {'損切':>6} {'大引':>6}")
    print("-" * 100)

    for _, row in df_results.iterrows():
        print(
            f"{row['label']:<15} "
            f"{row['total_profit']:>12,.0f}円 "
            f"{row['win_rate']:>7.1f}% "
            f"{row['avg_profit']:>9,.0f}円 "
            f"{row['exit_profit']:>6} "
            f"{row['exit_loss']:>6} "
            f"{row['exit_close']:>6}"
        )

    print()
    print("=" * 100)
    print("ベスト戦略")
    print("=" * 100)
    best = df_results.iloc[0]
    print(f"戦略: {best['label']}")
    print(f"累積利益: {best['total_profit']:,.0f}円")
    print(f"勝率: {best['win_rate']:.1f}%")
    print(f"平均利益: {best['avg_profit']:,.0f}円")
    print(f"決済内訳: 利確{best['exit_profit']}回, 損切{best['exit_loss']}回, 大引{best['exit_close']}回")
    print()


if __name__ == "__main__":
    main()
