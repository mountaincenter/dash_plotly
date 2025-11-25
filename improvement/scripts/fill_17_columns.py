#!/usr/bin/env python3
"""
fill_17_columns.py

grok_analysis_merged_v2_1.parquetの17カラムを充足

既存カラム: コピー
欠損カラム: yfinance + jquantsで算出
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# jquantsとv2.0.3/v2.1ロジックをインポート
sys.path.append(str(ROOT / "improvement" / "scripts"))
from generate_trading_recommendation_v2_0_3 import (
    fetch_jquants_fundamentals,
    load_backtest_stats,
)

import importlib.util
spec = importlib.util.spec_from_file_location(
    "v2_1_module",
    ROOT / "scripts" / "pipeline" / "generate_trading_recommendation_v2_1.py"
)
v2_1_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v2_1_module)

calculate_v2_0_3_score_and_action = v2_1_module.calculate_v2_0_3_score_and_action
calculate_v2_1_score_and_action = v2_1_module.calculate_v2_1_score_and_action

IMPROVEMENT_DIR = ROOT / "improvement"
TARGET_FILE = IMPROVEMENT_DIR / "data" / "grok_analysis_merged_v2_1.parquet"


def fetch_yfinance_data(ticker: str, start_date: str, end_date: str) -> dict:
    """
    yfinanceから株価データを取得

    Args:
        ticker: ティッカーシンボル（例: "4579.T"）
        start_date: 開始日（YYYY-MM-DD）
        end_date: 終了日（YYYY-MM-DD）

    Returns:
        株価データ辞書
    """
    try:
        # yfinanceでデータ取得
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)

        if hist.empty:
            print(f"  警告: {ticker} のyfinanceデータが空です")
            return None

        # ATR計算（14日間）
        if len(hist) >= 14:
            high_low = hist['High'] - hist['Low']
            high_close = abs(hist['High'] - hist['Close'].shift())
            low_close = abs(hist['Low'] - hist['Close'].shift())

            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = true_range.rolling(window=14).mean().iloc[-1]

            latest_close = hist['Close'].iloc[-1]
            atr_pct = (atr / latest_close * 100) if latest_close > 0 else 0
        else:
            atr_pct = 0

        # 最新データ
        latest = hist.iloc[-1]
        prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else latest['Close']

        # 前日変化率
        daily_change_pct = ((latest['Close'] - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # 25日移動平均
        ma25 = hist['Close'].rolling(window=25).mean().iloc[-1] if len(hist) >= 25 else latest['Close']

        return {
            'currentPrice': float(latest['Close']),
            'prevClose': float(prev_close),
            'dailyChangePct': float(daily_change_pct),
            'atrPct': float(atr_pct),
            'ma25': float(ma25),
            'high': float(latest['High']),
            'low': float(latest['Low']),
        }

    except Exception as e:
        print(f"  エラー: {ticker} のyfinanceデータ取得失敗: {e}")
        return None


def calculate_technical_from_yfinance(ticker: str, start_date: str, end_date: str) -> dict:
    """
    yfinanceからテクニカル指標を計算

    Returns:
        {'rsi_14d': float, 'volume_change_20d': float, 'price_vs_sma5_pct': float}
    """
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)

        if hist.empty or len(hist) < 20:
            return {}

        # RSI (14日)
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi_14d = rsi.iloc[-1] if not rsi.empty else None

        # 出来高変化率（20日平均比）
        if len(hist) >= 21:
            latest_volume = hist['Volume'].iloc[-1]
            avg_volume_20d = hist['Volume'].iloc[-21:-1].mean()
            volume_change_20d = latest_volume / avg_volume_20d if avg_volume_20d > 0 else None
        else:
            volume_change_20d = None

        # 5日線乖離率
        if len(hist) >= 5:
            latest_close = hist['Close'].iloc[-1]
            sma5 = hist['Close'].iloc[-5:].mean()
            price_vs_sma5_pct = ((latest_close - sma5) / sma5 * 100) if sma5 > 0 else None
        else:
            price_vs_sma5_pct = None

        return {
            'rsi_14d': float(rsi_14d) if pd.notna(rsi_14d) else None,
            'volume_change_20d': float(volume_change_20d) if pd.notna(volume_change_20d) else None,
            'price_vs_sma5_pct': float(price_vs_sma5_pct) if pd.notna(price_vs_sma5_pct) else None
        }

    except Exception as e:
        print(f"  エラー: {ticker} のテクニカル指標計算失敗: {e}")
        return {}


def main():
    print("=" * 80)
    print("17カラム充足処理")
    print("=" * 80)
    print()

    # 1. データ読み込み
    print("[1/4] データ読み込み中...")
    df = pd.read_parquet(TARGET_FILE)
    print(f"  総レコード数: {len(df)}")
    print()

    # 2. バックテスト統計読み込み
    print("[2/4] バックテスト統計読み込み中...")
    backtest_stats = load_backtest_stats()
    print()

    # 3. 日付ごとのtotal_stocks
    date_total_stocks = df.groupby('selection_date').size().to_dict()

    # 4. 各レコードを処理
    print("[3/4] 欠損カラムを算出中...")
    print("  データソース: yfinance + jquants (.env.jquants)")
    print()

    for idx, row in df.iterrows():
        ticker = row['ticker']
        selection_date = row['selection_date']
        total_stocks = date_total_stocks[selection_date]

        if (idx + 1) % 10 == 0:
            print(f"  [{idx+1}/{len(df)}] {ticker} ({selection_date})...")

        # yfinanceでデータ取得（30日分）
        end_date = pd.to_datetime(selection_date) + timedelta(days=1)
        start_date = end_date - timedelta(days=60)

        price_data = fetch_yfinance_data(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        technical = calculate_technical_from_yfinance(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        # データ取得失敗時は既存値を使用
        if price_data is None:
            price_data = {
                'currentPrice': row.get('prev_day_close', 0),
                'prevClose': row.get('prev_day_close', 0),
                'dailyChangePct': row.get('prev_day_change_pct', 0),
                'atrPct': 0,
                'ma25': row.get('prev_day_close', 0)
            }

        # atr_pct
        df.at[idx, 'atr_pct'] = price_data.get('atrPct', 0)

        # jquantsから財務情報取得
        fundamentals = fetch_jquants_fundamentals(ticker)

        # v2_0_3判定を計算（理由を取得）
        v2_0_3_score_calc, v2_0_3_action_calc, v2_0_3_reasons_list = calculate_v2_0_3_score_and_action(
            row, backtest_stats, fundamentals, price_data, total_stocks
        )

        df.at[idx, 'v2_0_3_reasons'] = ' / '.join(v2_0_3_reasons_list)

        # 既存のv2_0_3_action, v2_0_3_scoreを使用
        v2_0_3_action = row.get('v2_0_3_action')
        v2_0_3_score = row.get('v2_0_3_score')

        # テクニカル指標（既存値を優先、なければyfinanceから）
        tech_merged = {
            'rsi_14d': row.get('rsi_14d') if pd.notna(row.get('rsi_14d')) else technical.get('rsi_14d'),
            'volume_change_20d': row.get('volume_change_20d') if pd.notna(row.get('volume_change_20d')) else technical.get('volume_change_20d'),
            'price_vs_sma5_pct': row.get('price_vs_sma5_pct') if pd.notna(row.get('price_vs_sma5_pct')) else technical.get('price_vs_sma5_pct')
        }

        # v2_1判定を計算
        prev_close = price_data.get('prevClose', row.get('prev_day_close', 0))

        v2_1_score, v2_1_action, v2_1_reasons_list = calculate_v2_1_score_and_action(
            v2_0_3_action, v2_0_3_score, prev_close, tech_merged,
            row['grok_rank'], total_stocks
        )

        df.at[idx, 'v2_1_action'] = v2_1_action
        df.at[idx, 'v2_1_score'] = v2_1_score
        df.at[idx, 'v2_1_reasons'] = v2_1_reasons_list

        # stop_loss_pct（価格帯別）
        if v2_1_action == '売り':
            stop_loss_pct = 5.0
        elif v2_1_action == '買い':
            if prev_close >= 10000:
                stop_loss_pct = 2.5
            elif prev_close >= 5000:
                stop_loss_pct = 3.0
            elif prev_close >= 3000:
                stop_loss_pct = 3.0
            elif prev_close >= 1000:
                stop_loss_pct = 5.0
            else:
                stop_loss_pct = 0.0
        else:
            stop_loss_pct = 3.0

        df.at[idx, 'stop_loss_pct'] = round(stop_loss_pct, 1)

        # settlement_timing（固定値）
        df.at[idx, 'settlement_timing'] = '大引け'

    print()

    # 5. 保存
    print("[4/4] 保存中...")
    df.to_parquet(TARGET_FILE, index=False)
    print(f"  保存完了: {TARGET_FILE}")
    print()

    # 確認
    print("=== 17カラムの充足確認 ===")
    required_cols = [
        'ticker', 'company_name', 'grok_rank', 'prev_day_close', 'prev_day_change_pct',
        'atr_pct', 'v2_0_3_action', 'v2_0_3_score', 'v2_0_3_reasons',
        'v2_1_action', 'v2_1_score', 'v2_1_reasons',
        'rsi_14d', 'volume_change_20d', 'price_vs_sma5_pct',
        'stop_loss_pct', 'settlement_timing'
    ]

    for col in required_cols:
        non_null_count = df[col].notna().sum()
        print(f"  {col}: {non_null_count}/{len(df)}")

    print()
    print("完了!")


if __name__ == '__main__':
    main()
