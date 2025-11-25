#!/usr/bin/env python3
"""
fill_v2_1_data.py

grok_analysis_merged_v2_1.parquetの残りNaNカラムを埋める

処理:
1. 既存のgrok_analysis_merged_v2_1.parquetを読み込み
2. generate_trading_recommendation_v2_1.pyのロジックを使用して残りカラムを計算:
   - v2_0_3_reasons (v2.0.3の判定理由)
   - v2_1_action, v2_1_score, v2_1_reasons (v2.1の判定)
   - atr_pct (ATR)
   - stop_loss_pct (価格帯別損切り水準)
   - settlement_timing (固定値「大引け」)
3. 保存
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# generate_trading_recommendation_v2_1.pyから関数をインポート
sys.path.append(str(ROOT / "improvement" / "scripts"))
from generate_trading_recommendation_v2_0_3 import (
    fetch_jquants_fundamentals,
    fetch_prices_from_parquet,
    load_backtest_stats,
)

# generate_trading_recommendation_v2_1.pyから関数をインポート
import importlib.util
spec = importlib.util.spec_from_file_location(
    "v2_1_module",
    ROOT / "scripts" / "pipeline" / "generate_trading_recommendation_v2_1.py"
)
v2_1_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v2_1_module)

calculate_technical_indicators = v2_1_module.calculate_technical_indicators
calculate_v2_0_3_score_and_action = v2_1_module.calculate_v2_0_3_score_and_action
calculate_v2_1_score_and_action = v2_1_module.calculate_v2_1_score_and_action

IMPROVEMENT_DIR = ROOT / "improvement"
TARGET_FILE = IMPROVEMENT_DIR / "data" / "grok_analysis_merged_v2_1.parquet"
PRICES_FILE = ROOT / "data" / "parquet" / "prices_max_1d.parquet"


def main():
    print("=" * 80)
    print("grok_analysis_merged_v2_1.parquet の残りカラムを埋める")
    print("=" * 80)
    print()

    # 1. データ読み込み
    print("[1/5] データ読み込み中...")
    df = pd.read_parquet(TARGET_FILE)
    print(f"  総レコード数: {len(df)}")

    # prices読み込み
    if not PRICES_FILE.exists():
        print(f"エラー: {PRICES_FILE} が見つかりません")
        return 1
    prices_df = pd.read_parquet(PRICES_FILE)
    print(f"  prices: {len(prices_df)} レコード")
    print()

    # 2. バックテスト統計読み込み
    print("[2/5] バックテスト統計読み込み中...")
    backtest_stats = load_backtest_stats()
    print()

    # 3. NaNカラムの確認
    print("[3/5] NaNカラムを確認...")
    nan_cols = []
    for col in df.columns:
        if df[col].isna().all():
            nan_cols.append(col)
    print(f"  全てNaNのカラム ({len(nan_cols)}): {nan_cols}")
    print()

    # 4. 各レコードに対してカラムを計算
    print("[4/5] 残りカラムを計算中...")

    # dtypeをobjectに変換（文字列やリストを格納可能にする）
    for col in nan_cols:
        df[col] = df[col].astype('object')

    # 日付ごとのtotal_stocksを計算
    date_total_stocks = df.groupby('selection_date').size().to_dict()

    for idx, row in df.iterrows():
        ticker = row['ticker']
        selection_date = row['selection_date']
        total_stocks = date_total_stocks[selection_date]

        if (idx + 1) % 10 == 0:
            print(f"  [{idx+1}/{len(df)}] {ticker} ({selection_date})...")

        # 財務情報取得
        fundamentals = fetch_jquants_fundamentals(ticker)

        # 株価情報取得
        price_data = fetch_prices_from_parquet(ticker, lookback_days=30)

        if price_data is None:
            print(f"  警告: {ticker} の株価データが取得できません。スキップします。")
            # NaNのままにする
            continue

        # v2.0.3判断を生成（理由を取得するため）
        v2_0_3_score_calc, v2_0_3_action_calc, v2_0_3_reasons_list = calculate_v2_0_3_score_and_action(
            row, backtest_stats, fundamentals, price_data, total_stocks
        )

        # v2_0_3_reasonsを文字列に変換（' / 'で結合）
        df.at[idx, 'v2_0_3_reasons'] = ' / '.join(v2_0_3_reasons_list)

        # テクニカル指標を計算（既に保存済みだが、念のため再計算）
        technical = calculate_technical_indicators(ticker, prices_df)

        # v2.1判断を生成
        prev_close = price_data.get('prevClose', 0)

        # 既存のv2_0_3_action, v2_0_3_scoreを使用（保存済み）
        v2_0_3_action_existing = row.get('v2_0_3_action')
        v2_0_3_score_existing = row.get('v2_0_3_score')

        # v2.1判断を計算
        v2_1_score, v2_1_action, v2_1_reasons_list = calculate_v2_1_score_and_action(
            v2_0_3_action_existing, v2_0_3_score_existing, prev_close, technical,
            row['grok_rank'], total_stocks
        )

        df.at[idx, 'v2_1_action'] = v2_1_action
        df.at[idx, 'v2_1_score'] = v2_1_score
        df.at[idx, 'v2_1_reasons'] = v2_1_reasons_list  # リスト形式で保存

        # ATR
        df.at[idx, 'atr_pct'] = price_data.get('atrPct', 0)

        # 損切り水準（価格帯別）
        if v2_1_action == '売り':
            stop_loss_pct = 5.0
        elif v2_1_action == '買い':
            if prev_close >= 10000:
                stop_loss_pct = 2.5
            elif prev_close >= 5000:
                stop_loss_pct = 3.0  # 5,000-10,000円: 3%
            elif prev_close >= 3000:
                stop_loss_pct = 3.0
            elif prev_close >= 1000:
                stop_loss_pct = 5.0
            else:
                stop_loss_pct = 0.0  # 1000円以下は損切りなし
        else:
            stop_loss_pct = 3.0  # 静観はデフォルト

        df.at[idx, 'stop_loss_pct'] = round(stop_loss_pct, 1)

        # 決済タイミング（固定値）
        df.at[idx, 'settlement_timing'] = '大引け'

    print()

    # 5. 保存
    print("[5/5] 保存中...")
    df.to_parquet(TARGET_FILE, index=False)
    print(f"  保存完了: {TARGET_FILE}")
    print()

    # NaNの残りを確認
    print("=== 残りのNaNカラム ===")
    nan_cols_after = []
    for col in df.columns:
        if df[col].isna().all():
            nan_cols_after.append(col)

    if len(nan_cols_after) == 0:
        print("✅ 全てのNaNカラムが埋まりました！")
    else:
        print(f"⚠️  残りのNaNカラム ({len(nan_cols_after)}): {nan_cols_after}")

    print()
    print("完了!")


if __name__ == '__main__':
    main()
