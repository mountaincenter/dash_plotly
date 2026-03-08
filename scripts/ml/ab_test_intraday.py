#!/usr/bin/env python3
"""
A/Bテスト: 5分足イントラデイ特徴量の効果検証

A: 現行Model B (prev_day_features込み、31特徴量)
C: A + 5分足イントラデイ6特徴量 (37特徴量)

全てSHORT視点で評価。
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.metrics import roc_auc_score
from common_cfg.paths import PARQUET_DIR

FEATURES_PATH = PARQUET_DIR / "ml" / "archive_with_features.parquet"

# --- Model A: 現行Best (prev_day込み) ---
FEATURES_A = [
    'grok_rank', 'selection_score', 'buy_price', 'market_cap',
    'atr14_pct', 'vol_ratio', 'rsi9',
    'nikkei_change_pct', 'futures_change_pct',
    'volatility_5d', 'volatility_10d', 'volatility_20d',
    'ma5_deviation', 'ma25_deviation',
    'prev_day_return', 'return_5d', 'return_10d',
    'volume_ratio_5d', 'price_range_5d',
    'nikkei_vol_5d', 'nikkei_ret_5d', 'nikkei_ma5_dev',
    'topix_vol_5d', 'topix_ret_5d', 'topix_ma5_dev',
    'futures_vol_5d', 'futures_ret_5d', 'futures_ma5_dev',
    'usdjpy_vol_5d', 'usdjpy_ret_5d', 'usdjpy_ma5_dev',
    'prev_close_position', 'gap_ratio', 'prev_candle',
]

# --- Model C: A + 5分足イントラデイ ---
INTRADAY_COLS = [
    'prev_intraday_range', 'prev_intraday_volatility', 'prev_volume_am_ratio',
    'prev_close_gap', 'prev_am_return', 'prev_pm_return',
]
FEATURES_C = FEATURES_A + INTRADAY_COLS

TARGET_COLUMN = 'phase2_win'


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(FEATURES_PATH)
    is_stuck = (
        (df['buy_price'] == df['high']) &
        (df['high'] == df['daily_close']) &
        (df['high'] == df['low'])
    )
    df = df[~is_stuck]
    return df


def run_cv(df: pd.DataFrame, feature_cols: list[str], label: str) -> dict:
    """Walk-Forward CVで評価（全てSHORT視点）"""
    df = df.copy()
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])
    df = df.sort_values('backtest_date').reset_index(drop=True)

    weeks = df['backtest_date'].dt.to_period('W')
    unique_weeks = weeks.unique()
    min_train_weeks = 4

    all_preds = []
    all_true = []
    all_pnl = []

    params = {
        'objective': 'binary', 'metric': 'auc', 'boosting_type': 'gbdt',
        'num_leaves': 31, 'learning_rate': 0.05,
        'feature_fraction': 0.8, 'bagging_fraction': 0.8, 'bagging_freq': 5,
        'verbose': -1, 'n_estimators': 100, 'random_state': 42,
    }

    for i, test_week in enumerate(unique_weeks[min_train_weeks:], min_train_weeks):
        train_weeks = unique_weeks[:i]
        train_mask = np.isin(weeks, train_weeks)
        test_mask = weeks == test_week

        if train_mask.sum() < 50 or test_mask.sum() == 0:
            continue

        train_df = df[train_mask]
        test_df = df[test_mask]

        X_train = train_df[feature_cols].copy()
        X_test = test_df[feature_cols].copy()

        y_train = train_df[TARGET_COLUMN].astype(int).values
        y_test = test_df[TARGET_COLUMN].astype(int).values

        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train)

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        all_preds.extend(y_pred_proba)
        all_true.extend(y_test)
        all_pnl.extend((-test_df['profit_per_100_shares_phase2']).values)

    all_preds = np.array(all_preds)
    all_true = np.array(all_true)
    all_pnl = np.array(all_pnl)

    auc = roc_auc_score(all_true, all_preds)

    quintiles = pd.qcut(all_preds, 5, labels=['Q1(低)', 'Q2', 'Q3', 'Q4', 'Q5(高)'], duplicates='drop')
    q_results = []
    for q in ['Q1(低)', 'Q2', 'Q3', 'Q4', 'Q5(高)']:
        mask = quintiles == q
        if mask.sum() > 0:
            q_results.append({
                'quintile': q,
                'count': int(mask.sum()),
                'short_wr': float((all_true[mask] == 0).mean()),
                'short_pnl_total': float(all_pnl[mask].sum()),
                'short_pnl_mean': float(all_pnl[mask].mean()),
            })

    short_mask = all_preds <= 0.40
    short_wr = float((all_true[short_mask] == 0).mean()) if short_mask.sum() > 0 else 0
    short_pnl_total = float(all_pnl[short_mask].sum()) if short_mask.sum() > 0 else 0

    # Feature importance
    X_all = df[feature_cols].copy()
    y_all = df[TARGET_COLUMN].astype(int).values
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X_all, y_all)

    imp_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': final_model.feature_importances_
    }).sort_values('importance', ascending=False)

    return {
        'auc': auc,
        'short_wr': short_wr,
        'short_count': int(short_mask.sum()),
        'short_pnl_total': short_pnl_total,
        'total': len(all_true),
        'quintiles': q_results,
        'importance': imp_df,
    }


def main():
    df = load_data()

    # 両モデルの全特徴量でdropna
    all_cols = list(set(FEATURES_C + [TARGET_COLUMN]))
    df = df.dropna(subset=all_cols).copy()
    print(f"Data: {len(df)} rows")
    print(f"Date: {df['backtest_date'].min()} ~ {df['backtest_date'].max()}")

    result_a = run_cv(df, FEATURES_A, 'A')
    result_c = run_cv(df, FEATURES_C, 'C')

    print(f"\n{'='*70}")
    print("A/B Test: 5分足イントラデイ特徴量 (全てSHORT視点)")
    print(f"{'='*70}")

    print(f"\n## 全体比較")
    print(f"{'指標':<25} {'A(現行31)':<15} {'C(+5分足37)':<15} {'差分':<15}")
    print("-" * 70)
    print(f"{'AUC':<25} {result_a['auc']:<15.4f} {result_c['auc']:<15.4f} {result_c['auc']-result_a['auc']:+.4f}")
    print(f"{'SHORT勝率(prob≤0.40)':<25} {result_a['short_wr']*100:<15.1f} {result_c['short_wr']*100:<15.1f} {(result_c['short_wr']-result_a['short_wr'])*100:+.1f}pp")
    print(f"{'SHORT件数':<25} {result_a['short_count']:<15} {result_c['short_count']:<15}")
    print(f"{'SHORT合計損益(¥)':<25} {result_a['short_pnl_total']:<15,.0f} {result_c['short_pnl_total']:<15,.0f} {result_c['short_pnl_total']-result_a['short_pnl_total']:+,.0f}")

    print(f"\n## Q別比較（SHORT損益）")
    print(f"{'Q':<10} {'A勝率':>8} {'A損益':>12} {'C勝率':>8} {'C損益':>12} {'勝率差':>8} {'損益差':>12}")
    print("-" * 70)
    for qa, qc in zip(result_a['quintiles'], result_c['quintiles']):
        wr_diff = qc['short_wr'] - qa['short_wr']
        pnl_diff = qc['short_pnl_total'] - qa['short_pnl_total']
        print(f"{qa['quintile']:<10} {qa['short_wr']*100:>7.1f}% {qa['short_pnl_total']:>12,.0f} {qc['short_wr']*100:>7.1f}% {qc['short_pnl_total']:>12,.0f} {wr_diff*100:>+7.1f}pp {pnl_diff:>+12,.0f}")

    a_q123 = sum(q['short_pnl_total'] for q in result_a['quintiles'][:3])
    c_q123 = sum(q['short_pnl_total'] for q in result_c['quintiles'][:3])
    print("-" * 70)
    print(f"{'Q1-3計':<10} {'':>8} {a_q123:>12,.0f} {'':>8} {c_q123:>12,.0f} {'':>8} {c_q123-a_q123:>+12,.0f}")

    print(f"\n## 5分足特徴量の重要度 (Model C)")
    imp_c = result_c['importance']
    total_imp = imp_c['importance'].sum()
    for feat in INTRADAY_COLS:
        row = imp_c[imp_c['feature'] == feat]
        if len(row) > 0:
            v = row['importance'].iloc[0]
            rank = int((imp_c['importance'] > v).sum()) + 1
            print(f"  {feat:25s} {v:5.0f} ({v/total_imp*100:.1f}%) rank={rank}/{len(FEATURES_C)}")

    print(f"\n## Top 10 Features (Model C)")
    for i, (_, r) in enumerate(imp_c.head(10).iterrows()):
        marker = " ★" if r['feature'] in INTRADAY_COLS else ""
        print(f"  {i+1:2d}. {r['feature']:25s} {r['importance']:5.0f} ({r['importance']/total_imp*100:.1f}%){marker}")


if __name__ == "__main__":
    main()
