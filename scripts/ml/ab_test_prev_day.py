#!/usr/bin/env python3
"""
A/Bテスト: 前日OHLCV特徴量の効果を検証

A: 現行モデル（weekday, day_trade, shortable含む）
B: weekday/day_trade/shortableを除外し、prev_close_position/gap_ratio/prev_candleに差し替え

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

# --- Model A: 現行特徴量 ---
FEATURES_A = [
    'grok_rank', 'selection_score', 'buy_price', 'market_cap',
    'atr14_pct', 'vol_ratio', 'rsi9',
    'nikkei_change_pct', 'futures_change_pct',
    'shortable', 'day_trade',
    'volatility_5d', 'volatility_10d', 'volatility_20d',
    'ma5_deviation', 'ma25_deviation',
    'prev_day_return', 'return_5d', 'return_10d',
    'volume_ratio_5d', 'price_range_5d',
    'nikkei_vol_5d', 'nikkei_ret_5d', 'nikkei_ma5_dev',
    'topix_vol_5d', 'topix_ret_5d', 'topix_ma5_dev',
    'futures_vol_5d', 'futures_ret_5d', 'futures_ma5_dev',
    'usdjpy_vol_5d', 'usdjpy_ret_5d', 'usdjpy_ma5_dev',
    'weekday',
]

# --- Model B: 不要3つ除外 + 前日OHLCV3つ追加 ---
FEATURES_B = [
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

TARGET_COLUMN = 'phase2_win'


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(FEATURES_PATH)
    # 張り付き除外
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

    # weekdayがある場合はcategory扱い
    cat_features = ['weekday'] if 'weekday' in feature_cols else []

    # 週単位グループ
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

        if 'weekday' in feature_cols:
            X_train['weekday'] = X_train['weekday'].astype('category')
            X_test['weekday'] = X_test['weekday'].astype('category')

        y_train = train_df[TARGET_COLUMN].astype(int).values
        y_test = test_df[TARGET_COLUMN].astype(int).values

        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train,
                  categorical_feature=cat_features if cat_features else 'auto')

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        all_preds.extend(y_pred_proba)
        all_true.extend(y_test)
        all_pnl.extend((-test_df['profit_per_100_shares_phase2']).values)

    all_preds = np.array(all_preds)
    all_true = np.array(all_true)
    all_pnl = np.array(all_pnl)

    auc = roc_auc_score(all_true, all_preds)

    # 5分位分析（SHORT視点）
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

    # SHORT推奨(prob<=0.40)
    short_mask = all_preds <= 0.40
    short_wr = float((all_true[short_mask] == 0).mean()) if short_mask.sum() > 0 else 0
    short_pnl_total = float(all_pnl[short_mask].sum()) if short_mask.sum() > 0 else 0

    # Feature importance（全データ）
    X_all = df[feature_cols].copy()
    if 'weekday' in feature_cols:
        X_all['weekday'] = X_all['weekday'].astype('category')
    y_all = df[TARGET_COLUMN].astype(int).values
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X_all, y_all, categorical_feature=cat_features if cat_features else 'auto')

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

    # 両モデルで使う全特徴量のNaN除外
    all_cols = list(set(FEATURES_A + FEATURES_B + [TARGET_COLUMN]))
    df = df.dropna(subset=all_cols).copy()
    print(f"Data: {len(df)} rows")
    print(f"Date: {df['backtest_date'].min()} ~ {df['backtest_date'].max()}")

    result_a = run_cv(df, FEATURES_A, 'A')
    result_b = run_cv(df, FEATURES_B, 'B')

    # --- 出力 ---
    print(f"\n{'='*70}")
    print("A/B Test: 前日OHLCV特徴量 (全てSHORT視点)")
    print(f"{'='*70}")

    print(f"\n## 全体比較")
    print(f"{'指標':<25} {'A(現行)':<15} {'B(改善)':<15} {'差分':<15}")
    print("-" * 70)
    print(f"{'AUC':<25} {result_a['auc']:<15.4f} {result_b['auc']:<15.4f} {result_b['auc']-result_a['auc']:+.4f}")
    print(f"{'SHORT勝率(prob≤0.40)':<25} {result_a['short_wr']*100:<15.1f} {result_b['short_wr']*100:<15.1f} {(result_b['short_wr']-result_a['short_wr'])*100:+.1f}pp")
    print(f"{'SHORT件数':<25} {result_a['short_count']:<15} {result_b['short_count']:<15}")
    print(f"{'SHORT合計損益(¥)':<25} {result_a['short_pnl_total']:<15,.0f} {result_b['short_pnl_total']:<15,.0f} {result_b['short_pnl_total']-result_a['short_pnl_total']:+,.0f}")

    print(f"\n## 5分位別（SHORT損益）")
    print(f"{'分位':<10} {'A勝率':>8} {'A合計¥':>12} {'A平均¥':>8}  {'B勝率':>8} {'B合計¥':>12} {'B平均¥':>8}  {'勝率差':>8} {'損益差':>12}")
    print("-" * 100)
    for qa, qb in zip(result_a['quintiles'], result_b['quintiles']):
        wr_diff = qb['short_wr'] - qa['short_wr']
        pnl_diff = qb['short_pnl_total'] - qa['short_pnl_total']
        print(f"{qa['quintile']:<10} {qa['short_wr']*100:>7.1f}% {qa['short_pnl_total']:>12,.0f} {qa['short_pnl_mean']:>8,.0f}  {qb['short_wr']*100:>7.1f}% {qb['short_pnl_total']:>12,.0f} {qb['short_pnl_mean']:>8,.0f}  {wr_diff*100:>+7.1f}pp {pnl_diff:>+12,.0f}")

    # Q1-3 SHORT合計 / Q4-5合計
    a_q123 = sum(q['short_pnl_total'] for q in result_a['quintiles'][:3])
    b_q123 = sum(q['short_pnl_total'] for q in result_b['quintiles'][:3])
    a_q45 = sum(q['short_pnl_total'] for q in result_a['quintiles'][3:])
    b_q45 = sum(q['short_pnl_total'] for q in result_b['quintiles'][3:])
    print("-" * 100)
    print(f"{'Q1-3合計':<10} {'':>8} {a_q123:>12,.0f} {'':>8}  {'':>8} {b_q123:>12,.0f} {'':>8}  {'':>8} {b_q123-a_q123:>+12,.0f}")
    print(f"{'Q4-5合計':<10} {'':>8} {a_q45:>12,.0f} {'':>8}  {'':>8} {b_q45:>12,.0f} {'':>8}  {'':>8} {b_q45-a_q45:>+12,.0f}")

    print(f"\n## Feature Importance（除外/追加分のみ）")
    imp_a = result_a['importance']
    imp_b = result_b['importance']
    total_a = imp_a['importance'].sum()
    total_b = imp_b['importance'].sum()

    print(f"\n  A (除外対象):")
    for feat in ['weekday', 'day_trade', 'shortable']:
        row = imp_a[imp_a['feature'] == feat]
        if len(row) > 0:
            v = row['importance'].iloc[0]
            print(f"    {feat:25s} {v:5.0f} ({v/total_a*100:.1f}%)")

    print(f"\n  B (追加分):")
    for feat in ['prev_close_position', 'gap_ratio', 'prev_candle']:
        row = imp_b[imp_b['feature'] == feat]
        if len(row) > 0:
            v = row['importance'].iloc[0]
            print(f"    {feat:25s} {v:5.0f} ({v/total_b*100:.1f}%)")

    print(f"\n## B: Top 10 Features")
    for i, (_, r) in enumerate(imp_b.head(10).iterrows()):
        print(f"  {i+1:2d}. {r['feature']:25s} {r['importance']:5.0f} ({r['importance']/total_b*100:.1f}%)")


if __name__ == "__main__":
    main()
