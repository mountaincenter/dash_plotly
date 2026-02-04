#!/usr/bin/env python3
"""
train_model.py
LightGBMで騰落確率予測モデルを学習

=== 重要: 損益計算とショート戦略の解釈 ===

【archiveの損益計算（ロング基準）】
- buy_price = 寄付（Open）
- daily_close = 終値（Close）
- phase2_return = (daily_close - buy_price) / buy_price
- phase2_win = True if phase2_return > 0（株価上昇 = ロング利益）

【モデルの出力】
- prob_up = phase2_win=True の確率 = 株価上昇確率

【ショート戦略での解釈】
- prob_up 高い → 株価上昇予測 → ショート損失 → 避けるべき
- prob_up 低い → 株価下落予測 → ショート利益 → ショート推奨

【CV結果の読み方（ショート視点）】
- prob_up上位20%: ロング勝率65.7% → ショート勝率34.3% → 避ける
- prob_up下位20%: ロング勝率13.4% → ショート勝率86.6% → 推奨
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    roc_auc_score, accuracy_score, precision_score, recall_score, f1_score
)
import lightgbm as lgb
from common_cfg.paths import PARQUET_DIR

FEATURES_PATH = PARQUET_DIR / "ml" / "archive_with_features.parquet"
MODEL_DIR = ROOT / "models"

# 使用する特徴量
FEATURE_COLUMNS = [
    # 既存特徴量
    'grok_rank', 'selection_score', 'buy_price', 'market_cap',
    'atr14_pct', 'vol_ratio', 'rsi9', 'weekday',
    'nikkei_change_pct', 'futures_change_pct',
    'shortable', 'day_trade',
    # 銘柄個別の価格特徴量
    'volatility_5d', 'volatility_10d', 'volatility_20d',
    'ma5_deviation', 'ma25_deviation',
    'prev_day_return', 'return_5d', 'return_10d',
    'volume_ratio_5d', 'price_range_5d',
    # 市場特徴量（日経、TOPIX、先物、ドル円）
    'nikkei_vol_5d', 'nikkei_ret_5d', 'nikkei_ma5_dev',
    'topix_vol_5d', 'topix_ret_5d', 'topix_ma5_dev',
    'futures_vol_5d', 'futures_ret_5d', 'futures_ma5_dev',
    'usdjpy_vol_5d', 'usdjpy_ret_5d', 'usdjpy_ma5_dev',
]

# 目的変数
TARGET_COLUMN = 'phase2_win'

# ショート推奨閾値
SHORT_RECOMMEND_THRESHOLD = 0.40


def load_data() -> pd.DataFrame:
    """特徴量付きデータを読み込み（張り付き銘柄除外）"""
    print("[INFO] Loading features data...")
    df = pd.read_parquet(FEATURES_PATH)
    print(f"  Loaded: {len(df)} rows, {len(df.columns)} columns")

    # 張り付き銘柄を除外（取引不可能）
    df['is_stuck'] = (
        (df['buy_price'] == df['high']) &
        (df['high'] == df['daily_close']) &
        (df['high'] == df['low'])
    )
    stuck_count = df['is_stuck'].sum()
    df = df[~df['is_stuck']].drop(columns=['is_stuck'])
    print(f"  Excluded {stuck_count} stuck stocks (not tradeable)")
    print(f"  Tradeable: {len(df)} rows")

    return df


def prepare_data(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, list[str], np.ndarray]:
    """学習用データを準備（時系列順にソート）"""
    print("\n[INFO] Preparing data...")

    # 欠損値の確認
    available_features = [col for col in FEATURE_COLUMNS if col in df.columns]
    missing_features = [col for col in FEATURE_COLUMNS if col not in df.columns]

    if missing_features:
        print(f"  ⚠ Missing features: {missing_features}")

    print(f"  Using {len(available_features)} features")

    # 目的変数の確認
    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"Target column '{TARGET_COLUMN}' not found")

    # 欠損行を除外
    df_clean = df.dropna(subset=[TARGET_COLUMN] + available_features).copy()
    print(f"  Rows after removing NaN: {len(df_clean)}/{len(df)}")

    # 時系列順にソート
    df_clean['backtest_date'] = pd.to_datetime(df_clean['backtest_date'])
    df_clean = df_clean.sort_values('backtest_date').reset_index(drop=True)
    print(f"  Date range: {df_clean['backtest_date'].min().date()} ~ {df_clean['backtest_date'].max().date()}")

    # カテゴリカル変数をcategory型に変換（LightGBMのcategorical_feature用）
    X = df_clean[available_features].copy()
    if 'weekday' in X.columns:
        X['weekday'] = X['weekday'].astype('category')

    y = df_clean[TARGET_COLUMN].astype(int).values
    dates = df_clean['backtest_date'].values

    print(f"  Target distribution: Win={y.sum()}, Lose={len(y)-y.sum()} (Win rate: {y.mean()*100:.1f}%)")

    return X, y, available_features, dates


def train_and_evaluate(
    X: pd.DataFrame,
    y: np.ndarray,
    feature_names: list[str],
    dates: np.ndarray
) -> tuple[lgb.LGBMClassifier, dict]:
    """
    時系列Walk-Forward CVで学習・評価

    Args:
        X: 特徴量（DataFrame、weekdayはcategory型）
        y: 目的変数
        feature_names: 特徴量名
        dates: 日付配列（ソート済み）

    Returns:
        best_model: 全データで学習したモデル
        metrics: 評価指標
    """
    print("\n[INFO] Training with Time-Series Walk-Forward CV...")

    # カテゴリカル特徴量
    cat_features = [col for col in ['weekday'] if col in X.columns]
    if cat_features:
        print(f"  Categorical features: {cat_features}")

    # LightGBMパラメータ
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
        'n_estimators': 100,
        'random_state': 42
    }

    # 週単位でグループ化
    weeks = pd.to_datetime(dates).to_period('W')
    unique_weeks = weeks.unique()
    min_train_weeks = 4

    auc_scores = []
    acc_scores = []
    all_preds = []
    all_true = []

    print(f"  Total weeks: {len(unique_weeks)}")
    print(f"  Min train weeks: {min_train_weeks}")

    for i, test_week in enumerate(unique_weeks[min_train_weeks:], min_train_weeks):
        train_weeks = unique_weeks[:i]
        train_mask = np.isin(weeks, train_weeks)
        test_mask = weeks == test_week

        if train_mask.sum() < 50 or test_mask.sum() == 0:
            continue

        X_train, y_train = X[train_mask], y[train_mask]
        X_test, y_test = X[test_mask], y[test_mask]

        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train, categorical_feature=cat_features if cat_features else 'auto')

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        all_preds.extend(y_pred_proba)
        all_true.extend(y_test)

        if len(np.unique(y_test)) > 1:
            auc = roc_auc_score(y_test, y_pred_proba)
            auc_scores.append(auc)

    all_preds = np.array(all_preds)
    all_true = np.array(all_true)

    # 全体評価
    overall_auc = roc_auc_score(all_true, all_preds)
    y_pred = (all_preds >= 0.5).astype(int)
    overall_acc = accuracy_score(all_true, y_pred)
    overall_prec = precision_score(all_true, y_pred, zero_division=0)
    overall_rec = recall_score(all_true, y_pred, zero_division=0)
    overall_f1 = f1_score(all_true, y_pred, zero_division=0)

    # ショート戦略の評価（prob_up <= 0.40）
    short_mask = all_preds <= SHORT_RECOMMEND_THRESHOLD
    if short_mask.sum() > 0:
        short_win_rate = (all_true[short_mask] == 0).mean()  # phase2_win=False がショート勝ち
    else:
        short_win_rate = 0

    # 5分位分析（ショート視点）
    # prob_up低い順 = ショート推奨順にソート
    quintile_results = []
    quintiles = pd.qcut(all_preds, 5, labels=['Q1(低)', 'Q2', 'Q3', 'Q4', 'Q5(高)'], duplicates='drop')

    print(f"\n[5分位分析（ショート視点）]")
    print(f"  Q1(prob低) = ショート推奨, Q5(prob高) = ショート回避")
    print(f"  {'分位':<10} {'件数':<8} {'ショート勝率':<12} {'ロング勝率':<12}")

    for q in ['Q1(低)', 'Q2', 'Q3', 'Q4', 'Q5(高)']:
        mask = quintiles == q
        if mask.sum() > 0:
            count = int(mask.sum())
            long_win_rate = all_true[mask].mean()  # phase2_win=True率
            short_win_rate_q = 1 - long_win_rate   # ショート勝率
            quintile_results.append({
                'quintile': q,
                'count': count,
                'short_win_rate': float(short_win_rate_q),
                'long_win_rate': float(long_win_rate),
            })
            print(f"  {q:<10} {count:<8} {short_win_rate_q*100:<12.1f}% {long_win_rate*100:<12.1f}%")

    metrics = {
        'auc_mean': overall_auc,
        'auc_std': np.std(auc_scores) if auc_scores else 0,
        'accuracy_mean': overall_acc,
        'precision_mean': overall_prec,
        'recall_mean': overall_rec,
        'f1_mean': overall_f1,
        'short_win_rate': short_win_rate,
        'short_count': int(short_mask.sum()),
        'total_evaluated': len(all_true),
        'cv_method': 'time_series_walk_forward',
        'quintile_analysis': quintile_results,
    }

    print(f"\n[Time-Series CV Summary]")
    print(f"  Evaluated samples: {len(all_true)}")
    print(f"  AUC: {metrics['auc_mean']:.4f}")
    print(f"  Accuracy: {metrics['accuracy_mean']:.4f}")
    print(f"  Short (prob<=0.40): {metrics['short_count']} samples, {metrics['short_win_rate']*100:.1f}% win rate")

    # 全データで最終モデルを学習
    print("\n[INFO] Training final model on all data...")
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X, y, categorical_feature=cat_features if cat_features else 'auto')

    return final_model, metrics


def print_feature_importance(model: lgb.LGBMClassifier, feature_names: list[str]):
    """特徴量重要度を表示"""
    print("\n[Feature Importance]")
    importance = model.feature_importances_
    sorted_idx = np.argsort(importance)[::-1]

    for i, idx in enumerate(sorted_idx[:15]):
        print(f"  {i+1}. {feature_names[idx]}: {importance[idx]}")


def save_model(model: lgb.LGBMClassifier, feature_names: list[str], metrics: dict):
    """モデルを保存"""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODEL_DIR / "grok_lgbm_model.pkl"
    meta_path = MODEL_DIR / "grok_lgbm_meta.json"

    # モデル保存
    joblib.dump(model, model_path)
    print(f"\n✓ Model saved: {model_path}")

    # メタ情報保存
    import json
    meta = {
        'feature_names': feature_names,
        'target': TARGET_COLUMN,
        'metrics': metrics,
        'n_features': len(feature_names),
        'short_recommend_threshold': SHORT_RECOMMEND_THRESHOLD,
        'notes': {
            'strategy': 'SHORT',
            'interpretation': 'prob_up低い = 株価下落予測 = ショート推奨',
            'stuck_excluded': True,
        }
    }
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)
    print(f"✓ Meta saved: {meta_path}")


def main():
    """メイン処理"""
    print("=" * 60)
    print("Train ML Model for Price Movement Prediction")
    print("=" * 60)

    # データ読み込み
    df = load_data()

    # データ準備
    X, y, feature_names, dates = prepare_data(df)

    # 学習・評価（時系列CV）
    best_model, metrics = train_and_evaluate(X, y, feature_names, dates)

    # 特徴量重要度
    print_feature_importance(best_model, feature_names)

    # モデル保存
    save_model(best_model, feature_names, metrics)

    print("\n" + "=" * 60)
    print("Training completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
