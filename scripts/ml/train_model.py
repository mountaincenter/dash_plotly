#!/usr/bin/env python3
"""
train_model.py
LightGBMで騰落確率予測モデルを学習
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
    # 新規特徴量
    'volatility_5d', 'volatility_10d', 'volatility_20d',
    'ma5_deviation', 'ma25_deviation',
    'prev_day_return', 'return_5d', 'return_10d',
    'volume_ratio_5d', 'price_range_5d'
]

# 目的変数
TARGET_COLUMN = 'phase2_win'


def load_data() -> pd.DataFrame:
    """特徴量付きデータを読み込み"""
    print("[INFO] Loading features data...")
    df = pd.read_parquet(FEATURES_PATH)
    print(f"  Loaded: {len(df)} rows, {len(df.columns)} columns")
    return df


def prepare_data(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """学習用データを準備"""
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
    df_clean = df.dropna(subset=[TARGET_COLUMN] + available_features)
    print(f"  Rows after removing NaN: {len(df_clean)}/{len(df)}")

    X = df_clean[available_features].values
    y = df_clean[TARGET_COLUMN].astype(int).values

    print(f"  Target distribution: Win={y.sum()}, Lose={len(y)-y.sum()} (Win rate: {y.mean()*100:.1f}%)")

    return X, y, available_features


def train_and_evaluate(X: np.ndarray, y: np.ndarray, feature_names: list[str]) -> tuple[lgb.LGBMClassifier, dict]:
    """
    5-fold CVで学習・評価

    Returns:
        best_model: 最良のモデル
        metrics: 評価指標
    """
    print("\n[INFO] Training with 5-fold CV...")

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

    kfold = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    auc_scores = []
    acc_scores = []
    prec_scores = []
    rec_scores = []
    f1_scores = []
    best_auc = 0
    best_model = None

    for fold, (train_idx, val_idx) in enumerate(kfold.split(X, y), 1):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
        )

        # 予測
        y_pred_proba = model.predict_proba(X_val)[:, 1]
        y_pred = (y_pred_proba >= 0.5).astype(int)

        # 評価
        auc = roc_auc_score(y_val, y_pred_proba)
        acc = accuracy_score(y_val, y_pred)
        prec = precision_score(y_val, y_pred, zero_division=0)
        rec = recall_score(y_val, y_pred, zero_division=0)
        f1 = f1_score(y_val, y_pred, zero_division=0)

        auc_scores.append(auc)
        acc_scores.append(acc)
        prec_scores.append(prec)
        rec_scores.append(rec)
        f1_scores.append(f1)

        print(f"  Fold {fold}: AUC={auc:.4f}, Acc={acc:.4f}, Prec={prec:.4f}, Rec={rec:.4f}, F1={f1:.4f}")

        if auc > best_auc:
            best_auc = auc
            best_model = model

    metrics = {
        'auc_mean': np.mean(auc_scores),
        'auc_std': np.std(auc_scores),
        'accuracy_mean': np.mean(acc_scores),
        'precision_mean': np.mean(prec_scores),
        'recall_mean': np.mean(rec_scores),
        'f1_mean': np.mean(f1_scores),
    }

    print(f"\n[CV Summary]")
    print(f"  AUC: {metrics['auc_mean']:.4f} ± {metrics['auc_std']:.4f}")
    print(f"  Accuracy: {metrics['accuracy_mean']:.4f}")
    print(f"  Precision: {metrics['precision_mean']:.4f}")
    print(f"  Recall: {metrics['recall_mean']:.4f}")
    print(f"  F1: {metrics['f1_mean']:.4f}")

    return best_model, metrics


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
    X, y, feature_names = prepare_data(df)

    # 学習・評価
    best_model, metrics = train_and_evaluate(X, y, feature_names)

    # 特徴量重要度
    print_feature_importance(best_model, feature_names)

    # モデル保存
    save_model(best_model, feature_names, metrics)

    print("\n" + "=" * 60)
    print("Training completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
