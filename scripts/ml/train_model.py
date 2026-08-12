#!/usr/bin/env python3
"""
train_model.py
LightGBMで騰落確率予測モデルを学習（24特徴量 / bucket方式）

=== 損益計算とショート戦略の解釈 ===

【archiveの損益計算（ショート基準）】
- buy_price = 寄付（Open）
- daily_close = 終値（Close）
- phase2_return = (buy_price - daily_close) / buy_price
- phase2_win = True if phase2_return > 0（株価下落 = ショート利益）

【モデルの出力】
- y = 1 - phase2_win → y=1は「ショート負け」= ロング側
- prob_up = P(y=1) = ショートが負ける確率

【bucket方式（ショート視点）】
- SHORT (prob_up ≤ 0.45): ショート推奨
- SKIP  (prob_up > 0.45): ショート回避
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import (
    roc_auc_score, accuracy_score, precision_score, recall_score, f1_score
)
import lightgbm as lgb
from common_cfg.paths import PARQUET_DIR
from scripts.lib.grok_ml_contract import (
    FEATURE_COLUMNS,
    FEATURE_CONTRACT,
    MARKET_CAP_SOURCE,
    PRICE_HISTORY_SOURCE,
)

FEATURES_PATH = PARQUET_DIR / "ml" / "archive_with_features.parquet"
MODEL_DIR = ROOT / "models"

TARGET_COLUMN = 'phase2_win'

BUCKET_SHORT_THRESHOLD = 0.45

MIN_RELEASE_AUC = 0.50
MIN_RELEASE_SHORT_WIN_RATE = 0.50
MIN_RELEASE_SHORT_PF = 1.0
MIN_RELEASE_SHORT_COUNT = 100
MIN_RELEASE_TOTAL_EVALUATED = 500
MAX_AUC_REGRESSION = 0.01
MAX_SHORT_WIN_RATE_REGRESSION = 0.01


def _assign_bucket(prob: float) -> str:
    if prob <= BUCKET_SHORT_THRESHOLD:
        return 'SHORT'
    return 'SKIP'


def validate_model_release(
    candidate_meta: dict,
    previous_meta: dict | None = None,
) -> None:
    """Reject a statistically unusable or materially regressed model release."""
    if candidate_meta.get("feature_sources", {}).get("market_cap") != MARKET_CAP_SOURCE:
        raise ValueError("Candidate model has incompatible market_cap provenance")
    if candidate_meta.get("feature_sources", {}).get("price_history") != PRICE_HISTORY_SOURCE:
        raise ValueError("Candidate model has incompatible price-history provenance")
    if candidate_meta.get("feature_contract") != FEATURE_CONTRACT:
        raise ValueError("Candidate model has an incompatible feature-time contract")
    metrics = candidate_meta.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("Candidate model has no WFCV metrics")
    required = {
        "auc_mean",
        "short_win_rate",
        "short_count",
        "short_pnl_total",
        "short_pf",
        "total_evaluated",
    }
    missing = sorted(required - set(metrics))
    if missing:
        raise ValueError(f"Candidate model metrics are incomplete: {missing}")

    failures: list[str] = []
    if float(metrics["auc_mean"]) < MIN_RELEASE_AUC:
        failures.append(f"AUC {metrics['auc_mean']} < {MIN_RELEASE_AUC}")
    if float(metrics["short_win_rate"]) < MIN_RELEASE_SHORT_WIN_RATE:
        failures.append(
            f"SHORT win rate {metrics['short_win_rate']} < "
            f"{MIN_RELEASE_SHORT_WIN_RATE}"
        )
    if int(metrics["short_count"]) < MIN_RELEASE_SHORT_COUNT:
        failures.append(
            f"SHORT count {metrics['short_count']} < {MIN_RELEASE_SHORT_COUNT}"
        )
    if float(metrics["short_pnl_total"]) <= 0:
        failures.append("SHORT PnL is not positive")
    if float(metrics["short_pf"]) < MIN_RELEASE_SHORT_PF:
        failures.append(f"SHORT PF {metrics['short_pf']} < {MIN_RELEASE_SHORT_PF}")
    if int(metrics["total_evaluated"]) < MIN_RELEASE_TOTAL_EVALUATED:
        failures.append(
            f"evaluated rows {metrics['total_evaluated']} < "
            f"{MIN_RELEASE_TOTAL_EVALUATED}"
        )

    comparable_previous = (
        isinstance(previous_meta, dict)
        and previous_meta.get("feature_contract") == FEATURE_CONTRACT
        and previous_meta.get("feature_sources", {}).get("market_cap")
        == MARKET_CAP_SOURCE
        and previous_meta.get("feature_sources", {}).get("price_history")
        == PRICE_HISTORY_SOURCE
    )
    previous_metrics = previous_meta.get("metrics") if comparable_previous else None
    if isinstance(previous_metrics, dict):
        previous_auc = previous_metrics.get("auc_mean")
        previous_win_rate = previous_metrics.get("short_win_rate")
        previous_evaluated = previous_metrics.get("total_evaluated")
        if previous_auc is not None and float(metrics["auc_mean"]) < (
            float(previous_auc) - MAX_AUC_REGRESSION
        ):
            failures.append(
                f"AUC regression exceeds {MAX_AUC_REGRESSION}: "
                f"new={metrics['auc_mean']}, old={previous_auc}"
            )
        if previous_win_rate is not None and float(metrics["short_win_rate"]) < (
            float(previous_win_rate) - MAX_SHORT_WIN_RATE_REGRESSION
        ):
            failures.append(
                "SHORT win-rate regression exceeds "
                f"{MAX_SHORT_WIN_RATE_REGRESSION}: "
                f"new={metrics['short_win_rate']}, old={previous_win_rate}"
            )
        if previous_evaluated is not None and int(metrics["total_evaluated"]) < int(
            previous_evaluated
        ):
            failures.append(
                "WFCV coverage regressed: "
                f"new={metrics['total_evaluated']}, old={previous_evaluated}"
            )

    if failures:
        raise ValueError("Model release gate failed: " + "; ".join(failures))


def load_data() -> pd.DataFrame:
    print("[INFO] Loading features data...")
    df = pd.read_parquet(FEATURES_PATH)
    print(f"  Loaded: {len(df)} rows, {len(df.columns)} columns")

    df['is_stuck'] = (
        (df['buy_price'] == df['high']) &
        (df['high'] == df['daily_close']) &
        (df['high'] == df['low'])
    )
    stuck_count = df['is_stuck'].sum()
    df = df[~df['is_stuck']].drop(columns=['is_stuck'])
    print(f"  Excluded {stuck_count} stuck stocks (not tradeable)")

    phase2_return = pd.to_numeric(df.get('phase2_return'), errors='coerce')
    zero_return_mask = phase2_return.fillna(0).abs() < 1e-12
    zero_return_count = int(zero_return_mask.sum())
    df = df[~zero_return_mask]
    print(f"  Excluded {zero_return_count} zero-return rows (no directional label)")
    print(f"  Tradeable: {len(df)} rows")

    return df


def prepare_data(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, list[str], np.ndarray, np.ndarray, np.ndarray]:
    print("\n[INFO] Preparing data...")

    available_features = [col for col in FEATURE_COLUMNS if col in df.columns]
    missing_features = [col for col in FEATURE_COLUMNS if col not in df.columns]

    if missing_features:
        raise ValueError(
            f"Training data is missing required model features: {missing_features}"
        )

    print(f"  Using {len(available_features)} features")

    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"Target column '{TARGET_COLUMN}' not found")

    df_clean = df.dropna(subset=[TARGET_COLUMN] + available_features).copy()
    print(f"  Rows after removing NaN: {len(df_clean)}/{len(df)}")

    df_clean['backtest_date'] = pd.to_datetime(df_clean['backtest_date'])
    df_clean = df_clean.sort_values('backtest_date').reset_index(drop=True)
    if len(df_clean) < 100 or df_clean[TARGET_COLUMN].nunique() != 2:
        raise ValueError(
            "Training data is insufficient after point-in-time feature validation: "
            f"rows={len(df_clean)}, classes={df_clean[TARGET_COLUMN].nunique()}"
        )
    print(f"  Date range: {df_clean['backtest_date'].min().date()} ~ {df_clean['backtest_date'].max().date()}")

    X = df_clean[available_features].copy()
    y = (1 - df_clean[TARGET_COLUMN].astype(int)).values
    dates = df_clean['backtest_date'].values
    tickers = df_clean['ticker'].values

    pnl_col = 'profit_per_100_shares_phase2'
    if pnl_col in df_clean.columns:
        pnl_values = df_clean[pnl_col].values
    else:
        pnl_values = np.zeros(len(y))

    print(f"  Target distribution: Up={y.sum()}, Down={len(y)-y.sum()} (Up rate: {y.mean()*100:.1f}%)")

    return X, y, available_features, dates, pnl_values, tickers


def train_and_evaluate(
    X: pd.DataFrame,
    y: np.ndarray,
    feature_names: list[str],
    dates: np.ndarray,
    pnl_values: np.ndarray,
    tickers: np.ndarray,
) -> tuple[lgb.LGBMClassifier, dict]:
    print("\n[INFO] Training with Time-Series Walk-Forward CV...")

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

    weeks = pd.to_datetime(dates).to_period('W')
    unique_weeks = weeks.unique()
    min_train_weeks = 4

    auc_scores = []
    all_preds = []
    all_true = []
    all_pnl = []
    all_dates = []
    all_tickers = []

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
        model.fit(X_train, y_train)

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        all_preds.extend(y_pred_proba)
        all_true.extend(y_test)
        all_pnl.extend(pnl_values[test_mask])
        all_dates.extend(dates[test_mask])
        all_tickers.extend(tickers[test_mask])

        if len(np.unique(y_test)) > 1:
            auc = roc_auc_score(y_test, y_pred_proba)
            auc_scores.append(auc)

    all_preds = np.array(all_preds)
    all_true = np.array(all_true)
    all_pnl = np.array(all_pnl)
    if len(all_true) < 50 or len(np.unique(all_true)) != 2:
        raise ValueError(
            "Walk-forward validation produced insufficient out-of-sample data: "
            f"rows={len(all_true)}, classes={len(np.unique(all_true))}"
        )

    overall_auc = roc_auc_score(all_true, all_preds)
    y_pred = (all_preds >= 0.5).astype(int)
    overall_acc = accuracy_score(all_true, y_pred)
    overall_prec = precision_score(all_true, y_pred, zero_division=0)
    overall_rec = recall_score(all_true, y_pred, zero_division=0)
    overall_f1 = f1_score(all_true, y_pred, zero_division=0)

    buckets = np.array([_assign_bucket(p) for p in all_preds])

    bucket_results = []
    print(f"\n[Bucket分析（ショート視点）]")
    print(f"  SHORT ≤{BUCKET_SHORT_THRESHOLD}, SKIP >{BUCKET_SHORT_THRESHOLD}")
    print(f"  {'Bucket':<8} {'件数':<8} {'SHORT勝率':<12} {'SHORT損益(¥)':<15} {'PF':<8}")

    for bk in ['SHORT', 'SKIP']:
        mask = buckets == bk
        if mask.sum() > 0:
            count = int(mask.sum())
            short_wr = float((all_true[mask] == 0).mean())
            short_pnl = float(all_pnl[mask].sum())
            wins = all_pnl[mask][all_pnl[mask] > 0].sum()
            losses = abs(all_pnl[mask][all_pnl[mask] < 0].sum())
            pf = round(wins / losses, 2) if losses > 0 else float('inf')
            bucket_results.append({
                'bucket': bk,
                'count': count,
                'short_win_rate': short_wr,
                'short_pnl_total': short_pnl,
                'pf': pf,
            })
            print(f"  {bk:<8} {count:<8} {short_wr*100:<12.1f}% {short_pnl:>12,.0f}  {pf}")

    short_mask = buckets == 'SHORT'
    short_wr = float((all_true[short_mask] == 0).mean()) if short_mask.sum() > 0 else 0
    short_pnl = float(all_pnl[short_mask].sum()) if short_mask.sum() > 0 else 0
    short_wins = all_pnl[short_mask][all_pnl[short_mask] > 0].sum() if short_mask.sum() > 0 else 0
    short_losses = abs(all_pnl[short_mask][all_pnl[short_mask] < 0].sum()) if short_mask.sum() > 0 else 0
    short_pf = round(short_wins / short_losses, 2) if short_losses > 0 else float('inf')

    metrics = {
        'auc_mean': overall_auc,
        'auc_std': np.std(auc_scores) if auc_scores else 0,
        'accuracy_mean': overall_acc,
        'precision_mean': overall_prec,
        'recall_mean': overall_rec,
        'f1_mean': overall_f1,
        'short_win_rate': short_wr,
        'short_count': int(short_mask.sum()),
        'short_pnl_total': short_pnl,
        'short_pf': short_pf,
        'total_evaluated': len(all_true),
        'cv_method': 'time_series_walk_forward',
        'bucket_thresholds': {
            'short': BUCKET_SHORT_THRESHOLD,
        },
        'bucket_analysis': bucket_results,
    }

    print(f"\n[Time-Series CV Summary]")
    print(f"  Evaluated samples: {len(all_true)}")
    print(f"  AUC: {metrics['auc_mean']:.4f}")
    print(f"  SHORT bucket: {metrics['short_count']} samples, WR={metrics['short_win_rate']*100:.1f}%, PnL=¥{metrics['short_pnl_total']:,.0f}, PF={short_pf}")

    wfcv_df = pd.DataFrame({
        'backtest_date': all_dates,
        'ticker': all_tickers,
        'ml_prob': all_preds,
    })
    wfcv_path = MODEL_DIR / "wfcv_predictions.parquet"
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    wfcv_df.to_parquet(wfcv_path, index=False)
    print(f"\n✓ WFCV predictions saved: {wfcv_path} ({len(wfcv_df)} rows)")

    print("\n[INFO] Training final model on all data...")
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X, y)

    return final_model, metrics


def print_feature_importance(model: lgb.LGBMClassifier, feature_names: list[str]):
    print("\n[Feature Importance]")
    importance = model.feature_importances_
    sorted_idx = np.argsort(importance)[::-1]

    for i, idx in enumerate(sorted_idx[:15]):
        print(f"  {i+1}. {feature_names[idx]}: {importance[idx]}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_model(model: lgb.LGBMClassifier, feature_names: list[str], metrics: dict):
    if feature_names != list(FEATURE_COLUMNS):
        raise ValueError(
            "Refusing to save a model outside the fixed feature contract: "
            f"expected={list(FEATURE_COLUMNS)}, actual={feature_names}"
        )
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODEL_DIR / "grok_lgbm_model.pkl"
    meta_path = MODEL_DIR / "grok_lgbm_meta.json"
    model_descriptor, model_temp_name = tempfile.mkstemp(
        prefix=f".{model_path.name}.", suffix=".tmp", dir=MODEL_DIR
    )
    os.close(model_descriptor)
    meta_descriptor, meta_temp_name = tempfile.mkstemp(
        prefix=f".{meta_path.name}.", suffix=".tmp", dir=MODEL_DIR
    )
    os.close(meta_descriptor)
    model_temp = Path(model_temp_name)
    meta_temp = Path(meta_temp_name)
    try:
        joblib.dump(model, model_temp)
        model_sha256 = _file_sha256(model_temp)
        meta = {
            'feature_names': feature_names,
            'target': TARGET_COLUMN,
            'metrics': metrics,
            'n_features': len(feature_names),
            'model_sha256': model_sha256,
            'trained_at': datetime.now(timezone.utc).isoformat(),
            'feature_contract': FEATURE_CONTRACT,
            'feature_sources': {
                'market_cap': MARKET_CAP_SOURCE,
                'price_history': PRICE_HISTORY_SOURCE,
            },
            'bucket_thresholds': {
                'short': BUCKET_SHORT_THRESHOLD,
            },
            'notes': {
                'strategy': 'SHORT_BUCKET',
                'interpretation': 'SHORT=prob_up≤0.45, SKIP=>0.45',
                'stuck_excluded': True,
                'zero_return_excluded': True,
                'removed_features': [
                    'grok_rank',
                    'selection_score',
                    'buy_price',
                    'gap_ratio',
                ],
                'point_in_time_rule': (
                    'all features must be observable before the target trading day'
                ),
            }
        }
        with meta_temp.open('w', encoding='utf-8') as handle:
            json.dump(meta, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(model_temp, model_path)
        os.replace(meta_temp, meta_path)
    finally:
        model_temp.unlink(missing_ok=True)
        meta_temp.unlink(missing_ok=True)

    print(f"\n✓ Model saved: {model_path}")
    print(f"✓ Model SHA256: {model_sha256}")
    print(f"✓ Meta saved: {meta_path}")


def main():
    print("=" * 60)
    print("Train ML Model for Price Movement Prediction")
    print("=" * 60)

    df = load_data()
    X, y, feature_names, dates, pnl_values, tickers = prepare_data(df)
    best_model, metrics = train_and_evaluate(X, y, feature_names, dates, pnl_values, tickers)
    print_feature_importance(best_model, feature_names)
    save_model(best_model, feature_names, metrics)
    print(
        "[INFO] WFCV probabilities remain in models/wfcv_predictions.parquet; "
        "the canonical archive is not modified by model training."
    )

    print("\n" + "=" * 60)
    print("Training completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
