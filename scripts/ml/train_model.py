#!/usr/bin/env python3
"""
train_model.py
LightGBMで騰落確率予測モデルを学習（26特徴量 / bucket方式）

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
- DISC  (0.45 < prob_up ≤ 0.70): 裁量判断
- LONG  (prob_up > 0.70): ショート回避
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
from sklearn.metrics import (
    roc_auc_score, accuracy_score, precision_score, recall_score, f1_score
)
import lightgbm as lgb
from common_cfg.paths import PARQUET_DIR

FEATURES_PATH = PARQUET_DIR / "ml" / "archive_with_features.parquet"
MODEL_DIR = ROOT / "models"

FEATURE_COLUMNS = [
    # Grok由来（grok_rank/selection_scoreは棄却: 再選定でブレるノイズ、AUC+0.014で改善確認済み）
    'buy_price', 'market_cap',
    'atr14_pct', 'vol_ratio', 'rsi9',
    'nikkei_change_pct', 'futures_change_pct',
    # 銘柄個別の価格特徴量
    'volatility_5d', 'ma5_deviation', 'ma25_deviation',
    'prev_day_return', 'volume_ratio_5d', 'price_range_5d',
    # 市場特徴量
    'nikkei_vol_5d', 'nikkei_ret_5d',
    'topix_vol_5d', 'topix_ret_5d',
    'futures_ret_5d',
    'usdjpy_vol_5d', 'usdjpy_ret_5d',
    # 前日OHLCV由来
    'prev_close_position', 'gap_ratio', 'prev_candle',
    # テクニカル指標
    'macd_hist', 'bb_pctb', 'vol_trend',
]

TARGET_COLUMN = 'phase2_win'

BUCKET_SHORT_THRESHOLD = 0.45
BUCKET_LONG_THRESHOLD = 0.70


def _assign_bucket(prob: float) -> str:
    if prob <= BUCKET_SHORT_THRESHOLD:
        return 'SHORT'
    elif prob <= BUCKET_LONG_THRESHOLD:
        return 'DISC'
    return 'LONG'


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
    print(f"  Tradeable: {len(df)} rows")

    return df


def prepare_data(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, list[str], np.ndarray, np.ndarray, np.ndarray]:
    print("\n[INFO] Preparing data...")

    available_features = [col for col in FEATURE_COLUMNS if col in df.columns]
    missing_features = [col for col in FEATURE_COLUMNS if col not in df.columns]

    if missing_features:
        print(f"  ⚠ Missing features: {missing_features}")

    print(f"  Using {len(available_features)} features")

    if TARGET_COLUMN not in df.columns:
        raise ValueError(f"Target column '{TARGET_COLUMN}' not found")

    df_clean = df.dropna(subset=[TARGET_COLUMN] + available_features).copy()
    print(f"  Rows after removing NaN: {len(df_clean)}/{len(df)}")

    df_clean['backtest_date'] = pd.to_datetime(df_clean['backtest_date'])
    df_clean = df_clean.sort_values('backtest_date').reset_index(drop=True)
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

    print(f"  Target distribution: Win={y.sum()}, Lose={len(y)-y.sum()} (Win rate: {y.mean()*100:.1f}%)")

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

    overall_auc = roc_auc_score(all_true, all_preds)
    y_pred = (all_preds >= 0.5).astype(int)
    overall_acc = accuracy_score(all_true, y_pred)
    overall_prec = precision_score(all_true, y_pred, zero_division=0)
    overall_rec = recall_score(all_true, y_pred, zero_division=0)
    overall_f1 = f1_score(all_true, y_pred, zero_division=0)

    buckets = np.array([_assign_bucket(p) for p in all_preds])

    bucket_results = []
    print(f"\n[Bucket分析（ショート視点）]")
    print(f"  SHORT ≤{BUCKET_SHORT_THRESHOLD}, DISC ≤{BUCKET_LONG_THRESHOLD}, LONG >{BUCKET_LONG_THRESHOLD}")
    print(f"  {'Bucket':<8} {'件数':<8} {'SHORT勝率':<12} {'SHORT損益(¥)':<15} {'PF':<8}")

    for bk in ['SHORT', 'DISC', 'LONG']:
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
            'long': BUCKET_LONG_THRESHOLD,
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


def save_model(model: lgb.LGBMClassifier, feature_names: list[str], metrics: dict):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODEL_DIR / "grok_lgbm_model.pkl"
    meta_path = MODEL_DIR / "grok_lgbm_meta.json"

    joblib.dump(model, model_path)
    print(f"\n✓ Model saved: {model_path}")

    import json
    meta = {
        'feature_names': feature_names,
        'target': TARGET_COLUMN,
        'metrics': metrics,
        'n_features': len(feature_names),
        'bucket_thresholds': {
            'short': BUCKET_SHORT_THRESHOLD,
            'long': BUCKET_LONG_THRESHOLD,
        },
        'notes': {
            'strategy': 'SHORT_BUCKET',
            'interpretation': 'SHORT=prob_up≤0.45, DISC=0.45-0.70, LONG=>0.70',
            'stuck_excluded': True,
            'removed_features': ['grok_rank', 'selection_score'],
        }
    }
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)
    print(f"✓ Meta saved: {meta_path}")


def update_archive_with_prob():
    archive_path = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
    wfcv_path = MODEL_DIR / "wfcv_predictions.parquet"

    if not archive_path.exists() or not wfcv_path.exists():
        print("[WARN] archive or wfcv not found, skipping prob update")
        return

    arc = pd.read_parquet(archive_path)
    wfcv = pd.read_parquet(wfcv_path)

    original_cols = list(arc.columns)
    original_len = len(arc)

    arc["backtest_date"] = pd.to_datetime(arc["backtest_date"])
    wfcv["backtest_date"] = pd.to_datetime(wfcv["backtest_date"])

    for col in ["ml_grade", "ml_prob"]:
        if col in arc.columns:
            arc = arc.drop(columns=[col])

    wfcv_dedup = wfcv[["backtest_date", "ticker", "ml_prob"]].drop_duplicates(
        subset=["backtest_date", "ticker"], keep="last"
    )

    merged = arc.merge(
        wfcv_dedup,
        on=["backtest_date", "ticker"],
        how="left",
    )

    if len(merged) != original_len:
        print(f"[ERROR] Row count changed: {original_len} -> {len(merged)}. Aborting.")
        return

    for col in original_cols:
        if col in ("ml_grade", "ml_prob"):
            continue
        if col not in merged.columns:
            print(f"[ERROR] Column '{col}' lost after merge. Aborting.")
            return

    prob_matched = merged["ml_prob"].notna().sum()
    print(f"\n[INFO] Archive ml_prob update: {prob_matched}/{original_len} rows matched")

    merged.to_parquet(archive_path, index=False)
    print(f"✓ Archive updated with ml_prob: {archive_path}")


def main():
    print("=" * 60)
    print("Train ML Model for Price Movement Prediction")
    print("=" * 60)

    df = load_data()
    X, y, feature_names, dates, pnl_values, tickers = prepare_data(df)
    best_model, metrics = train_and_evaluate(X, y, feature_names, dates, pnl_values, tickers)
    print_feature_importance(best_model, feature_names)
    save_model(best_model, feature_names, metrics)
    update_archive_with_prob()

    print("\n" + "=" * 60)
    print("Training completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
