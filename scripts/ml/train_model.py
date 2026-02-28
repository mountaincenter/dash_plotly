#!/usr/bin/env python3
"""
train_model.py
LightGBMで騰落確率予測モデルを学習（28特徴量 / 4クラス Grade方式）

=== 損益計算とショート戦略の解釈 ===

【archiveの損益計算（ロング基準）】
- buy_price = 寄付（Open）
- daily_close = 終値（Close）
- phase2_return = (daily_close - buy_price) / buy_price
- phase2_win = True if phase2_return > 0（株価上昇 = ロング利益）

【モデルの出力】
- prob_up = phase2_win=True の確率 = 株価上昇確率

【4クラス Grade方式（ショート視点）】
- G1 (prob_up下位25%): 機械的SHORT推奨
- G2 (25-50%): 機械的SHORT推奨
- G3 (50-75%): 裁量判断
- G4 (上位25%): SKIP（ショート回避）
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

# 使用する特徴量（28個）
FEATURE_COLUMNS = [
    # Grok由来
    'grok_rank', 'selection_score', 'buy_price', 'market_cap',
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

# 目的変数
TARGET_COLUMN = 'phase2_win'

# 4クラスGrade数
N_GRADES = 4


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


def prepare_data(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, list[str], np.ndarray, np.ndarray, np.ndarray]:
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

    X = df_clean[available_features].copy()
    y = df_clean[TARGET_COLUMN].astype(int).values
    dates = df_clean['backtest_date'].values
    tickers = df_clean['ticker'].values

    # SHORT損益（-profit = ショート視点の損益）
    pnl_col = 'profit_per_100_shares_phase2'
    if pnl_col in df_clean.columns:
        pnl_values = (-df_clean[pnl_col]).values
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
    """
    時系列Walk-Forward CVで学習・評価

    Args:
        X: 特徴量DataFrame
        y: 目的変数
        feature_names: 特徴量名
        dates: 日付配列（ソート済み）
        pnl_values: SHORT損益配列（-profit_per_100_shares_phase2）
        tickers: 銘柄コード配列

    Returns:
        best_model: 全データで学習したモデル
        metrics: 評価指標
    """
    print("\n[INFO] Training with Time-Series Walk-Forward CV...")

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

    # 全体評価
    overall_auc = roc_auc_score(all_true, all_preds)
    y_pred = (all_preds >= 0.5).astype(int)
    overall_acc = accuracy_score(all_true, y_pred)
    overall_prec = precision_score(all_true, y_pred, zero_division=0)
    overall_rec = recall_score(all_true, y_pred, zero_division=0)
    overall_f1 = f1_score(all_true, y_pred, zero_division=0)

    # 4クラスGrade分析（ショート視点）
    grade_labels = ['G1', 'G2', 'G3', 'G4']
    grades = pd.qcut(all_preds, N_GRADES, labels=grade_labels, duplicates='drop')

    # Grade境界値を記録
    grade_boundaries = []
    for g in grade_labels:
        mask = grades == g
        if mask.sum() > 0:
            grade_boundaries.append(float(all_preds[mask].max()))
    # 最後のG4は1.0
    grade_boundaries[-1] = 1.0

    grade_results = []
    print(f"\n[4クラスGrade分析（ショート視点）]")
    print(f"  G1+G2 = 機械的SHORT, G3 = 裁量, G4 = SKIP")
    print(f"  {'Grade':<8} {'件数':<8} {'SHORT勝率':<12} {'SHORT損益(¥)':<15}")

    for g in grade_labels:
        mask = grades == g
        if mask.sum() > 0:
            count = int(mask.sum())
            short_wr = float((all_true[mask] == 0).mean())
            short_pnl = float(all_pnl[mask].sum())
            grade_results.append({
                'grade': g,
                'count': count,
                'short_win_rate': short_wr,
                'short_pnl_total': short_pnl,
            })
            print(f"  {g:<8} {count:<8} {short_wr*100:<12.1f}% {short_pnl:>12,.0f}")

    # G1+G2 合計
    g12_mask = (grades == 'G1') | (grades == 'G2')
    g12_wr = float((all_true[g12_mask] == 0).mean()) if g12_mask.sum() > 0 else 0
    g12_pnl = float(all_pnl[g12_mask].sum()) if g12_mask.sum() > 0 else 0
    print(f"  {'G1+G2':<8} {int(g12_mask.sum()):<8} {g12_wr*100:<12.1f}% {g12_pnl:>12,.0f}")

    metrics = {
        'auc_mean': overall_auc,
        'auc_std': np.std(auc_scores) if auc_scores else 0,
        'accuracy_mean': overall_acc,
        'precision_mean': overall_prec,
        'recall_mean': overall_rec,
        'f1_mean': overall_f1,
        'g12_win_rate': g12_wr,
        'g12_count': int(g12_mask.sum()),
        'g12_pnl_total': g12_pnl,
        'total_evaluated': len(all_true),
        'cv_method': 'time_series_walk_forward',
        'n_grades': N_GRADES,
        'grade_boundaries': grade_boundaries,
        'grade_analysis': grade_results,
    }

    print(f"\n[Time-Series CV Summary]")
    print(f"  Evaluated samples: {len(all_true)}")
    print(f"  AUC: {metrics['auc_mean']:.4f}")
    print(f"  Grade boundaries: {grade_boundaries}")
    print(f"  G1+G2 SHORT: {metrics['g12_count']} samples, WR={metrics['g12_win_rate']*100:.1f}%, PnL=¥{metrics['g12_pnl_total']:,.0f}")

    # WFCV予測をparquet保存
    wfcv_df = pd.DataFrame({
        'backtest_date': all_dates,
        'ticker': all_tickers,
        'ml_prob': all_preds,
        'ml_grade': np.asarray(grades),
    })
    wfcv_path = MODEL_DIR / "wfcv_predictions.parquet"
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    wfcv_df.to_parquet(wfcv_path, index=False)
    print(f"\n✓ WFCV predictions saved: {wfcv_path} ({len(wfcv_df)} rows)")

    # 全データで最終モデルを学習
    print("\n[INFO] Training final model on all data...")
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X, y)

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
        'n_grades': N_GRADES,
        'grade_boundaries': metrics.get('grade_boundaries', []),
        'notes': {
            'strategy': 'SHORT_4CLASS',
            'interpretation': 'G1+G2=機械的SHORT, G3=裁量, G4=SKIP',
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
    X, y, feature_names, dates, pnl_values, tickers = prepare_data(df)

    # 学習・評価（時系列CV）
    best_model, metrics = train_and_evaluate(X, y, feature_names, dates, pnl_values, tickers)

    # 特徴量重要度
    print_feature_importance(best_model, feature_names)

    # モデル保存
    save_model(best_model, feature_names, metrics)

    print("\n" + "=" * 60)
    print("Training completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
