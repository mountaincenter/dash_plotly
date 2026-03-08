#!/usr/bin/env python3
"""
predict_granville_ml.py
LightGBM でシグナルに ml_score を付与

日次: 既存モデルをロード → 推論のみ
再学習: --train フラグで walk-forward 学習 → モデル保存

モデル: data/models/granville_lgbm.pkl
特徴量: sma20_dist, sma50_dist, atr14_pct, rsi14, vol20, ret5d, vol_ratio,
        entry_price + rule dummy
目的変数: cap_eff（資本効率）
"""
from __future__ import annotations

import sys
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
MODEL_DIR = ROOT / "data" / "models"
MODEL_PATH = MODEL_DIR / "granville_lgbm.pkl"

FEATURE_COLS = [
    "sma20_dist", "sma50_dist", "atr14_pct", "rsi14",
    "vol20", "ret5d", "vol_ratio", "entry_price_est",
]
RULE_DUMMIES = ["rule_B1", "rule_B2", "rule_B3", "rule_B4"]


def add_rule_dummies(df: pd.DataFrame) -> pd.DataFrame:
    """ルールダミー変数を追加"""
    for r in ["B1", "B2", "B3", "B4"]:
        df[f"rule_{r}"] = (df["rule"] == r).astype(int)
    return df


def predict_signals(signal_date: str | None = None) -> int:
    """当日シグナルにml_scoreを付与"""
    print("[1/2] Loading model and signals...")

    if not MODEL_PATH.exists():
        print(f"[WARN] Model not found: {MODEL_PATH}")
        print("  Run with --train to train model first")
        print("  Falling back to rule-based priority only")
        return 0

    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)

    # 最新シグナルファイルを特定
    if signal_date:
        signal_path = GRANVILLE_DIR / f"signals_{signal_date}.parquet"
    else:
        signal_files = sorted(GRANVILLE_DIR.glob("signals_*.parquet"))
        if not signal_files:
            print("[ERROR] No signal files found")
            return 1
        signal_path = signal_files[-1]

    print(f"  Signal file: {signal_path.name}")
    df = pd.read_parquet(signal_path)

    if df.empty:
        print("[INFO] No signals to score")
        return 0

    df = add_rule_dummies(df)

    # 特徴量準備
    all_features = FEATURE_COLS + RULE_DUMMIES
    X = df[all_features].copy()

    # NaN処理（推論時は中央値で埋める）
    for col in all_features:
        if X[col].isna().any():
            median_val = X[col].median()
            X[col] = X[col].fillna(median_val if not pd.isna(median_val) else 0)

    # 推論
    print("\n[2/2] Predicting ml_score...")
    df["ml_score"] = model.predict(X.values)

    # ルールダミーを除去して保存
    df = df.drop(columns=RULE_DUMMIES)

    # ルール優先→ml_score降順でソート
    rule_priority = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
    df["_priority"] = df["rule"].map(rule_priority)
    df = df.sort_values(["_priority", "ml_score"], ascending=[True, False])
    df = df.drop(columns=["_priority"]).reset_index(drop=True)

    df.to_parquet(signal_path, index=False)
    print(f"[OK] Updated: {signal_path.name} ({len(df)} rows, ml_score added)")

    # S3アップロード
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, signal_path, f"granville/{signal_path.name}")
    except Exception as e:
        print(f"[WARN] S3 upload failed: {e}")

    # サマリー
    print(f"\nml_score distribution:")
    print(f"  mean={df['ml_score'].mean():.2f}, std={df['ml_score'].std():.2f}")
    print(f"  min={df['ml_score'].min():.2f}, max={df['ml_score'].max():.2f}")

    return 0


def train_model() -> int:
    """Walk-forward でモデルを学習・保存"""
    print("=" * 60)
    print("Training Granville LightGBM Model (Walk-Forward)")
    print("=" * 60)

    try:
        import lightgbm as lgb
    except ImportError:
        print("[ERROR] lightgbm not installed. pip install lightgbm")
        return 1

    # 検証済みトレードデータを読み込み
    trades_path = ROOT / "strategy_verification" / "data" / "processed" / "trades_cleaned_topix_v2.parquet"
    prices_path = GRANVILLE_DIR / "prices_topix.parquet"

    if not trades_path.exists():
        print(f"[ERROR] Training data not found: {trades_path}")
        return 1
    if not prices_path.exists():
        print(f"[ERROR] Price data not found: {prices_path}")
        return 1

    print("[1/3] Loading training data...")
    trades = pd.read_parquet(trades_path)
    trades = trades[trades["direction"] == "LONG"].copy()
    prices = pd.read_parquet(prices_path)
    prices["date"] = pd.to_datetime(prices["date"])

    # テクニカル指標計算
    print("[2/3] Computing features...")
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = prices.groupby("ticker")
    prices["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    prices["sma50"] = g["Close"].transform(lambda x: x.rolling(50, min_periods=50).mean())
    prices["prev_close"] = g["Close"].shift(1)
    prices["tr"] = np.maximum(
        prices["High"] - prices["Low"],
        np.maximum(abs(prices["High"] - prices["prev_close"]), abs(prices["Low"] - prices["prev_close"])),
    )
    prices["atr14"] = g["tr"].transform(lambda x: x.rolling(14, min_periods=14).mean())

    delta = g["Close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.groupby(prices["ticker"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(prices["ticker"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    rs = avg_gain / avg_loss.replace(0, np.nan)
    prices["rsi14"] = 100 - (100 / (1 + rs))

    prices["daily_ret"] = g["Close"].pct_change()
    prices["vol20"] = g["daily_ret"].transform(lambda x: x.rolling(20, min_periods=20).std())
    prices["ret5d"] = g["Close"].pct_change(5)
    prices["vol_avg20"] = g["Volume"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    prices["vol_ratio"] = prices["Volume"] / prices["vol_avg20"].replace(0, np.nan)

    # トレードに特徴量付与
    pf = prices[["ticker", "date", "Close", "sma20", "sma50", "atr14", "rsi14", "vol20", "ret5d", "vol_ratio"]].copy()
    pf = pf.rename(columns={"date": "signal_date"})

    trades["signal_date"] = pd.to_datetime(trades["signal_date"])
    pf["signal_date"] = pd.to_datetime(pf["signal_date"])

    df = trades.merge(pf, on=["ticker", "signal_date"], how="left")

    # 証拠金計算
    _LIMIT_TABLE = [
        (100, 30), (200, 50), (500, 80), (700, 100),
        (1000, 150), (1500, 300), (2000, 400), (3000, 500),
        (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
        (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
        (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
        (500000, 70000), (700000, 100000), (1000000, 150000),
    ]

    def _upper_limit(price: float) -> float:
        for threshold, limit in _LIMIT_TABLE:
            if price < threshold:
                return price + limit
        return price + 150000

    df["margin"] = df["entry_price"].apply(lambda p: _upper_limit(p) * 100)
    df["hold_days"] = df["hold_days"].clip(lower=1)
    df["cap_eff"] = df["pnl"] / df["margin"] / df["hold_days"] * 10000

    df["sma20_dist"] = (df["Close"] - df["sma20"]) / df["sma20"] * 100
    df["sma50_dist"] = (df["Close"] - df["sma50"]) / df["sma50"] * 100
    df["atr14_pct"] = df["atr14"] / df["Close"] * 100

    # entry_price_est は entry_price 相当
    df["entry_price_est"] = df["entry_price"]

    # ルールダミー
    df = add_rule_dummies(df)

    # Walk-forward学習
    print("[3/3] Walk-forward training...")
    all_features = FEATURE_COLS + RULE_DUMMIES
    df = df.dropna(subset=FEATURE_COLS).copy()
    df["year"] = pd.to_datetime(df["entry_date"]).dt.year
    years = sorted(df["year"].unique())
    test_years = [y for y in years if y >= years[0] + 3]

    final_model = None
    for test_year in test_years:
        train_years = [y for y in years if y < test_year and y >= test_year - 5]
        train = df[df["year"].isin(train_years)]
        test = df[df["year"] == test_year]

        if len(train) < 500 or len(test) < 100:
            continue

        model = lgb.LGBMRegressor(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_samples=50,
            random_state=42,
            verbose=-1,
            n_jobs=-1,
        )
        model.fit(train[all_features].values, train["cap_eff"].values)

        pred = model.predict(test[all_features].values)
        corr = np.corrcoef(pred, test["cap_eff"].values)[0, 1]
        print(f"  {test_year}: train={len(train):,} test={len(test):,} corr={corr:.3f}")
        final_model = model

    if final_model is None:
        print("[ERROR] No model trained (insufficient data)")
        return 1

    # 全データで最終モデルを学習
    print("\n  Training final model on all data...")
    final_model = lgb.LGBMRegressor(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=50,
        random_state=42,
        verbose=-1,
        n_jobs=-1,
    )
    final_model.fit(df[all_features].values, df["cap_eff"].values)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(final_model, f)
    print(f"\n[OK] Model saved: {MODEL_PATH}")

    # 特徴量重要度
    imp = pd.Series(final_model.feature_importances_, index=all_features).sort_values(ascending=False)
    print("\nFeature importance:")
    for feat, val in imp.items():
        print(f"  {feat}: {val}")

    return 0


def main() -> int:
    print("=" * 60)
    print("Granville ML Prediction")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    if "--train" in sys.argv:
        return train_model()
    else:
        return predict_signals()


if __name__ == "__main__":
    raise SystemExit(main())
