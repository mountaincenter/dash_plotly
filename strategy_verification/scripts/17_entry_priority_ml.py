#!/usr/bin/env python3
"""
17_entry_priority_ml.py
========================
エントリー優先順位: MLランキング vs ルールベースの比較検証

16_entry_priority_analysis.pyで「RSI低い順が最善」と判明。
ML（複数特徴量の複合）がさらに改善できるかを検証する。

手法:
  1. LightGBMで cap_eff を予測するランキングモデルを学習
  2. Walk-forward検証（学習3年→テスト1年のローリング）
  3. 同日シグナルからTop-K選択時のcap_eff/勝率/PnLを比較

比較対象:
  - ランダム（ベースライン）
  - ルール優先（B4>B1>B3>B2）
  - RSI低い順（単一指標最善）
  - ML予測スコア順
"""
from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import GroupKFold

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
REPORT_DIR = SV_DIR / "chapters" / "09_entry_priority"

FEATURE_COLS = [
    "sma20_dist", "sma50_dist", "atr14_pct", "rsi14",
    "vol20", "ret5d", "vol_ratio", "entry_price",
]
RULE_DUMMIES = ["rule_B1", "rule_B2", "rule_B3", "rule_B4"]
REGIME_DUMMIES = ["regime_Uptrend"]


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """特徴量の前処理"""
    df = df.copy()
    # ルールダミー
    for r in ["B1", "B2", "B3", "B4"]:
        df[f"rule_{r}"] = (df["rule"] == r).astype(int)
    # レジームダミー
    df["regime_Uptrend"] = (df["regime"] == "Uptrend").astype(int)
    # entry_dateを文字列に統一
    df["entry_date_str"] = df["entry_date"].astype(str)
    return df


def walk_forward_predict(df: pd.DataFrame) -> pd.DataFrame:
    """
    Walk-forward: 3年学習 → 1年テスト のローリング予測。
    各テスト年のML予測スコアを返す。
    """
    all_features = FEATURE_COLS + RULE_DUMMIES + REGIME_DUMMIES
    df = df.dropna(subset=FEATURE_COLS).copy()
    df["year"] = pd.to_datetime(df["entry_date"]).dt.year

    years = sorted(df["year"].unique())
    # 最低3年の学習データが必要
    test_years = [y for y in years if y >= years[0] + 3]

    predictions = []

    for test_year in test_years:
        train_years = [y for y in years if y < test_year and y >= test_year - 5]
        train = df[df["year"].isin(train_years)]
        test = df[df["year"] == test_year]

        if len(train) < 500 or len(test) < 100:
            continue

        X_train = train[all_features].values
        y_train = train["cap_eff"].values
        X_test = test[all_features].values

        # LightGBM回帰
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
        model.fit(X_train, y_train)

        pred = model.predict(X_test)
        test_result = test[["ticker", "entry_date_str", "rule", "pnl", "win",
                            "cap_eff", "margin", "rsi14", "ret_pct", "hold_days"]].copy()
        test_result["ml_score"] = pred
        test_result["test_year"] = test_year
        predictions.append(test_result)

        # 特徴量重要度（最後の年のみ表示）
        if test_year == test_years[-1]:
            imp = pd.Series(
                model.feature_importances_, index=all_features
            ).sort_values(ascending=False)
            print(f"\n=== 特徴量重要度 (train={train_years}, test={test_year}) ===")
            print(imp.to_string())

    return pd.concat(predictions, ignore_index=True)


def evaluate_ranking(preds: pd.DataFrame):
    """各ランキング方法のTop-K選択性能を比較"""
    rule_priority = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
    preds["rank_rule"] = preds["rule"].map(rule_priority)

    # 3件以上のシグナル日のみ
    day_counts = preds.groupby("entry_date_str").size()
    multi_days = day_counts[day_counts >= 3].index
    multi = preds[preds["entry_date_str"].isin(multi_days)].copy()
    print(f"\n3件以上シグナル日: {len(multi_days):,}日, トレード数: {len(multi):,}")

    methods = {
        "ルール優先(B4>B1)": ("rank_rule", True),
        "RSI低い順": ("rsi14", True),
        "ML予測スコア順": ("ml_score", False),
    }

    print(f"\n{'':>20} | {'Top-1':>40} | {'Top-3':>40} | {'Top-5':>40}")
    print(f"{'方法':>20} | {'cap_eff':>8} {'勝率':>6} {'PnL':>10} {'n':>7} | {'cap_eff':>8} {'勝率':>6} {'PnL':>10} {'n':>7} | {'cap_eff':>8} {'勝率':>6} {'PnL':>10} {'n':>7}")
    print("-" * 160)

    for method_name, (col, ascending) in methods.items():
        valid = multi.dropna(subset=[col])
        ranked = valid.sort_values(
            ["entry_date_str", col], ascending=[True, ascending]
        )
        ranked["day_rank"] = ranked.groupby("entry_date_str").cumcount() + 1

        parts = []
        for k in [1, 3, 5]:
            sel = ranked[ranked["day_rank"] <= k]
            ce = sel["cap_eff"].mean()
            wr = sel["win"].mean()
            pnl = sel["pnl"].mean()
            n = len(sel)
            parts.append(f"{ce:>+8.2f} {wr:>5.1%} {pnl:>+10,.0f} {n:>7,}")
        print(f"{method_name:>20} | {' | '.join(parts)}")

    # ランダム（全体平均）
    ce = multi["cap_eff"].mean()
    wr = multi["win"].mean()
    pnl = multi["pnl"].mean()
    n = len(multi)
    random_str = f"{ce:>+8.2f} {wr:>5.1%} {pnl:>+10,.0f} {n:>7,}"
    print(f"{'ランダム(全体平均)':>20} | {random_str} | {random_str} | {random_str}")

    # 年別のTop-1比較
    print(f"\n=== 年別 Top-1 cap_eff 比較 ===")
    print(f"{'年':>6} | {'ルール優先':>10} | {'RSI低い順':>10} | {'ML予測':>10} | {'全体平均':>10} | {'ML勝ち':>6}")
    print("-" * 70)

    for method_name, (col, ascending) in methods.items():
        valid = multi.dropna(subset=[col])
        ranked = valid.sort_values(
            ["entry_date_str", col], ascending=[True, ascending]
        )
        ranked["day_rank"] = ranked.groupby("entry_date_str").cumcount() + 1
        if method_name == "ルール優先(B4>B1)":
            top1_rule = ranked[ranked["day_rank"] == 1].copy()
            top1_rule["year"] = pd.to_datetime(top1_rule["entry_date_str"]).dt.year
        elif method_name == "RSI低い順":
            top1_rsi = ranked[ranked["day_rank"] == 1].copy()
            top1_rsi["year"] = pd.to_datetime(top1_rsi["entry_date_str"]).dt.year
        else:
            top1_ml = ranked[ranked["day_rank"] == 1].copy()
            top1_ml["year"] = pd.to_datetime(top1_ml["entry_date_str"]).dt.year

    multi_copy = multi.copy()
    multi_copy["year"] = pd.to_datetime(multi_copy["entry_date_str"]).dt.year

    for year in sorted(multi_copy["year"].unique()):
        rule_ce = top1_rule[top1_rule["year"] == year]["cap_eff"].mean() if year in top1_rule["year"].values else np.nan
        rsi_ce = top1_rsi[top1_rsi["year"] == year]["cap_eff"].mean() if year in top1_rsi["year"].values else np.nan
        ml_ce = top1_ml[top1_ml["year"] == year]["cap_eff"].mean() if year in top1_ml["year"].values else np.nan
        all_ce = multi_copy[multi_copy["year"] == year]["cap_eff"].mean()
        ml_wins = "o" if not np.isnan(ml_ce) and ml_ce > max(rule_ce or -999, rsi_ce or -999) else ""
        print(f"{year:>6} | {rule_ce:>+10.2f} | {rsi_ce:>+10.2f} | {ml_ce:>+10.2f} | {all_ce:>+10.2f} | {ml_wins:>6}")

    # 累計PnL比較（Top-1選択で1トレード100株の場合）
    print(f"\n=== 累計PnL比較 (Top-1選択) ===")
    rule_total = top1_rule["pnl"].sum()
    rsi_total = top1_rsi["pnl"].sum()
    ml_total = top1_ml["pnl"].sum()
    all_total = multi_copy.groupby("entry_date_str")["pnl"].first().sum()  # ランダム1件
    print(f"  ルール優先: {rule_total:>+15,.0f}円")
    print(f"  RSI低い順:  {rsi_total:>+15,.0f}円")
    print(f"  ML予測:     {ml_total:>+15,.0f}円")


def main():
    # データ読込
    df = pd.read_parquet(REPORT_DIR / "trades_with_features.parquet")
    print(f"データ: {len(df):,} trades")

    df = prepare_data(df)

    # Walk-forward予測
    print("\n=== Walk-forward ML予測 ===")
    preds = walk_forward_predict(df)
    print(f"\n予測対象: {len(preds):,} trades, 年: {preds['test_year'].min()}-{preds['test_year'].max()}")

    # ランキング評価
    evaluate_ranking(preds)

    # 保存
    preds.to_parquet(REPORT_DIR / "ml_predictions.parquet", index=False)
    print(f"\n保存: {REPORT_DIR / 'ml_predictions.parquet'}")


if __name__ == "__main__":
    main()
