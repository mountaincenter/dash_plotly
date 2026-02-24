#!/usr/bin/env python3
"""
グランビルIFD MLフィルター学習
==============================
26年分の日足データからシグナル生成 → 結果判定 → LightGBM学習

目的: シグナルが SL(-3.5%) に引っかかるか、利益で終わるかを予測
→ 高リスクシグナルを除外するフィルター
"""
from __future__ import annotations

import sys
import json
import time
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import lightgbm as lgb
from sklearn.metrics import roc_auc_score, accuracy_score

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO_DIR = ROOT / "improvement" / "data" / "macro"
MODEL_DIR = ROOT / "models"
BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]

SL_PCT = 3.5
HOLD_DAYS = 7


def notify_slack(message: str):
    """Slack通知"""
    try:
        from dotenv import dotenv_values
        env = dotenv_values(ROOT / ".env.slack")
        url = env.get("SLACK_WEBHOOK_CLAUDE", "")
        if not url:
            print("[WARN] No Slack webhook URL")
            return
        import urllib.request
        data = json.dumps({"text": message}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req)
        print("[INFO] Slack notification sent")
    except Exception as e:
        print(f"[WARN] Slack notification failed: {e}")


def load_data():
    """全データ読み込み + SMA等の計算"""
    print("[1/6] Loading data...")
    t0 = time.time()

    m = pd.read_parquet(ROOT / "data/parquet/meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data/parquet/prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Index
    idx = pd.read_parquet(ROOT / "data/parquet/index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]
    # 市場特徴量
    nk["nk225_ret_5d"] = nk["nk225_close"].pct_change(5)
    nk["nk225_vol_5d"] = nk["nk225_close"].pct_change().rolling(5).std()
    nk["nk225_ma5_dev"] = (nk["nk225_close"] / nk["nk225_close"].rolling(5).mean() - 1) * 100

    # CI
    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"})
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)

    daily = pd.DataFrame({"date": ps["date"].drop_duplicates().sort_values()})
    daily = daily.merge(ci, on="date", how="left").sort_values("date").ffill()

    # SMA / deviation
    g = ps.groupby("ticker")
    ps["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma25"] = g["Close"].transform(lambda x: x.rolling(25).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["sma5_above_sma20"] = ps["sma5"] > ps["sma20"]
    ps["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)

    # 追加特徴量
    ps["rsi14"] = g["Close"].transform(lambda x: _rsi(x, 14))
    ps["rsi9"] = g["Close"].transform(lambda x: _rsi(x, 9))
    ps["atr14"] = g.apply(lambda x: _atr(x, 14)).reset_index(level=0, drop=True)
    ps["atr14_pct"] = ps["atr14"] / ps["Close"] * 100
    ps["volatility_5d"] = g["Close"].transform(lambda x: x.pct_change().rolling(5).std() * 100)
    ps["volatility_10d"] = g["Close"].transform(lambda x: x.pct_change().rolling(10).std() * 100)
    ps["volatility_20d"] = g["Close"].transform(lambda x: x.pct_change().rolling(20).std() * 100)
    ps["ma5_deviation"] = (ps["Close"] / ps["sma5"] - 1) * 100
    ps["ma25_deviation"] = (ps["Close"] / ps["sma25"] - 1) * 100
    ps["prev_day_return"] = g["Close"].transform(lambda x: x.pct_change() * 100)
    ps["return_5d"] = g["Close"].transform(lambda x: x.pct_change(5) * 100)
    ps["return_10d"] = g["Close"].transform(lambda x: x.pct_change(10) * 100)
    ps["volume_ratio_5d"] = ps["Volume"] / g["Volume"].transform(lambda x: x.rolling(5).mean())
    ps["price_range_5d"] = g.apply(
        lambda x: (x["High"].rolling(5).max() - x["Low"].rolling(5).min()) / x["Close"] * 100
    ).reset_index(level=0, drop=True)
    ps["weekday"] = ps["date"].dt.weekday

    ps = ps.dropna(subset=["sma20"])

    ps = ps.merge(nk[["date", "market_uptrend", "nk225_ret_5d", "nk225_vol_5d", "nk225_ma5_dev"]], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0
    ps = ps.merge(m[["ticker", "sectors"]], on="ticker", how="left")

    print(f"  Done in {time.time()-t0:.1f}s: {len(ps):,} rows, {ps['ticker'].nunique()} tickers")
    print(f"  Date range: {ps['date'].min().date()} ~ {ps['date'].max().date()}")
    return ps


def _rsi(series, period):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _atr(df, period):
    h = df["High"]
    l = df["Low"]
    c = df["Close"].shift(1)
    tr = pd.concat([h - l, (h - c).abs(), (l - c).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def detect_signals(ps):
    """シグナル検出"""
    print("[2/6] Detecting signals...")
    dev = ps["dev_from_sma20"]
    ps["sig_A"] = (dev.between(-8, -3)) & (ps["Close"] > ps["prev_close"])
    ps["sig_B"] = (
        ps["sma20_up"] & (ps["Close"] > ps["sma20"]) &
        (dev.between(0, 2)) & (ps["prev_dev"] <= 0.5) &
        (ps["Close"] > ps["prev_close"])
    )
    # フィルター適用
    mask = (
        (ps["market_uptrend"] == True) &
        (ps["macro_ci_expand"] == True) &
        (~ps["sectors"].isin(BAD_SECTORS))
    )
    sig = ps[mask & (ps["sig_A"] | ps["sig_B"])].copy()
    sig["signal_type"] = "A"
    sig.loc[sig["sig_B"], "signal_type"] = "B"
    sig.loc[sig["sig_A"] & sig["sig_B"], "signal_type"] = "A+B"

    print(f"  Signals: {len(sig):,} (A={sig['sig_A'].sum():,}, B={sig['sig_B'].sum():,})")
    return sig


def simulate_outcomes(ps, signals):
    """各シグナルの結果を判定（SL -3.5%, 7日引け）"""
    print("[3/6] Simulating trade outcomes...")
    t0 = time.time()

    results = []
    for ticker in signals["ticker"].unique():
        tk = ps[ps["ticker"] == ticker].sort_values("date")
        dates = tk["date"].values
        opens = tk["Open"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        closes = tk["Close"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        sig_rows = signals[signals["ticker"] == ticker]
        for _, row in sig_rows.iterrows():
            sd = row["date"]
            if sd not in date_idx:
                continue
            idx = date_idx[sd]
            if idx + 1 >= len(dates):
                continue

            entry_idx = idx + 1
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            sl_price = entry_price * (1 - SL_PCT / 100)

            hit_sl = False
            exit_day = 0
            for d in range(HOLD_DAYS):
                ci = entry_idx + d
                if ci >= len(dates):
                    break
                if lows[ci] <= sl_price:
                    ret_pct = -SL_PCT
                    hit_sl = True
                    exit_day = d
                    break

            if not hit_sl:
                last_idx = min(entry_idx + HOLD_DAYS - 1, len(dates) - 1)
                exit_price = closes[last_idx]
                ret_pct = (exit_price / entry_price - 1) * 100
                exit_day = min(HOLD_DAYS - 1, len(dates) - 1 - entry_idx)

            # 100株 × entry_price で PnL
            pnl = int(round(entry_price * 100 * ret_pct / 100))

            results.append({
                "date": sd,
                "ticker": row["ticker"],
                "signal_type": row["signal_type"],
                "entry_price": entry_price,
                "ret_pct": ret_pct,
                "pnl": pnl,
                "exit_type": "SL" if hit_sl else "expire",
                "exit_day": exit_day,
                "win": ret_pct > 0,
                # 特徴量
                "dev_from_sma20": row["dev_from_sma20"],
                "sma20_slope": row["sma20_slope"],
                "rsi14": row.get("rsi14", np.nan),
                "rsi9": row.get("rsi9", np.nan),
                "atr14_pct": row.get("atr14_pct", np.nan),
                "volatility_5d": row.get("volatility_5d", np.nan),
                "volatility_10d": row.get("volatility_10d", np.nan),
                "volatility_20d": row.get("volatility_20d", np.nan),
                "ma5_deviation": row.get("ma5_deviation", np.nan),
                "ma25_deviation": row.get("ma25_deviation", np.nan),
                "prev_day_return": row.get("prev_day_return", np.nan),
                "return_5d": row.get("return_5d", np.nan),
                "return_10d": row.get("return_10d", np.nan),
                "volume_ratio_5d": row.get("volume_ratio_5d", np.nan),
                "price_range_5d": row.get("price_range_5d", np.nan),
                "weekday": row.get("weekday", np.nan),
                "nk225_ret_5d": row.get("nk225_ret_5d", np.nan),
                "nk225_vol_5d": row.get("nk225_vol_5d", np.nan),
                "nk225_ma5_dev": row.get("nk225_ma5_dev", np.nan),
                "close": row["Close"],
                "sectors": row.get("sectors", ""),
            })

    df = pd.DataFrame(results)
    print(f"  Done in {time.time()-t0:.1f}s: {len(df):,} trades")
    print(f"  Win rate: {df['win'].mean()*100:.1f}%")
    print(f"  SL hit: {(df['exit_type']=='SL').mean()*100:.1f}%")
    print(f"  Total PnL: ¥{df['pnl'].sum():+,}")
    return df


def build_features(df):
    """ML用特徴量を整理"""
    print("[4/6] Building feature matrix...")

    feature_cols = [
        "dev_from_sma20", "sma20_slope",
        "rsi14", "rsi9", "atr14_pct",
        "volatility_5d", "volatility_10d", "volatility_20d",
        "ma5_deviation", "ma25_deviation",
        "prev_day_return", "return_5d", "return_10d",
        "volume_ratio_5d", "price_range_5d",
        "weekday",
        "nk225_ret_5d", "nk225_vol_5d", "nk225_ma5_dev",
        "close",
    ]

    # signal_type をダミー変数化
    df["is_sig_A"] = df["signal_type"].str.contains("A").astype(int)
    df["is_sig_B"] = df["signal_type"].str.contains("B").astype(int)
    feature_cols += ["is_sig_A", "is_sig_B"]

    available = [c for c in feature_cols if c in df.columns]
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        print(f"  Missing features: {missing}")

    X = df[available].copy()
    if "weekday" in X.columns:
        X["weekday"] = X["weekday"].astype("category")

    y = df["win"].astype(int).values
    dates = pd.to_datetime(df["date"]).values

    # 欠損除外
    valid = X.notna().all(axis=1)
    X = X[valid]
    y = y[valid]
    dates = dates[valid]
    df_clean = df[valid].copy()

    print(f"  Features: {len(available)}")
    print(f"  Samples: {len(X):,} (dropped {(~valid).sum():,} NaN rows)")
    print(f"  Target: Win={y.sum():,} ({y.mean()*100:.1f}%), Lose={len(y)-y.sum():,}")
    return X, y, dates, available, df_clean


def train_walk_forward(X, y, dates, feature_names):
    """時系列Walk-Forward CVで学習・評価"""
    print("[5/6] Training with Walk-Forward CV...")
    t0 = time.time()

    params = {
        "objective": "binary",
        "metric": "auc",
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "n_estimators": 200,
        "random_state": 42,
    }

    cat_features = ["weekday"] if "weekday" in feature_names else []

    # 月単位でグループ化（日足26年→週だと多すぎるので月単位）
    months = pd.to_datetime(dates).to_period("M")
    unique_months = months.unique().sort_values()
    min_train_months = 24  # 最低2年分のトレーニングデータ

    all_preds = []
    all_true = []
    all_dates = []
    auc_scores = []

    print(f"  Total months: {len(unique_months)}")
    print(f"  Min train months: {min_train_months}")
    print(f"  Test months: {len(unique_months) - min_train_months}")

    for i in range(min_train_months, len(unique_months)):
        test_month = unique_months[i]
        train_months = unique_months[:i]
        train_mask = np.isin(months, train_months)
        test_mask = months == test_month

        if train_mask.sum() < 100 or test_mask.sum() == 0:
            continue

        X_train, y_train = X[train_mask], y[train_mask]
        X_test, y_test = X[test_mask], y[test_mask]

        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train, categorical_feature=cat_features if cat_features else "auto")

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        all_preds.extend(y_pred_proba)
        all_true.extend(y_test)
        all_dates.extend(dates[test_mask])

        if len(np.unique(y_test)) > 1:
            auc = roc_auc_score(y_test, y_pred_proba)
            auc_scores.append(auc)

    all_preds = np.array(all_preds)
    all_true = np.array(all_true)
    all_dates = np.array(all_dates)

    # 全体評価
    overall_auc = roc_auc_score(all_true, all_preds)
    y_pred = (all_preds >= 0.5).astype(int)
    overall_acc = accuracy_score(all_true, y_pred)

    print(f"\n  Walk-Forward CV Results ({time.time()-t0:.1f}s):")
    print(f"  Evaluated: {len(all_true):,} samples")
    print(f"  AUC: {overall_auc:.4f} (std: {np.std(auc_scores):.4f})")
    print(f"  Accuracy: {overall_acc:.4f}")

    # 5分位分析
    quintiles = pd.qcut(all_preds, 5, labels=["Q1(低)", "Q2", "Q3", "Q4", "Q5(高)"], duplicates="drop")
    quintile_results = []

    print(f"\n  [5分位分析（ロング視点）]")
    print(f"  {'分位':<10} {'件数':<8} {'勝率':<10} {'平均PnL':<12}")

    for q in ["Q1(低)", "Q2", "Q3", "Q4", "Q5(高)"]:
        mask = quintiles == q
        if mask.sum() == 0:
            continue
        count = int(mask.sum())
        win_rate = all_true[mask].mean()
        # 概算PnL（勝ちは平均+2%、負けは-3.5%SLが半分として）
        quintile_results.append({
            "quintile": q,
            "count": count,
            "win_rate": float(win_rate),
            "mean_prob": float(all_preds[mask].mean()),
        })
        print(f"  {q:<10} {count:<8} {win_rate*100:<10.1f}% {all_preds[mask].mean():<12.4f}")

    # 閾値別分析
    print(f"\n  [閾値別フィルター効果]")
    print(f"  {'閾値':<12} {'除外数':<8} {'除外率':<8} {'残り勝率':<10} {'除外の勝率':<10}")
    for th in [0.30, 0.35, 0.40, 0.45, 0.50]:
        skip = all_preds < th
        keep = ~skip
        if keep.sum() == 0 or skip.sum() == 0:
            continue
        print(f"  prob<{th:<6.2f} {skip.sum():<8} {skip.mean()*100:<7.1f}% {all_true[keep].mean()*100:<10.1f}% {all_true[skip].mean()*100:<10.1f}%")

    # 最終モデル（全データで学習）
    print(f"\n  Training final model on all {len(X):,} samples...")
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X, y, categorical_feature=cat_features if cat_features else "auto")

    metrics = {
        "auc_mean": float(overall_auc),
        "auc_std": float(np.std(auc_scores)),
        "accuracy": float(overall_acc),
        "total_evaluated": len(all_true),
        "total_samples": len(X),
        "cv_method": "time_series_walk_forward_monthly",
        "min_train_months": min_train_months,
        "quintile_analysis": quintile_results,
    }

    return final_model, metrics, all_preds, all_true, all_dates


def save_model(model, feature_names, metrics):
    """モデル保存"""
    print("[6/6] Saving model...")
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODEL_DIR / "granville_ml_filter.pkl"
    meta_path = MODEL_DIR / "granville_ml_filter_meta.json"

    joblib.dump(model, model_path)
    print(f"  Model: {model_path}")

    meta = {
        "feature_names": feature_names,
        "target": "win (ret_pct > 0)",
        "strategy": "LONG (Granville IFD)",
        "sl_pct": SL_PCT,
        "hold_days": HOLD_DAYS,
        "filters": {
            "market_uptrend": "N225 > SMA20",
            "ci_expand": "CI leading 3m change > 0",
            "bad_sectors_excluded": BAD_SECTORS,
        },
        "metrics": metrics,
        "interpretation": "prob_win高い = 利益予測 = エントリー推奨、prob_win低い = SL予測 = スキップ",
        "n_features": len(feature_names),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"  Meta: {meta_path}")

    # 特徴量重要度
    importance = model.feature_importances_
    sorted_idx = np.argsort(importance)[::-1]
    print(f"\n  [Feature Importance Top 10]")
    for i, idx in enumerate(sorted_idx[:10]):
        print(f"  {i+1}. {feature_names[idx]}: {importance[idx]}")


def main():
    print("=" * 70)
    print("Granville IFD ML Filter Training (26-year daily data)")
    print(f"SL: -{SL_PCT}% / Hold: {HOLD_DAYS} days")
    print("=" * 70)
    t_start = time.time()

    try:
        ps = load_data()
        signals = detect_signals(ps)
        trades = simulate_outcomes(ps, signals)
        X, y, dates, feature_names, df_clean = build_features(trades)
        model, metrics, preds, true, pred_dates = train_walk_forward(X, y, dates, feature_names)
        save_model(model, feature_names, metrics)

        elapsed = time.time() - t_start
        summary = (
            f"Granville ML Filter 学習完了\n"
            f"• データ: {len(trades):,}件 ({pd.to_datetime(dates[0]).date()} ~ {pd.to_datetime(dates[-1]).date()})\n"
            f"• AUC: {metrics['auc_mean']:.4f}\n"
            f"• Accuracy: {metrics['accuracy']:.4f}\n"
            f"• 特徴量: {len(feature_names)}個\n"
            f"• 処理時間: {elapsed:.0f}秒"
        )

        print(f"\n{'='*70}")
        print(summary)
        print(f"{'='*70}")

        notify_slack(summary)

    except Exception as e:
        import traceback
        error_msg = f"Granville ML Filter 学習失敗\n```\n{traceback.format_exc()}\n```"
        print(error_msg)
        notify_slack(error_msg)
        raise


if __name__ == "__main__":
    main()
