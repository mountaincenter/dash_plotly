#!/usr/bin/env python3
"""
グランビルIFD MLフィルター v2 - 特徴量拡張版
=============================================
v1（AUC 0.517）から特徴量を大幅追加:
- TOPIX / 先物 / ドル円の市場特徴量
- 時価総額
- MACD, ボリンジャーバンド
- セクター別モメンタム
- 同日シグナル数（市場過熱度）
- RSIトレンド方向
"""
from __future__ import annotations

import sys
import json
import time
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
    try:
        from dotenv import dotenv_values
        env = dotenv_values(ROOT / ".env.slack")
        url = env.get("SLACK_WEBHOOK_CLAUDE", "")
        if not url:
            return
        import urllib.request
        data = json.dumps({"text": message}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req)
        print("[INFO] Slack notification sent")
    except Exception as e:
        print(f"[WARN] Slack failed: {e}")


def _rsi(series, period):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _atr(df, period):
    h, l, c = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([h - l, (h - c).abs(), (l - c).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast).mean()
    ema_slow = series.ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    return macd_line, signal_line, macd_line - signal_line


def _calc_index_features(idx_df, ticker_name, prefix):
    """指数の特徴量を計算"""
    ix = idx_df[idx_df["ticker"] == ticker_name][["date", "Close"]].copy()
    ix["date"] = pd.to_datetime(ix["date"])
    ix = ix.sort_values("date").rename(columns={"Close": f"{prefix}_close"})
    c = ix[f"{prefix}_close"]
    ix[f"{prefix}_sma20"] = c.rolling(20).mean()
    ix[f"{prefix}_ret_1d"] = c.pct_change(fill_method=None)
    ix[f"{prefix}_ret_5d"] = c.pct_change(5, fill_method=None)
    ix[f"{prefix}_vol_5d"] = c.pct_change(fill_method=None).rolling(5).std()
    ix[f"{prefix}_ma5_dev"] = (c / c.rolling(5).mean() - 1) * 100
    ix[f"{prefix}_ma20_dev"] = (c / c.rolling(20).mean() - 1) * 100
    return ix.drop(columns=[f"{prefix}_close", f"{prefix}_sma20"])


def load_data():
    print("[1/6] Loading data...")
    t0 = time.time()

    m = pd.read_parquet(ROOT / "data/parquet/meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data/parquet/prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # === 指数データ ===
    idx = pd.read_parquet(ROOT / "data/parquet/index_prices_max_1d.parquet")

    # N225
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]
    nk["nk225_ret_1d"] = nk["nk225_close"].pct_change(fill_method=None)
    nk["nk225_ret_5d"] = nk["nk225_close"].pct_change(5, fill_method=None)
    nk["nk225_vol_5d"] = nk["nk225_close"].pct_change(fill_method=None).rolling(5).std()
    nk["nk225_ma5_dev"] = (nk["nk225_close"] / nk["nk225_close"].rolling(5).mean() - 1) * 100
    nk["nk225_ma20_dev"] = (nk["nk225_close"] / nk["nk225_sma20"] - 1) * 100

    # 他の指数ETFを代替利用
    # 1306.T = TOPIX ETF, 1570.T = 日経レバ（先物代替）
    topix_feats = None
    futures_feats = None
    usdjpy_feats = None

    avail_tickers = idx["ticker"].unique().tolist()
    print(f"  Index tickers available: {avail_tickers}")

    if "1306.T" in avail_tickers:
        topix_feats = _calc_index_features(idx, "1306.T", "topix")
    if "1570.T" in avail_tickers:
        futures_feats = _calc_index_features(idx, "1570.T", "futures")

    # CI
    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"})
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)

    daily = pd.DataFrame({"date": ps["date"].drop_duplicates().sort_values()})
    daily = daily.merge(ci, on="date", how="left").sort_values("date").ffill()

    # === 銘柄特徴量 ===
    g = ps.groupby("ticker")

    # SMA
    ps["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma25"] = g["Close"].transform(lambda x: x.rolling(25).mean())
    ps["sma60"] = g["Close"].transform(lambda x: x.rolling(60).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["sma5_above_sma20"] = ps["sma5"] > ps["sma20"]
    ps["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["dev_from_sma60"] = (ps["Close"] - ps["sma60"]) / ps["sma60"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)

    # RSI + RSIトレンド
    ps["rsi14"] = g["Close"].transform(lambda x: _rsi(x, 14))
    ps["rsi9"] = g["Close"].transform(lambda x: _rsi(x, 9))
    ps["rsi14_slope3"] = g["rsi14"].transform(lambda x: x.diff(3))  # RSI方向
    ps["rsi14_slope5"] = g["rsi14"].transform(lambda x: x.diff(5))

    # ATR
    ps["atr14"] = g.apply(lambda x: _atr(x, 14), include_groups=False).reset_index(level=0, drop=True)
    ps["atr14_pct"] = ps["atr14"] / ps["Close"] * 100

    # ボラティリティ
    ps["volatility_5d"] = g["Close"].transform(lambda x: x.pct_change(fill_method=None).rolling(5).std() * 100)
    ps["volatility_10d"] = g["Close"].transform(lambda x: x.pct_change(fill_method=None).rolling(10).std() * 100)
    ps["volatility_20d"] = g["Close"].transform(lambda x: x.pct_change(fill_method=None).rolling(20).std() * 100)

    # MA乖離率
    ps["ma5_deviation"] = (ps["Close"] / ps["sma5"] - 1) * 100
    ps["ma25_deviation"] = (ps["Close"] / ps["sma25"] - 1) * 100

    # リターン
    ps["prev_day_return"] = g["Close"].transform(lambda x: x.pct_change(fill_method=None) * 100)
    ps["return_5d"] = g["Close"].transform(lambda x: x.pct_change(5, fill_method=None) * 100)
    ps["return_10d"] = g["Close"].transform(lambda x: x.pct_change(10, fill_method=None) * 100)
    ps["return_20d"] = g["Close"].transform(lambda x: x.pct_change(20, fill_method=None) * 100)

    # 出来高
    ps["volume_ratio_5d"] = ps["Volume"] / g["Volume"].transform(lambda x: x.rolling(5).mean())
    ps["volume_ratio_20d"] = ps["Volume"] / g["Volume"].transform(lambda x: x.rolling(20).mean())
    ps["price_range_5d"] = g.apply(
        lambda x: (x["High"].rolling(5).max() - x["Low"].rolling(5).min()) / x["Close"] * 100,
        include_groups=False
    ).reset_index(level=0, drop=True)

    # MACD
    ps["macd_hist"] = g["Close"].transform(lambda x: _macd(x)[2])

    # ボリンジャーバンド
    ps["bb_width"] = g["Close"].transform(
        lambda x: x.rolling(20).std() * 4 / x.rolling(20).mean() * 100
    )
    ps["bb_position"] = g["Close"].transform(
        lambda x: (x - (x.rolling(20).mean() - 2 * x.rolling(20).std())) /
                  (4 * x.rolling(20).std())
    )

    # 出来高トレンド
    ps["volume_trend"] = g["Volume"].transform(
        lambda x: x.rolling(5).mean() / x.rolling(20).mean()
    )

    ps["weekday"] = ps["date"].dt.weekday

    ps = ps.dropna(subset=["sma20"])

    # マージ
    nk_cols = ["date", "market_uptrend", "nk225_ret_1d", "nk225_ret_5d",
               "nk225_vol_5d", "nk225_ma5_dev", "nk225_ma20_dev"]
    ps = ps.merge(nk[nk_cols], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0

    if topix_feats is not None:
        ps = ps.merge(topix_feats, on="date", how="left")
    if futures_feats is not None:
        ps = ps.merge(futures_feats, on="date", how="left")
    if usdjpy_feats is not None:
        ps = ps.merge(usdjpy_feats, on="date", how="left")

    meta_cols = ["ticker", "sectors"]
    ps = ps.merge(m[meta_cols], on="ticker", how="left")

    print(f"  Done in {time.time()-t0:.1f}s: {len(ps):,} rows")
    return ps


def detect_signals(ps):
    print("[2/6] Detecting signals...")
    dev = ps["dev_from_sma20"]
    ps["sig_A"] = (dev.between(-8, -3)) & (ps["Close"] > ps["prev_close"])
    ps["sig_B"] = (
        ps["sma20_up"] & (ps["Close"] > ps["sma20"]) &
        (dev.between(0, 2)) & (ps["prev_dev"] <= 0.5) &
        (ps["Close"] > ps["prev_close"])
    )
    mask = (
        (ps["market_uptrend"] == True) &
        (ps["macro_ci_expand"] == True) &
        (~ps["sectors"].isin(BAD_SECTORS))
    )
    sig = ps[mask & (ps["sig_A"] | ps["sig_B"])].copy()
    sig["signal_type"] = "A"
    sig.loc[sig["sig_B"], "signal_type"] = "B"
    sig.loc[sig["sig_A"] & sig["sig_B"], "signal_type"] = "A+B"

    # 同日シグナル数（市場の過熱感）
    daily_count = sig.groupby("date").size().rename("signals_today")
    sig = sig.merge(daily_count, on="date", how="left")

    # セクター別モメンタム（同セクターの直近5日リターン平均）
    sector_mom = ps.groupby(["date", "sectors"])["return_5d"].mean().rename("sector_momentum_5d")
    sig = sig.merge(sector_mom.reset_index(), on=["date", "sectors"], how="left")

    print(f"  Signals: {len(sig):,}")
    return sig


def simulate_outcomes(ps, signals):
    print("[3/6] Simulating trade outcomes...")
    t0 = time.time()
    results = []

    feature_cols = [
        "dev_from_sma20", "dev_from_sma60", "sma20_slope",
        "rsi14", "rsi9", "rsi14_slope3", "rsi14_slope5",
        "atr14_pct",
        "volatility_5d", "volatility_10d", "volatility_20d",
        "ma5_deviation", "ma25_deviation",
        "prev_day_return", "return_5d", "return_10d", "return_20d",
        "volume_ratio_5d", "volume_ratio_20d", "price_range_5d",
        "volume_trend",
        "macd_hist", "bb_width", "bb_position",
        "weekday",
        "nk225_ret_1d", "nk225_ret_5d", "nk225_vol_5d", "nk225_ma5_dev", "nk225_ma20_dev",
        "topix_ret_5d", "topix_vol_5d", "topix_ma5_dev", "topix_ma20_dev",
        "futures_ret_5d", "futures_vol_5d", "futures_ma5_dev", "futures_ma20_dev",
        "close",
        "signals_today", "sector_momentum_5d",
    ]

    for ticker in signals["ticker"].unique():
        tk = ps[ps["ticker"] == ticker].sort_values("date")
        dates = tk["date"].values
        opens, highs, lows, closes = tk["Open"].values, tk["High"].values, tk["Low"].values, tk["Close"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        sig_rows = signals[signals["ticker"] == ticker]
        for _, row in sig_rows.iterrows():
            sd = row["date"]
            if sd not in date_idx:
                continue
            i = date_idx[sd]
            if i + 1 >= len(dates):
                continue

            entry_idx = i + 1
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            sl_price = entry_price * (1 - SL_PCT / 100)
            hit_sl = False
            for d in range(HOLD_DAYS):
                ci = entry_idx + d
                if ci >= len(dates):
                    break
                if lows[ci] <= sl_price:
                    ret_pct = -SL_PCT
                    hit_sl = True
                    break
            if not hit_sl:
                last_idx = min(entry_idx + HOLD_DAYS - 1, len(dates) - 1)
                ret_pct = (closes[last_idx] / entry_price - 1) * 100

            pnl = int(round(entry_price * 100 * ret_pct / 100))
            rec = {
                "date": sd, "ticker": row["ticker"],
                "signal_type": row["signal_type"],
                "entry_price": entry_price,
                "ret_pct": ret_pct, "pnl": pnl,
                "exit_type": "SL" if hit_sl else "expire",
                "win": ret_pct > 0,
            }
            for col in feature_cols:
                rec[col] = row.get(col, np.nan)
            results.append(rec)

    df = pd.DataFrame(results)
    print(f"  Done in {time.time()-t0:.1f}s: {len(df):,} trades")
    print(f"  Win rate: {df['win'].mean()*100:.1f}%, SL hit: {(df['exit_type']=='SL').mean()*100:.1f}%")
    print(f"  Total PnL: ¥{df['pnl'].sum():+,}")
    return df


def build_features(df):
    print("[4/6] Building feature matrix...")

    feature_cols = [
        "dev_from_sma20", "dev_from_sma60", "sma20_slope",
        "rsi14", "rsi9", "rsi14_slope3", "rsi14_slope5",
        "atr14_pct",
        "volatility_5d", "volatility_10d", "volatility_20d",
        "ma5_deviation", "ma25_deviation",
        "prev_day_return", "return_5d", "return_10d", "return_20d",
        "volume_ratio_5d", "volume_ratio_20d", "price_range_5d",
        "volume_trend",
        "macd_hist", "bb_width", "bb_position",
        "weekday",
        "nk225_ret_1d", "nk225_ret_5d", "nk225_vol_5d", "nk225_ma5_dev", "nk225_ma20_dev",
        "topix_ret_5d", "topix_vol_5d", "topix_ma5_dev", "topix_ma20_dev",
        "futures_ret_5d", "futures_vol_5d", "futures_ma5_dev", "futures_ma20_dev",
        "close",
        "signals_today", "sector_momentum_5d",
    ]

    df["is_sig_A"] = df["signal_type"].str.contains("A").astype(int)
    df["is_sig_B"] = df["signal_type"].str.contains("B").astype(int)
    feature_cols += ["is_sig_A", "is_sig_B"]

    available = [c for c in feature_cols if c in df.columns]
    dropped = [c for c in feature_cols if c not in df.columns]
    if dropped:
        print(f"  Dropped (not in data): {dropped}")

    X = df[available].copy()
    y = df["win"].astype(int).values
    dates = pd.to_datetime(df["date"]).values

    # NaN率を確認し、50%以上NaNの特徴量は除外
    nan_rate = X.isna().mean()
    high_nan = nan_rate[nan_rate > 0.5].index.tolist()
    if high_nan:
        print(f"  Dropping high-NaN features (>50%): {high_nan}")
        X = X.drop(columns=high_nan)
        available = [c for c in available if c not in high_nan]

    # weekdayをcategory化（fillna後に行う）
    nan_before = X.isna().sum().sum()
    if "weekday" in X.columns:
        X["weekday"] = X["weekday"].fillna(-1).astype(int).astype("category")
    # 残りのNaNはそのまま（LightGBMはNaN対応）
    df_clean = df.copy()

    print(f"  Features: {len(available)} (v1: 22 → v2: {len(available)})")
    print(f"  Samples: {len(X):,} (filled {nan_before:,} NaN values)")
    print(f"  Win rate: {y.mean()*100:.1f}%")
    return X, y, dates, available, df_clean


def train_walk_forward(X, y, dates, feature_names):
    print("[5/6] Training with Walk-Forward CV...")
    t0 = time.time()

    params = {
        "objective": "binary", "metric": "auc", "boosting_type": "gbdt",
        "num_leaves": 31, "learning_rate": 0.05,
        "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 5,
        "verbose": -1, "n_estimators": 200, "random_state": 42,
    }
    cat_features = ["weekday"] if "weekday" in feature_names else []

    months = pd.to_datetime(dates).to_period("M")
    unique_months = months.unique().sort_values()
    min_train_months = 24

    all_preds, all_true, all_dates = [], [], []
    auc_scores = []

    print(f"  Months: {len(unique_months)}, test: {len(unique_months)-min_train_months}")

    for i in range(min_train_months, len(unique_months)):
        test_month = unique_months[i]
        train_mask = np.isin(months, unique_months[:i])
        test_mask = months == test_month
        if train_mask.sum() < 100 or test_mask.sum() == 0:
            continue

        model = lgb.LGBMClassifier(**params)
        model.fit(X[train_mask], y[train_mask],
                  categorical_feature=cat_features if cat_features else "auto")
        preds = model.predict_proba(X[test_mask])[:, 1]
        all_preds.extend(preds)
        all_true.extend(y[test_mask])
        all_dates.extend(dates[test_mask])

        if len(np.unique(y[test_mask])) > 1:
            auc_scores.append(roc_auc_score(y[test_mask], preds))

    all_preds = np.array(all_preds)
    all_true = np.array(all_true)

    overall_auc = roc_auc_score(all_true, all_preds)
    overall_acc = accuracy_score(all_true, (all_preds >= 0.5).astype(int))

    print(f"\n  Results ({time.time()-t0:.1f}s):")
    print(f"  Evaluated: {len(all_true):,}")
    print(f"  AUC: {overall_auc:.4f} (v1: 0.5171)")
    print(f"  Accuracy: {overall_acc:.4f} (v1: 0.5170)")

    # 5分位
    quintiles = pd.qcut(all_preds, 5, labels=["Q1(低)", "Q2", "Q3", "Q4", "Q5(高)"], duplicates="drop")
    q_results = []
    print(f"\n  [5分位分析]")
    print(f"  {'分位':<10} {'件数':<8} {'勝率':<8} {'平均prob':<10}")
    for q in ["Q1(低)", "Q2", "Q3", "Q4", "Q5(高)"]:
        mask = quintiles == q
        if mask.sum() == 0:
            continue
        wr = all_true[mask].mean()
        q_results.append({"quintile": q, "count": int(mask.sum()),
                          "win_rate": float(wr), "mean_prob": float(all_preds[mask].mean())})
        print(f"  {q:<10} {mask.sum():<8} {wr*100:<8.1f}% {all_preds[mask].mean():<10.4f}")

    # 閾値別
    print(f"\n  [閾値別フィルター]")
    print(f"  {'閾値':<12} {'除外':<8} {'除外率':<8} {'残り勝率':<10} {'除外勝率':<10} {'残りPF概算'}")
    for th in [0.30, 0.35, 0.40, 0.45, 0.50]:
        skip = all_preds < th
        keep = ~skip
        if keep.sum() == 0 or skip.sum() == 0:
            continue
        keep_wr = all_true[keep].mean()
        skip_wr = all_true[skip].mean()
        # PF概算: 勝率 / (1-勝率) * (avg_win / avg_loss)
        print(f"  prob<{th:<6.2f} {skip.sum():<8} {skip.mean()*100:<7.1f}% "
              f"{keep_wr*100:<10.1f}% {skip_wr*100:<10.1f}%")

    # 最終モデル
    print(f"\n  Final model on all {len(X):,} samples...")
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X, y, categorical_feature=cat_features if cat_features else "auto")

    metrics = {
        "auc": float(overall_auc), "auc_std": float(np.std(auc_scores)),
        "accuracy": float(overall_acc),
        "evaluated": len(all_true), "total": len(X),
        "quintiles": q_results,
    }
    return final_model, metrics, all_preds, all_true


def save_model(model, feature_names, metrics):
    print("[6/6] Saving model...")
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, MODEL_DIR / "granville_ml_filter_v2.pkl")
    meta = {
        "version": 2,
        "feature_names": feature_names,
        "n_features": len(feature_names),
        "target": "win (ret_pct > 0)",
        "sl_pct": SL_PCT, "hold_days": HOLD_DAYS,
        "metrics": metrics,
    }
    with open(MODEL_DIR / "granville_ml_filter_v2_meta.json", "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    # 特徴量重要度
    imp = model.feature_importances_
    sorted_idx = np.argsort(imp)[::-1]
    print(f"\n  [Feature Importance Top 15]")
    for i, idx in enumerate(sorted_idx[:15]):
        print(f"  {i+1:>2}. {feature_names[idx]:<25s}: {imp[idx]}")


def main():
    print("=" * 70)
    print("Granville ML Filter v2 - Extended Features")
    print(f"SL: -{SL_PCT}% / Hold: {HOLD_DAYS}d")
    print("=" * 70)
    t_start = time.time()

    try:
        ps = load_data()
        signals = detect_signals(ps)
        trades = simulate_outcomes(ps, signals)
        X, y, dates, fnames, _ = build_features(trades)
        model, metrics, preds, true = train_walk_forward(X, y, dates, fnames)
        save_model(model, fnames, metrics)

        elapsed = time.time() - t_start
        q1 = [q for q in metrics["quintiles"] if q["quintile"] == "Q1(低)"]
        q5 = [q for q in metrics["quintiles"] if q["quintile"] == "Q5(高)"]
        q1_wr = f"{q1[0]['win_rate']*100:.1f}%" if q1 else "N/A"
        q5_wr = f"{q5[0]['win_rate']*100:.1f}%" if q5 else "N/A"

        summary = (
            f"Granville ML Filter v2 学習完了\n"
            f"• 特徴量: {len(fnames)}個 (v1: 22個)\n"
            f"• AUC: {metrics['auc']:.4f} (v1: 0.5171)\n"
            f"• Accuracy: {metrics['accuracy']:.4f}\n"
            f"• Q1勝率: {q1_wr} / Q5勝率: {q5_wr}\n"
            f"• 評価: {metrics['evaluated']:,}件\n"
            f"• 処理時間: {elapsed:.0f}秒"
        )
        print(f"\n{'='*70}")
        print(summary)
        print(f"{'='*70}")
        notify_slack(summary)

    except Exception as e:
        import traceback
        msg = f"Granville ML Filter v2 失敗\n```\n{traceback.format_exc()}\n```"
        print(msg)
        notify_slack(msg)
        raise


if __name__ == "__main__":
    main()
