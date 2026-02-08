#!/usr/bin/env python3
"""
add_ml_prediction_to_grok_trending.py

grok_trending.parquetにML予測（prob_up, quintile）カラムを追加する

処理:
1. grok_trending.parquet を読み込み
2. MLモデルを読み込み
3. 価格データ・市場データから特徴量を計算
4. 各銘柄に対してML予測を実行
5. prob_up, quintile カラムを追加して保存

実行タイミング: 23:00パイプライン（market_cap追加後）
"""

from __future__ import annotations

import sys
from pathlib import Path
import os
import json

import numpy as np
import pandas as pd
import joblib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# パス設定
GROK_TRENDING_FILE = Path(os.getenv(
    "GROK_TRENDING_FILE",
    ROOT / "data" / "parquet" / "grok_trending.parquet"
))
GROK_PRICES_FILE = ROOT / "data" / "parquet" / "grok_prices_max_1d.parquet"
INDEX_PRICES_FILE = ROOT / "data" / "parquet" / "index_prices_max_1d.parquet"
FUTURES_PRICES_FILE = ROOT / "data" / "parquet" / "futures_prices_max_1d.parquet"
CURRENCY_PRICES_FILE = ROOT / "data" / "parquet" / "currency_prices_max_1d.parquet"
ML_MODEL_FILE = ROOT / "models" / "grok_lgbm_model.pkl"
ML_META_FILE = ROOT / "models" / "grok_lgbm_meta.json"


def get_quintile(prob: float) -> str:
    """prob_upから5分位を返す（学習時の分布に基づく近似）"""
    if prob <= 0.32:
        return "Q1"
    elif prob <= 0.40:
        return "Q2"
    elif prob <= 0.48:
        return "Q3"
    elif prob <= 0.55:
        return "Q4"
    else:
        return "Q5"


def load_ml_model():
    """MLモデルとメタ情報を読み込み"""
    if not ML_MODEL_FILE.exists() or not ML_META_FILE.exists():
        print(f"   ⚠️ MLモデルファイルが見つかりません")
        print(f"      model: {ML_MODEL_FILE.exists()}")
        print(f"      meta: {ML_META_FILE.exists()}")
        return None, None

    model = joblib.load(ML_MODEL_FILE)
    with open(ML_META_FILE, 'r') as f:
        meta = json.load(f)

    return model, meta


def load_market_data() -> dict:
    """市場データ（日経、TOPIX、先物、為替）を読み込み"""
    market_data = {}

    if INDEX_PRICES_FILE.exists():
        idx_df = pd.read_parquet(INDEX_PRICES_FILE)
        idx_df['date'] = pd.to_datetime(idx_df['date'])
        for key, ticker in [('nikkei', '^N225'), ('topix', '1306.T')]:
            df = idx_df[idx_df['ticker'] == ticker].copy()
            market_data[key] = df.sort_values('date').reset_index(drop=True)
    else:
        print(f"   ⚠️ index_prices_max_1d.parquet が見つかりません")

    if FUTURES_PRICES_FILE.exists():
        fut_df = pd.read_parquet(FUTURES_PRICES_FILE)
        fut_df['date'] = pd.to_datetime(fut_df['date'])
        market_data['futures'] = fut_df[fut_df['ticker'] == 'NKD=F'].sort_values('date').reset_index(drop=True)
    else:
        print(f"   ⚠️ futures_prices_max_1d.parquet が見つかりません")

    if CURRENCY_PRICES_FILE.exists():
        cur_df = pd.read_parquet(CURRENCY_PRICES_FILE)
        cur_df['date'] = pd.to_datetime(cur_df['date'])
        market_data['usdjpy'] = cur_df[cur_df['ticker'] == 'JPY=X'].sort_values('date').reset_index(drop=True)
    else:
        print(f"   ⚠️ currency_prices_max_1d.parquet が見つかりません")

    return market_data


def calc_market_features(target_date: pd.Timestamp, market_data: dict) -> dict:
    """市場指標の特徴量を計算"""
    features = {}

    for key in ['nikkei', 'topix', 'futures', 'usdjpy']:
        if key not in market_data:
            features[f'{key}_vol_5d'] = np.nan
            features[f'{key}_ret_5d'] = np.nan
            features[f'{key}_ma5_dev'] = np.nan
            continue

        df = market_data[key]
        df_past = df[df['date'] < target_date].tail(30)

        if len(df_past) < 5:
            features[f'{key}_vol_5d'] = np.nan
            features[f'{key}_ret_5d'] = np.nan
            features[f'{key}_ma5_dev'] = np.nan
            continue

        closes = df_past['Close'].values
        returns = np.diff(closes) / closes[:-1]
        features[f'{key}_vol_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan
        features[f'{key}_ret_5d'] = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else np.nan
        ma5 = np.mean(closes[-5:])
        features[f'{key}_ma5_dev'] = (closes[-1] - ma5) / ma5 * 100

    return features


def calc_price_features(ticker: str, target_date: pd.Timestamp, prices_df: pd.DataFrame) -> dict:
    """価格ベース特徴量を計算"""
    ticker_prices = prices_df[
        (prices_df['ticker'] == ticker) &
        (prices_df['date'] < target_date)
    ].sort_values('date').tail(60).dropna(subset=['Close'])

    if len(ticker_prices) < 5:
        return None

    closes = ticker_prices['Close'].values
    volumes = ticker_prices['Volume'].values
    highs = ticker_prices['High'].values
    lows = ticker_prices['Low'].values

    features = {}
    returns = np.diff(closes) / closes[:-1]
    features['volatility_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan
    features['volatility_10d'] = np.std(returns[-10:]) * 100 if len(returns) >= 10 else np.nan
    features['volatility_20d'] = np.std(returns[-20:]) * 100 if len(returns) >= 20 else np.nan

    ma5 = np.mean(closes[-5:])
    ma25 = np.mean(closes[-25:]) if len(closes) >= 25 else np.nan
    features['ma5_deviation'] = (closes[-1] - ma5) / ma5 * 100
    features['ma25_deviation'] = (closes[-1] - ma25) / ma25 * 100 if not np.isnan(ma25) else np.nan
    features['prev_day_return'] = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else np.nan
    features['return_5d'] = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else np.nan
    features['return_10d'] = (closes[-1] - closes[-10]) / closes[-10] * 100 if len(closes) >= 10 else np.nan

    if len(volumes) >= 5:
        avg_vol_5d = np.mean(volumes[-5:])
        features['volume_ratio_5d'] = volumes[-1] / avg_vol_5d if avg_vol_5d > 0 else np.nan
    else:
        features['volume_ratio_5d'] = np.nan

    if len(highs) >= 5 and len(lows) >= 5:
        features['price_range_5d'] = (np.max(highs[-5:]) - np.min(lows[-5:])) / np.min(lows[-5:]) * 100 if np.min(lows[-5:]) > 0 else np.nan
    else:
        features['price_range_5d'] = np.nan

    return features


def predict_ml_for_stocks(grok_df: pd.DataFrame, model, meta: dict, prices_df: pd.DataFrame, market_data: dict) -> dict:
    """grok_dfの各銘柄に対してML予測を実行"""
    if model is None:
        return {}

    feature_names = meta['feature_names']
    target_date = pd.to_datetime(grok_df['date'].iloc[0])
    market_features = calc_market_features(target_date, market_data)

    results = {}

    for _, row in grok_df.iterrows():
        ticker = row['ticker']

        existing_features = {
            'grok_rank': row.get('grok_rank'),
            'selection_score': row.get('selection_score'),
            'buy_price': row.get('Close'),
            'market_cap': row.get('market_cap'),
            'atr14_pct': row.get('atr14_pct'),
            'vol_ratio': row.get('vol_ratio'),
            'rsi9': row.get('rsi9'),
            'weekday': row.get('weekday'),
            'nikkei_change_pct': row.get('nikkei_change_pct'),
            'futures_change_pct': row.get('futures_change_pct'),
            'shortable': 1 if row.get('shortable') else 0,
            'day_trade': 1 if row.get('day_trade') else 0,
        }

        price_features = calc_price_features(ticker, target_date, prices_df)
        if price_features is None:
            results[ticker] = {'prob_up': None, 'quintile': None}
            continue

        all_features = {**existing_features, **price_features, **market_features}

        feature_vector = []
        for fname in feature_names:
            val = all_features.get(fname)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                feature_vector.append(0)
            else:
                feature_vector.append(float(val))

        try:
            X = pd.DataFrame([feature_vector], columns=feature_names)
            if 'weekday' in X.columns:
                X['weekday'] = X['weekday'].astype('category')
            prob = model.predict_proba(X)[0][1]
            results[ticker] = {
                'prob_up': round(float(prob), 3),
                'quintile': get_quintile(prob)
            }
        except Exception as e:
            print(f"   ⚠️ {ticker}: 予測失敗 - {e}")
            results[ticker] = {'prob_up': None, 'quintile': None}

    return results


def main():
    """メイン処理"""
    print("=== grok_trending.parquet に ML予測（prob_up, quintile）を追加 ===\n")

    # 1. grok_trending.parquet 読み込み
    print(f"1. 読み込み: {GROK_TRENDING_FILE}")
    if not GROK_TRENDING_FILE.exists():
        print(f"   エラー: ファイルが見つかりません")
        return 1

    df = pd.read_parquet(GROK_TRENDING_FILE)
    print(f"   銘柄数: {len(df)}")

    # 2. MLモデル読み込み
    print("\n2. MLモデル読み込み")
    model, meta = load_ml_model()
    if model is None:
        print("   ⚠️ MLモデルが読み込めません。prob_up/quintile はNullで追加します。")
        df['prob_up'] = None
        df['quintile'] = None
        df.to_parquet(GROK_TRENDING_FILE, index=False)
        print(f"\n   保存完了（prob_up/quintile = Null）")
        return 0

    print(f"   ✅ モデル読み込み完了")
    print(f"   特徴量数: {len(meta['feature_names'])}")

    # 3. 価格データ読み込み
    print("\n3. 価格データ読み込み")
    if not GROK_PRICES_FILE.exists():
        print(f"   エラー: grok_prices_max_1d.parquet が見つかりません")
        return 1

    prices_df = pd.read_parquet(GROK_PRICES_FILE)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    print(f"   ✅ grok_prices_max_1d: {len(prices_df)} レコード")

    # 4. 市場データ読み込み
    print("\n4. 市場データ読み込み")
    market_data = load_market_data()
    print(f"   ✅ 市場データ: {list(market_data.keys())}")

    # 5. ML予測実行
    print("\n5. ML予測実行中...")
    predictions = predict_ml_for_stocks(df, model, meta, prices_df, market_data)

    # 6. カラム追加
    prob_up_list = []
    quintile_list = []
    success_count = 0

    for _, row in df.iterrows():
        ticker = row['ticker']
        pred = predictions.get(ticker, {})
        prob_up = pred.get('prob_up')
        quintile = pred.get('quintile')
        prob_up_list.append(prob_up)
        quintile_list.append(quintile)
        if prob_up is not None:
            success_count += 1
            print(f"   {ticker}: prob_up={prob_up:.3f}, quintile={quintile}")

    df['prob_up'] = prob_up_list
    df['quintile'] = quintile_list

    print(f"\n6. カラム追加完了")
    print(f"   成功: {success_count}/{len(df)} 銘柄")

    # 7. 保存
    print(f"\n7. 保存: {GROK_TRENDING_FILE}")
    df.to_parquet(GROK_TRENDING_FILE, index=False)
    print("   ✅ 完了")

    # サマリー
    print(f"\n=== サマリー ===")
    valid_probs = df[df['prob_up'].notna()]['prob_up']
    if len(valid_probs) > 0:
        print(f"prob_up 平均: {valid_probs.mean():.3f}")
        print(f"prob_up 中央値: {valid_probs.median():.3f}")
        quintile_counts = df['quintile'].value_counts()
        print(f"Quintile分布:")
        for q in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
            count = quintile_counts.get(q, 0)
            print(f"  {q}: {count}銘柄")

    return 0


if __name__ == "__main__":
    sys.exit(main())
