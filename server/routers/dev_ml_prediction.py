"""
開発用: ML予測API

grok_trending.parquetの銘柄に対して騰落確率を予測
- GET /dev/ml/prediction: 当日銘柄の騰落確率を返す

データソース:
- 日足: grok_prices_max_1d.parquet
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
import joblib
import json
from datetime import datetime
import os
import tempfile

router = APIRouter()

# パス設定
BASE_DIR = Path(__file__).resolve().parents[2]
PARQUET_DIR = BASE_DIR / "data" / "parquet"
MODEL_DIR = BASE_DIR / "models"

GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
GROK_PRICES_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"
MODEL_PATH = MODEL_DIR / "grok_lgbm_model.pkl"
META_PATH = MODEL_DIR / "grok_lgbm_meta.json"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_model():
    """モデルとメタ情報を読み込み"""
    if not MODEL_PATH.exists():
        raise HTTPException(status_code=500, detail="モデルファイルが見つかりません")

    model = joblib.load(MODEL_PATH)

    with open(META_PATH, 'r') as f:
        meta = json.load(f)

    return model, meta


def load_grok_trending() -> pd.DataFrame:
    """grok_trending.parquetを読み込み"""
    if GROK_TRENDING_PATH.exists():
        return pd.read_parquet(GROK_TRENDING_PATH)

    # S3から取得
    try:
        import boto3
        s3_client = boto3.client("s3", region_name=AWS_REGION)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            s3_client.download_fileobj(S3_BUCKET, "parquet/grok_trending.parquet", tmp)
            tmp_path = tmp.name

        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"grok_trending読み込みエラー: {str(e)}")


def load_prices() -> pd.DataFrame:
    """日足データを読み込み"""
    if GROK_PRICES_PATH.exists():
        df = pd.read_parquet(GROK_PRICES_PATH)
        df['date'] = pd.to_datetime(df['date'])
        return df

    # S3から取得
    try:
        import boto3
        s3_client = boto3.client("s3", region_name=AWS_REGION)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            s3_client.download_fileobj(S3_BUCKET, "parquet/grok_prices_max_1d.parquet", tmp)
            tmp_path = tmp.name

        df = pd.read_parquet(tmp_path)
        df['date'] = pd.to_datetime(df['date'])
        os.unlink(tmp_path)
        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"価格データ読み込みエラー: {str(e)}")


def calc_price_features(ticker: str, target_date: pd.Timestamp, prices_df: pd.DataFrame) -> dict:
    """価格ベース特徴量を計算"""
    ticker_prices = prices_df[
        (prices_df['ticker'] == ticker) &
        (prices_df['date'] < target_date)
    ].sort_values('date').tail(60)

    # NaN行を除外
    ticker_prices = ticker_prices.dropna(subset=['Close'])

    if len(ticker_prices) < 5:
        return None

    closes = ticker_prices['Close'].values
    volumes = ticker_prices['Volume'].values
    highs = ticker_prices['High'].values
    lows = ticker_prices['Low'].values

    features = {}

    # ボラティリティ
    returns = np.diff(closes) / closes[:-1]
    features['volatility_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan
    features['volatility_10d'] = np.std(returns[-10:]) * 100 if len(returns) >= 10 else np.nan
    features['volatility_20d'] = np.std(returns[-20:]) * 100 if len(returns) >= 20 else np.nan

    # 移動平均乖離率
    ma5 = np.mean(closes[-5:])
    ma25 = np.mean(closes[-25:]) if len(closes) >= 25 else np.nan
    prev_close = closes[-1]

    features['ma5_deviation'] = (prev_close - ma5) / ma5 * 100
    features['ma25_deviation'] = (prev_close - ma25) / ma25 * 100 if not np.isnan(ma25) else np.nan

    # リターン
    if len(closes) >= 2:
        features['prev_day_return'] = (closes[-1] - closes[-2]) / closes[-2] * 100
    else:
        features['prev_day_return'] = np.nan

    features['return_5d'] = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else np.nan
    features['return_10d'] = (closes[-1] - closes[-10]) / closes[-10] * 100 if len(closes) >= 10 else np.nan

    # 出来高比
    if len(volumes) >= 5:
        avg_vol_5d = np.mean(volumes[-5:])
        features['volume_ratio_5d'] = volumes[-1] / avg_vol_5d if avg_vol_5d > 0 else np.nan
    else:
        features['volume_ratio_5d'] = np.nan

    # 価格レンジ
    if len(highs) >= 5 and len(lows) >= 5:
        high_5d = np.max(highs[-5:])
        low_5d = np.min(lows[-5:])
        features['price_range_5d'] = (high_5d - low_5d) / low_5d * 100 if low_5d > 0 else np.nan
    else:
        features['price_range_5d'] = np.nan

    return features


def get_confidence_level(prob: float) -> str:
    """確率からconfidence levelを返す"""
    if prob >= 0.65:
        return "high"
    elif prob >= 0.55:
        return "medium"
    else:
        return "low"


@router.get("/dev/ml/prediction")
async def get_ml_prediction():
    """
    当日銘柄の騰落確率を予測

    Returns:
        date: 対象日付
        model_auc: モデルのAUC
        predictions: 各銘柄の予測結果リスト
    """
    # モデル読み込み
    model, meta = load_model()
    feature_names = meta['feature_names']

    # データ読み込み
    grok_df = load_grok_trending()
    prices_df = load_prices()

    if grok_df.empty:
        raise HTTPException(status_code=404, detail="grok_trendingにデータがありません")

    # 対象日付
    target_date = pd.to_datetime(grok_df['date'].iloc[0])

    predictions = []

    for _, row in grok_df.iterrows():
        ticker = row['ticker']
        stock_name = row.get('stock_name', '')

        # 既存特徴量
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

        # 価格ベース特徴量
        price_features = calc_price_features(ticker, target_date, prices_df)

        if price_features is None:
            predictions.append({
                'ticker': ticker,
                'stock_name': stock_name,
                'prob_up': None,
                'confidence': 'unknown',
                'error': '価格データ不足'
            })
            continue

        # 全特徴量を結合
        all_features = {**existing_features, **price_features}

        # 特徴量ベクトル作成
        feature_vector = []
        missing_features = []

        for fname in feature_names:
            val = all_features.get(fname)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                missing_features.append(fname)
                feature_vector.append(0)
            else:
                feature_vector.append(float(val))

        # 予測
        try:
            prob = model.predict_proba([feature_vector])[0][1]
            predictions.append({
                'ticker': ticker,
                'stock_name': stock_name,
                'prob_up': round(float(prob), 3),
                'confidence': get_confidence_level(prob),
                'missing_features': missing_features if missing_features else None
            })
        except Exception as e:
            predictions.append({
                'ticker': ticker,
                'stock_name': stock_name,
                'prob_up': None,
                'confidence': 'error',
                'error': str(e)
            })

    # 確率でソート
    predictions.sort(key=lambda x: x.get('prob_up') or 0, reverse=True)

    return JSONResponse(content={
        'date': target_date.strftime('%Y-%m-%d'),
        'model_auc': meta['metrics']['auc_mean'],
        'total_stocks': len(grok_df),
        'predicted_stocks': len([p for p in predictions if p.get('prob_up') is not None]),
        'predictions': predictions,
        'generated_at': datetime.now().isoformat()
    })
