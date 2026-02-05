"""
開発用: ML予測API

grok_trending.parquetの銘柄に対して騰落確率を予測
- GET /dev/ml/prediction: 当日銘柄の騰落確率を返す

データソース:
- 日足: grok_prices_max_1d.parquet
- 市場: index_prices_max_1d.parquet, futures_prices_max_1d.parquet, currency_prices_max_1d.parquet

=== 重要: prob_up の解釈（ショート戦略） ===

【モデルの出力】
- prob_up = 株価上昇確率（ロング基準で計算）
- phase2_win = 終値 > 始値（ロング利益）

【ショート戦略での使い方】
- prob_up 高い → 株価上昇予測 → ショート損失リスク高 → 避ける
- prob_up 低い → 株価下落予測 → ショート利益期待 → 推奨

【実運用】
- prob_up下位の銘柄をショート対象として選定
- prob_up上位の銘柄はショート回避
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
INDEX_PRICES_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
FUTURES_PRICES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"
CURRENCY_PRICES_PATH = PARQUET_DIR / "currency_prices_max_1d.parquet"
MODEL_PATH = MODEL_DIR / "grok_lgbm_model.pkl"
META_PATH = MODEL_DIR / "grok_lgbm_meta.json"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_model():
    """モデルとメタ情報を読み込み（ローカル優先、S3フォールバック）"""
    if not MODEL_PATH.exists() or not META_PATH.exists():
        try:
            import boto3
            s3_client = boto3.client("s3", region_name=AWS_REGION)
            MODEL_DIR.mkdir(parents=True, exist_ok=True)

            if not MODEL_PATH.exists():
                s3_client.download_file(S3_BUCKET, "parquet/ml/grok_lgbm_model.pkl", str(MODEL_PATH))
            if not META_PATH.exists():
                s3_client.download_file(S3_BUCKET, "parquet/ml/grok_lgbm_meta.json", str(META_PATH))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"モデル読み込みエラー: {str(e)}")

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


def load_market_data() -> dict:
    """市場データ（日経、TOPIX、先物、為替）を読み込み"""
    market_data = {}

    # Index (日経225, TOPIX ETF)
    if INDEX_PRICES_PATH.exists():
        idx_df = pd.read_parquet(INDEX_PRICES_PATH)
        idx_df['date'] = pd.to_datetime(idx_df['date'])

        for key, ticker in [('nikkei', '^N225'), ('topix', '1306.T')]:
            df = idx_df[idx_df['ticker'] == ticker].copy()
            df = df.sort_values('date').reset_index(drop=True)
            market_data[key] = df

    # Futures (日経先物)
    if FUTURES_PRICES_PATH.exists():
        fut_df = pd.read_parquet(FUTURES_PRICES_PATH)
        fut_df['date'] = pd.to_datetime(fut_df['date'])
        df = fut_df[fut_df['ticker'] == 'NKD=F'].copy()
        df = df.sort_values('date').reset_index(drop=True)
        market_data['futures'] = df

    # Currency (ドル円)
    if CURRENCY_PRICES_PATH.exists():
        cur_df = pd.read_parquet(CURRENCY_PRICES_PATH)
        cur_df['date'] = pd.to_datetime(cur_df['date'])
        df = cur_df[cur_df['ticker'] == 'JPY=X'].copy()
        df = df.sort_values('date').reset_index(drop=True)
        market_data['usdjpy'] = df

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

        # 5日ボラティリティ
        returns = np.diff(closes) / closes[:-1]
        features[f'{key}_vol_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan

        # 5日リターン
        features[f'{key}_ret_5d'] = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else np.nan

        # MA5乖離率
        ma5 = np.mean(closes[-5:])
        features[f'{key}_ma5_dev'] = (closes[-1] - ma5) / ma5 * 100

    return features


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


def get_short_recommendation(prob: float, threshold: float = 0.40) -> dict:
    """
    prob_upからショート推奨度を判定

    Args:
        prob: 株価上昇確率（ロング基準）
        threshold: ショート推奨閾値（デフォルト0.40）

    Returns:
        recommendation: ショート推奨度
        reason: 理由
    """
    if prob <= 0.25:
        return {"recommendation": "strong_short", "reason": "株価下落確率が非常に高い"}
    elif prob <= threshold:
        return {"recommendation": "short", "reason": "株価下落確率が高い"}
    elif prob <= 0.55:
        return {"recommendation": "neutral", "reason": "方向性が不明確"}
    else:
        return {"recommendation": "avoid", "reason": "株価上昇予測のためショート回避"}


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
    market_data = load_market_data()

    if grok_df.empty:
        raise HTTPException(status_code=404, detail="grok_trendingにデータがありません")

    # 対象日付
    target_date = pd.to_datetime(grok_df['date'].iloc[0])

    # 市場特徴量（日付ごとに同じなので1回だけ計算）
    market_features = calc_market_features(target_date, market_data)

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

        # 全特徴量を結合（既存 + 価格 + 市場）
        all_features = {**existing_features, **price_features, **market_features}

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
            short_rec = get_short_recommendation(prob)
            predictions.append({
                'ticker': ticker,
                'stock_name': stock_name,
                'prob_up': round(float(prob), 3),
                'short_recommendation': short_rec['recommendation'],
                'short_reason': short_rec['reason'],
                'missing_features': missing_features if missing_features else None
            })
        except Exception as e:
            predictions.append({
                'ticker': ticker,
                'stock_name': stock_name,
                'prob_up': None,
                'short_recommendation': 'error',
                'error': str(e)
            })

    # ショート推奨順にソート（prob_up低い順 = ショート推奨順）
    predictions.sort(key=lambda x: x.get('prob_up') or 1.0)

    # 推奨銘柄数をカウント
    short_recommended = len([p for p in predictions if p.get('short_recommendation') in ['strong_short', 'short']])

    return JSONResponse(content={
        'date': target_date.strftime('%Y-%m-%d'),
        'model_auc': meta['metrics']['auc_mean'],
        'total_stocks': len(grok_df),
        'predicted_stocks': len([p for p in predictions if p.get('prob_up') is not None]),
        'short_recommended': short_recommended,
        'strategy': 'SHORT',
        'threshold': 0.40,
        'predictions': predictions,
        'generated_at': datetime.now().isoformat()
    })
