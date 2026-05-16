"""
開発用: デイトレードリスト管理API

grok_trending.parquet と grok_day_trade_list.parquet を使用
- GET /dev/day-trade-list: 一覧取得（grok_trending.parquetから、ML予測含む）
- PUT /dev/day-trade-list/{ticker}: 個別銘柄のフラグ更新
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
import tempfile
import os
import io
import joblib
import json

router = APIRouter()

# キャッシュ（モジュールレベル）
_ml_model_cache = {"model": None, "meta": None, "loaded": False}
_prices_cache = {"df": None, "loaded": False}

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
DAY_TRADE_LIST_PATH = BASE_DIR / "data" / "parquet" / "grok_day_trade_list.parquet"
GROK_TRENDING_PATH = BASE_DIR / "data" / "parquet" / "grok_trending.parquet"
GROK_ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
GROK_PRICES_PATH = BASE_DIR / "data" / "parquet" / "grok_prices_max_1d.parquet"
PRICES_FALLBACK_PATH = BASE_DIR / "data" / "parquet" / "prices_max_1d.parquet"
ML_MODEL_PATH = BASE_DIR / "models" / "grok_lgbm_model.pkl"
ML_META_PATH = BASE_DIR / "models" / "grok_lgbm_meta.json"
INDEX_PRICES_PATH = BASE_DIR / "data" / "parquet" / "index_prices_max_1d.parquet"
FUTURES_PRICES_PATH = BASE_DIR / "data" / "parquet" / "futures_prices_max_1d.parquet"
CURRENCY_PRICES_PATH = BASE_DIR / "data" / "parquet" / "currency_prices_max_1d.parquet"
LOCAL_MANUAL_LOG_DIR = BASE_DIR / "data" / "manual_edit_logs"
LOCAL_MANUAL_BACKUP_DIR = BASE_DIR / "data" / "manual_backups"

# 曜日名
WEEKDAY_NAMES = ['月', '火', '水', '木', '金', '土', '日']
MANUAL_FIELDS = {
    "shortable",
    "day_trade",
    "ng",
    "day_trade_available_shares",
    "margin_sell_balance",
    "margin_buy_balance",
}
BOOL_MANUAL_FIELDS = {"shortable", "day_trade", "ng"}
NUMERIC_MANUAL_FIELDS = {"day_trade_available_shares", "margin_sell_balance", "margin_buy_balance"}


def _storage_mode() -> str:
    mode = os.getenv("DATA_SOURCE_MODE") or os.getenv("STORAGE_MODE")
    if not mode:
        env = (os.getenv("ENV") or os.getenv("APP_ENV") or os.getenv("NODE_ENV") or "").lower()
        mode = "s3" if env in {"production", "staging"} else "local"
    mode = mode.lower()
    if mode not in {"local", "s3"}:
        raise HTTPException(status_code=500, detail=f"Invalid DATA_SOURCE_MODE: {mode}")
    return mode


def _read_s3_parquet(name: str) -> pd.DataFrame:
    import boto3
    from botocore.exceptions import ClientError

    bucket, _, region = _s3_settings()
    key = _s3_key(name)
    s3_client = boto3.client("s3", region_name=region)

    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            s3_client.download_fileobj(bucket, key, tmp_file)
            tmp_path = tmp_file.name

        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        return df
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"{name} がS3に見つかりません")
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")


def calc_price_limit(price: float) -> int:
    """制限値幅を計算"""
    if price < 100:
        return 30
    elif price < 200:
        return 50
    elif price < 500:
        return 80
    elif price < 700:
        return 100
    elif price < 1000:
        return 150
    elif price < 1500:
        return 300
    elif price < 2000:
        return 400
    elif price < 3000:
        return 500
    elif price < 5000:
        return 700
    elif price < 7000:
        return 1000
    elif price < 10000:
        return 1500
    elif price < 15000:
        return 3000
    elif price < 20000:
        return 4000
    elif price < 30000:
        return 5000
    elif price < 50000:
        return 7000
    elif price < 70000:
        return 10000
    elif price < 100000:
        return 15000
    elif price < 150000:
        return 30000
    elif price < 200000:
        return 40000
    elif price < 300000:
        return 50000
    elif price < 500000:
        return 70000
    elif price < 700000:
        return 100000
    elif price < 1000000:
        return 150000
    elif price < 1500000:
        return 300000
    elif price < 2000000:
        return 400000
    elif price < 3000000:
        return 500000
    elif price < 5000000:
        return 700000
    elif price < 7000000:
        return 1000000
    elif price < 10000000:
        return 1500000
    elif price < 15000000:
        return 3000000
    elif price < 20000000:
        return 4000000
    elif price < 30000000:
        return 5000000
    elif price < 50000000:
        return 7000000
    else:
        return 10000000


def calc_stop_flags(prices_df: pd.DataFrame) -> dict:
    """
    ストップ高/安フラグを計算してdictで返す

    Returns:
        dict: {ticker: {'is_stop_high': bool, 'is_stop_low': bool}}
    """
    prices_df = prices_df.sort_values(['ticker', 'date'])

    # 前日・前々日終値
    prices_df['prev_close'] = prices_df.groupby('ticker')['Close'].shift(1)
    prices_df['prev_prev_close'] = prices_df.groupby('ticker')['Close'].shift(2)

    # 制限値幅（前々日終値ベース）
    prices_df['price_limit'] = prices_df['prev_prev_close'].apply(
        lambda x: calc_price_limit(x) if pd.notna(x) else None
    )

    # ストップ高/安判定
    prices_df['is_stop_high'] = (
        prices_df['prev_close'] >= prices_df['prev_prev_close'] + prices_df['price_limit']
    )
    prices_df['is_stop_low'] = (
        prices_df['prev_close'] <= prices_df['prev_prev_close'] - prices_df['price_limit']
    )

    # 最新日付のデータのみ取得
    latest_date = prices_df['date'].max()
    latest_df = prices_df[prices_df['date'] == latest_date]

    return {
        row['ticker']: {
            'is_stop_high': bool(row['is_stop_high']) if pd.notna(row['is_stop_high']) else False,
            'is_stop_low': bool(row['is_stop_low']) if pd.notna(row['is_stop_low']) else False
        }
        for _, row in latest_df.iterrows()
    }


def load_grok_prices() -> pd.DataFrame:
    """grok_prices_max_1d.parquet + prices_max_1d.parquet（フォールバック）を読み込み"""
    global _prices_cache
    if _prices_cache["loaded"]:
        return _prices_cache["df"]

    dfs = []

    # メイン: grok_prices_max_1d.parquet
    if GROK_PRICES_PATH.exists():
        df = pd.read_parquet(GROK_PRICES_PATH)
        if 'date' in df.columns and df['date'].dtype == 'object':
            df['date'] = df['date'].str.replace(r'\+\d{2}:\d{2}$', '', regex=True)
            df['date'] = pd.to_datetime(df['date'], format='mixed')
        dfs.append(df)

    # フォールバック: prices_max_1d.parquet（grokにない銘柄用）
    if PRICES_FALLBACK_PATH.exists():
        df_fb = pd.read_parquet(PRICES_FALLBACK_PATH)
        if 'date' in df_fb.columns and df_fb['date'].dtype == 'object':
            df_fb['date'] = df_fb['date'].str.replace(r'\+\d{2}:\d{2}$', '', regex=True)
            df_fb['date'] = pd.to_datetime(df_fb['date'], format='mixed')
        dfs.append(df_fb)

    if dfs:
        combined = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['ticker', 'date'])
        _prices_cache["df"] = combined
        _prices_cache["loaded"] = True
        return combined

    return pd.DataFrame()


def load_ml_model():
    """MLモデルとメタ情報を読み込み（キャッシュ付き、S3フォールバック）"""
    global _ml_model_cache
    if _ml_model_cache["loaded"]:
        return _ml_model_cache["model"], _ml_model_cache["meta"]

    if not ML_MODEL_PATH.exists() or not ML_META_PATH.exists():
        try:
            import boto3
            s3_bucket = os.getenv("S3_BUCKET", "stock-api-data")
            s3_region = os.getenv("AWS_REGION", "ap-northeast-1")
            s3_client = boto3.client("s3", region_name=s3_region)
            ML_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

            if not ML_MODEL_PATH.exists():
                s3_client.download_file(s3_bucket, "parquet/ml/grok_lgbm_model.pkl", str(ML_MODEL_PATH))
            if not ML_META_PATH.exists():
                s3_client.download_file(s3_bucket, "parquet/ml/grok_lgbm_meta.json", str(ML_META_PATH))
        except Exception:
            return None, None

    if not ML_MODEL_PATH.exists() or not ML_META_PATH.exists():
        return None, None

    model = joblib.load(ML_MODEL_PATH)
    with open(ML_META_PATH, 'r') as f:
        meta = json.load(f)

    _ml_model_cache["model"] = model
    _ml_model_cache["meta"] = meta
    _ml_model_cache["loaded"] = True
    return model, meta


def load_market_data() -> dict:
    """市場データ（日経、TOPIX、先物、為替）を読み込み"""
    market_data = {}

    if INDEX_PRICES_PATH.exists():
        idx_df = pd.read_parquet(INDEX_PRICES_PATH)
        idx_df['date'] = pd.to_datetime(idx_df['date'])
        for key, ticker in [('nikkei', '^N225'), ('topix', '1306.T')]:
            df = idx_df[idx_df['ticker'] == ticker].copy()
            market_data[key] = df.sort_values('date').reset_index(drop=True)

    if FUTURES_PRICES_PATH.exists():
        fut_df = pd.read_parquet(FUTURES_PRICES_PATH)
        fut_df['date'] = pd.to_datetime(fut_df['date'])
        market_data['futures'] = fut_df[fut_df['ticker'] == 'NKD=F'].sort_values('date').reset_index(drop=True)

    if CURRENCY_PRICES_PATH.exists():
        cur_df = pd.read_parquet(CURRENCY_PRICES_PATH)
        cur_df['date'] = pd.to_datetime(cur_df['date'])
        market_data['usdjpy'] = cur_df[cur_df['ticker'] == 'JPY=X'].sort_values('date').reset_index(drop=True)

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


def calc_price_features(ticker: str, target_date: pd.Timestamp, prices_df: pd.DataFrame, buy_price: float = None) -> dict:
    """価格ベース特徴量を計算（28特徴量対応）"""
    ticker_prices = prices_df[
        (prices_df['ticker'] == ticker) &
        (prices_df['date'] < target_date)
    ].sort_values('date').tail(60).dropna(subset=['Close'])

    if len(ticker_prices) < 5:
        return None

    closes = ticker_prices['Close'].values
    opens = ticker_prices['Open'].values
    volumes = ticker_prices['Volume'].values
    highs = ticker_prices['High'].values
    lows = ticker_prices['Low'].values

    features = {}
    returns = np.diff(closes) / closes[:-1]
    features['volatility_5d'] = np.std(returns[-5:]) * 100 if len(returns) >= 5 else np.nan

    ma5 = np.mean(closes[-5:])
    ma25 = np.mean(closes[-25:]) if len(closes) >= 25 else np.nan
    features['ma5_deviation'] = (closes[-1] - ma5) / ma5 * 100
    features['ma25_deviation'] = (closes[-1] - ma25) / ma25 * 100 if not np.isnan(ma25) else np.nan
    features['prev_day_return'] = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else np.nan

    if len(volumes) >= 5:
        avg_vol_5d = np.mean(volumes[-5:])
        features['volume_ratio_5d'] = volumes[-1] / avg_vol_5d if avg_vol_5d > 0 else np.nan
    else:
        features['volume_ratio_5d'] = np.nan

    if len(highs) >= 5 and len(lows) >= 5:
        features['price_range_5d'] = (np.max(highs[-5:]) - np.min(lows[-5:])) / np.min(lows[-5:]) * 100 if np.min(lows[-5:]) > 0 else np.nan
    else:
        features['price_range_5d'] = np.nan

    # 前日OHLCV特徴量
    prev_range = highs[-1] - lows[-1]
    features['prev_close_position'] = (closes[-1] - lows[-1]) / prev_range if prev_range > 0 else 0.5
    features['gap_ratio'] = (buy_price - closes[-1]) / closes[-1] if closes[-1] > 0 and buy_price else 0
    features['prev_candle'] = (closes[-1] - opens[-1]) / opens[-1] if opens[-1] > 0 else 0

    # MACD Histogram
    close_s = pd.Series(closes)
    ema12 = close_s.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = close_s.ewm(span=26, adjust=False, min_periods=26).mean()
    macd_line = ema12 - ema26
    signal = macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
    hist = macd_line - signal
    features['macd_hist'] = float(hist.iloc[-1]) if not np.isnan(hist.iloc[-1]) else np.nan

    # Bollinger Bands %B
    if len(closes) >= 20:
        ma20 = close_s.rolling(20, min_periods=20).mean()
        sd20 = close_s.rolling(20, min_periods=20).std(ddof=0)
        upper = ma20 + 2.0 * sd20
        lower = ma20 - 2.0 * sd20
        bb_range = (upper - lower).replace(0, np.nan)
        pctb = (close_s - lower) / bb_range
        features['bb_pctb'] = float(pctb.iloc[-1]) if not np.isnan(pctb.iloc[-1]) else np.nan
    else:
        features['bb_pctb'] = np.nan

    # Volume Trend
    if len(volumes) >= 20:
        vol_s = pd.Series(volumes.astype(float))
        vol_ma5 = vol_s.rolling(5).mean()
        vol_ma20 = vol_s.rolling(20).mean()
        vt = vol_ma5 / vol_ma20.replace(0, np.nan)
        features['vol_trend'] = float(vt.iloc[-1]) if not np.isnan(vt.iloc[-1]) else np.nan
    else:
        features['vol_trend'] = np.nan

    return features


def get_grade(prob: float, boundaries: list[float]) -> str:
    """prob_upからGrade (G1-G4) を返す"""
    for i, b in enumerate(boundaries):
        if prob <= b:
            return f"G{i + 1}"
    return f"G{len(boundaries)}"


def predict_ml_for_stocks(grok_df: pd.DataFrame, model, meta: dict, prices_df: pd.DataFrame) -> dict:
    """grok_dfの各銘柄に対してML予測を実行（28特徴量/4クラスGrade方式）"""
    if model is None:
        return {}

    feature_names = meta['feature_names']
    grade_boundaries = meta.get('grade_boundaries', [0.25, 0.40, 0.55, 1.0])
    target_date = pd.to_datetime(grok_df['date'].iloc[0])
    market_data = load_market_data()
    market_features = calc_market_features(target_date, market_data)

    results = {}

    for _, row in grok_df.iterrows():
        ticker = row['ticker']
        close_price = row.get('Close')

        existing_features = {
            'grok_rank': row.get('grok_rank'),
            'selection_score': row.get('selection_score'),
            'buy_price': close_price,
            'market_cap': row.get('market_cap'),
            'atr14_pct': row.get('atr14_pct'),
            'vol_ratio': row.get('vol_ratio'),
            'rsi9': row.get('rsi9'),
            'nikkei_change_pct': row.get('nikkei_change_pct'),
            'futures_change_pct': row.get('futures_change_pct'),
        }

        price_features = calc_price_features(ticker, target_date, prices_df, buy_price=close_price)
        if price_features is None:
            results[ticker] = {'prob_up': None, 'grade': None}
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
            prob = model.predict_proba(X)[0][1]
            results[ticker] = {
                'prob_up': round(float(prob), 3),
                'grade': get_grade(prob, grade_boundaries)
            }
        except Exception:
            results[ticker] = {'prob_up': None, 'grade': None}

    return results


def load_day_trade_list() -> pd.DataFrame:
    """ローカルまたはS3からデイトレードリストを読み込み"""
    if _storage_mode() == "s3":
        return _read_s3_parquet("grok_day_trade_list.parquet")
    if not DAY_TRADE_LIST_PATH.exists():
        raise HTTPException(status_code=404, detail="ローカル grok_day_trade_list.parquet が見つかりません")
    return pd.read_parquet(DAY_TRADE_LIST_PATH)


def _s3_settings() -> tuple[str, str, str]:
    bucket = os.getenv("S3_BUCKET", "stock-api-data")
    prefix = os.getenv("S3_PREFIX", "parquet/").strip("/")
    region = os.getenv("AWS_REGION", "ap-northeast-1")
    return bucket, prefix, region


def _s3_key(name: str) -> str:
    _, prefix, _ = _s3_settings()
    return f"{prefix}/{name}" if prefix else name


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _backup_s3_object(s3_client, bucket: str, source_key: str, backup_name: str) -> str | None:
    backup_key = _s3_key(f"manual_backups/{backup_name}_{_now_stamp()}.parquet")
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=backup_key,
        )
        return backup_key
    except Exception as e:
        print(f"Warning: S3 backup skipped for {source_key}: {e}")
        return None


def _backup_local_file(path: Path, backup_name: str) -> str | None:
    if not path.exists():
        return None
    LOCAL_MANUAL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = LOCAL_MANUAL_BACKUP_DIR / f"{backup_name}_{_now_stamp()}.parquet"
    backup_path.write_bytes(path.read_bytes())
    return str(backup_path.relative_to(BASE_DIR))


def _validate_non_negative(value, field: str, ticker: str) -> None:
    if value is None or pd.isna(value):
        return
    if int(value) < 0:
        raise HTTPException(status_code=400, detail=f"{ticker}: {field} must be >= 0")


def _validate_manual_update(item: dict) -> dict:
    ticker = str(item.get("ticker", "")).strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    invalid = set(item) - MANUAL_FIELDS - {"ticker"}
    if invalid:
        raise HTTPException(status_code=400, detail=f"{ticker}: invalid fields: {sorted(invalid)}")

    cleaned = {"ticker": ticker}
    for field in BOOL_MANUAL_FIELDS:
        if field in item:
            value = item[field]
            if value is not None and not isinstance(value, bool):
                raise HTTPException(status_code=400, detail=f"{ticker}: {field} must be boolean")
            cleaned[field] = value
    for field in NUMERIC_MANUAL_FIELDS:
        if field in item:
            value = item[field]
            if value in ("", None):
                cleaned[field] = None
            else:
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    raise HTTPException(status_code=400, detail=f"{ticker}: {field} must be integer")
                _validate_non_negative(value, field, ticker)
                cleaned[field] = value
    return cleaned


def _validate_grok_trending_schema(df: pd.DataFrame) -> list[str]:
    warnings = []
    required = {"ticker", "date"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise HTTPException(status_code=500, detail=f"grok_trending.parquet missing columns: {missing}")
    if df["ticker"].astype(str).str.strip().eq("").any():
        raise HTTPException(status_code=500, detail="grok_trending.parquet has empty ticker")
    duplicate_count = int(df["ticker"].astype(str).duplicated().sum())
    if duplicate_count:
        raise HTTPException(status_code=500, detail=f"grok_trending.parquet has duplicate tickers: {duplicate_count}")

    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.isna().any():
        raise HTTPException(status_code=500, detail="grok_trending.parquet has invalid date values")
    unique_dates = dates.dt.strftime("%Y-%m-%d").unique().tolist()
    if len(unique_dates) != 1:
        warnings.append(f"date column has multiple dates: {unique_dates}")
    else:
        target_date = pd.to_datetime(unique_dates[0]).date()
        today_jst = datetime.now(ZoneInfo("Asia/Tokyo")).date()
        if target_date < today_jst:
            warnings.append(f"target date {target_date} is older than today {today_jst}")
    return warnings


def _write_manual_edit_log(payload: dict) -> str | None:
    stamp = _now_stamp()
    payload = _json_safe({"logged_at": stamp, **payload})
    LOCAL_MANUAL_LOG_DIR.mkdir(parents=True, exist_ok=True)
    local_path = LOCAL_MANUAL_LOG_DIR / f"grok_manual_edit_{stamp}.json"
    local_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if _storage_mode() == "local":
        return str(local_path.relative_to(BASE_DIR))

    import boto3
    bucket, _, region = _s3_settings()
    key = _s3_key(f"manual_edit_logs/{local_path.name}")
    try:
        s3_client = boto3.client("s3", region_name=region)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=local_path.read_bytes(),
            ContentType="application/json",
        )
        return key
    except Exception as e:
        print(f"Warning: manual edit log S3 upload failed: {e}")
        return None


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer.getvalue()


def save_day_trade_list(df: pd.DataFrame, backup: bool = True) -> dict:
    """Configured storage にデイトレードリストを保存"""
    mode = _storage_mode()
    if mode == "local":
        DAY_TRADE_LIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        backup_key = _backup_local_file(DAY_TRADE_LIST_PATH, "grok_day_trade_list") if backup else None
        df.to_parquet(DAY_TRADE_LIST_PATH, index=False)
        return {"mode": mode, "local_path": str(DAY_TRADE_LIST_PATH.relative_to(BASE_DIR)), "backup_key": backup_key}

    import boto3

    bucket, _, region = _s3_settings()
    key = _s3_key("grok_day_trade_list.parquet")

    s3_client = boto3.client("s3", region_name=region)
    backup_key = _backup_s3_object(s3_client, bucket, key, "grok_day_trade_list") if backup else None

    s3_client.put_object(Bucket=bucket, Key=key, Body=_parquet_bytes(df))
    return {"mode": mode, "s3_key": key, "backup_key": backup_key}


def load_grok_trending() -> pd.DataFrame:
    """ローカルまたはS3からgrok_trending.parquetを読み込み"""
    if _storage_mode() == "s3":
        return _read_s3_parquet("grok_trending.parquet")
    if not GROK_TRENDING_PATH.exists():
        raise HTTPException(status_code=404, detail="ローカル grok_trending.parquet が見つかりません")
    return pd.read_parquet(GROK_TRENDING_PATH)


def load_grok_archive() -> pd.DataFrame:
    """ローカルまたはS3からgrok_trending_archive.parquetを読み込み"""
    if GROK_ARCHIVE_PATH.exists():
        return pd.read_parquet(GROK_ARCHIVE_PATH)

    # S3から取得
    try:
        import boto3
        from botocore.exceptions import ClientError

        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = "parquet/backtest/grok_trending_archive.parquet"
        region = os.getenv("AWS_REGION", "ap-northeast-1")

        s3_client = boto3.client("s3", region_name=region)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            s3_client.download_fileobj(bucket, key, tmp_file)
            tmp_path = tmp_file.name

        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        return df

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return pd.DataFrame()  # 空のDataFrameを返す
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3読み込みエラー: {str(e)}")


def save_grok_trending(df: pd.DataFrame, changes: list[dict] | None = None) -> dict:
    """Configured storage にgrok_trending.parquetを保存"""
    mode = _storage_mode()
    warnings = _validate_grok_trending_schema(df)

    if mode == "local":
        GROK_TRENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
        backup_key = _backup_local_file(GROK_TRENDING_PATH, "grok_trending")
        df.to_parquet(GROK_TRENDING_PATH, index=False)
        log_key = _write_manual_edit_log({
            "target": "grok_trending.parquet",
            "mode": mode,
            "local_path": str(GROK_TRENDING_PATH.relative_to(BASE_DIR)),
            "backup_key": backup_key,
            "warnings": warnings,
            "changes": changes or [],
        })
        return {"mode": mode, "local_path": str(GROK_TRENDING_PATH.relative_to(BASE_DIR)), "backup_key": backup_key, "log_key": log_key, "warnings": warnings}

    import boto3

    bucket, _, region = _s3_settings()
    key = _s3_key("grok_trending.parquet")

    s3_client = boto3.client("s3", region_name=region)
    backup_key = _backup_s3_object(s3_client, bucket, key, "grok_trending")
    s3_client.put_object(Bucket=bucket, Key=key, Body=_parquet_bytes(df))

    log_key = _write_manual_edit_log({
        "target": "grok_trending.parquet",
        "mode": mode,
        "s3_key": key,
        "backup_key": backup_key,
        "warnings": warnings,
        "changes": changes or [],
    })
    return {"mode": mode, "s3_key": key, "backup_key": backup_key, "log_key": log_key, "warnings": warnings}


class DayTradeUpdateRequest(BaseModel):
    """銘柄フラグ更新リクエスト"""
    shortable: Optional[bool] = None  # 制度信用（空売り可）
    day_trade: Optional[bool] = None  # いちにち信用対象
    ng: Optional[bool] = None         # トレード不可
    day_trade_available_shares: Optional[int] = None  # 1人当たり売り可能株数
    margin_sell_balance: Optional[int] = None  # 売り残
    margin_buy_balance: Optional[int] = None   # 買い残


@router.get("/dev/day-trade-list")
async def get_day_trade_list():
    """
    デイトレードリスト一覧を取得（grok_trending.parquetから、ML予測含む）

    Returns:
    - total: 総銘柄数
    - summary: {shortable, day_trade, ng}
    - stocks: 銘柄リスト（appearance_count, prob_up, grade付き）
    """
    grok_df = load_grok_trending()
    day_trade_df = load_day_trade_list()

    # archiveから登場回数を計算（2025-11-04以降）
    try:
        archive_df = load_grok_archive()
        if not archive_df.empty:
            archive_df = archive_df[archive_df['selection_date'] >= '2025-11-04']
            appearance_counts = archive_df['ticker'].value_counts().to_dict()
        else:
            appearance_counts = {}
    except Exception:
        appearance_counts = {}

    # ストップ高/安フラグを計算
    try:
        prices_df = load_grok_prices()
        if not prices_df.empty:
            stop_flags = calc_stop_flags(prices_df)
        else:
            stop_flags = {}
            prices_df = pd.DataFrame()
    except Exception:
        stop_flags = {}
        prices_df = pd.DataFrame()

    # ML予測を実行
    ml_predictions = {}
    try:
        model, meta = load_ml_model()
        if model is not None and not prices_df.empty:
            prices_df['date'] = pd.to_datetime(prices_df['date'])
            ml_predictions = predict_ml_for_stocks(grok_df, model, meta, prices_df)
    except Exception as e:
        print(f"ML予測エラー: {e}")

    # day_trade_listをdictに変換（tickerでルックアップ）
    dtl_map = {row["ticker"]: row for _, row in day_trade_df.iterrows()}

    stocks = []
    for _, row in grok_df.iterrows():
        ticker = row.get("ticker", "")
        dtl = dtl_map.get(ticker, {})

        # grok_trending.parquetに信用区分カラムがあればそれを使用、なければday_trade_listから
        if "shortable" in row and pd.notna(row["shortable"]):
            shortable = bool(row["shortable"])
        else:
            shortable = bool(dtl.get("shortable", False))

        if "day_trade" in row and pd.notna(row["day_trade"]):
            day_trade = bool(row["day_trade"])
        else:
            day_trade = bool(dtl.get("day_trade", False))

        if "ng" in row and pd.notna(row["ng"]):
            ng = bool(row["ng"])
        else:
            ng = bool(dtl.get("ng", False))

        # 売り可能株数（None許容）
        if "day_trade_available_shares" in row and pd.notna(row["day_trade_available_shares"]):
            day_trade_available_shares = int(row["day_trade_available_shares"])
        elif "day_trade_available_shares" in dtl and pd.notna(dtl.get("day_trade_available_shares")):
            day_trade_available_shares = int(dtl["day_trade_available_shares"])
        else:
            day_trade_available_shares = None

        # 最大必要資金（100株）
        max_cost_100 = row.get("max_cost_100") if pd.notna(row.get("max_cost_100")) else None

        # 前日差
        price_diff = int(row.get("price_diff")) if pd.notna(row.get("price_diff")) else None

        # 売り残・買い残（grok_trending.parquetから直接取得）
        margin_sell_balance = int(row.get("margin_sell_balance")) if pd.notna(row.get("margin_sell_balance")) else None
        margin_buy_balance = int(row.get("margin_buy_balance")) if pd.notna(row.get("margin_buy_balance")) else None

        # ML予測結果を取得（parquetファイルのカラムを優先、なければ動的計算）
        prob_up = row.get('prob_up') if pd.notna(row.get('prob_up')) else None
        grade = row.get('grade') if pd.notna(row.get('grade')) else None
        if prob_up is None:
            ml_result = ml_predictions.get(ticker, {})
            prob_up = ml_result.get('prob_up')
            grade = ml_result.get('grade')

        stocks.append({
            "ticker": ticker,
            "stock_name": row.get("stock_name", ""),
            "grok_rank": row.get("grok_rank") if pd.notna(row.get("grok_rank")) else None,
            "close": row.get("Close") if pd.notna(row.get("Close")) else None,
            "price_diff": price_diff,
            "rsi9": round(row.get("rsi9"), 1) if pd.notna(row.get("rsi9")) else None,
            "atr_pct": round(row.get("atr14_pct"), 1) if pd.notna(row.get("atr14_pct")) else None,
            "prob_up": prob_up,
            "grade": grade,
            "shortable": shortable,
            "day_trade": day_trade,
            "ng": ng,
            "day_trade_available_shares": day_trade_available_shares,
            "margin_sell_balance": margin_sell_balance,
            "margin_buy_balance": margin_buy_balance,
            "appearance_count": appearance_counts.get(ticker, 0),
            "max_cost_100": int(max_cost_100) if max_cost_100 is not None else None,
            "short_recommended": bool(row.get("short_recommended")) if pd.notna(row.get("short_recommended")) else False,
            "reason_category": row.get("reason_category") if pd.notna(row.get("reason_category")) else None,
        })

    # ソート: 制度 → いちにち → NG → grok_rank
    stocks.sort(key=lambda s: (
        0 if s["shortable"] else (1 if s["day_trade"] else 2),
        s.get("grok_rank") or 999
    ))

    # 必要資金集計
    # 制度信用銘柄の合計
    total_funds_shortable = sum(
        s["max_cost_100"] for s in stocks
        if s["shortable"] and s["max_cost_100"] is not None
    )
    # いちにち信用除0銘柄の合計（制度信用除く）
    total_funds_day_trade_nonzero = sum(
        s["max_cost_100"] for s in stocks
        if s["day_trade"] and not s["shortable"]
        and s["day_trade_available_shares"] is not None
        and s["day_trade_available_shares"] > 0
        and s["max_cost_100"] is not None
    )
    # 合計: 制度信用 + いちにち信用除0（NGを除く）
    total_required_funds = sum(
        s["max_cost_100"] for s in stocks
        if not s["ng"]
        and s["max_cost_100"] is not None
        and (
            s["shortable"]
            or (
                s["day_trade"]
                and s["day_trade_available_shares"] is not None
                and s["day_trade_available_shares"] > 0
            )
        )
    )

    summary = {
        "unchecked": sum(1 for s in stocks if not s["shortable"] and not s["day_trade"] and not s["ng"]),
        "shortable": sum(1 for s in stocks if s["shortable"]),
        "day_trade": sum(1 for s in stocks if s["day_trade"] and not s["shortable"]),
        "day_trade_nonzero": sum(
            1 for s in stocks
            if s["day_trade"] and not s["shortable"]
            and s["day_trade_available_shares"] is not None
            and s["day_trade_available_shares"] > 0
        ),
        "ng": sum(1 for s in stocks if s["ng"]),
        "total_funds_shortable": total_funds_shortable,
        "total_funds_day_trade_nonzero": total_funds_day_trade_nonzero,
        "total_required_funds": total_required_funds,
    }

    return JSONResponse(content={
        "total": len(stocks),
        "summary": summary,
        "stocks": stocks,
        "storage": {
            "mode": _storage_mode(),
            "grokTrending": (
                {"s3_key": _s3_key("grok_trending.parquet")}
                if _storage_mode() == "s3"
                else {"local_path": str(GROK_TRENDING_PATH.relative_to(BASE_DIR))}
            ),
            "dayTradeList": (
                {"s3_key": _s3_key("grok_day_trade_list.parquet")}
                if _storage_mode() == "s3"
                else {"local_path": str(DAY_TRADE_LIST_PATH.relative_to(BASE_DIR))}
            ),
        },
    })


@router.get("/dev/day-trade-list/{ticker}")
async def get_day_trade_item(ticker: str):
    """
    個別銘柄のフラグを取得

    Parameters:
    - ticker: ティッカーシンボル (例: 2492.T)
    """
    df = load_day_trade_list()

    # tickerで検索
    ticker_str = str(ticker)
    match = df[df["ticker"].astype(str) == ticker_str]

    if match.empty:
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} が見つかりません")

    stock = match.iloc[0].to_dict()
    return JSONResponse(content=stock)


@router.put("/dev/day-trade-list/{ticker}")
async def update_day_trade_item(ticker: str, request: DayTradeUpdateRequest):
    """
    個別銘柄のフラグを更新

    Parameters:
    - ticker: ティッカーシンボル (例: 2492.T)
    - request: 更新するフラグ (shortable, day_trade, ng)

    Note:
    - 指定されたフィールドのみ更新（None は無視）
    - grok_day_trade_list.parquet と grok_trending.parquet の両方を更新
    """
    ticker_str = str(ticker)

    # grok_day_trade_list.parquet を更新
    dtl_df = load_day_trade_list()
    dtl_mask = dtl_df["ticker"].astype(str) == ticker_str

    if not dtl_mask.any():
        raise HTTPException(status_code=404, detail=f"ティッカー {ticker} が見つかりません")

    request_fields = getattr(request, "model_fields_set", getattr(request, "__fields_set__", set()))
    item = {"ticker": ticker_str}
    for field in MANUAL_FIELDS:
        if field in request_fields:
            item[field] = getattr(request, field)
    cleaned = _validate_manual_update(item)

    for field in BOOL_MANUAL_FIELDS | {"day_trade_available_shares"}:
        if field in cleaned and field != "ticker":
            dtl_df.loc[dtl_mask, field] = cleaned[field]

    day_trade_meta = save_day_trade_list(dtl_df)

    # grok_trending.parquet も更新
    try:
        grok_df = load_grok_trending()
        grok_mask = grok_df["ticker"].astype(str) == ticker_str

        if grok_mask.any():
            before = grok_df.loc[grok_mask, list(MANUAL_FIELDS & set(grok_df.columns))].iloc[0].to_dict()
            for field in MANUAL_FIELDS:
                if field in cleaned and field != "ticker":
                    grok_df.loc[grok_mask, field] = cleaned[field]
            after = grok_df.loc[grok_mask, list(MANUAL_FIELDS & set(grok_df.columns))].iloc[0].to_dict()
            grok_meta = save_grok_trending(grok_df, changes=[{
                "ticker": ticker_str,
                "before": before,
                "after": after,
                "source": "single_update",
            }])
        else:
            grok_meta = None
    except HTTPException:
        raise
    except Exception as e:
        print(f"Warning: grok_trending.parquet更新失敗: {str(e)}")
        grok_meta = None

    # 更新後のデータを返す
    updated = dtl_df[dtl_mask].iloc[0].to_dict()
    return JSONResponse(content={
        "message": "更新しました",
        "stock": updated,
        "storage": {
            "dayTradeList": day_trade_meta,
            "grokTrending": grok_meta,
        },
    })


@router.put("/dev/day-trade-list")
async def bulk_update_day_trade_list(updates: list[dict]):
    """
    複数銘柄を一括更新

    Parameters:
    - updates: [{ticker, shortable?, day_trade?, ng?}, ...]

    Returns:
    - updated: 更新された銘柄数
    - errors: エラーがあった銘柄

    Note:
    - grok_day_trade_list.parquet と grok_trending.parquet の両方を更新
    """
    dtl_df = load_day_trade_list()

    updated_count = 0
    errors = []
    updated_tickers = []

    cleaned_updates = [_validate_manual_update(item) for item in updates]

    for item in cleaned_updates:
        ticker = str(item.get("ticker", ""))
        mask = dtl_df["ticker"].astype(str) == ticker

        if not mask.any():
            errors.append({"ticker": ticker, "error": "見つかりません"})
            continue

        for field in BOOL_MANUAL_FIELDS | {"day_trade_available_shares"}:
            if field in item:
                dtl_df.loc[mask, field] = item[field]

        updated_count += 1
        updated_tickers.append((ticker, item))

    # grok_day_trade_list.parquet を保存
    day_trade_meta = save_day_trade_list(dtl_df)

    # grok_trending.parquet も更新
    try:
        grok_df = load_grok_trending()
        change_log = []

        for ticker, item in updated_tickers:
            mask = grok_df["ticker"].astype(str) == ticker
            if mask.any():
                before = grok_df.loc[mask, list(MANUAL_FIELDS & set(grok_df.columns))].iloc[0].to_dict()
                for field in MANUAL_FIELDS:
                    if field in item:
                        grok_df.loc[mask, field] = item[field]
                after = grok_df.loc[mask, list(MANUAL_FIELDS & set(grok_df.columns))].iloc[0].to_dict()
                change_log.append({
                    "ticker": ticker,
                    "before": before,
                    "after": after,
                    "source": "bulk_update",
                })

        grok_meta = save_grok_trending(grok_df, changes=change_log)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Warning: grok_trending.parquet更新失敗: {str(e)}")
        grok_meta = None

    return JSONResponse(content={
        "updated": updated_count,
        "errors": errors,
        "storage": {
            "dayTradeList": day_trade_meta,
            "grokTrending": grok_meta,
        },
    })


@router.get("/dev/day-trade-list/history/{ticker}")
async def get_day_trade_history(ticker: str):
    """
    銘柄の過去登場履歴を取得（grok_trending_archive.parquetから）

    Parameters:
    - ticker: ティッカーシンボル (例: 6993.T)

    Returns:
    - ticker: ティッカー
    - stock_name: 銘柄名
    - appearance_count: 登場回数
    - history: 過去の履歴リスト（日付降順）
        - date: 選定日 (YYYY-MM-DD)
        - weekday: 曜日 (月/火/水/木/金)
        - buy_price: 始値（寄付き）
        - high: 日中高値
        - low: 日中安値
        - sell_price: 前場終値
        - daily_close: 大引け終値
        - volume: 出来高
        - profit_phase1_short: 前場損益（ショート基準）
        - profit_phase2_short: 大引損益（ショート基準）
        - profit_phase1_long: 前場損益（ロング基準）
        - profit_phase2_long: 大引損益（ロング基準）
    """
    try:
        archive_df = load_grok_archive()
        if archive_df.empty:
            raise HTTPException(status_code=404, detail="アーカイブデータがありません")

        # 2025-11-04以降のみ
        archive_df = archive_df[archive_df['selection_date'] >= '2025-11-04']

        # 指定銘柄のデータを抽出
        ticker_df = archive_df[archive_df['ticker'] == ticker].copy()

        if ticker_df.empty:
            raise HTTPException(status_code=404, detail=f"ティッカー {ticker} の履歴がありません")

        # 日付でソート（降順）
        ticker_df = ticker_df.sort_values('selection_date', ascending=False)

        # 銘柄名を取得
        stock_name = ticker_df.iloc[0].get('stock_name', '')

        history = []
        for _, row in ticker_df.iterrows():
            selection_date = pd.to_datetime(row['selection_date'])
            weekday = WEEKDAY_NAMES[selection_date.weekday()]

            # 損益計算（ロング基準のデータを取得）
            profit_p1_long = row.get('profit_per_100_shares_phase1')
            profit_p2_long = row.get('profit_per_100_shares_phase2')

            # ショート基準は符号反転
            profit_p1_short = -profit_p1_long if pd.notna(profit_p1_long) else None
            profit_p2_short = -profit_p2_long if pd.notna(profit_p2_long) else None

            # 前日終値（アーカイブから取得）
            prev_close = int(row.get('prev_close')) if pd.notna(row.get('prev_close')) else None

            history.append({
                "date": selection_date.strftime('%Y-%m-%d'),
                "weekday": weekday,
                "prev_close": prev_close,
                "open": int(row.get('buy_price')) if pd.notna(row.get('buy_price')) else None,
                "high": int(row.get('high')) if pd.notna(row.get('high')) else None,
                "low": int(row.get('low')) if pd.notna(row.get('low')) else None,
                "close": int(row.get('daily_close')) if pd.notna(row.get('daily_close')) else None,
                "volume": int(row.get('volume')) if pd.notna(row.get('volume')) else None,
                "profit_phase1": int(profit_p1_short) if profit_p1_short is not None else None,
                "profit_phase2": int(profit_p2_short) if profit_p2_short is not None else None,
            })

        return JSONResponse(content={
            "ticker": ticker,
            "stock_name": stock_name,
            "appearance_count": len(history),
            "history": history
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"エラー: {str(e)}")
