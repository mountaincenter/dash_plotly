#!/usr/bin/env python3
"""
add_ml_prediction_to_grok_trending.py

grok_trending.parquetにML予測（prob_up）カラムを追加する

処理:
1. grok_trending.parquet を読み込み
2. MLモデルを読み込み（24特徴量）
3. 価格データ・市場データから特徴量を計算
4. 各銘柄に対してML予測を実行
5. prob_up カラムを追加して保存

実行タイミング: 23:00パイプライン（market_cap追加後）
"""

from __future__ import annotations

import sys
from pathlib import Path
import os
import json
import hashlib
import tempfile

import numpy as np
import pandas as pd
import joblib
from scripts.lib.grok_ml_contract import (
    FEATURE_COLUMNS,
    FEATURE_CONTRACT,
    MARKET_CAP_SOURCE,
    PRICE_HISTORY_SOURCE,
)

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
JQ_MARKET_CAP_ASOF_DATE = "jq_market_cap_asof_date"
JQ_MARKET_CAP_YEN_ASOF = "jq_market_cap_yen_asof"
MIN_PRICE_HISTORY_ROWS = 35


def validate_official_market_cap_input(frame: pd.DataFrame) -> None:
    """Reject legacy, look-ahead, or unit-divergent market-cap input."""
    required = {
        "date",
        "ticker",
        "market_cap",
        "market_cap_source",
        JQ_MARKET_CAP_ASOF_DATE,
        JQ_MARKET_CAP_YEN_ASOF,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            "ML input is missing official D-1 market-cap provenance: "
            f"{missing}"
        )

    invalid_source = frame["market_cap_source"].ne(MARKET_CAP_SOURCE)
    if invalid_source.any():
        raise ValueError(
            "ML input contains non-official market-cap source rows: "
            f"{int(invalid_source.sum())}"
        )

    target_date = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if target_date.isna().any() or target_date.nunique() != 1:
        raise ValueError("ML input must contain exactly one valid target date")
    tickers = frame["ticker"].astype(str).str.strip()
    if tickers.eq("").any() or tickers.duplicated().any():
        raise ValueError("ML input contains empty or duplicate tickers")
    asof_date = pd.to_datetime(
        frame[JQ_MARKET_CAP_ASOF_DATE], errors="coerce"
    ).dt.normalize()
    invalid_date = target_date.isna() | asof_date.isna() | asof_date.ge(target_date)
    if invalid_date.any():
        raise ValueError(
            "ML market-cap as-of date is not strictly before target date: "
            f"{int(invalid_date.sum())} rows"
        )

    market_cap = pd.to_numeric(frame["market_cap"], errors="coerce")
    official_cap = pd.to_numeric(
        frame[JQ_MARKET_CAP_YEN_ASOF], errors="coerce"
    )
    null_mismatch = market_cap.isna().ne(official_cap.isna())
    comparable = market_cap.notna() & official_cap.notna()
    value_mismatch = comparable & ~np.isclose(
        market_cap.astype(float),
        official_cap.astype(float),
        rtol=0.0,
        atol=0.5,
    )
    if null_mismatch.any() or value_mismatch.any():
        raise ValueError(
            "ML market_cap differs from official D-1 yen value: "
            f"{int((null_mismatch | value_mismatch).sum())} rows"
        )


def validate_model_market_cap_source(meta: dict) -> None:
    """Require a model trained with the same point-in-time cap definition."""
    actual = meta.get("feature_sources", {}).get("market_cap")
    if actual != MARKET_CAP_SOURCE:
        raise ValueError(
            "ML model market_cap source is incompatible: "
            f"expected={MARKET_CAP_SOURCE!r}, actual={actual!r}. "
            "Retrain after building the validated J-Quants D-1 master."
        )


def validate_model_package(meta: dict, model_path: Path = ML_MODEL_FILE) -> None:
    """Reject old or mismatched model/meta pairs before unpickling the model."""
    validate_model_market_cap_source(meta)
    feature_names = meta.get("feature_names")
    if not isinstance(feature_names, list) or not feature_names:
        raise ValueError("ML model metadata has no feature_names")
    if feature_names != list(FEATURE_COLUMNS):
        raise ValueError(
            "ML model feature names/order are incompatible with the fixed "
            f"contract: expected={list(FEATURE_COLUMNS)}, actual={feature_names}"
        )
    if meta.get("feature_sources", {}).get("price_history") != PRICE_HISTORY_SOURCE:
        raise ValueError("ML model price-history source is incompatible")
    if meta.get("n_features") != len(feature_names):
        raise ValueError("ML model metadata feature count is inconsistent")
    if meta.get("feature_contract") != FEATURE_CONTRACT:
        raise ValueError(
            "ML model feature-time contract is incompatible: "
            f"expected={FEATURE_CONTRACT!r}, actual={meta.get('feature_contract')!r}"
        )
    expected_sha256 = meta.get("model_sha256")
    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise ValueError("ML model metadata has no valid model_sha256")
    if not model_path.exists():
        raise FileNotFoundError(f"ML model file not found: {model_path}")
    digest = hashlib.sha256()
    with model_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    actual_sha256 = digest.hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError(
            "ML model bytes do not match metadata SHA256: "
            f"expected={expected_sha256}, actual={actual_sha256}"
        )


def _write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        frame.to_parquet(temporary_path, index=False)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def get_grade(prob: float, boundaries: list[float]) -> str:
    """prob_upからGrade (G1-G4) を返す"""
    for i, b in enumerate(boundaries):
        if prob <= b:
            return f"G{i + 1}"
    return f"G{len(boundaries)}"


def load_ml_model():
    """MLモデルとメタ情報を読み込み"""
    if not ML_MODEL_FILE.exists() or not ML_META_FILE.exists():
        raise FileNotFoundError(
            "ML model package is incomplete: "
            f"model={ML_MODEL_FILE.exists()}, meta={ML_META_FILE.exists()}"
        )
    with open(ML_META_FILE, 'r', encoding='utf-8') as f:
        meta = json.load(f)
    validate_model_package(meta, ML_MODEL_FILE)
    model = joblib.load(ML_MODEL_FILE)

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


def calc_price_features(
    ticker: str,
    target_date: pd.Timestamp,
    prices_df: pd.DataFrame,
) -> dict:
    """価格ベース特徴量を計算（24特徴量モデル対応）"""
    ticker_prices = prices_df[
        (prices_df['ticker'] == ticker) &
        (prices_df['date'] < target_date)
    ].sort_values('date').tail(60).dropna(subset=['Close'])

    if len(ticker_prices) < MIN_PRICE_HISTORY_ROWS:
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
    prev_high = highs[-1]
    prev_low = lows[-1]
    prev_close = closes[-1]
    prev_open = opens[-1]
    prev_range = prev_high - prev_low
    features['prev_close_position'] = (prev_close - prev_low) / prev_range if prev_range > 0 else 0.5
    features['prev_candle'] = (prev_close - prev_open) / prev_open if prev_open > 0 else 0

    # MACD Histogram: EMA(12) - EMA(26) → signal EMA(9) → hist = macd - signal
    close_s = pd.Series(closes)
    ema12 = close_s.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = close_s.ewm(span=26, adjust=False, min_periods=26).mean()
    macd_line = ema12 - ema26
    signal = macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
    hist = macd_line - signal
    features['macd_hist'] = float(hist.iloc[-1]) if not np.isnan(hist.iloc[-1]) else np.nan

    # Bollinger Bands %B: (close - lower) / (upper - lower), window=20, k=2
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

    # Volume Trend: SMA(vol, 5) / SMA(vol, 20)
    if len(volumes) >= 20:
        vol_s = pd.Series(volumes.astype(float))
        vol_ma5 = vol_s.rolling(5).mean()
        vol_ma20 = vol_s.rolling(20).mean()
        vt = vol_ma5 / vol_ma20.replace(0, np.nan)
        features['vol_trend'] = float(vt.iloc[-1]) if not np.isnan(vt.iloc[-1]) else np.nan
    else:
        features['vol_trend'] = np.nan

    return features


def predict_ml_for_stocks(grok_df: pd.DataFrame, model, meta: dict, prices_df: pd.DataFrame, market_data: dict) -> dict:
    """grok_dfの各銘柄に対して24特徴量モデルの予測を実行する。"""
    if model is None:
        return {}

    feature_names = meta['feature_names']
    grade_boundaries = meta.get('grade_boundaries', [0.25, 0.40, 0.55, 1.0])
    target_date = pd.to_datetime(grok_df['date'].iloc[0])
    market_features = calc_market_features(target_date, market_data)

    results = {}

    for _, row in grok_df.iterrows():
        ticker = row['ticker']
        existing_features = {
            'market_cap': row.get('market_cap'),
            'atr14_pct': row.get('atr14_pct'),
            'vol_ratio': row.get('vol_ratio'),
            'rsi9': row.get('rsi9'),
            'nikkei_change_pct': row.get('nikkei_change_pct'),
            'futures_change_pct': row.get('futures_change_pct'),
        }

        price_features = calc_price_features(ticker, target_date, prices_df)
        if price_features is None:
            available_rows = int(
                (
                    prices_df["ticker"].eq(ticker)
                    & prices_df["date"].lt(target_date)
                ).sum()
            )
            print(
                f"   ⚠️ {ticker}: 日足履歴不足 "
                f"({available_rows}/{MIN_PRICE_HISTORY_ROWS})"
            )
            results[ticker] = {'prob_up': None}
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
            }
        except Exception as e:
            print(f"   ⚠️ {ticker}: 予測失敗 - {e}")
            results[ticker] = {'prob_up': None}

    return results


def main():
    """メイン処理"""
    print("=== grok_trending.parquet に ML予測（prob_up）を追加 ===\n")

    # 1. grok_trending.parquet 読み込み
    print(f"1. 読み込み: {GROK_TRENDING_FILE}")
    if not GROK_TRENDING_FILE.exists():
        print(f"   エラー: ファイルが見つかりません")
        return 1

    df = pd.read_parquet(GROK_TRENDING_FILE)
    print(f"   銘柄数: {len(df)}")
    validate_official_market_cap_input(df)
    print("   ✅ 公式J-Quants D-1時価総額の由来・時点を検証")

    # 2. MLモデル読み込み
    print("\n2. MLモデル読み込み")
    try:
        model, meta = load_ml_model()
    except Exception as error:
        print(f"   エラー: 互換性のあるMLモデルを読み込めません: {error}")
        return 1

    print(f"   ✅ モデル読み込み完了")
    print("   ✅ モデル/metaのSHA256とmarket_cap学習ソースが一致")
    print(f"   特徴量数: {len(meta['feature_names'])}")
    print(f"   Grade boundaries: {meta.get('grade_boundaries', 'N/A')} (参考値、gradeカラムは廃止済み)")

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
    success_count = 0

    for _, row in df.iterrows():
        ticker = row['ticker']
        pred = predictions.get(ticker, {})
        prob_up = pred.get('prob_up')
        prob_up_list.append(prob_up)
        if prob_up is not None:
            success_count += 1
            print(f"   {ticker}: prob_up={prob_up:.3f}")

    if success_count != len(df):
        print(
            "   エラー: 全選定銘柄のML予測が揃っていません。"
            f"success={success_count}, expected={len(df)}"
        )
        return 1

    df['prob_up'] = prob_up_list

    # 旧カラム削除（存在する場合）
    for old_col in ['quintile', 'grade']:
        if old_col in df.columns:
            df = df.drop(columns=[old_col])

    print(f"\n6. カラム追加完了")
    print(f"   成功: {success_count}/{len(df)} 銘柄")

    # 7. 保存
    print(f"\n7. 保存: {GROK_TRENDING_FILE}")
    _write_parquet_atomic(df, GROK_TRENDING_FILE)
    print("   ✅ 完了")

    # サマリー
    print(f"\n=== サマリー ===")
    valid_probs = df[df['prob_up'].notna()]['prob_up']
    if len(valid_probs) > 0:
        print(f"prob_up 平均: {valid_probs.mean():.3f}")
        print(f"prob_up 中央値: {valid_probs.median():.3f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
