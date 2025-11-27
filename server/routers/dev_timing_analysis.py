# server/routers/dev_timing_analysis.py
"""
売買タイミング最適化分析データAPI
/api/dev/timing-analysis - タイミング分析データをJSONで返すエンドポイント
"""
from fastapi import APIRouter, HTTPException
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

router = APIRouter()

# データファイルのパス
DATA_PATH = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'
META_PATH = ROOT / 'data' / 'parquet' / 'meta_jquants.parquet'

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# 日付フィルター
START_DATE = "2025-11-14"


def load_meta_data() -> pd.DataFrame:
    """
    企業メタデータを読み込み（meta_jquants.parquet）
    """
    try:
        s3_key = f"{S3_PREFIX}meta_jquants.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
        meta_df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })
        return meta_df[['ticker', 'stock_name']]
    except Exception as e:
        print(f"[WARNING] Could not load meta from S3: {e}")
        if META_PATH.exists():
            meta_df = pd.read_parquet(META_PATH)
            return meta_df[['ticker', 'stock_name']]
    return pd.DataFrame(columns=['ticker', 'stock_name'])


def load_timing_data() -> pd.DataFrame:
    """
    タイミング分析データを読み込み
    - S3から読み込み（本番環境）
    - ローカルファイルにフォールバック（開発環境）
    - meta_jquants.parquetから企業名をマージ
    - 2025-11-14以降のデータでフィルター
    """
    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"[INFO] Loading timing data from S3: {s3_url}")

        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })
        print(f"[INFO] Successfully loaded {len(df)} records from S3")
    except Exception as e:
        print(f"[WARNING] Could not load from S3: {e}")
        if DATA_PATH.exists():
            print(f"[INFO] Loading from local file: {DATA_PATH}")
            df = pd.read_parquet(DATA_PATH)
        else:
            raise FileNotFoundError("Timing analysis data not found")

    # 日付フィルター（2025-11-14以降）
    if 'backtest_date' in df.columns:
        df['backtest_date'] = pd.to_datetime(df['backtest_date'], format='mixed')
        df = df[df['backtest_date'] >= START_DATE].copy()
        print(f"[INFO] Filtered to {len(df)} records from {START_DATE}")

    # meta_jquants.parquetから企業名をマージ
    meta_df = load_meta_data()
    if not meta_df.empty:
        df = df.drop(columns=['stock_name'], errors='ignore')
        df = df.merge(meta_df, on='ticker', how='left')
        print(f"[INFO] Merged company names from meta_jquants.parquet")

    return df


def safe_float(value: Any) -> float | None:
    """NaN/Infを安全にNoneに変換"""
    if pd.isna(value) or np.isinf(value):
        return None
    return float(value)


def calculate_timing_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """全体サマリー統計"""
    # タイミングデータがあるレコードのみ
    df_valid = df[df['morning_close_price'].notna()].copy()

    if len(df_valid) == 0:
        return None

    # 前場終値で売却した方が良かったケース
    morning_better_profit = int((df_valid['better_profit_timing'] == 'morning_close').sum())
    # 大引で売却した方が良かったケース
    day_better_profit = int((df_valid['better_profit_timing'] == 'day_close').sum())

    # 損失時のタイミング
    morning_better_loss = int((df_valid['better_loss_timing'] == 'morning_close').sum())
    day_better_loss = int((df_valid['better_loss_timing'] == 'day_close').sum())

    return {
        'total': len(df_valid),
        'profitTiming': {
            'morningBetter': morning_better_profit,
            'dayBetter': day_better_profit,
            'morningBetterPct': safe_float(morning_better_profit / len(df_valid) * 100),
        },
        'lossTiming': {
            'morningBetter': morning_better_loss,
            'dayBetter': day_better_loss,
            'morningBetterPct': safe_float(morning_better_loss / len(df_valid) * 100),
        },
        'avgProfitMorning': safe_float(df_valid['profit_morning'].mean()),
        'avgProfitDay': safe_float(df_valid['profit_day_close'].mean()),
        'avgProfitMorningPct': safe_float(df_valid['profit_morning_pct'].mean()),
        'avgProfitDayPct': safe_float(df_valid['profit_day_close_pct'].mean()),
    }


def calculate_recommendation_timing(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """売買推奨別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'recommendation_action' not in df_valid.columns:
        return None

    results = []
    for action in ['buy', 'sell', 'hold']:
        action_df = df_valid[df_valid['recommendation_action'] == action]
        if len(action_df) == 0:
            continue

        morning_better = int((action_df['better_profit_timing'] == 'morning_close').sum())

        results.append({
            'action': action,
            'total': len(action_df),
            'morningBetter': morning_better,
            'dayBetter': len(action_df) - morning_better,
            'morningBetterPct': safe_float(morning_better / len(action_df) * 100),
            'avgProfitMorning': safe_float(action_df['profit_morning'].mean()),
            'avgProfitDay': safe_float(action_df['profit_day_close'].mean()),
        })

    return results


def calculate_volatility_timing(df: pd.DataFrame) -> Dict[str, Any]:
    """ボラティリティ別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'morning_volatility' not in df_valid.columns:
        return None

    # ボラティリティで3分位
    df_valid['vol_group'] = pd.qcut(
        df_valid['morning_volatility'],
        q=3,
        labels=['低', '中', '高'],
        duplicates='drop'
    )

    results = []
    for group in ['低', '中', '高']:
        group_df = df_valid[df_valid['vol_group'] == group]
        if len(group_df) == 0:
            continue

        morning_better = int((group_df['better_profit_timing'] == 'morning_close').sum())

        results.append({
            'group': group,
            'total': len(group_df),
            'morningBetter': morning_better,
            'morningBetterPct': safe_float(morning_better / len(group_df) * 100),
            'avgVolatility': safe_float(group_df['morning_volatility'].mean()),
        })

    return results


def calculate_score_timing(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """スコア別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'selection_score' not in df_valid.columns:
        return None

    # スコアで3分位
    df_valid['score_group'] = pd.qcut(
        df_valid['selection_score'],
        q=3,
        labels=['低スコア', '中スコア', '高スコア'],
        duplicates='drop'
    )

    results = []
    for group in ['低スコア', '中スコア', '高スコア']:
        group_df = df_valid[df_valid['score_group'] == group]
        if len(group_df) == 0:
            continue

        morning_better = int((group_df['better_profit_timing'] == 'morning_close').sum())

        results.append({
            'group': group,
            'total': len(group_df),
            'morningBetter': morning_better,
            'morningBetterPct': safe_float(morning_better / len(group_df) * 100),
            'avgScore': safe_float(group_df['selection_score'].mean()),
        })

    return results


def calculate_marketcap_timing(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """時価総額別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'market_cap' not in df_valid.columns:
        return None

    # 時価総額で3分位
    df_valid['cap_group'] = pd.qcut(
        df_valid['market_cap'],
        q=3,
        labels=['小型株', '中型株', '大型株'],
        duplicates='drop'
    )

    results = []
    for group in ['小型株', '中型株', '大型株']:
        group_df = df_valid[df_valid['cap_group'] == group]
        if len(group_df) == 0:
            continue

        morning_better = int((group_df['better_profit_timing'] == 'morning_close').sum())
        win_morning = int((group_df['is_win_morning'] == True).sum())
        win_day = int((group_df['is_win_day_close'] == True).sum())

        results.append({
            'group': group,
            'total': len(group_df),
            'morningBetter': morning_better,
            'morningBetterPct': safe_float(morning_better / len(group_df) * 100),
            'avgProfitMorning': safe_float(group_df['profit_morning'].mean()),
            'avgProfitDay': safe_float(group_df['profit_day_close'].mean()),
            'winRateMorning': safe_float(win_morning / len(group_df) * 100),
            'winRateDay': safe_float(win_day / len(group_df) * 100),
        })

    return results


def calculate_price_level_timing(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """株価水準別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'morning_open' not in df_valid.columns:
        return None

    # 株価で3分位
    df_valid['price_group'] = pd.qcut(
        df_valid['morning_open'],
        q=3,
        labels=['低位株', '中位株', '高位株'],
        duplicates='drop'
    )

    results = []
    for group in ['低位株', '中位株', '高位株']:
        group_df = df_valid[df_valid['price_group'] == group]
        if len(group_df) == 0:
            continue

        morning_better = int((group_df['better_profit_timing'] == 'morning_close').sum())

        results.append({
            'group': group,
            'total': len(group_df),
            'morningBetter': morning_better,
            'morningBetterPct': safe_float(morning_better / len(group_df) * 100),
            'avgPrice': safe_float(group_df['morning_open'].mean()),
            'avgProfitMorning': safe_float(group_df['profit_morning'].mean()),
            'avgProfitDay': safe_float(group_df['profit_day_close'].mean()),
        })

    return results


def calculate_volume_timing(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """出来高別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'volume' not in df_valid.columns:
        return None

    # 出来高で3分位
    df_valid['vol_group'] = pd.qcut(
        df_valid['volume'],
        q=3,
        labels=['小商い', '中商い', '大商い'],
        duplicates='drop'
    )

    results = []
    for group in ['小商い', '中商い', '大商い']:
        group_df = df_valid[df_valid['vol_group'] == group]
        if len(group_df) == 0:
            continue

        morning_better = int((group_df['better_profit_timing'] == 'morning_close').sum())

        results.append({
            'group': group,
            'total': len(group_df),
            'morningBetter': morning_better,
            'morningBetterPct': safe_float(morning_better / len(group_df) * 100),
            'avgVolume': safe_float(group_df['volume'].mean()),
            'avgProfitMorning': safe_float(group_df['profit_morning'].mean()),
            'avgProfitDay': safe_float(group_df['profit_day_close'].mean()),
        })

    return results


def calculate_daily_timing(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """日別のタイミング統計"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    if 'backtest_date' not in df_valid.columns:
        return []

    daily = df_valid.groupby('backtest_date').agg({
        'ticker': 'count',
        'better_profit_timing': lambda x: (x == 'morning_close').sum(),
        'profit_morning': 'mean',
        'profit_day_close': 'mean',
    }).reset_index()

    results = []
    for _, row in daily.iterrows():
        total = int(row['ticker'])
        morning_better = int(row['better_profit_timing'])

        results.append({
            'date': row['backtest_date'].strftime('%Y-%m-%d'),
            'total': total,
            'morningBetter': morning_better,
            'morningBetterPct': safe_float(morning_better / total * 100),
            'avgProfitMorning': safe_float(row['profit_morning']),
            'avgProfitDay': safe_float(row['profit_day_close']),
        })

    return sorted(results, key=lambda x: x['date'])


def get_stock_details(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """株式レベルの詳細データ"""
    df_valid = df[df['morning_close_price'].notna()].copy()

    stocks = []
    for _, row in df_valid.iterrows():
        stocks.append({
            'ticker': row.get('ticker'),
            'companyName': row.get('stock_name', row.get('ticker')),
            'backtestDate': row.get('backtest_date').strftime('%Y-%m-%d') if pd.notna(row.get('backtest_date')) else None,
            'recommendationAction': row.get('recommendation_action'),
            'grokRank': int(row.get('grok_rank')) if pd.notna(row.get('grok_rank')) else None,
            'buyPrice': safe_float(row.get('morning_open')),
            'morningClosePrice': safe_float(row.get('morning_close_price')),
            'dayClosePrice': safe_float(row.get('day_close_price')),
            'profitMorning': safe_float(row.get('profit_morning')),
            'profitDayClose': safe_float(row.get('profit_day_close')),
            'profitMorningPct': safe_float(row.get('profit_morning_pct')),
            'profitDayClosePct': safe_float(row.get('profit_day_close_pct')),
            'betterProfitTiming': row.get('better_profit_timing'),
            'betterLossTiming': row.get('better_loss_timing'),
            'isWinMorning': bool(row.get('is_win_morning')) if pd.notna(row.get('is_win_morning')) else None,
            'isWinDayClose': bool(row.get('is_win_day_close')) if pd.notna(row.get('is_win_day_close')) else None,
            'highPrice': safe_float(row.get('high')),
            'lowPrice': safe_float(row.get('low')),
        })

    return stocks


@router.get("/api/dev/timing-analysis")
async def get_timing_analysis():
    """
    売買タイミング最適化分析データを取得

    Returns:
        dict: タイミング分析データ（JSON）
    """
    try:
        df = load_timing_data()

        # 勝率データを計算
        df_valid = df[df['morning_close_price'].notna()].copy()
        win_rate_morning = safe_float((df_valid['is_win_morning'] == True).sum() / len(df_valid) * 100) if len(df_valid) > 0 else None
        win_rate_day = safe_float((df_valid['is_win_day_close'] == True).sum() / len(df_valid) * 100) if len(df_valid) > 0 else None

        return {
            'summary': calculate_timing_summary(df),
            'byRecommendation': calculate_recommendation_timing(df),
            'byVolatility': calculate_volatility_timing(df),
            'byScore': calculate_score_timing(df),
            'byMarketCap': calculate_marketcap_timing(df),
            'byPriceLevel': calculate_price_level_timing(df),
            'byVolume': calculate_volume_timing(df),
            'daily': calculate_daily_timing(df),
            'stocks': get_stock_details(df),
            'winRates': {
                'morning': win_rate_morning,
                'dayClose': win_rate_day,
            },
            'metadata': {
                'totalRecords': len(df),
                'recordsWithTiming': len(df[df['morning_close_price'].notna()]),
                'dateRange': {
                    'start': df['backtest_date'].min().strftime('%Y-%m-%d') if 'backtest_date' in df.columns else None,
                    'end': df['backtest_date'].max().strftime('%Y-%m-%d') if 'backtest_date' in df.columns else None,
                }
            }
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Timing analysis data not found")
    except Exception as e:
        print(f"[ERROR] Failed to generate timing analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))
