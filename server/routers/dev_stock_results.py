# server/routers/dev_stock_results.py
"""
開発者向け取引結果API
/api/dev/stock-results/* - JSON APIエンドポイント
"""
from fastapi import APIRouter, HTTPException
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

# ファイルパス
STOCK_RESULTS_FILE = PARQUET_DIR / "stock_results.parquet"
STOCK_RESULTS_SUMMARY_FILE = PARQUET_DIR / "stock_results_summary.parquet"

# S3設定（環境変数から取得）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# キャッシュ
_results_cache: Optional[pd.DataFrame] = None
_summary_cache: Optional[pd.DataFrame] = None
_cache_timestamp: Optional[datetime] = None
CACHE_TTL_SECONDS = 60  # 1分に短縮


def clear_cache():
    """キャッシュをクリア"""
    global _results_cache, _summary_cache, _cache_timestamp
    _results_cache = None
    _summary_cache = None
    _cache_timestamp = None


def load_stock_results() -> pd.DataFrame:
    """
    取引結果データを読み込み
    - ローカルファイルを最優先（開発環境）
    - ローカルがなければS3から読み込み（本番環境）
    """
    global _results_cache, _cache_timestamp

    # キャッシュチェック
    if _results_cache is not None and _cache_timestamp is not None:
        if (datetime.now() - _cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
            return _results_cache

    # ローカルファイルを最優先
    if STOCK_RESULTS_FILE.exists():
        print(f"[INFO] Loading stock results from local file: {STOCK_RESULTS_FILE}")
        try:
            df = pd.read_parquet(STOCK_RESULTS_FILE)
            if '約定日' in df.columns:
                df['約定日'] = pd.to_datetime(df['約定日'])
            _results_cache = df
            _cache_timestamp = datetime.now()
            print(f"[INFO] Successfully loaded {len(df)} records from local file")
            return df
        except Exception as e:
            print(f"[ERROR] Failed to load local file: {e}")

    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}stock_results.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"

        print(f"[INFO] Loading stock results from S3: {s3_url}")

        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })

        if '約定日' in df.columns:
            df['約定日'] = pd.to_datetime(df['約定日'])

        _results_cache = df
        _cache_timestamp = datetime.now()
        print(f"[INFO] Successfully loaded {len(df)} records from S3")
        return df

    except Exception as e:
        print(f"[WARNING] Could not load stock results from S3: {type(e).__name__}: {e}")

    print(f"[WARNING] Stock results not found in local or S3")
    return pd.DataFrame()


def load_summary() -> pd.DataFrame:
    """サマリーデータを読み込み"""
    global _summary_cache

    if _summary_cache is not None and _cache_timestamp is not None:
        if (datetime.now() - _cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
            return _summary_cache

    # ローカルファイルを最優先
    if STOCK_RESULTS_SUMMARY_FILE.exists():
        try:
            df = pd.read_parquet(STOCK_RESULTS_SUMMARY_FILE)
            _summary_cache = df
            return df
        except Exception as e:
            print(f"[ERROR] Failed to load summary file: {e}")

    # S3から読み込み
    try:
        s3_url = f"s3://{S3_BUCKET}/{S3_PREFIX}stock_results_summary.parquet"
        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })
        _summary_cache = df
        return df
    except Exception as e:
        print(f"[WARNING] Could not load summary from S3: {e}")

    return pd.DataFrame()


@router.get("/api/dev/stock-results/summary")
async def get_stock_results_summary():
    """
    取引結果サマリー
    - 全体統計、ロング/ショート別統計
    """
    df = load_stock_results()
    summary_df = load_summary()

    if df.empty:
        raise HTTPException(status_code=404, detail="No stock results data found")

    # サマリーをdict化
    summary = {}
    if not summary_df.empty:
        for _, row in summary_df.iterrows():
            summary[row['metric']] = row['value']

    # 日別集計（グラフ用）
    daily_stats = []
    if '約定日' in df.columns:
        daily_groups = df.groupby(df['約定日'].dt.date)
        for trade_date, df_day in daily_groups:
            day_profit = df_day['実現損益'].sum()
            day_long = df_day[df_day['売買'] == 'ロング']['実現損益'].sum()
            day_short = df_day[df_day['売買'] == 'ショート']['実現損益'].sum()
            day_count = len(df_day)
            day_win = (df_day['実現損益'] > 0).sum()

            daily_stats.append({
                "date": trade_date.isoformat(),
                "profit": float(day_profit),
                "long_profit": float(day_long),
                "short_profit": float(day_short),
                "count": int(day_count),
                "win_count": int(day_win),
                "win_rate": float(day_win / day_count * 100) if day_count > 0 else 0,
            })

    # 日付でソート
    daily_stats.sort(key=lambda x: x["date"])

    # 累積損益を計算
    cumulative = 0.0
    cumulative_long = 0.0
    cumulative_short = 0.0
    for stat in daily_stats:
        cumulative += stat["profit"]
        cumulative_long += stat["long_profit"]
        cumulative_short += stat["short_profit"]
        stat["cumulative_profit"] = float(cumulative)
        stat["cumulative_long"] = float(cumulative_long)
        stat["cumulative_short"] = float(cumulative_short)

    # 損益区分別集計
    loss_distribution = []
    if '損益区分' in df.columns:
        loss_groups = df.groupby('損益区分').agg({
            '実現損益': ['sum', 'count']
        }).reset_index()
        loss_groups.columns = ['range', 'total', 'count']

        for _, row in loss_groups.iterrows():
            long_count = len(df[(df['損益区分'] == row['range']) & (df['売買'] == 'ロング')])
            short_count = len(df[(df['損益区分'] == row['range']) & (df['売買'] == 'ショート')])
            loss_distribution.append({
                "range": row['range'],
                "total": float(row['total']),
                "count": int(row['count']),
                "long": int(long_count),
                "short": int(short_count),
            })

    return {
        "summary": {
            "total_profit": summary.get('total_profit', 0),
            "total_count": int(summary.get('total_count', 0)),
            "win_count": int(summary.get('win_count', 0)),
            "lose_count": int(summary.get('lose_count', 0)),
            "win_rate": summary.get('win_rate', 0),
            "long_profit": summary.get('long_profit', 0),
            "long_count": int(summary.get('long_count', 0)),
            "long_win": int(summary.get('long_win', 0)),
            "long_lose": int(summary.get('long_lose', 0)),
            "long_win_rate": summary.get('long_win_rate', 0),
            "short_profit": summary.get('short_profit', 0),
            "short_count": int(summary.get('short_count', 0)),
            "short_win": int(summary.get('short_win', 0)),
            "short_lose": int(summary.get('short_lose', 0)),
            "short_win_rate": summary.get('short_win_rate', 0),
        },
        "daily_stats": daily_stats,
        "loss_distribution": loss_distribution,
        "updated_at": _cache_timestamp.isoformat() if _cache_timestamp else None,
    }


@router.get("/api/dev/stock-results/daily")
async def get_daily_results(view: str = "daily"):
    """
    日別/週別/月別の取引一覧
    view: daily, weekly, monthly
    """
    df = load_stock_results()

    if df.empty:
        raise HTTPException(status_code=404, detail="No stock results data found")

    # 週・月カラムを追加
    if '約定日' in df.columns:
        df['週'] = df['約定日'].dt.strftime("%Y/W%W")
        df['月'] = df['約定日'].dt.strftime("%Y/%m")

    # グループ化のキーを決定
    if view == "weekly":
        group_col = '週'
    elif view == "monthly":
        group_col = '月'
    else:
        group_col = '約定日'

    # グループごとに集計
    results = []

    if view == "daily":
        groups = df.groupby(df['約定日'].dt.date)
    else:
        groups = df.groupby(group_col)

    for group_key, df_group in groups:
        group_profit = df_group['実現損益'].sum()
        group_long = df_group[df_group['売買'] == 'ロング']['実現損益'].sum()
        group_short = df_group[df_group['売買'] == 'ショート']['実現損益'].sum()

        # 個別取引
        trades = []
        for _, row in df_group.iterrows():
            trade = {
                "code": str(row['コード']),
                "name": row['銘柄名'],
                "position": row['売買'],
                "qty": int(row['数量']),
                "avg_cost": float(row['平均取得価額']),
                "avg_price": float(row['平均単価']),
                "profit": float(row['実現損益']),
            }
            trades.append(trade)

        # ソート（損益降順）
        trades.sort(key=lambda x: x["profit"], reverse=True)

        key_str = group_key.isoformat() if hasattr(group_key, 'isoformat') else str(group_key)

        results.append({
            "key": key_str,
            "total_profit": float(group_profit),
            "long_profit": float(group_long),
            "short_profit": float(group_short),
            "count": len(df_group),
            "trades": trades,
        })

    # ソート（新しい順）
    results.sort(key=lambda x: x["key"], reverse=True)

    return {
        "view": view,
        "results": results,
    }


@router.get("/api/dev/stock-results/by-stock")
async def get_results_by_stock():
    """
    銘柄別の取引一覧
    """
    df = load_stock_results()

    if df.empty:
        raise HTTPException(status_code=404, detail="No stock results data found")

    # 銘柄別集計
    stock_groups = df.groupby(['コード', '銘柄名']).agg({
        '実現損益': 'sum'
    }).reset_index()
    stock_groups.columns = ['code', 'name', 'total_profit']
    stock_groups = stock_groups.sort_values('total_profit', ascending=False)

    results = []
    for _, stock_row in stock_groups.iterrows():
        code = stock_row['code']
        name = stock_row['name']

        stock_df = df[(df['コード'] == code) & (df['銘柄名'] == name)]
        stock_long = stock_df[stock_df['売買'] == 'ロング']['実現損益'].sum()
        stock_short = stock_df[stock_df['売買'] == 'ショート']['実現損益'].sum()

        # 個別取引
        trades = []
        for _, row in stock_df.iterrows():
            trade_date = row['約定日'].strftime("%Y/%m/%d") if pd.notna(row['約定日']) else ""
            trade = {
                "date": trade_date,
                "position": row['売買'],
                "qty": int(row['数量']),
                "avg_cost": float(row['平均取得価額']),
                "avg_price": float(row['平均単価']),
                "profit": float(row['実現損益']),
            }
            trades.append(trade)

        # 日付でソート（新しい順）
        trades.sort(key=lambda x: x["date"], reverse=True)

        results.append({
            "code": str(code),
            "name": name,
            "total_profit": float(stock_row['total_profit']),
            "long_profit": float(stock_long),
            "short_profit": float(stock_short),
            "count": len(stock_df),
            "trades": trades,
        })

    return {
        "results": results,
    }


@router.post("/api/dev/stock-results/refresh")
async def refresh_stock_results():
    """
    キャッシュをクリアして最新データを再読み込み
    S3アップロード後に呼び出すことでリアルタイム反映
    """
    clear_cache()

    # データを再読み込み
    df = load_stock_results()

    if df.empty:
        return {
            "status": "warning",
            "message": "Cache cleared but no data found",
            "count": 0,
        }

    return {
        "status": "success",
        "message": "Cache refreshed",
        "count": len(df),
        "updated_at": datetime.now().isoformat(),
    }
