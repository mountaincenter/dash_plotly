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
async def get_stock_results_summary(strategy: Optional[str] = None):
    """
    取引結果サマリー
    - 全体統計、ロング/ショート別統計
    - strategy: 戦略フィルター (grok / granville_ifd / llm / other)
    """
    df = load_stock_results()
    summary_df = load_summary()

    if df.empty:
        raise HTTPException(status_code=404, detail="No stock results data found")

    # 戦略フィルター適用
    if strategy and '戦略' in df.columns:
        df = df[df['戦略'] == strategy].copy()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for strategy: {strategy}")

    # サマリーをdict化
    summary = {}
    if not summary_df.empty:
        for _, row in summary_df.iterrows():
            summary[row['metric']] = row['value']

    # 戦略フィルター適用時はサマリーを再計算
    if strategy:
        total_profit = df['実現損益'].sum()
        total_count = len(df)
        win_count = (df['実現損益'] > 0).sum()
        lose_count = (df['実現損益'] < 0).sum()
        win_rate = win_count / total_count * 100 if total_count > 0 else 0
        long_df = df[df['売買'] == 'ロング']
        short_df = df[df['売買'] == 'ショート']
        summary = {
            'total_profit': total_profit,
            'total_count': total_count,
            'win_count': win_count,
            'lose_count': lose_count,
            'win_rate': win_rate,
            'long_profit': long_df['実現損益'].sum() if len(long_df) > 0 else 0,
            'long_count': len(long_df),
            'long_win': (long_df['実現損益'] > 0).sum() if len(long_df) > 0 else 0,
            'long_lose': (long_df['実現損益'] < 0).sum() if len(long_df) > 0 else 0,
            'long_win_rate': ((long_df['実現損益'] > 0).sum() / len(long_df) * 100) if len(long_df) > 0 else 0,
            'short_profit': short_df['実現損益'].sum() if len(short_df) > 0 else 0,
            'short_count': len(short_df),
            'short_win': (short_df['実現損益'] > 0).sum() if len(short_df) > 0 else 0,
            'short_lose': (short_df['実現損益'] < 0).sum() if len(short_df) > 0 else 0,
            'short_win_rate': ((short_df['実現損益'] > 0).sum() / len(short_df) * 100) if len(short_df) > 0 else 0,
        }

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
        "strategy_summary": _build_strategy_summary(summary),
        "updated_at": _cache_timestamp.isoformat() if _cache_timestamp else None,
    }


def _build_strategy_summary(summary: dict) -> list:
    """戦略別サマリーを構築"""
    strategies = []
    for s in ["grok", "granville_ifd", "llm", "other"]:
        profit = summary.get(f"{s}_profit", 0)
        count = int(summary.get(f"{s}_count", 0))
        win = int(summary.get(f"{s}_win", 0))
        win_rate = summary.get(f"{s}_win_rate", 0)
        if count > 0:
            strategies.append({
                "strategy": s,
                "profit": float(profit),
                "count": count,
                "win": win,
                "win_rate": float(win_rate),
            })
    return strategies


@router.get("/api/dev/stock-results/daily")
async def get_daily_results(view: str = "daily", strategy: Optional[str] = None):
    """
    日別/週別/月別の取引一覧
    view: daily, weekly, monthly
    strategy: 戦略フィルター (grok / granville_ifd / llm / other)
    """
    df = load_stock_results()

    # 戦略フィルター
    if strategy and '戦略' in df.columns:
        df = df[df['戦略'] == strategy].copy()

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


# 価格帯定義
PRICE_RANGES = [
    {"label": "~1,000円", "min": 0, "max": 1000},
    {"label": "1,000~3,000円", "min": 1000, "max": 3000},
    {"label": "3,000~5,000円", "min": 3000, "max": 5000},
    {"label": "5,000~10,000円", "min": 5000, "max": 10000},
    {"label": "10,000円~", "min": 10000, "max": float("inf")},
]

# 翌日以降の損切り銘柄（当日決済ではない）- 価格帯分析から除外
EXCLUDE_NON_SAME_DAY_LOSSES = [
    ("2025-12-26", "7042", -7120),     # アクセスグループHL
    ("2026-01-14", "2160", -8957),     # ジーエヌアイ
    ("2026-01-15", "369A", -17351),    # エータイ
    ("2026-01-16", "5243", -38490),    # NOTE（-38,490の方のみ）
    ("2026-01-20", "6167", -5013),     # 冨士ダイス
    ("2026-01-20", "215A", -18127),    # タイミー
    ("2026-01-21", "4062", -48393),    # イビデン
    ("2026-01-22", "8267", -1929),     # イオン
    ("2026-01-30", "4461", -116000),   # 第一工業製薬
]


def get_price_range(price: float) -> str:
    """価格から価格帯ラベルを取得"""
    for pr in PRICE_RANGES:
        if pr["min"] <= price < pr["max"]:
            return pr["label"]
    return PRICE_RANGES[-1]["label"]


@router.get("/api/dev/stock-results/price-range")
async def get_price_range_stats(from_date: Optional[str] = None):
    """
    価格帯別の損益統計
    - ロング/ショート別
    - from_date: 開始日フィルタ (例: 2025-12-22)
    """
    df = load_stock_results()

    if df.empty:
        raise HTTPException(status_code=404, detail="No stock results data found")

    # 日付フィルタ
    if from_date:
        df = df[df["約定日"] >= from_date].copy()

    # 翌日以降の損切り銘柄を除外
    def should_exclude(row):
        date_str = row["約定日"].strftime("%Y-%m-%d")
        code = str(row["コード"])
        profit = row["実現損益"]
        for ex_date, ex_code, ex_profit in EXCLUDE_NON_SAME_DAY_LOSSES:
            if date_str == ex_date and code == ex_code and int(profit) == ex_profit:
                return True
        return False

    exclude_mask = df.apply(should_exclude, axis=1)
    df = df[~exclude_mask].copy()

    # 価格帯を追加
    df["price_range"] = df["平均取得価額"].apply(get_price_range)
    df["is_win"] = df["実現損益"] > 0

    def calc_stats(sub_df: pd.DataFrame) -> Dict[str, Any]:
        if len(sub_df) == 0:
            return {"count": 0, "win_count": 0, "win_rate": 0, "profit": 0, "avg_profit": 0}
        return {
            "count": int(len(sub_df)),
            "win_count": int(sub_df["is_win"].sum()),
            "win_rate": round(float(sub_df["is_win"].mean() * 100), 1),
            "profit": int(sub_df["実現損益"].sum()),
            "avg_profit": int(sub_df["実現損益"].mean()),
        }

    results = {"long": [], "short": []}

    for pr in PRICE_RANGES:
        label = pr["label"]
        pr_df = df[df["price_range"] == label]

        long_df = pr_df[pr_df["売買"] == "ロング"]
        short_df = pr_df[pr_df["売買"] == "ショート"]

        long_stats = calc_stats(long_df)
        long_stats["label"] = label
        results["long"].append(long_stats)

        short_stats = calc_stats(short_df)
        short_stats["label"] = label
        results["short"].append(short_stats)

    # 合計
    long_all = df[df["売買"] == "ロング"]
    short_all = df[df["売買"] == "ショート"]

    long_total = calc_stats(long_all)
    long_total["label"] = "合計"
    results["long"].append(long_total)

    short_total = calc_stats(short_all)
    short_total["label"] = "合計"
    results["short"].append(short_total)

    return {
        "priceRanges": [pr["label"] for pr in PRICE_RANGES],
        "long": results["long"],
        "short": results["short"],
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


# Review用5分足データディレクトリ
REVIEW_DIR = ROOT / "review"

# ticker名とファイル名のマッピング
TICKER_FILE_MAP = {
    "3683": "3683_T_5m_60d.parquet",
    "4393": "4393_T_5m_60d.parquet",
    "4564": "4564_T_5m_60d.parquet",
    "kudan": "kudan_5m_60d.parquet",
    "meneki": "meneki_5m_60d.parquet",
}


@router.get("/api/dev/stock-results/review/prices")
async def get_review_prices(ticker: str):
    """
    Review用5分足価格データを取得

    Parameters:
    - ticker: 銘柄コード（例: 3683, 4393, kudan）

    Returns:
    - List[PriceRow]: date, Open, High, Low, Close, Volume
    """
    # tickerからファイル名を決定
    # まずマップから検索
    file_name = TICKER_FILE_MAP.get(ticker)

    if not file_name:
        # マップにない場合は {ticker}_T_5m_60d.parquet または {ticker}_5m_60d.parquet を試す
        candidates = [
            f"{ticker}_T_5m_60d.parquet",
            f"{ticker}_5m_60d.parquet",
        ]
        for candidate in candidates:
            if (REVIEW_DIR / candidate).exists():
                file_name = candidate
                break

    if not file_name:
        raise HTTPException(
            status_code=404,
            detail=f"Review data not found for ticker: {ticker}"
        )

    file_path = REVIEW_DIR / file_name

    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"File not found: {file_name}"
        )

    try:
        df = pd.read_parquet(file_path)

        # インデックスがDatetimeの場合、カラムに変換
        if df.index.name == "Datetime" or isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            df.rename(columns={"Datetime": "date", "index": "date"}, inplace=True)

        # date列がない場合はインデックスから作成
        if "date" not in df.columns and df.index.dtype == "datetime64[ns, Asia/Tokyo]":
            df["date"] = df.index.strftime("%Y-%m-%d %H:%M:%S")
            df = df.reset_index(drop=True)

        # datetime型を文字列に変換
        if "date" in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df["date"]):
                df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # 必要なカラムのみ抽出
        result = []
        for _, row in df.iterrows():
            price_row = {
                "date": str(row.get("date", "")),
                "Open": float(row["Open"]),
                "High": float(row["High"]),
                "Low": float(row["Low"]),
                "Close": float(row["Close"]),
                "Volume": int(row["Volume"]) if pd.notna(row.get("Volume")) else None,
            }
            result.append(price_row)

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error reading parquet file: {str(e)}"
        )


@router.get("/api/dev/stock-results/review/tickers")
async def get_review_tickers():
    """
    利用可能なReview用ticker一覧を取得
    """
    if not REVIEW_DIR.exists():
        return {"tickers": []}

    tickers = []
    for file_path in REVIEW_DIR.glob("*_5m_60d.parquet"):
        file_name = file_path.stem  # 拡張子を除いたファイル名
        # {ticker}_T_5m_60d または {ticker}_5m_60d からtickerを抽出
        if "_T_5m_60d" in file_name:
            ticker = file_name.replace("_T_5m_60d", "")
        else:
            ticker = file_name.replace("_5m_60d", "")
        tickers.append(ticker)

    return {"tickers": sorted(tickers)}
