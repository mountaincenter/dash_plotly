# server/routers/dev_backtest.py
"""
開発者向けGROKバックテスト結果API
/api/dev/backtest/* - JSON APIエンドポイント
"""
from fastapi import APIRouter, HTTPException
from pathlib import Path
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

BACKTEST_DIR = PARQUET_DIR / "backtest"
ARCHIVE_FILE = BACKTEST_DIR / "grok_trending_archive.parquet"


def load_archive_data() -> pd.DataFrame:
    """アーカイブファイルを読み込み"""
    if not ARCHIVE_FILE.exists():
        return pd.DataFrame()

    df = pd.read_parquet(ARCHIVE_FILE)

    # backtest_dateをdatetime型に変換
    if 'backtest_date' in df.columns:
        df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    return df


def calculate_daily_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """日別統計を計算"""
    # phase1_returnカラムが存在するかチェック
    if 'phase1_return' not in df.columns:
        return {
            "total_stocks": len(df),
            "valid_results": 0,
            "avg_return": None,
            "win_rate": None,
            "max_return": None,
            "min_return": None,
            "top5_avg_return": None,
            "top5_win_rate": None,
        }

    valid_results = df['phase1_return'].notna()
    df_valid = df[valid_results]

    if len(df_valid) == 0:
        return {
            "total_stocks": len(df),
            "valid_results": 0,
            "avg_return": None,
            "win_rate": None,
            "max_return": None,
            "min_return": None,
            "top5_avg_return": None,
            "top5_win_rate": None,
        }

    # 全体統計
    avg_return = float(df_valid['phase1_return'].mean())
    win_count = (df_valid['phase1_win'] == True).sum()
    win_rate = float(win_count / len(df_valid) * 100)
    max_return = float(df_valid['phase1_return'].max())
    min_return = float(df_valid['phase1_return'].min())

    # Top5統計
    if 'grok_rank' in df.columns:
        df_top5 = df[df['grok_rank'] <= 5]
        df_top5_valid = df_top5[df_top5['phase1_return'].notna()]
    else:
        df_top5_valid = pd.DataFrame()

    top5_avg_return = None
    top5_win_rate = None

    if len(df_top5_valid) > 0:
        top5_avg_return = float(df_top5_valid['phase1_return'].mean())
        top5_win_count = (df_top5_valid['phase1_win'] == True).sum()
        top5_win_rate = float(top5_win_count / len(df_top5_valid) * 100)

    return {
        "total_stocks": len(df),
        "valid_results": len(df_valid),
        "avg_return": avg_return,
        "win_rate": win_rate,
        "max_return": max_return,
        "min_return": min_return,
        "top5_avg_return": top5_avg_return,
        "top5_win_rate": top5_win_rate,
    }


@router.get("/api/dev/backtest/summary")
async def get_backtest_summary():
    """バックテスト全体サマリー"""
    df_all = load_archive_data()

    if df_all.empty:
        raise HTTPException(status_code=404, detail="No backtest data found")

    # 日付でグループ化
    daily_groups = df_all.groupby(df_all['backtest_date'].dt.date)

    daily_stats = []
    all_returns = []

    for backtest_date, df_day in daily_groups:
        stats = calculate_daily_stats(df_day)
        stats["date"] = backtest_date.isoformat()
        daily_stats.append(stats)

        # 有効なリターンを収集
        if 'phase1_return' in df_day.columns:
            valid_returns = df_day[df_day['phase1_return'].notna()]['phase1_return'].tolist()
            all_returns.extend(valid_returns)

    # 日付降順でソート
    daily_stats = sorted(daily_stats, key=lambda x: x["date"], reverse=True)

    # 全期間統計（リターンは小数形式なので100倍してパーセント表示）
    total_trades = sum(s["valid_results"] for s in daily_stats)
    overall_avg_return = (sum(all_returns) / len(all_returns) * 100) if all_returns else None
    overall_win_rate = (sum(1 for r in all_returns if r > 0) / len(all_returns) * 100) if all_returns else None
    overall_max_return = (max(all_returns) * 100) if all_returns else None
    overall_min_return = (min(all_returns) * 100) if all_returns else None

    # 日次統計もパーセント表示に変換
    for stats in daily_stats:
        if stats["avg_return"] is not None:
            stats["avg_return"] *= 100
        if stats["max_return"] is not None:
            stats["max_return"] *= 100
        if stats["min_return"] is not None:
            stats["min_return"] *= 100
        if stats["top5_avg_return"] is not None:
            stats["top5_avg_return"] *= 100

    return {
        "overall": {
            "total_trades": total_trades,
            "avg_return": overall_avg_return,
            "win_rate": overall_win_rate,
            "max_return": overall_max_return,
            "min_return": overall_min_return,
            "total_days": len(daily_stats),
        },
        "daily_stats": daily_stats,
    }


@router.get("/api/dev/backtest/daily/{date}")
async def get_daily_backtest(date: str):
    """特定日のバックテスト詳細"""
    df_all = load_archive_data()

    if df_all.empty:
        raise HTTPException(status_code=404, detail="No backtest data found")

    # 指定日付のデータを抽出
    df = df_all[df_all['backtest_date'].dt.date == pd.to_datetime(date).date()]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No backtest data for {date}")

    stats = calculate_daily_stats(df)

    # 統計をパーセント表示に変換
    if stats["avg_return"] is not None:
        stats["avg_return"] *= 100
    if stats["max_return"] is not None:
        stats["max_return"] *= 100
    if stats["min_return"] is not None:
        stats["min_return"] *= 100
    if stats["top5_avg_return"] is not None:
        stats["top5_avg_return"] *= 100

    # データをJSON形式に変換
    records = []
    for _, row in df.iterrows():
        buy_price = float(row["buy_price"]) if "buy_price" in df.columns and pd.notna(row.get("buy_price")) else None
        sell_price = float(row["sell_price"]) if "sell_price" in df.columns and pd.notna(row.get("sell_price")) else None
        phase1_return = float(row["phase1_return"] * 100) if "phase1_return" in df.columns and pd.notna(row.get("phase1_return")) else None

        # 100株あたりの利益額を計算
        profit_per_100 = None
        if buy_price is not None and sell_price is not None:
            profit_per_100 = (sell_price - buy_price) * 100

        record = {
            "ticker": row.get("ticker"),
            "stock_name": row.get("stock_name"),
            "selection_score": float(row["selection_score"]) if pd.notna(row.get("selection_score")) else None,
            "grok_rank": int(row["grok_rank"]) if pd.notna(row.get("grok_rank")) else None,
            "reason": row.get("reason"),
            "selected_time": row.get("selected_time"),
            "buy_price": buy_price,
            "sell_price": sell_price,
            "phase1_return": phase1_return,
            "phase1_win": bool(row["phase1_win"]) if "phase1_win" in df.columns and pd.notna(row.get("phase1_win")) else None,
            "profit_per_100": profit_per_100,
        }
        records.append(record)

    return {
        "date": date,
        "stats": stats,
        "results": records,
    }


@router.get("/api/dev/backtest/latest")
async def get_latest_backtest():
    """最新のバックテスト結果"""
    df_all = load_archive_data()

    if df_all.empty:
        raise HTTPException(status_code=404, detail="No backtest data found")

    # 最新日付を取得
    latest_date = df_all['backtest_date'].max().date()
    return await get_daily_backtest(latest_date.isoformat())


@router.get("/api/dev/backtest/dates")
async def get_available_dates():
    """利用可能な日付一覧"""
    df_all = load_archive_data()

    if df_all.empty:
        return {"dates": []}

    # ユニークな日付を取得してソート
    dates = sorted(df_all['backtest_date'].dt.date.unique(), reverse=True)
    dates_str = [d.isoformat() for d in dates]

    return {"dates": dates_str}
