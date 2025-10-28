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


def load_backtest_files() -> List[Path]:
    """バックテストファイル一覧を取得（日付降順）"""
    if not BACKTEST_DIR.exists():
        return []

    files = list(BACKTEST_DIR.glob("grok_trending_*.parquet"))
    return sorted(files, reverse=True)


def load_backtest_data(file_path: Path) -> pd.DataFrame:
    """バックテストファイルを読み込み"""
    df = pd.read_parquet(file_path)

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
    files = load_backtest_files()

    if not files:
        raise HTTPException(status_code=404, detail="No backtest data found")

    daily_stats = []
    all_returns = []

    for file in files:
        df = load_backtest_data(file)
        stats = calculate_daily_stats(df)

        # 日付を抽出
        date_str = file.stem.replace("grok_trending_", "")
        try:
            backtest_date = datetime.strptime(date_str, "%Y%m%d").date()
        except:
            continue

        stats["date"] = backtest_date.isoformat()
        daily_stats.append(stats)

        # 有効なリターンを収集（カラムが存在する場合のみ）
        if 'phase1_return' in df.columns:
            valid_returns = df[df['phase1_return'].notna()]['phase1_return'].tolist()
            all_returns.extend(valid_returns)

    # 全期間統計
    total_trades = sum(s["valid_results"] for s in daily_stats)
    overall_avg_return = sum(all_returns) / len(all_returns) if all_returns else None
    overall_win_rate = (sum(1 for r in all_returns if r > 0) / len(all_returns) * 100) if all_returns else None
    overall_max_return = max(all_returns) if all_returns else None
    overall_min_return = min(all_returns) if all_returns else None

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
    backtest_dir = PARQUET_DIR / "backtest"
    file_path = backtest_dir / f"grok_trending_{date.replace('-', '')}.parquet"

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"No backtest data for {date}")

    df = load_backtest_data(file_path)
    stats = calculate_daily_stats(df)

    # データをJSON形式に変換
    records = []
    for _, row in df.iterrows():
        record = {
            "ticker": row.get("ticker"),
            "stock_name": row.get("stock_name"),
            "selection_score": float(row["selection_score"]) if pd.notna(row.get("selection_score")) else None,
            "grok_rank": int(row["grok_rank"]) if pd.notna(row.get("grok_rank")) else None,
            "reason": row.get("reason"),
            "selected_time": row.get("selected_time"),
            "buy_price": float(row["buy_price"]) if "buy_price" in df.columns and pd.notna(row.get("buy_price")) else None,
            "sell_price": float(row["sell_price"]) if "sell_price" in df.columns and pd.notna(row.get("sell_price")) else None,
            "phase1_return": float(row["phase1_return"]) if "phase1_return" in df.columns and pd.notna(row.get("phase1_return")) else None,
            "phase1_win": bool(row["phase1_win"]) if "phase1_win" in df.columns and pd.notna(row.get("phase1_win")) else None,
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
    files = load_backtest_files()

    if not files:
        raise HTTPException(status_code=404, detail="No backtest data found")

    latest_file = files[0]
    date_str = latest_file.stem.replace("grok_trending_", "")

    try:
        backtest_date = datetime.strptime(date_str, "%Y%m%d").date()
        return await get_daily_backtest(backtest_date.isoformat())
    except:
        raise HTTPException(status_code=500, detail="Failed to parse date")


@router.get("/api/dev/backtest/dates")
async def get_available_dates():
    """利用可能な日付一覧"""
    files = load_backtest_files()

    dates = []
    for file in files:
        date_str = file.stem.replace("grok_trending_", "")
        try:
            backtest_date = datetime.strptime(date_str, "%Y%m%d").date()
            dates.append(backtest_date.isoformat())
        except:
            continue

    return {"dates": dates}
