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
import os
import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

BACKTEST_DIR = PARQUET_DIR / "backtest"
ARCHIVE_FILE = BACKTEST_DIR / "grok_trending_archive.parquet"

# S3設定（環境変数から取得）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# グローバルキャッシュ（起動時に一度だけロード）
_archive_cache = None


def load_archive_data() -> pd.DataFrame:
    """
    アーカイブファイルを読み込み
    - S3から読み込み（本番環境、常に最新）
    - S3が失敗したらローカルファイルを使用（開発環境）
    """
    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/grok_trending_archive.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"

        print(f"[INFO] Loading backtest archive from S3: {s3_url}")

        # S3から直接読み込み（pandas.read_parquet はs3://をサポート）
        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })

        if 'backtest_date' in df.columns:
            df['backtest_date'] = pd.to_datetime(df['backtest_date'], format='mixed')

        print(f"[INFO] Successfully loaded {len(df)} records from S3")
        return df

    except Exception as e:
        print(f"[WARNING] Could not load backtest archive from S3: {type(e).__name__}: {e}")

    # ローカルファイルにフォールバック
    if ARCHIVE_FILE.exists():
        print(f"[INFO] Loading backtest archive from local file: {ARCHIVE_FILE}")

        # Docker volume deadlock回避: 直接 /tmp にコピーしてから読み込む
        import subprocess
        tmp_file = f"/tmp/{ARCHIVE_FILE.name}"
        try:
            result = subprocess.run(
                ["dd", f"if={ARCHIVE_FILE}", f"of={tmp_file}", "bs=1M"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            if result.returncode == 0:
                df = pd.read_parquet(tmp_file)
                if 'backtest_date' in df.columns:
                    df['backtest_date'] = pd.to_datetime(df['backtest_date'], format='mixed')
                print(f"[INFO] Successfully loaded {len(df)} records from local file via dd")
                return df
        except Exception as e:
            print(f"[ERROR] Failed to load via dd: {e}")

        # dd失敗時は直接読み込みを試行
        try:
            df = pd.read_parquet(ARCHIVE_FILE)
            if 'backtest_date' in df.columns:
                df['backtest_date'] = pd.to_datetime(df['backtest_date'], format='mixed')
            return df
        except Exception as e:
            print(f"[ERROR] Failed to load directly: {e}")

    # どちらも失敗
    print(f"[WARNING] Backtest archive not found in S3 or local file")
    return pd.DataFrame()


def calculate_daily_stats(df: pd.DataFrame, phase: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    日別統計を計算

    Args:
        df: データフレーム
        phase: phase1, phase2, phase3
        config: phase_configの設定
    """
    return_col = config["return_col"]
    win_col = config["win_col"]
    profit_col = config["profit_col"]

    # 指定されたphaseのreturnカラムが存在するかチェック
    if return_col not in df.columns:
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

    valid_results = df[return_col].notna()
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
    avg_return = float(df_valid[return_col].mean())
    win_count = (df_valid[win_col] == True).sum()
    win_rate = float(win_count / len(df_valid) * 100)
    max_return = float(df_valid[return_col].max())
    min_return = float(df_valid[return_col].min())

    # 累計損益（100株あたり）
    total_profit_per_100 = 0.0
    if profit_col in df_valid.columns:
        total_profit_per_100 = float(df_valid[profit_col].sum())

    # Top5統計
    if 'grok_rank' in df.columns:
        df_top5 = df[df['grok_rank'] <= 5]
        df_top5_valid = df_top5[df_top5[return_col].notna()]
    else:
        df_top5_valid = pd.DataFrame()

    top5_avg_return = None
    top5_win_rate = None
    top5_total_profit_per_100 = None

    if len(df_top5_valid) > 0:
        top5_avg_return = float(df_top5_valid[return_col].mean())
        top5_win_count = (df_top5_valid[win_col] == True).sum()
        top5_win_rate = float(top5_win_count / len(df_top5_valid) * 100)

        # Top5累計損益
        if profit_col in df_top5_valid.columns:
            top5_total_profit_per_100 = float(df_top5_valid[profit_col].sum())

    return {
        "total_stocks": len(df),
        "valid_results": len(df_valid),
        "avg_return": avg_return,
        "win_rate": win_rate,
        "max_return": max_return,
        "min_return": min_return,
        "total_profit_per_100": total_profit_per_100,
        "top5_avg_return": top5_avg_return,
        "top5_win_rate": top5_win_rate,
        "top5_total_profit_per_100": top5_total_profit_per_100,
    }


@router.get("/api/dev/backtest/summary")
async def get_backtest_summary(
    prompt_version: str | None = None,
    phase: str = "phase1"
):
    """
    バックテスト全体サマリー（ダッシュボード用の完全なデータ）

    Args:
        prompt_version: フィルタするプロンプトバージョン (例: "v1_0_baseline")
                       指定しない場合は全バージョンのデータを表示
        phase: 表示するPhase (phase1, phase2, phase3)
               - phase1: 前場引け売り（11:30売却）
               - phase2: 大引け売り（15:30売却）
               - phase3: +3%利確/-3%損切り
    """
    df_all = load_archive_data()

    if df_all.empty:
        raise HTTPException(status_code=404, detail="No backtest data found")

    # Phaseに応じたカラムマッピング
    phase_config = {
        "phase1": {
            "return_col": "phase1_return",
            "win_col": "phase1_win",
            "profit_col": "profit_per_100_shares_phase1",
            "description": "前場引け売り（11:30売却）"
        },
        "phase2": {
            "return_col": "phase2_return",
            "win_col": "phase2_win",
            "profit_col": "profit_per_100_shares_phase2",
            "description": "大引け売り（15:30売却）"
        },
        "phase3": {
            "return_col": "phase3_3pct_return",
            "win_col": "phase3_3pct_win",
            "profit_col": "profit_per_100_shares_phase3_3pct",
            "description": "+3%利確/-3%損切り"
        }
    }

    # Phase検証
    if phase not in phase_config:
        raise HTTPException(status_code=400, detail=f"Invalid phase: {phase}. Must be one of: {list(phase_config.keys())}")

    config = phase_config[phase]
    return_col = config["return_col"]
    win_col = config["win_col"]
    profit_col = config["profit_col"]

    # 利用可能なバージョン一覧を取得
    available_versions = []
    if 'prompt_version' in df_all.columns:
        available_versions = sorted(df_all['prompt_version'].unique().tolist())

    # バージョンフィルタを適用
    current_version = prompt_version if prompt_version else "all"
    if prompt_version and 'prompt_version' in df_all.columns:
        df_all = df_all[df_all['prompt_version'] == prompt_version].copy()
        if df_all.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for version: {prompt_version}"
            )

    # 全体の有効なレコード（選択されたPhaseのデータがあるもの）
    df_valid = df_all[df_all[return_col].notna()].copy()

    if len(df_valid) == 0:
        raise HTTPException(status_code=404, detail=f"No valid backtest results found for {phase}")

    # === 全体統計 ===
    all_returns = df_valid[return_col].tolist()
    all_profits = df_valid[profit_col].tolist() if profit_col in df_valid.columns else ((df_valid['sell_price'] - df_valid['buy_price']) * 100).tolist()

    overall_stats = {
        "total_count": len(df_all),
        "valid_count": len(df_valid),
        "win_count": int((df_valid[win_col] == True).sum()),
        "lose_count": int((df_valid[win_col] == False).sum()),
        "win_rate": float((df_valid[win_col] == True).sum() / len(df_valid) * 100),
        "avg_return": float(sum(all_returns) / len(all_returns) * 100),
        "median_return": float(df_valid[return_col].median() * 100),
        "std_return": float(df_valid[return_col].std() * 100),
        "best_return": float(max(all_returns) * 100),
        "worst_return": float(min(all_returns) * 100),
        "avg_profit_per_100_shares": float(sum(all_profits) / len(all_profits)),
        "total_profit_per_100_shares": float(sum(all_profits)),
        "best_profit_per_100_shares": float(max(all_profits)),
        "worst_profit_per_100_shares": float(min(all_profits)),
        "total_days": int(df_all['backtest_date'].nunique()),
        "phase": phase,
        "phase_description": config["description"],
    }

    # === Top5統計 ===
    df_top5 = df_all[df_all['grok_rank'] <= 5]
    df_top5_valid = df_top5[df_top5[return_col].notna()].copy()

    if len(df_top5_valid) > 0:
        top5_returns = df_top5_valid[return_col].tolist()
        top5_profits = df_top5_valid[profit_col].tolist() if profit_col in df_top5_valid.columns else ((df_top5_valid['sell_price'] - df_top5_valid['buy_price']) * 100).tolist()

        top5_stats = {
            "total_count": len(df_top5),
            "valid_count": len(df_top5_valid),
            "win_count": int((df_top5_valid[win_col] == True).sum()),
            "lose_count": int((df_top5_valid[win_col] == False).sum()),
            "win_rate": float((df_top5_valid[win_col] == True).sum() / len(df_top5_valid) * 100),
            "avg_return": float(sum(top5_returns) / len(top5_returns) * 100),
            "median_return": float(df_top5_valid[return_col].median() * 100),
            "std_return": float(df_top5_valid[return_col].std() * 100),
            "best_return": float(max(top5_returns) * 100),
            "worst_return": float(min(top5_returns) * 100),
            "avg_profit_per_100_shares": float(sum(top5_profits) / len(top5_profits)),
            "total_profit_per_100_shares": float(sum(top5_profits)),
            "best_profit_per_100_shares": float(max(top5_profits)),
            "worst_profit_per_100_shares": float(min(top5_profits)),
            "outperformance": float((sum(top5_returns) / len(top5_returns) - sum(all_returns) / len(all_returns)) * 100),
            "outperformance_profit_per_100_shares": float(sum(top5_profits) / len(top5_profits) - sum(all_profits) / len(all_profits)),
        }
    else:
        top5_stats = {
            "total_count": 0,
            "valid_count": 0,
            "win_count": 0,
            "lose_count": 0,
            "win_rate": 0,
            "avg_return": 0,
            "median_return": 0,
            "std_return": 0,
            "best_return": 0,
            "worst_return": 0,
            "avg_profit_per_100_shares": 0,
            "total_profit_per_100_shares": 0,
            "best_profit_per_100_shares": 0,
            "worst_profit_per_100_shares": 0,
            "outperformance": 0,
            "outperformance_profit_per_100_shares": 0,
        }

    # === 日次統計 ===
    daily_groups = df_all.groupby(df_all['backtest_date'].dt.date)
    daily_stats_list = []

    for backtest_date, df_day in daily_groups:
        df_day_valid = df_day[df_day[return_col].notna()]

        if len(df_day_valid) > 0:
            win_count = (df_day_valid[win_col] == True).sum()
            day_profits = df_day_valid[profit_col].tolist() if profit_col in df_day_valid.columns else ((df_day_valid['sell_price'] - df_day_valid['buy_price']) * 100).tolist()

            # Top5のデータを計算
            df_day_top5 = df_day_valid.nsmallest(5, 'grok_rank') if len(df_day_valid) >= 5 else df_day_valid
            top5_profits = df_day_top5[profit_col].tolist() if profit_col in df_day_top5.columns else ((df_day_top5['sell_price'] - df_day_top5['buy_price']) * 100).tolist()
            top5_win_count = (df_day_top5[win_col] == True).sum()

            daily_stats_list.append({
                "date": backtest_date.isoformat(),
                "win_rate": float(win_count / len(df_day_valid) * 100),
                "avg_return": float(df_day_valid[return_col].mean() * 100),
                "count": len(df_day_valid),
                "total_profit_per_100": float(sum(day_profits)),
                "top5_total_profit_per_100": float(sum(top5_profits)),
                "top5_avg_return": float(df_day_top5[return_col].mean() * 100),
                "top5_win_rate": float(top5_win_count / len(df_day_top5) * 100),
            })

    # 日付でソート
    daily_stats_list.sort(key=lambda x: x["date"])

    # 累積損益を計算
    cumulative_profit = 0.0
    cumulative_top5_profit = 0.0
    for stat in daily_stats_list:
        cumulative_profit += stat["total_profit_per_100"]
        cumulative_top5_profit += stat["top5_total_profit_per_100"]
        stat["cumulative_profit_per_100"] = float(cumulative_profit)
        stat["cumulative_top5_profit_per_100"] = float(cumulative_top5_profit)

    # === トレンド分析 ===
    if len(daily_stats_list) > 0:
        recent_days = daily_stats_list[-5:]
        recent_avg = sum(d["win_rate"] for d in recent_days) / len(recent_days)
        overall_avg = sum(d["win_rate"] for d in daily_stats_list) / len(daily_stats_list)
        change = ((recent_avg - overall_avg) / abs(overall_avg) * 100) if overall_avg != 0 else 0

        if change > 10:
            trend = "improving"
        elif change < -10:
            trend = "declining"
        else:
            trend = "stable"

        trend_analysis = {
            "trend": trend,
            "recent_avg": recent_avg,
            "overall_avg": overall_avg,
            "change": change,
        }
    else:
        trend_analysis = {
            "trend": "stable",
            "recent_avg": 0,
            "overall_avg": 0,
            "change": 0,
        }

    # === アラート生成 ===
    alerts = []

    if overall_stats["win_rate"] < 40:
        alerts.append({
            "type": "danger",
            "title": "⚠️ 勝率が低下しています",
            "message": f"現在の勝率: {overall_stats['win_rate']:.1f}%。戦略の見直しを検討してください。",
            "action": "戦略を見直す",
        })
    elif overall_stats["win_rate"] >= 60:
        alerts.append({
            "type": "success",
            "title": "✅ 高い勝率を維持",
            "message": f"現在の勝率: {overall_stats['win_rate']:.1f}%。戦略は順調です。",
        })

    if trend_analysis["trend"] == "declining":
        alerts.append({
            "type": "warning",
            "title": "📉 パフォーマンスが低下傾向",
            "message": f"直近5日の平均リターン: {trend_analysis['recent_avg']:.2f}%（全期間: {trend_analysis['overall_avg']:.2f}%）",
            "action": "様子見を推奨",
        })
    elif trend_analysis["trend"] == "improving":
        alerts.append({
            "type": "success",
            "title": "📈 パフォーマンスが改善中",
            "message": f"直近5日の平均リターン: {trend_analysis['recent_avg']:.2f}%（全期間: {trend_analysis['overall_avg']:.2f}%）",
        })

    if top5_stats["outperformance"] > 0.5:
        alerts.append({
            "type": "success",
            "title": "⭐ Top5銘柄への絞り込みを推奨",
            "message": f"Top5は全体より平均{top5_stats['outperformance']:.2f}%高いリターンを記録しています。",
            "action": "Top5のみにトレード",
        })

    if overall_stats["valid_count"] < 10:
        alerts.append({
            "type": "warning",
            "title": "📊 データが不足しています",
            "message": f"有効なバックテスト結果: {overall_stats['valid_count']}件。統計的な信頼性を高めるため、より多くのデータが必要です。",
        })

    # === 直近レコード ===
    recent_records = df_all.sort_values('backtest_date', ascending=False).head(10).to_dict(orient='records')

    # NaN, NaT, Timestamp を JSON シリアライズ可能な形式に変換
    for record in recent_records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, pd.Timestamp):
                record[key] = value.isoformat()

    return {
        "overall_stats": overall_stats,
        "top5_stats": top5_stats,
        "daily_stats": daily_stats_list,
        "recent_records": recent_records,
        "trend_analysis": trend_analysis,
        "alerts": alerts,
        "available_versions": available_versions,
        "current_version": current_version,
    }


@router.get("/api/dev/backtest/daily/{date}")
async def get_daily_backtest(
    date: str,
    phase: str = "phase2"
):
    """
    特定日のバックテスト詳細

    Args:
        date: バックテスト日付 (YYYY-MM-DD)
        phase: 表示するPhase (phase1, phase2, phase3)
               - phase1: 前場引け売り（11:30売却）
               - phase2: 大引け売り（15:30売却）
               - phase3: +3%利確/-3%損切り
    """
    df_all = load_archive_data()

    if df_all.empty:
        raise HTTPException(status_code=404, detail="No backtest data found")

    # 指定日付のデータを抽出
    df = df_all[df_all['backtest_date'].dt.date == pd.to_datetime(date).date()]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No backtest data for {date}")

    # Phaseに応じたカラムマッピング
    phase_config = {
        "phase1": {
            "return_col": "phase1_return",
            "win_col": "phase1_win",
            "profit_col": "profit_per_100_shares_phase1",
            "sell_col": "sell_price",  # 前場引け値
            "description": "前場引け売り（11:30売却）"
        },
        "phase2": {
            "return_col": "phase2_return",
            "win_col": "phase2_win",
            "profit_col": "profit_per_100_shares_phase2",
            "sell_col": "daily_close",  # 大引け値
            "description": "大引け売り（15:30売却）"
        },
        "phase3": {
            "return_col": "phase3_3pct_return",
            "win_col": "phase3_3pct_win",
            "profit_col": "profit_per_100_shares_phase3_3pct",
            "sell_col": None,  # Phase3は動的に決まるため個別計算
            "description": "+3%利確/-3%損切り"
        }
    }

    # Phase検証
    if phase not in phase_config:
        raise HTTPException(status_code=400, detail=f"Invalid phase: {phase}. Must be one of: {list(phase_config.keys())}")

    config = phase_config[phase]

    stats = calculate_daily_stats(df, phase, config)

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
    return_col = config["return_col"]
    win_col = config["win_col"]
    profit_col = config["profit_col"]
    sell_col = config["sell_col"]

    records = []
    for _, row in df.iterrows():
        buy_price = float(row["buy_price"]) if "buy_price" in df.columns and pd.notna(row.get("buy_price")) else None

        # Phaseに応じた売値を取得
        if sell_col:
            sell_price = float(row[sell_col]) if sell_col in df.columns and pd.notna(row.get(sell_col)) else None
        else:
            # Phase3の場合は、利確/損切/EODのいずれかなので、buy_price + returnから計算
            if buy_price and return_col in df.columns and pd.notna(row.get(return_col)):
                sell_price = buy_price * (1 + row[return_col])
            else:
                sell_price = None

        # Phaseに応じたリターンと勝敗
        phase_return = float(row[return_col] * 100) if return_col in df.columns and pd.notna(row.get(return_col)) else None
        phase_win = bool(row[win_col]) if win_col in df.columns and pd.notna(row.get(win_col)) else None

        # 100株あたりの利益額を計算
        profit_per_100 = None
        if profit_col in df.columns and pd.notna(row.get(profit_col)):
            profit_per_100 = float(row[profit_col])
        elif buy_price is not None and sell_price is not None:
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
            "phase_return": phase_return,
            "phase_win": phase_win,
            "profit_per_100": profit_per_100,
            "morning_high": float(row["morning_high"]) if "morning_high" in df.columns and pd.notna(row.get("morning_high")) else None,
            "morning_low": float(row["morning_low"]) if "morning_low" in df.columns and pd.notna(row.get("morning_low")) else None,
            "morning_max_gain_pct": float(row["morning_max_gain_pct"]) if "morning_max_gain_pct" in df.columns and pd.notna(row.get("morning_max_gain_pct")) else None,
            "morning_max_drawdown_pct": float(row["morning_max_drawdown_pct"]) if "morning_max_drawdown_pct" in df.columns and pd.notna(row.get("morning_max_drawdown_pct")) else None,
            "high": float(row["high"]) if "high" in df.columns and pd.notna(row.get("high")) else None,
            "low": float(row["low"]) if "low" in df.columns and pd.notna(row.get("low")) else None,
            "daily_max_gain_pct": float(row["daily_max_gain_pct"]) if "daily_max_gain_pct" in df.columns and pd.notna(row.get("daily_max_gain_pct")) else None,
            "daily_max_drawdown_pct": float(row["daily_max_drawdown_pct"]) if "daily_max_drawdown_pct" in df.columns and pd.notna(row.get("daily_max_drawdown_pct")) else None,
            "morning_volume": int(row["morning_volume"]) if "morning_volume" in df.columns and pd.notna(row.get("morning_volume")) else None,
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
