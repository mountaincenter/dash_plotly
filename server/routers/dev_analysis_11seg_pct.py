"""
開発用: 11時間区分分析API（変化率版）

improvement/grok_trending_archive_11seg.parquet を使用した分析
- GET /dev/analysis-11seg-pct/summary: 11時間区分サマリー（変化率）
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import os

router = APIRouter()

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"

# 価格帯定義
PRICE_RANGES = [
    {"label": "~1,000円", "min": 0, "max": 1000},
    {"label": "1,000~3,000円", "min": 1000, "max": 3000},
    {"label": "3,000~5,000円", "min": 3000, "max": 5000},
    {"label": "5,000~10,000円", "min": 5000, "max": 10000},
    {"label": "10,000円~", "min": 10000, "max": float("inf")},
]

# 曜日名
WEEKDAY_NAMES = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]

# 11時間区分定義
TIME_SEGMENTS = [
    {"key": "seg_0930", "label": "-9:30", "time": "9:30"},
    {"key": "seg_1000", "label": "9:30-10:00", "time": "10:00"},
    {"key": "seg_1030", "label": "10:00-10:30", "time": "10:30"},
    {"key": "seg_1100", "label": "10:30-11:00", "time": "11:00"},
    {"key": "seg_1130", "label": "11:00-11:30", "time": "11:30"},
    {"key": "seg_1300", "label": "12:30-13:00", "time": "13:00"},
    {"key": "seg_1330", "label": "13:00-13:30", "time": "13:30"},
    {"key": "seg_1400", "label": "13:30-14:00", "time": "14:00"},
    {"key": "seg_1430", "label": "14:00-14:30", "time": "14:30"},
    {"key": "seg_1500", "label": "14:30-15:00", "time": "15:00"},
    {"key": "seg_1530", "label": "15:00-15:30", "time": "15:30"},
]


def load_archive() -> pd.DataFrame:
    """grok_trending_archive_11seg.parquetを読み込み"""
    if ARCHIVE_PATH.exists():
        df = pd.read_parquet(ARCHIVE_PATH)
    else:
        raise HTTPException(status_code=500, detail="11segアーカイブが見つかりません")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """データ前処理"""
    df = df[df["buy_price"].notna()].copy()

    if "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])
    else:
        raise HTTPException(status_code=500, detail="日付カラムが見つかりません")

    df = df[df["date"] >= "2025-11-04"]
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    df["weekday"] = df["date"].dt.weekday
    df["weekday_name"] = df["weekday"].map(lambda x: WEEKDAY_NAMES[x] if x < 5 else "")
    df["margin_type"] = df.apply(lambda r: "制度信用" if r["shortable"] else "いちにち信用", axis=1)

    df["is_ex0"] = df.apply(
        lambda r: True if r["shortable"] else (
            pd.isna(r.get("day_trade_available_shares")) or r.get("day_trade_available_shares", 0) > 0
        ),
        axis=1
    )

    def get_price_range(price):
        for pr in PRICE_RANGES:
            if pr["min"] <= price < pr["max"]:
                return pr["label"]
        return PRICE_RANGES[-1]["label"]

    df["price_range"] = df["buy_price"].apply(get_price_range)

    # 11seg: ショート基準に符号反転 + 変化率カラムを追加
    for seg in TIME_SEGMENTS:
        key = seg["key"]
        if key in df.columns:
            df[key] = -df[key]
        df[f"{key}_pct"] = df.apply(
            lambda r: r[key] / r["buy_price"] if pd.notna(r[key]) and r["buy_price"] > 0 else np.nan,
            axis=1
        )
        df[f"{key}_win"] = df[key] > 0

    return df


def calc_segment_stats_pct(df: pd.DataFrame) -> dict:
    """11時間区分の統計計算（変化率版）"""
    if len(df) == 0:
        return {seg["key"]: {"pct": 0, "winRate": 0, "count": 0} for seg in TIME_SEGMENTS}

    result = {}
    for seg in TIME_SEGMENTS:
        key = seg["key"]
        pct_key = f"{key}_pct"
        valid = df[pct_key].dropna()
        valid_profit = df[key].dropna()
        if len(valid) > 0:
            result[key] = {
                "pct": round(valid.mean(), 2),  # 平均変化率
                "winRate": round((valid_profit > 0).mean() * 100, 1),
                "count": len(valid),
            }
        else:
            result[key] = {"pct": 0, "winRate": 0, "count": 0}

    return result


def calc_weekday_data(df: pd.DataFrame) -> list:
    """曜日別データ"""
    result = []

    for wd in range(5):
        wd_df = df[df["weekday"] == wd]
        wd_name = WEEKDAY_NAMES[wd]

        # 制度信用
        seido_df = wd_df[wd_df["margin_type"] == "制度信用"]
        seido_data = {
            "type": "制度信用",
            "count": len(seido_df),
            "segments": calc_segment_stats_pct(seido_df),
            "priceRanges": [],
        }

        for pr in PRICE_RANGES:
            pr_df = seido_df[seido_df["price_range"] == pr["label"]]
            pr_data = {
                "label": pr["label"],
                "count": len(pr_df),
                "segments": calc_segment_stats_pct(pr_df),
            }
            seido_data["priceRanges"].append(pr_data)

        # いちにち信用
        ichinichi_df = wd_df[wd_df["margin_type"] == "いちにち信用"]
        ichinichi_ex0_df = ichinichi_df[ichinichi_df["is_ex0"]]

        ichinichi_data = {
            "type": "いちにち信用",
            "count": {"all": len(ichinichi_df), "ex0": len(ichinichi_ex0_df)},
            "segments": {
                "all": calc_segment_stats_pct(ichinichi_df),
                "ex0": calc_segment_stats_pct(ichinichi_ex0_df),
            },
            "priceRanges": {"all": [], "ex0": []},
        }

        for pr in PRICE_RANGES:
            pr_df = ichinichi_df[ichinichi_df["price_range"] == pr["label"]]
            pr_ex0_df = pr_df[pr_df["is_ex0"]]

            ichinichi_data["priceRanges"]["all"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "segments": calc_segment_stats_pct(pr_df),
            })
            ichinichi_data["priceRanges"]["ex0"].append({
                "label": pr["label"],
                "count": len(pr_ex0_df),
                "segments": calc_segment_stats_pct(pr_ex0_df),
            })

        result.append({
            "weekday": wd_name,
            "seido": seido_data,
            "ichinichi": ichinichi_data,
        })

    return result


@router.get("/dev/analysis-11seg-pct/summary")
async def get_11seg_pct_summary():
    """
    11時間区分サマリーAPI（変化率版）

    Returns:
    - timeSegments: 時間区分定義
    - overall: 全体統計（変化率）
    - weekdays: 曜日別データ（変化率）
    """
    df_raw = load_archive()
    df = prepare_data(df_raw.copy())

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    # 全体統計（all / ex0）
    df_ex0 = df[df["is_ex0"]]
    overall = {
        "count": {"all": len(df), "ex0": len(df_ex0)},
        "seidoCount": int((df["margin_type"] == "制度信用").sum()),
        "ichinichiCount": {"all": int((df["margin_type"] == "いちにち信用").sum()), "ex0": int((df_ex0["margin_type"] == "いちにち信用").sum())},
        "segments": {
            "all": calc_segment_stats_pct(df),
            "ex0": calc_segment_stats_pct(df_ex0),
        },
    }

    # 曜日別
    weekdays = calc_weekday_data(df)

    # 期間
    date_range = {
        "from": df["date"].min().strftime("%Y-%m-%d"),
        "to": df["date"].max().strftime("%Y-%m-%d"),
        "tradingDays": df["date"].nunique(),
    }

    return JSONResponse(content={
        "generatedAt": datetime.now().isoformat(),
        "timeSegments": TIME_SEGMENTS,
        "priceRanges": [pr["label"] for pr in PRICE_RANGES],
        "dateRange": date_range,
        "overall": overall,
        "weekdays": weekdays,
    })
