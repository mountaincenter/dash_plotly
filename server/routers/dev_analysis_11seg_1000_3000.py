"""
開発用: 1000-3000円価格帯特化 11時間区分分析API

improvement/grok_trending_archive_11seg.parquet を使用した分析
- GET /dev/analysis-11seg-1000-3000/summary: 1000-3000円特化サマリー
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
from datetime import datetime
import os

router = APIRouter()

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "improvement" / "grok_trending_archive_11seg.parquet"

# 異常日（極端相場）
EXTREME_DATES = [
    "2026-01-13",  # 日経-2.66%
    "2026-01-15",  # 前日1/14が日経+3.1%の大幅上昇、当日ショート-3.64%の異常日
]

# 価格帯定義（1000-3000円を細分化）
PRICE_RANGES = [
    {"label": "1,000~1,500円", "min": 1000, "max": 1500},
    {"label": "1,500~2,000円", "min": 1500, "max": 2000},
    {"label": "2,000~2,500円", "min": 2000, "max": 2500},
    {"label": "2,500~3,000円", "min": 2500, "max": 3000},
]

# 曜日名
WEEKDAY_NAMES = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]

# 4区分定義
# invert=True: ロング基準のカラムをショート基準に変換（符号反転）
FOUR_SEGMENTS = [
    {"key": "seg_1025", "label": "10:25", "time": "10:25", "col": "profit_per_100_shares_morning_early", "invert": True},
    {"key": "seg_1130", "label": "前場引け", "time": "11:30", "col": "seg_1130", "invert": False},
    {"key": "seg_1445", "label": "14:45", "time": "14:45", "col": "profit_per_100_shares_afternoon_early", "invert": True},
    {"key": "seg_1530", "label": "大引け", "time": "15:30", "col": "seg_1530", "invert": False},
]

# 11時間区分定義
ELEVEN_SEGMENTS = [
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


def prepare_data(df: pd.DataFrame, exclude_extreme: bool = False) -> pd.DataFrame:
    """データ前処理（1000-3000円のみフィルタ）

    Args:
        df: 入力DataFrame
        exclude_extreme: True の場合、異常日（1/13, 1/15）を除外
    """
    # フィルタ: buy_priceがあるもののみ
    df = df[df["buy_price"].notna()].copy()

    # 日付カラム正規化
    if "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])
    else:
        raise HTTPException(status_code=500, detail="日付カラムが見つかりません")

    # 2025-11-04以降のみ
    df = df[df["date"] >= "2025-11-04"]

    # 異常日除外
    if exclude_extreme:
        extreme_dates = pd.to_datetime(EXTREME_DATES)
        df = df[~df["date"].isin(extreme_dates)]

    # 制度信用 or いちにち信用のみ
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    # 1000-3000円のみフィルタ
    df = df[(df["buy_price"] >= 1000) & (df["buy_price"] < 3000)]

    # 曜日
    df["weekday"] = df["date"].dt.weekday
    df["weekday_name"] = df["weekday"].map(lambda x: WEEKDAY_NAMES[x] if x < 5 else "")

    # 信用区分
    df["margin_type"] = df.apply(lambda r: "制度信用" if r["shortable"] else "いちにち信用", axis=1)

    # 除0株フラグ
    df["is_ex0"] = df.apply(
        lambda r: True if r["shortable"] else (
            pd.isna(r.get("day_trade_available_shares")) or r.get("day_trade_available_shares", 0) > 0
        ),
        axis=1
    )

    # 価格帯
    def get_price_range(price):
        for pr in PRICE_RANGES:
            if pr["min"] <= price < pr["max"]:
                return pr["label"]
        return None

    df["price_range"] = df["buy_price"].apply(get_price_range)
    df = df[df["price_range"].notna()]

    return df


def calc_segment_stats_4seg(df: pd.DataFrame) -> dict:
    """4区分の統計計算"""
    if len(df) == 0:
        return {seg["key"]: {"profit": 0, "winRate": 0, "count": 0, "mean": 0} for seg in FOUR_SEGMENTS}

    result = {}
    for seg in FOUR_SEGMENTS:
        col = seg["col"]
        invert = seg.get("invert", False)
        if col not in df.columns:
            result[seg["key"]] = {"profit": 0, "winRate": 0, "count": 0, "mean": 0}
            continue

        valid = df[col].dropna()
        if invert:
            valid = -valid  # ロング基準→ショート基準に変換
        if len(valid) > 0:
            result[seg["key"]] = {
                "profit": int(valid.sum()),
                "winRate": round((valid > 0).mean() * 100, 1),
                "count": len(valid),
                "mean": int(valid.mean()),
            }
        else:
            result[seg["key"]] = {"profit": 0, "winRate": 0, "count": 0, "mean": 0}

    return result


def calc_segment_stats_11seg(df: pd.DataFrame) -> dict:
    """11時間区分の統計計算"""
    if len(df) == 0:
        return {seg["key"]: {"profit": 0, "winRate": 0, "count": 0, "mean": 0} for seg in ELEVEN_SEGMENTS}

    result = {}
    for seg in ELEVEN_SEGMENTS:
        key = seg["key"]
        if key not in df.columns:
            result[key] = {"profit": 0, "winRate": 0, "count": 0, "mean": 0}
            continue

        valid = df[key].dropna()
        if len(valid) > 0:
            result[key] = {
                "profit": int(valid.sum()),
                "winRate": round((valid > 0).mean() * 100, 1),
                "count": len(valid),
                "mean": int(valid.mean()),
            }
        else:
            result[key] = {"profit": 0, "winRate": 0, "count": 0, "mean": 0}

    return result


def calc_weekday_data(df: pd.DataFrame) -> list:
    """曜日別データ（4区分と11seg両方）"""
    result = []

    for wd in range(5):
        wd_df = df[df["weekday"] == wd]
        wd_name = WEEKDAY_NAMES[wd]

        # 制度信用
        seido_df = wd_df[wd_df["margin_type"] == "制度信用"]
        seido_data = {
            "type": "制度信用",
            "count": len(seido_df),
            "segments4": calc_segment_stats_4seg(seido_df),
            "segments11": calc_segment_stats_11seg(seido_df),
            "priceRanges": [],
        }

        for pr in PRICE_RANGES:
            pr_df = seido_df[seido_df["price_range"] == pr["label"]]
            pr_data = {
                "label": pr["label"],
                "count": len(pr_df),
                "segments4": calc_segment_stats_4seg(pr_df),
                "segments11": calc_segment_stats_11seg(pr_df),
            }
            seido_data["priceRanges"].append(pr_data)

        # いちにち信用
        ichinichi_df = wd_df[wd_df["margin_type"] == "いちにち信用"]
        ichinichi_ex0_df = ichinichi_df[ichinichi_df["is_ex0"]]

        ichinichi_data = {
            "type": "いちにち信用",
            "count": {"all": len(ichinichi_df), "ex0": len(ichinichi_ex0_df)},
            "segments4": {
                "all": calc_segment_stats_4seg(ichinichi_df),
                "ex0": calc_segment_stats_4seg(ichinichi_ex0_df),
            },
            "segments11": {
                "all": calc_segment_stats_11seg(ichinichi_df),
                "ex0": calc_segment_stats_11seg(ichinichi_ex0_df),
            },
            "priceRanges": {"all": [], "ex0": []},
        }

        for pr in PRICE_RANGES:
            pr_df = ichinichi_df[ichinichi_df["price_range"] == pr["label"]]
            pr_ex0_df = pr_df[pr_df["is_ex0"]]

            ichinichi_data["priceRanges"]["all"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "segments4": calc_segment_stats_4seg(pr_df),
                "segments11": calc_segment_stats_11seg(pr_df),
            })
            ichinichi_data["priceRanges"]["ex0"].append({
                "label": pr["label"],
                "count": len(pr_ex0_df),
                "segments4": calc_segment_stats_4seg(pr_ex0_df),
                "segments11": calc_segment_stats_11seg(pr_ex0_df),
            })

        result.append({
            "weekday": wd_name,
            "seido": seido_data,
            "ichinichi": ichinichi_data,
        })

    return result


@router.get("/dev/analysis-11seg-1000-3000/summary")
async def get_1000_3000_summary(exclude_extreme: bool = False):
    """
    1000-3000円価格帯特化サマリーAPI

    Args:
        exclude_extreme: True の場合、異常日（1/13, 1/15）を除外

    Returns:
    - fourSegments: 4区分定義
    - elevenSegments: 11時間区分定義
    - priceRanges: 価格帯定義
    - overall: 全体統計
    - weekdays: 曜日別データ
    """
    df_raw = load_archive()
    df = prepare_data(df_raw.copy(), exclude_extreme=exclude_extreme)

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    # 全体統計（all / ex0）
    df_ex0 = df[df["is_ex0"]]
    overall = {
        "count": {"all": len(df), "ex0": len(df_ex0)},
        "seidoCount": int((df["margin_type"] == "制度信用").sum()),
        "ichinichiCount": {
            "all": int((df["margin_type"] == "いちにち信用").sum()),
            "ex0": int((df_ex0["margin_type"] == "いちにち信用").sum())
        },
        "segments4": {
            "all": calc_segment_stats_4seg(df),
            "ex0": calc_segment_stats_4seg(df_ex0),
        },
        "segments11": {
            "all": calc_segment_stats_11seg(df),
            "ex0": calc_segment_stats_11seg(df_ex0),
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
        "fourSegments": [{"key": s["key"], "label": s["label"], "time": s["time"]} for s in FOUR_SEGMENTS],
        "elevenSegments": ELEVEN_SEGMENTS,
        "priceRanges": [pr["label"] for pr in PRICE_RANGES],
        "dateRange": date_range,
        "excludeExtreme": exclude_extreme,
        "extremeDates": EXTREME_DATES,
        "overall": overall,
        "weekdays": weekdays,
    })
