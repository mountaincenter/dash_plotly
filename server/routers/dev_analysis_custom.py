"""
開発用: カスタム分析API

grok_trending_archive.parquetを使用
4seg/11seg切替、価格帯の動的細分化、実額/比率切替を可能にする分析API
- GET /dev/analysis-custom/summary: カスタム分析サマリー
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

# 曜日名
WEEKDAY_NAMES = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]

# 11時間区分定義
TIME_SEGMENTS_11 = [
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

# 4時間区分定義
TIME_SEGMENTS_4 = [
    {"key": "seg_me", "label": "前場前半", "time": "10:00"},
    {"key": "seg_p1", "label": "前場引け", "time": "11:30"},
    {"key": "seg_ae", "label": "後場前半", "time": "14:00"},
    {"key": "seg_p2", "label": "大引け", "time": "15:30"},
]

# 手動除外日リスト
MANUAL_EXCLUDE_DATES = [
    "2026-01-15",
]


def load_archive(exclude_extreme: bool = False) -> pd.DataFrame:
    """grok_trending_archive.parquetを読み込み"""
    if not ARCHIVE_PATH.exists():
        raise HTTPException(status_code=500, detail="アーカイブが見つかりません")

    df = pd.read_parquet(ARCHIVE_PATH)

    # 極端相場除外
    if exclude_extreme:
        if "is_extreme_market" in df.columns:
            df = df[df["is_extreme_market"] == False].copy()
        if "backtest_date" in df.columns:
            df = df[~df["backtest_date"].isin(MANUAL_EXCLUDE_DATES)].copy()

    return df


def generate_price_ranges(price_min: int, price_max: int, price_step: int) -> list:
    """動的価格帯リストを生成"""
    if price_step <= 0:
        return [{"label": f"{price_min:,}~{price_max:,}円", "min": price_min, "max": price_max}]

    ranges = []
    current = price_min
    while current < price_max:
        next_val = min(current + price_step, price_max)
        if next_val >= 999999:
            label = f"{current:,}円~"
        else:
            label = f"{current:,}~{next_val:,}円"
        ranges.append({"label": label, "min": current, "max": next_val})
        current = next_val

    return ranges


def prepare_data(df: pd.DataFrame, price_ranges: list) -> pd.DataFrame:
    """データ前処理"""
    # フィルタ: buy_priceがあるもののみ
    df = df[df["buy_price"].notna()].copy()

    # 日付カラム正規化
    if "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])
    else:
        raise HTTPException(status_code=500, detail="日付カラムが見つかりません")

    # 2025-11-04以降のみ
    df = df[df["date"] >= "2025-11-04"]

    # 制度信用 or いちにち信用のみ
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

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
        for pr in price_ranges:
            if pr["min"] <= price < pr["max"]:
                return pr["label"]
        return price_ranges[-1]["label"] if price_ranges else ""

    df["price_range"] = df["buy_price"].apply(get_price_range)

    # 4seg用データ（profit_per_100_shares_*をセグメント形式に変換）
    # ショート基準: 符号反転
    if "profit_per_100_shares_morning_early" in df.columns:
        df["seg_me"] = -df["profit_per_100_shares_morning_early"].fillna(0)
    else:
        df["seg_me"] = 0

    if "profit_per_100_shares_phase1" in df.columns:
        df["seg_p1"] = -df["profit_per_100_shares_phase1"].fillna(0)
    else:
        df["seg_p1"] = 0

    if "profit_per_100_shares_afternoon_early" in df.columns:
        df["seg_ae"] = -df["profit_per_100_shares_afternoon_early"].fillna(0)
    else:
        df["seg_ae"] = 0

    if "profit_per_100_shares_phase2" in df.columns:
        df["seg_p2"] = -df["profit_per_100_shares_phase2"].fillna(0)
    else:
        df["seg_p2"] = 0

    # 11seg勝敗判定
    for seg in TIME_SEGMENTS_11:
        key = seg["key"]
        if key in df.columns:
            df[f"{key}_win"] = df[key] > 0

    return df


def calc_segment_stats_11(df: pd.DataFrame) -> dict:
    """11時間区分の統計計算（実額）"""
    if len(df) == 0:
        return {seg["key"]: {"profit": 0, "winRate": 0, "count": 0, "mean": 0} for seg in TIME_SEGMENTS_11}

    result = {}
    for seg in TIME_SEGMENTS_11:
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


def calc_segment_stats_4(df: pd.DataFrame) -> dict:
    """4時間区分の統計計算（実額）"""
    if len(df) == 0:
        return {seg["key"]: {"profit": 0, "winRate": 0, "count": 0, "mean": 0} for seg in TIME_SEGMENTS_4}

    result = {}
    for seg in TIME_SEGMENTS_4:
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


def calc_segment_stats_pct_11(df: pd.DataFrame) -> dict:
    """11時間区分の統計計算（比率: %リターン）"""
    if len(df) == 0:
        return {seg["key"]: {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0} for seg in TIME_SEGMENTS_11}

    result = {}
    for seg in TIME_SEGMENTS_11:
        key = seg["key"]
        if key not in df.columns:
            result[key] = {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0}
            continue

        # buy_priceが0の場合は除外
        valid_df = df[(df[key].notna()) & (df["buy_price"] > 0)].copy()
        if len(valid_df) > 0:
            # 比率計算: seg_value / buy_price * 100 (%)
            # 注: 実額は100株単位なので、100株の価格で割る
            pct_returns = valid_df[key] / (valid_df["buy_price"] * 100) * 100
            result[key] = {
                "pctReturn": round(pct_returns.sum(), 2),
                "winRate": round((valid_df[key] > 0).mean() * 100, 1),
                "count": len(valid_df),
                "meanPct": round(pct_returns.mean(), 3),
            }
        else:
            result[key] = {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0}

    return result


def calc_segment_stats_pct_4(df: pd.DataFrame) -> dict:
    """4時間区分の統計計算（比率: %リターン）"""
    if len(df) == 0:
        return {seg["key"]: {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0} for seg in TIME_SEGMENTS_4}

    result = {}
    for seg in TIME_SEGMENTS_4:
        key = seg["key"]
        if key not in df.columns:
            result[key] = {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0}
            continue

        valid_df = df[(df[key].notna()) & (df["buy_price"] > 0)].copy()
        if len(valid_df) > 0:
            pct_returns = valid_df[key] / (valid_df["buy_price"] * 100) * 100
            result[key] = {
                "pctReturn": round(pct_returns.sum(), 2),
                "winRate": round((valid_df[key] > 0).mean() * 100, 1),
                "count": len(valid_df),
                "meanPct": round(pct_returns.mean(), 3),
            }
        else:
            result[key] = {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0}

    return result


def calc_weekday_data(df: pd.DataFrame, price_ranges: list) -> list:
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
            "segments11": calc_segment_stats_11(seido_df),
            "segments4": calc_segment_stats_4(seido_df),
            "pctSegments11": calc_segment_stats_pct_11(seido_df),
            "pctSegments4": calc_segment_stats_pct_4(seido_df),
            "priceRanges": [],
        }

        for pr in price_ranges:
            pr_df = seido_df[seido_df["price_range"] == pr["label"]]
            pr_data = {
                "label": pr["label"],
                "count": len(pr_df),
                "segments11": calc_segment_stats_11(pr_df),
                "segments4": calc_segment_stats_4(pr_df),
                "pctSegments11": calc_segment_stats_pct_11(pr_df),
                "pctSegments4": calc_segment_stats_pct_4(pr_df),
            }
            seido_data["priceRanges"].append(pr_data)

        # いちにち信用
        ichinichi_df = wd_df[wd_df["margin_type"] == "いちにち信用"]
        ichinichi_ex0_df = ichinichi_df[ichinichi_df["is_ex0"]]

        ichinichi_data = {
            "type": "いちにち信用",
            "count": {"all": len(ichinichi_df), "ex0": len(ichinichi_ex0_df)},
            "segments11": {
                "all": calc_segment_stats_11(ichinichi_df),
                "ex0": calc_segment_stats_11(ichinichi_ex0_df),
            },
            "segments4": {
                "all": calc_segment_stats_4(ichinichi_df),
                "ex0": calc_segment_stats_4(ichinichi_ex0_df),
            },
            "pctSegments11": {
                "all": calc_segment_stats_pct_11(ichinichi_df),
                "ex0": calc_segment_stats_pct_11(ichinichi_ex0_df),
            },
            "pctSegments4": {
                "all": calc_segment_stats_pct_4(ichinichi_df),
                "ex0": calc_segment_stats_pct_4(ichinichi_ex0_df),
            },
            "priceRanges": {"all": [], "ex0": []},
        }

        for pr in price_ranges:
            pr_df = ichinichi_df[ichinichi_df["price_range"] == pr["label"]]
            pr_ex0_df = pr_df[pr_df["is_ex0"]]

            ichinichi_data["priceRanges"]["all"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "segments11": calc_segment_stats_11(pr_df),
                "segments4": calc_segment_stats_4(pr_df),
                "pctSegments11": calc_segment_stats_pct_11(pr_df),
                "pctSegments4": calc_segment_stats_pct_4(pr_df),
            })
            ichinichi_data["priceRanges"]["ex0"].append({
                "label": pr["label"],
                "count": len(pr_ex0_df),
                "segments11": calc_segment_stats_11(pr_ex0_df),
                "segments4": calc_segment_stats_4(pr_ex0_df),
                "pctSegments11": calc_segment_stats_pct_11(pr_ex0_df),
                "pctSegments4": calc_segment_stats_pct_4(pr_ex0_df),
            })

        result.append({
            "weekday": wd_name,
            "seido": seido_data,
            "ichinichi": ichinichi_data,
        })

    return result


@router.get("/dev/analysis-custom/summary")
async def get_custom_summary(
    exclude_extreme: bool = False,
    price_min: int = 0,
    price_max: int = 999999,
    price_step: int = 0,
):
    """
    カスタム分析サマリーAPI

    Query params:
    - exclude_extreme: True の場合、異常日を除外
    - price_min: 価格下限 (デフォルト: 0)
    - price_max: 価格上限 (デフォルト: 999999)
    - price_step: 細分化ステップ (0=細分化なし、デフォルト価格帯を使用)

    Returns:
    - timeSegments11: 11時間区分定義
    - timeSegments4: 4時間区分定義
    - priceRanges: 価格帯リスト
    - dateRange: データ期間
    - overall: 全体統計
    - weekdays: 曜日別データ
    """
    # 価格帯生成
    # 刻みは明示的な価格範囲指定時のみ有効（全範囲では無視）
    # データ最大は約20,000円なので、20,000円以下の範囲で刻みを適用
    use_step = price_step > 0 and (price_min > 0 or price_max <= 20000)
    if use_step:
        price_ranges = generate_price_ranges(price_min, price_max, price_step)
    else:
        # デフォルト価格帯（JSONシリアライズのためinfは999999999に置き換え）
        price_ranges = [
            {"label": "~1,000円", "min": 0, "max": 1000},
            {"label": "1,000~3,000円", "min": 1000, "max": 3000},
            {"label": "3,000~5,000円", "min": 3000, "max": 5000},
            {"label": "5,000~10,000円", "min": 5000, "max": 10000},
            {"label": "10,000円~", "min": 10000, "max": 999999999},
        ]
        # price_min/price_maxでフィルタ
        if price_min > 0 or price_max < 999999:
            price_ranges = [pr for pr in price_ranges if pr["max"] > price_min and pr["min"] < price_max]

    df_raw = load_archive(exclude_extreme=exclude_extreme)

    # 価格フィルタ
    if price_min > 0 or price_max < 999999:
        df_raw = df_raw[(df_raw["buy_price"] >= price_min) & (df_raw["buy_price"] < price_max)]

    df = prepare_data(df_raw.copy(), price_ranges)

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
        "segments11": {
            "all": calc_segment_stats_11(df),
            "ex0": calc_segment_stats_11(df_ex0),
        },
        "segments4": {
            "all": calc_segment_stats_4(df),
            "ex0": calc_segment_stats_4(df_ex0),
        },
        "pctSegments11": {
            "all": calc_segment_stats_pct_11(df),
            "ex0": calc_segment_stats_pct_11(df_ex0),
        },
        "pctSegments4": {
            "all": calc_segment_stats_pct_4(df),
            "ex0": calc_segment_stats_pct_4(df_ex0),
        },
    }

    # 曜日別
    weekdays = calc_weekday_data(df, price_ranges)

    # 期間
    date_range = {
        "from": df["date"].min().strftime("%Y-%m-%d"),
        "to": df["date"].max().strftime("%Y-%m-%d"),
        "tradingDays": df["date"].nunique(),
    }

    # 価格帯ラベルリスト
    price_range_labels = [pr["label"] for pr in price_ranges]

    return JSONResponse(content={
        "generatedAt": datetime.now().isoformat(),
        "timeSegments11": TIME_SEGMENTS_11,
        "timeSegments4": TIME_SEGMENTS_4,
        "priceRanges": price_range_labels,
        "priceRangeDetails": price_ranges,
        "dateRange": date_range,
        "overall": overall,
        "weekdays": weekdays,
        "excludeExtreme": exclude_extreme,
        "filters": {
            "priceMin": price_min,
            "priceMax": price_max,
            "priceStep": price_step,
        },
    })


def calc_grouped_details(df: pd.DataFrame, view: str) -> list:
    """
    日別/週別/月別/曜日別でグルーピングした詳細データ（11seg対応）
    """
    # グルーピングキーを追加
    if view == "weekly":
        df["group_key"] = df["date"].apply(lambda d: f"{d.isocalendar().year}/W{d.isocalendar().week:02d}")
    elif view == "monthly":
        df["group_key"] = df["date"].dt.strftime("%Y/%m")
    elif view == "weekday":
        df["group_key"] = df["weekday"].map(lambda x: WEEKDAY_NAMES[x] if x < 5 else "")
    else:  # daily
        df["group_key"] = df["date"].dt.strftime("%Y-%m-%d")

    result = []
    if view == "weekday":
        group_keys = WEEKDAY_NAMES
    else:
        group_keys = sorted(df["group_key"].unique(), reverse=True)[:30]

    seg_keys = [seg["key"] for seg in TIME_SEGMENTS_11]

    for key in group_keys:
        group_df = df[df["group_key"] == key]
        group_ex0_df = group_df[group_df["is_ex0"]]

        # 銘柄リスト
        stocks = []
        for _, row in group_df.iterrows():
            shares = row.get("day_trade_available_shares")

            stock_data = {
                "date": row["date"].strftime("%Y-%m-%d"),
                "ticker": row["ticker"],
                "stockName": row.get("stock_name", ""),
                "marginType": row["margin_type"],
                "priceRange": row["price_range"],
                "prevClose": int(row["prev_close"]) if pd.notna(row.get("prev_close")) else None,
                "buyPrice": int(row["buy_price"]) if pd.notna(row["buy_price"]) else None,
                "shares": int(shares) if pd.notna(shares) else None,
                "segments": {},
            }
            # 11seg値を追加
            for seg_key in seg_keys:
                if seg_key in row and pd.notna(row[seg_key]):
                    stock_data["segments"][seg_key] = int(row[seg_key])
                else:
                    stock_data["segments"][seg_key] = None
            stocks.append(stock_data)

        # グループ集計
        group_data = {
            "key": key,
            "count": {"all": len(group_df), "ex0": len(group_ex0_df)},
            "segments": {"all": {}, "ex0": {}},
            "stocks": stocks,
        }
        for seg_key in seg_keys:
            if seg_key in group_df.columns:
                group_data["segments"]["all"][seg_key] = int(group_df[seg_key].sum())
                group_data["segments"]["ex0"][seg_key] = int(group_ex0_df[seg_key].sum())
            else:
                group_data["segments"]["all"][seg_key] = 0
                group_data["segments"]["ex0"][seg_key] = 0

        result.append(group_data)

    return result


@router.get("/dev/analysis-custom/details")
async def get_custom_details(
    view: str = "daily",
    exclude_extreme: bool = False,
    price_min: int = 0,
    price_max: int = 999999,
    price_step: int = 0,
):
    """
    カスタム分析詳細API（11seg対応）

    Query params:
    - view: "daily" | "weekly" | "monthly" | "weekday"
    - exclude_extreme: True の場合、異常日を除外
    - price_min: 価格下限
    - price_max: 価格上限
    - price_step: 細分化ステップ
    """
    if view not in ("daily", "weekly", "monthly", "weekday"):
        raise HTTPException(status_code=400, detail="viewはdaily/weekly/monthly/weekdayのいずれかを指定")

    # 価格帯生成
    use_step = price_step > 0 and (price_min > 0 or price_max <= 20000)
    if use_step:
        price_ranges = generate_price_ranges(price_min, price_max, price_step)
    else:
        price_ranges = [
            {"label": "~1,000円", "min": 0, "max": 1000},
            {"label": "1,000~3,000円", "min": 1000, "max": 3000},
            {"label": "3,000~5,000円", "min": 3000, "max": 5000},
            {"label": "5,000~10,000円", "min": 5000, "max": 10000},
            {"label": "10,000円~", "min": 10000, "max": 999999999},
        ]
        if price_min > 0 or price_max < 999999:
            price_ranges = [pr for pr in price_ranges if pr["max"] > price_min and pr["min"] < price_max]

    df_raw = load_archive(exclude_extreme=exclude_extreme)

    # 価格フィルタ
    if price_min > 0 or price_max < 999999:
        df_raw = df_raw[(df_raw["buy_price"] >= price_min) & (df_raw["buy_price"] < price_max)]

    df = prepare_data(df_raw.copy(), price_ranges)

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    details = calc_grouped_details(df, view=view)

    return JSONResponse(content={
        "view": view,
        "excludeExtreme": exclude_extreme,
        "timeSegments": TIME_SEGMENTS_11,
        "results": details,
    })
