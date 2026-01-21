"""
開発用: Grok分析API

grok_trending_archive.parquet を使用した分析
- GET /dev/analysis/day-trade-summary: 分析サマリー（ショート/ロング/曜日別戦略）
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import tempfile

router = APIRouter()

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"

S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

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

# RSI/ATRセグメント閾値
SEIDO_RSI_THRESHOLD = 70
SEIDO_ATR_THRESHOLD = 8
ICHI_RSI_THRESHOLD = 90
ICHI_ATR_THRESHOLD = 9


def get_price_range_label(price: float | None) -> str:
    """価格から価格帯ラベルを取得"""
    if price is None or pd.isna(price):
        return ""
    for pr in PRICE_RANGES:
        if pr["min"] <= price < pr["max"]:
            return pr["label"]
    return ""


# 手動除外日リスト（極端相場基準に引っかからないが除外すべき日）
MANUAL_EXCLUDE_DATES = [
    "2026-01-15",  # 前日1/14が日経+3.1%の大幅上昇、当日ショート-3.64%の異常日
]


def load_archive(exclude_extreme: bool = False) -> pd.DataFrame:
    """
    grok_trending_archive.parquetを読み込み

    Args:
        exclude_extreme: True の場合、極端相場（日経±3%超）のデータを除外
                        + MANUAL_EXCLUDE_DATES も除外
    """
    if ARCHIVE_PATH.exists():
        df = pd.read_parquet(ARCHIVE_PATH)
    else:
        try:
            import boto3
            s3_client = boto3.client("s3", region_name=AWS_REGION)

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                s3_client.download_fileobj(
                    S3_BUCKET,
                    "parquet/backtest/grok_trending_archive.parquet",
                    tmp_file
                )
                tmp_path = tmp_file.name

            df = pd.read_parquet(tmp_path)
            os.unlink(tmp_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"archive読み込みエラー: {str(e)}")

    # 極端相場除外
    if exclude_extreme:
        if "is_extreme_market" in df.columns:
            df = df[df["is_extreme_market"] == False].copy()
        # 手動除外日も除外
        if "backtest_date" in df.columns:
            df = df[~df["backtest_date"].isin(MANUAL_EXCLUDE_DATES)].copy()

    return df


def get_extreme_market_info(df: pd.DataFrame) -> dict:
    """極端相場情報を取得（手動除外日を含む）"""
    days = []

    # is_extreme_market=True の日
    if "is_extreme_market" in df.columns:
        extreme_df = df[df["is_extreme_market"] == True]
        for date in extreme_df["backtest_date"].unique():
            day_df = extreme_df[extreme_df["backtest_date"] == date]
            reason = day_df["extreme_market_reason"].iloc[0]
            futures_pct = day_df["futures_change_pct"].iloc[0] if "futures_change_pct" in day_df.columns else None
            date_str = pd.to_datetime(date).strftime("%Y-%m-%d") if pd.notna(date) else None
            days.append({
                "date": date_str,
                "reason": reason,
                "futuresChangePct": round(futures_pct, 2) if futures_pct else None,
                "count": len(day_df),
            })

    # 手動除外日を追加
    for manual_date in MANUAL_EXCLUDE_DATES:
        if manual_date not in [d["date"] for d in days]:
            manual_df = df[df["backtest_date"] == manual_date]
            days.append({
                "date": manual_date,
                "reason": "手動除外",
                "futuresChangePct": None,
                "count": len(manual_df) if len(manual_df) > 0 else 0,
            })

    if not days:
        return {"available": True, "extremeDays": []}

    return {"available": True, "extremeDays": days}


def prepare_data(df: pd.DataFrame, mode: str = "short", weekday_positions: list[str] | None = None) -> pd.DataFrame:
    """
    データ前処理

    mode:
    - "short": 空売り（符号反転）
    - "long": ロング（そのまま）
    - "weekday_strategy": 曜日別戦略（weekday_positionsで指定）

    weekday_positions:
    - 長さ5のリスト ["S", "S", "S", "S", "L"] など
    - 月〜金のポジション（S=ショート, L=ロング）
    - デフォルト: ["S", "S", "S", "S", "L"]（月-木ショート、金ロング）
    """
    # フィルタ: buy_priceがあるもののみ
    df = df[df["buy_price"].notna()].copy()

    # 日付カラム正規化
    if "selection_date" in df.columns:
        df["date"] = pd.to_datetime(df["selection_date"])
    elif "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])
    else:
        raise HTTPException(status_code=500, detail="日付カラムが見つかりません")

    # 2025-11-04以降のみ（データ品質問題）
    df = df[df["date"] >= "2025-11-04"]

    # 制度信用 or いちにち信用のみ
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    # 曜日
    df["weekday"] = df["date"].dt.weekday  # 0=月, 4=金
    df["weekday_name"] = df["weekday"].map(lambda x: WEEKDAY_NAMES[x] if x < 5 else "")

    # 損益計算（modeに応じて）
    if mode == "short":
        # 空売り（符号反転）
        df["calc_p1"] = -df["profit_per_100_shares_phase1"].fillna(0)
        df["calc_p2"] = -df["profit_per_100_shares_phase2"].fillna(0)
        df["calc_win1"] = ~df["phase1_win"].fillna(True)
        df["calc_win2"] = ~df["phase2_win"].fillna(True)
        # 4区分用
        df["calc_me"] = -df["profit_per_100_shares_morning_early"].fillna(0)
        df["calc_ae"] = -df["profit_per_100_shares_afternoon_early"].fillna(0)
        df["calc_win_me"] = df["profit_per_100_shares_morning_early"].fillna(0) < 0
        df["calc_win_ae"] = df["profit_per_100_shares_afternoon_early"].fillna(0) < 0
    elif mode == "long":
        # ロング（そのまま）
        df["calc_p1"] = df["profit_per_100_shares_phase1"].fillna(0)
        df["calc_p2"] = df["profit_per_100_shares_phase2"].fillna(0)
        df["calc_win1"] = df["phase1_win"].fillna(False)
        df["calc_win2"] = df["phase2_win"].fillna(False)
        # 4区分用
        df["calc_me"] = df["profit_per_100_shares_morning_early"].fillna(0)
        df["calc_ae"] = df["profit_per_100_shares_afternoon_early"].fillna(0)
        df["calc_win_me"] = df["profit_per_100_shares_morning_early"].fillna(0) > 0
        df["calc_win_ae"] = df["profit_per_100_shares_afternoon_early"].fillna(0) > 0
    elif mode == "weekday_strategy":
        # 曜日別戦略（デフォルト: 月-木ショート、金ロング）
        if weekday_positions is None:
            weekday_positions = ["S", "S", "S", "S", "L"]

        # 曜日ごとにロングかどうか判定
        is_long = df["weekday"].map(lambda wd: weekday_positions[wd] == "L" if wd < 5 else False)

        df["calc_p1"] = np.where(
            is_long,
            df["profit_per_100_shares_phase1"].fillna(0),
            -df["profit_per_100_shares_phase1"].fillna(0)
        )
        df["calc_p2"] = np.where(
            is_long,
            df["profit_per_100_shares_phase2"].fillna(0),
            -df["profit_per_100_shares_phase2"].fillna(0)
        )
        df["calc_win1"] = np.where(
            is_long,
            df["phase1_win"].fillna(False),
            ~df["phase1_win"].fillna(True)
        )
        df["calc_win2"] = np.where(
            is_long,
            df["phase2_win"].fillna(False),
            ~df["phase2_win"].fillna(True)
        )
        # 4区分用
        df["calc_me"] = np.where(
            is_long,
            df["profit_per_100_shares_morning_early"].fillna(0),
            -df["profit_per_100_shares_morning_early"].fillna(0)
        )
        df["calc_ae"] = np.where(
            is_long,
            df["profit_per_100_shares_afternoon_early"].fillna(0),
            -df["profit_per_100_shares_afternoon_early"].fillna(0)
        )
        df["calc_win_me"] = np.where(
            is_long,
            df["profit_per_100_shares_morning_early"].fillna(0) > 0,
            df["profit_per_100_shares_morning_early"].fillna(0) < 0
        )
        df["calc_win_ae"] = np.where(
            is_long,
            df["profit_per_100_shares_afternoon_early"].fillna(0) > 0,
            df["profit_per_100_shares_afternoon_early"].fillna(0) < 0
        )

    # 信用区分
    df["margin_type"] = df.apply(lambda r: "制度信用" if r["shortable"] else "いちにち信用", axis=1)

    # 除0株フラグ（0株のみ除外、NaNは含む）
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
        return PRICE_RANGES[-1]["label"]

    df["price_range"] = df["buy_price"].apply(get_price_range)

    # RSI/ATRセグメント判定
    if "rsi9" in df.columns and "atr14_pct" in df.columns:
        # RSI該当判定（信用種別によって閾値が異なる）
        df["rsi_hit"] = (
            ((df["margin_type"] == "制度信用") & (df["rsi9"] >= SEIDO_RSI_THRESHOLD)) |
            ((df["margin_type"] == "いちにち信用") & (df["rsi9"] >= ICHI_RSI_THRESHOLD))
        )
        # ATR該当判定（信用種別によって閾値が異なる）
        df["atr_hit"] = (
            ((df["margin_type"] == "制度信用") & (df["atr14_pct"] >= SEIDO_ATR_THRESHOLD)) |
            ((df["margin_type"] == "いちにち信用") & (df["atr14_pct"] >= ICHI_ATR_THRESHOLD))
        )
        # セグメント判定（排他的）
        df["rsi_atr_segment"] = "excluded"
        df.loc[df["rsi_hit"] & ~df["atr_hit"], "rsi_atr_segment"] = "rsi_only"
        df.loc[~df["rsi_hit"] & df["atr_hit"], "rsi_atr_segment"] = "atr_only"
        df.loc[df["rsi_hit"] & df["atr_hit"], "rsi_atr_segment"] = "both"
    else:
        df["rsi_hit"] = False
        df["atr_hit"] = False
        df["rsi_atr_segment"] = "excluded"

    return df


def calc_stats(df: pd.DataFrame, segments: int = 2) -> dict:
    """統計計算 (segments: 2 or 4)"""
    if len(df) == 0:
        base = {
            "count": 0,
            "seidoCount": 0,
            "ichinichiCount": 0,
            "p1": 0,
            "p2": 0,
            "win1": 0,
            "win2": 0,
        }
        if segments == 4:
            base.update({"me": 0, "ae": 0, "winMe": 0, "winAe": 0})
        return base

    result = {
        "count": len(df),
        "seidoCount": int((df["margin_type"] == "制度信用").sum()),
        "ichinichiCount": int((df["margin_type"] == "いちにち信用").sum()),
        "p1": int(df["calc_p1"].sum()),
        "p2": int(df["calc_p2"].sum()),
        "win1": round(df["calc_win1"].mean() * 100, 1) if len(df) > 0 else 0,
        "win2": round(df["calc_win2"].mean() * 100, 1) if len(df) > 0 else 0,
    }

    if segments == 4:
        result.update({
            "me": int(df["calc_me"].sum()),
            "ae": int(df["calc_ae"].sum()),
            "winMe": round(df["calc_win_me"].mean() * 100, 1) if len(df) > 0 else 0,
            "winAe": round(df["calc_win_ae"].mean() * 100, 1) if len(df) > 0 else 0,
        })

    return result


def calc_period_stats(df: pd.DataFrame, period: str, segments: int = 2) -> dict:
    """期間別統計 (segments: 2 or 4)"""
    if len(df) == 0:
        empty_stats = calc_stats(pd.DataFrame(), segments=segments)
        return {"all": empty_stats, "ex0": empty_stats}

    max_date = df["date"].max()
    unique_dates = sorted(df["date"].unique(), reverse=True)

    if period == "daily":
        filtered = df[df["date"] == max_date]
    elif period == "weekly":
        recent_5 = unique_dates[:5]
        filtered = df[df["date"].isin(recent_5)]
    elif period == "monthly":
        max_month = max_date.strftime("%Y-%m")
        filtered = df[df["date"].dt.strftime("%Y-%m") == max_month]
    else:  # all
        filtered = df

    return {
        "all": calc_stats(filtered, segments=segments),
        "ex0": calc_stats(filtered[filtered["is_ex0"]], segments=segments),
    }


def calc_weekday_data(df: pd.DataFrame, mode: str = "short", weekday_positions: list[str] | None = None, segments: int = 2) -> list:
    """曜日別データ (segments: 2 or 4)"""
    result = []

    if weekday_positions is None:
        weekday_positions = ["S", "S", "S", "S", "L"]

    for wd in range(5):  # 月〜金
        wd_df = df[df["weekday"] == wd]
        wd_name = WEEKDAY_NAMES[wd]

        # 曜日別戦略の場合、曜日ごとのポジション表示
        if mode == "weekday_strategy":
            position = "ロング" if weekday_positions[wd] == "L" else "ショート"
        else:
            position = "ロング" if mode == "long" else "ショート"

        # 制度信用
        seido_df = wd_df[wd_df["margin_type"] == "制度信用"]
        seido_data = {
            "type": "制度信用",
            "count": len(seido_df),
            "p1Total": int(seido_df["calc_p1"].sum()),
            "p2Total": int(seido_df["calc_p2"].sum()),
            "priceRanges": [],
            "position": position,
        }
        if segments == 4:
            seido_data["meTotal"] = int(seido_df["calc_me"].sum())
            seido_data["aeTotal"] = int(seido_df["calc_ae"].sum())

        for pr in PRICE_RANGES:
            pr_df = seido_df[seido_df["price_range"] == pr["label"]]
            pr_data = {
                "label": pr["label"],
                "count": len(pr_df),
                "p1": int(pr_df["calc_p1"].sum()),
                "p2": int(pr_df["calc_p2"].sum()),
                "win1": round(pr_df["calc_win1"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "win2": round(pr_df["calc_win2"].mean() * 100, 0) if len(pr_df) > 0 else 0,
            }
            if segments == 4:
                pr_data["me"] = int(pr_df["calc_me"].sum())
                pr_data["ae"] = int(pr_df["calc_ae"].sum())
                pr_data["winMe"] = round(pr_df["calc_win_me"].mean() * 100, 0) if len(pr_df) > 0 else 0
                pr_data["winAe"] = round(pr_df["calc_win_ae"].mean() * 100, 0) if len(pr_df) > 0 else 0
            seido_data["priceRanges"].append(pr_data)

        # いちにち信用
        ichinichi_df = wd_df[wd_df["margin_type"] == "いちにち信用"]
        ichinichi_ex0_df = ichinichi_df[ichinichi_df["is_ex0"]]

        ichinichi_data = {
            "type": "いちにち信用",
            "count": {"all": len(ichinichi_df), "ex0": len(ichinichi_ex0_df)},
            "p1Total": {"all": int(ichinichi_df["calc_p1"].sum()), "ex0": int(ichinichi_ex0_df["calc_p1"].sum())},
            "p2Total": {"all": int(ichinichi_df["calc_p2"].sum()), "ex0": int(ichinichi_ex0_df["calc_p2"].sum())},
            "priceRanges": {"all": [], "ex0": []},
            "position": position,
        }
        if segments == 4:
            ichinichi_data["meTotal"] = {"all": int(ichinichi_df["calc_me"].sum()), "ex0": int(ichinichi_ex0_df["calc_me"].sum())}
            ichinichi_data["aeTotal"] = {"all": int(ichinichi_df["calc_ae"].sum()), "ex0": int(ichinichi_ex0_df["calc_ae"].sum())}

        for pr in PRICE_RANGES:
            pr_df = ichinichi_df[ichinichi_df["price_range"] == pr["label"]]
            pr_ex0_df = pr_df[pr_df["is_ex0"]]

            # 株数合計
            shares_all = pr_df["day_trade_available_shares"].fillna(0).sum()
            shares_ex0 = pr_ex0_df["day_trade_available_shares"].fillna(0).sum()

            pr_data_all = {
                "label": pr["label"],
                "count": len(pr_df),
                "shares": int(shares_all),
                "p1": int(pr_df["calc_p1"].sum()),
                "p2": int(pr_df["calc_p2"].sum()),
                "win1": round(pr_df["calc_win1"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "win2": round(pr_df["calc_win2"].mean() * 100, 0) if len(pr_df) > 0 else 0,
            }
            pr_data_ex0 = {
                "label": pr["label"],
                "count": len(pr_ex0_df),
                "shares": int(shares_ex0),
                "p1": int(pr_ex0_df["calc_p1"].sum()),
                "p2": int(pr_ex0_df["calc_p2"].sum()),
                "win1": round(pr_ex0_df["calc_win1"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0,
                "win2": round(pr_ex0_df["calc_win2"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0,
            }
            if segments == 4:
                pr_data_all["me"] = int(pr_df["calc_me"].sum())
                pr_data_all["ae"] = int(pr_df["calc_ae"].sum())
                pr_data_all["winMe"] = round(pr_df["calc_win_me"].mean() * 100, 0) if len(pr_df) > 0 else 0
                pr_data_all["winAe"] = round(pr_df["calc_win_ae"].mean() * 100, 0) if len(pr_df) > 0 else 0
                pr_data_ex0["me"] = int(pr_ex0_df["calc_me"].sum())
                pr_data_ex0["ae"] = int(pr_ex0_df["calc_ae"].sum())
                pr_data_ex0["winMe"] = round(pr_ex0_df["calc_win_me"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0
                pr_data_ex0["winAe"] = round(pr_ex0_df["calc_win_ae"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0
            ichinichi_data["priceRanges"]["all"].append(pr_data_all)
            ichinichi_data["priceRanges"]["ex0"].append(pr_data_ex0)

        # RSI/ATRセグメント別データ（segments==4の場合のみ）
        rsi_atr_segments = None
        if segments == 4 and "rsi_atr_segment" in wd_df.columns:
            rsi_atr_segments = calc_rsi_atr_segment_data(wd_df, position, segments)

        wd_data = {
            "weekday": wd_name,
            "seido": seido_data,
            "ichinichi": ichinichi_data,
            "position": position,
        }
        if rsi_atr_segments:
            wd_data["rsiAtrSegments"] = rsi_atr_segments

        result.append(wd_data)

    return result


def calc_rsi_atr_segment_data(wd_df: pd.DataFrame, position: str, segments: int = 4) -> dict:
    """
    RSI/ATRセグメント別データを計算

    Returns:
    {
        "counts": {"excluded": N, "rsi_only": N, "atr_only": N, "both": N, "all": N},
        "excluded": {"seido": {...}, "ichinichi": {...}},
        "rsi_only": {"seido": {...}, "ichinichi": {...}},
        "atr_only": {"seido": {...}, "ichinichi": {...}},
        "both": {"seido": {...}, "ichinichi": {...}},
    }
    """
    segment_keys = ["excluded", "rsi_only", "atr_only", "both"]
    result = {"counts": {"all": len(wd_df)}}

    for seg_key in segment_keys:
        seg_df = wd_df[wd_df["rsi_atr_segment"] == seg_key]
        result["counts"][seg_key] = len(seg_df)

        # 制度信用
        seido_df = seg_df[seg_df["margin_type"] == "制度信用"]
        seido_data = {
            "count": len(seido_df),
            "meTotal": int(seido_df["calc_me"].sum()) if len(seido_df) > 0 else 0,
            "p1Total": int(seido_df["calc_p1"].sum()) if len(seido_df) > 0 else 0,
            "aeTotal": int(seido_df["calc_ae"].sum()) if len(seido_df) > 0 else 0,
            "p2Total": int(seido_df["calc_p2"].sum()) if len(seido_df) > 0 else 0,
            "priceRanges": [],
            "position": position,
        }
        for pr in PRICE_RANGES:
            pr_df = seido_df[seido_df["price_range"] == pr["label"]]
            seido_data["priceRanges"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "me": int(pr_df["calc_me"].sum()) if len(pr_df) > 0 else 0,
                "p1": int(pr_df["calc_p1"].sum()) if len(pr_df) > 0 else 0,
                "ae": int(pr_df["calc_ae"].sum()) if len(pr_df) > 0 else 0,
                "p2": int(pr_df["calc_p2"].sum()) if len(pr_df) > 0 else 0,
                "winMe": round(pr_df["calc_win_me"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "win1": round(pr_df["calc_win1"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "winAe": round(pr_df["calc_win_ae"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "win2": round(pr_df["calc_win2"].mean() * 100, 0) if len(pr_df) > 0 else 0,
            })

        # いちにち信用
        ichinichi_df = seg_df[seg_df["margin_type"] == "いちにち信用"]
        ichinichi_ex0_df = ichinichi_df[ichinichi_df["is_ex0"]]
        ichinichi_data = {
            "count": {"all": len(ichinichi_df), "ex0": len(ichinichi_ex0_df)},
            "meTotal": {"all": int(ichinichi_df["calc_me"].sum()) if len(ichinichi_df) > 0 else 0, "ex0": int(ichinichi_ex0_df["calc_me"].sum()) if len(ichinichi_ex0_df) > 0 else 0},
            "p1Total": {"all": int(ichinichi_df["calc_p1"].sum()) if len(ichinichi_df) > 0 else 0, "ex0": int(ichinichi_ex0_df["calc_p1"].sum()) if len(ichinichi_ex0_df) > 0 else 0},
            "aeTotal": {"all": int(ichinichi_df["calc_ae"].sum()) if len(ichinichi_df) > 0 else 0, "ex0": int(ichinichi_ex0_df["calc_ae"].sum()) if len(ichinichi_ex0_df) > 0 else 0},
            "p2Total": {"all": int(ichinichi_df["calc_p2"].sum()) if len(ichinichi_df) > 0 else 0, "ex0": int(ichinichi_ex0_df["calc_p2"].sum()) if len(ichinichi_ex0_df) > 0 else 0},
            "priceRanges": {"all": [], "ex0": []},
            "position": position,
        }
        for pr in PRICE_RANGES:
            pr_df = ichinichi_df[ichinichi_df["price_range"] == pr["label"]]
            pr_ex0_df = pr_df[pr_df["is_ex0"]]
            ichinichi_data["priceRanges"]["all"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "me": int(pr_df["calc_me"].sum()) if len(pr_df) > 0 else 0,
                "p1": int(pr_df["calc_p1"].sum()) if len(pr_df) > 0 else 0,
                "ae": int(pr_df["calc_ae"].sum()) if len(pr_df) > 0 else 0,
                "p2": int(pr_df["calc_p2"].sum()) if len(pr_df) > 0 else 0,
                "winMe": round(pr_df["calc_win_me"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "win1": round(pr_df["calc_win1"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "winAe": round(pr_df["calc_win_ae"].mean() * 100, 0) if len(pr_df) > 0 else 0,
                "win2": round(pr_df["calc_win2"].mean() * 100, 0) if len(pr_df) > 0 else 0,
            })
            ichinichi_data["priceRanges"]["ex0"].append({
                "label": pr["label"],
                "count": len(pr_ex0_df),
                "me": int(pr_ex0_df["calc_me"].sum()) if len(pr_ex0_df) > 0 else 0,
                "p1": int(pr_ex0_df["calc_p1"].sum()) if len(pr_ex0_df) > 0 else 0,
                "ae": int(pr_ex0_df["calc_ae"].sum()) if len(pr_ex0_df) > 0 else 0,
                "p2": int(pr_ex0_df["calc_p2"].sum()) if len(pr_ex0_df) > 0 else 0,
                "winMe": round(pr_ex0_df["calc_win_me"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0,
                "win1": round(pr_ex0_df["calc_win1"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0,
                "winAe": round(pr_ex0_df["calc_win_ae"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0,
                "win2": round(pr_ex0_df["calc_win2"].mean() * 100, 0) if len(pr_ex0_df) > 0 else 0,
            })

        result[seg_key] = {"seido": seido_data, "ichinichi": ichinichi_data}

    return result


def calc_daily_details(df: pd.DataFrame, mode: str = "short", weekday_positions: list[str] | None = None, segments: int = 2) -> list:
    """日別詳細データ (segments: 2 or 4)"""
    result = []
    dates = sorted(df["date"].unique(), reverse=True)

    if weekday_positions is None:
        weekday_positions = ["S", "S", "S", "S", "L"]

    for date in dates[:30]:  # 直近30日分
        day_df = df[df["date"] == date]
        day_ex0_df = day_df[day_df["is_ex0"]]

        # 曜日別戦略の場合、曜日ごとのポジション表示
        weekday = date.weekday()
        if mode == "weekday_strategy":
            position = "ロング" if weekday_positions[weekday] == "L" else "ショート"
        else:
            position = "ロング" if mode == "long" else "ショート"

        stocks = []
        for _, row in day_df.iterrows():
            shares = row.get("day_trade_available_shares")
            stock_data = {
                "ticker": row["ticker"],
                "stockName": row.get("stock_name", ""),
                "marginType": row["margin_type"],
                "buyPrice": int(row["buy_price"]) if pd.notna(row["buy_price"]) else None,
                "shares": int(shares) if pd.notna(shares) else None,
                "p1": int(row["calc_p1"]),
                "p2": int(row["calc_p2"]),
                "win1": bool(row["calc_win1"]),
                "win2": bool(row["calc_win2"]),
            }
            if segments == 4:
                stock_data["me"] = int(row["calc_me"])
                stock_data["ae"] = int(row["calc_ae"])
                stock_data["winMe"] = bool(row["calc_win_me"])
                stock_data["winAe"] = bool(row["calc_win_ae"])
            stocks.append(stock_data)

        day_data = {
            "date": date.strftime("%Y-%m-%d"),
            "count": {"all": len(day_df), "ex0": len(day_ex0_df)},
            "p1": {"all": int(day_df["calc_p1"].sum()), "ex0": int(day_ex0_df["calc_p1"].sum())},
            "p2": {"all": int(day_df["calc_p2"].sum()), "ex0": int(day_ex0_df["calc_p2"].sum())},
            "stocks": stocks,
            "position": position,
        }
        if segments == 4:
            day_data["me"] = {"all": int(day_df["calc_me"].sum()), "ex0": int(day_ex0_df["calc_me"].sum())}
            day_data["ae"] = {"all": int(day_df["calc_ae"].sum()), "ex0": int(day_ex0_df["calc_ae"].sum())}
        result.append(day_data)

    return result


def calc_rsi_band_data(df: pd.DataFrame, segments: int = 4) -> dict:
    """
    RSI帯別データ（制度/いちにち × RSI 10区切り × 4区分）

    Returns:
    {
        "seido": [{"label": "0-10", "count": N, "me": X, "p1": X, "ae": X, "p2": X}, ...],
        "ichinichi": [{"label": "0-10", "count": N, "me": X, "p1": X, "ae": X, "p2": X}, ...]
    }
    """
    result = {"seido": [], "ichinichi": []}

    # RSI帯: 0-10, 10-20, ..., 90-100
    rsi_bands = [(i, i + 10) for i in range(0, 100, 10)]

    for margin_type, key in [("制度信用", "seido"), ("いちにち信用", "ichinichi")]:
        margin_df = df[df["margin_type"] == margin_type]

        for rsi_min, rsi_max in rsi_bands:
            # RSI9でフィルタ（NaN除外）
            if "rsi9" not in df.columns:
                band_df = pd.DataFrame()
            else:
                band_df = margin_df[
                    (margin_df["rsi9"] >= rsi_min) &
                    (margin_df["rsi9"] < rsi_max)
                ]

            band_data = {
                "label": f"{rsi_min}-{rsi_max}",
                "count": len(band_df),
                "me": int(band_df["calc_me"].sum()) if len(band_df) > 0 else 0,
                "p1": int(band_df["calc_p1"].sum()) if len(band_df) > 0 else 0,
                "ae": int(band_df["calc_ae"].sum()) if len(band_df) > 0 else 0,
                "p2": int(band_df["calc_p2"].sum()) if len(band_df) > 0 else 0,
            }
            result[key].append(band_data)

    return result


def calc_atr_band_data(df: pd.DataFrame, segments: int = 4) -> dict:
    """
    ATR帯別データ（制度/いちにち × ATR区切り × 4区分）

    Returns:
    {
        "seido": [{"label": "0-2", "count": N, "me": X, "p1": X, "ae": X, "p2": X}, ...],
        "ichinichi": [{"label": "0-2", "count": N, "me": X, "p1": X, "ae": X, "p2": X}, ...]
    }
    """
    result = {"seido": [], "ichinichi": []}

    # ATR帯: 制度信用用
    atr_bands_seido = [(0, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 10), (10, 15), (15, 100)]
    atr_labels_seido = ['0-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-10', '10-15', '>15']

    # ATR帯: いちにち信用用（8-9, 9-10を分ける）
    atr_bands_ichi = [(0, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10), (10, 15), (15, 100)]
    atr_labels_ichi = ['0-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-9', '9-10', '10-15', '>15']

    # 制度信用
    margin_df = df[df["margin_type"] == "制度信用"]
    for (atr_min, atr_max), label in zip(atr_bands_seido, atr_labels_seido):
        if "atr14_pct" not in df.columns:
            band_df = pd.DataFrame()
        else:
            band_df = margin_df[
                (margin_df["atr14_pct"] >= atr_min) &
                (margin_df["atr14_pct"] < atr_max)
            ]

        band_data = {
            "label": label,
            "count": len(band_df),
            "me": int(band_df["calc_me"].sum()) if len(band_df) > 0 else 0,
            "p1": int(band_df["calc_p1"].sum()) if len(band_df) > 0 else 0,
            "ae": int(band_df["calc_ae"].sum()) if len(band_df) > 0 else 0,
            "p2": int(band_df["calc_p2"].sum()) if len(band_df) > 0 else 0,
        }
        result["seido"].append(band_data)

    # いちにち信用
    margin_df = df[df["margin_type"] == "いちにち信用"]
    for (atr_min, atr_max), label in zip(atr_bands_ichi, atr_labels_ichi):
        if "atr14_pct" not in df.columns:
            band_df = pd.DataFrame()
        else:
            band_df = margin_df[
                (margin_df["atr14_pct"] >= atr_min) &
                (margin_df["atr14_pct"] < atr_max)
            ]

        band_data = {
            "label": label,
            "count": len(band_df),
            "me": int(band_df["calc_me"].sum()) if len(band_df) > 0 else 0,
            "p1": int(band_df["calc_p1"].sum()) if len(band_df) > 0 else 0,
            "ae": int(band_df["calc_ae"].sum()) if len(band_df) > 0 else 0,
            "p2": int(band_df["calc_p2"].sum()) if len(band_df) > 0 else 0,
        }
        result["ichinichi"].append(band_data)

    return result


def calc_analysis_for_mode(df_raw: pd.DataFrame, mode: str, weekday_positions: list[str] | None = None, segments: int = 2) -> dict:
    """指定モードで分析データを計算 (segments: 2 or 4)"""
    df = prepare_data(df_raw.copy(), mode=mode, weekday_positions=weekday_positions)

    if len(df) == 0:
        return None

    # 期間別統計
    period_stats = {
        "daily": calc_period_stats(df, "daily", segments=segments),
        "weekly": calc_period_stats(df, "weekly", segments=segments),
        "monthly": calc_period_stats(df, "monthly", segments=segments),
        "all": calc_period_stats(df, "all", segments=segments),
    }

    # 曜日別データ
    weekday_data = calc_weekday_data(df, mode=mode, weekday_positions=weekday_positions, segments=segments)

    # RSI帯別データ（4区分のみ）
    rsi_band_data = calc_rsi_band_data(df, segments=4) if segments == 4 else None

    # ATR帯別データ（4区分のみ）
    atr_band_data = calc_atr_band_data(df, segments=4) if segments == 4 else None

    # 日別詳細
    daily_details = calc_daily_details(df, mode=mode, weekday_positions=weekday_positions, segments=segments)

    # メタ情報
    meta = {
        "generatedAt": datetime.now().isoformat(),
        "dateRange": {
            "start": df["date"].min().strftime("%Y-%m-%d"),
            "end": df["date"].max().strftime("%Y-%m-%d"),
        },
        "totalRecords": len(df),
        "mode": mode,
        "segments": segments,
    }

    result = {
        "periodStats": period_stats,
        "weekdayData": weekday_data,
        "dailyDetails": daily_details,
        "meta": meta,
    }

    # RSI帯別データ（4区分時のみ）
    if rsi_band_data:
        result["rsiBandData"] = rsi_band_data

    # ATR帯別データ（4区分時のみ）
    if atr_band_data:
        result["atrBandData"] = atr_band_data

    return result


@router.get("/dev/analysis/day-trade-summary")
async def get_day_trade_summary(segments: int = 2, exclude_extreme: bool = False):
    """
    Grok分析サマリー

    Query params:
    - segments: 2（簡易版: 前場引け/大引け）or 4（詳細版: 前場前半/前場引け/後場前半/大引け）
    - exclude_extreme: True の場合、極端相場（日経±3%超）のデータを除外

    Returns:
    - short: ショート戦略の分析
    - long: ロング戦略の分析
    - weekdayStrategy: 曜日別戦略（月-木ショート、金ロング）の分析
    - extremeMarket: 極端相場情報

    各戦略に含まれる項目:
    - periodStats: 期間別統計（daily/weekly/monthly/all × all/ex0）
    - weekdayData: 曜日別データ（月〜金 × 制度/いちにち × 価格帯）
    - dailyDetails: 日別詳細（直近30日）
    - meta: メタ情報
    """
    if segments not in (2, 4):
        raise HTTPException(status_code=400, detail="segmentsは2または4を指定してください")

    # 極端相場情報は除外前のデータから取得
    df_all = load_archive(exclude_extreme=False)
    extreme_info = get_extreme_market_info(df_all)

    # 分析用データ（exclude_extremeに応じてフィルタ）
    df_raw = load_archive(exclude_extreme=exclude_extreme)

    # 3パターン計算
    short_data = calc_analysis_for_mode(df_raw, "short", segments=segments)
    long_data = calc_analysis_for_mode(df_raw, "long", segments=segments)
    weekday_strategy_data = calc_analysis_for_mode(df_raw, "weekday_strategy", segments=segments)

    if not short_data:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    return JSONResponse(content={
        "short": short_data,
        "long": long_data,
        "weekdayStrategy": weekday_strategy_data,
        "extremeMarket": extreme_info,
        "excludeExtreme": exclude_extreme,
    })


@router.get("/dev/analysis/custom-weekday")
async def get_custom_weekday_strategy(
    mon: str = "S",
    tue: str = "S",
    wed: str = "S",
    thu: str = "S",
    fri: str = "L",
    segments: int = 2,
    exclude_extreme: bool = False,
):
    """
    カスタム曜日別戦略

    Query params:
    - mon, tue, wed, thu, fri: 各曜日のポジション（S=ショート, L=ロング）
    - segments: 2（簡易版）or 4（詳細版）
    - exclude_extreme: True の場合、極端相場（日経±3%超）のデータを除外

    デフォルト: 月-木ショート、金ロング
    """
    # バリデーション
    if segments not in (2, 4):
        raise HTTPException(status_code=400, detail="segmentsは2または4を指定してください")

    positions = [mon.upper(), tue.upper(), wed.upper(), thu.upper(), fri.upper()]
    for p in positions:
        if p not in ("S", "L"):
            raise HTTPException(status_code=400, detail="ポジションはSまたはLで指定してください")

    df_raw = load_archive(exclude_extreme=exclude_extreme)
    data = calc_analysis_for_mode(df_raw, "weekday_strategy", weekday_positions=positions, segments=segments)

    if not data:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    # メタ情報にポジション設定を追加
    data["meta"]["weekdayPositions"] = positions
    data["meta"]["excludeExtreme"] = exclude_extreme

    return JSONResponse(content=data)


def calc_grouped_details(df: pd.DataFrame, view: str, mode: str = "short", weekday_positions: list[str] | None = None, segments: int = 2) -> list:
    """
    日別/週別/月別/曜日別でグルーピングした詳細データ

    view:
    - "daily": 日別 (YYYY-MM-DD)
    - "weekly": 週別 (YYYY/W##)
    - "monthly": 月別 (YYYY/MM)
    - "weekday": 曜日別 (月/火/水/木/金)

    segments:
    - 2: 簡易版（前場引け/大引け）
    - 4: 詳細版（前場前半/前場引け/後場前半/大引け）
    """
    if weekday_positions is None:
        weekday_positions = ["S", "S", "S", "S", "L"]

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
        # 曜日別は月〜金の順序で固定
        group_keys = WEEKDAY_NAMES
    else:
        group_keys = sorted(df["group_key"].unique(), reverse=True)[:30]  # 直近30グループ

    for key in group_keys:
        group_df = df[df["group_key"] == key]
        group_ex0_df = group_df[group_df["is_ex0"]]

        # 銘柄リスト
        stocks = []
        for _, row in group_df.iterrows():
            shares = row.get("day_trade_available_shares")
            weekday = row["date"].weekday()
            if mode == "weekday_strategy":
                position = "ロング" if weekday_positions[weekday] == "L" else "ショート"
            else:
                position = "ロング" if mode == "long" else "ショート"

            stock_data = {
                "date": row["date"].strftime("%Y-%m-%d"),
                "ticker": row["ticker"],
                "stockName": row.get("stock_name", ""),
                "marginType": row["margin_type"],
                "priceRange": get_price_range_label(row.get("buy_price")),
                "prevClose": int(row["prev_close"]) if pd.notna(row.get("prev_close")) else None,
                "buyPrice": int(row["buy_price"]) if pd.notna(row["buy_price"]) else None,
                "sellPrice": int(row["sell_price"]) if pd.notna(row.get("sell_price")) else None,
                "dailyClose": int(row["daily_close"]) if pd.notna(row.get("daily_close")) else None,
                "shares": int(shares) if pd.notna(shares) else None,
                "p1": int(row["calc_p1"]),
                "p2": int(row["calc_p2"]),
                "win1": bool(row["calc_win1"]),
                "win2": bool(row["calc_win2"]),
                "position": position,
            }
            if segments == 4:
                stock_data["me"] = int(row["calc_me"])
                stock_data["ae"] = int(row["calc_ae"])
                stock_data["winMe"] = bool(row["calc_win_me"])
                stock_data["winAe"] = bool(row["calc_win_ae"])
                # RSI/ATRフラグ
                stock_data["rsiHit"] = bool(row.get("rsi_hit", False))
                stock_data["atrHit"] = bool(row.get("atr_hit", False))
            stocks.append(stock_data)

        group_data = {
            "key": key,
            "count": {"all": len(group_df), "ex0": len(group_ex0_df)},
            "p1": {"all": int(group_df["calc_p1"].sum()), "ex0": int(group_ex0_df["calc_p1"].sum())},
            "p2": {"all": int(group_df["calc_p2"].sum()), "ex0": int(group_ex0_df["calc_p2"].sum())},
            "stocks": stocks,
        }
        if segments == 4:
            group_data["me"] = {"all": int(group_df["calc_me"].sum()), "ex0": int(group_ex0_df["calc_me"].sum())}
            group_data["ae"] = {"all": int(group_df["calc_ae"].sum()), "ex0": int(group_ex0_df["calc_ae"].sum())}
        result.append(group_data)

    return result


@router.get("/dev/analysis/details")
async def get_analysis_details(
    view: str = "daily",
    mode: str = "short",
    mon: str = "S",
    tue: str = "S",
    wed: str = "S",
    thu: str = "S",
    fri: str = "L",
    segments: int = 2,
    exclude_extreme: bool = False,
):
    """
    詳細データ

    Query params:
    - view: "daily" | "weekly" | "monthly" | "weekday"
    - mode: "short" | "long" | "weekday_strategy"
    - mon, tue, wed, thu, fri: 曜日別戦略のポジション（mode=weekday_strategyの場合のみ有効）
    - segments: 2（簡易版）or 4（詳細版）
    - exclude_extreme: True の場合、極端相場（日経±3%超）のデータを除外
    """
    if view not in ("daily", "weekly", "monthly", "weekday"):
        raise HTTPException(status_code=400, detail="viewはdaily/weekly/monthly/weekdayのいずれかを指定してください")
    if mode not in ("short", "long", "weekday_strategy"):
        raise HTTPException(status_code=400, detail="modeはshort/long/weekday_strategyのいずれかを指定してください")
    if segments not in (2, 4):
        raise HTTPException(status_code=400, detail="segmentsは2または4を指定してください")

    weekday_positions = None
    if mode == "weekday_strategy":
        weekday_positions = [mon.upper(), tue.upper(), wed.upper(), thu.upper(), fri.upper()]
        for p in weekday_positions:
            if p not in ("S", "L"):
                raise HTTPException(status_code=400, detail="ポジションはSまたはLで指定してください")

    df_raw = load_archive(exclude_extreme=exclude_extreme)
    df = prepare_data(df_raw.copy(), mode=mode, weekday_positions=weekday_positions)

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    details = calc_grouped_details(df, view=view, mode=mode, weekday_positions=weekday_positions, segments=segments)

    return JSONResponse(content={
        "view": view,
        "mode": mode,
        "segments": segments,
        "excludeExtreme": exclude_extreme,
        "results": details,
    })


# ============================================================
# 市場騰落表示API
# ============================================================


@router.get("/dev/analysis/market-status")
async def get_market_status():
    """
    市場騰落表示API

    grok_trending.parquetからnikkei_change_pct, futures_change_pctを取得
    """
    from server.routers.dev_day_trade_list import load_grok_trending

    result = {
        "generatedAt": datetime.now().isoformat(),
        "nikkei": None,
        "futures": None,
    }

    try:
        df = load_grok_trending()
    except Exception:
        return JSONResponse(content=result)

    if df.empty:
        return JSONResponse(content=result)

    # 最初の行から取得（全行同一値）
    row = df.iloc[0]

    nikkei_change_pct = row.get("nikkei_change_pct")
    futures_change_pct = row.get("futures_change_pct")

    if nikkei_change_pct is not None and not pd.isna(nikkei_change_pct):
        result["nikkei"] = {
            "changePct": round(float(nikkei_change_pct), 2),
            "close": row.get("nikkei_close"),
            "prevClose": row.get("nikkei_prev_close"),
            "date": row.get("nikkei_date"),
        }

    if futures_change_pct is not None and not pd.isna(futures_change_pct):
        result["futures"] = {
            "changePct": round(float(futures_change_pct), 2),
            "price": row.get("futures_price"),
            "prevPrice": row.get("nikkei_close"),  # 先物の比較基準は日経終値
            "date": row.get("futures_date"),
        }

    return JSONResponse(content=result)
