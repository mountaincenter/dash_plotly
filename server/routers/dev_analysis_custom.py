"""
開発用: カスタム分析API

grok_trending_archive.parquetを使用
方向別(SHORT/LONG)プール分離、閾値3区分(SHORT/DISC/LONG)、4seg/11seg切替
- GET /dev/analysis-custom/summary: カスタム分析サマリー
- GET /dev/analysis-custom/details: 詳細データ
- GET /dev/analysis-custom/lending-ratio-pf: 貸借倍率×bucket別PF
- GET /dev/analysis-custom/futures-gap-pf: 先物gap帯×bucket別PF
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import os
import tempfile

router = APIRouter()

# ファイルパス
BASE_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"

S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# 曜日名
WEEKDAY_NAMES = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]

# 閾値定義
PROB_SHORT_THRESHOLD = 0.45
PROB_LONG_THRESHOLD = 0.70

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

# 4時間区分定義（11segベース: 前場前半~10:30, 前場引け11:00, 後場前半~14:00, 大引け15:30）
TIME_SEGMENTS_4 = [
    {"key": "seg_1030", "label": "前場前半", "time": "10:30"},
    {"key": "seg_1100", "label": "前場引け", "time": "11:00"},
    {"key": "seg_1400", "label": "後場前半", "time": "14:00"},
    {"key": "seg_1530", "label": "大引け", "time": "15:30"},
]

# 手動除外日リスト
MANUAL_EXCLUDE_DATES = [
    "2026-01-13",  # 先物+4%でis_extreme_market=Trueだが念のため明示
    "2026-01-14",  # 日経+3.1%の大幅上昇日（先物基準では検出されず）
    "2026-01-15",  # 前日1/14の影響で異常日
]

# 最小件数閾値: これ未満のセルは統計値をnull
MIN_N_THRESHOLD = 10


def load_archive(exclude_extreme: bool = False) -> pd.DataFrame:
    """grok_trending_archive.parquetを読み込み"""
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


def prepare_data(df: pd.DataFrame, price_ranges: list, direction: str = "short") -> pd.DataFrame:
    """データ前処理

    direction: "short" or "long"
    - short: 制度+いちにち残あり、符号反転
    - long: 制度+いちにち全部、符号そのまま
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

    # 信用区分フィルター（全方向共通: 制度 or いちにち）
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    # 曜日
    df["weekday"] = df["date"].dt.weekday
    df["weekday_name"] = df["weekday"].map(lambda x: WEEKDAY_NAMES[x] if x < 5 else "")

    # 信用区分
    df["margin_type"] = df.apply(lambda r: "制度信用" if r["shortable"] else "いちにち信用", axis=1)

    # 除0株フラグ
    df["is_ex0"] = df.apply(
        lambda r: True if r["shortable"] else (
            pd.notna(r.get("day_trade_available_shares")) and r.get("day_trade_available_shares", 0) > 0
        ),
        axis=1
    )

    # 方向別プールフィルター
    if direction == "short":
        # SHORT: 制度+いちにち残あり のみ
        df = df[(df["margin_type"] == "制度信用") | ((df["margin_type"] == "いちにち信用") & (df["is_ex0"] == True))].copy()
    # long: 全部通す

    # 閾値3区分(ml_probがある行のみ)
    if "ml_prob" in df.columns:
        df["bucket"] = pd.cut(
            df["ml_prob"],
            bins=[-0.01, PROB_SHORT_THRESHOLD, PROB_LONG_THRESHOLD, 1.01],
            labels=["SHORT", "DISC", "LONG"]
        )
    else:
        df["bucket"] = None

    # 価格帯
    def get_price_range(price):
        for pr in price_ranges:
            if pr["min"] <= price < pr["max"]:
                return pr["label"]
        return price_ranges[-1]["label"] if price_ranges else ""

    df["price_range"] = df["buy_price"].apply(get_price_range)

    # archiveはSHORTベース: SHORT=そのまま、LONG=反転
    if direction == "long":
        for seg in TIME_SEGMENTS_11:
            key = seg["key"]
            if key in df.columns:
                df[key] = -1 * df[key]

    return df


def _calc_seg_stats(series: pd.Series) -> dict:
    """1つのseg列の統計計算（実額）。n < MIN_N_THRESHOLD ならnull"""
    valid = series.dropna()
    n = len(valid)
    if n == 0:
        return {"profit": 0, "winRate": 0, "count": 0, "mean": 0, "pf": None}
    if n < MIN_N_THRESHOLD:
        return {"profit": int(valid.sum()), "winRate": None, "count": n, "mean": None, "pf": None}
    wins = valid[valid > 0].sum()
    losses = abs(valid[valid <= 0].sum())
    pf = round(float(wins / losses), 2) if losses > 0 else None
    return {
        "profit": int(valid.sum()),
        "winRate": round(float((valid > 0).mean() * 100), 1),
        "count": n,
        "mean": int(valid.mean()),
        "pf": pf,
    }


def _calc_seg_stats_pct(seg_series: pd.Series, buy_price_series: pd.Series) -> dict:
    """1つのseg列の%リターン統計。buy_price * 100 で統一"""
    mask = seg_series.notna() & (buy_price_series > 0)
    seg_valid = seg_series[mask]
    bp_valid = buy_price_series[mask]
    n = len(seg_valid)
    if n == 0:
        return {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0, "pf": None}
    pct_returns = seg_valid / (bp_valid * 100) * 100
    if n < MIN_N_THRESHOLD:
        return {"pctReturn": round(float(pct_returns.sum()), 2), "winRate": None, "count": n, "meanPct": None, "pf": None}
    wins = pct_returns[pct_returns > 0].sum()
    losses = abs(pct_returns[pct_returns <= 0].sum())
    pf = round(float(wins / losses), 2) if losses > 0 else None
    return {
        "pctReturn": round(float(pct_returns.sum()), 2),
        "winRate": round(float((seg_valid > 0).mean() * 100), 1),
        "count": n,
        "meanPct": round(float(pct_returns.mean()), 3),
        "pf": pf,
    }


def calc_segment_stats(df: pd.DataFrame, segments: list) -> dict:
    """指定セグメント定義で統計計算（実額）"""
    return {seg["key"]: _calc_seg_stats(df[seg["key"]]) if seg["key"] in df.columns else {"profit": 0, "winRate": 0, "count": 0, "mean": 0, "pf": None} for seg in segments}


def calc_segment_stats_pct(df: pd.DataFrame, segments: list) -> dict:
    """指定セグメント定義で統計計算（%リターン）"""
    return {seg["key"]: _calc_seg_stats_pct(df[seg["key"]], df["buy_price"]) if seg["key"] in df.columns else {"pctReturn": 0.0, "winRate": 0, "count": 0, "meanPct": 0.0, "pf": None} for seg in segments}


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
            "segments11": calc_segment_stats(seido_df, TIME_SEGMENTS_11),
            "segments4": calc_segment_stats(seido_df, TIME_SEGMENTS_4),
            "pctSegments11": calc_segment_stats_pct(seido_df, TIME_SEGMENTS_11),
            "pctSegments4": calc_segment_stats_pct(seido_df, TIME_SEGMENTS_4),
            "priceRanges": [],
        }

        for pr in price_ranges:
            pr_df = seido_df[seido_df["price_range"] == pr["label"]]
            seido_data["priceRanges"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "segments11": calc_segment_stats(pr_df, TIME_SEGMENTS_11),
                "segments4": calc_segment_stats(pr_df, TIME_SEGMENTS_4),
                "pctSegments11": calc_segment_stats_pct(pr_df, TIME_SEGMENTS_11),
                "pctSegments4": calc_segment_stats_pct(pr_df, TIME_SEGMENTS_4),
            })

        # いちにち信用
        ichinichi_df = wd_df[wd_df["margin_type"] == "いちにち信用"]
        ichinichi_data = {
            "type": "いちにち信用",
            "count": len(ichinichi_df),
            "segments11": calc_segment_stats(ichinichi_df, TIME_SEGMENTS_11),
            "segments4": calc_segment_stats(ichinichi_df, TIME_SEGMENTS_4),
            "pctSegments11": calc_segment_stats_pct(ichinichi_df, TIME_SEGMENTS_11),
            "pctSegments4": calc_segment_stats_pct(ichinichi_df, TIME_SEGMENTS_4),
            "priceRanges": [],
        }

        for pr in price_ranges:
            pr_df = ichinichi_df[ichinichi_df["price_range"] == pr["label"]]
            ichinichi_data["priceRanges"].append({
                "label": pr["label"],
                "count": len(pr_df),
                "segments11": calc_segment_stats(pr_df, TIME_SEGMENTS_11),
                "segments4": calc_segment_stats(pr_df, TIME_SEGMENTS_4),
                "pctSegments11": calc_segment_stats_pct(pr_df, TIME_SEGMENTS_11),
                "pctSegments4": calc_segment_stats_pct(pr_df, TIME_SEGMENTS_4),
            })

        # 曜日ルール情報
        from server.routers.dev_day_trade_list import WEEKDAY_RULES
        wd_rule = WEEKDAY_RULES.get(wd, {})

        result.append({
            "weekday": wd_name,
            "seido": seido_data,
            "ichinichi": ichinichi_data,
            "weekday_rule": wd_rule if wd_rule else None,
        })

    return result


@router.get("/dev/analysis-custom/summary")
async def get_custom_summary(
    exclude_extreme: bool = False,
    price_min: int = 0,
    price_max: int = 999999,
    price_step: int = 0,
    direction: str = "short",
    buckets: str = "",
):
    """
    カスタム分析サマリーAPI

    Query params:
    - exclude_extreme: 異常日除外
    - price_min/price_max/price_step: 価格帯
    - direction: "short" or "long" (プール＋符号切替)
    - buckets: カンマ区切りの閾値区分フィルター (例: "SHORT", "SHORT,DISC")
    """
    if direction not in ("short", "long"):
        raise HTTPException(status_code=400, detail="directionはshort/longのいずれかを指定")

    bucket_filter = [b.strip() for b in buckets.split(",") if b.strip()] if buckets else []

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

    df = prepare_data(df_raw.copy(), price_ranges, direction=direction)

    # 閾値区分フィルター
    if bucket_filter and "bucket" in df.columns:
        df = df[df["bucket"].isin(bucket_filter)].copy()

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    # 全体統計
    overall = {
        "count": len(df),
        "seidoCount": int((df["margin_type"] == "制度信用").sum()),
        "ichinichiCount": int((df["margin_type"] == "いちにち信用").sum()),
        "segments11": calc_segment_stats(df, TIME_SEGMENTS_11),
        "segments4": calc_segment_stats(df, TIME_SEGMENTS_4),
        "pctSegments11": calc_segment_stats_pct(df, TIME_SEGMENTS_11),
        "pctSegments4": calc_segment_stats_pct(df, TIME_SEGMENTS_4),
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

    # bucket情報
    available_buckets = sorted(df["bucket"].dropna().unique().tolist()) if "bucket" in df.columns else []

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
        "direction": direction,
        "filters": {
            "priceMin": price_min,
            "priceMax": price_max,
            "priceStep": price_step,
            "buckets": bucket_filter,
        },
        "bucketInfo": {
            "available": len(available_buckets) > 0,
            "buckets": available_buckets,
            "thresholds": {
                "short": PROB_SHORT_THRESHOLD,
                "long": PROB_LONG_THRESHOLD,
            },
        },
    })


def calc_grouped_details(df: pd.DataFrame, view: str) -> list:
    """日別/週別/月別/曜日別でグルーピングした詳細データ"""
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
        group_keys = sorted(df["group_key"].unique(), reverse=True)

    seg_keys = [seg["key"] for seg in TIME_SEGMENTS_11]

    for key in group_keys:
        group_df = df[df["group_key"] == key]

        # 銘柄リスト
        stocks = []
        for _, row in group_df.iterrows():
            shares = row.get("day_trade_available_shares")
            ml_prob = row.get("ml_prob")
            bucket = row.get("bucket")

            stock_data = {
                "date": row["date"].strftime("%Y-%m-%d"),
                "ticker": row["ticker"],
                "stockName": row.get("stock_name", ""),
                "marginType": row["margin_type"],
                "priceRange": row["price_range"],
                "prevClose": int(row["prev_close"]) if pd.notna(row.get("prev_close")) else None,
                "buyPrice": int(row["buy_price"]) if pd.notna(row["buy_price"]) else None,
                "shares": int(shares) if pd.notna(shares) else None,
                "mlProb": round(float(ml_prob), 3) if pd.notna(ml_prob) else None,
                "bucket": str(bucket) if pd.notna(bucket) else None,
                "segments": {},
            }
            for seg_key in seg_keys:
                if seg_key in row and pd.notna(row[seg_key]):
                    stock_data["segments"][seg_key] = int(row[seg_key])
                else:
                    stock_data["segments"][seg_key] = None
            stocks.append(stock_data)

        # グループ集計
        group_data = {
            "key": key,
            "count": len(group_df),
            "segments": {},
            "stocks": stocks,
        }
        for seg_key in seg_keys:
            if seg_key in group_df.columns:
                group_data["segments"][seg_key] = int(group_df[seg_key].sum())
            else:
                group_data["segments"][seg_key] = 0

        result.append(group_data)

    return result


def _load_analysis_base(exclude_extreme: bool = False, direction: str = "short") -> pd.DataFrame:
    """分析用の共通データ前処理"""
    df = load_archive(exclude_extreme=exclude_extreme)
    if "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])
    elif "selection_date" in df.columns:
        df["date"] = pd.to_datetime(df["selection_date"])
    df = df[df["date"] >= "2025-11-04"]
    df = df[~((df["date"] >= "2025-10-29") & (df["date"] <= "2025-11-21"))]
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    # 信用区分
    df["margin_type"] = df.apply(lambda r: "制度信用" if r["shortable"] else "いちにち信用", axis=1)
    df["is_ex0"] = df.apply(
        lambda r: True if r["shortable"] else (
            pd.notna(r.get("day_trade_available_shares")) and r.get("day_trade_available_shares", 0) > 0
        ),
        axis=1
    )

    # 方向別プールフィルター
    if direction == "short":
        df = df[(df["margin_type"] == "制度信用") | ((df["margin_type"] == "いちにち信用") & (df["is_ex0"] == True))].copy()

    # 閾値3区分
    if "ml_prob" in df.columns:
        df["bucket"] = pd.cut(
            df["ml_prob"],
            bins=[-0.01, PROB_SHORT_THRESHOLD, PROB_LONG_THRESHOLD, 1.01],
            labels=["SHORT", "DISC", "LONG"]
        )

    return df


def _calc_bucket_pf(sub: pd.DataFrame, bucket: str, seg_col: str = "seg_1530", direction: str = "short") -> dict:
    """bucket別PF計算"""
    s = sub.copy()
    # 符号はprepare_dataで方向処理済みなので、ここでは元のseg値を使う
    # _load_analysis_baseでは符号未処理なので、ここで処理
    sign = -1 if direction == "short" else 1
    vals = s[seg_col] * sign
    n = len(vals)
    if n == 0:
        return {"pf": None, "n": 0, "avg": 0, "winRate": 0}
    if n < MIN_N_THRESHOLD:
        return {"pf": None, "n": n, "avg": None, "winRate": None}
    wins = vals[vals > 0].sum()
    losses = abs(vals[vals <= 0].sum())
    pf = round(float(wins / losses), 2) if losses > 0 else None
    avg = round(float(vals.mean()))
    wr = round(float((vals > 0).mean() * 100), 1)
    return {"pf": pf, "n": n, "avg": avg, "winRate": wr}


def _build_bucket_row(bin_data: pd.DataFrame, label: str, seg_col: str = "seg_1530", direction: str = "short") -> dict:
    """1ビン分のbucket別行データを構築"""
    row = {"label": label}
    for bucket in ["SHORT", "DISC", "LONG"]:
        b_sub = bin_data[bin_data["bucket"] == bucket] if "bucket" in bin_data.columns else pd.DataFrame()
        if len(b_sub) > 0:
            row[bucket] = _calc_bucket_pf(b_sub, bucket, seg_col, direction)
        else:
            row[bucket] = {"pf": None, "n": 0, "avg": 0, "winRate": 0}
    return row


def _make_date_range(df: pd.DataFrame) -> dict:
    return {
        "start": str(df["date"].min().date()) if len(df) > 0 else None,
        "end": str(df["date"].max().date()) if len(df) > 0 else None,
        "tradingDays": int(df["date"].dt.date.nunique()) if len(df) > 0 else 0,
    }


@router.get("/dev/analysis-custom/lending-ratio-pf")
async def get_lending_ratio_pf(exclude_extreme: bool = False, direction: str = "short"):
    """貸借倍率(買残/売残)×bucket別PFテーブル"""
    df = _load_analysis_base(exclude_extreme, direction=direction)
    df["lending_ratio"] = np.where(
        df["margin_sell_balance"].notna() & (df["margin_sell_balance"] > 0),
        df["margin_buy_balance"] / df["margin_sell_balance"],
        np.nan
    )
    valid = df.dropna(subset=["lending_ratio"])

    if "seg_1530" not in valid.columns:
        raise HTTPException(status_code=500, detail="seg_1530カラムがありません")

    ratio_bins = [
        {"label": "<2x", "min": 0, "max": 2},
        {"label": "2-5x", "min": 2, "max": 5},
        {"label": "5-20x", "min": 5, "max": 20},
        {"label": "20x+", "min": 20, "max": 1e9},
    ]

    rows = []
    for rb in ratio_bins:
        bin_data = valid[(valid["lending_ratio"] >= rb["min"]) & (valid["lending_ratio"] < rb["max"])]
        rows.append(_build_bucket_row(bin_data, rb["label"], direction=direction))

    return JSONResponse(content={
        "rows": rows,
        "dataRange": _make_date_range(valid),
        "totalWithBalance": len(valid),
        "totalAll": len(df),
        "direction": direction,
    })


@router.get("/dev/analysis-custom/futures-gap-pf")
async def get_futures_gap_pf(exclude_extreme: bool = False, direction: str = "short"):
    """先物gap帯×bucket別PFテーブル"""
    df = _load_analysis_base(exclude_extreme, direction=direction)

    if "futures_change_pct" not in df.columns or "seg_1530" not in df.columns:
        raise HTTPException(status_code=500, detail="必要カラムがありません")

    valid = df.dropna(subset=["futures_change_pct"])

    gap_bins = [
        {"label": "<-0.5%", "min": -999, "max": -0.5},
        {"label": "-0.5~0%", "min": -0.5, "max": 0},
        {"label": "0~0.5%", "min": 0, "max": 0.5},
        {"label": ">0.5%", "min": 0.5, "max": 999},
    ]

    rows = []
    for gb in gap_bins:
        bin_data = valid[(valid["futures_change_pct"] > gb["min"]) & (valid["futures_change_pct"] <= gb["max"])]
        rows.append(_build_bucket_row(bin_data, gb["label"], direction=direction))

    return JSONResponse(content={
        "rows": rows,
        "dataRange": _make_date_range(valid),
        "total": len(valid),
        "direction": direction,
    })


@router.get("/dev/analysis-custom/details")
async def get_custom_details(
    view: str = "daily",
    exclude_extreme: bool = False,
    price_min: int = 0,
    price_max: int = 999999,
    price_step: int = 0,
    direction: str = "short",
    buckets: str = "",
):
    """
    カスタム分析詳細API

    Query params:
    - view: "daily" | "weekly" | "monthly" | "weekday"
    - direction: "short" | "long"
    - buckets: カンマ区切りの閾値区分フィルター
    """
    if view not in ("daily", "weekly", "monthly", "weekday"):
        raise HTTPException(status_code=400, detail="viewはdaily/weekly/monthly/weekdayのいずれかを指定")
    if direction not in ("short", "long"):
        raise HTTPException(status_code=400, detail="directionはshort/longのいずれかを指定")

    bucket_filter = [b.strip() for b in buckets.split(",") if b.strip()] if buckets else []

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

    df = prepare_data(df_raw.copy(), price_ranges, direction=direction)

    # 閾値区分フィルター
    if bucket_filter and "bucket" in df.columns:
        df = df[df["bucket"].isin(bucket_filter)].copy()

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="分析対象データがありません")

    details = calc_grouped_details(df, view=view)

    return JSONResponse(content={
        "view": view,
        "excludeExtreme": exclude_extreme,
        "direction": direction,
        "timeSegments": TIME_SEGMENTS_11,
        "results": details,
    })
