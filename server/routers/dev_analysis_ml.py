"""
ML Grade × 4seg 分析API

wfcv_predictions.parquet (Walk-Forward CV out-of-sample予測) + archive → Grade別統計
Grade別 × 曜日別 × 4seg(10:25/前場引け/14:45/大引け) の統計
制度信用 / いちにち信用(全数/除0) 切替対応
"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
import json
import os
import time
import tempfile

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parents[2]
ML_DIR = BASE_DIR / "data" / "parquet" / "ml"
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
WFCV_PATH = ML_DIR / "wfcv_predictions.parquet"
MODEL_DIR = BASE_DIR / "models"

S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

WEEKDAY_NAMES = ["月", "火", "水", "木", "金"]

# 4seg: 列名 → 表示ラベル
SEG_DEFS = [
    ("profit_per_100_shares_morning_early", "10:25"),
    ("profit_per_100_shares_phase1", "前場引け"),
    ("profit_per_100_shares_afternoon_early", "14:45"),
    ("profit_per_100_shares_phase2", "大引け"),
]

CACHE_TTL = 60
_cache: dict = {}
_cache_time: dict = {}


def _load_parquet(local_path: Path, s3_key: str) -> pd.DataFrame:
    key = str(local_path)
    now = time.time()
    if key in _cache and (now - _cache_time.get(key, 0)) < CACHE_TTL:
        return _cache[key]

    if local_path.exists():
        df = pd.read_parquet(local_path)
    else:
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            s3.download_file(S3_BUCKET, s3_key, tmp.name)
            df = pd.read_parquet(tmp.name)

    _cache[key] = df
    _cache_time[key] = now
    return df


def _calc_seg_stats(df: pd.DataFrame) -> dict:
    """4seg統計（SHORT前提: 符号反転済み列を使用）"""
    segs = []
    for col, label in SEG_DEFS:
        s_col = f"s_{col}"
        w_col = f"w_{col}"
        if len(df) == 0:
            segs.append({"label": label, "pnl": 0, "wr": 0, "count": 0})
        else:
            valid = df[df[s_col].notna()]
            pnl = int(valid[s_col].sum()) if len(valid) > 0 else 0
            wr = round(valid[w_col].mean() * 100, 1) if len(valid) > 0 else 0
            segs.append({"label": label, "pnl": pnl, "wr": wr, "count": len(valid)})

    return {"count": len(df), "segs": segs}


def _build_grade_stats(df: pd.DataFrame) -> list:
    """Grade別統計 + 曜日別を構築"""
    grade_defs = [
        ("G1", df[df["ml_grade"] == "G1"]),
        ("G2", df[df["ml_grade"] == "G2"]),
        ("G3", df[df["ml_grade"] == "G3"]),
        ("G4", df[df["ml_grade"] == "G4"]),
        ("G1+G2", df[df["ml_grade"].isin(["G1", "G2"])]),
        ("全体", df),
    ]

    grade_stats = []
    for grade_name, gdf in grade_defs:
        stats = _calc_seg_stats(gdf)
        stats["grade"] = grade_name

        weekdays = []
        for wd in range(5):
            wd_df = gdf[gdf["weekday"] == wd]
            wd_stats = _calc_seg_stats(wd_df)
            wd_stats["name"] = WEEKDAY_NAMES[wd]
            weekdays.append(wd_stats)
        stats["weekdays"] = weekdays

        grade_stats.append(stats)

    return grade_stats


@router.get("/api/dev/analysis-ml/summary")
async def get_analysis_ml_summary():
    try:
        wfcv_df = _load_parquet(WFCV_PATH, "parquet/ml/wfcv_predictions.parquet")
        wfcv_df["backtest_date"] = pd.to_datetime(wfcv_df["backtest_date"])

        archive_df = _load_parquet(ARCHIVE_PATH, "parquet/backtest/grok_trending_archive.parquet")
        archive_df["backtest_date"] = pd.to_datetime(archive_df["backtest_date"])

        df = archive_df.merge(
            wfcv_df[["backtest_date", "ticker", "ml_grade", "ml_prob"]],
            on=["backtest_date", "ticker"],
            how="inner",
        )

        df = df[df["buy_price"].notna()].copy()
        df["date"] = pd.to_datetime(df["backtest_date"])
        df = df[df["date"] >= "2025-11-04"]
        df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

        df["weekday"] = df["date"].dt.weekday

        # SHORT mode: 符号反転 + 勝ち判定
        for col, _ in SEG_DEFS:
            df[f"s_{col}"] = -df[col].fillna(np.nan)
            df[f"w_{col}"] = df[col].apply(lambda v: v < 0 if pd.notna(v) else np.nan)

        date_min = df["date"].min().strftime("%Y-%m-%d")
        date_max = df["date"].max().strftime("%Y-%m-%d")

        # meta
        meta_path = MODEL_DIR / "grok_lgbm_meta.json"
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
        else:
            meta = {}

        # margin_type 分類
        df["margin_type"] = df["shortable"].apply(lambda x: "seido" if x else "ichinichi")

        # 除0: 12/22以降の day_trade_available_shares==0 or NaN を除外
        seido_df = df[df["margin_type"] == "seido"]
        ichi_df = df[df["margin_type"] == "ichinichi"]
        ex0_mask = (
            (ichi_df["date"] >= "2025-12-22")
            & (
                (ichi_df["day_trade_available_shares"] == 0)
                | ichi_df["day_trade_available_shares"].isna()
            )
        )
        ichi_ex0_df = ichi_df[~ex0_mask]

        # gradeStats: combined + margin別
        grade_stats = _build_grade_stats(df)
        grade_stats_seido = _build_grade_stats(seido_df)
        grade_stats_ichi = _build_grade_stats(ichi_df)
        grade_stats_ichi_ex0 = _build_grade_stats(ichi_ex0_df)

        # 月別 (combined + ex0)
        def _build_monthly(src_df: pd.DataFrame) -> list:
            src_df = src_df.copy()
            src_df["month"] = src_df["date"].dt.strftime("%Y-%m")
            months = sorted(src_df["month"].unique())
            result = []
            for m in months:
                m_df = src_df[src_df["month"] == m]
                m_data = {"month": m, "grades": {}}
                for gn in ["G1", "G2", "G3", "G4", "G1+G2", "全体"]:
                    if gn == "G1+G2":
                        g_df = m_df[m_df["ml_grade"].isin(["G1", "G2"])]
                    elif gn == "全体":
                        g_df = m_df
                    else:
                        g_df = m_df[m_df["ml_grade"] == gn]
                    m_data["grades"][gn] = _calc_seg_stats(g_df)
                result.append(m_data)
            return result

        # 除0: seido + ichinichiEx0
        ex0_df = pd.concat([seido_df, ichi_ex0_df], ignore_index=True)

        monthly = _build_monthly(df)
        monthly_ex0 = _build_monthly(ex0_df)

        return JSONResponse({
            "dateRange": f"{date_min} ~ {date_max}",
            "totalRecords": len(df),
            "wfcvRecords": len(wfcv_df),
            "modelInfo": {
                "auc": meta.get("metrics", {}).get("auc_mean") or meta.get("auc"),
                "featureCount": len(meta.get("feature_names", [])),
                "boundaries": meta.get("grade_boundaries", []),
            },
            "gradeStats": grade_stats,
            "gradeStatsByMargin": {
                "seido": grade_stats_seido,
                "ichinichi": grade_stats_ichi,
                "ichinichiEx0": grade_stats_ichi_ex0,
            },
            "marginCounts": {
                "seido": len(seido_df),
                "ichinichi": len(ichi_df),
                "ichinichiEx0": len(ichi_ex0_df),
            },
            "monthly": monthly,
            "monthlyEx0": monthly_ex0,
        })

    except Exception as e:
        import traceback
        return JSONResponse(
            {"error": str(e), "detail": traceback.format_exc()},
            status_code=500,
        )
