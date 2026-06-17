"""
開発用: カスタム分析API

grok_master_jquants_segments.parquetを優先使用
方向別(SHORT/LONG)プール分離、prob_regime区分、4seg/11seg切替
- GET /dev/analysis-custom/summary: カスタム分析サマリー
- GET /dev/analysis-custom/details: 詳細データ
- GET /dev/analysis-custom/lending-ratio-pf: 貸借倍率×prob_regime別PF
- GET /dev/analysis-custom/futures-gap-pf: 先物gap帯×prob_regime別PF
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
MASTER_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "grok_master_jquants_segments.parquet"

S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# 曜日名
WEEKDAY_NAMES = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]

# prob_regime閾値定義
PROB_REGIME_LOW_THRESHOLD = 0.40
PROB_REGIME_HIGH_THRESHOLD = 0.50
BUCKET_LABELS = ["LOW_PROB_HEAT", "MID_PROB_HEAT", "HIGH_PROB_HEAT"]

# 2025-12-22以降は信用区分・いちにち残数・NG判定が揃っている実運用分析対象。
LEGACY_START_DATE = pd.Timestamp("2025-11-04")
CREDIT_VERIFIED_START_DATE = pd.Timestamp("2025-12-22")


def assign_bucket(prob: float | None) -> str | None:
    """ML prob_upをGrok熱量レジームへ分類する。"""
    if prob is None or pd.isna(prob):
        return None
    return _assign_prob_group(prob)

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

# 4時間区分定義（11segベース: 前場前半10:30, 前場引け11:30, 後場前半14:00, 大引け15:30）
TIME_SEGMENTS_4 = [
    {"key": "seg_1030", "label": "前場前半", "time": "10:30"},
    {"key": "seg_1130", "label": "前場引け", "time": "11:30"},
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


def _download_parquet_from_s3(key: str, label: str) -> pd.DataFrame:
    try:
        import boto3
        s3_client = boto3.client("s3", region_name=AWS_REGION)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            s3_client.download_fileobj(S3_BUCKET, key, tmp_file)
            tmp_path = tmp_file.name

        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{label}読み込みエラー: {str(e)}")


def _normalize_jquants_master(df: pd.DataFrame) -> pd.DataFrame:
    """J-Quants masterの実行価格列を既存analysis APIのseg_*へ正規化する。"""
    out = df.copy()
    out["analysis_source"] = "grok_master_jquants_segments"

    for col in [
        "buy_price",
        "sell_price",
        "daily_close",
        "profit_per_100_shares_phase1",
        "profit_per_100_shares_phase2",
        "phase1_return",
        "phase2_return",
        "phase1_win",
        "phase2_win",
        "volume",
        "Value",
    ]:
        if col in out.columns and f"archive_{col}" not in out.columns:
            out[f"archive_{col}"] = out[col]

    preferred_cols = {
        "jq_buy_price": "buy_price",
        "jq_sell_price": "sell_price",
        "jq_daily_close": "daily_close",
        "jq_profit_per_100_shares_phase1": "profit_per_100_shares_phase1",
        "jq_profit_per_100_shares_phase2": "profit_per_100_shares_phase2",
        "jq_phase1_return": "phase1_return",
        "jq_phase2_return": "phase2_return",
        "jq_phase1_win": "phase1_win",
        "jq_phase2_win": "phase2_win",
        "jq_total_volume": "volume",
        "jq_total_value": "Value",
    }
    for source_col, target_col in preferred_cols.items():
        if source_col in out.columns:
            source = out[source_col]
            if target_col in out.columns:
                out[target_col] = source.combine_first(out[target_col])
            else:
                out[target_col] = source

    for seg in TIME_SEGMENTS_11:
        seg_col = seg["key"]
        jq_col = f"jq_{seg_col}"
        if jq_col not in out.columns:
            continue
        if seg_col in out.columns and f"archive_{seg_col}" not in out.columns:
            out[f"archive_{seg_col}"] = out[seg_col]
        if seg_col in out.columns:
            out[seg_col] = out[jq_col].combine_first(out[seg_col])
        else:
            out[seg_col] = out[jq_col]

    out.attrs["analysis_source"] = "grok_master_jquants_segments"
    out.attrs["price_basis"] = "jquants_minute"
    total_rows = len(out)
    out.attrs["jq_buy_price_coverage"] = (
        round(float(out["jq_buy_price"].notna().mean()), 6) if total_rows and "jq_buy_price" in out.columns else None
    )
    out.attrs["jq_seg_1530_coverage"] = (
        round(float(out["jq_seg_1530"].notna().mean()), 6) if total_rows and "jq_seg_1530" in out.columns else None
    )
    return out


def load_archive(exclude_extreme: bool = False) -> pd.DataFrame:
    """分析用データを読み込み。J-Quants masterがあれば優先し、なければarchiveへフォールバック。"""
    if MASTER_PATH.exists():
        df = _normalize_jquants_master(pd.read_parquet(MASTER_PATH))
    else:
        master_key = "parquet/backtest/grok_master_jquants_segments.parquet"
        archive_key = "parquet/backtest/grok_trending_archive.parquet"
        try:
            df = _normalize_jquants_master(_download_parquet_from_s3(master_key, "master"))
        except HTTPException:
            if ARCHIVE_PATH.exists():
                df = pd.read_parquet(ARCHIVE_PATH)
            else:
                df = _download_parquet_from_s3(archive_key, "archive")
            df["analysis_source"] = "grok_trending_archive"
            df.attrs["analysis_source"] = "grok_trending_archive"
            df.attrs["price_basis"] = "archive_seg"

    # 極端相場除外
    if exclude_extreme:
        if "is_extreme_market" in df.columns:
            df = df[df["is_extreme_market"] == False].copy()
        if "backtest_date" in df.columns:
            df = df[~df["backtest_date"].isin(MANUAL_EXCLUDE_DATES)].copy()

    if "analysis_source" in df.columns and df["analysis_source"].notna().any():
        df.attrs["analysis_source"] = str(df["analysis_source"].dropna().iloc[0])
        df.attrs["price_basis"] = "jquants_minute" if df.attrs["analysis_source"] == "grok_master_jquants_segments" else "archive_seg"

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


def _apply_analysis_scope(df: pd.DataFrame, include_legacy: bool = False) -> pd.DataFrame:
    """分析対象期間を適用する。デフォルトは信用区分が揃う2025-12-22以降。"""
    start_date = LEGACY_START_DATE if include_legacy else CREDIT_VERIFIED_START_DATE
    return df[df["date"] >= start_date].copy()


def _build_data_scope(df_raw: pd.DataFrame, df: pd.DataFrame, include_legacy: bool = False) -> dict:
    """レスポンスに分析対象スコープを明示する。"""
    analysis_source = df_raw.attrs.get("analysis_source") or (
        str(df_raw["analysis_source"].dropna().iloc[0]) if "analysis_source" in df_raw.columns and df_raw["analysis_source"].notna().any() else "unknown"
    )
    price_basis = df_raw.attrs.get("price_basis") or ("jquants_minute" if analysis_source == "grok_master_jquants_segments" else "archive_seg")
    jq_buy_price_coverage = df_raw.attrs.get("jq_buy_price_coverage")
    jq_seg_1530_coverage = df_raw.attrs.get("jq_seg_1530_coverage")
    raw = df_raw.copy()
    if "backtest_date" in raw.columns:
        raw["date"] = pd.to_datetime(raw["backtest_date"])
    elif "selection_date" in raw.columns:
        raw["date"] = pd.to_datetime(raw["selection_date"])
    elif "date" in raw.columns:
        raw["date"] = pd.to_datetime(raw["date"])
    legacy_mask = (raw["date"] >= LEGACY_START_DATE) & (raw["date"] < CREDIT_VERIFIED_START_DATE)
    return {
        "scope": "legacy_included" if include_legacy else "credit_verified",
        "analysisStartDate": (LEGACY_START_DATE if include_legacy else CREDIT_VERIFIED_START_DATE).strftime("%Y-%m-%d"),
        "creditVerifiedStartDate": CREDIT_VERIFIED_START_DATE.strftime("%Y-%m-%d"),
        "includeLegacy": include_legacy,
        "excludedLegacyRows": 0 if include_legacy else int(legacy_mask.sum()),
        "rows": int(len(df)),
        "analysisSource": analysis_source,
        "priceBasis": price_basis,
        "jqBuyPriceCoverage": jq_buy_price_coverage,
        "jqSeg1530Coverage": jq_seg_1530_coverage,
    }


def prepare_data(df: pd.DataFrame, price_ranges: list, direction: str = "short", include_legacy: bool = False) -> pd.DataFrame:
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

    df = _apply_analysis_scope(df, include_legacy=include_legacy)

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

    # prob_regime(ml_probがある行のみ)
    if "ml_prob" in df.columns:
        df["bucket"] = df["ml_prob"].apply(assign_bucket)
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

        result.append({
            "weekday": wd_name,
            "seido": seido_data,
            "ichinichi": ichinichi_data,
            "weekday_rule": None,
        })

    return result


def calc_strategy_candidates(df: pd.DataFrame) -> list:
    """曜日×信用区分×prob_regimeの戦略候補を4segで集計する。"""
    result = []
    margin_groups = [
        ("seido", "制度信用", df[df["margin_type"] == "制度信用"]),
        ("ichinichi_ex0", "いちにち除0", df[(df["margin_type"] == "いちにち信用") & (df["is_ex0"] == True)]),
    ]

    for wd in range(5):
        for margin_key, margin_label, margin_df in margin_groups:
            wd_df = margin_df[margin_df["weekday"] == wd]
            for bucket in BUCKET_LABELS:
                sub = wd_df[wd_df["bucket"] == bucket] if "bucket" in wd_df.columns else pd.DataFrame()
                segs = calc_segment_stats(sub, TIME_SEGMENTS_4)
                segment_rows = []
                for seg in TIME_SEGMENTS_4:
                    stats = segs.get(seg["key"], {})
                    segment_rows.append({
                        "key": seg["key"],
                        "label": seg["label"],
                        "time": seg["time"],
                        "profit": stats.get("profit", 0),
                        "winRate": stats.get("winRate"),
                        "count": stats.get("count", 0),
                        "mean": stats.get("mean"),
                        "pf": stats.get("pf"),
                    })

                valid_segments = [s for s in segment_rows if s["pf"] is not None]
                best = max(valid_segments, key=lambda s: (s["pf"], s["profit"])) if valid_segments else None
                close = next((s for s in segment_rows if s["key"] == "seg_1530"), None)
                best_pf = best["pf"] if best else None
                close_pf = close["pf"] if close else None
                total = best["profit"] if best else 0
                pf_delta = best_pf - close_pf if best_pf is not None and close_pf is not None else None

                if best_pf is not None and best_pf >= 1.5 and close_pf is not None and close_pf >= 1.0 and total > 0 and best["key"] == "seg_1530":
                    decision = "GO"
                    reason = "大引けでも期待値あり"
                elif best_pf is not None and best_pf >= 1.2 and total > 0:
                    decision = "CONDITIONAL"
                    if close_pf is not None and close_pf < 1.0:
                        reason = "大引けPF<1"
                    elif pf_delta is not None and pf_delta >= 0.3:
                        reason = f"PF差 +{pf_delta:.2f}"
                    else:
                        reason = "条件確認"
                else:
                    decision = "SKIP"
                    reason = "期待値不足"

                result.append({
                    "weekday": WEEKDAY_NAMES[wd],
                    "weekdayIndex": wd,
                    "marginKey": margin_key,
                    "marginLabel": margin_label,
                    "bucket": bucket,
                    "count": int(len(sub)),
                    "decision": decision,
                    "reason": reason,
                    "bestSegment": best,
                    "closeSegment": close,
                    "pfDelta": round(float(pf_delta), 2) if pf_delta is not None else None,
                    "segments": segment_rows,
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
    include_legacy: bool = False,
):
    """
    カスタム分析サマリーAPI

    Query params:
    - exclude_extreme: 異常日除外
    - price_min/price_max/price_step: 価格帯
    - direction: "short" or "long" (プール＋符号切替)
    - buckets: カンマ区切りのprob_regimeフィルター
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

    df = prepare_data(df_raw.copy(), price_ranges, direction=direction, include_legacy=include_legacy)

    # prob_regimeフィルター
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
    strategy_candidates = calc_strategy_candidates(df)

    # 期間
    date_range = {
        "from": df["date"].min().strftime("%Y-%m-%d"),
        "to": df["date"].max().strftime("%Y-%m-%d"),
        "tradingDays": df["date"].nunique(),
    }

    # 価格帯ラベルリスト
    price_range_labels = [pr["label"] for pr in price_ranges]

    # prob_regime情報。JSON互換のためbucketInfoキーは残す。
    available_buckets = sorted(df["bucket"].dropna().unique().tolist()) if "bucket" in df.columns else []

    return JSONResponse(content={
        "generatedAt": datetime.now().isoformat(),
        "timeSegments11": TIME_SEGMENTS_11,
        "timeSegments4": TIME_SEGMENTS_4,
        "priceRanges": price_range_labels,
        "priceRangeDetails": price_ranges,
        "dateRange": date_range,
        "dataScope": _build_data_scope(df_raw, df, include_legacy=include_legacy),
        "overall": overall,
        "weekdays": weekdays,
        "strategyCandidates": strategy_candidates,
        "excludeExtreme": exclude_extreme,
        "direction": direction,
        "filters": {
            "priceMin": price_min,
            "priceMax": price_max,
            "priceStep": price_step,
            "buckets": bucket_filter,
            "includeLegacy": include_legacy,
        },
        "bucketInfo": {
            "available": len(available_buckets) > 0,
            "buckets": available_buckets,
            "thresholds": {
                "low": PROB_REGIME_LOW_THRESHOLD,
                "high": PROB_REGIME_HIGH_THRESHOLD,
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


def _load_analysis_base(exclude_extreme: bool = False, direction: str = "short", include_legacy: bool = False) -> pd.DataFrame:
    """分析用の共通データ前処理"""
    df_raw = load_archive(exclude_extreme=exclude_extreme)
    df = df_raw.copy()
    if "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])
    elif "selection_date" in df.columns:
        df["date"] = pd.to_datetime(df["selection_date"])
    df = _apply_analysis_scope(df, include_legacy=include_legacy)
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

    # prob_regime
    if "ml_prob" in df.columns:
        df["bucket"] = df["ml_prob"].apply(assign_bucket)

    df.attrs["data_scope"] = _build_data_scope(df_raw, df, include_legacy=include_legacy)
    return df


def _calc_bucket_pf(sub: pd.DataFrame, bucket: str, seg_col: str = "seg_1530", direction: str = "short") -> dict:
    """prob_regime別PF計算"""
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
    """1ビン分のprob_regime別行データを構築"""
    row = {"label": label}
    for bucket in BUCKET_LABELS:
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


def _max_drawdown(pnl: pd.Series) -> float | None:
    vals = pnl.dropna().astype(float)
    if vals.empty:
        return None
    curve = vals.cumsum()
    return round(float((curve - curve.cummax()).min()), 2)


def _cvar05(pnl: pd.Series) -> float | None:
    vals = pnl.dropna().astype(float)
    if vals.empty:
        return None
    q05 = vals.quantile(0.05)
    tail = vals[vals <= q05]
    return round(float(tail.mean()), 2) if not tail.empty else None


def _risk_metrics(vals: pd.Series, dates: pd.Series | None = None) -> dict:
    valid = vals.dropna().astype(float)
    n = len(valid)
    if n == 0:
        return {
            "n": 0, "total": 0, "avg": None, "pf": None, "winRate": None,
            "q05": None, "cvar05": None, "worstTrade": None, "dailyMaxDD": None,
            "worstDay": None, "dailyPlusRate": None,
        }

    wins = valid[valid > 0].sum()
    losses = abs(valid[valid <= 0].sum())
    daily = pd.Series(dtype=float)
    if dates is not None:
        aligned_dates = dates.loc[valid.index]
        daily = valid.groupby(aligned_dates.dt.date).sum().sort_index()

    return {
        "n": n,
        "total": round(float(valid.sum()), 2),
        "avg": round(float(valid.mean()), 2),
        "pf": round(float(wins / losses), 2) if losses > 0 else None,
        "winRate": round(float((valid > 0).mean() * 100), 1),
        "q05": round(float(valid.quantile(0.05)), 2),
        "cvar05": _cvar05(valid),
        "worstTrade": round(float(valid.min()), 2),
        "dailyMaxDD": _max_drawdown(daily) if not daily.empty else None,
        "worstDay": round(float(daily.min()), 2) if not daily.empty else None,
        "dailyPlusRate": round(float((daily > 0).mean() * 100), 1) if not daily.empty else None,
    }


def _assign_prob_group(prob: float | None) -> str | None:
    if prob is None or pd.isna(prob):
        return None
    p = float(prob)
    if p < PROB_REGIME_LOW_THRESHOLD:
        return "LOW_PROB_HEAT"
    if p < PROB_REGIME_HIGH_THRESHOLD:
        return "MID_PROB_HEAT"
    return "HIGH_PROB_HEAT"


def _direction_multiplier(direction: str) -> int:
    # analysisのseg列はSHORT基準。LONGは反転する。
    return 1 if direction == "short" else -1


@router.get("/dev/analysis-custom/lending-ratio-pf")
async def get_lending_ratio_pf(exclude_extreme: bool = False, direction: str = "short", weekday: int = -1, include_legacy: bool = False):
    """貸借倍率(買残/売残)×prob_regime別PFテーブル（weekday: 0=月〜4=金, -1=全体）"""
    df = _load_analysis_base(exclude_extreme, direction=direction, include_legacy=include_legacy)
    if weekday >= 0:
        df = df[df["date"].dt.weekday == weekday]
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
        "dataScope": df.attrs.get("data_scope"),
        "totalWithBalance": len(valid),
        "totalAll": len(df),
        "direction": direction,
    })


@router.get("/dev/analysis-custom/futures-gap-pf")
async def get_futures_gap_pf(exclude_extreme: bool = False, direction: str = "short", weekday: int = -1, include_legacy: bool = False):
    """先物gap帯×prob_regime別PFテーブル（weekday: 0=月〜4=金, -1=全体）"""
    df = _load_analysis_base(exclude_extreme, direction=direction, include_legacy=include_legacy)
    if weekday >= 0:
        df = df[df["date"].dt.weekday == weekday]

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
        "dataScope": df.attrs.get("data_scope"),
        "total": len(valid),
        "direction": direction,
    })


@router.get("/dev/analysis-custom/nikkei-change-pf")
async def get_nikkei_change_pf(exclude_extreme: bool = False, direction: str = "short", weekday: int = -1, include_legacy: bool = False):
    """N225変化率帯×prob_regime別PFテーブル（weekday: 0=月〜4=金, -1=全体）"""
    df = _load_analysis_base(exclude_extreme, direction=direction, include_legacy=include_legacy)
    if weekday >= 0:
        df = df[df["date"].dt.weekday == weekday]

    if "nikkei_change_pct" not in df.columns or "seg_1530" not in df.columns:
        raise HTTPException(status_code=500, detail="必要カラムがありません")

    valid = df.dropna(subset=["nikkei_change_pct"])

    nikkei_bins = [
        {"label": "<-1%", "min": -999, "max": -1},
        {"label": "-1~0%", "min": -1, "max": 0},
        {"label": "0~1%", "min": 0, "max": 1},
        {"label": "1~2%", "min": 1, "max": 2},
        {"label": ">2%", "min": 2, "max": 999},
    ]

    rows = []
    for nb in nikkei_bins:
        bin_data = valid[(valid["nikkei_change_pct"] > nb["min"]) & (valid["nikkei_change_pct"] <= nb["max"])]
        rows.append(_build_bucket_row(bin_data, nb["label"], direction=direction))

    return JSONResponse(content={
        "rows": rows,
        "dataRange": _make_date_range(valid),
        "dataScope": df.attrs.get("data_scope"),
        "total": len(valid),
        "direction": direction,
    })


@router.get("/dev/analysis-custom/prob-bin-pf")
async def get_prob_bin_pf(
    view: str = "daily",
    price_min: int = 0,
    price_max: int = 999999,
    margin_type: str = "",
    prob_source: str = "hybrid",
    include_legacy: bool = False,
):
    """prob 0.1区切り × 日別/週別/月別/曜日別パフォーマンス

    prob_source:
    - hybrid: 再学習/WFCVのml_probを優先し、未付与期間のみml_prob_liveで補完。/dev/recommendationsの既定。
    - live: 22:00本番選定時のprob（ml_prob_live）を使用。
    - wfcv: WFCV検証用prob（ml_prob）を使用。
    """
    if view not in ("daily", "weekly", "monthly", "weekday"):
        raise HTTPException(status_code=400, detail="viewはdaily/weekly/monthly/weekdayのいずれか")
    if prob_source not in ("hybrid", "live", "wfcv"):
        raise HTTPException(status_code=400, detail="prob_sourceはhybrid/live/wfcvのいずれか")

    df = _load_analysis_base(exclude_extreme=False, direction="short", include_legacy=include_legacy)
    if prob_source == "hybrid":
        if "ml_prob" not in df.columns:
            df["ml_prob"] = np.nan
        if "ml_prob_live" not in df.columns:
            df["ml_prob_live"] = np.nan
        prob_col = "ml_prob_effective"
        df[prob_col] = df["ml_prob"].combine_first(df["ml_prob_live"])
    else:
        prob_col = "ml_prob_live" if prob_source == "live" else "ml_prob"
        if prob_col not in df.columns:
            df[prob_col] = np.nan
    df = df[df[prob_col].notna()].copy()

    if price_min > 0 or price_max < 999999:
        df = df[(df["buy_price"] >= price_min) & (df["buy_price"] < price_max)]
    if margin_type == "制度":
        df = df[df["margin_type"] == "制度信用"]
    elif margin_type == "いちにち":
        df = df[(df["margin_type"] == "いちにち信用") & (df["is_ex0"] == True)]

    prob_bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    prob_labels = ["0.0-0.1", "0.1-0.2", "0.2-0.3", "0.3-0.4", "0.4-0.5",
                   "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0"]
    prob_decisions = {
        "0.0-0.1": "LOW_PROB_HEAT",
        "0.1-0.2": "LOW_PROB_HEAT",
        "0.2-0.3": "LOW_PROB_HEAT",
        "0.3-0.4": "LOW_PROB_HEAT",
        "0.4-0.5": "MID_PROB_HEAT",
        "0.5-0.6": "HIGH_PROB_HEAT",
        "0.6-0.7": "HIGH_PROB_HEAT",
        "0.7-0.8": "HIGH_PROB_HEAT",
        "0.8-0.9": "HIGH_PROB_HEAT",
        "0.9-1.0": "HIGH_PROB_HEAT",
    }
    df["prob_bin"] = pd.cut(df[prob_col], bins=prob_bins, labels=prob_labels, right=True, include_lowest=True)

    if view == "weekly":
        df["group_key"] = df["date"].apply(lambda d: f"{d.isocalendar().year}/W{d.isocalendar().week:02d}")
    elif view == "monthly":
        df["group_key"] = df["date"].dt.strftime("%Y/%m")
    elif view == "weekday":
        df["group_key"] = df["date"].dt.weekday.map(lambda x: WEEKDAY_NAMES[x] if x < 5 else "")
    else:
        df["group_key"] = df["date"].dt.strftime("%Y-%m-%d")

    group_keys = WEEKDAY_NAMES if view == "weekday" else sorted(df["group_key"].unique(), reverse=True)
    seg_col = "seg_1530"
    # archiveはSHORTベース（prepare_data L183参照）: そのまま使用
    sign = 1

    results = []
    for gk in group_keys:
        gdf = df[df["group_key"] == gk]
        if len(gdf) == 0:
            continue
        bins_data = []
        for label in prob_labels:
            sub = gdf[gdf["prob_bin"] == label]
            n = len(sub)
            if n == 0:
                bins_data.append({"label": label, "decision": prob_decisions.get(label), "n": 0, "pf": None, "winRate": None, "avg": None, "total": None, "avgReturn": None})
                continue
            vals = sub[seg_col].dropna() * sign
            if len(vals) == 0:
                bins_data.append({"label": label, "decision": prob_decisions.get(label), "n": n, "pf": None, "winRate": None, "avg": None, "total": None, "avgReturn": None})
                continue
            gp = float(vals[vals > 0].sum())
            gl = float(abs(vals[vals <= 0].sum()))
            pf = round(gp / gl, 2) if gl > 0 else None
            wr = round(float((vals > 0).mean() * 100), 1)
            avg = round(float(vals.mean()))
            total = int(vals.sum())
            bp = sub["buy_price"].dropna()
            pct_vals = sub[seg_col].dropna() / (bp[sub[seg_col].notna()].values * 100) * 100 if len(bp) > 0 else pd.Series(dtype=float)
            avg_ret = round(float(pct_vals.mean()), 2) if len(pct_vals) > 0 else None
            bins_data.append({"label": label, "decision": prob_decisions.get(label), "n": n, "pf": pf, "winRate": wr, "avg": avg, "total": total, "avgReturn": avg_ret})
        # 合計
        all_vals = gdf[seg_col].dropna() * sign
        gp_all = float(all_vals[all_vals > 0].sum()) if len(all_vals) > 0 else 0
        gl_all = float(abs(all_vals[all_vals <= 0].sum())) if len(all_vals) > 0 else 0
        all_bp = gdf["buy_price"].dropna()
        all_pct = gdf[seg_col].dropna() / (all_bp[gdf[seg_col].notna()].values * 100) * 100 if len(all_bp) > 0 else pd.Series(dtype=float)
        results.append({
            "key": gk,
            "count": len(gdf),
            "total": int(all_vals.sum()) if len(all_vals) > 0 else 0,
            "pf": round(gp_all / gl_all, 2) if gl_all > 0 else None,
            "winRate": round(float((all_vals > 0).mean() * 100), 1) if len(all_vals) > 0 else None,
            "avgReturn": round(float(all_pct.mean()), 2) if len(all_pct) > 0 else None,
            "bins": bins_data,
        })

    return JSONResponse(content={
        "view": view,
        "probSource": prob_source,
        "probColumn": prob_col,
        "probLabels": prob_labels,
        "dataRange": _make_date_range(df),
        "dataScope": df.attrs.get("data_scope"),
        "total": len(df),
        "results": results,
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
    include_legacy: bool = False,
):
    """
    カスタム分析詳細API

    Query params:
    - view: "daily" | "weekly" | "monthly" | "weekday"
    - direction: "short" | "long"
    - buckets: カンマ区切りのprob_regimeフィルター
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

    df = prepare_data(df_raw.copy(), price_ranges, direction=direction, include_legacy=include_legacy)

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
        "dataScope": _build_data_scope(df_raw, df, include_legacy=include_legacy),
        "timeSegments": TIME_SEGMENTS_11,
        "results": details,
    })


@router.get("/dev/analysis-custom/weekday-risk-matrix")
async def get_weekday_risk_matrix(
    weekday: int = 0,
    direction: str = "short",
    segment_mode: str = "4seg",
    exclude_extreme: bool = False,
    include_legacy: bool = False,
):
    """曜日別の実務向けリスク行列。

    曜日×信用区分×prob分類×時間帯ごとに、100株損益・%損益・PF・左尾・日次DDを返す。
    segment_mode: "4seg" | "11seg"
    """
    if weekday < 0 or weekday > 4:
        raise HTTPException(status_code=400, detail="weekdayは0-4")
    if direction not in ("short", "long"):
        raise HTTPException(status_code=400, detail="directionはshort/longのいずれか")
    if segment_mode not in ("4seg", "11seg"):
        raise HTTPException(status_code=400, detail="segment_modeは4seg/11segのいずれか")

    df = _load_analysis_base(exclude_extreme, direction=direction, include_legacy=include_legacy)
    df = df[df["date"].dt.weekday == weekday].copy()
    sign = _direction_multiplier(direction)
    segments = TIME_SEGMENTS_4 if segment_mode == "4seg" else TIME_SEGMENTS_11

    if "ml_prob" in df.columns:
        df["prob_group"] = df["ml_prob"].apply(_assign_prob_group)
    else:
        df["prob_group"] = None

    margin_groups = [
        {"key": "all", "label": "全体", "filter": lambda d: d},
        {"key": "seido", "label": "制度信用", "filter": lambda d: d[d["margin_type"] == "制度信用"]},
        {"key": "ichinichi_ex0", "label": "いちにち除0", "filter": lambda d: d[(d["margin_type"] == "いちにち信用") & (d["is_ex0"] == True)]},
    ]
    prob_groups = [
        {"key": "all", "label": "全体", "filter": lambda d: d},
        {"key": "low", "label": "LOW_PROB_HEAT", "filter": lambda d: d[d["prob_group"] == "LOW_PROB_HEAT"]},
        {"key": "mid", "label": "MID_PROB_HEAT", "filter": lambda d: d[d["prob_group"] == "MID_PROB_HEAT"]},
        {"key": "high", "label": "HIGH_PROB_HEAT", "filter": lambda d: d[d["prob_group"] == "HIGH_PROB_HEAT"]},
    ]

    rows = []
    for mg in margin_groups:
        mg_df = mg["filter"](df)
        for pg in prob_groups:
            sub = pg["filter"](mg_df)
            segment_rows = []
            for seg in segments:
                key = seg["key"]
                if key not in sub.columns:
                    amount_metrics = _risk_metrics(pd.Series(dtype=float))
                    pct_metrics = _risk_metrics(pd.Series(dtype=float))
                else:
                    amount_vals = sub[key] * sign
                    pct_vals = amount_vals / (sub["buy_price"] * 100) * 100
                    amount_metrics = _risk_metrics(amount_vals, sub["date"])
                    pct_metrics = _risk_metrics(pct_vals, sub["date"])
                segment_rows.append({
                    "key": key,
                    "label": seg["label"],
                    "time": seg["time"],
                    "amount": amount_metrics,
                    "pct": pct_metrics,
                })

            candidates = [s for s in segment_rows if s["amount"]["n"] >= MIN_N_THRESHOLD and s["amount"]["pf"] is not None]
            best = max(candidates, key=lambda s: (s["amount"]["pf"], s["amount"]["total"])) if candidates else None
            rows.append({
                "marginKey": mg["key"],
                "marginLabel": mg["label"],
                "probKey": pg["key"],
                "probLabel": pg["label"],
                "count": int(len(sub)),
                "bestSegment": {
                    "key": best["key"],
                    "label": best["label"],
                    "pf": best["amount"]["pf"],
                    "total": best["amount"]["total"],
                    "dailyMaxDD": best["amount"]["dailyMaxDD"],
                    "cvar05": best["amount"]["cvar05"],
                } if best else None,
                "segments": segment_rows,
            })

    return JSONResponse(content={
        "weekday": weekday,
        "weekdayName": WEEKDAY_NAMES[weekday],
        "direction": direction,
        "segmentMode": segment_mode,
        "timeSegments": segments,
        "dataRange": _make_date_range(df),
        "dataScope": df.attrs.get("data_scope"),
        "rows": rows,
    })


@router.get("/dev/analysis-custom/weekday-panels")
async def get_weekday_panels(
    weekday: int = 0,
    direction: str = "short",
    exclude_extreme: bool = False,
    include_legacy: bool = False,
):
    """曜日別分析パネル一括API（#2〜#6のデータを1リクエストで返す）

    weekday: 0=月〜4=金
    """
    if weekday < 0 or weekday > 4:
        raise HTTPException(status_code=400, detail="weekdayは0-4")
    if direction not in ("short", "long"):
        raise HTTPException(status_code=400, detail="directionはshort/longのいずれか")

    df = _load_analysis_base(exclude_extreme, direction=direction, include_legacy=include_legacy)
    df = df[df["date"].dt.weekday == weekday]
    sign = -1 if direction == "short" else 1

    result = {}

    # --- #2 流動性・ポジションサイズ ---
    liquidity = {}
    for bucket in BUCKET_LABELS:
        b_df = df[df["bucket"] == bucket] if "bucket" in df.columns else pd.DataFrame()
        if len(b_df) == 0:
            liquidity[bucket] = {"n": 0, "volume_median": None, "shares_median": None,
                                 "sell_balance_median": None, "buy_balance_median": None}
            continue
        liquidity[bucket] = {
            "n": len(b_df),
            "volume_median": int(b_df["volume"].median()) if "volume" in b_df.columns and b_df["volume"].notna().any() else None,
            "shares_median": int(b_df["day_trade_available_shares"].median()) if "day_trade_available_shares" in b_df.columns and b_df["day_trade_available_shares"].notna().any() else None,
            "sell_balance_median": int(b_df["margin_sell_balance"].median()) if "margin_sell_balance" in b_df.columns and b_df["margin_sell_balance"].notna().any() else None,
            "buy_balance_median": int(b_df["margin_buy_balance"].median()) if "margin_buy_balance" in b_df.columns and b_df["margin_buy_balance"].notna().any() else None,
        }
    result["liquidity"] = liquidity

    # --- #3 Phase遷移マトリクス ---
    phase_matrix = []
    if "phase1_return" in df.columns and "phase2_return" in df.columns:
        p1 = df["phase1_return"] * sign
        p2 = df["phase2_return"] * sign
        p1_bins = [
            {"label": "大損(<-2%)", "min": -999, "max": -0.02},
            {"label": "小損(-2~0%)", "min": -0.02, "max": 0},
            {"label": "小益(0~+2%)", "min": 0, "max": 0.02},
            {"label": "大益(>+2%)", "min": 0.02, "max": 999},
        ]
        for pb in p1_bins:
            mask = (p1 > pb["min"]) & (p1 <= pb["max"])
            sub = df[mask]
            p2_vals = (sub["phase2_return"] * sign) if len(sub) > 0 else pd.Series(dtype=float)
            n = len(p2_vals)
            phase_matrix.append({
                "phase1_label": pb["label"],
                "n": n,
                "phase2_avg_pct": round(float(p2_vals.mean() * 100), 2) if n > 0 else None,
                "phase2_win_rate": round(float((p2_vals > 0).mean() * 100), 1) if n > 0 else None,
                "phase2_pf": round(float(p2_vals[p2_vals > 0].sum() / abs(p2_vals[p2_vals <= 0].sum())), 2) if n > 0 and abs(p2_vals[p2_vals <= 0].sum()) > 0 else None,
            })
    result["phase_matrix"] = phase_matrix

    # --- #4 エクスカーション ---
    excursion = {}
    exc_cols = {
        "morning_max_gain_pct": "前場最大含み益%",
        "morning_max_drawdown_pct": "前場最大含み損%",
        "daily_max_gain_pct": "日中最大含み益%",
        "daily_max_drawdown_pct": "日中最大含み損%",
    }
    for col, label in exc_cols.items():
        if col not in df.columns:
            continue
        vals = df[col].dropna()
        # ショート方向の場合、gain/drawdownの符号を反転
        if direction == "short":
            if "gain" in col:
                vals = -vals  # ショートでの含み益=株価下落
            elif "drawdown" in col:
                vals = -vals  # ショートでの含み損=株価上昇
        if len(vals) == 0:
            excursion[col] = {"label": label, "n": 0, "p10": None, "p25": None, "p50": None, "p75": None, "p90": None}
            continue
        excursion[col] = {
            "label": label,
            "n": len(vals),
            "p10": round(float(vals.quantile(0.1)), 2),
            "p25": round(float(vals.quantile(0.25)), 2),
            "p50": round(float(vals.quantile(0.5)), 2),
            "p75": round(float(vals.quantile(0.75)), 2),
            "p90": round(float(vals.quantile(0.9)), 2),
        }
    result["excursion"] = excursion

    # --- #5 多段トリガー比較 ---
    stop_loss = []
    for pct in ["1pct", "2pct", "3pct"]:
        ret_col = f"phase3_{pct}_return"
        reason_col = f"phase3_{pct}_exit_reason"
        pnl_col = f"profit_per_100_shares_phase3_{pct}"
        if ret_col not in df.columns:
            continue
        vals = df[pnl_col].dropna() * sign if pnl_col in df.columns else df[ret_col].dropna() * sign
        n = len(vals)
        wins = vals[vals > 0].sum() if n > 0 else 0
        losses = abs(vals[vals <= 0].sum()) if n > 0 else 0
        pf = round(float(wins / losses), 2) if losses > 0 else None
        # Exit理由の内訳
        reason_counts = {}
        if reason_col in df.columns:
            reason_counts = df[reason_col].dropna().value_counts().to_dict()
            reason_counts = {k: int(v) for k, v in reason_counts.items()}
        stop_loss.append({
            "trigger": pct.replace("pct", "%"),
            "n": n,
            "pf": pf,
            "avg": round(float(vals.mean())) if n > 0 else None,
            "win_rate": round(float((vals > 0).mean() * 100), 1) if n > 0 else None,
            "exit_reasons": reason_counts,
        })
    result["stop_loss"] = stop_loss

    # --- #6 朝利確 vs 引けホールド ---
    hold_vs_exit = {}
    seg_pairs = [
        ("profit_per_100_shares_morning_early", "前場早期利確"),
        ("profit_per_100_shares_afternoon_early", "後場早期利確"),
        ("seg_1530", "引け決済"),
    ]
    for col, label in seg_pairs:
        if col not in df.columns:
            continue
        vals = df[col].dropna() * sign
        n = len(vals)
        wins = vals[vals > 0].sum() if n > 0 else 0
        losses = abs(vals[vals <= 0].sum()) if n > 0 else 0
        hold_vs_exit[col] = {
            "label": label,
            "n": n,
            "total_pnl": round(float(vals.sum())) if n > 0 else None,
            "avg": round(float(vals.mean())) if n > 0 else None,
            "pf": round(float(wins / losses), 2) if losses > 0 else None,
            "win_rate": round(float((vals > 0).mean() * 100), 1) if n > 0 else None,
        }
    # 吐き出し率: 引け決済 vs 前場最高値からの差
    if "daily_max_gain_pct" in df.columns and "seg_1530" in df.columns:
        valid_exc = df.dropna(subset=["daily_max_gain_pct", "seg_1530"])
        if len(valid_exc) > 0:
            max_gain = valid_exc["daily_max_gain_pct"].abs()
            final_pnl = (valid_exc["seg_1530"] * sign)
            # 利益のある銘柄のうち、最大含み益から引けまでに何%吐き出したか
            profitable = valid_exc[final_pnl > 0]
            if len(profitable) > 0:
                max_g = profitable["daily_max_gain_pct"].abs()
                final_r = (profitable["seg_1530"] * sign) / (profitable["buy_price"] * 100) * 100
                giveback_pct = float(((max_g - final_r.abs()) / max_g).clip(0, 1).mean() * 100)
                hold_vs_exit["giveback_pct"] = round(giveback_pct, 1)
    result["hold_vs_exit"] = hold_vs_exit

    result["weekday"] = weekday
    result["direction"] = direction
    result["n_total"] = len(df)
    result["dataRange"] = _make_date_range(df)
    result["dataScope"] = df.attrs.get("data_scope")

    return JSONResponse(content=result)
