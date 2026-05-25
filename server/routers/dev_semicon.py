"""AI/semiconductor trend-following prototype API.

This is a semi-discretionary signal surface, not an auto-trading engine.
It turns the existing semiconductor risk report artifacts into a compact
BUY/WATCH/AVOID dashboard for /dev/semicon.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

ROOT = Path(__file__).resolve().parents[2]
SEMICON_OUT = ROOT / "scripts" / "analysis" / "semiconductor" / "output"
ANALYSIS_DIR = ROOT / "data" / "analysis"
PRICES_PATH = SEMICON_OUT / "prices_raw.parquet"
FUNDAMENTALS_PATH = SEMICON_OUT / "yfinance_fundamentals_summary.csv"
REPORT_PATH = SEMICON_OUT / "ai_semiconductor_yf_entry_risk_report.html"
BACKTEST_SUMMARY_PATH = SEMICON_OUT / "semicon_trend_backtest_summary.csv"
BACKTEST_REPORT_PATH = SEMICON_OUT / "semicon_trend_backtest_report.html"
HOLD_STOCKS_PATH = ROOT / "data" / "csv" / "hold_stocks.csv"

SEMICON_ENTRY_JSON = ANALYSIS_DIR / "semicon_entry_decisions.json"
SEMICON_DOMESTIC_JSON = ANALYSIS_DIR / "semicon_domestic_candidates.json"

S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET") or "stock-api-data"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
APP_ENV = (os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or os.getenv("STAGE") or "local").strip().lower()
RAW_SEMICON_DATA_SOURCE = (os.getenv("SEMICON_DATA_SOURCE") or "").strip().lower()
if RAW_SEMICON_DATA_SOURCE in {"local", "s3"}:
    SEMICON_DATA_SOURCE = RAW_SEMICON_DATA_SOURCE
    SEMICON_DATA_SOURCE_REASON = "SEMICON_DATA_SOURCE"
    SEMICON_DATA_SOURCE_ERROR = None
elif RAW_SEMICON_DATA_SOURCE:
    SEMICON_DATA_SOURCE = "local"
    SEMICON_DATA_SOURCE_REASON = "invalid_SEMICON_DATA_SOURCE"
    SEMICON_DATA_SOURCE_ERROR = f"invalid SEMICON_DATA_SOURCE={RAW_SEMICON_DATA_SOURCE!r}; expected local or s3"
elif APP_ENV in {"production", "prod"}:
    SEMICON_DATA_SOURCE = "s3"
    SEMICON_DATA_SOURCE_REASON = "APP_ENV"
    SEMICON_DATA_SOURCE_ERROR = None
else:
    SEMICON_DATA_SOURCE = "local"
    SEMICON_DATA_SOURCE_REASON = "default_local"
    SEMICON_DATA_SOURCE_ERROR = None
USE_S3_DATA_SOURCE = SEMICON_DATA_SOURCE == "s3"

S3_ENTRY_KEY = "analysis/semicon_entry_decisions.json"
S3_DOMESTIC_KEY = "analysis/semicon_domestic_candidates.json"
S3_REPORT_KEY = "analysis/semicon_entry_risk_report.html"
S3_BACKTEST_REPORT_KEY = "analysis/semicon_trend_backtest_report.html"

router = APIRouter()


@dataclass(frozen=True)
class SemiconStock:
    code: str
    name: str
    label: str
    segment: str
    market: str = "JP"


UNIVERSE = [
    SemiconStock("6857", "アドバンテスト", "A", "テスタ/検査"),
    SemiconStock("6146", "ディスコ", "A", "後工程装置"),
    SemiconStock("8035", "東京エレクトロン", "A", "エッチング/成膜/塗布"),
    SemiconStock("6920", "レーザーテック", "A", "EUVマスク検査"),
    SemiconStock("7735", "SCREEN", "A", "洗浄装置"),
    SemiconStock("6525", "KOKUSAI ELECTRIC", "A", "成膜装置"),
    SemiconStock("4062", "イビデン", "A", "パッケージ基板"),
    SemiconStock("4186", "東京応化工業", "A", "フォトレジスト"),
    SemiconStock("4004", "レゾナック", "A", "パッケージ/CMP材料"),
    SemiconStock("3436", "SUMCO", "A", "シリコンウエハ"),
    SemiconStock("4063", "信越化学工業", "A", "シリコンウエハ"),
    SemiconStock("6723", "ルネサス", "A", "マイコン/アナログ"),
    SemiconStock("285A", "キオクシア", "A", "NAND"),
    SemiconStock("6503", "三菱電機", "A", "パワー半導体/重電"),
    SemiconStock("6504", "富士電機", "A", "パワー半導体"),
    SemiconStock("6501", "日立製作所", "A/B", "検査装置/電力"),
    SemiconStock("5801", "古河電工", "B", "光通信/電力ケーブル/冷却"),
    SemiconStock("5803", "フジクラ", "B", "光通信/高密度配線"),
    SemiconStock("6981", "村田製作所", "B", "MLCC/電源部品/EMI"),
    SemiconStock("6367", "ダイキン工業", "B", "冷却/空調"),
    SemiconStock("5802", "住友電工", "C", "光通信/電線"),
    SemiconStock("5805", "SWCC", "C", "電線/電力ケーブル"),
    SemiconStock("6645", "オムロン", "C", "電源/制御"),
    SemiconStock("1963", "日揮HD", "C", "設備/EPC"),
    SemiconStock("1979", "大気社", "C", "空調/クリーンルーム"),
    SemiconStock("1802", "大林組", "D", "建設"),
    SemiconStock("1925", "大和ハウス", "D", "建設/不動産"),
    SemiconStock("8801", "三井不動産", "D", "不動産/DC"),
    SemiconStock("8802", "三菱地所", "D", "不動産/DC"),
]

OVERSEAS = {
    "NVDA": "NVIDIA",
    "^SOX": "SOX",
    "MU": "Micron",
    "TSM": "TSMC",
    "^IXIC": "NASDAQ",
    "NQ=F": "NASDAQ先物",
    "NKD=F": "日経先物CME",
    "JPY=X": "USDJPY",
}

SEGMENT_GROUPS = {
    "半導体主力": {"NAND", "成膜装置", "テスタ/検査", "後工程装置", "エッチング/成膜/塗布", "EUVマスク検査", "洗浄装置"},
    "AIインフラ": {"光通信/高密度配線", "光通信/電力ケーブル/冷却", "MLCC/電源部品/EMI", "パッケージ基板", "マイコン/アナログ", "成膜装置"},
    "電力": {"検査装置/電力", "パワー半導体/重電", "パワー半導体", "光通信/電線", "電線/電力ケーブル"},
    "光通信": {"光通信/高密度配線", "光通信/電力ケーブル/冷却", "光通信/電線", "電線/電力ケーブル"},
    "冷却/空調": {"冷却/空調", "空調/クリーンルーム"},
    "材料/基板": {"パッケージ基板", "フォトレジスト", "パッケージ/CMP材料", "シリコンウエハ"},
    "DC建設/不動産": {"建設", "建設/不動産", "不動産/DC", "設備/EPC"},
}

INDICATOR_CODES = {"6857", "6146", "8035", "6920", "7735", "285A"}
HEAVY_WATCH_CODES = {"4062", "5801"}
UNIVERSE_BY_CODE = {stock.code: stock for stock in UNIVERSE}
HOLD_EXPOSURE_EXTRA_BY_CODE = {
    "6055": SemiconStock("6055", "ジャパンマテリアル", "B", "特殊ガス/半導体材料"),
    "6323": SemiconStock("6323", "ローツェ", "A", "搬送装置/半導体装置"),
}


def _s3_client():
    import boto3

    return boto3.client("s3", region_name=AWS_REGION)


def _read_local_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def _read_s3_json(key: str) -> dict[str, Any] | None:
    if not S3_BUCKET:
        return None
    try:
        obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _read_semicon_json(local_path: Path, s3_key: str) -> dict[str, Any] | None:
    if USE_S3_DATA_SOURCE:
        return _read_s3_json(s3_key)
    return _read_local_json(local_path)


def _read_semicon_html(local_path: Path, s3_key: str) -> str | None:
    if USE_S3_DATA_SOURCE:
        if not S3_BUCKET:
            return None
        try:
            obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=s3_key)
            return obj["Body"].read().decode("utf-8")
        except Exception:
            return None
    if not local_path.exists():
        return None
    return local_path.read_text(encoding="utf-8")


def _safe_float(v: object) -> float | None:
    try:
        if v is None or pd.isna(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_percent(v: object) -> float | None:
    if v is None or pd.isna(v):
        return None
    text = str(v).replace("%", "").replace("+", "").replace(",", "").strip()
    if text in {"", "-", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_number(v: object) -> float | None:
    if v is None or pd.isna(v):
        return None
    text = str(v).replace(",", "").strip()
    if text in {"", "-", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pct(now: float, before: float) -> float | None:
    if before == 0 or pd.isna(now) or pd.isna(before):
        return None
    return float((now / before - 1.0) * 100.0)


def _max_dd(series: pd.Series) -> float | None:
    s = series.dropna().astype(float)
    if s.empty:
        return None
    return float((s / s.cummax() - 1.0).min() * 100.0)


def _cvar05(series: pd.Series) -> float | None:
    ret = series.dropna().astype(float).pct_change().dropna()
    if ret.empty:
        return None
    cutoff = ret.quantile(0.05)
    tail = ret[ret <= cutoff]
    if tail.empty:
        return None
    return float(tail.mean() * 100.0)


def _metric_row(code: str, series: pd.Series) -> dict[str, float | str | None]:
    s = series.dropna().astype(float)
    latest_date = s.index[-1].date().isoformat() if not s.empty and hasattr(s.index[-1], "date") else ""
    if len(s) < 80:
        return {"code": code, "date": latest_date, "missing": True}
    close = float(s.iloc[-1])
    ma25 = s.rolling(25).mean()
    hi20 = float(s.tail(20).max())
    ret5 = _pct(close, float(s.iloc[-6])) if len(s) >= 6 else None
    ret20 = _pct(close, float(s.iloc[-21])) if len(s) >= 21 else None
    vs25 = _pct(close, float(ma25.iloc[-1]))
    dist20hi = _pct(close, hi20)
    cvar05 = _cvar05(s)
    return {
        "code": code,
        "date": latest_date,
        "close": close,
        "ret5": ret5,
        "ret20": ret20,
        "vs25": vs25,
        "dist20hi": dist20hi,
        "max_dd_60d": _max_dd(s.tail(60)),
        "cvar05": cvar05,
        "missing": False,
    }


def _market_regime(rows: list[dict[str, Any]]) -> dict[str, Any]:
    lookup = {r["ticker"]: r for r in rows}
    keys = ["^SOX", "NVDA", "MU", "TSM", "^IXIC", "NQ=F"]
    positives = sum(1 for k in keys if (lookup.get(k, {}).get("ret5") or 0) > 0)
    negatives_1d = sum(1 for k in ["^SOX", "NVDA", "MU", "^IXIC"] if (lookup.get(k, {}).get("ret1") or 0) < -1.5)
    sox_ret5 = lookup.get("^SOX", {}).get("ret5")
    nvda_ret5 = lookup.get("NVDA", {}).get("ret5")
    if negatives_1d >= 2:
        state = "RISK_OFF"
        label = "米半導体が短期逆風"
    elif positives >= 4 and (sox_ret5 or 0) > 0 and (nvda_ret5 or 0) > 0:
        state = "RISK_ON"
        label = "米半導体が追い風"
    else:
        state = "NEUTRAL"
        label = "米地合いは中立"
    return {
        "state": state,
        "label": label,
        "positive_count": positives,
        "negative_1d_count": negatives_1d,
        "sox_ret5": sox_ret5,
        "nvda_ret5": nvda_ret5,
    }


def _score_stock(stock: SemiconStock, metrics: dict[str, Any], market_state: str, fundamentals: dict[str, Any]) -> dict[str, Any]:
    score = {"A": 4.0, "A/B": 3.5, "B": 2.0, "C": 0.5, "D": 0.0}.get(stock.label, 0.0)
    reasons = []
    warnings = []
    ret5 = metrics.get("ret5")
    ret20 = metrics.get("ret20")
    vs25 = metrics.get("vs25")
    dist20hi = metrics.get("dist20hi")
    cvar05 = metrics.get("cvar05")
    max_dd_60d = metrics.get("max_dd_60d")
    revenue_growth = fundamentals.get("yf_revenue_growth")
    op_margin = fundamentals.get("yf_operating_margin")

    if market_state == "RISK_ON":
        score += 2
        reasons.append("米半導体追い風")
    elif market_state == "RISK_OFF":
        score -= 3
        warnings.append("米半導体逆風")

    if ret5 is not None and ret5 > 0:
        score += 1.5
        reasons.append("5日順張り")
    if ret20 is not None and ret20 > 0:
        score += 1.0
        reasons.append("20日順張り")
    if vs25 is not None and vs25 > 0:
        score += 1.0
        reasons.append("25日線上")
    if dist20hi is not None and -5 <= dist20hi <= 0:
        score += 1.0
        reasons.append("20日高値圏")
    if revenue_growth is not None and revenue_growth > 20:
        score += 1.0
        reasons.append("売上成長")
    if op_margin is not None and op_margin > 20:
        score += 0.5
        reasons.append("高OP率")

    if vs25 is not None and vs25 > 18:
        score -= 2.0
        warnings.append("25日線乖離過熱")
    elif vs25 is not None and vs25 > 12:
        score -= 1.0
        warnings.append("寄り高注意")
    if cvar05 is not None and cvar05 <= -6:
        score -= 2.0
        warnings.append("左尾深い")
    elif cvar05 is not None and cvar05 <= -4:
        score -= 1.0
        warnings.append("左尾注意")
    if max_dd_60d is not None and max_dd_60d <= -25:
        score -= 1.0
        warnings.append("60日DD深い")

    if stock.label in {"C", "D"} and score >= 8:
        decision = "WATCH"
    elif score >= 9 and market_state != "RISK_OFF" and "左尾深い" not in warnings:
        decision = "BUY_CANDIDATE"
    elif score >= 5:
        decision = "WATCH"
    else:
        decision = "AVOID"

    if decision == "BUY_CANDIDATE":
        entry_rule = "前日高値超えまたは寄り後VWAP上維持。寄りで飛びすぎなら待つ"
    elif decision == "WATCH":
        entry_rule = "地合いと寄付差を確認。高寄り・左尾悪化なら入らない"
    else:
        entry_rule = "翌営業日の順張り対象外"

    return {
        "code": stock.code,
        "ticker": f"{stock.code}.T",
        "name": stock.name,
        "label": stock.label,
        "segment": stock.segment,
        "decision": decision,
        "score": round(score, 2),
        "reasons": reasons,
        "warnings": warnings,
        "entry_rule": entry_rule,
        **metrics,
        "revenue_growth": revenue_growth,
        "op_margin": op_margin,
    }


def _load_fundamentals() -> dict[str, dict[str, Any]]:
    if not FUNDAMENTALS_PATH.exists():
        return {}
    df = pd.read_csv(FUNDAMENTALS_PATH)
    rows = {}
    for _, row in df.iterrows():
        code = str(row.get("code", "")).strip()
        rows[code] = {
            "yf_revenue_growth": _safe_float(row.get("yf_revenue_growth")),
            "yf_operating_margin": _safe_float(row.get("yf_operating_margin")),
        }
    return rows


def _decision_from_report(value: object) -> str:
    text = str(value)
    if "見送り" in text:
        return "AVOID"
    return "WATCH"


def _entry_trigger_price(rule: object) -> float | None:
    text = str(rule)
    match = re.search(r"前日高値([0-9,]+(?:\.[0-9]+)?)超え", text)
    if match:
        return _parse_number(match.group(1))
    return None


def _segment_name(raw_segment: object) -> list[str]:
    segment = str(raw_segment)
    groups = [name for name, members in SEGMENT_GROUPS.items() if segment in members]
    return groups or ["その他"]


def _build_segment_strength(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded = []
    for signal in signals:
        for group in _segment_name(signal.get("segment")):
            expanded.append(
                {
                    "segment": group,
                    "code": signal.get("code"),
                    "name": signal.get("name"),
                    "ret5": signal.get("ret5"),
                    "ret20": signal.get("ret20"),
                    "vs25": signal.get("vs25"),
                    "score": signal.get("score"),
                    "decision": signal.get("decision"),
                }
            )
    if not expanded:
        return []

    df = pd.DataFrame(expanded)
    rows = []
    for segment, g in df.groupby("segment", sort=False):
        ret5 = pd.to_numeric(g["ret5"], errors="coerce")
        ret20 = pd.to_numeric(g["ret20"], errors="coerce")
        vs25 = pd.to_numeric(g["vs25"], errors="coerce")
        score = pd.to_numeric(g["score"], errors="coerce")
        leader_row = g.loc[score.fillna(-999).idxmax()] if not g.empty else None
        rows.append(
            {
                "segment": segment,
                "count": int(len(g)),
                "avg_ret5": _safe_float(ret5.mean()),
                "avg_ret20": _safe_float(ret20.mean()),
                "avg_vs25": _safe_float(vs25.mean()),
                "breadth5": _safe_float((ret5 > 0).mean() * 100.0),
                "breadth20": _safe_float((ret20 > 0).mean() * 100.0),
                "watch_count": int((g["decision"] != "AVOID").sum()),
                "leader_code": str(leader_row["code"]) if leader_row is not None else "",
                "leader_name": str(leader_row["name"]) if leader_row is not None else "",
                "leader_score": _safe_float(leader_row["score"]) if leader_row is not None else None,
            }
        )
    return sorted(rows, key=lambda r: ((r["avg_ret5"] or -999), (r["avg_ret20"] or -999)), reverse=True)


def _trade_bucket(signal: dict[str, Any]) -> tuple[str, list[str]]:
    code = str(signal.get("code", ""))
    decision = str(signal.get("decision", ""))
    close = _safe_float(signal.get("close"))
    ret5 = _safe_float(signal.get("ret5"))
    vs25 = _safe_float(signal.get("vs25"))
    cvar = _safe_float(signal.get("cvar05"))
    left_tail = str(signal.get("left_tail", ""))
    judgement = str(signal.get("judgement", ""))
    reasons: list[str] = []

    if code in INDICATOR_CODES:
        reasons.append("値嵩/主力の温度計")
        return "指標銘柄", reasons

    if decision == "AVOID" or "見送り" in judgement:
        reasons.append("判定が見送り")
        return "見送り", reasons

    if close is not None and close >= 20000:
        reasons.append("株価2万円以上")
        return "過熱注意", reasons

    if code in HEAVY_WATCH_CODES:
        reasons.append("値嵩寄りの監視銘柄")
        return "過熱注意", reasons

    if (vs25 is not None and vs25 >= 25) or (ret5 is not None and ret5 >= 18):
        reasons.append("短期過熱")
        return "過熱注意", reasons

    if left_tail == "高" or (cvar is not None and cvar <= -6):
        reasons.append("左尾高")
        return "過熱注意", reasons

    if "寄り後条件付き" in judgement or decision == "WATCH":
        reasons.append("寄り後条件付き")
        return "実弾候補", reasons

    reasons.append("条件未分類")
    return "見送り", reasons


def _attach_trade_buckets(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for signal in signals:
        bucket, reasons = _trade_bucket(signal)
        enriched = dict(signal)
        enriched["trade_bucket"] = bucket
        enriched["trade_bucket_reasons"] = reasons
        rows.append(enriched)
    return rows


def _bucket_summary(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = ["実弾候補", "指標銘柄", "過熱注意", "見送り"]
    rows = []
    for bucket in order:
        items = [s for s in signals if s.get("trade_bucket") == bucket]
        rows.append(
            {
                "bucket": bucket,
                "count": len(items),
                "leaders": [
                    {"code": s.get("code"), "name": s.get("name"), "score": s.get("score")}
                    for s in sorted(items, key=lambda x: _safe_float(x.get("score")) or 0, reverse=True)[:3]
                ],
            }
        )
    return rows


def _attach_entry_decisions(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    segment_lookup = {r["segment"]: r for r in _build_segment_strength(signals)}
    rows = []
    for signal in signals:
        bucket = str(signal.get("trade_bucket", ""))
        ret5 = _safe_float(signal.get("ret5"))
        ret20 = _safe_float(signal.get("ret20"))
        vs25 = _safe_float(signal.get("vs25"))
        score = _safe_float(signal.get("score")) or 0.0
        cvar = _safe_float(signal.get("cvar05"))
        left_tail = str(signal.get("left_tail", ""))
        groups = _segment_name(signal.get("segment"))
        group_stats = [segment_lookup[g] for g in groups if g in segment_lookup]
        segment_strong = any((g.get("avg_ret5") or 0) > 0 and (g.get("breadth5") or 0) >= 50 for g in group_stats)
        segment_hot = any((g.get("avg_ret5") or 0) >= 6 or (g.get("avg_ret20") or 0) >= 30 for g in group_stats)

        priority = score
        reasons: list[str] = []
        status = "WAIT"

        if bucket == "実弾候補":
            priority += 4
            reasons.append("実弾候補")
            status = "READY"
        elif bucket == "過熱注意":
            priority -= 1
            reasons.append("過熱注意")
            status = "WAIT"
        elif bucket == "指標銘柄":
            priority -= 2
            reasons.append("指標銘柄")
            status = "WAIT"
        else:
            priority -= 5
            reasons.append("見送り区分")
            status = "AVOID"

        if segment_strong:
            priority += 2
            reasons.append("セグメント強い")
        else:
            priority -= 2
            reasons.append("セグメント弱い")
            if status == "READY":
                status = "WAIT"

        if segment_hot:
            reasons.append("テーマ過熱")
            if bucket != "実弾候補":
                priority -= 1

        if ret5 is not None and ret5 > 0:
            priority += 1
            reasons.append("5日上昇")
        if ret20 is not None and ret20 > 0:
            priority += 0.5
            reasons.append("20日上昇")
        if vs25 is not None and vs25 > 0:
            priority += 0.5
            reasons.append("25日線上")

        if vs25 is not None and vs25 >= 25:
            priority -= 3
            reasons.append("25日線乖離過大")
            status = "WAIT" if status == "READY" else status
        if ret5 is not None and ret5 >= 18:
            priority -= 2
            reasons.append("5日急騰")
            status = "WAIT" if status == "READY" else status
        if left_tail == "高" or (cvar is not None and cvar <= -6):
            priority -= 2
            reasons.append("左尾注意")
            status = "WAIT" if status == "READY" else status

        enriched = dict(signal)
        enriched["entry_status"] = status
        enriched["entry_priority"] = round(priority, 2)
        enriched["entry_reasons"] = reasons
        rows.append(enriched)

    return sorted(rows, key=lambda r: (_safe_float(r.get("entry_priority")) or -999), reverse=True)


def _load_backtest_summary() -> dict[str, Any]:
    if not BACKTEST_SUMMARY_PATH.exists():
        return {
            "available": False,
            "rows": [],
            "report_url": None,
            "takeaway": "未検証",
        }

    df = pd.read_csv(BACKTEST_SUMMARY_PATH)
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rows.append(
            {
                "variant": str(row.get("variant", "")),
                "n": int(row.get("n", 0)),
                "days": int(row.get("days", 0)),
                "pf": _safe_float(row.get("pf")),
                "win_rate": _safe_float(row.get("win_rate")),
                "sum_pnl_100": _safe_float(row.get("sum_pnl_100")),
                "avg_pnl_100": _safe_float(row.get("avg_pnl_100")),
                "max_dd_100": _safe_float(row.get("max_dd_100")),
                "worst_trade_100": _safe_float(row.get("worst_trade_100")),
                "q05_100": _safe_float(row.get("q05_100")),
                "cvar05_100": _safe_float(row.get("cvar05_100")),
                "from": str(row.get("from", "")),
                "to": str(row.get("to", "")),
            }
        )

    by_variant = {r["variant"]: r for r in rows}
    top1 = by_variant.get("market_momentum_guard_top1") or by_variant.get("market_momentum_top1")
    top3 = by_variant.get("market_momentum_top3")
    if top1 and top3:
        takeaway = "市場モメンタム通過時もTop3は弱い。実運用候補はTop1限定"
    elif top1:
        takeaway = "実運用候補は市場モメンタム通過時のTop1限定"
    else:
        takeaway = "検証サマリー要確認"

    return {
        "available": True,
        "rows": rows,
        "report_url": "/api/dev/semicon/backtest-report" if BACKTEST_REPORT_PATH.exists() else None,
        "takeaway": takeaway,
    }


def _load_hold_short_exposures() -> list[dict[str, Any]]:
    if not HOLD_STOCKS_PATH.exists():
        return []
    try:
        df = pd.read_csv(HOLD_STOCKS_PATH)
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        code = str(row.get("コード", "")).strip()
        side = str(row.get("売買", "")).strip()
        stock = UNIVERSE_BY_CODE.get(code) or HOLD_EXPOSURE_EXTRA_BY_CODE.get(code)
        if stock is None or side != "売建":
            continue

        pnl = _parse_number(row.get("評価損益額合計(円)"))
        pnl_pct = _parse_number(row.get("評価損益率(%)"))
        qty = _parse_number(row.get("建玉数量合計(株/口)"))
        current_price = _parse_number(row.get("時価(円)"))
        entry_value = _parse_number(row.get("建玉金額合計(円)"))
        risk_level = "高" if (pnl_pct is not None and pnl_pct <= -20) else "中" if (pnl_pct is not None and pnl_pct <= -10) else "低"
        note = "AI/半導体周辺テーマの売建。踏み上げ警戒" if risk_level in {"高", "中"} else "テーマ該当売建"
        rows.append(
            {
                "code": code,
                "ticker": f"{code}.T",
                "name": str(row.get("銘柄名", stock.name)).strip(),
                "segment": stock.segment,
                "label": stock.label,
                "side": side,
                "quantity": qty,
                "current_price": current_price,
                "entry_value": entry_value,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "risk_level": risk_level,
                "note": note,
            }
        )
    return sorted(rows, key=lambda r: (_safe_float(r.get("pnl")) or 0.0))


def _build_payload_from_report() -> dict[str, Any] | None:
    if not REPORT_PATH.exists():
        return None
    try:
        tables = pd.read_html(REPORT_PATH)
    except ValueError:
        return None
    if len(tables) < 3:
        return None

    coverage = tables[0]
    overseas_table = tables[1]
    ranking = tables[2]

    data_date = None
    if {"データ", "最新日"}.issubset(coverage.columns):
        daily = coverage[coverage["データ"].astype(str).str.contains("J-Quants 公式日足", na=False)]
        if not daily.empty:
            data_date = str(daily.iloc[0]["最新日"])
    if not data_date and "日足基準日" in ranking.columns and not ranking.empty:
        data_date = str(ranking["日足基準日"].max())

    overseas_rows = []
    for _, row in overseas_table.iterrows():
        ticker = str(row.get("指標", "")).strip()
        overseas_rows.append(
            {
                "ticker": ticker,
                "name": OVERSEAS.get(ticker, ticker),
                "date": str(row.get("日付", "")),
                "close": _parse_number(row.get("終値")),
                "ret1": _parse_percent(row.get("1日%")),
                "ret5": _parse_percent(row.get("5日%")),
                "ret20": _parse_percent(row.get("20日%")),
            }
        )
    market = _market_regime(overseas_rows)

    signals = []
    for _, row in ranking.iterrows():
        decision = _decision_from_report(row.get("判定"))
        entry_rule = str(row.get("翌日条件", ""))
        entry_trigger = _entry_trigger_price(entry_rule)
        reasons = []
        warnings = []
        label = str(row.get("根拠", ""))
        if str(row.get("判定", "")) == "寄り後条件付き":
            reasons.append("HTML統合ランキング: 寄り後条件付き")
        if str(row.get("判定", "")) == "押し目/回復待ち":
            reasons.append("HTML統合ランキング: 押し目/回復待ち")
        if str(row.get("左尾", "")) == "高":
            warnings.append("左尾高")
        signals.append(
            {
                "code": str(row.get("コード", "")),
                "ticker": f"{row.get('コード')}.T",
                "name": str(row.get("銘柄", "")),
                "label": label,
                "segment": str(row.get("分類", "")),
                "decision": decision,
                "score": _parse_number(row.get("統合点")) or 0,
                "reasons": reasons,
                "warnings": warnings,
                "entry_rule": entry_rule,
                "entry_trigger_price": entry_trigger,
                "date": str(row.get("日足基準日", "")),
                "close": _parse_number(row.get("公式終値")),
                "ret5": _parse_percent(row.get("5日")),
                "ret20": _parse_percent(row.get("20日")),
                "vs25": _parse_percent(row.get("25日線比")),
                "dist20hi": _parse_percent(row.get("20日高値比")),
                "max_dd_60d": None,
                "cvar05": _parse_percent(row.get("CVaR5")),
                "revenue_growth": _parse_percent(row.get("営業益YoY")),
                "op_margin": None,
                "left_tail": str(row.get("左尾", "")),
                "judgement": str(row.get("判定", "")),
            }
        )
    signals = _attach_entry_decisions(_attach_trade_buckets(signals))

    parsed_date = pd.to_datetime(data_date, errors="coerce")
    stale_days = (date.today() - parsed_date.date()).days if not pd.isna(parsed_date) else None
    return {
        "generated_at": pd.Timestamp.fromtimestamp(REPORT_PATH.stat().st_mtime, tz="Asia/Tokyo").isoformat(),
        "data_date": data_date,
        "data_stale": stale_days is not None and stale_days >= 3,
        "stale_days": stale_days,
        "market": market,
        "signals": signals,
        "segment_strength": _build_segment_strength(signals),
        "bucket_summary": _bucket_summary(signals),
        "overseas": overseas_rows,
        "report_available": True,
        "report_url": "/api/dev/semicon/report",
        "source": "ai_semiconductor_yf_entry_risk_report.html",
        "operation": {
            "headline": "無条件買いなし。条件付き監視",
            "primary_action": "寄り後条件を満たす銘柄だけ小さく候補化",
            "morning_checks": [
                "SOX/NVIDIA/Micron/TSMC/NASDAQ先物/CMEを確認",
                "寄付差が過大なら待つ",
                "前日高値超えまたはVWAP上維持を確認",
                "左尾高の銘柄はロットを落とす",
            ],
            "avoid_rules": [
                "SOX/NVIDIA/Micronが同時に崩れる",
                "寄りで飛びすぎて前日高値超え後にVWAPを割る",
                "決算・材料・地政学でボラが読みにくい",
                "左尾高なのに通常ロットで入る",
            ],
        },
        "backtest": _load_backtest_summary(),
        "hold_short_exposures": _load_hold_short_exposures(),
        "counts": {
            "buy": sum(1 for s in signals if s["decision"] == "BUY_CANDIDATE"),
            "watch": sum(1 for s in signals if s["decision"] == "WATCH"),
            "avoid": sum(1 for s in signals if s["decision"] == "AVOID"),
            "total": len(signals),
        },
    }


def _normalize_semicon_payload(data: dict[str, Any], source: str, us_pending: bool = False) -> dict[str, Any]:
    signals = data.get("signals")
    if signals is None:
        signals = data.get("rows") or data.get("candidates") or []
    if not isinstance(signals, list):
        signals = []

    market = data.get("market")
    if not isinstance(market, dict):
        market = {
            "state": "US_PENDING" if us_pending else "NO_DATA",
            "label": "米国判定待ち" if us_pending else "データ未取得",
        }

    payload = dict(data)
    payload.update(
        {
            "generated_at": data.get("generated_at"),
            "data_date": data.get("data_date") or data.get("as_of") or data.get("date"),
            "market": market,
            "signals": signals,
            "segment_strength": data.get("segment_strength") or _build_segment_strength(signals),
            "bucket_summary": data.get("bucket_summary") or _bucket_summary(signals),
            "overseas": data.get("overseas") or [],
            "report_available": bool(data.get("report_available")),
            "report_url": data.get("report_url"),
            "backtest": data.get("backtest") or _load_backtest_summary(),
            "hold_short_exposures": data.get("hold_short_exposures") or _load_hold_short_exposures(),
            "source": source,
            "source_environment": APP_ENV,
            "source_data_mode": SEMICON_DATA_SOURCE,
            "source_data_mode_reason": SEMICON_DATA_SOURCE_REASON,
            "source_data_mode_error": SEMICON_DATA_SOURCE_ERROR,
            "us_pending": us_pending,
            "counts": data.get("counts")
            or {
                "buy": sum(1 for s in signals if isinstance(s, dict) and s.get("decision") == "BUY_CANDIDATE"),
                "watch": sum(1 for s in signals if isinstance(s, dict) and s.get("decision") == "WATCH"),
                "avoid": sum(1 for s in signals if isinstance(s, dict) and s.get("decision") == "AVOID"),
                "total": len(signals),
            },
        }
    )
    return payload


def _build_payload_from_environment_json() -> dict[str, Any] | None:
    entry = _read_semicon_json(SEMICON_ENTRY_JSON, S3_ENTRY_KEY)
    if entry is not None:
        source = S3_ENTRY_KEY if USE_S3_DATA_SOURCE else SEMICON_ENTRY_JSON.name
        return _normalize_semicon_payload(entry, source)

    domestic = _read_semicon_json(SEMICON_DOMESTIC_JSON, S3_DOMESTIC_KEY)
    if domestic is not None:
        source = S3_DOMESTIC_KEY if USE_S3_DATA_SOURCE else SEMICON_DOMESTIC_JSON.name
        return _normalize_semicon_payload(domestic, source, us_pending=True)

    return None


def build_payload() -> dict[str, Any]:
    env_payload = _build_payload_from_environment_json()
    if env_payload is not None:
        return env_payload

    if USE_S3_DATA_SOURCE:
        return {
            "generated_at": None,
            "data_date": None,
            "market": {"state": "NO_DATA", "label": "semicon S3 artifact not found"},
            "signals": [],
            "segment_strength": [],
            "bucket_summary": [],
            "overseas": [],
            "report_available": False,
            "source": "s3_json_missing",
            "source_environment": APP_ENV,
            "source_data_mode": SEMICON_DATA_SOURCE,
            "source_data_mode_reason": SEMICON_DATA_SOURCE_REASON,
            "source_data_mode_error": SEMICON_DATA_SOURCE_ERROR,
            "counts": {"buy": 0, "watch": 0, "avoid": 0, "total": 0},
        }

    report_payload = _build_payload_from_report()
    if report_payload is not None:
        report_payload["source_environment"] = APP_ENV
        report_payload["source_data_mode"] = SEMICON_DATA_SOURCE
        report_payload["source_data_mode_reason"] = SEMICON_DATA_SOURCE_REASON
        report_payload["source_data_mode_error"] = SEMICON_DATA_SOURCE_ERROR
        return report_payload

    if not PRICES_PATH.exists():
        return {
            "generated_at": None,
            "data_date": None,
            "market": {"state": "NO_DATA", "label": "データ未取得"},
            "signals": [],
            "overseas": [],
            "report_available": REPORT_PATH.exists(),
            "source_environment": APP_ENV,
            "source_data_mode": SEMICON_DATA_SOURCE,
            "source_data_mode_reason": SEMICON_DATA_SOURCE_REASON,
            "source_data_mode_error": SEMICON_DATA_SOURCE_ERROR,
        }

    prices = pd.read_parquet(PRICES_PATH).sort_index()
    fundamentals = _load_fundamentals()
    overseas_rows = []
    for ticker, name in OVERSEAS.items():
        if ticker not in prices.columns:
            continue
        metrics = _metric_row(ticker, prices[ticker])
        if metrics.get("missing"):
            continue
        overseas_rows.append(
            {
                "ticker": ticker,
                "name": name,
                "date": metrics.get("date"),
                "close": metrics.get("close"),
                "ret1": _pct(float(prices[ticker].dropna().iloc[-1]), float(prices[ticker].dropna().iloc[-2])) if len(prices[ticker].dropna()) >= 2 else None,
                "ret5": metrics.get("ret5"),
                "ret20": metrics.get("ret20"),
            }
        )
    market = _market_regime(overseas_rows)

    signals = []
    for stock in UNIVERSE:
        if stock.code not in prices.columns:
            continue
        metrics = _metric_row(stock.code, prices[stock.code])
        if metrics.get("missing"):
            continue
        signals.append(_score_stock(stock, metrics, market["state"], fundamentals.get(stock.code, {})))
    signals = _attach_entry_decisions(_attach_trade_buckets(signals))
    data_date = str(prices.index.max().date()) if not prices.empty and hasattr(prices.index.max(), "date") else None
    stale_days = (date.today() - prices.index.max().date()).days if data_date and hasattr(prices.index.max(), "date") else None
    return {
        "generated_at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
        "data_date": data_date,
        "data_stale": stale_days is not None and stale_days >= 3,
        "stale_days": stale_days,
        "market": market,
        "signals": signals,
        "segment_strength": _build_segment_strength(signals),
        "bucket_summary": _bucket_summary(signals),
        "overseas": overseas_rows,
        "report_available": REPORT_PATH.exists(),
        "report_url": "/api/dev/semicon/report",
        "source": "prices_raw.parquet",
        "source_environment": APP_ENV,
        "source_data_mode": SEMICON_DATA_SOURCE,
        "source_data_mode_reason": SEMICON_DATA_SOURCE_REASON,
        "source_data_mode_error": SEMICON_DATA_SOURCE_ERROR,
        "backtest": _load_backtest_summary(),
        "hold_short_exposures": _load_hold_short_exposures(),
        "counts": {
            "buy": sum(1 for s in signals if s["decision"] == "BUY_CANDIDATE"),
            "watch": sum(1 for s in signals if s["decision"] == "WATCH"),
            "avoid": sum(1 for s in signals if s["decision"] == "AVOID"),
            "total": len(signals),
        },
    }


@router.get("/api/dev/semicon/signals")
async def get_semicon_signals():
    return build_payload()


@router.get("/api/dev/semicon/report")
async def get_semicon_report():
    html = _read_semicon_html(REPORT_PATH, S3_REPORT_KEY)
    if html is None:
        return JSONResponse(status_code=404, content={"detail": "semiconductor report not found"})
    return HTMLResponse(content=html, media_type="text/html")


@router.get("/api/dev/semicon/backtest-report")
async def get_semicon_backtest_report():
    html = _read_semicon_html(BACKTEST_REPORT_PATH, S3_BACKTEST_REPORT_KEY)
    if html is None:
        return JSONResponse(status_code=404, content={"detail": "semiconductor backtest report not found"})
    return HTMLResponse(content=html, media_type="text/html")
