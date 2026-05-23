"""AI/semiconductor trend-following prototype API.

This is a semi-discretionary signal surface, not an auto-trading engine.
It turns the existing semiconductor risk report artifacts into a compact
BUY/WATCH/AVOID dashboard for /dev/semicon.
"""
from __future__ import annotations

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
PRICES_PATH = SEMICON_OUT / "prices_raw.parquet"
FUNDAMENTALS_PATH = SEMICON_OUT / "yfinance_fundamentals_summary.csv"
REPORT_PATH = SEMICON_OUT / "ai_semiconductor_yf_entry_risk_report.html"

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
                "entry_rule": str(row.get("翌日条件", "")),
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
    signals = sorted(signals, key=lambda r: (r["decision"] != "AVOID", r["score"]), reverse=True)

    parsed_date = pd.to_datetime(data_date, errors="coerce")
    stale_days = (date.today() - parsed_date.date()).days if not pd.isna(parsed_date) else None
    return {
        "generated_at": pd.Timestamp.fromtimestamp(REPORT_PATH.stat().st_mtime, tz="Asia/Tokyo").isoformat(),
        "data_date": data_date,
        "data_stale": stale_days is not None and stale_days >= 3,
        "stale_days": stale_days,
        "market": market,
        "signals": signals,
        "overseas": overseas_rows,
        "report_available": True,
        "report_url": "/api/dev/semicon/report",
        "source": "ai_semiconductor_yf_entry_risk_report.html",
        "counts": {
            "buy": sum(1 for s in signals if s["decision"] == "BUY_CANDIDATE"),
            "watch": sum(1 for s in signals if s["decision"] == "WATCH"),
            "avoid": sum(1 for s in signals if s["decision"] == "AVOID"),
            "total": len(signals),
        },
    }


def build_payload() -> dict[str, Any]:
    report_payload = _build_payload_from_report()
    if report_payload is not None:
        return report_payload

    if not PRICES_PATH.exists():
        return {
            "generated_at": None,
            "data_date": None,
            "market": {"state": "NO_DATA", "label": "データ未取得"},
            "signals": [],
            "overseas": [],
            "report_available": REPORT_PATH.exists(),
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
    signals = sorted(signals, key=lambda r: (r["decision"] == "BUY_CANDIDATE", r["score"]), reverse=True)
    data_date = str(prices.index.max().date()) if not prices.empty and hasattr(prices.index.max(), "date") else None
    stale_days = (date.today() - prices.index.max().date()).days if data_date and hasattr(prices.index.max(), "date") else None
    return {
        "generated_at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
        "data_date": data_date,
        "data_stale": stale_days is not None and stale_days >= 3,
        "stale_days": stale_days,
        "market": market,
        "signals": signals,
        "overseas": overseas_rows,
        "report_available": REPORT_PATH.exists(),
        "report_url": "/api/dev/semicon/report",
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
    if not REPORT_PATH.exists():
        return JSONResponse(status_code=404, content={"detail": "semiconductor report not found"})
    return HTMLResponse(content=REPORT_PATH.read_text(encoding="utf-8"), media_type="text/html")
