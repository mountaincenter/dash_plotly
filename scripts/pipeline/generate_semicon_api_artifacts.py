from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.routers import dev_semicon  # noqa: E402


OUT_DIR = ROOT / "data" / "analysis"
PRICE_PATH = ROOT / "data" / "parquet" / "granville" / "prices_topix.parquet"
PRICE_SOURCE_LABEL = "data/parquet/granville/prices_topix.parquet"
FUTURES_PATH = ROOT / "data" / "parquet" / "futures_prices_max_1d.parquet"
FUTURES_SOURCE_LABEL = "data/parquet/futures_prices_max_1d.parquet"

MARKET_INDICATORS = [
    ("CL=F", "WTI原油", "地政学・インフレ圧力", "上昇は日本株のコスト増。半導体ロングは寄り天警戒", "down"),
    ("GC=F", "Gold", "安全資産・有事警戒", "上昇継続はリスクオフ警戒。株高と同時なら混在", "down"),
    ("HG=F", "Copper", "景気・電力/インフラ需要", "上昇は景気敏感・電力周辺に追い風。ただし原油高と併発なら混在", "up"),
    ("NKD=F", "日経CME", "日本寄付前の指数地合い", "強くても寄り天あり。寄り後30分の維持を確認", "up"),
]


def safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def pct(now: float | None, before: float | None) -> float | None:
    if now is None or before in {None, 0}:
        return None
    return float((now / before - 1.0) * 100.0)


def max_dd(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float((s / s.cummax() - 1.0).min() * 100.0)


def cvar05(series: pd.Series) -> float | None:
    ret = pd.to_numeric(series, errors="coerce").dropna().pct_change().dropna()
    if ret.empty:
        return None
    cutoff = ret.quantile(0.05)
    tail = ret[ret <= cutoff]
    if tail.empty:
        return None
    return float(tail.mean() * 100.0)


def price_metrics_by_code(signals: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], str | None]:
    if not PRICE_PATH.exists():
        raise FileNotFoundError(f"required price parquet not found: {PRICE_PATH}")

    codes = {str(s.get("code") or "").strip() for s in signals if isinstance(s, dict)}
    codes = {c for c in codes if c}
    tickers = {f"{c}.T" for c in codes}
    prices = pd.read_parquet(PRICE_PATH, columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices = prices[prices["ticker"].astype(str).isin(tickers)].dropna(subset=["date", "Close"]).copy()
    if prices.empty:
        raise RuntimeError(f"no semicon tickers found in {PRICE_PATH}")

    prices["code"] = prices["ticker"].astype(str).str.replace(".T", "", regex=False)
    prices = prices.sort_values(["code", "date"])
    metrics: dict[str, dict[str, Any]] = {}
    for code, g in prices.groupby("code", sort=False):
        g = g.sort_values("date").reset_index(drop=True)
        if len(g) < 25:
            continue
        latest = g.iloc[-1]
        close = safe_float(latest.get("Close"))
        if close is None:
            continue
        prev_close = safe_float(g.iloc[-2].get("Close")) if len(g) >= 2 else None
        close_5 = safe_float(g.iloc[-6].get("Close")) if len(g) >= 6 else None
        close_20 = safe_float(g.iloc[-21].get("Close")) if len(g) >= 21 else None
        ma25 = safe_float(g["Close"].tail(25).mean())
        high20 = safe_float(g["High"].tail(20).max()) if len(g) >= 20 else None
        latest_high = safe_float(latest.get("High"))
        cvar = cvar05(g["Close"])
        metrics[code] = {
            "date": latest["date"].strftime("%Y-%m-%d"),
            "open": safe_float(latest.get("Open")),
            "high": latest_high,
            "low": safe_float(latest.get("Low")),
            "close": close,
            "ret1": pct(close, prev_close),
            "ret5": pct(close, close_5),
            "ret20": pct(close, close_20),
            "vs25": pct(close, ma25),
            "dist20hi": pct(close, high20),
            "max_dd_60d": max_dd(g["Close"].tail(60)),
            "cvar05": cvar,
            "entry_trigger_price": latest_high,
            "left_tail": "高" if cvar is not None and cvar <= -6 else "中" if cvar is not None and cvar <= -4 else "低",
        }

    data_date = prices["date"].max().strftime("%Y-%m-%d")
    return metrics, data_date


def market_indicator_rows() -> tuple[list[dict[str, Any]], str | None]:
    if not FUTURES_PATH.exists():
        raise FileNotFoundError(f"required futures parquet not found: {FUTURES_PATH}")

    tickers = [ticker for ticker, *_ in MARKET_INDICATORS]
    raw = pd.read_parquet(FUTURES_PATH, columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw[raw["ticker"].astype(str).isin(tickers)].dropna(subset=["date", "Close"]).copy()
    if raw.empty:
        raise RuntimeError(f"no market indicators found in {FUTURES_PATH}")

    raw = raw.sort_values(["ticker", "date"])
    latest_date = raw["date"].max().strftime("%Y-%m-%d")
    rows: list[dict[str, Any]] = []
    for ticker, name, role, risk_note, good_when in MARKET_INDICATORS:
        g = raw[raw["ticker"].astype(str) == ticker].sort_values("date").reset_index(drop=True)
        if g.empty:
            rows.append(
                {
                    "ticker": ticker,
                    "name": name,
                    "role": role,
                    "risk_note": risk_note,
                    "good_when": good_when,
                    "source": FUTURES_SOURCE_LABEL,
                    "missing": True,
                }
            )
            continue
        latest = g.iloc[-1]
        close = safe_float(latest.get("Close"))
        rows.append(
            {
                "ticker": ticker,
                "name": name,
                "role": role,
                "risk_note": risk_note,
                "good_when": good_when,
                "date": latest["date"].strftime("%Y-%m-%d"),
                "open": safe_float(latest.get("Open")),
                "high": safe_float(latest.get("High")),
                "low": safe_float(latest.get("Low")),
                "close": close,
                "volume": safe_float(latest.get("Volume")),
                "ret1": pct(close, safe_float(g.iloc[-2].get("Close")) if len(g) >= 2 else None),
                "ret5": pct(close, safe_float(g.iloc[-6].get("Close")) if len(g) >= 6 else None),
                "ret20": pct(close, safe_float(g.iloc[-21].get("Close")) if len(g) >= 21 else None),
                "source": FUTURES_SOURCE_LABEL,
                "missing": False,
            }
        )
    return rows, latest_date


def enrich_payload_prices(payload: dict[str, Any]) -> dict[str, Any]:
    signals = payload.get("signals") or []
    if not isinstance(signals, list):
        signals = []
    metrics, data_date = price_metrics_by_code(signals)
    market_indicators, market_indicator_date = market_indicator_rows()
    enriched = []
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        row = dict(signal)
        code = str(row.get("code") or "").strip()
        m = metrics.get(code)
        if m:
            row.update(m)
            if row.get("decision") != "AVOID" and m.get("entry_trigger_price") is not None:
                row["entry_rule"] = f"公式日足の前日高値{m['entry_trigger_price']:.0f}超え確認。分足はVWAP上維持のみ参考"
        enriched.append(row)

    enriched = dev_semicon._attach_classification_meta(enriched)
    enriched = dev_semicon._attach_entry_decisions(dev_semicon._attach_trade_buckets(enriched))
    flow_analysis = dev_semicon._build_flow_analysis(enriched)
    morning_pilot = payload.get("morning_pilot") or dev_semicon._load_morning_pilot()
    payload = dict(payload)
    payload.update(
        {
            "data_date": data_date,
            "price_data_date": data_date,
            "price_source": PRICE_SOURCE_LABEL,
            "market_indicator_date": market_indicator_date,
            "market_indicator_source": FUTURES_SOURCE_LABEL,
            "market_indicators": market_indicators,
            "signals": enriched,
            "flow_analysis": flow_analysis,
            "morning_pilot": morning_pilot,
            "segment_strength": dev_semicon._build_segment_strength(enriched),
            "bucket_summary": dev_semicon._bucket_summary(enriched),
            "counts": {
                "buy": sum(1 for s in enriched if s.get("decision") == "BUY_CANDIDATE"),
                "watch": sum(1 for s in enriched if s.get("decision") == "WATCH"),
                "avoid": sum(1 for s in enriched if s.get("decision") == "AVOID"),
                "total": len(enriched),
            },
        }
    )
    return payload


def json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def build_universe_payload() -> dict[str, Any]:
    seed_signals = [{"code": stock.code} for stock in dev_semicon.UNIVERSE]
    metrics, data_date = price_metrics_by_code(seed_signals)
    market_indicators, market_indicator_date = market_indicator_rows()
    fundamentals = dev_semicon._load_fundamentals()
    signals = []
    for stock in dev_semicon.UNIVERSE:
        metric = metrics.get(stock.code)
        if not metric:
            continue
        signals.append(dev_semicon._score_stock(stock, metric, "NEUTRAL", fundamentals.get(stock.code, {})))

    signals = dev_semicon._attach_classification_meta(signals)
    signals = dev_semicon._attach_entry_decisions(dev_semicon._attach_trade_buckets(signals))
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "data_date": data_date,
        "price_data_date": data_date,
        "price_source": PRICE_SOURCE_LABEL,
        "market": {"state": "US_PENDING", "label": "米国判定待ち"},
        "signals": signals,
        "classification_basis": dev_semicon.CLASSIFICATION_BASIS,
        "segment_strength": dev_semicon._build_segment_strength(signals),
        "flow_analysis": dev_semicon._build_flow_analysis(signals),
        "bucket_summary": dev_semicon._bucket_summary(signals),
        "market_indicators": market_indicators,
        "market_indicator_date": market_indicator_date,
        "market_indicator_source": FUTURES_SOURCE_LABEL,
        "overseas": [],
        "report_available": dev_semicon.REPORT_PATH.exists(),
        "report_url": "/api/dev/semicon/report" if dev_semicon.REPORT_PATH.exists() else None,
        "source": PRICE_SOURCE_LABEL,
        "backtest": dev_semicon._load_backtest_summary(),
        "morning_pilot": dev_semicon._load_morning_pilot(),
        "hold_short_exposures": dev_semicon._load_hold_short_exposures(),
        "counts": {
            "buy": sum(1 for s in signals if s.get("decision") == "BUY_CANDIDATE"),
            "watch": sum(1 for s in signals if s.get("decision") == "WATCH"),
            "avoid": sum(1 for s in signals if s.get("decision") == "AVOID"),
            "total": len(signals),
        },
    }


def build_payload(mode: str) -> dict[str, Any]:
    payload = build_universe_payload()
    payload = enrich_payload_prices(dict(payload))
    payload["artifact_mode"] = mode
    payload["artifact_generated_at"] = datetime.now().astimezone().isoformat()

    if mode == "domestic":
        payload["us_pending"] = True
        payload["market"] = {
            "state": "US_PENDING",
            "label": "米国判定待ち",
            "previous_snapshot": payload.get("market"),
        }
    else:
        payload["us_pending"] = False

    return payload


def output_path(mode: str) -> Path:
    name = "semicon_domestic_candidates.json" if mode == "domestic" else "semicon_entry_decisions.json"
    return OUT_DIR / name


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate semicon API JSON artifacts from local semicon report outputs.")
    parser.add_argument("--mode", choices=["domestic", "entry"], required=True)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_payload(args.mode)
    path = output_path(args.mode)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")

    counts = payload.get("counts") or {}
    print(
        " ".join(
            [
                f"WROTE {path}",
                f"mode={args.mode}",
                f"source={payload.get('source')}",
                f"data_date={payload.get('data_date')}",
                f"signals={counts.get('total', len(payload.get('signals') or []))}",
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
