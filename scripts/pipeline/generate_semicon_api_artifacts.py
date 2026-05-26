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
PRICE_PATH = ROOT / "data" / "parquet" / "prices_topix500_oc.parquet"
PRICE_SOURCE_LABEL = "data/parquet/prices_topix500_oc.parquet"
PRICE_COLUMNS = ["Date", "Code", "AdjO", "AdjH", "AdjL", "AdjC", "AdjV"]


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
    raw = pd.read_parquet(PRICE_PATH)
    missing_cols = [c for c in PRICE_COLUMNS if c not in raw.columns]
    if missing_cols:
        raise RuntimeError(f"{PRICE_PATH} is missing required columns: {missing_cols}")

    prices = raw[PRICE_COLUMNS].copy()
    prices["date"] = pd.to_datetime(prices["Date"], errors="coerce")
    prices["code"] = prices["Code"].astype(str).str[:4]
    prices = prices[prices["code"].isin(codes)].dropna(subset=["date", "AdjC"]).copy()
    if prices.empty:
        raise RuntimeError(f"no semicon codes found in {PRICE_PATH}")

    prices = prices.sort_values(["code", "date"])
    metrics: dict[str, dict[str, Any]] = {}
    for code, g in prices.groupby("code", sort=False):
        g = g.sort_values("date").reset_index(drop=True)
        if len(g) < 25:
            continue
        latest = g.iloc[-1]
        close = safe_float(latest.get("AdjC"))
        if close is None:
            continue
        prev_close = safe_float(g.iloc[-2].get("AdjC")) if len(g) >= 2 else None
        close_5 = safe_float(g.iloc[-6].get("AdjC")) if len(g) >= 6 else None
        close_20 = safe_float(g.iloc[-21].get("AdjC")) if len(g) >= 21 else None
        ma25 = safe_float(g["AdjC"].tail(25).mean())
        high20 = safe_float(g["AdjH"].tail(20).max()) if len(g) >= 20 else None
        latest_high = safe_float(latest.get("AdjH"))
        cvar = cvar05(g["AdjC"])
        metrics[code] = {
            "date": latest["date"].strftime("%Y-%m-%d"),
            "open": safe_float(latest.get("AdjO")),
            "high": latest_high,
            "low": safe_float(latest.get("AdjL")),
            "close": close,
            "ret1": pct(close, prev_close),
            "ret5": pct(close, close_5),
            "ret20": pct(close, close_20),
            "vs25": pct(close, ma25),
            "dist20hi": pct(close, high20),
            "max_dd_60d": max_dd(g["AdjC"].tail(60)),
            "cvar05": cvar,
            "entry_trigger_price": latest_high,
            "left_tail": "高" if cvar is not None and cvar <= -6 else "中" if cvar is not None and cvar <= -4 else "低",
        }

    data_date = prices["date"].max().strftime("%Y-%m-%d")
    return metrics, data_date


def enrich_payload_prices(payload: dict[str, Any]) -> dict[str, Any]:
    signals = payload.get("signals") or []
    if not isinstance(signals, list):
        signals = []
    metrics, data_date = price_metrics_by_code(signals)
    enriched = []
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        row = dict(signal)
        code = str(row.get("code") or "").strip()
        m = metrics.get(code)
        if not m:
            continue
        row.update(m)
        if row.get("decision") != "AVOID" and m.get("entry_trigger_price") is not None:
            row["entry_rule"] = f"公式日足の前日高値{m['entry_trigger_price']:.0f}超え確認。分足はVWAP上維持のみ参考"
        enriched.append(row)

    enriched = dev_semicon._attach_entry_decisions(dev_semicon._attach_trade_buckets(enriched))
    payload = dict(payload)
    payload.update(
        {
            "data_date": data_date,
            "price_data_date": data_date,
            "price_source": PRICE_SOURCE_LABEL,
            "signals": enriched,
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


def build_payload(mode: str) -> dict[str, Any]:
    payload = dev_semicon._build_payload_from_report() or dev_semicon.build_payload()
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
