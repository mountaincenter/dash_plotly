#!/usr/bin/env python3
"""Append frozen 200A market-flow shadow signals and evaluate the Phase 3 gate."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import download_file


FEATURES_PATH = PARQUET_DIR / "jquants_minute_watch_features.parquet"
SEMICON_UNIVERSE_PATH = PARQUET_DIR / "semicon_watch_universe.parquet"
FORWARD_PATH = PARQUET_DIR / "market_flow_200a_forward.parquet"
STATUS_PATH = PARQUET_DIR / "market_flow_200a_phase_status.json"

STRATEGY_VERSION = "mf200a_v1_20260723"
FROZEN_AT = "2026-07-22"
FORWARD_START_DATE = "2026-07-23"
ROUND_TRIP_COST_BPS = 5.0
SHADOW_SHARES = 30
PHASE3_STAGE1_SHARES = 1

ROUTES = (
    {
        "rule_id": "S_0930_SELF_TO_1400",
        "priority": 1,
        "side": "short",
        "signal_time": "09:30",
        "exit_time": "14:00",
        "definition": "200A is below session open and VWAP at 09:30.",
    },
    {
        "rule_id": "S_1000_BREADTH_TO_1130",
        "priority": 2,
        "side": "short",
        "signal_time": "10:00",
        "exit_time": "11:30",
        "definition": (
            "200A is below open/VWAP, semicon weighted return is negative, "
            "semicon above-VWAP rate is <= 40%, and 2644 is below VWAP at 10:00."
        ),
    },
    {
        "rule_id": "L_1400_STRICT_TO_1530",
        "priority": 3,
        "side": "long",
        "signal_time": "14:00",
        "exit_time": "15:30",
        "definition": (
            "200A is above open/VWAP, semicon weighted return is positive, "
            "semicon above-VWAP rate is >= 60%, and 2644 is above open/VWAP at 14:00."
        ),
    },
)

PHASE3_GATE = {
    "minimum_completed_primary_signals": 20,
    "minimum_net_profit_factor": 1.20,
    "minimum_net_average_bps": 0.0,
    "minimum_execution_completion_rate_pct": 90.0,
    "maximum_consecutive_losses": 5,
}

FROZEN_BACKTEST = {
    "source": "market_flow_200a_forward_outcomes.parquet",
    "source_sha256": "5662578bce495321405bcb15a366054e37591b3bcf7c93d61d517b1b4de14a3b",
    "date_min": "2025-07-18",
    "date_max": "2026-07-22",
    "round_trip_cost_bps": 5.0,
    "one_primary_trade_per_day": True,
    "all": {"n": 148, "net_average_bps": 21.21, "net_profit_factor": 1.59},
    "train": {"n": 99, "net_average_bps": 12.85, "net_profit_factor": 1.45},
    "test": {"n": 49, "net_average_bps": 38.10, "net_profit_factor": 1.75},
}

FORWARD_COLUMNS = [
    "strategy_version",
    "trade_date",
    "rule_id",
    "priority",
    "side",
    "signal_time",
    "exit_time",
    "signal_available",
    "triggered",
    "primary_selected",
    "execution_status",
    "entry_time",
    "entry_price",
    "exit_price",
    "gross_return_bps",
    "net_return_bps",
    "shadow_pnl_yen_30",
    "phase3_stage1_pnl_yen_1",
    "etf_200a_close",
    "etf_200a_session_open",
    "etf_200a_session_vwap",
    "etf_200a_ret_from_open_pct",
    "etf_200a_above_vwap",
    "etf_2644_close",
    "etf_2644_session_open",
    "etf_2644_session_vwap",
    "etf_2644_ret_from_open_pct",
    "etf_2644_above_vwap",
    "semicon_weighted_open_return_pct",
    "semicon_above_vwap_rate_pct",
    "semicon_active_count",
    "recorded_at",
]


def resolve_storage_mode() -> tuple[str, bool]:
    app_env = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("STAGE")
        or "local"
    ).strip().lower()
    production = app_env in {"production", "prod"}
    expected = "s3" if production else "local"
    configured = (os.getenv("STORAGE_MODE") or expected).strip().lower()
    if configured != expected:
        raise RuntimeError(
            f"storage mode mismatch: APP_ENV={app_env} requires STORAGE_MODE={expected}, "
            f"got {configured}"
        )
    return app_env, production


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate frozen 200A forward-shadow records.")
    parser.add_argument("--features", type=Path, default=FEATURES_PATH)
    parser.add_argument("--semicon-universe", type=Path, default=SEMICON_UNIVERSE_PATH)
    parser.add_argument("--forward-output", type=Path, default=FORWARD_PATH)
    parser.add_argument("--status-output", type=Path, default=STATUS_PATH)
    parser.add_argument("--start-date", default=FORWARD_START_DATE)
    parser.add_argument("--no-s3-restore", action="store_true")
    return parser.parse_args()


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, engine="pyarrow", index=False)
    os.replace(temporary, path)


def atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def restore_forward_from_s3_if_missing(path: Path) -> bool:
    _, use_s3 = resolve_storage_mode()
    if not use_s3:
        return path.exists()
    config = load_s3_config()
    if not config.bucket:
        raise RuntimeError("Production S3 bucket is not configured for 200A forward history")
    if not download_file(config, path.name, path):
        raise RuntimeError(
            f"Production S3 restore failed for 200A forward history: {path.name}"
        )
    return True


def normalize_features(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "trading_date",
        "time",
        "ticker",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "cum_value",
        "session_open",
        "session_vwap",
        "ret_from_open_pct",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"minute feature columns missing: {missing}")

    out = frame.copy()
    out["trading_date"] = pd.to_datetime(out["trading_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out["time"] = out["time"].astype(str).str.slice(0, 5)
    for column in [
        "open",
        "high",
        "low",
        "close",
        "cum_value",
        "session_open",
        "session_vwap",
        "ret_from_open_pct",
    ]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.dropna(subset=["trading_date", "ticker", "datetime", "close"]).copy()
    return out.sort_values(["trading_date", "ticker", "datetime"], kind="mergesort").reset_index(drop=True)


def active_semicon_stock_tickers(universe: pd.DataFrame) -> set[str]:
    required = {"ticker", "instrument_type", "active"}
    missing = sorted(required - set(universe.columns))
    if missing:
        raise ValueError(f"semicon universe columns missing: {missing}")
    active = universe["active"].fillna(False).astype(bool)
    stocks = universe["instrument_type"].astype(str).eq("stock")
    return set(universe.loc[active & stocks, "ticker"].astype(str))


def state_at(day: pd.DataFrame, signal_time: str) -> pd.DataFrame:
    state = day[day["time"].le(signal_time)].copy()
    if state.empty:
        return state
    state = state.sort_values(["ticker", "datetime"], kind="mergesort")
    return state.drop_duplicates("ticker", keep="last").reset_index(drop=True)


def ticker_state(state: pd.DataFrame, ticker: str) -> pd.Series | None:
    rows = state[state["ticker"].astype(str).eq(ticker)]
    return rows.iloc[-1] if not rows.empty else None


def above_vwap(row: pd.Series | None) -> bool | None:
    if row is None:
        return None
    close = finite(row.get("close"))
    vwap = finite(row.get("session_vwap"))
    return close >= vwap if close is not None and vwap is not None else None


def weighted_semicon_state(state: pd.DataFrame, tickers: set[str]) -> dict[str, Any]:
    semicon = state[state["ticker"].astype(str).isin(tickers)].copy()
    semicon = semicon[
        semicon["cum_value"].gt(0)
        & semicon["ret_from_open_pct"].notna()
        & semicon["session_vwap"].notna()
    ]
    if semicon.empty:
        return {
            "weighted_return": None,
            "above_vwap_rate": None,
            "active_count": 0,
        }
    weighted_return = float(
        np.average(semicon["ret_from_open_pct"], weights=semicon["cum_value"])
    )
    rate = float(semicon["close"].ge(semicon["session_vwap"]).mean() * 100.0)
    return {
        "weighted_return": weighted_return,
        "above_vwap_rate": rate,
        "active_count": int(semicon["ticker"].nunique()),
    }


def route_triggered(
    rule_id: str,
    row_200a: pd.Series,
    row_2644: pd.Series | None,
    semicon: dict[str, Any],
) -> bool:
    return_200a = finite(row_200a.get("ret_from_open_pct"))
    above_200a = above_vwap(row_200a)
    if return_200a is None or above_200a is None:
        return False
    if rule_id == "S_0930_SELF_TO_1400":
        return return_200a < 0 and not above_200a

    return_2644 = finite(row_2644.get("ret_from_open_pct")) if row_2644 is not None else None
    above_2644 = above_vwap(row_2644)
    weighted_return = finite(semicon.get("weighted_return"))
    vwap_rate = finite(semicon.get("above_vwap_rate"))
    if None in {return_2644, above_2644, weighted_return, vwap_rate}:
        return False
    if rule_id == "S_1000_BREADTH_TO_1130":
        return (
            return_200a < 0
            and not above_200a
            and weighted_return < 0
            and vwap_rate <= 40
            and not above_2644
        )
    if rule_id == "L_1400_STRICT_TO_1530":
        return (
            return_200a > 0
            and above_200a
            and weighted_return > 0
            and vwap_rate >= 60
            and return_2644 > 0
            and above_2644
        )
    raise ValueError(f"unknown rule: {rule_id}")


def execution_outcome(
    day_200a: pd.DataFrame,
    *,
    signal_time: str,
    exit_time: str,
    side: str,
) -> dict[str, Any]:
    future = day_200a[day_200a["time"].gt(signal_time)].sort_values("datetime", kind="mergesort")
    if future.empty:
        return {"execution_status": "missing_entry"}
    entry = future.iloc[0]
    eligible_exit = future[
        future["time"].le(exit_time) & future["datetime"].ge(entry["datetime"])
    ]
    if eligible_exit.empty:
        return {
            "execution_status": "missing_exit",
            "entry_time": str(entry["time"]),
            "entry_price": finite(entry.get("open")),
        }
    entry_price = finite(entry.get("open"))
    exit_price = finite(eligible_exit.iloc[-1].get("close"))
    if entry_price is None or entry_price <= 0 or exit_price is None:
        return {"execution_status": "invalid_price"}
    long_bps = (exit_price / entry_price - 1.0) * 10_000.0
    gross_bps = long_bps if side == "long" else -long_bps
    net_bps = gross_bps - ROUND_TRIP_COST_BPS
    return {
        "execution_status": "completed",
        "entry_time": str(entry["time"]),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return_bps": gross_bps,
        "net_return_bps": net_bps,
        "shadow_pnl_yen_30": net_bps / 10_000.0 * entry_price * SHADOW_SHARES,
        "phase3_stage1_pnl_yen_1": net_bps / 10_000.0 * entry_price * PHASE3_STAGE1_SHARES,
    }


def build_forward_rows(
    features: pd.DataFrame,
    semicon_tickers: set[str],
    *,
    start_date: str = FORWARD_START_DATE,
    recorded_at: str | None = None,
) -> pd.DataFrame:
    minute = normalize_features(features)
    minute = minute[minute["trading_date"].ge(start_date)].copy()
    if minute.empty:
        return pd.DataFrame(columns=FORWARD_COLUMNS)
    recorded = recorded_at or datetime.now().astimezone().isoformat()
    records: list[dict[str, Any]] = []

    for trade_date, day in minute.groupby("trading_date", sort=True):
        day_200a = day[day["ticker"].astype(str).eq("200A.T")].copy()
        day_rows: list[dict[str, Any]] = []
        for route in ROUTES:
            state = state_at(day, str(route["signal_time"]))
            row_200a = ticker_state(state, "200A.T")
            row_2644 = ticker_state(state, "2644.T")
            semicon = weighted_semicon_state(state, semicon_tickers)
            signal_available = row_200a is not None and (
                route["rule_id"] == "S_0930_SELF_TO_1400"
                or (
                    row_2644 is not None
                    and semicon["weighted_return"] is not None
                    and semicon["above_vwap_rate"] is not None
                )
            )
            triggered = bool(
                signal_available
                and route_triggered(str(route["rule_id"]), row_200a, row_2644, semicon)
            )
            record: dict[str, Any] = {
                "strategy_version": STRATEGY_VERSION,
                "trade_date": str(trade_date),
                "rule_id": route["rule_id"],
                "priority": route["priority"],
                "side": route["side"],
                "signal_time": route["signal_time"],
                "exit_time": route["exit_time"],
                "signal_available": bool(signal_available),
                "triggered": triggered,
                "primary_selected": False,
                "execution_status": "not_triggered" if signal_available else "missing_signal_inputs",
                "etf_200a_close": finite(row_200a.get("close")) if row_200a is not None else None,
                "etf_200a_session_open": finite(row_200a.get("session_open")) if row_200a is not None else None,
                "etf_200a_session_vwap": finite(row_200a.get("session_vwap")) if row_200a is not None else None,
                "etf_200a_ret_from_open_pct": finite(row_200a.get("ret_from_open_pct")) if row_200a is not None else None,
                "etf_200a_above_vwap": above_vwap(row_200a),
                "etf_2644_close": finite(row_2644.get("close")) if row_2644 is not None else None,
                "etf_2644_session_open": finite(row_2644.get("session_open")) if row_2644 is not None else None,
                "etf_2644_session_vwap": finite(row_2644.get("session_vwap")) if row_2644 is not None else None,
                "etf_2644_ret_from_open_pct": finite(row_2644.get("ret_from_open_pct")) if row_2644 is not None else None,
                "etf_2644_above_vwap": above_vwap(row_2644),
                "semicon_weighted_open_return_pct": finite(semicon["weighted_return"]),
                "semicon_above_vwap_rate_pct": finite(semicon["above_vwap_rate"]),
                "semicon_active_count": int(semicon["active_count"]),
                "recorded_at": recorded,
            }
            if triggered:
                record.update(
                    execution_outcome(
                        day_200a,
                        signal_time=str(route["signal_time"]),
                        exit_time=str(route["exit_time"]),
                        side=str(route["side"]),
                    )
                )
            day_rows.append(record)

        triggered_rows = [row for row in day_rows if row["triggered"]]
        if triggered_rows:
            primary = min(triggered_rows, key=lambda row: int(row["priority"]))
            primary["primary_selected"] = True
        records.extend(day_rows)

    output = pd.DataFrame(records)
    for column in FORWARD_COLUMNS:
        if column not in output.columns:
            output[column] = None
    return output[FORWARD_COLUMNS].sort_values(
        ["trade_date", "priority"], kind="mergesort"
    ).reset_index(drop=True)


def merge_forward(existing: pd.DataFrame, generated: pd.DataFrame) -> pd.DataFrame:
    frames = [frame for frame in [existing, generated] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=FORWARD_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    for column in FORWARD_COLUMNS:
        if column not in combined.columns:
            combined[column] = None
    combined = combined[FORWARD_COLUMNS]
    combined = combined.sort_values(
        ["strategy_version", "trade_date", "priority", "recorded_at"],
        kind="mergesort",
    )
    combined = combined.drop_duplicates(
        ["strategy_version", "trade_date", "rule_id"], keep="last"
    )
    return combined.sort_values(["trade_date", "priority"], kind="mergesort").reset_index(drop=True)


def profit_factor(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(clean[clean.gt(0)].sum())
    losses = float(-clean[clean.lt(0)].sum())
    return gains / losses if losses > 0 else None


def maximum_drawdown(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    cumulative = clean.cumsum().to_numpy()
    peaks = np.maximum.accumulate(np.r_[0.0, cumulative])[:-1]
    return float((cumulative - peaks).min())


def maximum_loss_streak(values: pd.Series) -> int:
    longest = 0
    current = 0
    for value in pd.to_numeric(values, errors="coerce").dropna():
        current = current + 1 if value < 0 else 0
        longest = max(longest, current)
    return longest


def build_phase_status(forward: pd.DataFrame) -> dict[str, Any]:
    current = forward[forward["strategy_version"].eq(STRATEGY_VERSION)].copy()
    selected = current[current["primary_selected"].fillna(False).astype(bool)].copy()
    completed = selected[selected["execution_status"].eq("completed")].sort_values("trade_date")
    selected_count = int(len(selected))
    completed_count = int(len(completed))
    completion_rate = completed_count / selected_count * 100.0 if selected_count else 0.0
    net = pd.to_numeric(completed.get("net_return_bps", pd.Series(dtype=float)), errors="coerce").dropna()
    pnl_30 = pd.to_numeric(completed.get("shadow_pnl_yen_30", pd.Series(dtype=float)), errors="coerce").dropna()
    pf = profit_factor(net)
    average = finite(net.mean()) if not net.empty else None
    median = finite(net.median()) if not net.empty else None
    win_rate = finite(net.gt(0).mean() * 100.0) if not net.empty else None
    max_streak = maximum_loss_streak(net)
    profit_factor_passed = bool(
        (pf is not None and pf >= PHASE3_GATE["minimum_net_profit_factor"])
        or (not net.empty and not net.lt(0).any() and net.gt(0).any())
    )

    checks = {
        "sample_count": completed_count >= PHASE3_GATE["minimum_completed_primary_signals"],
        "profit_factor": profit_factor_passed,
        "average_return": average is not None and average > PHASE3_GATE["minimum_net_average_bps"],
        "execution_completion": completion_rate >= PHASE3_GATE["minimum_execution_completion_rate_pct"],
        "loss_streak": max_streak <= PHASE3_GATE["maximum_consecutive_losses"],
    }
    eligible = all(checks.values())
    blocked_reasons = [key for key, passed in checks.items() if not passed]
    latest_primary = None
    if not selected.empty:
        latest = selected.sort_values(["trade_date", "priority"]).iloc[-1]
        latest_primary = {
            "trade_date": str(latest.get("trade_date")),
            "rule_id": str(latest.get("rule_id")),
            "side": str(latest.get("side")),
            "execution_status": str(latest.get("execution_status")),
            "net_return_bps": finite(latest.get("net_return_bps")),
            "shadow_pnl_yen_30": finite(latest.get("shadow_pnl_yen_30")),
        }

    return {
        "schema_version": 1,
        "generated_at": datetime.now().astimezone().isoformat(),
        "strategy_version": STRATEGY_VERSION,
        "frozen_at": FROZEN_AT,
        "forward_start_date": FORWARD_START_DATE,
        "mode": "phase3_live_eligible" if eligible else "phase2_forward_shadow",
        "phase3_live_eligible": eligible,
        "automatic_ordering": False,
        "phase3_stage1_shares": PHASE3_STAGE1_SHARES,
        "one_primary_trade_per_day": True,
        "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
        "gate": PHASE3_GATE,
        "gate_checks": checks,
        "blocked_reasons": blocked_reasons,
        "forward_metrics": {
            "calendar_dates": int(current["trade_date"].nunique()) if not current.empty else 0,
            "triggered_shadow_signals": int(current["triggered"].fillna(False).astype(bool).sum()) if not current.empty else 0,
            "selected_primary_signals": selected_count,
            "completed_primary_signals": completed_count,
            "execution_completion_rate_pct": completion_rate,
            "net_average_bps": average,
            "net_median_bps": median,
            "net_win_rate_pct": win_rate,
            "net_profit_factor": pf,
            "maximum_consecutive_losses": max_streak,
            "total_shadow_pnl_yen_30": finite(pnl_30.sum()) if not pnl_30.empty else 0.0,
            "maximum_shadow_drawdown_yen_30": maximum_drawdown(pnl_30),
        },
        "latest_primary": latest_primary,
        "routes": list(ROUTES),
        "frozen_backtest": FROZEN_BACKTEST,
        "limitations": [
            "Phase 3 eligibility is a gate, not an automated order.",
            "Observed broker spread, stock-loan availability, and order latency are not in J-Quants minute bars.",
            "Rules and thresholds must remain frozen throughout Phase 2.",
        ],
    }


def main() -> int:
    args = parse_args()
    app_env, use_s3 = resolve_storage_mode()
    if not args.features.exists():
        raise FileNotFoundError(f"minute features not found: {args.features}")
    if not args.semicon_universe.exists():
        raise FileNotFoundError(f"semicon universe not found: {args.semicon_universe}")
    if args.no_s3_restore and use_s3:
        raise RuntimeError("--no-s3-restore is not allowed in production")
    if not args.no_s3_restore:
        restore_forward_from_s3_if_missing(args.forward_output)

    features = pd.read_parquet(args.features)
    universe = pd.read_parquet(args.semicon_universe)
    existing = pd.read_parquet(args.forward_output) if args.forward_output.exists() else pd.DataFrame()
    generated = build_forward_rows(
        features,
        active_semicon_stock_tickers(universe),
        start_date=args.start_date,
    )
    forward = merge_forward(existing, generated)
    status = build_phase_status(forward)

    atomic_parquet(forward, args.forward_output)
    atomic_json(status, args.status_output)
    print("=== Market Flow 200A forward shadow ===")
    print(f"storage  : {'s3' if use_s3 else 'local'} ({app_env})")
    print(f"strategy : {STRATEGY_VERSION}")
    print(f"features : {args.features}")
    print(f"generated: {len(generated):,} rows")
    print(f"forward  : {args.forward_output} rows={len(forward):,}")
    print(f"status   : {args.status_output} mode={status['mode']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
