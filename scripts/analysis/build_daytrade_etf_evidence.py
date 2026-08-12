#!/usr/bin/env python3
"""Build the immutable 200A Daytrade ETF backtest evidence artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUN = (
    ROOT
    / "data/analysis/etf_200a_1306_mechanism/runs/20260811T_mechanism_v11"
)
DEFAULT_OUTPUT = ROOT / "server/data/daytrade_etf_backtest_v11.json"

TICKER = "200A.T"
FEATURE = "semis_strength_bin"
LEVEL = "00_1to2"
STRATEGY_VERSION = "etf0910_v2_20260811"
EXPECTED = {
    "trades": 51,
    "profit_factor_bps": 2.5299650025572635,
    "total_pnl_yen": 6550.0,
    "win_rate_pct": 66.66666666666666,
    "worst_pnl_yen": -500.0,
    "max_drawdown_yen": -1000.0,
}
KEYS = ["ticker", "trading_date", "period_role", "external_direction", "direction"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanism-run", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def finite(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    return finite(value)


def profit_factor(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="raise")
    gains = float(numeric[numeric > 0].sum())
    losses = float(-numeric[numeric < 0].sum())
    if losses == 0:
        return None
    return gains / losses


def drawdown(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    equity = pd.to_numeric(values, errors="raise").cumsum()
    peak = equity.cummax().clip(lower=0.0)
    return equity, equity - peak


def summary(frame: pd.DataFrame) -> dict[str, Any]:
    pnl = pd.to_numeric(frame["stop0_pnl_yen"], errors="raise")
    returns = pd.to_numeric(frame["stop0_return_bps"], errors="raise")
    _, dd = drawdown(pnl)
    tail_size = max(1, int(math.ceil(len(pnl) * 0.05)))
    return {
        "trades": int(len(frame)),
        "wins": int(pnl.gt(0).sum()),
        "losses": int(pnl.lt(0).sum()),
        "flats": int(pnl.eq(0).sum()),
        "win_rate_pct": float(pnl.gt(0).mean() * 100.0),
        "profit_factor_bps": profit_factor(returns),
        "cash_profit_factor": profit_factor(pnl),
        "total_pnl_yen": float(pnl.sum()),
        "mean_pnl_yen": float(pnl.mean()),
        "median_pnl_yen": float(pnl.median()),
        "mean_return_bps": float(returns.mean()),
        "median_return_bps": float(returns.median()),
        "worst_pnl_yen": float(pnl.min()),
        "cvar_5pct_yen": float(pnl.nsmallest(tail_size).mean()),
        "max_drawdown_yen": float(dd.min()),
        "stop_count": int(frame["stop_exit_reason"].isin(["stop", "stop_gap"]).sum()),
        "stop_rate_pct": float(
            frame["stop_exit_reason"].isin(["stop", "stop_gap"]).mean() * 100.0
        ),
    }


def scoped_summary(
    frame: pd.DataFrame, group_column: str, *, label_column: str | None = None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value, group in frame.groupby(group_column, sort=False):
        row = summary(group.sort_values("trading_date"))
        row["date_start"] = group["trading_date"].min().date().isoformat()
        row["date_end"] = group["trading_date"].max().date().isoformat()
        row[label_column or group_column] = str(value)
        rows.append(row)
    return rows


def load_and_validate(run: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    required = {
        "enriched_signals.parquet",
        "path_diagnostics.parquet",
        "feature_summary.csv",
        "feature_role_summary.csv",
        "candidate_assessment.csv",
        "manifest.json",
        "audit.json",
    }
    missing = sorted(name for name in required if not (run / name).exists())
    if missing:
        raise FileNotFoundError(f"mechanism v11 artifacts missing: {missing}")

    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    audit = json.loads((run / "audit.json").read_text(encoding="utf-8"))
    if manifest.get("run_id") != "20260811T_mechanism_v11":
        raise ValueError(f"unexpected run_id: {manifest.get('run_id')}")
    if audit.get("status") != "pass" or audit.get("failures"):
        raise ValueError("mechanism v11 audit did not pass")
    if manifest.get("live_authorized") is not False:
        raise ValueError("mechanism v11 live_authorized must remain false")
    if manifest.get("prior_outcomes_known") is not True:
        raise ValueError("mechanism v11 must disclose prior outcomes known")

    for name, expected in manifest["artifacts"].items():
        path = run / name
        if not path.exists():
            raise FileNotFoundError(path)
        actual = sha256_file(path)
        if actual != expected["sha256"]:
            raise ValueError(f"artifact hash mismatch: {name}")

    signals = pd.read_parquet(run / "enriched_signals.parquet")
    paths = pd.read_parquet(run / "path_diagnostics.parquet")
    merged = signals.merge(paths, on=KEYS, suffixes=("_signal", "_path"), validate="one_to_one")
    selected = merged[
        merged["ticker"].eq(TICKER) & merged[FEATURE].eq(LEVEL)
    ].copy()
    selected["trading_date"] = pd.to_datetime(selected["trading_date"], errors="raise")
    selected = selected.sort_values("trading_date").reset_index(drop=True)

    candidate = pd.read_csv(run / "candidate_assessment.csv")
    candidate = candidate[
        candidate["ticker"].eq(TICKER)
        & candidate["feature"].eq(FEATURE)
        & candidate["level"].eq(LEVEL)
    ]
    if len(candidate) != 1 or not bool(candidate.iloc[0]["forward_candidate"]):
        raise ValueError("the fixed 200A candidate row is absent")

    pooled = pd.read_csv(run / "feature_summary.csv")
    pooled = pooled[
        pooled["ticker"].eq(TICKER)
        & pooled["feature"].eq(FEATURE)
        & pooled["level"].eq(LEVEL)
    ]
    if len(pooled) != 1:
        raise ValueError("the fixed 200A pooled summary is absent")

    calculated = summary(selected)
    for field, expected in EXPECTED.items():
        actual = calculated[field]
        if not math.isclose(float(actual), expected, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError(f"historical evidence mismatch: {field}={actual} expected={expected}")
    official = pooled.iloc[0]
    official_checks = {
        "trades": calculated["trades"],
        "profit_factor": calculated["profit_factor_bps"],
        "total_pnl_yen": calculated["total_pnl_yen"],
        "win_rate_pct": calculated["win_rate_pct"],
        "worst_pnl_yen": calculated["worst_pnl_yen"],
        "max_drawdown_yen": calculated["max_drawdown_yen"],
    }
    for official_field, actual in official_checks.items():
        if not math.isclose(
            float(official[official_field]), float(actual), rel_tol=0.0, abs_tol=1e-9
        ):
            raise ValueError(f"official summary mismatch: {official_field}")
    return selected, manifest, candidate.iloc[0].to_dict()


def build_payload(run: Path) -> dict[str, Any]:
    frame, manifest, candidate = load_and_validate(run)
    equity, dd = drawdown(frame["stop0_pnl_yen"])
    frame["cumulative_pnl_yen"] = equity
    frame["drawdown_yen"] = dd
    frame["month"] = frame["trading_date"].dt.to_period("M").astype(str)
    frame["quarter"] = frame["trading_date"].dt.to_period("Q").astype(str)

    no_stop = frame.copy()
    no_stop["stop0_pnl_yen"] = no_stop["no_stop_close_pnl_yen"]
    no_stop["stop0_return_bps"] = no_stop["no_stop_close_return_bps"]
    no_stop["stop_exit_reason"] = "session_close"

    input_files = [
        "enriched_signals.parquet",
        "path_diagnostics.parquet",
        "feature_summary.csv",
        "feature_role_summary.csv",
        "candidate_assessment.csv",
        "manifest.json",
        "audit.json",
    ]
    fields = [
        "trading_date",
        "period_role",
        "selector_label",
        "external_context_date",
        "semiconductor_proxy_ret1_pct",
        "market_proxy_ret1_pct",
        "semis_vs_market_pct",
        "us_semis_positive_count",
        "us_semis_negative_count",
        "external_direction",
        "direction",
        "reference_open_time",
        "reference_open",
        "observation_time",
        "observation_close",
        "open_to_observation_bps",
        "entry_time_signal",
        "entry_price_signal",
        "quantity",
        "session_close_time",
        "session_close",
        "stop_exit_time",
        "stop_exit_reason",
        "stop0_return_bps",
        "stop0_pnl_yen",
        "no_stop_close_return_bps",
        "no_stop_close_pnl_yen",
        "mfe_yen",
        "mfe_time",
        "mae_yen",
        "mae_time",
        "cumulative_pnl_yen",
        "drawdown_yen",
    ]
    trades: list[dict[str, Any]] = []
    for row in frame[fields].to_dict(orient="records"):
        direction_sign = 1.0 if row["direction"] == "LONG" else -1.0
        row["stop_price"] = float(row["entry_price_signal"] - direction_sign * 50.0)
        row["exit_price"] = float(
            row["entry_price_signal"]
            + direction_sign * row["stop0_pnl_yen"] / row["quantity"]
        )
        row["watch_direction"] = row["direction"]
        row["trading_date"] = pd.Timestamp(row["trading_date"]).date().isoformat()
        row["external_context_date"] = pd.Timestamp(row["external_context_date"]).date().isoformat()
        row["entry_time"] = row.pop("entry_time_signal")
        row["entry_price"] = row.pop("entry_price_signal")
        row["exit_time"] = row.pop("stop_exit_time")
        row["exit_reason"] = row.pop("stop_exit_reason")
        row["return_bps"] = row.pop("stop0_return_bps")
        row["pnl_yen"] = row.pop("stop0_pnl_yen")
        row["no_stop_return_bps"] = row.pop("no_stop_close_return_bps")
        row["no_stop_pnl_yen"] = row.pop("no_stop_close_pnl_yen")
        if row["exit_reason"] == "session_close" and not math.isclose(
            row["exit_price"], row["session_close"], rel_tol=0.0, abs_tol=1e-9
        ):
            raise ValueError(f"session-close exit price mismatch: {row['trading_date']}")
        if row["exit_reason"] == "stop" and not math.isclose(
            row["exit_price"], row["stop_price"], rel_tol=0.0, abs_tol=1e-9
        ):
            raise ValueError(f"stop exit price mismatch: {row['trading_date']}")
        trades.append(clean(row))

    payload: dict[str, Any] = {
        "schema_version": 1,
        "strategy_version": STRATEGY_VERSION,
        "evidence_id": "200a_0910_smh_abs_1to2_stop50_v11",
        "generated_at": manifest["generated_at"],
        "source_run": {
            "run_id": manifest["run_id"],
            "contract": manifest["contract"],
            "audit_status": "historical_run_pass_artifacts_reverified",
            "prior_outcomes_known": True,
            "multiple_testing_disclosed": bool(candidate["multiple_testing_disclosed"]),
            "live_authorized": False,
            "assessment": "retrospective_selected_candidate_not_untouched_confirmation",
            "input_artifacts": {
                name: {
                    "bytes": int((run / name).stat().st_size),
                    "sha256": sha256_file(run / name),
                }
                for name in input_files
            },
        },
        "parameters": {
            "ticker": TICKER,
            "quantity": 10,
            "cost_bps": 0,
            "special_short_fee_included": False,
            "slippage_included": False,
            "external_selector": "US semiconductor breadth plus SMH relative to QQQ",
            "strength_gate": "1.0% <= abs(SMH daily return) < 2.0%",
            "trade_direction": "inverse to the eligible US semiconductor direction",
            "confirmation": "09:09 or 09:10 close must move in the watch direction from the JPX open",
            "entry": "next actual bar open by 09:15",
            "stop": "50 JPY per unit adverse from entry; 500 JPY planned loss for 10 units",
            "exit": "same-session close when the stop is not reached",
        },
        "headline": {
            **summary(frame),
            "date_start": frame["trading_date"].min().date().isoformat(),
            "date_end": frame["trading_date"].max().date().isoformat(),
            "bootstrap_mean_ci_low_bps": float(candidate["bootstrap_mean_ci_low_bps"]),
            "bootstrap_mean_ci_high_bps": float(candidate["bootstrap_mean_ci_high_bps"]),
        },
        "period_roles": scoped_summary(frame, "period_role"),
        "directions": scoped_summary(frame, "direction"),
        "months": scoped_summary(frame, "month"),
        "quarters": scoped_summary(frame, "quarter"),
        "stop_comparison": [
            {"arm": "stop_50_yen", **summary(frame)},
            {"arm": "session_close_no_stop", **summary(no_stop)},
        ],
        "equity_curve": [
            {
                "trading_date": trade["trading_date"],
                "pnl_yen": trade["pnl_yen"],
                "cumulative_pnl_yen": trade["cumulative_pnl_yen"],
                "drawdown_yen": trade["drawdown_yen"],
            }
            for trade in trades
        ],
        "trades": trades,
        "disclosures": [
            "This candidate was selected retrospectively after base outcomes were known.",
            "This is not an untouched confirmation sample and is not live-trading authorization.",
            "The displayed result uses 0 bps cost; special short fees and slippage are not included.",
            "Forward observations from 2026-08-05 onward are separate from these 51 historical trades.",
        ],
    }
    canonical = json.dumps(clean(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["payload_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return clean(payload)


def main() -> int:
    args = parse_args()
    payload = build_payload(args.mechanism_run)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    headline = payload["headline"]
    print(
        "Daytrade ETF evidence: "
        f"trades={headline['trades']} PF(bps)={headline['profit_factor_bps']:.6f} "
        f"P/L={headline['total_pnl_yen']:.0f} maxDD={headline['max_drawdown_yen']:.0f}"
    )
    print(f"output={args.output}")
    print(f"payload_sha256={payload['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
