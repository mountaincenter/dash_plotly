#!/usr/bin/env python3
"""Validate and publish the 16:45 Market Flow checkpoint before legacy stages."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_files


TOP100_PATH = PARQUET_DIR / "trading_value_top100.parquet"
HISTORY_PATH = PARQUET_DIR / "trading_value_top_history.parquet"
BASKET_PATH = PARQUET_DIR / "market_basket_turnover.parquet"
SEMICON_PATH = PARQUET_DIR / "semicon_watch_universe.parquet"
FEATURES_PATH = PARQUET_DIR / "jquants_minute_watch_features.parquet"
FORWARD_PATH = PARQUET_DIR / "market_flow_200a_forward.parquet"
STATUS_PATH = PARQUET_DIR / "market_flow_200a_phase_status.json"
VALIDATION_PATH = PARQUET_DIR / "market_flow_checkpoint_validation.json"

REQUIRED_ETFS = {"200A.T", "2644.T"}
REQUIRED_BASKETS = {"topix500", "topix_full"}
REQUIRED_RULES = {
    "S_0930_SELF_TO_1400",
    "S_1000_BREADTH_TO_1130",
    "L_1400_STRICT_TO_1530",
}
VALID_EXECUTION_STATUSES = {"completed", "not_triggered"}
MINUTE_COVERAGE_MIN_PCT = 90.0

PUBLISH_FILES = [
    TOP100_PATH,
    HISTORY_PATH,
    BASKET_PATH,
    SEMICON_PATH,
    FORWARD_PATH,
    STATUS_PATH,
    VALIDATION_PATH,
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
    parser = argparse.ArgumentParser(
        description="Validate and publish the current Market Flow checkpoint."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate locally and write the validation JSON without uploading to S3.",
    )
    return parser.parse_args()


def iso_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def assert_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing columns: {missing}")


def assert_ranked_snapshot(
    frame: pd.DataFrame,
    target_date: str,
    expected_rows: int,
    label: str,
) -> pd.DataFrame:
    assert_columns(frame, {"date", "ticker", "rank"}, label)
    dated = frame.loc[iso_date(frame["date"]).eq(target_date)].copy()
    ranks = pd.to_numeric(dated["rank"], errors="coerce").dropna().astype(int)
    expected_ranks = set(range(1, expected_rows + 1))
    actual_ranks = set(ranks.tolist())
    if len(dated) != expected_rows:
        raise ValueError(
            f"{label} expected {expected_rows} rows for {target_date}, got {len(dated)}"
        )
    if dated["ticker"].astype(str).nunique() != expected_rows:
        raise ValueError(f"{label} ticker uniqueness failed for {target_date}")
    if actual_ranks != expected_ranks:
        raise ValueError(f"{label} rank coverage failed for {target_date}")
    return dated


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} is not a JSON object")
    return payload


def file_stats(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(ROOT)),
        "size_bytes": path.stat().st_size,
        "modified_at": datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat(),
    }


def validate_checkpoint() -> dict[str, Any]:
    required_paths = [
        TOP100_PATH,
        HISTORY_PATH,
        BASKET_PATH,
        SEMICON_PATH,
        FEATURES_PATH,
        FORWARD_PATH,
        STATUS_PATH,
    ]
    missing = [str(path.relative_to(ROOT)) for path in required_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Market Flow checkpoint inputs missing: {missing}")

    top100 = pd.read_parquet(TOP100_PATH)
    assert_columns(top100, {"date"}, "Top100")
    target_dates = iso_date(top100["date"]).dropna()
    if target_dates.empty:
        raise ValueError("Top100 has no valid date")
    target_date = target_dates.max()
    top100_today = assert_ranked_snapshot(top100, target_date, 100, "Top100")

    history = pd.read_parquet(HISTORY_PATH)
    history_today = assert_ranked_snapshot(history, target_date, 150, "Top150 history")

    baskets = pd.read_parquet(BASKET_PATH)
    assert_columns(
        baskets,
        {
            "date",
            "basket_key",
            "turnover_value",
            "latest_vs_5d_avg",
        },
        "Market basket",
    )
    basket_today = baskets.loc[iso_date(baskets["date"]).eq(target_date)].copy()
    basket_keys = set(basket_today["basket_key"].astype(str))
    if not REQUIRED_BASKETS.issubset(basket_keys):
        missing_baskets = sorted(REQUIRED_BASKETS - basket_keys)
        raise ValueError(f"Market basket missing for {target_date}: {missing_baskets}")
    turnover = pd.to_numeric(basket_today["turnover_value"], errors="coerce")
    if not bool(turnover.fillna(0).gt(0).all()):
        raise ValueError(f"Market basket contains non-positive turnover for {target_date}")
    basket_dates = baskets.assign(_date=iso_date(baskets["date"]))
    for basket_key in sorted(REQUIRED_BASKETS):
        history_days = basket_dates.loc[
            basket_dates["basket_key"].astype(str).eq(basket_key),
            "_date",
        ].nunique()
        if history_days < 6:
            raise ValueError(
                f"Market basket {basket_key} has only {history_days} history days"
            )
        latest_ratio = pd.to_numeric(
            basket_today.loc[
                basket_today["basket_key"].astype(str).eq(basket_key),
                "latest_vs_5d_avg",
            ],
            errors="coerce",
        )
        if latest_ratio.empty or latest_ratio.isna().any():
            raise ValueError(
                f"Market basket {basket_key} has no 5-day comparison for {target_date}"
            )

    semicon = pd.read_parquet(SEMICON_PATH)
    assert_columns(semicon, {"ticker", "active", "fetch_minute"}, "Semicon universe")
    active_mask = semicon["active"].fillna(False).astype(bool)
    minute_mask = semicon["fetch_minute"].fillna(False).astype(bool)
    active_tickers = set(semicon.loc[active_mask & minute_mask, "ticker"].astype(str))
    if not REQUIRED_ETFS.issubset(active_tickers):
        missing_etfs = sorted(REQUIRED_ETFS - active_tickers)
        raise ValueError(f"Semicon universe missing required ETFs: {missing_etfs}")

    features = pd.read_parquet(FEATURES_PATH)
    assert_columns(
        features,
        {"trading_date", "ticker", "datetime", "time"},
        "Minute features",
    )
    feature_today = features.loc[
        iso_date(features["trading_date"]).eq(target_date)
    ].copy()
    if feature_today.empty:
        raise ValueError(f"Minute features are empty for {target_date}")
    duplicate_count = int(
        feature_today.duplicated(["ticker", "datetime"], keep=False).sum()
    )
    if duplicate_count:
        raise ValueError(
            f"Minute features contain {duplicate_count} duplicate ticker/datetime rows"
        )
    fetched_tickers = set(feature_today["ticker"].astype(str))
    covered_tickers = active_tickers & fetched_tickers
    coverage_pct = (
        len(covered_tickers) / len(active_tickers) * 100.0 if active_tickers else 0.0
    )
    if not math.isfinite(coverage_pct) or coverage_pct < MINUTE_COVERAGE_MIN_PCT:
        raise ValueError(
            f"Minute coverage {coverage_pct:.2f}% is below "
            f"{MINUTE_COVERAGE_MIN_PCT:.2f}% for {target_date}"
        )
    latest_times = (
        feature_today.assign(ticker=feature_today["ticker"].astype(str))
        .groupby("ticker")["time"]
        .max()
    )
    for ticker in sorted(REQUIRED_ETFS):
        latest_time = str(latest_times.get(ticker, ""))
        if latest_time < "15:30":
            raise ValueError(
                f"Minute features for {ticker} end at {latest_time or 'missing'}, "
                f"expected 15:30"
            )

    forward = pd.read_parquet(FORWARD_PATH)
    assert_columns(
        forward,
        {
            "trade_date",
            "rule_id",
            "strategy_version",
            "signal_available",
            "triggered",
            "primary_selected",
            "execution_status",
        },
        "200A forward",
    )
    forward_today = forward.loc[
        iso_date(forward["trade_date"]).eq(target_date)
    ].copy()
    actual_rules = set(forward_today["rule_id"].astype(str))
    if len(forward_today) != len(REQUIRED_RULES) or actual_rules != REQUIRED_RULES:
        raise ValueError(
            f"200A forward route coverage failed for {target_date}: "
            f"{sorted(actual_rules)}"
        )
    if not bool(forward_today["signal_available"].fillna(False).astype(bool).all()):
        raise ValueError(f"200A forward has unavailable signals for {target_date}")
    statuses = set(forward_today["execution_status"].astype(str))
    if not statuses.issubset(VALID_EXECUTION_STATUSES):
        raise ValueError(f"200A forward has unresolved statuses: {sorted(statuses)}")
    primary_count = int(
        forward_today["primary_selected"].fillna(False).astype(bool).sum()
    )
    triggered_count = int(forward_today["triggered"].fillna(False).astype(bool).sum())
    expected_primary_count = 1 if triggered_count else 0
    if primary_count != expected_primary_count:
        raise ValueError(
            f"200A forward selected {primary_count} primary routes; "
            f"expected {expected_primary_count} for {triggered_count} triggers"
        )

    phase_status = load_json(STATUS_PATH)
    if phase_status.get("automatic_ordering") is not False:
        raise ValueError("Market Flow automatic_ordering must remain false")
    if phase_status.get("strategy_version") != forward_today["strategy_version"].iloc[0]:
        raise ValueError("Market Flow strategy version mismatch")

    return {
        "schema_version": 1,
        "status": "passed",
        "validated_at": datetime.now().astimezone().isoformat(),
        "target_date": target_date,
        "checks": {
            "top100_rows": len(top100_today),
            "top150_rows": len(history_today),
            "basket_keys": sorted(basket_keys),
            "semicon_active_minute_tickers": len(active_tickers),
            "minute_covered_tickers": len(covered_tickers),
            "minute_coverage_pct": coverage_pct,
            "minute_duplicate_rows": duplicate_count,
            "required_etf_latest_times": {
                ticker: str(latest_times.get(ticker))
                for ticker in sorted(REQUIRED_ETFS)
            },
            "forward_routes": sorted(actual_rules),
            "forward_triggered_count": triggered_count,
            "forward_primary_count": primary_count,
            "automatic_ordering": phase_status.get("automatic_ordering"),
        },
        "files": {
            path.name: file_stats(path)
            for path in required_paths
        },
    }


def save_validation(payload: dict[str, Any]) -> None:
    VALIDATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = VALIDATION_PATH.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(VALIDATION_PATH)


def main() -> int:
    args = parse_args()
    app_env, use_s3 = resolve_storage_mode()
    print("=== Validate and publish Market Flow checkpoint ===")
    print(f"environment: {app_env}")
    print(f"storage    : {'s3' if use_s3 else 'local'}")
    try:
        validation = validate_checkpoint()
    except Exception as exc:
        failed = {
            "schema_version": 1,
            "status": "failed",
            "validated_at": datetime.now().astimezone().isoformat(),
            "reason": str(exc),
        }
        save_validation(failed)
        print(f"[ERROR] Market Flow checkpoint validation failed: {exc}")
        return 1

    save_validation(validation)
    print(
        "[OK] validation passed: "
        f"date={validation['target_date']} "
        f"coverage={validation['checks']['minute_coverage_pct']:.2f}%"
    )
    if args.dry_run:
        print("[OK] dry-run: S3 upload skipped")
        return 0
    if not use_s3:
        print("[OK] development/local mode: validation saved locally; S3 upload skipped")
        return 0

    config = load_s3_config()
    if not upload_files(config, PUBLISH_FILES, base_dir=PARQUET_DIR):
        print("[ERROR] Market Flow checkpoint S3 upload failed")
        return 1
    print(f"[OK] Market Flow checkpoint published: {validation['target_date']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
