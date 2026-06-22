#!/usr/bin/env python3
"""
Fetch and normalize J-Quants CLI minute CSVs for ETF / TOPIX500 / semiconductor universes.

Raw CSV layout:
  data/jquants_csv/topix_etf_minute/YYYY-MM-DD/CODE.csv

Normalized outputs:
  data/parquet/jquants_minute_universe.parquet
  data/parquet/jquants_minute_universe_features.parquet

The script is deliberately conservative with rate limits. Use --dry-run first,
then fetch small groups/dates before expanding to TOPIX500.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR


DEFAULT_UNIVERSE = PARQUET_DIR / "topix_etf_universe.parquet"
DEFAULT_DAILY = PARQUET_DIR / "prices_max_1d.parquet"
DEFAULT_OUTPUT = PARQUET_DIR / "jquants_minute_universe.parquet"
DEFAULT_FEATURES_OUTPUT = PARQUET_DIR / "jquants_minute_universe_features.parquet"
DEFAULT_RAW_DIR = ROOT / "data" / "jquants_csv" / "topix_etf_minute"
DEFAULT_ENV_FILE = ROOT / ".env.jquants"

PLAN_RPM = {
    "free": 5,
    "light": 60,
    "standard": 120,
    "premium": 500,
}


class RateLimitError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchTarget:
    ticker: str
    query_code: str
    trading_date: str
    stock_name: str | None
    instrument_type: str | None
    groups: str | None
    priority: int | None
    universe_group: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch J-Quants minute bars for a universe master.")
    parser.add_argument("--universe-path", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--calendar-path", type=Path, default=DEFAULT_DAILY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--features-output", type=Path, default=DEFAULT_FEATURES_OUTPUT)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--date", help="Fetch one trading date (YYYY-MM-DD).")
    parser.add_argument("--from", dest="date_from", help="Fetch trading dates from YYYY-MM-DD.")
    parser.add_argument("--to", dest="date_to", help="Fetch trading dates to YYYY-MM-DD.")
    parser.add_argument("--tickers", nargs="*", help="Optional ticker filter, e.g. 200A.T 6981.T.")
    parser.add_argument("--codes", nargs="*", help="Optional J-Quants code filter, e.g. 200A 6981.")
    parser.add_argument("--groups", nargs="*", help="Optional group filter, e.g. etf_live_training semicon_core.")
    parser.add_argument("--instrument-types", nargs="*", help="Optional type filter, e.g. etf stock.")
    parser.add_argument("--max-targets", type=int, default=0, help="Limit target pairs for smoke tests.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Minimum seconds to sleep between CLI calls.")
    parser.add_argument("--requests-per-minute", type=float, default=0, help="Client-side request cap. Default is conservative from JQUANTS_PLAN.")
    parser.add_argument("--rate-limit-wait", type=float, default=180.0, help="Base seconds to wait before retrying after HTTP 429.")
    parser.add_argument("--max-retries", type=int, default=8, help="Retries per pair after transient failures.")
    parser.add_argument("--checkpoint-every", type=int, default=100, help="Save parquet after this many successful pairs.")
    parser.add_argument("--refresh", action="store_true", help="Refetch pairs already present in the output parquet.")
    parser.add_argument("--refresh-raw-csv", action="store_true", help="Call jquants even when the raw CSV already exists.")
    parser.add_argument("--dry-run", action="store_true", help="Print target pairs without calling jquants.")
    parser.add_argument("--keep-empty-csv", action="store_true", help="Keep empty CSV files for failed/empty fetches.")
    return parser.parse_args()


def load_env_file(path: Path) -> dict[str, str]:
    env = os.environ.copy()
    if not path.exists():
        return env

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in env:
            env[key] = value

    if "JQUANTS_BASE_URL" not in env and "JQUANTS_API_BASE_URL" in env:
        env["JQUANTS_BASE_URL"] = env["JQUANTS_API_BASE_URL"]
    return env


def resolve_requests_per_minute(args: argparse.Namespace, env: dict[str, str]) -> float:
    if args.requests_per_minute > 0:
        return args.requests_per_minute

    plan = env.get("JQUANTS_PLAN", "").strip().lower()
    cap = PLAN_RPM.get(plan, 60)
    # Use a low default because TOPIX500 x multiple days can easily hit rolling
    # limits. Raise explicitly only after confirming the plan allowance.
    return max(1.0, min(cap * 0.25, 30.0))


def clean_string(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None
    return text.removesuffix(".0")


def parse_bool(value: object, default: bool = False) -> bool:
    if value is None or pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def split_groups(value: object) -> set[str]:
    text = clean_string(value)
    if text is None:
        return set()
    return {part.strip() for part in text.split("|") if part.strip()}


def load_universe(path: Path, args: argparse.Namespace) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"universe not found: {path}")

    df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    required = {"ticker", "jquants_query_code", "stock_name", "groups", "fetch_minute", "active"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"missing universe columns in {path}: {missing}")

    df = df.copy()
    for col in ["priority", "display_order", "instrument_type", "universe_group"]:
        if col not in df.columns:
            df[col] = None
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["jquants_query_code"] = df["jquants_query_code"].astype(str).str.strip().str.removesuffix(".0")
    df = df[df["ticker"].ne("") & df["jquants_query_code"].ne("")]
    df = df[df["fetch_minute"].map(parse_bool) & df["active"].map(parse_bool)]

    if args.tickers:
        tickers = {ticker.strip() for ticker in args.tickers}
        df = df[df["ticker"].isin(tickers)]
    if args.codes:
        codes = {code.strip().removesuffix(".0") for code in args.codes}
        df = df[df["jquants_query_code"].isin(codes)]
    if args.instrument_types and "instrument_type" in df.columns:
        types = {value.strip() for value in args.instrument_types}
        df = df[df["instrument_type"].isin(types)]
    if args.groups:
        groups = {value.strip() for value in args.groups}
        df = df[df["groups"].map(lambda value: bool(split_groups(value) & groups))]

    return df.sort_values(["display_order", "priority", "ticker"], na_position="last").reset_index(drop=True)


def load_trading_dates(args: argparse.Namespace) -> list[str]:
    if args.date:
        return [args.date]

    if args.calendar_path.exists():
        daily = pd.read_parquet(args.calendar_path, columns=["date"])
        dates = pd.to_datetime(daily["date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").drop_duplicates()
        if args.date_from:
            dates = dates[dates.ge(args.date_from)]
        if args.date_to:
            dates = dates[dates.le(args.date_to)]
        dates = sorted(dates.tolist())
        if dates:
            return dates[-1:] if not args.date_from and not args.date_to else dates

    if not args.date_from and not args.date_to:
        return [pd.Timestamp.now(tz="Asia/Tokyo").strftime("%Y-%m-%d")]

    start = pd.Timestamp(args.date_from)
    end = pd.Timestamp(args.date_to or args.date_from)
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start, end)]


def build_targets(universe: pd.DataFrame, trading_dates: list[str], max_targets: int) -> list[FetchTarget]:
    targets: list[FetchTarget] = []
    for row in universe.itertuples(index=False):
        for trading_date in trading_dates:
            targets.append(
                FetchTarget(
                    ticker=str(row.ticker),
                    query_code=str(row.jquants_query_code),
                    trading_date=trading_date,
                    stock_name=clean_string(getattr(row, "stock_name", None)),
                    instrument_type=clean_string(getattr(row, "instrument_type", None)),
                    groups=clean_string(getattr(row, "groups", None)),
                    priority=int(getattr(row, "priority")) if clean_string(getattr(row, "priority", None)) else None,
                    universe_group=clean_string(getattr(row, "universe_group", None)),
                )
            )
    if max_targets > 0:
        targets = targets[:max_targets]
    return targets


def load_existing_keys(output: Path) -> set[tuple[str, str]]:
    if not output.exists():
        return set()
    existing = pd.read_parquet(output, columns=["ticker", "trading_date"])
    if existing.empty:
        return set()
    existing["trading_date"] = existing["trading_date"].astype(str)
    return set(zip(existing["ticker"].astype(str), existing["trading_date"]))


def raw_csv_path(raw_dir: Path, target: FetchTarget) -> Path:
    safe_code = target.query_code.replace("/", "_")
    return raw_dir / target.trading_date / f"{safe_code}.csv"


def fetch_target(
    target: FetchTarget,
    raw_dir: Path,
    env: dict[str, str],
    keep_empty_csv: bool,
    refresh_raw_csv: bool,
) -> pd.DataFrame:
    path = raw_csv_path(raw_dir, target)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists() or refresh_raw_csv:
        cmd = [
            "jquants",
            "--output",
            "csv",
            "--save",
            str(path),
            "eq",
            "minute",
            "--code",
            target.query_code,
            "--date",
            target.trading_date,
        ]
        result = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            if path.exists() and not keep_empty_csv and path.stat().st_size == 0:
                path.unlink()
            stderr = result.stderr.strip() or result.stdout.strip()
            if "status 429" in stderr or "Rate limit" in stderr or "レート制限" in stderr:
                raise RateLimitError(f"jquants CLI rate limited for {target.ticker} {target.trading_date}: {stderr}")
            raise RuntimeError(f"jquants CLI failed for {target.ticker} {target.trading_date}: {stderr}")

    if not path.exists() or path.stat().st_size == 0:
        if path.exists() and not keep_empty_csv:
            path.unlink()
        return pd.DataFrame()

    raw = pd.read_csv(path)
    if raw.empty:
        if not keep_empty_csv:
            path.unlink(missing_ok=True)
        return raw
    return normalize_minute_csv(raw, target, path)


def normalize_minute_csv(raw: pd.DataFrame, target: FetchTarget, path: Path) -> pd.DataFrame:
    rename = {
        "Date": "trading_date",
        "Time": "time",
        "Code": "jquants_code",
        "O": "open",
        "H": "high",
        "L": "low",
        "C": "close",
        "Vo": "volume",
        "Va": "value",
    }
    missing = sorted(set(rename) - set(raw.columns))
    if missing:
        raise ValueError(f"missing columns in {path}: {missing}")

    df = raw.rename(columns=rename)[list(rename.values())].copy()
    df["ticker"] = target.ticker
    df["query_code"] = target.query_code
    df["stock_name"] = target.stock_name
    df["instrument_type"] = target.instrument_type
    df["groups"] = target.groups
    df["priority"] = target.priority
    df["universe_group"] = target.universe_group
    df["jquants_code"] = df["jquants_code"].astype(str)
    df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["datetime"] = pd.to_datetime(df["trading_date"] + " " + df["time"].astype(str), errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["datetime"].notna()]
    df = df[df[["open", "high", "low", "close"]].notna().any(axis=1)]
    if df.empty:
        return df

    df["source"] = "jquants_cli"
    df["interval"] = "1m"
    df["raw_csv"] = str(path.relative_to(ROOT))
    df["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def add_intraday_features(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
    result = result.dropna(subset=["ticker", "trading_date", "datetime"]).sort_values(["ticker", "trading_date", "datetime"])
    keys = ["ticker", "trading_date"]

    for col in ["open", "high", "low", "close", "volume", "value"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    result["minute_of_day"] = result["datetime"].dt.hour * 60 + result["datetime"].dt.minute
    result["session_open"] = result.groupby(keys, sort=False)["open"].transform("first")
    result["session_high_so_far"] = result.groupby(keys, sort=False)["high"].cummax()
    result["session_low_so_far"] = result.groupby(keys, sort=False)["low"].cummin()
    result["typical_price"] = (result["high"] + result["low"] + result["close"]) / 3
    fallback_value = result["typical_price"] * result["volume"].fillna(0)
    result["bar_value"] = result["value"].where(result["value"].notna(), fallback_value)
    result["bar_vwap"] = result["bar_value"] / result["volume"].where(result["volume"] > 0)
    result["cum_value"] = result.groupby(keys, sort=False)["bar_value"].cumsum()
    result["cum_volume"] = result.groupby(keys, sort=False)["volume"].cumsum()
    result["session_vwap"] = result["cum_value"] / result["cum_volume"].where(result["cum_volume"] > 0)
    result["bar_return_pct"] = result.groupby(keys, sort=False)["close"].pct_change() * 100
    result["ret_from_open_pct"] = (result["close"] / result["session_open"] - 1) * 100
    result["close_vs_vwap_pct"] = (result["close"] / result["session_vwap"] - 1) * 100
    result["high_from_open_pct"] = (result["session_high_so_far"] / result["session_open"] - 1) * 100
    result["low_from_open_pct"] = (result["session_low_so_far"] / result["session_open"] - 1) * 100
    result["above_open"] = result["close"].gt(result["session_open"])
    result["above_vwap"] = result["close"].gt(result["session_vwap"])
    result["bar_no"] = result.groupby(keys, sort=False).cumcount() + 1
    return result.reset_index(drop=True)


def merge_existing(output: Path, fetched: pd.DataFrame) -> pd.DataFrame:
    if output.exists():
        existing = pd.read_parquet(output)
        combined = pd.concat([existing, fetched], ignore_index=True)
    else:
        combined = fetched

    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce")
    combined["trading_date"] = combined["trading_date"].astype(str)
    combined = combined.dropna(subset=["ticker", "datetime"])
    combined = combined.sort_values(["ticker", "datetime", "fetched_at"])
    combined = combined.drop_duplicates(["ticker", "datetime"], keep="last")
    return combined.reset_index(drop=True)


def save_outputs(output: Path, features_output: Path, combined: pd.DataFrame) -> pd.DataFrame:
    output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output, engine="pyarrow", index=False)
    features = add_intraday_features(combined)
    features_output.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(features_output, engine="pyarrow", index=False)
    return features


def main() -> int:
    args = parse_args()
    universe = load_universe(args.universe_path, args)
    trading_dates = load_trading_dates(args)
    targets = build_targets(universe, trading_dates, args.max_targets)
    existing_keys = load_existing_keys(args.output)
    if not args.refresh:
        targets = [target for target in targets if (target.ticker, target.trading_date) not in existing_keys]

    print("=== J-Quants minute universe fetch ===")
    print(f"universe: {args.universe_path}")
    print(f"output  : {args.output}")
    print(f"features: {args.features_output}")
    print(f"raw_dir : {args.raw_dir}")
    print(f"dates   : {trading_dates[0]} - {trading_dates[-1]} ({len(trading_dates)})")
    print(f"universe rows: {len(universe)}")
    print(f"targets : {len(targets)}")
    if not universe.empty:
        print("\nby groups")
        group_counts: dict[str, int] = {}
        for groups in universe["groups"]:
            for group in split_groups(groups):
                group_counts[group] = group_counts.get(group, 0) + 1
        for group, count in sorted(group_counts.items(), key=lambda item: (-item[1], item[0]))[:20]:
            print(f"  {group:24s} {count:5d}")
    if targets:
        print("\nsample")
        for target in targets[:20]:
            print(f"  {target.trading_date} {target.ticker} code={target.query_code} groups={target.groups}")

    if args.dry_run:
        return 0
    if not targets:
        print("[OK] nothing to fetch")
        return 0

    env = load_env_file(args.env_file)
    requests_per_minute = resolve_requests_per_minute(args, env)
    inter_request_sleep = max(args.sleep, 60.0 / requests_per_minute)
    print(f"\nrate    : {requests_per_minute:.1f} requests/min, sleep>={inter_request_sleep:.2f}s")

    frames: list[pd.DataFrame] = []
    failures: list[str] = []
    empty: list[str] = []
    fetched_rows_total = 0
    latest_features: pd.DataFrame | None = None

    def save_checkpoint(force: bool = False) -> None:
        nonlocal frames, fetched_rows_total, latest_features
        if not frames:
            return
        if not force and args.checkpoint_every > 0 and len(frames) < args.checkpoint_every:
            return

        fetched = pd.concat(frames, ignore_index=True)
        combined = merge_existing(args.output, fetched)
        latest_features = save_outputs(args.output, args.features_output, combined)
        fetched_rows_total += len(fetched)
        pairs = latest_features[["ticker", "trading_date"]].drop_duplicates().shape[0]
        print(
            f"  -> checkpoint saved: fetched_rows_total={fetched_rows_total:,} "
            f"total_rows={len(latest_features):,} pairs={pairs}",
            flush=True,
        )
        frames = []

    for idx, target in enumerate(targets, start=1):
        print(f"[{idx}/{len(targets)}] {target.trading_date} {target.ticker} code={target.query_code}", flush=True)
        df = pd.DataFrame()
        failed_current = False
        for attempt in range(1, args.max_retries + 2):
            try:
                df = fetch_target(target, args.raw_dir, env, args.keep_empty_csv, args.refresh_raw_csv)
                break
            except RateLimitError as exc:
                if attempt > args.max_retries:
                    failures.append(f"{target.ticker}/{target.trading_date}: {exc}")
                    failed_current = True
                    print(f"  -> failed after rate-limit retries: {exc}")
                    break
                wait_seconds = args.rate_limit_wait * attempt
                print(
                    f"  -> 429/rate limited; waiting {wait_seconds:.0f}s before retry "
                    f"({attempt}/{args.max_retries})",
                    flush=True,
                )
                time.sleep(wait_seconds)
            except Exception as exc:
                if attempt > args.max_retries:
                    failures.append(f"{target.ticker}/{target.trading_date}: {exc}")
                    failed_current = True
                    print(f"  -> failed: {exc}")
                    break
                wait_seconds = min(30.0 * attempt, args.rate_limit_wait)
                print(
                    f"  -> transient failure; waiting {wait_seconds:.0f}s before retry "
                    f"({attempt}/{args.max_retries}): {exc}",
                    flush=True,
                )
                time.sleep(wait_seconds)

        if failed_current:
            continue

        if df.empty:
            empty.append(f"{target.ticker}/{target.trading_date}")
            print("  -> empty")
        else:
            frames.append(df)
            print(f"  -> rows={len(df):,} {df['datetime'].min()} - {df['datetime'].max()}")
            save_checkpoint(force=False)
        if idx < len(targets) and inter_request_sleep > 0:
            time.sleep(inter_request_sleep)

    save_checkpoint(force=True)

    if fetched_rows_total == 0:
        print("[ERROR] no data fetched", file=sys.stderr)
        if empty:
            print(f"empty: {len(empty)}")
        if failures:
            print(f"failures: {len(failures)}")
            print("\n".join(failures[:20]))
        return 1

    features = latest_features if latest_features is not None else pd.read_parquet(args.features_output)
    print("=== saved ===")
    print(f"fetched_rows: {fetched_rows_total:,}")
    print(f"total_rows  : {len(features):,}")
    print(f"pairs       : {features[['ticker', 'trading_date']].drop_duplicates().shape[0]}")
    print(f"tickers     : {features['ticker'].nunique()}")
    print(f"range       : {features['datetime'].min()} - {features['datetime'].max()}")
    print(f"output      : {args.output}")
    print(f"features    : {args.features_output}")
    if empty:
        print(f"empty       : {len(empty)}")
    if failures:
        print(f"failures    : {len(failures)}")
        print("\n".join(failures[:20]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
