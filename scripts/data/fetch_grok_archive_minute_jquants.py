#!/usr/bin/env python3
"""
Fetch J-Quants minute bars for grok_trending_archive pairs.

This script intentionally calls the `jquants` CLI instead of the API directly.
By default it fetches only the latest archive backtest date; use --all-archive
or date filters for explicit backfills.
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
DEFAULT_ARCHIVE = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
DEFAULT_OUTPUT = ROOT / "data" / "parquet" / "grok_jquants_minute.parquet"
DEFAULT_RAW_DIR = ROOT / "data" / "jquants_csv" / "grok_minute"
DEFAULT_ENV_FILE = ROOT / ".env.jquants"

PLAN_RPM = {
    "free": 5,
    "light": 60,
    "standard": 120,
    "premium": 500,
}
JQUANTS_CLI_TIMEOUT_SECONDS = 180


class RateLimitError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchTarget:
    ticker: str
    query_code: str
    trading_date: str
    stock_name: str | None
    selection_date: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch J-Quants minute bars for Grok archive ticker/date pairs.")
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--date", help="Fetch one archive backtest date (YYYY-MM-DD).")
    parser.add_argument("--from", dest="date_from", help="Fetch archive backtest dates from YYYY-MM-DD.")
    parser.add_argument("--to", dest="date_to", help="Fetch archive backtest dates to YYYY-MM-DD.")
    parser.add_argument("--all-archive", action="store_true", help="Fetch every archive ticker/date pair.")
    parser.add_argument("--tickers", nargs="*", help="Optional ticker filter, e.g. 9262.T 350A.T.")
    parser.add_argument("--max-pairs", type=int, default=0, help="Limit target pairs for smoke tests.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Seconds to sleep between CLI calls.")
    parser.add_argument("--requests-per-minute", type=float, default=0, help="Client-side request cap. Default is half of JQUANTS_PLAN cap.")
    parser.add_argument("--rate-limit-wait", type=float, default=90.0, help="Seconds to wait before retrying after HTTP 429.")
    parser.add_argument("--max-retries", type=int, default=8, help="Retries per pair after transient failures.")
    parser.add_argument("--checkpoint-every", type=int, default=50, help="Save parquet after this many successful pairs.")
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
    # Use a conservative half-cap by default. This avoids rolling-window
    # throttling and leaves room for manual/parallel API use.
    return max(1.0, cap * 0.5)


def normalize_date(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def ticker_to_code(ticker: object, code: object | None) -> str:
    code_str = "" if code is None or pd.isna(code) else str(code).strip()
    if code_str and code_str.lower() != "nan":
        return code_str.removesuffix(".0")
    ticker_str = str(ticker).strip()
    return ticker_str.split(".", 1)[0]


def load_targets(args: argparse.Namespace) -> list[FetchTarget]:
    if not args.archive_path.exists():
        raise FileNotFoundError(f"archive not found: {args.archive_path}")

    archive = pd.read_parquet(args.archive_path)
    required = {"ticker", "backtest_date"}
    missing = sorted(required - set(archive.columns))
    if missing:
        raise ValueError(f"missing archive columns: {missing}")

    df = archive.copy()
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["trading_date"] = df["backtest_date"].map(normalize_date)
    df["selection_date_norm"] = df["selection_date"].map(normalize_date) if "selection_date" in df.columns else None
    df["stock_name_norm"] = df["stock_name"].astype(str) if "stock_name" in df.columns else None
    code_series = df["code"] if "code" in df.columns else pd.Series([None] * len(df), index=df.index)
    df["query_code"] = [ticker_to_code(ticker, code) for ticker, code in zip(df["ticker"], code_series)]
    df = df[df["ticker"].ne("") & df["trading_date"].notna() & df["query_code"].ne("")]

    if args.tickers:
        ticker_set = {ticker.strip() for ticker in args.tickers}
        df = df[df["ticker"].isin(ticker_set)]

    if args.date:
        df = df[df["trading_date"].eq(args.date)]
    elif args.date_from or args.date_to:
        if args.date_from:
            df = df[df["trading_date"].ge(args.date_from)]
        if args.date_to:
            df = df[df["trading_date"].le(args.date_to)]
    elif not args.all_archive:
        latest_date = df["trading_date"].max()
        df = df[df["trading_date"].eq(latest_date)]

    cols = ["ticker", "query_code", "trading_date", "stock_name_norm", "selection_date_norm"]
    df = df[cols].drop_duplicates(["ticker", "trading_date"]).sort_values(["trading_date", "ticker"])

    targets = [
        FetchTarget(
            ticker=row.ticker,
            query_code=row.query_code,
            trading_date=row.trading_date,
            stock_name=None if pd.isna(row.stock_name_norm) else str(row.stock_name_norm),
            selection_date=None if pd.isna(row.selection_date_norm) else str(row.selection_date_norm),
        )
        for row in df.itertuples(index=False)
    ]
    if args.max_pairs > 0:
        targets = targets[: args.max_pairs]
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
    base_dir = raw_dir if raw_dir.is_absolute() else ROOT / raw_dir
    return base_dir / target.trading_date / f"{safe_code}.csv"


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
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=JQUANTS_CLI_TIMEOUT_SECONDS,
        )
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

    df = pd.read_csv(path)
    if df.empty:
        if not keep_empty_csv:
            path.unlink(missing_ok=True)
        return df
    return normalize_minute_csv(df, target, path)


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
    df["selection_date"] = target.selection_date
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


def add_session_vwap(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in ["high", "low", "close", "volume", "value"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    result["typical_price"] = (result["high"] + result["low"] + result["close"]) / 3
    fallback_value = result["typical_price"] * result["volume"].fillna(0)
    result["bar_value"] = result["value"].where(result["value"].notna(), fallback_value)
    result["bar_vwap"] = result["bar_value"] / result["volume"].where(result["volume"] > 0)
    result = result.sort_values(["ticker", "datetime", "fetched_at"])
    keys = ["ticker", "trading_date"]
    result["_cum_value"] = result.groupby(keys, sort=False)["bar_value"].cumsum()
    result["_cum_volume"] = result.groupby(keys, sort=False)["volume"].cumsum()
    result["session_vwap"] = result["_cum_value"] / result["_cum_volume"].where(result["_cum_volume"] > 0)
    return result.drop(columns=["_cum_value", "_cum_volume"])


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
    combined = add_session_vwap(combined)
    columns = [
        "ticker",
        "query_code",
        "jquants_code",
        "stock_name",
        "selection_date",
        "trading_date",
        "time",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "typical_price",
        "bar_value",
        "bar_vwap",
        "session_vwap",
        "source",
        "interval",
        "raw_csv",
        "fetched_at",
    ]
    for col in columns:
        if col not in combined.columns:
            combined[col] = None
    string_columns = [
        "ticker",
        "query_code",
        "jquants_code",
        "stock_name",
        "selection_date",
        "trading_date",
        "time",
        "source",
        "interval",
        "raw_csv",
        "fetched_at",
    ]
    for col in string_columns:
        combined[col] = combined[col].astype("string")
    return combined[columns].reset_index(drop=True)


def main() -> int:
    args = parse_args()
    targets = load_targets(args)
    existing_keys = load_existing_keys(args.output)
    if not args.refresh:
        targets = [target for target in targets if (target.ticker, target.trading_date) not in existing_keys]

    print("=== grok archive minute jquants fetch ===")
    print(f"archive: {args.archive_path}")
    print(f"output : {args.output}")
    print(f"raw_dir: {args.raw_dir}")
    print(f"targets: {len(targets)}")
    if targets:
        print(f"range  : {targets[0].trading_date} - {targets[-1].trading_date}")
        print("sample :", ", ".join(f"{t.ticker}/{t.trading_date}" for t in targets[:10]))

    if args.dry_run:
        return 0
    if not targets:
        print("[OK] nothing to fetch")
        return 0

    env = load_env_file(args.env_file)
    requests_per_minute = resolve_requests_per_minute(args, env)
    min_interval = 60.0 / requests_per_minute
    inter_request_sleep = max(args.sleep, min_interval)
    print(f"rate  : {requests_per_minute:.1f} requests/min, sleep>={inter_request_sleep:.2f}s")

    frames: list[pd.DataFrame] = []
    failures: list[str] = []
    empty: list[str] = []
    fetched_rows_total = 0
    saved_combined: pd.DataFrame | None = None

    def save_checkpoint(force: bool = False) -> None:
        nonlocal frames, fetched_rows_total, saved_combined
        if not frames:
            return
        if not force and args.checkpoint_every > 0 and len(frames) < args.checkpoint_every:
            return

        fetched = pd.concat(frames, ignore_index=True)
        combined = merge_existing(args.output, fetched)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(args.output, engine="pyarrow", index=False)
        fetched_rows_total += len(fetched)
        saved_combined = combined
        print(
            f"  -> checkpoint saved: fetched_rows_total={fetched_rows_total:,} "
            f"total_rows={len(combined):,} pairs={combined[['ticker', 'trading_date']].drop_duplicates().shape[0]}",
            flush=True,
        )
        frames = []

    for idx, target in enumerate(targets, start=1):
        print(f"[{idx}/{len(targets)}] {target.ticker} {target.trading_date} code={target.query_code}", flush=True)
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
                    f"  -> rate limited; waiting {wait_seconds:.0f}s before retry "
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

    combined = saved_combined if saved_combined is not None else pd.read_parquet(args.output)

    print("=== saved ===")
    print(f"fetched_rows: {fetched_rows_total:,}")
    print(f"total_rows  : {len(combined):,}")
    print(f"pairs       : {combined[['ticker', 'trading_date']].drop_duplicates().shape[0]}")
    print(f"tickers     : {combined['ticker'].nunique()}")
    print(f"range       : {combined['datetime'].min()} - {combined['datetime'].max()}")
    print(f"output      : {args.output}")
    if empty:
        print(f"empty       : {len(empty)}")
    if failures:
        print(f"failures    : {len(failures)}")
        print("\n".join(failures[:20]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
