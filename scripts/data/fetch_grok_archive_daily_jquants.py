#!/usr/bin/env python3
"""
Fetch J-Quants daily bars for grok_trending_archive rows via the jquants CLI.

The script keeps raw date-level CSV files and writes a normalized daily parquet
for analysis. It intentionally invokes the jquants CLI instead of calling the
API directly. The archive parquet is treated as read-only input.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARCHIVE = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
DEFAULT_ANALYSIS_DIR = ROOT / "data" / "analysis" / "grok_intraday_jquants"
DEFAULT_OUTPUT = DEFAULT_ANALYSIS_DIR / "grok_jquants_daily.parquet"
DEFAULT_RAW_DIR = ROOT / "data" / "jquants_csv" / "grok_daily"
DEFAULT_ENV_FILE = ROOT / ".env.jquants"

PLAN_RPM = {
    "free": 5,
    "light": 60,
    "standard": 120,
    "premium": 500,
}
JQUANTS_CLI_TIMEOUT_SECONDS = 240


class RateLimitError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Grok archive daily bars with the jquants CLI.")
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--date", help="Fetch one archive backtest date (YYYY-MM-DD).")
    parser.add_argument("--from", dest="date_from", help="Fetch archive backtest dates from YYYY-MM-DD.")
    parser.add_argument("--to", dest="date_to", help="Fetch archive backtest dates to YYYY-MM-DD.")
    parser.add_argument("--all-archive", action="store_true", help="Fetch every archive backtest date.")
    parser.add_argument("--max-dates", type=int, default=0, help="Limit date count for smoke tests.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Seconds to sleep between CLI calls.")
    parser.add_argument("--requests-per-minute", type=float, default=0, help="Client-side request cap.")
    parser.add_argument("--rate-limit-wait", type=float, default=120.0, help="Seconds to wait before retrying after HTTP 429.")
    parser.add_argument("--max-retries", type=int, default=8, help="Retries per date after transient failures.")
    parser.add_argument("--refresh", action="store_true", help="Refetch dates already present in the output parquet.")
    parser.add_argument("--refresh-raw-csv", action="store_true", help="Call jquants even when the raw CSV already exists.")
    parser.add_argument("--dry-run", action="store_true", help="Print targets without calling jquants or writing files.")
    parser.add_argument("--keep-empty-csv", action="store_true", help="Keep empty CSV files for failed/empty fetches.")
    parser.add_argument("--quiet", action="store_true", help="Reduce per-date stdout for long fetches.")
    parser.add_argument("--progress-every", type=int, default=10, help="Print compact progress every N dates when --quiet is used.")
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
    return max(1.0, cap * 0.5)


def normalize_date(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def clean_string(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text.removesuffix(".0")


def ticker_to_code(ticker: object, code: object | None = None) -> str:
    code_text = clean_string(code)
    if code_text:
        return code_text
    ticker_text = clean_string(ticker) or ""
    return ticker_text.split(".", 1)[0]


def normalize_jquants_code(value: object) -> str:
    text = clean_string(value) or ""
    if len(text) == 5 and text.endswith("0"):
        return text[:-1]
    return text


def load_archive_targets(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
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
    code_series = df["code"] if "code" in df.columns else pd.Series([None] * len(df), index=df.index)
    df["query_code"] = [ticker_to_code(ticker, code) for ticker, code in zip(df["ticker"], code_series)]
    df = df[df["ticker"].ne("") & df["trading_date"].notna() & df["query_code"].ne("")]

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

    pairs = df[["ticker", "query_code", "trading_date"]].drop_duplicates(["ticker", "trading_date"])
    dates = pd.DataFrame({"trading_date": sorted(pairs["trading_date"].dropna().unique().tolist())})
    if args.max_dates > 0:
        dates = dates.head(args.max_dates)
        pairs = pairs[pairs["trading_date"].isin(dates["trading_date"])].copy()
    return pairs.reset_index(drop=True), dates.reset_index(drop=True)


def load_existing_dates(output: Path) -> set[str]:
    if not output.exists():
        return set()
    existing = pd.read_parquet(output, columns=["trading_date"])
    if existing.empty:
        return set()
    return set(existing["trading_date"].astype(str).dropna().unique().tolist())


def raw_csv_path(raw_dir: Path, trading_date: str) -> Path:
    base_dir = raw_dir if raw_dir.is_absolute() else ROOT / raw_dir
    return base_dir / f"{trading_date}.csv"


def fetch_date(
    trading_date: str,
    raw_dir: Path,
    env: dict[str, str],
    keep_empty_csv: bool,
    refresh_raw_csv: bool,
) -> pd.DataFrame:
    path = raw_csv_path(raw_dir, trading_date)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists() or refresh_raw_csv:
        cmd = [
            "jquants",
            "--output",
            "csv",
            "--save",
            str(path),
            "eq",
            "daily",
            "--date",
            trading_date,
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
                raise RateLimitError(f"jquants CLI rate limited for {trading_date}: {stderr}")
            raise RuntimeError(f"jquants CLI failed for {trading_date}: {stderr}")

    if not path.exists() or path.stat().st_size == 0:
        if path.exists() and not keep_empty_csv:
            path.unlink()
        return pd.DataFrame()

    df = pd.read_csv(path)
    if df.empty:
        if not keep_empty_csv:
            path.unlink(missing_ok=True)
        return df
    return normalize_daily_csv(df, path)


def normalize_daily_csv(raw: pd.DataFrame, path: Path) -> pd.DataFrame:
    rename = {
        "Date": "trading_date",
        "Code": "jquants_code",
        "O": "open",
        "H": "high",
        "L": "low",
        "C": "close",
        "Vo": "volume",
        "Va": "value",
        "UL": "limit_up_flag",
        "LL": "limit_down_flag",
        "AdjC": "adj_close",
        "AdjVo": "adj_volume",
    }
    required = {"Date", "Code", "C", "Vo"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"missing columns in {path}: {missing}")

    available = {src: dst for src, dst in rename.items() if src in raw.columns}
    df = raw.rename(columns=available)[list(available.values())].copy()
    df["jquants_code"] = df["jquants_code"].astype(str).map(normalize_jquants_code)
    df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["open", "high", "low", "close", "volume", "value", "adj_close", "adj_volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["trading_date", "jquants_code"])
    if df.empty:
        return df

    df["source"] = "jquants_cli"
    df["raw_csv"] = str(path.relative_to(ROOT))
    df["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def merge_existing(output: Path, fetched: pd.DataFrame) -> pd.DataFrame:
    if output.exists():
        existing = pd.read_parquet(output)
        combined = pd.concat([existing, fetched], ignore_index=True)
    else:
        combined = fetched

    combined["trading_date"] = combined["trading_date"].astype(str)
    combined["jquants_code"] = combined["jquants_code"].astype(str)
    combined = combined.dropna(subset=["trading_date", "jquants_code"])
    combined = combined.sort_values(["trading_date", "jquants_code", "fetched_at"])
    combined = combined.drop_duplicates(["trading_date", "jquants_code"], keep="last")
    columns = [
        "trading_date",
        "jquants_code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "limit_up_flag",
        "limit_down_flag",
        "adj_close",
        "adj_volume",
        "source",
        "raw_csv",
        "fetched_at",
    ]
    for col in columns:
        if col not in combined.columns:
            combined[col] = None
    return combined[columns].reset_index(drop=True)


def filter_to_pairs(daily: pd.DataFrame, pairs: pd.DataFrame) -> pd.DataFrame:
    target = pairs.copy()
    target["query_code"] = target["query_code"].astype(str)
    target["_join_code"] = target["query_code"]
    daily = daily.rename(columns={"jquants_code": "_join_code"}).copy()
    daily["_join_code"] = daily["_join_code"].astype(str)
    result = target.merge(daily, on=["trading_date", "_join_code"], how="left")
    result = result.rename(columns={"_join_code": "jquants_code"})
    return result


def main() -> int:
    args = parse_args()
    pairs, dates_df = load_archive_targets(args)
    existing_dates = load_existing_dates(args.output)
    dates = dates_df["trading_date"].astype(str).tolist()
    if not args.refresh:
        dates = [date for date in dates if date not in existing_dates]

    print("=== grok archive daily jquants fetch ===")
    print(f"archive: {args.archive_path}")
    print(f"output : {args.output}")
    print(f"raw_dir: {args.raw_dir}")
    print(f"pairs  : {len(pairs):,}")
    print(f"dates  : {len(dates):,}")
    if dates:
        print(f"range  : {dates[0]} - {dates[-1]}")
        print("sample :", ", ".join(dates[:10]))

    if args.dry_run:
        return 0

    fetched_frames: list[pd.DataFrame] = []
    failures: list[str] = []
    empty: list[str] = []

    if dates:
        env = load_env_file(args.env_file)
        requests_per_minute = resolve_requests_per_minute(args, env)
        min_interval = 60.0 / requests_per_minute
        inter_request_sleep = max(args.sleep, min_interval)
        print(f"rate   : {requests_per_minute:.1f} requests/min, sleep>={inter_request_sleep:.2f}s")

        for idx, trading_date in enumerate(dates, start=1):
            if args.quiet:
                if idx == 1 or idx == len(dates) or (args.progress_every > 0 and idx % args.progress_every == 0):
                    print(f"[{idx}/{len(dates)}] {trading_date}", flush=True)
            else:
                print(f"[{idx}/{len(dates)}] {trading_date}", flush=True)

            df = pd.DataFrame()
            failed_current = False
            for attempt in range(1, args.max_retries + 2):
                try:
                    df = fetch_date(trading_date, args.raw_dir, env, args.keep_empty_csv, args.refresh_raw_csv)
                    break
                except RateLimitError as exc:
                    if attempt > args.max_retries:
                        failures.append(f"{trading_date}: {exc}")
                        failed_current = True
                        print(f"  -> failed after rate-limit retries: {exc}", flush=True)
                        break
                    wait_seconds = args.rate_limit_wait * attempt
                    print(f"  -> rate limited; waiting {wait_seconds:.0f}s ({attempt}/{args.max_retries})", flush=True)
                    time.sleep(wait_seconds)
                except Exception as exc:
                    if attempt > args.max_retries:
                        failures.append(f"{trading_date}: {exc}")
                        failed_current = True
                        print(f"  -> failed: {exc}", flush=True)
                        break
                    wait_seconds = min(30.0 * attempt, args.rate_limit_wait)
                    print(f"  -> transient failure; waiting {wait_seconds:.0f}s ({attempt}/{args.max_retries}): {exc}", flush=True)
                    time.sleep(wait_seconds)

            if failed_current:
                continue
            if df.empty:
                empty.append(trading_date)
                if not args.quiet:
                    print("  -> empty", flush=True)
            else:
                fetched_frames.append(df)
                if not args.quiet:
                    print(f"  -> rows={len(df):,}", flush=True)
            if idx < len(dates) and inter_request_sleep > 0:
                time.sleep(inter_request_sleep)

    if fetched_frames:
        fetched = pd.concat(fetched_frames, ignore_index=True)
        combined = merge_existing(args.output, fetched)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(args.output, engine="pyarrow", index=False)
    elif args.output.exists():
        combined = pd.read_parquet(args.output)
    else:
        combined = pd.DataFrame()

    if combined.empty:
        print("[ERROR] no daily data available", file=sys.stderr)
        if empty:
            print(f"empty: {len(empty)}")
        if failures:
            print(f"failures: {len(failures)}")
            print("\n".join(failures[:20]))
        return 1

    daily_pairs = filter_to_pairs(combined, pairs)
    matched_pairs = int(daily_pairs["close"].notna().sum()) if "close" in daily_pairs.columns else 0
    print("=== saved daily ===")
    print(f"total_daily_rows : {len(combined):,}")
    print(f"archive_pairs    : {len(pairs):,}")
    print(f"matched_pairs    : {matched_pairs:,}")
    print(f"output           : {args.output}")
    if empty:
        print(f"empty_dates      : {len(empty)}")
    if failures:
        print(f"failures         : {len(failures)}")
        print("\n".join(failures[:20]))

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
