#!/usr/bin/env python3
"""
Fetch J-Quants minute bars for pre-trade context around Grok archive pairs.

This script intentionally calls the `jquants` CLI instead of the API directly.
It expands each Grok archive (ticker, backtest_date) row into prior trading
dates such as T-1 through T-5, then fetches missing ticker/date pairs only.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.data.fetch_grok_archive_minute_jquants import (  # noqa: E402
    DEFAULT_ENV_FILE,
    FetchTarget,
    RateLimitError,
    fetch_target,
    load_env_file,
    load_existing_keys,
    merge_existing,
    normalize_date,
    resolve_requests_per_minute,
    ticker_to_code,
)


DEFAULT_ARCHIVE = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
DEFAULT_CALENDAR = ROOT / "data" / "parquet" / "grok_prices_max_1d.parquet"
DEFAULT_OUTPUT = ROOT / "data" / "analysis" / "grok_intraday_jquants" / "grok_jquants_minute_context.parquet"
DEFAULT_TARGET_MAP = ROOT / "data" / "analysis" / "grok_intraday_jquants" / "grok_jquants_minute_context_targets.parquet"
DEFAULT_RAW_DIR = ROOT / "data" / "jquants_csv" / "grok_minute_context"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch prior-day J-Quants minute context for Grok archive pairs.")
    parser.add_argument(
        "--source-kind",
        choices=["archive", "trending"],
        default="archive",
        help="Input schema: archive uses backtest_date, trending uses current grok_trending date.",
    )
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--calendar-path", type=Path, default=DEFAULT_CALENDAR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--target-map-output", type=Path, default=DEFAULT_TARGET_MAP)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--lags", type=int, nargs="*", default=[1, 2, 3, 4, 5], help="Prior trading-day lags to fetch.")
    parser.add_argument("--date", help="Anchor Grok backtest date (YYYY-MM-DD).")
    parser.add_argument("--from", dest="date_from", help="Anchor Grok backtest dates from YYYY-MM-DD.")
    parser.add_argument("--to", dest="date_to", help="Anchor Grok backtest dates to YYYY-MM-DD.")
    parser.add_argument("--tickers", nargs="*", help="Optional ticker filter, e.g. 9262.T 350A.T.")
    parser.add_argument("--max-pairs", type=int, default=0, help="Limit unique context ticker/date pairs for smoke tests.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Seconds to sleep between CLI calls.")
    parser.add_argument("--requests-per-minute", type=float, default=0, help="Client-side request cap. Default follows plan logic.")
    parser.add_argument("--rate-limit-wait", type=float, default=120.0, help="Seconds to wait before retrying after HTTP 429.")
    parser.add_argument("--max-retries", type=int, default=8, help="Retries per pair after transient failures.")
    parser.add_argument("--checkpoint-every", type=int, default=50, help="Save parquet after this many successful pairs.")
    parser.add_argument("--refresh", action="store_true", help="Refetch pairs already present in the output parquet.")
    parser.add_argument("--refresh-raw-csv", action="store_true", help="Call jquants even when the raw CSV already exists.")
    parser.add_argument("--dry-run", action="store_true", help="Print target pairs without calling jquants or writing files.")
    parser.add_argument("--keep-empty-csv", action="store_true", help="Keep empty CSV files for failed/empty fetches.")
    parser.add_argument("--quiet", action="store_true", help="Reduce per-target stdout for long backfills.")
    parser.add_argument("--progress-every", type=int, default=100, help="Print compact progress every N targets when --quiet is used.")
    return parser.parse_args()


def load_trading_calendar(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"calendar not found: {path}")

    dates = pd.read_parquet(path, columns=["date"])["date"]
    parsed = pd.to_datetime(dates, errors="coerce").dropna().dt.normalize()
    values = sorted(parsed.dt.strftime("%Y-%m-%d").drop_duplicates().tolist())
    if not values:
        raise ValueError(f"no trading dates in calendar: {path}")
    return values


def prior_trading_date(calendar: list[str], anchor: str, lag: int) -> str | None:
    before = [date for date in calendar if date < anchor]
    if len(before) < lag:
        return None
    return before[-lag]


def load_context_targets(args: argparse.Namespace) -> tuple[list[FetchTarget], pd.DataFrame]:
    if not args.archive_path.exists():
        raise FileNotFoundError(f"source not found: {args.archive_path}")

    source = pd.read_parquet(args.archive_path)
    date_col = "backtest_date" if args.source_kind == "archive" else "date"
    required = {"ticker", date_col}
    missing = sorted(required - set(source.columns))
    if missing:
        raise ValueError(f"missing {args.source_kind} source columns: {missing}")

    df = source.copy()
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["anchor_backtest_date"] = df[date_col].map(normalize_date)
    if args.source_kind == "archive":
        selection_col = "selection_date"
    else:
        selection_col = "price_asof_date" if "price_asof_date" in df.columns else "selection_date"
    df["selection_date_norm"] = df[selection_col].map(normalize_date) if selection_col in df.columns else None
    df["stock_name_norm"] = df["stock_name"].astype(str) if "stock_name" in df.columns else None
    code_series = df["code"] if "code" in df.columns else pd.Series([None] * len(df), index=df.index)
    df["query_code"] = [ticker_to_code(ticker, code) for ticker, code in zip(df["ticker"], code_series)]
    df = df[df["ticker"].ne("") & df["anchor_backtest_date"].notna() & df["query_code"].ne("")]

    if args.tickers:
        ticker_set = {ticker.strip() for ticker in args.tickers}
        df = df[df["ticker"].isin(ticker_set)]
    if args.date:
        df = df[df["anchor_backtest_date"].eq(args.date)]
    else:
        if args.date_from:
            df = df[df["anchor_backtest_date"].ge(args.date_from)]
        if args.date_to:
            df = df[df["anchor_backtest_date"].le(args.date_to)]

    base_cols = ["ticker", "query_code", "anchor_backtest_date", "stock_name_norm", "selection_date_norm"]
    base = df[base_cols].drop_duplicates(["ticker", "anchor_backtest_date"]).sort_values(["anchor_backtest_date", "ticker"])

    calendar = load_trading_calendar(args.calendar_path)
    lags = sorted({lag for lag in args.lags if lag > 0})
    rows: list[dict[str, object]] = []
    for row in base.itertuples(index=False):
        for lag in lags:
            context_date = prior_trading_date(calendar, row.anchor_backtest_date, lag)
            if context_date is None:
                continue
            rows.append(
                {
                    "ticker": row.ticker,
                    "query_code": row.query_code,
                    "stock_name": None if pd.isna(row.stock_name_norm) else str(row.stock_name_norm),
                    "selection_date": None if pd.isna(row.selection_date_norm) else str(row.selection_date_norm),
                    "anchor_backtest_date": row.anchor_backtest_date,
                    "context_trading_date": context_date,
                    "context_lag": lag,
                }
            )

    target_map = pd.DataFrame(rows)
    if target_map.empty:
        return [], target_map

    target_map = target_map.drop_duplicates(["ticker", "anchor_backtest_date", "context_lag"])
    unique_pairs = (
        target_map.sort_values(["context_trading_date", "ticker", "anchor_backtest_date", "context_lag"])
        .drop_duplicates(["ticker", "context_trading_date"])
        .copy()
    )
    if args.max_pairs > 0:
        unique_pairs = unique_pairs.head(args.max_pairs)
        allowed = set(zip(unique_pairs["ticker"], unique_pairs["context_trading_date"]))
        target_map = target_map[
            target_map[["ticker", "context_trading_date"]].apply(tuple, axis=1).isin(allowed)
        ].copy()

    targets = [
        FetchTarget(
            ticker=row.ticker,
            query_code=row.query_code,
            trading_date=row.context_trading_date,
            stock_name=row.stock_name,
            selection_date=row.selection_date,
        )
        for row in unique_pairs.itertuples(index=False)
    ]
    return targets, target_map.reset_index(drop=True)


def merge_target_map(output: Path, new_map: pd.DataFrame) -> pd.DataFrame:
    if output.exists():
        existing = pd.read_parquet(output)
        combined = pd.concat([existing, new_map], ignore_index=True)
    else:
        combined = new_map

    if combined.empty:
        return combined

    combined["ticker"] = combined["ticker"].astype(str)
    combined["anchor_backtest_date"] = combined["anchor_backtest_date"].astype(str)
    combined["context_trading_date"] = combined["context_trading_date"].astype(str)
    combined["context_lag"] = pd.to_numeric(combined["context_lag"], errors="coerce").astype("Int64")
    combined = combined.drop_duplicates(["ticker", "anchor_backtest_date", "context_lag"], keep="last")
    return combined.sort_values(["anchor_backtest_date", "ticker", "context_lag"]).reset_index(drop=True)


def main() -> int:
    args = parse_args()
    targets, target_map = load_context_targets(args)
    existing_keys = load_existing_keys(args.output)
    if not args.refresh:
        targets = [target for target in targets if (target.ticker, target.trading_date) not in existing_keys]

    print("=== grok archive prior-minute context jquants fetch ===")
    print(f"source    : {args.archive_path}")
    print(f"source_kind: {args.source_kind}")
    print(f"calendar  : {args.calendar_path}")
    print(f"output    : {args.output}")
    print(f"target_map: {args.target_map_output}")
    print(f"raw_dir   : {args.raw_dir}")
    print(f"lags      : {','.join(map(str, sorted({lag for lag in args.lags if lag > 0})))}")
    print(f"anchor rows: {target_map[['ticker', 'anchor_backtest_date']].drop_duplicates().shape[0] if not target_map.empty else 0}")
    print(f"map rows  : {len(target_map)}")
    print(f"targets   : {len(targets)}")
    if targets:
        print(f"range     : {targets[0].trading_date} - {targets[-1].trading_date}")
        print("sample    :", ", ".join(f"{t.ticker}/{t.trading_date}" for t in targets[:10]))

    if args.dry_run:
        return 0

    args.target_map_output.parent.mkdir(parents=True, exist_ok=True)
    merged_target_map = merge_target_map(args.target_map_output, target_map)
    merged_target_map.to_parquet(args.target_map_output, engine="pyarrow", index=False)

    if not targets:
        print("[OK] nothing to fetch")
        return 0

    env = load_env_file(args.env_file)
    requests_per_minute = resolve_requests_per_minute(args, env)
    min_interval = 60.0 / requests_per_minute
    inter_request_sleep = max(args.sleep, min_interval)
    print(f"rate      : {requests_per_minute:.1f} requests/min, sleep>={inter_request_sleep:.2f}s")

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
        if args.quiet:
            if idx == 1 or idx == len(targets) or (args.progress_every > 0 and idx % args.progress_every == 0):
                print(f"[{idx}/{len(targets)}] {target.ticker} {target.trading_date} code={target.query_code}", flush=True)
        else:
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
            if not args.quiet:
                print("  -> empty")
        else:
            frames.append(df)
            if not args.quiet:
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
    print(f"target_map  : {args.target_map_output}")
    if empty:
        print(f"empty       : {len(empty)}")
    if failures:
        print(f"failures    : {len(failures)}")
        print("\n".join(failures[:20]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
