#!/usr/bin/env python3
"""
Generate watch-universe daily bars from J-Quants.

This is the daily counterpart to fetch_watch_minute_jquants.py. It keeps the
frontend-facing prices_max_1d.parquet and tech_snapshot_1d.parquet aligned with
the focused grok + top100 + semicon universe.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import download_file
from scripts.lib.jquants_fetcher import JQuantsFetcher
from server.services.tech_utils_v2 import evaluate_latest_snapshot

UNIVERSE_PATH = PARQUET_DIR / "watch_minute_universe.parquet"
ALL_STOCKS_PATH = PARQUET_DIR / "all_stocks.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
TECH_SNAPSHOT_PATH = PARQUET_DIR / "tech_snapshot_1d.parquet"
HISTORY_START = "2024-01-01"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch watch-universe daily bars with J-Quants.")
    parser.add_argument("--date", help="Target trading date YYYY-MM-DD. Default: latest J-Quants trading day.")
    parser.add_argument("--history-start", default=HISTORY_START)
    parser.add_argument("--universe-path", type=Path, default=UNIVERSE_PATH)
    parser.add_argument("--all-stocks-path", type=Path, default=ALL_STOCKS_PATH)
    parser.add_argument("--prices-out", type=Path, default=PRICES_1D_PATH)
    parser.add_argument("--tech-out", type=Path, default=TECH_SNAPSHOT_PATH)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--bootstrap-missing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def clean_string(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None
    return text.removesuffix(".0")


def normalize_code(value: object) -> str:
    text = clean_string(value) or ""
    if text.endswith(".T"):
        text = text[:-2]
    if len(text) == 5 and text.endswith("0"):
        text = text[:-1]
    return text


def daily_query_code(value: object) -> str:
    code = normalize_code(value)
    if len(code) == 4:
        return f"{code}0"
    return code


def load_universe(args: argparse.Namespace) -> pd.DataFrame:
    if args.universe_path.exists():
        df = pd.read_parquet(args.universe_path)
        code_col = "jquants_query_code" if "jquants_query_code" in df.columns else "code"
    elif args.all_stocks_path.exists():
        df = pd.read_parquet(args.all_stocks_path)
        code_col = "code"
    else:
        raise FileNotFoundError(f"watch universe not found: {args.universe_path}")

    required = {"ticker", code_col}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"missing universe columns: {missing}")

    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.strip()
    out["code"] = out[code_col].map(normalize_code)
    out["daily_query_code"] = out[code_col].map(daily_query_code)
    out = out[out["ticker"].str.endswith(".T") & out["code"].ne("") & out["daily_query_code"].ne("")]
    out = out.drop_duplicates("ticker", keep="first")
    return out[["ticker", "code", "daily_query_code"]].sort_values("ticker").reset_index(drop=True)


def resolve_target_date(fetcher: JQuantsFetcher, explicit: str | None) -> str:
    if explicit:
        return pd.Timestamp(explicit).strftime("%Y-%m-%d")
    for key in ["TARGET_TRADING_DATE", "LATEST_TRADING_DAY", "JQUANTS_TARGET_DATE"]:
        value = os.getenv(key)
        if value:
            return pd.Timestamp(value).strftime("%Y-%m-%d")
    return fetcher.get_latest_trading_day()


def download_existing_prices(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            download_file(cfg, "prices_max_1d.parquet", path)
    except Exception as exc:
        print(f"[WARN] S3 fallback for prices_max_1d failed: {exc}")


def load_existing(path: Path, tickers: set[str]) -> pd.DataFrame:
    download_existing_prices(path)
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        print(f"[WARN] failed to read existing prices: {exc}")
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
    required = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[required].copy()
    df["ticker"] = df["ticker"].astype(str)
    df = df[df["ticker"].isin(tickers)]
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    return df.dropna(subset=["date", "ticker"]).reset_index(drop=True)


def latest_date_str(df: pd.DataFrame) -> str | None:
    if df.empty or "date" not in df.columns:
        return None
    latest = pd.to_datetime(df["date"], errors="coerce").max()
    if pd.isna(latest):
        return None
    return latest.strftime("%Y-%m-%d")


def pick_numeric(df: pd.DataFrame, preferred: str, fallback: str) -> pd.Series:
    preferred_values = pd.to_numeric(df[preferred], errors="coerce") if preferred in df.columns else pd.Series(pd.NA, index=df.index)
    fallback_values = pd.to_numeric(df[fallback], errors="coerce") if fallback in df.columns else pd.Series(pd.NA, index=df.index)
    return preferred_values.where(preferred_values.notna(), fallback_values)


def normalize_daily(raw: pd.DataFrame, target_tickers: set[str]) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
    if "Date" not in raw.columns or "Code" not in raw.columns:
        raise ValueError("J-Quants daily response missing Date/Code")

    df = raw.copy()
    df["code"] = df["Code"].map(normalize_code)
    df["ticker"] = df["code"] + ".T"
    df = df[df["ticker"].isin(target_tickers)]
    if df.empty:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

    normalized = pd.DataFrame(
        {
            "date": pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None),
            "Open": pick_numeric(df, "AdjustmentOpen", "Open"),
            "High": pick_numeric(df, "AdjustmentHigh", "High"),
            "Low": pick_numeric(df, "AdjustmentLow", "Low"),
            "Close": pick_numeric(df, "AdjustmentClose", "Close"),
            "Volume": pick_numeric(df, "AdjustmentVolume", "Volume"),
            "ticker": df["ticker"].astype(str),
        }
    )
    normalized = normalized.dropna(subset=["date", "Close", "ticker"])
    return normalized.reset_index(drop=True)


def fetch_daily_bars(
    fetcher: JQuantsFetcher,
    code: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    params: dict[str, str] = {}
    if code:
        params["code"] = code
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    data = fetcher.client.request_with_pagination(
        "/equities/bars/daily",
        params=params,
        data_key="data",
        max_pages=500,
    )
    if not data:
        return pd.DataFrame()

    raw = pd.DataFrame(data)
    raw = fetcher._normalize_columns(raw)
    if "Date" in raw.columns:
        raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce")
    for col in [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "TurnoverValue",
        "AdjustmentOpen",
        "AdjustmentHigh",
        "AdjustmentLow",
        "AdjustmentClose",
        "AdjustmentVolume",
    ]:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce")
    return raw


def fetch_market_date(fetcher: JQuantsFetcher, target_date: str, target_tickers: set[str]) -> pd.DataFrame:
    raw = fetch_daily_bars(fetcher, from_date=target_date, to_date=target_date)
    return normalize_daily(raw, target_tickers)


def fetch_history_for_missing(
    fetcher: JQuantsFetcher,
    universe: pd.DataFrame,
    missing_tickers: set[str],
    start_date: str,
    target_date: str,
    sleep_seconds: float,
) -> pd.DataFrame:
    if not missing_tickers:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

    frames: list[pd.DataFrame] = []
    targets = universe[universe["ticker"].isin(missing_tickers)].reset_index(drop=True)
    for idx, row in targets.iterrows():
        ticker = str(row["ticker"])
        code = str(row["daily_query_code"])
        print(f"  [{idx + 1}/{len(targets)}] bootstrap {ticker} code={code}")
        try:
            raw = fetch_daily_bars(fetcher, code=code, from_date=start_date, to_date=target_date)
            normalized = normalize_daily(raw, {ticker})
            if not normalized.empty:
                frames.append(normalized)
                print(f"    rows={len(normalized):,}")
            else:
                print("    empty")
        except Exception as exc:
            print(f"    [WARN] failed: {exc}")
        if idx + 1 < len(targets) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])


def merge_prices(existing: pd.DataFrame, bootstrap: pd.DataFrame, latest: pd.DataFrame) -> pd.DataFrame:
    frames = [df for df in [existing, bootstrap, latest] if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce").dt.tz_localize(None)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        combined[col] = pd.to_numeric(combined[col], errors="coerce")
    combined = combined.dropna(subset=["date", "ticker", "Close"])
    combined = combined.sort_values(["ticker", "date"])
    combined = combined.drop_duplicates(["ticker", "date"], keep="last")
    return combined[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]].reset_index(drop=True)


def generate_tech_snapshot(prices: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    snapshots: list[dict[str, object]] = []
    for ticker, grp in prices.groupby("ticker"):
        work = grp.sort_values("date").dropna(subset=["Close"]).copy()
        if len(work) < 20:
            continue
        try:
            snapshots.append(evaluate_latest_snapshot(work.set_index("date")))
        except Exception as exc:
            print(f"  [WARN] tech snapshot failed for {ticker}: {exc}")

    snapshot_df = pd.DataFrame(snapshots, columns=["ticker", "date", "values", "votes", "overall"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_df.to_parquet(output_path, engine="pyarrow", index=False)
    return snapshot_df


def main() -> int:
    args = parse_args()
    print("=== Fetch watch daily data (J-Quants) ===")
    universe = load_universe(args)
    tickers = set(universe["ticker"])
    print(f"universe: {len(universe)} tickers")
    if args.dry_run:
        print(universe.head(20).to_string(index=False))
        return 0

    fetcher = JQuantsFetcher()
    target_date = resolve_target_date(fetcher, args.date)
    print(f"target_date: {target_date}")
    print(f"history_start: {args.history_start}")

    existing = load_existing(args.prices_out, tickers)
    existing_tickers = set(existing["ticker"].unique()) if not existing.empty else set()
    missing_tickers = tickers - existing_tickers
    print(f"existing rows: {len(existing):,}, tickers={len(existing_tickers)}")
    print(f"missing history tickers: {len(missing_tickers)}")

    effective_target_date = target_date
    target_daily_available = True
    try:
        latest = fetch_market_date(fetcher, target_date, tickers)
    except requests.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        existing_latest_date = latest_date_str(existing)
        if status_code == 400 and existing_latest_date:
            print(
                "[WARN] target daily bars are not available from J-Quants yet; "
                f"target={target_date}, fallback={existing_latest_date}"
            )
            latest = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
            effective_target_date = existing_latest_date
            target_daily_available = False
        else:
            raise

    if latest.empty:
        existing_latest_date = latest_date_str(existing)
        if existing_latest_date and existing_latest_date < target_date:
            print(
                "[WARN] target daily bars returned no rows; "
                f"target={target_date}, fallback={existing_latest_date}"
            )
            effective_target_date = existing_latest_date
            target_daily_available = False

    print(f"latest rows: {len(latest):,}, tickers={latest['ticker'].nunique() if not latest.empty else 0}")
    print(f"effective_target_date: {effective_target_date}")

    if args.bootstrap_missing:
        if target_daily_available:
            missing_tickers = missing_tickers | (tickers - set(latest["ticker"].unique()))
        else:
            print("[WARN] skipping target-date completeness check because target daily bars are unavailable")
        bootstrap = fetch_history_for_missing(
            fetcher,
            universe,
            missing_tickers,
            args.history_start,
            effective_target_date,
            args.sleep,
        )
    else:
        bootstrap = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

    combined = merge_prices(existing, bootstrap, latest)
    if combined.empty:
        print("[ERROR] no daily prices available")
        return 1

    latest_date = pd.to_datetime(combined["date"], errors="coerce").max().strftime("%Y-%m-%d")
    if latest_date < target_date:
        if not target_daily_available and latest_date >= effective_target_date:
            print(
                "[WARN] daily prices are behind target because same-day J-Quants daily bars "
                f"are unavailable: latest={latest_date}, target={target_date}"
            )
        else:
            print(f"[ERROR] daily prices are stale: latest={latest_date}, target={target_date}")
            return 1

    args.prices_out.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(args.prices_out, engine="pyarrow", index=False)
    snapshot_df = generate_tech_snapshot(combined, args.tech_out)
    print(f"[OK] saved prices: {args.prices_out} rows={len(combined):,} tickers={combined['ticker'].nunique()}")
    print(f"[OK] saved tech  : {args.tech_out} rows={len(snapshot_df):,}")
    print(f"range: {combined['date'].min().date()} - {combined['date'].max().date()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
