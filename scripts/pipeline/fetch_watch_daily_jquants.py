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
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import download_file
from scripts.lib.jquants_daily_fields import (
    JQ_DAILY_TRADE_STATUS,
    JQUANTS_DAILY_FIELD_COLUMNS,
    classify_jquants_daily_trade_status,
    missing_raw_daily_fields,
    normalize_jquants_daily_fields,
)
from scripts.lib.jquants_fetcher import JQuantsFetcher
from server.services.tech_utils_v2 import evaluate_latest_snapshot

UNIVERSE_PATH = PARQUET_DIR / "watch_minute_universe.parquet"
ALL_STOCKS_PATH = PARQUET_DIR / "all_stocks.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
TECH_SNAPSHOT_PATH = PARQUET_DIR / "tech_snapshot_1d.parquet"
DAILY_FEATURES_PATH = PARQUET_DIR / "jquants" / "watch_daily_features.parquet"
HISTORY_START = "2024-01-01"

DAILY_FEATURE_COLUMNS = [
    "trading_date",
    "ticker",
    "jquants_code",
    *JQUANTS_DAILY_FIELD_COLUMNS,
    JQ_DAILY_TRADE_STATUS,
    "source",
    "fetched_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch watch-universe daily bars with J-Quants.")
    parser.add_argument("--date", help="Target trading date YYYY-MM-DD. Default: latest J-Quants trading day.")
    parser.add_argument("--history-start", default=HISTORY_START)
    parser.add_argument("--universe-path", type=Path, default=UNIVERSE_PATH)
    parser.add_argument("--all-stocks-path", type=Path, default=ALL_STOCKS_PATH)
    parser.add_argument("--prices-out", type=Path, default=PRICES_1D_PATH)
    parser.add_argument("--tech-out", type=Path, default=TECH_SNAPSHOT_PATH)
    parser.add_argument(
        "--daily-features-out",
        type=Path,
        default=DAILY_FEATURES_PATH,
        help="Nullable J-Quants MktCap/ExRT/AdjFactor sidecar.",
    )
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


def download_existing_daily_features(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            download_file(cfg, "jquants/watch_daily_features.parquet", path)
    except Exception as exc:
        print(f"[WARN] S3 fallback for watch daily features failed: {exc}")


def load_existing_daily_features(path: Path) -> pd.DataFrame:
    download_existing_daily_features(path)
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=DAILY_FEATURE_COLUMNS)
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        print(f"[WARN] failed to read existing watch daily features: {exc}")
        return pd.DataFrame(columns=DAILY_FEATURE_COLUMNS)

    df = normalize_jquants_daily_fields(df)
    for col in DAILY_FEATURE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df["trading_date"] = pd.to_datetime(
        df["trading_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["trading_date", "ticker", "jquants_code"])
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["jquants_code"] = df["jquants_code"].map(normalize_code)
    return df[DAILY_FEATURE_COLUMNS].reset_index(drop=True)


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


def normalize_daily_features(
    raw: pd.DataFrame,
    target_tickers: set[str],
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=DAILY_FEATURE_COLUMNS)
    if "Date" not in raw.columns or "Code" not in raw.columns:
        raise ValueError("J-Quants daily response missing Date/Code")
    missing = missing_raw_daily_fields(raw.columns)
    if missing:
        raise ValueError(
            "J-Quants daily response does not support required fields: "
            f"{missing}"
        )

    df = normalize_jquants_daily_fields(raw)
    df[JQ_DAILY_TRADE_STATUS] = classify_jquants_daily_trade_status(df)
    df["jquants_code"] = df["Code"].map(normalize_code)
    df["ticker"] = df["jquants_code"] + ".T"
    df = df[df["ticker"].isin(target_tickers)].copy()
    if df.empty:
        return pd.DataFrame(columns=DAILY_FEATURE_COLUMNS)

    df["trading_date"] = pd.to_datetime(
        df["Date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df["source"] = "jquants_api_v2"
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df = df.dropna(subset=["trading_date", "ticker", "jquants_code"])
    return df[DAILY_FEATURE_COLUMNS].reset_index(drop=True)


def merge_daily_features(
    existing: pd.DataFrame,
    latest: pd.DataFrame,
) -> pd.DataFrame:
    frames = [df for df in [existing, latest] if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame(columns=DAILY_FEATURE_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    combined = normalize_jquants_daily_fields(combined)
    for col in DAILY_FEATURE_COLUMNS:
        if col not in combined.columns:
            combined[col] = pd.NA
    combined["trading_date"] = pd.to_datetime(
        combined["trading_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    combined = combined.dropna(subset=["trading_date", "ticker", "jquants_code"])
    combined["ticker"] = combined["ticker"].astype(str).str.strip()
    combined["jquants_code"] = combined["jquants_code"].map(normalize_code)
    combined = combined.sort_values(["trading_date", "ticker", "fetched_at"])
    combined = combined.drop_duplicates(["trading_date", "ticker"], keep="last")
    return combined[DAILY_FEATURE_COLUMNS].reset_index(drop=True)


def fetch_daily_bars(
    fetcher: JQuantsFetcher,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    if date and (from_date or to_date):
        raise ValueError("J-Quants daily request cannot mix date with from/to")
    params: dict[str, str] = {}
    if code:
        params["code"] = code
    if date:
        params["date"] = date
    elif not code and (from_date or to_date):
        if from_date and to_date and from_date == to_date:
            # V2 requires ``date`` for one-day all-market retrieval.
            params["date"] = from_date
        else:
            raise ValueError(
                "J-Quants all-market daily retrieval requires exactly one date"
            )
    else:
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
    raw = normalize_jquants_daily_fields(raw)
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
    raw = fetch_daily_bars(fetcher, date=target_date)
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


def generate_tech_snapshot(prices: pd.DataFrame) -> pd.DataFrame:
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
    return snapshot_df


def write_parquet_atomic(frame: pd.DataFrame, output_path: Path) -> None:
    """Install one complete parquet only after serialization succeeds."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        frame.to_parquet(temporary_path, engine="pyarrow", index=False)
        reloaded = pd.read_parquet(temporary_path)
        if len(reloaded) != len(frame) or reloaded.columns.tolist() != frame.columns.tolist():
            raise RuntimeError(
                f"serialized parquet verification failed: {output_path}"
            )
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


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

    latest_raw = fetch_daily_bars(
        fetcher,
        date=target_date,
    )
    latest = normalize_daily(latest_raw, tickers)
    target_features = normalize_daily_features(latest_raw, tickers)
    previous_trading_date = fetcher.get_previous_trading_day(target_date)
    if not previous_trading_date:
        print(f"[ERROR] previous trading day not found for {target_date}")
        return 1
    previous_raw = fetch_daily_bars(
        fetcher,
        date=previous_trading_date,
    )
    previous_features = normalize_daily_features(previous_raw, tickers)
    latest_features = pd.concat(
        [previous_features, target_features],
        ignore_index=True,
    )
    print(f"latest rows: {len(latest):,}, tickers={latest['ticker'].nunique() if not latest.empty else 0}")
    print(
        f"daily fields ({previous_trading_date}, {target_date}): "
        f"{len(latest_features):,}, "
        f"MktCap={int(latest_features['jq_mkt_cap_million_yen'].notna().sum()) if not latest_features.empty else 0}, "
        f"ExRT={int(latest_features['jq_ex_rights_type'].notna().sum()) if not latest_features.empty else 0}"
    )

    if args.bootstrap_missing:
        missing_tickers = missing_tickers | (tickers - set(latest["ticker"].unique()))
        bootstrap = fetch_history_for_missing(
            fetcher,
            universe,
            missing_tickers,
            args.history_start,
            target_date,
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
        print(f"[ERROR] daily prices are stale: latest={latest_date}, target={target_date}")
        return 1

    existing_features = load_existing_daily_features(args.daily_features_out)
    daily_features = merge_daily_features(existing_features, latest_features)
    if daily_features.empty:
        print("[ERROR] no J-Quants daily feature rows available")
        return 1
    latest_feature_date = daily_features["trading_date"].max()
    if latest_feature_date < target_date:
        print(
            "[ERROR] J-Quants daily features are stale: "
            f"latest={latest_feature_date}, target={target_date}"
        )
        return 1
    previous_coverage = set(
        daily_features.loc[
            daily_features["trading_date"].eq(previous_trading_date),
            "ticker",
        ].astype(str)
    )
    missing_previous = sorted(tickers - previous_coverage)
    if missing_previous:
        print(
            "[WARN] previous-trading-day daily features are unavailable for "
            f"{len(missing_previous)} watch-universe tickers on "
            f"{previous_trading_date}; selected Grok tickers are checked "
            "strictly before market-cap attachment/archive publication: "
            f"missing={missing_previous}"
        )
    snapshot_df = generate_tech_snapshot(combined)
    write_parquet_atomic(combined, args.prices_out)
    write_parquet_atomic(daily_features, args.daily_features_out)
    write_parquet_atomic(snapshot_df, args.tech_out)
    print(f"[OK] saved prices: {args.prices_out} rows={len(combined):,} tickers={combined['ticker'].nunique()}")
    print(
        f"[OK] saved fields: {args.daily_features_out} "
        f"rows={len(daily_features):,}"
    )
    print(f"[OK] saved tech  : {args.tech_out} rows={len(snapshot_df):,}")
    print(f"range: {combined['date'].min().date()} - {combined['date'].max().date()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
