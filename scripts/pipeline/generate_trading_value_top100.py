#!/usr/bin/env python3
"""
Generate daily trading-value Top100 and Top150 history from J-Quants eq daily.

Outputs:
  - data/parquet/trading_value_top100.parquet
  - data/parquet/trading_value_top_history.parquet
  - data/jquants_csv/master/trading_value_top100.csv
  - data/csv/baibai_generated.csv
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import download_file
from scripts.pipeline.generate_market_basket_turnover import (
    MARKET_BASKET_TURNOVER_PATH,
    save_market_basket_turnover_from_daily,
)
from scripts.lib.jquants_fetcher import JQuantsFetcher

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
PARQUET_OUT = PARQUET_DIR / "trading_value_top100.parquet"
HISTORY_OUT = PARQUET_DIR / "trading_value_top_history.parquet"
CSV_MASTER_OUT = ROOT / "data" / "jquants_csv" / "master" / "trading_value_top100.csv"
CSV_BAIBAI_OUT = ROOT / "data" / "csv" / "baibai_generated.csv"

ETF_META_BY_TICKER = {
    "1306.T": {"stock_name": "TOPIX連動ETF", "sectors": "指数ETF"},
    "1321.T": {"stock_name": "日経225連動ETF", "sectors": "指数ETF"},
    "1458.T": {"stock_name": "楽天日経レバETF", "sectors": "指数・レバETF"},
    "1570.T": {"stock_name": "日経平均レバレッジETF", "sectors": "指数・レバETF"},
    "1579.T": {"stock_name": "日経平均ブル2倍ETF", "sectors": "指数・レバETF"},
    "1357.T": {"stock_name": "日経平均ダブルインバースETF", "sectors": "インバースETF"},
    "1360.T": {"stock_name": "日経平均ベア2倍ETF", "sectors": "インバースETF"},
    "200A.T": {"stock_name": "日経半導体株ETF", "sectors": "半導体ETF"},
    "213A.T": {"stock_name": "日経半導体株ETF", "sectors": "半導体ETF"},
    "2243.T": {"stock_name": "半導体関連ETF", "sectors": "半導体ETF"},
    "2644.T": {"stock_name": "半導体関連-日本株ETF", "sectors": "半導体ETF"},
    "346A.T": {"stock_name": "半導体ETF", "sectors": "半導体ETF"},
}


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
    parser = argparse.ArgumentParser(description="Generate trading-value Top100/Top150 history with J-Quants CLI.")
    parser.add_argument("--date", help="Target date YYYY-MM-DD. Default: latest J-Quants trading day.")
    parser.add_argument("--top-n", type=int, default=100, help="Compatibility output size for trading_value_top100.parquet.")
    parser.add_argument("--history-top-n", type=int, default=150, help="History output size for each trading day.")
    parser.add_argument("--history-days", type=int, default=1, help="Backfill the latest N local trading days ending at --date.")
    parser.add_argument("--history-from", help="Backfill history from YYYY-MM-DD.")
    parser.add_argument("--history-to", help="Backfill history to YYYY-MM-DD. Default: --date.")
    parser.add_argument("--no-history", action="store_true", help="Do not write trading_value_top_history.parquet.")
    parser.add_argument("--skip-if-fresh", action="store_true", help="Skip generation when output already has target date.")
    parser.add_argument("--parquet-out", type=Path, default=PARQUET_OUT)
    parser.add_argument("--history-out", type=Path, default=HISTORY_OUT)
    parser.add_argument("--market-basket-out", type=Path, default=MARKET_BASKET_TURNOVER_PATH)
    parser.add_argument("--skip-market-baskets", action="store_true", help="Do not update market_basket_turnover.parquet.")
    parser.add_argument("--csv-master-out", type=Path, default=CSV_MASTER_OUT)
    parser.add_argument("--csv-baibai-out", type=Path, default=CSV_BAIBAI_OUT)
    return parser.parse_args()


def truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def default_skip_if_fresh() -> bool:
    if "TRADING_VALUE_TOP100_SKIP_IF_FRESH" in os.environ:
        return truthy(os.getenv("TRADING_VALUE_TOP100_SKIP_IF_FRESH"))
    return os.getenv("SKIP_GROK_GENERATION", "false").lower() != "true"


def latest_local_price_date() -> str:
    if not PRICES_1D_PATH.exists():
        raise FileNotFoundError(f"prices_max_1d.parquet not found: {PRICES_1D_PATH}")
    prices = pd.read_parquet(PRICES_1D_PATH, columns=["date"])
    if prices.empty:
        raise RuntimeError(f"prices_max_1d.parquet is empty: {PRICES_1D_PATH}")
    return pd.to_datetime(prices["date"], errors="coerce").dropna().max().strftime("%Y-%m-%d")


def latest_jquants_trading_day() -> str:
    for key in ["TARGET_TRADING_DATE", "LATEST_TRADING_DAY", "JQUANTS_TARGET_DATE"]:
        value = os.getenv(key)
        if value:
            return pd.Timestamp(value).strftime("%Y-%m-%d")
    try:
        return JQuantsFetcher().get_latest_trading_day()
    except Exception as exc:
        print(f"[WARN] failed to resolve latest J-Quants trading day: {exc}")
        return latest_local_price_date()


def restore_existing(path: Path, s3_name: str, label: str) -> bool:
    _, use_s3 = resolve_storage_mode()
    if not use_s3:
        return path.exists()
    cfg = load_s3_config()
    if not cfg.bucket:
        raise RuntimeError(f"Production S3 bucket is not configured for {label}")
    if not download_file(cfg, s3_name, path):
        raise RuntimeError(f"Production S3 restore failed for {label}: {s3_name}")
    return True


def download_existing_top100(path: Path) -> bool:
    return restore_existing(
        path,
        "trading_value_top100.parquet",
        "trading-value Top100",
    )


def download_existing_history(path: Path) -> bool:
    return restore_existing(
        path,
        path.name,
        "trading-value Top150 history",
    )


def parquet_has_date(path: Path, target_date: str, min_rows: int, *, downloader) -> bool:
    if not downloader(path):
        return False
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception as exc:
        print(f"[WARN] cannot read freshness for {path.name}: {exc}")
        return False
    if df.empty:
        return False
    dates = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return int(dates.eq(target_date).sum()) >= min_rows


def is_fresh(args: argparse.Namespace, target_date: str) -> bool:
    top100_fresh = parquet_has_date(args.parquet_out, target_date, args.top_n, downloader=download_existing_top100)
    if args.no_history:
        return top100_fresh
    history_fresh = parquet_has_date(
        args.history_out,
        target_date,
        args.history_top_n,
        downloader=download_existing_history,
    )
    return top100_fresh and history_fresh


def should_skip_when_fresh(args: argparse.Namespace, target_date: str, skip_if_fresh: bool) -> bool:
    if not skip_if_fresh:
        return False
    if args.history_from or max(args.history_days, 1) > 1:
        return False
    return is_fresh(args, target_date)


def local_trading_dates_until(target_date: str, days: int) -> list[str]:
    if days <= 1:
        return [target_date]
    if not PRICES_1D_PATH.exists():
        print(f"[WARN] prices_max_1d.parquet not found for history-days: {PRICES_1D_PATH}")
        return [target_date]
    prices = pd.read_parquet(PRICES_1D_PATH, columns=["date"])
    dates = pd.to_datetime(prices["date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").drop_duplicates()
    dates = sorted(date for date in dates.tolist() if date <= target_date)
    if not dates:
        return [target_date]
    selected = dates[-days:]
    if target_date not in selected and target_date <= dates[-1]:
        selected = sorted(set(selected + [target_date]))[-days:]
    return selected


def resolve_history_range(args: argparse.Namespace, target_date: str) -> tuple[str, str]:
    if args.history_from:
        return pd.Timestamp(args.history_from).strftime("%Y-%m-%d"), pd.Timestamp(args.history_to or target_date).strftime("%Y-%m-%d")
    dates = local_trading_dates_until(target_date, max(args.history_days, 1))
    return dates[0], dates[-1]


def _load_jquants_json(cmd: list[str], timeout: int) -> pd.DataFrame:
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"jquants eq daily failed rc={result.returncode}: {stderr}")
    if not result.stdout.strip():
        raise RuntimeError("jquants eq daily returned empty stdout")
    data = json.loads(result.stdout)
    return pd.DataFrame(data)


def run_jquants_calendar(start_date: str, end_date: str) -> list[str]:
    cmd = ["jquants", "-o", "json", "mkt", "calendar", "--from", start_date, "--to", end_date]
    try:
        calendar = _load_jquants_json(cmd, timeout=120)
    except Exception as exc:
        print(f"[WARN] failed to fetch J-Quants calendar, using weekdays: {exc}")
        return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start_date, end_date)]
    if calendar.empty or "Date" not in calendar.columns:
        return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start_date, end_date)]
    if "HolDiv" in calendar.columns:
        calendar = calendar[calendar["HolDiv"].astype(str).eq("1")].copy()
    dates = pd.to_datetime(calendar["Date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").tolist()
    return sorted(set(dates))


def run_jquants_range(start_date: str, end_date: str) -> pd.DataFrame:
    if start_date == end_date:
        return run_jquants_daily(start_date)
    trading_dates = run_jquants_calendar(start_date, end_date)
    if not trading_dates:
        raise RuntimeError(f"no trading dates found for {start_date}..{end_date}")
    frames = []
    for idx, trading_date in enumerate(trading_dates, 1):
        print(f"[INFO] fetching J-Quants daily {idx}/{len(trading_dates)}: {trading_date}", flush=True)
        frames.append(run_jquants_daily(trading_date))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_jquants_daily(date: str) -> pd.DataFrame:
    cmd = ["jquants", "-o", "json", "eq", "daily", "--date", date]
    return _load_jquants_json(cmd, timeout=120)


def build_top_by_trading_value(
    daily: pd.DataFrame,
    date: str,
    top_n: int,
    *,
    category: str,
    tag: str,
) -> pd.DataFrame:
    if daily.empty:
        raise RuntimeError(f"J-Quants daily is empty for {date}")
    required = {"Code", "O", "H", "L", "C", "Vo", "Va"}
    missing = sorted(required - set(daily.columns))
    if missing:
        raise ValueError(f"missing J-Quants daily columns: {missing}")

    df = daily.copy()
    df["code"] = df["Code"].map(normalize_code)
    for src, dst in [("O", "Open"), ("H", "High"), ("L", "Low"), ("C", "Close"), ("Vo", "Volume"), ("Va", "trading_value")]:
        df[dst] = pd.to_numeric(df[src], errors="coerce")
    df = df[df["trading_value"].fillna(0).gt(0)].copy()
    df = df.sort_values("trading_value", ascending=False).head(top_n).reset_index(drop=True)
    df["rank"] = df.index + 1
    df["ticker"] = df["code"] + ".T"
    df["date"] = date
    df["price_diff"] = df["Close"] - df["Open"]
    df["open_to_close_pct"] = (df["Close"] / df["Open"] - 1.0) * 100.0
    df["trading_value_billion"] = df["trading_value"] / 1_000_000_000.0
    df["rank_band"] = df["rank"].map(rank_band)
    df["categories"] = [[category] for _ in range(len(df))]
    df["tags"] = [[tag] for _ in range(len(df))]
    df["vol_ratio"] = None
    df["atr14_pct"] = None
    df["rsi14"] = None
    df["score"] = (top_n + 1 - df["rank"]).astype(float)
    df["key_signal"] = tag

    meta = load_meta()
    if not meta.empty:
        df = df.merge(meta, on=["ticker", "code"], how="left", suffixes=("", "_meta"))
    for col in ["stock_name", "market", "sectors", "series", "topixnewindexseries"]:
        if col not in df.columns:
            df[col] = None
    df["stock_name"] = df["stock_name"].fillna(df["code"])
    df = apply_known_etf_metadata(df)
    return df


def _missing_text(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    return series.isna() | text.isin({"", "None", "nan", "NaN", "UNKNOWN"})


def apply_known_etf_metadata(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ["stock_name", "market", "sectors", "series", "topixnewindexseries"]:
        if col not in out.columns:
            out[col] = None
    tickers = out["ticker"].astype(str)
    codes = out["code"].astype(str)
    for ticker, meta in ETF_META_BY_TICKER.items():
        code = ticker.removesuffix(".T")
        mask = tickers.eq(ticker) | codes.eq(code)
        if not bool(mask.any()):
            continue
        stock_name_text = out["stock_name"].astype(str).str.strip()
        missing_name = _missing_text(out["stock_name"]) | stock_name_text.eq(code) | stock_name_text.eq(ticker)
        out.loc[mask & missing_name, "stock_name"] = meta["stock_name"]
        out.loc[mask & _missing_text(out["market"]), "market"] = "ETF"
        out.loc[mask & _missing_text(out["sectors"]), "sectors"] = meta["sectors"]
        out.loc[mask & _missing_text(out["series"]), "series"] = "ETF"
        out.loc[mask & _missing_text(out["topixnewindexseries"]), "topixnewindexseries"] = "ETF"
    return out


def rank_band(rank: object) -> str:
    value = pd.to_numeric(rank, errors="coerce")
    if pd.isna(value):
        return "unknown"
    rank_int = int(value)
    if rank_int <= 30:
        return "top1_30"
    if rank_int <= 100:
        return "top31_100"
    return "top101_150"


def normalize_top100_from_history(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    out = df.sort_values("rank", kind="mergesort").head(top_n).copy()
    out["categories"] = [["TOP100"] for _ in range(len(out))]
    out["tags"] = [["trading_value_top100"] for _ in range(len(out))]
    out["score"] = (top_n + 1 - pd.to_numeric(out["rank"], errors="coerce")).astype(float)
    out["key_signal"] = "trading_value_top100"
    return out


def build_history(daily: pd.DataFrame, history_top_n: int) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    if "Date" not in daily.columns:
        raise ValueError("J-Quants daily response is missing Date")
    df = daily.copy()
    df["_date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["_date"].notna()].copy()
    frames = []
    for date, group in df.groupby("_date", sort=True):
        frames.append(
            build_top_by_trading_value(
                group.drop(columns=["_date"]),
                str(date),
                history_top_n,
                category="TOP150",
                tag="trading_value_top150",
            )
        )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def save_history(df: pd.DataFrame, path: Path) -> None:
    if df.empty:
        print("[WARN] no Top150 history rows to save")
        return
    existing = pd.DataFrame()
    if download_existing_history(path):
        try:
            existing = pd.read_parquet(path)
        except Exception as exc:
            print(f"[WARN] cannot read existing history, replacing it: {exc}")
    combined = pd.concat([existing, df], ignore_index=True) if not existing.empty else df.copy()
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    combined["ticker"] = combined["ticker"].astype(str)
    combined["rank"] = pd.to_numeric(combined["rank"], errors="coerce")
    combined["rank_band"] = combined["rank"].map(rank_band)
    combined = combined.dropna(subset=["date", "ticker", "rank"]).copy()
    combined = combined.drop_duplicates(["date", "ticker"], keep="last")
    combined = combined.sort_values(["date", "rank"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path, engine="pyarrow", index=False)
    new_dates = sorted(df["date"].astype(str).unique().tolist())
    print(f"[OK] saved Top150 history: {path} rows={len(combined)} added_dates={new_dates[0]}..{new_dates[-1]}")


def build_top100(daily: pd.DataFrame, date: str, top_n: int) -> pd.DataFrame:
    return build_top_by_trading_value(
        daily,
        date,
        top_n,
        category="TOP100",
        tag="trading_value_top100",
    )


def normalize_code(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) == 5 and text.endswith("0"):
        text = text[:4]
    return text


def load_meta() -> pd.DataFrame:
    if not META_JQUANTS_PATH.exists():
        return pd.DataFrame(columns=["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries"])
    meta = pd.read_parquet(META_JQUANTS_PATH)
    keep = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries"]
    for col in keep:
        if col not in meta.columns:
            meta[col] = None
    meta = meta[keep].copy()
    meta["code"] = meta["code"].map(normalize_code)
    return meta.drop_duplicates("code", keep="first")


def save_outputs(df: pd.DataFrame, args: argparse.Namespace) -> None:
    args.parquet_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.parquet_out, engine="pyarrow", index=False)
    print(f"[OK] saved parquet: {args.parquet_out} rows={len(df)}")

    args.csv_master_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.csv_master_out, index=False)
    print(f"[OK] saved csv: {args.csv_master_out}")

    baibai_cols = {
        "rank": "No.",
        "code": "コード",
        "stock_name": "銘柄",
        "market": "市場",
        "Close": "現在値",
        "Volume": "出来高",
        "trading_value": "売買代金",
        "Open": "始値",
        "High": "高値",
        "Low": "安値",
        "date": "日付",
    }
    args.csv_baibai_out.parent.mkdir(parents=True, exist_ok=True)
    df[list(baibai_cols)].rename(columns=baibai_cols).to_csv(args.csv_baibai_out, index=False)
    print(f"[OK] saved baibai-compatible csv: {args.csv_baibai_out}")


def main() -> int:
    args = parse_args()
    app_env, use_s3 = resolve_storage_mode()
    date = args.date or latest_jquants_trading_day()
    skip_if_fresh = args.skip_if_fresh or default_skip_if_fresh()
    print("=== Generate trading-value Top100/Top150 history ===")
    print(f"date : {date}")
    print(f"top_n: {args.top_n}")
    print(f"history_top_n: {args.history_top_n}")
    print(f"mode : {'skip-if-fresh' if skip_if_fresh else 'force'}")
    print(f"storage: {'s3' if use_s3 else 'local'} ({app_env})")
    if should_skip_when_fresh(args, date, skip_if_fresh):
        print(f"[OK] existing Top100/Top150 history is fresh: {date}")
        return 0

    if args.no_history:
        daily = run_jquants_daily(date)
        top100 = build_top100(daily, date, args.top_n)
    else:
        history_from, history_to = resolve_history_range(args, date)
        print(f"history_range: {history_from}..{history_to}")
        daily = run_jquants_range(history_from, history_to)
        history = build_history(daily, max(args.history_top_n, args.top_n))
        if history.empty:
            raise RuntimeError(f"Top150 history is empty for {history_from}..{history_to}")
        save_history(history[history["rank"].le(args.history_top_n)].copy(), args.history_out)
        target_history = history[history["date"].astype(str).eq(date)].copy()
        if target_history.empty:
            raise RuntimeError(f"target date not found in J-Quants response: {date}")
        top100 = normalize_top100_from_history(target_history, args.top_n)

    if not args.skip_market_baskets:
        try:
            save_market_basket_turnover_from_daily(daily, args.market_basket_out)
        except Exception as exc:
            print(f"[WARN] market basket turnover generation failed: {exc}")

    save_outputs(top100, args)
    print(top100[["rank", "ticker", "stock_name", "trading_value_billion"]].head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
