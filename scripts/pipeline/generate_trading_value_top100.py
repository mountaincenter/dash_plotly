#!/usr/bin/env python3
"""
Generate daily trading-value Top100 from J-Quants eq daily.

Outputs:
  - data/parquet/trading_value_top100.parquet
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
from scripts.lib.jquants_fetcher import JQuantsFetcher

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
PARQUET_OUT = PARQUET_DIR / "trading_value_top100.parquet"
CSV_MASTER_OUT = ROOT / "data" / "jquants_csv" / "master" / "trading_value_top100.csv"
CSV_BAIBAI_OUT = ROOT / "data" / "csv" / "baibai_generated.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate trading-value Top100 with J-Quants CLI.")
    parser.add_argument("--date", help="Target date YYYY-MM-DD. Default: latest J-Quants trading day.")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--skip-if-fresh", action="store_true", help="Skip generation when output already has target date.")
    parser.add_argument("--parquet-out", type=Path, default=PARQUET_OUT)
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


def download_existing_top100(path: Path) -> bool:
    if path.exists():
        return True
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            return bool(download_file(cfg, "trading_value_top100.parquet", path))
    except Exception as exc:
        print(f"[WARN] S3 fallback for trading_value_top100 failed: {exc}")
    return path.exists()


def is_fresh(path: Path, target_date: str, top_n: int) -> bool:
    if not download_existing_top100(path):
        return False
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception as exc:
        print(f"[WARN] cannot read existing top100 freshness: {exc}")
        return False
    if df.empty:
        return False
    latest = pd.to_datetime(df["date"], errors="coerce").dropna().max()
    return bool(pd.notna(latest) and latest.strftime("%Y-%m-%d") == target_date and len(df) >= top_n)


def run_jquants_daily(date: str) -> pd.DataFrame:
    cmd = ["jquants", "-o", "json", "eq", "daily", "--date", date]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=120, check=False)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"jquants eq daily failed rc={result.returncode}: {stderr}")
    if not result.stdout.strip():
        raise RuntimeError("jquants eq daily returned empty stdout")
    data = json.loads(result.stdout)
    return pd.DataFrame(data)


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


def build_top100(daily: pd.DataFrame, date: str, top_n: int) -> pd.DataFrame:
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
    df["categories"] = [["TOP100"] for _ in range(len(df))]
    df["tags"] = [["trading_value_top100"] for _ in range(len(df))]
    df["vol_ratio"] = None
    df["atr14_pct"] = None
    df["rsi14"] = None
    df["score"] = (top_n + 1 - df["rank"]).astype(float)
    df["key_signal"] = "trading_value_top100"

    meta = load_meta()
    if not meta.empty:
        df = df.merge(meta, on=["ticker", "code"], how="left", suffixes=("", "_meta"))
    for col in ["stock_name", "market", "sectors", "series", "topixnewindexseries"]:
        if col not in df.columns:
            df[col] = None
    df["stock_name"] = df["stock_name"].fillna(df["code"])
    return df


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
    date = args.date or latest_jquants_trading_day()
    skip_if_fresh = args.skip_if_fresh or default_skip_if_fresh()
    print("=== Generate trading-value Top100 ===")
    print(f"date : {date}")
    print(f"top_n: {args.top_n}")
    print(f"mode : {'skip-if-fresh' if skip_if_fresh else 'force'}")
    if skip_if_fresh and is_fresh(args.parquet_out, date, args.top_n):
        print(f"[OK] existing Top100 is fresh: {args.parquet_out}")
        return 0
    daily = run_jquants_daily(date)
    top100 = build_top100(daily, date, args.top_n)
    save_outputs(top100, args)
    print(top100[["rank", "ticker", "stock_name", "trading_value_billion"]].head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
