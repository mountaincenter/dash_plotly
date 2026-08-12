#!/usr/bin/env python3
"""
generate_grok_prices_max_1d.py
派生J-Quants台帳（なければ読取専用正本）の銘柄に対して日足データを取得
grok_prices_max_1d.parquet を生成
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers
from common_cfg.paths import PARQUET_DIR

ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
DERIVED_LEDGER_PATH = (
    PARQUET_DIR / "backtest" / "grok_jquants_backtest_ledger.parquet"
)
GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
OUTPUT_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"
JQUANTS_WATCH_PRICES_PATH = PARQUET_DIR / "prices_max_1d.parquet"
MIN_INFERENCE_HISTORY_ROWS = 35
PRICE_COLUMNS = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]


def load_all_tickers() -> list[str]:
    """Derived ledger/canonical archive + current selection tickers."""
    tickers = set()

    history_path = DERIVED_LEDGER_PATH if DERIVED_LEDGER_PATH.exists() else ARCHIVE_PATH
    if history_path.exists():
        print(f"[INFO] Loading backtest history: {history_path}")
        df = pd.read_parquet(history_path)
        archive_tickers = df["ticker"].unique().tolist()
        tickers.update(archive_tickers)
        print(f"  ✓ History: {len(archive_tickers)} tickers")
    else:
        print(f"[WARN] Backtest history not found: {history_path}")

    # 現在のgrok_trending から取得（新規選定銘柄を含める）
    if GROK_TRENDING_PATH.exists():
        print(f"[INFO] Loading current grok_trending: {GROK_TRENDING_PATH}")
        df = pd.read_parquet(GROK_TRENDING_PATH)
        current_tickers = df["ticker"].unique().tolist()
        new_tickers = set(current_tickers) - tickers
        tickers.update(current_tickers)
        print(f"  ✓ Current: {len(current_tickers)} tickers ({len(new_tickers)} new)")
    else:
        print(f"[WARN] grok_trending not found: {GROK_TRENDING_PATH}")

    if not tickers:
        raise FileNotFoundError("No tickers found in archive or grok_trending")

    print(f"  ✓ Total unique tickers: {len(tickers)}")
    return list(tickers)


def normalize_price_source(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    """Normalize one adjusted daily source without hiding missing columns."""
    missing = sorted(set(PRICE_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing daily price columns: {missing}")
    out = frame[PRICE_COLUMNS].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    out["ticker"] = out["ticker"].astype(str).str.strip()
    for column in ["Open", "High", "Low", "Close", "Volume"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.dropna(subset=["date", "ticker", "Close"])
    out = out[out["ticker"].ne("")]
    return out.reset_index(drop=True)


def merge_price_sources(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    jquants_watch: pd.DataFrame,
) -> pd.DataFrame:
    """Merge daily history, preferring fetched yfinance then existing then J-Quants."""
    frames: list[pd.DataFrame] = []
    for priority, (label, frame) in enumerate(
        [
            ("J-Quants watch daily", jquants_watch),
            ("existing Grok history", existing),
            ("fresh yfinance history", fetched),
        ]
    ):
        if frame is None or frame.empty:
            continue
        normalized = normalize_price_source(frame, label)
        normalized["_source_priority"] = priority
        frames.append(normalized)
    if not frames:
        raise ValueError("No usable Grok daily price source is available")
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["ticker", "date", "_source_priority"])
    combined = combined.drop_duplicates(["ticker", "date"], keep="last")
    return combined.drop(columns=["_source_priority"]).reset_index(drop=True)


def validate_current_price_coverage(
    prices: pd.DataFrame,
    trending: pd.DataFrame,
    *,
    minimum_rows: int = MIN_INFERENCE_HISTORY_ROWS,
) -> None:
    """Require enough point-in-time history for every current recommendation."""
    required = {"ticker", "date"}
    missing = sorted(required - set(trending.columns))
    if missing:
        raise ValueError(f"grok_trending is missing coverage columns: {missing}")
    targets = trending[["ticker", "date"]].copy()
    targets["ticker"] = targets["ticker"].astype(str).str.strip()
    targets["date"] = pd.to_datetime(targets["date"], errors="raise").dt.normalize()
    if targets["ticker"].duplicated().any():
        raise ValueError("grok_trending contains duplicate tickers")

    available = normalize_price_source(prices, "merged Grok history")
    failures: list[dict[str, object]] = []
    for row in targets.itertuples(index=False):
        count = int(
            (
                available["ticker"].eq(row.ticker)
                & available["date"].lt(row.date)
            ).sum()
        )
        if count < minimum_rows:
            failures.append({"ticker": row.ticker, "rows": count})
    if failures:
        raise ValueError(
            "Current Grok tickers lack point-in-time daily history: "
            f"minimum={minimum_rows}, failures={failures}"
        )


def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        frame.to_parquet(temporary, engine="pyarrow", index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    """メイン処理"""
    print("=" * 60)
    print("Generate grok_prices_max_1d.parquet")
    print("=" * 60)

    # 1. archive + 現在のgrok_trending銘柄を取得
    tickers = load_all_tickers()

    existing = (
        pd.read_parquet(OUTPUT_PATH)
        if OUTPUT_PATH.exists() and OUTPUT_PATH.stat().st_size > 0
        else pd.DataFrame(columns=PRICE_COLUMNS)
    )
    jquants_watch = (
        pd.read_parquet(JQUANTS_WATCH_PRICES_PATH)
        if JQUANTS_WATCH_PRICES_PATH.exists()
        and JQUANTS_WATCH_PRICES_PATH.stat().st_size > 0
        else pd.DataFrame(columns=PRICE_COLUMNS)
    )

    # 2. yfinanceで日足データを取得。新規/IPOはJ-Quants watch日足で補完する。
    print(f"\n[INFO] Fetching daily prices for {len(tickers)} tickers...")
    try:
        fetched = fetch_prices_for_tickers(
            tickers=tickers,
            period="max",
            interval="1d",
            fallback_period=None
        )
    except Exception as error:
        print(f"  [WARN] fresh yfinance history unavailable: {error}")
        fetched = pd.DataFrame(columns=PRICE_COLUMNS)

    try:
        combined = merge_price_sources(existing, fetched, jquants_watch)
        trending = pd.read_parquet(GROK_TRENDING_PATH)
        validate_current_price_coverage(combined, trending)
        write_parquet_atomic(combined, OUTPUT_PATH)
        print(f"\n  ✓ Saved: {OUTPUT_PATH}")
        print(f"    - Rows: {len(combined):,}")
        print(f"    - Tickers: {combined['ticker'].nunique()}")
        print(f"    - Date range: {combined['date'].min()} ~ {combined['date'].max()}")
        print(
            f"    - Current Grok coverage: all tickers >= "
            f"{MIN_INFERENCE_HISTORY_ROWS} pre-target rows"
        )

        return True
    except Exception as error:
        print(f"  ✗ Failed: {error}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
