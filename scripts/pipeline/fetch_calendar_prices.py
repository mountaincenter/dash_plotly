#!/usr/bin/env python3
"""
fetch_calendar_prices.py
TOPIX 500 (Core30+Large70+Mid400) の日足価格データ取得

Semicon / Market Flowで使用するAdjOHLCV・売買代金を更新する。

動作モード:
  - 通常（日次）: 差分更新のみ
  - --full またはschema更新時: 全量リフレッシュ（分割調整の遡及反映）

実行方法:
    python3 scripts/pipeline/fetch_calendar_prices.py [--full]
"""
from __future__ import annotations

import io
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

TOPIX500_OUTPUT = PARQUET_DIR / "prices_topix500_oc.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"

TOPIX500_CLASSES = ["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"]
BACKTEST_START = "2022-04-01"
TOPIX500_PRICE_COLUMNS = ["Date", "Code", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo", "Va"]


def topix500_needs_schema_refresh() -> bool:
    """既存TOPIX500価格がOHLCV拡張前なら全量更新する。"""
    if not TOPIX500_OUTPUT.exists():
        return False
    existing_cols = set(pd.read_parquet(TOPIX500_OUTPUT).columns)
    return not set(TOPIX500_PRICE_COLUMNS).issubset(existing_cols)


def subscription_start() -> str:
    """J-Quants Standardの取得可能開始日（today - 10年 + 1日）"""
    today = date.today()
    start = today.replace(year=today.year - 10) + timedelta(days=1)
    return start.isoformat()


def get_topix500_codes() -> set[str]:
    meta = pd.read_parquet(META_PATH)
    codes_4d = meta[meta["topixnewindexseries"].isin(TOPIX500_CLASSES)]["code"].tolist()
    return {c + "0" for c in codes_4d}


def jquants_fetch(args: list[str]) -> str:
    """jquants CLI実行、エラー時は空文字返却"""
    result = subprocess.run(
        ["jquants", "--output", "csv"] + args,
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def fetch_topix500_differential(codes: set[str]) -> tuple[pd.DataFrame, int]:
    """TOPIX 500 差分更新（--date 一括取得）"""
    if TOPIX500_OUTPUT.exists():
        existing = pd.read_parquet(TOPIX500_OUTPUT)
        existing["Date"] = pd.to_datetime(existing["Date"])
        last_date = existing["Date"].max().date()
    else:
        existing = pd.DataFrame(columns=TOPIX500_PRICE_COLUMNS)
        last_date = date.fromisoformat(BACKTEST_START) - timedelta(days=1)

    today = date.today()
    fetch_date = last_date + timedelta(days=1)
    new_frames = []
    fetched = 0

    while fetch_date <= today:
        if fetch_date.weekday() >= 5:
            fetch_date += timedelta(days=1)
            continue

        stdout = jquants_fetch(["eq", "daily", "--date", fetch_date.isoformat()])
        if stdout.strip():
            df = pd.read_csv(io.StringIO(stdout))
            df["Code"] = df["Code"].astype(str)
            filtered = df[df["Code"].isin(codes)][TOPIX500_PRICE_COLUMNS].copy()
            if not filtered.empty:
                new_frames.append(filtered)
                fetched += 1
        time.sleep(0.3)
        fetch_date += timedelta(days=1)

    if not new_frames:
        return existing, 0

    new_df = pd.concat(new_frames, ignore_index=True)
    new_df["Date"] = pd.to_datetime(new_df["Date"])
    new_df["Code"] = new_df["Code"].astype(str)

    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Date", "Code"], keep="last")
    combined = combined.sort_values(["Code", "Date"]).reset_index(drop=True)
    return combined, fetched


def fetch_topix500_full(codes: set[str]) -> pd.DataFrame:
    """TOPIX 500 全量リフレッシュ（コード別取得）"""
    from_date = max(BACKTEST_START, subscription_start())
    all_frames = []
    codes_list = sorted(codes)
    total = len(codes_list)

    for i, code in enumerate(codes_list, 1):
        stdout = jquants_fetch(
            ["eq", "daily", "--code", code, "--from", from_date]
        )
        if stdout.strip() and "Date" in stdout:
            df = pd.read_csv(io.StringIO(stdout), usecols=TOPIX500_PRICE_COLUMNS)
            df["Code"] = df["Code"].astype(str)
            if not df.empty:
                all_frames.append(df)

        if i % 50 == 0:
            print(f"    {i}/{total} codes fetched")
        time.sleep(0.3)

    if not all_frames:
        return pd.DataFrame(columns=TOPIX500_PRICE_COLUMNS)

    combined = pd.concat(all_frames, ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["Date"])
    combined = combined.sort_values(["Code", "Date"]).reset_index(drop=True)
    return combined


def main() -> int:
    force_full = "--full" in sys.argv
    schema_refresh = topix500_needs_schema_refresh()
    full_mode = force_full or schema_refresh

    print("=" * 60)
    print("Fetch TOPIX 500 Prices")
    mode_reason = "--full flag" if force_full else "schema refresh" if schema_refresh else "differential"
    print(f"  Mode: {'FULL REFRESH' if full_mode else 'DIFFERENTIAL'} ({mode_reason})")
    print("=" * 60)

    # --- TOPIX 500 ---
    print("\n[1/1] TOPIX 500 AdjOHLCV+Va...")
    codes = get_topix500_codes()
    print(f"  Codes: {len(codes)}")

    if full_mode:
        print("  Full refresh (split adjustment sync)...")
        df_topix = fetch_topix500_full(codes)
        print(f"  Total: {len(df_topix):,} rows, {df_topix['Code'].nunique()} codes")
    else:
        if TOPIX500_OUTPUT.exists():
            existing = pd.read_parquet(TOPIX500_OUTPUT)
            existing["Date"] = pd.to_datetime(existing["Date"])
            print(f"  Existing: {len(existing):,} rows, last={existing['Date'].max().date()}")

        df_topix, n_dates = fetch_topix500_differential(codes)
        print(f"  New dates: {n_dates}")
        print(f"  Total: {len(df_topix):,} rows, {df_topix['Code'].nunique()} codes")

    if not df_topix.empty:
        print(f"  Range: {df_topix['Date'].min().date()} → {df_topix['Date'].max().date()}")
        df_topix.to_parquet(TOPIX500_OUTPUT, index=False)
        print(f"  Saved: {TOPIX500_OUTPUT.name}")
    else:
        print("  ⚠️ WARNING: TOPIX500 data is empty (jquants API failure or no existing data)")

    print("\n[OK] Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
