#!/usr/bin/env python3
"""
fetch_calendar_prices.py
Calendar Trades用の価格データ取得

1. 1306.T ETF: AdjO + AdjC（四半期末戦略用）
2. TOPIX 500 (Core30+Large70+Mid400): AdjO + AdjC（SQ-4戦略用）

動作モード:
  - 通常（日次）: 差分更新のみ
  - SQ-4前日 or --full: 全量リフレッシュ（分割調整の遡及反映）

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

ETF_OUTPUT = PARQUET_DIR / "etf_1306_prices.parquet"
TOPIX500_OUTPUT = PARQUET_DIR / "prices_topix500_oc.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"

ETF_CODE = "13060"
TOPIX500_CLASSES = ["TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"]
BACKTEST_START = "2022-04-01"
TOPIX500_PRICE_COLUMNS = ["Date", "Code", "AdjO", "AdjH", "AdjL", "AdjC", "AdjV"]


def is_pre_sq4() -> bool:
    """明日がSQ-4エントリー日かどうか"""
    if not CALENDAR_PATH.exists():
        return False
    cal = pd.read_parquet(CALENDAR_PATH)
    cal["date"] = pd.to_datetime(cal["date"])
    tomorrow = pd.Timestamp(date.today() + timedelta(days=1))
    return bool(cal[cal["date"] == tomorrow]["sq4_entry"].any())


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


def fetch_1306_differential() -> pd.DataFrame:
    """1306 差分更新"""
    if ETF_OUTPUT.exists():
        existing = pd.read_parquet(ETF_OUTPUT)
        last_date = pd.to_datetime(existing["date"]).max().date()
        from_date = (last_date + timedelta(days=1)).isoformat()
    else:
        from_date = BACKTEST_START

    stdout = jquants_fetch(["eq", "daily", "--code", ETF_CODE, "--from", from_date])
    if not stdout.strip():
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO(stdout))
    df = df[["Date", "AdjO", "AdjC"]].rename(
        columns={"Date": "date", "AdjO": "Open", "AdjC": "Close"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def fetch_1306_full() -> pd.DataFrame:
    """1306 全量取得"""
    from_date = max(BACKTEST_START, subscription_start())
    stdout = jquants_fetch(["eq", "daily", "--code", ETF_CODE, "--from", from_date])
    if not stdout.strip():
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO(stdout))
    df = df[["Date", "AdjO", "AdjC"]].rename(
        columns={"Date": "date", "AdjO": "Open", "AdjC": "Close"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date").reset_index(drop=True)
    return df


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
    pre_sq4 = is_pre_sq4()
    schema_refresh = topix500_needs_schema_refresh()
    full_mode = force_full or pre_sq4 or schema_refresh

    print("=" * 60)
    print("Fetch Calendar Prices (1306 + TOPIX 500)")
    mode_reason = "--full flag" if force_full else "SQ-4前日" if pre_sq4 else "schema refresh" if schema_refresh else "differential"
    print(f"  Mode: {'FULL REFRESH' if full_mode else 'DIFFERENTIAL'} ({mode_reason})")
    print("=" * 60)

    # --- 1306 ETF ---
    print("\n[1/2] 1306.T ETF...")
    if full_mode:
        df_1306 = fetch_1306_full()
        print(f"  Full fetch: {len(df_1306)} rows")
    else:
        df_new = fetch_1306_differential()
        if ETF_OUTPUT.exists() and not df_new.empty:
            existing = pd.read_parquet(ETF_OUTPUT)
            df_1306 = pd.concat([existing, df_new], ignore_index=True)
            df_1306["date"] = pd.to_datetime(df_1306["date"]).dt.date
            df_1306 = df_1306.drop_duplicates(subset=["date"], keep="last")
            df_1306 = df_1306.sort_values("date").reset_index(drop=True)
            print(f"  Appended {len(df_new)} new rows → total {len(df_1306)}")
        elif ETF_OUTPUT.exists():
            df_1306 = pd.read_parquet(ETF_OUTPUT)
            print(f"  No new data. Existing: {len(df_1306)} rows")
        else:
            df_1306 = fetch_1306_full()
            print(f"  Initial fetch: {len(df_1306)} rows")

    if not df_1306.empty:
        df_1306.to_parquet(ETF_OUTPUT, index=False)
        print(f"  Saved: {ETF_OUTPUT.name}")
    else:
        print("  ⚠️ WARNING: 1306 data is empty (jquants API failure or no existing data)")

    # --- TOPIX 500 ---
    print("\n[2/2] TOPIX 500 AdjO+AdjC...")
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
