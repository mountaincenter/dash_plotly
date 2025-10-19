#!/usr/bin/env python3
"""
Trading Calendar API Test
取引カレンダーAPIの動作確認と直近営業日の取得テスト
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient


def test_trading_calendar() -> int:
    """取引カレンダーAPIのテスト"""
    print("=" * 60)
    print("Trading Calendar API Test")
    print("=" * 60)
    print()

    # 現在時刻
    now = datetime.now()
    print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Weekday: {now.strftime('%A')} ({now.weekday()})")
    print()

    # J-Quantsクライアント初期化
    print("[STEP 1] Initializing J-Quants client...")
    try:
        client = JQuantsClient()
        print(f"  ✓ Plan: {client.plan}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # 取引カレンダー取得
    print("\n[STEP 2] Fetching trading calendar...")
    try:
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=30)

        params = {
            "from": str(from_date),
            "to": str(to_date)
        }

        print(f"  Request params: from={from_date}, to={to_date}")

        response = client.request("/markets/trading_calendar", params=params)

        if not response or "trading_calendar" not in response:
            raise RuntimeError("Failed to fetch trading calendar")

        calendar = pd.DataFrame(response["trading_calendar"])
        print(f"  ✓ Received {len(calendar)} days")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # 全データを表示
    print("\n[STEP 3] Displaying all calendar data...")
    print()
    print("Date        | HolidayDivision | Day of Week")
    print("-" * 50)
    for _, row in calendar.iterrows():
        date_obj = pd.to_datetime(row["Date"])
        day_of_week = date_obj.strftime("%A")
        print(f"{row['Date']} | {row['HolidayDivision']:^15} | {day_of_week}")
    print()

    # HolidayDivision の説明
    print("HolidayDivision values:")
    print("  0: 非営業日 (Non-trading day)")
    print("  1: 営業日 (Trading day)")
    print("  2: 半日立会 (Half-day trading)")
    print("  3: 祝日取引のある非営業日 (Holiday trading for derivatives)")
    print()

    # 営業日のみフィルタ
    print("[STEP 4] Filtering trading days (HolidayDivision == 1)...")
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    print(f"  ✓ Found {len(trading_days)} trading days")
    print()

    if trading_days.empty:
        print("  ✗ No trading days found")
        return 1

    # Date列をdatetimeに変換
    trading_days["Date"] = pd.to_datetime(trading_days["Date"])

    print("Trading days (営業日のみ):")
    for _, row in trading_days.iterrows():
        day_of_week = row["Date"].strftime("%A")
        print(f"  {row['Date'].strftime('%Y-%m-%d')} ({day_of_week})")
    print()

    # 今日より前の営業日のみ
    print("[STEP 5] Filtering past trading days (Date < today)...")
    today = pd.Timestamp(datetime.now().date())
    print(f"  Today: {today.strftime('%Y-%m-%d')}")

    past_trading_days = trading_days[trading_days["Date"] < today].copy()
    print(f"  ✓ Found {len(past_trading_days)} past trading days")
    print()

    if past_trading_days.empty:
        print("  ✗ No past trading days found")
        return 1

    print("Past trading days:")
    for _, row in past_trading_days.iterrows():
        day_of_week = row["Date"].strftime("%A")
        print(f"  {row['Date'].strftime('%Y-%m-%d')} ({day_of_week})")
    print()

    # ソートして最新を取得
    print("[STEP 6] Getting latest trading day...")
    past_trading_days = past_trading_days.sort_values("Date", ascending=False)
    latest_trading_day = past_trading_days.iloc[0]["Date"]

    print(f"  ✓ Latest trading day: {latest_trading_day.strftime('%Y-%m-%d')} ({latest_trading_day.strftime('%A')})")
    print()

    # サマリー
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total days in calendar: {len(calendar)}")
    print(f"Trading days (HolidayDivision=0): {len(trading_days)}")
    print(f"Past trading days (< today): {len(past_trading_days)}")
    print(f"Latest trading day: {latest_trading_day.strftime('%Y-%m-%d')} ({latest_trading_day.strftime('%A')})")
    print("=" * 60)

    print("\n✅ Trading calendar test completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(test_trading_calendar())
