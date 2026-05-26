#!/usr/bin/env python3
"""
check_today_trading.py
07:00 JST pipeline用の当日営業日判定。

check_trading_day.py は 16:00-翌3:00 の終値更新窓を判定するため、
朝のCME/寄前更新では使わない。
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_client import JQuantsClient


def check_today_is_trading() -> tuple[bool, str]:
    print("=" * 60)
    print("Today Trading Check (for 07:00 run)")
    print("=" * 60)

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    now_jst = now_utc + timedelta(hours=9)
    today_str = now_jst.strftime("%Y-%m-%d")

    print(f"Current time (UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current time (JST): {now_jst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Today's date (JST): {today_str}")

    try:
        client = JQuantsClient()
        print(f"J-Quants connected: {client.plan}")
    except Exception as exc:
        print(f"ERROR: Failed to connect to J-Quants: {exc}")
        return False, today_str

    try:
        response = client.request(
            "/markets/calendar",
            params={
                "from": today_str.replace("-", ""),
                "to": today_str.replace("-", ""),
            },
        )
        calendar_data = response.get("data", []) if response else []
    except Exception as exc:
        print(f"ERROR: Failed to check trading calendar: {exc}")
        return False, today_str

    today_record = next((row for row in calendar_data if row.get("Date") == today_str), None)
    if not today_record:
        print(f"ERROR: Today's date not found in calendar: {today_str}")
        return False, today_str

    hol_div = str(today_record.get("HolDiv", ""))
    print("\nToday's calendar info:")
    print(f"  Date: {today_record.get('Date')}")
    print(f"  HolDiv: {hol_div}")

    if hol_div == "1":
        print("✅ Today is a TRADING day")
        return True, today_str

    print("❌ Today is NOT a trading day")
    return False, today_str


def main() -> int:
    is_trading, today = check_today_is_trading()

    print("\n" + "=" * 60)
    print("Result")
    print("=" * 60)
    print(f"IS_TODAY_TRADING: {str(is_trading).lower()}")
    print(f"TODAY_DATE: {today}")
    print("=" * 60)

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"is_today_trading={str(is_trading).lower()}\n")
            f.write(f"today_date={today}\n")
        print(f"\n✅ Wrote to GITHUB_OUTPUT: {github_output}")

    return 0 if is_trading else 1


if __name__ == "__main__":
    raise SystemExit(main())
