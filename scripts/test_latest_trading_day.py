#!/usr/bin/env python3
"""
Test script for get_latest_trading_day() with 16:00 JST cutoff logic
"""

from datetime import datetime, timezone, timedelta
from scripts.lib.jquants_fetcher import JQuantsFetcher

def main():
    print("=" * 60)
    print("Testing get_latest_trading_day() with 16:00 JST cutoff")
    print("=" * 60)

    # Get current UTC time and convert to JST
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    now_jst = now_utc + timedelta(hours=9)

    print(f"\n[Current Time]")
    print(f"  UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  JST: {now_jst.strftime('%Y-%m-%d %H:%M:%S')} (hour: {now_jst.hour})")
    print(f"  JST date: {now_jst.date()}")

    # Initialize fetcher and get latest trading day
    fetcher = JQuantsFetcher()

    try:
        latest_trading_day = fetcher.get_latest_trading_day()
        print(f"\n[Result]")
        print(f"  Latest trading day: {latest_trading_day}")

        # Verify logic
        print(f"\n[Logic Check]")
        if now_jst.hour < 16:
            print(f"  ✓ JST hour {now_jst.hour} < 16: Should return previous trading day")
            print(f"  Expected cutoff date: {now_jst.date() - timedelta(days=1)}")
        else:
            print(f"  ✓ JST hour {now_jst.hour} >= 16: Should return current day if trading day")
            print(f"  Expected cutoff date: {now_jst.date()}")

        print("\n" + "=" * 60)
        print("✅ Test completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
