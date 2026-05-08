#!/usr/bin/env python3
"""
check_last_trading_day_of_month.py
今日が今月の最終営業日かどうかを判定。

Exit codes:
  0: 今日は月の最終営業日 → 月次処理を実行すべき
  1: 今日は月の最終営業日ではない → スキップ
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


def check() -> bool:
    now_jst = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
    today = now_jst.date()
    today_str = today.strftime("%Y-%m-%d")
    today_month = today.month

    print(f"Today (JST): {today_str} (month {today_month})")

    try:
        client = JQuantsClient()
    except Exception as e:
        print(f"ERROR: J-Quants connection failed: {e}")
        return False

    end_date = today + timedelta(days=10)
    try:
        cal_resp = client.request(
            "/markets/calendar",
            params={"from": today_str, "to": end_date.strftime("%Y-%m-%d")},
        )
        cal_data = cal_resp.get("data", [])
    except Exception as e:
        print(f"ERROR: Calendar fetch failed: {e}")
        return False

    trading_days = sorted(
        d["Date"] for d in cal_data
        if d.get("Date", "") > today_str and d.get("HolDiv") == "1"
    )

    if not trading_days:
        print("No trading days found after today")
        return True

    next_trading = datetime.strptime(trading_days[0], "%Y-%m-%d").date()
    print(f"Next trading day: {trading_days[0]} (month {next_trading.month})")

    is_last = next_trading.month != today_month
    if is_last:
        print(f"Today is the LAST trading day of month {today_month}")
    else:
        print(f"Not the last trading day of month")

    return is_last


def main() -> int:
    result = check()
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"is_last_trading_day_of_month={str(result).lower()}\n")
    return 0 if result else 1


if __name__ == "__main__":
    raise SystemExit(main())
