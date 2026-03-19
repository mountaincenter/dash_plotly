#!/usr/bin/env python3
"""
check_last_trading_day_of_week.py
今日が今週の最終営業日かどうかを判定

J-Quantsカレンダーで翌営業日を取得し、翌営業日が来週（月曜以降）なら
今日が週の最終営業日と判定する。

Exit codes:
  0: 今日は週の最終営業日 → 週次処理を実行すべき
  1: 今日は週の最終営業日ではない → スキップ
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_client import JQuantsClient


def check() -> bool:
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    now_jst = now_utc + timedelta(hours=9)
    today = now_jst.date()
    today_str = today.strftime("%Y-%m-%d")
    today_weeknum = today.isocalendar()[1]

    print(f"Today (JST): {today_str} (week {today_weeknum})")

    try:
        client = JQuantsClient()
    except Exception as e:
        print(f"ERROR: J-Quants connection failed: {e}")
        # フォールバック: 金曜判定
        return today.weekday() == 4

    # 今日から7日後までのカレンダーを取得
    end_date = today + timedelta(days=7)
    try:
        cal_resp = client.request(
            "/markets/calendar",
            params={
                "from": today_str,
                "to": end_date.strftime("%Y-%m-%d"),
            }
        )
        cal_data = cal_resp.get("data", [])
    except Exception as e:
        print(f"ERROR: Calendar fetch failed: {e}")
        return today.weekday() == 4

    # 今日より後の営業日を取得
    trading_days = []
    for d in cal_data:
        date_str = d.get("Date", "")
        is_trading = d.get("HolDiv") == "1"
        if date_str > today_str and is_trading:
            trading_days.append(date_str)

    trading_days.sort()

    if not trading_days:
        print("No trading days found after today in the next 7 days")
        return True  # 翌営業日がなければ最終営業日

    next_trading = datetime.strptime(trading_days[0], "%Y-%m-%d").date()
    next_weeknum = next_trading.isocalendar()[1]

    print(f"Next trading day: {trading_days[0]} (week {next_weeknum})")

    is_last = next_weeknum != today_weeknum
    if is_last:
        print(f"✅ Today is the LAST trading day of week {today_weeknum}")
    else:
        print(f"ℹ️  Not the last trading day (next: {trading_days[0]})")

    return is_last


def main() -> int:
    result = check()
    # GitHub Actions用
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"is_last_trading_day={str(result).lower()}\n")
    return 0 if result else 1


if __name__ == "__main__":
    raise SystemExit(main())
