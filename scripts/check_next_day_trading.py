#!/usr/bin/env python3
"""
check_next_day_trading.py
翌日営業日判定スクリプト（GitHub Actions 23時実行用）

23時実行時に「翌日（暦日）が営業日かどうか」をチェック
翌日が営業日の場合のみGrok選定を実行すべき

Exit codes:
  0: 翌日は営業日 → パイプライン実行すべき
  1: 翌日は休業日 → パイプラインスキップすべき

環境変数出力（GitHub Actions用）:
  IS_NEXT_DAY_TRADING: true/false
  NEXT_DAY_DATE: YYYY-MM-DD
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

# 2025-2026 年末年始スキップ日（翌営業日として判定される日）
# データ汚染防止のため、特殊相場日を除外
SKIP_DATES_YEAR_END = [
    "2025-12-26",  # 権利確定日（特殊需給）
    "2025-12-29",  # 閑散相場
    "2025-12-30",  # 大納会
    "2026-01-05",  # 大発会
]


def check_next_day_is_trading() -> tuple[bool, str]:
    """
    翌日（暦日）が営業日かどうかをチェック

    Returns:
        (is_trading, next_day_date)
        - is_trading: 翌日が営業日か
        - next_day_date: 翌日の日付（YYYY-MM-DD）
    """
    print("=" * 60)
    print("Next Day Trading Check (for 23:00 run)")
    print("=" * 60)

    # 現在時刻（GitHub ActionsはUTCなのでJSTに変換）
    now_utc_aware = datetime.now(timezone.utc)
    now_utc = now_utc_aware.replace(tzinfo=None)
    now_jst = now_utc + timedelta(hours=9)

    print(f"Current time (UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current time (JST): {now_jst.strftime('%Y-%m-%d %H:%M:%S')}")

    # 翌日の日付を計算
    tomorrow_jst = now_jst + timedelta(days=1)
    tomorrow_date_str = tomorrow_jst.strftime('%Y-%m-%d')

    print(f"Tomorrow's date (JST): {tomorrow_date_str}")

    # J-Quants クライアント初期化
    try:
        client = JQuantsClient()
        print(f"J-Quants connected: {client.plan}")
    except Exception as e:
        print(f"ERROR: Failed to connect to J-Quants: {e}")
        return False, tomorrow_date_str

    # J-Quants Trading Calendar APIから翌日の情報を取得
    try:
        # 翌日の前後30日間のカレンダーを取得
        from_date = (tomorrow_jst - timedelta(days=10)).strftime('%Y%m%d')
        to_date = (tomorrow_jst + timedelta(days=10)).strftime('%Y%m%d')

        print(f"Fetching trading calendar from {from_date} to {to_date}")

        params = {
            "from": from_date,
            "to": to_date
        }
        # v2: /markets/calendar（v1は/markets/trading_calendar）
        response = client.request("/markets/calendar", params=params)

        if not response or 'data' not in response:
            print(f"ERROR: Invalid response from trading calendar API")
            return False, tomorrow_date_str

        calendar_data = response['data']

        if not calendar_data:
            print(f"ERROR: Empty trading calendar data")
            return False, tomorrow_date_str

        # 翌日のデータを探す
        tomorrow_record = None
        for record in calendar_data:
            record_date = record.get('Date', '')
            if record_date == tomorrow_date_str:
                tomorrow_record = record
                break

        if not tomorrow_record:
            print(f"⚠️ Tomorrow's date not found in calendar: {tomorrow_date_str}")
            print(f"Available dates: {[r.get('Date') for r in calendar_data[:5]]}")
            # データが見つからない場合は安全のためスキップ
            return False, tomorrow_date_str

        # v2: HolDiv を確認（v1はHolidayDivision）
        holiday_division = tomorrow_record.get('HolDiv', '')

        print(f"\nTomorrow's calendar info:")
        print(f"  Date: {tomorrow_record.get('Date')}")
        print(f"  HolDiv: {holiday_division}")

        # HolDiv の判定
        # "1": 営業日
        # "0": 休業日（土日祝）
        # "2": 特別休業日（年末年始など）
        if holiday_division == "1":
            # 年末年始スキップ日チェック
            if tomorrow_date_str in SKIP_DATES_YEAR_END:
                print(f"⏭️ Tomorrow is in SKIP_DATES_YEAR_END: {tomorrow_date_str}")
                print("❌ Skipping due to special market conditions (year-end/new-year)")
                return False, tomorrow_date_str
            print("✅ Tomorrow is a TRADING day")
            return True, tomorrow_date_str
        elif holiday_division == "0":
            print("❌ Tomorrow is a NON-TRADING day (weekend/holiday)")
            return False, tomorrow_date_str
        elif holiday_division == "2":
            print("❌ Tomorrow is a SPECIAL NON-TRADING day (year-end, etc.)")
            return False, tomorrow_date_str
        else:
            print(f"⚠️ Unknown HolDiv: {holiday_division}")
            # 不明な場合は安全のためスキップ
            return False, tomorrow_date_str

    except Exception as e:
        print(f"ERROR: Failed to check trading calendar: {e}")
        import traceback
        traceback.print_exc()
        return False, tomorrow_date_str


def main() -> int:
    """メイン処理"""
    is_trading, next_day_date = check_next_day_is_trading()

    # 結果表示
    print("\n" + "=" * 60)
    print("Result")
    print("=" * 60)
    print(f"IS_NEXT_DAY_TRADING: {str(is_trading).lower()}")
    print(f"NEXT_DAY_DATE: {next_day_date}")
    print("=" * 60)

    if is_trading:
        print("\n✅ Next day is trading day - will execute Grok selection")
    else:
        print("\n❌ Next day is NOT trading day - skipping Grok selection")

    # GitHub Actions用の環境変数出力
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"is_next_day_trading={str(is_trading).lower()}\n")
            f.write(f"next_day_date={next_day_date}\n")
        print(f"\n✅ Wrote to GITHUB_OUTPUT: {github_output}")

    # 終了コード: 営業日なら0（実行すべき）、休業日なら1（スキップすべき）
    return 0 if is_trading else 1


if __name__ == "__main__":
    raise SystemExit(main())
