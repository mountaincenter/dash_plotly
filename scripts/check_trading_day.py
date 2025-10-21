#!/usr/bin/env python3
"""
check_trading_day.py
営業日判定スクリプト（GitHub Actions用）

営業日の16:00～翌2:00（26:00）の間のみパイプラインを実行
週次meta_jquants更新判定も実施（毎週金曜または金曜が休みなら最終営業日）

終了コード:
  0: 実行OK（営業日の16:00～26:00）
  1: 実行スキップ（営業時間外または非営業日）

環境変数出力（GitHub Actions用）:
  SHOULD_RUN: true/false
  FORCE_META_UPDATE: true/false（週次更新が必要な場合）
  LATEST_TRADING_DAY: YYYY-MM-DD
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from datetime import datetime, time, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher


def get_weekly_update_day(trading_days: list[datetime]) -> datetime:
    """
    今週の週次更新日を取得（金曜日または金曜が休みなら最終営業日）

    Args:
        trading_days: 今週の営業日リスト（datetime）

    Returns:
        週次更新を実施すべき営業日（datetime）
    """
    if not trading_days:
        return None

    # 今週の日付範囲（月曜～日曜）
    today = datetime.now().date()
    # 今週の月曜日を計算
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)

    # 今週の営業日のみフィルタ
    this_week_trading = [
        td for td in trading_days
        if monday <= td.date() <= sunday
    ]

    if not this_week_trading:
        return None

    # 金曜日（weekday=4）が営業日か確認
    friday_trading = [td for td in this_week_trading if td.weekday() == 4]

    if friday_trading:
        # 金曜日が営業日ならそれを返す
        return friday_trading[0]
    else:
        # 金曜日が非営業日なら、今週の最終営業日を返す
        return max(this_week_trading)


def check_trading_window() -> tuple[bool, bool, str]:
    """
    営業日の16:00～翌2:00の実行ウィンドウかチェック

    Returns:
        (should_run, force_meta_update, latest_trading_day)
        - should_run: パイプラインを実行すべきか
        - force_meta_update: meta_jquants強制更新フラグ
        - latest_trading_day: 最新営業日（YYYY-MM-DD）
    """
    print("=" * 60)
    print("Trading Day Check")
    print("=" * 60)

    # 現在時刻（GitHub ActionsはUTCなのでJSTに変換）
    # IMPORTANT: GitHub Actions runners use UTC timezone
    # We must explicitly get UTC time, not local time
    from datetime import timezone

    # Get current UTC time (timezone-aware)
    now_utc_aware = datetime.now(timezone.utc)
    # Convert to naive datetime for calculation
    now_utc = now_utc_aware.replace(tzinfo=None)
    # Convert UTC to JST (+9 hours)
    now_jst = now_utc + timedelta(hours=9)

    print(f"Current time (UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current time (JST): {now_jst.strftime('%Y-%m-%d %H:%M:%S')}")

    # J-Quants クライアント初期化
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)
        print(f"J-Quants connected: {client.plan}")
    except Exception as e:
        print(f"ERROR: Failed to connect to J-Quants: {e}")
        return False, False, ""

    # 最新営業日を取得
    try:
        latest_trading_day_str = fetcher.get_latest_trading_day()
        latest_trading_day = datetime.strptime(latest_trading_day_str, "%Y-%m-%d")
        print(f"Latest trading day: {latest_trading_day_str}")
    except Exception as e:
        print(f"ERROR: Failed to get latest trading day: {e}")
        return False, False, ""

    # 実行ウィンドウの判定
    # 営業日の16:00 ～ 翌3:00（27:00）
    window_start = latest_trading_day.replace(hour=16, minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(hours=11)  # 16:00 + 11h = 翌3:00

    print(f"Execution window: {window_start.strftime('%Y-%m-%d %H:%M')} ~ {window_end.strftime('%Y-%m-%d %H:%M')}")
    print(f"Current time:     {now_jst.strftime('%Y-%m-%d %H:%M')}")

    in_window = window_start <= now_jst <= window_end

    if in_window:
        print("✅ Within execution window")
    else:
        print("❌ Outside execution window")
        return False, False, latest_trading_day_str

    # 週次meta_jquants更新判定
    force_meta_update = False

    try:
        # 今週+来週の営業日を取得（余裕を持って2週間分）
        from_date = (now_jst - timedelta(days=7)).date()
        to_date = (now_jst + timedelta(days=7)).date()

        trading_calendar = fetcher.get_trading_calendar(from_date=from_date, to_date=to_date)

        if trading_calendar.empty:
            print("WARN: Trading calendar is empty")
        else:
            # HolidayDivision == "1" (営業日) のみフィルタ
            # 0: 非営業日、1: 営業日、2: 半日立会、3: 祝日取引のある非営業日
            trading_calendar = trading_calendar[trading_calendar["HolidayDivision"] == "1"].copy()

            # Date列をdatetimeに変換
            trading_days = pd.to_datetime(trading_calendar['Date']).tolist()

            # 今週の週次更新日を取得
            weekly_update_day = get_weekly_update_day(trading_days)

            if weekly_update_day:
                update_day_str = weekly_update_day.strftime('%Y-%m-%d')
                print(f"Weekly update day: {update_day_str} ({weekly_update_day.strftime('%A')})")

                # 今日が週次更新日か判定
                if latest_trading_day.date() == weekly_update_day.date():
                    force_meta_update = True
                    print("✅ Weekly meta_jquants update required (Friday or last trading day of week)")
                else:
                    print("ℹ️  Not a weekly update day")
            else:
                print("WARN: Could not determine weekly update day")

    except Exception as e:
        print(f"WARN: Weekly update check failed: {e}")
        import traceback
        traceback.print_exc()

    return True, force_meta_update, latest_trading_day_str


def main() -> int:
    """メイン処理"""
    should_run, force_meta_update, latest_trading_day = check_trading_window()

    # 結果表示
    print("\n" + "=" * 60)
    print("Result")
    print("=" * 60)
    print(f"SHOULD_RUN: {str(should_run).lower()}")
    print(f"FORCE_META_UPDATE: {str(force_meta_update).lower()}")
    print(f"LATEST_TRADING_DAY: {latest_trading_day}")
    print("=" * 60)

    # GitHub Actions用の環境変数出力
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"should_run={str(should_run).lower()}\n")
            f.write(f"force_meta_update={str(force_meta_update).lower()}\n")
            f.write(f"latest_trading_day={latest_trading_day}\n")
        print(f"\n✅ Wrote to GITHUB_OUTPUT: {github_output}")

    # 終了コード: 実行OKなら0、スキップなら1
    return 0 if should_run else 1


if __name__ == "__main__":
    import pandas as pd  # get_weekly_update_day内で使用
    raise SystemExit(main())
