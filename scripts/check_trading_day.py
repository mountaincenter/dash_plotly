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

    # JQuantsデータ未公開対策: 16:00-18:00に実行時、latest_trading_dayが
    # 前日の場合、JQuantsカレンダーで今日が営業日か確認する
    if not in_window and 16 <= now_jst.hour <= 18:
        today = now_jst.date()
        today_str = today.strftime("%Y-%m-%d")
        try:
            cal_resp = fetcher.client.request(
                "/markets/calendar",
                params={"from": today_str, "to": today_str}
            )
            cal_data = cal_resp.get("data", [])
            today_is_trading = any(
                d["Date"] == today_str and d["HolDiv"] == "1"
                for d in cal_data
            )
        except Exception:
            today_is_trading = False

        if today_is_trading:
            window_start_today = now_jst.replace(hour=16, minute=0, second=0, microsecond=0)
            window_end_today = window_start_today + timedelta(hours=11)
            in_window = window_start_today <= now_jst <= window_end_today
            if in_window:
                print(f"ℹ️  JQuants data not yet available for today ({today_str})")
                print(f"   JQuants calendar confirms today is a trading day")
                latest_trading_day_str = today_str

    if in_window:
        print("✅ Within execution window")
    else:
        print("❌ Outside execution window")
        return False, False, latest_trading_day_str

    # 週次meta_jquants更新判定
    # TODO: 週次更新ロジックは後で実装
    force_meta_update = False
    print("ℹ️  Weekly meta_jquants update check: not implemented yet")

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
    raise SystemExit(main())
