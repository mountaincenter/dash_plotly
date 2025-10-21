#!/usr/bin/env python3
"""
タイムゾーン変換の検証スクリプト
GitHub Actions環境をシミュレートして動作確認
"""

from datetime import datetime, timezone, timedelta

def test_timezone_conversion():
    """UTC→JST変換のテスト"""
    print("=" * 70)
    print("タイムゾーン変換テスト")
    print("=" * 70)

    # GitHub Actions cron: 0 7 * * * (UTC 07:00)
    # シミュレート: UTC 2025-10-21 07:09:16

    # 方法1: 現在時刻から取得（実際の動作）
    print("\n【方法1: 実際の環境での動作】")
    now_utc_real = datetime.now(timezone.utc).replace(tzinfo=None)
    now_jst_real = now_utc_real + timedelta(hours=9)
    print(f"UTC: {now_utc_real.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"JST: {now_jst_real.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"計算: {now_utc_real.hour}時 + 9 = {now_jst_real.hour}時")

    # 方法2: GitHub Actions実行時刻をシミュレート
    print("\n【方法2: GitHub Actions UTC 07:00実行時のシミュレーション】")
    simulated_utc = datetime(2025, 10, 21, 7, 9, 16)  # UTC 07:09:16
    simulated_jst = simulated_utc + timedelta(hours=9)
    print(f"UTC: {simulated_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"JST: {simulated_jst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"計算: 7時 + 9 = {simulated_jst.hour}時")

    # 実行ウィンドウチェック
    print("\n【実行ウィンドウチェック】")
    latest_trading_day = datetime(2025, 10, 21)  # 営業日: 2025-10-21
    window_start = latest_trading_day.replace(hour=16, minute=0, second=0)
    window_end = window_start + timedelta(hours=10)  # 16:00 + 10h = 翌2:00

    print(f"最新営業日: {latest_trading_day.strftime('%Y-%m-%d')}")
    print(f"実行ウィンドウ: {window_start.strftime('%Y-%m-%d %H:%M')} ~ {window_end.strftime('%Y-%m-%d %H:%M')}")
    print(f"現在時刻(JST): {simulated_jst.strftime('%Y-%m-%d %H:%M')}")

    in_window = window_start <= simulated_jst <= window_end
    print(f"\n判定: {'✅ Within execution window' if in_window else '❌ Outside execution window'}")

    # 数学的検証
    print("\n【数学的検証】")
    print("UTC 07:00実行時:")
    print(f"  JST時刻 = 07:00 + 09:00 = 16:00")
    print(f"  ウィンドウ開始 = 16:00")
    print(f"  ウィンドウ終了 = 02:00（翌日）")
    print(f"  16:00 >= 16:00 かつ 16:00 <= 02:00（翌日） → ✅ TRUE")

    return in_window

def test_all_cron_times():
    """すべてのcron実行時刻でテスト"""
    print("\n" + "=" * 70)
    print("全cron実行時刻のテスト")
    print("=" * 70)

    test_cases = [
        ("メイン実行 (07:00 UTC)", 7, 0, True),
        ("メイン実行終了間際 (07:59 UTC)", 7, 59, True),
        ("フォールバック (17:00 UTC)", 17, 0, False),  # 02:00 = ウィンドウ終了時刻
        ("フォールバック終了間際 (17:59 UTC)", 17, 59, False),
    ]

    latest_trading_day = datetime(2025, 10, 21)
    window_start = latest_trading_day.replace(hour=16, minute=0, second=0)
    window_end = window_start + timedelta(hours=10)

    all_passed = True

    for name, hour_utc, minute_utc, expected_result in test_cases:
        utc_time = datetime(2025, 10, 21, hour_utc, minute_utc, 0)
        jst_time = utc_time + timedelta(hours=9)

        in_window = window_start <= jst_time <= window_end
        passed = in_window == expected_result

        status = "✅ PASS" if passed else "❌ FAIL"
        result_str = "実行" if in_window else "スキップ"

        print(f"\n{name}: {status}")
        print(f"  UTC: {utc_time.strftime('%H:%M')}")
        print(f"  JST: {jst_time.strftime('%H:%M')}")
        print(f"  期待: {'実行' if expected_result else 'スキップ'}")
        print(f"  実際: {result_str}")

        if not passed:
            all_passed = False

    print("\n" + "=" * 70)
    if all_passed:
        print("✅ すべてのテストケースが合格")
    else:
        print("❌ テストケースに失敗がありました")
    print("=" * 70)

    return all_passed

if __name__ == "__main__":
    print("\n🧪 GitHub Actions タイムゾーン変換検証\n")

    # テスト1: 基本的な変換
    result1 = test_timezone_conversion()

    # テスト2: すべてのcron時刻
    result2 = test_all_cron_times()

    print("\n" + "=" * 70)
    print("最終結果")
    print("=" * 70)
    if result1 and result2:
        print("✅ すべてのテストが合格 - タイムゾーン変換は正しく動作します")
        exit(0)
    else:
        print("❌ テストに失敗しました")
        exit(1)
