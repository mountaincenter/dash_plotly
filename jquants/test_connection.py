#!/usr/bin/env python3
"""
J-Quants API接続テスト
認証とデータ取得が正常に行えるか確認
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_authentication():
    """認証テスト"""
    print("=" * 60)
    print("J-Quants API Connection Test")
    print("=" * 60)

    try:
        from jquants.client import JQuantsClient

        print("\n[STEP 1] Creating client...")
        client = JQuantsClient()

        print(f"  ✓ Refresh token loaded: {client.refresh_token[:10]}...")
        print(f"  ✓ Base URL: {client.base_url}")
        print(f"  ✓ Plan: {client.plan}")

        print("\n[STEP 2] Getting ID token...")
        headers = client.get_headers()

        if "Authorization" in headers:
            id_token = headers["Authorization"].replace("Bearer ", "")
            print(f"  ✓ ID token obtained: {id_token[:20]}...")
            print("  ✓ Authentication successful!")
            return client
        else:
            print("  ✗ Failed to get ID token")
            return None

    except Exception as e:
        print(f"  ✗ Authentication failed: {e}")
        return None


def test_listed_info(client: JQuantsClient):
    """上場銘柄情報取得テスト"""
    print("\n[STEP 3] Fetching listed stocks info...")

    try:
        from jquants.fetcher import JQuantsFetcher

        fetcher = JQuantsFetcher(client)
        df = fetcher.get_listed_info()

        if df.empty:
            print("  ✗ No data received")
            return False

        print(f"  ✓ Retrieved {len(df)} stocks")
        print(f"  ✓ Columns: {', '.join(df.columns.tolist()[:5])}...")

        if len(df) > 0:
            sample = df.iloc[0]
            print(f"  ✓ Sample: {sample.get('Code', 'N/A')} - {sample.get('CompanyName', 'N/A')}")

        return True

    except Exception as e:
        print(f"  ✗ Failed to fetch listed info: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_price_data(client: JQuantsClient):
    """株価データ取得テスト"""
    print("\n[STEP 4] Fetching price data (sample: 7203 - Toyota)...")

    try:
        from jquants.fetcher import JQuantsFetcher
        from datetime import date, timedelta

        fetcher = JQuantsFetcher(client)

        # 無料プランは12週間遅延のため、過去のデータを取得
        to_date = date.today() - timedelta(days=84)  # 12週間前
        from_date = to_date - timedelta(days=7)

        df = fetcher.get_prices_daily(
            code="7203",
            from_date=from_date,
            to_date=to_date
        )

        if df.empty:
            print("  ⚠ No price data received (may be due to free plan 12-week delay)")
            print("  → This is expected for free plan users")
            return True

        print(f"  ✓ Retrieved {len(df)} rows")
        print(f"  ✓ Columns: {', '.join(df.columns.tolist())}")

        if len(df) > 0:
            sample = df.iloc[-1]
            print(f"  ✓ Latest: Date={sample.get('Date', 'N/A')}, Close={sample.get('Close', 'N/A')}")

        return True

    except Exception as e:
        print(f"  ✗ Failed to fetch price data: {e}")
        import traceback
        traceback.print_exc()
        return False


def main() -> int:
    """メイン処理"""

    # 認証テスト
    client = test_authentication()
    if client is None:
        print("\n" + "=" * 60)
        print("❌ Authentication failed. Please check your .env.jquants file.")
        print("=" * 60)
        return 1

    # 上場銘柄情報取得テスト
    listed_ok = test_listed_info(client)

    # 株価データ取得テスト
    price_ok = test_price_data(client)

    # 結果サマリー
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Authentication:    {'✓ PASS' if client else '✗ FAIL'}")
    print(f"Listed Info:       {'✓ PASS' if listed_ok else '✗ FAIL'}")
    print(f"Price Data:        {'✓ PASS' if price_ok else '✗ FAIL'}")
    print("=" * 60)

    if client and listed_ok:
        print("\n✅ J-Quants API connection is working!")
        if client.plan == "free":
            print("📌 Note: Free plan has 12-week delay for price data")
        return 0
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
