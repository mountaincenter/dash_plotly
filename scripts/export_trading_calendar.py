#!/usr/bin/env python3
"""
営業日カレンダーをJ-Quants APIから取得してJSONファイルに出力
フロントエンドで使用するための静的ファイル生成
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from lib.jquants_client import JQuantsClient


def main():
    """営業日カレンダーをJSON出力"""
    client = JQuantsClient()

    # 今月と翌月の営業日を取得
    today = datetime.now()
    from_date = datetime(today.year, today.month, 1)
    to_date = datetime(today.year, today.month + 2, 1) if today.month <= 10 else datetime(today.year + 1, 1, 1)

    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")

    print(f"Fetching trading calendar from {from_str} to {to_str}...")

    # J-Quants APIから取得
    response = client.request(
        endpoint="/markets/trading_calendar",
        params={
            "from": from_str,
            "to": to_str
        }
    )

    # デバッグ: レスポンス構造を確認
    print("Sample response:", response["trading_calendar"][:2] if response["trading_calendar"] else "empty")

    # 営業日のみ抽出（holiday_division: "1" = 営業日）
    trading_days = [
        day["Date"]
        for day in response["trading_calendar"]
        if day.get("holiday_division") == "1" or day.get("HolidayDivision") == "1"
    ]

    print(f"Found {len(trading_days)} trading days")

    # フロントエンドのpublicディレクトリに出力
    output_dir = Path(__file__).resolve().parents[2] / "stock-frontend" / "public"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "trading-calendar.json"

    output_data = {
        "tradingDays": trading_days,
        "generatedAt": datetime.now().isoformat(),
        "from": from_str,
        "to": to_str
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"✓ Trading calendar exported to: {output_path}")


if __name__ == "__main__":
    main()
