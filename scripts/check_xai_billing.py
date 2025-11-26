#!/usr/bin/env python3
"""
check_xai_billing.py
xAI Management API を使って billing 情報を取得し、Slack通知用セクションを出力

実行方法:
    python3 scripts/check_xai_billing.py

環境変数:
    XAI_MANAGEMENT_API_KEY: xAI Management API key
    XAI_TEAM_ID: xAI Team ID

出力:
    /tmp/billing_section.txt - パイプライン成功通知に統合されるSlack用JSONセクション
"""

import os
import sys
import requests
from typing import Optional, Dict, Any

# 設定
MANAGEMENT_API_BASE = "https://management-api.x.ai"
MANAGEMENT_API_KEY = os.getenv("XAI_MANAGEMENT_API_KEY")
TEAM_ID = os.getenv("XAI_TEAM_ID")


def get_billing_preview() -> Optional[Dict[str, Any]]:
    """
    当月の billing プレビューを取得

    Returns:
        dict: billing 情報、または取得失敗時は None
    """
    if not MANAGEMENT_API_KEY:
        print("❌ Error: XAI_MANAGEMENT_API_KEY environment variable not set")
        return None

    if not TEAM_ID:
        print("❌ Error: XAI_TEAM_ID environment variable not set")
        return None

    url = f"{MANAGEMENT_API_BASE}/v1/billing/teams/{TEAM_ID}/postpaid/invoice/preview"
    headers = {
        "Authorization": f"Bearer {MANAGEMENT_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        print(f"📡 Fetching billing info from: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        return data

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        print(f"   Response: {e.response.text if e.response else 'N/A'}")
        return None
    except Exception as e:
        print(f"❌ Error fetching billing info: {e}")
        return None


def format_usd_cents(cents_str: str) -> str:
    """
    USD cents (文字列) を USD に変換してフォーマット

    Args:
        cents_str: USD cents (例: "123456" = $1,234.56)

    Returns:
        フォーマット済み文字列 (例: "$1,234.56")
    """
    try:
        cents = int(cents_str)
        dollars = cents / 100
        return f"${dollars:,.2f}"
    except (ValueError, TypeError):
        return "$0.00"


def save_billing_section(billing_data: Dict[str, Any], output_path: str = "/tmp/billing_section.txt") -> bool:
    """
    Slack通知用のbillingセクションをファイルに保存

    Args:
        billing_data: billing API からのレスポンス
        output_path: 出力ファイルパス

    Returns:
        保存成功時 True
    """
    try:
        core_invoice = billing_data.get("coreInvoice", {})

        # 正しいフィールドを取得
        total_credits_val = core_invoice.get("prepaidCredits", {}).get("val", "0")
        used_this_cycle_val = core_invoice.get("totalWithCorr", {}).get("val", "0")

        # 残高計算
        try:
            total_credits = abs(int(total_credits_val))
            used_this_cycle = abs(int(used_this_cycle_val))
            remaining_credits = total_credits - used_this_cycle
        except (ValueError, TypeError):
            total_credits = 0
            used_this_cycle = 0
            remaining_credits = 0

        # Slack用のセクションJSON生成
        section = f'''{{
  "type": "section",
  "fields": [
    {{"type": "mrkdwn", "text": "*💰 xAI残高:*\\n{format_usd_cents(str(remaining_credits))}"}},
    {{"type": "mrkdwn", "text": "*今回使用:*\\n{format_usd_cents(str(used_this_cycle))}"}}
  ]
}}'''

        with open(output_path, 'w') as f:
            f.write(section)

        print(f"✅ Billing section saved to {output_path}")
        return True

    except Exception as e:
        print(f"❌ Error saving billing section: {e}")
        return False


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("xAI Billing Check")
    print("=" * 60)

    # billing 情報取得
    billing_data = get_billing_preview()

    if not billing_data:
        print("\n❌ Failed to fetch billing information")
        return 1

    # 結果を表示
    core_invoice = billing_data.get("coreInvoice", {})

    # 正しいフィールドを取得
    total_credits_val = core_invoice.get("prepaidCredits", {}).get("val", "0")
    used_this_cycle_val = core_invoice.get("totalWithCorr", {}).get("val", "0")

    try:
        # prepaidCredits.val はマイナス表示されているため、絶対値を取る
        total_credits = abs(int(total_credits_val))
        used_this_cycle = abs(int(used_this_cycle_val))
        remaining_credits = total_credits - used_this_cycle
    except (ValueError, TypeError):
        total_credits = 0
        used_this_cycle = 0
        remaining_credits = 0

    billing_cycle = billing_data.get("billingCycle", {})

    print("\n" + "=" * 60)
    print("Billing Summary")
    print("=" * 60)

    year = billing_cycle.get('year', 'N/A')
    month = billing_cycle.get('month', 'N/A')
    if isinstance(month, int):
        cycle_text = f"{year}-{month:02d}"
    else:
        cycle_text = f"{year}-{month}"
    print(f"Billing Cycle: {cycle_text}")
    print(f"Total prepaid credits: {format_usd_cents(str(total_credits))}")
    print(f"今回使用したクレジット: {format_usd_cents(str(used_this_cycle))}")
    print(f"残高: {format_usd_cents(str(remaining_credits))}")
    print("=" * 60)

    # 使用量詳細
    lines = core_invoice.get("lines", [])
    if lines:
        print("\n使用量詳細:")
        for line in lines:
            desc = line.get("description", "Unknown")
            unit_type = line.get("unitType", "")
            num_units = line.get("numUnits", "0")
            amount = line.get("amount", "0")

            print(f"  - {desc} ({unit_type}): {num_units} units = {format_usd_cents(amount)}")

    print()

    # Slack通知用セクションをファイルに保存
    save_billing_section(billing_data)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
