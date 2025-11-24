#!/usr/bin/env python3
"""
check_xai_billing.py
xAI Management API を使って billing 情報を取得し Slack に通知

実行方法:
    python3 scripts/check_xai_billing.py

環境変数:
    XAI_MANAGEMENT_API_KEY: xAI Management API key
    XAI_TEAM_ID: xAI Team ID
    SLACK_WEBHOOK_URL: Slack Incoming Webhook URL (オプション)
"""

import os
import sys
import json
import requests
from datetime import datetime
from typing import Optional, Dict, Any

# 設定
MANAGEMENT_API_BASE = "https://management-api.x.ai"
MANAGEMENT_API_KEY = os.getenv("XAI_MANAGEMENT_API_KEY")
TEAM_ID = os.getenv("XAI_TEAM_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


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


def send_slack_notification(billing_data: Dict[str, Any]) -> bool:
    """
    Slack に billing 情報を通知

    Args:
        billing_data: billing API からのレスポンス

    Returns:
        送信成功時 True
    """
    if not SLACK_WEBHOOK_URL:
        print("ℹ️  SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False

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

        # 使用量詳細（モデル別）
        lines = core_invoice.get("lines", [])
        usage_details = []

        # モデル別に集計
        model_usage = {}
        for line in lines:
            desc = line.get("description", "Unknown")
            amount = line.get("amount", "0")

            # モデル名を抽出 (例: "Chat grok-2-1212-1.0.0" → "grok-2-1212")
            model_name = desc.split()[-1] if desc else "Unknown"

            if model_name not in model_usage:
                model_usage[model_name] = 0

            try:
                model_usage[model_name] += int(amount)
            except (ValueError, TypeError):
                pass

        # 使用量テキスト生成
        for model, amount_cents in sorted(model_usage.items(), key=lambda x: x[1], reverse=True):
            if amount_cents > 0:
                usage_details.append(f"• {model}: {format_usd_cents(str(amount_cents))}")

        usage_text = "\n".join(usage_details) if usage_details else "• No usage this month"

        # billing cycle
        billing_cycle = core_invoice.get("billingCycle", {})
        year = billing_cycle.get('year', 'N/A')
        month = billing_cycle.get('month', 'N/A')
        if isinstance(month, int):
            cycle_text = f"{year}-{month:02d}"
        else:
            cycle_text = f"{year}-{month}"

        # Slack メッセージ構築
        message = {
            "text": "💰 xAI API Billing Report",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*💰 xAI API Billing Report*\nBilling Cycle: `{cycle_text}`"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Total prepaid credits:*\n{format_usd_cents(str(total_credits))}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*今回使用:*\n{format_usd_cents(str(used_this_cycle))}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*残高:*\n{format_usd_cents(str(remaining_credits))}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*使用量詳細:*\n{usage_text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S JST')}"
                        }
                    ]
                }
            ]
        }

        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()

        print("✅ Slack notification sent successfully")
        return True

    except Exception as e:
        print(f"❌ Error sending Slack notification: {e}")
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

    # Slack 通知
    send_slack_notification(billing_data)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
