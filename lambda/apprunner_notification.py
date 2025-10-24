"""
App Runner デプロイ完了通知 Lambda関数
EventBridge から App Runner のステータス変更イベントを受け取り、Slack に通知する
"""
import json
import os
import urllib.request
from datetime import datetime

SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
SERVICE_URL = os.environ.get('SERVICE_URL', 'https://your-app-runner-url.awsapprunner.com')


def lambda_handler(event, context):
    """
    EventBridge からのイベントを処理

    イベント例:
    {
      "source": "aws.apprunner",
      "detail-type": "AppRunner Service Status Change",
      "detail": {
        "serviceArn": "arn:aws:apprunner:...",
        "serviceName": "dash-plotly",
        "status": "RUNNING",
        "operationStatus": "CREATE_SUCCEEDED"
      }
    }
    """

    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not configured")
        return {
            'statusCode': 200,
            'body': json.dumps('Skipped: No webhook URL')
        }

    detail = event.get('detail', {})
    service_name = detail.get('serviceName', 'Unknown')
    status = detail.get('status', 'Unknown')
    operation_status = detail.get('operationStatus', 'Unknown')

    # デプロイ完了のイベントのみ通知
    if status != 'RUNNING':
        print(f"Skipping notification for status: {status}")
        return {
            'statusCode': 200,
            'body': json.dumps('Skipped: Not RUNNING status')
        }

    # Slack メッセージを構築
    message = {
        "text": f"✅ App Runner Deployment Completed: {service_name}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"✅ *App Runner Deployment Completed*\n{service_name} is now running."
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Service:*\n`{service_name}`"},
                    {"type": "mrkdwn", "text": f"*Status:*\n`{status}`"},
                    {"type": "mrkdwn", "text": f"*Operation:*\n`{operation_status}`"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"}
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Open App"},
                        "url": SERVICE_URL
                    }
                ]
            }
        ]
    }

    # Slack に送信
    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps(message).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req) as response:
            response_body = response.read().decode('utf-8')
            print(f"Slack notification sent: {response_body}")

        return {
            'statusCode': 200,
            'body': json.dumps('Notification sent successfully')
        }

    except Exception as e:
        print(f"Error sending Slack notification: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
