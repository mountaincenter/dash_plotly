"""
App Runner デプロイ完了通知 Lambda関数
EventBridge から App Runner のステータス変更イベントを受け取り、Slack に通知する
"""
import json
import os
import urllib.request
from datetime import datetime

SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
SERVICE_URL = os.environ.get('SERVICE_URL', 'https://muuq3bv2n2.ap-northeast-1.awsapprunner.com')


def lambda_handler(event, context):
    """
    EventBridge からのイベントを処理

    イベント例:
    {
      "source": "aws.apprunner",
      "detail-type": "AppRunner Service Status Change",
      "detail": {
        "serviceArn": "arn:aws:apprunner:...",
        "serviceName": "stock-api",
        "status": "RUNNING",
        "operationStatus": "CREATE_SUCCEEDED"
      }
    }
    """

    # デバッグ: イベント全体をログ出力
    print(f"Received event: {json.dumps(event, indent=2)}")

    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not configured")
        return {
            'statusCode': 200,
            'body': json.dumps('Skipped: No webhook URL')
        }

    detail = event.get('detail', {})
    service_name = detail.get('serviceName', 'Unknown')
    operation_status = detail.get('operationStatus', 'Unknown')

    print(f"Service: {service_name}, Operation Status: {operation_status}")

    # デプロイ完了のイベントのみ通知（複数パターンに対応）
    if operation_status not in ['DeploymentCompletedSuccessfully', 'UpdateServiceCompletedSuccessfully']:
        print(f"Skipping notification for operation status: {operation_status}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Skipped: {operation_status}')
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
