# App Runner デプロイ通知セットアップ

EventBridge + Lambda で App Runner のデプロイ完了を Slack に通知するベストプラクティス実装

## アーキテクチャ

```
App Runner (デプロイ完了)
  → EventBridge (イベント検知)
    → Lambda (apprunner_notification.py)
      → Slack Webhook
```

## セットアップ手順

### 1. Lambda 関数の作成

1. AWS Lambda コンソールを開く
2. 「関数の作成」をクリック
3. 以下の設定で作成:
   - **関数名**: `apprunner-deployment-notification`
   - **ランタイム**: Python 3.12
   - **実行ロール**: 新しいロールを作成（基本的な Lambda アクセス権限）

4. `apprunner_notification.py` のコードをコピー＆ペースト

5. 環境変数を設定:
   - `SLACK_WEBHOOK_URL`: あなたの Slack Incoming Webhook URL
   - `SERVICE_URL`: `https://YOUR-APP-RUNNER-URL.awsapprunner.com`

### 2. EventBridge ルールの作成

1. EventBridge コンソールを開く
2. 「ルールを作成」をクリック
3. 以下の設定:

**基本情報:**
- **名前**: `apprunner-deployment-to-slack`
- **説明**: App Runner デプロイ完了時に Slack 通知

**イベントパターン:**
```json
{
  "source": ["aws.apprunner"],
  "detail-type": ["AppRunner Service Status Change"],
  "detail": {
    "serviceArn": ["arn:aws:apprunner:ap-northeast-1:980921748690:service/dash-plotly/4d8b5908dadd4971aafb4fae072d5ee6"],
    "status": ["RUNNING"]
  }
}
```

**ターゲット:**
- **ターゲットタイプ**: AWS のサービス
- **ターゲット**: Lambda 関数
- **関数**: `apprunner-deployment-notification`

### 3. テスト

App Runner に新しいデプロイをトリガーして、Slack に通知が届くことを確認:

1. ECR に新しいイメージを push（GitHub Actions で自動的に実行される）
2. App Runner が自動的にデプロイを開始
3. デプロイ完了後、EventBridge → Lambda → Slack の流れで通知が送信される

## トラブルシューティング

### 通知が届かない場合

1. **EventBridge ルールが有効になっているか確認**
   - EventBridge コンソールでルールのステータスを確認

2. **Lambda のログを確認**
   - CloudWatch Logs で Lambda の実行ログを確認
   - エラーがないか確認

3. **環境変数が正しく設定されているか確認**
   - Lambda の設定タブで環境変数を確認

4. **Slack Webhook URL が有効か確認**
   - curl でテスト送信してみる

## ベストプラクティス

✅ **このアプローチの利点:**
- App Runner のネイティブなイベント通知を使用
- GitHub Actions のワークフローはシンプル（ECR push のみ）
- デプロイ完了を確実に検知できる
- スケーラブルで保守しやすい

❌ **避けるべきアンチパターン:**
- GitHub Actions でポーリングして待機
- 不確実なタイミングでの通知
- GitHub Actions の実行時間の無駄遣い
