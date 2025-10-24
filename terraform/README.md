# Terraform: App Runner デプロイ通知

EventBridge + Lambda で App Runner のデプロイ完了を Slack に通知

## ファイル構成

```
terraform/
├── main.tf              # プロバイダー設定
├── variables.tf         # 変数定義
├── outputs.tf           # 出力定義
├── iam.tf              # IAMロール
├── lambda.tf           # Lambda関数
├── eventbridge.tf      # EventBridgeルール
├── terraform.tfvars.example  # 変数の例
└── README.md           # このファイル
```

## セットアップ手順

### 1. 変数ファイルの作成

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

`terraform.tfvars` を編集して、実際の値を設定:

```hcl
aws_region              = "ap-northeast-1"
apprunner_service_arn   = "arn:aws:apprunner:ap-northeast-1:980921748690:service/dash-plotly/4d8b5908dadd4971aafb4fae072d5ee6"
apprunner_service_url   = "https://xxxxx.ap-northeast-1.awsapprunner.com"
slack_webhook_url       = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### 2. Terraform 初期化

```bash
terraform init
```

### 3. 実行計画の確認

```bash
terraform plan
```

### 4. リソースの作成

```bash
terraform apply
```

確認プロンプトで `yes` を入力してリソースを作成

### 5. 作成されたリソースの確認

```bash
terraform output
```

## 作成されるリソース

- **IAMロール**: `apprunner-notification-lambda-role`
  - Lambda実行用のロール
  - CloudWatch Logs への書き込み権限

- **Lambda関数**: `apprunner-deployment-notification`
  - App Runnerのイベントを受け取りSlack通知
  - Python 3.12ランタイム

- **EventBridgeルール**: `apprunner-deployment-to-slack`
  - App Runner が RUNNING になったときにトリガー
  - Lambda関数を呼び出し

## テスト

App Runner に新しいデプロイをトリガーして、Slack に通知が届くことを確認:

1. GitHub Actions で ECR に新しいイメージを push
2. App Runner が自動的にデプロイを開始
3. デプロイ完了後、Slack に通知が送信される

## リソースの削除

```bash
terraform destroy
```

## トラブルシューティング

### Lambda関数のログ確認

```bash
aws logs tail /aws/lambda/apprunner-deployment-notification --follow
```

### EventBridgeルールの確認

```bash
aws events list-rules --name-prefix apprunner
```

### Lambda関数の手動テスト

AWSコンソールでLambda関数を開き、テストイベントを作成して実行
