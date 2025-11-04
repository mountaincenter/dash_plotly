# Terraform: AWS インフラ管理

このディレクトリには、Stock API プロジェクトの AWS インフラを管理する Terraform コードが含まれています。

## 📋 管理対象リソース

1. **App Runner サービス** (stock-api) - APIサーバー
2. **ECR リポジトリ** (stock-api) - Dockerイメージレジストリ
3. **S3 バケット** (stock-api-data) - データストレージ
4. **EventBridge + Lambda** - デプロイ通知システム
5. **Route53 + カスタムドメイン** (stock.api.ymnk.jp)
6. **IAM ロール** - App Runner, Lambda用

## ファイル構成

```
terraform/
├── main.tf                       # プロバイダー設定
├── variables.tf                  # 変数定義
├── outputs.tf                    # 出力定義
├── apprunner_service.tf          # App Runnerサービス
├── apprunner_iam.tf              # App Runner IAMロール
├── apprunner_custom_domain.tf    # App Runnerカスタムドメイン
├── ecr.tf                        # ECRリポジトリ
├── s3.tf                         # S3バケット
├── lambda.tf                     # Lambda関数
├── eventbridge.tf                # EventBridgeルール
├── route53.tf                    # Route53ホストゾーン
├── terraform.tfvars.example      # 変数の例
├── README.md                     # このファイル
└── ROUTE53_SETUP.md              # Route53セットアップガイド
```

## セットアップ手順

### 1. 変数ファイルの作成

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

`terraform.tfvars` を編集して、実際の値を設定:

```hcl
aws_region        = "ap-northeast-1"
slack_webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
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

### App Runner

- **App Runner サービス**: `stock-api`
  - CPU: 1024 (1 vCPU), Memory: 2048 MB
  - **Auto Deployments**: 有効 (`auto_deployments_enabled = true`)
  - ECRから `latest` タグを自動デプロイ
  - カスタムドメイン: `stock.api.ymnk.jp`
  - Health Check: TCP (Port 8000)

- **IAMロール**:
  - `AppRunnerECRAccessRole-stock-api` - ECRアクセス用
  - `AppRunnerInstanceRole-stock-api` - S3読み取り専用

### ECR

- **ECRリポジトリ**: `stock-api`
  - イメージライフサイクルポリシー設定済み
  - 最新10イメージを保持（タグ付き）
  - タグなしイメージは7日後削除
  - イメージスキャン: プッシュ時自動

### S3

- **S3バケット**: `stock-api-data`
  - バージョニング有効
  - サーバーサイド暗号化 (AES256)
  - パブリックアクセスブロック
  - 古いバージョン30日後削除

### デプロイ通知

- **Lambda関数**: `apprunner-deployment-notification`
  - App Runnerのデプロイ完了をSlack通知
  - Python 3.12ランタイム

- **EventBridgeルール**: `apprunner-deployment-to-slack`
  - App Runnerデプロイ完了時にトリガー
  - Lambda関数を呼び出し

### Route53 + カスタムドメイン

- **Route53 ホストゾーン**: `api.ymnk.jp`
  - Vercelから委譲されたサブドメイン

- **App Runner カスタムドメイン**: `stock.api.ymnk.jp`
  - ACM証明書自動発行
  - HTTPS自動対応

**Route53セットアップの詳細は [ROUTE53_SETUP.md](./ROUTE53_SETUP.md) を参照**

## 🧪 テスト・検証

### 1. App Runner 自動デプロイのテスト

```bash
# 1. ECRに新しいイメージをpush (GitHub Actionsまたはローカル)
docker build -t stock-api .
docker tag stock-api:latest <ecr-url>/stock-api:latest
docker push <ecr-url>/stock-api:latest

# 2. App Runnerが自動的にデプロイを開始（auto_deployments_enabled=true）
# 3. デプロイ完了後、Slackに通知が送信される
```

### 2. Slack通知のテスト

GitHub Actions の **Test Slack Notification** ワークフローを実行:

```bash
# GitHub UI から:
Actions → Test Slack Notification → Run workflow
→ 通知タイプを選択 (grok_formatted_all推奨)
```

### 3. Data Pipeline の実行

```bash
# GitHub UI から:
Actions → Data Pipeline → Run workflow
→ skip_trading_day_check: true (テスト時)
→ force_meta_jquants: true (強制更新時)
```

## リソースの削除

```bash
terraform destroy
```

## 🆘 トラブルシューティング

### 1. S3 Access Denied (403)

```bash
# IAMポリシーのバケット名を確認
aws iam get-policy-version \
  --policy-arn arn:aws:iam::980921748690:policy/dash-plotly-s3-sync-policy \
  --version-id $(aws iam get-policy --policy-arn arn:aws:iam::980921748690:policy/dash-plotly-s3-sync-policy --query 'Policy.DefaultVersionId' --output text)

# 正しいバケット名: stock-api-data
```

### 2. ECR Push Failed

```bash
# IAMポリシーのリポジトリ名を確認
aws iam get-policy-version \
  --policy-arn arn:aws:iam::980921748690:policy/dash-plotly-ecr-push-policy \
  --version-id $(aws iam get-policy --policy-arn arn:aws:iam::980921748690:policy/dash-plotly-ecr-push-policy --query 'Policy.DefaultVersionId' --output text)

# 正しいリポジトリ名: stock-api
```

### 3. App Runner デプロイが自動実行されない

```bash
# Auto Deploymentsが有効か確認
aws apprunner describe-service \
  --service-arn <service-arn> \
  --query 'Service.SourceConfiguration.AutoDeploymentsEnabled'

# 期待値: true
# falseの場合は terraform apply で有効化
```

### 4. Slack通知が重複する

```bash
# EventBridgeルールを確認
aws events list-rules --region ap-northeast-1 | jq '.Rules[] | select(.Name | contains("apprunner"))'

# テストルール (test-apprunner-all-events) が存在する場合は削除
aws events remove-targets --rule test-apprunner-all-events --ids TestLambda
aws events delete-rule --name test-apprunner-all-events
```

### 5. Lambda関数のログ確認

```bash
# リアルタイムでログを確認
aws logs tail /aws/lambda/apprunner-deployment-notification --follow

# 最近のエラーを検索
aws logs filter-log-events \
  --log-group-name /aws/lambda/apprunner-deployment-notification \
  --filter-pattern "ERROR"
```

### 6. EventBridgeルールの確認

```bash
# ルール一覧
aws events list-rules --name-prefix apprunner

# ターゲット確認
aws events list-targets-by-rule --rule apprunner-deployment-to-slack

# ルールの無効化/有効化
aws events disable-rule --name apprunner-deployment-to-slack
aws events enable-rule --name apprunner-deployment-to-slack
```

### 7. Terraform State のリフレッシュ

```bash
# 現在のAWS状態と同期
terraform refresh

# 差分確認
terraform plan

# 特定リソースのみターゲット
terraform apply -target=aws_apprunner_service.stock_api
```

### 8. App Runner デプロイ履歴の確認

```bash
# デプロイ履歴
aws apprunner list-operations \
  --service-arn <service-arn> \
  --max-results 10

# サービス詳細
aws apprunner describe-service --service-arn <service-arn>
```

---

## 📚 関連ドキュメント

- [Architecture Overview](../docs/ARCHITECTURE.md) - システム全体のアーキテクチャ
- [ROUTE53_SETUP.md](./ROUTE53_SETUP.md) - Route53セットアップガイド
- [GitHub Actions Workflows](../.github/workflows/) - CI/CDパイプライン

---

## 📝 Notes

- **削除されたリソース**:
  - `test-apprunner-all-events` EventBridgeルール（重複通知の原因で削除済み）

---

Generated with [Claude Code](https://claude.com/claude-code)
