# Stock API Architecture

## 📋 Overview

このプロジェクトは、株式データの自動収集・処理・配信を行うフルマネージドなサーバーレスアーキテクチャです。

- **データソース**: J-Quants API, yFinance
- **AI 分析**: Grok (xAI) によるトレンド銘柄選定
- **インフラ**: AWS (S3, ECR, App Runner, EventBridge, Lambda, Route53)
- **CI/CD**: GitHub Actions
- **IaC**: Terraform

---

## 🏗️ System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          GitHub Actions                              │
│                                                                       │
│  ┌──────────────────┐         ┌──────────────────┐                  │
│  │  Data Pipeline   │         │  ECR Deploy      │                  │
│  │  (毎日16:00/26:00)│────────▶│  (コード変更時)   │                  │
│  └────────┬─────────┘         └────────┬─────────┘                  │
│           │                            │                             │
└───────────┼────────────────────────────┼─────────────────────────────┘
            │                            │
            ▼                            ▼
    ┌───────────────┐           ┌───────────────┐
    │   S3 Bucket   │           │  ECR Registry │
    │stock-api-data │           │   stock-api   │
    └───────┬───────┘           └───────┬───────┘
            │                           │
            │                           ▼
            │                  ┌─────────────────┐
            │                  │   App Runner    │
            │                  │   stock-api     │
            │                  │  (Auto Deploy)  │
            │                  └────────┬────────┘
            │                           │
            └───────────────────────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │  Route53 Domain  │
                              │stock.api.ymnk.jp │
                              └──────────────────┘
```

### Event-Driven Notification Flow

```
┌─────────────────┐
│   App Runner    │
│  (Deployment)   │
└────────┬────────┘
         │
         │ イベント発行
         ▼
┌─────────────────┐
│  EventBridge    │
│  Rule: apprunner│
│  -deployment    │
└────────┬────────┘
         │
         │ トリガー
         ▼
┌─────────────────┐
│  Lambda         │
│  apprunner-     │
│  notification   │
└────────┬────────┘
         │
         │ Webhook
         ▼
┌─────────────────┐
│  Slack Channel  │
│  デプロイ通知    │
└─────────────────┘
```

---

## 🗂️ Data Pipeline Architecture

### Daily Data Update Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                     GitHub Actions - Data Pipeline               │
│                                                                  │
│  1. Check Trading Day                                            │
│     ├─ J-Quants API: 営業日カレンダー取得                          │
│     └─ 16:00-02:00 JST の実行ウィンドウ確認                        │
│                                                                  │
│  2. Fetch Stock Data                                             │
│     ├─ J-Quants API: 上場銘柄情報 (meta_jquants.parquet)          │
│     │   └─ フィルタ: プライム/スタンダード/グロース (3,790銘柄)       │
│     ├─ yFinance: 株価データ (prices_max_1d.parquet)               │
│     └─ CSV: Core30 + 政策銘柄 (meta.parquet)                      │
│                                                                  │
│  3. AI Analysis (Grok)                                           │
│     ├─ 銘柄スクリーニング: 出来高・ボラティリティ・時価総額          │
│     ├─ Grok API: トレンド分析・材料検証                            │
│     └─ 出力: grok_trending.parquet (12銘柄)                       │
│                                                                  │
│  4. Generate all_stocks.parquet                                  │
│     └─ meta_jquants + prices + indicators を結合                  │
│                                                                  │
│  5. Upload to S3                                                 │
│     ├─ S3 Bucket: stock-api-data                                 │
│     ├─ Prefix: parquet/                                          │
│     └─ Files: meta.parquet, meta_jquants.parquet,                │
│               prices_max_1d.parquet, all_stocks.parquet,         │
│               grok_trending.parquet                              │
│                                                                  │
│  6. Archive for Backtest                                         │
│     └─ S3: parquet/backtest/grok_trending_YYYYMMDD.parquet       │
│         (7日間保持)                                               │
│                                                                  │
│  7. Slack Notification                                           │
│     └─ 改善版フォーマット: 全銘柄・セクション分割                    │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Trigger ECR Deployment                       │
│                                                                  │
│  workflow_call → deploy-ecr.yml                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🔐 IAM & Security

### GitHub Actions OIDC

```
GitHub Actions
     │
     │ AssumeRole (OIDC)
     ▼
┌─────────────────────────────────────┐
│ GitHubActions-DashPlotly (IAM Role) │
│                                     │
│ Policies:                           │
│ ├─ dash-plotly-s3-sync-policy      │
│ │  └─ s3:GetObject, PutObject      │
│ │     Resource: stock-api-data     │
│ │                                  │
│ └─ dash-plotly-ecr-push-policy     │
│    └─ ecr:PushImage, BatchCheck    │
│       Resource: stock-api          │
└─────────────────────────────────────┘
```

### App Runner IAM Roles

```
┌──────────────────────────────────────┐
│ AppRunnerECRAccessRole-stock-api     │
│ (ECRアクセス用)                       │
│                                      │
│ Trust: build.apprunner.amazonaws.com │
│ Policy: AWSAppRunnerServicePolicy... │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│ AppRunnerInstanceRole-stock-api      │
│ (S3アクセス用)                        │
│                                      │
│ Trust: tasks.apprunner.amazonaws.com │
│ Policy:                              │
│ └─ s3:GetObject, ListBucket          │
│    Resource: stock-api-data          │
└──────────────────────────────────────┘
```

---

## 📊 Data Files

### S3 Bucket Structure

```
s3://stock-api-data/
├── parquet/
│   ├── meta.parquet              # Core30 + 政策銘柄 (静的)
│   ├── meta_jquants.parquet      # J-Quants 全銘柄 (3,790銘柄)
│   ├── prices_max_1d.parquet     # 日足株価データ
│   ├── all_stocks.parquet        # 統合データ (meta + prices + indicators)
│   ├── grok_trending.parquet     # GROK選定銘柄 (12銘柄)
│   ├── manifest.json             # メタデータ
│   └── backtest/
│       ├── grok_trending_20251025.parquet
│       ├── grok_trending_20251024.parquet
│       └── ... (7日間保持)
```

### Data Schema

#### meta_jquants.parquet

```python
{
    "ticker": str,           # 例: "7203.T"
    "code": str,             # 例: "7203"
    "stock_name": str,       # 例: "トヨタ自動車"
    "market": str,           # プライム/スタンダード/グロース
    "sectors": str,          # 33業種
    "series": str,           # 17業種
    "topixnewindexseries": str  # TOPIX区分
}
```

#### grok_trending.parquet

```python
{
    "ticker": str,           # 例: "4592.T"
    "stock_name": str,       # 例: "サンバイオ"
    "tags": str,             # 例: "バイオ材料+株クラバズ"
    "reason": str,           # 選定理由 (100文字)
    "selected_time": str,    # 例: "16:00"
    "selected_date": str     # 例: "2025-10-25"
}
```

---

## 🚀 Deployment Flow

### 1. Code Change → ECR Build

```
1. git push origin main
   ↓
2. GitHub Actions: deploy-ecr.yml
   ├─ Docker build
   ├─ ECR login
   └─ Push: stock-api:latest, stock-api:<git-sha>
   ↓
3. App Runner: Auto Deploy (enabled)
   ├─ Pull latest image
   ├─ Deploy new version
   └─ Health check
   ↓
4. EventBridge: Deployment event
   ↓
5. Lambda: Slack notification
```

### 2. Data Update → App Deployment

```
1. GitHub Actions: data-pipeline.yml (Scheduled)
   ├─ 16:00 JST (UTC 07:00)
   └─ 26:00 JST (UTC 17:00)
   ↓
2. Data Pipeline Execution
   ├─ Fetch data from J-Quants, yFinance
   ├─ Generate GROK trending
   ├─ Upload to S3
   └─ Slack: GROK銘柄通知 (改善版フォーマット)
   ↓
3. workflow_call: deploy-ecr.yml
   ├─ Build & Push Docker image
   └─ Trigger App Runner deployment
   ↓
4. EventBridge + Lambda
   └─ Slack: デプロイ完了通知
```

---

## 🛠️ Infrastructure as Code (Terraform)

### Terraform Modules

```
terraform/
├── main.tf                      # Provider設定
├── variables.tf                 # 変数定義
├── terraform.tfvars.example     # 変数サンプル
├── outputs.tf                   # 出力値
│
├── s3.tf                        # S3バケット
├── ecr.tf                       # ECRレジストリ
├── apprunner_service.tf         # App Runnerサービス
├── apprunner_iam.tf             # App Runner IAMロール
├── apprunner_custom_domain.tf   # カスタムドメイン
├── route53.tf                   # Route53ホストゾーン
├── lambda.tf                    # Lambda関数
├── eventbridge.tf               # EventBridgeルール
└── iam.tf                       # Lambda IAMロール
```

### Key Resources

| Resource Type      | Name                                | Purpose          |
| ------------------ | ----------------------------------- | ---------------- |
| S3 Bucket          | `stock-api-data`                    | データストレージ |
| ECR Repository     | `stock-api`                         | Docker イメージ  |
| App Runner Service | `stock-api`                         | API サーバー     |
| Route53 Zone       | `api.ymnk.jp`                       | DNS 管理         |
| Lambda Function    | `apprunner-deployment-notification` | デプロイ通知     |
| EventBridge Rule   | `apprunner-deployment-to-slack`     | イベント検知     |

### Terraform State

```bash
# 初期化
terraform init

# プラン確認
terraform plan

# 適用
terraform apply

# 特定リソースのみ適用
terraform apply -target=aws_apprunner_service.stock_api
```

---

## 🔧 Configuration

### Environment Variables (App Runner)

```bash
AWS_REGION=ap-northeast-1
DATA_BUCKET=stock-api-data
PARQUET_PREFIX=parquet
MASTER_META_KEY=parquet/meta.parquet
ALL_STOCKS_KEY=parquet/all_stocks.parquet
CORE30_PRICES_KEY=parquet/prices_max_1d.parquet
```

### GitHub Secrets

```bash
# AWS
AWS_ROLE_ARN=arn:aws:iam::980921748690:role/GitHubActions-DashPlotly
AWS_REGION=ap-northeast-1

# API Keys
JQUANTS_REFRESH_TOKEN=<token>
XAI_API_KEY=<key>

# Slack
SLACK_INCOMING_WEBHOOK_URL=<url>
```

---

## 📈 Monitoring & Observability

### CloudWatch Logs

```
/aws/apprunner/stock-api/<service-id>/application
└─ App Runnerアプリケーションログ

/aws/lambda/apprunner-deployment-notification
└─ Lambda実行ログ
```

### EventBridge Events

```javascript
{
  "source": "aws.apprunner",
  "detail-type": "AppRunner Service Operation Status Change",
  "detail": {
    "operationStatus": [
      "DeploymentCompletedSuccessfully",
      "UpdateServiceCompletedSuccessfully"
    ]
  }
}
```

### Slack Notifications

1. **Data Pipeline Success**

   - データ統計 (銘柄数、最終更新日)
   - GROK 銘柄リスト (改善版フォーマット)

2. **App Runner Deployment**
   - デプロイ完了通知
   - サービス URL

---

## 🔄 Workflow Schedule

### Data Pipeline

- **Primary**: 16:00 JST (UTC 07:00) - メイン実行
- **Fallback**: 26:00 JST (UTC 17:00) - フォールバック
- **Condition**: 営業日の 16:00-02:00 ウィンドウ内

### Manual Trigger

```bash
# Data Pipeline (workflow_dispatch)
- skip_trading_day_check: 営業日チェックをスキップ
- force_meta_jquants: meta_jquants強制更新

# Slack Notification Test
- grok_formatted_all: 全銘柄表示（改善版）
- grok_formatted: 5銘柄表示（改善版）
- grok_current: 現在のフォーマット
- pipeline_success: パイプライン成功通知
```

---

## 🎯 Performance & Scalability

### App Runner Configuration

```
CPU: 1024 (1 vCPU)
Memory: 2048 MB
Auto Scaling: Enabled
Min Instances: 1
Max Instances: 10
Concurrency: 100
```

### S3 Performance

```
Lifecycle:
- Noncurrent versions deleted after 30 days

Encryption: AES256

Public Access: Blocked
```

### ECR Image Lifecycle

```
Keep last 10 images
Expire untagged images after 7 days
```

---

## 🔒 Security Best Practices

1. **IAM Least Privilege**

   - GitHub Actions: S3/ECR 最小権限
   - App Runner: S3 読み取り専用

2. **Secrets Management**

   - GitHub Secrets: API keys, tokens
   - 環境変数: 非機密情報のみ

3. **Network Security**

   - App Runner: Public access (API 用)
   - S3: Bucket policy + Public access block

4. **Encryption**
   - S3: At-rest encryption (AES256)
   - ECR: Image scanning enabled

---

## 📚 Related Documentation

- [Terraform README](../terraform/README.md)
- [Route53 Setup Guide](../terraform/ROUTE53_SETUP.md)
- [Scalping Philosophy](./SCALPING_PHILOSOPHY.md)
- [GitHub Actions Schedule Verification](./github-actions-schedule-verification.md)

---

## 🆘 Troubleshooting

### Common Issues

1. **S3 Access Denied (403)**

   - IAM ポリシーのバケット名を確認
   - `stock-api-data` が正しいか確認

2. **ECR Push Failed**

   - IAM ポリシーのリポジトリ名を確認
   - `stock-api` が正しいか確認

3. **App Runner Deployment Failed**

   - `auto_deployments_enabled = true` を確認
   - ECR イメージが正しく push されているか確認

4. **Slack Notification Duplicate**

   - EventBridge ルールが重複していないか確認
   - `test-apprunner-all-events` が削除されているか確認

5. **GROK API Error**
   - `XAI_API_KEY` が正しく設定されているか確認
   - API 制限に達していないか確認

---

## 📝 Changelog

### 2025-10-25

- ✅ App Runner 自動デプロイ有効化
- ✅ GROK Slack 通知フォーマット改善（全銘柄表示）
- ✅ EventBridge 重複ルール削除
- ✅ IAM ポリシー修正（S3/ECR バケット名・リポジトリ名）
- ✅ 市場フィルタリング追加（3 市場のみ）

### 2025-10-24

- ✅ Terraform 全リソース作成
- ✅ Route53 カスタムドメイン設定
- ✅ EventBridge + Lambda 通知システム構築

---

## 👥 Contributors

Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
