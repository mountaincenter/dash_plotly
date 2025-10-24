# AWS リージョン
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

# App Runner サービス ARN
variable "apprunner_service_arn" {
  description = "App Runner service ARN"
  type        = string
  default     = "arn:aws:apprunner:ap-northeast-1:980921748690:service/dash-plotly/4d8b5908dadd4971aafb4fae072d5ee6"
}

# App Runner サービス URL
variable "apprunner_service_url" {
  description = "App Runner service URL"
  type        = string
  default     = "https://YOUR-APP-RUNNER-URL.awsapprunner.com"
}

# Slack Webhook URL（機密情報なので環境変数やSecrets Managerから取得推奨）
variable "slack_webhook_url" {
  description = "Slack Incoming Webhook URL"
  type        = string
  sensitive   = true
}
