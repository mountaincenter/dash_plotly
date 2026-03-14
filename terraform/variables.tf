# AWS リージョン
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

# Slack Webhook URL（機密情報なので環境変数やSecrets Managerから取得推奨）
variable "slack_webhook_url" {
  description = "Slack Incoming Webhook URL"
  type        = string
  sensitive   = true
}
