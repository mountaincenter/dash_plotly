# AWS リージョン
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-1"
}

# 環境名（production / staging）
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["production", "staging"], var.environment)
    error_message = "environment must be 'production' or 'staging'"
  }
}

# Slack Webhook URL（機密情報なので環境変数やSecrets Managerから取得推奨）
variable "slack_webhook_url" {
  description = "Slack Incoming Webhook URL"
  type        = string
  sensitive   = true
}
