# 基本情報
output "region" {
  description = "Current AWS region"
  value       = data.aws_region.current.id
}

output "account_id" {
  description = "Current AWS account ID"
  value       = data.aws_caller_identity.current.account_id
}

# Lambda関数
output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.apprunner_notification.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.apprunner_notification.arn
}

# EventBridge
output "eventbridge_rule_name" {
  description = "EventBridge rule name"
  value       = aws_cloudwatch_event_rule.apprunner_deployment.name
}

output "eventbridge_rule_arn" {
  description = "EventBridge rule ARN"
  value       = aws_cloudwatch_event_rule.apprunner_deployment.arn
}
