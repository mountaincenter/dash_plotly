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

# Cognito
output "cognito_user_pool_id" {
  description = "Cognito User Pool ID"
  value       = aws_cognito_user_pool.main.id
}

output "cognito_user_pool_arn" {
  description = "Cognito User Pool ARN"
  value       = aws_cognito_user_pool.main.arn
}

output "cognito_user_pool_domain" {
  description = "Cognito User Pool Domain"
  value       = aws_cognito_user_pool_domain.main.domain
}

output "cognito_user_pool_endpoint" {
  description = "Cognito User Pool Endpoint"
  value       = aws_cognito_user_pool.main.endpoint
}

output "cognito_client_id_nextjs" {
  description = "Cognito Client ID for Next.js"
  value       = aws_cognito_user_pool_client.nextjs.id
}

output "cognito_client_id_api" {
  description = "Cognito Client ID for API"
  value       = aws_cognito_user_pool_client.api.id
}

output "cognito_hosted_ui_url" {
  description = "Cognito Hosted UI URL"
  value       = "https://${aws_cognito_user_pool_domain.main.domain}.auth.${data.aws_region.current.id}.amazoncognito.com"
}
