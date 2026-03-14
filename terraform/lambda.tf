# Lambda関数のソースコードをzipにパッケージング
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "../lambda/apprunner_notification.py"
  output_path = "${path.module}/lambda_function.zip"
}

# Lambda関数
resource "aws_lambda_function" "apprunner_notification" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "apprunner-deployment-notification"
  role            = aws_iam_role.lambda_execution_role.arn
  handler         = "apprunner_notification.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime         = "python3.12"
  timeout         = 30

  environment {
    variables = {
      SLACK_WEBHOOK_URL = var.slack_webhook_url
      # 新しいstock-apiサービスのURLを動的に参照
      SERVICE_URL       = "https://${aws_apprunner_service.stock_api.service_url}"
    }
  }

  tags = {
    Name        = "apprunner-deployment-notification"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# EventBridgeからLambdaを呼び出す権限
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.apprunner_notification.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.apprunner_deployment.arn
}
