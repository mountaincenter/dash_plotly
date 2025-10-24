# EventBridge ルール: App Runner のステータス変更を検知
resource "aws_cloudwatch_event_rule" "apprunner_status_change" {
  name        = "apprunner-deployment-to-slack"
  description = "Trigger Lambda when App Runner deployment completes"

  event_pattern = jsonencode({
    source      = ["aws.apprunner"]
    detail-type = ["AppRunner Service Status Change"]
    detail = {
      serviceArn = [var.apprunner_service_arn]
      status     = ["RUNNING"]
    }
  })

  tags = {
    Name        = "apprunner-deployment-to-slack"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# EventBridge ターゲット: Lambda関数を呼び出す
resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.apprunner_status_change.name
  target_id = "SendToLambda"
  arn       = aws_lambda_function.apprunner_notification.arn
}
