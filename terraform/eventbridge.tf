# EventBridge ルール: App Runner のデプロイ完了を検知
resource "aws_cloudwatch_event_rule" "apprunner_deployment" {
  name        = "apprunner-deployment-to-slack"
  description = "Trigger Lambda when App Runner deployment completes"

  event_pattern = jsonencode({
    source      = ["aws.apprunner"]
    detail-type = ["AppRunner Service Operation Status Change"]
    # 新しいstock-apiサービスのARNを使用
    resources   = [aws_apprunner_service.stock_api.arn]
    detail = {
      operationStatus = ["DeploymentCompletedSuccessfully", "UpdateServiceCompletedSuccessfully"]
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
  rule      = aws_cloudwatch_event_rule.apprunner_deployment.name
  target_id = "SendToLambda"
  arn       = aws_lambda_function.apprunner_notification.arn
}
