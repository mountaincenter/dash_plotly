locals {
  github_workflow_dispatch_schedules = {
    stock_1645 = {
      description         = "Dispatch Data Pipeline at 16:45 JST"
      mode                = "stock_1645"
      name                = "dash-plotly-data-pipeline-stock-1645"
      schedule_expression = "cron(45 16 * * ? *)"
    }
    grok_2215 = {
      description         = "Dispatch Data Pipeline at 22:15 JST"
      mode                = "grok_2215"
      name                = "dash-plotly-data-pipeline-grok-2215"
      schedule_expression = "cron(15 22 * * ? *)"
    }
    cme_0700 = {
      description         = "Dispatch Data Pipeline at 07:00 JST"
      mode                = "cme_0700"
      name                = "dash-plotly-data-pipeline-cme-0700"
      schedule_expression = "cron(0 7 * * ? *)"
    }
  }
}

resource "aws_iam_role" "github_workflow_dispatch_scheduler" {
  name = "github-workflow-dispatch-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "github-workflow-dispatch-scheduler-role"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_iam_policy" "github_workflow_dispatch_scheduler_invoke" {
  name        = "github-workflow-dispatch-scheduler-invoke"
  description = "Allow EventBridge Scheduler to invoke GitHub workflow dispatch Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeGithubWorkflowDispatchLambda"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.github_workflow_dispatch.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_workflow_dispatch_scheduler_invoke" {
  role       = aws_iam_role.github_workflow_dispatch_scheduler.name
  policy_arn = aws_iam_policy.github_workflow_dispatch_scheduler_invoke.arn
}

resource "aws_scheduler_schedule" "github_workflow_dispatch" {
  for_each = local.github_workflow_dispatch_schedules

  name                         = each.value.name
  description                  = each.value.description
  schedule_expression          = each.value.schedule_expression
  schedule_expression_timezone = "Asia/Tokyo"
  state                        = var.github_dispatch_schedules_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.github_workflow_dispatch.arn
    role_arn = aws_iam_role.github_workflow_dispatch_scheduler.arn

    input = jsonencode({
      mode        = each.value.mode
      ref         = var.github_dispatch_ref
      repository  = "${var.github_dispatch_owner}/${var.github_dispatch_repo}"
      workflow_id = var.github_dispatch_workflow_id
    })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.github_workflow_dispatch_scheduler_invoke
  ]
}

output "github_workflow_dispatch_schedule_names" {
  description = "EventBridge Scheduler schedule names for GitHub workflow dispatch"
  value       = [for schedule in aws_scheduler_schedule.github_workflow_dispatch : schedule.name]
}
