data "archive_file" "github_workflow_dispatch_lambda_zip" {
  type        = "zip"
  source_file = "../lambda/github_workflow_dispatch.py"
  output_path = "${path.module}/github_workflow_dispatch_lambda.zip"
}

resource "aws_iam_role" "github_workflow_dispatch_lambda" {
  name = "github-workflow-dispatch-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "github-workflow-dispatch-lambda-role"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_iam_role_policy_attachment" "github_workflow_dispatch_lambda_basic_execution" {
  role       = aws_iam_role.github_workflow_dispatch_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "github_workflow_dispatch_parameter_access" {
  name        = "github-workflow-dispatch-parameter-access"
  description = "Allow GitHub workflow dispatch Lambda to read only its token parameter"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadGithubTokenParameter"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter"
        ]
        Resource = "arn:aws:ssm:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:parameter${var.github_dispatch_token_parameter_name}"
      },
      {
        Sid    = "DecryptSsmSecureStringViaSsm"
        Effect = "Allow"
        Action = [
          "kms:Decrypt"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:CallerAccount" = data.aws_caller_identity.current.account_id
            "kms:ViaService"    = "ssm.${data.aws_region.current.id}.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_workflow_dispatch_parameter_access" {
  role       = aws_iam_role.github_workflow_dispatch_lambda.name
  policy_arn = aws_iam_policy.github_workflow_dispatch_parameter_access.arn
}

resource "aws_lambda_function" "github_workflow_dispatch" {
  filename         = data.archive_file.github_workflow_dispatch_lambda_zip.output_path
  function_name    = "github-workflow-dispatch"
  role             = aws_iam_role.github_workflow_dispatch_lambda.arn
  handler          = "github_workflow_dispatch.lambda_handler"
  source_code_hash = data.archive_file.github_workflow_dispatch_lambda_zip.output_base64sha256
  runtime          = "python3.12"
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      GITHUB_API_VERSION          = "2022-11-28"
      GITHUB_OWNER                = var.github_dispatch_owner
      GITHUB_REF                  = var.github_dispatch_ref
      GITHUB_REPO                 = var.github_dispatch_repo
      GITHUB_TOKEN_PARAMETER_NAME = var.github_dispatch_token_parameter_name
      GITHUB_WORKFLOW_ID          = var.github_dispatch_workflow_id
    }
  }

  tags = {
    Name        = "github-workflow-dispatch"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

output "github_workflow_dispatch_lambda_name" {
  description = "GitHub workflow dispatch Lambda function name"
  value       = aws_lambda_function.github_workflow_dispatch.function_name
}

output "github_workflow_dispatch_lambda_arn" {
  description = "GitHub workflow dispatch Lambda function ARN"
  value       = aws_lambda_function.github_workflow_dispatch.arn
}
