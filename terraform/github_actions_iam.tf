# GitHub Actions OIDC Provider (既存をimport)
data "aws_iam_openid_connect_provider" "github_actions" {
  arn = "arn:aws:iam::980921748690:oidc-provider/token.actions.githubusercontent.com"
}

# GitHub Actions IAM Role
resource "aws_iam_role" "github_actions" {
  name = "GitHubActions-DashPlotly"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = data.aws_iam_openid_connect_provider.github_actions.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:mountaincenter/dash_plotly:*"
          }
        }
      }
    ]
  })

  tags = {
    Name        = "GitHubActions-DashPlotly"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# S3 Sync Policy (parquetディレクトリへのアクセス)
resource "aws_iam_policy" "github_actions_s3_sync" {
  name = "dash-plotly-s3-sync-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucketPrefixOnly"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.stock_api_data.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "parquet/*",
              "parquet/"
            ]
          }
        }
      },
      {
        Sid    = "ObjectsRWForSync"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:RestoreObject"
        ]
        Resource = "${aws_s3_bucket.stock_api_data.arn}/parquet/*"
      }
    ]
  })
}

# Attach S3 Sync Policy
resource "aws_iam_role_policy_attachment" "github_actions_s3_sync" {
  role       = aws_iam_role.github_actions.name
  policy_arn = aws_iam_policy.github_actions_s3_sync.arn
}

# ECR Push Policy
resource "aws_iam_policy" "github_actions_ecr_push" {
  name = "dash-plotly-ecr-push-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AuthToken"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid    = "PushPullOnRepo"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:PutImage",
          "ecr:DescribeImages"
        ]
        Resource = "arn:aws:ecr:ap-northeast-1:980921748690:repository/stock-api"
      },
      {
        Sid      = "DescribeRepo"
        Effect   = "Allow"
        Action   = "ecr:DescribeRepositories"
        Resource = "*"
      }
    ]
  })
}

# Attach ECR Push Policy
resource "aws_iam_role_policy_attachment" "github_actions_ecr_push" {
  role       = aws_iam_role.github_actions.name
  policy_arn = aws_iam_policy.github_actions_ecr_push.arn
}

# Outputs
output "github_actions_role_arn" {
  description = "GitHub Actions IAM role ARN"
  value       = aws_iam_role.github_actions.arn
}

output "github_actions_role_name" {
  description = "GitHub Actions IAM role name"
  value       = aws_iam_role.github_actions.name
}
