# App Runner ECR Access Role
resource "aws_iam_role" "apprunner_ecr_access" {
  name = "AppRunnerECRAccessRole-stock-api"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "build.apprunner.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "AppRunnerECRAccessRole-stock-api"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# Attach ECR read policy
resource "aws_iam_role_policy_attachment" "apprunner_ecr_access" {
  role       = aws_iam_role.apprunner_ecr_access.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

# App Runner Instance Role (for accessing S3, etc.)
resource "aws_iam_role" "apprunner_instance" {
  name = "AppRunnerInstanceRole-stock-api"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "tasks.apprunner.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "AppRunnerInstanceRole-stock-api"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# S3 read policy for App Runner instance
resource "aws_iam_role_policy" "apprunner_s3_read" {
  name = "S3ReadPolicy"
  role = aws_iam_role.apprunner_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.stock_api_data.arn,
          "${aws_s3_bucket.stock_api_data.arn}/*"
        ]
      }
    ]
  })
}

# Output
output "apprunner_ecr_access_role_arn" {
  description = "App Runner ECR access role ARN"
  value       = aws_iam_role.apprunner_ecr_access.arn
}

output "apprunner_instance_role_arn" {
  description = "App Runner instance role ARN"
  value       = aws_iam_role.apprunner_instance.arn
}
