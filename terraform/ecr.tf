# ECR Repository for stock-api
resource "aws_ecr_repository" "stock_api" {
  name                 = "stock-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name        = "stock-api"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# ECR Lifecycle Policy - keep last 10 images
resource "aws_ecr_lifecycle_policy" "stock_api" {
  repository = aws_ecr_repository.stock_api.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 images"
        selection = {
          tagStatus     = "any"
          countType     = "imageCountMoreThan"
          countNumber   = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Output
output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.stock_api.repository_url
}

output "ecr_repository_arn" {
  description = "ECR repository ARN"
  value       = aws_ecr_repository.stock_api.arn
}

# ==========================================
# Staging ECR Repository
# ==========================================

resource "aws_ecr_repository" "stock_api_staging" {
  name                 = "stock-api-staging"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name        = "stock-api-staging"
    Environment = "staging"
    ManagedBy   = "terraform"
  }
}

resource "aws_ecr_lifecycle_policy" "stock_api_staging" {
  repository = aws_ecr_repository.stock_api_staging.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

output "ecr_repository_staging_url" {
  description = "ECR repository URL for staging"
  value       = aws_ecr_repository.stock_api_staging.repository_url
}

output "ecr_repository_staging_arn" {
  description = "ECR repository ARN for staging"
  value       = aws_ecr_repository.stock_api_staging.arn
}
