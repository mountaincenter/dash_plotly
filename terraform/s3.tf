# S3 Bucket for Stock API Data
resource "aws_s3_bucket" "stock_api_data" {
  bucket = "stock-api-data"

  tags = {
    Name        = "stock-api-data"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "stock_api_data" {
  bucket = aws_s3_bucket.stock_api_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "stock_api_data" {
  bucket = aws_s3_bucket.stock_api_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "stock_api_data" {
  bucket = aws_s3_bucket.stock_api_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle rule for old versions (optional - keeps last 30 days of versions)
resource "aws_s3_bucket_lifecycle_configuration" "stock_api_data" {
  bucket = aws_s3_bucket.stock_api_data.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# Output bucket name and ARN
output "s3_bucket_name" {
  description = "S3 bucket name for stock API data"
  value       = aws_s3_bucket.stock_api_data.bucket
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN for stock API data"
  value       = aws_s3_bucket.stock_api_data.arn
}
