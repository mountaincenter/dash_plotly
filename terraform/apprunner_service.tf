# App Runner Service
resource "aws_apprunner_service" "stock_api" {
  service_name = "stock-api"

  source_configuration {
    authentication_configuration {
      access_role_arn = aws_iam_role.apprunner_ecr_access.arn
    }

    image_repository {
      image_identifier      = "${aws_ecr_repository.stock_api.repository_url}:latest"
      image_repository_type = "ECR"

      image_configuration {
        port = "8000"

        runtime_environment_variables = {
          AWS_REGION         = "ap-northeast-1"
          DATA_BUCKET        = aws_s3_bucket.stock_api_data.bucket
          PARQUET_PREFIX     = "parquet"
          MASTER_META_KEY    = "parquet/meta.parquet"
          ALL_STOCKS_KEY     = "parquet/all_stocks.parquet"
          CORE30_PRICES_KEY  = "parquet/prices_max_1d.parquet"
        }
      }
    }

    auto_deployments_enabled = true
  }

  instance_configuration {
    cpu               = "1024"
    memory            = "2048"
    instance_role_arn = aws_iam_role.apprunner_instance.arn
  }

  health_check_configuration {
    protocol            = "TCP"
    path                = "/"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 1
    unhealthy_threshold = 5
  }

  network_configuration {
    ingress_configuration {
      is_publicly_accessible = true
    }

    ip_address_type = "IPV4"
  }

  tags = {
    Name        = "stock-api"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# Output
output "apprunner_service_url" {
  description = "App Runner service URL"
  value       = "https://${aws_apprunner_service.stock_api.service_url}"
}

output "apprunner_service_arn" {
  description = "App Runner service ARN"
  value       = aws_apprunner_service.stock_api.arn
}

output "apprunner_service_id" {
  description = "App Runner service ID"
  value       = aws_apprunner_service.stock_api.service_id
}
