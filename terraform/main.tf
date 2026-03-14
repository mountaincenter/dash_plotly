terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "ap-northeast-1"
}

# データソースで現在のリージョンを取得
data "aws_region" "current" {}

# データソースで現在のアカウントIDを取得
data "aws_caller_identity" "current" {}
