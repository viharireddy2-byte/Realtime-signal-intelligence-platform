terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }

  # Local state is fine for review/plan. Before any real `apply`, uncomment
  # and point this at a real bucket + lock table:
  #
  # backend "s3" {
  #   bucket         = "CHANGE-ME-signal-intel-tfstate"
  #   key            = "signal-intel-platform/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "CHANGE-ME-signal-intel-tf-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "signal-intel-platform"
      ManagedBy   = "terraform"
      Environment = var.environment_size
    }
  }
}
