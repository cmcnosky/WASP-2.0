terraform {
  required_version = ">= 1.8.0, < 2.0.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.5"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.55"
    }
  }

  # Supply an environment-specific S3 backend configuration at init time.
  # Never share a state object between paper and live.
  backend "s3" {}
}
