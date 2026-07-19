variable "project_name" {
  description = "Stable lowercase project identifier used in AWS names and tags."
  type        = string
  default     = "alpaca-autotrader"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,31}$", var.project_name))
    error_message = "project_name must be 3-32 lowercase letters, numbers, or hyphens."
  }
}

variable "environment" {
  description = "Trust domain. Broker endpoints are derived from this value in application code."
  type        = string

  validation {
    condition     = contains(["paper", "live"], var.environment)
    error_message = "environment must be paper or live."
  }
}

variable "expected_aws_account_id" {
  description = "Exact AWS account permitted for this environment."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_aws_account_id))
    error_message = "expected_aws_account_id must be exactly 12 digits."
  }
}

variable "aws_region" {
  description = "AWS region selected by the recorded region benchmark."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = contains(["us-east-1", "us-east-2"], var.aws_region)
    error_message = "The v1 benchmark scope permits only us-east-1 or us-east-2."
  }
}

variable "vpc_cidr" {
  description = "Environment-unique VPC CIDR."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "Two public egress subnet CIDRs, one per availability zone."
  type        = list(string)
  default     = ["10.42.0.0/24", "10.42.1.0/24"]

  validation {
    condition     = length(var.public_subnet_cidrs) == 2
    error_message = "Exactly two public subnet CIDRs are required."
  }
}

variable "private_app_subnet_cidrs" {
  description = "Two private application subnet CIDRs, one per availability zone."
  type        = list(string)
  default     = ["10.42.10.0/24", "10.42.11.0/24"]

  validation {
    condition     = length(var.private_app_subnet_cidrs) == 2
    error_message = "Exactly two private application subnet CIDRs are required."
  }
}

variable "private_db_subnet_cidrs" {
  description = "Two isolated database subnet CIDRs, one per availability zone."
  type        = list(string)
  default     = ["10.42.20.0/24", "10.42.21.0/24"]

  validation {
    condition     = length(var.private_db_subnet_cidrs) == 2
    error_message = "Exactly two private database subnet CIDRs are required."
  }
}

variable "container_image_digest" {
  description = "Immutable sha256 digest already present in this environment's ECR repository."
  type        = string

  validation {
    condition     = can(regex("^sha256:[0-9a-f]{64}$", var.container_image_digest))
    error_message = "container_image_digest must be sha256 followed by 64 lowercase hex characters."
  }
}

variable "container_cpu" {
  description = "Fargate CPU units."
  type        = number
  default     = 512

  validation {
    condition     = contains([256, 512, 1024, 2048], var.container_cpu)
    error_message = "container_cpu must be a supported bounded v1 size."
  }
}

variable "container_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 1024

  validation {
    condition     = var.container_memory >= 512 && var.container_memory <= 8192
    error_message = "container_memory must be between 512 and 8192 MiB."
  }
}

variable "execution_mode" {
  description = "Broker mutation authority requested for this deployment. Defaults fail-closed."
  type        = string
  default     = "read_only"

  validation {
    condition     = contains(["read_only", "paper", "live"], var.execution_mode)
    error_message = "execution_mode must be read_only, paper, or live."
  }
}

variable "deploy_application" {
  description = "Start the ECS task only after a real long-running reconcile loop is implemented and approved."
  type        = bool
  default     = false
}

variable "runtime_ready_approval_id" {
  description = "Non-secret evidence reference proving the image has an approved long-running runtime entrypoint."
  type        = string
  default     = null
  nullable    = true
}

variable "live_activation_approval_id" {
  description = "Non-secret operator approval reference required for live execution mode."
  type        = string
  default     = null
  nullable    = true
}

variable "db_instance_class" {
  description = "Optional bounded override; defaults differ for paper/live."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.db_instance_class == null || contains([
      "db.t4g.micro",
      "db.t4g.small",
      "db.t4g.medium"
    ], var.db_instance_class)
    error_message = "db_instance_class must be a reviewed small Graviton class."
  }
}

variable "database_name" {
  description = "Initial PostgreSQL database name."
  type        = string
  default     = "alpaca_autotrader"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,62}$", var.database_name))
    error_message = "database_name must be a valid lowercase PostgreSQL identifier."
  }
}

variable "database_username" {
  description = "RDS bootstrap administrator name; password is managed by RDS."
  type        = string
  default     = "trader_admin"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,30}$", var.database_username))
    error_message = "database_username must be a valid lowercase identifier."
  }
}

variable "alert_email" {
  description = "Optional operator address. AWS requires subscription confirmation."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.alert_email == null || can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", var.alert_email))
    error_message = "alert_email must be null or a plausible email address."
  }
}

variable "monthly_budget_usd" {
  description = "Account-level operating ceiling; use only in a dedicated environment account."
  type        = number
  default     = 1000

  validation {
    condition     = var.monthly_budget_usd > 0 && var.monthly_budget_usd <= 1000
    error_message = "monthly_budget_usd must be positive and no greater than the $1,000 ceiling."
  }
}

variable "create_account_budget" {
  description = "Create an account-level AWS budget; enable only when this is a dedicated account and alert_email is set."
  type        = bool
  default     = false
}

variable "github_repository" {
  description = "GitHub owner/repository allowed to assume the environment release role."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must be owner/repository."
  }
}

variable "github_oidc_provider_arn" {
  description = "Existing GitHub OIDC provider ARN. Leave null to create one in a dedicated AWS account."
  type        = string
  default     = null
  nullable    = true
}
