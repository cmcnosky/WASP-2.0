provider "aws" {
  region = var.aws_region

  allowed_account_ids = [var.expected_aws_account_id]

  default_tags {
    tags = {
      Application = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      PrivateUse  = "true"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name_prefix        = "${var.project_name}-${var.environment}"
  is_live            = var.environment == "live"
  availability_zones = slice(data.aws_availability_zones.available.names, 0, 2)

  execution_mode_matches_environment = (
    var.execution_mode == "read_only" ||
    (var.environment == "paper" && var.execution_mode == "paper") ||
    (var.environment == "live" && var.execution_mode == "live")
  )
  live_activation_is_referenced = (
    var.execution_mode != "live" ||
    try(length(trimspace(var.live_activation_approval_id)), 0) >= 8
  )
  runtime_is_approved = (
    !var.deploy_application ||
    try(length(trimspace(var.runtime_ready_approval_id)), 0) >= 8
  )
  deployment_is_paper_read_only = (
    !var.deploy_application ||
    (var.environment == "paper" && var.execution_mode == "read_only")
  )
  # Keep deployment mechanically unavailable until the binary has a tested,
  # long-running observer entrypoint. An approval reference cannot substitute
  # for code that does not yet exist.
  observer_entrypoint_is_implemented = false
  deployment_has_observer_entrypoint = (
    !var.deploy_application || local.observer_entrypoint_is_implemented
  )
  mutation_has_runtime = var.execution_mode == "read_only" || var.deploy_application
  deployment_has_real_ca_digest = (
    !var.deploy_application ||
    var.expected_rds_ca_bundle_sha256 != "0000000000000000000000000000000000000000000000000000000000000000"
  )
  fargate_cpu_memory_pair_is_supported = (
    (var.container_cpu == 256 && contains([512, 1024, 2048], var.container_memory)) ||
    (var.container_cpu == 512 && contains([1024, 2048, 3072, 4096], var.container_memory)) ||
    (var.container_cpu == 1024 && var.container_memory >= 2048 && var.container_memory <= 8192 && var.container_memory % 1024 == 0) ||
    (var.container_cpu == 2048 && var.container_memory >= 4096 && var.container_memory <= 8192 && var.container_memory % 1024 == 0)
  )

  db_instance_class = coalesce(
    var.db_instance_class,
    local.is_live ? "db.t4g.small" : "db.t4g.micro"
  )
  backup_retention_days = local.is_live ? 35 : 7
  object_retention_days = local.is_live ? 2555 : 365
  nat_gateway_count     = local.is_live ? 2 : 1
  metric_namespace      = "AlpacaAutotrader/${var.environment}"

  common_tags = {
    DataClassification = "confidential"
    TradingAuthority   = var.execution_mode
  }
}
