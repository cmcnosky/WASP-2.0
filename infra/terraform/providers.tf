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

check "aws_account_is_exact" {
  assert {
    condition     = data.aws_caller_identity.current.account_id == var.expected_aws_account_id
    error_message = "The authenticated AWS account does not match expected_aws_account_id."
  }
}

check "execution_mode_matches_environment" {
  assert {
    condition = (
      var.execution_mode == "read_only" ||
      (var.environment == "paper" && var.execution_mode == "paper") ||
      (
        var.environment == "live" &&
        var.execution_mode == "live" &&
        try(length(trimspace(var.live_activation_approval_id)), 0) >= 8
      )
    )
    error_message = "Paper/live mutation mode must match its environment; live also requires an explicit approval reference."
  }
}

check "runtime_is_deployable" {
  assert {
    condition = (
      !var.deploy_application ||
      try(length(trimspace(var.runtime_ready_approval_id)), 0) >= 8
    )
    error_message = "deploy_application requires reviewed evidence for a real long-running reconcile runtime."
  }

  assert {
    condition     = var.execution_mode == "read_only" || var.deploy_application
    error_message = "Broker mutation mode cannot be requested while the application task is disabled."
  }
}

check "account_budget_has_destination" {
  assert {
    condition     = !var.create_account_budget || var.alert_email != null
    error_message = "create_account_budget requires alert_email."
  }
}

check "live_has_alert_destination" {
  assert {
    condition     = !local.is_live || var.alert_email != null
    error_message = "Live infrastructure requires a confirmed operator alert email."
  }
}
