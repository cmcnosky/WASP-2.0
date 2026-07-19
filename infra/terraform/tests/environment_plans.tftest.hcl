mock_provider "aws" {}
mock_provider "archive" {}

override_data {
  target = data.aws_iam_policy_document.ecs_task_assume
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.ecs_execution_secrets
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.app
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.github_release_assume
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.github_release
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.rds_monitor_assume
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.alerts
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.lambda_assume
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.deadman
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.kms
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.bucket_tls["data"]
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_iam_policy_document.bucket_tls["audit"]
  values = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
}

override_data {
  target = data.aws_caller_identity.current
  values = {
    account_id = "111111111111"
    arn        = "arn:aws:iam::111111111111:user/terraform-test"
    user_id    = "terraform-test"
  }
}

override_data {
  target = data.aws_availability_zones.available
  values = {
    names    = ["us-east-1a", "us-east-1b"]
    zone_ids = ["use1-az1", "use1-az2"]
    state    = "available"
  }
}

run "paper_null_approval_plans" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = false
  }

  assert {
    condition     = aws_ecs_service.app.desired_count == 0
    error_message = "Paper plan must keep the unapproved runtime stopped."
  }
}

run "live_null_approval_plans_read_only" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "222222222222"
      arn        = "arn:aws:iam::222222222222:user/terraform-test"
      user_id    = "terraform-test"
    }
  }

  variables {
    environment                   = "live"
    expected_aws_account_id       = "222222222222"
    database_name                 = "alpaca_autotrader_live"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = "operator@example.invalid"
    create_account_budget         = true
  }

  assert {
    condition     = aws_ecs_service.app.desired_count == 0
    error_message = "Live plan must keep the unapproved runtime stopped."
  }
}

run "account_mismatch_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "222222222222"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_kms_key.main]
}

run "execution_environment_mismatch_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "live"
    deploy_application            = true
    runtime_ready_approval_id     = "runtime-approved"
    live_activation_approval_id   = "live-approved"
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "mutation_without_runtime_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "paper"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "runtime_without_approval_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = true
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "ca_placeholder_blocks_deployment_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = true
    runtime_ready_approval_id     = "runtime-approved"
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "unsupported_fargate_pair_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    container_cpu                 = 256
    container_memory              = 4096
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "database_environment_suffix_mismatch_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_live"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [aws_db_instance.main]
}

run "live_without_alert_destination_blocks_plan" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "222222222222"
      arn        = "arn:aws:iam::222222222222:user/terraform-test"
      user_id    = "terraform-test"
    }
  }

  variables {
    environment                   = "live"
    expected_aws_account_id       = "222222222222"
    database_name                 = "alpaca_autotrader_live"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = false
  }

  expect_failures = [
    aws_ecs_task_definition.app,
    aws_sns_topic.alerts,
  ]
}

run "budget_without_alert_destination_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "read_only"
    deploy_application            = false
    alert_email                   = null
    create_account_budget         = true
  }

  expect_failures = [aws_budgets_budget.account]
}

run "live_mutation_without_activation_blocks_plan" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "222222222222"
      arn        = "arn:aws:iam::222222222222:user/terraform-test"
      user_id    = "terraform-test"
    }
  }

  variables {
    environment                   = "live"
    expected_aws_account_id       = "222222222222"
    database_name                 = "alpaca_autotrader_live"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "live"
    deploy_application            = true
    runtime_ready_approval_id     = "runtime-approved"
    alert_email                   = "operator@example.invalid"
    create_account_budget         = true
  }

  expect_failures = [aws_ecs_task_definition.app]
}
