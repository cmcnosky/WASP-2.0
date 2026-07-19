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

override_resource {
  target = aws_db_instance.main
  values = {
    address = "alpaca-autotrader-paper.example.us-east-1.rds.amazonaws.com"
    port    = 5432
  }
}

override_resource {
  target = aws_ecr_repository.app
  values = {
    repository_url = "111111111111.dkr.ecr.us-east-1.amazonaws.com/alpaca-autotrader-paper"
  }
}

override_resource {
  target = aws_secretsmanager_secret.alpaca
  values = {
    arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:alpaca-api"
  }
}

override_resource {
  target = aws_secretsmanager_secret.paper_observer_database
  values = {
    arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:paper-observer-database"
  }
}

override_resource {
  target = aws_secretsmanager_secret.paper_observer_identity
  values = {
    arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:paper-observer-identity"
  }
}

override_resource {
  target = aws_iam_role.app
  values = {
    arn = "arn:aws:iam::111111111111:role/alpaca-autotrader-paper-observer"
  }
}

run "paper_null_approval_plans" {
  command = plan

  variables {
    environment                            = "paper"
    expected_aws_account_id                = "111111111111"
    database_name                          = "alpaca_autotrader_paper"
    rds_ca_cert_identifier                 = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256          = "0000000000000000000000000000000000000000000000000000000000000000"
    github_repository                      = "owner/repository"
    container_image_digest                 = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    expected_alpaca_account_fingerprint    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    expected_observer_database_host_sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    execution_mode                         = "read_only"
    deploy_application                     = false
    alert_email                            = null
    create_account_budget                  = false
  }

  assert {
    condition     = aws_ecs_service.app.desired_count == 0
    error_message = "Paper plan must keep the unapproved runtime stopped."
  }

  assert {
    condition = (
      length(aws_secretsmanager_secret.paper_observer_database) == 1 &&
      length(aws_secretsmanager_secret.paper_observer_identity) == 1 &&
      length(aws_iam_role_policy.ecs_execution_secrets) == 1
    )
    error_message = "Paper must have separate empty observer database and identity secret containers."
  }

  assert {
    condition     = local.paper_observer_container.command == ["paper-observer"]
    error_message = "The stopped task definition must select the exact GET-only paper-observer command."
  }

  assert {
    condition = toset([
      for item in local.paper_observer_environment : item.name
      ]) == toset([
      "APP_ENVIRONMENT",
      "AWS_REGION",
      "EXECUTION_MODE",
      "EXPECTED_ALPACA_ACCOUNT_FINGERPRINT",
      "EXPECTED_IMAGE_DIGEST",
      "EXPECTED_OBSERVER_DATABASE_HOST_SHA256",
      "EXPECTED_OBSERVER_RDS_CA_BUNDLE_SHA256",
      "METRIC_NAMESPACE",
      "OBSERVER_DATABASE_HOST",
      "OBSERVER_DATABASE_NAME",
      "OBSERVER_DATABASE_PORT",
      "OBSERVER_DATABASE_REQUIRE_TLS",
      "RUST_LOG"
    ])
    error_message = "The paper observer task environment must contain only the reviewed non-secret inputs."
  }

  assert {
    condition = toset([
      for item in local.paper_observer_secrets : item.name
      ]) == toset([
      "ALPACA_ACCOUNT_FINGERPRINT_SALT_HEX",
      "ALPACA_API_KEY_ID",
      "ALPACA_API_SECRET_KEY",
      "OBSERVER_DATABASE_PASSWORD",
      "OBSERVER_DATABASE_USER",
      "OBSERVER_RDS_CA_BUNDLE_PEM"
    ])
    error_message = "The paper observer task must receive only the reviewed secret inputs."
  }

  assert {
    condition = {
      for item in local.paper_observer_environment : item.name => item.value
      if item.name != "OBSERVER_DATABASE_HOST"
      } == {
      APP_ENVIRONMENT                        = "paper"
      AWS_REGION                             = "us-east-1"
      EXECUTION_MODE                         = "read_only"
      EXPECTED_ALPACA_ACCOUNT_FINGERPRINT    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      EXPECTED_IMAGE_DIGEST                  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      EXPECTED_OBSERVER_DATABASE_HOST_SHA256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      EXPECTED_OBSERVER_RDS_CA_BUNDLE_SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
      METRIC_NAMESPACE                       = "AlpacaAutotrader/paper"
      OBSERVER_DATABASE_NAME                 = "alpaca_autotrader_paper"
      OBSERVER_DATABASE_PORT                 = "5432"
      OBSERVER_DATABASE_REQUIRE_TLS          = "true"
      RUST_LOG                               = "info"
    }
    error_message = "The paper observer task must bind exact paper/read-only values and independent evidence inputs."
  }

  assert {
    condition = (
      local.paper_observer_container.command == ["paper-observer"] &&
      var.container_image_digest == "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" &&
      aws_iam_role.app.tags["Runtime"] == "get-only-paper-observer"
    )
    error_message = "The task must use the digest-pinned image and the no-policy observer task role."
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

  assert {
    condition = (
      length(aws_secretsmanager_secret.paper_observer_database) == 0 &&
      length(aws_secretsmanager_secret.paper_observer_identity) == 0 &&
      length(aws_iam_role_policy.ecs_execution_secrets) == 0 &&
      length(local.paper_observer_secrets) == 0
    )
    error_message = "Live infrastructure must not create or inject paper-observer secrets."
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

run "paper_observer_external_evidence_missing_blocks_plan" {
  command = plan

  variables {
    environment                            = "paper"
    expected_aws_account_id                = "111111111111"
    database_name                          = "alpaca_autotrader_paper"
    rds_ca_cert_identifier                 = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256          = "1111111111111111111111111111111111111111111111111111111111111111"
    github_repository                      = "owner/repository"
    container_image_digest                 = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    expected_alpaca_account_fingerprint    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    expected_observer_database_host_sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    execution_mode                         = "read_only"
    deploy_application                     = true
    runtime_ready_approval_id              = "runtime-approved"
    alert_email                            = null
    create_account_budget                  = false
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "live_read_only_deployment_blocks_plan" {
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
    execution_mode                = "read_only"
    deploy_application            = true
    runtime_ready_approval_id     = "runtime-approved"
    alert_email                   = "operator@example.invalid"
    create_account_budget         = true
  }

  expect_failures = [aws_ecs_task_definition.app]
}

run "paper_mutation_deployment_blocks_plan" {
  command = plan

  variables {
    environment                   = "paper"
    expected_aws_account_id       = "111111111111"
    database_name                 = "alpaca_autotrader_paper"
    rds_ca_cert_identifier        = "rds-ca-rsa2048-g1"
    expected_rds_ca_bundle_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
    github_repository             = "owner/repository"
    container_image_digest        = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    execution_mode                = "paper"
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
