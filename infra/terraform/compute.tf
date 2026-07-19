resource "aws_cloudwatch_log_group" "app" {
  name              = "/aws/${local.name_prefix}/app"
  retention_in_days = local.is_live ? 365 : 30
  kms_key_id        = aws_kms_key.main.arn

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-app" })
}

resource "aws_ecs_cluster" "main" {
  name = local.name_prefix

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(local.common_tags, { Name = local.name_prefix })
}

data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "${local.name_prefix}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "ecs_execution_secrets" {
  statement {
    sid    = "ReadEnvironmentSecrets"
    effect = "Allow"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      aws_secretsmanager_secret.alpaca.arn,
      aws_secretsmanager_secret.runtime_database.arn
    ]
  }

  statement {
    sid       = "DecryptEnvironmentSecrets"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.main.arn]
  }
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name   = "environment-secrets"
  role   = aws_iam_role.ecs_execution.id
  policy = data.aws_iam_policy_document.ecs_execution_secrets.json
}

resource "aws_iam_role" "app" {
  name               = "${local.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "app" {
  statement {
    sid    = "ReadWriteVersionedData"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAttributes",
      "s3:GetObjectVersion",
      "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }

  statement {
    sid    = "WriteAuditExports"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.audit.arn}/*"]
  }

  statement {
    sid    = "ListEnvironmentBuckets"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketVersions"
    ]
    resources = [
      aws_s3_bucket.data.arn,
      aws_s3_bucket.audit.arn
    ]
  }

  statement {
    sid    = "EnvironmentEncryption"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:GenerateDataKey"
    ]
    resources = [aws_kms_key.main.arn]
  }

  statement {
    sid       = "PublishApplicationMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [local.metric_namespace]
    }
  }
}

resource "aws_iam_role_policy" "app" {
  name   = "runtime-minimum"
  role   = aws_iam_role.app.id
  policy = data.aws_iam_policy_document.app.json
}

resource "aws_ecs_task_definition" "app" {
  family                   = local.name_prefix
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.app.arn

  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }

  ephemeral_storage {
    size_in_gib = 21
  }

  container_definitions = jsonencode([{
    name      = "app"
    image     = "${aws_ecr_repository.app.repository_url}@${var.container_image_digest}"
    essential = true
    user      = "65532"

    readonlyRootFilesystem = true
    stopTimeout            = 120

    linuxParameters = {
      initProcessEnabled = true
      capabilities = {
        drop = ["ALL"]
      }
    }

    environment = [
      { name = "APP_ENVIRONMENT", value = var.environment },
      { name = "EXECUTION_MODE", value = var.execution_mode },
      { name = "ACTIVATION_APPROVAL_ID", value = var.live_activation_approval_id == null ? "" : var.live_activation_approval_id },
      { name = "DATABASE_HOST", value = aws_db_instance.main.address },
      { name = "EXPECTED_DATABASE_HOST_SHA256", value = sha256(aws_db_instance.main.address) },
      { name = "DATABASE_PORT", value = tostring(aws_db_instance.main.port) },
      { name = "DATABASE_NAME", value = var.database_name },
      { name = "DATABASE_REQUIRE_TLS", value = "true" },
      { name = "EXPECTED_RDS_CA_BUNDLE_SHA256", value = var.expected_rds_ca_bundle_sha256 },
      { name = "DATA_BUCKET", value = aws_s3_bucket.data.id },
      { name = "AUDIT_BUCKET", value = aws_s3_bucket.audit.id },
      { name = "METRIC_NAMESPACE", value = local.metric_namespace },
      { name = "AWS_REGION", value = var.aws_region },
      { name = "RUST_LOG", value = "info" }
    ]

    secrets = [
      {
        name      = "DATABASE_USER"
        valueFrom = "${aws_secretsmanager_secret.runtime_database.arn}:username::"
      },
      {
        name      = "DATABASE_PASSWORD"
        valueFrom = "${aws_secretsmanager_secret.runtime_database.arn}:password::"
      },
      {
        name      = "RDS_CA_BUNDLE_PEM"
        valueFrom = "${aws_secretsmanager_secret.runtime_database.arn}:ca_bundle_pem::"
      },
      {
        name      = "ALPACA_API_KEY_ID"
        valueFrom = "${aws_secretsmanager_secret.alpaca.arn}:api_key_id::"
      },
      {
        name      = "ALPACA_API_SECRET_KEY"
        valueFrom = "${aws_secretsmanager_secret.alpaca.arn}:api_secret_key::"
      }
    ]

    healthCheck = {
      command     = ["CMD", "/app/alpaca-autotrader", "health", "--local"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 20
    }

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.app.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "app"
      }
    }
  }])

  tags = merge(local.common_tags, {
    Name        = local.name_prefix
    ImageDigest = var.container_image_digest
  })

  lifecycle {
    precondition {
      condition     = local.execution_mode_matches_environment
      error_message = "Paper/live mutation mode must match its environment."
    }

    precondition {
      condition     = local.live_activation_is_referenced
      error_message = "Live mutation mode requires an explicit approval reference."
    }

    precondition {
      condition     = local.runtime_is_approved
      error_message = "deploy_application requires reviewed evidence for a real long-running reconcile runtime."
    }

    precondition {
      condition     = local.mutation_has_runtime
      error_message = "Broker mutation mode cannot be requested while the application task is disabled."
    }

    precondition {
      condition     = local.deployment_has_real_ca_digest
      error_message = "deploy_application requires the real approved RDS root bundle digest, not the example placeholder."
    }

    precondition {
      condition     = local.fargate_cpu_memory_pair_is_supported
      error_message = "container_cpu and container_memory must form a supported bounded Fargate combination."
    }

    precondition {
      condition     = !local.is_live || var.alert_email != null
      error_message = "A live application task requires a confirmed operator alert email."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_execution,
    aws_iam_role_policy.ecs_execution_secrets
  ]
}

resource "aws_ecs_service" "app" {
  name             = local.name_prefix
  cluster          = aws_ecs_cluster.main.id
  task_definition  = aws_ecs_task_definition.app.arn
  desired_count    = var.deploy_application ? 1 : 0
  launch_type      = "FARGATE"
  platform_version = "1.4.0"

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = [for subnet in aws_subnet.app : subnet.id]
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = false
  }

  enable_execute_command = false
  propagate_tags         = "SERVICE"

  tags = merge(local.common_tags, { Name = local.name_prefix })
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.github_oidc_provider_arn == null ? 1 : 0

  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  tags = merge(local.common_tags, { Name = "github-actions" })
}

locals {
  github_provider_arn = coalesce(
    var.github_oidc_provider_arn,
    try(aws_iam_openid_connect_provider.github[0].arn, null)
  )
}

data "aws_iam_policy_document" "github_release_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:environment:${var.environment}"]
    }
  }
}

resource "aws_iam_role" "github_release" {
  name                 = "${local.name_prefix}-github-release"
  assume_role_policy   = data.aws_iam_policy_document.github_release_assume.json
  max_session_duration = 3600
  tags                 = local.common_tags
}

data "aws_iam_policy_document" "github_release" {
  statement {
    sid       = "ECRAuthentication"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "PublishImmutableImage"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:DescribeImages",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart"
    ]
    resources = [aws_ecr_repository.app.arn]
  }

  statement {
    sid    = "RegisterAndDeployTask"
    effect = "Allow"
    actions = [
      "ecs:DescribeServices",
      "ecs:DescribeTaskDefinition",
      "ecs:RegisterTaskDefinition",
      "ecs:UpdateService"
    ]
    resources = ["*"]
  }

  statement {
    sid       = "PassExactTaskRoles"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.app.arn, aws_iam_role.ecs_execution.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "github_release" {
  name   = "immutable-release"
  role   = aws_iam_role.github_release.id
  policy = data.aws_iam_policy_document.github_release.json
}
