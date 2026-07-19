data "aws_iam_policy_document" "kms" {
  statement {
    sid    = "AccountAdministration"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["logs.${var.aws_region}.amazonaws.com"]
    }

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:ReEncrypt*",
      "kms:DescribeKey"
    ]
    resources = ["*"]

    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/${local.name_prefix}/*"]
    }
  }

  statement {
    sid    = "AWSServiceEncryption"
    effect = "Allow"

    principals {
      type = "Service"
      identifiers = [
        "cloudwatch.amazonaws.com",
        "events.amazonaws.com",
        "rds.amazonaws.com",
        "secretsmanager.amazonaws.com",
        "sns.amazonaws.com"
      ]
    }

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:ReEncrypt*",
      "kms:DescribeKey",
      "kms:CreateGrant"
    ]
    resources = ["*"]
  }
}

resource "aws_kms_key" "main" {
  description             = "${local.name_prefix} environment encryption"
  deletion_window_in_days = local.is_live ? 30 : 14
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.kms.json

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-kms" })

  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.account_id == var.expected_aws_account_id
      error_message = "The authenticated AWS account does not match expected_aws_account_id."
    }
  }
}

resource "aws_kms_alias" "main" {
  name          = "alias/${local.name_prefix}"
  target_key_id = aws_kms_key.main.key_id
}

resource "aws_security_group" "app" {
  name_prefix = "${local.name_prefix}-app-"
  description = "No ingress; bounded DNS, HTTPS, and PostgreSQL egress"
  vpc_id      = aws_vpc.main.id

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-app" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "database" {
  name_prefix = "${local.name_prefix}-db-"
  description = "PostgreSQL only from the application task"
  vpc_id      = aws_vpc.main.id

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-db" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_egress_rule" "app_https" {
  security_group_id = aws_security_group.app.id
  description       = "TLS to Alpaca and AWS public endpoints through NAT"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "app_dns_udp" {
  security_group_id = aws_security_group.app.id
  description       = "DNS to the VPC resolver"
  ip_protocol       = "udp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "app_dns_tcp" {
  security_group_id = aws_security_group.app.id
  description       = "TCP DNS fallback to the VPC resolver"
  ip_protocol       = "tcp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "app_database" {
  security_group_id            = aws_security_group.app.id
  description                  = "PostgreSQL TLS to the environment database"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.database.id
}

resource "aws_vpc_security_group_ingress_rule" "database_app" {
  security_group_id            = aws_security_group.database.id
  description                  = "PostgreSQL from the environment application"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.app.id
}

resource "aws_secretsmanager_secret" "alpaca" {
  name                    = "${local.name_prefix}/alpaca-api"
  description             = "Populate directly with api_key_id and api_secret_key JSON keys"
  kms_key_id              = aws_kms_key.main.arn
  recovery_window_in_days = local.is_live ? 30 : 7

  tags = merge(local.common_tags, {
    Name          = "${local.name_prefix}-alpaca-api"
    CredentialFor = var.environment
  })
}

resource "aws_secretsmanager_secret" "runtime_database" {
  name                    = "${local.name_prefix}/runtime-database"
  description             = "Least-privilege runtime username/password plus approved RDS root PEM; never the RDS master"
  kms_key_id              = aws_kms_key.main.arn
  recovery_window_in_days = local.is_live ? 30 : 7

  tags = merge(local.common_tags, {
    Name          = "${local.name_prefix}-runtime-database"
    CredentialFor = "application-runtime-only"
  })
}

resource "aws_secretsmanager_secret" "paper_observer_database" {
  count = local.is_live ? 0 : 1

  name                    = "${local.name_prefix}/paper-observer-database"
  description             = "GET-only observer username/password plus approved RDS root PEM; never the runtime or RDS master login"
  kms_key_id              = aws_kms_key.main.arn
  recovery_window_in_days = 7

  tags = merge(local.common_tags, {
    Name          = "${local.name_prefix}-paper-observer-database"
    CredentialFor = "paper-observer-database-only"
  })
}

resource "aws_secretsmanager_secret" "paper_observer_identity" {
  count = local.is_live ? 0 : 1

  name                    = "${local.name_prefix}/paper-observer-identity"
  description             = "Stable paper account-fingerprint salt only; populate account_fingerprint_salt_hex out of band"
  kms_key_id              = aws_kms_key.main.arn
  recovery_window_in_days = 7

  tags = merge(local.common_tags, {
    Name          = "${local.name_prefix}-paper-observer-identity"
    CredentialFor = "paper-observer-fingerprint-only"
  })
}

# Intentionally no aws_secretsmanager_secret_version resources. Terraform state
# must contain neither broker nor database credential values. The operator
# populates every secret out of band after the least-privilege role is created.
