resource "aws_db_subnet_group" "main" {
  name       = local.name_prefix
  subnet_ids = [for subnet in aws_subnet.database : subnet.id]

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-db" })
}

resource "aws_db_parameter_group" "main" {
  name_prefix = "${local.name_prefix}-"
  family      = "postgres17"
  description = "${local.name_prefix} PostgreSQL safety parameters"

  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "password_encryption"
    value        = "scram-sha-256"
    apply_method = "immediate"
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-postgres17" })

  lifecycle {
    create_before_destroy = true
  }
}

data "aws_iam_policy_document" "rds_monitor_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["monitoring.rds.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "rds_monitor" {
  count = local.is_live ? 1 : 0

  name               = "${local.name_prefix}-rds-monitor"
  assume_role_policy = data.aws_iam_policy_document.rds_monitor_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "rds_monitor" {
  count = local.is_live ? 1 : 0

  role       = aws_iam_role.rds_monitor[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

resource "aws_db_instance" "main" {
  identifier = local.name_prefix

  engine         = "postgres"
  engine_version = "17"
  instance_class = local.db_instance_class

  db_name  = var.database_name
  username = var.database_username
  port     = 5432

  manage_master_user_password   = true
  master_user_secret_kms_key_id = aws_kms_key.main.arn

  allocated_storage     = 20
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.main.arn

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.database.id]
  publicly_accessible    = false

  multi_az                        = local.is_live
  backup_retention_period         = local.backup_retention_days
  backup_window                   = "03:00-04:00"
  maintenance_window              = "sun:05:00-sun:06:00"
  auto_minor_version_upgrade      = true
  copy_tags_to_snapshot           = true
  delete_automated_backups        = !local.is_live
  deletion_protection             = local.is_live
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  parameter_group_name = aws_db_parameter_group.main.name
  ca_cert_identifier   = var.rds_ca_cert_identifier

  performance_insights_enabled          = local.is_live
  performance_insights_kms_key_id       = local.is_live ? aws_kms_key.main.arn : null
  performance_insights_retention_period = local.is_live ? 7 : null

  monitoring_interval = local.is_live ? 60 : 0
  monitoring_role_arn = local.is_live ? aws_iam_role.rds_monitor[0].arn : null

  # V1 deliberately uses the separate Secrets Manager runtime login. Do not
  # enable a second, unimplemented authentication path.
  iam_database_authentication_enabled = false

  apply_immediately         = false
  skip_final_snapshot       = !local.is_live
  final_snapshot_identifier = local.is_live ? "${local.name_prefix}-final" : null

  tags = merge(local.common_tags, {
    Name       = local.name_prefix
    BackupTier = local.is_live ? "live-35-day-multiaz" : "paper-7-day-single-az"
  })

  lifecycle {
    precondition {
      condition     = endswith(var.database_name, "_${var.environment}")
      error_message = "database_name must end in the exact paper or live environment suffix."
    }
  }

  depends_on = [aws_iam_role_policy_attachment.rds_monitor]
}
