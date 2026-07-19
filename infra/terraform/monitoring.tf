resource "aws_sns_topic" "alerts" {
  name              = "${local.name_prefix}-alerts"
  kms_master_key_id = aws_kms_key.main.arn

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-alerts" })

  lifecycle {
    precondition {
      condition     = !local.is_live || var.alert_email != null
      error_message = "Live infrastructure requires a confirmed operator alert email."
    }
  }
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alert_email == null ? 0 : 1

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

data "aws_iam_policy_document" "alerts" {
  statement {
    sid    = "AccountAdministration"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["sns:*"]
    resources = [aws_sns_topic.alerts.arn]
  }

  statement {
    sid    = "AWSMonitoringPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com", "events.amazonaws.com"]
    }

    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts.json
}

locals {
  safety_alarm_metrics = {
    BrokerLocalDrift   = "Broker/local reconciliation differs"
    InvalidRelease     = "Release or activation authority is invalid"
    MarketDataStale    = "Market data is stale"
    OrderRejection     = "Broker rejected an order"
    ProtectionDisabled = "Required broker protection is missing"
    RiskHalt           = "A loss, drawdown, or other risk gate halted execution"
    SlowConsumer       = "A bounded event queue is too slow or saturated"
    StreamDisconnected = "A required broker or market-data stream disconnected"
    SubmissionUnknown  = "An order submission outcome is ambiguous"
    UnknownOrder       = "A broker order cannot be mapped to a durable local intent"
  }
}

resource "aws_cloudwatch_metric_alarm" "broker_event_persistence_latency" {
  count = var.deploy_application ? 1 : 0

  alarm_name          = "${local.name_prefix}-broker-event-persistence-p99"
  alarm_description   = "Broker event persistence exceeds the 250 ms p99 objective"
  namespace           = local.metric_namespace
  metric_name         = "BrokerEventPersistenceLatencyMs"
  dimensions          = { Environment = var.environment }
  extended_statistic  = "p99"
  period              = 60
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = 250
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "decision_to_submit_latency" {
  count = var.deploy_application ? 1 : 0

  alarm_name          = "${local.name_prefix}-decision-to-submit-p99"
  alarm_description   = "Internal decision-to-submit exceeds the 500 ms p99 objective"
  namespace           = local.metric_namespace
  metric_name         = "DecisionToSubmitLatencyMs"
  dimensions          = { Environment = var.environment }
  extended_statistic  = "p99"
  period              = 60
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = 500
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "ecs_running_tasks" {
  count = var.deploy_application ? 1 : 0

  alarm_name        = "${local.name_prefix}-running-task-missing"
  alarm_description = "ECS Container Insights reports fewer than one running service task"
  namespace         = "ECS/ContainerInsights"
  metric_name       = "RunningTaskCount"
  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = aws_ecs_service.app.name
  }
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "safety" {
  for_each = var.deploy_application ? local.safety_alarm_metrics : {}

  alarm_name          = "${local.name_prefix}-${each.key}"
  alarm_description   = each.value
  namespace           = local.metric_namespace
  metric_name         = each.key
  dimensions          = { Environment = var.environment }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  datapoints_to_alarm = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "heartbeat" {
  count = var.deploy_application ? 1 : 0

  alarm_name          = "${local.name_prefix}-heartbeat-missing"
  alarm_description   = "Application heartbeat is absent"
  namespace           = local.metric_namespace
  metric_name         = "Heartbeat"
  dimensions          = { Environment = var.environment }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 5
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "database_cpu" {
  alarm_name          = "${local.name_prefix}-database-cpu"
  alarm_description   = "RDS CPU exceeds the bounded operating threshold"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  dimensions          = { DBInstanceIdentifier = aws_db_instance.main.identifier }
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  comparison_operator = "GreaterThanThreshold"
  threshold           = 80
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "database_storage" {
  alarm_name          = "${local.name_prefix}-database-storage"
  alarm_description   = "RDS free storage is below 5 GiB"
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  dimensions          = { DBInstanceIdentifier = aws_db_instance.main.identifier }
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 5368709120
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_event_rule" "ecs_task_stopped" {
  name        = "${local.name_prefix}-task-stopped"
  description = "Alert whenever an application task stops"

  event_pattern = jsonencode({
    source        = ["aws.ecs"]
    "detail-type" = ["ECS Task State Change"]
    detail = {
      clusterArn    = [aws_ecs_cluster.main.arn]
      lastStatus    = ["STOPPED"]
      desiredStatus = ["STOPPED"]
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "ecs_task_stopped" {
  rule      = aws_cloudwatch_event_rule.ecs_task_stopped.name
  target_id = "operator-alerts"
  arn       = aws_sns_topic.alerts.arn
}

data "archive_file" "deadman" {
  type        = "zip"
  source_file = "${path.module}/lambda/deadman.py"
  output_path = "${path.module}/deadman.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "deadman" {
  name               = "${local.name_prefix}-deadman"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = local.common_tags
}

resource "aws_cloudwatch_log_group" "deadman" {
  name              = "/aws/${local.name_prefix}/deadman"
  retention_in_days = local.is_live ? 365 : 30
  kms_key_id        = aws_kms_key.main.arn

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-deadman" })
}

data "aws_iam_policy_document" "deadman" {
  statement {
    sid    = "ReadAndPublishHeartbeatMetrics"
    effect = "Allow"
    actions = [
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "WriteDeadmanLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["${aws_cloudwatch_log_group.deadman.arn}:*"]
  }
}

resource "aws_iam_role_policy" "deadman" {
  name   = "telemetry-only"
  role   = aws_iam_role.deadman.id
  policy = data.aws_iam_policy_document.deadman.json
}

resource "aws_lambda_function" "deadman" {
  function_name                  = "${local.name_prefix}-deadman"
  description                    = "Checks only the application heartbeat metric; has no broker credentials"
  role                           = aws_iam_role.deadman.arn
  runtime                        = "python3.12"
  handler                        = "deadman.handler"
  timeout                        = 15
  memory_size                    = 128
  reserved_concurrent_executions = 1

  filename         = data.archive_file.deadman.output_path
  source_code_hash = data.archive_file.deadman.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT         = var.environment
      MAXIMUM_AGE_SECONDS = "420"
      METRIC_NAMESPACE    = local.metric_namespace
    }
  }

  tracing_config {
    mode = "PassThrough"
  }

  tags = local.common_tags

  depends_on = [aws_cloudwatch_log_group.deadman]
}

resource "aws_cloudwatch_event_rule" "deadman" {
  name                = "${local.name_prefix}-deadman"
  description         = "Run the independent heartbeat verifier every five minutes"
  schedule_expression = "rate(5 minutes)"
  state               = var.deploy_application ? "ENABLED" : "DISABLED"
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "deadman" {
  rule      = aws_cloudwatch_event_rule.deadman.name
  target_id = "deadman-lambda"
  arn       = aws_lambda_function.deadman.arn
}

resource "aws_lambda_permission" "eventbridge_deadman" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.deadman.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.deadman.arn
}

resource "aws_cloudwatch_metric_alarm" "deadman" {
  count = var.deploy_application ? 1 : 0

  alarm_name          = "${local.name_prefix}-deadman-unhealthy"
  alarm_description   = "Independent dead-man monitor found no fresh heartbeat"
  namespace           = local.metric_namespace
  metric_name         = "DeadmanHealthy"
  dimensions          = { Environment = var.environment }
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

resource "aws_budgets_budget" "account" {
  count = var.create_account_budget ? 1 : 0

  name         = "${local.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [coalesce(var.alert_email, "unset@example.invalid")]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [coalesce(var.alert_email, "unset@example.invalid")]
  }

  lifecycle {
    precondition {
      condition     = var.alert_email != null
      error_message = "create_account_budget requires alert_email."
    }
  }
}
