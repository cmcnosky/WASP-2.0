output "aws_account_id" {
  description = "Verified deployment account."
  value       = data.aws_caller_identity.current.account_id
}

output "environment" {
  description = "Fixed trust domain for this stack."
  value       = var.environment
}

output "vpc_id" {
  description = "Environment-isolated VPC."
  value       = aws_vpc.main.id
}

output "ecr_repository_url" {
  description = "Push the reviewed immutable image here before ECS deployment."
  value       = aws_ecr_repository.app.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "ecs_service_name" {
  value = aws_ecs_service.app.name
}

output "database_endpoint" {
  description = "Private RDS endpoint; not a credential."
  value       = aws_db_instance.main.endpoint
}

output "alpaca_secret_arn" {
  description = "Populate out of band; Terraform intentionally creates no secret value."
  value       = aws_secretsmanager_secret.alpaca.arn
}

output "runtime_database_secret_arn" {
  description = "Populate with a least-privilege runtime role only; never copy the RDS master credential."
  value       = aws_secretsmanager_secret.runtime_database.arn
}

output "paper_observer_database_secret_arn" {
  description = "Paper only; populate with the dedicated observer login and approved CA bundle."
  value       = local.is_live ? null : aws_secretsmanager_secret.paper_observer_database[0].arn
}

output "paper_observer_identity_secret_arn" {
  description = "Paper only; populate account_fingerprint_salt_hex out of band and never place it in Terraform state."
  value       = local.is_live ? null : aws_secretsmanager_secret.paper_observer_identity[0].arn
}

output "data_bucket" {
  value = aws_s3_bucket.data.id
}

output "audit_bucket" {
  value = aws_s3_bucket.audit.id
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "github_release_role_arn" {
  description = "Environment-scoped GitHub OIDC image-publishing identity; ECS deployment and PassRole are explicitly denied."
  value       = aws_iam_role.github_release.arn
}

output "deployment_hold" {
  description = "Visible reminder that infrastructure success is not trading authority."
  value = !var.deploy_application ? (
    "HOLD: ECS desired count is zero until a real long-running runtime is approved"
    ) : var.execution_mode == "live" ? (
    "LIVE REQUESTED: application must still validate the activation permit and reconcile"
  ) : "HOLD: broker mutation is not live-authorized"
}
