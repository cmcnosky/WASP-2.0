# AWS Terraform baseline

This root creates one isolated paper **or** live environment. It is intentionally
not a reusable multi-environment module: initialize it with an environment's own
S3 state key, AWS account allowlist, credentials, VPC, database, keys, secrets,
and deployment role. Prefer separate AWS accounts for paper/research and live.

Terraform success is not order authority. The default `execution_mode` is
`read_only`; live mode additionally requires an operator approval reference and
the application must validate the actual signed activation permit and broker
account fingerprint.

The current binary has bounded CLI commands but no long-running reconcile loop.
Terraform therefore defaults `deploy_application=false`, keeping ECS desired
count at zero. Do not set it true until that runtime exists, its local health
check and shutdown behavior pass, and `runtime_ready_approval_id` identifies the
review evidence. Runtime alarms are not created and the dead-man schedule stays
disabled while the task is intentionally absent; there is no fake heartbeat
process. Infrastructure/database alarms remain active.

## Resources

- Two-AZ VPC with private Fargate and isolated database subnets, no public task
  IP, inbound listener, load balancer, SSH, or ECS Exec.
- One ECS on-demand service with digest-pinned task and stop-before-start
  deployment settings; desired count remains zero until the runtime-ready gate.
- PostgreSQL 17 RDS: paper Single-AZ/7-day PITR; live Multi-AZ/35-day PITR,
  enhanced monitoring, deletion protection, and final snapshot.
- KMS-encrypted/versioned/object-locked S3 data and audit buckets, immutable ECR,
  and empty environment-specific Alpaca/runtime-database Secrets Manager
  secrets. The task cannot read the RDS master secret.
- CloudWatch/SNS safety alarms, ECS-stop event, and independent EventBridge/
  Lambda dead-man check with telemetry permissions only.
- Environment-scoped GitHub OIDC image-publishing role with explicit ECS
  deployment and `iam:PassRole` denies, plus an optional dedicated-account
  monthly budget.

The baseline uses one NAT gateway in paper and two in live. Before live, price
this exact plan and include recurring AWS/data cost in the strategy hurdle.

## Non-secret inputs

Copy the matching file from `environments/` to a location that will not be
committed, replace placeholders, and review every value. Do not put secrets in a
tfvars file. Build the OCI image locally to establish its digest before creating
the ECS task definition.

Backend buckets, KMS keys, and DynamoDB lock tables are operator-owned bootstrap
infrastructure and are not created by this state. Paper and live must never
share a state key or lock table. Initialize:

```sh
terraform init -backend-config=environments/backend-paper.hcl.example
terraform fmt -check -recursive
terraform validate
terraform plan -var-file=/absolute/private/path/paper.tfvars -out=/tmp/paper.tfplan
terraform show /tmp/paper.tfplan
```

Do not run `terraform apply` until an operator authorizes the exact AWS account,
plan, estimated cost, and environment. CI validates and plans only; it never
applies.

## Bootstrap order

The ECR repository and runtime secrets must exist before a task can start. Use a
separately reviewed bootstrap plan targeting the foundational resources or
split the approved rollout into two saved plans:

1. Create network, KMS, ECR, storage, database, monitoring, roles, and empty
   runtime secrets with execution read-only.
2. Build/test/scan the exact image, push it through the environment OIDC role,
   and record its registry digest (not a tag).
3. Using an isolated migration-only process, create the non-owner runtime DB role
   and exact grants described in `docs/runbooks/DATABASE_BOOTSTRAP.md`. Populate
   its secret with JSON keys `username`, `password`, and `ca_bundle_pem`. The CA
   value is the current AWS-published root certificate for the exact RDS region;
   record and approve its SHA-256 digest, set that digest separately in
   `expected_rds_ca_bundle_sha256`, and verify it matches the pinned
   `rds_ca_cert_identifier`. The RDS-managed master secret must never be granted
   to ECS.
4. Populate the Alpaca secret directly with JSON keys `api_key_id` and
   `api_secret_key`; do not print either secret.
5. Keep `deploy_application=false` while the image lacks a long-running
   reconcile loop. After that implementation and review, set its approval ID and
   plan the digest-pinned task/service; confirm no public IP/inbound rule, desired
   count one, and read-only execution mode.

If a staged first apply is operationally inconvenient, refactor foundation and
runtime into distinct states before provisioning; do not use a placeholder image
or secret to make ECS appear healthy.

## Environment isolation

Application code derives Alpaca hosts from the typed environment. Terraform does
not accept or inject a broker URL. The provider uses `allowed_account_ids`, and
an additional blocking KMS-resource precondition compares caller identity. Use
different GitHub environments and protection rules. If both stacks temporarily
share an AWS account, pass the existing GitHub provider ARN to the second stack;
every other named resource and state still remains distinct.

The current Terraform revision cannot deploy an application task: the task
precondition is closed and the GitHub OIDC role may publish images but is
explicitly denied ECS deployment and `iam:PassRole`. A later reviewed code
change must remove both holds only after the observer entrypoint is implemented
and tested. Live would then start in read-only for at least five reconciled
trading sessions; mutation still requires the complete live-readiness runbook.

## Validation and drills

`../../scripts/check-infra.sh` runs format, init-without-backend, validation, two
valid stopped-environment plans, and thirteen negative mocked plans that prove
the account, environment/execution, activation, runtime/entrypoint, CA digest,
Fargate, database-name, alert, and budget preconditions block unsafe plans. Before live
authority, complete and record task-kill, deployment rollback, RDS failover,
PITR restore, credential rotation, alert delivery, dead-man, and region
benchmark drills. Always recover into reconcile-only.
