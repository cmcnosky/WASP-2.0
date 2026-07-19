# Deployment and rollback

## Preconditions

- CI is green for the immutable commit and image digest.
- Vulnerability review, SBOM, release/certificate digest, database migration
  compatibility, and environment-specific Terraform plan are approved.
- The destination AWS account ID matches the environment allowlist.
- Live begins with `execution_mode=read_only`; changing it requires the live
  activation runbook and a valid approval ID.

## Deploy

1. Record current task definition, image digest, database migration version,
   executor fence, and reconciliation report.
2. Confirm the old process is read-only or halted and has no ambiguous intent.
3. Apply reviewed database migrations using a one-off execution-disabled task.
   Migrations must be backward compatible with the rollback image.
4. Update the digest-pinned ECS task definition. The service uses desired count
   one, minimum healthy percent zero, and maximum percent 100 so the previous
   task stops before the replacement starts.
5. The new task starts reconcile-only, verifies account/environment/release,
   acquires a new fence, and produces a clean reconciliation report.
6. Verify local health, heartbeat, logs, metrics, dead-man check, alarm delivery,
   data freshness, and database state. Keep live read-only for the declared
   observation period.

Do not use ECS Exec, SSH, ad hoc container mutation, floating image tags, or a
second concurrently active executor.

## Rollback

1. Enter reconcile-only and cancel/resolve any known outstanding entry intent
   according to the order state machine. Do not blindly retry or flatten.
2. Stop the failed task and deploy the last approved digest/task definition.
3. Do not reverse a database migration unless its reviewed down-path is safe;
   prefer forward repair.
4. The rollback task acquires a new fence and reconciles before authority can be
   restored.
5. Capture cause, timeline, affected intents, broker truth, data/ledger checks,
   and follow-up owner. A live rollback is a promotion-blocking incident until
   reviewed.

Success is a healthy read-only task, clean broker/local reconciliation, current
heartbeat, working alerts, and no ambiguous orders. It is not automatic
restoration of live submission.
