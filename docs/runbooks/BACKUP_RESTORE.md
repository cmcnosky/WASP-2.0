# Backup and restore

## Policy

Paper RDS retains seven days of automated backups. Live uses Multi-AZ, deletion
protection, encrypted automated backups/PITR for 35 days, and a final snapshot on
authorized deletion. S3 versioning, KMS encryption, public-access blocks, and
governance object retention protect datasets and audits. Backup and state
resources are environment/account isolated.

Snapshots are not proof of recovery. Paper restore is tested before live
activation; live restore is tested at least quarterly and after material schema
or infrastructure changes.

## Restore drill

1. Announce the drill, verify execution is disabled, and capture source database
   ARN, backup time, migration version, image/release hashes, and expected ledger
   checkpoints.
2. Restore the selected point into a new isolated database. Never overwrite the
   authoritative database.
3. Use a read-only/reconcile-disabled task role and no broker credentials to run
   migrations/checks against the restored database.
4. Verify schema, append-only event counts/checksums, outbox completeness,
   projections, cash/position conservation, permits, kill state, and latest
   expected timestamps. Replay projections from events and compare them.
5. Record recovery-point error, recovery time, discrepancies, logs, and cost.
6. Destroy the temporary task/database only after evidence is retained and exact
   targets are reviewed. Follow the normal approval path for live resources.

## Disaster recovery

An actual restore starts with execution disabled. After database integrity
checks, query Alpaca for current orders, activities, fills, positions, and cash.
Append reconciliation evidence rather than rewriting history. Resume requires a
new executor fence and clean report. Restoration never implies live activation.
