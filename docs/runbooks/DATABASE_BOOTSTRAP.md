# Database bootstrap and runtime grants

## Stop condition

**HOLD — do not start the ECS service** until all migrations, including
`0005_runtime_authority.sql`, and the SQL security tests pass in the target
database; a unique login has been bound only to the `alpaca_trader_runtime`
NOLOGIN role; and its secret is populated. Terraform creates the empty runtime
secret but deliberately gives ECS no access to the RDS-managed master secret.

## Required runtime contract

An isolated migration-only process may retrieve the RDS master secret. It has no
broker credential and cannot run concurrently with an active executor. Migration
`0005_runtime_authority.sql` creates the fixed NOLOGIN runtime/operator roles,
revokes public authority, and grants the tested runtime contract. After applying
it, bootstrap must create a unique non-owner LOGIN with `INHERIT`, grant only
`alpaca_trader_runtime` to that login, and verify that it:

- revoke schema `CREATE`, role creation, ownership, DDL, truncate, trigger,
  reference, and blanket table update/delete authority;
- grant database connect, schema usage, required sequence usage, read access,
  and insert only on the exact append-only/runtime-write tables;
- can execute only the reviewed fenced lease, readiness-gated outbox, and
  automated-halt functions;
- revoke public execute on those functions and fix every security-definer
  function's `search_path` to trusted schemas;
- prohibit direct update/delete of audit tables and direct unfenced mutation of
  executor lease or order-outbox authority;
- retain the RDS master and migration role outside the ECS task/execution role.

The exact grants live in the checked-in migration and SQL security tests, not an
ad hoc runbook command. Tests prove that the runtime role cannot create or alter
objects, promote a release/permit, clear kill state, change immutable events,
bypass a fencing token, or call ungranted functions—and can perform required
normal runtime transactions. The environment bootstrap procedure must also test
the actual LOGIN, not just `SET ROLE` as an administrator.

## Secret population

After tests pass, generate a strong unique runtime password without placing it
in shell history, arguments, Terraform, Git, logs, or chat. Write JSON keys
`username` and `password` directly to the environment's `runtime-database`
Secrets Manager secret. Restart into reconcile-only and verify TLS connection,
schema version, runtime permission self-check, and broker/local reconciliation.

Rotate by creating/replacing the runtime credential, restarting
stop-before-start into reconcile-only, validating permissions and reconciliation,
then invalidating old sessions. Never solve a permission failure by injecting
the master credential or broadening grants interactively.
