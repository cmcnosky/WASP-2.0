# Database bootstrap and runtime grants

## Stop condition

**HOLD — do not start the ECS service** until all migrations, including
`0005_runtime_authority.sql` and `0009_paper_observer_evidence.sql`, and the SQL
security tests pass in the target database. An executor login must be bound only
to `alpaca_trader_runtime`; an observer login must be bound only to
`alpaca_trader_observer`. These are different credentials and may never be
combined. Terraform creates the empty runtime secret but deliberately gives ECS
no access to the RDS-managed master secret. The executor LOGIN name must end in
the exact trust-domain suffix (`_paper` or `_live`); the observer LOGIN must end
in `_observer_paper`. Startup checks these bindings before opening a store.

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

## Paper observer contract

The paper observer is a separate GET-only and append-only trust domain. After
applying migration `0009`, bootstrap must create a unique non-owner LOGIN ending
in `_observer_paper`, grant it only `alpaca_trader_observer`, and verify that it:

- is an inheriting LOGIN on PostgreSQL 17 primary RDS over hostname-validated
  TLS and owns no database, schema, relation, sequence, or function;
- has no direct table, sequence, DDL, executor-lease, order-outbox, release,
  permit, kill-state, or broker-mutation authority;
- can execute only the nine reviewed paper-observer functions and select only
  the immutable `paper_observer_schema_attestations` relation;
- rejects any additional direct or inherited role membership; and
- fails startup if an attested observer function, trigger, constraint, owner,
  or grant differs from the checked migration contract.

The connector consumes only the observer-prefixed database inputs:
`OBSERVER_DATABASE_HOST`, `OBSERVER_DATABASE_PORT`,
`OBSERVER_DATABASE_NAME`, `OBSERVER_DATABASE_USER`,
`OBSERVER_DATABASE_PASSWORD`, `OBSERVER_DATABASE_REQUIRE_TLS`, and
`OBSERVER_RDS_CA_BUNDLE_PEM`, plus independently reviewed host and CA digests.
Terraform maps `OBSERVER_DATABASE_USER`, `OBSERVER_DATABASE_PASSWORD`, and
`OBSERVER_RDS_CA_BUNDLE_PEM` from the paper-only
`paper-observer-database` secret; it never reuses the executor runtime secret.
The stopped task definition also maps the fingerprint salt from the separate
`paper-observer-identity` secret. This offline wiring is a bootstrap contract,
not authority to start ECS; the deployment-evidence precondition remains closed.

## Secret population

After tests pass, generate strong unique runtime and observer passwords without
placing either in shell history, arguments, Terraform, Git, logs, or chat.
Write JSON keys `username`, `password`, and `ca_bundle_pem` directly to the
environment's `runtime-database` and, for paper only,
`paper-observer-database` Secrets Manager secrets. The credentials must be
different and bound to only their corresponding database role.
`ca_bundle_pem` must be the current AWS-published root certificate for the exact
RDS region, downloaded over a separately verified administrative path; do not
include a private key or a non-AWS trust bundle. Record its SHA-256 digest in
the deployment evidence and set the same digest through the independently
reviewed Terraform variable `expected_rds_ca_bundle_sha256`; each connector
fails closed if the secret value does not match. Confirm the database instance
reports the approved `rds_ca_cert_identifier` before starting any task.

For paper only, populate `paper-observer-identity` with the single JSON key
`account_fingerprint_salt_hex`. It must decode to 32–1024 random bytes, remain
stable across Alpaca API-key rotation, and never appear in Terraform, logs,
commands, Git, or chat. Do not populate or start the observer until the separate
safe account-fingerprint bootstrap command exists and its non-secret result is
independently reviewed.
Restart into reconcile-only and verify hostname-validated TLS, the pinned bundle
digest, schema version, runtime permission self-check, and broker/local
reconciliation.

Rotate by creating/replacing the runtime credential, restarting
stop-before-start into reconcile-only, validating permissions and reconciliation,
then invalidating old sessions. Never solve a permission failure by injecting
the master credential or broadening grants interactively.
