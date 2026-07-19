# Credential rotation

## Broker key

1. Enter reconcile-only, verify no ambiguous submissions, and record current
   account fingerprint—not the key.
2. The operator creates a replacement key through Alpaca and writes JSON keys
   `api_key_id` and `api_secret_key` directly to the environment-specific
   Secrets Manager secret. Do not use Terraform secret values or CLI arguments.
3. Stop-before-start deploy/restart the task. It reads the new version at
   startup, authenticates with a bounded account request, verifies the account
   fingerprint, and remains read-only while reconciling.
4. Revoke the old key only after new-key verification, then verify the old key no
   longer authenticates without logging either value.
5. Record secret version IDs, UTC times, operator, account fingerprint, and
   checks. Never record secret contents.

## Database credential

RDS manages the migration-only master password in Secrets Manager, and ECS has
no permission to read it. Runtime uses the separate non-owner credential and
grants in `DATABASE_BOOTSTRAP.md`.

Rotate the runtime role through the isolated migration path, populate a new
runtime secret version directly, restart reconcile-only, verify TLS database
connectivity and the permission self-check, reconcile broker/local state, then
invalidate old sessions. A failure leaves execution disabled. Rotate the RDS
master separately and never copy its value into the runtime secret.

## AWS/deployment identity

GitHub uses OIDC and has no long-lived AWS key. Rotate trust policy or role by
reviewed Terraform change, verify repository/branch/environment claims, and test
paper first. KMS rotation is enabled automatically; changing keys requires
verified decrypt/copy/restore procedures and cannot make old evidence unreadable.
