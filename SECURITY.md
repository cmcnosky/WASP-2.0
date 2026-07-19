# Security policy

## Reporting

This is a private repository. Report suspected credential exposure, incorrect
live authority, broker-state ambiguity, dependency compromise, or data
contamination directly to the repository owner. Do not open a public issue or
paste evidence containing secrets into chat.

For an active incident, first disable execution through the approved operator
path, preserve broker protection, and reconcile broker truth. Do not blindly
flatten positions while state is uncertain. Follow
`docs/runbooks/INCIDENT_RESPONSE.md`.

## Secrets

- Alpaca keys live only in the environment-specific AWS Secrets Manager secret.
- Database credentials are generated and managed by RDS/Secrets Manager.
- CI uses short-lived GitHub OIDC federation; long-lived AWS access keys are
  prohibited.
- Application logs, metrics, traces, error payloads, and support bundles must
  redact authorization headers, secret values, account identifiers, and personal
  data.
- Secret rotation must be rehearsed in paper before live and must force a
  reconcile-only restart.

Use JSON keys `api_key_id` and `api_secret_key` when populating the Alpaca
secret. Never create a Terraform secret value: doing so writes plaintext-derived
material into Terraform state.

The RDS master credential is migration/bootstrap-only and is never readable by
the ECS execution or task role. Application runtime uses a separate non-owner
secret with JSON keys `username`, `password`, and `ca_bundle_pem`; that role has
no DDL, role, ownership, audit-table update/delete, or unfenced lease/outbox
authority. The runtime requires hostname-verified TLS against the approved
AWS-published RDS root and compares the exact bundle digest with a separately
reviewed, nonsecret deployment value before trusting it.

## Access and isolation

Paper/research and live should use separate AWS accounts. If that is temporarily
impossible, they must at least use separate VPCs, roles, KMS keys, Secrets
Manager secrets, databases, state backends, and deployment identities. The
Terraform module verifies the expected AWS account ID and fixes the broker host
from the environment value.

Production tasks have no public IP, inbound listener, SSH path, or load balancer.
The task role is distinct from the ECS execution role and receives least-privilege
access to its own buckets, metrics, and secrets.

While the runtime entrypoint is incomplete, the GitHub OIDC role is an image
publisher only. Its policy explicitly denies ECS task/service deployment and
`iam:PassRole`, so a compromised CI identity cannot turn a pushed image into a
credential-bearing task. Removing those denies is a separately reviewed
promotion change, not an operator variable or approval-string action.

## Supply chain

- Commit Rust and Python lockfiles and deploy only immutable container digests.
- Treat the pinned distroless image's system root store as attested supply-chain
  state for broker HTTPS. The runtime filesystem is read-only; an image/root
  store change requires a new SBOM, scan, review, and immutable digest.
- Review all direct dependencies for purpose, maintenance, license, and
  transitive risk; record the decision in `docs/DEPENDENCIES.md`.
- CI performs locked builds, vulnerability checks, secret scanning, image
  scanning, and SBOM generation. A vulnerability exception must identify an
  owner, rationale, compensating control, and expiry.
- Do not execute downloaded scripts by piping network responses to a shell.

## Supported versions

Only the current main branch and the currently deployed immutable live image are
supported. Security fixes require a new release certificate and normal
promotion; emergency deployment may shorten observation windows but may not
bypass reconciliation, authority, or account-isolation gates.
