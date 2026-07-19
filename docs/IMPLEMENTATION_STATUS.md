# Implementation status

**Default authority: HOLD — do not trade.** This document distinguishes code
that has passed offline checks from designs and externally gated work. A file,
test double, Terraform resource, or passing paper test is not evidence that a
live capability exists or that a strategy is profitable.

Status terms:

- **Offline verified**: implemented in this repository and exercised without
  broker, cloud, live credentials, or prior-project material.
- **Structural only**: contracts, schemas, configuration, or test scaffolding
  exist, but the end-to-end operational capability has not been demonstrated.
- **Absent or external gate**: not implemented, not run, or dependent on
  operator approval, account entitlement, purchased data, AWS, or fresh market
  evidence.

## Phase matrix

| Phase | Offline verified | Structural only | Absent or external gate |
|---|---|---|---|
| 0. Clean room and foundation | Clean-room charter and audit, architecture and threat model, locked Rust/Python dependencies, secret scan, OCI build definition, Terraform validation, PostgreSQL disposable check, CI definitions, and SBOM/security workflows. Paper/live endpoint isolation is encoded in configuration tests. | AWS resources are plan-only. The OCI provenance/SBOM workflow exists but its artifact is not a live deployment approval. | Operator account classification, current Alpaca agreements/data rights, external dependency/license review, AWS account authority, and CI evidence from a protected remote branch. |
| 1. Domain, ledger, and simulator | Checked fixed-point domain types; immutable release/permit and append-only execution/research schemas; lease fencing; intent/outbox authority; durable `SUBMISSION_UNKNOWN` and restart discovery; cumulative-fill/terminal-fill guards; exact release/permit/core rematerialization at persistence; kill/reconciliation gates; deterministic decision replay; Rust/Python compiled parity; SQL invariant and concurrency tests. A Rust RDS connector requires hostname-verified TLS, an independently expected CA-bundle digest, environment-bound endpoint/database/login, non-privileged primary-session checks, schema attestation, and a monitored connection handle. | Accounting and order lifecycle components remain offline kernels. The database connector compiles and its store is exercised against disposable PostgreSQL, but no real RDS TLS/session integration has run. Replay proves deterministic decisions, not historical profit. | Real-RDS connector/grant test, independent end-to-end accounting reproduction, true event/fill simulator, complete operational modes, restore-to-reconcile proof, and byte-for-byte replay over certified datasets. |
| 2. Data acquisition and certification | Data provenance and availability contracts are documented. | Market observations and calendar evidence have typed/offline representations; S3 and data-bucket controls are declared in Terraform. | SIP entitlement, live/historical ingestion, immutable raw and Parquet datasets, calendar/clock integration, corporate actions, correction versioning, availability-time certification, defect quarantine, independent samples, and a certified dataset manifest. |
| 3. First strategy research | The 12-configuration preregistration, chronological periods, tamper-evident experiment ledger, statistical calculation utilities, and fail-closed certification gate evaluator exist in Python. Python decision calls fail if the compiled Rust module is unavailable. | Statistical utilities accept supplied evidence; they do not create evidence. Candidate decision logic can be replayed offline, but no approved universe/data/cost manifest or qualified release exists. | Frozen 8–12 ETF universe, certified 2016–2026 data, all 12 trials, modeled fills/cost stress, sealed holdout, SPA/equivalent familywise result, declared power analysis, concentration checks, independent reproduction, 60-session prospective evidence, economic hurdle, and operator-approved release. |
| 4. Alpaca execution slice | Broker-neutral ports, exhaustive fail-closed lifecycle states, deterministic client order identity, durable ambiguous-submission recovery rules, authority checks, reconciliation contracts, and offline order-safety tests. A direct paper-only Alpaca HTTPS adapter implements typed account/position/order/fill/SIP-quote reads, complete bounded order pagination, current clock/calendar session evidence, whole-share DAY marketable-limit POSTs, stable-ID recovery, cancellation-request evidence, request-ID/payload hashing, bounded redacted errors, and explicit rate/deadline controls. The transport mechanically rejects live/foreign hosts, arbitrary headers, proxies, redirects, hidden retries, stale POST deadlines, oversized bodies, and budget recreation after restart. | All broker behavior is verified with deterministic transports/fixtures, not an Alpaca paper account. The PostgreSQL store and HTTP adapter are not yet connected by a long-running coordinator. | Single market-data WebSocket, `trade_updates`, end-to-end local/broker projection reconciliation, partial-fill/cancel races through the durable worker, catastrophic broker protection, real paper contract evidence, and complete current-schema drift monitoring. |
| 5. AWS platform | Terraform formatting/validation, two valid stopped-environment plans, and ten negative mocked plans; declared private networking, ECS, RDS, S3, ECR, KMS/Secrets Manager, alarms, SNS, dead-man monitor, OIDC, budgets, immutable image digests, and deployment gate. Blocking resource preconditions enforce the expected AWS account, environment/execution pairing, runtime and live-activation references, non-placeholder RDS CA digest, environment database name, Fargate CPU/memory pair, and required alert/budget destinations. Application desired count defaults to zero until separately approved. | Infrastructure is an un-applied baseline. A declared alarm, backup, Multi-AZ database, role, or network path has not been exercised. The long-running production runtime is not implemented, so ECS is intentionally disabled. | AWS account/environment authorization, Pricing Calculator estimate, `us-east-1`/`us-east-2` measurement, apply/inspection, actual-login/RDS TLS bootstrap, paper/live separation proof, task-kill/failover/restore/rollback/rotation/pager drills, and active/passive fencing before material capital. |
| 6. Shadow, paper, and fault campaign | Unit/property-style safety tests and PostgreSQL serialization-race tests cover bounded offline failure cases. | Fault-campaign requirements and acceptance criteria are documented. | A production runtime, shadow/paper deployment, 60 trading sessions, 100 controlled lifecycle scenarios, broker/network/database fault injection, delivered alerts, zero-difference reconciliation record, and drill evidence. Paper P&L will not count as profitability evidence. |
| 7. First live purchase and sale | Initial capital/risk caps and activation/runbook gates are encoded in documentation and authority contracts. | Live Terraform variables and activation-permit schema are deliberately non-authorizing scaffolding. | Every prior gate; verified personal account/SIP rights; live secrets entered by the operator; approved certificate and permit; deployed read-only runtime; five live reconciliation sessions; real certified signal; bounded entry/protection/exit; final execution-cost and reconciled-P&L report. No ceremonial trade is allowed. |
| 8. Scaling and operations | Scale constraints, reporting obligations, fail-back-to-shadow triggers, and human promotion requirements are documented. | Monitoring/reporting storage and cloud alarms are declared but not proven in operation. | At least 20 live sessions and 30 completed round trips without critical operational incident, statistically adequate live evidence, cost-model conformance, operator promotion, active/passive recovery, cross-region backups, monthly reports, tax-lot reconciliation, and continuing dependency/API review. |

## What the current checks establish

`./scripts/check.sh` is the required local repository gate. It checks the
clean-room boundary and secrets, Rust formatting/lints/tests, dependency-free
Python tests, the actual compiled PyO3 module against the Rust CLI, Terraform
format/validation, Compose structure, and all PostgreSQL migrations, ledger
invariants, and serialization races. Pinned container images are used when the
host lacks the required toolchain.

These checks establish offline consistency only. They do not establish Alpaca
entitlement, market-data quality, AWS readiness, broker compatibility,
statistical edge, expected profitability, or live-trading authority.

## Exact next-owner actions

1. **Codex:** implement the long-running paper read-only coordinator: consume the
   verified database handle, acquire/renew one fence, load local projections,
   obtain complete broker account/position/order/fill truth, persist a startup
   reconciliation, remain mutation-disabled, monitor shutdown/connection loss,
   and emit only redacted heartbeat/alert evidence.
2. **Codex:** implement a provider-free market-data certification vertical
   slice and the true event/fill/cost/accounting backtest path before producing
   any performance or profitability evidence.
3. **Work:** confirm the exact individual Alpaca account classification and
   record a current review of customer, automated-trading, market-data,
   retention, and storage terms. Keep all credentials out of developer
   machines, Git, chat, and fixtures.
4. **Work:** authorize specific paper/research AWS accounts and a budget only
   after reviewing the plan and current price estimate. Do not authorize live
   infrastructure or order submission at this stage.

Until those and every later readiness gate have evidence, the only valid
operational conclusion is **HOLD — do not trade**.
