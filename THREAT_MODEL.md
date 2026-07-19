# Threat model

## Assets and safety properties

Protected assets are brokerage authority, cash and positions, market-data
entitlements, strategy releases, activation permits, signing material,
credentials, the append-only ledger, immutable research data, audit evidence,
and the operator's personal information.

The system must prevent unauthorized orders, duplicate orders, cross-environment
orders, orders based on stale or future data, silent strategy mutation,
self-promotion, secret disclosure, irreconcilable accounting, and continued
trading after loss of authority or broker truth.

## Trust boundaries

1. The human operator and release/activation process.
2. GitHub CI and short-lived AWS deployment identity.
3. Paper/research AWS trust domain.
4. Live AWS trust domain.
5. The Alpaca HTTPS and WebSocket APIs.
6. Public package registries and source dependencies.
7. Historical/live data and corporate-action inputs.

Research is untrusted for execution authority. It can produce candidate evidence
but cannot access live credentials, mutate live releases, or submit orders.
Broker acknowledgements are not trusted as position state; fills and subsequent
reconciliation are required.

## Principal threats and controls

| Threat | Primary controls | Failure behavior |
|---|---|---|
| Stolen broker/cloud credential | Secrets Manager/KMS, OIDC, least privilege, rotation, redaction | Hard halt, revoke, reconcile |
| Paper process reaches live broker | Fixed endpoint derived from environment; separate account/VPC/role/secret/state | Startup refuses mismatch |
| Duplicate executor/order | One desired task, stop-before-start deployment, PostgreSQL lease/fence, deterministic client order ID | Reconcile-only |
| Lost or timed-out submission response | Durable intent before submit; lookup by client order ID | `SUBMISSION_UNKNOWN`; no retry |
| Unknown broker status or event loss | Explicit state machine, bounded queues, sequence/freshness checks, periodic reconciliation | Fail closed |
| Compromised strategy/release | Immutable digest, certificate, expiry, operator activation permit | Reject release and halt |
| Backtest leakage/overfitting | Availability timestamps, immutable datasets, preregistration, sealed holdout, trial ledger | Strategy is not qualified |
| Corrupted ledger/database | Append-only events, transactional outbox, checksums, PITR, restore drills, independent accounting | Halt and restore/reconcile |
| Dependency/build compromise | Lockfiles, minimal image, SBOM, scans, digest-pinned deploy | Block promotion |
| Malicious or accidental operator action | Least privilege, explicit environment/account fingerprints, approval record, bounded capital | Reject or require reapproval |
| AWS or network outage | Multi-AZ live DB, alarms, dead-man monitor, retry budgets, recovery runbooks | Stop decisions; reconcile on recovery |
| Clock or calendar error | Alpaca calendar/clock, UTC persistence, skew and freshness alarms | Skip decision/halt |
| Secret or personal data in logs | Structured allowlisted logging and scanner | Rotate, quarantine, incident process |

## Residual risks

Alpaca, exchanges, market data, AWS, the public Internet, and broker-side order
protection can fail. Stops do not guarantee execution prices. Paper simulation
does not reproduce live liquidity or fills. Statistical qualification does not
guarantee future profit. The operator retains emergency broker access and accepts
these residual risks only through a live activation permit.

Review this model after any material API, dependency, account, strategy,
execution, infrastructure, or regulatory change and at least quarterly while
live.
