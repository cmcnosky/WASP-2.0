# Operations handbook

## Ownership and normal state

The system operates autonomously only inside a valid activation permit. The
operator owns account funding, legal/data entitlement, release approval,
capital increases, hard-halt clearance, emergency broker access, and decisions
to restore live authority.

Every process start, reconnect, deploy, failover, or restore enters
reconcile-only. It may resume only after broker/local orders, fills, positions,
and cash agree; the environment/account/release/permit are valid; data is fresh;
protection is present; and a current execution fence is held.

## Required telemetry

Emit structured logs and CloudWatch metrics without credentials or personal
account identifiers. Required signals include heartbeat, market-data age,
stream connection, queue saturation, event persistence latency, decision
latency, executor fence, unknown orders, reconciliation mismatches, order
rejections, active protection, kill state, release/permit validity, exposure,
realized/unrealized P&L, loss/drawdown gates, and task/database health.

The infrastructure alarms on absent heartbeat, task stop, database pressure,
and any positive safety-counter metric. Alerts identify environment, immutable
image/release, category, first/last occurrence, and a non-secret correlation ID.
The provider-free `health --local` command proves only that the image can launch
the binary without secrets or network access. It is not observer liveness,
database/lease readiness, cycle freshness, or reconciliation health; deployment
stays blocked until those signals and a runtime-aware health boundary exist.

## Daily procedure

1. Verify task, database, data stream, calendar, fence, release, permit,
   protection, and alert delivery health before the scheduled decision window.
2. Reconcile broker activities, orders, fills, positions, and cash against the
   ledger. Any difference enters reconcile-only.
3. Review decisions skipped due to data, risk, authority, or objective misses.
4. After the session, reconcile again and export an immutable signed audit
   summary to S3.
5. Never manually trade the automated account while execution authority is
   enabled. An external/manual order is a hard reconciliation event.

## Monthly procedure

Produce strategy, risk, execution-cost, cloud/data-cost, incident, dependency,
backup, and access reviews. Compare observed spread/slippage/fills and P&L with
the certified model. Review the threat model and all expiring exceptions,
permits, certificates, keys, and dependencies.

## Runbooks

- [Deployment and rollback](runbooks/DEPLOYMENT.md)
- [Live activation and first round trip](runbooks/LIVE_ACTIVATION.md)
- [Incident response](runbooks/INCIDENT_RESPONSE.md)
- [Backup and restore](runbooks/BACKUP_RESTORE.md)
- [Credential rotation](runbooks/CREDENTIAL_ROTATION.md)
- [Database bootstrap and runtime grants](runbooks/DATABASE_BOOTSTRAP.md)
- [Region benchmark](runbooks/REGION_BENCHMARK.md)

Qualification evidence also follows [data governance](DATA_GOVERNANCE.md), the
[research protocol](RESEARCH_PROTOCOL.md), and the [fault campaign](FAULT_CAMPAIGN.md).
