# Incident response

## Priorities

1. Protect life and account authority.
2. Prevent duplicate or unauthorized orders without destroying broker
   protection.
3. Establish broker truth before changing positions.
4. Preserve append-only evidence and credentials confidentiality.
5. Recover only into reconcile-only/read-only mode.

## Immediate actions

- On unknown order, fill, fence, account, data, protection, or release state:
  trigger hard halt, block new exposure, and page the operator.
- Cancel only known entry orders through the state machine. Do not blindly retry,
  replace, liquidate, or infer that a timeout means failure.
- Use deterministic client order IDs and broker queries to resolve submission
  ambiguity. Compare orders, activities, fills, positions, and cash.
- If credential compromise is possible, disable execution, revoke/rotate using
  the credential runbook, and inspect CloudTrail and broker activity without
  printing secret values.
- If clean-room contamination is suspected, stop builds and promotion, record
  path/hash/introducing commit without opening the artifact, preserve Git
  evidence, and notify the owner.

## Severity

- **SEV-1:** unauthorized/duplicate live order, unexplained live position/cash,
  credential exposure, two active executors, or protection failure with
  exposure. Hard halt and immediate operator action.
- **SEV-2:** ambiguous paper/live submission, persistent reconciliation
  mismatch, database restore/failover, stale data at a decision, or missed
  dead-man/critical alarm. Reconcile-only and promotion hold.
- **SEV-3:** degraded objective, rejected order spike, delayed data outside a
  decision window, or non-sensitive build/dependency issue. Safe skip and
  bounded remediation.

## Recovery and closure

Recovery requires known broker truth, valid ledger/accounting, current fence,
fresh data, valid release/permit, active protection, delivered alerts, and a
read-only observation. SEV-1/2 clearance requires operator approval and a new
activation permit when authority or release evidence changed.

The incident record contains UTC timeline, immutable image/release, environment
and redacted account fingerprint, affected intent IDs, broker request IDs, root
and contributing causes, customer/account impact, evidence locations,
containment, recovery checks, residual risk, and assigned preventive actions.
Promote repeated workflow corrections into code, tests, `AGENTS.md`, or a
mechanical check.
