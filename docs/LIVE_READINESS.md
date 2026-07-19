# Live readiness and authority gates

**Default state: HOLD — do not trade.** A checked box without linked evidence is
not complete. The operator owns every approval marked **Work**; the application
or Codex may not infer it.

## Gate 1 — Clean room and build

- [ ] `./scripts/check.sh` passes from a clean checkout.
- [ ] No prior-project material, external local path, credential, or unreviewed
  direct dependency is present.
- [ ] CI produces locked tests, vulnerability results, image scan, provenance,
  and SBOM for the exact image digest.

## Gate 2 — Account, terms, and data (**Work**)

- [ ] The operator confirms an individual personal Alpaca Trading API account.
- [ ] Current customer agreement, automated-trading disclosure, market-data
  entitlement/storage terms, and pricing are reviewed and recorded.
- [ ] SIP access is verified for the exact account if the certified data design
  requires it; credentials are populated directly in live Secrets Manager.

## Gate 3 — Data and strategy

- [ ] Every input has provenance, checksum, feed/adjustment identity, and an
  availability-at-decision assertion; critical defects are zero.
- [ ] Preregistered trial ledger, chronological validation, sealed holdout,
  independent reproduction, cost stress, statistical tests, power, and
  drawdown gates pass.
- [ ] The one-sided 95% lower confidence bound for net annualized out-of-sample
  return exceeds `(annual AWS + data cost) / capital + 2 percentage points`.
- [ ] The immutable release and certificate digests are recorded; failed
  holdouts were not retuned.

## Gate 4 — Execution and recovery

- [ ] Deterministic Rust/Python parity, accounting conservation, append-only
  replay, order lifecycle, ambiguous result, and reconciliation tests pass.
- [ ] At least 60 trading sessions and 100 controlled lifecycle scenarios pass
  with zero duplicate/unexplained fills or unresolved cash/position differences.
- [ ] Kill, stale data, stream loss, crash, partial fill, 429/5xx/timeout,
  database failover, backup restore, rollback, credential rotation, and alert
  drills pass.

## Gate 5 — Cloud and operations (**Work** approval)

- [ ] Paper and live AWS account IDs, state backends, VPCs, roles, keys, secrets,
  databases, and deployment identities are verified separate.
- [ ] Current AWS estimate is below the $1,000/month ceiling and included in the
  strategy hurdle; `us-east-1` versus `us-east-2` broker latency is measured.
- [ ] Live Multi-AZ/PITR/deletion protection, two-AZ egress, alarms, dead-man,
  restore, and manual Alpaca emergency access are exercised.
- [ ] Exact immutable image digest is deployed with `execution_mode=read_only`
  for at least five successful live reconciliation sessions.

## Gate 6 — Activation permit (**Work**)

- [ ] The operator signs the exact environment, AWS account, Alpaca account
  fingerprint, release/certificate/image hashes, capital caps, risk caps,
  validity, and expiry.
- [ ] Initial gross exposure is at most `min($1,000, 5% of equity)`, one whole
  share position, one strategy; planned per-trade loss is at most 0.10%, daily
  soft halt 0.25%, and pilot hard drawdown 1%.
- [ ] Terraform `execution_mode=live` includes the approval ID and the application
  independently validates the permit. A real certified signal—not a ceremonial
  trade—controls entry and exit.

Execution remains or returns to HOLD on unclear entitlement, stale/defective
data, insufficient statistical evidence, negative economic hurdle, expired or
invalid authority, unknown order state, broker/local mismatch, protection loss,
provider degradation, or a material unreviewed change.
