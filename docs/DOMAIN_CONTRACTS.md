# Domain contracts and invariants

Serialized contracts are versioned, canonical, and hashable. Times are UTC with
explicit source and receive timestamps. Money, prices, and quantities use
checked fixed-point representations at all accounting and order boundaries.
Application SHA-256 digests use exactly 64 lowercase hexadecimal characters;
OCI image digests retain the registry-standard `sha256:` prefix.
Application fixed-point values use six decimal places in Rust and PostgreSQL;
whole-share quantities must also be integral at broker-event and fill boundaries.

| Contract | Required contents |
|---|---|
| `StrategyRelease` | Immutable code, parameters, universe, data, cost-model, and statistical-certificate hashes; validity window and expiry |
| `DecisionSnapshot` | As-of time, completed observations, market session, account snapshot, availability assertions, and input-data hash |
| `TargetPortfolio` | Desired whole-share-compatible weights/positions and reason codes; no broker fields |
| `RiskDecision` | Approved/reduced/rejected targets, exact limit snapshot, authority evidence, and reason codes |
| `OrderIntent` | Durable ID, deterministic client order ID, release, symbol, side, whole quantity, limit, DAY time in force, decision evidence, and fence |
| `BrokerEvent` | Provider/client IDs, lifecycle status, fill data, provider/receive times, raw-payload hash, and request ID |
| `ReconciliationReport` | Local/broker orders, fills, positions, cash, unresolved differences, and resume decision |
| `ActivationPermit` | Environment, account fingerprint, exact strategy-release hash, capital/risk caps, validity, expiry, and operator approval |
| `KillState` | Soft/hard severity, trigger, timestamp, evidence, and operator-only clearance state |

Invariants:

- The same snapshot and release produce byte-identical target, risk, and intent
  output in backtest, shadow, paper, and live modes.
- Future-appended observations cannot alter an earlier decision.
- Strategy code cannot reach a broker adapter.
- Intent commits before submission; a deterministic client order ID is never
  reused for a different economic intent.
- Fill events—not acknowledgements—control positions and P&L.
- Unknown statuses and invalid state transitions fail closed.
- One fenced executor owns an account. A stale fencing token cannot submit,
  cancel, replace, or process a sole data stream.
- A release cannot train, promote, expand limits, or clear a hard halt itself.
- Paper and live credentials, endpoints, account fingerprints, databases,
  roles, and state never overlap.
