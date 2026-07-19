# Decision log

Decisions are append-only. Superseding an entry requires a new dated entry that
names the old one and explains migration and safety impact.

## 0001 — Clean-room implementation

- **Status:** accepted at repository creation
- **Decision:** Start from a blank Git history and prohibit inspection or reuse
  of previous project material. Use only repository requirements, current public
  primary documentation, new entitled data, and original work.
- **Reason:** Independent provenance is a hard requirement.

## 0002 — Rust modular monolith plus PyO3

- **Status:** accepted
- **Decision:** Deploy one Rust process with internal module boundaries. Python
  calls the compiled strategy/backtest/risk core through PyO3 and contains no
  independently implemented live decision logic.
- **Reason:** One deterministic execution path minimizes research/production
  drift while retaining an effective research environment.

## 0003 — Low-frequency v1 scope

- **Status:** accepted
- **Decision:** V1 is long-only, unleveraged, whole-share, regular-hours trading
  of a frozen universe of liquid U.S.-listed equity ETFs at daily/swing cadence.
  It uses DAY marketable-limit orders and is not an HFT system.
- **Reason:** This is compatible with a small personal account and makes costs,
  risk, testing, and broker recovery tractable.

## 0004 — Durable intent and broker reconciliation

- **Status:** accepted
- **Decision:** PostgreSQL is execution authority. Commit an intent before
  submission, identify it deterministically, and recover uncertain submissions
  through lookup/reconciliation. Only fills update accounting.
- **Reason:** Network timeouts and broker event races make naive retries unsafe.

## 0005 — AWS managed baseline

- **Status:** accepted
- **Decision:** Begin with one ECS/Fargate task, RDS PostgreSQL, S3, ECR, Secrets
  Manager/KMS, CloudWatch/SNS, EventBridge, and a credential-free dead-man
  Lambda. No Kubernetes, message broker, cache, public dashboard, public IP,
  load balancer, or inbound application listener.
- **Reason:** This meets recovery and operational requirements with the least
  distributed-system surface.

## 0006 — Paper/live isolation and startup authority

- **Status:** accepted
- **Decision:** Prefer separate AWS accounts and state backends. Broker hosts are
  derived from immutable environment identity. Every deploy starts read-only,
  reconciles, acquires a fence, and validates a human activation permit.
- **Reason:** Configuration mistakes must not turn paper activity into live
  orders or create two active executors.

## 0007 — Profitability qualification

- **Status:** accepted
- **Decision:** A strategy must clear statistical, cost, holdout, shadow, and
  execution gates. Cloud and data costs count against its economic hurdle. “No
  strategy qualified” is a valid outcome; no trade is forced.
- **Reason:** A profitable first trade cannot be guaranteed, and a backtest alone
  is not evidence adequate for autonomous capital.

## 0008 — Predeployment canonical hash profile cutover

- **Status:** accepted on 2026-07-19 before any authorized paper/live deployment
- **Decision:** Application JSON evidence hashes use canonical profile
  `wasp-json-sha256-v1`: recursively lexical object-key order, preserved array
  order, compact UTF-8 JSON, UTC RFC 3339 timestamps using Rust/Chrono AutoSi
  fractional precision (zero, three, six, or nine digits), and exact signed
  `i128` or unsigned `u128` integers, then SHA-256. Every JSON floating-point
  number is forbidden. Statistical values that become evidence must use an
  explicit, versioned integer or decimal-string encoding. Rust and Python must
  reproduce the same digest. Python-owned datetimes are microsecond-limited;
  finer timestamps remain typed Rust/wire evidence. This profile replaces the
  earlier Rust-struct-declaration-order behavior for every
  `HashDigest::of_json` caller, including release, authority, decision, plan,
  intent, reconciliation, and persistence evidence.
- **Mechanical binding:** The cutover migration refuses to stamp the profile if
  any pre-existing runtime, broker, research, or observer rows are present. It
  records the profile digest in both the runtime and paper-observer schema
  attestation manifests, and both compiled schema verifiers require that exact
  attestation. Each Python experiment-ledger entry carries `hash_profile` in
  its hashed material; a missing or mismatched profile fails verification. The
  PyO3 module exports both the profile and the compiled performance-request byte
  ceiling, and the Python bridge refuses to load a core that omits or disagrees
  with either value. The CLI, bridge, PyO3 boundary, and Rust CLI enforce the
  bounded performance-request size before or at deserialization.
- **Migration and safety impact:** This changes deterministic hashes, IDs, and
  rematerialization evidence. The repository's current authority/status records
  state that no real RDS, Alpaca paper, or live deployment has run, so no
  authoritative persisted runtime state is recognized for migration. Disposable
  test data, earlier generated local artifacts, and earlier local experiment
  ledgers without the profile field are invalidated. If any unrecorded external
  state is later discovered, keep execution disabled and do not attempt recovery
  with this profile. Any future hash-profile change must be versioned and ship
  an explicit recovery/migration decision before deployment.
- **Reason:** Field-declaration order is not a language-neutral canonical
  contract. A single versioned profile is required for Rust/Python parity and
  durable evidence verification.
