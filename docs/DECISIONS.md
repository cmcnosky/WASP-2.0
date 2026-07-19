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
