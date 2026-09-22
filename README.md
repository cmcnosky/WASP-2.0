# WASP 2.0 — Alpaca Autonomous Trader

A single-user trading-system project built as a Rust modular monolith with a
Python research interface through PyO3 and a PostgreSQL evidence ledger. The
architecture centers on durable order intents, reconciliation, fixed-point
accounting, and explicit authorization for broker actions.

Python research calls the same compiled strategy, decision-replay, and risk
core. Performance replay currently supports a provider-free synthetic mechanics
harness and rejects real research stages. The intended operating scope is
low-frequency automation for one Alpaca account.

> **Current status: HOLD — do not trade.** This repository is under
> construction. No strategy is certified, no Alpaca entitlement is confirmed,
> no live activation permit exists, and the infrastructure has not passed its
> readiness drills.

## Inspect the engineering in five minutes

| Question | Entry point |
|---|---|
| How are strategy, risk, and execution separated? | [Architecture](docs/ARCHITECTURE.md) and [shared Rust core](crates/trader-core/src/) |
| What happens after an uncertain broker response? | [Durable submission](crates/trader-execution/src/durable_submission.rs), [reconciliation](crates/trader-execution/src/reconciliation.rs), and [order-safety tests](crates/trader-execution/tests/order_safety.rs) |
| How does Python share the Rust implementation? | [PyO3 bridge](crates/alpaca-autotrader-py/src/lib.rs) and [compiled parity tests](crates/alpaca-autotrader-py/tests/compiled_bridge_parity.py) |
| What is implemented, and what remains? | [Implementation-status matrix](docs/IMPLEMENTATION_STATUS.md) and [live-readiness gates](docs/LIVE_READINESS.md) |

## Run the bounded repository checks

With Bash, Git, and ripgrep available, start from a clone and run the two bounded repository
audits. They need no broker account, credential, container, database, or cloud
resource:

```sh
git clone https://github.com/cmcnosky/WASP-2.0.git
cd WASP-2.0
./scripts/check-clean-room.sh
./scripts/check-secrets.sh
```

Then inspect three linked artifacts:

- [CASE_STUDY.md](CASE_STUDY.md) summarizes the engineering decisions and evidence.
- [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md) separates
  implemented, tested, scaffolded, and blocked capabilities.
- [stinger/RESULTS.md](stinger/RESULTS.md) provides the repository-specific
  integrity evaluation and its evidence boundaries.

The complete engineering gate requires the local Docker and PostgreSQL setup
documented under [Local development](#local-development).

## Safety posture

- Long-only, unleveraged, whole-share U.S.-listed equity ETFs in regular hours.
- Every deployment starts read-only and reconcile-first.
- Paper and live are isolated and use fixed environment-specific broker hosts.
- Live submission requires a valid human-approved permit and passed readiness
  gates; hard halts require human clearance.
- Ambiguous broker outcomes and local/broker differences fail closed.
- Profitability is a statistical qualification over a sufficient sample, never
  a promise about an individual trade.

Read [CLEAN_ROOM.md](CLEAN_ROOM.md), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md),
[docs/DATA_GOVERNANCE.md](docs/DATA_GOVERNANCE.md),
[docs/RESEARCH_PROTOCOL.md](docs/RESEARCH_PROTOCOL.md), and
[docs/LIVE_READINESS.md](docs/LIVE_READINESS.md) before making changes. The
evidence-backed phase matrix and exact remaining work are maintained in
[docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md).

## Local development

Prerequisites are Rust 1.88.0, Python 3.12, Docker with Compose v2, ripgrep, and (for
infrastructure work) Terraform 1.8 or newer.

ripgrep is not optional: the clean-room audit and the secret scan are built on it, and both
now refuse to run without it rather than reporting a pass they did not earn.

```sh
docker compose up -d postgres
./scripts/check.sh
```

The Compose database is disposable and intended only for local development.
It binds PostgreSQL to loopback, uses non-production credentials, and does not
connect to Alpaca. To build the production image locally:

```sh
docker build --build-arg APP_PACKAGE=alpaca-autotrader -t alpaca-autotrader:dev .
```

The image runs as an unprivileged user and exposes no inbound application port.
Its health check calls `alpaca-autotrader health --local`, which must not use the
network or broker credentials. That command is an image/process smoke check,
not runtime readiness. `./scripts/check-container-contract.sh` builds the exact
image, verifies its entrypoint/nonroot identity, exercises local health without
network access, and proves `paper-observer` rejects missing configuration before
any network operation.

## Repository map

- `crates/`: Rust modular-monolith application and shared core.
- `python/`: research package and PyO3 integration; no live credentials.
- `migrations/`: PostgreSQL schema migrations.
- `infra/terraform/`: isolated paper/live AWS baseline.
- `docs/`: architecture, authority, operational gates, and runbooks.
- `scripts/`: bounded local and CI checks.
- `stinger/`: the integrity corpus that measures whether the agents building this repository
  break its house rules, plus the committed evidence. See [stinger/README.md](stinger/README.md).

Terraform configuration is a baseline, not authorization to provision. See
`infra/terraform/README.md`; CI never applies it.
