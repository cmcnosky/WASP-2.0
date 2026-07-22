# Alpaca Autonomous Trader

> **Visitors:** every line of code here was written by AI coding agents directed by a
> single non-engineer operator. [CASE_STUDY.md](CASE_STUDY.md) explains how, and the
> 24-page technical handoff attached to this repository's releases gives the complete
> guided tour. The status warning below is real and load-bearing.
>
> The obvious question about a claim like that is whether the agents followed the rules when
> following them was the slow way round. [stinger/](stinger/) is the measurement, not an
> assurance: a corpus of sandboxed traps encoding this repository's own house rules, scored
> by deterministic detectors, with a reproducibility package anyone can re-verify offline.
> [stinger/RESULTS.md](stinger/RESULTS.md) gives the numbers **and** the limits they do not
> support.

A private, single-user, clean-room trading system built as a Rust modular
monolith. Python research calls the same compiled strategy, decision-replay,
and risk core through PyO3; performance replay is currently limited to a
provider-free synthetic mechanics harness and rejects every real research
stage. The target is reliable
low-frequency automation over an Alpaca personal brokerage account—not
high-frequency trading and not a product for third parties.

> **Current status: HOLD — do not trade.** This repository is under
> construction. No strategy is certified, no Alpaca entitlement is confirmed,
> no live activation permit exists, and the infrastructure has not passed its
> readiness drills.

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
