# WASP 2.0 — design notes and evidence

WASP 2.0 is a single-user trading-system project built around a Rust core,
a Python research interface, and a PostgreSQL ledger. Its central engineering
question is how to keep research and execution consistent while making broker
actions subject to explicit authority, durable records, and reconciliation.

**Operating status: HOLD — do not trade.** The repository's implementation
record and live-readiness gates define the current boundaries. A test result
or completed build does not authorize activation.

## Project direction

Chris McNosky directs the project scope, architecture, acceptance criteria,
review, correction requirements, and release decisions. The working agreements
in [AGENTS.md](AGENTS.md) require evidence for those decisions and prohibit the
software from approving its own release or clearing its own hard halts.

## Engineering decisions to inspect

### One shared strategy and risk core

The [Rust core](crates/trader-core/src/) contains the strategy, risk, replay,
and accounting logic. The [PyO3 bridge](crates/alpaca-autotrader-py/src/lib.rs)
exposes compiled behavior to Python research. This makes cross-language
consistency an explicit test target; see the
[compiled bridge parity checks](crates/alpaca-autotrader-py/tests/compiled_bridge_parity.py).

### Durable order intent and reconciliation

The execution layer records intent before submission. Ambiguous broker outcomes
require reconciliation against stable order identity. The relevant starting
points are [durable submission](crates/trader-execution/src/durable_submission.rs),
[reconciliation](crates/trader-execution/src/reconciliation.rs), and the
[order-safety regression tests](crates/trader-execution/tests/order_safety.rs).

### Explicit authority and readiness

Paper and live environments have separate configuration and broker hosts.
Readiness, human-approved activation, and halt handling are defined in
[the live-readiness contract](docs/LIVE_READINESS.md). The
[implementation-status matrix](docs/IMPLEMENTATION_STATUS.md) distinguishes
verified mechanisms, structural work, and remaining requirements.

### Evidence retained with the implementation

[The engineering gate](scripts/check.sh) includes repository audits and
Rust, Python, compiled-bridge, and database checks. The repository-specific
[integrity evaluation](stinger/RESULTS.md) retains its scenario results and
measurement limits. [The clean-room record](CLEAN_ROOM.md) documents source
provenance and the boundary against reusing earlier implementations.

## Review path

1. Read [the architecture](docs/ARCHITECTURE.md) for the module and authority boundaries.
2. Trace an order through the execution code and its tests.
3. Compare the behavior with [implementation status](docs/IMPLEMENTATION_STATUS.md).
4. Inspect [CI](https://github.com/cmcnosky/WASP-2.0/actions/workflows/ci.yml) and [dependency and image checks](https://github.com/cmcnosky/WASP-2.0/actions/workflows/security.yml) for the revision under review.

The current research path is a provider-free synthetic mechanics harness.
Real research, broker readiness, and activation require their own evidence;
the project makes no claim of demonstrated trading profitability.

## Contact

Chris McNosky · Dallas–Fort Worth, TX · cmcnosky@gmail.com ·
[Project portfolio](https://cmcnosky.github.io/)
