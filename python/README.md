# Alpaca autotrader research package

This package is the clean-room Python research and experiment-orchestration layer. It does
not contain strategy, portfolio-construction, sizing, risk-decision, or order-planning logic.
Those decisions are made only by the compiled Rust extension named
`alpaca_autotrader_core`.

If the extension is absent, incompatible, returns invalid JSON, or raises an error, the bridge
fails closed. There is deliberately no Python decision fallback.

The bridge also delegates order-intent materialization to Rust. Callers must provide the exact
released snapshot, risk decision, order plan, and fresh post-decision quote; Python cannot create
or alter executable order fields.

The package provides:

- the locked chronological research protocol and 12-configuration preregistration;
- a hash-chained, append-only experiment ledger interface;
- canonical hashing and provenance records;
- stdlib-only probabilistic/deflated Sharpe, trial-hurdle, effective-sample,
  minimum-track-record, moving-block-bootstrap, and CSCV/PBO calculations;
- statistical/economic calculations over supplied return evidence, which must
  eventually be bound to immutable Rust backtest outputs and an independent
  reproduction before promotion;
- a CLI for protocol generation, ledger verification, gate reports, and Rust-core calls.

Python 3.12 or later is required. Run the dependency-free test suite from this directory:

```sh
PYTHONPATH=src python3.12 -m unittest discover -s tests -v
```

Show CLI help:

```sh
PYTHONPATH=src python3.12 -m alpaca_autotrader_research --help
```

The local JSONL ledger is tamper-evident and append-only through this API. The production
ledger must additionally use durable database permissions, backups, and retention controls.
Every ledger entry includes `hash_profile="wasp-json-sha256-v1"` in its hashed
material. Verification fails closed for a missing or different profile, so
pre-cutover local ledgers without that field are intentionally incompatible.

The same hash profile is used for cross-language application evidence. It sorts
object keys recursively, preserves array order, uses compact UTF-8 JSON and UTC
RFC 3339 timestamps, supports exact signed `i128` and unsigned `u128` integer
values, and forbids every JSON float. Statistical code may calculate with
floating point, but a result that becomes hashed evidence must first use an
explicit, versioned integer or decimal-string representation. The Python bridge
loads the PyO3 core only when it exports the exact profile and a positive
performance-request byte ceiling. The current ceiling is 16 MiB and is enforced
by the Python CLI and bridge as well as the PyO3 and Rust CLI boundaries.

Certification callers must supply timestamp-aligned net after-cost returns for the economic
bootstrap and net excess returns for Sharpe-family calculations. The PBO matrix must contain
all 12 preregistered configurations and must not contain sealed-holdout observations.
The legacy Rust `backtest` entry point deliberately reports
`performance_evidence_available=false`. The separate Rust
`performance_backtest` entry point is a resource-capped mechanics harness. It
can derive deterministic execution and accounting metrics from small synthetic
fixtures through the same compiled decision core, but it does not apply the
locked non-fill/partial-fill probabilities or stress-percentile rule, is not a
multi-year backtester, rejects every real research stage, and always reports
`stressed_performance_evidence_available=false` and
`qualifies_as_strategy_evidence=false`. No current repository path can produce
qualifying return evidence.

Harness decision sessions use a half-open interval ending before a required,
hash-bound terminal valuation at or after the final certified session's regular
close. The request contains one globally ordered, unique dividend-event stream;
each manifest dividend partition binds a cumulative availability prefix by
event count and hash, each decision binds exactly the prefix then available,
and terminal valuation binds the complete known stream. These rules prevent a
last decision from executing across the declared research boundary and permit a
complete result without manufacturing a final trade. They do not turn synthetic
fixtures into certified data, efficacy evidence, stressed results, a qualified
strategy, or a deployed paper/live system.
