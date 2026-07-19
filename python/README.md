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

Certification callers must supply timestamp-aligned net after-cost returns for the economic
bootstrap and net excess returns for Sharpe-family calculations. The PBO matrix must contain
all 12 preregistered configurations and must not contain sealed-holdout observations.
The current Rust `backtest` entry point deliberately reports
`performance_evidence_available=false`; no current repository path can produce
qualifying return evidence.
