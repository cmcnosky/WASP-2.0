# First strategy research protocol

This is a bounded research hypothesis, not trading advice or preauthorization.
The trial ledger is append-only and records every attempted, abandoned, failed,
and completed configuration before outcomes are examined.

## Frozen experiment family

Create an 8–12 symbol manifest of highly liquid, unleveraged U.S.-listed equity
index ETFs that existed before 2015. Fund classification and early-development
sample liquidity may select the universe; backtest returns may not.

At weekly or monthly after-close decisions, rank by trailing total return. Hold
the single strongest asset only when its absolute momentum is positive and its
price is above its trend filter; otherwise target cash. The family contains
exactly 12 configurations:

- momentum lookback: 63, 126, or 252 sessions;
- trend lookback: 126 or 252 sessions;
- rebalance: weekly or monthly.

Development is 2016–2022, chronological walk-forward validation is 2023–2024,
and 2025 through June 2026 is a sealed one-shot historical holdout. A later 60
trading-session prospective shadow period is genuinely fresh evidence. Shuffled
cross-validation and tuning against the holdout are prohibited. A failed
holdout rejects the release; a new family needs a new preregistration and new
untouched evidence.

## Replay and costs

The deterministic Rust production core executes at the next eligible quote
after decision time and modeled latency, never at the price that produced the
signal. Preserve point-in-time membership, availability times, early closes,
halts, dividends, splits, and symbol changes.

Model spread, latency slippage, fees, non-fill, partial fill, opportunity cost,
dividends, halts, and capacity/liquidity. Stress variable cost at the greater of
twice the modeled value or the empirical 95th-percentile cost bucket. Record
seed, core/release/data/cost hashes, parameters, trial number, code/runtime, and
complete results. Python analysis must reproduce Rust outputs rather than
recalculate decisions.

## Qualification

The economic hurdle is:

`(annual recurring AWS + market-data cost) / planned live capital + 2 percentage points`

The one-sided 95% lower confidence bound for annualized out-of-sample return
after all costs must exceed it. Also require:

- deflated Sharpe probability at least 0.95;
- probability of backtest overfitting no more than 0.10 when estimable;
- Hansen SPA or an equivalent familywise test with `p <= 0.05`;
- at least 80% power for the preregistered minimum worthwhile edge;
- stressed drawdown inside the exact certified account limit;
- no result dominated by one asset, period, or small trade cluster;
- independent reproduction of headline return and drawdown.

Methods, block lengths, dependence assumptions, trial count, annualization,
confidence-interval construction, minimum effect, cost model, rejection rules,
and missing-data rules must be fixed before the sealed holdout. These governance
thresholds reduce error; they do not guarantee future profit.

Qualification produces an immutable statistical certificate bound to exact
code, parameters, universe, data, cost model, and risk hashes with an expiry. A
material change is a new release and repeats certification. “No strategy
qualified” is a successful safe outcome.
