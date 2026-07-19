# Shadow, paper, and fault campaign

Run the production binary for at least 60 trading sessions in shadow plus paper
before live qualification. Paper P&L is operational evidence only; it is not
profitability evidence.

The controlled campaign covers at least 100 independently recorded lifecycle
scenarios, including:

- duplicate, missing, late, corrupted, and out-of-order market/broker events;
- death before submit, after submit response loss, and during partial fill;
- HTTP timeout, 429, 5xx, DNS/TLS failure, rate exhaustion, and WebSocket loss;
- cancel/replace/fill races, unknown future statuses, and lost acknowledgements;
- database restart/failover, executor lease loss, stale fencing token, attempted
  overlapping deployment, backup restore, and rollback;
- stale quotes, clock skew, DST, early close, exchange halt/LULD, symbol/corporate
  action, and a corrected dataset;
- unexpected/manual broker order, position/cash difference, account restriction,
  missing protection, and ambiguous activity;
- incorrect environment/account/release/permit, paper attempt to select live,
  expired authority, leaked credential simulation, and rotation;
- per-trade/daily/drawdown breach, automated halt, operator hard halt, and
  controlled-liquidation authorization;
- missing heartbeat, task stop, queue saturation, persistence/decision latency
  objective breach, database pressure, and every operator alert path.

Every injected fault records seed/input, expected transition, intent/client
order/fence IDs, immutable image/release, observed events, timing, alerts,
reconciliation, and result. Recovery always enters reconcile-only before any
decision or submission.

Acceptance requires zero duplicate orders, unexplained fills, or unresolved
cash/position/order differences; byte-reproducible decisions; delivered alerts;
complete append-only evidence; tested backup restoration; and 100 passing
scenarios. Any critical failure resets the relevant observation window after
repair and independent review.
