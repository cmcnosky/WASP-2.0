# Live activation and first round trip

## Stop condition

**HOLD — do not proceed** unless every item in `docs/LIVE_READINESS.md` has
linked evidence and the operator has approved the exact activation permit. No
deadline, desire for a test, favorable backtest, or paper profit overrides this.

## Read-only observation

1. Deploy the approved digest to the isolated live AWS account with Terraform
   `execution_mode=read_only`.
2. Populate live Alpaca credentials directly in that environment's Secrets
   Manager secret. Never print or retrieve the values for verification; use a
   bounded authenticated account request and record only a salted fingerprint.
3. Complete at least five trading sessions of account, calendar, data, order,
   position, activity, and cash reconciliation with no broker mutations.
4. Exercise manual Alpaca emergency access and confirm alert delivery.

## Activation

1. The operator approves a permit naming environment, AWS and Alpaca account
   fingerprints, image/release/certificate hashes, initial gross exposure,
   per-trade/daily/drawdown caps, validity, and expiry.
2. Obtain and review the Terraform plan setting `execution_mode=live` and the
   permit approval ID. Confirm the fixed host is `https://api.alpaca.markets`.
3. Deploy stop-before-start. Startup remains reconcile-only until the
   application validates the permit, broker truth, fence, freshness, risk, and
   protection gates.
4. Wait for the frozen strategy's real signal. If a whole share violates any
   notional or risk cap, skip it. Never force a ceremonial trade.

## First purchase and sale

- Maximum gross exposure is `min($1,000, 5% of account equity)`, with one
  whole-share position and one certified strategy.
- Planned loss per trade is at most 0.10% of equity; daily soft halt is 0.25%;
  pilot hard drawdown is 1%.
- Persist intent before submitting one whole-share DAY marketable-limit entry.
  Verify each broker event, fill, protection, and reconciliation.
- Exit only when the frozen strategy or a certified risk rule says to exit. Do
  not hold indefinitely to manufacture a profitable result.
- Produce an execution report with decision/arrival quotes, submit/ack/fill
  times, spread, slippage, fees, opportunity cost, and reconciled P&L.

A win does not prove profitability and a loss does not by itself disprove
expectancy. Do not retune from the first round trip.

## Automatic return to HOLD

Return to shadow/read-only on expired authority, unexpected drawdown, abnormal
live cost/P&L, unresolved reconciliation, provider degradation, missing
protection, missed service objective, or material API/dependency change. Only
the operator can clear a hard halt or issue a new permit.
