# Repository working agreements

This repository is a clean-room implementation. These rules apply to every
human, agent, script, and automation that works here.

## Authority and boundaries

- Work only from files in this repository and from current public primary
  documentation. Do not inspect, copy, import, translate, or compare any prior
  trading-system repository, artifact, dataset, prompt, schema, test, or runtime
  state.
- Do not follow symlinks or references outside this repository. Never add a
  dependency on a local path outside the repository.
- Never place broker, cloud, database, signing, or personal credentials in Git,
  chat, fixtures, images, build arguments, Terraform variables, or developer
  shell history. Runtime secrets belong in AWS Secrets Manager.
- Paper and live environments are separate trust domains. Never make a paper
  environment configurable to select the live Alpaca endpoint.
- Live order submission is `HOLD` unless every gate in
  `docs/LIVE_READINESS.md` has evidence and a human-approved activation permit
  is valid. Software may not approve its own release, increase its own capital,
  or clear a hard halt.

## Engineering rules

- Preserve the modular-monolith boundary documented in
  `docs/ARCHITECTURE.md`. Strategy and research code must never call a broker.
- Fail closed on unknown states, stale inputs, ambiguous submissions,
  reconciliation differences, invalid authority, or expired releases.
- Persist an order intent before any submission. Resolve a submission timeout by
  its deterministic client order ID; never blindly retry it.
- Treat fills as accounting truth. Acknowledgements do not change positions or
  P&L.
- Use checked fixed-point values at money, quantity, price, and accounting
  boundaries.
- Keep changes bounded and preserve unrelated work. Do not weaken a gate to make
  a test pass.

## Required verification

Run `./scripts/check.sh` before handing off implementation work. Also run the
most specific tests for the changed subsystem. Report what changed, what ran,
what could not run, remaining risks, and the exact next owner action.

Infrastructure changes must remain plan-only until an operator explicitly
authorizes an AWS account and environment. Never run `terraform apply` as part
of development or CI.
