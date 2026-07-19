# Market-data acquisition and certification

## Authority gate

**HOLD — do not acquire, store, or use non-public/private-feed data** until the
operator confirms the exact personal Alpaca account entitlement, storage,
retention, and derived-use terms. Recheck the current SIP plan and price when
ingestion is ready; no price copied into this repository is authorization.

No data, API response, credentials, or account artifact is committed to Git.
Research roles have no live broker secret. Data jobs write only to the
environment data bucket and never submit orders.

## Immutable artifacts

For each historical bars/trades/quotes response, calendar/clock snapshot, asset
manifest, and corporate-action input, retain compressed raw bytes and normalized
Parquet as separate immutable S3 objects. Every object/version records:

- provider/feed and entitlement identity;
- request endpoint, non-secret parameters, symbol/universe manifest, adjustment
  mode, and requested interval;
- provider/source timestamps, ingest/receive time, earliest strategy-available
  time, timezone, and market session;
- schema/normalizer version, row count, byte count, SHA-256, parent/raw object,
  correction/supersession relationship, and request ID;
- owner/job/release identity and certification status.

Raw and total-return-adjusted series are distinct. A correction creates a new
version and evidence event; it never overwrites or silently changes an input
used by a recorded decision or experiment.

## Certification

Certification fails closed on unresolved gaps, duplicate keys, non-monotonic
timestamps, observations after their declared availability time, timezone/DST
errors, missing early closes, symbol changes, splits/dividends, bad ticks,
halts/LULD, late bars, inconsistent adjustment, schema drift, or checksum/
provenance mismatch. Quarantine affected symbols/intervals; do not interpolate a
decision-critical input without a preregistered method and explicit evidence.

Use Alpaca calendar and clock data rather than hardcoded weekdays/session times.
Treat corporate actions as delayed until their observed publication time—never
assume event-time availability. Random certified samples and headline return
series require an independent calculation/reconciliation.

## Data access

- Production reads only immutable certified versions named by the strategy
  release and valid as of the decision timestamp.
- Backtests have no network access and receive a fixed dataset manifest.
- Python research can read designated research snapshots but cannot write or
  promote production releases.
- S3 versioning, KMS encryption, public-access blocks, object retention, and
  audit logs protect evidence. Deletion/retention exceptions require operator
  review and must preserve every release-referenced version.

The gate passes only when every strategy input has complete provenance and
availability evidence, critical defects are zero, and the certified dataset hash
is frozen in the preregistration and release.
