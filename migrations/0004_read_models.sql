BEGIN;

CREATE VIEW current_activation_permits AS
SELECT p.*
FROM activation_permits AS p
JOIN strategy_releases AS r ON r.release_id = p.strategy_release_id
LEFT JOIN activation_permit_revocations AS v ON v.permit_id = p.permit_id
WHERE v.permit_id IS NULL
  AND p.issued_at <= clock_timestamp()
  AND p.expires_at > clock_timestamp()
  AND r.status = 'certified'
  AND r.valid_from <= clock_timestamp()
  AND r.valid_until > clock_timestamp()
  AND 1 = (
      SELECT COUNT(*)
      FROM activation_permits AS candidate
      JOIN strategy_releases AS candidate_release
        ON candidate_release.release_id = candidate.strategy_release_id
      LEFT JOIN activation_permit_revocations AS candidate_revocation
        ON candidate_revocation.permit_id = candidate.permit_id
      WHERE candidate.environment = p.environment
        AND candidate.account_fingerprint = p.account_fingerprint
        AND candidate_revocation.permit_id IS NULL
        AND candidate.issued_at <= clock_timestamp()
        AND candidate.expires_at > clock_timestamp()
        AND candidate_release.status = 'certified'
        AND candidate_release.valid_from <= clock_timestamp()
        AND candidate_release.valid_until > clock_timestamp()
  );

CREATE VIEW current_intent_states AS
SELECT DISTINCT ON (intent_id)
    intent_id,
    state,
    reason_code,
    detail,
    fencing_token,
    occurred_at,
    intent_state_event_id,
    event_sequence
FROM intent_state_events
ORDER BY intent_id, event_sequence DESC;

CREATE VIEW current_broker_order_states AS
SELECT DISTINCT ON (broker_order_id)
    broker_order_id,
    client_order_id,
    provider_status,
    recognized_status,
    cumulative_filled_quantity,
    average_fill_price,
    provider_occurred_at,
    received_at,
    x_request_id,
    raw_hash,
    broker_event_id,
    event_sequence
FROM broker_order_events
ORDER BY broker_order_id, event_sequence DESC;

CREATE VIEW current_experiment_states AS
SELECT DISTINCT ON (experiment_id)
    experiment_id,
    status,
    result_hash,
    detail,
    occurred_at,
    experiment_event_id,
    event_sequence
FROM experiment_events
ORDER BY experiment_id, event_sequence DESC;

CREATE VIEW broker_position_quantities AS
SELECT
    o.environment,
    o.account_fingerprint,
    f.symbol,
    SUM(CASE WHEN f.side = 'buy' THEN f.quantity ELSE -f.quantity END) AS quantity,
    MAX(f.executed_at) AS last_fill_at
FROM fills AS f
JOIN broker_orders AS o ON o.broker_order_id = f.broker_order_id
GROUP BY o.environment, o.account_fingerprint, f.symbol;

CREATE VIEW latest_reconciliation AS
SELECT DISTINCT ON (environment, account_fingerprint)
    environment,
    account_fingerprint,
    reconciliation_id,
    authority_sequence,
    trigger,
    kill_event_id,
    fencing_token,
    started_at,
    completed_at,
    outcome,
    resumable,
    account_snapshot_id,
    evidence_hash
FROM reconciliation_runs
ORDER BY environment, account_fingerprint, authority_sequence DESC;

CREATE VIEW execution_readiness AS
SELECT
    p.environment,
    p.account_fingerprint,
    p.permit_id,
    p.strategy_release_id,
    k.severity AS kill_severity,
    k.kill_event_id,
    k.authority_sequence AS kill_authority_sequence,
    r.reconciliation_id,
    r.completed_at AS reconciled_at,
    r.outcome AS reconciliation_outcome,
    r.resumable,
    a.account_snapshot_id,
    a.account_status,
    a.recognized_status AS recognized_account_status,
    l.owner_id AS lease_owner_id,
    l.fencing_token,
    l.lease_until,
    (
        k.severity = 'clear'
        AND r.outcome IN ('clean', 'resolved')
        AND r.resumable
        AND r.completed_at >= l.acquired_at
        AND r.kill_event_id = k.kill_event_id
        AND r.fencing_token = l.fencing_token
        AND r.completed_at >= k.created_at
        AND r.completed_at >= p.issued_at
        AND r.completed_at <= clock_timestamp()
        AND r.completed_at > clock_timestamp() - INTERVAL '5 minutes'
        AND a.recognized_status
        AND a.account_status = 'ACTIVE'
        AND NOT a.trading_blocked
        AND NOT a.account_blocked
        AND NOT EXISTS (
            SELECT 1
            FROM reconciliation_diffs AS difference
            WHERE difference.reconciliation_id = r.reconciliation_id
              AND difference.resolution IN ('unresolved', 'escalated')
        )
        AND NOT EXISTS (
            SELECT 1
            FROM order_intents AS intent
            LEFT JOIN current_intent_states AS intent_state
              ON intent_state.intent_id = intent.intent_id
            WHERE intent.environment = p.environment
              AND intent.account_fingerprint = p.account_fingerprint
              AND (
                  intent_state.intent_id IS NULL
                  OR intent_state.state IN ('submission_unknown', 'blocked')
              )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM broker_orders AS broker_order
            LEFT JOIN current_broker_order_states AS broker_state
              ON broker_state.broker_order_id = broker_order.broker_order_id
            WHERE broker_order.environment = p.environment
              AND broker_order.account_fingerprint = p.account_fingerprint
              AND (
                  broker_state.broker_order_id IS NULL
                  OR NOT broker_state.recognized_status
              )
        )
        AND l.lease_until >= clock_timestamp()
    ) AS ready
FROM current_activation_permits AS p
LEFT JOIN current_kill_state AS k
    ON k.environment = p.environment
   AND k.account_fingerprint = p.account_fingerprint
LEFT JOIN latest_reconciliation AS r
    ON r.environment = p.environment
   AND r.account_fingerprint = p.account_fingerprint
LEFT JOIN account_snapshots AS a
    ON a.account_snapshot_id = r.account_snapshot_id
LEFT JOIN executor_leases AS l
    ON l.environment = p.environment
   AND l.account_fingerprint = p.account_fingerprint;

COMMIT;
