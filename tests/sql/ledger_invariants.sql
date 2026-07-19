\set ON_ERROR_STOP on

INSERT INTO strategy_releases (
    release_id, name, version, release_hash, code_hash, parameters_hash, universe_hash,
    data_hash, cost_model_hash, certificate_hash, status, valid_from, valid_until
) VALUES (
    '30000000-0000-0000-0000-000000000001',
    'test-release',
    '0.0.1',
    repeat('9', 64),
    repeat('a', 64),
    repeat('b', 64),
    repeat('c', 64),
    repeat('d', 64),
    repeat('e', 64),
    repeat('f', 64),
    'certified',
    '2026-07-18T00:00:00Z',
    '2026-08-18T00:00:00Z'
);

INSERT INTO activation_permits (
    permit_id, environment, account_fingerprint, strategy_release_id, strategy_release_hash,
    max_gross_notional, max_position_notional, max_daily_loss, max_drawdown,
    risk_limits_hash, issued_at, expires_at, operator_subject, approval_digest
) VALUES (
    '40000000-0000-0000-0000-000000000001',
    'paper',
    'acct-test',
    '30000000-0000-0000-0000-000000000001',
    repeat('9', 64),
    500.00,
    500.00,
    25.00,
    100.00,
    repeat('0', 64),
    '2026-07-18T00:00:00Z',
    '2026-08-18T00:00:00Z',
    'operator-test',
    repeat('1', 64)
);

INSERT INTO kill_events (
    kill_event_id, environment, account_fingerprint, severity,
    reason_code, detail, actor, operator_approved, approval_digest, occurred_at
) VALUES (
    '50000000-0000-0000-0000-000000000001',
    'paper',
    'acct-test',
    'hard',
    'TEST_HALT',
    '{"test":true}',
    'test-suite',
    FALSE,
    NULL,
    '2026-07-18T00:00:00Z'
);

DO $$
DECLARE
    v_severity TEXT;
BEGIN
    SELECT severity INTO v_severity
    FROM current_kill_state
    WHERE environment = 'paper' AND account_fingerprint = 'acct-test';

    IF v_severity <> 'hard' THEN
        RAISE EXCEPTION 'expected current hard kill state, got %', v_severity;
    END IF;
END;
$$;

DO $$
BEGIN
    BEGIN
        UPDATE activation_permits
        SET max_gross_notional = 999.00
        WHERE permit_id = '40000000-0000-0000-0000-000000000001';
        RAISE EXCEPTION 'immutable activation permit was rewritten';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'immutable activation permit was rewritten' THEN
                RAISE;
            END IF;
    END;
END;
$$;

DO $$
DECLARE
    v_token BIGINT;
    v_competing_token BIGINT;
    v_renewed BOOLEAN;
BEGIN
    v_token := acquire_executor_lease(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000001',
        INTERVAL '15 seconds'
    );
    IF v_token <> 1 THEN
        RAISE EXCEPTION 'expected initial fencing token 1, got %', v_token;
    END IF;

    v_competing_token := acquire_executor_lease(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000002',
        INTERVAL '15 seconds'
    );
    IF v_competing_token IS NOT NULL THEN
        RAISE EXCEPTION 'competing owner acquired active lease';
    END IF;

    v_renewed := renew_executor_lease(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000001',
        v_token,
        INTERVAL '15 seconds'
    );
    IF NOT v_renewed THEN
        RAISE EXCEPTION 'valid owner could not renew lease';
    END IF;

    v_renewed := renew_executor_lease(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000001',
        v_token + 1,
        INTERVAL '15 seconds'
    );
    IF v_renewed THEN
        RAISE EXCEPTION 'stale fencing token renewed lease';
    END IF;
END;
$$;

INSERT INTO activation_permits (
    permit_id, environment, account_fingerprint, strategy_release_id, strategy_release_hash,
    max_gross_notional, max_position_notional, max_daily_loss, max_drawdown,
    risk_limits_hash, issued_at, expires_at, operator_subject, approval_digest
) VALUES (
    '40000000-0000-0000-0000-000000000002',
    'paper',
    'lease-test',
    '30000000-0000-0000-0000-000000000001',
    repeat('9', 64),
    500.00,
    500.00,
    25.00,
    100.00,
    repeat('5', 64),
    '2026-07-18T00:00:00Z',
    '2026-08-18T00:00:00Z',
    'operator-test',
    repeat('9', 64)
);

INSERT INTO kill_events (
    kill_event_id, environment, account_fingerprint, severity,
    reason_code, detail, actor, operator_approved, approval_digest, occurred_at
) VALUES (
    '50000000-0000-0000-0000-000000000004',
    'paper',
    'lease-test',
    'clear',
    'TEST_INITIAL_AUTHORITY',
    '{}',
    'operator-test',
    TRUE,
    repeat('a', 64),
    clock_timestamp()
);

INSERT INTO account_snapshots (
    account_snapshot_id, environment, account_fingerprint, broker_timestamp,
    received_at, account_status, recognized_status, cash, equity, buying_power,
    trading_blocked, transfers_blocked, account_blocked, payload, payload_hash
) VALUES (
    '75500000-0000-0000-0000-000000000001',
    'paper',
    'lease-test',
    clock_timestamp(),
    clock_timestamp(),
    'ACTIVE',
    TRUE,
    10000.00,
    10000.00,
    10000.00,
    FALSE,
    FALSE,
    FALSE,
    '{}',
    repeat('b', 64)
);

INSERT INTO reconciliation_runs (
    reconciliation_id, environment, account_fingerprint, trigger,
    kill_event_id, fencing_token, started_at
) VALUES (
    '76000000-0000-0000-0000-000000000001',
    'paper',
    'lease-test',
    'startup',
    '50000000-0000-0000-0000-000000000004',
    1,
    clock_timestamp() - INTERVAL '100 milliseconds'
);

UPDATE reconciliation_runs
SET completed_at = clock_timestamp(),
    outcome = 'clean',
    resumable = TRUE,
    account_snapshot_id = '75500000-0000-0000-0000-000000000001',
    evidence_hash = repeat('b', 64)
WHERE reconciliation_id = '76000000-0000-0000-0000-000000000001';

INSERT INTO decision_snapshots (
    decision_id, strategy_release_id, environment, account_fingerprint,
    market_session, as_of, input_data_hash, account_snapshot_hash, payload
) VALUES (
    '70000000-0000-0000-0000-000000000001',
    '30000000-0000-0000-0000-000000000001',
    'paper',
    'lease-test',
    '2026-07-18',
    '2026-07-18T20:00:00Z',
    repeat('2', 64),
    repeat('3', 64),
    '{}'
);

INSERT INTO target_portfolios (
    target_portfolio_id, decision_id, strategy_release_id, reason_code, payload_hash
) VALUES (
    '71000000-0000-0000-0000-000000000001',
    '70000000-0000-0000-0000-000000000001',
    '30000000-0000-0000-0000-000000000001',
    'TEST_TARGET',
    repeat('4', 64)
);

INSERT INTO target_positions (
    target_portfolio_id, symbol, target_quantity, target_weight, reason_code
) VALUES (
    '71000000-0000-0000-0000-000000000001',
    'SPY',
    1,
    1,
    'TEST_TARGET'
);

INSERT INTO risk_decisions (
    risk_decision_id, target_portfolio_id, activation_permit_id, outcome, reason_codes,
    limit_snapshot, limit_snapshot_hash, decided_at
) VALUES (
    '72000000-0000-0000-0000-000000000001',
    '71000000-0000-0000-0000-000000000001',
    '40000000-0000-0000-0000-000000000002',
    'approved',
    ARRAY['TEST_APPROVAL'],
    '{}',
    repeat('5', 64),
    '2026-07-18T20:00:01Z'
);

INSERT INTO order_plans (
    order_plan_id, risk_decision_id, strategy_release_id, symbol, side,
    whole_quantity, decision_reference_price, decision_evidence_hash, created_at
) VALUES (
    '72500000-0000-0000-0000-000000000001',
    '72000000-0000-0000-0000-000000000001',
    '30000000-0000-0000-0000-000000000001',
    'SPY',
    'buy',
    1,
    499.00,
    repeat('6', 64),
    '2026-07-18T20:00:00Z'
);

INSERT INTO order_intents (
    intent_id, order_plan_id, risk_decision_id, strategy_release_id, environment,
    account_fingerprint, client_order_id, symbol, side, whole_quantity,
    order_type, limit_price, time_in_force, decision_at, arrival_quote,
    quote_provider_at, quote_received_at, quote_valid_until,
    quote_payload_hash, decision_evidence_hash, materialization_evidence_hash
) VALUES (
    '73000000-0000-0000-0000-000000000001',
    '72500000-0000-0000-0000-000000000001',
    '72000000-0000-0000-0000-000000000001',
    '30000000-0000-0000-0000-000000000001',
    'paper',
    'lease-test',
    'test-73000000000000000000000000000001',
    'SPY',
    'buy',
    1,
    'limit',
    500.00,
    'day',
    '2026-07-18T20:00:00Z',
    499.50,
    clock_timestamp() - INTERVAL '2 seconds',
    clock_timestamp() - INTERVAL '1 second',
    clock_timestamp() + INTERVAL '10 seconds',
    repeat('d', 64),
    repeat('6', 64),
    repeat('7', 64)
);

DO $$
BEGIN
    BEGIN
        INSERT INTO order_intents (
            intent_id, order_plan_id, risk_decision_id, strategy_release_id, environment,
            account_fingerprint, client_order_id, symbol, side, whole_quantity,
            order_type, limit_price, time_in_force, decision_at, arrival_quote,
            quote_provider_at, quote_received_at, quote_valid_until,
            quote_payload_hash, decision_evidence_hash, materialization_evidence_hash
        )
        SELECT
            '73000000-0000-0000-0000-000000000099',
            order_plan_id,
            risk_decision_id,
            strategy_release_id,
            environment,
            account_fingerprint,
            'test-mismatched-decision-evidence',
            symbol,
            side,
            whole_quantity,
            order_type,
            limit_price,
            time_in_force,
            decision_at,
            arrival_quote,
            quote_provider_at,
            quote_received_at,
            quote_valid_until,
            quote_payload_hash,
            repeat('0', 64),
            materialization_evidence_hash
        FROM order_intents
        WHERE intent_id = '73000000-0000-0000-0000-000000000001';
        RAISE EXCEPTION 'intent accepted mismatched decision evidence';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'intent accepted mismatched decision evidence' THEN
                RAISE;
            END IF;
            IF SQLERRM <> 'materialized order intent differs from its approved order plan' THEN
                RAISE;
            END IF;
    END;
END;
$$;

INSERT INTO intent_state_events (
    intent_state_event_id, intent_id, state, reason_code,
    detail, fencing_token, occurred_at
) VALUES (
    '73500000-0000-0000-0000-000000000001',
    '73000000-0000-0000-0000-000000000001',
    'persisted',
    'TEST_INTENT_PERSISTED',
    '{}',
    1,
    clock_timestamp()
), (
    '73500000-0000-0000-0000-000000000002',
    '73000000-0000-0000-0000-000000000001',
    'eligible',
    'TEST_INTENT_ELIGIBLE',
    '{}',
    1,
    clock_timestamp()
);

INSERT INTO order_outbox (
    outbox_id, intent_id, environment, account_fingerprint,
    created_fencing_token, payload, available_at
) VALUES (
    '74000000-0000-0000-0000-000000000001',
    '73000000-0000-0000-0000-000000000001',
    'paper',
    'lease-test',
    1,
    '{"client_order_id":"test-73000000000000000000000000000001"}',
    clock_timestamp() - INTERVAL '1 second'
);

DO $$
DECLARE
    v_rows INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox(
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'active executor could not claim durable outbox item';
    END IF;
END;
$$;

DO $$
DECLARE
    v_state TEXT;
    v_rows INTEGER;
BEGIN
    SELECT state INTO v_state
    FROM current_intent_states
    WHERE intent_id = '73000000-0000-0000-0000-000000000001';
    IF v_state <> 'dispatch_started' THEN
        RAISE EXCEPTION 'outbox claim did not atomically record first dispatch';
    END IF;

    UPDATE order_outbox
    SET claimed_at = clock_timestamp() - INTERVAL '31 seconds'
    WHERE outbox_id = '74000000-0000-0000-0000-000000000001';

    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox(
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'dispatch-started outbox item was blindly reclaimed';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox_recovery(
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'lookup-only recovery could not claim ambiguous dispatch';
    END IF;
END;
$$;

DO $$
DECLARE
    v_rows INTEGER;
    v_appended BOOLEAN;
    v_state TEXT;
    v_ready BOOLEAN;
    v_occurred_at TIMESTAMPTZ := clock_timestamp();
BEGIN
    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox_recovery_v2(
        'paper',
        'wrong-account',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'scoped recovery crossed its account authority domain';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox_recovery_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'scoped lookup recovery could not claim ambiguous dispatch';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM list_unresolved_order_outboxes_v2(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000001',
        1,
        100
    ) AS unresolved
    WHERE unresolved.outbox_id = '74000000-0000-0000-0000-000000000001'
      AND unresolved.current_state = 'dispatch_started';
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'restart discovery omitted the dispatched outbox';
    END IF;

    v_appended := append_submission_unknown_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1,
        '73500000-0000-0000-0000-000000000099',
        'TEST_SUBMISSION_UNKNOWN',
        '{"source":"test"}',
        v_occurred_at
    );
    IF NOT v_appended THEN
        RAISE EXCEPTION 'current fenced executor could not append submission unknown';
    END IF;

    -- The deterministic retry must compare the already-committed evidence
    -- rather than append another state event.
    v_appended := append_submission_unknown_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1,
        '73500000-0000-0000-0000-000000000099',
        'TEST_SUBMISSION_UNKNOWN',
        '{"source":"test"}',
        v_occurred_at
    );
    IF NOT v_appended THEN
        RAISE EXCEPTION 'submission-unknown deterministic retry did not recover';
    END IF;

    SELECT state INTO v_state
    FROM current_intent_states
    WHERE intent_id = '73000000-0000-0000-0000-000000000001';
    SELECT ready INTO v_ready
    FROM execution_readiness
    WHERE environment = 'paper' AND account_fingerprint = 'lease-test';
    IF v_state <> 'submission_unknown' OR v_ready IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'submission unknown did not block execution readiness';
    END IF;
END;
$$;

INSERT INTO broker_orders (
    broker_order_id, intent_id, client_order_id, environment,
    account_fingerprint, first_seen_at, raw_hash
) VALUES (
    'broker-confirmed-test',
    '73000000-0000-0000-0000-000000000001',
    'test-73000000000000000000000000000001',
    'paper',
    'lease-test',
    clock_timestamp(),
    repeat('e', 64)
);

INSERT INTO broker_order_events (
    broker_event_id, broker_order_id, client_order_id, provider_status,
    recognized_status, cumulative_filled_quantity, average_fill_price,
    provider_occurred_at, received_at, x_request_id, raw_payload, raw_hash
) VALUES (
    '74500000-0000-0000-0000-000000000001',
    'broker-confirmed-test',
    'test-73000000000000000000000000000001',
    'accepted',
    TRUE,
    0,
    NULL,
    clock_timestamp(),
    clock_timestamp(),
    'request-test-1',
    '{"status":"accepted"}',
    repeat('f', 64)
);

INSERT INTO intent_state_events (
    intent_state_event_id, intent_id, state, reason_code,
    detail, fencing_token, occurred_at
) VALUES (
    '73500000-0000-0000-0000-000000000003',
    '73000000-0000-0000-0000-000000000001',
    'broker_confirmed',
    'TEST_BROKER_CONFIRMED',
    '{}',
    1,
    clock_timestamp()
);

DO $$
DECLARE
    v_finalized BOOLEAN;
    v_rows INTEGER;
    v_ready BOOLEAN;
BEGIN
    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox_completion_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'accepted order was incorrectly claimable for completion';
    END IF;

    v_finalized := finalize_order_outbox_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1,
        'NONTERMINAL_ACCEPTED'
    );
    IF v_finalized THEN
        RAISE EXCEPTION 'accepted order incorrectly finalized its durable outbox';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM list_unresolved_order_outboxes_v2(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000001',
        1,
        100
    ) AS unresolved
    WHERE unresolved.outbox_id = '74000000-0000-0000-0000-000000000001'
      AND unresolved.current_state = 'broker_confirmed';
    SELECT ready INTO v_ready
    FROM execution_readiness
    WHERE environment = 'paper' AND account_fingerprint = 'lease-test';
    IF v_rows <> 1 OR v_ready IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'accepted order was not blocking and restart-discoverable';
    END IF;
END;
$$;

INSERT INTO broker_order_events (
    broker_event_id, broker_order_id, client_order_id, provider_status,
    recognized_status, cumulative_filled_quantity, average_fill_price,
    provider_occurred_at, received_at, x_request_id, raw_payload, raw_hash
) VALUES (
    '74500000-0000-0000-0000-000000000003',
    'broker-confirmed-test',
    'test-73000000000000000000000000000001',
    'done_for_day', TRUE, 0, NULL,
    clock_timestamp(), clock_timestamp(), 'request-test-done-for-day',
    '{"status":"done_for_day"}', repeat('1', 64)
), (
    '74500000-0000-0000-0000-000000000004',
    'broker-confirmed-test',
    'test-73000000000000000000000000000001',
    'calculated', TRUE, 0, NULL,
    clock_timestamp(), clock_timestamp(), 'request-test-calculated',
    '{"status":"calculated"}', repeat('2', 64)
);

INSERT INTO broker_order_events (
    broker_event_id, broker_order_id, client_order_id, provider_status,
    recognized_status, cumulative_filled_quantity, average_fill_price,
    provider_occurred_at, received_at, x_request_id, raw_payload, raw_hash
) VALUES (
    '74500000-0000-0000-0000-000000000002',
    'broker-confirmed-test',
    'test-73000000000000000000000000000001',
    'canceled',
    TRUE,
    0,
    NULL,
    clock_timestamp(),
    clock_timestamp(),
    'request-test-2',
    '{"status":"canceled"}',
    repeat('0', 64)
);

INSERT INTO intent_state_events (
    intent_state_event_id, intent_id, state, reason_code,
    detail, fencing_token, occurred_at
) VALUES (
    '73500000-0000-0000-0000-000000000004',
    '73000000-0000-0000-0000-000000000001',
    'terminal',
    'TEST_BROKER_TERMINAL',
    '{}',
    1,
    clock_timestamp()
);

DO $$
DECLARE
    v_finalized BOOLEAN;
    v_rows INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox_completion_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'terminal outbox was not reclaimed for completion only';
    END IF;

    v_finalized := finalize_order_outbox_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1,
        'BROKER_CONFIRMED'
    );
    IF NOT v_finalized THEN
        RAISE EXCEPTION 'current fenced executor could not finalize confirmed outbox item';
    END IF;

    v_finalized := finalize_order_outbox_v2(
        'paper',
        'lease-test',
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1,
        'DUPLICATE_FINALIZE'
    );
    IF v_finalized THEN
        RAISE EXCEPTION 'completed outbox item finalized twice';
    END IF;
END;
$$;

UPDATE executor_leases
SET renewed_at = clock_timestamp() - INTERVAL '2 seconds',
    lease_until = clock_timestamp() - INTERVAL '1 second'
WHERE environment = 'paper' AND account_fingerprint = 'lease-test';

DO $$
DECLARE
    v_token BIGINT;
    v_rows INTEGER;
BEGIN
    v_token := acquire_executor_lease(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000002',
        INTERVAL '15 seconds'
    );
    IF v_token <> 2 THEN
        RAISE EXCEPTION 'expected takeover fencing token 2, got %', v_token;
    END IF;

    INSERT INTO account_snapshots (
        account_snapshot_id, environment, account_fingerprint, broker_timestamp,
        received_at, account_status, recognized_status, cash, equity, buying_power,
        trading_blocked, transfers_blocked, account_blocked, payload, payload_hash
    ) VALUES (
        '75500000-0000-0000-0000-000000000002',
        'paper',
        'lease-test',
        clock_timestamp(),
        clock_timestamp(),
        'ACTIVE',
        TRUE,
        10000.00,
        10000.00,
        10000.00,
        FALSE,
        FALSE,
        FALSE,
        '{}',
        repeat('c', 64)
    );

    INSERT INTO reconciliation_runs (
        reconciliation_id, environment, account_fingerprint, trigger,
        kill_event_id, fencing_token, started_at
    ) VALUES (
        '76000000-0000-0000-0000-000000000002',
        'paper',
        'lease-test',
        'failover',
        '50000000-0000-0000-0000-000000000004',
        v_token,
        clock_timestamp() - INTERVAL '10 milliseconds'
    );

    UPDATE reconciliation_runs
    SET completed_at = clock_timestamp(),
        outcome = 'clean',
        resumable = TRUE,
        account_snapshot_id = '75500000-0000-0000-0000-000000000002',
        evidence_hash = repeat('c', 64)
    WHERE reconciliation_id = '76000000-0000-0000-0000-000000000002';

    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox(
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002',
        v_token
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'reconciled executor takeover blindly reclaimed prior dispatch';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox(
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001',
        1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'stale executor reclaimed outbox item';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM claim_order_outbox_recovery(
        '74000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002',
        v_token
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'completed outbox item was reclaimed for recovery';
    END IF;
END;
$$;

DO $$
BEGIN
    BEGIN
        UPDATE order_outbox
        SET payload = '{"tampered":true}'
        WHERE outbox_id = '74000000-0000-0000-0000-000000000001';
        RAISE EXCEPTION 'durable outbox payload was rewritten';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'durable outbox payload was rewritten' THEN
                RAISE;
            END IF;
    END;
END;
$$;

INSERT INTO activation_permit_revocations (
    revocation_id, permit_id, revoked_at, operator_subject,
    reason_code, approval_digest
) VALUES (
    '75000000-0000-0000-0000-000000000001',
    '40000000-0000-0000-0000-000000000001',
    '2026-07-18T20:05:00Z',
    'operator-test',
    'TEST_REVOCATION',
    repeat('7', 64)
);

INSERT INTO kill_events (
    kill_event_id, environment, account_fingerprint, severity,
    reason_code, detail, actor, operator_approved, approval_digest, occurred_at
) VALUES (
    '50000000-0000-0000-0000-000000000003',
    'paper',
    'acct-test',
    'clear',
    'TEST_OPERATOR_CLEAR',
    '{}',
    'operator-test',
    TRUE,
    repeat('8', 64),
    '2026-07-18T20:06:00Z'
);

DO $$
BEGIN
    IF has_table_privilege('alpaca_trader_runtime', 'strategy_releases', 'INSERT') THEN
        RAISE EXCEPTION 'runtime role can promote a strategy release';
    END IF;
    IF has_table_privilege('alpaca_trader_runtime', 'kill_events', 'INSERT') THEN
        RAISE EXCEPTION 'runtime role can directly clear kill state';
    END IF;
    IF has_table_privilege('alpaca_trader_runtime', 'order_intents', 'UPDATE') THEN
        RAISE EXCEPTION 'runtime role can rewrite an order intent';
    END IF;
    IF has_column_privilege(
        'alpaca_trader_runtime', 'intent_state_events', 'event_sequence', 'INSERT'
    ) OR has_column_privilege(
        'alpaca_trader_runtime', 'broker_order_events', 'event_sequence', 'INSERT'
    ) THEN
        RAISE EXCEPTION 'runtime role can forge server-ordered event sequence';
    END IF;
    IF has_column_privilege(
        'alpaca_trader_operator', 'experiment_events', 'event_sequence', 'INSERT'
    ) THEN
        RAISE EXCEPTION 'operator role can forge experiment event sequence';
    END IF;
    IF has_function_privilege(
        'alpaca_trader_runtime',
        'record_runtime_kill_event(uuid,text,text,text,text,jsonb,text,timestamp with time zone)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'runtime role retained the unfenced automated-halt function';
    END IF;
    IF NOT has_function_privilege(
        'alpaca_trader_runtime',
        'record_runtime_kill_event_v2(text,text,uuid,bigint,uuid,text,text,jsonb,text,timestamp with time zone)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'runtime role cannot record a fenced automated halt';
    END IF;
    IF NOT has_function_privilege(
        'alpaca_trader_runtime',
        'claim_order_outbox_recovery_v2(text,text,uuid,uuid,bigint)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'runtime role lacks lookup-only recovery capability';
    END IF;
    IF has_function_privilege(
        'alpaca_trader_runtime',
        'claim_order_outbox_recovery(uuid,uuid,bigint)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'runtime role retained unscoped recovery capability';
    END IF;
END;
$$;

DO $$
DECLARE
    v_mismatches INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_mismatches
    FROM runtime_schema_attestations AS manifest
    WHERE CASE manifest.object_kind
        WHEN 'function' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(pg_get_functiondef(procedure.oid), 'UTF8')), 'hex')
            FROM pg_proc AS procedure
            WHERE procedure.oid = to_regprocedure(manifest.object_identity)
        )
        WHEN 'view' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(pg_get_viewdef(relation.oid, true), 'UTF8')), 'hex')
            FROM pg_class AS relation
            WHERE relation.oid = to_regclass(manifest.object_identity)
              AND relation.relkind = 'v'
        )
        WHEN 'trigger' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(pg_get_triggerdef(trigger.oid, true), 'UTF8')), 'hex')
            FROM pg_trigger AS trigger
            JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname || '.' || relation.relname || '.' || trigger.tgname
                = manifest.object_identity
              AND NOT trigger.tgisinternal
              AND trigger.tgenabled IN ('O', 'A')
        )
        WHEN 'constraint' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(pg_get_constraintdef(con.oid, true), 'UTF8')), 'hex')
            FROM pg_constraint AS con
            JOIN pg_class AS relation ON relation.oid = con.conrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname || '.' || relation.relname || '.' || con.conname
                = manifest.object_identity
              AND con.convalidated
        )
    END;
    IF v_mismatches <> 0 THEN
        RAISE EXCEPTION 'runtime safety-definition attestation mismatch count %', v_mismatches;
    END IF;

    IF (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'function') < 39
       OR (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'trigger') < 28
       OR (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'constraint') < 106
    THEN
        RAISE EXCEPTION 'runtime safety-definition attestation is incomplete';
    END IF;
END;
$$;

CREATE ROLE alpaca_trader_test_login
    LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT alpaca_trader_runtime TO alpaca_trader_test_login;

SET SESSION AUTHORIZATION alpaca_trader_test_login;

DO $$
DECLARE
    v_unsafe_relations INTEGER;
    v_unsafe_sequences INTEGER;
BEGIN
    IF current_user <> 'alpaca_trader_test_login'
       OR session_user <> 'alpaca_trader_test_login'
       OR NOT pg_has_role(current_user, 'alpaca_trader_runtime', 'USAGE')
       OR pg_has_role(current_user, 'alpaca_trader_operator', 'MEMBER')
       OR NOT has_database_privilege(current_user, current_database(), 'CONNECT')
       OR has_database_privilege(current_user, current_database(), 'CREATE')
       OR has_database_privilege(current_user, current_database(), 'TEMPORARY')
       OR NOT has_schema_privilege(current_user, 'public', 'USAGE')
       OR has_schema_privilege(current_user, 'public', 'CREATE')
    THEN
        RAISE EXCEPTION 'actual runtime login authority is not connect/usage-only';
    END IF;

    SELECT COUNT(*) INTO v_unsafe_relations
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'public'
      AND relation.relkind IN ('r', 'p', 'v', 'm')
      AND (
          NOT has_table_privilege(current_user, relation.oid, 'SELECT')
          OR has_table_privilege(current_user, relation.oid, 'INSERT')
          OR has_table_privilege(current_user, relation.oid, 'UPDATE')
          OR has_table_privilege(current_user, relation.oid, 'DELETE')
          OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
          OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
          OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
      );
    SELECT COUNT(*) INTO v_unsafe_sequences
    FROM pg_class AS sequence
    JOIN pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
    WHERE namespace.nspname = 'public'
      AND sequence.relkind = 'S'
      AND (
          has_sequence_privilege(current_user, sequence.oid, 'USAGE')
          OR has_sequence_privilege(current_user, sequence.oid, 'SELECT')
          OR has_sequence_privilege(current_user, sequence.oid, 'UPDATE')
      );
    IF v_unsafe_relations <> 0 OR v_unsafe_sequences <> 0 THEN
        RAISE EXCEPTION 'actual runtime login inherited unsafe relation/sequence capability';
    END IF;
    IF NOT has_function_privilege(
        current_user,
        'insert_order_intent_v2(text,text,uuid,bigint,uuid,uuid,uuid,uuid,text,text,text,bigint,numeric,text,timestamp with time zone,numeric,timestamp with time zone,timestamp with time zone,timestamp with time zone,text,text,text,timestamp with time zone)',
        'EXECUTE'
    ) OR has_function_privilege(
        current_user,
        'claim_order_outbox(uuid,uuid,bigint)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'actual runtime login function allowlist drifted';
    END IF;
END;
$$;

RESET SESSION AUTHORIZATION;

ALTER ROLE alpaca_trader_test_login CREATEDB;
SET SESSION AUTHORIZATION alpaca_trader_test_login;
DO $$
BEGIN
    IF NOT (SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user) THEN
        RAISE EXCEPTION 'actual-login drift fixture did not become unsafe';
    END IF;
END;
$$;
RESET SESSION AUTHORIZATION;
ALTER ROLE alpaca_trader_test_login NOCREATEDB;
REVOKE alpaca_trader_runtime FROM alpaca_trader_test_login;
DROP ROLE alpaca_trader_test_login;

SET ROLE alpaca_trader_runtime;

DO $$
BEGIN
    BEGIN
        INSERT INTO reconciliation_runs (
            reconciliation_id, authority_sequence, environment,
            account_fingerprint, trigger, kill_event_id, fencing_token, started_at
        ) OVERRIDING SYSTEM VALUE VALUES (
            '76000000-0000-0000-0000-000000000099',
            9223372036854775807,
            'paper',
            'lease-test',
            'manual',
            '50000000-0000-0000-0000-000000000004',
            2,
            clock_timestamp()
        );
        RAISE EXCEPTION 'runtime forged reconciliation authority ordering';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
END;
$$;

DO $$
DECLARE
    v_decision_at TIMESTAMPTZ := clock_timestamp() - INTERVAL '3 seconds';
    v_provider_at TIMESTAMPTZ;
    v_received_at TIMESTAMPTZ;
    v_valid_until TIMESTAMPTZ;
    v_started_at TIMESTAMPTZ;
    v_snapshot_at TIMESTAMPTZ;
    v_rows INTEGER;
    v_ok BOOLEAN;
BEGIN
    v_provider_at := v_decision_at + INTERVAL '1 second';
    v_received_at := v_decision_at + INTERVAL '2 seconds';
    v_valid_until := v_received_at + INTERVAL '10 seconds';
    IF NOT renew_executor_lease(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000002', 2, INTERVAL '60 seconds'
    ) THEN
        RAISE EXCEPTION 'runtime wrapper smoke could not renew its fence';
    END IF;

    v_ok := insert_decision_snapshot_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '80000000-0000-0000-0000-000000000001',
        '30000000-0000-0000-0000-000000000001',
        v_decision_at::date, v_decision_at, repeat('1', 64), repeat('2', 64),
        '{"source":"runtime-wrapper-smoke"}'
    );
    v_ok := v_ok AND insert_target_portfolio_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '81000000-0000-0000-0000-000000000001',
        '80000000-0000-0000-0000-000000000001',
        '30000000-0000-0000-0000-000000000001', 'TEST_TARGET', repeat('3', 64)
    );
    v_ok := v_ok AND insert_target_position_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '81000000-0000-0000-0000-000000000001', 'SPY', 1, 1, 'TEST_TARGET'
    );
    v_ok := v_ok AND insert_risk_decision_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '82000000-0000-0000-0000-000000000001',
        '81000000-0000-0000-0000-000000000001',
        '40000000-0000-0000-0000-000000000002', 'approved',
        ARRAY['TEST_APPROVAL'], '{}', repeat('5', 64), v_decision_at
    );
    v_ok := v_ok AND insert_order_plan_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '82500000-0000-0000-0000-000000000001',
        '82000000-0000-0000-0000-000000000001',
        '30000000-0000-0000-0000-000000000001', 'SPY', 'buy', 1,
        499, repeat('6', 64), v_decision_at
    );
    v_ok := v_ok AND insert_order_intent_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '83000000-0000-0000-0000-000000000001',
        '82500000-0000-0000-0000-000000000001',
        '82000000-0000-0000-0000-000000000001',
        '30000000-0000-0000-0000-000000000001',
        'test-83000000000000000000000000000001', 'SPY', 'buy', 1, 500, 'day',
        v_decision_at, 499.5, v_provider_at, v_received_at, v_valid_until,
        repeat('7', 64), repeat('6', 64), repeat('8', 64), v_received_at
    );
    v_ok := v_ok AND insert_intent_state_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '83500000-0000-0000-0000-000000000001',
        '83000000-0000-0000-0000-000000000001',
        'persisted', 'TEST_PERSISTED', '{}', v_received_at
    );
    v_ok := v_ok AND insert_intent_state_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '83500000-0000-0000-0000-000000000002',
        '83000000-0000-0000-0000-000000000001',
        'eligible', 'TEST_ELIGIBLE', '{}', v_received_at
    );
    v_ok := v_ok AND insert_order_outbox_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '84000000-0000-0000-0000-000000000001',
        '83000000-0000-0000-0000-000000000001',
        '{"client_order_id":"test-83000000000000000000000000000001"}',
        v_received_at
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'runtime execution-chain wrapper returned false';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_order_outbox_v2(
        'paper', 'lease-test', '84000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'runtime wrapper outbox did not atomically begin dispatch';
    END IF;
    IF NOT append_submission_unknown_v2(
        'paper', 'lease-test', '84000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2,
        '83500000-0000-0000-0000-000000000003',
        'TEST_SUBMISSION_UNKNOWN', '{"bounded":true}', clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'runtime wrapper could not durably record submission unknown';
    END IF;

    v_ok := insert_broker_order_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        'broker-wrapper-test', '83000000-0000-0000-0000-000000000001',
        'test-83000000000000000000000000000001', clock_timestamp(), repeat('9', 64)
    );
    v_ok := v_ok AND insert_fill_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        'wrapper-fill-1', 'broker-wrapper-test',
        '83000000-0000-0000-0000-000000000001', 1, 500, 0,
        clock_timestamp(), clock_timestamp(), repeat('a', 64)
    );
    v_ok := v_ok AND insert_broker_event_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '84500000-0000-0000-0000-000000000001', 'broker-wrapper-test',
        'test-83000000000000000000000000000001', 'filled', TRUE, 1, 500,
        clock_timestamp(), clock_timestamp(), 'wrapper-request-1',
        '{"status":"filled"}', repeat('b', 64)
    );
    v_ok := v_ok AND insert_intent_state_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '83500000-0000-0000-0000-000000000004',
        '83000000-0000-0000-0000-000000000001',
        'broker_confirmed', 'TEST_BROKER_CONFIRMED', '{}', clock_timestamp()
    );
    v_ok := v_ok AND insert_intent_state_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '83500000-0000-0000-0000-000000000005',
        '83000000-0000-0000-0000-000000000001',
        'terminal', 'TEST_TERMINAL', '{}', clock_timestamp()
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'runtime broker/fill wrapper returned false';
    END IF;
    SELECT COUNT(*) INTO v_rows FROM claim_order_outbox_completion_v2(
        'paper', 'lease-test', '84000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2
    );
    IF v_rows <> 1 OR NOT finalize_order_outbox_v2(
        'paper', 'lease-test', '84000000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2, 'TEST_TERMINAL'
    ) THEN
        RAISE EXCEPTION 'runtime terminal wrapper could not finalize its outbox';
    END IF;

    v_started_at := clock_timestamp() - INTERVAL '10 milliseconds';
    v_snapshot_at := clock_timestamp();
    v_ok := insert_account_snapshot_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '85000000-0000-0000-0000-000000000001', v_snapshot_at, v_snapshot_at,
        'ACTIVE', TRUE, 10000, 10000, 10000, FALSE, FALSE, FALSE,
        '{"source":"wrapper"}', repeat('c', 64)
    );
    v_ok := v_ok AND insert_reconciliation_run_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '86000000-0000-0000-0000-000000000001', 'manual',
        '50000000-0000-0000-0000-000000000004', v_started_at
    );
    v_ok := v_ok AND finalize_reconciliation_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '86000000-0000-0000-0000-000000000001', clock_timestamp(),
        'clean', TRUE, '85000000-0000-0000-0000-000000000001', repeat('d', 64)
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'runtime clean reconciliation wrapper returned false';
    END IF;

    v_started_at := clock_timestamp() - INTERVAL '10 milliseconds';
    v_snapshot_at := clock_timestamp();
    v_ok := insert_account_snapshot_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '85000000-0000-0000-0000-000000000002', v_snapshot_at, v_snapshot_at,
        'ACTIVE', TRUE, 10000, 10000, 10000, FALSE, FALSE, FALSE,
        '{"source":"wrapper-diff"}', repeat('e', 64)
    );
    v_ok := v_ok AND insert_reconciliation_run_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '86000000-0000-0000-0000-000000000002', 'manual',
        '50000000-0000-0000-0000-000000000004', v_started_at
    );
    v_ok := v_ok AND insert_reconciliation_diff_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '86500000-0000-0000-0000-000000000001',
        '86000000-0000-0000-0000-000000000002',
        'cash', 'wrapper-diff', 'bounded mismatch'
    );
    v_ok := v_ok AND finalize_reconciliation_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '86000000-0000-0000-0000-000000000002', clock_timestamp(),
        'blocked', FALSE, '85000000-0000-0000-0000-000000000002', repeat('f', 64)
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'runtime blocked reconciliation wrapper returned false';
    END IF;
END;
$$;

DO $$
DECLARE
    v_recorded BOOLEAN;
BEGIN
    v_recorded := record_runtime_kill_event_v2(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000002',
        2,
        '77000000-0000-0000-0000-000000000001',
        'soft',
        'TEST_RUNTIME_HALT',
        '{"test":true}',
        'runtime-test',
        clock_timestamp()
    );
    IF NOT v_recorded THEN
        RAISE EXCEPTION 'runtime role could not record an automated halt';
    END IF;

    v_recorded := record_runtime_kill_event_v2(
        'paper',
        'lease-test',
        '60000000-0000-0000-0000-000000000002',
        2,
        '77000000-0000-0000-0000-000000000002',
        'clear',
        'ILLEGAL_RUNTIME_CLEAR',
        '{}',
        'runtime-test',
        clock_timestamp()
    );
    IF v_recorded THEN
        RAISE EXCEPTION 'runtime role cleared kill state through its function';
    END IF;
END;
$$;

RESET ROLE;

SET ROLE alpaca_trader_operator;

DO $$
BEGIN
    BEGIN
        INSERT INTO kill_events (
            kill_event_id, authority_sequence, environment, account_fingerprint,
            severity, reason_code, detail, actor, operator_approved,
            approval_digest, occurred_at
        ) OVERRIDING SYSTEM VALUE VALUES (
            '77000000-0000-0000-0000-000000000099',
            9223372036854775807,
            'paper',
            'lease-test',
            'clear',
            'FORGED_ORDERING',
            '{}',
            'operator-test',
            TRUE,
            repeat('e', 64),
            clock_timestamp()
        );
        RAISE EXCEPTION 'operator forged kill authority ordering';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
END;
$$;

RESET ROLE;

DO $$
BEGIN
    BEGIN
        INSERT INTO kill_events (
            kill_event_id, environment, account_fingerprint, severity,
            reason_code, detail, actor, occurred_at
        ) VALUES (
            '50000000-0000-0000-0000-000000000002',
            'paper',
            'acct-test',
            'clear',
            'ILLEGAL_AUTOMATIC_CLEAR',
            '{}',
            'test-suite',
            '2026-07-18T00:01:00Z'
        );
        RAISE EXCEPTION 'hard halt was cleared without operator approval';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        UPDATE kill_events
        SET reason_code = 'ILLEGAL_REWRITE'
        WHERE kill_event_id = '50000000-0000-0000-0000-000000000001';
        RAISE EXCEPTION 'append-only trigger allowed mutation';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'append-only trigger allowed mutation' THEN
                RAISE;
            END IF;
    END;
END;
$$;
