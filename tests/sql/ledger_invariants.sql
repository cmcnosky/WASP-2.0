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
    v_requested_at TIMESTAMPTZ := clock_timestamp();
    v_not_dispatched_at TIMESTAMPTZ;
    v_accepted_at TIMESTAMPTZ;
    v_rows INTEGER;
    v_ready BOOLEAN;
BEGIN
    IF persist_cancel_intent_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1,
        '74600000-0000-0000-0000-000000000001',
        '74700000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000002',
        'test-73000000000000000000000000000001',
        'broker-confirmed-test', 'TEST_CANCEL_REQUEST', v_requested_at,
        '{"command":"unbound-cancel"}'
    ) OR EXISTS (
        SELECT 1 FROM cancel_intents
        WHERE cancel_intent_id = '74600000-0000-0000-0000-000000000001'
    ) THEN
        RAISE EXCEPTION 'unbound cancel payload created partial durable authority';
    END IF;

    IF NOT persist_cancel_intent_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1,
        '74600000-0000-0000-0000-000000000001',
        '74700000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000002',
        'test-73000000000000000000000000000001',
        'broker-confirmed-test', 'TEST_CANCEL_REQUEST', v_requested_at,
        jsonb_build_object(
            'cancel_intent_id', '74600000-0000-0000-0000-000000000001'::uuid,
            'client_order_id', 'test-73000000000000000000000000000001',
            'provider_order_id', 'broker-confirmed-test',
            'reason_code', 'TEST_CANCEL_REQUEST',
            'requested_at', v_requested_at
        )
    ) THEN
        RAISE EXCEPTION 'durable cancellation intent/outbox did not commit atomically';
    END IF;

    IF NOT persist_cancel_intent_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1,
        '74600000-0000-0000-0000-000000000001',
        '74700000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000002',
        'test-73000000000000000000000000000001',
        'broker-confirmed-test', 'TEST_CANCEL_REQUEST', v_requested_at,
        jsonb_build_object(
            'cancel_intent_id', '74600000-0000-0000-0000-000000000001'::uuid,
            'client_order_id', 'test-73000000000000000000000000000001',
            'provider_order_id', 'broker-confirmed-test',
            'reason_code', 'TEST_CANCEL_REQUEST',
            'requested_at', v_requested_at
        )
    ) THEN
        RAISE EXCEPTION 'exact cancellation persistence retry did not recover';
    END IF;

    IF persist_cancel_intent_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1,
        '74600000-0000-0000-0000-000000000001',
        '74700000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000001',
        '74800000-0000-0000-0000-000000000002',
        'test-73000000000000000000000000000001',
        'broker-confirmed-test', 'CONFLICTING_CANCEL_REASON', v_requested_at,
        jsonb_build_object(
            'cancel_intent_id', '74600000-0000-0000-0000-000000000001'::uuid,
            'client_order_id', 'test-73000000000000000000000000000001',
            'provider_order_id', 'broker-confirmed-test',
            'reason_code', 'CONFLICTING_CANCEL_REASON',
            'requested_at', v_requested_at
        )
    ) THEN
        RAISE EXCEPTION 'conflicting duplicate cancellation was accepted';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 1 OR NOT EXISTS (
        SELECT 1 FROM current_cancel_states
        WHERE cancel_intent_id = '74600000-0000-0000-0000-000000000001'
          AND state = 'dispatch_started'
    ) THEN
        RAISE EXCEPTION 'cancel claim did not durably record dispatch before DELETE authority';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'dispatched cancellation was authorized for a second DELETE';
    END IF;

    v_not_dispatched_at := clock_timestamp();
    IF NOT append_cancel_not_dispatched_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000010',
        'broker-confirmed-test', 1, 'TRANSPORT_BEFORE_SEND',
        'request budget denied before transport I/O', repeat('d', 64),
        v_not_dispatched_at
    ) OR NOT append_cancel_not_dispatched_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000010',
        'broker-confirmed-test', 1, 'TRANSPORT_BEFORE_SEND',
        'request budget denied before transport I/O', repeat('d', 64),
        v_not_dispatched_at
    ) THEN
        RAISE EXCEPTION 'proven pre-I/O non-dispatch was not durable and idempotent';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM list_unresolved_cancel_outboxes_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1, 100
    ) AS unresolved
    WHERE unresolved.cancel_outbox_id = '74700000-0000-0000-0000-000000000001'
      AND unresolved.current_state = 'not_dispatched'
      AND unresolved.state_reason_code = 'TRANSPORT_BEFORE_SEND'
      AND unresolved.state_not_dispatched_provider_order_id = 'broker-confirmed-test'
      AND unresolved.state_dispatch_attempt_count = 1
      AND unresolved.state_evidence_hash = repeat('d', 64)
      AND unresolved.detail = 'request budget denied before transport I/O'
      AND unresolved.terminal_broker_event_id IS NULL;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'not-dispatched cancellation was not restart-discoverable with exact evidence';
    END IF;

    -- Exercise terminal completion directly from not_dispatched inside a
    -- subtransaction, then roll the synthetic terminal truth back so the same
    -- durable cancellation can continue through the retry-path checks below.
    BEGIN
        INSERT INTO broker_order_events (
            broker_event_id, broker_order_id, client_order_id, provider_status,
            recognized_status, cumulative_filled_quantity, average_fill_price,
            provider_occurred_at, received_at, x_request_id, raw_payload, raw_hash
        ) VALUES (
            '74500000-0000-0000-0000-000000000020',
            'broker-confirmed-test',
            'test-73000000000000000000000000000001',
            'canceled', TRUE, 0, NULL,
            clock_timestamp(), clock_timestamp(), 'request-not-dispatched-terminal',
            '{"status":"canceled","path":"not_dispatched"}', repeat('9', 64)
        );
        INSERT INTO intent_state_events (
            intent_state_event_id, intent_id, state, reason_code,
            detail, fencing_token, occurred_at
        ) VALUES (
            '73500000-0000-0000-0000-000000000020',
            '73000000-0000-0000-0000-000000000001',
            'terminal', 'TEST_NOT_DISPATCHED_TERMINAL', '{}', 1,
            clock_timestamp()
        );
        SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_completion_v2(
            'paper', 'lease-test',
            '74700000-0000-0000-0000-000000000001',
            '60000000-0000-0000-0000-000000000001', 1
        );
        IF v_rows <> 1 OR NOT finalize_cancel_outbox_v2(
            'paper', 'lease-test',
            '74700000-0000-0000-0000-000000000001',
            '60000000-0000-0000-0000-000000000001', 1,
            '74800000-0000-0000-0000-000000000020',
            '74500000-0000-0000-0000-000000000020',
            'BROKER_TERMINAL_CANCELED'
        ) THEN
            RAISE EXCEPTION 'not-dispatched cancellation did not permit terminal-only completion';
        END IF;
        RAISE EXCEPTION 'rollback-not-dispatched-terminal-proof';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM <> 'rollback-not-dispatched-terminal-proof' THEN
                RAISE;
            END IF;
    END;
    IF EXISTS (
        SELECT 1 FROM broker_order_events
        WHERE broker_event_id = '74500000-0000-0000-0000-000000000020'
    ) OR NOT EXISTS (
        SELECT 1 FROM current_cancel_states
        WHERE cancel_intent_id = '74600000-0000-0000-0000-000000000001'
          AND state = 'not_dispatched'
          AND dispatch_attempt_count = 1
    ) THEN
        RAISE EXCEPTION 'not-dispatched terminal proof did not roll back cleanly';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_recovery_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'not-dispatched cancellation incorrectly authorized lookup-only recovery';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_retry_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    ) AS retry
    WHERE retry.attempt_count = 2
      AND retry.current_state = 'dispatch_started';
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'durable non-dispatch did not authorize exactly one retry attempt';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_retry_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'retry dispatch marker authorized a second DELETE after a lost response';
    END IF;

    IF append_cancel_not_dispatched_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000011',
        'broker-confirmed-test', 1, 'TRANSPORT_BEFORE_SEND',
        'stale worker evidence', repeat('e', 64), clock_timestamp()
    ) OR EXISTS (
        SELECT 1 FROM cancel_state_events
        WHERE cancel_state_event_id = '74800000-0000-0000-0000-000000000011'
    ) THEN
        RAISE EXCEPTION 'stale dispatch attempt overwrote a newer retry marker';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_recovery_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'lost retry-claim response did not fail closed to lookup-only recovery';
    END IF;

    v_accepted_at := clock_timestamp();
    IF NOT append_cancel_request_accepted_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000003',
        'broker-confirmed-test', 'cancel-request-accepted', repeat('a', 64),
        v_accepted_at
    ) OR NOT append_cancel_request_accepted_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000003',
        'broker-confirmed-test', 'cancel-request-accepted', repeat('a', 64),
        v_accepted_at
    ) THEN
        RAISE EXCEPTION 'cancel acceptance was not durable and idempotent';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_recovery_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'accepted cancellation was not restart-recoverable';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM list_unresolved_cancel_outboxes_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1, 100
    ) AS unresolved
    WHERE unresolved.cancel_outbox_id = '74700000-0000-0000-0000-000000000001'
      AND unresolved.current_state = 'request_accepted'
      AND unresolved.request_id = 'cancel-request-accepted';
    SELECT ready INTO v_ready FROM execution_readiness_v2
    WHERE environment = 'paper' AND account_fingerprint = 'lease-test';
    IF v_rows <> 1 OR v_ready IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'accepted cancellation was not blocking and discoverable';
    END IF;

    BEGIN
        INSERT INTO cancel_state_events (
            cancel_state_event_id, cancel_intent_id, state, reason_code,
            broker_event_id, fencing_token, occurred_at
        ) VALUES (
            '74800000-0000-0000-0000-000000000099',
            '74600000-0000-0000-0000-000000000001',
            'terminal', 'ILLEGAL_ACK_TERMINAL',
            '74500000-0000-0000-0000-000000000001', 1, clock_timestamp()
        );
        RAISE EXCEPTION 'nonterminal broker acknowledgement became terminal cancel truth';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM <> 'terminal cancel state lacks terminal broker truth' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE cancel_outbox
        SET completed_at = clock_timestamp(), completion_reason = 'ILLEGAL_ACK_TERMINAL'
        WHERE cancel_outbox_id = '74700000-0000-0000-0000-000000000001';
        RAISE EXCEPTION 'nonterminal cancellation outbox completed directly';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM <> 'cancel outbox completion lacks terminal cancel evidence' THEN
                RAISE;
            END IF;
    END;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_completion_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 OR finalize_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000004',
        '74500000-0000-0000-0000-000000000001', 'NONTERMINAL_ACK'
    ) THEN
        RAISE EXCEPTION '204 acceptance was incorrectly treated as terminal cancellation';
    END IF;
END;
$$;

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

    SELECT COUNT(*) INTO v_rows
    FROM list_unresolved_cancel_outboxes_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000001', 1, 100
    ) AS unresolved
    WHERE unresolved.cancel_outbox_id = '74700000-0000-0000-0000-000000000001'
      AND unresolved.current_state = 'request_accepted'
      AND unresolved.terminal_broker_event_id = '74500000-0000-0000-0000-000000000002'
      AND unresolved.terminal_provider_order_id = 'broker-confirmed-test'
      AND unresolved.terminal_client_order_id = 'test-73000000000000000000000000000001'
      AND unresolved.terminal_provider_status = 'canceled'
      AND unresolved.terminal_recognized_status
      AND unresolved.terminal_cumulative_filled_quantity::numeric = 0
      AND unresolved.terminal_average_fill_price IS NULL
      AND unresolved.terminal_provider_occurred_at = (
          SELECT provider_occurred_at FROM broker_order_events
          WHERE broker_event_id = '74500000-0000-0000-0000-000000000002'
      )
      AND unresolved.terminal_received_at = (
          SELECT received_at FROM broker_order_events
          WHERE broker_event_id = '74500000-0000-0000-0000-000000000002'
      )
      AND unresolved.terminal_request_id = 'request-test-2'
      AND unresolved.terminal_raw_hash = repeat('0', 64);
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'restart discovery did not expose exact current terminal broker evidence';
    END IF;

    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'terminal broker truth reauthorized first cancel dispatch';
    END IF;
    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_retry_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'terminal broker truth reauthorized retry cancel dispatch';
    END IF;
    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_recovery_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 0 THEN
        RAISE EXCEPTION 'terminal broker truth exposed lookup instead of completion-only work';
    END IF;

    SELECT COUNT(*) INTO v_rows
    FROM claim_cancel_outbox_completion_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'terminal broker truth did not expose cancel completion-only work';
    END IF;

    IF finalize_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000098',
        '74500000-0000-0000-0000-000000000002',
        'BROKER_TERMINAL_FILLED'
    ) OR EXISTS (
        SELECT 1 FROM cancel_state_events
        WHERE cancel_state_event_id = '74800000-0000-0000-0000-000000000098'
    ) THEN
        RAISE EXCEPTION 'cancel finalization accepted a reason mismatched to broker status';
    END IF;

    v_finalized := finalize_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000004',
        '74500000-0000-0000-0000-000000000002',
        'BROKER_TERMINAL_CANCELED'
    );
    IF NOT v_finalized OR NOT finalize_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000004',
        '74500000-0000-0000-0000-000000000002',
        'BROKER_TERMINAL_CANCELED'
    ) THEN
        RAISE EXCEPTION 'terminal cancel finalization was not durable and idempotent';
    END IF;
    IF finalize_cancel_outbox_v2(
        'paper', 'lease-test',
        '74700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000001', 1,
        '74800000-0000-0000-0000-000000000004',
        '74500000-0000-0000-0000-000000000001',
        'BROKER_TERMINAL_CANCELED'
    ) OR EXISTS (
        SELECT 1 FROM cancel_outbox
        WHERE cancel_outbox_id = '74700000-0000-0000-0000-000000000001'
          AND completed_at IS NULL
    ) OR NOT EXISTS (
        SELECT 1 FROM current_cancel_states
        WHERE cancel_intent_id = '74600000-0000-0000-0000-000000000001'
          AND state = 'terminal'
          AND broker_event_id = '74500000-0000-0000-0000-000000000002'
    ) THEN
        RAISE EXCEPTION 'cancel completion was not bound to exact terminal broker evidence';
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
    IF has_table_privilege('alpaca_trader_runtime', 'cancel_intents', 'INSERT')
       OR has_table_privilege('alpaca_trader_runtime', 'cancel_state_events', 'INSERT')
       OR has_table_privilege('alpaca_trader_runtime', 'cancel_outbox', 'UPDATE')
    THEN
        RAISE EXCEPTION 'runtime role bypasses fenced cancellation functions';
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
    IF NOT has_function_privilege(
        'alpaca_trader_runtime',
        'claim_order_outbox_v3(text,text,uuid,uuid,bigint)',
        'EXECUTE'
    ) OR has_function_privilege(
        'alpaca_trader_runtime',
        'claim_order_outbox_v2(text,text,uuid,uuid,bigint)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        'alpaca_trader_runtime',
        'persist_cancel_intent_v2(text,text,uuid,bigint,uuid,uuid,uuid,uuid,text,text,text,timestamp with time zone,jsonb)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        'alpaca_trader_runtime',
        'finalize_cancel_outbox_v2(text,text,uuid,uuid,bigint,uuid,uuid,text)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        'alpaca_trader_runtime',
        'claim_cancel_outbox_retry_v2(text,text,uuid,uuid,bigint)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        'alpaca_trader_runtime',
        'append_cancel_not_dispatched_v2(text,text,uuid,uuid,bigint,uuid,text,integer,text,text,text,timestamp with time zone)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'runtime cancellation function allowlist drifted';
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

    IF (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'function') <> 54
       OR (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'view') <> 3
       OR (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'trigger') <> 34
       OR (SELECT COUNT(*) FROM runtime_schema_attestations WHERE object_kind = 'constraint') <> 139
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
    ) OR NOT has_function_privilege(
        current_user,
        'claim_order_outbox_v3(text,text,uuid,uuid,bigint)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        current_user,
        'append_cancel_unknown_v2(text,text,uuid,uuid,bigint,uuid,text,timestamp with time zone)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        current_user,
        'claim_cancel_outbox_retry_v2(text,text,uuid,uuid,bigint)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        current_user,
        'append_cancel_not_dispatched_v2(text,text,uuid,uuid,bigint,uuid,text,integer,text,text,text,timestamp with time zone)',
        'EXECUTE'
    ) OR has_function_privilege(
        current_user,
        'claim_order_outbox_v2(text,text,uuid,uuid,bigint)',
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
    v_cancel_requested_at TIMESTAMPTZ;
    v_cancel_unknown_at TIMESTAMPTZ;
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

    SELECT COUNT(*) INTO v_rows FROM claim_order_outbox_v3(
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
    v_ok := v_ok AND insert_broker_event_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '84500000-0000-0000-0000-000000000000', 'broker-wrapper-test',
        'test-83000000000000000000000000000001', 'accepted', TRUE, 0, NULL,
        clock_timestamp(), clock_timestamp(), 'wrapper-request-accepted',
        '{"status":"accepted"}', repeat('1', 64)
    );
    v_ok := v_ok AND insert_intent_state_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '83500000-0000-0000-0000-000000000004',
        '83000000-0000-0000-0000-000000000001',
        'broker_confirmed', 'TEST_BROKER_CONFIRMED', '{}', clock_timestamp()
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'runtime accepted broker wrapper returned false';
    END IF;

    v_cancel_requested_at := clock_timestamp();
    IF NOT persist_cancel_intent_v2(
        'paper', 'lease-test', '60000000-0000-0000-0000-000000000002', 2,
        '84600000-0000-0000-0000-000000000001',
        '84700000-0000-0000-0000-000000000001',
        '84800000-0000-0000-0000-000000000001',
        '84800000-0000-0000-0000-000000000002',
        'test-83000000000000000000000000000001', 'broker-wrapper-test',
        'TEST_RUNTIME_CANCEL_UNKNOWN', v_cancel_requested_at,
        jsonb_build_object(
            'cancel_intent_id', '84600000-0000-0000-0000-000000000001'::uuid,
            'client_order_id', 'test-83000000000000000000000000000001',
            'provider_order_id', 'broker-wrapper-test',
            'reason_code', 'TEST_RUNTIME_CANCEL_UNKNOWN',
            'requested_at', v_cancel_requested_at
        )
    ) THEN
        RAISE EXCEPTION 'runtime could not durably persist cancellation before DELETE';
    END IF;
    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_v2(
        'paper', 'lease-test', '84700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'runtime could not claim cancellation dispatch once';
    END IF;
    v_cancel_unknown_at := clock_timestamp();
    IF NOT append_cancel_unknown_v2(
        'paper', 'lease-test', '84700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2,
        '84800000-0000-0000-0000-000000000003',
        'http timeout after cancel request write', v_cancel_unknown_at
    ) OR NOT append_cancel_unknown_v2(
        'paper', 'lease-test', '84700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2,
        '84800000-0000-0000-0000-000000000003',
        'http timeout after cancel request write', v_cancel_unknown_at
    ) THEN
        RAISE EXCEPTION 'runtime cancel unknown was not durable and idempotent';
    END IF;
    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_recovery_v2(
        'paper', 'lease-test', '84700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2
    );
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'cancel unknown was not restart-recoverable without redispatch';
    END IF;
    SELECT COUNT(*) INTO v_rows
    FROM list_unresolved_cancel_outboxes_v2(
        'paper', 'lease-test',
        '60000000-0000-0000-0000-000000000002', 2, 100
    ) AS unresolved
    WHERE unresolved.cancel_outbox_id = '84700000-0000-0000-0000-000000000001'
      AND unresolved.current_state = 'cancel_unknown'
      AND unresolved.detail = 'http timeout after cancel request write';
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'cancel unknown was not restart-discoverable with exact evidence';
    END IF;

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
        '83500000-0000-0000-0000-000000000005',
        '83000000-0000-0000-0000-000000000001',
        'terminal', 'TEST_TERMINAL', '{}', clock_timestamp()
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'runtime broker/fill wrapper returned false';
    END IF;
    SELECT COUNT(*) INTO v_rows FROM claim_cancel_outbox_completion_v2(
        'paper', 'lease-test', '84700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2
    );
    IF v_rows <> 1 OR NOT finalize_cancel_outbox_v2(
        'paper', 'lease-test', '84700000-0000-0000-0000-000000000001',
        '60000000-0000-0000-0000-000000000002', 2,
        '84800000-0000-0000-0000-000000000004',
        '84500000-0000-0000-0000-000000000001',
        'BROKER_TERMINAL_FILLED'
    ) THEN
        RAISE EXCEPTION 'runtime could not finalize unknown cancel from terminal broker truth';
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
