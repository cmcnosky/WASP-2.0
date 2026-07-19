BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'alpaca_trader_runtime') THEN
        CREATE ROLE alpaca_trader_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'alpaca_trader_operator') THEN
        CREATE ROLE alpaca_trader_operator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END;
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM alpaca_trader_runtime, alpaca_trader_operator;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM alpaca_trader_runtime, alpaca_trader_operator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM alpaca_trader_runtime, alpaca_trader_operator;

GRANT USAGE ON SCHEMA public TO alpaca_trader_runtime, alpaca_trader_operator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO alpaca_trader_runtime, alpaca_trader_operator;

GRANT INSERT ON
    decision_snapshots,
    target_portfolios,
    target_positions,
    risk_decisions,
    order_plans,
    order_intents,
    order_outbox,
    broker_orders,
    fills,
    account_snapshots,
    reconciliation_diffs,
    data_artifacts,
    incident_events
TO alpaca_trader_runtime;

GRANT INSERT (
    intent_state_event_id,
    intent_id,
    state,
    reason_code,
    detail,
    fencing_token,
    occurred_at
) ON intent_state_events TO alpaca_trader_runtime;

GRANT INSERT (
    broker_event_id,
    broker_order_id,
    client_order_id,
    provider_status,
    recognized_status,
    cumulative_filled_quantity,
    average_fill_price,
    provider_occurred_at,
    received_at,
    x_request_id,
    raw_payload,
    raw_hash
) ON broker_order_events TO alpaca_trader_runtime;

-- Do not grant INSERT on the generated authority sequence. PostgreSQL permits
-- an explicit identity value with OVERRIDING SYSTEM VALUE when a role has
-- table-wide INSERT, which would let runtime fabricate recency.
GRANT INSERT (
    reconciliation_id,
    environment,
    account_fingerprint,
    trigger,
    kill_event_id,
    fencing_token,
    started_at,
    completed_at,
    outcome,
    resumable,
    account_snapshot_id,
    evidence_hash
) ON reconciliation_runs TO alpaca_trader_runtime;

GRANT UPDATE (
    completed_at,
    outcome,
    resumable,
    account_snapshot_id,
    evidence_hash
) ON reconciliation_runs TO alpaca_trader_runtime;

GRANT INSERT ON
    strategy_releases,
    activation_permits,
    activation_permit_revocations,
    experiments
TO alpaca_trader_operator;

GRANT INSERT (
    experiment_event_id,
    experiment_id,
    status,
    result_hash,
    detail,
    occurred_at
) ON experiment_events TO alpaca_trader_operator;

-- Keep the server-generated kill ordering outside operator-supplied columns.
GRANT INSERT (
    kill_event_id,
    environment,
    account_fingerprint,
    severity,
    reason_code,
    detail,
    actor,
    operator_approved,
    approval_digest,
    occurred_at
) ON kill_events TO alpaca_trader_operator;

ALTER FUNCTION acquire_executor_lease(TEXT, TEXT, UUID, INTERVAL)
    SECURITY DEFINER;
ALTER FUNCTION acquire_executor_lease(TEXT, TEXT, UUID, INTERVAL)
    SET search_path = pg_catalog, public;
ALTER FUNCTION renew_executor_lease(TEXT, TEXT, UUID, BIGINT, INTERVAL)
    SECURITY DEFINER;
ALTER FUNCTION renew_executor_lease(TEXT, TEXT, UUID, BIGINT, INTERVAL)
    SET search_path = pg_catalog, public;

CREATE OR REPLACE FUNCTION claim_order_outbox(
    p_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS SETOF order_outbox
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    WITH claimed AS (
        UPDATE public.order_outbox AS o
        SET claimed_by = p_owner_id,
            claimed_at = clock_timestamp(),
            claim_fencing_token = p_fencing_token,
            attempt_count = o.attempt_count + 1,
            last_error = NULL
        FROM public.execution_readiness AS authority,
             public.order_intents AS intent,
             public.risk_decisions AS risk,
             public.current_intent_states AS intent_state
        WHERE o.outbox_id = p_outbox_id
          AND o.completed_at IS NULL
          AND o.available_at <= clock_timestamp()
          AND authority.environment = o.environment
          AND authority.account_fingerprint = o.account_fingerprint
          AND authority.ready
          AND authority.lease_owner_id = p_owner_id
          AND authority.fencing_token = p_fencing_token
          AND intent.intent_id = o.intent_id
          AND intent.environment = authority.environment
          AND intent.account_fingerprint = authority.account_fingerprint
          AND intent.strategy_release_id = authority.strategy_release_id
          AND risk.risk_decision_id = intent.risk_decision_id
          AND risk.activation_permit_id = authority.permit_id
          AND risk.outcome IN ('approved', 'reduced')
          AND intent_state.intent_id = intent.intent_id
          AND intent_state.state = 'eligible'
          AND clock_timestamp() >= intent.quote_received_at
          AND clock_timestamp() < intent.quote_valid_until
          AND NOT EXISTS (
              SELECT 1
              FROM public.intent_state_events AS prior_dispatch
              WHERE prior_dispatch.intent_id = intent.intent_id
                AND prior_dispatch.state = 'dispatch_started'
          )
        RETURNING o.*
    ), recorded AS (
        INSERT INTO public.intent_state_events (
            intent_state_event_id,
            intent_id,
            state,
            reason_code,
            detail,
            fencing_token,
            occurred_at
        )
        SELECT
            claimed.outbox_id,
            claimed.intent_id,
            'dispatch_started',
            'OUTBOX_CLAIMED_FOR_FIRST_DISPATCH',
            '{}'::jsonb,
            p_fencing_token,
            clock_timestamp()
        FROM claimed
        RETURNING intent_id
    )
    SELECT claimed.*
    FROM claimed
    JOIN recorded ON recorded.intent_id = claimed.intent_id;
END;
$$;

CREATE OR REPLACE FUNCTION finalize_order_outbox(
    p_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_completion_reason TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    IF p_completion_reason IS NULL
        OR length(trim(p_completion_reason)) = 0
        OR length(p_completion_reason) > 128
    THEN
        RETURN FALSE;
    END IF;

    UPDATE public.order_outbox AS o
    SET completed_at = clock_timestamp(),
        completion_reason = p_completion_reason
    FROM public.executor_leases AS lease,
         public.order_intents AS intent,
         public.current_intent_states AS intent_state,
         public.current_broker_order_states AS broker_state,
         public.broker_orders AS broker_order
    WHERE o.outbox_id = p_outbox_id
      AND o.completed_at IS NULL
      AND o.claimed_by = p_owner_id
      AND o.claim_fencing_token = p_fencing_token
      AND lease.environment = o.environment
      AND lease.account_fingerprint = o.account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
      AND intent.intent_id = o.intent_id
      AND intent.environment = lease.environment
      AND intent.account_fingerprint = lease.account_fingerprint
      AND intent_state.intent_id = intent.intent_id
      AND intent_state.state IN ('broker_confirmed', 'terminal')
      AND broker_order.intent_id = intent.intent_id
      AND broker_state.broker_order_id = broker_order.broker_order_id
      AND broker_state.recognized_status;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END;
$$;

-- Lookup-only recovery claim. Holding this claim never makes the first-dispatch
-- function eligible again; the caller may only reconcile by client_order_id.
CREATE OR REPLACE FUNCTION claim_order_outbox_recovery(
    p_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS SETOF order_outbox
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    UPDATE public.order_outbox AS o
    SET claimed_by = p_owner_id,
        claimed_at = clock_timestamp(),
        claim_fencing_token = p_fencing_token,
        attempt_count = o.attempt_count + 1,
        last_error = NULL
    FROM public.executor_leases AS lease,
         public.order_intents AS intent,
         public.current_intent_states AS intent_state
    WHERE o.outbox_id = p_outbox_id
      AND o.completed_at IS NULL
      AND lease.environment = o.environment
      AND lease.account_fingerprint = o.account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
      AND intent.intent_id = o.intent_id
      AND intent.environment = lease.environment
      AND intent.account_fingerprint = lease.account_fingerprint
      AND intent_state.intent_id = intent.intent_id
      AND intent_state.state IN (
          'dispatch_started', 'submission_unknown', 'acknowledged',
          'broker_confirmed', 'blocked'
      )
      AND EXISTS (
          SELECT 1
          FROM public.intent_state_events AS first_dispatch
          WHERE first_dispatch.intent_id = intent.intent_id
            AND first_dispatch.state = 'dispatch_started'
      )
      AND p_fencing_token >= o.created_fencing_token
      AND (
          o.claim_fencing_token IS NULL
          OR p_fencing_token > o.claim_fencing_token
          OR (
              p_fencing_token = o.claim_fencing_token
              AND o.claimed_by = p_owner_id
          )
      )
    RETURNING o.*;
END;
$$;

CREATE OR REPLACE FUNCTION record_runtime_kill_event(
    p_kill_event_id UUID,
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_severity TEXT,
    p_reason_code TEXT,
    p_detail JSONB,
    p_actor TEXT,
    p_occurred_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_severity NOT IN ('soft', 'hard', 'liquidation') THEN
        RETURN FALSE;
    END IF;

    INSERT INTO public.kill_events (
        kill_event_id,
        environment,
        account_fingerprint,
        severity,
        reason_code,
        detail,
        actor,
        operator_approved,
        approval_digest,
        occurred_at
    ) VALUES (
        p_kill_event_id,
        p_environment,
        p_account_fingerprint,
        p_severity,
        p_reason_code,
        COALESCE(p_detail, '{}'::jsonb),
        p_actor,
        FALSE,
        NULL,
        p_occurred_at
    );
    RETURN TRUE;
END;
$$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
    acquire_executor_lease(TEXT, TEXT, UUID, INTERVAL),
    renew_executor_lease(TEXT, TEXT, UUID, BIGINT, INTERVAL),
    claim_order_outbox(UUID, UUID, BIGINT),
    claim_order_outbox_recovery(UUID, UUID, BIGINT),
    finalize_order_outbox(UUID, UUID, BIGINT, TEXT),
    record_runtime_kill_event(UUID, TEXT, TEXT, TEXT, TEXT, JSONB, TEXT, TIMESTAMPTZ)
TO alpaca_trader_runtime;

GRANT USAGE ON SEQUENCE
    reconciliation_runs_authority_sequence_seq
TO alpaca_trader_runtime;
GRANT USAGE ON SEQUENCE
    kill_events_authority_sequence_seq
TO alpaca_trader_operator;

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM PUBLIC;

COMMIT;
