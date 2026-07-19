BEGIN;

-- Cancellation is a separate durable command stream. A broker DELETE is never
-- authorized until this intent, its initial states, and its outbox row commit
-- atomically under the current executor fence.
CREATE TABLE cancel_intents (
    cancel_intent_id UUID PRIMARY KEY,
    intent_id UUID NOT NULL REFERENCES order_intents(intent_id),
    client_order_id TEXT NOT NULL CHECK (octet_length(client_order_id) BETWEEN 1 AND 128),
    provider_order_id TEXT NOT NULL CHECK (octet_length(provider_order_id) BETWEEN 1 AND 128),
    environment TEXT NOT NULL CHECK (environment IN ('paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    reason_code TEXT NOT NULL CHECK (octet_length(trim(reason_code)) BETWEEN 1 AND 128),
    requested_at TIMESTAMPTZ NOT NULL,
    created_fencing_token BIGINT NOT NULL CHECK (created_fencing_token > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (environment, account_fingerprint, provider_order_id),
    UNIQUE (cancel_intent_id, intent_id)
);

CREATE TABLE cancel_state_events (
    cancel_state_event_id UUID PRIMARY KEY,
    cancel_intent_id UUID NOT NULL REFERENCES cancel_intents(cancel_intent_id),
    event_sequence BIGINT GENERATED ALWAYS AS IDENTITY,
    state TEXT NOT NULL CHECK (state IN (
        'persisted', 'eligible', 'dispatch_started',
        'request_accepted', 'cancel_unknown', 'not_dispatched', 'terminal'
    )),
    reason_code TEXT NOT NULL CHECK (octet_length(trim(reason_code)) BETWEEN 1 AND 128),
    request_id TEXT,
    payload_hash TEXT CHECK (payload_hash IS NULL OR payload_hash ~ '^[0-9a-f]{64}$'),
    not_dispatched_provider_order_id TEXT,
    dispatch_attempt_count INTEGER,
    evidence_hash TEXT,
    broker_event_id UUID REFERENCES broker_order_events(broker_event_id),
    detail TEXT NOT NULL DEFAULT '' CHECK (octet_length(detail) <= 512),
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cancel_intent_id, event_sequence),
    CHECK (
        (state = 'request_accepted'
         AND request_id IS NOT NULL
         AND octet_length(trim(request_id)) BETWEEN 1 AND 128
         AND payload_hash IS NOT NULL
         AND not_dispatched_provider_order_id IS NULL
         AND dispatch_attempt_count IS NULL
         AND evidence_hash IS NULL
         AND broker_event_id IS NULL
         AND detail = '')
        OR (state = 'cancel_unknown'
            AND request_id IS NULL
            AND payload_hash IS NULL
            AND not_dispatched_provider_order_id IS NULL
            AND dispatch_attempt_count IS NULL
            AND evidence_hash IS NULL
            AND broker_event_id IS NULL
            AND octet_length(detail) BETWEEN 1 AND 512)
        OR (state = 'not_dispatched'
            AND reason_code = 'TRANSPORT_BEFORE_SEND'
            AND request_id IS NULL
            AND payload_hash IS NULL
            AND not_dispatched_provider_order_id IS NOT NULL
            AND octet_length(trim(not_dispatched_provider_order_id)) BETWEEN 1 AND 128
            AND dispatch_attempt_count IS NOT NULL
            AND dispatch_attempt_count > 0
            AND evidence_hash IS NOT NULL
            AND evidence_hash ~ '^[0-9a-f]{64}$'
            AND broker_event_id IS NULL
            AND octet_length(detail) BETWEEN 1 AND 512
            AND length(trim(detail)) > 0)
        OR (state = 'dispatch_started'
            AND request_id IS NULL
            AND payload_hash IS NULL
            AND not_dispatched_provider_order_id IS NULL
            AND dispatch_attempt_count IS NOT NULL
            AND dispatch_attempt_count > 0
            AND evidence_hash IS NULL
            AND broker_event_id IS NULL
            AND detail = '')
        OR (state = 'terminal'
            AND request_id IS NULL
            AND payload_hash IS NULL
            AND not_dispatched_provider_order_id IS NULL
            AND dispatch_attempt_count IS NULL
            AND evidence_hash IS NULL
            AND broker_event_id IS NOT NULL
            AND detail = '')
        OR (state IN ('persisted', 'eligible')
            AND request_id IS NULL
            AND payload_hash IS NULL
            AND not_dispatched_provider_order_id IS NULL
            AND dispatch_attempt_count IS NULL
            AND evidence_hash IS NULL
            AND broker_event_id IS NULL
            AND detail = '')
    )
);

CREATE INDEX cancel_state_events_replay_idx
    ON cancel_state_events (cancel_intent_id, event_sequence);

CREATE VIEW current_cancel_states AS
SELECT DISTINCT ON (cancel_intent_id)
    cancel_intent_id,
    state,
    reason_code,
    request_id,
    payload_hash,
    not_dispatched_provider_order_id,
    dispatch_attempt_count,
    evidence_hash,
    broker_event_id,
    detail,
    fencing_token,
    occurred_at,
    cancel_state_event_id,
    event_sequence
FROM cancel_state_events
ORDER BY cancel_intent_id, event_sequence DESC;

CREATE TABLE cancel_outbox (
    cancel_outbox_id UUID PRIMARY KEY,
    cancel_intent_id UUID NOT NULL UNIQUE REFERENCES cancel_intents(cancel_intent_id),
    environment TEXT NOT NULL CHECK (environment IN ('paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    created_fencing_token BIGINT NOT NULL CHECK (created_fencing_token > 0),
    payload JSONB NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    claimed_by UUID,
    claimed_at TIMESTAMPTZ,
    claim_fencing_token BIGINT CHECK (claim_fencing_token > 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    completed_at TIMESTAMPTZ,
    completion_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK ((claimed_by IS NULL AND claimed_at IS NULL)
        OR (claimed_by IS NOT NULL AND claimed_at IS NOT NULL)),
    CHECK ((completed_at IS NULL AND completion_reason IS NULL)
        OR (completed_at IS NOT NULL AND completion_reason IS NOT NULL)),
    CHECK (completion_reason IS NULL
        OR octet_length(trim(completion_reason)) BETWEEN 1 AND 128)
);

CREATE INDEX cancel_outbox_unresolved_idx
    ON cancel_outbox (available_at, cancel_outbox_id)
    WHERE completed_at IS NULL;

CREATE VIEW execution_readiness_v2 AS
SELECT
    readiness.environment,
    readiness.account_fingerprint,
    readiness.permit_id,
    readiness.strategy_release_id,
    readiness.kill_severity,
    readiness.kill_event_id,
    readiness.kill_authority_sequence,
    readiness.reconciliation_id,
    readiness.reconciled_at,
    readiness.reconciliation_outcome,
    readiness.resumable,
    readiness.account_snapshot_id,
    readiness.account_status,
    readiness.recognized_account_status,
    readiness.lease_owner_id,
    readiness.fencing_token,
    readiness.lease_until,
    readiness.ready
    AND NOT EXISTS (
        SELECT 1
        FROM public.cancel_outbox AS cancellation
        WHERE cancellation.environment = readiness.environment
          AND cancellation.account_fingerprint = readiness.account_fingerprint
          AND cancellation.completed_at IS NULL
    ) AS ready
FROM public.execution_readiness AS readiness;

CREATE OR REPLACE FUNCTION enforce_cancel_intent_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.broker_orders AS broker_order
        JOIN public.order_intents AS intent ON intent.intent_id = broker_order.intent_id
        WHERE broker_order.intent_id = NEW.intent_id
          AND broker_order.client_order_id = NEW.client_order_id
          AND broker_order.broker_order_id = NEW.provider_order_id
          AND broker_order.environment = NEW.environment
          AND broker_order.account_fingerprint = NEW.account_fingerprint
          AND intent.environment = NEW.environment
          AND intent.account_fingerprint = NEW.account_fingerprint
    ) THEN
        RAISE EXCEPTION 'cancel intent does not match its durable broker order';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_cancel_state_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_environment TEXT;
    v_account_fingerprint TEXT;
    v_intent_id UUID;
    v_provider_order_id TEXT;
    v_previous_state TEXT;
    v_previous_dispatch_attempt_count INTEGER;
    v_previous_occurred_at TIMESTAMPTZ;
BEGIN
    SELECT
        cancellation.environment,
        cancellation.account_fingerprint,
        cancellation.intent_id,
        cancellation.provider_order_id
    INTO
        v_environment,
        v_account_fingerprint,
        v_intent_id,
        v_provider_order_id
    FROM public.cancel_intents AS cancellation
    WHERE cancellation.cancel_intent_id = NEW.cancel_intent_id
    FOR UPDATE;

    IF NOT FOUND OR NOT EXISTS (
        SELECT 1
        FROM public.executor_leases AS lease
        WHERE lease.environment = v_environment
          AND lease.account_fingerprint = v_account_fingerprint
          AND lease.fencing_token = NEW.fencing_token
          AND lease.lease_until >= clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'cancel state lacks current fenced authority';
    END IF;
    IF NEW.occurred_at > clock_timestamp() + INTERVAL '5 seconds' THEN
        RAISE EXCEPTION 'cancel state timestamp is unreasonably future-dated';
    END IF;

    SELECT state.state, state.dispatch_attempt_count, state.occurred_at
    INTO v_previous_state, v_previous_dispatch_attempt_count, v_previous_occurred_at
    FROM public.cancel_state_events AS state
    WHERE state.cancel_intent_id = NEW.cancel_intent_id
    ORDER BY state.event_sequence DESC
    LIMIT 1;

    IF v_previous_occurred_at IS NOT NULL AND NEW.occurred_at < v_previous_occurred_at THEN
        RAISE EXCEPTION 'cancel state timestamps must be monotonic';
    END IF;
    IF NEW.state = 'dispatch_started'
       AND (
           (v_previous_state = 'eligible' AND NEW.dispatch_attempt_count <> 1)
           OR (
               v_previous_state = 'not_dispatched'
               AND NEW.dispatch_attempt_count <> v_previous_dispatch_attempt_count + 1
           )
       )
    THEN
        RAISE EXCEPTION 'cancel dispatch marker has the wrong attempt count';
    END IF;
    IF NEW.state = 'not_dispatched'
       AND (
           NEW.dispatch_attempt_count IS DISTINCT FROM v_previous_dispatch_attempt_count
           OR NEW.not_dispatched_provider_order_id IS DISTINCT FROM v_provider_order_id
       )
    THEN
        RAISE EXCEPTION 'cancel non-dispatch evidence does not match its dispatch attempt';
    END IF;
    IF v_previous_state IS NULL AND NEW.state <> 'persisted' THEN
        RAISE EXCEPTION 'first cancel state must be persisted';
    ELSIF v_previous_state IS NOT NULL AND NOT (
        (v_previous_state = 'persisted' AND NEW.state = 'eligible')
        OR (v_previous_state = 'eligible' AND NEW.state IN ('dispatch_started', 'terminal'))
        OR (v_previous_state = 'dispatch_started'
            AND NEW.state IN (
                'request_accepted', 'cancel_unknown', 'not_dispatched', 'terminal'
            ))
        OR (v_previous_state = 'not_dispatched'
            AND NEW.state IN ('dispatch_started', 'terminal'))
        OR (v_previous_state IN ('request_accepted', 'cancel_unknown')
            AND NEW.state = 'terminal')
    ) THEN
        RAISE EXCEPTION 'invalid cancel state transition from % to %',
            v_previous_state, NEW.state;
    END IF;

    IF NEW.state = 'terminal' AND NOT EXISTS (
        SELECT 1
        FROM public.current_intent_states AS intent_state
        JOIN public.current_broker_order_states AS broker_state
          ON broker_state.broker_order_id = v_provider_order_id
        WHERE intent_state.intent_id = v_intent_id
          AND intent_state.state = 'terminal'
          AND broker_state.broker_event_id = NEW.broker_event_id
          AND broker_state.recognized_status
          AND NEW.reason_code = 'BROKER_TERMINAL_' || upper(broker_state.provider_status)
          AND broker_state.provider_status IN (
              'filled', 'canceled', 'expired', 'replaced', 'rejected'
          )
    ) THEN
        RAISE EXCEPTION 'terminal cancel state lacks terminal broker truth';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_cancel_outbox_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.cancel_intents AS cancellation
        JOIN public.executor_leases AS lease
          ON lease.environment = cancellation.environment
         AND lease.account_fingerprint = cancellation.account_fingerprint
        WHERE cancellation.cancel_intent_id = NEW.cancel_intent_id
          AND cancellation.environment = NEW.environment
          AND cancellation.account_fingerprint = NEW.account_fingerprint
          AND cancellation.created_fencing_token = NEW.created_fencing_token
          AND lease.fencing_token = NEW.created_fencing_token
          AND lease.lease_until >= clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'cancel outbox does not match intent and current fence';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION protect_cancel_outbox()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'cancel outbox rows cannot be deleted';
    END IF;
    IF NEW.cancel_outbox_id <> OLD.cancel_outbox_id
       OR NEW.cancel_intent_id <> OLD.cancel_intent_id
       OR NEW.environment <> OLD.environment
       OR NEW.account_fingerprint <> OLD.account_fingerprint
       OR NEW.created_fencing_token <> OLD.created_fencing_token
       OR NEW.payload <> OLD.payload
       OR NEW.available_at <> OLD.available_at
       OR NEW.created_at <> OLD.created_at
    THEN
        RAISE EXCEPTION 'immutable cancel outbox authority was changed';
    END IF;
    IF OLD.completed_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'completed cancel outbox cannot be changed';
    END IF;
    IF OLD.completed_at IS NULL AND NEW.completed_at IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM public.current_cancel_states AS cancel_state
        WHERE cancel_state.cancel_intent_id = OLD.cancel_intent_id
          AND cancel_state.state = 'terminal'
          AND cancel_state.reason_code = NEW.completion_reason
          AND cancel_state.occurred_at <= NEW.completed_at
    ) THEN
        RAISE EXCEPTION 'cancel outbox completion lacks terminal cancel evidence';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER cancel_intents_enforce_chain
BEFORE INSERT ON cancel_intents
FOR EACH ROW EXECUTE FUNCTION enforce_cancel_intent_chain();

CREATE TRIGGER cancel_intents_reject_mutation
BEFORE UPDATE OR DELETE ON cancel_intents
FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation();

CREATE TRIGGER cancel_state_events_enforce_transition
BEFORE INSERT ON cancel_state_events
FOR EACH ROW EXECUTE FUNCTION enforce_cancel_state_transition();

CREATE TRIGGER cancel_state_events_reject_mutation
BEFORE UPDATE OR DELETE ON cancel_state_events
FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation();

CREATE TRIGGER cancel_outbox_enforce_chain
BEFORE INSERT ON cancel_outbox
FOR EACH ROW EXECUTE FUNCTION enforce_cancel_outbox_chain();

CREATE TRIGGER cancel_outbox_protect_authority
BEFORE UPDATE OR DELETE ON cancel_outbox
FOR EACH ROW EXECUTE FUNCTION protect_cancel_outbox();

-- The original v2 order claim remains immutable and attested.  This successor
-- adds unresolved cancellation work to the first-dispatch readiness gate.
CREATE OR REPLACE FUNCTION claim_order_outbox_v3(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS SETOF order_outbox
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH claimed AS (
        UPDATE public.order_outbox AS outbox
        SET claimed_by = p_owner_id,
            claimed_at = clock_timestamp(),
            claim_fencing_token = p_fencing_token,
            attempt_count = outbox.attempt_count + 1,
            last_error = NULL
        FROM public.execution_readiness_v2 AS authority,
             public.order_intents AS intent,
             public.risk_decisions AS risk,
             public.current_intent_states AS intent_state
        WHERE outbox.outbox_id = p_outbox_id
          AND outbox.environment = p_environment
          AND outbox.account_fingerprint = p_account_fingerprint
          AND outbox.completed_at IS NULL
          AND outbox.available_at <= clock_timestamp()
          AND authority.environment = p_environment
          AND authority.account_fingerprint = p_account_fingerprint
          AND authority.ready
          AND authority.lease_owner_id = p_owner_id
          AND authority.fencing_token = p_fencing_token
          AND intent.intent_id = outbox.intent_id
          AND intent.environment = p_environment
          AND intent.account_fingerprint = p_account_fingerprint
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
        RETURNING outbox.*
    ), recorded AS (
        INSERT INTO public.intent_state_events (
            intent_state_event_id, intent_id, state, reason_code,
            detail, fencing_token, occurred_at
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

CREATE OR REPLACE FUNCTION persist_cancel_intent_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_cancel_intent_id UUID,
    p_cancel_outbox_id UUID,
    p_persisted_event_id UUID,
    p_eligible_event_id UUID,
    p_client_order_id TEXT,
    p_provider_order_id TEXT,
    p_reason_code TEXT,
    p_requested_at TIMESTAMPTZ,
    p_payload JSONB
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_intent_id UUID;
BEGIN
    IF p_cancel_intent_id IS NULL
       OR p_cancel_outbox_id IS NULL
       OR p_persisted_event_id IS NULL
       OR p_eligible_event_id IS NULL
       OR p_client_order_id IS NULL
       OR octet_length(trim(p_client_order_id)) NOT BETWEEN 1 AND 128
       OR p_provider_order_id IS NULL
       OR octet_length(trim(p_provider_order_id)) NOT BETWEEN 1 AND 128
       OR p_reason_code IS NULL
       OR octet_length(trim(p_reason_code)) NOT BETWEEN 1 AND 128
       OR p_requested_at IS NULL
       OR p_requested_at > clock_timestamp() + INTERVAL '5 seconds'
       OR p_payload IS NULL
       OR jsonb_typeof(p_payload) <> 'object'
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    IF NOT (p_payload ?& ARRAY[
        'cancel_intent_id', 'client_order_id', 'provider_order_id',
        'reason_code', 'requested_at'
    ])
       OR (SELECT COUNT(*) FROM jsonb_object_keys(p_payload)) <> 5
       OR p_payload ->> 'cancel_intent_id' <> p_cancel_intent_id::text
       OR p_payload ->> 'client_order_id' <> p_client_order_id
       OR p_payload ->> 'provider_order_id' <> p_provider_order_id
       OR p_payload ->> 'reason_code' <> p_reason_code
    THEN
        RETURN FALSE;
    END IF;
    BEGIN
        IF (p_payload ->> 'requested_at')::timestamptz IS DISTINCT FROM p_requested_at THEN
            RETURN FALSE;
        END IF;
    EXCEPTION
        WHEN invalid_datetime_format THEN RETURN FALSE;
    END;

    -- An exact deterministic retry succeeds even if broker truth advanced
    -- after the original transaction committed.  Any identity collision fails
    -- before this function can create a second cancellation command.
    IF EXISTS (
        SELECT 1 FROM public.cancel_intents AS cancellation
        WHERE cancellation.cancel_intent_id = p_cancel_intent_id
           OR (
               cancellation.environment = p_environment
               AND cancellation.account_fingerprint = p_account_fingerprint
               AND cancellation.provider_order_id = p_provider_order_id
           )
    ) OR EXISTS (
        SELECT 1 FROM public.cancel_outbox AS outbox
        WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
    ) OR EXISTS (
        SELECT 1 FROM public.cancel_state_events AS event
        WHERE event.cancel_state_event_id IN (p_persisted_event_id, p_eligible_event_id)
    ) THEN
        RETURN EXISTS (
            SELECT 1
            FROM public.cancel_intents AS cancellation
            JOIN public.cancel_outbox AS outbox
              ON outbox.cancel_intent_id = cancellation.cancel_intent_id
            JOIN public.cancel_state_events AS persisted
              ON persisted.cancel_intent_id = cancellation.cancel_intent_id
             AND persisted.cancel_state_event_id = p_persisted_event_id
            JOIN public.cancel_state_events AS eligible
              ON eligible.cancel_intent_id = cancellation.cancel_intent_id
             AND eligible.cancel_state_event_id = p_eligible_event_id
            WHERE cancellation.cancel_intent_id = p_cancel_intent_id
              AND cancellation.client_order_id = p_client_order_id
              AND cancellation.provider_order_id = p_provider_order_id
              AND cancellation.environment = p_environment
              AND cancellation.account_fingerprint = p_account_fingerprint
              AND cancellation.reason_code = p_reason_code
              AND cancellation.requested_at = p_requested_at
              AND cancellation.created_fencing_token = p_fencing_token
              AND outbox.cancel_outbox_id = p_cancel_outbox_id
              AND outbox.environment = p_environment
              AND outbox.account_fingerprint = p_account_fingerprint
              AND outbox.created_fencing_token = p_fencing_token
              AND outbox.payload = p_payload
              AND outbox.available_at = p_requested_at
              AND persisted.state = 'persisted'
              AND persisted.reason_code = 'CANCEL_INTENT_PERSISTED'
              AND persisted.fencing_token = p_fencing_token
              AND persisted.occurred_at = p_requested_at
              AND eligible.state = 'eligible'
              AND eligible.reason_code = 'CANCEL_INTENT_ELIGIBLE'
              AND eligible.fencing_token = p_fencing_token
              AND eligible.occurred_at = p_requested_at
        );
    END IF;

    SELECT broker_order.intent_id
    INTO v_intent_id
    FROM public.broker_orders AS broker_order
    JOIN public.order_intents AS intent ON intent.intent_id = broker_order.intent_id
    JOIN public.current_intent_states AS intent_state
      ON intent_state.intent_id = intent.intent_id
    JOIN public.current_broker_order_states AS broker_state
      ON broker_state.broker_order_id = broker_order.broker_order_id
    WHERE broker_order.broker_order_id = p_provider_order_id
      AND broker_order.client_order_id = p_client_order_id
      AND broker_order.environment = p_environment
      AND broker_order.account_fingerprint = p_account_fingerprint
      AND intent.environment = p_environment
      AND intent.account_fingerprint = p_account_fingerprint
      AND intent_state.state <> 'terminal'
      AND broker_state.recognized_status
      AND broker_state.provider_status NOT IN (
          'filled', 'canceled', 'expired', 'replaced', 'rejected'
      )
      AND p_requested_at >= broker_order.first_seen_at
    FOR UPDATE OF broker_order, intent;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    INSERT INTO public.cancel_intents (
        cancel_intent_id, intent_id, client_order_id, provider_order_id,
        environment, account_fingerprint, reason_code, requested_at,
        created_fencing_token
    ) VALUES (
        p_cancel_intent_id, v_intent_id, p_client_order_id, p_provider_order_id,
        p_environment, p_account_fingerprint, p_reason_code, p_requested_at,
        p_fencing_token
    );

    INSERT INTO public.cancel_state_events (
        cancel_state_event_id, cancel_intent_id, state, reason_code,
        fencing_token, occurred_at
    ) VALUES (
        p_persisted_event_id, p_cancel_intent_id, 'persisted',
        'CANCEL_INTENT_PERSISTED', p_fencing_token, p_requested_at
    );

    INSERT INTO public.cancel_state_events (
        cancel_state_event_id, cancel_intent_id, state, reason_code,
        fencing_token, occurred_at
    ) VALUES (
        p_eligible_event_id, p_cancel_intent_id, 'eligible',
        'CANCEL_INTENT_ELIGIBLE', p_fencing_token, p_requested_at
    );

    INSERT INTO public.cancel_outbox (
        cancel_outbox_id, cancel_intent_id, environment, account_fingerprint,
        created_fencing_token, payload, available_at
    ) VALUES (
        p_cancel_outbox_id, p_cancel_intent_id, p_environment,
        p_account_fingerprint, p_fencing_token, p_payload, p_requested_at
    );

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION claim_cancel_outbox_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS TABLE (
    cancel_outbox_id UUID,
    cancel_intent_id UUID,
    client_order_id TEXT,
    provider_order_id TEXT,
    reason_code TEXT,
    requested_at TIMESTAMPTZ,
    environment TEXT,
    account_fingerprint TEXT,
    created_fencing_token BIGINT,
    claim_fencing_token BIGINT,
    payload JSONB,
    available_at TIMESTAMPTZ,
    claimed_by UUID,
    claimed_at TIMESTAMPTZ,
    attempt_count INTEGER,
    current_state TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH claimed AS (
        UPDATE public.cancel_outbox AS outbox
        SET claimed_by = p_owner_id,
            claimed_at = clock_timestamp(),
            claim_fencing_token = p_fencing_token,
            attempt_count = outbox.attempt_count + 1
        FROM public.cancel_intents AS cancellation,
             public.current_cancel_states AS cancel_state,
             public.current_intent_states AS intent_state,
             public.current_broker_order_states AS broker_state
        WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
          AND outbox.environment = p_environment
          AND outbox.account_fingerprint = p_account_fingerprint
          AND outbox.completed_at IS NULL
          AND outbox.available_at <= clock_timestamp()
          AND cancellation.cancel_intent_id = outbox.cancel_intent_id
          AND cancellation.environment = p_environment
          AND cancellation.account_fingerprint = p_account_fingerprint
          AND cancel_state.cancel_intent_id = cancellation.cancel_intent_id
          AND cancel_state.state = 'eligible'
          AND intent_state.intent_id = cancellation.intent_id
          AND intent_state.state <> 'terminal'
          AND broker_state.broker_order_id = cancellation.provider_order_id
          AND broker_state.recognized_status
          AND broker_state.provider_status NOT IN (
              'filled', 'canceled', 'expired', 'replaced', 'rejected'
          )
          AND p_fencing_token >= outbox.created_fencing_token
          AND (
              outbox.claim_fencing_token IS NULL
              OR p_fencing_token > outbox.claim_fencing_token
              OR (
                  p_fencing_token = outbox.claim_fencing_token
                  AND outbox.claimed_by = p_owner_id
              )
          )
        RETURNING
            outbox.cancel_outbox_id,
            outbox.cancel_intent_id,
            cancellation.client_order_id,
            cancellation.provider_order_id,
            cancellation.reason_code,
            cancellation.requested_at,
            outbox.environment,
            outbox.account_fingerprint,
            outbox.created_fencing_token,
            outbox.claim_fencing_token,
            outbox.payload,
            outbox.available_at,
            outbox.claimed_by,
            outbox.claimed_at,
            outbox.attempt_count
    ), recorded AS (
        INSERT INTO public.cancel_state_events AS recorded_event (
            cancel_state_event_id, cancel_intent_id, state, reason_code,
            dispatch_attempt_count, fencing_token, occurred_at
        )
        SELECT
            claimed.cancel_outbox_id,
            claimed.cancel_intent_id,
            'dispatch_started',
            'CANCEL_DISPATCH_STARTED',
            claimed.attempt_count,
            p_fencing_token,
            clock_timestamp()
        FROM claimed
        RETURNING recorded_event.cancel_intent_id
    )
    SELECT
        claimed.cancel_outbox_id,
        claimed.cancel_intent_id,
        claimed.client_order_id,
        claimed.provider_order_id,
        claimed.reason_code,
        claimed.requested_at,
        claimed.environment,
        claimed.account_fingerprint,
        claimed.created_fencing_token,
        claimed.claim_fencing_token,
        claimed.payload,
        claimed.available_at,
        claimed.claimed_by,
        claimed.claimed_at,
        claimed.attempt_count,
        'dispatch_started'::text
    FROM claimed
    JOIN recorded ON recorded.cancel_intent_id = claimed.cancel_intent_id;
END;
$$;

CREATE OR REPLACE FUNCTION claim_cancel_outbox_retry_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS TABLE (
    cancel_outbox_id UUID,
    cancel_intent_id UUID,
    client_order_id TEXT,
    provider_order_id TEXT,
    reason_code TEXT,
    requested_at TIMESTAMPTZ,
    environment TEXT,
    account_fingerprint TEXT,
    created_fencing_token BIGINT,
    claim_fencing_token BIGINT,
    payload JSONB,
    available_at TIMESTAMPTZ,
    claimed_by UUID,
    claimed_at TIMESTAMPTZ,
    attempt_count INTEGER,
    current_state TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH claimed AS (
        UPDATE public.cancel_outbox AS outbox
        SET claimed_by = p_owner_id,
            claimed_at = clock_timestamp(),
            claim_fencing_token = p_fencing_token,
            attempt_count = outbox.attempt_count + 1
        FROM public.cancel_intents AS cancellation,
             public.current_cancel_states AS cancel_state,
             public.current_intent_states AS intent_state,
             public.current_broker_order_states AS broker_state
        WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
          AND outbox.environment = p_environment
          AND outbox.account_fingerprint = p_account_fingerprint
          AND outbox.completed_at IS NULL
          AND outbox.available_at <= clock_timestamp()
          AND cancellation.cancel_intent_id = outbox.cancel_intent_id
          AND cancellation.environment = p_environment
          AND cancellation.account_fingerprint = p_account_fingerprint
          AND cancel_state.cancel_intent_id = cancellation.cancel_intent_id
          AND cancel_state.state = 'not_dispatched'
          AND cancel_state.reason_code = 'TRANSPORT_BEFORE_SEND'
          AND cancel_state.not_dispatched_provider_order_id = cancellation.provider_order_id
          AND cancel_state.dispatch_attempt_count = outbox.attempt_count
          AND cancel_state.evidence_hash IS NOT NULL
          AND intent_state.intent_id = cancellation.intent_id
          AND intent_state.state <> 'terminal'
          AND broker_state.broker_order_id = cancellation.provider_order_id
          AND broker_state.client_order_id = cancellation.client_order_id
          AND broker_state.recognized_status
          AND broker_state.provider_status NOT IN (
              'filled', 'canceled', 'expired', 'replaced', 'rejected'
          )
          AND p_fencing_token >= outbox.created_fencing_token
          AND (
              outbox.claim_fencing_token IS NULL
              OR p_fencing_token > outbox.claim_fencing_token
              OR (
                  p_fencing_token = outbox.claim_fencing_token
                  AND outbox.claimed_by = p_owner_id
              )
          )
        RETURNING
            outbox.cancel_outbox_id,
            outbox.cancel_intent_id,
            cancellation.client_order_id,
            cancellation.provider_order_id,
            cancellation.reason_code,
            cancellation.requested_at,
            outbox.environment,
            outbox.account_fingerprint,
            outbox.created_fencing_token,
            outbox.claim_fencing_token,
            outbox.payload,
            outbox.available_at,
            outbox.claimed_by,
            outbox.claimed_at,
            outbox.attempt_count
    ), recorded AS (
        INSERT INTO public.cancel_state_events AS recorded_event (
            cancel_state_event_id, cancel_intent_id, state, reason_code,
            dispatch_attempt_count, fencing_token, occurred_at
        )
        SELECT
            substr(encode(sha256(convert_to(
                'cancel-dispatch:' || claimed.cancel_outbox_id::text || ':'
                    || claimed.attempt_count::text,
                'UTF8'
            )), 'hex'), 1, 32)::uuid,
            claimed.cancel_intent_id,
            'dispatch_started',
            'CANCEL_RETRY_DISPATCH_STARTED',
            claimed.attempt_count,
            p_fencing_token,
            clock_timestamp()
        FROM claimed
        RETURNING recorded_event.cancel_intent_id
    )
    SELECT
        claimed.cancel_outbox_id,
        claimed.cancel_intent_id,
        claimed.client_order_id,
        claimed.provider_order_id,
        claimed.reason_code,
        claimed.requested_at,
        claimed.environment,
        claimed.account_fingerprint,
        claimed.created_fencing_token,
        claimed.claim_fencing_token,
        claimed.payload,
        claimed.available_at,
        claimed.claimed_by,
        claimed.claimed_at,
        claimed.attempt_count,
        'dispatch_started'::text
    FROM claimed
    JOIN recorded ON recorded.cancel_intent_id = claimed.cancel_intent_id;
END;
$$;

CREATE OR REPLACE FUNCTION claim_cancel_outbox_recovery_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS TABLE (
    cancel_outbox_id UUID,
    cancel_intent_id UUID,
    client_order_id TEXT,
    provider_order_id TEXT,
    reason_code TEXT,
    requested_at TIMESTAMPTZ,
    environment TEXT,
    account_fingerprint TEXT,
    created_fencing_token BIGINT,
    claim_fencing_token BIGINT,
    payload JSONB,
    available_at TIMESTAMPTZ,
    claimed_by UUID,
    claimed_at TIMESTAMPTZ,
    attempt_count INTEGER,
    current_state TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE public.cancel_outbox AS outbox
    SET claimed_by = p_owner_id,
        claimed_at = clock_timestamp(),
        claim_fencing_token = p_fencing_token,
        attempt_count = outbox.attempt_count + 1
    FROM public.cancel_intents AS cancellation,
         public.current_cancel_states AS cancel_state,
         public.current_intent_states AS intent_state,
         public.current_broker_order_states AS broker_state
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND cancellation.cancel_intent_id = outbox.cancel_intent_id
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND cancel_state.cancel_intent_id = cancellation.cancel_intent_id
      AND cancel_state.state IN (
          'dispatch_started', 'request_accepted', 'cancel_unknown'
      )
      AND intent_state.intent_id = cancellation.intent_id
      AND intent_state.state <> 'terminal'
      AND broker_state.broker_order_id = cancellation.provider_order_id
      AND broker_state.recognized_status
      AND broker_state.provider_status NOT IN (
          'filled', 'canceled', 'expired', 'replaced', 'rejected'
      )
      AND p_fencing_token >= outbox.created_fencing_token
      AND (
          outbox.claim_fencing_token IS NULL
          OR p_fencing_token > outbox.claim_fencing_token
          OR (
              p_fencing_token = outbox.claim_fencing_token
              AND outbox.claimed_by = p_owner_id
          )
      )
    RETURNING
        outbox.cancel_outbox_id,
        outbox.cancel_intent_id,
        cancellation.client_order_id,
        cancellation.provider_order_id,
        cancellation.reason_code,
        cancellation.requested_at,
        outbox.environment,
        outbox.account_fingerprint,
        outbox.created_fencing_token,
        outbox.claim_fencing_token,
        outbox.payload,
        outbox.available_at,
        outbox.claimed_by,
        outbox.claimed_at,
        outbox.attempt_count,
        cancel_state.state;
END;
$$;

CREATE OR REPLACE FUNCTION claim_cancel_outbox_completion_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS TABLE (
    cancel_outbox_id UUID,
    cancel_intent_id UUID,
    client_order_id TEXT,
    provider_order_id TEXT,
    reason_code TEXT,
    requested_at TIMESTAMPTZ,
    environment TEXT,
    account_fingerprint TEXT,
    created_fencing_token BIGINT,
    claim_fencing_token BIGINT,
    payload JSONB,
    available_at TIMESTAMPTZ,
    claimed_by UUID,
    claimed_at TIMESTAMPTZ,
    attempt_count INTEGER,
    current_state TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE public.cancel_outbox AS outbox
    SET claimed_by = p_owner_id,
        claimed_at = clock_timestamp(),
        claim_fencing_token = p_fencing_token,
        attempt_count = outbox.attempt_count + 1
    FROM public.cancel_intents AS cancellation,
         public.current_cancel_states AS cancel_state,
         public.current_intent_states AS intent_state,
         public.current_broker_order_states AS broker_state
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND cancellation.cancel_intent_id = outbox.cancel_intent_id
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND cancel_state.cancel_intent_id = cancellation.cancel_intent_id
      AND cancel_state.state <> 'terminal'
      AND intent_state.intent_id = cancellation.intent_id
      AND intent_state.state = 'terminal'
      AND broker_state.broker_order_id = cancellation.provider_order_id
      AND broker_state.recognized_status
      AND broker_state.provider_status IN (
          'filled', 'canceled', 'expired', 'replaced', 'rejected'
      )
      AND p_fencing_token >= outbox.created_fencing_token
    RETURNING
        outbox.cancel_outbox_id,
        outbox.cancel_intent_id,
        cancellation.client_order_id,
        cancellation.provider_order_id,
        cancellation.reason_code,
        cancellation.requested_at,
        outbox.environment,
        outbox.account_fingerprint,
        outbox.created_fencing_token,
        outbox.claim_fencing_token,
        outbox.payload,
        outbox.available_at,
        outbox.claimed_by,
        outbox.claimed_at,
        outbox.attempt_count,
        cancel_state.state;
END;
$$;

CREATE OR REPLACE FUNCTION append_cancel_request_accepted_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_state_event_id UUID,
    p_provider_order_id TEXT,
    p_request_id TEXT,
    p_payload_hash TEXT,
    p_accepted_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cancel_intent_id UUID;
BEGIN
    IF p_state_event_id IS NULL
       OR p_provider_order_id IS NULL
       OR octet_length(trim(p_provider_order_id)) NOT BETWEEN 1 AND 128
       OR p_request_id IS NULL
       OR octet_length(trim(p_request_id)) NOT BETWEEN 1 AND 128
       OR p_payload_hash IS NULL
       OR p_payload_hash !~ '^[0-9a-f]{64}$'
       OR p_accepted_at IS NULL
       OR p_accepted_at > clock_timestamp() + INTERVAL '5 seconds'
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.cancel_state_events AS event
        WHERE event.cancel_state_event_id = p_state_event_id
    ) THEN
        RETURN EXISTS (
            SELECT 1
            FROM public.cancel_state_events AS event
            JOIN public.cancel_intents AS cancellation
              ON cancellation.cancel_intent_id = event.cancel_intent_id
            JOIN public.cancel_outbox AS outbox
              ON outbox.cancel_intent_id = cancellation.cancel_intent_id
            WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
              AND outbox.environment = p_environment
              AND outbox.account_fingerprint = p_account_fingerprint
              AND cancellation.provider_order_id = p_provider_order_id
              AND event.cancel_state_event_id = p_state_event_id
              AND event.state = 'request_accepted'
              AND event.reason_code = 'CANCEL_REQUEST_ACCEPTED'
              AND event.request_id = p_request_id
              AND event.payload_hash = p_payload_hash
              AND event.fencing_token = p_fencing_token
              AND event.occurred_at = p_accepted_at
        );
    END IF;

    SELECT cancellation.cancel_intent_id
    INTO v_cancel_intent_id
    FROM public.cancel_outbox AS outbox
    JOIN public.cancel_intents AS cancellation
      ON cancellation.cancel_intent_id = outbox.cancel_intent_id
    JOIN public.current_cancel_states AS cancel_state
      ON cancel_state.cancel_intent_id = cancellation.cancel_intent_id
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND outbox.claimed_by = p_owner_id
      AND outbox.claim_fencing_token = p_fencing_token
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND cancellation.provider_order_id = p_provider_order_id
      AND cancel_state.state = 'dispatch_started'
    FOR UPDATE OF outbox;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    INSERT INTO public.cancel_state_events (
        cancel_state_event_id, cancel_intent_id, state, reason_code,
        request_id, payload_hash, fencing_token, occurred_at
    ) VALUES (
        p_state_event_id, v_cancel_intent_id, 'request_accepted',
        'CANCEL_REQUEST_ACCEPTED', p_request_id, p_payload_hash,
        p_fencing_token, p_accepted_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION append_cancel_unknown_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_state_event_id UUID,
    p_detail TEXT,
    p_occurred_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cancel_intent_id UUID;
BEGIN
    IF p_state_event_id IS NULL
       OR p_detail IS NULL
       OR octet_length(p_detail) NOT BETWEEN 1 AND 512
       OR length(trim(p_detail)) = 0
       OR p_occurred_at IS NULL
       OR p_occurred_at > clock_timestamp() + INTERVAL '5 seconds'
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.cancel_state_events AS event
        WHERE event.cancel_state_event_id = p_state_event_id
    ) THEN
        RETURN EXISTS (
            SELECT 1
            FROM public.cancel_state_events AS event
            JOIN public.cancel_outbox AS outbox
              ON outbox.cancel_intent_id = event.cancel_intent_id
            WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
              AND outbox.environment = p_environment
              AND outbox.account_fingerprint = p_account_fingerprint
              AND event.cancel_state_event_id = p_state_event_id
              AND event.state = 'cancel_unknown'
              AND event.reason_code = 'CANCEL_OUTCOME_UNKNOWN'
              AND event.detail = p_detail
              AND event.fencing_token = p_fencing_token
              AND event.occurred_at = p_occurred_at
        );
    END IF;

    SELECT cancellation.cancel_intent_id
    INTO v_cancel_intent_id
    FROM public.cancel_outbox AS outbox
    JOIN public.cancel_intents AS cancellation
      ON cancellation.cancel_intent_id = outbox.cancel_intent_id
    JOIN public.current_cancel_states AS cancel_state
      ON cancel_state.cancel_intent_id = cancellation.cancel_intent_id
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND outbox.claimed_by = p_owner_id
      AND outbox.claim_fencing_token = p_fencing_token
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND cancel_state.state = 'dispatch_started'
    FOR UPDATE OF outbox;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    INSERT INTO public.cancel_state_events (
        cancel_state_event_id, cancel_intent_id, state, reason_code,
        detail, fencing_token, occurred_at
    ) VALUES (
        p_state_event_id, v_cancel_intent_id, 'cancel_unknown',
        'CANCEL_OUTCOME_UNKNOWN', p_detail, p_fencing_token, p_occurred_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION append_cancel_not_dispatched_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_state_event_id UUID,
    p_provider_order_id TEXT,
    p_expected_attempt_count INTEGER,
    p_reason_code TEXT,
    p_detail TEXT,
    p_evidence_hash TEXT,
    p_observed_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cancel_intent_id UUID;
BEGIN
    IF p_state_event_id IS NULL
       OR p_provider_order_id IS NULL
       OR trim(p_provider_order_id) <> p_provider_order_id
       OR octet_length(p_provider_order_id) NOT BETWEEN 1 AND 128
       OR p_expected_attempt_count IS NULL
       OR p_expected_attempt_count < 1
       OR p_reason_code IS DISTINCT FROM 'TRANSPORT_BEFORE_SEND'
       OR p_detail IS NULL
       OR trim(p_detail) <> p_detail
       OR octet_length(p_detail) NOT BETWEEN 1 AND 512
       OR p_evidence_hash IS NULL
       OR p_evidence_hash !~ '^[0-9a-f]{64}$'
       OR p_observed_at IS NULL
       OR p_observed_at > clock_timestamp() + INTERVAL '5 seconds'
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.cancel_state_events AS event
        WHERE event.cancel_state_event_id = p_state_event_id
    ) THEN
        RETURN EXISTS (
            SELECT 1
            FROM public.cancel_state_events AS event
            JOIN public.cancel_intents AS cancellation
              ON cancellation.cancel_intent_id = event.cancel_intent_id
            JOIN public.cancel_outbox AS outbox
              ON outbox.cancel_intent_id = cancellation.cancel_intent_id
            WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
              AND outbox.environment = p_environment
              AND outbox.account_fingerprint = p_account_fingerprint
              AND cancellation.provider_order_id = p_provider_order_id
              AND event.cancel_state_event_id = p_state_event_id
              AND event.state = 'not_dispatched'
              AND event.reason_code = p_reason_code
              AND event.not_dispatched_provider_order_id = p_provider_order_id
              AND event.dispatch_attempt_count = p_expected_attempt_count
              AND event.evidence_hash = p_evidence_hash
              AND event.detail = p_detail
              AND event.fencing_token = p_fencing_token
              AND event.occurred_at = p_observed_at
        );
    END IF;

    SELECT cancellation.cancel_intent_id
    INTO v_cancel_intent_id
    FROM public.cancel_outbox AS outbox
    JOIN public.cancel_intents AS cancellation
      ON cancellation.cancel_intent_id = outbox.cancel_intent_id
    JOIN public.current_cancel_states AS cancel_state
      ON cancel_state.cancel_intent_id = cancellation.cancel_intent_id
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND outbox.claimed_by = p_owner_id
      AND outbox.claim_fencing_token = p_fencing_token
      AND outbox.attempt_count = p_expected_attempt_count
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND cancellation.provider_order_id = p_provider_order_id
      AND cancel_state.state = 'dispatch_started'
      AND cancel_state.dispatch_attempt_count = p_expected_attempt_count
    FOR UPDATE OF outbox;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    INSERT INTO public.cancel_state_events (
        cancel_state_event_id, cancel_intent_id, state, reason_code,
        not_dispatched_provider_order_id, dispatch_attempt_count,
        evidence_hash, detail, fencing_token, occurred_at
    ) VALUES (
        p_state_event_id, v_cancel_intent_id, 'not_dispatched', p_reason_code,
        p_provider_order_id, p_expected_attempt_count, p_evidence_hash,
        p_detail, p_fencing_token, p_observed_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION finalize_cancel_outbox_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_cancel_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_terminal_state_event_id UUID,
    p_terminal_broker_event_id UUID,
    p_completion_reason TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cancel_intent_id UUID;
    v_provider_status TEXT;
    v_rows INTEGER;
BEGIN
    IF p_terminal_state_event_id IS NULL
       OR p_terminal_broker_event_id IS NULL
       OR p_completion_reason IS NULL
       OR octet_length(trim(p_completion_reason)) NOT BETWEEN 1 AND 128
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.cancel_outbox AS outbox
        WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
          AND outbox.completed_at IS NOT NULL
    ) THEN
        RETURN EXISTS (
            SELECT 1
            FROM public.cancel_outbox AS outbox
            JOIN public.cancel_state_events AS event
              ON event.cancel_intent_id = outbox.cancel_intent_id
            WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
              AND outbox.environment = p_environment
              AND outbox.account_fingerprint = p_account_fingerprint
              AND outbox.completion_reason = p_completion_reason
              AND event.cancel_state_event_id = p_terminal_state_event_id
              AND event.state = 'terminal'
              AND event.reason_code = p_completion_reason
              AND event.broker_event_id = p_terminal_broker_event_id
              AND event.fencing_token = p_fencing_token
        );
    END IF;

    SELECT cancellation.cancel_intent_id, broker_state.provider_status
    INTO v_cancel_intent_id, v_provider_status
    FROM public.cancel_outbox AS outbox
    JOIN public.cancel_intents AS cancellation
      ON cancellation.cancel_intent_id = outbox.cancel_intent_id
    JOIN public.current_cancel_states AS cancel_state
      ON cancel_state.cancel_intent_id = cancellation.cancel_intent_id
    JOIN public.current_intent_states AS intent_state
      ON intent_state.intent_id = cancellation.intent_id
    JOIN public.current_broker_order_states AS broker_state
      ON broker_state.broker_order_id = cancellation.provider_order_id
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND outbox.claimed_by = p_owner_id
      AND outbox.claim_fencing_token = p_fencing_token
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND cancel_state.state <> 'terminal'
      AND intent_state.state = 'terminal'
      AND broker_state.broker_event_id = p_terminal_broker_event_id
      AND broker_state.recognized_status
      AND p_completion_reason = 'BROKER_TERMINAL_' || upper(broker_state.provider_status)
      AND broker_state.provider_status IN (
          'filled', 'canceled', 'expired', 'replaced', 'rejected'
      )
    FOR UPDATE OF outbox;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    INSERT INTO public.cancel_state_events (
        cancel_state_event_id, cancel_intent_id, state, reason_code,
        broker_event_id, fencing_token, occurred_at
    ) VALUES (
        p_terminal_state_event_id, v_cancel_intent_id, 'terminal',
        p_completion_reason, p_terminal_broker_event_id,
        p_fencing_token, clock_timestamp()
    );

    UPDATE public.cancel_outbox AS outbox
    SET completed_at = clock_timestamp(),
        completion_reason = p_completion_reason
    WHERE outbox.cancel_outbox_id = p_cancel_outbox_id
      AND outbox.completed_at IS NULL;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END;
$$;

CREATE OR REPLACE FUNCTION list_unresolved_cancel_outboxes_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_limit INTEGER
) RETURNS TABLE (
    cancel_outbox_id UUID,
    cancel_intent_id UUID,
    client_order_id TEXT,
    provider_order_id TEXT,
    reason_code TEXT,
    requested_at TIMESTAMPTZ,
    created_fencing_token BIGINT,
    payload JSONB,
    available_at TIMESTAMPTZ,
    current_state TEXT,
    request_id TEXT,
    payload_hash TEXT,
    detail TEXT,
    broker_event_id UUID,
    state_occurred_at TIMESTAMPTZ,
    state_reason_code TEXT,
    state_not_dispatched_provider_order_id TEXT,
    state_dispatch_attempt_count INTEGER,
    state_evidence_hash TEXT,
    terminal_broker_event_id UUID,
    terminal_provider_order_id TEXT,
    terminal_client_order_id TEXT,
    terminal_provider_status TEXT,
    terminal_recognized_status BOOLEAN,
    terminal_cumulative_filled_quantity TEXT,
    terminal_average_fill_price TEXT,
    terminal_provider_occurred_at TIMESTAMPTZ,
    terminal_received_at TIMESTAMPTZ,
    terminal_request_id TEXT,
    terminal_raw_hash TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_limit < 1 OR p_limit > 1000 OR NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        outbox.cancel_outbox_id,
        outbox.cancel_intent_id,
        cancellation.client_order_id,
        cancellation.provider_order_id,
        cancellation.reason_code,
        cancellation.requested_at,
        outbox.created_fencing_token,
        outbox.payload,
        outbox.available_at,
        cancel_state.state,
        cancel_state.request_id,
        cancel_state.payload_hash,
        cancel_state.detail,
        cancel_state.broker_event_id,
        cancel_state.occurred_at,
        cancel_state.reason_code,
        cancel_state.not_dispatched_provider_order_id,
        cancel_state.dispatch_attempt_count,
        cancel_state.evidence_hash,
        terminal_broker.broker_event_id,
        terminal_broker.broker_order_id,
        terminal_broker.client_order_id,
        terminal_broker.provider_status,
        terminal_broker.recognized_status,
        terminal_broker.cumulative_filled_quantity::text,
        terminal_broker.average_fill_price::text,
        terminal_broker.provider_occurred_at,
        terminal_broker.received_at,
        terminal_broker.x_request_id,
        terminal_broker.raw_hash
    FROM public.cancel_outbox AS outbox
    JOIN public.cancel_intents AS cancellation
      ON cancellation.cancel_intent_id = outbox.cancel_intent_id
    JOIN public.current_cancel_states AS cancel_state
      ON cancel_state.cancel_intent_id = cancellation.cancel_intent_id
    LEFT JOIN LATERAL (
        SELECT broker_state.*
        FROM public.current_intent_states AS intent_state
        JOIN public.current_broker_order_states AS broker_state
          ON broker_state.broker_order_id = cancellation.provider_order_id
        WHERE intent_state.intent_id = cancellation.intent_id
          AND intent_state.state = 'terminal'
          AND broker_state.client_order_id = cancellation.client_order_id
          AND broker_state.recognized_status
          AND broker_state.provider_status IN (
              'filled', 'canceled', 'expired', 'replaced', 'rejected'
          )
    ) AS terminal_broker ON TRUE
    WHERE outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND cancellation.environment = p_environment
      AND cancellation.account_fingerprint = p_account_fingerprint
      AND p_fencing_token >= outbox.created_fencing_token
      AND cancel_state.state <> 'terminal'
    ORDER BY outbox.available_at, outbox.cancel_outbox_id
    LIMIT p_limit;
END;
$$;

INSERT INTO runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'function', signature,
    encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(signature)), 'UTF8')), 'hex')
FROM (VALUES
    ('public.claim_order_outbox_v3(text,text,uuid,uuid,bigint)'),
    ('public.persist_cancel_intent_v2(text,text,uuid,bigint,uuid,uuid,uuid,uuid,text,text,text,timestamp with time zone,jsonb)'),
    ('public.claim_cancel_outbox_v2(text,text,uuid,uuid,bigint)'),
    ('public.claim_cancel_outbox_retry_v2(text,text,uuid,uuid,bigint)'),
    ('public.claim_cancel_outbox_recovery_v2(text,text,uuid,uuid,bigint)'),
    ('public.claim_cancel_outbox_completion_v2(text,text,uuid,uuid,bigint)'),
    ('public.append_cancel_request_accepted_v2(text,text,uuid,uuid,bigint,uuid,text,text,text,timestamp with time zone)'),
    ('public.append_cancel_unknown_v2(text,text,uuid,uuid,bigint,uuid,text,timestamp with time zone)'),
    ('public.append_cancel_not_dispatched_v2(text,text,uuid,uuid,bigint,uuid,text,integer,text,text,text,timestamp with time zone)'),
    ('public.finalize_cancel_outbox_v2(text,text,uuid,uuid,bigint,uuid,uuid,text)'),
    ('public.list_unresolved_cancel_outboxes_v2(text,text,uuid,bigint,integer)'),
    ('public.enforce_cancel_intent_chain()'),
    ('public.enforce_cancel_state_transition()'),
    ('public.enforce_cancel_outbox_chain()'),
    ('public.protect_cancel_outbox()')
) AS required_function(signature);

INSERT INTO runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'view', identity,
    encode(sha256(convert_to(pg_get_viewdef(identity::regclass, true), 'UTF8')), 'hex')
FROM (VALUES
    ('public.current_cancel_states'),
    ('public.execution_readiness_v2')
) AS required_view(identity);

INSERT INTO runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'trigger', namespace.nspname || '.' || relation.relname || '.' || trigger.tgname,
    encode(sha256(convert_to(pg_get_triggerdef(trigger.oid, true), 'UTF8')), 'hex')
FROM pg_trigger AS trigger
JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND relation.relname IN ('cancel_intents', 'cancel_state_events', 'cancel_outbox')
  AND NOT trigger.tgisinternal;

INSERT INTO runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'constraint', namespace.nspname || '.' || relation.relname || '.' || con.conname,
    encode(sha256(convert_to(pg_get_constraintdef(con.oid, true), 'UTF8')), 'hex')
FROM pg_constraint AS con
JOIN pg_class AS relation ON relation.oid = con.conrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND relation.relname IN ('cancel_intents', 'cancel_state_events', 'cancel_outbox');

REVOKE ALL ON cancel_intents, cancel_state_events, cancel_outbox,
    current_cancel_states, execution_readiness_v2
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator;
GRANT SELECT ON cancel_intents, cancel_state_events, cancel_outbox,
    current_cancel_states, execution_readiness_v2
TO alpaca_trader_runtime, alpaca_trader_operator;
REVOKE ALL ON SEQUENCE cancel_state_events_event_sequence_seq
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator;

REVOKE ALL ON FUNCTION
    claim_order_outbox_v3(TEXT, TEXT, UUID, UUID, BIGINT),
    persist_cancel_intent_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, UUID, UUID, TEXT, TEXT, TEXT, TIMESTAMPTZ, JSONB),
    claim_cancel_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_cancel_outbox_retry_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_cancel_outbox_recovery_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_cancel_outbox_completion_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    append_cancel_request_accepted_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    append_cancel_unknown_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, TIMESTAMPTZ),
    append_cancel_not_dispatched_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, INTEGER, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    finalize_cancel_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, UUID, TEXT),
    list_unresolved_cancel_outboxes_v2(TEXT, TEXT, UUID, BIGINT, INTEGER),
    enforce_cancel_intent_chain(),
    enforce_cancel_state_transition(),
    enforce_cancel_outbox_chain(),
    protect_cancel_outbox()
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator;

REVOKE EXECUTE ON FUNCTION
    claim_order_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT)
FROM alpaca_trader_runtime;

GRANT EXECUTE ON FUNCTION
    claim_order_outbox_v3(TEXT, TEXT, UUID, UUID, BIGINT),
    persist_cancel_intent_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, UUID, UUID, TEXT, TEXT, TEXT, TIMESTAMPTZ, JSONB),
    claim_cancel_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_cancel_outbox_retry_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_cancel_outbox_recovery_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_cancel_outbox_completion_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    append_cancel_request_accepted_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    append_cancel_unknown_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, TIMESTAMPTZ),
    append_cancel_not_dispatched_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, INTEGER, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    finalize_cancel_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, UUID, TEXT),
    list_unresolved_cancel_outboxes_v2(TEXT, TEXT, UUID, BIGINT, INTEGER)
TO alpaca_trader_runtime;

COMMIT;
