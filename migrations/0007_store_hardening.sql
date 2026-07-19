BEGIN;

-- Store-side writes use this server-side assertion at the start and end of
-- every transaction.  The row lock prevents a concurrent lease takeover from
-- crossing a durable write performed under an older fence.
CREATE OR REPLACE FUNCTION assert_current_executor_lease(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.executor_leases AS lease
    WHERE lease.environment = p_environment
      AND lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
    FOR UPDATE;
    RETURN FOUND;
END;
$$;

-- Unlike the historical three-argument function, every v2 authority function
-- binds the caller-supplied ID to the store's environment and account domain.
CREATE OR REPLACE FUNCTION claim_order_outbox_v2(
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
        FROM public.execution_readiness AS authority,
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

CREATE OR REPLACE FUNCTION claim_order_outbox_recovery_v2(
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
    UPDATE public.order_outbox AS outbox
    SET claimed_by = p_owner_id,
        claimed_at = clock_timestamp(),
        claim_fencing_token = p_fencing_token,
        attempt_count = outbox.attempt_count + 1,
        last_error = NULL
    FROM public.order_intents AS intent,
         public.current_intent_states AS intent_state
    WHERE outbox.outbox_id = p_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND intent.intent_id = outbox.intent_id
      AND intent.environment = p_environment
      AND intent.account_fingerprint = p_account_fingerprint
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
      AND p_fencing_token >= outbox.created_fencing_token
      AND (
          outbox.claim_fencing_token IS NULL
          OR p_fencing_token > outbox.claim_fencing_token
          OR (
              p_fencing_token = outbox.claim_fencing_token
              AND outbox.claimed_by = p_owner_id
          )
      )
    RETURNING outbox.*;
END;
$$;

-- A terminal reclaim can only complete durable bookkeeping.  It is kept
-- separate from lookup recovery so a restarted worker cannot issue another
-- broker mutation after terminal truth is present.
CREATE OR REPLACE FUNCTION claim_order_outbox_completion_v2(
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
    UPDATE public.order_outbox AS outbox
    SET claimed_by = p_owner_id,
        claimed_at = clock_timestamp(),
        claim_fencing_token = p_fencing_token,
        attempt_count = outbox.attempt_count + 1,
        last_error = NULL
    FROM public.order_intents AS intent,
         public.current_intent_states AS intent_state
    WHERE outbox.outbox_id = p_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND intent.intent_id = outbox.intent_id
      AND intent.environment = p_environment
      AND intent.account_fingerprint = p_account_fingerprint
      AND intent_state.intent_id = intent.intent_id
      AND intent_state.state = 'terminal'
      AND EXISTS (
          SELECT 1
          FROM public.intent_state_events AS first_dispatch
          WHERE first_dispatch.intent_id = intent.intent_id
            AND first_dispatch.state = 'dispatch_started'
      )
      AND p_fencing_token >= outbox.created_fencing_token
    RETURNING outbox.*;
END;
$$;

CREATE OR REPLACE FUNCTION append_submission_unknown_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_state_event_id UUID,
    p_reason_code TEXT,
    p_detail JSONB,
    p_occurred_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_intent_id UUID;
BEGIN
    IF p_reason_code IS NULL
       OR length(trim(p_reason_code)) = 0
       OR length(p_reason_code) > 128
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    SELECT outbox.intent_id
    INTO v_intent_id
    FROM public.order_outbox AS outbox
    JOIN public.order_intents AS intent ON intent.intent_id = outbox.intent_id
    JOIN public.current_intent_states AS intent_state
      ON intent_state.intent_id = intent.intent_id
    WHERE outbox.outbox_id = p_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND outbox.claimed_by = p_owner_id
      AND outbox.claim_fencing_token = p_fencing_token
      AND intent.environment = p_environment
      AND intent.account_fingerprint = p_account_fingerprint
      AND intent_state.state = 'dispatch_started'
    FOR UPDATE OF outbox;

    IF NOT FOUND THEN
        -- Deterministic retries may observe the event written by an ambiguous
        -- prior commit; only an exact authority/evidence match is accepted.
        RETURN EXISTS (
            SELECT 1
            FROM public.intent_state_events AS event
            JOIN public.order_intents AS intent ON intent.intent_id = event.intent_id
            JOIN public.order_outbox AS outbox ON outbox.intent_id = intent.intent_id
            WHERE event.intent_state_event_id = p_state_event_id
              AND event.state = 'submission_unknown'
              AND event.reason_code = p_reason_code
              AND event.detail = COALESCE(p_detail, '{}'::jsonb)
              AND event.fencing_token = p_fencing_token
              AND event.occurred_at = p_occurred_at
              AND outbox.outbox_id = p_outbox_id
              AND outbox.environment = p_environment
              AND outbox.account_fingerprint = p_account_fingerprint
        );
    END IF;

    INSERT INTO public.intent_state_events (
        intent_state_event_id, intent_id, state, reason_code,
        detail, fencing_token, occurred_at
    ) VALUES (
        p_state_event_id, v_intent_id, 'submission_unknown', p_reason_code,
        COALESCE(p_detail, '{}'::jsonb), p_fencing_token, p_occurred_at
    )
    ON CONFLICT (intent_state_event_id) DO NOTHING;

    RETURN EXISTS (
        SELECT 1
        FROM public.intent_state_events AS event
        WHERE event.intent_state_event_id = p_state_event_id
          AND event.intent_id = v_intent_id
          AND event.state = 'submission_unknown'
          AND event.reason_code = p_reason_code
          AND event.detail = COALESCE(p_detail, '{}'::jsonb)
          AND event.fencing_token = p_fencing_token
          AND event.occurred_at = p_occurred_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION finalize_order_outbox_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
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
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       )
    THEN
        RETURN FALSE;
    END IF;

    UPDATE public.order_outbox AS outbox
    SET completed_at = clock_timestamp(),
        completion_reason = p_completion_reason
    FROM public.order_intents AS intent,
         public.current_intent_states AS intent_state,
         public.current_broker_order_states AS broker_state,
         public.broker_orders AS broker_order
    WHERE outbox.outbox_id = p_outbox_id
      AND outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND outbox.claimed_by = p_owner_id
      AND outbox.claim_fencing_token = p_fencing_token
      AND intent.intent_id = outbox.intent_id
      AND intent.environment = p_environment
      AND intent.account_fingerprint = p_account_fingerprint
      AND intent_state.intent_id = intent.intent_id
      AND intent_state.state = 'terminal'
      AND broker_order.intent_id = intent.intent_id
      AND broker_state.broker_order_id = broker_order.broker_order_id
      AND broker_state.recognized_status
      AND broker_state.provider_status IN (
          'filled', 'canceled', 'expired', 'replaced', 'rejected'
      );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END;
$$;

CREATE OR REPLACE FUNCTION list_unresolved_order_outboxes_v2(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_limit INTEGER
) RETURNS TABLE (
    outbox_id UUID,
    intent_id UUID,
    created_fencing_token BIGINT,
    payload JSONB,
    available_at TIMESTAMPTZ,
    current_state TEXT
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
        outbox.outbox_id,
        outbox.intent_id,
        outbox.created_fencing_token,
        outbox.payload,
        outbox.available_at,
        intent_state.state
    FROM public.order_outbox AS outbox
    JOIN public.order_intents AS intent ON intent.intent_id = outbox.intent_id
    JOIN public.current_intent_states AS intent_state
      ON intent_state.intent_id = intent.intent_id
    WHERE outbox.environment = p_environment
      AND outbox.account_fingerprint = p_account_fingerprint
      AND outbox.completed_at IS NULL
      AND intent.environment = p_environment
      AND intent.account_fingerprint = p_account_fingerprint
    ORDER BY outbox.available_at, outbox.outbox_id
    LIMIT p_limit;
END;
$$;

-- Readiness must remain false for every state that requires reconciliation or
-- recovery before another first dispatch can be considered.
CREATE OR REPLACE VIEW execution_readiness AS
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
            FROM public.reconciliation_diffs AS difference
            WHERE difference.reconciliation_id = r.reconciliation_id
              AND difference.resolution IN ('unresolved', 'escalated')
        )
        AND NOT EXISTS (
            SELECT 1
            FROM public.order_intents AS intent
            LEFT JOIN public.current_intent_states AS intent_state
              ON intent_state.intent_id = intent.intent_id
            WHERE intent.environment = p.environment
              AND intent.account_fingerprint = p.account_fingerprint
              AND (
                  intent_state.intent_id IS NULL
                  OR intent_state.state IN (
                      'dispatch_started', 'submission_unknown', 'acknowledged',
                      'broker_confirmed', 'blocked'
                  )
              )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM public.broker_orders AS broker_order
            LEFT JOIN public.current_broker_order_states AS broker_state
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
FROM public.current_activation_permits AS p
LEFT JOIN public.current_kill_state AS k
    ON k.environment = p.environment
   AND k.account_fingerprint = p.account_fingerprint
LEFT JOIN public.latest_reconciliation AS r
    ON r.environment = p.environment
   AND r.account_fingerprint = p.account_fingerprint
LEFT JOIN public.account_snapshots AS a
    ON a.account_snapshot_id = r.account_snapshot_id
LEFT JOIN public.executor_leases AS l
    ON l.environment = p.environment
   AND l.account_fingerprint = p.account_fingerprint;

-- Provider statuses such as done_for_day, stopped, suspended, and calculated
-- are observable nonterminal states.  Mirror the reviewed Rust lifecycle graph
-- and permit later provider-time progress while rejecting regressions,
-- contradictions, and any mutation after truly terminal broker truth.
CREATE OR REPLACE FUNCTION enforce_broker_event_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_order_quantity BIGINT;
    v_previous_status TEXT;
    v_previous_recognized BOOLEAN;
    v_previous_filled NUMERIC(38, 6);
    v_previous_average NUMERIC(38, 6);
    v_previous_provider_at TIMESTAMPTZ;
    v_previous_received_at TIMESTAMPTZ;
    v_expected_recognized BOOLEAN;
    v_same_observation BOOLEAN := FALSE;
    v_transition_allowed BOOLEAN := FALSE;
BEGIN
    SELECT intent.whole_quantity
    INTO v_order_quantity
    FROM public.broker_orders AS broker_order
    JOIN public.order_intents AS intent ON intent.intent_id = broker_order.intent_id
    WHERE broker_order.broker_order_id = NEW.broker_order_id
      AND broker_order.client_order_id = NEW.client_order_id
    FOR UPDATE OF broker_order;

    IF NOT FOUND OR NEW.cumulative_filled_quantity > v_order_quantity THEN
        RAISE EXCEPTION 'broker event does not match order identity or fill bounds';
    END IF;

    SELECT
        event.provider_status,
        event.recognized_status,
        event.cumulative_filled_quantity,
        event.average_fill_price,
        event.provider_occurred_at,
        event.received_at
    INTO
        v_previous_status,
        v_previous_recognized,
        v_previous_filled,
        v_previous_average,
        v_previous_provider_at,
        v_previous_received_at
    FROM public.broker_order_events AS event
    WHERE event.broker_order_id = NEW.broker_order_id
    ORDER BY event.event_sequence DESC
    LIMIT 1;

    NEW.event_sequence := COALESCE((
        SELECT MAX(event.event_sequence) + 1
        FROM public.broker_order_events AS event
        WHERE event.broker_order_id = NEW.broker_order_id
    ), 1);
    v_expected_recognized := NEW.provider_status IN (
        'accepted', 'new', 'pending_new', 'accepted_for_bidding',
        'partially_filled', 'filled', 'done_for_day', 'canceled', 'expired',
        'replaced', 'pending_cancel', 'pending_replace', 'stopped', 'rejected',
        'suspended', 'calculated'
    );
    IF NEW.recognized_status IS DISTINCT FROM v_expected_recognized THEN
        RAISE EXCEPTION 'recognized_status does not match the provider-status allowlist';
    END IF;
    IF NEW.provider_occurred_at IS NULL
       OR NEW.provider_occurred_at > NEW.received_at
       OR NEW.received_at > clock_timestamp() + INTERVAL '5 seconds'
    THEN
        RAISE EXCEPTION 'broker event provider/receive timestamps are invalid';
    END IF;
    IF v_previous_filled IS NOT NULL
       AND NEW.cumulative_filled_quantity < v_previous_filled
    THEN
        RAISE EXCEPTION 'broker cumulative fill quantity regressed';
    END IF;
    IF v_previous_received_at IS NOT NULL AND NEW.received_at < v_previous_received_at THEN
        RAISE EXCEPTION 'broker receive ordering regressed';
    END IF;
    IF v_previous_provider_at IS NOT NULL
       AND NEW.provider_occurred_at < v_previous_provider_at
    THEN
        RAISE EXCEPTION 'older non-duplicate provider event requires quarantine';
    END IF;
    v_same_observation := v_previous_status IS NOT NULL
        AND NEW.provider_occurred_at = v_previous_provider_at
        AND NEW.provider_status = v_previous_status
        AND NEW.cumulative_filled_quantity = v_previous_filled
        AND NEW.average_fill_price IS NOT DISTINCT FROM v_previous_average;
    IF v_previous_provider_at IS NOT NULL
       AND NEW.provider_occurred_at = v_previous_provider_at
       AND NOT v_same_observation
    THEN
        RAISE EXCEPTION 'same provider timestamp produced contradictory order semantics';
    END IF;
    IF v_previous_recognized IS FALSE THEN
        RAISE EXCEPTION 'events after an unknown provider status require reconciliation and review';
    END IF;

    IF v_previous_status IS NULL OR v_same_observation THEN
        v_transition_allowed := TRUE;
    ELSIF v_previous_status IN ('filled', 'canceled', 'expired', 'replaced', 'rejected') THEN
        v_transition_allowed := FALSE;
    ELSIF v_previous_status = 'accepted' THEN
        v_transition_allowed := NEW.provider_status IN (
            'accepted', 'pending_new', 'new', 'accepted_for_bidding',
            'partially_filled', 'filled', 'done_for_day', 'pending_cancel',
            'pending_replace', 'canceled', 'expired', 'replaced', 'stopped',
            'rejected', 'suspended', 'calculated'
        );
    ELSIF v_previous_status = 'pending_new' THEN
        v_transition_allowed := NEW.provider_status IN (
            'pending_new', 'new', 'accepted_for_bidding', 'partially_filled',
            'filled', 'done_for_day', 'pending_cancel', 'pending_replace',
            'canceled', 'expired', 'replaced', 'stopped', 'rejected',
            'suspended', 'calculated'
        );
    ELSIF v_previous_status = 'accepted_for_bidding' THEN
        v_transition_allowed := NEW.provider_status IN (
            'accepted_for_bidding', 'new', 'partially_filled', 'filled',
            'done_for_day', 'pending_cancel', 'pending_replace', 'canceled',
            'expired', 'replaced', 'stopped', 'rejected', 'suspended', 'calculated'
        );
    ELSIF v_previous_status = 'new' THEN
        v_transition_allowed := NEW.provider_status IN (
            'new', 'partially_filled', 'filled', 'done_for_day',
            'pending_cancel', 'pending_replace', 'canceled', 'expired',
            'replaced', 'stopped', 'rejected', 'suspended', 'calculated'
        );
    ELSIF v_previous_status = 'pending_cancel' THEN
        v_transition_allowed := NEW.provider_status IN (
            'pending_cancel', 'partially_filled', 'filled', 'done_for_day',
            'canceled', 'expired', 'stopped', 'calculated'
        );
    ELSIF v_previous_status = 'pending_replace' THEN
        v_transition_allowed := NEW.provider_status IN (
            'pending_replace', 'new', 'partially_filled', 'filled',
            'done_for_day', 'canceled', 'expired', 'replaced', 'stopped', 'calculated'
        );
    ELSIF v_previous_status = 'partially_filled' THEN
        v_transition_allowed := NEW.provider_status IN (
            'partially_filled', 'filled', 'done_for_day', 'pending_cancel',
            'pending_replace', 'canceled', 'expired', 'replaced', 'stopped',
            'suspended', 'calculated'
        );
    ELSIF v_previous_status = 'done_for_day' THEN
        v_transition_allowed := NEW.provider_status IN (
            'done_for_day', 'filled', 'canceled', 'expired', 'calculated'
        );
    ELSIF v_previous_status = 'stopped' THEN
        v_transition_allowed := NEW.provider_status IN (
            'stopped', 'partially_filled', 'filled', 'rejected',
            'canceled', 'expired', 'calculated'
        );
    ELSIF v_previous_status = 'suspended' THEN
        v_transition_allowed := NEW.provider_status IN (
            'suspended', 'new', 'partially_filled', 'filled',
            'pending_cancel', 'canceled', 'expired', 'rejected'
        );
    ELSIF v_previous_status = 'calculated' THEN
        v_transition_allowed := NEW.provider_status IN (
            'calculated', 'filled', 'done_for_day', 'canceled', 'expired'
        );
    END IF;
    IF NOT v_transition_allowed THEN
        RAISE EXCEPTION 'provider order state violated the explicit transition graph';
    END IF;

    IF NEW.provider_status = 'partially_filled' AND (
        NEW.cumulative_filled_quantity <= 0
        OR NEW.cumulative_filled_quantity >= v_order_quantity
        OR NEW.average_fill_price IS NULL
    ) THEN
        RAISE EXCEPTION 'partial-fill event lacks bounded quantity and average price';
    END IF;
    IF NEW.provider_status = 'filled' AND (
        NEW.cumulative_filled_quantity <> v_order_quantity
        OR NEW.average_fill_price IS NULL
    ) THEN
        RAISE EXCEPTION 'filled event lacks full quantity and average price';
    END IF;
    IF (NEW.cumulative_filled_quantity = 0 AND NEW.average_fill_price IS NOT NULL)
       OR (NEW.cumulative_filled_quantity > 0 AND NEW.average_fill_price IS NULL)
    THEN
        RAISE EXCEPTION 'filled quantity and average fill price are inconsistent';
    END IF;
    RETURN NEW;
END;
$$;

-- A broker event is not durable fill truth unless its cumulative quantity is
-- exactly the sum of the immutable fill rows already inserted in the same
-- serializable transaction.
CREATE OR REPLACE FUNCTION enforce_broker_event_fill_truth()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_durable_filled NUMERIC(38, 6);
BEGIN
    SELECT COALESCE(SUM(fill.quantity), 0)
    INTO v_durable_filled
    FROM public.fills AS fill
    WHERE fill.broker_order_id = NEW.broker_order_id;

    IF NEW.cumulative_filled_quantity <> v_durable_filled THEN
        RAISE EXCEPTION 'broker cumulative quantity does not equal durable fill truth';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER broker_order_events_require_fill_truth
BEFORE INSERT ON broker_order_events
FOR EACH ROW EXECUTE FUNCTION enforce_broker_event_fill_truth();

CREATE OR REPLACE FUNCTION enforce_terminal_intent_fill_truth()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_broker_cumulative NUMERIC(38, 6);
    v_durable_filled NUMERIC(38, 6);
BEGIN
    IF NEW.state <> 'terminal' THEN
        RETURN NEW;
    END IF;

    SELECT broker_state.cumulative_filled_quantity
    INTO v_broker_cumulative
    FROM public.broker_orders AS broker_order
    JOIN public.current_broker_order_states AS broker_state
      ON broker_state.broker_order_id = broker_order.broker_order_id
    WHERE broker_order.intent_id = NEW.intent_id
      AND broker_state.recognized_status;

    SELECT COALESCE(SUM(fill.quantity), 0)
    INTO v_durable_filled
    FROM public.fills AS fill
    WHERE fill.intent_id = NEW.intent_id;

    IF v_broker_cumulative IS NULL OR v_broker_cumulative <> v_durable_filled THEN
        RAISE EXCEPTION 'terminal intent lacks exact broker and durable fill truth';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER intent_state_events_require_terminal_fill_truth
BEFORE INSERT ON intent_state_events
FOR EACH ROW EXECUTE FUNCTION enforce_terminal_intent_fill_truth();

-- Runtime has no direct table mutation privilege.  These narrowly-scoped
-- functions are the only supported ledger write boundary and each rechecks
-- the current owner/account/environment fence on the server.
CREATE OR REPLACE FUNCTION insert_decision_snapshot_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_decision_id UUID, p_release_id UUID,
    p_market_session DATE, p_as_of TIMESTAMPTZ, p_input_data_hash TEXT,
    p_account_snapshot_hash TEXT, p_payload JSONB
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN RAISE EXCEPTION 'decision write lacks current execution authority'; END IF;
    INSERT INTO public.decision_snapshots (
        decision_id, strategy_release_id, environment, account_fingerprint,
        market_session, as_of, input_data_hash, account_snapshot_hash, payload
    ) VALUES (
        p_decision_id, p_release_id, p_environment, p_account_fingerprint,
        p_market_session, p_as_of, p_input_data_hash, p_account_snapshot_hash, p_payload
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_target_portfolio_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_target_id UUID, p_decision_id UUID,
    p_release_id UUID, p_reason_code TEXT, p_payload_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.decision_snapshots AS decision
        WHERE decision.decision_id = p_decision_id
          AND decision.environment = p_environment
          AND decision.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'target write lacks current execution authority'; END IF;
    INSERT INTO public.target_portfolios (
        target_portfolio_id, decision_id, strategy_release_id, reason_code, payload_hash
    ) VALUES (p_target_id, p_decision_id, p_release_id, p_reason_code, p_payload_hash);
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_target_position_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_target_id UUID, p_symbol TEXT,
    p_target_quantity BIGINT, p_target_weight NUMERIC, p_reason_code TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.target_portfolios AS target
        JOIN public.decision_snapshots AS decision ON decision.decision_id = target.decision_id
        WHERE target.target_portfolio_id = p_target_id
          AND decision.environment = p_environment
          AND decision.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'target-position write lacks current execution authority'; END IF;
    INSERT INTO public.target_positions (
        target_portfolio_id, symbol, target_quantity, target_weight, reason_code
    ) VALUES (p_target_id, p_symbol, p_target_quantity, p_target_weight, p_reason_code);
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_risk_decision_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_risk_id UUID, p_target_id UUID,
    p_permit_id UUID, p_outcome TEXT, p_reason_codes TEXT[],
    p_limit_snapshot JSONB, p_limit_hash TEXT, p_decided_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.target_portfolios AS target
        JOIN public.decision_snapshots AS decision ON decision.decision_id = target.decision_id
        WHERE target.target_portfolio_id = p_target_id
          AND decision.environment = p_environment
          AND decision.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'risk write lacks current execution authority'; END IF;
    INSERT INTO public.risk_decisions (
        risk_decision_id, target_portfolio_id, activation_permit_id, outcome,
        reason_codes, limit_snapshot, limit_snapshot_hash, decided_at
    ) VALUES (
        p_risk_id, p_target_id, p_permit_id, p_outcome,
        p_reason_codes, p_limit_snapshot, p_limit_hash, p_decided_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_order_plan_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_plan_id UUID, p_risk_id UUID,
    p_release_id UUID, p_symbol TEXT, p_side TEXT, p_quantity BIGINT,
    p_reference_price NUMERIC, p_evidence_hash TEXT, p_created_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.risk_decisions AS risk
        JOIN public.target_portfolios AS target ON target.target_portfolio_id = risk.target_portfolio_id
        JOIN public.decision_snapshots AS decision ON decision.decision_id = target.decision_id
        WHERE risk.risk_decision_id = p_risk_id
          AND decision.environment = p_environment
          AND decision.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'plan write lacks current execution authority'; END IF;
    INSERT INTO public.order_plans (
        order_plan_id, risk_decision_id, strategy_release_id, symbol, side,
        whole_quantity, decision_reference_price, decision_evidence_hash, created_at
    ) VALUES (
        p_plan_id, p_risk_id, p_release_id, p_symbol, p_side,
        p_quantity, p_reference_price, p_evidence_hash, p_created_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_order_intent_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_intent_id UUID, p_plan_id UUID, p_risk_id UUID,
    p_release_id UUID, p_client_order_id TEXT, p_symbol TEXT, p_side TEXT,
    p_quantity BIGINT, p_limit_price NUMERIC, p_time_in_force TEXT,
    p_decision_at TIMESTAMPTZ, p_arrival_quote NUMERIC,
    p_quote_provider_at TIMESTAMPTZ, p_quote_received_at TIMESTAMPTZ,
    p_quote_valid_until TIMESTAMPTZ, p_quote_payload_hash TEXT,
    p_decision_evidence_hash TEXT, p_materialization_evidence_hash TEXT,
    p_created_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.order_plans AS plan
        JOIN public.risk_decisions AS risk ON risk.risk_decision_id = plan.risk_decision_id
        JOIN public.target_portfolios AS target ON target.target_portfolio_id = risk.target_portfolio_id
        JOIN public.decision_snapshots AS decision ON decision.decision_id = target.decision_id
        WHERE plan.order_plan_id = p_plan_id
          AND decision.environment = p_environment
          AND decision.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'intent write lacks current execution authority'; END IF;
    INSERT INTO public.order_intents (
        intent_id, order_plan_id, risk_decision_id, strategy_release_id,
        environment, account_fingerprint, client_order_id, symbol, side,
        whole_quantity, order_type, limit_price, time_in_force, decision_at,
        arrival_quote, quote_provider_at, quote_received_at, quote_valid_until,
        quote_payload_hash, decision_evidence_hash, materialization_evidence_hash, created_at
    ) VALUES (
        p_intent_id, p_plan_id, p_risk_id, p_release_id,
        p_environment, p_account_fingerprint, p_client_order_id, p_symbol, p_side,
        p_quantity, 'limit', p_limit_price, p_time_in_force, p_decision_at,
        p_arrival_quote, p_quote_provider_at, p_quote_received_at, p_quote_valid_until,
        p_quote_payload_hash, p_decision_evidence_hash, p_materialization_evidence_hash, p_created_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_intent_state_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_state_event_id UUID, p_intent_id UUID,
    p_state TEXT, p_reason_code TEXT, p_detail JSONB, p_occurred_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.order_intents AS intent
        WHERE intent.intent_id = p_intent_id
          AND intent.environment = p_environment
          AND intent.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'intent-state write lacks current execution authority'; END IF;
    INSERT INTO public.intent_state_events (
        intent_state_event_id, intent_id, state, reason_code, detail,
        fencing_token, occurred_at
    ) VALUES (
        p_state_event_id, p_intent_id, p_state, p_reason_code, p_detail,
        p_fencing_token, p_occurred_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_order_outbox_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_outbox_id UUID, p_intent_id UUID,
    p_payload JSONB, p_available_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.order_intents AS intent
        WHERE intent.intent_id = p_intent_id
          AND intent.environment = p_environment
          AND intent.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'outbox write lacks current execution authority'; END IF;
    INSERT INTO public.order_outbox (
        outbox_id, intent_id, environment, account_fingerprint,
        created_fencing_token, payload, available_at
    ) VALUES (
        p_outbox_id, p_intent_id, p_environment, p_account_fingerprint,
        p_fencing_token, p_payload, p_available_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_broker_order_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_broker_order_id TEXT, p_intent_id UUID,
    p_client_order_id TEXT, p_first_seen_at TIMESTAMPTZ, p_raw_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.order_intents AS intent
        WHERE intent.intent_id = p_intent_id
          AND intent.environment = p_environment
          AND intent.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'broker-order write lacks current execution authority'; END IF;
    INSERT INTO public.broker_orders (
        broker_order_id, intent_id, client_order_id, environment,
        account_fingerprint, first_seen_at, raw_hash
    ) VALUES (
        p_broker_order_id, p_intent_id, p_client_order_id, p_environment,
        p_account_fingerprint, p_first_seen_at, p_raw_hash
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_fill_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_fill_id TEXT, p_broker_order_id TEXT,
    p_intent_id UUID, p_quantity NUMERIC, p_price NUMERIC, p_fee NUMERIC,
    p_executed_at TIMESTAMPTZ, p_received_at TIMESTAMPTZ, p_raw_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE v_inserted INTEGER;
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.broker_orders AS broker_order
        WHERE broker_order.broker_order_id = p_broker_order_id
          AND broker_order.intent_id = p_intent_id
          AND broker_order.environment = p_environment
          AND broker_order.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'fill write lacks current execution authority'; END IF;
    INSERT INTO public.fills (
        fill_id, broker_order_id, intent_id, symbol, side, quantity, price,
        fee, executed_at, received_at, raw_hash
    )
    SELECT p_fill_id, p_broker_order_id, intent.intent_id, intent.symbol,
           intent.side, p_quantity, p_price, p_fee,
           p_executed_at, p_received_at, p_raw_hash
    FROM public.order_intents AS intent WHERE intent.intent_id = p_intent_id;
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> 1 THEN RAISE EXCEPTION 'fill parent is absent'; END IF;
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_broker_event_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_broker_event_id UUID, p_broker_order_id TEXT,
    p_client_order_id TEXT, p_provider_status TEXT, p_recognized_status BOOLEAN,
    p_cumulative_quantity NUMERIC, p_average_fill_price NUMERIC,
    p_provider_occurred_at TIMESTAMPTZ, p_received_at TIMESTAMPTZ,
    p_request_id TEXT, p_raw_payload JSONB, p_raw_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.broker_orders AS broker_order
        WHERE broker_order.broker_order_id = p_broker_order_id
          AND broker_order.environment = p_environment
          AND broker_order.account_fingerprint = p_account_fingerprint
    ) THEN RAISE EXCEPTION 'broker-event write lacks current execution authority'; END IF;
    INSERT INTO public.broker_order_events (
        broker_event_id, broker_order_id, client_order_id, provider_status,
        recognized_status, cumulative_filled_quantity, average_fill_price,
        provider_occurred_at, received_at, x_request_id, raw_payload, raw_hash
    ) VALUES (
        p_broker_event_id, p_broker_order_id, p_client_order_id, p_provider_status,
        p_recognized_status, p_cumulative_quantity, p_average_fill_price,
        p_provider_occurred_at, p_received_at, p_request_id, p_raw_payload, p_raw_hash
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_account_snapshot_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_snapshot_id UUID, p_broker_timestamp TIMESTAMPTZ,
    p_received_at TIMESTAMPTZ, p_account_status TEXT, p_recognized_status BOOLEAN,
    p_cash NUMERIC, p_equity NUMERIC, p_buying_power NUMERIC,
    p_trading_blocked BOOLEAN, p_transfers_blocked BOOLEAN,
    p_account_blocked BOOLEAN, p_payload JSONB, p_payload_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN RAISE EXCEPTION 'account-snapshot write lacks current execution authority'; END IF;
    INSERT INTO public.account_snapshots (
        account_snapshot_id, environment, account_fingerprint, broker_timestamp,
        received_at, account_status, recognized_status, cash, equity, buying_power,
        trading_blocked, transfers_blocked, account_blocked, payload, payload_hash
    ) VALUES (
        p_snapshot_id, p_environment, p_account_fingerprint, p_broker_timestamp,
        p_received_at, p_account_status, p_recognized_status, p_cash, p_equity,
        p_buying_power, p_trading_blocked, p_transfers_blocked,
        p_account_blocked, p_payload, p_payload_hash
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_reconciliation_run_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_reconciliation_id UUID, p_trigger TEXT,
    p_kill_event_id UUID, p_started_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN RAISE EXCEPTION 'reconciliation write lacks current execution authority'; END IF;
    INSERT INTO public.reconciliation_runs (
        reconciliation_id, environment, account_fingerprint, trigger,
        kill_event_id, fencing_token, started_at
    ) VALUES (
        p_reconciliation_id, p_environment, p_account_fingerprint, p_trigger,
        p_kill_event_id, p_fencing_token, p_started_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_reconciliation_diff_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_diff_id UUID, p_reconciliation_id UUID,
    p_category TEXT, p_key TEXT, p_detail TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) OR NOT EXISTS (
        SELECT 1 FROM public.reconciliation_runs AS reconciliation
        WHERE reconciliation.reconciliation_id = p_reconciliation_id
          AND reconciliation.environment = p_environment
          AND reconciliation.account_fingerprint = p_account_fingerprint
          AND reconciliation.fencing_token = p_fencing_token
    ) THEN RAISE EXCEPTION 'reconciliation-diff write lacks current execution authority'; END IF;
    INSERT INTO public.reconciliation_diffs (
        reconciliation_diff_id, reconciliation_id, category, key,
        local_value, broker_value, resolution, detail
    ) VALUES (
        p_diff_id, p_reconciliation_id, p_category, p_key,
        NULL, NULL, 'unresolved', p_detail
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION finalize_reconciliation_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_reconciliation_id UUID, p_completed_at TIMESTAMPTZ,
    p_outcome TEXT, p_resumable BOOLEAN, p_account_snapshot_id UUID,
    p_evidence_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE v_updated INTEGER;
BEGIN
    IF NOT public.assert_current_executor_lease(
        p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
    ) THEN RAISE EXCEPTION 'reconciliation finalization lacks current authority'; END IF;
    UPDATE public.reconciliation_runs
    SET completed_at = p_completed_at, outcome = p_outcome,
        resumable = p_resumable, account_snapshot_id = p_account_snapshot_id,
        evidence_hash = p_evidence_hash
    WHERE reconciliation_id = p_reconciliation_id
      AND environment = p_environment
      AND account_fingerprint = p_account_fingerprint
      AND fencing_token = p_fencing_token
      AND completed_at IS NULL;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION 'reconciliation did not finalize exactly once'; END IF;
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION record_runtime_kill_event_v2(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_kill_event_id UUID, p_severity TEXT,
    p_reason_code TEXT, p_detail JSONB, p_actor TEXT, p_occurred_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF p_severity NOT IN ('soft', 'hard', 'liquidation')
       OR NOT public.assert_current_executor_lease(
           p_environment, p_account_fingerprint, p_owner_id, p_fencing_token
       ) THEN RETURN FALSE; END IF;
    INSERT INTO public.kill_events (
        kill_event_id, environment, account_fingerprint, severity, reason_code,
        detail, actor, operator_approved, approval_digest, occurred_at
    ) VALUES (
        p_kill_event_id, p_environment, p_account_fingerprint, p_severity,
        p_reason_code, COALESCE(p_detail, '{}'::jsonb), p_actor, FALSE, NULL, p_occurred_at
    );
    RETURN TRUE;
END;
$$;

CREATE TABLE runtime_schema_attestations (
    object_kind TEXT NOT NULL CHECK (object_kind IN ('function', 'view', 'trigger', 'constraint')),
    object_identity TEXT NOT NULL,
    definition_sha256 TEXT NOT NULL CHECK (definition_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (object_kind, object_identity)
);

CREATE TRIGGER runtime_schema_attestations_reject_mutation
BEFORE UPDATE OR DELETE ON runtime_schema_attestations
FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation();

INSERT INTO runtime_schema_attestations (object_kind, object_identity, definition_sha256)
SELECT
    'function', signature,
    encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(signature)), 'UTF8')), 'hex')
FROM (VALUES
    ('public.acquire_executor_lease(text,text,uuid,interval)'),
    ('public.renew_executor_lease(text,text,uuid,bigint,interval)'),
    ('public.assert_current_executor_lease(text,text,uuid,bigint)'),
    ('public.claim_order_outbox_v2(text,text,uuid,uuid,bigint)'),
    ('public.claim_order_outbox_recovery_v2(text,text,uuid,uuid,bigint)'),
    ('public.claim_order_outbox_completion_v2(text,text,uuid,uuid,bigint)'),
    ('public.append_submission_unknown_v2(text,text,uuid,uuid,bigint,uuid,text,jsonb,timestamp with time zone)'),
    ('public.finalize_order_outbox_v2(text,text,uuid,uuid,bigint,text)'),
    ('public.list_unresolved_order_outboxes_v2(text,text,uuid,bigint,integer)'),
    ('public.insert_decision_snapshot_v2(text,text,uuid,bigint,uuid,uuid,date,timestamp with time zone,text,text,jsonb)'),
    ('public.insert_target_portfolio_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text)'),
    ('public.insert_target_position_v2(text,text,uuid,bigint,uuid,text,bigint,numeric,text)'),
    ('public.insert_risk_decision_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text[],jsonb,text,timestamp with time zone)'),
    ('public.insert_order_plan_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text,bigint,numeric,text,timestamp with time zone)'),
    ('public.insert_order_intent_v2(text,text,uuid,bigint,uuid,uuid,uuid,uuid,text,text,text,bigint,numeric,text,timestamp with time zone,numeric,timestamp with time zone,timestamp with time zone,timestamp with time zone,text,text,text,timestamp with time zone)'),
    ('public.insert_intent_state_v2(text,text,uuid,bigint,uuid,uuid,text,text,jsonb,timestamp with time zone)'),
    ('public.insert_order_outbox_v2(text,text,uuid,bigint,uuid,uuid,jsonb,timestamp with time zone)'),
    ('public.insert_broker_order_v2(text,text,uuid,bigint,text,uuid,text,timestamp with time zone,text)'),
    ('public.insert_fill_v2(text,text,uuid,bigint,text,text,uuid,numeric,numeric,numeric,timestamp with time zone,timestamp with time zone,text)'),
    ('public.insert_broker_event_v2(text,text,uuid,bigint,uuid,text,text,text,boolean,numeric,numeric,timestamp with time zone,timestamp with time zone,text,jsonb,text)'),
    ('public.insert_account_snapshot_v2(text,text,uuid,bigint,uuid,timestamp with time zone,timestamp with time zone,text,boolean,numeric,numeric,numeric,boolean,boolean,boolean,jsonb,text)'),
    ('public.insert_reconciliation_run_v2(text,text,uuid,bigint,uuid,text,uuid,timestamp with time zone)'),
    ('public.insert_reconciliation_diff_v2(text,text,uuid,bigint,uuid,uuid,text,text,text)'),
    ('public.finalize_reconciliation_v2(text,text,uuid,bigint,uuid,timestamp with time zone,text,boolean,uuid,text)'),
    ('public.record_runtime_kill_event_v2(text,text,uuid,bigint,uuid,text,text,jsonb,text,timestamp with time zone)'),
    ('public.enforce_intent_state_transition()'),
    ('public.enforce_order_outbox_chain()'),
    ('public.enforce_broker_event_chain()'),
    ('public.enforce_broker_event_fill_truth()'),
    ('public.enforce_terminal_intent_fill_truth()'),
    ('public.enforce_fill_chain()'),
    ('public.enforce_reconciliation_report()'),
    ('public.enforce_target_portfolio_chain()'),
    ('public.enforce_risk_decision_chain()'),
    ('public.enforce_order_plan_chain()'),
    ('public.enforce_order_intent_chain()'),
    ('public.enforce_broker_order_chain()'),
    ('public.prevent_late_reconciliation_diff()'),
    ('public.reject_audit_mutation()')
) AS required_function(signature);

INSERT INTO runtime_schema_attestations (object_kind, object_identity, definition_sha256)
SELECT
    'view', 'public.execution_readiness',
    encode(sha256(convert_to(pg_get_viewdef('public.execution_readiness'::regclass, true), 'UTF8')), 'hex');

INSERT INTO runtime_schema_attestations (object_kind, object_identity, definition_sha256)
SELECT
    'trigger', table_name || '.' || trigger.tgname,
    encode(sha256(convert_to(pg_get_triggerdef(trigger.oid, true), 'UTF8')), 'hex')
FROM pg_trigger AS trigger
JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
CROSS JOIN LATERAL (SELECT namespace.nspname || '.' || relation.relname AS table_name) AS identity
WHERE namespace.nspname = 'public'
  AND relation.relname IN (
      'decision_snapshots', 'target_portfolios', 'target_positions',
      'risk_decisions', 'order_plans', 'order_intents', 'intent_state_events',
      'order_outbox', 'broker_orders', 'broker_order_events', 'fills',
      'account_snapshots', 'reconciliation_runs', 'reconciliation_diffs',
      'runtime_schema_attestations'
  )
  AND NOT trigger.tgisinternal;

INSERT INTO runtime_schema_attestations (object_kind, object_identity, definition_sha256)
SELECT
    'constraint', namespace.nspname || '.' || relation.relname || '.' || con.conname,
    encode(sha256(convert_to(pg_get_constraintdef(con.oid, true), 'UTF8')), 'hex')
FROM pg_constraint AS con
JOIN pg_class AS relation ON relation.oid = con.conrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND relation.relname IN (
      'decision_snapshots', 'target_portfolios', 'target_positions',
      'risk_decisions', 'order_plans', 'order_intents', 'intent_state_events',
      'order_outbox', 'broker_orders', 'broker_order_events', 'fills',
      'account_snapshots', 'reconciliation_runs', 'reconciliation_diffs',
      'runtime_schema_attestations'
  );

-- Reset the runtime role to a read-and-execute capability.  No table DML,
-- sequence capability, unsafe function, schema CREATE, database CREATE, or
-- temporary-object privilege survives directly or through PUBLIC.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM alpaca_trader_runtime;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO alpaca_trader_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM alpaca_trader_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM alpaca_trader_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM alpaca_trader_runtime;
GRANT USAGE ON SCHEMA public TO alpaca_trader_runtime;

DO $$
BEGIN
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format(
        'REVOKE CREATE, TEMPORARY ON DATABASE %I FROM alpaca_trader_runtime',
        current_database()
    );
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO alpaca_trader_runtime, alpaca_trader_operator',
        current_database()
    );
END;
$$;

GRANT EXECUTE ON FUNCTION
    acquire_executor_lease(TEXT, TEXT, UUID, INTERVAL),
    renew_executor_lease(TEXT, TEXT, UUID, BIGINT, INTERVAL),
    assert_current_executor_lease(TEXT, TEXT, UUID, BIGINT),
    claim_order_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_order_outbox_recovery_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    claim_order_outbox_completion_v2(TEXT, TEXT, UUID, UUID, BIGINT),
    append_submission_unknown_v2(TEXT, TEXT, UUID, UUID, BIGINT, UUID, TEXT, JSONB, TIMESTAMPTZ),
    finalize_order_outbox_v2(TEXT, TEXT, UUID, UUID, BIGINT, TEXT),
    list_unresolved_order_outboxes_v2(TEXT, TEXT, UUID, BIGINT, INTEGER),
    insert_decision_snapshot_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, DATE, TIMESTAMPTZ, TEXT, TEXT, JSONB),
    insert_target_portfolio_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, UUID, TEXT, TEXT),
    insert_target_position_v2(TEXT, TEXT, UUID, BIGINT, UUID, TEXT, BIGINT, NUMERIC, TEXT),
    insert_risk_decision_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, UUID, TEXT, TEXT[], JSONB, TEXT, TIMESTAMPTZ),
    insert_order_plan_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, UUID, TEXT, TEXT, BIGINT, NUMERIC, TEXT, TIMESTAMPTZ),
    insert_order_intent_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, UUID, UUID, TEXT, TEXT, TEXT, BIGINT, NUMERIC, TEXT, TIMESTAMPTZ, NUMERIC, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    insert_intent_state_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, TEXT, TEXT, JSONB, TIMESTAMPTZ),
    insert_order_outbox_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, JSONB, TIMESTAMPTZ),
    insert_broker_order_v2(TEXT, TEXT, UUID, BIGINT, TEXT, UUID, TEXT, TIMESTAMPTZ, TEXT),
    insert_fill_v2(TEXT, TEXT, UUID, BIGINT, TEXT, TEXT, UUID, NUMERIC, NUMERIC, NUMERIC, TIMESTAMPTZ, TIMESTAMPTZ, TEXT),
    insert_broker_event_v2(TEXT, TEXT, UUID, BIGINT, UUID, TEXT, TEXT, TEXT, BOOLEAN, NUMERIC, NUMERIC, TIMESTAMPTZ, TIMESTAMPTZ, TEXT, JSONB, TEXT),
    insert_account_snapshot_v2(TEXT, TEXT, UUID, BIGINT, UUID, TIMESTAMPTZ, TIMESTAMPTZ, TEXT, BOOLEAN, NUMERIC, NUMERIC, NUMERIC, BOOLEAN, BOOLEAN, BOOLEAN, JSONB, TEXT),
    insert_reconciliation_run_v2(TEXT, TEXT, UUID, BIGINT, UUID, TEXT, UUID, TIMESTAMPTZ),
    insert_reconciliation_diff_v2(TEXT, TEXT, UUID, BIGINT, UUID, UUID, TEXT, TEXT, TEXT),
    finalize_reconciliation_v2(TEXT, TEXT, UUID, BIGINT, UUID, TIMESTAMPTZ, TEXT, BOOLEAN, UUID, TEXT),
    record_runtime_kill_event_v2(TEXT, TEXT, UUID, BIGINT, UUID, TEXT, TEXT, JSONB, TEXT, TIMESTAMPTZ)
TO alpaca_trader_runtime;

COMMIT;
