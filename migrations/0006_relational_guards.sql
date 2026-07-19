BEGIN;

CREATE OR REPLACE FUNCTION enforce_activation_revocation_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.activation_permits AS permit
        WHERE permit.permit_id = NEW.permit_id
          AND NEW.revoked_at >= permit.issued_at
    ) THEN
        RAISE EXCEPTION 'activation permit revocation predates or lacks its permit';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER activation_permit_revocations_enforce_chain
BEFORE INSERT ON activation_permit_revocations
FOR EACH ROW EXECUTE FUNCTION enforce_activation_revocation_chain();

CREATE OR REPLACE FUNCTION enforce_target_portfolio_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.decision_snapshots AS decision
        WHERE decision.decision_id = NEW.decision_id
          AND decision.strategy_release_id = NEW.strategy_release_id
    ) THEN
        RAISE EXCEPTION 'target portfolio release does not match its decision snapshot';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER target_portfolios_enforce_chain
BEFORE INSERT ON target_portfolios
FOR EACH ROW EXECUTE FUNCTION enforce_target_portfolio_chain();

CREATE OR REPLACE FUNCTION enforce_intent_state_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_environment TEXT;
    v_account_fingerprint TEXT;
    v_previous_state TEXT;
    v_previous_sequence BIGINT;
    v_previous_occurred_at TIMESTAMPTZ;
BEGIN
    SELECT intent.environment, intent.account_fingerprint
    INTO v_environment, v_account_fingerprint
    FROM public.order_intents AS intent
    WHERE intent.intent_id = NEW.intent_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'intent state lacks a durable intent';
    END IF;
    IF NEW.fencing_token IS NULL OR NOT EXISTS (
        SELECT 1
        FROM public.executor_leases AS lease
        WHERE lease.environment = v_environment
          AND lease.account_fingerprint = v_account_fingerprint
          AND lease.fencing_token = NEW.fencing_token
          AND lease.lease_until >= clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'intent state lacks the current execution fence';
    END IF;
    IF NEW.occurred_at > clock_timestamp() + INTERVAL '5 seconds' THEN
        RAISE EXCEPTION 'intent state timestamp is unreasonably future-dated';
    END IF;

    SELECT state.state, state.event_sequence, state.occurred_at
    INTO v_previous_state, v_previous_sequence, v_previous_occurred_at
    FROM public.intent_state_events AS state
    WHERE state.intent_id = NEW.intent_id
    ORDER BY state.event_sequence DESC
    LIMIT 1;

    NEW.event_sequence := COALESCE(v_previous_sequence + 1, 1);
    IF v_previous_occurred_at IS NOT NULL AND NEW.occurred_at < v_previous_occurred_at THEN
        RAISE EXCEPTION 'intent state timestamps must be monotonic';
    END IF;
    IF v_previous_state IS NULL AND NEW.state <> 'persisted' THEN
        RAISE EXCEPTION 'first intent state must be persisted';
    ELSIF v_previous_state IS NOT NULL AND NOT (
        (v_previous_state = 'persisted' AND NEW.state IN ('eligible', 'blocked'))
        OR (v_previous_state = 'eligible' AND NEW.state IN ('dispatch_started', 'blocked'))
        OR (v_previous_state = 'dispatch_started' AND NEW.state IN (
            'acknowledged', 'submission_unknown', 'broker_confirmed', 'blocked'
        ))
        OR (v_previous_state = 'submission_unknown' AND NEW.state IN (
            'broker_confirmed', 'blocked'
        ))
        OR (v_previous_state = 'acknowledged' AND NEW.state IN (
            'broker_confirmed', 'terminal', 'blocked'
        ))
        OR (v_previous_state = 'broker_confirmed' AND NEW.state IN ('terminal', 'blocked'))
        OR (v_previous_state = 'blocked' AND NEW.state = 'broker_confirmed')
    ) THEN
        RAISE EXCEPTION 'invalid intent state transition from % to %',
            v_previous_state, NEW.state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER intent_state_events_enforce_transition
BEFORE INSERT ON intent_state_events
FOR EACH ROW EXECUTE FUNCTION enforce_intent_state_transition();

CREATE OR REPLACE FUNCTION enforce_risk_decision_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_environment TEXT;
    v_account_fingerprint TEXT;
    v_release_id UUID;
BEGIN
    SELECT
        decision.environment,
        decision.account_fingerprint,
        decision.strategy_release_id
    INTO v_environment, v_account_fingerprint, v_release_id
    FROM public.target_portfolios AS target
    JOIN public.decision_snapshots AS decision
      ON decision.decision_id = target.decision_id
    WHERE target.target_portfolio_id = NEW.target_portfolio_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'risk decision lacks a complete target and decision chain';
    END IF;

    IF NEW.outcome IN ('approved', 'reduced')
       AND v_environment IN ('paper', 'live')
       AND NOT EXISTS (
           SELECT 1
           FROM public.activation_permits AS permit
           LEFT JOIN public.activation_permit_revocations AS revocation
             ON revocation.permit_id = permit.permit_id
           WHERE permit.permit_id = NEW.activation_permit_id
             AND permit.environment = v_environment
             AND permit.account_fingerprint = v_account_fingerprint
             AND permit.strategy_release_id = v_release_id
             AND permit.risk_limits_hash = NEW.limit_snapshot_hash
             AND revocation.permit_id IS NULL
       )
    THEN
        RAISE EXCEPTION 'approved paper/live risk decision lacks matching permit authority';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER risk_decisions_enforce_chain
BEFORE INSERT ON risk_decisions
FOR EACH ROW EXECUTE FUNCTION enforce_risk_decision_chain();

CREATE OR REPLACE FUNCTION enforce_order_plan_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.risk_decisions AS risk
        JOIN public.target_portfolios AS target
          ON target.target_portfolio_id = risk.target_portfolio_id
        WHERE risk.risk_decision_id = NEW.risk_decision_id
          AND risk.outcome IN ('approved', 'reduced')
          AND target.strategy_release_id = NEW.strategy_release_id
    ) THEN
        RAISE EXCEPTION 'order plan does not match an approved risk decision';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER order_plans_enforce_chain
BEFORE INSERT ON order_plans
FOR EACH ROW EXECUTE FUNCTION enforce_order_plan_chain();

CREATE OR REPLACE FUNCTION enforce_order_intent_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_environment TEXT;
    v_account_fingerprint TEXT;
    v_release_id UUID;
    v_as_of TIMESTAMPTZ;
    v_outcome TEXT;
    v_permit_id UUID;
BEGIN
    SELECT
        decision.environment,
        decision.account_fingerprint,
        decision.strategy_release_id,
        decision.as_of,
        risk.outcome,
        risk.activation_permit_id
    INTO
        v_environment,
        v_account_fingerprint,
        v_release_id,
        v_as_of,
        v_outcome,
        v_permit_id
    FROM public.risk_decisions AS risk
    JOIN public.target_portfolios AS target
      ON target.target_portfolio_id = risk.target_portfolio_id
    JOIN public.decision_snapshots AS decision
      ON decision.decision_id = target.decision_id
    WHERE risk.risk_decision_id = NEW.risk_decision_id;

    IF NOT FOUND
       OR v_outcome NOT IN ('approved', 'reduced')
       OR NEW.strategy_release_id <> v_release_id
       OR NEW.environment <> v_environment
       OR NEW.account_fingerprint IS DISTINCT FROM v_account_fingerprint
       OR NEW.decision_at <> v_as_of
    THEN
        RAISE EXCEPTION 'order intent does not match its approved decision authority chain';
    END IF;

    IF NEW.environment IN ('paper', 'live')
       AND NOT EXISTS (
           SELECT 1
           FROM public.activation_permits AS permit
           LEFT JOIN public.activation_permit_revocations AS revocation
             ON revocation.permit_id = permit.permit_id
           WHERE permit.permit_id = v_permit_id
             AND permit.environment = NEW.environment
             AND permit.account_fingerprint = NEW.account_fingerprint
             AND permit.strategy_release_id = NEW.strategy_release_id
             AND permit.issued_at <= NEW.decision_at
             AND permit.expires_at > NEW.decision_at
             AND revocation.permit_id IS NULL
       )
    THEN
        RAISE EXCEPTION 'order intent lacks valid activation permit at decision time';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM public.order_plans AS plan
        WHERE plan.order_plan_id = NEW.order_plan_id
          AND plan.risk_decision_id = NEW.risk_decision_id
          AND plan.strategy_release_id = NEW.strategy_release_id
          AND plan.symbol = NEW.symbol
          AND plan.side = NEW.side
          AND plan.whole_quantity = NEW.whole_quantity
          AND plan.decision_evidence_hash = NEW.decision_evidence_hash
          AND plan.created_at = NEW.decision_at
    ) THEN
        RAISE EXCEPTION 'materialized order intent differs from its approved order plan';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER order_intents_enforce_chain
BEFORE INSERT ON order_intents
FOR EACH ROW EXECUTE FUNCTION enforce_order_intent_chain();

CREATE OR REPLACE FUNCTION enforce_order_outbox_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.order_intents AS intent
        JOIN public.executor_leases AS lease
          ON lease.environment = intent.environment
         AND lease.account_fingerprint = intent.account_fingerprint
        WHERE intent.intent_id = NEW.intent_id
          AND intent.environment = NEW.environment
          AND intent.account_fingerprint = NEW.account_fingerprint
          AND NEW.environment IN ('paper', 'live')
          AND lease.fencing_token = NEW.created_fencing_token
          AND lease.lease_until >= clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'outbox item does not match intent and active creation fence';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER order_outbox_enforce_chain
BEFORE INSERT ON order_outbox
FOR EACH ROW EXECUTE FUNCTION enforce_order_outbox_chain();

CREATE OR REPLACE FUNCTION enforce_broker_order_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.order_intents AS intent
        WHERE intent.intent_id = NEW.intent_id
          AND intent.client_order_id = NEW.client_order_id
          AND intent.environment = NEW.environment
          AND intent.account_fingerprint = NEW.account_fingerprint
    ) THEN
        RAISE EXCEPTION 'broker order does not match its durable intent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER broker_orders_enforce_chain
BEFORE INSERT ON broker_orders
FOR EACH ROW EXECUTE FUNCTION enforce_broker_order_chain();

CREATE OR REPLACE FUNCTION enforce_broker_event_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_order_quantity BIGINT;
    v_previous_sequence BIGINT;
    v_previous_status TEXT;
    v_previous_recognized BOOLEAN;
    v_previous_filled NUMERIC(38, 6);
    v_expected_recognized BOOLEAN;
BEGIN
    SELECT intent.whole_quantity
    INTO v_order_quantity
    FROM public.broker_orders AS broker_order
    JOIN public.order_intents AS intent
      ON intent.intent_id = broker_order.intent_id
    WHERE broker_order.broker_order_id = NEW.broker_order_id
      AND broker_order.client_order_id = NEW.client_order_id
    FOR UPDATE OF broker_order;

    IF NOT FOUND OR NEW.cumulative_filled_quantity > v_order_quantity THEN
        RAISE EXCEPTION 'broker event does not match order identity or fill bounds';
    END IF;

    SELECT
        event.event_sequence,
        event.provider_status,
        event.recognized_status,
        event.cumulative_filled_quantity
    INTO
        v_previous_sequence,
        v_previous_status,
        v_previous_recognized,
        v_previous_filled
    FROM public.broker_order_events AS event
    WHERE event.broker_order_id = NEW.broker_order_id
    ORDER BY event.event_sequence DESC
    LIMIT 1;

    NEW.event_sequence := COALESCE(v_previous_sequence + 1, 1);
    v_expected_recognized := NEW.provider_status IN (
        'accepted', 'new', 'pending_new', 'accepted_for_bidding',
        'partially_filled', 'filled', 'done_for_day', 'canceled', 'expired',
        'replaced', 'pending_cancel', 'pending_replace', 'stopped', 'rejected',
        'suspended', 'calculated'
    );
    IF NEW.recognized_status IS DISTINCT FROM v_expected_recognized THEN
        RAISE EXCEPTION 'recognized_status does not match the provider-status allowlist';
    END IF;
    IF NEW.received_at > clock_timestamp() + INTERVAL '5 seconds' THEN
        RAISE EXCEPTION 'broker event receive timestamp is unreasonably future-dated';
    END IF;
    IF v_previous_filled IS NOT NULL
       AND NEW.cumulative_filled_quantity < v_previous_filled
    THEN
        RAISE EXCEPTION 'broker cumulative fill quantity regressed';
    END IF;
    IF v_previous_recognized IS FALSE THEN
        RAISE EXCEPTION 'events after an unknown provider status require reconciliation and review';
    END IF;
    IF v_previous_status IN (
        'filled', 'done_for_day', 'canceled', 'expired', 'replaced',
        'stopped', 'rejected', 'suspended', 'calculated'
    ) THEN
        RAISE EXCEPTION 'new non-duplicate broker event followed a terminal status';
    END IF;
    IF v_previous_status = 'partially_filled'
       AND NEW.provider_status IN ('accepted', 'new', 'pending_new', 'accepted_for_bidding')
    THEN
        RAISE EXCEPTION 'broker order status regressed after a partial fill';
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
    IF NEW.cumulative_filled_quantity > 0 AND NEW.average_fill_price IS NULL THEN
        RAISE EXCEPTION 'filled quantity requires an average fill price';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER broker_order_events_enforce_chain
BEFORE INSERT ON broker_order_events
FOR EACH ROW EXECUTE FUNCTION enforce_broker_event_chain();

CREATE OR REPLACE FUNCTION enforce_experiment_event_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_registered_at TIMESTAMPTZ;
    v_previous_status TEXT;
    v_previous_sequence BIGINT;
BEGIN
    SELECT experiment.registered_at
    INTO v_registered_at
    FROM public.experiments AS experiment
    WHERE experiment.experiment_id = NEW.experiment_id
    FOR UPDATE;

    IF NOT FOUND OR NEW.occurred_at < v_registered_at
       OR NEW.occurred_at > clock_timestamp() + INTERVAL '5 seconds'
    THEN
        RAISE EXCEPTION 'experiment event has invalid parent or timestamp';
    END IF;

    SELECT event.status, event.event_sequence
    INTO v_previous_status, v_previous_sequence
    FROM public.experiment_events AS event
    WHERE event.experiment_id = NEW.experiment_id
    ORDER BY event.event_sequence DESC
    LIMIT 1;

    NEW.event_sequence := COALESCE(v_previous_sequence + 1, 1);
    IF v_previous_status IS NULL AND NEW.status <> 'registered' THEN
        RAISE EXCEPTION 'first experiment event must be registered';
    ELSIF v_previous_status = 'registered'
          AND NEW.status NOT IN ('running', 'failed', 'abandoned')
    THEN
        RAISE EXCEPTION 'invalid experiment transition from registered';
    ELSIF v_previous_status = 'running'
          AND NEW.status NOT IN ('completed', 'failed', 'abandoned')
    THEN
        RAISE EXCEPTION 'invalid experiment transition from running';
    ELSIF v_previous_status IN ('completed', 'failed', 'abandoned') THEN
        RAISE EXCEPTION 'terminal experiment cannot append another state';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER experiment_events_enforce_transition
BEFORE INSERT ON experiment_events
FOR EACH ROW EXECUTE FUNCTION enforce_experiment_event_transition();

CREATE OR REPLACE FUNCTION prevent_late_reconciliation_diff()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_completed_at TIMESTAMPTZ;
BEGIN
    -- Serialize difference appends with completion of the parent run. The row
    -- lock is held through the inserting transaction, so completion either
    -- observes this difference or the insert observes a completed run.
    SELECT reconciliation.completed_at
    INTO v_completed_at
    FROM public.reconciliation_runs AS reconciliation
    WHERE reconciliation.reconciliation_id = NEW.reconciliation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'reconciliation parent does not exist';
    END IF;
    IF v_completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'cannot append a difference after reconciliation completion';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reconciliation_diffs_prevent_late_append
BEFORE INSERT ON reconciliation_diffs
FOR EACH ROW EXECUTE FUNCTION prevent_late_reconciliation_diff();

CREATE OR REPLACE FUNCTION enforce_fill_chain()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_order_quantity BIGINT;
    v_prior_quantity NUMERIC(38, 6);
BEGIN
    SELECT intent.whole_quantity
    INTO v_order_quantity
    FROM public.broker_orders AS broker_order
    JOIN public.order_intents AS intent
      ON intent.intent_id = broker_order.intent_id
    WHERE broker_order.broker_order_id = NEW.broker_order_id
      AND broker_order.intent_id = NEW.intent_id
      AND intent.symbol = NEW.symbol
      AND intent.side = NEW.side
    FOR UPDATE OF broker_order;

    IF NOT FOUND OR NEW.quantity <> trunc(NEW.quantity) THEN
        RAISE EXCEPTION 'fill does not match its whole-share intent';
    END IF;

    SELECT COALESCE(SUM(fill.quantity), 0)
    INTO v_prior_quantity
    FROM public.fills AS fill
    WHERE fill.intent_id = NEW.intent_id;

    IF v_prior_quantity + NEW.quantity > v_order_quantity THEN
        RAISE EXCEPTION 'cumulative fills exceed the durable intent quantity';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER fills_enforce_chain
BEFORE INSERT ON fills
FOR EACH ROW EXECUTE FUNCTION enforce_fill_chain();

CREATE OR REPLACE FUNCTION enforce_reconciliation_report()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'reconciliation must be inserted open and finalized once';
    END IF;
    IF NEW.started_at > clock_timestamp() + INTERVAL '5 seconds'
       OR (NEW.completed_at IS NOT NULL AND (
           NEW.completed_at < NEW.started_at
           OR NEW.completed_at > clock_timestamp() + INTERVAL '5 seconds'
       ))
    THEN
        RAISE EXCEPTION 'reconciliation timestamps are invalid';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        -- UPDATE already takes a row lock; make the serialization contract
        -- explicit beside the matching lock in the difference trigger.
        PERFORM 1
        FROM public.reconciliation_runs AS reconciliation
        WHERE reconciliation.reconciliation_id = OLD.reconciliation_id
        FOR UPDATE;
    END IF;
    IF NEW.outcome = 'clean' AND EXISTS (
        SELECT 1
        FROM public.reconciliation_diffs AS difference
        WHERE difference.reconciliation_id = NEW.reconciliation_id
    ) THEN
        RAISE EXCEPTION 'clean reconciliation cannot contain differences';
    END IF;
    IF NEW.resumable AND (
        NEW.completed_at IS NULL
        OR NEW.outcome NOT IN ('clean', 'resolved')
        OR NEW.account_snapshot_id IS NULL
        OR EXISTS (
            SELECT 1
            FROM public.reconciliation_diffs AS difference
            WHERE difference.reconciliation_id = NEW.reconciliation_id
              AND difference.resolution IN ('unresolved', 'escalated')
        )
    ) THEN
        RAISE EXCEPTION 'resumable reconciliation lacks complete resolved evidence';
    END IF;
    IF NEW.account_snapshot_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM public.account_snapshots AS account
        WHERE account.account_snapshot_id = NEW.account_snapshot_id
          AND account.environment = NEW.environment
          AND account.account_fingerprint = NEW.account_fingerprint
          AND account.received_at >= NEW.started_at
          AND account.received_at <= clock_timestamp() + INTERVAL '5 seconds'
    ) THEN
        RAISE EXCEPTION 'reconciliation account snapshot is stale or from another authority domain';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM public.kill_events AS kill
        JOIN public.executor_leases AS lease
          ON lease.environment = kill.environment
         AND lease.account_fingerprint = kill.account_fingerprint
        WHERE kill.kill_event_id = NEW.kill_event_id
          AND kill.environment = NEW.environment
          AND kill.account_fingerprint = NEW.account_fingerprint
          AND lease.fencing_token = NEW.fencing_token
    ) THEN
        RAISE EXCEPTION 'reconciliation does not bind current account authority evidence';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reconciliation_runs_enforce_report
BEFORE INSERT OR UPDATE ON reconciliation_runs
FOR EACH ROW EXECUTE FUNCTION enforce_reconciliation_report();

REVOKE ALL ON FUNCTION
    enforce_activation_revocation_chain(),
    enforce_target_portfolio_chain(),
    enforce_risk_decision_chain(),
    enforce_order_plan_chain(),
    enforce_order_intent_chain(),
    enforce_order_outbox_chain(),
    enforce_broker_order_chain(),
    enforce_broker_event_chain(),
    enforce_intent_state_transition(),
    enforce_experiment_event_transition(),
    prevent_late_reconciliation_diff(),
    enforce_fill_chain(),
    enforce_reconciliation_report()
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator;

COMMIT;
