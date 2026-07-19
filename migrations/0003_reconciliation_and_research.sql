BEGIN;

CREATE TABLE account_snapshots (
    account_snapshot_id UUID PRIMARY KEY,
    environment TEXT NOT NULL CHECK (environment IN ('shadow', 'paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    broker_timestamp TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    account_status TEXT NOT NULL,
    recognized_status BOOLEAN NOT NULL,
    cash NUMERIC(38, 6) NOT NULL,
    equity NUMERIC(38, 6) NOT NULL CHECK (equity >= 0),
    buying_power NUMERIC(38, 6) NOT NULL CHECK (buying_power >= 0),
    trading_blocked BOOLEAN NOT NULL,
    transfers_blocked BOOLEAN NOT NULL,
    account_blocked BOOLEAN NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX account_snapshots_latest_idx
    ON account_snapshots (environment, account_fingerprint, received_at DESC);

CREATE TABLE reconciliation_runs (
    reconciliation_id UUID PRIMARY KEY,
    authority_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    environment TEXT NOT NULL CHECK (environment IN ('shadow', 'paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN (
        'startup', 'reconnect', 'session_open', 'session_close',
        'ambiguous_submission', 'manual', 'restore', 'failover'
    )),
    kill_event_id UUID NOT NULL REFERENCES kill_events(kill_event_id),
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('clean', 'resolved', 'blocked', 'failed')),
    resumable BOOLEAN NOT NULL DEFAULT FALSE,
    account_snapshot_id UUID REFERENCES account_snapshots(account_snapshot_id),
    evidence_hash TEXT CHECK (evidence_hash IS NULL OR evidence_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((completed_at IS NULL AND outcome IS NULL)
        OR (completed_at IS NOT NULL AND outcome IS NOT NULL))
);

CREATE TABLE reconciliation_diffs (
    reconciliation_diff_id UUID PRIMARY KEY,
    reconciliation_id UUID NOT NULL REFERENCES reconciliation_runs(reconciliation_id),
    category TEXT NOT NULL CHECK (category IN ('cash', 'position', 'order', 'fill', 'account', 'protection')),
    key TEXT NOT NULL,
    local_value JSONB,
    broker_value JSONB,
    resolution TEXT NOT NULL CHECK (resolution IN ('unresolved', 'accepted_broker', 'accepted_local', 'corrected', 'escalated')),
    detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX reconciliation_diffs_run_idx
    ON reconciliation_diffs (reconciliation_id, category, key);

CREATE TABLE experiments (
    experiment_id UUID PRIMARY KEY,
    family_id UUID NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    specification_hash TEXT NOT NULL CHECK (specification_hash ~ '^[0-9a-f]{64}$'),
    code_hash TEXT NOT NULL CHECK (code_hash ~ '^[0-9a-f]{64}$'),
    data_hash TEXT NOT NULL CHECK (data_hash ~ '^[0-9a-f]{64}$'),
    configuration JSONB NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('development', 'validation', 'holdout', 'shadow', 'live')),
    registered_at TIMESTAMPTZ NOT NULL,
    UNIQUE (family_id, sequence_number)
);

CREATE TABLE experiment_events (
    experiment_event_id UUID PRIMARY KEY,
    experiment_id UUID NOT NULL REFERENCES experiments(experiment_id),
    event_sequence BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('registered', 'running', 'completed', 'failed', 'abandoned')),
    result_hash TEXT CHECK (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'),
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status = 'completed' AND result_hash IS NOT NULL)
        OR (status <> 'completed')),
    UNIQUE (experiment_id, event_sequence)
);

CREATE INDEX experiment_events_timeline_idx
    ON experiment_events (experiment_id, occurred_at, experiment_event_id);

CREATE TABLE data_artifacts (
    artifact_id UUID PRIMARY KEY,
    logical_name TEXT NOT NULL,
    version TEXT NOT NULL,
    source TEXT NOT NULL,
    feed TEXT NOT NULL,
    adjustment_mode TEXT NOT NULL CHECK (adjustment_mode IN ('raw', 'split', 'dividend', 'all')),
    as_of TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    object_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    request_id TEXT,
    metadata JSONB NOT NULL,
    quarantined BOOLEAN NOT NULL DEFAULT FALSE,
    quarantine_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (logical_name, version),
    CHECK (available_at >= as_of),
    CHECK ((quarantined AND quarantine_reason IS NOT NULL)
        OR (NOT quarantined AND quarantine_reason IS NULL))
);

CREATE TABLE incident_events (
    incident_event_id UUID PRIMARY KEY,
    incident_id UUID NOT NULL,
    environment TEXT NOT NULL CHECK (environment IN ('shadow', 'paper', 'live')),
    account_fingerprint TEXT,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    detail JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX incident_events_timeline_idx
    ON incident_events (incident_id, occurred_at, incident_event_id);

CREATE OR REPLACE FUNCTION reject_audit_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit table % is append-only', TG_TABLE_NAME;
END;
$$;

DO $$
DECLARE
    v_table TEXT;
BEGIN
    FOREACH v_table IN ARRAY ARRAY[
        'strategy_releases',
        'activation_permits',
        'activation_permit_revocations',
        'kill_events',
        'decision_snapshots',
        'target_portfolios',
        'target_positions',
        'risk_decisions',
        'order_plans',
        'order_intents',
        'intent_state_events',
        'broker_orders',
        'broker_order_events',
        'fills',
        'account_snapshots',
        'reconciliation_diffs',
        'experiments',
        'experiment_events',
        'data_artifacts',
        'incident_events'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER %I_reject_mutation BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation()',
            v_table,
            v_table
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION protect_order_outbox_authority()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.outbox_id <> OLD.outbox_id
        OR NEW.intent_id <> OLD.intent_id
        OR NEW.environment <> OLD.environment
        OR NEW.account_fingerprint <> OLD.account_fingerprint
        OR NEW.created_fencing_token <> OLD.created_fencing_token
        OR NEW.payload <> OLD.payload
        OR NEW.created_at <> OLD.created_at
    THEN
        RAISE EXCEPTION 'order_outbox authority and payload fields are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER order_outbox_protect_authority
BEFORE UPDATE ON order_outbox
FOR EACH ROW EXECUTE FUNCTION protect_order_outbox_authority();

CREATE OR REPLACE FUNCTION protect_reconciliation_completion()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.reconciliation_id <> OLD.reconciliation_id
        OR NEW.environment <> OLD.environment
        OR NEW.account_fingerprint <> OLD.account_fingerprint
        OR NEW.trigger <> OLD.trigger
        OR NEW.started_at <> OLD.started_at
        OR NEW.created_at <> OLD.created_at
    THEN
        RAISE EXCEPTION 'reconciliation identity fields are immutable';
    END IF;
    IF OLD.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'completed reconciliation reports are immutable';
    END IF;
    IF NEW.completed_at IS NULL OR NEW.outcome IS NULL THEN
        RAISE EXCEPTION 'reconciliation may only update through one final completion';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reconciliation_runs_protect_completion
BEFORE UPDATE ON reconciliation_runs
FOR EACH ROW EXECUTE FUNCTION protect_reconciliation_completion();

COMMIT;
