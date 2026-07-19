BEGIN;

CREATE TABLE decision_snapshots (
    decision_id UUID PRIMARY KEY,
    strategy_release_id UUID NOT NULL REFERENCES strategy_releases(release_id),
    environment TEXT NOT NULL CHECK (environment IN ('backtest', 'shadow', 'paper', 'live')),
    account_fingerprint TEXT,
    market_session DATE NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    input_data_hash TEXT NOT NULL CHECK (input_data_hash ~ '^[0-9a-f]{64}$'),
    account_snapshot_hash TEXT NOT NULL CHECK (account_snapshot_hash ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((environment = 'backtest' AND account_fingerprint IS NULL)
        OR (environment <> 'backtest' AND account_fingerprint IS NOT NULL))
);

CREATE TABLE target_portfolios (
    target_portfolio_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL UNIQUE REFERENCES decision_snapshots(decision_id),
    strategy_release_id UUID NOT NULL REFERENCES strategy_releases(release_id),
    reason_code TEXT NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE target_positions (
    target_portfolio_id UUID NOT NULL REFERENCES target_portfolios(target_portfolio_id),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,14}$'),
    target_quantity BIGINT NOT NULL CHECK (target_quantity >= 0),
    target_weight NUMERIC(20, 6) NOT NULL CHECK (target_weight >= 0 AND target_weight <= 1),
    reason_code TEXT NOT NULL,
    PRIMARY KEY (target_portfolio_id, symbol)
);

CREATE TABLE risk_decisions (
    risk_decision_id UUID PRIMARY KEY,
    target_portfolio_id UUID NOT NULL UNIQUE REFERENCES target_portfolios(target_portfolio_id),
    activation_permit_id UUID REFERENCES activation_permits(permit_id),
    outcome TEXT NOT NULL CHECK (outcome IN ('approved', 'reduced', 'rejected', 'halted')),
    reason_codes TEXT[] NOT NULL,
    limit_snapshot JSONB NOT NULL,
    limit_snapshot_hash TEXT NOT NULL CHECK (limit_snapshot_hash ~ '^[0-9a-f]{64}$'),
    decided_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE order_plans (
    order_plan_id UUID PRIMARY KEY,
    risk_decision_id UUID NOT NULL REFERENCES risk_decisions(risk_decision_id),
    strategy_release_id UUID NOT NULL REFERENCES strategy_releases(release_id),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,14}$'),
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    whole_quantity BIGINT NOT NULL CHECK (whole_quantity > 0),
    decision_reference_price NUMERIC(38, 6) NOT NULL CHECK (decision_reference_price > 0),
    decision_evidence_hash TEXT NOT NULL CHECK (decision_evidence_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE order_intents (
    intent_id UUID PRIMARY KEY,
    order_plan_id UUID NOT NULL UNIQUE REFERENCES order_plans(order_plan_id),
    risk_decision_id UUID NOT NULL REFERENCES risk_decisions(risk_decision_id),
    strategy_release_id UUID NOT NULL REFERENCES strategy_releases(release_id),
    environment TEXT NOT NULL CHECK (environment IN ('paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    client_order_id TEXT NOT NULL CHECK (length(client_order_id) BETWEEN 1 AND 128),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,14}$'),
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    whole_quantity BIGINT NOT NULL CHECK (whole_quantity > 0),
    order_type TEXT NOT NULL CHECK (order_type = 'limit'),
    limit_price NUMERIC(38, 6) NOT NULL CHECK (limit_price > 0),
    time_in_force TEXT NOT NULL CHECK (time_in_force = 'day'),
    decision_at TIMESTAMPTZ NOT NULL,
    arrival_quote NUMERIC(38, 6) NOT NULL CHECK (arrival_quote > 0),
    quote_provider_at TIMESTAMPTZ NOT NULL,
    quote_received_at TIMESTAMPTZ NOT NULL,
    quote_valid_until TIMESTAMPTZ NOT NULL,
    quote_payload_hash TEXT NOT NULL CHECK (quote_payload_hash ~ '^[0-9a-f]{64}$'),
    decision_evidence_hash TEXT NOT NULL CHECK (decision_evidence_hash ~ '^[0-9a-f]{64}$'),
    materialization_evidence_hash TEXT NOT NULL
        CHECK (materialization_evidence_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (environment, account_fingerprint, client_order_id),
    CHECK (quote_provider_at > decision_at),
    CHECK (quote_received_at >= quote_provider_at),
    CHECK (quote_valid_until > quote_received_at),
    CHECK (quote_valid_until <= quote_received_at + INTERVAL '15 seconds'),
    CHECK ((side = 'buy' AND limit_price >= arrival_quote)
        OR (side = 'sell' AND limit_price <= arrival_quote))
);

CREATE TABLE intent_state_events (
    intent_state_event_id UUID PRIMARY KEY,
    intent_id UUID NOT NULL REFERENCES order_intents(intent_id),
    event_sequence BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'persisted', 'eligible', 'dispatch_started', 'acknowledged',
        'submission_unknown', 'broker_confirmed', 'terminal', 'blocked'
    )),
    reason_code TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    fencing_token BIGINT,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (intent_id, event_sequence),
    CHECK (length(trim(reason_code)) > 0)
);

CREATE INDEX intent_state_events_replay_idx
    ON intent_state_events (intent_id, occurred_at, intent_state_event_id);

CREATE TABLE order_outbox (
    outbox_id UUID PRIMARY KEY,
    intent_id UUID NOT NULL UNIQUE REFERENCES order_intents(intent_id),
    environment TEXT NOT NULL CHECK (environment IN ('paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    created_fencing_token BIGINT NOT NULL CHECK (created_fencing_token > 0),
    claim_fencing_token BIGINT CHECK (claim_fencing_token > 0),
    payload JSONB NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    claimed_by UUID,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    completion_reason TEXT,
    last_error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((claimed_by IS NULL AND claimed_at IS NULL)
        OR (claimed_by IS NOT NULL AND claimed_at IS NOT NULL)),
    CHECK ((completed_at IS NULL AND completion_reason IS NULL)
        OR (completed_at IS NOT NULL AND completion_reason IS NOT NULL))
);

CREATE INDEX order_outbox_ready_idx
    ON order_outbox (available_at, outbox_id)
    WHERE completed_at IS NULL;

CREATE OR REPLACE FUNCTION claim_order_outbox(
    p_outbox_id UUID,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS SETOF order_outbox
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    UPDATE order_outbox AS o
    SET claimed_by = p_owner_id,
        claimed_at = clock_timestamp(),
        claim_fencing_token = p_fencing_token,
        attempt_count = o.attempt_count + 1,
        last_error = NULL
    FROM executor_leases AS l
    WHERE o.outbox_id = p_outbox_id
      AND o.completed_at IS NULL
      AND o.available_at <= clock_timestamp()
      AND l.environment = o.environment
      AND l.account_fingerprint = o.account_fingerprint
      AND l.owner_id = p_owner_id
      AND l.fencing_token = p_fencing_token
      AND l.lease_until >= clock_timestamp()
      AND (o.claimed_at IS NULL OR o.claimed_at < clock_timestamp() - INTERVAL '30 seconds')
    RETURNING o.*;
END;
$$;

CREATE TABLE broker_orders (
    broker_order_id TEXT PRIMARY KEY,
    intent_id UUID NOT NULL REFERENCES order_intents(intent_id),
    client_order_id TEXT NOT NULL,
    environment TEXT NOT NULL CHECK (environment IN ('paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    raw_hash TEXT NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (environment, account_fingerprint, client_order_id)
);

CREATE TABLE broker_order_events (
    broker_event_id UUID PRIMARY KEY,
    broker_order_id TEXT NOT NULL REFERENCES broker_orders(broker_order_id),
    event_sequence BIGINT NOT NULL,
    client_order_id TEXT NOT NULL,
    provider_status TEXT NOT NULL,
    recognized_status BOOLEAN NOT NULL,
    cumulative_filled_quantity NUMERIC(38, 6) NOT NULL CHECK (
        cumulative_filled_quantity >= 0
        AND cumulative_filled_quantity = trunc(cumulative_filled_quantity)
    ),
    average_fill_price NUMERIC(38, 6) CHECK (
        average_fill_price IS NULL OR average_fill_price > 0
    ),
    provider_occurred_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    x_request_id TEXT,
    raw_payload JSONB NOT NULL,
    raw_hash TEXT NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (broker_order_id, event_sequence),
    UNIQUE (broker_order_id, raw_hash)
);

CREATE INDEX broker_order_events_replay_idx
    ON broker_order_events (broker_order_id, received_at, broker_event_id);

CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    broker_order_id TEXT NOT NULL REFERENCES broker_orders(broker_order_id),
    intent_id UUID NOT NULL REFERENCES order_intents(intent_id),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity NUMERIC(38, 6) NOT NULL CHECK (quantity > 0),
    price NUMERIC(38, 6) NOT NULL CHECK (price > 0),
    fee NUMERIC(38, 6) NOT NULL DEFAULT 0 CHECK (fee >= 0),
    executed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    raw_hash TEXT NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX fills_intent_idx ON fills (intent_id, executed_at, fill_id);

COMMIT;
