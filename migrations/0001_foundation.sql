BEGIN;

CREATE TABLE strategy_releases (
    release_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    release_hash TEXT NOT NULL CHECK (release_hash ~ '^[0-9a-f]{64}$'),
    code_hash TEXT NOT NULL CHECK (code_hash ~ '^[0-9a-f]{64}$'),
    parameters_hash TEXT NOT NULL CHECK (parameters_hash ~ '^[0-9a-f]{64}$'),
    universe_hash TEXT NOT NULL CHECK (universe_hash ~ '^[0-9a-f]{64}$'),
    data_hash TEXT NOT NULL CHECK (data_hash ~ '^[0-9a-f]{64}$'),
    cost_model_hash TEXT NOT NULL CHECK (cost_model_hash ~ '^[0-9a-f]{64}$'),
    certificate_hash TEXT NOT NULL CHECK (certificate_hash ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('candidate', 'certified', 'rejected', 'expired')),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (valid_until > valid_from),
    UNIQUE (name, version),
    UNIQUE (release_id, release_hash)
);

CREATE TABLE activation_permits (
    permit_id UUID PRIMARY KEY,
    environment TEXT NOT NULL CHECK (environment IN ('shadow', 'paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    strategy_release_id UUID NOT NULL,
    strategy_release_hash TEXT NOT NULL CHECK (strategy_release_hash ~ '^[0-9a-f]{64}$'),
    max_gross_notional NUMERIC(38, 6) NOT NULL CHECK (max_gross_notional > 0),
    max_position_notional NUMERIC(38, 6) NOT NULL CHECK (max_position_notional > 0),
    max_daily_loss NUMERIC(38, 6) NOT NULL CHECK (max_daily_loss > 0),
    max_drawdown NUMERIC(38, 6) NOT NULL CHECK (max_drawdown > 0),
    risk_limits_hash TEXT NOT NULL CHECK (risk_limits_hash ~ '^[0-9a-f]{64}$'),
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    operator_subject TEXT NOT NULL,
    approval_digest TEXT NOT NULL CHECK (approval_digest ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > issued_at),
    CHECK (max_position_notional <= max_gross_notional),
    FOREIGN KEY (strategy_release_id, strategy_release_hash)
        REFERENCES strategy_releases(release_id, release_hash)
);

CREATE TABLE activation_permit_revocations (
    revocation_id UUID PRIMARY KEY,
    permit_id UUID NOT NULL UNIQUE REFERENCES activation_permits(permit_id),
    revoked_at TIMESTAMPTZ NOT NULL,
    operator_subject TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    approval_digest TEXT NOT NULL CHECK (approval_digest ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE kill_events (
    kill_event_id UUID PRIMARY KEY,
    authority_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    environment TEXT NOT NULL CHECK (environment IN ('shadow', 'paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('clear', 'soft', 'hard', 'liquidation')),
    reason_code TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor TEXT NOT NULL,
    operator_approved BOOLEAN NOT NULL DEFAULT FALSE,
    approval_digest TEXT CHECK (approval_digest IS NULL OR approval_digest ~ '^[0-9a-f]{64}$'),
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        severity <> 'clear'
        OR (operator_approved AND approval_digest IS NOT NULL)
    )
);

CREATE INDEX kill_events_current_idx
    ON kill_events (environment, account_fingerprint, occurred_at DESC, kill_event_id DESC);

CREATE VIEW current_kill_state AS
SELECT DISTINCT ON (environment, account_fingerprint)
    kill_event_id,
    authority_sequence,
    environment,
    account_fingerprint,
    severity,
    reason_code,
    detail,
    actor,
    occurred_at,
    created_at
FROM kill_events
ORDER BY environment, account_fingerprint, authority_sequence DESC;

CREATE TABLE executor_leases (
    environment TEXT NOT NULL CHECK (environment IN ('shadow', 'paper', 'live')),
    account_fingerprint TEXT NOT NULL,
    owner_id UUID NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    acquired_at TIMESTAMPTZ NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (environment, account_fingerprint),
    CHECK (lease_until > renewed_at)
);

CREATE OR REPLACE FUNCTION acquire_executor_lease(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_ttl INTERVAL
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_token BIGINT;
BEGIN
    IF p_environment NOT IN ('shadow', 'paper', 'live') THEN
        RAISE EXCEPTION 'invalid execution environment';
    END IF;
    IF p_ttl <= INTERVAL '0 seconds' OR p_ttl > INTERVAL '60 seconds' THEN
        RAISE EXCEPTION 'lease TTL must be in (0, 60] seconds';
    END IF;

    INSERT INTO executor_leases (
        environment, account_fingerprint, owner_id, fencing_token,
        acquired_at, renewed_at, lease_until
    ) VALUES (
        p_environment, p_account_fingerprint, p_owner_id, 1,
        clock_timestamp(), clock_timestamp(), clock_timestamp() + p_ttl
    )
    ON CONFLICT (environment, account_fingerprint) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        fencing_token = executor_leases.fencing_token + 1,
        acquired_at = clock_timestamp(),
        renewed_at = clock_timestamp(),
        lease_until = clock_timestamp() + p_ttl
    WHERE executor_leases.lease_until < clock_timestamp()
       OR executor_leases.owner_id = p_owner_id
    RETURNING fencing_token INTO v_token;

    RETURN v_token;
END;
$$;

CREATE OR REPLACE FUNCTION renew_executor_lease(
    p_environment TEXT,
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_ttl INTERVAL
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    IF p_ttl <= INTERVAL '0 seconds' OR p_ttl > INTERVAL '60 seconds' THEN
        RETURN FALSE;
    END IF;

    UPDATE executor_leases
    SET renewed_at = clock_timestamp(),
        lease_until = clock_timestamp() + p_ttl
    WHERE environment = p_environment
      AND account_fingerprint = p_account_fingerprint
      AND owner_id = p_owner_id
      AND fencing_token = p_fencing_token
      AND lease_until >= clock_timestamp();

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows = 1;
END;
$$;

COMMIT;
