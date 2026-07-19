BEGIN;

-- The paper observer is a distinct trust domain. It can elect one read-only
-- observer and append non-authorizing evidence, but it cannot inherit any of
-- the execution/runtime or operator capabilities.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
        WHERE rolname = 'alpaca_trader_observer'
    ) THEN
        CREATE ROLE alpaca_trader_observer
            NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOREPLICATION NOBYPASSRLS;
    END IF;
END;
$$;

CREATE TABLE paper_observer_leases (
    account_fingerprint TEXT PRIMARY KEY
        CHECK (account_fingerprint ~ '^[0-9a-f]{64}$'),
    owner_id UUID NOT NULL
        CHECK (owner_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    acquired_at TIMESTAMPTZ NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    CHECK (lease_until > renewed_at),
    CHECK (renewed_at >= acquired_at)
);

-- A cycle row is the durable start marker. A cycle without a completion is
-- interrupted evidence, never implicit success.
CREATE TABLE paper_observer_cycles (
    observer_cycle_id UUID PRIMARY KEY,
    environment TEXT NOT NULL DEFAULT 'paper' CHECK (environment = 'paper'),
    account_fingerprint TEXT NOT NULL
        CHECK (account_fingerprint ~ '^[0-9a-f]{64}$'),
    owner_id UUID NOT NULL
        CHECK (owner_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    trigger TEXT NOT NULL CHECK (trigger IN ('startup', 'periodic', 'reconnect')),
    mode TEXT NOT NULL DEFAULT 'reconcile_only' CHECK (mode = 'reconcile_only'),
    started_at TIMESTAMPTZ NOT NULL,
    identity_hash TEXT NOT NULL CHECK (identity_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        account_fingerprint, owner_id, fencing_token, started_at, trigger
    )
);

CREATE INDEX paper_observer_cycles_account_time_idx
    ON paper_observer_cycles (account_fingerprint, started_at DESC);

-- V1 deliberately has no available local cash or stable REST FILL identity.
-- A later accounting migration must replace these fail-closed constraints
-- only after independent transfers/dividends/fees/corporate-action evidence
-- and exact Alpaca FILL activity identity exist.
CREATE TABLE paper_observer_local_projections (
    projection_id UUID PRIMARY KEY,
    observer_cycle_id UUID NOT NULL UNIQUE
        REFERENCES paper_observer_cycles(observer_cycle_id),
    observed_at TIMESTAMPTZ NOT NULL,
    cash_basis_status TEXT NOT NULL CHECK (cash_basis_status = 'missing'),
    accounting_cash NUMERIC(38, 6) CHECK (accounting_cash IS NULL),
    cash_basis_evidence_hash TEXT CHECK (cash_basis_evidence_hash IS NULL),
    fill_identity_status TEXT NOT NULL CHECK (fill_identity_status = 'missing'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    domain_hash TEXT NOT NULL CHECK (domain_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (payload ->> 'schema' = 'wasp2/paper-observer-local-projection/v1'),
    CHECK (payload ->> 'environment' = 'paper'),
    CHECK (payload #>> '{cash_basis,status}' = 'missing'),
    CHECK (NOT ((payload #> '{cash_basis}') ? 'amount')),
    CHECK (NOT ((payload #> '{cash_basis}') ? 'evidence_hash')),
    CHECK (payload #>> '{fill_identity_basis,status}' = 'missing'),
    CHECK (payload #>> '{order_identity_basis,status}' = 'missing')
);

CREATE TABLE paper_observer_pages (
    page_evidence_id UUID PRIMARY KEY,
    observer_cycle_id UUID NOT NULL
        REFERENCES paper_observer_cycles(observer_cycle_id),
    snapshot_round SMALLINT NOT NULL CHECK (snapshot_round IN (1, 2)),
    source TEXT NOT NULL CHECK (source IN (
        'account', 'positions', 'open_orders', 'closed_orders',
        'fill_activities'
    )),
    page_ordinal INTEGER NOT NULL CHECK (page_ordinal >= 0),
    request_parameters_hash TEXT NOT NULL
        CHECK (request_parameters_hash ~ '^[0-9a-f]{64}$'),
    request_id TEXT CHECK (
        request_id IS NULL
        OR octet_length(trim(request_id)) BETWEEN 1 AND 128
    ),
    raw_payload_hash TEXT NOT NULL CHECK (raw_payload_hash ~ '^[0-9a-f]{64}$'),
    received_at TIMESTAMPTZ NOT NULL,
    item_count INTEGER NOT NULL CHECK (item_count >= 0),
    completion_witness TEXT CHECK (completion_witness IN (
        'single', 'short_page', 'timestamp_horizon_crossed'
    )),
    evidence_hash TEXT NOT NULL CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (observer_cycle_id, snapshot_round, source, page_ordinal),
    CHECK (
        (source IN ('account', 'positions')
         AND page_ordinal = 0
         AND completion_witness = 'single')
        OR (source = 'open_orders'
            AND completion_witness IS DISTINCT FROM 'single'
            AND completion_witness IS DISTINCT FROM 'timestamp_horizon_crossed')
        OR (source = 'closed_orders'
            AND completion_witness IS DISTINCT FROM 'single')
        OR (source = 'fill_activities'
            AND completion_witness IS DISTINCT FROM 'single'
            AND completion_witness IS DISTINCT FROM 'timestamp_horizon_crossed')
    )
);

CREATE INDEX paper_observer_pages_manifest_idx
    ON paper_observer_pages (
        observer_cycle_id, snapshot_round, source, page_ordinal
    );

CREATE TABLE paper_observer_differences (
    observer_difference_id UUID PRIMARY KEY,
    observer_cycle_id UUID NOT NULL
        REFERENCES paper_observer_cycles(observer_cycle_id),
    difference_ordinal INTEGER NOT NULL CHECK (difference_ordinal >= 0),
    category TEXT NOT NULL CHECK (category IN (
        'policy', 'projection', 'cash', 'position', 'order', 'fill',
        'account', 'snapshot', 'data'
    )),
    kind TEXT NOT NULL CHECK (kind IN (
        'missing_locally', 'missing_at_broker', 'quantity_mismatch',
        'cash_mismatch', 'status_mismatch', 'unknown_provider_state'
    )),
    subject TEXT NOT NULL
        CHECK (octet_length(trim(subject)) BETWEEN 1 AND 256),
    local_value JSONB,
    broker_value JSONB,
    detail TEXT NOT NULL
        CHECK (octet_length(detail) BETWEEN 1 AND 512),
    detail_code TEXT NOT NULL
        CHECK (octet_length(trim(detail_code)) BETWEEN 1 AND 128),
    evidence_hash TEXT NOT NULL CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (observer_cycle_id, difference_ordinal),
    CHECK (
        subject <> 'independent_cash_basis'
        OR (kind = 'missing_locally' AND local_value IS NULL)
    ),
    CHECK (
        subject <> 'stable_rest_fill_identity'
        OR (kind = 'missing_locally' AND local_value IS NULL)
    )
);

CREATE TABLE paper_observer_completions (
    observer_completion_id UUID PRIMARY KEY,
    observer_cycle_id UUID NOT NULL UNIQUE
        REFERENCES paper_observer_cycles(observer_cycle_id),
    outcome TEXT NOT NULL CHECK (outcome IN ('blocked', 'failed')),
    failure_stage TEXT CHECK (failure_stage IN (
        'configuration', 'local_projection', 'broker_round_1',
        'broker_round_2', 'fence_renewal', 'database_connection',
        'persistence', 'shutdown'
    )),
    reason_codes TEXT[] NOT NULL
        CHECK (cardinality(reason_codes) BETWEEN 1 AND 32),
    local_projection_hash TEXT CHECK (
        local_projection_hash IS NULL
        OR local_projection_hash ~ '^[0-9a-f]{64}$'
    ),
    broker_snapshot_hashes TEXT[] NOT NULL
        CHECK (cardinality(broker_snapshot_hashes) BETWEEN 0 AND 2),
    page_count INTEGER NOT NULL CHECK (page_count >= 0),
    page_manifest_hash TEXT NOT NULL
        CHECK (page_manifest_hash ~ '^[0-9a-f]{64}$'),
    difference_count INTEGER NOT NULL CHECK (difference_count >= 0),
    difference_manifest_hash TEXT NOT NULL
        CHECK (difference_manifest_hash ~ '^[0-9a-f]{64}$'),
    result_payload JSONB NOT NULL CHECK (jsonb_typeof(result_payload) = 'object'),
    result_hash TEXT NOT NULL CHECK (result_hash ~ '^[0-9a-f]{64}$'),
    completed_fencing_token BIGINT NOT NULL CHECK (completed_fencing_token > 0),
    completed_at TIMESTAMPTZ NOT NULL,
    execution_authorizing BOOLEAN GENERATED ALWAYS AS (FALSE) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (result_payload ->> 'environment' = 'paper'),
    CHECK (result_payload ->> 'mode' = 'reconcile_only'),
    CHECK (result_payload -> 'resumable' = 'false'::jsonb),
    CHECK (result_payload ->> 'outcome' = outcome),
    CHECK (
        (outcome = 'blocked'
         AND failure_stage IS NULL
         AND local_projection_hash IS NOT NULL
         AND cardinality(broker_snapshot_hashes) = 2
         AND reason_codes @> ARRAY['read_only_policy']::text[]
         AND reason_codes @> ARRAY['independent_cash_basis_missing']::text[]
         AND reason_codes @> ARRAY['stable_rest_fill_identity_missing']::text[]
         AND reason_codes @> ARRAY['canonical_local_order_truth_missing']::text[])
        OR (outcome = 'failed' AND failure_stage IS NOT NULL)
    )
);

CREATE TABLE paper_observer_schema_attestations (
    object_kind TEXT NOT NULL CHECK (object_kind IN (
        'function', 'view', 'relation', 'trigger', 'constraint'
    )),
    object_identity TEXT NOT NULL,
    definition_sha256 TEXT NOT NULL
        CHECK (definition_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (object_kind, object_identity)
);

-- Every observer-evidence table is append-only. The lease is the only mutable
-- observer table and is inaccessible except through the two lease wrappers.
DO $$
DECLARE
    v_table TEXT;
BEGIN
    FOREACH v_table IN ARRAY ARRAY[
        'paper_observer_cycles',
        'paper_observer_local_projections',
        'paper_observer_pages',
        'paper_observer_differences',
        'paper_observer_completions',
        'paper_observer_schema_attestations'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER %I_reject_mutation '
            'BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION public.reject_audit_mutation()',
            v_table,
            v_table
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_paper_observer_child_append()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_started_at TIMESTAMPTZ;
    v_observed_at TIMESTAMPTZ;
BEGIN
    SELECT cycle.started_at
    INTO v_started_at
    FROM public.paper_observer_cycles AS cycle
    WHERE cycle.observer_cycle_id = NEW.observer_cycle_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer child has no cycle';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.paper_observer_completions AS completion
        WHERE completion.observer_cycle_id = NEW.observer_cycle_id
    ) THEN
        RAISE EXCEPTION 'paper observer cycle is already terminal';
    END IF;

    v_observed_at := COALESCE(
        (to_jsonb(NEW) ->> 'observed_at')::timestamptz,
        (to_jsonb(NEW) ->> 'received_at')::timestamptz
    );
    IF v_observed_at IS NULL
       OR v_observed_at < v_started_at
       OR v_observed_at > clock_timestamp() + INTERVAL '5 seconds'
    THEN
        RAISE EXCEPTION 'paper observer child timestamp is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER paper_observer_local_projections_enforce_open_cycle
BEFORE INSERT ON paper_observer_local_projections
FOR EACH ROW EXECUTE FUNCTION enforce_paper_observer_child_append();

CREATE TRIGGER paper_observer_pages_enforce_open_cycle
BEFORE INSERT ON paper_observer_pages
FOR EACH ROW EXECUTE FUNCTION enforce_paper_observer_child_append();

CREATE TRIGGER paper_observer_differences_enforce_open_cycle
BEFORE INSERT ON paper_observer_differences
FOR EACH ROW EXECUTE FUNCTION enforce_paper_observer_child_append();

CREATE OR REPLACE FUNCTION enforce_paper_observer_completion()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_started_at TIMESTAMPTZ;
    v_account_fingerprint TEXT;
    v_fencing_token BIGINT;
    v_page_count INTEGER;
    v_page_manifest TEXT;
    v_difference_count INTEGER;
    v_difference_manifest TEXT;
BEGIN
    SELECT cycle.started_at, cycle.account_fingerprint, cycle.fencing_token
    INTO v_started_at, v_account_fingerprint, v_fencing_token
    FROM public.paper_observer_cycles AS cycle
    WHERE cycle.observer_cycle_id = NEW.observer_cycle_id
    FOR UPDATE;

    IF NOT FOUND
       OR NEW.completed_at < v_started_at
       OR NEW.completed_at > clock_timestamp() + INTERVAL '5 seconds'
    THEN
        RAISE EXCEPTION 'paper observer completion timestamp or parent is invalid';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(NEW.reason_codes) AS reason(code)
        WHERE reason.code IS NULL
           OR reason.code !~ '^[a-z][a-z0-9_]{0,127}$'
    ) OR EXISTS (
        SELECT 1
        FROM unnest(NEW.broker_snapshot_hashes) AS snapshot(hash)
        WHERE snapshot.hash IS NULL OR snapshot.hash !~ '^[0-9a-f]{64}$'
    ) THEN
        RAISE EXCEPTION 'paper observer completion contains invalid reason or snapshot hash';
    END IF;
    IF (NEW.result_payload ->> 'local_evidence_hash')
       IS DISTINCT FROM NEW.local_projection_hash
    THEN
        RAISE EXCEPTION 'paper observer result is not bound to its local projection';
    END IF;
    IF (NEW.result_payload ->> 'cycle_id')::uuid <> NEW.observer_cycle_id
       OR (NEW.result_payload ->> 'generated_at')::timestamptz <> NEW.completed_at
       OR (NEW.result_payload ->> 'failure_stage') IS DISTINCT FROM NEW.failure_stage
       OR ARRAY(
           SELECT reason.value
           FROM jsonb_array_elements_text(NEW.result_payload -> 'reasons')
                WITH ORDINALITY AS reason(value, ordinal)
           ORDER BY reason.ordinal
       ) IS DISTINCT FROM NEW.reason_codes
       OR ARRAY(
           SELECT snapshot.value
           FROM jsonb_array_elements_text(
               NEW.result_payload -> 'broker_evidence_hashes'
           ) WITH ORDINALITY AS snapshot(value, ordinal)
           ORDER BY snapshot.ordinal
       ) IS DISTINCT FROM NEW.broker_snapshot_hashes
       OR jsonb_typeof(NEW.result_payload -> 'normalized_broker_snapshots') <> 'array'
       OR jsonb_array_length(
           NEW.result_payload -> 'normalized_broker_snapshots'
       ) <> cardinality(NEW.broker_snapshot_hashes)
       OR jsonb_typeof(NEW.result_payload -> 'source_page_evidence') <> 'array'
       OR jsonb_array_length(NEW.result_payload -> 'source_page_evidence')
          <> NEW.page_count
       OR (NEW.outcome = 'failed'
           AND (cardinality(NEW.broker_snapshot_hashes) <> 0
                OR NEW.page_count <> 0))
    THEN
        RAISE EXCEPTION 'paper observer result columns disagree with its payload';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(NEW.result_payload -> 'source_page_evidence')
             AS result_page(value)
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.paper_observer_pages AS page
            WHERE page.observer_cycle_id = NEW.observer_cycle_id
              AND page.snapshot_round = (result_page.value ->> 'snapshot_round')::smallint
              AND page.source = result_page.value ->> 'kind'
              AND page.page_ordinal = (result_page.value ->> 'page_ordinal')::integer
              AND page.request_parameters_hash = result_page.value ->> 'request_parameters_hash'
              AND page.request_id IS NOT DISTINCT FROM (result_page.value ->> 'request_id')
              AND page.raw_payload_hash = result_page.value ->> 'raw_payload_hash'
              AND page.received_at = (result_page.value ->> 'received_at')::timestamptz
              AND page.item_count = (result_page.value ->> 'item_count')::integer
              AND page.completion_witness IS NOT DISTINCT FROM
                  (result_page.value ->> 'completion_witness')
              AND page.evidence_hash = result_page.value ->> 'evidence_hash'
        )
    ) THEN
        RAISE EXCEPTION 'paper observer page rows disagree with result payload';
    END IF;
    IF NEW.result_payload -> 'reconciliation' = 'null'::jsonb THEN
        IF NEW.difference_count <> 0 THEN
            RAISE EXCEPTION 'paper observer failure hides reconciliation differences';
        END IF;
    ELSE
        IF (NEW.result_payload #>> '{reconciliation,generated_at}')::timestamptz
              <> NEW.completed_at
           OR NEW.result_payload #>> '{reconciliation,account_fingerprint}'
              <> v_account_fingerprint
           OR (NEW.result_payload #>> '{reconciliation,execution_fencing_token}')::bigint
              <> v_fencing_token
           OR (NEW.result_payload #>> '{reconciliation,may_resume_execution}')::boolean
           OR jsonb_typeof(NEW.result_payload #> '{reconciliation,differences}') <> 'array'
           OR jsonb_array_length(NEW.result_payload #> '{reconciliation,differences}')
              <> NEW.difference_count
        THEN
            RAISE EXCEPTION 'paper observer reconciliation header disagrees with result';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM jsonb_array_elements(
                NEW.result_payload #> '{reconciliation,differences}'
            ) WITH ORDINALITY AS result_difference(value, ordinal)
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.paper_observer_differences AS difference
                WHERE difference.observer_cycle_id = NEW.observer_cycle_id
                  AND difference.difference_ordinal = result_difference.ordinal - 1
                  AND difference.kind = result_difference.value ->> 'kind'
                  AND difference.subject = result_difference.value ->> 'subject'
                  AND difference.local_value IS NOT DISTINCT FROM NULLIF(
                      result_difference.value -> 'local_value', 'null'::jsonb
                  )
                  AND difference.broker_value IS NOT DISTINCT FROM NULLIF(
                      result_difference.value -> 'broker_value', 'null'::jsonb
                  )
                  AND difference.detail = result_difference.value ->> 'detail'
            )
        ) THEN
            RAISE EXCEPTION 'paper observer difference rows disagree with result payload';
        END IF;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.paper_observer_local_projections AS projection
        WHERE projection.observer_cycle_id = NEW.observer_cycle_id
          AND projection.observed_at > NEW.completed_at
    ) OR EXISTS (
        SELECT 1 FROM public.paper_observer_pages AS page
        WHERE page.observer_cycle_id = NEW.observer_cycle_id
          AND page.received_at > NEW.completed_at
    ) OR EXISTS (
        SELECT 1 FROM public.paper_observer_differences AS difference
        WHERE difference.observer_cycle_id = NEW.observer_cycle_id
          AND difference.observed_at > NEW.completed_at
    ) THEN
        RAISE EXCEPTION 'paper observer completion precedes child evidence';
    END IF;

    SELECT
        COUNT(*)::integer,
        encode(sha256(convert_to(COALESCE(string_agg(
            page.evidence_hash,
            E'\n' ORDER BY
                page.snapshot_round,
                CASE page.source
                    WHEN 'account' THEN 1
                    WHEN 'positions' THEN 2
                    WHEN 'open_orders' THEN 3
                    WHEN 'closed_orders' THEN 4
                    WHEN 'fill_activities' THEN 5
                END,
                page.page_ordinal
        ), ''), 'UTF8')), 'hex')
    INTO v_page_count, v_page_manifest
    FROM public.paper_observer_pages AS page
    WHERE page.observer_cycle_id = NEW.observer_cycle_id;

    SELECT
        COUNT(*)::integer,
        encode(sha256(convert_to(COALESCE(string_agg(
            difference.evidence_hash,
            E'\n' ORDER BY difference.difference_ordinal
        ), ''), 'UTF8')), 'hex')
    INTO v_difference_count, v_difference_manifest
    FROM public.paper_observer_differences AS difference
    WHERE difference.observer_cycle_id = NEW.observer_cycle_id;

    IF NEW.page_count <> v_page_count
       OR NEW.page_manifest_hash <> v_page_manifest
       OR NEW.difference_count <> v_difference_count
       OR NEW.difference_manifest_hash <> v_difference_manifest
    THEN
        RAISE EXCEPTION 'paper observer completion manifest does not match children';
    END IF;

    IF NEW.outcome = 'blocked' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM public.paper_observer_local_projections AS projection
            WHERE projection.observer_cycle_id = NEW.observer_cycle_id
              AND projection.domain_hash = NEW.local_projection_hash
              AND projection.cash_basis_status = 'missing'
              AND projection.accounting_cash IS NULL
              AND projection.fill_identity_status = 'missing'
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.paper_observer_differences AS difference
            WHERE difference.observer_cycle_id = NEW.observer_cycle_id
              AND difference.subject = 'independent_cash_basis'
              AND difference.kind = 'missing_locally'
              AND difference.local_value IS NULL
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.paper_observer_differences AS difference
            WHERE difference.observer_cycle_id = NEW.observer_cycle_id
              AND difference.subject = 'stable_rest_fill_identity'
              AND difference.kind = 'missing_locally'
              AND difference.local_value IS NULL
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.paper_observer_differences AS difference
            WHERE difference.observer_cycle_id = NEW.observer_cycle_id
              AND difference.subject = 'canonical_local_order_truth'
              AND difference.kind = 'missing_locally'
              AND difference.local_value IS NULL
        ) THEN
            RAISE EXCEPTION 'blocked paper observer result hides missing local evidence';
        END IF;

        -- Every source in both rounds must have a contiguous, explicitly
        -- completed page sequence. Account and positions are single pages.
        IF EXISTS (
            WITH expected(snapshot_round, source) AS (
                VALUES
                    (1::smallint, 'account'::text),
                    (1::smallint, 'positions'::text),
                    (1::smallint, 'open_orders'::text),
                    (1::smallint, 'closed_orders'::text),
                    (1::smallint, 'fill_activities'::text),
                    (2::smallint, 'account'::text),
                    (2::smallint, 'positions'::text),
                    (2::smallint, 'open_orders'::text),
                    (2::smallint, 'closed_orders'::text),
                    (2::smallint, 'fill_activities'::text)
            ), summary AS (
                SELECT
                    expected.snapshot_round,
                    expected.source,
                    COUNT(page.*)::integer AS page_count,
                    MIN(page.page_ordinal) AS first_ordinal,
                    MAX(page.page_ordinal) AS last_ordinal,
                    COUNT(*) FILTER (
                        WHERE page.completion_witness IS NOT NULL
                    )::integer AS witness_count,
                    MAX(page.completion_witness) FILTER (
                        WHERE page.page_ordinal = (
                            SELECT MAX(last_page.page_ordinal)
                            FROM public.paper_observer_pages AS last_page
                            WHERE last_page.observer_cycle_id = NEW.observer_cycle_id
                              AND last_page.snapshot_round = expected.snapshot_round
                              AND last_page.source = expected.source
                        )
                    ) AS final_witness
                FROM expected
                LEFT JOIN public.paper_observer_pages AS page
                  ON page.observer_cycle_id = NEW.observer_cycle_id
                 AND page.snapshot_round = expected.snapshot_round
                 AND page.source = expected.source
                GROUP BY expected.snapshot_round, expected.source
            )
            SELECT 1
            FROM summary
            WHERE page_count = 0
               OR first_ordinal <> 0
               OR page_count <> last_ordinal + 1
               OR witness_count <> 1
               OR (source IN ('account', 'positions')
                   AND (page_count <> 1 OR final_witness <> 'single'))
               OR (source = 'open_orders' AND final_witness <> 'short_page')
               OR (source = 'closed_orders'
                   AND final_witness NOT IN (
                       'short_page', 'timestamp_horizon_crossed'
                   ))
               OR (source = 'fill_activities' AND final_witness <> 'short_page')
        ) THEN
            RAISE EXCEPTION 'blocked paper observer result lacks complete page evidence';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER paper_observer_completions_enforce_evidence
BEFORE INSERT ON paper_observer_completions
FOR EACH ROW EXECUTE FUNCTION enforce_paper_observer_completion();

CREATE OR REPLACE FUNCTION acquire_paper_observer_lease_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_ttl INTERVAL
) RETURNS TABLE (fencing_token BIGINT, lease_until TIMESTAMPTZ)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_account_fingerprint !~ '^[0-9a-f]{64}$'
       OR p_owner_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR p_ttl <= INTERVAL '0 seconds'
       OR p_ttl > INTERVAL '60 seconds'
    THEN
        RAISE EXCEPTION 'invalid paper observer lease request';
    END IF;

    RETURN QUERY
    INSERT INTO public.paper_observer_leases AS lease (
        account_fingerprint, owner_id, fencing_token,
        acquired_at, renewed_at, lease_until
    ) VALUES (
        p_account_fingerprint, p_owner_id, 1,
        clock_timestamp(), clock_timestamp(), clock_timestamp() + p_ttl
    )
    ON CONFLICT (account_fingerprint) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        fencing_token = lease.fencing_token + 1,
        acquired_at = clock_timestamp(),
        renewed_at = clock_timestamp(),
        lease_until = clock_timestamp() + p_ttl
    WHERE lease.lease_until < clock_timestamp()
       OR lease.owner_id = p_owner_id
    RETURNING lease.fencing_token, lease.lease_until;
END;
$$;

CREATE OR REPLACE FUNCTION renew_paper_observer_lease_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_ttl INTERVAL
) RETURNS TABLE (fencing_token BIGINT, lease_until TIMESTAMPTZ)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_account_fingerprint !~ '^[0-9a-f]{64}$'
       OR p_fencing_token <= 0
       OR p_ttl <= INTERVAL '0 seconds'
       OR p_ttl > INTERVAL '60 seconds'
    THEN
        RETURN;
    END IF;

    RETURN QUERY
    UPDATE public.paper_observer_leases AS lease
    SET renewed_at = clock_timestamp(),
        lease_until = clock_timestamp() + p_ttl
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
    RETURNING lease.fencing_token, lease.lease_until;
END;
$$;

CREATE OR REPLACE FUNCTION read_paper_observer_projection_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT
) RETURNS TABLE (observed_at TIMESTAMPTZ, payload JSONB)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_observed_at TIMESTAMPTZ;
    v_payload JSONB;
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
    ;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer projection lacks current fence';
    END IF;

    SELECT jsonb_build_object(
        'schema', 'wasp2/paper-observer-local-projection/v1',
        'environment', 'paper',
        'account_fingerprint', p_account_fingerprint,
        'cash_basis', jsonb_build_object('status', 'missing'),
        'fill_identity_basis', jsonb_build_object('status', 'missing'),
        'order_identity_basis', jsonb_build_object('status', 'missing'),
        'positions_basis', 'durable_fill_ledger_partial',
        'positions', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'symbol', position.symbol,
                'quantity', position.quantity::text
            ) ORDER BY position.symbol)
            FROM public.broker_position_quantities AS position
            WHERE position.environment = 'paper'
              AND position.account_fingerprint = p_account_fingerprint
        ), '[]'::jsonb),
        'orders', '[]'::jsonb,
        'order_ledger_facts', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'intent_id', intent.intent_id,
                'client_order_id', intent.client_order_id,
                'provider_order_id', broker_order.broker_order_id,
                'symbol', intent.symbol,
                'side', intent.side,
                'whole_quantity', intent.whole_quantity,
                'limit_price', intent.limit_price::text,
                'time_in_force', intent.time_in_force,
                'intent_state', intent_state.state,
                'provider_status', broker_state.provider_status,
                'recognized_status', broker_state.recognized_status,
                'cumulative_filled_quantity',
                    broker_state.cumulative_filled_quantity::text
            ) ORDER BY intent.client_order_id)
            FROM public.order_intents AS intent
            LEFT JOIN public.current_intent_states AS intent_state
              ON intent_state.intent_id = intent.intent_id
            LEFT JOIN public.broker_orders AS broker_order
              ON broker_order.intent_id = intent.intent_id
             AND broker_order.environment = intent.environment
             AND broker_order.account_fingerprint = intent.account_fingerprint
            LEFT JOIN public.current_broker_order_states AS broker_state
              ON broker_state.broker_order_id = broker_order.broker_order_id
            WHERE intent.environment = 'paper'
              AND intent.account_fingerprint = p_account_fingerprint
        ), '[]'::jsonb),
        'unresolved_order_outboxes', COALESCE((
            SELECT jsonb_agg(outbox.outbox_id ORDER BY outbox.outbox_id)
            FROM public.order_outbox AS outbox
            WHERE outbox.environment = 'paper'
              AND outbox.account_fingerprint = p_account_fingerprint
              AND outbox.completed_at IS NULL
        ), '[]'::jsonb),
        'unresolved_cancel_outboxes', COALESCE((
            SELECT jsonb_agg(outbox.cancel_outbox_id ORDER BY outbox.cancel_outbox_id)
            FROM public.cancel_outbox AS outbox
            WHERE outbox.environment = 'paper'
              AND outbox.account_fingerprint = p_account_fingerprint
              AND outbox.completed_at IS NULL
        ), '[]'::jsonb),
        'blockers', jsonb_build_array(
            'independent_cash_basis_missing',
            'stable_rest_fill_identity_missing',
            'canonical_local_order_truth_missing'
        )
    ) INTO v_payload;
    v_observed_at := clock_timestamp();

    IF NOT EXISTS (
        SELECT 1
        FROM public.paper_observer_leases AS lease
        WHERE lease.account_fingerprint = p_account_fingerprint
          AND lease.owner_id = p_owner_id
          AND lease.fencing_token = p_fencing_token
          AND lease.lease_until >= clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'paper observer fence expired during projection';
    END IF;

    RETURN QUERY SELECT v_observed_at, v_payload;
END;
$$;

CREATE OR REPLACE FUNCTION begin_paper_observer_cycle_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_observer_cycle_id UUID,
    p_trigger TEXT,
    p_started_at TIMESTAMPTZ,
    p_identity_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
    FOR UPDATE;
    IF NOT FOUND
       OR p_observer_cycle_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR p_started_at < clock_timestamp() - INTERVAL '5 seconds'
       OR p_started_at > clock_timestamp() + INTERVAL '5 seconds'
       OR p_identity_hash !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION 'paper observer cycle lacks current bounded authority';
    END IF;

    INSERT INTO public.paper_observer_cycles (
        observer_cycle_id, environment, account_fingerprint, owner_id,
        fencing_token, trigger, mode, started_at, identity_hash
    ) VALUES (
        p_observer_cycle_id, 'paper', p_account_fingerprint, p_owner_id,
        p_fencing_token, p_trigger, 'reconcile_only', p_started_at,
        p_identity_hash
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION record_paper_observer_projection_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_observer_cycle_id UUID,
    p_projection_id UUID,
    p_observed_at TIMESTAMPTZ,
    p_payload JSONB,
    p_payload_hash TEXT,
    p_domain_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    JOIN public.paper_observer_cycles AS cycle
      ON cycle.account_fingerprint = lease.account_fingerprint
     AND cycle.owner_id = lease.owner_id
     AND cycle.fencing_token = lease.fencing_token
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
      AND cycle.observer_cycle_id = p_observer_cycle_id
    FOR UPDATE OF lease;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer projection write lacks current cycle fence';
    END IF;

    INSERT INTO public.paper_observer_local_projections (
        projection_id, observer_cycle_id, observed_at, cash_basis_status,
        accounting_cash, cash_basis_evidence_hash, fill_identity_status,
        payload, payload_hash, domain_hash
    ) VALUES (
        p_projection_id, p_observer_cycle_id, p_observed_at, 'missing',
        NULL, NULL, 'missing', p_payload, p_payload_hash, p_domain_hash
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION record_paper_observer_page_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_observer_cycle_id UUID,
    p_page_evidence_id UUID,
    p_snapshot_round SMALLINT,
    p_source TEXT,
    p_page_ordinal INTEGER,
    p_request_parameters_hash TEXT,
    p_request_id TEXT,
    p_raw_payload_hash TEXT,
    p_received_at TIMESTAMPTZ,
    p_item_count INTEGER,
    p_completion_witness TEXT,
    p_evidence_hash TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    JOIN public.paper_observer_cycles AS cycle
      ON cycle.account_fingerprint = lease.account_fingerprint
     AND cycle.owner_id = lease.owner_id
     AND cycle.fencing_token = lease.fencing_token
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
      AND cycle.observer_cycle_id = p_observer_cycle_id
    FOR UPDATE OF lease;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer page write lacks current cycle fence';
    END IF;

    INSERT INTO public.paper_observer_pages (
        page_evidence_id, observer_cycle_id, snapshot_round, source,
        page_ordinal, request_parameters_hash, request_id, raw_payload_hash,
        received_at, item_count, completion_witness, evidence_hash
    ) VALUES (
        p_page_evidence_id, p_observer_cycle_id, p_snapshot_round, p_source,
        p_page_ordinal, p_request_parameters_hash, p_request_id,
        p_raw_payload_hash, p_received_at, p_item_count,
        p_completion_witness, p_evidence_hash
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION record_paper_observer_difference_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_observer_cycle_id UUID,
    p_observer_difference_id UUID,
    p_difference_ordinal INTEGER,
    p_category TEXT,
    p_kind TEXT,
    p_subject TEXT,
    p_local_value JSONB,
    p_broker_value JSONB,
    p_detail TEXT,
    p_detail_code TEXT,
    p_evidence_hash TEXT,
    p_observed_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    JOIN public.paper_observer_cycles AS cycle
      ON cycle.account_fingerprint = lease.account_fingerprint
     AND cycle.owner_id = lease.owner_id
     AND cycle.fencing_token = lease.fencing_token
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
      AND cycle.observer_cycle_id = p_observer_cycle_id
    FOR UPDATE OF lease;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer difference write lacks current cycle fence';
    END IF;

    INSERT INTO public.paper_observer_differences (
        observer_difference_id, observer_cycle_id, difference_ordinal,
        category, kind, subject, local_value, broker_value, detail,
        detail_code, evidence_hash, observed_at
    ) VALUES (
        p_observer_difference_id, p_observer_cycle_id, p_difference_ordinal,
        p_category, p_kind, p_subject, p_local_value, p_broker_value,
        p_detail, p_detail_code, p_evidence_hash, p_observed_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION complete_paper_observer_cycle_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_observer_cycle_id UUID,
    p_observer_completion_id UUID,
    p_outcome TEXT,
    p_failure_stage TEXT,
    p_reason_codes TEXT[],
    p_broker_snapshot_hashes TEXT[],
    p_page_count INTEGER,
    p_page_manifest_hash TEXT,
    p_difference_count INTEGER,
    p_difference_manifest_hash TEXT,
    p_result_payload JSONB,
    p_result_hash TEXT,
    p_completed_at TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_projection_hash TEXT;
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    JOIN public.paper_observer_cycles AS cycle
      ON cycle.account_fingerprint = lease.account_fingerprint
     AND cycle.owner_id = lease.owner_id
     AND cycle.fencing_token = lease.fencing_token
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
      AND cycle.observer_cycle_id = p_observer_cycle_id
    FOR UPDATE OF lease;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer completion lacks current cycle fence';
    END IF;

    SELECT projection.domain_hash
    INTO v_projection_hash
    FROM public.paper_observer_local_projections AS projection
    WHERE projection.observer_cycle_id = p_observer_cycle_id;

    INSERT INTO public.paper_observer_completions (
        observer_completion_id, observer_cycle_id, outcome, failure_stage,
        reason_codes, local_projection_hash, broker_snapshot_hashes,
        page_count, page_manifest_hash, difference_count,
        difference_manifest_hash, result_payload, result_hash,
        completed_fencing_token, completed_at
    ) VALUES (
        p_observer_completion_id, p_observer_cycle_id, p_outcome,
        p_failure_stage, p_reason_codes, v_projection_hash,
        p_broker_snapshot_hashes, p_page_count, p_page_manifest_hash,
        p_difference_count, p_difference_manifest_hash, p_result_payload,
        p_result_hash, p_fencing_token, p_completed_at
    );
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION inspect_paper_observer_cycle_v1(
    p_account_fingerprint TEXT,
    p_owner_id UUID,
    p_fencing_token BIGINT,
    p_observer_cycle_id UUID
) RETURNS TABLE (
    identity_hash TEXT,
    projection_hash TEXT,
    page_count INTEGER,
    page_manifest_hash TEXT,
    difference_count INTEGER,
    difference_manifest_hash TEXT,
    outcome TEXT,
    result_hash TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM 1
    FROM public.paper_observer_leases AS lease
    WHERE lease.account_fingerprint = p_account_fingerprint
      AND lease.owner_id = p_owner_id
      AND lease.fencing_token = p_fencing_token
      AND lease.lease_until >= clock_timestamp()
    ;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'paper observer inspection lacks current fence';
    END IF;

    RETURN QUERY
    SELECT
        cycle.identity_hash,
        projection.domain_hash,
        COALESCE(completion.page_count, page_summary.page_count),
        COALESCE(completion.page_manifest_hash, page_summary.page_manifest_hash),
        COALESCE(completion.difference_count, difference_summary.difference_count),
        COALESCE(
            completion.difference_manifest_hash,
            difference_summary.difference_manifest_hash
        ),
        completion.outcome,
        completion.result_hash
    FROM public.paper_observer_cycles AS cycle
    LEFT JOIN public.paper_observer_local_projections AS projection
      ON projection.observer_cycle_id = cycle.observer_cycle_id
    LEFT JOIN public.paper_observer_completions AS completion
      ON completion.observer_cycle_id = cycle.observer_cycle_id
    CROSS JOIN LATERAL (
        SELECT
            COUNT(*)::integer AS page_count,
            encode(sha256(convert_to(COALESCE(string_agg(
                page.evidence_hash,
                E'\n' ORDER BY
                    page.snapshot_round,
                    CASE page.source
                        WHEN 'account' THEN 1
                        WHEN 'positions' THEN 2
                        WHEN 'open_orders' THEN 3
                        WHEN 'closed_orders' THEN 4
                        WHEN 'fill_activities' THEN 5
                    END,
                    page.page_ordinal
            ), ''), 'UTF8')), 'hex') AS page_manifest_hash
        FROM public.paper_observer_pages AS page
        WHERE page.observer_cycle_id = cycle.observer_cycle_id
    ) AS page_summary
    CROSS JOIN LATERAL (
        SELECT
            COUNT(*)::integer AS difference_count,
            encode(sha256(convert_to(COALESCE(string_agg(
                difference.evidence_hash,
                E'\n' ORDER BY difference.difference_ordinal
            ), ''), 'UTF8')), 'hex') AS difference_manifest_hash
        FROM public.paper_observer_differences AS difference
        WHERE difference.observer_cycle_id = cycle.observer_cycle_id
    ) AS difference_summary
    WHERE cycle.observer_cycle_id = p_observer_cycle_id
      AND cycle.environment = 'paper'
      AND cycle.account_fingerprint = p_account_fingerprint;
END;
$$;

-- Observer-specific safety attestations remain separate from the execution
-- manifest so the existing PgExecutionStore allowlist cannot accidentally
-- reinterpret observer capabilities as execution capabilities.
INSERT INTO paper_observer_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'function', signature,
    encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(signature)), 'UTF8')), 'hex')
FROM (VALUES
    ('public.acquire_paper_observer_lease_v1(text,uuid,interval)'),
    ('public.renew_paper_observer_lease_v1(text,uuid,bigint,interval)'),
    ('public.read_paper_observer_projection_v1(text,uuid,bigint)'),
    ('public.begin_paper_observer_cycle_v1(text,uuid,bigint,uuid,text,timestamp with time zone,text)'),
    ('public.record_paper_observer_projection_v1(text,uuid,bigint,uuid,uuid,timestamp with time zone,jsonb,text,text)'),
    ('public.record_paper_observer_page_v1(text,uuid,bigint,uuid,uuid,smallint,text,integer,text,text,text,timestamp with time zone,integer,text,text)'),
    ('public.record_paper_observer_difference_v1(text,uuid,bigint,uuid,uuid,integer,text,text,text,jsonb,jsonb,text,text,text,timestamp with time zone)'),
    ('public.complete_paper_observer_cycle_v1(text,uuid,bigint,uuid,uuid,text,text,text[],text[],integer,text,integer,text,jsonb,text,timestamp with time zone)'),
    ('public.inspect_paper_observer_cycle_v1(text,uuid,bigint,uuid)'),
    ('public.enforce_paper_observer_child_append()'),
    ('public.enforce_paper_observer_completion()'),
    ('public.reject_audit_mutation()')
) AS required_function(signature);

INSERT INTO paper_observer_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'view', identity,
    encode(sha256(convert_to(
        pg_get_viewdef(to_regclass(identity), true), 'UTF8'
    )), 'hex')
FROM (VALUES
    ('public.broker_position_quantities'),
    ('public.current_intent_states'),
    ('public.current_broker_order_states')
) AS required_view(identity);

INSERT INTO paper_observer_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'relation', required.identity,
    encode(sha256(convert_to(
        relation.relkind::text || E'\n' || COALESCE(string_agg(
            concat_ws('|',
                attribute.attnum::text,
                attribute.attname,
                format_type(attribute.atttypid, attribute.atttypmod),
                attribute.attnotnull::text,
                attribute.attidentity,
                attribute.attgenerated,
                COALESCE(pg_get_expr(default_value.adbin, default_value.adrelid), '')
            ),
            E'\n' ORDER BY attribute.attnum
        ), ''),
        'UTF8'
    )), 'hex')
FROM (VALUES
    ('public.order_intents'),
    ('public.intent_state_events'),
    ('public.broker_orders'),
    ('public.broker_order_events'),
    ('public.fills'),
    ('public.order_outbox'),
    ('public.cancel_outbox')
) AS required(identity)
JOIN pg_catalog.pg_class AS relation
  ON relation.oid = to_regclass(required.identity)
JOIN pg_catalog.pg_attribute AS attribute
  ON attribute.attrelid = relation.oid
 AND attribute.attnum > 0
 AND NOT attribute.attisdropped
LEFT JOIN pg_catalog.pg_attrdef AS default_value
  ON default_value.adrelid = relation.oid
 AND default_value.adnum = attribute.attnum
GROUP BY required.identity, relation.relkind;

INSERT INTO paper_observer_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'trigger', namespace.nspname || '.' || relation.relname || '.' || trigger.tgname,
    encode(sha256(convert_to(pg_get_triggerdef(trigger.oid, true), 'UTF8')), 'hex')
FROM pg_catalog.pg_trigger AS trigger
JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND relation.relname IN (
      'paper_observer_cycles', 'paper_observer_local_projections',
      'paper_observer_pages', 'paper_observer_differences',
      'paper_observer_completions', 'paper_observer_schema_attestations'
  )
  AND NOT trigger.tgisinternal;

INSERT INTO paper_observer_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'constraint', namespace.nspname || '.' || relation.relname || '.' || con.conname,
    encode(sha256(convert_to(pg_get_constraintdef(con.oid, true), 'UTF8')), 'hex')
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS relation ON relation.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND relation.relname IN (
      'paper_observer_leases', 'paper_observer_cycles',
      'paper_observer_local_projections', 'paper_observer_pages',
      'paper_observer_differences', 'paper_observer_completions',
      'paper_observer_schema_attestations'
  );

REVOKE ALL ON
    paper_observer_leases,
    paper_observer_cycles,
    paper_observer_local_projections,
    paper_observer_pages,
    paper_observer_differences,
    paper_observer_completions,
    paper_observer_schema_attestations
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator,
    alpaca_trader_observer;

-- The observer may read only this immutable, hash-only manifest so startup
-- can compare current definitions with the migration-time definitions.
GRANT SELECT ON paper_observer_schema_attestations
TO alpaca_trader_observer;

-- Preserve the existing runtime schema verifier's select-only relation
-- invariant without giving runtime any observer function or write capability.
GRANT SELECT ON
    paper_observer_leases,
    paper_observer_cycles,
    paper_observer_local_projections,
    paper_observer_pages,
    paper_observer_differences,
    paper_observer_completions,
    paper_observer_schema_attestations
TO alpaca_trader_runtime, alpaca_trader_operator;

REVOKE ALL ON FUNCTION
    acquire_paper_observer_lease_v1(TEXT, UUID, INTERVAL),
    renew_paper_observer_lease_v1(TEXT, UUID, BIGINT, INTERVAL),
    read_paper_observer_projection_v1(TEXT, UUID, BIGINT),
    begin_paper_observer_cycle_v1(TEXT, UUID, BIGINT, UUID, TEXT, TIMESTAMPTZ, TEXT),
    record_paper_observer_projection_v1(TEXT, UUID, BIGINT, UUID, UUID, TIMESTAMPTZ, JSONB, TEXT, TEXT),
    record_paper_observer_page_v1(TEXT, UUID, BIGINT, UUID, UUID, SMALLINT, TEXT, INTEGER, TEXT, TEXT, TEXT, TIMESTAMPTZ, INTEGER, TEXT, TEXT),
    record_paper_observer_difference_v1(TEXT, UUID, BIGINT, UUID, UUID, INTEGER, TEXT, TEXT, TEXT, JSONB, JSONB, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    complete_paper_observer_cycle_v1(TEXT, UUID, BIGINT, UUID, UUID, TEXT, TEXT, TEXT[], TEXT[], INTEGER, TEXT, INTEGER, TEXT, JSONB, TEXT, TIMESTAMPTZ),
    inspect_paper_observer_cycle_v1(TEXT, UUID, BIGINT, UUID),
    enforce_paper_observer_child_append(),
    enforce_paper_observer_completion()
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator,
    alpaca_trader_observer;

GRANT EXECUTE ON FUNCTION
    acquire_paper_observer_lease_v1(TEXT, UUID, INTERVAL),
    renew_paper_observer_lease_v1(TEXT, UUID, BIGINT, INTERVAL),
    read_paper_observer_projection_v1(TEXT, UUID, BIGINT),
    begin_paper_observer_cycle_v1(TEXT, UUID, BIGINT, UUID, TEXT, TIMESTAMPTZ, TEXT),
    record_paper_observer_projection_v1(TEXT, UUID, BIGINT, UUID, UUID, TIMESTAMPTZ, JSONB, TEXT, TEXT),
    record_paper_observer_page_v1(TEXT, UUID, BIGINT, UUID, UUID, SMALLINT, TEXT, INTEGER, TEXT, TEXT, TEXT, TIMESTAMPTZ, INTEGER, TEXT, TEXT),
    record_paper_observer_difference_v1(TEXT, UUID, BIGINT, UUID, UUID, INTEGER, TEXT, TEXT, TEXT, JSONB, JSONB, TEXT, TEXT, TEXT, TIMESTAMPTZ),
    complete_paper_observer_cycle_v1(TEXT, UUID, BIGINT, UUID, UUID, TEXT, TEXT, TEXT[], TEXT[], INTEGER, TEXT, INTEGER, TEXT, JSONB, TEXT, TIMESTAMPTZ),
    inspect_paper_observer_cycle_v1(TEXT, UUID, BIGINT, UUID)
TO alpaca_trader_observer;

REVOKE ALL ON SCHEMA public FROM alpaca_trader_observer;
GRANT USAGE ON SCHEMA public TO alpaca_trader_observer;

DO $$
BEGIN
    EXECUTE format(
        'REVOKE ALL ON DATABASE %I FROM alpaca_trader_observer',
        current_database()
    );
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO alpaca_trader_observer',
        current_database()
    );
END;
$$;

COMMIT;
