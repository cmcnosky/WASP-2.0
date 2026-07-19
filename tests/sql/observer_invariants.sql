\set ON_ERROR_STOP on

-- The observer must remain separate from risk-domain account snapshots and
-- execution reconciliation even when broker-looking cash evidence exists.
INSERT INTO account_snapshots (
    account_snapshot_id, environment, account_fingerprint, broker_timestamp,
    received_at, account_status, recognized_status, cash, equity, buying_power,
    trading_blocked, transfers_blocked, account_blocked, payload, payload_hash
) VALUES (
    '90000000-0000-0000-0000-000000000001',
    'paper', repeat('a', 64), clock_timestamp(), clock_timestamp(),
    'ACTIVE', TRUE, 999999, 999999, 999999,
    FALSE, FALSE, FALSE, '{"source":"risk-domain-fixture"}', repeat('9', 64)
);

DO $$
DECLARE
    v_account CONSTANT TEXT := repeat('a', 64);
    v_owner CONSTANT UUID := '90000000-0000-0000-0000-000000000010';
    v_cycle CONSTANT UUID := '90000000-0000-0000-0000-000000000020';
    v_failed_cycle CONSTANT UUID := '90000000-0000-0000-0000-000000000021';
    v_incomplete_cycle CONSTANT UUID := '90000000-0000-0000-0000-000000000022';
    v_token BIGINT;
    v_started_at TIMESTAMPTZ;
    v_observed_at TIMESTAMPTZ;
    v_completed_at TIMESTAMPTZ;
    v_projection JSONB;
    v_page_payload JSONB;
    v_difference_payload JSONB;
    v_result_payload JSONB;
    v_page_count INTEGER;
    v_page_manifest TEXT;
    v_difference_count INTEGER;
    v_difference_manifest TEXT;
    v_empty_manifest TEXT;
    v_round SMALLINT;
    v_source TEXT;
    v_witness TEXT;
    v_before_accounts BIGINT;
    v_before_reconciliations BIGINT;
    v_before_execution_leases BIGINT;
    v_readiness_hash TEXT;
    v_readiness_v2_hash TEXT;
    v_inspection RECORD;
BEGIN
    SELECT COUNT(*) INTO v_before_accounts FROM public.account_snapshots;
    SELECT COUNT(*) INTO v_before_reconciliations FROM public.reconciliation_runs;
    SELECT COUNT(*) INTO v_before_execution_leases
    FROM public.executor_leases
    WHERE environment = 'paper' AND account_fingerprint = v_account;
    SELECT encode(sha256(convert_to(pg_get_viewdef(
        'public.execution_readiness'::regclass, true
    ), 'UTF8')), 'hex') INTO v_readiness_hash;
    SELECT encode(sha256(convert_to(pg_get_viewdef(
        'public.execution_readiness_v2'::regclass, true
    ), 'UTF8')), 'hex') INTO v_readiness_v2_hash;
    SELECT encode(sha256(convert_to('', 'UTF8')), 'hex') INTO v_empty_manifest;

    SELECT lease.fencing_token
    INTO v_token
    FROM public.acquire_paper_observer_lease_v1(
        v_account, v_owner, INTERVAL '60 seconds'
    ) AS lease;
    IF v_token <> 1 THEN
        RAISE EXCEPTION 'first independent observer fence was not one';
    END IF;

    v_started_at := clock_timestamp();
    IF NOT public.begin_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_cycle, 'startup', v_started_at,
        repeat('1', 64)
    ) THEN
        RAISE EXCEPTION 'paper observer cycle did not begin';
    END IF;

    SELECT projection.observed_at, projection.payload
    INTO v_observed_at, v_projection
    FROM public.read_paper_observer_projection_v1(
        v_account, v_owner, v_token
    ) AS projection;
    IF v_projection #>> '{cash_basis,status}' <> 'missing'
       OR (v_projection #> '{cash_basis}') ? 'amount'
       OR (v_projection #> '{cash_basis}') ? 'evidence_hash'
       OR v_projection #>> '{fill_identity_basis,status}' <> 'missing'
       OR v_projection #>> '{order_identity_basis,status}' <> 'missing'
       OR v_projection -> 'orders' <> '[]'::jsonb
       OR v_projection::text LIKE '%999999%'
    THEN
        RAISE EXCEPTION 'observer projection fabricated a local cash/fill basis';
    END IF;
    IF NOT public.record_paper_observer_projection_v1(
        v_account, v_owner, v_token, v_cycle,
        '90000000-0000-0000-0000-000000000030',
        v_observed_at, v_projection, repeat('2', 64), repeat('d', 64)
    ) THEN
        RAISE EXCEPTION 'observer projection did not persist';
    END IF;

    FOR v_round IN 1..2 LOOP
        FOREACH v_source IN ARRAY ARRAY[
            'account', 'positions', 'open_orders', 'closed_orders',
            'fill_activities'
        ] LOOP
            v_witness := CASE
                WHEN v_source IN ('account', 'positions') THEN 'single'
                ELSE 'short_page'
            END;
            IF NOT public.record_paper_observer_page_v1(
                v_account, v_owner, v_token, v_cycle, gen_random_uuid(),
                v_round::smallint, v_source, 0,
                encode(sha256(convert_to(
                    format('request:%s:%s', v_round, v_source), 'UTF8'
                )), 'hex'),
                format('request-%s-%s', v_round, v_source),
                encode(sha256(convert_to(
                    format('payload:%s:%s', v_round, v_source), 'UTF8'
                )), 'hex'),
                clock_timestamp(), 0, v_witness,
                encode(sha256(convert_to(
                    format('evidence:%s:%s', v_round, v_source), 'UTF8'
                )), 'hex')
            ) THEN
                RAISE EXCEPTION 'observer page did not persist';
            END IF;
        END LOOP;
    END LOOP;

    IF NOT public.record_paper_observer_difference_v1(
        v_account, v_owner, v_token, v_cycle,
        '90000000-0000-0000-0000-000000000040', 0,
        'cash', 'missing_locally', 'independent_cash_basis',
        NULL, '"999999.000000"'::jsonb,
        'independent durable accounting cash evidence is missing',
        'INDEPENDENT_CASH_BASIS_MISSING', repeat('3', 64), clock_timestamp()
    ) OR NOT public.record_paper_observer_difference_v1(
        v_account, v_owner, v_token, v_cycle,
        '90000000-0000-0000-0000-000000000041', 1,
        'fill', 'missing_locally', 'stable_rest_fill_identity',
        NULL, NULL,
        'stable REST fill-activity identity evidence is missing',
        'STABLE_REST_FILL_IDENTITY_MISSING', repeat('4', 64), clock_timestamp()
    ) OR NOT public.record_paper_observer_difference_v1(
        v_account, v_owner, v_token, v_cycle,
        '90000000-0000-0000-0000-000000000042', 2,
        'order', 'missing_locally', 'canonical_local_order_truth',
        NULL, NULL,
        'canonical durable local order truth is missing',
        'CANONICAL_LOCAL_ORDER_TRUTH_MISSING', repeat('5', 64), clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'observer missing-basis differences did not persist';
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
    WHERE page.observer_cycle_id = v_cycle;

    SELECT
        COUNT(*)::integer,
        encode(sha256(convert_to(COALESCE(string_agg(
            difference.evidence_hash,
            E'\n' ORDER BY difference.difference_ordinal
        ), ''), 'UTF8')), 'hex')
    INTO v_difference_count, v_difference_manifest
    FROM public.paper_observer_differences AS difference
    WHERE difference.observer_cycle_id = v_cycle;

    v_completed_at := clock_timestamp();
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'snapshot_round', page.snapshot_round,
        'kind', page.source,
        'page_ordinal', page.page_ordinal,
        'request_parameters_hash', page.request_parameters_hash,
        'request_id', page.request_id,
        'raw_payload_hash', page.raw_payload_hash,
        'received_at', page.received_at,
        'item_count', page.item_count,
        'completion_witness', page.completion_witness,
        'evidence_hash', page.evidence_hash
    ) ORDER BY
        page.snapshot_round,
        CASE page.source
            WHEN 'account' THEN 1
            WHEN 'positions' THEN 2
            WHEN 'open_orders' THEN 3
            WHEN 'closed_orders' THEN 4
            WHEN 'fill_activities' THEN 5
        END,
        page.page_ordinal
    ), '[]'::jsonb)
    INTO v_page_payload
    FROM public.paper_observer_pages AS page
    WHERE page.observer_cycle_id = v_cycle;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'kind', difference.kind,
        'subject', difference.subject,
        'local_value', difference.local_value,
        'broker_value', difference.broker_value,
        'detail', difference.detail
    ) ORDER BY difference.difference_ordinal), '[]'::jsonb)
    INTO v_difference_payload
    FROM public.paper_observer_differences AS difference
    WHERE difference.observer_cycle_id = v_cycle;

    v_result_payload := jsonb_build_object(
        'cycle_id', v_cycle,
        'generated_at', v_completed_at,
        'environment', 'paper',
        'mode', 'reconcile_only',
        'resumable', FALSE,
        'outcome', 'blocked',
        'failure_stage', NULL,
        'broker_snapshot_stable', TRUE,
        'reasons', to_jsonb(ARRAY[
            'read_only_policy', 'independent_cash_basis_missing',
            'stable_rest_fill_identity_missing',
            'canonical_local_order_truth_missing',
            'reconciliation_differences'
        ]::text[]),
        'local_evidence_hash', repeat('d', 64),
        'broker_evidence_hashes', to_jsonb(ARRAY[
            repeat('5', 64), repeat('6', 64)
        ]::text[]),
        'normalized_broker_snapshots', jsonb_build_array(
            jsonb_build_object('fixture_round', 1),
            jsonb_build_object('fixture_round', 2)
        ),
        'source_page_evidence', v_page_payload,
        'reconciliation', jsonb_build_object(
            'generated_at', v_completed_at,
            'account_fingerprint', v_account,
            'execution_fencing_token', v_token,
            'differences', v_difference_payload,
            'may_resume_execution', FALSE
        )
    );
    IF NOT public.complete_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_cycle,
        '90000000-0000-0000-0000-000000000050',
        'blocked', NULL,
        ARRAY[
            'read_only_policy', 'independent_cash_basis_missing',
            'stable_rest_fill_identity_missing',
            'canonical_local_order_truth_missing',
            'reconciliation_differences'
        ],
        ARRAY[repeat('5', 64), repeat('6', 64)],
        v_page_count, v_page_manifest,
        v_difference_count, v_difference_manifest,
        v_result_payload,
        repeat('7', 64), v_completed_at
    ) THEN
        RAISE EXCEPTION 'blocked observer completion did not persist';
    END IF;

    SELECT * INTO v_inspection
    FROM public.inspect_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_cycle
    );
    IF v_inspection.identity_hash <> repeat('1', 64)
       OR v_inspection.projection_hash <> repeat('d', 64)
       OR v_inspection.page_count <> 10
       OR v_inspection.page_manifest_hash <> v_page_manifest
       OR v_inspection.difference_count <> 3
       OR v_inspection.difference_manifest_hash <> v_difference_manifest
       OR v_inspection.outcome <> 'blocked'
       OR v_inspection.result_hash <> repeat('7', 64)
    THEN
        RAISE EXCEPTION 'deterministic observer commit inspection disagreed';
    END IF;

    IF (SELECT execution_authorizing
        FROM public.paper_observer_completions
        WHERE observer_cycle_id = v_cycle)
    THEN
        RAISE EXCEPTION 'observer completion became execution-authorizing';
    END IF;

    -- A caught broker error can be recorded with no fabricated projection,
    -- pages, differences, or broker snapshot hashes.
    v_started_at := clock_timestamp();
    PERFORM public.begin_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_failed_cycle, 'periodic',
        v_started_at, repeat('8', 64)
    );
    v_completed_at := clock_timestamp();
    v_result_payload := jsonb_build_object(
        'cycle_id', v_failed_cycle,
        'generated_at', v_completed_at,
        'environment', 'paper',
        'mode', 'reconcile_only',
        'resumable', FALSE,
        'outcome', 'failed',
        'failure_stage', 'broker_round_1',
        'broker_snapshot_stable', FALSE,
        'reasons', to_jsonb(ARRAY[
            'read_only_policy', 'broker_snapshot_unavailable'
        ]::text[]),
        'local_evidence_hash', NULL,
        'broker_evidence_hashes', '[]'::jsonb,
        'normalized_broker_snapshots', '[]'::jsonb,
        'source_page_evidence', '[]'::jsonb,
        'reconciliation', NULL
    );
    PERFORM public.complete_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_failed_cycle,
        '90000000-0000-0000-0000-000000000051',
        'failed', 'broker_round_1',
        ARRAY['read_only_policy', 'broker_snapshot_unavailable'],
        ARRAY[]::text[], 0, v_empty_manifest, 0, v_empty_manifest,
        v_result_payload, repeat('8', 64), v_completed_at
    );

    -- A blocked result cannot omit its projection/page/difference evidence.
    v_started_at := clock_timestamp();
    PERFORM public.begin_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_incomplete_cycle, 'periodic',
        v_started_at, repeat('a', 64)
    );
    BEGIN
        PERFORM public.complete_paper_observer_cycle_v1(
            v_account, v_owner, v_token, v_incomplete_cycle,
            '90000000-0000-0000-0000-000000000052',
            'blocked', NULL,
            ARRAY[
                'read_only_policy', 'independent_cash_basis_missing',
                'stable_rest_fill_identity_missing',
                'canonical_local_order_truth_missing'
            ],
            ARRAY[repeat('b', 64), repeat('c', 64)],
            0, v_empty_manifest, 0, v_empty_manifest,
            jsonb_build_object(
                'environment', 'paper', 'mode', 'reconcile_only',
                'resumable', FALSE, 'outcome', 'blocked',
                'local_evidence_hash', NULL
            ),
            repeat('d', 64), clock_timestamp()
        );
        RAISE EXCEPTION 'blocked observer completion accepted incomplete evidence';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'blocked observer completion accepted incomplete evidence' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        PERFORM public.record_paper_observer_page_v1(
            v_account, v_owner, v_token, v_cycle, gen_random_uuid(),
            1::smallint, 'account', 0, repeat('1', 64), NULL, repeat('2', 64),
            clock_timestamp(), 0, 'single', repeat('3', 64)
        );
        RAISE EXCEPTION 'late observer page appended after completion';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'late observer page appended after completion' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE public.paper_observer_completions
        SET outcome = 'failed'
        WHERE observer_cycle_id = v_cycle;
        RAISE EXCEPTION 'append-only observer completion was mutable';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'append-only observer completion was mutable' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        PERFORM * FROM public.read_paper_observer_projection_v1(
            v_account, v_owner, v_token + 1
        );
        RAISE EXCEPTION 'stale observer fence read a projection';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'stale observer fence read a projection' THEN
                RAISE;
            END IF;
    END;

    IF (SELECT COUNT(*) FROM public.account_snapshots) <> v_before_accounts
       OR (SELECT COUNT(*) FROM public.reconciliation_runs) <> v_before_reconciliations
       OR (SELECT COUNT(*) FROM public.executor_leases
           WHERE environment = 'paper'
             AND account_fingerprint = v_account) <> v_before_execution_leases
       OR encode(sha256(convert_to(pg_get_viewdef(
           'public.execution_readiness'::regclass, true
       ), 'UTF8')), 'hex') <> v_readiness_hash
       OR encode(sha256(convert_to(pg_get_viewdef(
           'public.execution_readiness_v2'::regclass, true
       ), 'UTF8')), 'hex') <> v_readiness_v2_hash
    THEN
        RAISE EXCEPTION 'observer evidence altered execution authority state';
    END IF;

    -- Reacquisition rotates only the independent observer fence. Exact old
    -- commit evidence remains inspectable under the new current fence.
    SELECT lease.fencing_token
    INTO v_token
    FROM public.acquire_paper_observer_lease_v1(
        v_account, v_owner, INTERVAL '60 seconds'
    ) AS lease;
    IF v_token <> 2 THEN
        RAISE EXCEPTION 'observer fence did not rotate independently';
    END IF;
    PERFORM * FROM public.inspect_paper_observer_cycle_v1(
        v_account, v_owner, v_token, v_cycle
    );
END;
$$;

-- Attestation hashes cover every observer function, trigger, and constraint.
DO $$
DECLARE
    v_mismatches INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_mismatches
    FROM public.paper_observer_schema_attestations AS manifest
    WHERE CASE manifest.object_kind
        WHEN 'function' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(
                pg_get_functiondef(procedure.oid), 'UTF8'
            )), 'hex')
            FROM pg_catalog.pg_proc AS procedure
            WHERE procedure.oid = to_regprocedure(manifest.object_identity)
        )
        WHEN 'view' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(
                pg_get_viewdef(relation.oid, true), 'UTF8'
            )), 'hex')
            FROM pg_catalog.pg_class AS relation
            WHERE relation.oid = to_regclass(manifest.object_identity)
              AND relation.relkind = 'v'
        )
        WHEN 'relation' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(
                relation.relkind::text || E'\n' || COALESCE(string_agg(
                    concat_ws('|',
                        attribute.attnum::text,
                        attribute.attname,
                        format_type(attribute.atttypid, attribute.atttypmod),
                        attribute.attnotnull::text,
                        attribute.attidentity,
                        attribute.attgenerated,
                        COALESCE(pg_get_expr(
                            default_value.adbin, default_value.adrelid
                        ), '')
                    ),
                    E'\n' ORDER BY attribute.attnum
                ), ''),
                'UTF8'
            )), 'hex')
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = relation.oid
             AND attribute.attnum > 0
             AND NOT attribute.attisdropped
            LEFT JOIN pg_catalog.pg_attrdef AS default_value
              ON default_value.adrelid = relation.oid
             AND default_value.adnum = attribute.attnum
            WHERE relation.oid = to_regclass(manifest.object_identity)
              AND relation.relkind IN ('r', 'p')
            GROUP BY relation.relkind
        )
        WHEN 'trigger' THEN manifest.definition_sha256 IS DISTINCT FROM (
            SELECT encode(sha256(convert_to(
                pg_get_triggerdef(trigger.oid, true), 'UTF8'
            )), 'hex')
            FROM pg_catalog.pg_trigger AS trigger
            JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
            JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname || '.' || relation.relname || '.' || trigger.tgname
                = manifest.object_identity
              AND NOT trigger.tgisinternal
              AND trigger.tgenabled IN ('O', 'A')
        )
        WHEN 'constraint' THEN manifest.definition_sha256 IS DISTINCT FROM CASE
            WHEN manifest.object_identity = 'application.json_hash_profile' THEN
                '0372e64987504c848a5146bbf31d5123e4e9e09dac09f57d150ede3b767eab45'
            ELSE (
                SELECT encode(sha256(convert_to(
                    pg_get_constraintdef(con.oid, true), 'UTF8'
                )), 'hex')
                FROM pg_catalog.pg_constraint AS con
                JOIN pg_catalog.pg_class AS relation ON relation.oid = con.conrelid
                JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname || '.' || relation.relname || '.' || con.conname
                    = manifest.object_identity
                  AND con.convalidated
            )
        END
    END;

    IF v_mismatches <> 0
       OR (SELECT COUNT(*) FROM public.paper_observer_schema_attestations
           WHERE object_kind = 'function') <> 12
       OR (SELECT COUNT(*) FROM public.paper_observer_schema_attestations
           WHERE object_kind = 'view') <> 3
       OR (SELECT COUNT(*) FROM public.paper_observer_schema_attestations
           WHERE object_kind = 'relation') <> 7
       OR (SELECT COUNT(*) FROM public.paper_observer_schema_attestations
           WHERE object_kind = 'trigger') <> 10
       OR (SELECT COUNT(*) FROM public.paper_observer_schema_attestations
           WHERE object_kind = 'constraint') <> 81
       OR (SELECT COUNT(*) FROM public.paper_observer_schema_attestations
           WHERE object_kind = 'constraint'
             AND object_identity = 'application.json_hash_profile'
             AND definition_sha256 = '0372e64987504c848a5146bbf31d5123e4e9e09dac09f57d150ede3b767eab45') <> 1
    THEN
        RAISE EXCEPTION 'paper observer safety attestation is incomplete or stale';
    END IF;
END;
$$;

-- The runtime verifier treats the nine callable wrappers and two observer
-- enforcement triggers as trusted SECURITY DEFINER boundaries. The shared
-- append-only rejection trigger remains SECURITY INVOKER. Conflating these
-- classes would reject the migrated schema or broaden that shared helper.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
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
            ('public.enforce_paper_observer_completion()')
        ) AS required(signature)
        LEFT JOIN pg_catalog.pg_proc AS procedure
          ON procedure.oid = to_regprocedure(required.signature)
        WHERE procedure.oid IS NULL
           OR NOT procedure.prosecdef
           OR NOT (
                procedure.proconfig @>
                    ARRAY['search_path=pg_catalog, public']::text[]
           )
    ) OR EXISTS (
        SELECT 1
        FROM (VALUES
            ('public.reject_audit_mutation()')
        ) AS required(signature)
        LEFT JOIN pg_catalog.pg_proc AS procedure
          ON procedure.oid = to_regprocedure(required.signature)
        WHERE procedure.oid IS NULL OR procedure.prosecdef
    ) THEN
        RAISE EXCEPTION 'observer function security mode disagrees with runtime verifier';
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS procedure
        CROSS JOIN LATERAL aclexplode(COALESCE(
            procedure.proacl,
            acldefault('f', procedure.proowner)
        )) AS privilege
        WHERE procedure.oid = to_regprocedure(
            'public.acquire_paper_observer_lease_v1(text,uuid,interval)'
        )
          AND privilege.grantee = 0
          AND privilege.privilege_type = 'EXECUTE'
    ) OR has_function_privilege(
        'alpaca_trader_runtime',
        'public.complete_paper_observer_cycle_v1(text,uuid,bigint,uuid,uuid,text,text,text[],text[],integer,text,integer,text,jsonb,text,timestamp with time zone)',
        'EXECUTE'
    ) OR has_function_privilege(
        'alpaca_trader_operator',
        'public.begin_paper_observer_cycle_v1(text,uuid,bigint,uuid,text,timestamp with time zone,text)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'observer function authority escaped its dedicated role';
    END IF;
    IF (SELECT rolcanlogin OR rolinherit OR rolsuper OR rolcreatedb
               OR rolcreaterole OR rolreplication OR rolbypassrls
        FROM pg_catalog.pg_roles
        WHERE rolname = 'alpaca_trader_observer')
    THEN
        RAISE EXCEPTION 'observer group role has unsafe attributes';
    END IF;
END;
$$;

CREATE ROLE alpaca_trader_observer_test_login
    LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
GRANT alpaca_trader_observer TO alpaca_trader_observer_test_login;

SET SESSION AUTHORIZATION alpaca_trader_observer_test_login;

DO $$
DECLARE
    v_function_count INTEGER;
    v_relation_privileges INTEGER;
    v_sequence_privileges INTEGER;
BEGIN
    IF current_user <> 'alpaca_trader_observer_test_login'
       OR session_user <> 'alpaca_trader_observer_test_login'
       OR NOT pg_has_role(current_user, 'alpaca_trader_observer', 'USAGE')
       OR pg_has_role(current_user, 'alpaca_trader_runtime', 'MEMBER')
       OR pg_has_role(current_user, 'alpaca_trader_operator', 'MEMBER')
       OR NOT has_database_privilege(current_user, current_database(), 'CONNECT')
       OR has_database_privilege(current_user, current_database(), 'CREATE')
       OR has_database_privilege(current_user, current_database(), 'TEMPORARY')
       OR NOT has_schema_privilege(current_user, 'public', 'USAGE')
       OR has_schema_privilege(current_user, 'public', 'CREATE')
    THEN
        RAISE EXCEPTION 'actual observer login authority is unsafe';
    END IF;

    SELECT COUNT(*) INTO v_relation_privileges
    FROM pg_catalog.pg_class AS relation
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'public'
      AND relation.relkind IN ('r', 'p', 'v', 'm')
      AND (
          has_table_privilege(current_user, relation.oid, 'SELECT')
          OR has_table_privilege(current_user, relation.oid, 'INSERT')
          OR has_table_privilege(current_user, relation.oid, 'UPDATE')
          OR has_table_privilege(current_user, relation.oid, 'DELETE')
          OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
          OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
          OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
      );
    SELECT COUNT(*) INTO v_sequence_privileges
    FROM pg_catalog.pg_class AS sequence
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
    WHERE namespace.nspname = 'public'
      AND sequence.relkind = 'S'
      AND (
          has_sequence_privilege(current_user, sequence.oid, 'USAGE')
          OR has_sequence_privilege(current_user, sequence.oid, 'SELECT')
          OR has_sequence_privilege(current_user, sequence.oid, 'UPDATE')
      );
    SELECT COUNT(*) INTO v_function_count
    FROM pg_catalog.pg_proc AS procedure
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname = 'public'
      AND procedure.prokind = 'f'
      AND has_function_privilege(current_user, procedure.oid, 'EXECUTE');
    IF v_relation_privileges <> 1
       OR NOT has_table_privilege(
           current_user,
           'public.paper_observer_schema_attestations',
           'SELECT'
       )
       OR v_sequence_privileges <> 0
       OR v_function_count <> 9
    THEN
        RAISE EXCEPTION 'observer login has relation, sequence, or function privilege drift';
    END IF;

    PERFORM * FROM public.inspect_paper_observer_cycle_v1(
        repeat('a', 64),
        '90000000-0000-0000-0000-000000000010',
        2,
        '90000000-0000-0000-0000-000000000020'
    );

    BEGIN
        PERFORM * FROM public.paper_observer_cycles;
        RAISE EXCEPTION 'observer login read an evidence table directly';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
    BEGIN
        PERFORM public.finalize_reconciliation_v2(
            'paper', repeat('a', 64),
            '90000000-0000-0000-0000-000000000010', 2,
            '90000000-0000-0000-0000-000000000099', clock_timestamp(),
            'clean', TRUE, NULL, repeat('f', 64)
        );
        RAISE EXCEPTION 'observer login could finalize execution reconciliation';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
END;
$$;

RESET SESSION AUTHORIZATION;
REVOKE alpaca_trader_observer FROM alpaca_trader_observer_test_login;
DROP ROLE alpaca_trader_observer_test_login;

DO $$
BEGIN
    BEGIN
        UPDATE public.paper_observer_schema_attestations
        SET definition_sha256 = repeat('0', 64)
        WHERE object_kind = 'constraint'
          AND object_identity = 'application.json_hash_profile';
        RAISE EXCEPTION 'observer attestation was mutable';
    EXCEPTION
        WHEN raise_exception THEN
            IF SQLERRM = 'observer attestation was mutable' THEN
                RAISE;
            END IF;
    END;
END;
$$;
