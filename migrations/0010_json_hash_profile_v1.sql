BEGIN;

-- This is a predeployment cutover. Refuse to stamp v1 onto any database that
-- already contains runtime, broker, research, or observer state. The two
-- attestation manifests are the only seeded tables in a fresh migrated store.
DO $$
DECLARE
    v_table RECORD;
    v_has_rows BOOLEAN;
BEGIN
    FOR v_table IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name NOT IN (
              'runtime_schema_attestations',
              'paper_observer_schema_attestations'
          )
        ORDER BY table_name
    LOOP
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM public.%I LIMIT 1)',
            v_table.table_name
        ) INTO v_has_rows;
        IF v_has_rows THEN
            RAISE EXCEPTION
                'json hash profile v1 requires an empty predeployment database';
        END IF;
    END LOOP;
END;
$$;

-- SHA-256("wasp-json-sha256-v1"). Both compiled schema verifiers synthesize
-- the same reserved observation, so new binary/old DB and old binary/new DB
-- combinations fail closed before any execution or recovery capability exists.
INSERT INTO runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
) VALUES (
    'constraint',
    'application.json_hash_profile',
    '0372e64987504c848a5146bbf31d5123e4e9e09dac09f57d150ede3b767eab45'
);

INSERT INTO paper_observer_schema_attestations (
    object_kind, object_identity, definition_sha256
) VALUES (
    'constraint',
    'application.json_hash_profile',
    '0372e64987504c848a5146bbf31d5123e4e9e09dac09f57d150ede3b767eab45'
);

COMMIT;
