\set ON_ERROR_STOP on

DO $$
DECLARE
    v_completed_at TIMESTAMPTZ;
    v_difference_count BIGINT;
    v_fill_quantity NUMERIC(38, 6);
    v_fill_count BIGINT;
BEGIN
    SELECT completed_at
    INTO v_completed_at
    FROM reconciliation_runs
    WHERE reconciliation_id = '76000000-0000-0000-0000-000000000090';

    SELECT count(*)
    INTO v_difference_count
    FROM reconciliation_diffs
    WHERE reconciliation_id = '76000000-0000-0000-0000-000000000090';

    IF v_completed_at IS NOT NULL OR v_difference_count <> 1 THEN
        RAISE EXCEPTION
            'reconciliation race was not serialized: completed=%, differences=%',
            v_completed_at,
            v_difference_count;
    END IF;

    SELECT COALESCE(sum(quantity), 0), count(*)
    INTO v_fill_quantity, v_fill_count
    FROM fills
    WHERE broker_order_id = 'broker-confirmed-test';

    IF v_fill_quantity <> 1 OR v_fill_count <> 1 THEN
        RAISE EXCEPTION
            'fill race exceeded the intent: quantity=%, fills=%',
            v_fill_quantity,
            v_fill_count;
    END IF;
END;
$$;
