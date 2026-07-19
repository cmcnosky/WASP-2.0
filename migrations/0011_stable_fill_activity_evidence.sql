BEGIN;

-- This runtime has not been deployed. Refuse to attach new semantics to any
-- prior fill row. The already-attested fills.raw_hash continues to identify
-- the exact provider response page; stable activity identity lives in a new
-- append-only companion relation so the observer's fills relation attestation
-- remains unchanged.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.fills LIMIT 1) THEN
        RAISE EXCEPTION
            'stable fill activity evidence requires an empty predeployment fills table';
    END IF;
END;
$$;

CREATE TABLE public.fill_activity_evidence (
    fill_id TEXT PRIMARY KEY REFERENCES public.fills(fill_id),
    activity_evidence_hash TEXT NOT NULL
        CHECK (activity_evidence_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TRIGGER fill_activity_evidence_reject_mutation
BEFORE UPDATE OR DELETE ON public.fill_activity_evidence
FOR EACH ROW EXECUTE FUNCTION public.reject_audit_mutation();

CREATE FUNCTION public.enforce_fill_activity_evidence()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.fill_activity_evidence AS evidence
        WHERE evidence.fill_id = NEW.fill_id
    ) THEN
        RAISE EXCEPTION 'fill lacks stable activity evidence';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER fills_require_activity_evidence
AFTER INSERT ON public.fills
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.enforce_fill_activity_evidence();

CREATE FUNCTION public.insert_fill_v3(
    p_environment TEXT, p_account_fingerprint TEXT, p_owner_id UUID,
    p_fencing_token BIGINT, p_fill_id TEXT, p_broker_order_id TEXT,
    p_intent_id UUID, p_quantity NUMERIC, p_price NUMERIC, p_fee NUMERIC,
    p_executed_at TIMESTAMPTZ, p_received_at TIMESTAMPTZ,
    p_raw_hash TEXT, p_activity_evidence_hash TEXT
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
    ) THEN
        RAISE EXCEPTION 'fill write lacks current execution authority';
    END IF;
    IF p_fill_id IS NULL OR btrim(p_fill_id) = ''
       OR p_quantity <= 0 OR p_price <= 0 OR p_fee < 0
       OR p_received_at < p_executed_at
       OR p_raw_hash !~ '^[0-9a-f]{64}$'
       OR p_activity_evidence_hash !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION 'fill write evidence is incomplete or invalid';
    END IF;

    INSERT INTO public.fills (
        fill_id, broker_order_id, intent_id, symbol, side, quantity, price,
        fee, executed_at, received_at, raw_hash
    )
    SELECT p_fill_id, p_broker_order_id, intent.intent_id, intent.symbol,
           intent.side, p_quantity, p_price, p_fee,
           p_executed_at, p_received_at, p_raw_hash
    FROM public.order_intents AS intent
    WHERE intent.intent_id = p_intent_id;
    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    IF v_inserted <> 1 THEN
        RAISE EXCEPTION 'fill parent is absent';
    END IF;

    INSERT INTO public.fill_activity_evidence (
        fill_id, activity_evidence_hash
    ) VALUES (
        p_fill_id, p_activity_evidence_hash
    );
    RETURN TRUE;
END;
$$;

INSERT INTO public.runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'function', signature,
    encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(signature)), 'UTF8')), 'hex')
FROM (VALUES
    ('public.enforce_fill_activity_evidence()'),
    ('public.insert_fill_v3(text,text,uuid,bigint,text,text,uuid,numeric,numeric,numeric,timestamp with time zone,timestamp with time zone,text,text)')
) AS required_function(signature);

INSERT INTO public.runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'trigger', namespace.nspname || '.' || relation.relname || '.' || trigger.tgname,
    encode(sha256(convert_to(pg_get_triggerdef(trigger.oid, true), 'UTF8')), 'hex')
FROM pg_trigger AS trigger
JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND (
      (relation.relname = 'fills'
       AND trigger.tgname = 'fills_require_activity_evidence')
      OR (relation.relname = 'fill_activity_evidence'
          AND trigger.tgname = 'fill_activity_evidence_reject_mutation')
  )
  AND NOT trigger.tgisinternal;

INSERT INTO public.runtime_schema_attestations (
    object_kind, object_identity, definition_sha256
)
SELECT
    'constraint', namespace.nspname || '.' || relation.relname || '.' || con.conname,
    encode(sha256(convert_to(pg_get_constraintdef(con.oid, true), 'UTF8')), 'hex')
FROM pg_constraint AS con
JOIN pg_class AS relation ON relation.oid = con.conrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public'
  AND relation.relname = 'fill_activity_evidence';

REVOKE ALL ON public.fill_activity_evidence
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator;
GRANT SELECT ON public.fill_activity_evidence
TO alpaca_trader_runtime, alpaca_trader_operator;

REVOKE ALL ON FUNCTION
    public.enforce_fill_activity_evidence(),
    public.insert_fill_v3(
        TEXT, TEXT, UUID, BIGINT, TEXT, TEXT, UUID, NUMERIC, NUMERIC, NUMERIC,
        TIMESTAMPTZ, TIMESTAMPTZ, TEXT, TEXT
    )
FROM PUBLIC, alpaca_trader_runtime, alpaca_trader_operator;

REVOKE EXECUTE ON FUNCTION public.insert_fill_v2(
    TEXT, TEXT, UUID, BIGINT, TEXT, TEXT, UUID, NUMERIC, NUMERIC, NUMERIC,
    TIMESTAMPTZ, TIMESTAMPTZ, TEXT
) FROM alpaca_trader_runtime;

GRANT EXECUTE ON FUNCTION public.insert_fill_v3(
    TEXT, TEXT, UUID, BIGINT, TEXT, TEXT, UUID, NUMERIC, NUMERIC, NUMERIC,
    TIMESTAMPTZ, TIMESTAMPTZ, TEXT, TEXT
) TO alpaca_trader_runtime;

COMMIT;
