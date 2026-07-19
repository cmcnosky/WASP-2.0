\set ON_ERROR_STOP on

INSERT INTO reconciliation_runs (
    reconciliation_id,
    environment,
    account_fingerprint,
    trigger,
    kill_event_id,
    fencing_token,
    started_at
) VALUES (
    '76000000-0000-0000-0000-000000000090',
    'paper',
    'lease-test',
    'manual',
    '77000000-0000-0000-0000-000000000001',
    2,
    clock_timestamp()
);
