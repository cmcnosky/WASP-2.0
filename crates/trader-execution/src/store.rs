//! PostgreSQL-backed durable execution authority.
//!
//! The caller owns connection establishment and must supply a `Client` whose
//! transport was authenticated with certificate and hostname verification.
//! This module deliberately accepts no DSN, username, password, or secret so
//! credentials cannot enter command arguments, logs, fixtures, or this API.

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use thiserror::Error;
use tokio_postgres::{Client, IsolationLevel, Row, Transaction};
use trader_core::{
    evaluate_decision, materialize_order_intent, AccountSnapshot, AccountStatus, ActivationPermit,
    BrokerEvent, DecisionSnapshot, Environment, FreshExecutionQuote, HashDigest, Money,
    OrderIntent, OrderPlan, OrderSide, Price, ReconciliationDifferenceKind, ReconciliationReport,
    RiskDecision, RiskDisposition, StrategyRelease, TargetPortfolio, TimeInForce, WholeQuantity,
};
use uuid::Uuid;

const MAX_LEASE_TTL: Duration = Duration::from_secs(60);

const ACQUIRE_LEASE_SQL: &str = r#"
WITH acquired AS (
    SELECT public.acquire_executor_lease(
        $1,
        $2,
        $3,
        $4::bigint * INTERVAL '1 microsecond'
    ) AS fencing_token
)
SELECT acquired.fencing_token, lease.lease_until
FROM acquired
JOIN public.executor_leases AS lease
  ON lease.environment = $1
 AND lease.account_fingerprint = $2
 AND lease.owner_id = $3
 AND lease.fencing_token = acquired.fencing_token
WHERE acquired.fencing_token IS NOT NULL
"#;

const RENEW_LEASE_SQL: &str = r#"
WITH renewed AS (
    SELECT public.renew_executor_lease(
        $1,
        $2,
        $3,
        $4,
        $5::bigint * INTERVAL '1 microsecond'
    ) AS renewed
)
SELECT lease.fencing_token, lease.lease_until
FROM renewed
JOIN public.executor_leases AS lease
  ON lease.environment = $1
 AND lease.account_fingerprint = $2
 AND lease.owner_id = $3
 AND lease.fencing_token = $4
WHERE renewed.renewed
"#;

const ASSERT_CURRENT_LEASE_SQL: &str =
    "SELECT public.assert_current_executor_lease($1, $2, $3, $4) AS current";
const CLAIM_FIRST_DISPATCH_SQL: &str =
    "SELECT * FROM public.claim_order_outbox_v2($1, $2, $3, $4, $5)";
const CLAIM_RECOVERY_SQL: &str =
    "SELECT * FROM public.claim_order_outbox_recovery_v2($1, $2, $3, $4, $5)";
const CLAIM_COMPLETION_SQL: &str =
    "SELECT * FROM public.claim_order_outbox_completion_v2($1, $2, $3, $4, $5)";
const FINALIZE_OUTBOX_SQL: &str =
    "SELECT public.finalize_order_outbox_v2($1, $2, $3, $4, $5, $6) AS finalized";
const APPEND_SUBMISSION_UNKNOWN_SQL: &str = r#"
SELECT public.append_submission_unknown_v2(
    $1, $2, $3, $4, $5, $6, $7, $8, $9
) AS appended
"#;
const LIST_UNRESOLVED_OUTBOXES_SQL: &str = r#"
SELECT * FROM public.list_unresolved_order_outboxes_v2($1, $2, $3, $4, $5)
"#;

const SCHEMA_COLUMNS_SQL: &str = r#"
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    numeric_precision::integer AS numeric_precision,
    numeric_scale::integer AS numeric_scale
FROM information_schema.columns
WHERE table_schema = 'public'
"#;

const SCHEMA_GUARDS_SQL: &str = r#"
WITH required_function(signature) AS (
    VALUES
        ('public.acquire_executor_lease(text,text,uuid,interval)'),
        ('public.renew_executor_lease(text,text,uuid,bigint,interval)'),
        ('public.assert_current_executor_lease(text,text,uuid,bigint)'),
        ('public.claim_order_outbox_v2(text,text,uuid,uuid,bigint)'),
        ('public.claim_order_outbox_recovery_v2(text,text,uuid,uuid,bigint)'),
        ('public.claim_order_outbox_completion_v2(text,text,uuid,uuid,bigint)'),
        ('public.append_submission_unknown_v2(text,text,uuid,uuid,bigint,uuid,text,jsonb,timestamp with time zone)'),
        ('public.finalize_order_outbox_v2(text,text,uuid,uuid,bigint,text)'),
        ('public.list_unresolved_order_outboxes_v2(text,text,uuid,bigint,integer)'),
        ('public.insert_decision_snapshot_v2(text,text,uuid,bigint,uuid,uuid,date,timestamp with time zone,text,text,jsonb)'),
        ('public.insert_target_portfolio_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text)'),
        ('public.insert_target_position_v2(text,text,uuid,bigint,uuid,text,bigint,numeric,text)'),
        ('public.insert_risk_decision_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text[],jsonb,text,timestamp with time zone)'),
        ('public.insert_order_plan_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text,bigint,numeric,text,timestamp with time zone)'),
        ('public.insert_order_intent_v2(text,text,uuid,bigint,uuid,uuid,uuid,uuid,text,text,text,bigint,numeric,text,timestamp with time zone,numeric,timestamp with time zone,timestamp with time zone,timestamp with time zone,text,text,text,timestamp with time zone)'),
        ('public.insert_intent_state_v2(text,text,uuid,bigint,uuid,uuid,text,text,jsonb,timestamp with time zone)'),
        ('public.insert_order_outbox_v2(text,text,uuid,bigint,uuid,uuid,jsonb,timestamp with time zone)'),
        ('public.insert_broker_order_v2(text,text,uuid,bigint,text,uuid,text,timestamp with time zone,text)'),
        ('public.insert_fill_v2(text,text,uuid,bigint,text,text,uuid,numeric,numeric,numeric,timestamp with time zone,timestamp with time zone,text)'),
        ('public.insert_broker_event_v2(text,text,uuid,bigint,uuid,text,text,text,boolean,numeric,numeric,timestamp with time zone,timestamp with time zone,text,jsonb,text)'),
        ('public.insert_account_snapshot_v2(text,text,uuid,bigint,uuid,timestamp with time zone,timestamp with time zone,text,boolean,numeric,numeric,numeric,boolean,boolean,boolean,jsonb,text)'),
        ('public.insert_reconciliation_run_v2(text,text,uuid,bigint,uuid,text,uuid,timestamp with time zone)'),
        ('public.insert_reconciliation_diff_v2(text,text,uuid,bigint,uuid,uuid,text,text,text)'),
        ('public.finalize_reconciliation_v2(text,text,uuid,bigint,uuid,timestamp with time zone,text,boolean,uuid,text)'),
        ('public.record_runtime_kill_event_v2(text,text,uuid,bigint,uuid,text,text,jsonb,text,timestamp with time zone)'),
        ('public.enforce_intent_state_transition()'),
        ('public.enforce_order_outbox_chain()'),
        ('public.enforce_broker_event_chain()'),
        ('public.enforce_broker_event_fill_truth()'),
        ('public.enforce_terminal_intent_fill_truth()'),
        ('public.enforce_fill_chain()'),
        ('public.enforce_reconciliation_report()'),
        ('public.enforce_target_portfolio_chain()'),
        ('public.enforce_risk_decision_chain()'),
        ('public.enforce_order_plan_chain()'),
        ('public.enforce_order_intent_chain()'),
        ('public.enforce_broker_order_chain()'),
        ('public.prevent_late_reconciliation_diff()'),
        ('public.reject_audit_mutation()')
), critical_table(table_name) AS (
    VALUES
        ('decision_snapshots'), ('target_portfolios'), ('target_positions'),
        ('risk_decisions'), ('order_plans'), ('order_intents'),
        ('intent_state_events'), ('order_outbox'), ('broker_orders'),
        ('broker_order_events'), ('fills'), ('account_snapshots'),
        ('reconciliation_runs'), ('reconciliation_diffs'),
        ('runtime_schema_attestations')
), observed(object_kind, object_identity, definition_sha256, safe) AS (
    SELECT
        'function', required.signature,
        CASE WHEN procedure.oid IS NULL THEN NULL ELSE
            encode(sha256(convert_to(pg_get_functiondef(procedure.oid), 'UTF8')), 'hex')
        END,
        COALESCE(
            procedure.prosecdef
            AND procedure.prokind = 'f'
            AND procedure.proconfig @> ARRAY['search_path=pg_catalog, public']::text[],
            FALSE
        )
    FROM required_function AS required
    LEFT JOIN pg_proc AS procedure ON procedure.oid = to_regprocedure(required.signature)
    UNION ALL
    SELECT
        'view', 'public.execution_readiness',
        CASE WHEN relation.oid IS NULL THEN NULL ELSE
            encode(sha256(convert_to(pg_get_viewdef(relation.oid, true), 'UTF8')), 'hex')
        END,
        COALESCE(relation.relkind = 'v', FALSE)
    FROM (SELECT to_regclass('public.execution_readiness') AS oid) AS expected
    LEFT JOIN pg_class AS relation ON relation.oid = expected.oid
    UNION ALL
    SELECT
        'trigger', namespace.nspname || '.' || relation.relname || '.' || trigger.tgname,
        encode(sha256(convert_to(pg_get_triggerdef(trigger.oid, true), 'UTF8')), 'hex'),
        trigger.tgenabled IN ('O', 'A')
    FROM pg_trigger AS trigger
    JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    JOIN critical_table AS critical ON critical.table_name = relation.relname
    WHERE namespace.nspname = 'public' AND NOT trigger.tgisinternal
    UNION ALL
    SELECT
        'constraint', namespace.nspname || '.' || relation.relname || '.' || con.conname,
        encode(sha256(convert_to(pg_get_constraintdef(con.oid, true), 'UTF8')), 'hex'),
        con.convalidated
    FROM pg_constraint AS con
    JOIN pg_class AS relation ON relation.oid = con.conrelid
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    JOIN critical_table AS critical ON critical.table_name = relation.relname
    WHERE namespace.nspname = 'public'
), compared AS (
    SELECT
        COALESCE(manifest.object_kind, observed.object_kind) AS object_kind,
        COALESCE(manifest.object_identity, observed.object_identity) AS object_identity,
        manifest.definition_sha256 AS expected_hash,
        observed.definition_sha256 AS observed_hash,
        COALESCE(observed.safe, FALSE) AS safe
    FROM public.runtime_schema_attestations AS manifest
    FULL OUTER JOIN observed
      ON observed.object_kind = manifest.object_kind
     AND observed.object_identity = manifest.object_identity
)
SELECT
    'attestation:' || object_kind || ':' || object_identity AS object_name,
    expected_hash IS NOT NULL AND expected_hash = observed_hash AND safe AS present
FROM compared
"#;

const RUNTIME_PRIVILEGES_SQL: &str = r#"
WITH allowed_function(signature) AS (
    VALUES
        ('public.acquire_executor_lease(text,text,uuid,interval)'),
        ('public.renew_executor_lease(text,text,uuid,bigint,interval)'),
        ('public.assert_current_executor_lease(text,text,uuid,bigint)'),
        ('public.claim_order_outbox_v2(text,text,uuid,uuid,bigint)'),
        ('public.claim_order_outbox_recovery_v2(text,text,uuid,uuid,bigint)'),
        ('public.claim_order_outbox_completion_v2(text,text,uuid,uuid,bigint)'),
        ('public.append_submission_unknown_v2(text,text,uuid,uuid,bigint,uuid,text,jsonb,timestamp with time zone)'),
        ('public.finalize_order_outbox_v2(text,text,uuid,uuid,bigint,text)'),
        ('public.list_unresolved_order_outboxes_v2(text,text,uuid,bigint,integer)'),
        ('public.insert_decision_snapshot_v2(text,text,uuid,bigint,uuid,uuid,date,timestamp with time zone,text,text,jsonb)'),
        ('public.insert_target_portfolio_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text)'),
        ('public.insert_target_position_v2(text,text,uuid,bigint,uuid,text,bigint,numeric,text)'),
        ('public.insert_risk_decision_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text[],jsonb,text,timestamp with time zone)'),
        ('public.insert_order_plan_v2(text,text,uuid,bigint,uuid,uuid,uuid,text,text,bigint,numeric,text,timestamp with time zone)'),
        ('public.insert_order_intent_v2(text,text,uuid,bigint,uuid,uuid,uuid,uuid,text,text,text,bigint,numeric,text,timestamp with time zone,numeric,timestamp with time zone,timestamp with time zone,timestamp with time zone,text,text,text,timestamp with time zone)'),
        ('public.insert_intent_state_v2(text,text,uuid,bigint,uuid,uuid,text,text,jsonb,timestamp with time zone)'),
        ('public.insert_order_outbox_v2(text,text,uuid,bigint,uuid,uuid,jsonb,timestamp with time zone)'),
        ('public.insert_broker_order_v2(text,text,uuid,bigint,text,uuid,text,timestamp with time zone,text)'),
        ('public.insert_fill_v2(text,text,uuid,bigint,text,text,uuid,numeric,numeric,numeric,timestamp with time zone,timestamp with time zone,text)'),
        ('public.insert_broker_event_v2(text,text,uuid,bigint,uuid,text,text,text,boolean,numeric,numeric,timestamp with time zone,timestamp with time zone,text,jsonb,text)'),
        ('public.insert_account_snapshot_v2(text,text,uuid,bigint,uuid,timestamp with time zone,timestamp with time zone,text,boolean,numeric,numeric,numeric,boolean,boolean,boolean,jsonb,text)'),
        ('public.insert_reconciliation_run_v2(text,text,uuid,bigint,uuid,text,uuid,timestamp with time zone)'),
        ('public.insert_reconciliation_diff_v2(text,text,uuid,bigint,uuid,uuid,text,text,text)'),
        ('public.finalize_reconciliation_v2(text,text,uuid,bigint,uuid,timestamp with time zone,text,boolean,uuid,text)'),
        ('public.record_runtime_kill_event_v2(text,text,uuid,bigint,uuid,text,text,jsonb,text,timestamp with time zone)')
), checks(object_name, present) AS (
    SELECT
        'role:current-login',
        login.rolcanlogin
        AND login.rolinherit
        AND NOT login.rolsuper
        AND NOT login.rolcreatedb
        AND NOT login.rolcreaterole
        AND NOT login.rolreplication
        AND NOT login.rolbypassrls
        AND current_user = session_user
        AND current_user <> 'alpaca_trader_runtime'
        AND pg_has_role(current_user, 'alpaca_trader_runtime', 'USAGE')
        AND NOT pg_has_role(current_user, 'alpaca_trader_operator', 'MEMBER')
    FROM pg_roles AS login WHERE login.rolname = current_user
    UNION ALL
    SELECT
        'role:alpaca_trader_runtime',
        NOT runtime.rolcanlogin
        AND NOT runtime.rolinherit
        AND NOT runtime.rolsuper
        AND NOT runtime.rolcreatedb
        AND NOT runtime.rolcreaterole
        AND NOT runtime.rolreplication
        AND NOT runtime.rolbypassrls
    FROM pg_roles AS runtime WHERE runtime.rolname = 'alpaca_trader_runtime'
    UNION ALL
    SELECT 'database:connect-only',
        has_database_privilege(current_user, current_database(), 'CONNECT')
        AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
        AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
    UNION ALL
    SELECT 'schema:public:usage-only',
        has_schema_privilege(current_user, 'public', 'USAGE')
        AND NOT has_schema_privilege(current_user, 'public', 'CREATE')
    UNION ALL
    SELECT
        'relation:' || namespace.nspname || '.' || relation.relname || ':select-only',
        has_table_privilege(current_user, relation.oid, 'SELECT')
        AND NOT has_table_privilege(current_user, relation.oid, 'INSERT')
        AND NOT has_table_privilege(current_user, relation.oid, 'UPDATE')
        AND NOT has_table_privilege(current_user, relation.oid, 'DELETE')
        AND NOT has_table_privilege(current_user, relation.oid, 'TRUNCATE')
        AND NOT has_table_privilege(current_user, relation.oid, 'REFERENCES')
        AND NOT has_table_privilege(current_user, relation.oid, 'TRIGGER')
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'public' AND relation.relkind IN ('r', 'p', 'v', 'm')
    UNION ALL
    SELECT
        'sequence:' || namespace.nspname || '.' || sequence.relname || ':none',
        NOT has_sequence_privilege(current_user, sequence.oid, 'USAGE')
        AND NOT has_sequence_privilege(current_user, sequence.oid, 'SELECT')
        AND NOT has_sequence_privilege(current_user, sequence.oid, 'UPDATE')
    FROM pg_class AS sequence
    JOIN pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
    WHERE namespace.nspname = 'public' AND sequence.relkind = 'S'
    UNION ALL
    SELECT
        'function:' || allowed.signature || ':execute',
        to_regprocedure(allowed.signature) IS NOT NULL
        AND has_function_privilege(current_user, to_regprocedure(allowed.signature), 'EXECUTE')
    FROM allowed_function AS allowed
    UNION ALL
    SELECT
        'function:' || procedure.oid::regprocedure::text || ':forbidden-execute',
        FALSE
    FROM pg_proc AS procedure
    JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname = 'public'
      AND procedure.prokind IN ('f', 'p')
      AND has_function_privilege(current_user, procedure.oid, 'EXECUTE')
      AND NOT EXISTS (
          SELECT 1 FROM allowed_function AS allowed
          WHERE to_regprocedure(allowed.signature) = procedure.oid
      )
)
SELECT object_name, present FROM checks
"#;

const INSERT_DECISION_SQL: &str =
    "SELECT public.insert_decision_snapshot_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)";
const INSERT_TARGET_SQL: &str =
    "SELECT public.insert_target_portfolio_v2($1,$2,$3,$4,$5,$6,$7,$8,$9)";
const INSERT_TARGET_POSITION_SQL: &str =
    "SELECT public.insert_target_position_v2($1,$2,$3,$4,$5,$6,$7,$8::text::numeric,$9)";
const INSERT_RISK_SQL: &str =
    "SELECT public.insert_risk_decision_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)";
const INSERT_PLAN_SQL: &str =
    "SELECT public.insert_order_plan_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::numeric,$12,$13)";
const INSERT_INTENT_SQL: &str = r#"
SELECT public.insert_order_intent_v2(
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::text::numeric,$14,$15,
    $16::text::numeric,$17,$18,$19,$20,$21,$22,$23
)
"#;
const INSERT_INTENT_STATE_SQL: &str =
    "SELECT public.insert_intent_state_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)";
const INSERT_OUTBOX_SQL: &str = "SELECT public.insert_order_outbox_v2($1,$2,$3,$4,$5,$6,$7,$8)";
const INSERT_BROKER_ORDER_SQL: &str =
    "SELECT public.insert_broker_order_v2($1,$2,$3,$4,$5,$6,$7,$8,$9)";
const INSERT_BROKER_EVENT_SQL: &str = r#"
SELECT public.insert_broker_event_v2(
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text::numeric,$11::text::numeric,
    $12,$13,$14,$15,$16
)
"#;
const INSERT_FILL_SQL: &str = r#"
SELECT public.insert_fill_v2(
    $1,$2,$3,$4,$5,$6,$7,$8::text::numeric,$9::text::numeric,
    $10::text::numeric,$11,$12,$13
)
"#;
const INSERT_ACCOUNT_SNAPSHOT_SQL: &str = r#"
SELECT public.insert_account_snapshot_v2(
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text::numeric,$11::text::numeric,
    $12::text::numeric,$13,$14,$15,$16,$17
)
"#;
const INSERT_RECONCILIATION_SQL: &str =
    "SELECT public.insert_reconciliation_run_v2($1,$2,$3,$4,$5,$6,$7,$8)";
const INSERT_RECONCILIATION_DIFF_SQL: &str =
    "SELECT public.insert_reconciliation_diff_v2($1,$2,$3,$4,$5,$6,$7,$8,$9)";
const FINALIZE_RECONCILIATION_SQL: &str =
    "SELECT public.finalize_reconciliation_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("unsafe PostgreSQL store configuration: {0}")]
    UnsafeConfiguration(String),
    #[error("invalid PostgreSQL store input: {0}")]
    InvalidInput(String),
    #[error("PostgreSQL schema contract mismatch: {0:?}")]
    SchemaMismatch(Vec<String>),
    #[error("PostgreSQL operation {operation} failed")]
    Database {
        operation: &'static str,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("PostgreSQL commit outcome for {operation} is unknown; resolve by deterministic key")]
    CommitUnknown {
        operation: &'static str,
        recovery: Box<CommitRecoveryKey>,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("serialization for PostgreSQL persistence failed")]
    Serialization(#[source] serde_json::Error),
}

impl StoreError {
    fn database(operation: &'static str, source: tokio_postgres::Error) -> Self {
        Self::Database { operation, source }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitRecoveryKey {
    ExecutionChain {
        decision_id: Uuid,
        target_portfolio_id: Uuid,
        risk_decision_id: Uuid,
        order_plan_id: Uuid,
        intent_id: Uuid,
        outbox_id: Uuid,
        decision_payload_hash: HashDigest,
        target_payload_hash: HashDigest,
        risk_limits_hash: HashDigest,
        outbox_payload_hash: HashDigest,
    },
    BrokerEvent {
        broker_event_id: Uuid,
        raw_payload_hash: HashDigest,
        cumulative_filled_quantity: WholeQuantity,
    },
    Reconciliation {
        reconciliation_id: Uuid,
        evidence_hash: HashDigest,
    },
    SubmissionUnknown {
        outbox_id: Uuid,
        state_event_id: Uuid,
        evidence_hash: HashDigest,
    },
    OutboxFinalization {
        outbox_id: Uuid,
        completion_reason: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitResolution {
    Committed,
    NotCommitted,
    ConflictingEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseTrustAnchor {
    PinnedBundleSha256(HashDigest),
}

/// Non-secret proof obligation for a caller-supplied, TLS-authenticated client.
/// There is intentionally no plaintext/disable/prefer TLS variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TlsRequiredDatabaseConfig {
    pub(crate) environment: Environment,
    pub(crate) server_name: String,
    pub(crate) trust_anchor: DatabaseTrustAnchor,
}

impl TlsRequiredDatabaseConfig {
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        let DatabaseTrustAnchor::PinnedBundleSha256(bundle_digest) = &self.trust_anchor;
        debug_assert_eq!(bundle_digest.as_hex().len(), 64);
        let server_name = self.server_name.trim();
        if server_name.is_empty()
            || server_name.contains(['/', '@', ':'])
            || !server_name.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '-')
            })
        {
            return Err(StoreError::UnsafeConfiguration(
                "server_name must be a bare DNS name used for TLS hostname verification".into(),
            ));
        }
        if server_name.starts_with('.')
            || server_name.ends_with('.')
            || server_name.split('.').any(str::is_empty)
        {
            return Err(StoreError::UnsafeConfiguration(
                "server_name is not a valid TLS DNS name".into(),
            ));
        }
        Ok(())
    }
}

pub struct PgExecutionStore {
    client: Client,
    config: TlsRequiredDatabaseConfig,
}

impl PgExecutionStore {
    /// Accept a caller-connected client only after the caller has enforced the
    /// represented certificate and hostname checks. Construction immediately
    /// validates the compiled migration/schema contract and fails closed.
    pub(crate) async fn from_verified_tls_client(
        client: Client,
        config: TlsRequiredDatabaseConfig,
    ) -> Result<Self, StoreError> {
        config.validate()?;
        let store = Self { client, config };
        store.verify_schema().await?;
        Ok(store)
    }

    pub fn environment(&self) -> Environment {
        self.config.environment
    }

    pub async fn verify_schema(&self) -> Result<(), StoreError> {
        let rows = self
            .client
            .query(SCHEMA_COLUMNS_SQL, &[])
            .await
            .map_err(|error| StoreError::database("verify_schema_columns", error))?;
        let observed = rows
            .iter()
            .map(schema_column_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut mismatches = compare_schema_columns(&observed);

        let guards = self
            .client
            .query(SCHEMA_GUARDS_SQL, &[])
            .await
            .map_err(|error| StoreError::database("verify_schema_guards", error))?;
        for row in guards {
            let name: String = row
                .try_get("object_name")
                .map_err(|error| StoreError::database("decode_schema_guard", error))?;
            let present: bool = row
                .try_get("present")
                .map_err(|error| StoreError::database("decode_schema_guard", error))?;
            if !present {
                mismatches.push(name);
            }
        }
        let privileges = self
            .client
            .query(RUNTIME_PRIVILEGES_SQL, &[])
            .await
            .map_err(|error| StoreError::database("verify_runtime_privileges", error))?;
        for row in privileges {
            let name: String = row
                .try_get("object_name")
                .map_err(|error| StoreError::database("decode_runtime_privilege", error))?;
            let present: bool = row
                .try_get("present")
                .map_err(|error| StoreError::database("decode_runtime_privilege", error))?;
            if !present {
                mismatches.push(name);
            }
        }
        mismatches.sort();
        mismatches.dedup();
        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(StoreError::SchemaMismatch(mismatches))
        }
    }

    async fn claim(
        &self,
        sql: &'static str,
        operation: &'static str,
        kind: OutboxClaimKind,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError> {
        self.validate_lease_domain(lease)?;
        let token = positive_i64(lease.fencing_token, "claim fencing_token")?;
        let environment = environment_sql(lease.environment);
        let account = lease.account_fingerprint.as_hex();
        let row = self
            .client
            .query_opt(
                sql,
                &[&environment, &account, &outbox_id, &lease.owner_id, &token],
            )
            .await
            .map_err(|error| StoreError::database(operation, error))?;
        row.map(|row| decode_claimed_outbox(&row, kind, lease))
            .transpose()
    }

    fn validate_lease_domain(&self, lease: &FencedLease) -> Result<(), StoreError> {
        if lease.environment != self.config.environment {
            return Err(StoreError::InvalidInput(
                "lease environment does not match store isolation".into(),
            ));
        }
        positive_i64(lease.fencing_token, "lease fencing_token")?;
        Ok(())
    }
}

async fn assert_current_lease(
    transaction: &Transaction<'_>,
    lease: &FencedLease,
) -> Result<(), StoreError> {
    let environment = environment_sql(lease.environment);
    let account = lease.account_fingerprint.as_hex();
    let token = positive_i64(lease.fencing_token, "lease fencing_token")?;
    let current: bool = transaction
        .query_one(
            ASSERT_CURRENT_LEASE_SQL,
            &[&environment, &account, &lease.owner_id, &token],
        )
        .await
        .map_err(|error| StoreError::database("assert_current_executor_lease", error))?
        .try_get("current")
        .map_err(|error| StoreError::database("decode_current_executor_lease", error))?;
    if !current {
        return Err(StoreError::InvalidInput(
            "write lacks the current owner/account/environment execution fence".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct DurableExecutionChain<'a> {
    pub release: &'a StrategyRelease,
    pub activation_permit: &'a ActivationPermit,
    pub snapshot: &'a DecisionSnapshot,
    pub target: &'a TargetPortfolio,
    pub risk: &'a RiskDecision,
    pub plan: &'a OrderPlan,
    pub intent: &'a OrderIntent,
    pub lease: &'a FencedLease,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedExecutionChain {
    pub target_portfolio_id: Uuid,
    pub risk_decision_id: Uuid,
    pub outbox_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FencedLease {
    pub environment: Environment,
    pub account_fingerprint: HashDigest,
    pub owner_id: Uuid,
    pub fencing_token: u64,
    pub lease_until: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxClaimKind {
    FirstDispatch,
    RecoveryLookupOnly,
    TerminalCompletionOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedOutbox {
    pub kind: OutboxClaimKind,
    pub outbox_id: Uuid,
    pub intent_id: Uuid,
    pub environment: Environment,
    pub account_fingerprint: HashDigest,
    pub created_fencing_token: u64,
    pub claim_fencing_token: u64,
    pub payload: Value,
    pub available_at: DateTime<Utc>,
    pub claimed_by: Uuid,
    pub claimed_at: DateTime<Utc>,
    pub attempt_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerFill {
    pub fill_id: String,
    pub quantity: WholeQuantity,
    pub price: Price,
    pub fee: Money,
    pub executed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_payload_hash: HashDigest,
}

#[derive(Clone, Debug)]
pub struct BrokerEventWrite<'a> {
    pub intent_id: &'a str,
    pub event: &'a BrokerEvent,
    pub raw_payload: &'a Value,
    pub fills: &'a [BrokerFill],
    pub lease: &'a FencedLease,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerWriteResult {
    pub broker_event_id: Uuid,
    pub duplicate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconciliationTrigger {
    Startup,
    Reconnect,
    SessionOpen,
    SessionClose,
    AmbiguousSubmission,
    Manual,
    Restore,
    Failover,
}

impl ReconciliationTrigger {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Reconnect => "reconnect",
            Self::SessionOpen => "session_open",
            Self::SessionClose => "session_close",
            Self::AmbiguousSubmission => "ambiguous_submission",
            Self::Manual => "manual",
            Self::Restore => "restore",
            Self::Failover => "failover",
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccountSnapshotEvidence<'a> {
    pub snapshot: &'a AccountSnapshot,
    pub broker_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub transfers_blocked: bool,
    pub account_blocked: bool,
}

#[derive(Clone, Debug)]
pub struct ReconciliationWrite<'a> {
    pub report: &'a ReconciliationReport,
    pub trigger: ReconciliationTrigger,
    pub kill_event_id: &'a str,
    pub started_at: DateTime<Utc>,
    pub account: AccountSnapshotEvidence<'a>,
    pub lease: &'a FencedLease,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedOutbox {
    pub outbox_id: Uuid,
    pub intent_id: Uuid,
    pub created_fencing_token: u64,
    pub payload: Value,
    pub available_at: DateTime<Utc>,
    pub current_state: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedReconciliation {
    pub reconciliation_id: Uuid,
    pub account_snapshot_id: Uuid,
}

#[async_trait]
pub trait ExecutionStore: Send {
    async fn persist_execution_chain(
        &mut self,
        chain: &DurableExecutionChain<'_>,
    ) -> Result<PersistedExecutionChain, StoreError>;

    async fn acquire_lease(
        &mut self,
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<FencedLease>, StoreError>;

    async fn renew_lease(
        &mut self,
        lease: &FencedLease,
        ttl: Duration,
    ) -> Result<Option<FencedLease>, StoreError>;

    async fn claim_first_dispatch(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError>;

    async fn claim_recovery(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError>;

    async fn claim_terminal_completion(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError>;

    async fn discover_unresolved_outboxes(
        &mut self,
        lease: &FencedLease,
        limit: u16,
    ) -> Result<Vec<UnresolvedOutbox>, StoreError>;

    async fn append_submission_unknown(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        reason_code: &str,
        detail: &Value,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, StoreError>;

    async fn finalize_outbox(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        completion_reason: &str,
    ) -> Result<bool, StoreError>;

    async fn record_broker_event(
        &mut self,
        write: &BrokerEventWrite<'_>,
    ) -> Result<BrokerWriteResult, StoreError>;

    async fn record_reconciliation(
        &mut self,
        write: &ReconciliationWrite<'_>,
    ) -> Result<PersistedReconciliation, StoreError>;

    async fn resolve_commit(
        &mut self,
        key: &CommitRecoveryKey,
    ) -> Result<CommitResolution, StoreError>;
}

#[async_trait]
impl ExecutionStore for PgExecutionStore {
    async fn persist_execution_chain(
        &mut self,
        chain: &DurableExecutionChain<'_>,
    ) -> Result<PersistedExecutionChain, StoreError> {
        let prepared = PreparedChain::new(chain, self.config.environment)?;
        let recovery = prepared.commit_recovery_key();
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|error| StoreError::database("begin_execution_chain", error))?;
        assert_current_lease(&transaction, chain.lease).await?;
        verify_database_release_and_permit(&transaction, chain, &prepared).await?;
        insert_execution_chain(&transaction, chain, &prepared, self.config.environment).await?;
        assert_current_lease(&transaction, chain.lease).await?;
        if let Err(source) = transaction.commit().await {
            return Err(StoreError::CommitUnknown {
                operation: "commit_execution_chain",
                recovery: Box::new(recovery),
                source,
            });
        }
        Ok(PersistedExecutionChain {
            target_portfolio_id: prepared.target_portfolio_id,
            risk_decision_id: prepared.risk_decision_id,
            outbox_id: prepared.outbox_id,
        })
    }

    async fn acquire_lease(
        &mut self,
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<FencedLease>, StoreError> {
        let ttl_micros = validate_ttl(ttl)?;
        let environment = environment_sql(self.config.environment);
        let account = account_fingerprint.as_hex();
        let row = self
            .client
            .query_opt(
                ACQUIRE_LEASE_SQL,
                &[&environment, &account, &owner_id, &ttl_micros],
            )
            .await
            .map_err(|error| StoreError::database("acquire_executor_lease", error))?;
        row.map(|row| decode_lease(&row, self.config.environment, account_fingerprint, owner_id))
            .transpose()
    }

    async fn renew_lease(
        &mut self,
        lease: &FencedLease,
        ttl: Duration,
    ) -> Result<Option<FencedLease>, StoreError> {
        if lease.environment != self.config.environment {
            return Err(StoreError::InvalidInput(
                "lease environment does not match store isolation".into(),
            ));
        }
        let ttl_micros = validate_ttl(ttl)?;
        let token = positive_i64(lease.fencing_token, "lease fencing_token")?;
        let environment = environment_sql(lease.environment);
        let account = lease.account_fingerprint.as_hex();
        let row = self
            .client
            .query_opt(
                RENEW_LEASE_SQL,
                &[&environment, &account, &lease.owner_id, &token, &ttl_micros],
            )
            .await
            .map_err(|error| StoreError::database("renew_executor_lease", error))?;
        row.map(|row| {
            decode_lease(
                &row,
                lease.environment,
                lease.account_fingerprint,
                lease.owner_id,
            )
        })
        .transpose()
    }

    async fn claim_first_dispatch(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError> {
        self.claim(
            CLAIM_FIRST_DISPATCH_SQL,
            "claim_order_outbox_first_dispatch",
            OutboxClaimKind::FirstDispatch,
            outbox_id,
            lease,
        )
        .await
    }

    async fn claim_recovery(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError> {
        self.claim(
            CLAIM_RECOVERY_SQL,
            "claim_order_outbox_recovery",
            OutboxClaimKind::RecoveryLookupOnly,
            outbox_id,
            lease,
        )
        .await
    }

    async fn claim_terminal_completion(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, StoreError> {
        self.claim(
            CLAIM_COMPLETION_SQL,
            "claim_order_outbox_terminal_completion",
            OutboxClaimKind::TerminalCompletionOnly,
            outbox_id,
            lease,
        )
        .await
    }

    async fn discover_unresolved_outboxes(
        &mut self,
        lease: &FencedLease,
        limit: u16,
    ) -> Result<Vec<UnresolvedOutbox>, StoreError> {
        self.validate_lease_domain(lease)?;
        if limit == 0 || limit > 1000 {
            return Err(StoreError::InvalidInput(
                "unresolved outbox discovery limit must be 1 through 1000".into(),
            ));
        }
        let environment = environment_sql(lease.environment);
        let account = lease.account_fingerprint.as_hex();
        let token = positive_i64(lease.fencing_token, "discovery fencing_token")?;
        let limit = i32::from(limit);
        self.client
            .query(
                LIST_UNRESOLVED_OUTBOXES_SQL,
                &[&environment, &account, &lease.owner_id, &token, &limit],
            )
            .await
            .map_err(|error| StoreError::database("list_unresolved_order_outboxes", error))?
            .iter()
            .map(decode_unresolved_outbox)
            .collect()
    }

    async fn append_submission_unknown(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        reason_code: &str,
        detail: &Value,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        self.validate_lease_domain(lease)?;
        let reason = reason_code.trim();
        if reason.is_empty() || reason.len() > 128 {
            return Err(StoreError::InvalidInput(
                "submission-unknown reason must contain 1 through 128 bytes".into(),
            ));
        }
        let environment = environment_sql(lease.environment);
        let account = lease.account_fingerprint.as_hex();
        let token = positive_i64(lease.fencing_token, "submission-unknown fencing_token")?;
        let state_event_id = stable_child_uuid(outbox_id, "state:submission-unknown");
        let evidence_hash = submission_unknown_evidence_hash(reason, detail, token, occurred_at)?;
        let recovery = CommitRecoveryKey::SubmissionUnknown {
            outbox_id,
            state_event_id,
            evidence_hash,
        };
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|error| StoreError::database("begin_append_submission_unknown", error))?;
        let appended: bool = transaction
            .query_one(
                APPEND_SUBMISSION_UNKNOWN_SQL,
                &[
                    &environment,
                    &account,
                    &outbox_id,
                    &lease.owner_id,
                    &token,
                    &state_event_id,
                    &reason,
                    detail,
                    &occurred_at,
                ],
            )
            .await
            .map_err(|error| StoreError::database("append_submission_unknown", error))?
            .try_get("appended")
            .map_err(|error| StoreError::database("decode_append_submission_unknown", error))?;
        if !appended {
            transaction.rollback().await.map_err(|error| {
                StoreError::database("rollback_append_submission_unknown", error)
            })?;
            return Ok(false);
        }
        assert_current_lease(&transaction, lease).await?;
        if let Err(source) = transaction.commit().await {
            return Err(StoreError::CommitUnknown {
                operation: "commit_append_submission_unknown",
                recovery: Box::new(recovery),
                source,
            });
        }
        Ok(true)
    }

    async fn finalize_outbox(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        completion_reason: &str,
    ) -> Result<bool, StoreError> {
        let reason = completion_reason.trim();
        if reason.is_empty() || reason.len() > 128 {
            return Err(StoreError::InvalidInput(
                "outbox completion reason must contain 1 through 128 bytes".into(),
            ));
        }
        self.validate_lease_domain(lease)?;
        let token = positive_i64(lease.fencing_token, "outbox completion fencing_token")?;
        let environment = environment_sql(lease.environment);
        let account = lease.account_fingerprint.as_hex();
        let recovery = CommitRecoveryKey::OutboxFinalization {
            outbox_id,
            completion_reason: reason.to_owned(),
        };
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|error| StoreError::database("begin_finalize_order_outbox", error))?;
        let finalized: bool = transaction
            .query_one(
                FINALIZE_OUTBOX_SQL,
                &[
                    &environment,
                    &account,
                    &outbox_id,
                    &lease.owner_id,
                    &token,
                    &reason,
                ],
            )
            .await
            .map_err(|error| StoreError::database("finalize_order_outbox", error))?
            .try_get("finalized")
            .map_err(|error| StoreError::database("decode_finalize_order_outbox", error))?;
        if !finalized {
            transaction
                .rollback()
                .await
                .map_err(|error| StoreError::database("rollback_finalize_order_outbox", error))?;
            return Ok(false);
        }
        assert_current_lease(&transaction, lease).await?;
        if let Err(source) = transaction.commit().await {
            return Err(StoreError::CommitUnknown {
                operation: "commit_finalize_order_outbox",
                recovery: Box::new(recovery),
                source,
            });
        }
        Ok(true)
    }

    async fn record_broker_event(
        &mut self,
        write: &BrokerEventWrite<'_>,
    ) -> Result<BrokerWriteResult, StoreError> {
        let prepared = PreparedBrokerWrite::new(write)?;
        self.validate_lease_domain(write.lease)?;
        let recovery = prepared.commit_recovery_key(write);
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|error| StoreError::database("begin_broker_event", error))?;
        assert_current_lease(&transaction, write.lease).await?;
        let result =
            insert_broker_event(&transaction, write, &prepared, self.config.environment).await?;
        assert_current_lease(&transaction, write.lease).await?;
        if let Err(source) = transaction.commit().await {
            return Err(StoreError::CommitUnknown {
                operation: "commit_broker_event",
                recovery: Box::new(recovery),
                source,
            });
        }
        Ok(result)
    }

    async fn record_reconciliation(
        &mut self,
        write: &ReconciliationWrite<'_>,
    ) -> Result<PersistedReconciliation, StoreError> {
        let prepared = PreparedReconciliation::new(write, self.config.environment)?;
        self.validate_lease_domain(write.lease)?;
        let recovery = prepared.commit_recovery_key();
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|error| StoreError::database("begin_reconciliation", error))?;
        assert_current_lease(&transaction, write.lease).await?;
        insert_reconciliation(&transaction, write, &prepared, self.config.environment).await?;
        assert_current_lease(&transaction, write.lease).await?;
        if let Err(source) = transaction.commit().await {
            return Err(StoreError::CommitUnknown {
                operation: "commit_reconciliation",
                recovery: Box::new(recovery),
                source,
            });
        }
        Ok(PersistedReconciliation {
            reconciliation_id: prepared.reconciliation_id,
            account_snapshot_id: prepared.account_snapshot_id,
        })
    }

    async fn resolve_commit(
        &mut self,
        key: &CommitRecoveryKey,
    ) -> Result<CommitResolution, StoreError> {
        resolve_commit_key(&self.client, key).await
    }
}

async fn verify_database_release_and_permit(
    transaction: &Transaction<'_>,
    chain: &DurableExecutionChain<'_>,
    prepared: &PreparedChain,
) -> Result<(), StoreError> {
    let release_hash = chain
        .release
        .release_hash()
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?
        .as_hex();
    let universe_hash = HashDigest::of_json(&chain.release.universe)
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?
        .as_hex();
    let environment = environment_sql(chain.activation_permit.environment);
    let account = chain.activation_permit.account_fingerprint.as_hex();
    let matches: bool = transaction
        .query_opt(
            r#"
SELECT
    release.release_hash = $3
    AND release.code_hash = $4
    AND release.parameters_hash = $5
    AND release.universe_hash = $6
    AND release.data_hash = $7
    AND release.cost_model_hash = $8
    AND release.certificate_hash = $9
    AND release.status = 'certified'
    AND release.valid_from = $10
    AND release.valid_until = $11
    AND permit.environment = $12
    AND permit.account_fingerprint = $13
    AND permit.strategy_release_hash = $3
    AND permit.max_gross_notional = $14::text::numeric
    AND permit.max_position_notional = $15::text::numeric
    AND permit.max_daily_loss = $16::text::numeric
    AND permit.max_drawdown = $17::text::numeric
    AND permit.risk_limits_hash = $18
    AND permit.issued_at = $19
    AND permit.expires_at = $20
    AND permit.operator_subject = $21
    AND permit.approval_digest = $22 AS matches
FROM public.strategy_releases AS release
JOIN public.activation_permits AS permit
  ON permit.strategy_release_id = release.release_id
WHERE release.release_id = $1 AND permit.permit_id = $2
"#,
            &[
                &prepared.release_id,
                &prepared.activation_permit_id,
                &release_hash,
                &chain.release.code_hash.as_hex(),
                &chain.release.parameters_hash.as_hex(),
                &universe_hash,
                &chain.release.data_hash.as_hex(),
                &chain.release.cost_model_hash.as_hex(),
                &chain.release.statistical_certificate_hash.as_hex(),
                &chain.release.valid_from,
                &chain.release.expires_at,
                &environment,
                &account,
                &chain.activation_permit.max_gross_notional.to_string(),
                &chain.activation_permit.max_position_notional.to_string(),
                &chain.activation_permit.max_daily_loss.to_string(),
                &chain.activation_permit.max_drawdown.to_string(),
                &chain.activation_permit.risk_limits_hash.as_hex(),
                &chain.activation_permit.issued_at,
                &chain.activation_permit.expires_at,
                &chain.activation_permit.operator_subject,
                &chain.activation_permit.approval_digest.as_hex(),
            ],
        )
        .await
        .map_err(|error| StoreError::database("verify_release_and_permit_authority", error))?
        .map(|row| {
            row.try_get("matches")
                .map_err(|error| StoreError::database("decode_release_permit_authority", error))
        })
        .transpose()?
        .unwrap_or(false);
    if !matches {
        return Err(StoreError::InvalidInput(
            "database release/permit evidence does not exactly match the evaluated authority"
                .into(),
        ));
    }
    Ok(())
}

struct PreparedChain {
    release_id: Uuid,
    decision_id: Uuid,
    target_portfolio_id: Uuid,
    risk_decision_id: Uuid,
    order_plan_id: Uuid,
    intent_id: Uuid,
    activation_permit_id: Uuid,
    persisted_state_event_id: Uuid,
    eligible_state_event_id: Uuid,
    outbox_id: Uuid,
    fencing_token: i64,
    decision_payload: Value,
    decision_payload_hash: HashDigest,
    target_hash: HashDigest,
    target_payload_hash: String,
    target_reason_code: String,
    risk_limits_payload: Value,
    risk_limits_hash: String,
    risk_limits_digest: HashDigest,
    outbox_payload: Value,
    outbox_payload_hash: HashDigest,
}

impl PreparedChain {
    fn new(
        chain: &DurableExecutionChain<'_>,
        environment: Environment,
    ) -> Result<Self, StoreError> {
        validate_execution_chain(chain, environment)?;
        let release_id = parse_uuid(&chain.release.release_id, "strategy release_id")?;
        let decision_id = parse_uuid(&chain.snapshot.decision_id, "decision_id")?;
        let order_plan_id = parse_uuid(&chain.plan.plan_id, "order plan_id")?;
        let intent_id = parse_uuid(&chain.intent.intent_id, "intent_id")?;
        let activation_permit_id =
            parse_uuid(&chain.activation_permit.permit_id, "activation permit_id")?;
        let target_hash = HashDigest::of_json(chain.target)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        let risk_hash = HashDigest::of_json(chain.risk)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        let target_portfolio_id = stable_child_uuid(decision_id, &format!("target:{target_hash}"));
        let risk_decision_id = stable_child_uuid(target_portfolio_id, &format!("risk:{risk_hash}"));
        let decision_payload =
            serde_json::to_value(chain.snapshot).map_err(StoreError::Serialization)?;
        let outbox_payload =
            serde_json::to_value(chain.intent).map_err(StoreError::Serialization)?;
        let decision_payload_hash = HashDigest::of_json(&decision_payload)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        let outbox_payload_hash = HashDigest::of_json(&outbox_payload)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        let risk_limits_digest = HashDigest::of_json(&chain.risk.limits)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        Ok(Self {
            release_id,
            decision_id,
            target_portfolio_id,
            risk_decision_id,
            order_plan_id,
            intent_id,
            activation_permit_id,
            persisted_state_event_id: stable_child_uuid(intent_id, "state:persisted"),
            eligible_state_event_id: stable_child_uuid(intent_id, "state:eligible"),
            outbox_id: stable_child_uuid(intent_id, "outbox:intent-committed"),
            fencing_token: positive_i64(chain.lease.fencing_token, "created fencing_token")?,
            decision_payload,
            decision_payload_hash,
            target_hash,
            target_payload_hash: target_hash.as_hex(),
            target_reason_code: join_reason_codes(
                &chain.target.reason_codes,
                "target reason_codes",
            )?,
            risk_limits_payload: serde_json::to_value(&chain.risk.limits)
                .map_err(StoreError::Serialization)?,
            risk_limits_hash: risk_limits_digest.as_hex(),
            risk_limits_digest,
            outbox_payload,
            outbox_payload_hash,
        })
    }

    fn commit_recovery_key(&self) -> CommitRecoveryKey {
        CommitRecoveryKey::ExecutionChain {
            decision_id: self.decision_id,
            target_portfolio_id: self.target_portfolio_id,
            risk_decision_id: self.risk_decision_id,
            order_plan_id: self.order_plan_id,
            intent_id: self.intent_id,
            outbox_id: self.outbox_id,
            decision_payload_hash: self.decision_payload_hash,
            target_payload_hash: self.target_hash,
            risk_limits_hash: self.risk_limits_digest,
            outbox_payload_hash: self.outbox_payload_hash,
        }
    }
}

async fn insert_execution_chain(
    transaction: &Transaction<'_>,
    chain: &DurableExecutionChain<'_>,
    prepared: &PreparedChain,
    store_environment: Environment,
) -> Result<(), StoreError> {
    let environment = environment_sql(store_environment);
    let account = chain.snapshot.account.account_fingerprint.as_hex();
    let input_hash = chain.snapshot.input_data_hash.as_hex();
    let account_snapshot_hash = chain.snapshot.account_snapshot_hash.as_hex();
    transaction
        .execute(
            INSERT_DECISION_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.decision_id,
                &prepared.release_id,
                &chain.snapshot.market_session,
                &chain.snapshot.as_of,
                &input_hash,
                &account_snapshot_hash,
                &prepared.decision_payload,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_decision_snapshot", error))?;

    transaction
        .execute(
            INSERT_TARGET_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.target_portfolio_id,
                &prepared.decision_id,
                &prepared.release_id,
                &prepared.target_reason_code,
                &prepared.target_payload_hash,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_target_portfolio", error))?;

    for position in &chain.target.positions {
        let quantity = quantity_i64(position.target_quantity, "target quantity")?;
        let weight = position.target_weight.to_string();
        let reason = join_reason_codes(&position.reason_codes, "target position reason_codes")?;
        transaction
            .execute(
                INSERT_TARGET_POSITION_SQL,
                &[
                    &environment,
                    &account,
                    &chain.lease.owner_id,
                    &prepared.fencing_token,
                    &prepared.target_portfolio_id,
                    &position.symbol.as_str(),
                    &quantity,
                    &weight,
                    &reason,
                ],
            )
            .await
            .map_err(|error| StoreError::database("insert_target_position", error))?;
    }

    let risk_outcome = risk_disposition_sql(chain.risk.disposition);
    transaction
        .execute(
            INSERT_RISK_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.risk_decision_id,
                &prepared.target_portfolio_id,
                &prepared.activation_permit_id,
                &risk_outcome,
                &chain.risk.reason_codes,
                &prepared.risk_limits_payload,
                &prepared.risk_limits_hash,
                &chain.intent.decision_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_risk_decision", error))?;

    let plan_side = order_side_sql(chain.plan.side);
    let plan_quantity = quantity_i64(chain.plan.quantity, "plan quantity")?;
    let decision_reference_price = chain.plan.decision_reference_price.to_string();
    let decision_evidence = chain.plan.decision_evidence_hash.as_hex();
    transaction
        .execute(
            INSERT_PLAN_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.order_plan_id,
                &prepared.risk_decision_id,
                &prepared.release_id,
                &chain.plan.symbol.as_str(),
                &plan_side,
                &plan_quantity,
                &decision_reference_price,
                &decision_evidence,
                &chain.plan.created_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_order_plan", error))?;

    let intent_side = order_side_sql(chain.intent.side);
    let intent_quantity = quantity_i64(chain.intent.quantity, "intent quantity")?;
    let limit_price = chain.intent.limit_price.to_string();
    let arrival_quote = chain.intent.arrival_quote.to_string();
    let time_in_force = time_in_force_sql(chain.intent.time_in_force);
    let quote_payload_hash = chain.intent.quote_payload_hash.as_hex();
    let intent_decision_evidence = chain.intent.decision_evidence_hash.as_hex();
    let materialization_evidence = chain.intent.materialization_evidence_hash.as_hex();
    transaction
        .execute(
            INSERT_INTENT_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.intent_id,
                &prepared.order_plan_id,
                &prepared.risk_decision_id,
                &prepared.release_id,
                &chain.intent.client_order_id,
                &chain.intent.symbol.as_str(),
                &intent_side,
                &intent_quantity,
                &limit_price,
                &time_in_force,
                &chain.intent.decision_at,
                &arrival_quote,
                &chain.intent.quote_provider_at,
                &chain.intent.quote_received_at,
                &chain.intent.quote_valid_until,
                &quote_payload_hash,
                &intent_decision_evidence,
                &materialization_evidence,
                &chain.intent.created_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_order_intent", error))?;

    let empty_detail = json!({});
    transaction
        .execute(
            INSERT_INTENT_STATE_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.persisted_state_event_id,
                &prepared.intent_id,
                &"persisted",
                &"INTENT_AND_OUTBOX_TRANSACTION_STARTED",
                &empty_detail,
                &chain.intent.created_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_persisted_intent_state", error))?;
    transaction
        .execute(
            INSERT_INTENT_STATE_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.eligible_state_event_id,
                &prepared.intent_id,
                &"eligible",
                &"DURABLE_AUTHORITY_CHAIN_COMPLETE",
                &empty_detail,
                &chain.intent.created_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_eligible_intent_state", error))?;
    transaction
        .execute(
            INSERT_OUTBOX_SQL,
            &[
                &environment,
                &account,
                &chain.lease.owner_id,
                &prepared.fencing_token,
                &prepared.outbox_id,
                &prepared.intent_id,
                &prepared.outbox_payload,
                &chain.intent.created_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_order_outbox", error))?;
    Ok(())
}

fn validate_execution_chain(
    chain: &DurableExecutionChain<'_>,
    environment: Environment,
) -> Result<(), StoreError> {
    if !matches!(environment, Environment::Paper | Environment::Live) {
        return Err(StoreError::InvalidInput(
            "durable order chains are permitted only in paper/live stores".into(),
        ));
    }
    if chain.lease.environment != environment
        || chain.lease.account_fingerprint != chain.snapshot.account.account_fingerprint
    {
        return Err(StoreError::InvalidInput(
            "execution chain does not match its fenced environment/account authority".into(),
        ));
    }
    validate_store_symbols(chain)?;

    chain
        .release
        .validate()
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    chain
        .activation_permit
        .validate(chain.snapshot.as_of)
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    let release_hash = chain
        .release
        .release_hash()
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    let risk_limits_hash = HashDigest::of_json(&chain.risk.limits)
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    if chain.activation_permit.environment != environment
        || chain.activation_permit.account_fingerprint != chain.snapshot.account.account_fingerprint
        || chain.activation_permit.strategy_release_id != chain.release.release_id
        || chain.activation_permit.strategy_release_hash != release_hash
        || chain.activation_permit.risk_limits_hash != risk_limits_hash
        || chain.risk.limits.max_gross_exposure > chain.activation_permit.max_gross_notional
        || chain.risk.limits.max_gross_exposure > chain.activation_permit.max_position_notional
        || chain.risk.limits.max_order_notional > chain.activation_permit.max_position_notional
        || chain.risk.limits.max_planned_loss > chain.activation_permit.max_daily_loss
        || chain.risk.limits.daily_loss_limit > chain.activation_permit.max_daily_loss
        || chain.risk.limits.hard_drawdown_limit > chain.activation_permit.max_drawdown
    {
        return Err(StoreError::InvalidInput(
            "activation permit does not exactly authorize the release, account, limits, and caps"
                .into(),
        ));
    }

    let evaluated = evaluate_decision(chain.snapshot, chain.release, &chain.risk.limits)
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    if evaluated.target != *chain.target || evaluated.risk != *chain.risk {
        return Err(StoreError::InvalidInput(
            "target or risk decision is not an exact output of the released Rust core".into(),
        ));
    }
    if !evaluated.order_plans.contains(chain.plan) {
        return Err(StoreError::InvalidInput(
            "order plan is not an exact output of the released Rust core".into(),
        ));
    }
    let quote = FreshExecutionQuote {
        symbol: chain.intent.symbol.clone(),
        raw_price: chain.intent.arrival_quote,
        provider_at: chain.intent.quote_provider_at,
        received_at: chain.intent.quote_received_at,
        valid_until: chain.intent.quote_valid_until,
        payload_hash: chain.intent.quote_payload_hash,
    };
    let materialized = materialize_order_intent(
        chain.snapshot,
        chain.release,
        chain.risk,
        chain.plan,
        &quote,
    )
    .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    if materialized != *chain.intent {
        return Err(StoreError::InvalidInput(
            "order intent is not an exact materialization of the released Rust core".into(),
        ));
    }
    let mut resulting_position_notional = Money::ZERO;
    for position in &chain.risk.approved_positions {
        let reference_notional = position
            .raw_reference_price
            .checked_mul_quantity(position.target_quantity.get())
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        let conservative_notional = if position.symbol == chain.intent.symbol {
            let executable_notional = chain
                .intent
                .limit_price
                .checked_mul_quantity(position.target_quantity.get())
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
            if executable_notional > reference_notional {
                executable_notional
            } else {
                reference_notional
            }
        } else {
            reference_notional
        };
        resulting_position_notional = Money(
            resulting_position_notional
                .fixed()
                .checked_add(conservative_notional.fixed())
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?,
        );
    }
    if resulting_position_notional > chain.activation_permit.max_position_notional {
        return Err(StoreError::InvalidInput(
            "exact rematerialization exceeds the activation permit position-notional cap".into(),
        ));
    }
    positive_i64(chain.lease.fencing_token, "created fencing_token")?;
    Ok(())
}

fn validate_store_symbols(chain: &DurableExecutionChain<'_>) -> Result<(), StoreError> {
    for symbol in chain
        .release
        .universe
        .iter()
        .chain(chain.snapshot.observations.iter().map(|item| &item.symbol))
        .chain(
            chain
                .snapshot
                .account
                .positions
                .iter()
                .map(|item| &item.symbol),
        )
        .chain(chain.target.positions.iter().map(|item| &item.symbol))
        .chain(
            chain
                .risk
                .approved_positions
                .iter()
                .map(|item| &item.symbol),
        )
        .chain(std::iter::once(&chain.plan.symbol))
        .chain(std::iter::once(&chain.intent.symbol))
    {
        validate_store_symbol(symbol.as_str())?;
    }
    Ok(())
}

fn validate_store_symbol(symbol: &str) -> Result<(), StoreError> {
    let mut characters = symbol.chars();
    let valid = (1..=15).contains(&symbol.len())
        && characters
            .next()
            .is_some_and(|value| value.is_ascii_uppercase())
        && characters.all(|value| {
            value.is_ascii_uppercase() || value.is_ascii_digit() || matches!(value, '.' | '-')
        });
    if !valid {
        return Err(StoreError::InvalidInput(format!(
            "symbol {symbol} is valid in core memory but not in the durable PostgreSQL contract"
        )));
    }
    Ok(())
}

struct PreparedBrokerWrite {
    intent_id: Uuid,
    broker_order_id: String,
    broker_event_id: Uuid,
    fencing_token: i64,
    recognized_status: bool,
}

impl PreparedBrokerWrite {
    fn new(write: &BrokerEventWrite<'_>) -> Result<Self, StoreError> {
        let intent_id = parse_uuid(write.intent_id, "broker event intent_id")?;
        let broker_order_id = write
            .event
            .provider_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                StoreError::InvalidInput("broker event lacks a non-empty provider order ID".into())
            })?
            .to_owned();
        if write.event.client_order_id.trim().is_empty() {
            return Err(StoreError::InvalidInput(
                "broker event lacks client_order_id".into(),
            ));
        }
        if write.event.received_at < write.event.provider_timestamp {
            return Err(StoreError::InvalidInput(
                "broker event was received before its provider timestamp".into(),
            ));
        }
        if write.event.filled_quantity != WholeQuantity::ZERO && write.event.fill_price.is_none() {
            return Err(StoreError::InvalidInput(
                "filled broker quantity lacks an average fill price".into(),
            ));
        }
        let mut fill_ids = std::collections::BTreeSet::new();
        for fill in write.fills {
            validate_fill(fill)?;
            if write.event.filled_quantity == WholeQuantity::ZERO {
                return Err(StoreError::InvalidInput(
                    "incremental fill cannot accompany a zero-filled broker event".into(),
                ));
            }
            if !fill_ids.insert(fill.fill_id.as_str()) {
                return Err(StoreError::InvalidInput(
                    "broker event includes duplicate incremental fill IDs".into(),
                ));
            }
        }
        let broker_event_id = stable_named_uuid(&format!(
            "broker-event:{broker_order_id}:{}",
            write.event.raw_payload_hash
        ));
        Ok(Self {
            intent_id,
            broker_order_id,
            broker_event_id,
            fencing_token: positive_i64(write.lease.fencing_token, "broker event fencing_token")?,
            recognized_status: is_recognized_broker_status(&write.event.status),
        })
    }

    fn commit_recovery_key(&self, write: &BrokerEventWrite<'_>) -> CommitRecoveryKey {
        CommitRecoveryKey::BrokerEvent {
            broker_event_id: self.broker_event_id,
            raw_payload_hash: write.event.raw_payload_hash,
            cumulative_filled_quantity: write.event.filled_quantity,
        }
    }
}

async fn insert_broker_event(
    transaction: &Transaction<'_>,
    write: &BrokerEventWrite<'_>,
    prepared: &PreparedBrokerWrite,
    expected_environment: Environment,
) -> Result<BrokerWriteResult, StoreError> {
    let intent_row = transaction
        .query_opt(
            r#"
SELECT environment, account_fingerprint, client_order_id
FROM public.order_intents
WHERE intent_id = $1
FOR UPDATE
"#,
            &[&prepared.intent_id],
        )
        .await
        .map_err(|error| StoreError::database("lock_broker_event_intent", error))?
        .ok_or_else(|| StoreError::InvalidInput("broker event references unknown intent".into()))?;
    let environment: String = intent_row
        .try_get("environment")
        .map_err(|error| StoreError::database("decode_broker_intent_environment", error))?;
    let account: String = intent_row
        .try_get("account_fingerprint")
        .map_err(|error| StoreError::database("decode_broker_intent_account", error))?;
    let client_order_id: String = intent_row
        .try_get("client_order_id")
        .map_err(|error| StoreError::database("decode_broker_intent_client_id", error))?;
    if parse_environment(&environment)? != expected_environment
        || write.lease.environment != expected_environment
        || write.lease.account_fingerprint.as_hex() != account
        || client_order_id != write.event.client_order_id
    {
        return Err(StoreError::InvalidInput(
            "broker event identity does not match the isolated durable intent".into(),
        ));
    }
    let has_current_fence = transaction
        .query_opt(
            r#"
SELECT 1
FROM public.executor_leases
WHERE environment = $1
  AND account_fingerprint = $2
  AND owner_id = $3
  AND fencing_token = $4
  AND lease_until >= clock_timestamp()
"#,
            &[
                &environment,
                &account,
                &write.lease.owner_id,
                &prepared.fencing_token,
            ],
        )
        .await
        .map_err(|error| StoreError::database("verify_broker_event_fence", error))?
        .is_some();
    if !has_current_fence {
        return Err(StoreError::InvalidInput(
            "broker event lacks the current execution fence".into(),
        ));
    }

    let broker_order = transaction
        .query_opt(
            r#"
SELECT intent_id, client_order_id, environment, account_fingerprint
FROM public.broker_orders
WHERE broker_order_id = $1
FOR UPDATE
"#,
            &[&prepared.broker_order_id],
        )
        .await
        .map_err(|error| StoreError::database("load_broker_order", error))?;
    if let Some(row) = broker_order {
        let existing_intent: Uuid = row
            .try_get("intent_id")
            .map_err(|error| StoreError::database("decode_broker_order_intent", error))?;
        let existing_client: String = row
            .try_get("client_order_id")
            .map_err(|error| StoreError::database("decode_broker_order_client", error))?;
        let existing_environment: String = row
            .try_get("environment")
            .map_err(|error| StoreError::database("decode_broker_order_environment", error))?;
        let existing_account: String = row
            .try_get("account_fingerprint")
            .map_err(|error| StoreError::database("decode_broker_order_account", error))?;
        if existing_intent != prepared.intent_id
            || existing_client != client_order_id
            || existing_environment != environment
            || existing_account != account
        {
            return Err(StoreError::InvalidInput(
                "provider order ID is already bound to different authority".into(),
            ));
        }
    } else {
        let raw_hash = write.event.raw_payload_hash.as_hex();
        transaction
            .execute(
                INSERT_BROKER_ORDER_SQL,
                &[
                    &environment,
                    &account,
                    &write.lease.owner_id,
                    &prepared.fencing_token,
                    &prepared.broker_order_id,
                    &prepared.intent_id,
                    &client_order_id,
                    &write.event.received_at,
                    &raw_hash,
                ],
            )
            .await
            .map_err(|error| StoreError::database("insert_broker_order", error))?;
    }

    for fill in write.fills {
        let existing_fill = transaction
            .query_opt(
                r#"
SELECT
    broker_order_id = $2
    AND intent_id = $3
    AND quantity = $4::text::numeric
    AND price = $5::text::numeric
    AND fee = $6::text::numeric
    AND executed_at = $7
    AND received_at = $8
    AND raw_hash = $9 AS matches
FROM public.fills
WHERE fill_id = $1
"#,
                &[
                    &fill.fill_id,
                    &prepared.broker_order_id,
                    &prepared.intent_id,
                    &fill.quantity.get().to_string(),
                    &fill.price.to_string(),
                    &fill.fee.to_string(),
                    &fill.executed_at,
                    &fill.received_at,
                    &fill.raw_payload_hash.as_hex(),
                ],
            )
            .await
            .map_err(|error| StoreError::database("deduplicate_fill", error))?;
        if let Some(row) = existing_fill {
            let matches: bool = row
                .try_get("matches")
                .map_err(|error| StoreError::database("decode_existing_fill", error))?;
            if !matches {
                return Err(StoreError::InvalidInput(
                    "fill_id is already bound to different payload evidence".into(),
                ));
            }
        } else {
            let quantity = fill.quantity.get().to_string();
            let price = fill.price.to_string();
            let fee = fill.fee.to_string();
            let fill_hash = fill.raw_payload_hash.as_hex();
            let inserted = transaction
                .execute(
                    INSERT_FILL_SQL,
                    &[
                        &environment,
                        &account,
                        &write.lease.owner_id,
                        &prepared.fencing_token,
                        &fill.fill_id,
                        &prepared.broker_order_id,
                        &prepared.intent_id,
                        &quantity,
                        &price,
                        &fee,
                        &fill.executed_at,
                        &fill.received_at,
                        &fill_hash,
                    ],
                )
                .await
                .map_err(|error| StoreError::database("insert_fill", error))?;
            if inserted != 1 {
                return Err(StoreError::InvalidInput(
                    "fill insertion could not resolve its durable intent".into(),
                ));
            }
        }
    }

    let filled_quantity = write.event.filled_quantity.get().to_string();
    let durable_fills_match: bool = transaction
        .query_one(
            r#"
SELECT COALESCE(SUM(quantity), 0) = $2::text::numeric AS matches
FROM public.fills
WHERE broker_order_id = $1
"#,
            &[&prepared.broker_order_id, &filled_quantity],
        )
        .await
        .map_err(|error| StoreError::database("verify_broker_cumulative_fills", error))?
        .try_get("matches")
        .map_err(|error| StoreError::database("decode_broker_cumulative_fills", error))?;
    if !durable_fills_match {
        return Err(StoreError::InvalidInput(
            "broker cumulative filled quantity does not equal durable incremental fills".into(),
        ));
    }

    let raw_hash = write.event.raw_payload_hash.as_hex();
    if let Some(row) = transaction
        .query_opt(
            r#"
SELECT broker_event_id = $3 AND cumulative_filled_quantity = $4::text::numeric AS matches
FROM public.broker_order_events
WHERE broker_order_id = $1 AND raw_hash = $2
"#,
            &[
                &prepared.broker_order_id,
                &raw_hash,
                &prepared.broker_event_id,
                &filled_quantity,
            ],
        )
        .await
        .map_err(|error| StoreError::database("deduplicate_broker_event", error))?
    {
        let matches: bool = row
            .try_get("matches")
            .map_err(|error| StoreError::database("decode_duplicate_broker_event", error))?;
        if !matches {
            return Err(StoreError::InvalidInput(
                "broker event evidence hash is bound to conflicting durable evidence".into(),
            ));
        }
        return Ok(BrokerWriteResult {
            broker_event_id: prepared.broker_event_id,
            duplicate: true,
        });
    }

    let average_fill_price = write.event.fill_price.map(|price| price.to_string());
    transaction
        .execute(
            INSERT_BROKER_EVENT_SQL,
            &[
                &environment,
                &account,
                &write.lease.owner_id,
                &prepared.fencing_token,
                &prepared.broker_event_id,
                &prepared.broker_order_id,
                &write.event.client_order_id,
                &write.event.status,
                &prepared.recognized_status,
                &filled_quantity,
                &average_fill_price,
                &write.event.provider_timestamp,
                &write.event.received_at,
                &write.event.request_id,
                write.raw_payload,
                &raw_hash,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_broker_order_event", error))?;

    let current_state: String = transaction
        .query_one(
            "SELECT state FROM public.current_intent_states WHERE intent_id = $1",
            &[&prepared.intent_id],
        )
        .await
        .map_err(|error| StoreError::database("load_current_intent_state", error))?
        .try_get("state")
        .map_err(|error| StoreError::database("decode_current_intent_state", error))?;
    let transitions = broker_state_transitions(
        &current_state,
        &write.event.status,
        prepared.recognized_status,
    )?;
    for (index, state) in transitions.iter().enumerate() {
        let event_id = stable_child_uuid(
            prepared.broker_event_id,
            &format!("intent-state:{index}:{state}"),
        );
        let reason = format!("BROKER_EVENT_{}", state.to_ascii_uppercase());
        let detail = json!({
            "broker_event_id": prepared.broker_event_id,
            "provider_status": write.event.status,
        });
        transaction
            .execute(
                INSERT_INTENT_STATE_SQL,
                &[
                    &environment,
                    &account,
                    &write.lease.owner_id,
                    &prepared.fencing_token,
                    &event_id,
                    &prepared.intent_id,
                    state,
                    &reason,
                    &detail,
                    &write.event.received_at,
                ],
            )
            .await
            .map_err(|error| StoreError::database("append_broker_intent_state", error))?;
    }
    Ok(BrokerWriteResult {
        broker_event_id: prepared.broker_event_id,
        duplicate: false,
    })
}

fn validate_fill(fill: &BrokerFill) -> Result<(), StoreError> {
    if fill.fill_id.trim().is_empty()
        || fill.quantity == WholeQuantity::ZERO
        || !fill.price.fixed().is_positive()
        || fill.fee.is_negative()
        || fill.received_at < fill.executed_at
    {
        return Err(StoreError::InvalidInput(
            "fill identity, quantity, price, fee, or timestamps are invalid".into(),
        ));
    }
    Ok(())
}

struct PreparedReconciliation {
    kill_event_id: Uuid,
    reconciliation_id: Uuid,
    account_snapshot_id: Uuid,
    fencing_token: i64,
    account_payload: Value,
    account_payload_hash: String,
    evidence_hash: String,
    evidence_digest: HashDigest,
}

impl PreparedReconciliation {
    fn new(write: &ReconciliationWrite<'_>, environment: Environment) -> Result<Self, StoreError> {
        write
            .report
            .validate()
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        if write.report.account_fingerprint != write.account.snapshot.account_fingerprint
            || write.lease.environment != environment
            || write.lease.account_fingerprint != write.report.account_fingerprint
            || write.lease.fencing_token != write.report.execution_fencing_token
        {
            return Err(StoreError::InvalidInput(
                "reconciliation, account snapshot, and fenced authority differ".into(),
            ));
        }
        if write.started_at > write.report.generated_at
            || write.account.received_at < write.started_at
            || write.account.received_at > write.report.generated_at
        {
            return Err(StoreError::InvalidInput(
                "reconciliation/account observation timestamps are not monotonic".into(),
            ));
        }
        if !matches!(
            environment,
            Environment::Shadow | Environment::Paper | Environment::Live
        ) {
            return Err(StoreError::InvalidInput(
                "unsupported reconciliation environment".into(),
            ));
        }
        let kill_event_id = parse_uuid(write.kill_event_id, "reconciliation kill_event_id")?;
        let evidence_hash = HashDigest::of_json(write.report)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
        let stable_material = format!(
            "reconciliation:{}:{}:{}:{}",
            environment_sql(environment),
            write.report.account_fingerprint,
            write.started_at.to_rfc3339(),
            evidence_hash
        );
        let reconciliation_id = stable_named_uuid(&stable_material);
        let account_snapshot_id = stable_child_uuid(reconciliation_id, "account-snapshot");
        let account_payload =
            serde_json::to_value(write.account.snapshot).map_err(StoreError::Serialization)?;
        let account_payload_hash = HashDigest::of_json(write.account.snapshot)
            .map_err(|error| StoreError::InvalidInput(error.to_string()))?
            .as_hex();
        Ok(Self {
            kill_event_id,
            reconciliation_id,
            account_snapshot_id,
            fencing_token: positive_i64(write.lease.fencing_token, "reconciliation fencing_token")?,
            account_payload,
            account_payload_hash,
            evidence_hash: evidence_hash.as_hex(),
            evidence_digest: evidence_hash,
        })
    }

    fn commit_recovery_key(&self) -> CommitRecoveryKey {
        CommitRecoveryKey::Reconciliation {
            reconciliation_id: self.reconciliation_id,
            evidence_hash: self.evidence_digest,
        }
    }
}

async fn insert_reconciliation(
    transaction: &Transaction<'_>,
    write: &ReconciliationWrite<'_>,
    prepared: &PreparedReconciliation,
    environment: Environment,
) -> Result<(), StoreError> {
    let environment = environment_sql(environment);
    let account = write.report.account_fingerprint.as_hex();
    let (account_status, recognized_status) = account_status_sql(&write.account.snapshot.status);
    let cash = write.account.snapshot.cash.to_string();
    let equity = write.account.snapshot.equity.to_string();
    let buying_power = write.account.snapshot.buying_power.to_string();
    transaction
        .execute(
            INSERT_ACCOUNT_SNAPSHOT_SQL,
            &[
                &environment,
                &account,
                &write.lease.owner_id,
                &prepared.fencing_token,
                &prepared.account_snapshot_id,
                &write.account.broker_timestamp,
                &write.account.received_at,
                &account_status,
                &recognized_status,
                &cash,
                &equity,
                &buying_power,
                &write.account.snapshot.trading_blocked,
                &write.account.transfers_blocked,
                &write.account.account_blocked,
                &prepared.account_payload,
                &prepared.account_payload_hash,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_account_snapshot", error))?;

    let trigger = write.trigger.as_sql();
    transaction
        .execute(
            INSERT_RECONCILIATION_SQL,
            &[
                &environment,
                &account,
                &write.lease.owner_id,
                &prepared.fencing_token,
                &prepared.reconciliation_id,
                &trigger,
                &prepared.kill_event_id,
                &write.started_at,
            ],
        )
        .await
        .map_err(|error| StoreError::database("insert_reconciliation_run", error))?;

    for (index, difference) in write.report.differences.iter().enumerate() {
        let difference_id = stable_child_uuid(
            prepared.reconciliation_id,
            &format!("difference:{index}:{}", difference.subject),
        );
        let category = reconciliation_category(&difference.kind, &difference.subject);
        transaction
            .execute(
                INSERT_RECONCILIATION_DIFF_SQL,
                &[
                    &environment,
                    &account,
                    &write.lease.owner_id,
                    &prepared.fencing_token,
                    &difference_id,
                    &prepared.reconciliation_id,
                    &category,
                    &difference.subject,
                    &difference.detail,
                ],
            )
            .await
            .map_err(|error| StoreError::database("insert_reconciliation_difference", error))?;
    }

    let outcome = if write.report.may_resume_execution {
        "clean"
    } else {
        "blocked"
    };
    let updated = transaction
        .execute(
            FINALIZE_RECONCILIATION_SQL,
            &[
                &environment,
                &account,
                &write.lease.owner_id,
                &prepared.fencing_token,
                &prepared.reconciliation_id,
                &write.report.generated_at,
                &outcome,
                &write.report.may_resume_execution,
                &prepared.account_snapshot_id,
                &prepared.evidence_hash,
            ],
        )
        .await
        .map_err(|error| StoreError::database("finalize_reconciliation_report", error))?;
    if updated != 1 {
        return Err(StoreError::InvalidInput(
            "reconciliation report did not finalize exactly once".into(),
        ));
    }
    Ok(())
}

fn decode_lease(
    row: &Row,
    environment: Environment,
    account_fingerprint: HashDigest,
    owner_id: Uuid,
) -> Result<FencedLease, StoreError> {
    let token: i64 = row
        .try_get("fencing_token")
        .map_err(|error| StoreError::database("decode_lease_fencing_token", error))?;
    let lease_until = row
        .try_get("lease_until")
        .map_err(|error| StoreError::database("decode_lease_until", error))?;
    Ok(FencedLease {
        environment,
        account_fingerprint,
        owner_id,
        fencing_token: positive_u64(token, "lease fencing_token")?,
        lease_until,
    })
}

fn decode_claimed_outbox(
    row: &Row,
    kind: OutboxClaimKind,
    lease: &FencedLease,
) -> Result<ClaimedOutbox, StoreError> {
    let environment: String = row
        .try_get("environment")
        .map_err(|error| StoreError::database("decode_outbox_environment", error))?;
    let environment = parse_environment(&environment)?;
    if environment != lease.environment {
        return Err(StoreError::InvalidInput(
            "claimed outbox escaped store environment isolation".into(),
        ));
    }
    let account: String = row
        .try_get("account_fingerprint")
        .map_err(|error| StoreError::database("decode_outbox_account", error))?;
    let account_fingerprint = HashDigest::from_str(&account)
        .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
    if account_fingerprint != lease.account_fingerprint {
        return Err(StoreError::InvalidInput(
            "claimed outbox escaped store account isolation".into(),
        ));
    }
    let created: i64 = row
        .try_get("created_fencing_token")
        .map_err(|error| StoreError::database("decode_outbox_creation_fence", error))?;
    let claim: Option<i64> = row
        .try_get("claim_fencing_token")
        .map_err(|error| StoreError::database("decode_outbox_claim_fence", error))?;
    let claimed_by: Option<Uuid> = row
        .try_get("claimed_by")
        .map_err(|error| StoreError::database("decode_outbox_claimed_by", error))?;
    let claimed_at: Option<DateTime<Utc>> = row
        .try_get("claimed_at")
        .map_err(|error| StoreError::database("decode_outbox_claimed_at", error))?;
    let attempts: i32 = row
        .try_get("attempt_count")
        .map_err(|error| StoreError::database("decode_outbox_attempt_count", error))?;
    let claim_fencing_token = positive_u64(
        claim.ok_or_else(|| {
            StoreError::InvalidInput("claimed outbox lacks claim fencing token".into())
        })?,
        "outbox claim fence",
    )?;
    let claimed_by = claimed_by
        .ok_or_else(|| StoreError::InvalidInput("claimed outbox lacks claiming owner".into()))?;
    if claim_fencing_token != lease.fencing_token || claimed_by != lease.owner_id {
        return Err(StoreError::InvalidInput(
            "claimed outbox does not match the requesting owner and fence".into(),
        ));
    }
    Ok(ClaimedOutbox {
        kind,
        outbox_id: row
            .try_get("outbox_id")
            .map_err(|error| StoreError::database("decode_outbox_id", error))?,
        intent_id: row
            .try_get("intent_id")
            .map_err(|error| StoreError::database("decode_outbox_intent_id", error))?,
        environment,
        account_fingerprint,
        created_fencing_token: positive_u64(created, "outbox creation fence")?,
        claim_fencing_token,
        payload: row
            .try_get("payload")
            .map_err(|error| StoreError::database("decode_outbox_payload", error))?,
        available_at: row
            .try_get("available_at")
            .map_err(|error| StoreError::database("decode_outbox_available_at", error))?,
        claimed_by,
        claimed_at: claimed_at.ok_or_else(|| {
            StoreError::InvalidInput("claimed outbox lacks claim timestamp".into())
        })?,
        attempt_count: u32::try_from(attempts)
            .map_err(|_| StoreError::InvalidInput("outbox attempt count is negative".into()))?,
    })
}

fn decode_unresolved_outbox(row: &Row) -> Result<UnresolvedOutbox, StoreError> {
    let created: i64 = row
        .try_get("created_fencing_token")
        .map_err(|error| StoreError::database("decode_unresolved_creation_fence", error))?;
    let current_state: String = row
        .try_get("current_state")
        .map_err(|error| StoreError::database("decode_unresolved_current_state", error))?;
    if !matches!(
        current_state.as_str(),
        "eligible"
            | "dispatch_started"
            | "submission_unknown"
            | "acknowledged"
            | "broker_confirmed"
            | "terminal"
            | "blocked"
    ) {
        return Err(StoreError::InvalidInput(
            "unresolved outbox has an unknown current intent state".into(),
        ));
    }
    Ok(UnresolvedOutbox {
        outbox_id: row
            .try_get("outbox_id")
            .map_err(|error| StoreError::database("decode_unresolved_outbox_id", error))?,
        intent_id: row
            .try_get("intent_id")
            .map_err(|error| StoreError::database("decode_unresolved_intent_id", error))?,
        created_fencing_token: positive_u64(created, "unresolved creation fence")?,
        payload: row
            .try_get("payload")
            .map_err(|error| StoreError::database("decode_unresolved_payload", error))?,
        available_at: row
            .try_get("available_at")
            .map_err(|error| StoreError::database("decode_unresolved_available_at", error))?,
        current_state,
    })
}

fn environment_sql(environment: Environment) -> &'static str {
    match environment {
        Environment::Shadow => "shadow",
        Environment::Paper => "paper",
        Environment::Live => "live",
    }
}

fn parse_environment(value: &str) -> Result<Environment, StoreError> {
    match value {
        "shadow" => Ok(Environment::Shadow),
        "paper" => Ok(Environment::Paper),
        "live" => Ok(Environment::Live),
        _ => Err(StoreError::InvalidInput(format!(
            "database returned unknown environment {value}"
        ))),
    }
}

fn order_side_sql(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn time_in_force_sql(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::Day => "day",
    }
}

fn risk_disposition_sql(disposition: RiskDisposition) -> &'static str {
    match disposition {
        RiskDisposition::Approved => "approved",
        RiskDisposition::Reduced => "reduced",
        RiskDisposition::Rejected => "rejected",
    }
}

fn account_status_sql(status: &AccountStatus) -> (&'static str, bool) {
    match status {
        AccountStatus::Active => ("ACTIVE", true),
        AccountStatus::Restricted => ("RESTRICTED", true),
        AccountStatus::Closed => ("CLOSED", true),
        AccountStatus::Unknown => ("UNKNOWN", false),
    }
}

fn reconciliation_category(kind: &ReconciliationDifferenceKind, subject: &str) -> &'static str {
    match kind {
        ReconciliationDifferenceKind::CashMismatch => "cash",
        ReconciliationDifferenceKind::QuantityMismatch => "position",
        ReconciliationDifferenceKind::StatusMismatch
        | ReconciliationDifferenceKind::UnknownProviderState => "order",
        ReconciliationDifferenceKind::MissingLocally
        | ReconciliationDifferenceKind::MissingAtBroker => {
            if subject.starts_with("fill:") {
                "fill"
            } else if subject
                .chars()
                .all(|character| character.is_ascii_uppercase() || matches!(character, '.' | '-'))
            {
                "position"
            } else {
                "order"
            }
        }
    }
}

fn is_recognized_broker_status(status: &str) -> bool {
    matches!(
        status,
        "accepted"
            | "new"
            | "pending_new"
            | "accepted_for_bidding"
            | "partially_filled"
            | "filled"
            | "done_for_day"
            | "canceled"
            | "expired"
            | "replaced"
            | "pending_cancel"
            | "pending_replace"
            | "stopped"
            | "rejected"
            | "suspended"
            | "calculated"
    )
}

fn is_terminal_broker_status(status: &str) -> bool {
    matches!(
        status,
        "filled" | "canceled" | "expired" | "replaced" | "rejected"
    )
}

fn broker_state_transitions(
    current: &str,
    status: &str,
    recognized: bool,
) -> Result<Vec<&'static str>, StoreError> {
    if !recognized {
        return match current {
            "persisted" | "eligible" | "dispatch_started" | "submission_unknown"
            | "acknowledged" | "broker_confirmed" => Ok(vec!["blocked"]),
            "blocked" => Ok(Vec::new()),
            "terminal" => Err(StoreError::InvalidInput(
                "unknown broker status followed a terminal intent".into(),
            )),
            _ => Err(StoreError::InvalidInput(format!(
                "database returned unknown intent state {current}"
            ))),
        };
    }
    let terminal = is_terminal_broker_status(status);
    let confirms = terminal || status == "partially_filled";
    match current {
        "dispatch_started" => {
            if terminal {
                Ok(vec!["broker_confirmed", "terminal"])
            } else if confirms {
                Ok(vec!["broker_confirmed"])
            } else {
                Ok(vec!["acknowledged"])
            }
        }
        "submission_unknown" | "blocked" => {
            if terminal {
                Ok(vec!["broker_confirmed", "terminal"])
            } else {
                Ok(vec!["broker_confirmed"])
            }
        }
        "acknowledged" => {
            if terminal {
                Ok(vec!["broker_confirmed", "terminal"])
            } else if confirms {
                Ok(vec!["broker_confirmed"])
            } else {
                Ok(Vec::new())
            }
        }
        "broker_confirmed" => {
            if terminal {
                Ok(vec!["terminal"])
            } else {
                Ok(Vec::new())
            }
        }
        "terminal" => Err(StoreError::InvalidInput(
            "new broker event followed a terminal intent".into(),
        )),
        "persisted" | "eligible" => Err(StoreError::InvalidInput(
            "broker event arrived before protected first dispatch".into(),
        )),
        _ => Err(StoreError::InvalidInput(format!(
            "database returned unknown intent state {current}"
        ))),
    }
}

fn validate_ttl(ttl: Duration) -> Result<i64, StoreError> {
    if ttl.is_zero() || ttl > MAX_LEASE_TTL {
        return Err(StoreError::InvalidInput(
            "executor lease TTL must be greater than zero and at most 60 seconds".into(),
        ));
    }
    i64::try_from(ttl.as_micros())
        .map_err(|_| StoreError::InvalidInput("executor lease TTL overflow".into()))
}

fn positive_i64(value: u64, field: &str) -> Result<i64, StoreError> {
    if value == 0 {
        return Err(StoreError::InvalidInput(format!(
            "{field} must be positive"
        )));
    }
    i64::try_from(value).map_err(|_| StoreError::InvalidInput(format!("{field} exceeds BIGINT")))
}

fn positive_u64(value: i64, field: &str) -> Result<u64, StoreError> {
    if value <= 0 {
        return Err(StoreError::InvalidInput(format!(
            "{field} must be positive"
        )));
    }
    u64::try_from(value).map_err(|_| StoreError::InvalidInput(format!("{field} is invalid")))
}

fn quantity_i64(quantity: WholeQuantity, field: &str) -> Result<i64, StoreError> {
    i64::try_from(quantity.get())
        .map_err(|_| StoreError::InvalidInput(format!("{field} exceeds BIGINT")))
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, StoreError> {
    Uuid::parse_str(value).map_err(|_| StoreError::InvalidInput(format!("{field} is not a UUID")))
}

fn stable_child_uuid(namespace: Uuid, label: &str) -> Uuid {
    Uuid::new_v5(&namespace, label.as_bytes())
}

fn stable_named_uuid(label: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, label.as_bytes())
}

fn submission_unknown_evidence_hash(
    reason_code: &str,
    detail: &Value,
    fencing_token: i64,
    occurred_at: DateTime<Utc>,
) -> Result<HashDigest, StoreError> {
    HashDigest::of_json(&json!({
        "reason_code": reason_code,
        "detail": detail,
        "fencing_token": fencing_token,
        "occurred_at": occurred_at,
    }))
    .map_err(|error| StoreError::InvalidInput(error.to_string()))
}

async fn resolve_commit_key(
    client: &Client,
    key: &CommitRecoveryKey,
) -> Result<CommitResolution, StoreError> {
    match key {
        CommitRecoveryKey::ExecutionChain {
            decision_id,
            target_portfolio_id,
            risk_decision_id,
            order_plan_id,
            intent_id,
            outbox_id,
            decision_payload_hash,
            target_payload_hash,
            risk_limits_hash,
            outbox_payload_hash,
        } => {
            let row = client
                .query_opt(
                    r#"
SELECT
    decision.payload AS decision_payload,
    target.payload_hash AS target_payload_hash,
    risk.limit_snapshot_hash AS risk_limits_hash,
    outbox.payload AS outbox_payload
FROM public.decision_snapshots AS decision
JOIN public.target_portfolios AS target
  ON target.target_portfolio_id = $2 AND target.decision_id = decision.decision_id
JOIN public.risk_decisions AS risk
  ON risk.risk_decision_id = $3 AND risk.target_portfolio_id = target.target_portfolio_id
JOIN public.order_plans AS plan
  ON plan.order_plan_id = $4 AND plan.risk_decision_id = risk.risk_decision_id
JOIN public.order_intents AS intent
  ON intent.intent_id = $5
 AND intent.order_plan_id = plan.order_plan_id
 AND intent.risk_decision_id = risk.risk_decision_id
JOIN public.order_outbox AS outbox
  ON outbox.outbox_id = $6 AND outbox.intent_id = intent.intent_id
WHERE decision.decision_id = $1
"#,
                    &[
                        decision_id,
                        target_portfolio_id,
                        risk_decision_id,
                        order_plan_id,
                        intent_id,
                        outbox_id,
                    ],
                )
                .await
                .map_err(|error| StoreError::database("resolve_execution_chain_commit", error))?;
            let Some(row) = row else {
                return Ok(CommitResolution::NotCommitted);
            };
            let decision_payload: Value = row
                .try_get("decision_payload")
                .map_err(|error| StoreError::database("decode_recovered_decision", error))?;
            let outbox_payload: Value = row
                .try_get("outbox_payload")
                .map_err(|error| StoreError::database("decode_recovered_outbox", error))?;
            let stored_target: String = row
                .try_get("target_payload_hash")
                .map_err(|error| StoreError::database("decode_recovered_target_hash", error))?;
            let stored_risk: String = row
                .try_get("risk_limits_hash")
                .map_err(|error| StoreError::database("decode_recovered_risk_hash", error))?;
            let decision_matches = HashDigest::of_json(&decision_payload)
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?
                == *decision_payload_hash;
            let outbox_matches = HashDigest::of_json(&outbox_payload)
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?
                == *outbox_payload_hash;
            let hashes_match = HashDigest::from_str(&stored_target)
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?
                == *target_payload_hash
                && HashDigest::from_str(&stored_risk)
                    .map_err(|error| StoreError::InvalidInput(error.to_string()))?
                    == *risk_limits_hash;
            Ok(if decision_matches && outbox_matches && hashes_match {
                CommitResolution::Committed
            } else {
                CommitResolution::ConflictingEvidence
            })
        }
        CommitRecoveryKey::BrokerEvent {
            broker_event_id,
            raw_payload_hash,
            cumulative_filled_quantity,
        } => {
            let expected = cumulative_filled_quantity.get().to_string();
            let row = client
                .query_opt(
                    r#"
SELECT raw_hash = $2 AS raw_matches,
       cumulative_filled_quantity = $3::text::numeric AS quantity_matches
FROM public.broker_order_events
WHERE broker_event_id = $1
"#,
                    &[broker_event_id, &raw_payload_hash.as_hex(), &expected],
                )
                .await
                .map_err(|error| StoreError::database("resolve_broker_event_commit", error))?;
            let Some(row) = row else {
                return Ok(CommitResolution::NotCommitted);
            };
            let raw_matches: bool = row
                .try_get("raw_matches")
                .map_err(|error| StoreError::database("decode_recovered_broker_hash", error))?;
            let quantity_matches: bool = row
                .try_get("quantity_matches")
                .map_err(|error| StoreError::database("decode_recovered_broker_quantity", error))?;
            Ok(if raw_matches && quantity_matches {
                CommitResolution::Committed
            } else {
                CommitResolution::ConflictingEvidence
            })
        }
        CommitRecoveryKey::Reconciliation {
            reconciliation_id,
            evidence_hash,
        } => {
            let row = client
                .query_opt(
                    "SELECT evidence_hash FROM public.reconciliation_runs WHERE reconciliation_id = $1 AND completed_at IS NOT NULL",
                    &[reconciliation_id],
                )
                .await
                .map_err(|error| StoreError::database("resolve_reconciliation_commit", error))?;
            let Some(row) = row else {
                return Ok(CommitResolution::NotCommitted);
            };
            let stored: String = row
                .try_get("evidence_hash")
                .map_err(|error| StoreError::database("decode_recovered_reconciliation", error))?;
            Ok(
                if HashDigest::from_str(&stored)
                    .map_err(|error| StoreError::InvalidInput(error.to_string()))?
                    == *evidence_hash
                {
                    CommitResolution::Committed
                } else {
                    CommitResolution::ConflictingEvidence
                },
            )
        }
        CommitRecoveryKey::SubmissionUnknown {
            outbox_id,
            state_event_id,
            evidence_hash,
        } => {
            let row = client
                .query_opt(
                    r#"
SELECT
    event.reason_code,
    event.detail,
    event.fencing_token,
    event.occurred_at
FROM public.intent_state_events AS event
JOIN public.order_outbox AS outbox ON outbox.intent_id = event.intent_id
WHERE outbox.outbox_id = $1
  AND event.intent_state_event_id = $2
  AND event.state = 'submission_unknown'
"#,
                    &[outbox_id, state_event_id],
                )
                .await
                .map_err(|error| {
                    StoreError::database("resolve_submission_unknown_commit", error)
                })?;
            let Some(row) = row else {
                return Ok(CommitResolution::NotCommitted);
            };
            let reason_code: String = row
                .try_get("reason_code")
                .map_err(|error| StoreError::database("decode_submission_unknown_reason", error))?;
            let detail: Value = row
                .try_get("detail")
                .map_err(|error| StoreError::database("decode_submission_unknown_detail", error))?;
            let fencing_token: i64 = row
                .try_get("fencing_token")
                .map_err(|error| StoreError::database("decode_submission_unknown_fence", error))?;
            let occurred_at: DateTime<Utc> = row
                .try_get("occurred_at")
                .map_err(|error| StoreError::database("decode_submission_unknown_time", error))?;
            let stored_hash = submission_unknown_evidence_hash(
                &reason_code,
                &detail,
                fencing_token,
                occurred_at,
            )?;
            Ok(if stored_hash == *evidence_hash {
                CommitResolution::Committed
            } else {
                CommitResolution::ConflictingEvidence
            })
        }
        CommitRecoveryKey::OutboxFinalization {
            outbox_id,
            completion_reason,
        } => {
            let row = client
                .query_opt(
                    r#"
SELECT completed_at IS NOT NULL AS completed, completion_reason
FROM public.order_outbox
WHERE outbox_id = $1
"#,
                    &[outbox_id],
                )
                .await
                .map_err(|error| {
                    StoreError::database("resolve_outbox_finalization_commit", error)
                })?;
            let Some(row) = row else {
                return Ok(CommitResolution::NotCommitted);
            };
            let completed: bool = row
                .try_get("completed")
                .map_err(|error| StoreError::database("decode_outbox_finalization_state", error))?;
            if !completed {
                return Ok(CommitResolution::NotCommitted);
            }
            let stored_reason: Option<String> = row
                .try_get("completion_reason")
                .map_err(|error| StoreError::database("decode_outbox_completion_reason", error))?;
            Ok(
                if stored_reason.as_deref() == Some(completion_reason.as_str()) {
                    CommitResolution::Committed
                } else {
                    CommitResolution::ConflictingEvidence
                },
            )
        }
    }
}

fn join_reason_codes(reason_codes: &[String], field: &str) -> Result<String, StoreError> {
    if reason_codes.is_empty()
        || reason_codes
            .iter()
            .any(|reason| reason.trim().is_empty() || reason.contains('|'))
    {
        return Err(StoreError::InvalidInput(format!(
            "{field} must contain non-empty unambiguous values"
        )));
    }
    Ok(reason_codes.join("|"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SchemaColumn {
    table: String,
    column: String,
    data_type: String,
    nullable: bool,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpectedSchemaColumn {
    table: &'static str,
    column: &'static str,
    data_type: &'static str,
    nullable: bool,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
}

macro_rules! column {
    ($table:literal, $column:literal, $type:literal, $nullable:literal) => {
        ExpectedSchemaColumn {
            table: $table,
            column: $column,
            data_type: $type,
            nullable: $nullable,
            numeric_precision: None,
            numeric_scale: None,
        }
    };
}

macro_rules! numeric_column {
    ($table:literal, $column:literal, $nullable:literal, $precision:literal, $scale:literal) => {
        ExpectedSchemaColumn {
            table: $table,
            column: $column,
            data_type: "numeric",
            nullable: $nullable,
            numeric_precision: Some($precision),
            numeric_scale: Some($scale),
        }
    };
}

const EXPECTED_SCHEMA_COLUMNS: &[ExpectedSchemaColumn] = &[
    column!("strategy_releases", "release_id", "uuid", false),
    column!("activation_permits", "permit_id", "uuid", false),
    column!("executor_leases", "fencing_token", "bigint", false),
    column!(
        "executor_leases",
        "lease_until",
        "timestamp with time zone",
        false
    ),
    column!("decision_snapshots", "decision_id", "uuid", false),
    column!("decision_snapshots", "strategy_release_id", "uuid", false),
    column!("decision_snapshots", "environment", "text", false),
    column!("decision_snapshots", "account_fingerprint", "text", true),
    column!("decision_snapshots", "market_session", "date", false),
    column!(
        "decision_snapshots",
        "as_of",
        "timestamp with time zone",
        false
    ),
    column!("decision_snapshots", "payload", "jsonb", false),
    column!("target_portfolios", "target_portfolio_id", "uuid", false),
    column!("target_positions", "target_quantity", "bigint", false),
    numeric_column!("target_positions", "target_weight", false, 20, 6),
    column!("risk_decisions", "risk_decision_id", "uuid", false),
    column!("risk_decisions", "reason_codes", "ARRAY", false),
    column!("risk_decisions", "limit_snapshot", "jsonb", false),
    column!("order_plans", "order_plan_id", "uuid", false),
    numeric_column!("order_plans", "decision_reference_price", false, 38, 6),
    column!("order_intents", "intent_id", "uuid", false),
    column!("order_intents", "client_order_id", "text", false),
    numeric_column!("order_intents", "limit_price", false, 38, 6),
    numeric_column!("order_intents", "arrival_quote", false, 38, 6),
    column!(
        "order_intents",
        "quote_provider_at",
        "timestamp with time zone",
        false
    ),
    column!(
        "order_intents",
        "quote_received_at",
        "timestamp with time zone",
        false
    ),
    column!(
        "order_intents",
        "quote_valid_until",
        "timestamp with time zone",
        false
    ),
    column!("order_intents", "decision_evidence_hash", "text", false),
    column!(
        "order_intents",
        "materialization_evidence_hash",
        "text",
        false
    ),
    column!(
        "intent_state_events",
        "intent_state_event_id",
        "uuid",
        false
    ),
    column!("intent_state_events", "event_sequence", "bigint", false),
    column!("intent_state_events", "state", "text", false),
    column!("order_outbox", "outbox_id", "uuid", false),
    column!("order_outbox", "created_fencing_token", "bigint", false),
    column!("order_outbox", "claim_fencing_token", "bigint", true),
    column!("order_outbox", "payload", "jsonb", false),
    column!("broker_orders", "broker_order_id", "text", false),
    column!("broker_order_events", "broker_event_id", "uuid", false),
    column!("broker_order_events", "event_sequence", "bigint", false),
    numeric_column!(
        "broker_order_events",
        "cumulative_filled_quantity",
        false,
        38,
        6
    ),
    numeric_column!("broker_order_events", "average_fill_price", true, 38, 6),
    column!("broker_order_events", "raw_payload", "jsonb", false),
    column!("fills", "fill_id", "text", false),
    numeric_column!("fills", "quantity", false, 38, 6),
    numeric_column!("fills", "price", false, 38, 6),
    numeric_column!("fills", "fee", false, 38, 6),
    column!("account_snapshots", "account_snapshot_id", "uuid", false),
    numeric_column!("account_snapshots", "cash", false, 38, 6),
    numeric_column!("account_snapshots", "equity", false, 38, 6),
    numeric_column!("account_snapshots", "buying_power", false, 38, 6),
    column!("account_snapshots", "payload", "jsonb", false),
    column!("reconciliation_runs", "reconciliation_id", "uuid", false),
    column!("reconciliation_runs", "authority_sequence", "bigint", false),
    column!(
        "reconciliation_runs",
        "completed_at",
        "timestamp with time zone",
        true
    ),
    column!("reconciliation_runs", "resumable", "boolean", false),
    column!(
        "reconciliation_diffs",
        "reconciliation_diff_id",
        "uuid",
        false
    ),
    column!("reconciliation_diffs", "resolution", "text", false),
    column!("runtime_schema_attestations", "object_kind", "text", false),
    column!(
        "runtime_schema_attestations",
        "object_identity",
        "text",
        false
    ),
    column!(
        "runtime_schema_attestations",
        "definition_sha256",
        "text",
        false
    ),
];

fn schema_column_from_row(row: &Row) -> Result<SchemaColumn, StoreError> {
    let nullable: String = row
        .try_get("is_nullable")
        .map_err(|error| StoreError::database("decode_schema_nullable", error))?;
    Ok(SchemaColumn {
        table: row
            .try_get("table_name")
            .map_err(|error| StoreError::database("decode_schema_table", error))?,
        column: row
            .try_get("column_name")
            .map_err(|error| StoreError::database("decode_schema_column", error))?,
        data_type: row
            .try_get("data_type")
            .map_err(|error| StoreError::database("decode_schema_type", error))?,
        nullable: nullable == "YES",
        numeric_precision: row
            .try_get("numeric_precision")
            .map_err(|error| StoreError::database("decode_schema_precision", error))?,
        numeric_scale: row
            .try_get("numeric_scale")
            .map_err(|error| StoreError::database("decode_schema_scale", error))?,
    })
}

fn compare_schema_columns(observed: &[SchemaColumn]) -> Vec<String> {
    let observed: BTreeMap<_, _> = observed
        .iter()
        .map(|column| ((column.table.as_str(), column.column.as_str()), column))
        .collect();
    let mut mismatches = Vec::new();
    for expected in EXPECTED_SCHEMA_COLUMNS {
        let key = (expected.table, expected.column);
        match observed.get(&key) {
            None => mismatches.push(format!(
                "column:{}.{}:missing",
                expected.table, expected.column
            )),
            Some(actual)
                if actual.data_type != expected.data_type
                    || actual.nullable != expected.nullable
                    || (expected.numeric_precision.is_some()
                        && actual.numeric_precision != expected.numeric_precision)
                    || (expected.numeric_scale.is_some()
                        && actual.numeric_scale != expected.numeric_scale) =>
            {
                mismatches.push(format!(
                    "column:{}.{}:definition",
                    expected.table, expected.column
                ));
            }
            Some(_) => {}
        }
    }
    mismatches
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn tls_config_has_no_plaintext_or_credential_bearing_variant() {
        let valid = TlsRequiredDatabaseConfig {
            environment: Environment::Live,
            server_name: "database.example.internal".into(),
            trust_anchor: DatabaseTrustAnchor::PinnedBundleSha256(HashDigest::sha256(b"test-ca")),
        };
        assert!(valid.validate().is_ok());

        for unsafe_name in [
            "postgres://user:secret@database.example",
            "user@database.example",
            "database.example:5432",
            "database.example/path",
        ] {
            let mut invalid = valid.clone();
            invalid.server_name = unsafe_name.into();
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn schema_comparison_detects_missing_and_changed_contracts() {
        let mut observed: Vec<_> = EXPECTED_SCHEMA_COLUMNS
            .iter()
            .map(|expected| SchemaColumn {
                table: expected.table.into(),
                column: expected.column.into(),
                data_type: expected.data_type.into(),
                nullable: expected.nullable,
                numeric_precision: expected.numeric_precision,
                numeric_scale: expected.numeric_scale,
            })
            .collect();
        assert!(compare_schema_columns(&observed).is_empty());

        observed.retain(|column| {
            !(column.table == "order_outbox" && column.column == "created_fencing_token")
        });
        observed
            .iter_mut()
            .find(|column| column.table == "order_intents" && column.column == "limit_price")
            .unwrap()
            .numeric_scale = Some(2);
        let mismatches: BTreeSet<_> = compare_schema_columns(&observed).into_iter().collect();
        assert!(mismatches.contains("column:order_outbox.created_fencing_token:missing"));
        assert!(mismatches.contains("column:order_intents.limit_price:definition"));
    }

    #[test]
    fn dispatch_and_recovery_use_distinct_server_authority_functions() {
        assert!(CLAIM_FIRST_DISPATCH_SQL.contains("claim_order_outbox_v2("));
        assert!(!CLAIM_FIRST_DISPATCH_SQL.contains("recovery"));
        assert!(CLAIM_RECOVERY_SQL.contains("claim_order_outbox_recovery_v2("));
        assert!(CLAIM_COMPLETION_SQL.contains("claim_order_outbox_completion_v2("));
        assert!(SCHEMA_GUARDS_SQL.contains("prosecdef"));
        assert!(SCHEMA_GUARDS_SQL.contains("runtime_schema_attestations"));
        assert!(SCHEMA_GUARDS_SQL.contains("definition_sha256"));
        assert!(SCHEMA_GUARDS_SQL.contains("pg_get_triggerdef"));
        assert!(SCHEMA_GUARDS_SQL.contains("tgenabled"));
        assert!(RUNTIME_PRIVILEGES_SQL.contains("has_function_privilege"));
        assert!(RUNTIME_PRIVILEGES_SQL.contains("has_database_privilege"));
    }

    #[test]
    fn runtime_mutations_use_only_fenced_security_definer_boundaries() {
        for statement in [
            INSERT_DECISION_SQL,
            INSERT_TARGET_SQL,
            INSERT_TARGET_POSITION_SQL,
            INSERT_RISK_SQL,
            INSERT_PLAN_SQL,
            INSERT_INTENT_SQL,
            INSERT_INTENT_STATE_SQL,
            INSERT_BROKER_ORDER_SQL,
            INSERT_BROKER_EVENT_SQL,
            INSERT_FILL_SQL,
            INSERT_ACCOUNT_SNAPSHOT_SQL,
            INSERT_RECONCILIATION_DIFF_SQL,
        ] {
            let normalized = statement.to_ascii_uppercase();
            assert!(normalized.contains("SELECT PUBLIC.INSERT_"));
            assert!(!normalized.contains("INSERT INTO"));
            assert!(!normalized.contains("UPDATE "));
            assert!(!normalized.contains("DELETE "));
        }
        assert!(FINALIZE_RECONCILIATION_SQL
            .to_ascii_uppercase()
            .contains("SELECT PUBLIC.FINALIZE_RECONCILIATION_V2"));
    }

    #[test]
    fn broker_status_mapping_matches_database_allowlist_and_fails_closed() {
        for status in [
            "accepted",
            "new",
            "pending_new",
            "accepted_for_bidding",
            "partially_filled",
            "filled",
            "done_for_day",
            "canceled",
            "expired",
            "replaced",
            "pending_cancel",
            "pending_replace",
            "stopped",
            "rejected",
            "suspended",
            "calculated",
        ] {
            assert!(is_recognized_broker_status(status));
        }
        assert!(!is_recognized_broker_status("future_provider_state"));
        assert_eq!(
            broker_state_transitions("dispatch_started", "future_provider_state", false).unwrap(),
            ["blocked"]
        );
    }

    #[test]
    fn broker_state_projection_handles_first_fill_and_recovery() {
        assert_eq!(
            broker_state_transitions("dispatch_started", "filled", true).unwrap(),
            ["broker_confirmed", "terminal"]
        );
        assert_eq!(
            broker_state_transitions("submission_unknown", "accepted", true).unwrap(),
            ["broker_confirmed"]
        );
        assert!(broker_state_transitions("terminal", "accepted", true).is_err());
    }

    #[test]
    fn lease_ttl_is_bounded_by_database_contract() {
        assert!(validate_ttl(Duration::ZERO).is_err());
        assert_eq!(validate_ttl(Duration::from_secs(30)).unwrap(), 30_000_000);
        assert_eq!(validate_ttl(Duration::from_secs(60)).unwrap(), 60_000_000);
        assert!(validate_ttl(Duration::from_secs(61)).is_err());
        assert!(ACQUIRE_LEASE_SQL.contains("$4::bigint * INTERVAL '1 microsecond'"));
        assert!(RENEW_LEASE_SQL.contains("$5::bigint * INTERVAL '1 microsecond'"));
    }

    #[test]
    fn store_rejects_symbols_outside_the_narrower_sql_contract() {
        assert!(validate_store_symbol("SPY").is_ok());
        assert!(validate_store_symbol("BRK.B").is_ok());
        assert!(validate_store_symbol(".SPY").is_err());
        assert!(validate_store_symbol("ABCDEFGHIJKLMNOP").is_err());
    }

    #[test]
    fn durable_recovery_and_fill_truth_are_explicit_contracts() {
        assert!(APPEND_SUBMISSION_UNKNOWN_SQL.contains("append_submission_unknown_v2"));
        assert!(LIST_UNRESOLVED_OUTBOXES_SQL.contains("list_unresolved_order_outboxes_v2"));
        let migration = include_str!("../../../migrations/0007_store_hardening.sql");
        assert!(migration.contains("broker_order_events_require_fill_truth"));
        assert!(migration.contains("intent_state_events_require_terminal_fill_truth"));
        assert!(migration.contains("intent_state.state = 'terminal'"));
        assert!(!is_terminal_broker_status("accepted"));
        assert!(!is_terminal_broker_status("partially_filled"));
    }

    #[test]
    fn derived_database_ids_are_deterministic_and_domain_separated() {
        let parent = Uuid::parse_str("2e01c7b7-19f1-46a0-b39c-f64006da7e3d").unwrap();
        assert_eq!(
            stable_child_uuid(parent, "state:persisted"),
            stable_child_uuid(parent, "state:persisted")
        );
        assert_ne!(
            stable_child_uuid(parent, "state:persisted"),
            stable_child_uuid(parent, "outbox:intent-committed")
        );
    }
}
