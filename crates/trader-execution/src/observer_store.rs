//! Dedicated PostgreSQL store for the paper read-only observer.
//!
//! This module is intentionally unrelated to `PgExecutionStore`. It owns a
//! separately verified PostgreSQL client, implements only `CoordinatorStore`,
//! and calls only the nine observer functions introduced by migration 0009.

use std::{collections::BTreeMap, str::FromStr};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_postgres::{Client, IsolationLevel, Row};
use trader_core::{
    Environment, HashDigest, OrderSide, Price, ReconciliationDifference,
    ReconciliationDifferenceKind, Symbol, WholeQuantity,
};
use uuid::Uuid;

use crate::coordinator::{
    AccountingCashEvidence, CanonicalOrderEvidence, CoordinatorMode, CoordinatorPortError,
    CoordinatorStore, LocalOrderLedgerFact, LocalProjection, LocalProjectionBlocker, ObserverCycle,
    ObserverCycleKey, ObserverPersistenceKey, ObserverPersistenceResolution, PageCompletionWitness,
    PaperObserverLease, SourcePageEvidence, SourcePageKind, StableFillIdentityEvidence,
    StartupFailureStage, StartupOutcome, StartupReason, StartupResult,
};

const MAX_LEASE_MICROSECONDS: i64 = 60_000_000;
const PROJECTION_SCHEMA: &str = "wasp2/paper-observer-local-projection/v1";
const PROJECTION_POSITIONS_BASIS: &str = "durable_fill_ledger_partial";

const ACQUIRE_LEASE_SQL: &str = r#"
SELECT fencing_token, lease_until
FROM public.acquire_paper_observer_lease_v1(
    $1, $2, $3::bigint * INTERVAL '1 microsecond'
)
"#;

const RENEW_LEASE_SQL: &str = r#"
SELECT fencing_token, lease_until
FROM public.renew_paper_observer_lease_v1(
    $1, $2, $3, $4::bigint * INTERVAL '1 microsecond'
)
"#;

const READ_PROJECTION_SQL: &str = r#"
SELECT observed_at, payload
FROM public.read_paper_observer_projection_v1($1, $2, $3)
"#;

const BEGIN_CYCLE_SQL: &str = r#"
SELECT public.begin_paper_observer_cycle_v1(
    $1, $2, $3, $4, $5, $6, $7
) AS persisted
"#;

const RECORD_PROJECTION_SQL: &str = r#"
SELECT public.record_paper_observer_projection_v1(
    $1, $2, $3, $4, $5, $6, $7, $8, $9
) AS persisted
"#;

const RECORD_PAGE_SQL: &str = r#"
SELECT public.record_paper_observer_page_v1(
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
) AS persisted
"#;

const RECORD_DIFFERENCE_SQL: &str = r#"
SELECT public.record_paper_observer_difference_v1(
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
) AS persisted
"#;

const COMPLETE_CYCLE_SQL: &str = r#"
SELECT public.complete_paper_observer_cycle_v1(
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
) AS persisted
"#;

const INSPECT_CYCLE_SQL: &str = r#"
SELECT
    identity_hash, projection_hash, page_count, page_manifest_hash,
    difference_count, difference_manifest_hash, outcome, result_hash
FROM public.inspect_paper_observer_cycle_v1($1, $2, $3, $4)
"#;

/// Verifies the observer's exact callable surface, its read-only access to the
/// immutable attestation manifest, the current definitions covered by that
/// manifest, and the absence of authority in every non-system schema.
const OBSERVER_SCHEMA_AND_PRIVILEGES_SQL: &str = r#"
WITH allowed_function(signature, result_type) AS (
    VALUES
        ('public.acquire_paper_observer_lease_v1(text,uuid,interval)',
         'TABLE(fencing_token bigint, lease_until timestamp with time zone)'),
        ('public.renew_paper_observer_lease_v1(text,uuid,bigint,interval)',
         'TABLE(fencing_token bigint, lease_until timestamp with time zone)'),
        ('public.read_paper_observer_projection_v1(text,uuid,bigint)',
         'TABLE(observed_at timestamp with time zone, payload jsonb)'),
        ('public.begin_paper_observer_cycle_v1(text,uuid,bigint,uuid,text,timestamp with time zone,text)',
         'boolean'),
        ('public.record_paper_observer_projection_v1(text,uuid,bigint,uuid,uuid,timestamp with time zone,jsonb,text,text)',
         'boolean'),
        ('public.record_paper_observer_page_v1(text,uuid,bigint,uuid,uuid,smallint,text,integer,text,text,text,timestamp with time zone,integer,text,text)',
         'boolean'),
        ('public.record_paper_observer_difference_v1(text,uuid,bigint,uuid,uuid,integer,text,text,text,jsonb,jsonb,text,text,text,timestamp with time zone)',
         'boolean'),
        ('public.complete_paper_observer_cycle_v1(text,uuid,bigint,uuid,uuid,text,text,text[],text[],integer,text,integer,text,jsonb,text,timestamp with time zone)',
         'boolean'),
        ('public.inspect_paper_observer_cycle_v1(text,uuid,bigint,uuid)',
         'TABLE(identity_hash text, projection_hash text, page_count integer, page_manifest_hash text, difference_count integer, difference_manifest_hash text, outcome text, result_hash text)')
), attested_function(signature, security_definer, trusted_search_path) AS (
    VALUES
        ('public.acquire_paper_observer_lease_v1(text,uuid,interval)', TRUE, TRUE),
        ('public.renew_paper_observer_lease_v1(text,uuid,bigint,interval)', TRUE, TRUE),
        ('public.read_paper_observer_projection_v1(text,uuid,bigint)', TRUE, TRUE),
        ('public.begin_paper_observer_cycle_v1(text,uuid,bigint,uuid,text,timestamp with time zone,text)', TRUE, TRUE),
        ('public.record_paper_observer_projection_v1(text,uuid,bigint,uuid,uuid,timestamp with time zone,jsonb,text,text)', TRUE, TRUE),
        ('public.record_paper_observer_page_v1(text,uuid,bigint,uuid,uuid,smallint,text,integer,text,text,text,timestamp with time zone,integer,text,text)', TRUE, TRUE),
        ('public.record_paper_observer_difference_v1(text,uuid,bigint,uuid,uuid,integer,text,text,text,jsonb,jsonb,text,text,text,timestamp with time zone)', TRUE, TRUE),
        ('public.complete_paper_observer_cycle_v1(text,uuid,bigint,uuid,uuid,text,text,text[],text[],integer,text,integer,text,jsonb,text,timestamp with time zone)', TRUE, TRUE),
        ('public.inspect_paper_observer_cycle_v1(text,uuid,bigint,uuid)', TRUE, TRUE),
        ('public.enforce_paper_observer_child_append()', TRUE, TRUE),
        ('public.enforce_paper_observer_completion()', TRUE, TRUE),
        ('public.reject_audit_mutation()', FALSE, FALSE)
), required_view(identity) AS (
    VALUES
        ('public.broker_position_quantities'),
        ('public.current_intent_states'),
        ('public.current_broker_order_states')
), required_relation(identity) AS (
    VALUES
        ('public.order_intents'),
        ('public.intent_state_events'),
        ('public.broker_orders'),
        ('public.broker_order_events'),
        ('public.fills'),
        ('public.order_outbox'),
        ('public.cancel_outbox')
), critical_table(table_name) AS (
    VALUES
        ('paper_observer_leases'),
        ('paper_observer_cycles'),
        ('paper_observer_local_projections'),
        ('paper_observer_pages'),
        ('paper_observer_differences'),
        ('paper_observer_completions'),
        ('paper_observer_schema_attestations')
), required_trigger(table_name, trigger_name, function_signature) AS (
    VALUES
        ('paper_observer_cycles', 'paper_observer_cycles_reject_mutation',
         'public.reject_audit_mutation()'),
        ('paper_observer_local_projections',
         'paper_observer_local_projections_reject_mutation',
         'public.reject_audit_mutation()'),
        ('paper_observer_pages', 'paper_observer_pages_reject_mutation',
         'public.reject_audit_mutation()'),
        ('paper_observer_differences', 'paper_observer_differences_reject_mutation',
         'public.reject_audit_mutation()'),
        ('paper_observer_completions', 'paper_observer_completions_reject_mutation',
         'public.reject_audit_mutation()'),
        ('paper_observer_schema_attestations',
         'paper_observer_schema_attestations_reject_mutation',
         'public.reject_audit_mutation()'),
        ('paper_observer_local_projections',
         'paper_observer_local_projections_enforce_open_cycle',
         'public.enforce_paper_observer_child_append()'),
        ('paper_observer_pages', 'paper_observer_pages_enforce_open_cycle',
         'public.enforce_paper_observer_child_append()'),
        ('paper_observer_differences',
         'paper_observer_differences_enforce_open_cycle',
         'public.enforce_paper_observer_child_append()'),
        ('paper_observer_completions',
         'paper_observer_completions_enforce_evidence',
         'public.enforce_paper_observer_completion()')
), observed(object_kind, object_identity, definition_sha256, safe) AS (
    SELECT
        'function', required.signature,
        CASE WHEN procedure.oid IS NULL THEN NULL ELSE
            encode(sha256(convert_to(pg_get_functiondef(procedure.oid), 'UTF8')), 'hex')
        END,
        COALESCE(
            procedure.prokind = 'f'
            AND procedure.prosecdef = required.security_definer
            AND (
                NOT required.trusted_search_path
                OR procedure.proconfig @>
                    ARRAY['search_path=pg_catalog, public']::text[]
            )
            AND procedure.proowner = (
                SELECT relowner FROM pg_class
                WHERE oid = 'public.paper_observer_schema_attestations'::regclass
            ),
            FALSE
        )
    FROM attested_function AS required
    LEFT JOIN pg_proc AS procedure
      ON procedure.oid = to_regprocedure(required.signature)
    UNION ALL
    SELECT
        'view', required.identity,
        CASE WHEN relation.oid IS NULL THEN NULL ELSE
            encode(sha256(convert_to(pg_get_viewdef(relation.oid, true), 'UTF8')), 'hex')
        END,
        COALESCE(relation.relkind = 'v', FALSE)
    FROM required_view AS required
    LEFT JOIN pg_class AS relation
      ON relation.oid = to_regclass(required.identity)
    UNION ALL
    SELECT
        'relation', required.identity,
        CASE WHEN relation.oid IS NULL THEN NULL ELSE
            encode(sha256(convert_to(
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
        END,
        COALESCE(relation.relkind IN ('r', 'p'), FALSE)
    FROM required_relation AS required
    LEFT JOIN pg_class AS relation
      ON relation.oid = to_regclass(required.identity)
    LEFT JOIN pg_attribute AS attribute
      ON attribute.attrelid = relation.oid
     AND attribute.attnum > 0
     AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef AS default_value
      ON default_value.adrelid = relation.oid
     AND default_value.adnum = attribute.attnum
    GROUP BY required.identity, relation.oid, relation.relkind
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
    FROM public.paper_observer_schema_attestations AS manifest
    FULL OUTER JOIN observed
      ON observed.object_kind = manifest.object_kind
     AND observed.object_identity = manifest.object_identity
), checks(object_name, present) AS (
    SELECT 'role:current-login', COALESCE((
        SELECT
            login.rolcanlogin
            AND login.rolinherit
            AND NOT login.rolsuper
            AND NOT login.rolcreatedb
            AND NOT login.rolcreaterole
            AND NOT login.rolreplication
            AND NOT login.rolbypassrls
            AND current_user = session_user
            AND current_user <> 'alpaca_trader_observer'
            AND pg_has_role(current_user, 'alpaca_trader_observer', 'MEMBER')
            AND NOT pg_has_role(current_user, 'alpaca_trader_runtime', 'MEMBER')
            AND NOT pg_has_role(current_user, 'alpaca_trader_operator', 'MEMBER')
        FROM pg_roles AS login
        WHERE login.rolname = current_user
    ), FALSE)
    UNION ALL
    SELECT 'role:alpaca_trader_observer', COALESCE((
        SELECT
            NOT observer.rolcanlogin
            AND NOT observer.rolinherit
            AND NOT observer.rolsuper
            AND NOT observer.rolcreatedb
            AND NOT observer.rolcreaterole
            AND NOT observer.rolreplication
            AND NOT observer.rolbypassrls
            AND NOT EXISTS (
                SELECT 1 FROM pg_database WHERE datdba = observer.oid
            )
            AND NOT EXISTS (
                SELECT 1 FROM pg_namespace WHERE nspowner = observer.oid
            )
            AND NOT EXISTS (
                SELECT 1 FROM pg_class WHERE relowner = observer.oid
            )
            AND NOT EXISTS (
                SELECT 1 FROM pg_proc WHERE proowner = observer.oid
            )
        FROM pg_roles AS observer
        WHERE observer.rolname = 'alpaca_trader_observer'
    ), FALSE)
    UNION ALL
    SELECT 'database:connect-only',
        has_database_privilege(current_user, current_database(), 'CONNECT')
        AND NOT has_database_privilege(current_user, current_database(), 'CREATE')
        AND NOT has_database_privilege(current_user, current_database(), 'TEMPORARY')
    UNION ALL
    SELECT 'schema:' || namespace.nspname || ':bounded',
        CASE WHEN namespace.nspname = 'public' THEN
            has_schema_privilege(current_user, namespace.oid, 'USAGE')
            AND NOT has_schema_privilege(current_user, namespace.oid, 'CREATE')
        ELSE
            NOT has_schema_privilege(current_user, namespace.oid, 'USAGE')
            AND NOT has_schema_privilege(current_user, namespace.oid, 'CREATE')
        END
    FROM pg_namespace AS namespace
    WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
      AND namespace.nspname !~ '^pg_(toast|temp)'
    UNION ALL
    SELECT 'table:' || required.table_name || ':present',
        relation.oid IS NOT NULL
        AND relation.relkind IN ('r', 'p')
        AND relation.relowner = (
            SELECT relowner FROM pg_class
            WHERE oid = 'public.paper_observer_schema_attestations'::regclass
        )
    FROM critical_table AS required
    LEFT JOIN pg_class AS relation
      ON relation.oid = to_regclass('public.' || required.table_name)
    UNION ALL
    SELECT 'relation:' || namespace.nspname || '.' || relation.relname || ':bounded',
        CASE WHEN namespace.nspname = 'public'
                  AND relation.relname = 'paper_observer_schema_attestations' THEN
            has_table_privilege(current_user, relation.oid, 'SELECT')
        ELSE
            NOT has_table_privilege(current_user, relation.oid, 'SELECT')
        END
        AND NOT has_table_privilege(current_user, relation.oid, 'INSERT')
        AND NOT has_table_privilege(current_user, relation.oid, 'UPDATE')
        AND NOT has_table_privilege(current_user, relation.oid, 'DELETE')
        AND NOT has_table_privilege(current_user, relation.oid, 'TRUNCATE')
        AND NOT has_table_privilege(current_user, relation.oid, 'REFERENCES')
        AND NOT has_table_privilege(current_user, relation.oid, 'TRIGGER')
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
      AND namespace.nspname !~ '^pg_(toast|temp)'
      AND relation.relkind IN ('r', 'p', 'v', 'm')
    UNION ALL
    SELECT 'sequence:' || namespace.nspname || '.' || sequence.relname || ':none',
        NOT has_sequence_privilege(current_user, sequence.oid, 'USAGE')
        AND NOT has_sequence_privilege(current_user, sequence.oid, 'SELECT')
        AND NOT has_sequence_privilege(current_user, sequence.oid, 'UPDATE')
    FROM pg_class AS sequence
    JOIN pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
    WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
      AND namespace.nspname !~ '^pg_(toast|temp)'
      AND sequence.relkind = 'S'
    UNION ALL
    SELECT 'function:' || allowed.signature || ':exact-execute',
        procedure.oid IS NOT NULL
        AND procedure.prokind = 'f'
        AND procedure.prosecdef
        AND procedure.proconfig @> ARRAY['search_path=pg_catalog, public']::text[]
        AND pg_get_function_result(procedure.oid) = allowed.result_type
        AND has_function_privilege(current_user, procedure.oid, 'EXECUTE')
        AND procedure.proowner = (
            SELECT relowner FROM pg_class
            WHERE oid = 'public.paper_observer_schema_attestations'::regclass
        )
    FROM allowed_function AS allowed
    LEFT JOIN pg_proc AS procedure
      ON procedure.oid = to_regprocedure(allowed.signature)
    UNION ALL
    SELECT 'function:' || procedure.oid::regprocedure::text || ':forbidden-execute', FALSE
    FROM pg_proc AS procedure
    JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
      AND namespace.nspname !~ '^pg_(toast|temp)'
      AND procedure.prokind IN ('f', 'p')
      AND has_function_privilege(current_user, procedure.oid, 'EXECUTE')
      AND NOT EXISTS (
          SELECT 1 FROM allowed_function AS allowed
          WHERE to_regprocedure(allowed.signature) = procedure.oid
      )
    UNION ALL
    SELECT 'trigger:' || required.table_name || '.' || required.trigger_name,
        trigger.oid IS NOT NULL
        AND trigger.tgenabled IN ('O', 'A')
        AND trigger.tgfoid = to_regprocedure(required.function_signature)
    FROM required_trigger AS required
    LEFT JOIN pg_class AS relation
      ON relation.oid = to_regclass('public.' || required.table_name)
    LEFT JOIN pg_trigger AS trigger
      ON trigger.tgrelid = relation.oid
     AND trigger.tgname = required.trigger_name
     AND NOT trigger.tgisinternal
    UNION ALL
    SELECT 'constraint:' || namespace.nspname || '.' || relation.relname || '.' || con.conname,
        con.convalidated
    FROM pg_constraint AS con
    JOIN pg_class AS relation ON relation.oid = con.conrelid
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    JOIN critical_table AS critical ON critical.table_name = relation.relname
    WHERE namespace.nspname = 'public'
    UNION ALL
    SELECT 'attestation:' || object_kind || ':' || object_identity,
        expected_hash IS NOT NULL
        AND expected_hash = observed_hash
        AND safe
    FROM compared
)
SELECT object_name, present FROM checks
"#;

/// Fixed, redacted construction failures. Database messages and values never
/// cross this boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ObserverStoreError {
    #[error("paper observer store configuration was rejected")]
    UnsafeConfiguration,
    #[error("paper observer store schema or privileges failed verification")]
    VerificationFailed,
}

/// Evidence that the separately connected client used an approved TLS name
/// and pinned trust bundle. The secret-bearing connector constructs this type.
pub(crate) struct ObserverStoreConfig {
    server_name: String,
    trust_digest: HashDigest,
}

impl ObserverStoreConfig {
    pub(crate) const fn new(server_name: String, trust_digest: HashDigest) -> Self {
        Self {
            server_name,
            trust_digest,
        }
    }

    fn validate(&self) -> Result<(), ObserverStoreError> {
        let server_name = self.server_name.trim();
        if server_name.is_empty()
            || server_name != self.server_name
            || server_name.len() > 253
            || server_name.contains(['/', '@', ':'])
            || server_name.starts_with('.')
            || server_name.ends_with('.')
            || server_name.split('.').any(|label| {
                label.is_empty()
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '-')
            })
            || self.trust_digest.as_hex().len() != 64
        {
            return Err(ObserverStoreError::UnsafeConfiguration);
        }
        Ok(())
    }
}

struct CachedProjection {
    observed_at: DateTime<Utc>,
    payload: Value,
    payload_hash: HashDigest,
    domain_hash: HashDigest,
}

struct CycleInspection {
    identity_hash: String,
    projection_hash: Option<String>,
    page_count: i32,
    page_manifest_hash: String,
    difference_count: i32,
    difference_manifest_hash: String,
    outcome: Option<String>,
    result_hash: Option<String>,
}

struct PersistedDifference {
    category: &'static str,
    kind: &'static str,
    subject: String,
    local_value: Option<Value>,
    broker_value: Option<Value>,
    detail: String,
    detail_code: &'static str,
    evidence_hash: HashDigest,
}

/// PostgreSQL implementation of only the provider-free coordinator port.
///
/// There is deliberately no client accessor, execution-store conversion, or
/// `Deref` implementation.
pub(crate) struct PgObserverStore {
    client: Client,
    active_lease: Option<PaperObserverLease>,
    projections: BTreeMap<Uuid, CachedProjection>,
}

impl PgObserverStore {
    pub(crate) async fn from_verified_client(
        client: Client,
        config: ObserverStoreConfig,
    ) -> Result<Self, ObserverStoreError> {
        config.validate()?;
        let store = Self {
            client,
            active_lease: None,
            projections: BTreeMap::new(),
        };
        store.verify_schema_and_privileges().await?;
        Ok(store)
    }

    async fn verify_schema_and_privileges(&self) -> Result<(), ObserverStoreError> {
        let rows = self
            .client
            .query(OBSERVER_SCHEMA_AND_PRIVILEGES_SQL, &[])
            .await
            .map_err(|_| ObserverStoreError::VerificationFailed)?;
        if rows.is_empty() {
            return Err(ObserverStoreError::VerificationFailed);
        }
        for row in rows {
            let present: bool = row
                .try_get("present")
                .map_err(|_| ObserverStoreError::VerificationFailed)?;
            if !present {
                return Err(ObserverStoreError::VerificationFailed);
            }
        }
        Ok(())
    }

    fn validate_active_lease(
        &self,
        lease: &PaperObserverLease,
    ) -> Result<i64, CoordinatorPortError> {
        let active = self
            .active_lease
            .as_ref()
            .ok_or(CoordinatorPortError::StoreUnavailable)?;
        if lease.environment != Environment::Paper
            || active != lease
            || lease.owner_id.is_nil()
            || lease.fencing_token == 0
        {
            return Err(CoordinatorPortError::StoreUnavailable);
        }
        i64::try_from(lease.fencing_token).map_err(|_| CoordinatorPortError::StoreUnavailable)
    }

    fn validate_cycle_lease(
        &self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<i64, CoordinatorPortError> {
        let token = self.validate_active_lease(lease)?;
        if cycle.account_fingerprint() != lease.account_fingerprint
            || cycle.owner_id() != lease.owner_id
            || cycle.fencing_token() != lease.fencing_token
            || cycle.cycle_id().is_nil()
        {
            return Err(CoordinatorPortError::ConflictingEvidence);
        }
        Ok(token)
    }

    async fn inspect_cycle(
        &self,
        cycle_id: Uuid,
    ) -> Result<Option<CycleInspection>, CoordinatorPortError> {
        let lease = self
            .active_lease
            .as_ref()
            .ok_or(CoordinatorPortError::StoreUnavailable)?;
        let token = i64::try_from(lease.fencing_token)
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        let row = self
            .client
            .query_opt(
                INSPECT_CYCLE_SQL,
                &[
                    &lease.account_fingerprint.as_hex(),
                    &lease.owner_id,
                    &token,
                    &cycle_id,
                ],
            )
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        row.map(decode_inspection).transpose()
    }
}

#[async_trait]
impl CoordinatorStore for PgObserverStore {
    async fn acquire_observer_lease(
        &mut self,
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
        let ttl_micros = validated_ttl(ttl)?;
        if owner_id.is_nil() {
            return Err(CoordinatorPortError::StoreUnavailable);
        }
        let token_and_expiry = self
            .client
            .query_opt(
                ACQUIRE_LEASE_SQL,
                &[&account_fingerprint.as_hex(), &owner_id, &ttl_micros],
            )
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
            .map(decode_lease_row)
            .transpose()?;
        let lease = token_and_expiry.map(|(fencing_token, lease_until)| PaperObserverLease {
            environment: Environment::Paper,
            account_fingerprint,
            owner_id,
            fencing_token,
            lease_until,
        });
        self.active_lease = lease.clone();
        Ok(lease)
    }

    async fn renew_observer_lease(
        &mut self,
        lease: &PaperObserverLease,
        ttl: Duration,
    ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
        let ttl_micros = validated_ttl(ttl)?;
        let token = self.validate_active_lease(lease)?;
        let token_and_expiry = self
            .client
            .query_opt(
                RENEW_LEASE_SQL,
                &[
                    &lease.account_fingerprint.as_hex(),
                    &lease.owner_id,
                    &token,
                    &ttl_micros,
                ],
            )
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
            .map(decode_lease_row)
            .transpose()?;
        let renewed = token_and_expiry.map(|(fencing_token, lease_until)| PaperObserverLease {
            environment: Environment::Paper,
            account_fingerprint: lease.account_fingerprint,
            owner_id: lease.owner_id,
            fencing_token,
            lease_until,
        });
        self.active_lease = renewed.clone();
        Ok(renewed)
    }

    async fn begin_cycle(
        &mut self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<(), CoordinatorPortError> {
        let token = self.validate_cycle_lease(cycle, lease)?;
        match self.resolve_cycle_start(&cycle.key()).await? {
            ObserverPersistenceResolution::Committed => return Ok(()),
            ObserverPersistenceResolution::ConflictingEvidence => {
                return Err(CoordinatorPortError::ConflictingEvidence)
            }
            ObserverPersistenceResolution::NotCommitted => {}
        }

        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        let row = transaction
            .query_one(
                BEGIN_CYCLE_SQL,
                &[
                    &cycle.account_fingerprint().as_hex(),
                    &cycle.owner_id(),
                    &token,
                    &cycle.cycle_id(),
                    &"startup",
                    &cycle.started_at(),
                    &cycle.key().evidence_hash.as_hex(),
                ],
            )
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        require_persisted(&row)?;
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorPortError::CycleStartOutcomeUnknown)
    }

    async fn resolve_cycle_start(
        &mut self,
        key: &ObserverCycleKey,
    ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
        let Some(inspection) = self.inspect_cycle(key.cycle_id).await? else {
            return Ok(ObserverPersistenceResolution::NotCommitted);
        };
        if inspection.identity_hash == key.evidence_hash.as_hex() {
            Ok(ObserverPersistenceResolution::Committed)
        } else {
            Ok(ObserverPersistenceResolution::ConflictingEvidence)
        }
    }

    async fn load_local_projection(
        &mut self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<LocalProjection, CoordinatorPortError> {
        let token = self.validate_cycle_lease(cycle, lease)?;
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        let row = transaction
            .query_one(
                READ_PROJECTION_SQL,
                &[
                    &cycle.account_fingerprint().as_hex(),
                    &cycle.owner_id(),
                    &token,
                ],
            )
            .await
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        let observed_at: DateTime<Utc> = row
            .try_get("observed_at")
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        let payload: Value = row
            .try_get("payload")
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        let projection =
            decode_projection(payload.clone(), cycle.account_fingerprint(), observed_at)?;
        let payload_hash = HashDigest::of_json(&payload)
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        let domain_hash = HashDigest::of_json(&projection)
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        self.projections.insert(
            cycle.cycle_id(),
            CachedProjection {
                observed_at,
                payload,
                payload_hash,
                domain_hash,
            },
        );
        Ok(projection)
    }

    async fn persist_cycle_result(
        &mut self,
        cycle: &ObserverCycle,
        result: &StartupResult,
        key: &ObserverPersistenceKey,
        lease: &PaperObserverLease,
    ) -> Result<(), CoordinatorPortError> {
        let token = self.validate_cycle_lease(cycle, lease)?;
        validate_result_identity(cycle, result, key)?;
        match self.resolve_cycle_completion(key).await? {
            ObserverPersistenceResolution::Committed => return Ok(()),
            ObserverPersistenceResolution::ConflictingEvidence => {
                return Err(CoordinatorPortError::ConflictingEvidence)
            }
            ObserverPersistenceResolution::NotCommitted => {}
        }

        let projection = self.projections.get(&cycle.cycle_id());
        match (result.local_evidence_hash(), projection) {
            (Some(expected), Some(cached)) if expected == cached.domain_hash => {}
            (Some(_), _) => return Err(CoordinatorPortError::ConflictingEvidence),
            (None, _) => {}
        }

        let pages = sorted_pages(result.source_page_evidence());
        let page_manifest_hash = manifest_hash(
            pages
                .iter()
                .map(|page| page.evidence_hash.as_hex())
                .collect::<Vec<_>>(),
        );
        let differences = persisted_differences(result.reconciliation())?;
        let difference_manifest_hash = manifest_hash(
            differences
                .iter()
                .map(|difference| difference.evidence_hash.as_hex())
                .collect::<Vec<_>>(),
        );
        let page_count =
            i32::try_from(pages.len()).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        let difference_count =
            i32::try_from(differences.len()).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        let result_payload =
            serde_json::to_value(result).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        let reason_codes = result
            .reasons()
            .iter()
            .map(reason_code)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let broker_hashes = result
            .broker_evidence_hashes()
            .iter()
            .map(HashDigest::as_hex)
            .collect::<Vec<_>>();
        let outcome = outcome_code(result.outcome());
        let failure_stage = result.failure_stage().map(failure_stage_code);
        let completion_id = deterministic_child_id(
            cycle.cycle_id(),
            &format!("completion:v1:{}", key.evidence_hash),
        );

        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;

        if let Some(projection) = projection {
            let projection_id = deterministic_child_id(
                cycle.cycle_id(),
                &format!("projection:v1:{}", projection.payload_hash),
            );
            let row = transaction
                .query_one(
                    RECORD_PROJECTION_SQL,
                    &[
                        &cycle.account_fingerprint().as_hex(),
                        &cycle.owner_id(),
                        &token,
                        &cycle.cycle_id(),
                        &projection_id,
                        &projection.observed_at,
                        &projection.payload,
                        &projection.payload_hash.as_hex(),
                        &projection.domain_hash.as_hex(),
                    ],
                )
                .await
                .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
            require_persisted(&row)?;
        }

        for page in pages {
            let page_id = deterministic_child_id(
                cycle.cycle_id(),
                &format!(
                    "page:v1:{}:{}:{}:{}",
                    page.snapshot_round,
                    source_code(page.kind),
                    page.page_ordinal,
                    page.evidence_hash
                ),
            );
            let snapshot_round = i16::from(page.snapshot_round);
            let page_ordinal = i32::try_from(page.page_ordinal)
                .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
            let item_count = i32::try_from(page.item_count)
                .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
            let completion_witness = page.completion_witness.map(witness_code);
            let row = transaction
                .query_one(
                    RECORD_PAGE_SQL,
                    &[
                        &cycle.account_fingerprint().as_hex(),
                        &cycle.owner_id(),
                        &token,
                        &cycle.cycle_id(),
                        &page_id,
                        &snapshot_round,
                        &source_code(page.kind),
                        &page_ordinal,
                        &page.request_parameters_hash.as_hex(),
                        &page.request_id,
                        &page.raw_payload_hash.as_hex(),
                        &page.received_at,
                        &item_count,
                        &completion_witness,
                        &page.evidence_hash.as_hex(),
                    ],
                )
                .await
                .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
            require_persisted(&row)?;
        }

        for (ordinal, difference) in differences.iter().enumerate() {
            let ordinal =
                i32::try_from(ordinal).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
            let difference_id = deterministic_child_id(
                cycle.cycle_id(),
                &format!("difference:v1:{ordinal}:{}", difference.evidence_hash),
            );
            let row = transaction
                .query_one(
                    RECORD_DIFFERENCE_SQL,
                    &[
                        &cycle.account_fingerprint().as_hex(),
                        &cycle.owner_id(),
                        &token,
                        &cycle.cycle_id(),
                        &difference_id,
                        &ordinal,
                        &difference.category,
                        &difference.kind,
                        &difference.subject,
                        &difference.local_value,
                        &difference.broker_value,
                        &difference.detail,
                        &difference.detail_code,
                        &difference.evidence_hash.as_hex(),
                        &result.generated_at(),
                    ],
                )
                .await
                .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
            require_persisted(&row)?;
        }

        let row = transaction
            .query_one(
                COMPLETE_CYCLE_SQL,
                &[
                    &cycle.account_fingerprint().as_hex(),
                    &cycle.owner_id(),
                    &token,
                    &cycle.cycle_id(),
                    &completion_id,
                    &outcome,
                    &failure_stage,
                    &reason_codes,
                    &broker_hashes,
                    &page_count,
                    &page_manifest_hash.as_hex(),
                    &difference_count,
                    &difference_manifest_hash.as_hex(),
                    &result_payload,
                    &key.evidence_hash.as_hex(),
                    &result.generated_at(),
                ],
            )
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        require_persisted(&row)?;
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorPortError::CycleCompletionOutcomeUnknown)?;
        self.projections.remove(&cycle.cycle_id());
        Ok(())
    }

    async fn resolve_cycle_completion(
        &mut self,
        key: &ObserverPersistenceKey,
    ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
        let Some(inspection) = self.inspect_cycle(key.cycle_id).await? else {
            return Ok(ObserverPersistenceResolution::NotCommitted);
        };
        if inspection.identity_hash.is_empty() {
            return Ok(ObserverPersistenceResolution::ConflictingEvidence);
        }
        match inspection.result_hash.as_deref() {
            None => Ok(ObserverPersistenceResolution::NotCommitted),
            Some(hash) if hash == key.evidence_hash.as_hex() => {
                self.projections.remove(&key.cycle_id);
                Ok(ObserverPersistenceResolution::Committed)
            }
            Some(_) => Ok(ObserverPersistenceResolution::ConflictingEvidence),
        }
    }
}

fn validated_ttl(ttl: Duration) -> Result<i64, CoordinatorPortError> {
    let micros = ttl
        .num_microseconds()
        .ok_or(CoordinatorPortError::StoreUnavailable)?;
    if !(1..=MAX_LEASE_MICROSECONDS).contains(&micros) {
        return Err(CoordinatorPortError::StoreUnavailable);
    }
    Ok(micros)
}

fn decode_lease_row(row: Row) -> Result<(u64, DateTime<Utc>), CoordinatorPortError> {
    let token: i64 = row
        .try_get("fencing_token")
        .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    let lease_until = row
        .try_get("lease_until")
        .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    let token = u64::try_from(token).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    if token == 0 {
        return Err(CoordinatorPortError::StoreUnavailable);
    }
    Ok((token, lease_until))
}

fn decode_inspection(row: Row) -> Result<CycleInspection, CoordinatorPortError> {
    let inspection = CycleInspection {
        identity_hash: row
            .try_get("identity_hash")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        projection_hash: row
            .try_get("projection_hash")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        page_count: row
            .try_get("page_count")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        page_manifest_hash: row
            .try_get("page_manifest_hash")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        difference_count: row
            .try_get("difference_count")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        difference_manifest_hash: row
            .try_get("difference_manifest_hash")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        outcome: row
            .try_get("outcome")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
        result_hash: row
            .try_get("result_hash")
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?,
    };
    if HashDigest::from_str(&inspection.identity_hash).is_err()
        || inspection
            .projection_hash
            .as_ref()
            .is_some_and(|hash| HashDigest::from_str(hash).is_err())
        || inspection.page_count < 0
        || HashDigest::from_str(&inspection.page_manifest_hash).is_err()
        || inspection.difference_count < 0
        || HashDigest::from_str(&inspection.difference_manifest_hash).is_err()
        || inspection
            .result_hash
            .as_ref()
            .is_some_and(|hash| HashDigest::from_str(hash).is_err())
        || inspection
            .outcome
            .as_deref()
            .is_some_and(|outcome| !matches!(outcome, "blocked" | "failed"))
    {
        return Err(CoordinatorPortError::ConflictingEvidence);
    }
    Ok(inspection)
}

fn require_persisted(row: &Row) -> Result<(), CoordinatorPortError> {
    match row.try_get::<_, bool>("persisted") {
        Ok(true) => Ok(()),
        _ => Err(CoordinatorPortError::StoreUnavailable),
    }
}

fn validate_result_identity(
    cycle: &ObserverCycle,
    result: &StartupResult,
    key: &ObserverPersistenceKey,
) -> Result<(), CoordinatorPortError> {
    let calculated =
        HashDigest::of_json(result).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    let normalized_hashes = result
        .normalized_broker_snapshots()
        .iter()
        .map(HashDigest::of_json)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    if result.cycle_id() != cycle.cycle_id()
        || key.cycle_id != cycle.cycle_id()
        || key.evidence_hash != calculated
        || result.environment() != Environment::Paper
        || result.mode() != CoordinatorMode::ReconcileOnly
        || result.resumable()
        || normalized_hashes != result.broker_evidence_hashes()
        || (result.outcome() == StartupOutcome::Blocked
            && result.normalized_broker_snapshots().len() != 2)
        || (result.outcome() == StartupOutcome::Failed
            && (!result.normalized_broker_snapshots().is_empty()
                || !result.broker_evidence_hashes().is_empty()
                || !result.source_page_evidence().is_empty()))
    {
        return Err(CoordinatorPortError::ConflictingEvidence);
    }
    Ok(())
}

fn deterministic_child_id(cycle_id: Uuid, material: &str) -> Uuid {
    Uuid::new_v5(&cycle_id, material.as_bytes())
}

fn manifest_hash(hashes: Vec<String>) -> HashDigest {
    HashDigest::sha256(hashes.join("\n"))
}

fn sorted_pages(pages: &[SourcePageEvidence]) -> Vec<&SourcePageEvidence> {
    let mut pages = pages.iter().collect::<Vec<_>>();
    pages.sort_by_key(|page| {
        (
            page.snapshot_round,
            source_rank(page.kind),
            page.page_ordinal,
        )
    });
    pages
}

fn source_rank(source: SourcePageKind) -> u8 {
    match source {
        SourcePageKind::Account => 1,
        SourcePageKind::Positions => 2,
        SourcePageKind::OpenOrders => 3,
        SourcePageKind::ClosedOrders => 4,
        SourcePageKind::FillActivities => 5,
    }
}

fn source_code(source: SourcePageKind) -> &'static str {
    match source {
        SourcePageKind::Account => "account",
        SourcePageKind::Positions => "positions",
        SourcePageKind::OpenOrders => "open_orders",
        SourcePageKind::ClosedOrders => "closed_orders",
        SourcePageKind::FillActivities => "fill_activities",
    }
}

fn witness_code(witness: PageCompletionWitness) -> &'static str {
    match witness {
        PageCompletionWitness::Single => "single",
        PageCompletionWitness::ShortPage => "short_page",
        PageCompletionWitness::TimestampHorizonCrossed => "timestamp_horizon_crossed",
    }
}

fn outcome_code(outcome: StartupOutcome) -> &'static str {
    match outcome {
        StartupOutcome::Blocked => "blocked",
        StartupOutcome::Failed => "failed",
    }
}

fn failure_stage_code(stage: StartupFailureStage) -> &'static str {
    match stage {
        StartupFailureStage::Configuration => "configuration",
        StartupFailureStage::LocalProjection => "local_projection",
        StartupFailureStage::BrokerRound1 => "broker_round_1",
        StartupFailureStage::BrokerRound2 => "broker_round_2",
        StartupFailureStage::FenceRenewal => "fence_renewal",
        StartupFailureStage::DatabaseConnection => "database_connection",
        StartupFailureStage::Persistence => "persistence",
        StartupFailureStage::Shutdown => "shutdown",
    }
}

fn reason_code(reason: &StartupReason) -> &'static str {
    match reason {
        StartupReason::ReadOnlyPolicy => "read_only_policy",
        StartupReason::IndependentCashBasisMissing => "independent_cash_basis_missing",
        StartupReason::StableRestFillIdentityMissing => "stable_rest_fill_identity_missing",
        StartupReason::CanonicalLocalOrderTruthMissing => "canonical_local_order_truth_missing",
        StartupReason::LocalAccountFingerprintMismatch => "local_account_fingerprint_mismatch",
        StartupReason::BrokerAccountFingerprintMismatch => "broker_account_fingerprint_mismatch",
        StartupReason::BrokerSnapshotUnstable => "broker_snapshot_unstable",
        StartupReason::BrokerAccountNotActive => "broker_account_not_active",
        StartupReason::BrokerTradingBlocked => "broker_trading_blocked",
        StartupReason::BrokerAccountBlocked => "broker_account_blocked",
        StartupReason::BrokerTransfersBlocked => "broker_transfers_blocked",
        StartupReason::BrokerTradeSuspendedByUser => "broker_trade_suspended_by_user",
        StartupReason::BrokerAccountNotUsd => "broker_account_not_usd",
        StartupReason::BrokerAccruedFeesNonzero => "broker_accrued_fees_nonzero",
        StartupReason::BrokerPendingTransfers => "broker_pending_transfers",
        StartupReason::BrokerPositionIdentityIncomplete => "broker_position_identity_incomplete",
        StartupReason::ReconciliationDifferences => "reconciliation_differences",
        StartupReason::LocalProjectionUnavailable => "local_projection_unavailable",
        StartupReason::BrokerSnapshotUnavailable => "broker_snapshot_unavailable",
        StartupReason::SourcePageEvidenceUnavailable => "source_page_evidence_unavailable",
        StartupReason::EvidenceHashUnavailable => "evidence_hash_unavailable",
        StartupReason::BrokerSnapshotTimedOut => "broker_snapshot_timed_out",
        StartupReason::UnresolvedOrderOutbox => "unresolved_order_outbox",
        StartupReason::UnresolvedCancelOutbox => "unresolved_cancel_outbox",
    }
}

fn persisted_differences(
    report: Option<&trader_core::ReconciliationReport>,
) -> Result<Vec<PersistedDifference>, CoordinatorPortError> {
    report
        .map(|report| {
            report
                .differences
                .iter()
                .map(persisted_difference)
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn persisted_difference(
    difference: &ReconciliationDifference,
) -> Result<PersistedDifference, CoordinatorPortError> {
    if difference.subject.trim() != difference.subject
        || difference.subject.is_empty()
        || difference.subject.len() > 256
        || difference
            .local_value
            .as_ref()
            .is_some_and(|value| value.len() > 512)
        || difference
            .broker_value
            .as_ref()
            .is_some_and(|value| value.len() > 512)
        || difference.detail.trim().is_empty()
        || difference.detail.len() > 512
    {
        return Err(CoordinatorPortError::StoreUnavailable);
    }
    let category = difference_category(difference);
    let kind = difference_kind(&difference.kind);
    let detail_code = difference_detail_code(difference);
    let local_value = difference
        .local_value
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    let broker_value = difference
        .broker_value
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    let material = PersistedDifferenceHashMaterial {
        category,
        kind,
        subject: &difference.subject,
        local_value: &local_value,
        broker_value: &broker_value,
        detail: &difference.detail,
        detail_code,
    };
    let evidence_hash =
        HashDigest::of_json(&material).map_err(|_| CoordinatorPortError::StoreUnavailable)?;
    Ok(PersistedDifference {
        category,
        kind,
        subject: difference.subject.clone(),
        local_value,
        broker_value,
        detail: difference.detail.clone(),
        detail_code,
        evidence_hash,
    })
}

#[derive(Serialize)]
struct PersistedDifferenceHashMaterial<'a> {
    category: &'a str,
    kind: &'a str,
    subject: &'a str,
    local_value: &'a Option<Value>,
    broker_value: &'a Option<Value>,
    detail: &'a str,
    detail_code: &'a str,
}

fn difference_category(difference: &ReconciliationDifference) -> &'static str {
    match difference.subject.as_str() {
        "independent_cash_basis" | "cash" => "cash",
        "stable_rest_fill_identity" => "fill",
        "canonical_local_order_truth" => "order",
        "local_account"
        | "broker_account"
        | "broker_account_status"
        | "broker_trading_block"
        | "broker_account_block"
        | "broker_transfer_block"
        | "broker_user_trade_suspension"
        | "broker_account_currency"
        | "broker_accrued_fees"
        | "broker_pending_transfers" => "account",
        "broker_position_asset_identity" => "position",
        "broker_snapshot" => "snapshot",
        subject if subject.starts_with("fill:") => "fill",
        subject
            if subject.starts_with("order_")
                || subject.starts_with("cancel_")
                || subject.starts_with("order_identity:")
                || subject.starts_with("order_contract:") =>
        {
            "order"
        }
        subject if Symbol::new(subject).is_ok() => "position",
        _ => "order",
    }
}

fn difference_kind(kind: &ReconciliationDifferenceKind) -> &'static str {
    match kind {
        ReconciliationDifferenceKind::MissingLocally => "missing_locally",
        ReconciliationDifferenceKind::MissingAtBroker => "missing_at_broker",
        ReconciliationDifferenceKind::QuantityMismatch => "quantity_mismatch",
        ReconciliationDifferenceKind::CashMismatch => "cash_mismatch",
        ReconciliationDifferenceKind::StatusMismatch => "status_mismatch",
        ReconciliationDifferenceKind::UnknownProviderState => "unknown_provider_state",
    }
}

fn difference_detail_code(difference: &ReconciliationDifference) -> &'static str {
    match difference.subject.as_str() {
        "independent_cash_basis" => "INDEPENDENT_CASH_BASIS_MISSING",
        "stable_rest_fill_identity" => "STABLE_REST_FILL_IDENTITY_MISSING",
        "canonical_local_order_truth" => "CANONICAL_LOCAL_ORDER_TRUTH_MISSING",
        "broker_accrued_fees" => "BROKER_ACCRUED_FEES_NONZERO",
        "broker_pending_transfers" => "BROKER_PENDING_TRANSFERS",
        "broker_position_asset_identity" => "BROKER_POSITION_IDENTITY_INCOMPLETE",
        _ => match difference.kind {
            ReconciliationDifferenceKind::MissingLocally => "MISSING_LOCALLY",
            ReconciliationDifferenceKind::MissingAtBroker => "MISSING_AT_BROKER",
            ReconciliationDifferenceKind::QuantityMismatch => "QUANTITY_MISMATCH",
            ReconciliationDifferenceKind::CashMismatch => "CASH_MISMATCH",
            ReconciliationDifferenceKind::StatusMismatch => "STATUS_MISMATCH",
            ReconciliationDifferenceKind::UnknownProviderState => "UNKNOWN_PROVIDER_STATE",
        },
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionWire {
    schema: String,
    environment: String,
    account_fingerprint: HashDigest,
    cash_basis: MissingBasisWire,
    fill_identity_basis: MissingBasisWire,
    order_identity_basis: MissingBasisWire,
    positions_basis: String,
    positions: Vec<PositionWire>,
    orders: Vec<Value>,
    order_ledger_facts: Vec<OrderLedgerFactWire>,
    unresolved_order_outboxes: Vec<Uuid>,
    unresolved_cancel_outboxes: Vec<Uuid>,
    blockers: Vec<LocalProjectionBlocker>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MissingBasisWire {
    status: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionWire {
    symbol: Symbol,
    quantity: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderLedgerFactWire {
    intent_id: Uuid,
    client_order_id: String,
    provider_order_id: Option<String>,
    symbol: Symbol,
    side: OrderSide,
    whole_quantity: WholeQuantity,
    limit_price: String,
    time_in_force: String,
    intent_state: Option<String>,
    provider_status: Option<String>,
    recognized_status: Option<bool>,
    cumulative_filled_quantity: Option<String>,
}

fn decode_projection(
    payload: Value,
    expected_account: HashDigest,
    observed_at: DateTime<Utc>,
) -> Result<LocalProjection, CoordinatorPortError> {
    let wire: ProjectionWire =
        serde_json::from_value(payload).map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
    if wire.schema != PROJECTION_SCHEMA
        || wire.environment != "paper"
        || wire.account_fingerprint != expected_account
        || wire.cash_basis.status != "missing"
        || wire.fill_identity_basis.status != "missing"
        || wire.order_identity_basis.status != "missing"
        || wire.positions_basis != PROJECTION_POSITIONS_BASIS
        || !wire.orders.is_empty()
        || !has_exact_blockers(&wire.blockers)
    {
        return Err(CoordinatorPortError::ProjectionUnavailable);
    }

    let mut positions = BTreeMap::new();
    for position in wire.positions {
        let quantity = position
            .quantity
            .parse::<u64>()
            .map(WholeQuantity::new)
            .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
        if positions.insert(position.symbol, quantity).is_some() {
            return Err(CoordinatorPortError::ProjectionUnavailable);
        }
    }
    let order_ledger_facts = wire
        .order_ledger_facts
        .into_iter()
        .map(|fact| {
            let limit_price = Price::from_str(&fact.limit_price)
                .map_err(|_| CoordinatorPortError::ProjectionUnavailable)?;
            let cumulative_filled_quantity = fact
                .cumulative_filled_quantity
                .map(|quantity| {
                    quantity
                        .parse::<u64>()
                        .map(WholeQuantity::new)
                        .map_err(|_| CoordinatorPortError::ProjectionUnavailable)
                })
                .transpose()?;
            Ok(LocalOrderLedgerFact {
                intent_id: fact.intent_id,
                client_order_id: fact.client_order_id,
                provider_order_id: fact.provider_order_id,
                symbol: fact.symbol,
                side: fact.side,
                whole_quantity: fact.whole_quantity,
                limit_price,
                time_in_force: fact.time_in_force,
                intent_state: fact.intent_state,
                provider_status: fact.provider_status,
                recognized_status: fact.recognized_status,
                cumulative_filled_quantity,
            })
        })
        .collect::<Result<Vec<_>, CoordinatorPortError>>()?;

    Ok(LocalProjection {
        observed_at,
        account_fingerprint: wire.account_fingerprint,
        accounting_cash: AccountingCashEvidence::Missing,
        positions,
        canonical_orders: CanonicalOrderEvidence::Missing,
        order_ledger_facts,
        unresolved_order_outboxes: wire.unresolved_order_outboxes,
        unresolved_cancel_outboxes: wire.unresolved_cancel_outboxes,
        blockers: wire.blockers,
        stable_fill_identities: StableFillIdentityEvidence::Missing,
    })
}

fn has_exact_blockers(blockers: &[LocalProjectionBlocker]) -> bool {
    blockers.len() == 3
        && blockers.contains(&LocalProjectionBlocker::IndependentCashBasisMissing)
        && blockers.contains(&LocalProjectionBlocker::StableRestFillIdentityMissing)
        && blockers.contains(&LocalProjectionBlocker::CanonicalLocalOrderTruthMissing)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn projection_payload(account: HashDigest) -> Value {
        json!({
            "schema": PROJECTION_SCHEMA,
            "environment": "paper",
            "account_fingerprint": account,
            "cash_basis": {"status": "missing"},
            "fill_identity_basis": {"status": "missing"},
            "order_identity_basis": {"status": "missing"},
            "positions_basis": PROJECTION_POSITIONS_BASIS,
            "positions": [{"symbol": "SPY", "quantity": "2"}],
            "orders": [],
            "order_ledger_facts": [{
                "intent_id": "10000000-0000-0000-0000-000000000001",
                "client_order_id": "client-1",
                "provider_order_id": null,
                "symbol": "SPY",
                "side": "buy",
                "whole_quantity": 2,
                "limit_price": "501.250000",
                "time_in_force": "day",
                "intent_state": "persisted",
                "provider_status": null,
                "recognized_status": null,
                "cumulative_filled_quantity": null
            }],
            "unresolved_order_outboxes": [],
            "unresolved_cancel_outboxes": [],
            "blockers": [
                "independent_cash_basis_missing",
                "stable_rest_fill_identity_missing",
                "canonical_local_order_truth_missing"
            ]
        })
    }

    #[test]
    fn migration_projection_decodes_without_fabricating_complete_evidence() {
        let account = HashDigest::sha256("paper-account");
        let observed_at = "2026-07-19T14:00:00Z".parse().unwrap();
        let projection =
            decode_projection(projection_payload(account), account, observed_at).unwrap();
        assert_eq!(
            projection.positions[&Symbol::new("SPY").unwrap()],
            WholeQuantity::new(2)
        );
        assert!(matches!(
            projection.accounting_cash,
            AccountingCashEvidence::Missing
        ));
        assert!(matches!(
            projection.canonical_orders,
            CanonicalOrderEvidence::Missing
        ));
        assert!(matches!(
            projection.stable_fill_identities,
            StableFillIdentityEvidence::Missing
        ));
        assert_eq!(projection.order_ledger_facts.len(), 1);
    }

    #[test]
    fn projection_rejects_fabricated_or_incomplete_basis() {
        let account = HashDigest::sha256("paper-account");
        let observed_at = "2026-07-19T14:00:00Z".parse().unwrap();
        let mut payload = projection_payload(account);
        payload["cash_basis"] = json!({"status": "complete", "amount": "1000"});
        assert_eq!(
            decode_projection(payload, account, observed_at),
            Err(CoordinatorPortError::ProjectionUnavailable)
        );

        let mut payload = projection_payload(account);
        payload["orders"] = json!([{"client_order_id": "client-1"}]);
        assert_eq!(
            decode_projection(payload, account, observed_at),
            Err(CoordinatorPortError::ProjectionUnavailable)
        );
    }

    #[test]
    fn manifests_match_postgresql_newline_contract() {
        let hashes = vec!["a".repeat(64), "b".repeat(64)];
        assert_eq!(
            manifest_hash(hashes.clone()),
            HashDigest::sha256(hashes.join("\n"))
        );
        assert_eq!(manifest_hash(Vec::new()), HashDigest::sha256(""));
    }

    #[test]
    fn differences_preserve_exact_values_and_safety_taxonomy() {
        let fee = persisted_difference(&ReconciliationDifference {
            kind: ReconciliationDifferenceKind::StatusMismatch,
            subject: "broker_accrued_fees".into(),
            local_value: None,
            broker_value: Some("0.01".into()),
            detail: "broker reports nonzero accrued fees".into(),
        })
        .unwrap();
        assert_eq!(fee.category, "account");
        assert_eq!(fee.kind, "status_mismatch");
        assert_eq!(fee.local_value, None);
        assert_eq!(fee.broker_value, Some(json!("0.01")));
        assert_eq!(fee.detail_code, "BROKER_ACCRUED_FEES_NONZERO");

        let cash = persisted_difference(&ReconciliationDifference {
            kind: ReconciliationDifferenceKind::CashMismatch,
            subject: "cash".into(),
            local_value: Some("999.00".into()),
            broker_value: Some("1000.00".into()),
            detail: "local and broker cash differ".into(),
        })
        .unwrap();
        assert_eq!(cash.category, "cash");
        assert_eq!(cash.kind, "cash_mismatch");
        assert_eq!(cash.detail_code, "CASH_MISMATCH");
    }

    #[test]
    fn store_config_rejects_non_dns_escape_hatches() {
        let digest = HashDigest::sha256("ca");
        assert!(
            ObserverStoreConfig::new("wasp2.abc.us-east-1.rds.amazonaws.com".into(), digest)
                .validate()
                .is_ok()
        );
        assert_eq!(
            ObserverStoreConfig::new("postgres://example.com".into(), digest).validate(),
            Err(ObserverStoreError::UnsafeConfiguration)
        );
    }

    #[test]
    fn runtime_verifier_models_callable_and_trigger_security_modes() {
        assert!(OBSERVER_SCHEMA_AND_PRIVILEGES_SQL.contains(
            "('public.acquire_paper_observer_lease_v1(text,uuid,interval)', TRUE, TRUE)"
        ));
        assert!(OBSERVER_SCHEMA_AND_PRIVILEGES_SQL
            .contains("('public.enforce_paper_observer_child_append()', TRUE, TRUE)"));
        assert!(OBSERVER_SCHEMA_AND_PRIVILEGES_SQL
            .contains("('public.reject_audit_mutation()', FALSE, FALSE)"));
        assert!(OBSERVER_SCHEMA_AND_PRIVILEGES_SQL
            .contains("procedure.prosecdef = required.security_definer"));
    }
}
