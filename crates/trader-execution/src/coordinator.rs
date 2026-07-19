//! Provider-free startup coordination for the paper, read-only runtime.
//!
//! This kernel deliberately has no execution-enabled state and no broker
//! mutation capability. It acquires a fence, compares two complete read-only
//! broker snapshots, reconciles them with the durable local projection, renews
//! the fence, and persists a result that can never authorize execution.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trader_core::{
    AccountStatus, Environment, Fixed, HashDigest, Money, OrderSide, Price,
    ReconciliationDifference, ReconciliationDifferenceKind, ReconciliationReport, Symbol,
    WholeQuantity,
};
use uuid::Uuid;

use crate::reconciliation::{reconcile, ReconciliationInput};

const OBSERVER_CYCLE_NAMESPACE: Uuid = Uuid::from_u128(0x5d39f77e_7a95_5cc8_a1ef_dfcda2650a27);
const FILL_IDENTITY_SCHEMA: &[u8] = b"wasp2/stable-rest-fill-identity/v1";

/// Injected wall clock for terminal observer evidence. Broker and database
/// work may take time; a cycle completion must describe when that work ended,
/// not merely reuse the cycle's start or last source timestamp.
pub trait CoordinatorClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCoordinatorClock;

impl CoordinatorClock for SystemCoordinatorClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Exact configuration accepted by this kernel. Paper and read-only are
/// encoded in the type rather than represented by mutable booleans or modes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperReadOnlyConfig {
    pub expected_account_fingerprint: HashDigest,
    pub owner_id: Uuid,
    pub fence_ttl: Duration,
}

impl PaperReadOnlyConfig {
    pub const fn environment(&self) -> Environment {
        Environment::Paper
    }

    pub const fn mode(&self) -> CoordinatorMode {
        CoordinatorMode::ReconcileOnly
    }

    pub fn validate(&self) -> Result<(), CoordinatorError> {
        if self.owner_id.is_nil() {
            return Err(CoordinatorError::UnsafeConfiguration(
                "coordinator owner ID must not be nil".into(),
            ));
        }
        if self.fence_ttl <= Duration::zero() {
            return Err(CoordinatorError::UnsafeConfiguration(
                "coordinator fence TTL must be positive".into(),
            ));
        }
        if self.fence_ttl > Duration::seconds(60) {
            return Err(CoordinatorError::UnsafeConfiguration(
                "coordinator fence TTL must not exceed 60 seconds".into(),
            ));
        }
        self.snapshot_timeout()?;
        Ok(())
    }

    fn snapshot_timeout(&self) -> Result<std::time::Duration, CoordinatorError> {
        let ttl = self.fence_ttl.to_std().map_err(|_| {
            CoordinatorError::UnsafeConfiguration(
                "coordinator fence TTL must convert to a positive runtime duration".into(),
            )
        })?;
        let timeout = ttl / 3;
        if timeout.is_zero() || timeout >= ttl {
            return Err(CoordinatorError::UnsafeConfiguration(
                "coordinator fence TTL must allow a nonzero broker timeout below the TTL".into(),
            ));
        }
        Ok(timeout)
    }
}

/// The only runtime mode this coordinator can represent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorMode {
    ReconcileOnly,
}

/// A paper observer lease is intentionally a different capability from an
/// execution fence. It cannot be passed to the executor or converted into its
/// fenced mutation authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PaperObserverLease {
    pub environment: Environment,
    pub account_fingerprint: HashDigest,
    pub owner_id: Uuid,
    pub fencing_token: u64,
    pub lease_until: DateTime<Utc>,
}

/// Independent cash evidence. A broker snapshot is never an accounting basis.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum AccountingCashEvidence {
    Missing,
    Complete {
        cash: Money,
        basis_hash: HashDigest,
        coverage_hash: HashDigest,
    },
}

impl AccountingCashEvidence {
    fn cash(&self) -> Option<Money> {
        match self {
            Self::Missing => None,
            Self::Complete { cash, .. } => Some(*cash),
        }
    }
}

/// Stable REST FILL-activity identity evidence. An empty complete set means
/// that a complete activity traversal found no fills; `Missing` means the
/// traversal/identity contract was not proven and must block reconciliation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum StableFillIdentityEvidence {
    Missing,
    Complete {
        identity_schema_hash: HashDigest,
        coverage_hash: HashDigest,
        fingerprints: Vec<HashDigest>,
    },
}

impl StableFillIdentityEvidence {
    fn sorted_fingerprints(&self) -> Option<Vec<HashDigest>> {
        match self {
            Self::Missing => None,
            Self::Complete { fingerprints, .. } => {
                let mut fingerprints = fingerprints.clone();
                fingerprints.sort_unstable();
                Some(fingerprints)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalOrderTruth {
    pub client_order_id: String,
    pub provider_order_id: Option<String>,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub whole_quantity: WholeQuantity,
    pub limit_price: Price,
    pub time_in_force: String,
    pub provider_status: Option<String>,
    pub recognized_status: Option<bool>,
    pub cumulative_filled_quantity: Option<WholeQuantity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CanonicalOrderEvidence {
    Missing,
    Complete {
        coverage_hash: HashDigest,
        orders: BTreeMap<String, LocalOrderTruth>,
    },
}

/// Partial facts are retained for audit but can never stand in for complete
/// canonical local order truth.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalOrderLedgerFact {
    pub intent_id: Uuid,
    pub client_order_id: String,
    pub provider_order_id: Option<String>,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub whole_quantity: WholeQuantity,
    pub limit_price: Price,
    pub time_in_force: String,
    pub intent_state: Option<String>,
    pub provider_status: Option<String>,
    pub recognized_status: Option<bool>,
    pub cumulative_filled_quantity: Option<WholeQuantity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalProjectionBlocker {
    IndependentCashBasisMissing,
    StableRestFillIdentityMissing,
    CanonicalLocalOrderTruthMissing,
}

/// Durable local state required for account/order/fill reconciliation. Missing
/// cash, fill, or canonical-order identity is explicit evidence, never an
/// implicit zero/empty value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProjection {
    /// Time at which the store completed the transactionally consistent local
    /// read. Terminal evidence may never predate this observation.
    pub observed_at: DateTime<Utc>,
    pub account_fingerprint: HashDigest,
    pub accounting_cash: AccountingCashEvidence,
    pub positions: BTreeMap<Symbol, WholeQuantity>,
    pub canonical_orders: CanonicalOrderEvidence,
    pub order_ledger_facts: Vec<LocalOrderLedgerFact>,
    pub unresolved_order_outboxes: Vec<Uuid>,
    pub unresolved_cancel_outboxes: Vec<Uuid>,
    pub blockers: Vec<LocalProjectionBlocker>,
    pub stable_fill_identities: StableFillIdentityEvidence,
}

/// Canonical order identity, economic contract, lifecycle state, and provider
/// timestamps. Reconciliation must not collapse this to client ID plus status:
/// a wrong-symbol or wrong-quantity open order can still fill while the
/// observer is read-only.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrderTruth {
    pub provider_order_id: String,
    pub client_order_id: String,
    pub symbol: Symbol,
    pub asset_class: String,
    pub side: OrderSide,
    pub quantity: Option<WholeQuantity>,
    pub notional: Option<Money>,
    pub filled_quantity: WholeQuantity,
    pub average_fill_price: Option<Price>,
    pub limit_price: Option<Price>,
    pub order_class: String,
    pub order_type: String,
    pub time_in_force: String,
    pub status: String,
    pub extended_hours: bool,
    pub submitted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Complete broker truth needed by the provider-free startup kernel. The port
/// exposes no submit, replace, or cancel method.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerSnapshot {
    pub account_fingerprint: HashDigest,
    pub account_status: AccountStatus,
    pub trading_blocked: bool,
    pub account_blocked: bool,
    pub transfers_blocked: bool,
    pub trade_suspended_by_user: bool,
    pub usd_currency: bool,
    pub cash: Money,
    pub buying_power: Money,
    pub non_marginable_buying_power: Money,
    pub equity: Money,
    pub last_equity: Money,
    pub portfolio_value: Money,
    pub long_market_value: Money,
    pub short_market_value: Money,
    pub accrued_fees: Money,
    pub pending_transfer_in: Money,
    pub pending_transfer_out: Money,
    pub initial_margin: Money,
    pub maintenance_margin: Money,
    pub last_maintenance_margin: Money,
    pub regt_buying_power: Money,
    pub multiplier: Fixed,
    pub shorting_enabled: bool,
    pub positions: BTreeMap<Symbol, WholeQuantity>,
    pub position_asset_ids: BTreeMap<Symbol, String>,
    pub position_available_quantities: BTreeMap<Symbol, WholeQuantity>,
    pub orders: BTreeMap<String, OrderTruth>,
    pub fill_fingerprints: Vec<HashDigest>,
    /// Ordered hashes of the raw account, position, order, and fill pages used
    /// to construct this normalized snapshot.
    pub source_evidence_hashes: Vec<HashDigest>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePageKind {
    Account,
    Positions,
    OpenOrders,
    ClosedOrders,
    FillActivities,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PageCompletionWitness {
    Single,
    ShortPage,
    TimestampHorizonCrossed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePageEvidence {
    pub snapshot_round: u8,
    pub kind: SourcePageKind,
    pub page_ordinal: u32,
    pub request_parameters_hash: HashDigest,
    pub request_id: Option<String>,
    pub raw_payload_hash: HashDigest,
    pub received_at: DateTime<Utc>,
    pub item_count: u32,
    pub completion_witness: Option<PageCompletionWitness>,
    pub evidence_hash: HashDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedBrokerSnapshot {
    pub snapshot: BrokerSnapshot,
    pub pages: Vec<SourcePageEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupReason {
    ReadOnlyPolicy,
    IndependentCashBasisMissing,
    StableRestFillIdentityMissing,
    CanonicalLocalOrderTruthMissing,
    LocalAccountFingerprintMismatch,
    BrokerAccountFingerprintMismatch,
    BrokerSnapshotUnstable,
    BrokerAccountNotActive,
    BrokerTradingBlocked,
    BrokerAccountBlocked,
    BrokerTransfersBlocked,
    BrokerTradeSuspendedByUser,
    BrokerAccountNotUsd,
    BrokerAccruedFeesNonzero,
    BrokerPendingTransfers,
    BrokerPositionIdentityIncomplete,
    ReconciliationDifferences,
    LocalProjectionUnavailable,
    BrokerSnapshotUnavailable,
    BrokerSnapshotTimedOut,
    SourcePageEvidenceUnavailable,
    EvidenceHashUnavailable,
    UnresolvedOrderOutbox,
    UnresolvedCancelOutbox,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupOutcome {
    Blocked,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupFailureStage {
    Configuration,
    LocalProjection,
    BrokerRound1,
    BrokerRound2,
    FenceRenewal,
    DatabaseConnection,
    Persistence,
    Shutdown,
}

/// Durable reason a read-only observer cycle was started.
///
/// This is part of the cycle identity. A periodic supervisor must never write
/// every cycle as `startup`, and a reconnect audit must remain distinguishable
/// from both process startup and ordinary scheduling.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverCycleTrigger {
    Startup,
    Periodic,
    Reconnect,
}

impl ObserverCycleTrigger {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Periodic => "periodic",
            Self::Reconnect => "reconnect",
        }
    }
}

/// Deterministic durable identity for the open observer cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverCycleKey {
    pub cycle_id: Uuid,
    pub evidence_hash: HashDigest,
}

/// Deterministic durable identity for the terminal observer result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverPersistenceKey {
    pub cycle_id: Uuid,
    pub evidence_hash: HashDigest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverPersistenceResolution {
    Committed,
    NotCommitted,
    ConflictingEvidence,
}

/// A cycle is committed before any broker read. Its type contains no release,
/// permit, order, or execution-enabled state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverCycle {
    cycle_id: Uuid,
    environment: Environment,
    mode: CoordinatorMode,
    resumable: bool,
    account_fingerprint: HashDigest,
    owner_id: Uuid,
    fencing_token: u64,
    trigger: ObserverCycleTrigger,
    started_at: DateTime<Utc>,
    evidence_hash: HashDigest,
}

impl ObserverCycle {
    fn new(
        config: &PaperReadOnlyConfig,
        lease: &PaperObserverLease,
        trigger: ObserverCycleTrigger,
        started_at: DateTime<Utc>,
    ) -> Result<Self, CoordinatorError> {
        let identity = format!(
            "paper-observer-cycle/v1:{}:{}:{}:{}:{}",
            config.expected_account_fingerprint,
            config.owner_id,
            lease.fencing_token,
            trigger.as_str(),
            started_at.to_rfc3339()
        );
        let cycle_id = Uuid::new_v5(&OBSERVER_CYCLE_NAMESPACE, identity.as_bytes());
        let material = ObserverCycleHashMaterial {
            cycle_id,
            environment: Environment::Paper,
            mode: CoordinatorMode::ReconcileOnly,
            resumable: false,
            account_fingerprint: config.expected_account_fingerprint,
            owner_id: config.owner_id,
            fencing_token: lease.fencing_token,
            trigger,
            started_at,
        };
        let evidence_hash = HashDigest::of_json(&material)
            .map_err(|_| CoordinatorError::EvidenceHashUnavailable)?;
        Ok(Self {
            cycle_id,
            environment: Environment::Paper,
            mode: CoordinatorMode::ReconcileOnly,
            resumable: false,
            account_fingerprint: config.expected_account_fingerprint,
            owner_id: config.owner_id,
            fencing_token: lease.fencing_token,
            trigger,
            started_at,
            evidence_hash,
        })
    }

    pub const fn key(&self) -> ObserverCycleKey {
        ObserverCycleKey {
            cycle_id: self.cycle_id,
            evidence_hash: self.evidence_hash,
        }
    }

    pub const fn environment(&self) -> Environment {
        self.environment
    }

    pub const fn mode(&self) -> CoordinatorMode {
        self.mode
    }

    pub const fn resumable(&self) -> bool {
        self.resumable
    }

    pub const fn cycle_id(&self) -> Uuid {
        self.cycle_id
    }

    pub const fn account_fingerprint(&self) -> HashDigest {
        self.account_fingerprint
    }

    pub const fn owner_id(&self) -> Uuid {
        self.owner_id
    }

    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    pub const fn trigger(&self) -> ObserverCycleTrigger {
        self.trigger
    }

    pub const fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub const fn evidence_hash(&self) -> HashDigest {
        self.evidence_hash
    }
}

#[derive(Serialize)]
struct ObserverCycleHashMaterial {
    cycle_id: Uuid,
    environment: Environment,
    mode: CoordinatorMode,
    resumable: bool,
    account_fingerprint: HashDigest,
    owner_id: Uuid,
    fencing_token: u64,
    trigger: ObserverCycleTrigger,
    started_at: DateTime<Utc>,
}

/// Persisted startup evidence. Its private fields and single-variant mode keep
/// it non-authorizing even when reconciliation evidence is otherwise equal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartupResult {
    cycle_id: Uuid,
    generated_at: DateTime<Utc>,
    environment: Environment,
    mode: CoordinatorMode,
    resumable: bool,
    outcome: StartupOutcome,
    failure_stage: Option<StartupFailureStage>,
    broker_snapshot_stable: bool,
    reasons: Vec<StartupReason>,
    local_evidence_hash: Option<HashDigest>,
    broker_evidence_hashes: Vec<HashDigest>,
    normalized_broker_snapshots: Vec<BrokerSnapshot>,
    source_page_evidence: Vec<SourcePageEvidence>,
    reconciliation: Option<ReconciliationReport>,
}

impl StartupResult {
    pub const fn cycle_id(&self) -> Uuid {
        self.cycle_id
    }

    pub const fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }

    pub const fn environment(&self) -> Environment {
        self.environment
    }

    pub const fn mode(&self) -> CoordinatorMode {
        self.mode
    }

    pub const fn resumable(&self) -> bool {
        self.resumable
    }

    pub const fn outcome(&self) -> StartupOutcome {
        self.outcome
    }

    pub const fn failure_stage(&self) -> Option<StartupFailureStage> {
        self.failure_stage
    }

    pub const fn broker_snapshot_stable(&self) -> bool {
        self.broker_snapshot_stable
    }

    pub fn reasons(&self) -> &[StartupReason] {
        &self.reasons
    }

    pub const fn reconciliation(&self) -> Option<&ReconciliationReport> {
        self.reconciliation.as_ref()
    }

    pub const fn local_evidence_hash(&self) -> Option<HashDigest> {
        self.local_evidence_hash
    }

    pub fn broker_evidence_hashes(&self) -> &[HashDigest] {
        &self.broker_evidence_hashes
    }

    pub fn normalized_broker_snapshots(&self) -> &[BrokerSnapshot] {
        &self.normalized_broker_snapshots
    }

    pub fn source_page_evidence(&self) -> &[SourcePageEvidence] {
        &self.source_page_evidence
    }

    /// Returns true only when a persisted paper observation is semantically
    /// clean. `Blocked` alone is never sufficient: every read-only cycle is
    /// blocked by policy, including cycles that found broker/local drift.
    pub fn is_clean_read_only_observation(&self) -> bool {
        let has_both_snapshot_rounds = [1_u8, 2_u8].into_iter().all(|round| {
            self.source_page_evidence
                .iter()
                .any(|page| page.snapshot_round == round)
        });
        let reconciliation_is_clean = self.reconciliation.as_ref().is_some_and(|report| {
            report.validate().is_ok()
                && report.differences.is_empty()
                && !report.may_resume_execution
        });

        self.environment == Environment::Paper
            && self.mode == CoordinatorMode::ReconcileOnly
            && !self.resumable
            && self.outcome == StartupOutcome::Blocked
            && self.failure_stage.is_none()
            && self.broker_snapshot_stable
            && self.reasons == [StartupReason::ReadOnlyPolicy]
            && self.local_evidence_hash.is_some()
            && self.broker_evidence_hashes.len() == 2
            && self.normalized_broker_snapshots.len() == 2
            && has_both_snapshot_rounds
            && reconciliation_is_clean
    }

    pub fn persistence_key(&self) -> Result<ObserverPersistenceKey, CoordinatorError> {
        let evidence_hash =
            HashDigest::of_json(self).map_err(|_| CoordinatorError::EvidenceHashUnavailable)?;
        Ok(ObserverPersistenceKey {
            cycle_id: self.cycle_id,
            evidence_hash,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum CoordinatorPortError {
    #[error("read-only Alpaca broker configuration rejected")]
    BrokerConfigurationRejected,
    #[error("read-only Alpaca broker snapshot unavailable")]
    BrokerSnapshotUnavailable,
    #[error("read-only Alpaca broker source evidence unavailable")]
    BrokerSourceEvidenceUnavailable,
    #[error("paper observer store unavailable")]
    StoreUnavailable,
    #[error("paper observer local projection unavailable")]
    ProjectionUnavailable,
    #[error("paper observer cycle-start commit outcome is unknown")]
    CycleStartOutcomeUnknown,
    #[error("paper observer completion commit outcome is unknown")]
    CycleCompletionOutcomeUnknown,
    #[error("paper observer persistence evidence conflicts")]
    ConflictingEvidence,
}

impl CoordinatorPortError {
    /// Compatibility shim for existing provider adapters. The supplied text is
    /// never retained or formatted; only reviewed fixed messages are mapped.
    pub fn new(message: impl AsRef<str>) -> Self {
        match message.as_ref() {
            "read-only Alpaca broker configuration rejected" => Self::BrokerConfigurationRejected,
            "read-only Alpaca broker snapshot unavailable" => Self::BrokerSnapshotUnavailable,
            _ => Self::StoreUnavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum CoordinatorError {
    #[error("unsafe paper coordinator configuration: {0}")]
    UnsafeConfiguration(String),
    #[error("execution fence is unavailable")]
    FenceUnavailable,
    #[error("execution fence was invalid or lost")]
    FenceLost,
    #[error("paper observer store operation {operation} failed")]
    Store { operation: &'static str },
    #[error("read-only broker snapshot unavailable")]
    BrokerUnavailable,
    #[error("startup evidence could not be hashed")]
    EvidenceHashUnavailable,
    #[error("broker source-page evidence is incomplete")]
    SourcePageEvidenceUnavailable,
    #[error("paper observer persistence outcome could not be resolved")]
    PersistenceUnresolved,
    #[error("paper observer persistence evidence conflicts")]
    PersistenceConflict,
}

#[async_trait]
pub trait CoordinatorStore: Send {
    async fn acquire_observer_lease(
        &mut self,
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<PaperObserverLease>, CoordinatorPortError>;

    async fn renew_observer_lease(
        &mut self,
        lease: &PaperObserverLease,
        ttl: Duration,
    ) -> Result<Option<PaperObserverLease>, CoordinatorPortError>;

    async fn begin_cycle(
        &mut self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<(), CoordinatorPortError>;

    async fn resolve_cycle_start(
        &mut self,
        key: &ObserverCycleKey,
    ) -> Result<ObserverPersistenceResolution, CoordinatorPortError>;

    async fn load_local_projection(
        &mut self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<LocalProjection, CoordinatorPortError>;

    async fn persist_cycle_result(
        &mut self,
        cycle: &ObserverCycle,
        result: &StartupResult,
        key: &ObserverPersistenceKey,
        lease: &PaperObserverLease,
    ) -> Result<(), CoordinatorPortError>;

    async fn resolve_cycle_completion(
        &mut self,
        key: &ObserverPersistenceKey,
    ) -> Result<ObserverPersistenceResolution, CoordinatorPortError>;
}

#[async_trait]
pub trait ReadOnlyBroker: Send {
    async fn read_snapshot(&mut self) -> Result<BrokerSnapshot, CoordinatorPortError>;

    /// Structured page evidence is a separate method so the existing direct
    /// adapter remains source compatible while failing closed until it exposes
    /// the exact pagination/request contract required for durable observation.
    async fn read_snapshot_with_evidence(
        &mut self,
        _snapshot_round: u8,
    ) -> Result<ObservedBrokerSnapshot, CoordinatorPortError> {
        Err(CoordinatorPortError::BrokerSourceEvidenceUnavailable)
    }
}

/// Runs one fail-closed startup reconciliation. Two broker reads are required
/// to prove stable evidence. A clean result is still read-only and never
/// resumable; a separate future runtime slice must not reinterpret it as an
/// activation decision.
pub async fn run_startup_reconciliation<S, B, C>(
    config: &PaperReadOnlyConfig,
    store: &mut S,
    broker: &mut B,
    started_at: DateTime<Utc>,
    clock: &C,
) -> Result<StartupResult, CoordinatorError>
where
    S: CoordinatorStore,
    B: ReadOnlyBroker,
    C: CoordinatorClock,
{
    run_observer_reconciliation(
        config,
        store,
        broker,
        ObserverCycleTrigger::Startup,
        started_at,
        clock,
    )
    .await
}

/// Runs one fail-closed observer reconciliation with an explicit durable
/// trigger. The startup wrapper above remains for callers that perform only a
/// single startup check; long-running supervisors must use this function so
/// periodic and reconnect evidence cannot masquerade as startup evidence.
pub async fn run_observer_reconciliation<S, B, C>(
    config: &PaperReadOnlyConfig,
    store: &mut S,
    broker: &mut B,
    trigger: ObserverCycleTrigger,
    started_at: DateTime<Utc>,
    clock: &C,
) -> Result<StartupResult, CoordinatorError>
where
    S: CoordinatorStore,
    B: ReadOnlyBroker,
    C: CoordinatorClock,
{
    config.validate()?;
    let snapshot_timeout = config.snapshot_timeout()?;

    let initial_lease = store
        .acquire_observer_lease(
            config.expected_account_fingerprint,
            config.owner_id,
            config.fence_ttl,
        )
        .await
        .map_err(|error| store_error("acquire_observer_lease", error))?
        .ok_or(CoordinatorError::FenceUnavailable)?;
    if !valid_lease(&initial_lease, config, started_at) {
        return Err(CoordinatorError::FenceLost);
    }

    let cycle = ObserverCycle::new(config, &initial_lease, trigger, started_at)?;
    begin_cycle(store, &cycle, &initial_lease).await?;

    let local = match store.load_local_projection(&cycle, &initial_lease).await {
        Ok(local) => local,
        Err(_) => {
            return finish_failed_cycle(
                config,
                store,
                &cycle,
                &initial_lease,
                latest_evidence_time(&cycle, None, &[], clock),
                StartupFailureStage::LocalProjection,
                StartupReason::LocalProjectionUnavailable,
                None,
            )
            .await;
        }
    };
    let local_observed_at = latest_evidence_time(&cycle, Some(&local), &[], clock);
    let local_evidence_hash = match HashDigest::of_json(&local) {
        Ok(hash) => hash,
        Err(_) => {
            return finish_failed_cycle(
                config,
                store,
                &cycle,
                &initial_lease,
                local_observed_at,
                StartupFailureStage::LocalProjection,
                StartupReason::EvidenceHashUnavailable,
                None,
            )
            .await;
        }
    };

    let first_observed =
        match tokio::time::timeout(snapshot_timeout, broker.read_snapshot_with_evidence(1)).await {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(_)) => {
                return finish_failed_cycle(
                    config,
                    store,
                    &cycle,
                    &initial_lease,
                    latest_evidence_time(&cycle, Some(&local), &[], clock),
                    StartupFailureStage::BrokerRound1,
                    StartupReason::BrokerSnapshotUnavailable,
                    Some(local_evidence_hash),
                )
                .await;
            }
            Err(_) => {
                return finish_failed_cycle(
                    config,
                    store,
                    &cycle,
                    &initial_lease,
                    latest_evidence_time(&cycle, Some(&local), &[], clock),
                    StartupFailureStage::BrokerRound1,
                    StartupReason::BrokerSnapshotTimedOut,
                    Some(local_evidence_hash),
                )
                .await;
            }
        };
    let mut first = first_observed.snapshot;
    first.fill_fingerprints.sort_unstable();
    let first_hash = match HashDigest::of_json(&first) {
        Ok(hash) => hash,
        Err(_) => {
            return finish_failed_cycle(
                config,
                store,
                &cycle,
                &initial_lease,
                latest_evidence_time(&cycle, Some(&local), &[], clock),
                StartupFailureStage::BrokerRound1,
                StartupReason::EvidenceHashUnavailable,
                Some(local_evidence_hash),
            )
            .await;
        }
    };
    let first_pages = match validate_source_pages(first_observed.pages, 1, cycle.started_at()) {
        Ok(evidence) => evidence,
        Err(_) => {
            return finish_failed_cycle(
                config,
                store,
                &cycle,
                &initial_lease,
                latest_evidence_time(&cycle, Some(&local), &[], clock),
                StartupFailureStage::BrokerRound1,
                StartupReason::SourcePageEvidenceUnavailable,
                Some(local_evidence_hash),
            )
            .await;
        }
    };
    if !snapshot_is_bound_to_pages(&first, &first_pages) {
        return finish_failed_cycle(
            config,
            store,
            &cycle,
            &initial_lease,
            latest_evidence_time(&cycle, Some(&local), &first_pages, clock),
            StartupFailureStage::BrokerRound1,
            StartupReason::SourcePageEvidenceUnavailable,
            Some(local_evidence_hash),
        )
        .await;
    }
    let first_completed_at = latest_evidence_time(&cycle, Some(&local), &first_pages, clock);
    let first_renewed = renew_checked(config, store, &initial_lease, first_completed_at).await?;

    let second_observed =
        match tokio::time::timeout(snapshot_timeout, broker.read_snapshot_with_evidence(2)).await {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(_)) => {
                return finish_failed_cycle(
                    config,
                    store,
                    &cycle,
                    &first_renewed,
                    latest_evidence_time(&cycle, Some(&local), &first_pages, clock),
                    StartupFailureStage::BrokerRound2,
                    StartupReason::BrokerSnapshotUnavailable,
                    Some(local_evidence_hash),
                )
                .await;
            }
            Err(_) => {
                return finish_failed_cycle(
                    config,
                    store,
                    &cycle,
                    &first_renewed,
                    latest_evidence_time(&cycle, Some(&local), &first_pages, clock),
                    StartupFailureStage::BrokerRound2,
                    StartupReason::BrokerSnapshotTimedOut,
                    Some(local_evidence_hash),
                )
                .await;
            }
        };
    let mut second = second_observed.snapshot;
    second.fill_fingerprints.sort_unstable();
    let second_hash = match HashDigest::of_json(&second) {
        Ok(hash) => hash,
        Err(_) => {
            return finish_failed_cycle(
                config,
                store,
                &cycle,
                &first_renewed,
                latest_evidence_time(&cycle, Some(&local), &first_pages, clock),
                StartupFailureStage::BrokerRound2,
                StartupReason::EvidenceHashUnavailable,
                Some(local_evidence_hash),
            )
            .await;
        }
    };
    let second_pages = match validate_source_pages(second_observed.pages, 2, cycle.started_at()) {
        Ok(evidence) => evidence,
        Err(_) => {
            return finish_failed_cycle(
                config,
                store,
                &cycle,
                &first_renewed,
                latest_evidence_time(&cycle, Some(&local), &first_pages, clock),
                StartupFailureStage::BrokerRound2,
                StartupReason::SourcePageEvidenceUnavailable,
                Some(local_evidence_hash),
            )
            .await;
        }
    };
    if !snapshot_is_bound_to_pages(&second, &second_pages) {
        let completed_at = latest_evidence_time(&cycle, Some(&local), &second_pages, clock);
        return finish_failed_cycle(
            config,
            store,
            &cycle,
            &first_renewed,
            completed_at,
            StartupFailureStage::BrokerRound2,
            StartupReason::SourcePageEvidenceUnavailable,
            Some(local_evidence_hash),
        )
        .await;
    }

    // Raw account/position pages contain changing mark-to-market fields. Keep
    // their hashes as audit evidence, but require stability only for the
    // normalized reconciliation truth that can change cash, positions, orders,
    // fills, restrictions, or account identity.
    let stable = same_reconciliation_truth(&first, &second);
    let mut source_pages = first_pages;
    source_pages.extend(second_pages);
    let completed_at = latest_evidence_time(&cycle, Some(&local), &source_pages, clock);
    let final_lease = renew_checked(config, store, &first_renewed, completed_at).await?;

    let result = build_result(
        config,
        &cycle,
        completed_at,
        final_lease.fencing_token,
        local,
        first,
        second,
        stable,
        local_evidence_hash,
        vec![first_hash, second_hash],
        source_pages,
    );
    persist_result(store, &cycle, &result, &final_lease).await?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn build_result(
    config: &PaperReadOnlyConfig,
    cycle: &ObserverCycle,
    now: DateTime<Utc>,
    fencing_token: u64,
    local: LocalProjection,
    first: BrokerSnapshot,
    second: BrokerSnapshot,
    stable: bool,
    local_evidence_hash: HashDigest,
    broker_evidence_hashes: Vec<HashDigest>,
    source_page_evidence: Vec<SourcePageEvidence>,
) -> StartupResult {
    let broker_order_statuses: BTreeMap<String, String> = second
        .orders
        .iter()
        .map(|(client_id, order)| (client_id.clone(), order.status.clone()))
        .collect();
    let (local_orders, local_order_statuses, missing_order_identity) = match &local.canonical_orders
    {
        CanonicalOrderEvidence::Missing => (None, broker_order_statuses.clone(), true),
        CanonicalOrderEvidence::Complete { orders, .. } => (
            Some(orders),
            orders
                .iter()
                .filter_map(|(client_id, order)| {
                    order
                        .provider_status
                        .as_ref()
                        .map(|status| (client_id.clone(), status.clone()))
                })
                .collect(),
            false,
        ),
    };
    let (local_cash, missing_cash) = match local.accounting_cash.cash() {
        Some(cash) => (cash, false),
        // The generic reconciler requires a value. Use the broker value only to
        // suppress a fabricated numerical comparison, then append an explicit
        // missing-evidence difference below.
        None => (second.cash, true),
    };
    let expected_fill_schema = HashDigest::sha256(FILL_IDENTITY_SCHEMA);
    let (local_fill_fingerprints, missing_fill_identity) = match &local.stable_fill_identities {
        StableFillIdentityEvidence::Complete {
            identity_schema_hash,
            ..
        } if *identity_schema_hash == expected_fill_schema => (
            local
                .stable_fill_identities
                .sorted_fingerprints()
                .expect("complete fill evidence returns fingerprints"),
            false,
        ),
        StableFillIdentityEvidence::Missing | StableFillIdentityEvidence::Complete { .. } => {
            (second.fill_fingerprints.clone(), true)
        }
    };
    let mut report = reconcile(ReconciliationInput {
        generated_at: now,
        account_fingerprint: config.expected_account_fingerprint,
        execution_fencing_token: fencing_token,
        local_cash,
        broker_cash: second.cash,
        local_positions: local.positions.clone(),
        broker_positions: second.positions.clone(),
        local_order_statuses,
        broker_order_statuses,
        local_fill_fingerprints,
        broker_fill_fingerprints: second.fill_fingerprints.clone(),
    });
    let mut reasons = vec![StartupReason::ReadOnlyPolicy];

    if missing_cash {
        reasons.push(StartupReason::IndependentCashBasisMissing);
        push_difference_values(
            &mut report,
            ReconciliationDifferenceKind::MissingLocally,
            "independent_cash_basis",
            None,
            Some(second.cash.to_string()),
            "independent durable accounting cash evidence is missing",
        );
    }
    if missing_fill_identity {
        reasons.push(StartupReason::StableRestFillIdentityMissing);
        push_difference_values(
            &mut report,
            ReconciliationDifferenceKind::MissingLocally,
            "stable_rest_fill_identity",
            None,
            Some(second.fill_fingerprints.len().to_string()),
            "stable REST fill-activity identity evidence is missing or incompatible",
        );
    }
    if missing_order_identity {
        reasons.push(StartupReason::CanonicalLocalOrderTruthMissing);
        push_difference_values(
            &mut report,
            ReconciliationDifferenceKind::MissingLocally,
            "canonical_local_order_truth",
            None,
            Some(second.orders.len().to_string()),
            "canonical durable local order truth is missing",
        );
    }
    if local.account_fingerprint != config.expected_account_fingerprint {
        reasons.push(StartupReason::LocalAccountFingerprintMismatch);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "local_account",
            "local projection account fingerprint does not match configuration",
        );
    }
    if second.account_fingerprint != config.expected_account_fingerprint {
        reasons.push(StartupReason::BrokerAccountFingerprintMismatch);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_account",
            "broker account fingerprint does not match configuration",
        );
    }
    if !stable {
        reasons.push(StartupReason::BrokerSnapshotUnstable);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_snapshot",
            "broker truth changed between consecutive read-only startup rounds",
        );
    }
    if second.account_status != AccountStatus::Active {
        reasons.push(StartupReason::BrokerAccountNotActive);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_account_status",
            "broker account is not active",
        );
    }
    if second.trading_blocked {
        reasons.push(StartupReason::BrokerTradingBlocked);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_trading_block",
            "broker reports trading is blocked",
        );
    }
    if second.account_blocked {
        reasons.push(StartupReason::BrokerAccountBlocked);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_account_block",
            "broker reports the account is blocked",
        );
    }
    if second.transfers_blocked {
        reasons.push(StartupReason::BrokerTransfersBlocked);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_transfer_block",
            "broker reports transfers are blocked",
        );
    }
    if second.trade_suspended_by_user {
        reasons.push(StartupReason::BrokerTradeSuspendedByUser);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_user_trade_suspension",
            "broker reports trading is suspended by the user",
        );
    }
    if !second.usd_currency {
        reasons.push(StartupReason::BrokerAccountNotUsd);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_account_currency",
            "broker account currency is not USD",
        );
    }
    if second.accrued_fees != Money::ZERO {
        reasons.push(StartupReason::BrokerAccruedFeesNonzero);
        push_difference_values(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_accrued_fees",
            None,
            Some(second.accrued_fees.to_string()),
            "broker reports nonzero accrued fees without independent accounting evidence",
        );
    }
    if second.pending_transfer_in != Money::ZERO || second.pending_transfer_out != Money::ZERO {
        reasons.push(StartupReason::BrokerPendingTransfers);
        push_difference_values(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_pending_transfers",
            None,
            Some(format!(
                "in={} out={}",
                second.pending_transfer_in, second.pending_transfer_out
            )),
            "broker reports a pending transfer that is not independently accounted",
        );
    }
    if second.positions.keys().ne(second.position_asset_ids.keys())
        || second
            .positions
            .keys()
            .ne(second.position_available_quantities.keys())
        || second
            .position_asset_ids
            .values()
            .any(|asset_id| asset_id.trim().is_empty())
        || second.positions.iter().any(|(symbol, held)| {
            second
                .position_available_quantities
                .get(symbol)
                .is_none_or(|available| available.get() > held.get())
        })
    {
        reasons.push(StartupReason::BrokerPositionIdentityIncomplete);
        push_difference_values(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            "broker_position_asset_identity",
            None,
            Some(format!(
                "positions={} asset_ids={}",
                second.positions.len(),
                second.position_asset_ids.len()
            )),
            "broker position asset identity is incomplete or inconsistent",
        );
    }
    if let Some(local_orders) = local_orders {
        compare_order_contracts(&mut report, local_orders, &second.orders);
    }
    for outbox_id in &local.unresolved_order_outboxes {
        reasons.push(StartupReason::UnresolvedOrderOutbox);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            &format!("order_outbox:{outbox_id}"),
            "durable order outbox remains unresolved",
        );
    }
    for outbox_id in &local.unresolved_cancel_outboxes {
        reasons.push(StartupReason::UnresolvedCancelOutbox);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::StatusMismatch,
            &format!("cancel_outbox:{outbox_id}"),
            "durable cancellation outbox remains unresolved",
        );
    }
    reasons.sort_by_key(|reason| format!("{reason:?}"));
    reasons.dedup();
    if !report.differences.is_empty() {
        reasons.push(StartupReason::ReconciliationDifferences);
    }

    // The generic reconciler reports whether equal evidence could resume. This
    // coordinator is structurally read-only, so it always clears that bit.
    report.may_resume_execution = false;
    StartupResult {
        cycle_id: cycle.cycle_id(),
        generated_at: now,
        environment: Environment::Paper,
        mode: CoordinatorMode::ReconcileOnly,
        resumable: false,
        outcome: StartupOutcome::Blocked,
        failure_stage: None,
        broker_snapshot_stable: stable,
        reasons,
        local_evidence_hash: Some(local_evidence_hash),
        broker_evidence_hashes,
        normalized_broker_snapshots: vec![first, second],
        source_page_evidence,
        reconciliation: Some(report),
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_failed_cycle<S: CoordinatorStore>(
    config: &PaperReadOnlyConfig,
    store: &mut S,
    cycle: &ObserverCycle,
    lease: &PaperObserverLease,
    now: DateTime<Utc>,
    failure_stage: StartupFailureStage,
    reason: StartupReason,
    local_evidence_hash: Option<HashDigest>,
) -> Result<StartupResult, CoordinatorError> {
    let renewed = store
        .renew_observer_lease(lease, config.fence_ttl)
        .await
        .map_err(|error| store_error("renew_observer_lease", error))?
        .ok_or(CoordinatorError::FenceLost)?;
    if !same_fence(lease, &renewed) || !valid_lease(&renewed, config, now) {
        return Err(CoordinatorError::FenceLost);
    }
    let result = StartupResult {
        cycle_id: cycle.cycle_id(),
        generated_at: now,
        environment: Environment::Paper,
        mode: CoordinatorMode::ReconcileOnly,
        resumable: false,
        outcome: StartupOutcome::Failed,
        failure_stage: Some(failure_stage),
        broker_snapshot_stable: false,
        reasons: vec![StartupReason::ReadOnlyPolicy, reason],
        local_evidence_hash,
        broker_evidence_hashes: Vec::new(),
        normalized_broker_snapshots: Vec::new(),
        source_page_evidence: Vec::new(),
        reconciliation: None,
    };
    persist_result(store, cycle, &result, &renewed).await?;
    Ok(result)
}

async fn begin_cycle<S: CoordinatorStore>(
    store: &mut S,
    cycle: &ObserverCycle,
    lease: &PaperObserverLease,
) -> Result<(), CoordinatorError> {
    match store.begin_cycle(cycle, lease).await {
        Ok(()) => Ok(()),
        Err(CoordinatorPortError::CycleStartOutcomeUnknown) => {
            match store
                .resolve_cycle_start(&cycle.key())
                .await
                .map_err(|error| store_error("resolve_cycle_start", error))?
            {
                ObserverPersistenceResolution::Committed => Ok(()),
                ObserverPersistenceResolution::NotCommitted => {
                    Err(CoordinatorError::PersistenceUnresolved)
                }
                ObserverPersistenceResolution::ConflictingEvidence => {
                    Err(CoordinatorError::PersistenceConflict)
                }
            }
        }
        Err(CoordinatorPortError::ConflictingEvidence) => {
            Err(CoordinatorError::PersistenceConflict)
        }
        Err(error) => Err(store_error("begin_cycle", error)),
    }
}

async fn persist_result<S: CoordinatorStore>(
    store: &mut S,
    cycle: &ObserverCycle,
    result: &StartupResult,
    lease: &PaperObserverLease,
) -> Result<(), CoordinatorError> {
    let key = result.persistence_key()?;
    match store.persist_cycle_result(cycle, result, &key, lease).await {
        Ok(()) => Ok(()),
        Err(CoordinatorPortError::CycleCompletionOutcomeUnknown) => {
            match store
                .resolve_cycle_completion(&key)
                .await
                .map_err(|error| store_error("resolve_cycle_completion", error))?
            {
                ObserverPersistenceResolution::Committed => Ok(()),
                ObserverPersistenceResolution::NotCommitted => {
                    Err(CoordinatorError::PersistenceUnresolved)
                }
                ObserverPersistenceResolution::ConflictingEvidence => {
                    Err(CoordinatorError::PersistenceConflict)
                }
            }
        }
        Err(CoordinatorPortError::ConflictingEvidence) => {
            Err(CoordinatorError::PersistenceConflict)
        }
        Err(error) => Err(store_error("persist_cycle_result", error)),
    }
}

fn validate_source_pages(
    mut pages: Vec<SourcePageEvidence>,
    expected_round: u8,
    cycle_started_at: DateTime<Utc>,
) -> Result<Vec<SourcePageEvidence>, CoordinatorError> {
    if !matches!(expected_round, 1 | 2) || pages.is_empty() {
        return Err(CoordinatorError::SourcePageEvidenceUnavailable);
    }
    pages.sort_by_key(|page| (page.kind, page.page_ordinal));
    let required_sources = [
        SourcePageKind::Account,
        SourcePageKind::Positions,
        SourcePageKind::OpenOrders,
        SourcePageKind::ClosedOrders,
        SourcePageKind::FillActivities,
    ];
    for source in required_sources {
        let source_pages = pages
            .iter()
            .filter(|page| page.kind == source)
            .collect::<Vec<_>>();
        if source_pages.is_empty()
            || source_pages
                .iter()
                .enumerate()
                .any(|(index, page)| page.page_ordinal != u32::try_from(index).unwrap_or(u32::MAX))
        {
            return Err(CoordinatorError::SourcePageEvidenceUnavailable);
        }
        for (index, page) in source_pages.iter().enumerate() {
            if page.snapshot_round != expected_round
                || page.received_at < cycle_started_at
                || page
                    .request_id
                    .as_ref()
                    .is_some_and(|id| id.trim().is_empty() || id.len() > 128)
                || HashDigest::of_json(&SourcePageHashMaterial::from(*page))
                    .map_err(|_| CoordinatorError::EvidenceHashUnavailable)?
                    != page.evidence_hash
            {
                return Err(CoordinatorError::SourcePageEvidenceUnavailable);
            }
            let is_last = index + 1 == source_pages.len();
            let witness_valid = match source {
                SourcePageKind::Account | SourcePageKind::Positions => {
                    source_pages.len() == 1
                        && page.page_ordinal == 0
                        && page.completion_witness == Some(PageCompletionWitness::Single)
                }
                SourcePageKind::OpenOrders | SourcePageKind::FillActivities => {
                    if is_last {
                        page.completion_witness == Some(PageCompletionWitness::ShortPage)
                    } else {
                        page.completion_witness.is_none()
                    }
                }
                SourcePageKind::ClosedOrders => {
                    if is_last {
                        matches!(
                            page.completion_witness,
                            Some(
                                PageCompletionWitness::ShortPage
                                    | PageCompletionWitness::TimestampHorizonCrossed
                            )
                        )
                    } else {
                        page.completion_witness.is_none()
                    }
                }
            };
            if !witness_valid {
                return Err(CoordinatorError::SourcePageEvidenceUnavailable);
            }
        }
    }
    Ok(pages)
}

#[derive(Serialize)]
struct SourcePageHashMaterial<'a> {
    schema: &'static str,
    snapshot_round: u8,
    kind: SourcePageKind,
    page_ordinal: u32,
    request_parameters_hash: HashDigest,
    request_id: &'a Option<String>,
    raw_payload_hash: HashDigest,
    received_at: DateTime<Utc>,
    item_count: u32,
    completion_witness: Option<PageCompletionWitness>,
}

impl<'a> From<&'a SourcePageEvidence> for SourcePageHashMaterial<'a> {
    fn from(page: &'a SourcePageEvidence) -> Self {
        Self {
            schema: "wasp2/alpaca-source-page-evidence/v1",
            snapshot_round: page.snapshot_round,
            kind: page.kind,
            page_ordinal: page.page_ordinal,
            request_parameters_hash: page.request_parameters_hash,
            request_id: &page.request_id,
            raw_payload_hash: page.raw_payload_hash,
            received_at: page.received_at,
            item_count: page.item_count,
            completion_witness: page.completion_witness,
        }
    }
}

fn snapshot_is_bound_to_pages(snapshot: &BrokerSnapshot, pages: &[SourcePageEvidence]) -> bool {
    snapshot.source_evidence_hashes.len() == pages.len()
        && snapshot
            .source_evidence_hashes
            .iter()
            .zip(pages)
            .all(|(snapshot_hash, page)| *snapshot_hash == page.raw_payload_hash)
}

fn latest_evidence_time(
    cycle: &ObserverCycle,
    local: Option<&LocalProjection>,
    pages: &[SourcePageEvidence],
    clock: &impl CoordinatorClock,
) -> DateTime<Utc> {
    local
        .into_iter()
        .map(|projection| projection.observed_at)
        .chain(pages.iter().map(|page| page.received_at))
        .chain(std::iter::once(clock.now()))
        .fold(cycle.started_at(), std::cmp::max)
}

async fn renew_checked<S: CoordinatorStore>(
    config: &PaperReadOnlyConfig,
    store: &mut S,
    previous: &PaperObserverLease,
    evidence_time: DateTime<Utc>,
) -> Result<PaperObserverLease, CoordinatorError> {
    let renewed = store
        .renew_observer_lease(previous, config.fence_ttl)
        .await
        .map_err(|error| store_error("renew_observer_lease", error))?
        .ok_or(CoordinatorError::FenceLost)?;
    if !same_fence(previous, &renewed) || !valid_lease(&renewed, config, evidence_time) {
        return Err(CoordinatorError::FenceLost);
    }
    Ok(renewed)
}

fn compare_order_contracts(
    report: &mut ReconciliationReport,
    local: &BTreeMap<String, LocalOrderTruth>,
    broker: &BTreeMap<String, OrderTruth>,
) {
    let client_ids: BTreeSet<_> = local.keys().chain(broker.keys()).collect();
    for client_id in client_ids {
        if local
            .get(client_id)
            .is_some_and(|order| order.client_order_id != *client_id)
            || broker
                .get(client_id)
                .is_some_and(|order| order.client_order_id != *client_id)
        {
            push_difference(
                report,
                ReconciliationDifferenceKind::StatusMismatch,
                &format!("order_identity:{client_id}"),
                "order map key does not match its canonical client identity",
            );
            continue;
        }
        match (local.get(client_id), broker.get(client_id)) {
            (Some(local_order), Some(broker_order))
                if local_order.provider_order_id.as_deref()
                    != Some(broker_order.provider_order_id.as_str())
                    || local_order.symbol != broker_order.symbol
                    || local_order.side != broker_order.side
                    || Some(local_order.whole_quantity) != broker_order.quantity
                    || Some(local_order.limit_price) != broker_order.limit_price
                    || local_order.time_in_force != broker_order.time_in_force
                    || local_order.provider_status.as_deref()
                        != Some(broker_order.status.as_str())
                    || local_order.recognized_status != Some(true)
                    || local_order.cumulative_filled_quantity
                        != Some(broker_order.filled_quantity) =>
            {
                push_difference(
                    report,
                    ReconciliationDifferenceKind::StatusMismatch,
                    &format!("order_contract:{client_id}"),
                    "local and broker order identity or economic contract differs",
                );
            }
            _ => {}
        }
    }
}

fn same_reconciliation_truth(first: &BrokerSnapshot, second: &BrokerSnapshot) -> bool {
    first.account_fingerprint == second.account_fingerprint
        && first.account_status == second.account_status
        && first.trading_blocked == second.trading_blocked
        && first.account_blocked == second.account_blocked
        && first.transfers_blocked == second.transfers_blocked
        && first.trade_suspended_by_user == second.trade_suspended_by_user
        && first.usd_currency == second.usd_currency
        && first.cash == second.cash
        && first.buying_power == second.buying_power
        && first.non_marginable_buying_power == second.non_marginable_buying_power
        && first.equity == second.equity
        && first.last_equity == second.last_equity
        && first.portfolio_value == second.portfolio_value
        && first.long_market_value == second.long_market_value
        && first.short_market_value == second.short_market_value
        && first.accrued_fees == second.accrued_fees
        && first.pending_transfer_in == second.pending_transfer_in
        && first.pending_transfer_out == second.pending_transfer_out
        && first.initial_margin == second.initial_margin
        && first.maintenance_margin == second.maintenance_margin
        && first.last_maintenance_margin == second.last_maintenance_margin
        && first.regt_buying_power == second.regt_buying_power
        && first.multiplier == second.multiplier
        && first.shorting_enabled == second.shorting_enabled
        && first.positions == second.positions
        && first.position_asset_ids == second.position_asset_ids
        && first.position_available_quantities == second.position_available_quantities
        && first.orders == second.orders
        && first.fill_fingerprints == second.fill_fingerprints
}

fn push_difference(
    report: &mut ReconciliationReport,
    kind: ReconciliationDifferenceKind,
    subject: &str,
    detail: &str,
) {
    push_difference_values(report, kind, subject, None, None, detail);
}

fn push_difference_values(
    report: &mut ReconciliationReport,
    kind: ReconciliationDifferenceKind,
    subject: &str,
    local_value: Option<String>,
    broker_value: Option<String>,
    detail: &str,
) {
    report.differences.push(ReconciliationDifference {
        kind,
        subject: subject.into(),
        local_value,
        broker_value,
        detail: detail.into(),
    });
}

fn valid_lease(
    lease: &PaperObserverLease,
    config: &PaperReadOnlyConfig,
    now: DateTime<Utc>,
) -> bool {
    lease.environment == Environment::Paper
        && lease.account_fingerprint == config.expected_account_fingerprint
        && lease.owner_id == config.owner_id
        && lease.fencing_token > 0
        && lease.lease_until > now
}

fn same_fence(previous: &PaperObserverLease, renewed: &PaperObserverLease) -> bool {
    previous.environment == renewed.environment
        && previous.account_fingerprint == renewed.account_fingerprint
        && previous.owner_id == renewed.owner_id
        && previous.fencing_token == renewed.fencing_token
        && renewed.lease_until > previous.lease_until
}

fn store_error(operation: &'static str, _error: CoordinatorPortError) -> CoordinatorError {
    CoordinatorError::Store { operation }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-07-19T14:00:00Z".parse().unwrap()
    }

    struct TestClock;

    impl CoordinatorClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            now() + Duration::seconds(3)
        }
    }

    fn config() -> PaperReadOnlyConfig {
        PaperReadOnlyConfig {
            expected_account_fingerprint: HashDigest::sha256("paper-account"),
            owner_id: Uuid::from_u128(7),
            fence_ttl: Duration::seconds(30),
        }
    }

    fn lease(config: &PaperReadOnlyConfig, seconds: i64) -> PaperObserverLease {
        PaperObserverLease {
            environment: Environment::Paper,
            account_fingerprint: config.expected_account_fingerprint,
            owner_id: config.owner_id,
            fencing_token: 11,
            lease_until: now() + Duration::seconds(seconds),
        }
    }

    fn local(config: &PaperReadOnlyConfig) -> LocalProjection {
        LocalProjection {
            observed_at: now(),
            account_fingerprint: config.expected_account_fingerprint,
            accounting_cash: AccountingCashEvidence::Complete {
                cash: Money::from_units(1_000).unwrap(),
                basis_hash: HashDigest::sha256("cash-basis"),
                coverage_hash: HashDigest::sha256("cash-coverage"),
            },
            positions: BTreeMap::new(),
            canonical_orders: CanonicalOrderEvidence::Complete {
                coverage_hash: HashDigest::sha256("order-coverage"),
                orders: BTreeMap::new(),
            },
            order_ledger_facts: Vec::new(),
            unresolved_order_outboxes: Vec::new(),
            unresolved_cancel_outboxes: Vec::new(),
            blockers: Vec::new(),
            stable_fill_identities: StableFillIdentityEvidence::Complete {
                identity_schema_hash: HashDigest::sha256(FILL_IDENTITY_SCHEMA),
                coverage_hash: HashDigest::sha256("fill-coverage"),
                fingerprints: Vec::new(),
            },
        }
    }

    fn broker(config: &PaperReadOnlyConfig) -> BrokerSnapshot {
        BrokerSnapshot {
            account_fingerprint: config.expected_account_fingerprint,
            account_status: AccountStatus::Active,
            trading_blocked: false,
            account_blocked: false,
            transfers_blocked: false,
            trade_suspended_by_user: false,
            usd_currency: true,
            cash: Money::from_units(1_000).unwrap(),
            buying_power: Money::from_units(1_000).unwrap(),
            non_marginable_buying_power: Money::from_units(1_000).unwrap(),
            equity: Money::from_units(1_000).unwrap(),
            last_equity: Money::from_units(1_000).unwrap(),
            portfolio_value: Money::from_units(1_000).unwrap(),
            long_market_value: Money::ZERO,
            short_market_value: Money::ZERO,
            accrued_fees: Money::ZERO,
            pending_transfer_in: Money::ZERO,
            pending_transfer_out: Money::ZERO,
            initial_margin: Money::ZERO,
            maintenance_margin: Money::ZERO,
            last_maintenance_margin: Money::ZERO,
            regt_buying_power: Money::from_units(1_000).unwrap(),
            multiplier: Fixed::ONE,
            shorting_enabled: false,
            positions: BTreeMap::new(),
            position_asset_ids: BTreeMap::new(),
            position_available_quantities: BTreeMap::new(),
            orders: BTreeMap::new(),
            fill_fingerprints: Vec::new(),
            source_evidence_hashes: Vec::new(),
        }
    }

    fn order(client_order_id: &str, symbol: &str, quantity: u64) -> OrderTruth {
        OrderTruth {
            provider_order_id: format!("provider-{client_order_id}"),
            client_order_id: client_order_id.into(),
            symbol: Symbol::new(symbol).unwrap(),
            asset_class: "us_equity".into(),
            side: OrderSide::Buy,
            quantity: Some(WholeQuantity::new(quantity)),
            notional: None,
            filled_quantity: WholeQuantity::ZERO,
            average_fill_price: None,
            limit_price: Some("500.00".parse().unwrap()),
            order_class: "simple".into(),
            order_type: "limit".into(),
            time_in_force: "day".into(),
            status: "accepted".into(),
            extended_hours: false,
            submitted_at: "2026-07-19T13:30:00Z".parse().unwrap(),
            updated_at: "2026-07-19T13:30:01Z".parse().unwrap(),
        }
    }

    struct FakeStore {
        acquired: Result<Option<PaperObserverLease>, CoordinatorPortError>,
        renewed: Result<Option<PaperObserverLease>, CoordinatorPortError>,
        begin: Result<(), CoordinatorPortError>,
        start_resolution: Result<ObserverPersistenceResolution, CoordinatorPortError>,
        local: Result<LocalProjection, CoordinatorPortError>,
        completion: Result<(), CoordinatorPortError>,
        completion_resolution: Result<ObserverPersistenceResolution, CoordinatorPortError>,
        begun: Vec<ObserverCycleKey>,
        persisted: Vec<StartupResult>,
    }

    #[async_trait]
    impl CoordinatorStore for FakeStore {
        async fn acquire_observer_lease(
            &mut self,
            _account_fingerprint: HashDigest,
            _owner_id: Uuid,
            _ttl: Duration,
        ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
            self.acquired.clone()
        }

        async fn renew_observer_lease(
            &mut self,
            lease: &PaperObserverLease,
            ttl: Duration,
        ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
            let mut renewed = self.renewed.clone()?;
            if let Some(renewed) = &mut renewed {
                if renewed.lease_until <= lease.lease_until {
                    renewed.lease_until = lease.lease_until + ttl;
                }
            }
            Ok(renewed)
        }

        async fn begin_cycle(
            &mut self,
            cycle: &ObserverCycle,
            _lease: &PaperObserverLease,
        ) -> Result<(), CoordinatorPortError> {
            self.begun.push(cycle.key());
            self.begin
        }

        async fn resolve_cycle_start(
            &mut self,
            _key: &ObserverCycleKey,
        ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
            self.start_resolution
        }

        async fn load_local_projection(
            &mut self,
            _cycle: &ObserverCycle,
            _lease: &PaperObserverLease,
        ) -> Result<LocalProjection, CoordinatorPortError> {
            self.local.clone()
        }

        async fn persist_cycle_result(
            &mut self,
            _cycle: &ObserverCycle,
            result: &StartupResult,
            _key: &ObserverPersistenceKey,
            _lease: &PaperObserverLease,
        ) -> Result<(), CoordinatorPortError> {
            self.persisted.push(result.clone());
            self.completion
        }

        async fn resolve_cycle_completion(
            &mut self,
            _key: &ObserverPersistenceKey,
        ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
            self.completion_resolution
        }
    }

    struct FakeBroker {
        snapshots: VecDeque<Result<BrokerSnapshot, CoordinatorPortError>>,
    }

    #[async_trait]
    impl ReadOnlyBroker for FakeBroker {
        async fn read_snapshot(&mut self) -> Result<BrokerSnapshot, CoordinatorPortError> {
            self.snapshots
                .pop_front()
                .expect("test supplied both broker rounds")
        }

        async fn read_snapshot_with_evidence(
            &mut self,
            snapshot_round: u8,
        ) -> Result<ObservedBrokerSnapshot, CoordinatorPortError> {
            let pages = source_pages(snapshot_round);
            let mut snapshot = self.read_snapshot().await?;
            snapshot.source_evidence_hashes =
                pages.iter().map(|page| page.raw_payload_hash).collect();
            Ok(ObservedBrokerSnapshot { snapshot, pages })
        }
    }

    fn source_page(
        snapshot_round: u8,
        kind: SourcePageKind,
        completion_witness: PageCompletionWitness,
    ) -> SourcePageEvidence {
        let mut page = SourcePageEvidence {
            snapshot_round,
            kind,
            page_ordinal: 0,
            request_parameters_hash: HashDigest::sha256(format!("request-{kind:?}")),
            request_id: Some(format!("request-{snapshot_round}-{kind:?}")),
            raw_payload_hash: HashDigest::sha256(format!("payload-{snapshot_round}-{kind:?}")),
            received_at: now() + Duration::seconds(i64::from(snapshot_round)),
            item_count: 0,
            completion_witness: Some(completion_witness),
            evidence_hash: HashDigest::sha256("pending"),
        };
        page.evidence_hash = HashDigest::of_json(&SourcePageHashMaterial::from(&page)).unwrap();
        page
    }

    fn source_pages(snapshot_round: u8) -> Vec<SourcePageEvidence> {
        vec![
            source_page(
                snapshot_round,
                SourcePageKind::Account,
                PageCompletionWitness::Single,
            ),
            source_page(
                snapshot_round,
                SourcePageKind::Positions,
                PageCompletionWitness::Single,
            ),
            source_page(
                snapshot_round,
                SourcePageKind::OpenOrders,
                PageCompletionWitness::ShortPage,
            ),
            source_page(
                snapshot_round,
                SourcePageKind::ClosedOrders,
                PageCompletionWitness::ShortPage,
            ),
            source_page(
                snapshot_round,
                SourcePageKind::FillActivities,
                PageCompletionWitness::ShortPage,
            ),
        ]
    }

    fn store(config: &PaperReadOnlyConfig, local: LocalProjection) -> FakeStore {
        FakeStore {
            acquired: Ok(Some(lease(config, 30))),
            renewed: Ok(Some(lease(config, 60))),
            begin: Ok(()),
            start_resolution: Ok(ObserverPersistenceResolution::Committed),
            local: Ok(local),
            completion: Ok(()),
            completion_resolution: Ok(ObserverPersistenceResolution::Committed),
            begun: Vec::new(),
            persisted: Vec::new(),
        }
    }

    fn broker_port(snapshots: impl IntoIterator<Item = BrokerSnapshot>) -> FakeBroker {
        FakeBroker {
            snapshots: snapshots.into_iter().map(Ok).collect(),
        }
    }

    #[tokio::test]
    async fn stable_equal_evidence_is_still_nonresumable() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result =
            run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                .await
                .unwrap();

        assert_eq!(result.environment(), Environment::Paper);
        assert_eq!(result.mode(), CoordinatorMode::ReconcileOnly);
        assert!(!result.resumable());
        assert!(result.broker_snapshot_stable());
        assert_eq!(result.generated_at(), now() + Duration::seconds(3));
        assert!(result
            .source_page_evidence()
            .iter()
            .all(|page| page.received_at <= result.generated_at()));
        let reconciliation = result.reconciliation().unwrap();
        assert!(reconciliation.differences.is_empty());
        assert!(!reconciliation.may_resume_execution);
        assert_eq!(result.reasons(), &[StartupReason::ReadOnlyPolicy]);
        assert!(result.is_clean_read_only_observation());
        assert_eq!(store.persisted, vec![result]);
    }

    #[tokio::test]
    async fn clean_observation_predicate_fails_closed_for_every_evidence_class() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        let mut broker = broker_port([snapshot.clone(), snapshot]);
        let clean = run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
            .await
            .unwrap();
        assert!(clean.is_clean_read_only_observation());

        let assert_dirty = |dirty: StartupResult| {
            assert!(
                !dirty.is_clean_read_only_observation(),
                "mutated evidence must never be classified clean"
            );
        };

        let mut dirty = clean.clone();
        dirty.environment = Environment::Shadow;
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.resumable = true;
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.outcome = StartupOutcome::Failed;
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.failure_stage = Some(StartupFailureStage::Persistence);
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.broker_snapshot_stable = false;
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.reasons.clear();
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.local_evidence_hash = None;
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.broker_evidence_hashes.pop();
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.normalized_broker_snapshots.pop();
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty
            .source_page_evidence
            .retain(|page| page.snapshot_round == 1);
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.reconciliation = None;
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty
            .reconciliation
            .as_mut()
            .unwrap()
            .differences
            .push(ReconciliationDifference {
                kind: ReconciliationDifferenceKind::StatusMismatch,
                subject: "test-drift".into(),
                local_value: Some("local".into()),
                broker_value: Some("broker".into()),
                detail: "test-only reconciliation drift".into(),
            });
        assert_dirty(dirty);
        let mut dirty = clean.clone();
        dirty.reconciliation.as_mut().unwrap().may_resume_execution = true;
        assert_dirty(dirty);

        for reason in [
            StartupReason::IndependentCashBasisMissing,
            StartupReason::StableRestFillIdentityMissing,
            StartupReason::CanonicalLocalOrderTruthMissing,
            StartupReason::LocalAccountFingerprintMismatch,
            StartupReason::BrokerAccountFingerprintMismatch,
            StartupReason::BrokerSnapshotUnstable,
            StartupReason::BrokerAccountNotActive,
            StartupReason::BrokerTradingBlocked,
            StartupReason::BrokerAccountBlocked,
            StartupReason::BrokerTransfersBlocked,
            StartupReason::BrokerTradeSuspendedByUser,
            StartupReason::BrokerAccountNotUsd,
            StartupReason::BrokerAccruedFeesNonzero,
            StartupReason::BrokerPendingTransfers,
            StartupReason::BrokerPositionIdentityIncomplete,
            StartupReason::ReconciliationDifferences,
            StartupReason::LocalProjectionUnavailable,
            StartupReason::BrokerSnapshotUnavailable,
            StartupReason::BrokerSnapshotTimedOut,
            StartupReason::SourcePageEvidenceUnavailable,
            StartupReason::EvidenceHashUnavailable,
            StartupReason::UnresolvedOrderOutbox,
            StartupReason::UnresolvedCancelOutbox,
        ] {
            let mut dirty = clean.clone();
            dirty.reasons.push(reason);
            assert_dirty(dirty);
        }
    }

    #[tokio::test]
    async fn missing_independent_cash_basis_is_persisted_as_blocked() {
        let config = config();
        let mut local = local(&config);
        local.accounting_cash = AccountingCashEvidence::Missing;
        let snapshot = broker(&config);
        let mut store = store(&config, local);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result =
            run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                .await
                .unwrap();

        assert!(!result.resumable());
        assert!(result
            .reasons()
            .contains(&StartupReason::IndependentCashBasisMissing));
        assert!(!result.is_clean_read_only_observation());
        assert!(result
            .reconciliation()
            .unwrap()
            .differences
            .iter()
            .any(|difference| difference.subject == "independent_cash_basis"));
        assert_eq!(store.persisted.len(), 1);
    }

    #[tokio::test]
    async fn broker_account_fingerprint_mismatch_is_blocked() {
        let config = config();
        let mut snapshot = broker(&config);
        snapshot.account_fingerprint = HashDigest::sha256("different-account");
        let mut store = store(&config, local(&config));
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result =
            run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                .await
                .unwrap();

        assert!(result
            .reasons()
            .contains(&StartupReason::BrokerAccountFingerprintMismatch));
        assert!(!result.resumable());
        assert!(!result.is_clean_read_only_observation());
    }

    #[tokio::test]
    async fn lost_fence_prevents_result_persistence() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        store.renewed = Ok(None);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
            .await
            .unwrap_err();

        assert_eq!(error, CoordinatorError::FenceLost);
        assert!(store.persisted.is_empty());
    }

    #[tokio::test]
    async fn mismatched_initial_fence_never_reaches_broker_or_persistence() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        let mut wrong_lease = lease(&config, 30);
        wrong_lease.account_fingerprint = HashDigest::sha256("wrong-account");
        store.acquired = Ok(Some(wrong_lease));
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
            .await
            .unwrap_err();

        assert_eq!(error, CoordinatorError::FenceLost);
        assert_eq!(broker.snapshots.len(), 2);
        assert!(store.persisted.is_empty());
    }

    #[tokio::test]
    async fn changed_renewal_fence_never_reaches_persistence() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        let mut changed_lease = lease(&config, 60);
        changed_lease.fencing_token += 1;
        store.renewed = Ok(Some(changed_lease));
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
            .await
            .unwrap_err();

        assert_eq!(error, CoordinatorError::FenceLost);
        assert!(store.persisted.is_empty());
    }

    #[tokio::test]
    async fn renewal_failure_prevents_result_persistence() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        store.renewed = Err(CoordinatorPortError::new("database unavailable"));
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CoordinatorError::Store {
                operation: "renew_observer_lease",
                ..
            }
        ));
        assert!(store.persisted.is_empty());
    }

    #[tokio::test]
    async fn broker_drift_is_persisted_as_unstable_and_blocked() {
        let config = config();
        let first = broker(&config);
        let mut second = first.clone();
        second.cash = Money::from_units(999).unwrap();
        let mut store = store(&config, local(&config));
        let mut broker = broker_port([first, second]);

        let result =
            run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                .await
                .unwrap();

        assert!(!result.broker_snapshot_stable());
        assert!(result
            .reasons()
            .contains(&StartupReason::BrokerSnapshotUnstable));
        assert!(result
            .reconciliation()
            .unwrap()
            .differences
            .iter()
            .any(|difference| difference.subject == "broker_snapshot"));
        assert!(!result.resumable());
    }

    #[tokio::test]
    async fn changing_raw_marks_are_preserved_without_false_instability() {
        let config = config();
        let mut first = broker(&config);
        first
            .source_evidence_hashes
            .push(HashDigest::sha256("account-page-v1"));
        let mut second = first.clone();
        second.source_evidence_hashes[0] = HashDigest::sha256("account-page-v2");
        let mut store = store(&config, local(&config));
        let mut broker = broker_port([first, second]);

        let result =
            run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                .await
                .unwrap();

        assert!(result.broker_snapshot_stable());
        assert!(!result
            .reasons()
            .contains(&StartupReason::BrokerSnapshotUnstable));
        assert_ne!(
            result.broker_evidence_hashes()[0],
            result.broker_evidence_hashes()[1]
        );
    }

    #[tokio::test]
    async fn same_status_wrong_order_contract_is_blocked() {
        let config = config();
        let mut local = local(&config);
        match &mut local.canonical_orders {
            CanonicalOrderEvidence::Complete { orders, .. } => {
                let broker_order = order("client-1", "SPY", 1);
                orders.insert(
                    "client-1".into(),
                    LocalOrderTruth {
                        client_order_id: broker_order.client_order_id,
                        provider_order_id: Some(broker_order.provider_order_id),
                        symbol: broker_order.symbol,
                        side: broker_order.side,
                        whole_quantity: broker_order.quantity.unwrap(),
                        limit_price: broker_order.limit_price.unwrap(),
                        time_in_force: broker_order.time_in_force,
                        provider_status: Some(broker_order.status),
                        recognized_status: Some(true),
                        cumulative_filled_quantity: Some(broker_order.filled_quantity),
                    },
                );
            }
            CanonicalOrderEvidence::Missing => panic!("test fixture has canonical order truth"),
        }
        let mut snapshot = broker(&config);
        snapshot
            .orders
            .insert("client-1".into(), order("client-1", "QQQ", 100));
        let mut store = store(&config, local);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result =
            run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                .await
                .unwrap();

        assert!(result.broker_snapshot_stable());
        assert!(result
            .reconciliation()
            .unwrap()
            .differences
            .iter()
            .any(|difference| {
                difference.subject == "order_contract:client-1"
                    && difference.kind == ReconciliationDifferenceKind::StatusMismatch
            }));
        assert!(!result.resumable());
    }

    #[tokio::test]
    async fn every_account_restriction_and_local_identity_mismatch_blocks_startup() {
        #[derive(Clone, Copy)]
        enum Restriction {
            LocalFingerprint,
            AccountStatus,
            TradingBlocked,
            AccountBlocked,
            TransfersBlocked,
            TradeSuspendedByUser,
            NonUsd,
            AccruedFees,
            PendingTransfers,
            PositionAssetIdentity,
            PositionAvailability,
        }

        let cases = [
            (
                Restriction::LocalFingerprint,
                StartupReason::LocalAccountFingerprintMismatch,
            ),
            (
                Restriction::AccountStatus,
                StartupReason::BrokerAccountNotActive,
            ),
            (
                Restriction::TradingBlocked,
                StartupReason::BrokerTradingBlocked,
            ),
            (
                Restriction::AccountBlocked,
                StartupReason::BrokerAccountBlocked,
            ),
            (
                Restriction::TransfersBlocked,
                StartupReason::BrokerTransfersBlocked,
            ),
            (
                Restriction::TradeSuspendedByUser,
                StartupReason::BrokerTradeSuspendedByUser,
            ),
            (Restriction::NonUsd, StartupReason::BrokerAccountNotUsd),
            (
                Restriction::AccruedFees,
                StartupReason::BrokerAccruedFeesNonzero,
            ),
            (
                Restriction::PendingTransfers,
                StartupReason::BrokerPendingTransfers,
            ),
            (
                Restriction::PositionAssetIdentity,
                StartupReason::BrokerPositionIdentityIncomplete,
            ),
            (
                Restriction::PositionAvailability,
                StartupReason::BrokerPositionIdentityIncomplete,
            ),
        ];

        for (restriction, expected_reason) in cases {
            let config = config();
            let mut local = local(&config);
            let mut snapshot = broker(&config);
            match restriction {
                Restriction::LocalFingerprint => {
                    local.account_fingerprint = HashDigest::sha256("wrong-local-account");
                }
                Restriction::AccountStatus => snapshot.account_status = AccountStatus::Restricted,
                Restriction::TradingBlocked => snapshot.trading_blocked = true,
                Restriction::AccountBlocked => snapshot.account_blocked = true,
                Restriction::TransfersBlocked => snapshot.transfers_blocked = true,
                Restriction::TradeSuspendedByUser => {
                    snapshot.trade_suspended_by_user = true;
                }
                Restriction::NonUsd => snapshot.usd_currency = false,
                Restriction::AccruedFees => {
                    snapshot.accrued_fees = "0.01".parse().unwrap();
                }
                Restriction::PendingTransfers => {
                    snapshot.pending_transfer_in = Money::from_units(1).unwrap();
                }
                Restriction::PositionAssetIdentity => {
                    snapshot
                        .positions
                        .insert(Symbol::new("SPY").unwrap(), WholeQuantity::new(1));
                }
                Restriction::PositionAvailability => {
                    let symbol = Symbol::new("SPY").unwrap();
                    snapshot
                        .positions
                        .insert(symbol.clone(), WholeQuantity::new(1));
                    snapshot
                        .position_asset_ids
                        .insert(symbol.clone(), "asset-spy".into());
                    snapshot
                        .position_available_quantities
                        .insert(symbol, WholeQuantity::new(2));
                }
            }
            let mut store = store(&config, local);
            let mut broker = broker_port([snapshot.clone(), snapshot]);

            let result =
                run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
                    .await
                    .unwrap();

            assert!(result.reasons().contains(&expected_reason));
            assert!(!result.resumable());
            let reconciliation = result.reconciliation().unwrap();
            assert!(!reconciliation.may_resume_execution);
            assert!(!reconciliation.differences.is_empty());
            assert_eq!(store.persisted.len(), 1);
        }
    }

    #[test]
    fn fence_ttl_must_be_positive_and_at_most_sixty_seconds() {
        let mut config = config();
        config.fence_ttl = Duration::zero();
        assert!(matches!(
            config.validate(),
            Err(CoordinatorError::UnsafeConfiguration(_))
        ));

        config.fence_ttl = Duration::seconds(61);
        assert!(matches!(
            config.validate(),
            Err(CoordinatorError::UnsafeConfiguration(_))
        ));

        config.fence_ttl = Duration::seconds(60);
        assert_eq!(config.validate(), Ok(()));
    }

    #[tokio::test]
    async fn unavailable_initial_fence_fails_closed() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        store.acquired = Ok(None);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now(), &TestClock)
            .await
            .unwrap_err();

        assert_eq!(error, CoordinatorError::FenceUnavailable);
        assert!(store.persisted.is_empty());
    }
}
