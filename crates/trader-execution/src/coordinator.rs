//! Provider-free startup coordination for the paper, read-only runtime.
//!
//! This kernel deliberately has no execution-enabled state and no broker
//! mutation capability. It acquires a fence, compares two complete read-only
//! broker snapshots, reconciles them with the durable local projection, renews
//! the fence, and persists a result that can never authorize execution.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use thiserror::Error;
use trader_core::{
    AccountStatus, Environment, HashDigest, Money, OrderSide, Price, ReconciliationDifference,
    ReconciliationDifferenceKind, ReconciliationReport, Symbol, WholeQuantity,
};
use uuid::Uuid;

use crate::{
    reconciliation::{reconcile, ReconciliationInput},
    store::FencedLease,
};

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

    fn validate(&self) -> Result<(), CoordinatorError> {
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
        Ok(())
    }
}

/// The only runtime mode this coordinator can represent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorMode {
    ReconcileOnly,
}

/// Durable local state required for an account/order/fill reconciliation.
/// `accounting_cash_baseline` is optional only so a missing baseline can be
/// represented and persisted as a blocked startup result instead of guessed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProjection {
    pub account_fingerprint: HashDigest,
    pub accounting_cash_baseline: Option<Money>,
    pub positions: BTreeMap<Symbol, WholeQuantity>,
    pub orders: BTreeMap<String, OrderTruth>,
    pub fill_fingerprints: Vec<HashDigest>,
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
    pub positions: BTreeMap<Symbol, WholeQuantity>,
    pub orders: BTreeMap<String, OrderTruth>,
    pub fill_fingerprints: Vec<HashDigest>,
    /// Ordered hashes of the raw account, position, order, and fill pages used
    /// to construct this normalized snapshot.
    pub source_evidence_hashes: Vec<HashDigest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupReason {
    ReadOnlyPolicy,
    MissingAccountingBaseline,
    LocalAccountFingerprintMismatch,
    BrokerAccountFingerprintMismatch,
    BrokerSnapshotUnstable,
    BrokerAccountNotActive,
    BrokerTradingBlocked,
    BrokerAccountBlocked,
    BrokerTransfersBlocked,
    BrokerTradeSuspendedByUser,
    BrokerAccountNotUsd,
    ReconciliationDifferences,
}

/// Persisted startup evidence. Its private fields and single-variant mode keep
/// it non-authorizing even when reconciliation evidence is otherwise equal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartupResult {
    generated_at: DateTime<Utc>,
    environment: Environment,
    mode: CoordinatorMode,
    resumable: bool,
    broker_snapshot_stable: bool,
    reasons: Vec<StartupReason>,
    local_evidence_hash: HashDigest,
    broker_evidence_hashes: [HashDigest; 2],
    reconciliation: ReconciliationReport,
}

impl StartupResult {
    pub const fn environment(&self) -> Environment {
        self.environment
    }

    pub const fn mode(&self) -> CoordinatorMode {
        self.mode
    }

    pub const fn resumable(&self) -> bool {
        self.resumable
    }

    pub const fn broker_snapshot_stable(&self) -> bool {
        self.broker_snapshot_stable
    }

    pub fn reasons(&self) -> &[StartupReason] {
        &self.reasons
    }

    pub const fn reconciliation(&self) -> &ReconciliationReport {
        &self.reconciliation
    }

    pub const fn local_evidence_hash(&self) -> HashDigest {
        self.local_evidence_hash
    }

    pub const fn broker_evidence_hashes(&self) -> [HashDigest; 2] {
        self.broker_evidence_hashes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("{message}")]
pub struct CoordinatorPortError {
    message: String,
}

impl CoordinatorPortError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
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
    #[error("coordinator store operation {operation} failed: {detail}")]
    Store {
        operation: &'static str,
        detail: String,
    },
    #[error("read-only broker snapshot failed: {0}")]
    Broker(String),
    #[error("startup evidence could not be hashed: {0}")]
    Evidence(String),
}

#[async_trait]
pub trait CoordinatorStore: Send {
    async fn acquire_fence(
        &mut self,
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<FencedLease>, CoordinatorPortError>;

    async fn renew_fence(
        &mut self,
        lease: &FencedLease,
        ttl: Duration,
    ) -> Result<Option<FencedLease>, CoordinatorPortError>;

    async fn load_local_projection(
        &mut self,
        lease: &FencedLease,
    ) -> Result<LocalProjection, CoordinatorPortError>;

    async fn persist_startup_result(
        &mut self,
        result: &StartupResult,
        lease: &FencedLease,
    ) -> Result<(), CoordinatorPortError>;
}

#[async_trait]
pub trait ReadOnlyBroker: Send {
    async fn read_snapshot(&mut self) -> Result<BrokerSnapshot, CoordinatorPortError>;
}

/// Runs one fail-closed startup reconciliation. Two broker reads are required
/// to prove stable evidence. A clean result is still read-only and never
/// resumable; a separate future runtime slice must not reinterpret it as an
/// activation decision.
pub async fn run_startup_reconciliation<S, B>(
    config: &PaperReadOnlyConfig,
    store: &mut S,
    broker: &mut B,
    now: DateTime<Utc>,
) -> Result<StartupResult, CoordinatorError>
where
    S: CoordinatorStore,
    B: ReadOnlyBroker,
{
    config.validate()?;

    let lease = store
        .acquire_fence(
            config.expected_account_fingerprint,
            config.owner_id,
            config.fence_ttl,
        )
        .await
        .map_err(|error| store_error("acquire_fence", error))?
        .ok_or(CoordinatorError::FenceUnavailable)?;
    if !valid_lease(&lease, config, now) {
        return Err(CoordinatorError::FenceLost);
    }

    let mut local = store
        .load_local_projection(&lease)
        .await
        .map_err(|error| store_error("load_local_projection", error))?;
    local.fill_fingerprints.sort_unstable();

    let mut first = broker
        .read_snapshot()
        .await
        .map_err(|error| CoordinatorError::Broker(error.to_string()))?;
    let mut second = broker
        .read_snapshot()
        .await
        .map_err(|error| CoordinatorError::Broker(error.to_string()))?;
    first.fill_fingerprints.sort_unstable();
    second.fill_fingerprints.sort_unstable();

    // Raw account/position pages contain changing mark-to-market fields. Keep
    // their hashes as audit evidence, but require stability only for the
    // normalized reconciliation truth that can change cash, positions, orders,
    // fills, restrictions, or account identity.
    let stable = same_reconciliation_truth(&first, &second);
    let local_evidence_hash = HashDigest::of_json(&local)
        .map_err(|error| CoordinatorError::Evidence(error.to_string()))?;
    let broker_evidence_hashes = [
        HashDigest::of_json(&first)
            .map_err(|error| CoordinatorError::Evidence(error.to_string()))?,
        HashDigest::of_json(&second)
            .map_err(|error| CoordinatorError::Evidence(error.to_string()))?,
    ];

    let renewed = store
        .renew_fence(&lease, config.fence_ttl)
        .await
        .map_err(|error| store_error("renew_fence", error))?
        .ok_or(CoordinatorError::FenceLost)?;
    if !same_fence(&lease, &renewed) || !valid_lease(&renewed, config, now) {
        return Err(CoordinatorError::FenceLost);
    }

    let result = build_result(
        config,
        now,
        renewed.fencing_token,
        local,
        second,
        stable,
        local_evidence_hash,
        broker_evidence_hashes,
    );
    store
        .persist_startup_result(&result, &renewed)
        .await
        .map_err(|error| store_error("persist_startup_result", error))?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn build_result(
    config: &PaperReadOnlyConfig,
    now: DateTime<Utc>,
    fencing_token: u64,
    local: LocalProjection,
    second: BrokerSnapshot,
    stable: bool,
    local_evidence_hash: HashDigest,
    broker_evidence_hashes: [HashDigest; 2],
) -> StartupResult {
    let local_order_statuses = local
        .orders
        .iter()
        .map(|(client_id, order)| (client_id.clone(), order.status.clone()))
        .collect();
    let broker_order_statuses = second
        .orders
        .iter()
        .map(|(client_id, order)| (client_id.clone(), order.status.clone()))
        .collect();
    let mut report = reconcile(ReconciliationInput {
        generated_at: now,
        account_fingerprint: config.expected_account_fingerprint,
        execution_fencing_token: fencing_token,
        local_cash: local.accounting_cash_baseline.unwrap_or(Money::ZERO),
        broker_cash: second.cash,
        local_positions: local.positions.clone(),
        broker_positions: second.positions.clone(),
        local_order_statuses,
        broker_order_statuses,
        local_fill_fingerprints: local.fill_fingerprints.clone(),
        broker_fill_fingerprints: second.fill_fingerprints.clone(),
    });
    let mut reasons = vec![StartupReason::ReadOnlyPolicy];

    if local.accounting_cash_baseline.is_none() {
        reasons.push(StartupReason::MissingAccountingBaseline);
        push_difference(
            &mut report,
            ReconciliationDifferenceKind::MissingLocally,
            "local_accounting_baseline",
            "durable local cash baseline is missing",
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
    compare_order_contracts(&mut report, &local.orders, &second.orders);
    if !report.differences.is_empty() {
        reasons.push(StartupReason::ReconciliationDifferences);
    }

    // The generic reconciler reports whether equal evidence could resume. This
    // coordinator is structurally read-only, so it always clears that bit.
    report.may_resume_execution = false;
    StartupResult {
        generated_at: now,
        environment: Environment::Paper,
        mode: CoordinatorMode::ReconcileOnly,
        resumable: false,
        broker_snapshot_stable: stable,
        reasons,
        local_evidence_hash,
        broker_evidence_hashes,
        reconciliation: report,
    }
}

fn compare_order_contracts(
    report: &mut ReconciliationReport,
    local: &BTreeMap<String, OrderTruth>,
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
            (Some(local_order), Some(broker_order)) if local_order != broker_order => {
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
        && first.positions == second.positions
        && first.orders == second.orders
        && first.fill_fingerprints == second.fill_fingerprints
}

fn push_difference(
    report: &mut ReconciliationReport,
    kind: ReconciliationDifferenceKind,
    subject: &str,
    detail: &str,
) {
    report.differences.push(ReconciliationDifference {
        kind,
        subject: subject.into(),
        detail: detail.into(),
    });
}

fn valid_lease(lease: &FencedLease, config: &PaperReadOnlyConfig, now: DateTime<Utc>) -> bool {
    lease.environment == Environment::Paper
        && lease.account_fingerprint == config.expected_account_fingerprint
        && lease.owner_id == config.owner_id
        && lease.fencing_token > 0
        && lease.lease_until > now
}

fn same_fence(previous: &FencedLease, renewed: &FencedLease) -> bool {
    previous.environment == renewed.environment
        && previous.account_fingerprint == renewed.account_fingerprint
        && previous.owner_id == renewed.owner_id
        && previous.fencing_token == renewed.fencing_token
        && renewed.lease_until > previous.lease_until
}

fn store_error(operation: &'static str, error: CoordinatorPortError) -> CoordinatorError {
    CoordinatorError::Store {
        operation,
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-07-19T14:00:00Z".parse().unwrap()
    }

    fn config() -> PaperReadOnlyConfig {
        PaperReadOnlyConfig {
            expected_account_fingerprint: HashDigest::sha256("paper-account"),
            owner_id: Uuid::from_u128(7),
            fence_ttl: Duration::seconds(30),
        }
    }

    fn lease(config: &PaperReadOnlyConfig, seconds: i64) -> FencedLease {
        FencedLease {
            environment: Environment::Paper,
            account_fingerprint: config.expected_account_fingerprint,
            owner_id: config.owner_id,
            fencing_token: 11,
            lease_until: now() + Duration::seconds(seconds),
        }
    }

    fn local(config: &PaperReadOnlyConfig) -> LocalProjection {
        LocalProjection {
            account_fingerprint: config.expected_account_fingerprint,
            accounting_cash_baseline: Some(Money::from_units(1_000).unwrap()),
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
            fill_fingerprints: Vec::new(),
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
            positions: BTreeMap::new(),
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
        acquired: Result<Option<FencedLease>, CoordinatorPortError>,
        renewed: Result<Option<FencedLease>, CoordinatorPortError>,
        local: Result<LocalProjection, CoordinatorPortError>,
        persisted: Vec<StartupResult>,
    }

    #[async_trait]
    impl CoordinatorStore for FakeStore {
        async fn acquire_fence(
            &mut self,
            _account_fingerprint: HashDigest,
            _owner_id: Uuid,
            _ttl: Duration,
        ) -> Result<Option<FencedLease>, CoordinatorPortError> {
            self.acquired.clone()
        }

        async fn renew_fence(
            &mut self,
            _lease: &FencedLease,
            _ttl: Duration,
        ) -> Result<Option<FencedLease>, CoordinatorPortError> {
            self.renewed.clone()
        }

        async fn load_local_projection(
            &mut self,
            _lease: &FencedLease,
        ) -> Result<LocalProjection, CoordinatorPortError> {
            self.local.clone()
        }

        async fn persist_startup_result(
            &mut self,
            result: &StartupResult,
            _lease: &FencedLease,
        ) -> Result<(), CoordinatorPortError> {
            self.persisted.push(result.clone());
            Ok(())
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
    }

    fn store(config: &PaperReadOnlyConfig, local: LocalProjection) -> FakeStore {
        FakeStore {
            acquired: Ok(Some(lease(config, 30))),
            renewed: Ok(Some(lease(config, 60))),
            local: Ok(local),
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

        let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap();

        assert_eq!(result.environment(), Environment::Paper);
        assert_eq!(result.mode(), CoordinatorMode::ReconcileOnly);
        assert!(!result.resumable());
        assert!(result.broker_snapshot_stable());
        assert!(result.reconciliation().differences.is_empty());
        assert!(!result.reconciliation().may_resume_execution);
        assert_eq!(result.reasons(), &[StartupReason::ReadOnlyPolicy]);
        assert_eq!(store.persisted, vec![result]);
    }

    #[tokio::test]
    async fn missing_accounting_baseline_is_persisted_as_blocked() {
        let config = config();
        let mut local = local(&config);
        local.accounting_cash_baseline = None;
        let snapshot = broker(&config);
        let mut store = store(&config, local);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap();

        assert!(!result.resumable());
        assert!(result
            .reasons()
            .contains(&StartupReason::MissingAccountingBaseline));
        assert!(result
            .reconciliation()
            .differences
            .iter()
            .any(|difference| difference.subject == "local_accounting_baseline"));
        assert_eq!(store.persisted.len(), 1);
    }

    #[tokio::test]
    async fn broker_account_fingerprint_mismatch_is_blocked() {
        let config = config();
        let mut snapshot = broker(&config);
        snapshot.account_fingerprint = HashDigest::sha256("different-account");
        let mut store = store(&config, local(&config));
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap();

        assert!(result
            .reasons()
            .contains(&StartupReason::BrokerAccountFingerprintMismatch));
        assert!(!result.resumable());
    }

    #[tokio::test]
    async fn lost_fence_prevents_result_persistence() {
        let config = config();
        let snapshot = broker(&config);
        let mut store = store(&config, local(&config));
        store.renewed = Ok(None);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now())
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

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now())
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

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now())
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

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CoordinatorError::Store {
                operation: "renew_fence",
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

        let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap();

        assert!(!result.broker_snapshot_stable());
        assert!(result
            .reasons()
            .contains(&StartupReason::BrokerSnapshotUnstable));
        assert!(result
            .reconciliation()
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

        let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
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
        local
            .orders
            .insert("client-1".into(), order("client-1", "SPY", 1));
        let mut snapshot = broker(&config);
        snapshot
            .orders
            .insert("client-1".into(), order("client-1", "QQQ", 100));
        let mut store = store(&config, local);
        let mut broker = broker_port([snapshot.clone(), snapshot]);

        let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap();

        assert!(result.broker_snapshot_stable());
        assert!(result
            .reconciliation()
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
            }
            let mut store = store(&config, local);
            let mut broker = broker_port([snapshot.clone(), snapshot]);

            let result = run_startup_reconciliation(&config, &mut store, &mut broker, now())
                .await
                .unwrap();

            assert!(result.reasons().contains(&expected_reason));
            assert!(!result.resumable());
            assert!(!result.reconciliation().may_resume_execution);
            assert!(!result.reconciliation().differences.is_empty());
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

        let error = run_startup_reconciliation(&config, &mut store, &mut broker, now())
            .await
            .unwrap_err();

        assert_eq!(error, CoordinatorError::FenceUnavailable);
        assert!(store.persisted.is_empty());
    }
}
