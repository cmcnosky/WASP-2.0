//! Supervised, strictly read-only paper observer runtime.
//!
//! This composition root can construct only the observer PostgreSQL boundary
//! and Alpaca's GET-only transport. It contains no executor, order mutation
//! port, execution store, release, permit, or live endpoint.

use std::{
    env,
    io::{self, Write},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use trader_core::{Environment, HashDigest};
use uuid::Uuid;

use crate::{
    alpaca::AlpacaReadOnlyBroker,
    coordinator::{
        run_observer_reconciliation, CoordinatorClock, CoordinatorMode, CoordinatorPortError,
        CoordinatorStore, LocalProjection, ObserverCycle, ObserverCycleKey, ObserverCycleTrigger,
        ObserverPersistenceKey, ObserverPersistenceResolution, PaperObserverLease,
        PaperReadOnlyConfig, ReadOnlyBroker, StartupOutcome, StartupResult,
    },
    observer_database::{ObserverDatabaseError, PaperObserverDatabaseConfig},
};

const APP_ENVIRONMENT_ENV: &str = "APP_ENVIRONMENT";
const EXECUTION_MODE_ENV: &str = "EXECUTION_MODE";
const EXPECTED_ACCOUNT_FINGERPRINT_ENV: &str = "EXPECTED_ALPACA_ACCOUNT_FINGERPRINT";
const ACCOUNT_FINGERPRINT_SALT_ENV: &str = "ALPACA_ACCOUNT_FINGERPRINT_SALT_HEX";
const EXPECTED_IMAGE_DIGEST_ENV: &str = "EXPECTED_IMAGE_DIGEST";
const METRIC_NAMESPACE_ENV: &str = "METRIC_NAMESPACE";

const FENCE_TTL: Duration = Duration::seconds(60);
const LEASE_RENEW_INTERVAL: StdDuration = StdDuration::from_secs(10);
const CYCLE_INTERVAL: StdDuration = StdDuration::from_secs(60);
const HEARTBEAT_INTERVAL: StdDuration = StdDuration::from_secs(30);
const ACQUIRE_RETRY_BASE: StdDuration = StdDuration::from_secs(1);
const ACQUIRE_RETRY_CAP: StdDuration = StdDuration::from_secs(4);
const ACQUIRE_ATTEMPTS: usize = 4;
const STORE_CHANNEL_CAPACITY: usize = 16;
const MIN_FINGERPRINT_SALT_BYTES: usize = 32;
const MAX_FINGERPRINT_SALT_BYTES: usize = 1024;
const MAX_METRIC_NAMESPACE_BYTES: usize = 255;

/// Fixed, redacted failures for the paper observer process.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObserverRuntimeError {
    #[error("paper observer configuration was rejected")]
    UnsafeConfiguration,
    #[error("paper observer database could not be established")]
    DatabaseUnavailable,
    #[error("paper observer database connection ended")]
    DatabaseConnectionEnded,
    #[error("paper observer broker could not be constructed")]
    BrokerUnavailable,
    #[error("paper observer lease was unavailable")]
    LeaseUnavailable,
    #[error("paper observer lease was lost")]
    LeaseLost,
    #[error("paper observer store actor ended")]
    StoreActorEnded,
    #[error("paper observer reconciliation failed closed")]
    ReconciliationFailed,
    #[error("paper observer wall clock regressed or stopped")]
    ClockInvalid,
    #[error("paper observer health publication failed")]
    HealthPublicationFailed,
    #[error("paper observer shutdown handler could not be installed")]
    ShutdownUnavailable,
}

/// Secret-owning process inputs. Intentionally not `Debug`, `Clone`, or
/// serializable.
struct PaperObserverProcessConfig {
    coordinator: PaperReadOnlyConfig,
    fingerprint_salt: Vec<u8>,
    image_digest: String,
    metric_namespace: String,
}

impl PaperObserverProcessConfig {
    fn from_env() -> Result<Self, ObserverRuntimeError> {
        if required_env(APP_ENVIRONMENT_ENV)? != "paper"
            || required_env(EXECUTION_MODE_ENV)? != "read_only"
        {
            return Err(ObserverRuntimeError::UnsafeConfiguration);
        }
        let expected_account_fingerprint =
            HashDigest::from_str(&required_env(EXPECTED_ACCOUNT_FINGERPRINT_ENV)?)
                .map_err(|_| ObserverRuntimeError::UnsafeConfiguration)?;
        let fingerprint_salt = decode_secret_hex(required_env(ACCOUNT_FINGERPRINT_SALT_ENV)?)?;
        let image_digest = required_env(EXPECTED_IMAGE_DIGEST_ENV)?;
        validate_image_digest(&image_digest)?;
        let metric_namespace = required_env(METRIC_NAMESPACE_ENV)?;
        validate_metric_namespace(&metric_namespace)?;

        let coordinator = PaperReadOnlyConfig {
            expected_account_fingerprint,
            owner_id: Uuid::new_v4(),
            fence_ttl: FENCE_TTL,
        };
        coordinator
            .validate()
            .map_err(|_| ObserverRuntimeError::UnsafeConfiguration)?;
        Ok(Self {
            coordinator,
            fingerprint_salt,
            image_digest,
            metric_namespace,
        })
    }
}

impl Drop for PaperObserverProcessConfig {
    fn drop(&mut self) {
        self.fingerprint_salt.fill(0);
    }
}

fn required_env(name: &'static str) -> Result<String, ObserverRuntimeError> {
    env::var(name).map_err(|_| ObserverRuntimeError::UnsafeConfiguration)
}

fn decode_secret_hex(encoded: String) -> Result<Vec<u8>, ObserverRuntimeError> {
    let mut encoded = encoded.into_bytes();
    if encoded.len() % 2 != 0
        || !(MIN_FINGERPRINT_SALT_BYTES * 2..=MAX_FINGERPRINT_SALT_BYTES * 2)
            .contains(&encoded.len())
        || !encoded
            .iter()
            .copied()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        encoded.fill(0);
        return Err(ObserverRuntimeError::UnsafeConfiguration);
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.chunks_exact(2) {
        let high = decode_hex_nibble(pair[0]);
        let low = decode_hex_nibble(pair[1]);
        decoded.push((high << 4) | low);
    }
    // The decoded bytes move into a zeroizing broker wrapper. Scrub this
    // intermediate environment copy before its allocation is released.
    encoded.fill(0);
    Ok(decoded)
}

fn decode_hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

fn validate_image_digest(value: &str) -> Result<(), ObserverRuntimeError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ObserverRuntimeError::UnsafeConfiguration);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || hex.bytes().all(|byte| byte == b'0')
    {
        return Err(ObserverRuntimeError::UnsafeConfiguration);
    }
    Ok(())
}

fn validate_metric_namespace(value: &str) -> Result<(), ObserverRuntimeError> {
    if value.is_empty()
        || value.len() > MAX_METRIC_NAMESPACE_BYTES
        || !value.ends_with("/paper")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return Err(ObserverRuntimeError::UnsafeConfiguration);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreActorError {
    StoreOperationFailed,
    LeaseLost,
}

enum StoreCommand {
    Acquire {
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
        response: oneshot::Sender<Result<Option<PaperObserverLease>, CoordinatorPortError>>,
    },
    Renew {
        lease: PaperObserverLease,
        ttl: Duration,
        response: oneshot::Sender<Result<Option<PaperObserverLease>, CoordinatorPortError>>,
    },
    BeginCycle {
        cycle: ObserverCycle,
        lease: PaperObserverLease,
        response: oneshot::Sender<Result<(), CoordinatorPortError>>,
    },
    ResolveCycleStart {
        key: ObserverCycleKey,
        response: oneshot::Sender<Result<ObserverPersistenceResolution, CoordinatorPortError>>,
    },
    LoadLocalProjection {
        cycle: ObserverCycle,
        lease: PaperObserverLease,
        response: oneshot::Sender<Result<LocalProjection, CoordinatorPortError>>,
    },
    PersistCycleResult {
        cycle: ObserverCycle,
        result: Box<StartupResult>,
        key: ObserverPersistenceKey,
        lease: PaperObserverLease,
        response: oneshot::Sender<Result<(), CoordinatorPortError>>,
    },
    ResolveCycleCompletion {
        key: ObserverPersistenceKey,
        response: oneshot::Sender<Result<ObserverPersistenceResolution, CoordinatorPortError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

/// Cloneable request proxy for a single serialized observer-store actor.
#[derive(Clone)]
struct ObserverStoreProxy {
    sender: mpsc::Sender<StoreCommand>,
}

#[derive(Clone)]
struct StoreActorControl {
    sender: mpsc::Sender<StoreCommand>,
}

impl StoreActorControl {
    async fn shutdown(&self) {
        let (response, receiver) = oneshot::channel();
        if self
            .sender
            .send(StoreCommand::Shutdown { response })
            .await
            .is_ok()
        {
            let _ = receiver.await;
        }
    }
}

fn spawn_store_actor<S>(
    store: S,
    config: PaperReadOnlyConfig,
    renew_interval: StdDuration,
) -> (
    ObserverStoreProxy,
    StoreActorControl,
    JoinHandle<Result<(), StoreActorError>>,
)
where
    S: CoordinatorStore + 'static,
{
    let (sender, receiver) = mpsc::channel(STORE_CHANNEL_CAPACITY);
    let proxy = ObserverStoreProxy {
        sender: sender.clone(),
    };
    let control = StoreActorControl { sender };
    let task = tokio::spawn(run_store_actor(store, config, renew_interval, receiver));
    (proxy, control, task)
}

async fn run_store_actor<S>(
    mut store: S,
    config: PaperReadOnlyConfig,
    renew_interval: StdDuration,
    mut receiver: mpsc::Receiver<StoreCommand>,
) -> Result<(), StoreActorError>
where
    S: CoordinatorStore,
{
    let mut active_lease: Option<PaperObserverLease> = None;
    let mut renewals = tokio::time::interval(renew_interval);
    renewals.set_missed_tick_behavior(MissedTickBehavior::Delay);
    renewals.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = renewals.tick(), if active_lease.is_some() => {
                let previous = active_lease.clone().expect("guarded active observer lease");
                let renewed = store
                    .renew_observer_lease(&previous, config.fence_ttl)
                    .await
                    .map_err(|_| StoreActorError::StoreOperationFailed)?
                    .ok_or(StoreActorError::LeaseLost)?;
                if !valid_renewal(&previous, &renewed, &config) {
                    return Err(StoreActorError::LeaseLost);
                }
                active_lease = Some(renewed);
            }
            command = receiver.recv() => {
                let Some(command) = command else {
                    return Ok(());
                };
                match command {
                    StoreCommand::Acquire { account_fingerprint, owner_id, ttl, response } => {
                        if account_fingerprint != config.expected_account_fingerprint
                            || owner_id != config.owner_id
                            || ttl != config.fence_ttl
                        {
                            let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                            return Err(StoreActorError::StoreOperationFailed);
                        }
                        if let Some(active) = active_lease.as_ref() {
                            let _ = response.send(Ok(Some(active.clone())));
                            continue;
                        }
                        let acquired = store
                            .acquire_observer_lease(account_fingerprint, owner_id, ttl)
                            .await;
                        match acquired {
                            Ok(Some(lease)) if valid_acquired_lease(&lease, &config) => {
                                active_lease = Some(lease.clone());
                                let _ = response.send(Ok(Some(lease)));
                            }
                            Ok(None) => {
                                let _ = response.send(Ok(None));
                            }
                            Ok(Some(_)) => {
                                let _ = response.send(Ok(None));
                                return Err(StoreActorError::LeaseLost);
                            }
                            Err(_) => {
                                let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                                return Err(StoreActorError::StoreOperationFailed);
                            }
                        }
                    }
                    StoreCommand::Renew { lease, ttl, response } => {
                        let Some(current) = active_lease.clone() else {
                            let _ = response.send(Ok(None));
                            return Err(StoreActorError::LeaseLost);
                        };
                        if ttl != config.fence_ttl || !same_lease_identity(&lease, &current) {
                            let _ = response.send(Ok(None));
                            return Err(StoreActorError::LeaseLost);
                        }
                        match store.renew_observer_lease(&current, ttl).await {
                            Ok(Some(renewed)) if valid_renewal(&current, &renewed, &config) => {
                                active_lease = Some(renewed.clone());
                                let _ = response.send(Ok(Some(renewed)));
                            }
                            Ok(_) => {
                                let _ = response.send(Ok(None));
                                return Err(StoreActorError::LeaseLost);
                            }
                            Err(_) => {
                                let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                                return Err(StoreActorError::StoreOperationFailed);
                            }
                        }
                    }
                    StoreCommand::BeginCycle { cycle, lease, response } => {
                        let Some(current) = current_lease(&active_lease, &lease) else {
                            let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                            return Err(StoreActorError::LeaseLost);
                        };
                        let result = store.begin_cycle(&cycle, current).await;
                        let terminal = !matches!(
                            &result,
                            Ok(()) | Err(CoordinatorPortError::CycleStartOutcomeUnknown)
                        );
                        let _ = response.send(result);
                        if terminal { return Err(StoreActorError::StoreOperationFailed); }
                    }
                    StoreCommand::ResolveCycleStart { key, response } => {
                        if active_lease.is_none() {
                            let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                            return Err(StoreActorError::LeaseLost);
                        }
                        let result = store.resolve_cycle_start(&key).await;
                        let terminal = result.is_err();
                        let _ = response.send(result);
                        if terminal { return Err(StoreActorError::StoreOperationFailed); }
                    }
                    StoreCommand::LoadLocalProjection { cycle, lease, response } => {
                        let Some(current) = current_lease(&active_lease, &lease) else {
                            let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                            return Err(StoreActorError::LeaseLost);
                        };
                        let result = store.load_local_projection(&cycle, current).await;
                        let terminal = !matches!(
                            &result,
                            Ok(_) | Err(CoordinatorPortError::ProjectionUnavailable)
                        );
                        let _ = response.send(result);
                        if terminal { return Err(StoreActorError::StoreOperationFailed); }
                    }
                    StoreCommand::PersistCycleResult { cycle, result, key, lease, response } => {
                        let Some(current) = current_lease(&active_lease, &lease) else {
                            let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                            return Err(StoreActorError::LeaseLost);
                        };
                        let outcome = store.persist_cycle_result(&cycle, &result, &key, current).await;
                        let terminal = !matches!(
                            &outcome,
                            Ok(()) | Err(CoordinatorPortError::CycleCompletionOutcomeUnknown)
                        );
                        let _ = response.send(outcome);
                        if terminal { return Err(StoreActorError::StoreOperationFailed); }
                    }
                    StoreCommand::ResolveCycleCompletion { key, response } => {
                        if active_lease.is_none() {
                            let _ = response.send(Err(CoordinatorPortError::StoreUnavailable));
                            return Err(StoreActorError::LeaseLost);
                        }
                        let result = store.resolve_cycle_completion(&key).await;
                        let terminal = result.is_err();
                        let _ = response.send(result);
                        if terminal { return Err(StoreActorError::StoreOperationFailed); }
                    }
                    StoreCommand::Shutdown { response } => {
                        let _ = response.send(());
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn current_lease<'a>(
    active: &'a Option<PaperObserverLease>,
    requested: &PaperObserverLease,
) -> Option<&'a PaperObserverLease> {
    active
        .as_ref()
        .filter(|current| same_lease_identity(requested, current))
}

fn same_lease_identity(left: &PaperObserverLease, right: &PaperObserverLease) -> bool {
    left.environment == right.environment
        && left.account_fingerprint == right.account_fingerprint
        && left.owner_id == right.owner_id
        && left.fencing_token == right.fencing_token
}

fn valid_acquired_lease(lease: &PaperObserverLease, config: &PaperReadOnlyConfig) -> bool {
    lease.environment == Environment::Paper
        && lease.account_fingerprint == config.expected_account_fingerprint
        && lease.owner_id == config.owner_id
        && lease.fencing_token > 0
        && lease.lease_until > Utc::now()
}

fn valid_renewal(
    previous: &PaperObserverLease,
    renewed: &PaperObserverLease,
    config: &PaperReadOnlyConfig,
) -> bool {
    valid_acquired_lease(renewed, config)
        && same_lease_identity(previous, renewed)
        && renewed.lease_until > previous.lease_until
}

#[async_trait]
impl CoordinatorStore for ObserverStoreProxy {
    async fn acquire_observer_lease(
        &mut self,
        account_fingerprint: HashDigest,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::Acquire {
                account_fingerprint,
                owner_id,
                ttl,
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }

    async fn renew_observer_lease(
        &mut self,
        lease: &PaperObserverLease,
        ttl: Duration,
    ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::Renew {
                lease: lease.clone(),
                ttl,
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }

    async fn begin_cycle(
        &mut self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<(), CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::BeginCycle {
                cycle: cycle.clone(),
                lease: lease.clone(),
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }

    async fn resolve_cycle_start(
        &mut self,
        key: &ObserverCycleKey,
    ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::ResolveCycleStart {
                key: *key,
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }

    async fn load_local_projection(
        &mut self,
        cycle: &ObserverCycle,
        lease: &PaperObserverLease,
    ) -> Result<LocalProjection, CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::LoadLocalProjection {
                cycle: cycle.clone(),
                lease: lease.clone(),
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }

    async fn persist_cycle_result(
        &mut self,
        cycle: &ObserverCycle,
        result: &StartupResult,
        key: &ObserverPersistenceKey,
        lease: &PaperObserverLease,
    ) -> Result<(), CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::PersistCycleResult {
                cycle: cycle.clone(),
                result: Box::new(result.clone()),
                key: *key,
                lease: lease.clone(),
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }

    async fn resolve_cycle_completion(
        &mut self,
        key: &ObserverPersistenceKey,
    ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::ResolveCycleCompletion {
                key: *key,
                response,
            })
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?;
        receiver
            .await
            .map_err(|_| CoordinatorPortError::StoreUnavailable)?
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ObserverCycleSummary {
    cycle_id: Uuid,
    outcome: StartupOutcome,
    evidence_hash: HashDigest,
    reason_count: usize,
}

#[async_trait]
trait ObserverCycleRunner: Send {
    async fn run_cycle(
        &mut self,
        trigger: ObserverCycleTrigger,
        started_at: DateTime<Utc>,
    ) -> Result<ObserverCycleSummary, ObserverRuntimeError>;
}

struct CoordinatorCycleRunner<B, C> {
    config: PaperReadOnlyConfig,
    store: ObserverStoreProxy,
    broker: B,
    clock: C,
}

#[async_trait]
impl<B, C> ObserverCycleRunner for CoordinatorCycleRunner<B, C>
where
    B: ReadOnlyBroker,
    C: CoordinatorClock,
{
    async fn run_cycle(
        &mut self,
        trigger: ObserverCycleTrigger,
        started_at: DateTime<Utc>,
    ) -> Result<ObserverCycleSummary, ObserverRuntimeError> {
        let result = run_observer_reconciliation(
            &self.config,
            &mut self.store,
            &mut self.broker,
            trigger,
            started_at,
            &self.clock,
        )
        .await
        .map_err(|_| ObserverRuntimeError::ReconciliationFailed)?;
        if result.environment() != Environment::Paper
            || result.mode() != CoordinatorMode::ReconcileOnly
            || result.resumable()
        {
            return Err(ObserverRuntimeError::ReconciliationFailed);
        }
        let evidence_hash = result
            .persistence_key()
            .map_err(|_| ObserverRuntimeError::ReconciliationFailed)?
            .evidence_hash;
        Ok(ObserverCycleSummary {
            cycle_id: result.cycle_id(),
            outcome: result.outcome(),
            evidence_hash,
            reason_count: result.reasons().len(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RuntimeState {
    Starting,
    Healthy,
    Degraded,
    Stopping,
}

#[derive(Clone, Copy)]
struct RuntimeHealth {
    state: RuntimeState,
    healthy: bool,
    reason_code: &'static str,
}

type SharedRuntimeHealth = Arc<Mutex<RuntimeHealth>>;

fn update_health(
    health: &SharedRuntimeHealth,
    state: RuntimeState,
    healthy: bool,
    reason_code: &'static str,
) {
    let mut guard = health
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = RuntimeHealth {
        state,
        healthy,
        reason_code,
    };
}

fn current_health(health: &SharedRuntimeHealth) -> RuntimeHealth {
    *health
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatRecord<'a> {
    schema: &'static str,
    environment: &'static str,
    mode: &'static str,
    image_digest: &'a str,
    sequence: u64,
    emitted_at: DateTime<Utc>,
    state: RuntimeState,
    healthy: bool,
    reason_code: &'static str,
}

trait HeartbeatSink: Send + Sync + 'static {
    fn emit(&self, record: &HeartbeatRecord<'_>) -> Result<(), ObserverRuntimeError>;
}

struct StdoutHeartbeatSink {
    metric_namespace: String,
}

#[derive(Serialize)]
struct EmbeddedMetricEnvelope<'a> {
    #[serde(rename = "_aws")]
    aws: EmbeddedMetricMetadata<'a>,
    #[serde(rename = "Environment")]
    environment: &'static str,
    #[serde(rename = "Heartbeat")]
    heartbeat: u8,
    observer: &'a HeartbeatRecord<'a>,
}

#[derive(Serialize)]
struct EmbeddedMetricMetadata<'a> {
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "CloudWatchMetrics")]
    cloud_watch_metrics: [EmbeddedMetricDirective<'a>; 1],
}

#[derive(Serialize)]
struct EmbeddedMetricDirective<'a> {
    #[serde(rename = "Namespace")]
    namespace: &'a str,
    #[serde(rename = "Dimensions")]
    dimensions: [[&'static str; 1]; 1],
    #[serde(rename = "Metrics")]
    metrics: [EmbeddedMetricDefinition; 1],
}

#[derive(Serialize)]
struct EmbeddedMetricDefinition {
    #[serde(rename = "Name")]
    name: &'static str,
    #[serde(rename = "Unit")]
    unit: &'static str,
}

impl HeartbeatSink for StdoutHeartbeatSink {
    fn emit(&self, record: &HeartbeatRecord<'_>) -> Result<(), ObserverRuntimeError> {
        let envelope = EmbeddedMetricEnvelope {
            aws: EmbeddedMetricMetadata {
                timestamp: record.emitted_at.timestamp_millis(),
                cloud_watch_metrics: [EmbeddedMetricDirective {
                    namespace: &self.metric_namespace,
                    dimensions: [["Environment"]],
                    metrics: [EmbeddedMetricDefinition {
                        name: "Heartbeat",
                        unit: "Count",
                    }],
                }],
            },
            environment: "paper",
            heartbeat: u8::from(record.healthy),
            observer: record,
        };
        let line = serde_json::to_string(&envelope)
            .map_err(|_| ObserverRuntimeError::HealthPublicationFailed)?;
        writeln!(io::stdout().lock(), "{line}")
            .map_err(|_| ObserverRuntimeError::HealthPublicationFailed)
    }
}

async fn run_heartbeat_loop<C, S>(
    clock: C,
    sink: S,
    image_digest: String,
    health: SharedRuntimeHealth,
    mut shutdown: watch::Receiver<bool>,
    interval: StdDuration,
) -> Result<(), ObserverRuntimeError>
where
    C: CoordinatorClock,
    S: HeartbeatSink,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sequence = 0_u64;
    let mut last_emitted_at: Option<DateTime<Utc>> = None;
    loop {
        let stopping = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                changed.is_err() || *shutdown.borrow()
            }
            _ = ticker.tick() => false,
        };
        let emitted_at = clock.now();
        if last_emitted_at.is_some_and(|previous| emitted_at <= previous) {
            return Err(ObserverRuntimeError::ClockInvalid);
        }
        sequence = sequence
            .checked_add(1)
            .ok_or(ObserverRuntimeError::HealthPublicationFailed)?;
        let snapshot = current_health(&health);
        sink.emit(&HeartbeatRecord {
            schema: "wasp2/paper-observer-heartbeat/v1",
            environment: "paper",
            mode: "reconcile_only",
            image_digest: &image_digest,
            sequence,
            emitted_at,
            state: snapshot.state,
            healthy: snapshot.healthy,
            reason_code: snapshot.reason_code,
        })?;
        last_emitted_at = Some(emitted_at);
        if stopping {
            return Ok(());
        }
    }
}

async fn run_cycle_loop<R, C>(
    mut runner: R,
    clock: C,
    health: SharedRuntimeHealth,
    mut shutdown: watch::Receiver<bool>,
    cycle_interval: StdDuration,
) -> Result<(), ObserverRuntimeError>
where
    R: ObserverCycleRunner,
    C: CoordinatorClock,
{
    let mut next_start = Instant::now();
    let mut trigger = ObserverCycleTrigger::Startup;
    let mut last_started_at: Option<DateTime<Utc>> = None;
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            _ = tokio::time::sleep_until(next_start) => {}
        }
        let started_at = clock.now();
        if last_started_at.is_some_and(|previous| started_at <= previous) {
            return Err(ObserverRuntimeError::ClockInvalid);
        }
        last_started_at = Some(started_at);

        let cycle = runner.run_cycle(trigger, started_at);
        let summary = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                continue;
            }
            result = cycle => result?,
        };
        match summary.outcome {
            StartupOutcome::Blocked => {
                update_health(
                    &health,
                    RuntimeState::Healthy,
                    true,
                    "cycle_blocked_expected",
                );
            }
            StartupOutcome::Failed => {
                update_health(&health, RuntimeState::Degraded, false, "cycle_failed");
            }
        }
        tracing::info!(
            event = "paper_observer_cycle_completed",
            cycle_id = %summary.cycle_id,
            trigger = trigger.as_str(),
            outcome = ?summary.outcome,
            evidence_hash = %summary.evidence_hash,
            reason_count = summary.reason_count,
        );

        trigger = ObserverCycleTrigger::Periodic;
        next_start += cycle_interval;
        let now = Instant::now();
        while next_start <= now {
            next_start += cycle_interval;
        }
    }
}

async fn acquire_initial_lease(
    mut store: ObserverStoreProxy,
    config: PaperReadOnlyConfig,
) -> Result<(), ObserverRuntimeError> {
    let mut delay = ACQUIRE_RETRY_BASE;
    for attempt in 0..ACQUIRE_ATTEMPTS {
        let acquired = store
            .acquire_observer_lease(
                config.expected_account_fingerprint,
                config.owner_id,
                config.fence_ttl,
            )
            .await
            .map_err(|_| ObserverRuntimeError::LeaseLost)?;
        if acquired.is_some() {
            return Ok(());
        }
        if attempt + 1 == ACQUIRE_ATTEMPTS {
            break;
        }
        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2).min(ACQUIRE_RETRY_CAP);
    }
    Err(ObserverRuntimeError::LeaseUnavailable)
}

async fn operating_system_shutdown() -> Result<(), ObserverRuntimeError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| ObserverRuntimeError::ShutdownUnavailable)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(|_| ObserverRuntimeError::ShutdownUnavailable)?;
            }
            _ = terminate.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|_| ObserverRuntimeError::ShutdownUnavailable)
    }
}

enum RuntimeStop {
    Shutdown(Result<(), ObserverRuntimeError>),
    Cycle(Result<(), ObserverRuntimeError>),
    Heartbeat(Result<(), ObserverRuntimeError>),
    Store(Result<Result<(), StoreActorError>, tokio::task::JoinError>),
    Database(Result<Result<(), ObserverDatabaseError>, tokio::task::JoinError>),
}

/// Runs the concrete paper observer from environment-injected secrets.
///
/// Configuration and all construction checks complete before the first
/// network operation. The only broker transport built here is GET-only.
pub async fn run_paper_observer_from_env() -> Result<(), ObserverRuntimeError> {
    let mut process = PaperObserverProcessConfig::from_env()?;
    let database_config = PaperObserverDatabaseConfig::from_env()
        .map_err(|_| ObserverRuntimeError::UnsafeConfiguration)?;
    let fingerprint_salt = std::mem::take(&mut process.fingerprint_salt);
    let broker = AlpacaReadOnlyBroker::from_read_only_env(fingerprint_salt)
        .map_err(|_| ObserverRuntimeError::BrokerUnavailable)?;

    let database = database_config
        .connect()
        .await
        .map_err(|_| ObserverRuntimeError::DatabaseUnavailable)?;
    let (store, mut database_task) = database
        .into_supervised_parts()
        .map_err(|_| ObserverRuntimeError::DatabaseConnectionEnded)?;
    let (proxy, control, mut store_task) =
        spawn_store_actor(store, process.coordinator.clone(), LEASE_RENEW_INTERVAL);

    let health = Arc::new(Mutex::new(RuntimeHealth {
        state: RuntimeState::Starting,
        healthy: false,
        reason_code: "starting",
    }));
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let heartbeat_sink = StdoutHeartbeatSink {
        metric_namespace: process.metric_namespace.clone(),
    };
    let mut heartbeat_task = tokio::spawn(run_heartbeat_loop(
        crate::coordinator::SystemCoordinatorClock,
        heartbeat_sink,
        process.image_digest.clone(),
        Arc::clone(&health),
        shutdown_receiver.clone(),
        HEARTBEAT_INTERVAL,
    ));
    let mut shutdown_signal = Box::pin(operating_system_shutdown());
    let mut acquisition_task = tokio::spawn(acquire_initial_lease(
        proxy.clone(),
        process.coordinator.clone(),
    ));

    let acquisition = tokio::select! {
        result = &mut acquisition_task => {
            result.map_err(|_| ObserverRuntimeError::LeaseLost)?
        }
        result = &mut shutdown_signal => {
            update_health(&health, RuntimeState::Stopping, false, "operator_shutdown");
            let _ = shutdown_sender.send(true);
            acquisition_task.abort();
            finish_runtime_tasks(&control, &mut store_task, &mut database_task, &mut heartbeat_task).await;
            return result;
        }
        result = &mut database_task => {
            acquisition_task.abort();
            store_task.abort();
            heartbeat_task.abort();
            let _ = result;
            return Err(ObserverRuntimeError::DatabaseConnectionEnded);
        }
        result = &mut store_task => {
            acquisition_task.abort();
            database_task.abort();
            heartbeat_task.abort();
            let _ = result;
            return Err(ObserverRuntimeError::StoreActorEnded);
        }
        result = &mut heartbeat_task => {
            acquisition_task.abort();
            store_task.abort();
            database_task.abort();
            return result
                .map_err(|_| ObserverRuntimeError::HealthPublicationFailed)?;
        }
    };
    acquisition?;

    let runner = CoordinatorCycleRunner {
        config: process.coordinator.clone(),
        store: proxy,
        broker,
        clock: crate::coordinator::SystemCoordinatorClock,
    };
    let mut cycle_task = tokio::spawn(run_cycle_loop(
        runner,
        crate::coordinator::SystemCoordinatorClock,
        Arc::clone(&health),
        shutdown_receiver,
        CYCLE_INTERVAL,
    ));

    let stop = tokio::select! {
        result = &mut shutdown_signal => RuntimeStop::Shutdown(result),
        result = &mut cycle_task => RuntimeStop::Cycle(
            result.unwrap_or(Err(ObserverRuntimeError::ReconciliationFailed))
        ),
        result = &mut heartbeat_task => RuntimeStop::Heartbeat(
            result.unwrap_or(Err(ObserverRuntimeError::HealthPublicationFailed))
        ),
        result = &mut store_task => RuntimeStop::Store(result),
        result = &mut database_task => RuntimeStop::Database(result),
    };

    let final_result = match stop {
        RuntimeStop::Shutdown(result) => {
            update_health(&health, RuntimeState::Stopping, false, "operator_shutdown");
            result
        }
        RuntimeStop::Cycle(result) => {
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "cycle_runtime_ended",
            );
            result.and(Err(ObserverRuntimeError::ReconciliationFailed))
        }
        RuntimeStop::Heartbeat(result) => {
            update_health(&health, RuntimeState::Degraded, false, "heartbeat_ended");
            result.and(Err(ObserverRuntimeError::HealthPublicationFailed))
        }
        RuntimeStop::Store(result) => {
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "lease_or_store_lost",
            );
            let _ = result;
            Err(ObserverRuntimeError::StoreActorEnded)
        }
        RuntimeStop::Database(result) => {
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "database_connection_ended",
            );
            let _ = result;
            Err(ObserverRuntimeError::DatabaseConnectionEnded)
        }
    };
    let _ = shutdown_sender.send(true);
    cycle_task.abort();
    finish_runtime_tasks(
        &control,
        &mut store_task,
        &mut database_task,
        &mut heartbeat_task,
    )
    .await;
    final_result
}

async fn finish_runtime_tasks(
    control: &StoreActorControl,
    store_task: &mut JoinHandle<Result<(), StoreActorError>>,
    database_task: &mut JoinHandle<Result<(), ObserverDatabaseError>>,
    heartbeat_task: &mut JoinHandle<Result<(), ObserverRuntimeError>>,
) {
    control.shutdown().await;
    if tokio::time::timeout(StdDuration::from_secs(10), &mut *store_task)
        .await
        .is_err()
    {
        store_task.abort();
    }
    if tokio::time::timeout(StdDuration::from_secs(2), &mut *heartbeat_task)
        .await
        .is_err()
    {
        heartbeat_task.abort();
    }
    if tokio::time::timeout(StdDuration::from_secs(5), &mut *database_task)
        .await
        .is_err()
    {
        database_task.abort();
    }
}
