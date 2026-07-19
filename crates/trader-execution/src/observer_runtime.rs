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
const CYCLE_DRAIN_TIMEOUT: StdDuration = StdDuration::from_secs(70);
const CONTROL_SHUTDOWN_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const STORE_SHUTDOWN_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const HEARTBEAT_SHUTDOWN_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const DATABASE_SHUTDOWN_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const MAX_CLOCK_SKEW: StdDuration = StdDuration::from_secs(5);
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
    async fn shutdown(&self) -> Result<(), ObserverRuntimeError> {
        tokio::time::timeout(CONTROL_SHUTDOWN_TIMEOUT, async {
            let (response, receiver) = oneshot::channel();
            self.sender
                .send(StoreCommand::Shutdown { response })
                .await
                .map_err(|_| ObserverRuntimeError::StoreActorEnded)?;
            receiver
                .await
                .map_err(|_| ObserverRuntimeError::StoreActorEnded)
        })
        .await
        .map_err(|_| ObserverRuntimeError::StoreActorEnded)?
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
    clean_observation: bool,
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
            clean_observation: result.is_clean_read_only_observation(),
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
    let mut last_emission: Option<(DateTime<Utc>, Instant)> = None;
    loop {
        let stopping = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                changed.is_err() || *shutdown.borrow()
            }
            _ = ticker.tick() => false,
        };
        let emitted_at = clock.now();
        let emitted_monotonic = Instant::now();
        if let Some((previous_wall, previous_monotonic)) = last_emission {
            let wall_delta = emitted_at
                .signed_duration_since(previous_wall)
                .to_std()
                .map_err(|_| ObserverRuntimeError::ClockInvalid)?;
            if wall_delta.is_zero()
                || duration_difference(
                    wall_delta,
                    emitted_monotonic.duration_since(previous_monotonic),
                ) > MAX_CLOCK_SKEW
            {
                return Err(ObserverRuntimeError::ClockInvalid);
            }
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
        last_emission = Some((emitted_at, emitted_monotonic));
        if stopping {
            return Ok(());
        }
    }
}

fn duration_difference(left: StdDuration, right: StdDuration) -> StdDuration {
    left.abs_diff(right)
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

        // Once a cycle begins, let its durable start/resolve/persist protocol
        // reach a terminal outcome. Cancelling this future on SIGTERM could
        // leave an ambiguous cycle that a fresh owner cannot resolve.
        let summary = runner.run_cycle(trigger, started_at).await?;
        match (summary.outcome, summary.clean_observation) {
            (StartupOutcome::Blocked, true) => {
                update_health(
                    &health,
                    RuntimeState::Healthy,
                    true,
                    "cycle_reconciled_clean",
                );
            }
            (StartupOutcome::Blocked, false) => {
                update_health(
                    &health,
                    RuntimeState::Degraded,
                    false,
                    "cycle_blocked_with_findings",
                );
            }
            (StartupOutcome::Failed, _) => {
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

        if *shutdown.borrow() {
            return Ok(());
        }

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
    Cycle(Result<Result<(), ObserverRuntimeError>, tokio::task::JoinError>),
    Heartbeat(Result<Result<(), ObserverRuntimeError>, tokio::task::JoinError>),
    Store(Result<Result<(), StoreActorError>, tokio::task::JoinError>),
    Database(Result<Result<(), ObserverDatabaseError>, tokio::task::JoinError>),
}

enum AcquisitionStop {
    Acquired(Result<Result<(), ObserverRuntimeError>, tokio::task::JoinError>),
    Shutdown(Result<(), ObserverRuntimeError>),
    Heartbeat(Result<Result<(), ObserverRuntimeError>, tokio::task::JoinError>),
    Store(Result<Result<(), StoreActorError>, tokio::task::JoinError>),
    Database(Result<Result<(), ObserverDatabaseError>, tokio::task::JoinError>),
}

fn store_stop_error(
    result: Result<Result<(), StoreActorError>, tokio::task::JoinError>,
) -> ObserverRuntimeError {
    match result {
        Ok(Err(StoreActorError::LeaseLost)) => ObserverRuntimeError::LeaseLost,
        Ok(Ok(())) | Ok(Err(StoreActorError::StoreOperationFailed)) | Err(_) => {
            ObserverRuntimeError::StoreActorEnded
        }
    }
}

fn cycle_stop_error(
    result: Result<Result<(), ObserverRuntimeError>, tokio::task::JoinError>,
) -> ObserverRuntimeError {
    match result {
        Ok(Err(error)) => error,
        Ok(Ok(())) | Err(_) => ObserverRuntimeError::ReconciliationFailed,
    }
}

fn heartbeat_stop_error(
    result: Result<Result<(), ObserverRuntimeError>, tokio::task::JoinError>,
) -> ObserverRuntimeError {
    match result {
        Ok(Err(error)) => error,
        Ok(Ok(())) | Err(_) => ObserverRuntimeError::HealthPublicationFailed,
    }
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
    let (store, database_task) = database
        .into_supervised_parts()
        .map_err(|_| ObserverRuntimeError::DatabaseConnectionEnded)?;
    let (proxy, control, store_task) =
        spawn_store_actor(store, process.coordinator.clone(), LEASE_RENEW_INTERVAL);
    let mut database_task = Some(database_task);
    let mut store_task = Some(store_task);

    let health = Arc::new(Mutex::new(RuntimeHealth {
        state: RuntimeState::Starting,
        healthy: false,
        reason_code: "starting",
    }));
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let heartbeat_sink = StdoutHeartbeatSink {
        metric_namespace: process.metric_namespace.clone(),
    };
    let heartbeat_task = tokio::spawn(run_heartbeat_loop(
        crate::coordinator::SystemCoordinatorClock,
        heartbeat_sink,
        process.image_digest.clone(),
        Arc::clone(&health),
        shutdown_receiver.clone(),
        HEARTBEAT_INTERVAL,
    ));
    let mut heartbeat_task = Some(heartbeat_task);
    let mut shutdown_signal = Box::pin(operating_system_shutdown());
    let acquisition_task = tokio::spawn(acquire_initial_lease(
        proxy.clone(),
        process.coordinator.clone(),
    ));
    let mut acquisition_task = Some(acquisition_task);

    let acquisition_stop = tokio::select! {
        biased;
        result = database_task.as_mut().expect("database task present") => {
            AcquisitionStop::Database(result)
        }
        result = store_task.as_mut().expect("store task present") => {
            AcquisitionStop::Store(result)
        }
        result = heartbeat_task.as_mut().expect("heartbeat task present") => {
            AcquisitionStop::Heartbeat(result)
        }
        result = acquisition_task.as_mut().expect("acquisition task present") => {
            AcquisitionStop::Acquired(result)
        }
        result = &mut shutdown_signal => AcquisitionStop::Shutdown(result),
    };

    let stop_before_cycles = match acquisition_stop {
        AcquisitionStop::Acquired(result) => {
            acquisition_task.take();
            match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => {
                    update_health(&health, RuntimeState::Degraded, false, "lease_unavailable");
                    Some(Err(error))
                }
                Err(_) => {
                    update_health(&health, RuntimeState::Degraded, false, "lease_task_ended");
                    Some(Err(ObserverRuntimeError::LeaseLost))
                }
            }
        }
        AcquisitionStop::Shutdown(result) => {
            update_health(&health, RuntimeState::Stopping, false, "operator_shutdown");
            Some(result)
        }
        AcquisitionStop::Heartbeat(result) => {
            heartbeat_task.take();
            update_health(&health, RuntimeState::Degraded, false, "heartbeat_ended");
            Some(Err(heartbeat_stop_error(result)))
        }
        AcquisitionStop::Store(result) => {
            store_task.take();
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "lease_or_store_lost",
            );
            Some(Err(store_stop_error(result)))
        }
        AcquisitionStop::Database(result) => {
            database_task.take();
            let _ = result;
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "database_connection_ended",
            );
            Some(Err(ObserverRuntimeError::DatabaseConnectionEnded))
        }
    };
    if let Some(primary) = stop_before_cycles {
        let _ = shutdown_sender.send(true);
        abort_task(acquisition_task.take()).await;
        let cleanup =
            finish_runtime_tasks(&control, None, store_task, database_task, heartbeat_task).await;
        return prefer_primary(primary, cleanup);
    }

    let runner = CoordinatorCycleRunner {
        config: process.coordinator.clone(),
        store: proxy,
        broker,
        clock: crate::coordinator::SystemCoordinatorClock,
    };
    let cycle_task = tokio::spawn(run_cycle_loop(
        runner,
        crate::coordinator::SystemCoordinatorClock,
        Arc::clone(&health),
        shutdown_receiver,
        CYCLE_INTERVAL,
    ));
    let mut cycle_task = Some(cycle_task);

    let stop = tokio::select! {
        biased;
        result = database_task.as_mut().expect("database task present") => {
            RuntimeStop::Database(result)
        },
        result = store_task.as_mut().expect("store task present") => {
            RuntimeStop::Store(result)
        },
        result = heartbeat_task.as_mut().expect("heartbeat task present") => {
            RuntimeStop::Heartbeat(result)
        },
        result = cycle_task.as_mut().expect("cycle task present") => RuntimeStop::Cycle(result),
        result = &mut shutdown_signal => RuntimeStop::Shutdown(result),
    };

    let primary = match stop {
        RuntimeStop::Shutdown(result) => {
            update_health(&health, RuntimeState::Stopping, false, "operator_shutdown");
            result
        }
        RuntimeStop::Cycle(result) => {
            cycle_task.take();
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "cycle_runtime_ended",
            );
            Err(cycle_stop_error(result))
        }
        RuntimeStop::Heartbeat(result) => {
            heartbeat_task.take();
            update_health(&health, RuntimeState::Degraded, false, "heartbeat_ended");
            Err(heartbeat_stop_error(result))
        }
        RuntimeStop::Store(result) => {
            store_task.take();
            update_health(
                &health,
                RuntimeState::Degraded,
                false,
                "lease_or_store_lost",
            );
            Err(store_stop_error(result))
        }
        RuntimeStop::Database(result) => {
            database_task.take();
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
    let cleanup = finish_runtime_tasks(
        &control,
        cycle_task,
        store_task,
        database_task,
        heartbeat_task,
    )
    .await;
    prefer_primary(primary, cleanup)
}

fn prefer_primary(
    primary: Result<(), ObserverRuntimeError>,
    cleanup: Result<(), ObserverRuntimeError>,
) -> Result<(), ObserverRuntimeError> {
    match primary {
        Err(error) => Err(error),
        Ok(()) => cleanup,
    }
}

async fn abort_task<T>(task: Option<JoinHandle<T>>) {
    if let Some(task) = task {
        task.abort();
        let _ = task.await;
    }
}

fn record_cleanup_error(
    first_error: &mut Option<ObserverRuntimeError>,
    error: ObserverRuntimeError,
) {
    if first_error.is_none() {
        *first_error = Some(error);
    }
}

async fn finish_runtime_tasks(
    control: &StoreActorControl,
    cycle_task: Option<JoinHandle<Result<(), ObserverRuntimeError>>>,
    store_task: Option<JoinHandle<Result<(), StoreActorError>>>,
    database_task: Option<JoinHandle<Result<(), ObserverDatabaseError>>>,
    heartbeat_task: Option<JoinHandle<Result<(), ObserverRuntimeError>>>,
) -> Result<(), ObserverRuntimeError> {
    let mut first_error = None;

    if let Some(mut task) = cycle_task {
        match tokio::time::timeout(CYCLE_DRAIN_TIMEOUT, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => record_cleanup_error(&mut first_error, error),
            Ok(Err(_)) => {
                record_cleanup_error(&mut first_error, ObserverRuntimeError::ReconciliationFailed)
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                record_cleanup_error(&mut first_error, ObserverRuntimeError::ReconciliationFailed);
            }
        }
    }

    if let Err(error) = control.shutdown().await {
        record_cleanup_error(&mut first_error, error);
    }

    if let Some(mut task) = store_task {
        match tokio::time::timeout(STORE_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(result) => record_cleanup_error(&mut first_error, store_stop_error(result)),
            Err(_) => {
                task.abort();
                let _ = task.await;
                record_cleanup_error(&mut first_error, ObserverRuntimeError::StoreActorEnded);
            }
        }
    }
    if let Some(mut task) = heartbeat_task {
        match tokio::time::timeout(HEARTBEAT_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(result) => record_cleanup_error(&mut first_error, heartbeat_stop_error(result)),
            Err(_) => {
                task.abort();
                let _ = task.await;
                record_cleanup_error(
                    &mut first_error,
                    ObserverRuntimeError::HealthPublicationFailed,
                );
            }
        }
    }
    if let Some(mut task) = database_task {
        match tokio::time::timeout(DATABASE_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(_) => record_cleanup_error(
                &mut first_error,
                ObserverRuntimeError::DatabaseConnectionEnded,
            ),
            Err(_) => {
                task.abort();
                let _ = task.await;
                record_cleanup_error(
                    &mut first_error,
                    ObserverRuntimeError::DatabaseConnectionEnded,
                );
            }
        }
    }

    first_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Clone, Copy)]
    enum RenewalAction {
        Success,
        Missing,
        ChangedToken,
    }

    struct ScriptedStoreState {
        acquisition_results: VecDeque<bool>,
        renewal_results: VecDeque<RenewalAction>,
        acquisition_times: Vec<Instant>,
        acquire_calls: usize,
        renew_calls: usize,
        active_lease: Option<PaperObserverLease>,
    }

    struct ScriptedStore {
        state: Arc<Mutex<ScriptedStoreState>>,
    }

    impl ScriptedStore {
        fn new(
            acquisition_results: impl IntoIterator<Item = bool>,
            renewal_results: impl IntoIterator<Item = RenewalAction>,
        ) -> (Self, Arc<Mutex<ScriptedStoreState>>) {
            let state = Arc::new(Mutex::new(ScriptedStoreState {
                acquisition_results: acquisition_results.into_iter().collect(),
                renewal_results: renewal_results.into_iter().collect(),
                acquisition_times: Vec::new(),
                acquire_calls: 0,
                renew_calls: 0,
                active_lease: None,
            }));
            (
                Self {
                    state: Arc::clone(&state),
                },
                state,
            )
        }
    }

    #[async_trait]
    impl CoordinatorStore for ScriptedStore {
        async fn acquire_observer_lease(
            &mut self,
            account_fingerprint: HashDigest,
            owner_id: Uuid,
            ttl: Duration,
        ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.acquire_calls += 1;
            state.acquisition_times.push(Instant::now());
            if !state.acquisition_results.pop_front().unwrap_or(true) {
                return Ok(None);
            }
            let next_token = state
                .active_lease
                .as_ref()
                .map_or(1, |lease| lease.fencing_token + 1);
            let lease = PaperObserverLease {
                environment: Environment::Paper,
                account_fingerprint,
                owner_id,
                fencing_token: next_token,
                lease_until: Utc::now() + ttl,
            };
            state.active_lease = Some(lease.clone());
            Ok(Some(lease))
        }

        async fn renew_observer_lease(
            &mut self,
            lease: &PaperObserverLease,
            ttl: Duration,
        ) -> Result<Option<PaperObserverLease>, CoordinatorPortError> {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.renew_calls += 1;
            if state.active_lease.as_ref() != Some(lease) {
                return Err(CoordinatorPortError::StoreUnavailable);
            }
            let action = state
                .renewal_results
                .pop_front()
                .unwrap_or(RenewalAction::Success);
            match action {
                RenewalAction::Success => {
                    let mut renewed = lease.clone();
                    renewed.lease_until += ttl;
                    state.active_lease = Some(renewed.clone());
                    Ok(Some(renewed))
                }
                RenewalAction::Missing => {
                    state.active_lease = None;
                    Ok(None)
                }
                RenewalAction::ChangedToken => {
                    let mut renewed = lease.clone();
                    renewed.fencing_token += 1;
                    renewed.lease_until += ttl;
                    state.active_lease = Some(renewed.clone());
                    Ok(Some(renewed))
                }
            }
        }

        async fn begin_cycle(
            &mut self,
            _cycle: &ObserverCycle,
            _lease: &PaperObserverLease,
        ) -> Result<(), CoordinatorPortError> {
            Err(CoordinatorPortError::StoreUnavailable)
        }

        async fn resolve_cycle_start(
            &mut self,
            _key: &ObserverCycleKey,
        ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
            Err(CoordinatorPortError::StoreUnavailable)
        }

        async fn load_local_projection(
            &mut self,
            _cycle: &ObserverCycle,
            _lease: &PaperObserverLease,
        ) -> Result<LocalProjection, CoordinatorPortError> {
            Err(CoordinatorPortError::StoreUnavailable)
        }

        async fn persist_cycle_result(
            &mut self,
            _cycle: &ObserverCycle,
            _result: &StartupResult,
            _key: &ObserverPersistenceKey,
            _lease: &PaperObserverLease,
        ) -> Result<(), CoordinatorPortError> {
            Err(CoordinatorPortError::StoreUnavailable)
        }

        async fn resolve_cycle_completion(
            &mut self,
            _key: &ObserverPersistenceKey,
        ) -> Result<ObserverPersistenceResolution, CoordinatorPortError> {
            Err(CoordinatorPortError::StoreUnavailable)
        }
    }

    fn test_config() -> PaperReadOnlyConfig {
        PaperReadOnlyConfig {
            expected_account_fingerprint: HashDigest::sha256("paper-observer-test-account"),
            owner_id: Uuid::from_u128(0x8f14_e45f_ceea_567a_a5a3_6d3e_124f_4a21),
            fence_ttl: FENCE_TTL,
        }
    }

    async fn yield_to_runtime() {
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn lease_renews_concurrently_and_cycle_copies_may_be_stale() {
        let config = test_config();
        let (store, trace) = ScriptedStore::new([true], []);
        let (mut proxy, control, task) =
            spawn_store_actor(store, config.clone(), StdDuration::from_secs(10));
        let first = proxy
            .acquire_observer_lease(
                config.expected_account_fingerprint,
                config.owner_id,
                config.fence_ttl,
            )
            .await
            .unwrap()
            .unwrap();

        tokio::time::advance(StdDuration::from_secs(10)).await;
        yield_to_runtime().await;
        tokio::time::advance(StdDuration::from_secs(10)).await;
        yield_to_runtime().await;

        let current = proxy
            .acquire_observer_lease(
                config.expected_account_fingerprint,
                config.owner_id,
                config.fence_ttl,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.fencing_token, current.fencing_token);
        assert!(current.lease_until > first.lease_until);
        assert_eq!(
            trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .acquire_calls,
            1,
            "a supervisor tenure must acquire the database lease exactly once"
        );

        let renewed_from_stale_copy = proxy
            .renew_observer_lease(&first, config.fence_ttl)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.fencing_token, renewed_from_stale_copy.fencing_token);
        assert!(renewed_from_stale_copy.lease_until > current.lease_until);

        control.shutdown().await.unwrap();
        assert_eq!(task.await.unwrap(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn lease_loss_is_terminal_and_never_reacquires() {
        for action in [RenewalAction::Missing, RenewalAction::ChangedToken] {
            let config = test_config();
            let (store, trace) = ScriptedStore::new([true], [action]);
            let (mut proxy, _control, task) =
                spawn_store_actor(store, config.clone(), StdDuration::from_secs(10));
            proxy
                .acquire_observer_lease(
                    config.expected_account_fingerprint,
                    config.owner_id,
                    config.fence_ttl,
                )
                .await
                .unwrap()
                .unwrap();
            tokio::time::advance(StdDuration::from_secs(10)).await;
            yield_to_runtime().await;

            assert_eq!(task.await.unwrap(), Err(StoreActorError::LeaseLost));
            assert!(proxy
                .acquire_observer_lease(
                    config.expected_account_fingerprint,
                    config.owner_id,
                    config.fence_ttl,
                )
                .await
                .is_err());
            let trace = trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(trace.acquire_calls, 1);
            assert_eq!(trace.renew_calls, 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn lease_acquisition_backoff_is_bounded_and_resets_on_success() {
        let config = test_config();
        let (store, trace) = ScriptedStore::new([false, false, true], []);
        let (proxy, control, actor_task) =
            spawn_store_actor(store, config.clone(), StdDuration::from_secs(10));
        let acquisition_task = tokio::spawn(acquire_initial_lease(proxy, config));
        yield_to_runtime().await;
        let origin = trace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .acquisition_times[0];

        tokio::time::advance(StdDuration::from_secs(1)).await;
        yield_to_runtime().await;
        tokio::time::advance(StdDuration::from_secs(2)).await;
        yield_to_runtime().await;
        assert_eq!(acquisition_task.await.unwrap(), Ok(()));

        let times = trace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .acquisition_times
            .clone();
        assert_eq!(times.len(), 3);
        assert_eq!(times[0].duration_since(origin), StdDuration::ZERO);
        assert_eq!(times[1].duration_since(origin), StdDuration::from_secs(1));
        assert_eq!(times[2].duration_since(origin), StdDuration::from_secs(3));

        control.shutdown().await.unwrap();
        assert_eq!(actor_task.await.unwrap(), Ok(()));
    }

    struct QueueClock {
        values: Arc<Mutex<VecDeque<DateTime<Utc>>>>,
    }

    impl QueueClock {
        fn new(values: impl IntoIterator<Item = DateTime<Utc>>) -> Self {
            Self {
                values: Arc::new(Mutex::new(values.into_iter().collect())),
            }
        }
    }

    impl CoordinatorClock for QueueClock {
        fn now(&self) -> DateTime<Utc> {
            self.values
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .expect("scripted wall-clock value")
        }
    }

    struct CycleTrace {
        starts: Vec<(ObserverCycleTrigger, Instant, DateTime<Utc>)>,
        delays: VecDeque<StdDuration>,
        active: usize,
        max_active: usize,
    }

    struct ScriptedCycleRunner {
        trace: Arc<Mutex<CycleTrace>>,
    }

    #[async_trait]
    impl ObserverCycleRunner for ScriptedCycleRunner {
        async fn run_cycle(
            &mut self,
            trigger: ObserverCycleTrigger,
            started_at: DateTime<Utc>,
        ) -> Result<ObserverCycleSummary, ObserverRuntimeError> {
            let (delay, ordinal) = {
                let mut trace = self
                    .trace
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                trace.active += 1;
                trace.max_active = trace.max_active.max(trace.active);
                trace.starts.push((trigger, Instant::now(), started_at));
                (
                    trace.delays.pop_front().unwrap_or(StdDuration::ZERO),
                    trace.starts.len(),
                )
            };
            tokio::time::sleep(delay).await;
            self.trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active -= 1;
            Ok(ObserverCycleSummary {
                cycle_id: Uuid::from_u128(ordinal as u128),
                outcome: StartupOutcome::Blocked,
                clean_observation: false,
                evidence_hash: HashDigest::sha256(format!("cycle-{ordinal}")),
                reason_count: 4,
            })
        }
    }

    fn cycle_runner(
        delays: impl IntoIterator<Item = StdDuration>,
    ) -> (ScriptedCycleRunner, Arc<Mutex<CycleTrace>>) {
        let trace = Arc::new(Mutex::new(CycleTrace {
            starts: Vec::new(),
            delays: delays.into_iter().collect(),
            active: 0,
            max_active: 0,
        }));
        (
            ScriptedCycleRunner {
                trace: Arc::clone(&trace),
            },
            trace,
        )
    }

    struct FixedSummaryRunner {
        summary: ObserverCycleSummary,
    }

    #[async_trait]
    impl ObserverCycleRunner for FixedSummaryRunner {
        async fn run_cycle(
            &mut self,
            _trigger: ObserverCycleTrigger,
            _started_at: DateTime<Utc>,
        ) -> Result<ObserverCycleSummary, ObserverRuntimeError> {
            Ok(self.summary)
        }
    }

    fn wall_time(seconds: i64) -> DateTime<Utc> {
        "2026-07-19T14:00:00Z".parse::<DateTime<Utc>>().unwrap() + Duration::seconds(seconds)
    }

    fn starting_health() -> SharedRuntimeHealth {
        Arc::new(Mutex::new(RuntimeHealth {
            state: RuntimeState::Starting,
            healthy: false,
            reason_code: "starting",
        }))
    }

    #[tokio::test(start_paused = true)]
    async fn cycle_health_requires_semantically_clean_evidence() {
        for (outcome, clean_observation, expected_state, expected_reason) in [
            (
                StartupOutcome::Blocked,
                true,
                RuntimeState::Healthy,
                "cycle_reconciled_clean",
            ),
            (
                StartupOutcome::Blocked,
                false,
                RuntimeState::Degraded,
                "cycle_blocked_with_findings",
            ),
            (
                StartupOutcome::Failed,
                false,
                RuntimeState::Degraded,
                "cycle_failed",
            ),
        ] {
            let health = starting_health();
            let (shutdown_sender, shutdown_receiver) = watch::channel(false);
            let task = tokio::spawn(run_cycle_loop(
                FixedSummaryRunner {
                    summary: ObserverCycleSummary {
                        cycle_id: Uuid::from_u128(1),
                        outcome,
                        clean_observation,
                        evidence_hash: HashDigest::sha256("cycle-evidence"),
                        reason_count: usize::from(!clean_observation),
                    },
                },
                QueueClock::new([wall_time(0)]),
                Arc::clone(&health),
                shutdown_receiver,
                StdDuration::from_secs(60),
            ));
            yield_to_runtime().await;

            let snapshot = current_health(&health);
            assert_eq!(snapshot.state, expected_state);
            assert_eq!(snapshot.healthy, clean_observation);
            assert_eq!(snapshot.reason_code, expected_reason);

            shutdown_sender.send(true).unwrap();
            yield_to_runtime().await;
            assert_eq!(task.await.unwrap(), Ok(()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_cycle_drains_to_a_terminal_summary() {
        let (runner, trace) = cycle_runner([StdDuration::from_secs(30)]);
        let health = starting_health();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(run_cycle_loop(
            runner,
            QueueClock::new([wall_time(0)]),
            health,
            shutdown_receiver,
            StdDuration::from_secs(60),
        ));
        yield_to_runtime().await;
        assert_eq!(
            trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active,
            1
        );

        shutdown_sender.send(true).unwrap();
        yield_to_runtime().await;
        assert!(
            !task.is_finished(),
            "shutdown must not cancel a durably-started cycle"
        );

        tokio::time::advance(StdDuration::from_secs(30)).await;
        yield_to_runtime().await;
        assert_eq!(task.await.unwrap(), Ok(()));
        assert_eq!(
            trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active,
            0
        );
    }

    #[tokio::test]
    async fn cleanup_propagates_background_task_failure() {
        let config = test_config();
        let (store, _) = ScriptedStore::new([], []);
        let (_proxy, control, store_task) =
            spawn_store_actor(store, config, StdDuration::from_secs(10));
        let heartbeat_task = tokio::spawn(async { Ok(()) });
        let database_task = tokio::spawn(async { Err(ObserverDatabaseError::ConnectionEnded) });

        assert_eq!(
            finish_runtime_tasks(
                &control,
                None,
                Some(store_task),
                Some(database_task),
                Some(heartbeat_task),
            )
            .await,
            Err(ObserverRuntimeError::DatabaseConnectionEnded)
        );
    }

    #[test]
    fn primary_safety_failure_survives_cleanup_failure() {
        assert_eq!(
            prefer_primary(
                Err(ObserverRuntimeError::LeaseLost),
                Err(ObserverRuntimeError::StoreActorEnded),
            ),
            Err(ObserverRuntimeError::LeaseLost)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_scheduler_skips_missed_intervals_without_overlap() {
        let origin = Instant::now();
        let (runner, trace) = cycle_runner([StdDuration::from_secs(75), StdDuration::ZERO]);
        let clock = QueueClock::new([wall_time(0), wall_time(120)]);
        let health = starting_health();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(run_cycle_loop(
            runner,
            clock,
            health,
            shutdown_receiver,
            StdDuration::from_secs(60),
        ));
        yield_to_runtime().await;

        tokio::time::advance(StdDuration::from_secs(75)).await;
        yield_to_runtime().await;
        assert_eq!(
            trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .starts
                .len(),
            1
        );
        tokio::time::advance(StdDuration::from_secs(44)).await;
        yield_to_runtime().await;
        assert_eq!(
            trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .starts
                .len(),
            1
        );
        tokio::time::advance(StdDuration::from_secs(1)).await;
        yield_to_runtime().await;

        {
            let starts = trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(starts.starts.len(), 2);
            assert_eq!(starts.starts[0].0, ObserverCycleTrigger::Startup);
            assert_eq!(starts.starts[1].0, ObserverCycleTrigger::Periodic);
            assert_eq!(starts.starts[0].1.duration_since(origin), StdDuration::ZERO);
            assert_eq!(
                starts.starts[1].1.duration_since(origin),
                StdDuration::from_secs(120)
            );
            assert_eq!(starts.max_active, 1);
        }

        shutdown_sender.send(true).unwrap();
        yield_to_runtime().await;
        assert_eq!(task.await.unwrap(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn clock_regression_stops_before_a_second_cycle() {
        let (runner, trace) = cycle_runner([StdDuration::ZERO]);
        let clock = QueueClock::new([wall_time(10), wall_time(9)]);
        let (_shutdown_sender, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(run_cycle_loop(
            runner,
            clock,
            starting_health(),
            shutdown_receiver,
            StdDuration::from_secs(60),
        ));
        yield_to_runtime().await;
        tokio::time::advance(StdDuration::from_secs(60)).await;
        yield_to_runtime().await;
        assert_eq!(task.await.unwrap(), Err(ObserverRuntimeError::ClockInvalid));
        assert_eq!(
            trace
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .starts
                .len(),
            1
        );
    }

    #[derive(Clone)]
    struct RecordingHeartbeatSink {
        records: Arc<Mutex<Vec<String>>>,
    }

    impl HeartbeatSink for RecordingHeartbeatSink {
        fn emit(&self, record: &HeartbeatRecord<'_>) -> Result<(), ObserverRuntimeError> {
            self.records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(serde_json::to_string(record).unwrap());
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_is_monotonic_redacted_and_tracks_state() {
        let health = starting_health();
        let records = Arc::new(Mutex::new(Vec::new()));
        let sink = RecordingHeartbeatSink {
            records: Arc::clone(&records),
        };
        let clock = QueueClock::new([wall_time(0), wall_time(30), wall_time(31)]);
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(run_heartbeat_loop(
            clock,
            sink,
            format!("sha256:{}", "a".repeat(64)),
            Arc::clone(&health),
            shutdown_receiver,
            StdDuration::from_secs(30),
        ));
        yield_to_runtime().await;
        update_health(
            &health,
            RuntimeState::Healthy,
            true,
            "cycle_reconciled_clean",
        );
        tokio::time::advance(StdDuration::from_secs(30)).await;
        yield_to_runtime().await;
        update_health(&health, RuntimeState::Stopping, false, "operator_shutdown");
        shutdown_sender.send(true).unwrap();
        yield_to_runtime().await;
        assert_eq!(task.await.unwrap(), Ok(()));

        let records = records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(records.len(), 3);
        let parsed = records
            .iter()
            .map(|record| serde_json::from_str::<serde_json::Value>(record).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(parsed[0]["sequence"], 1);
        assert_eq!(parsed[1]["sequence"], 2);
        assert_eq!(parsed[2]["sequence"], 3);
        assert_eq!(parsed[0]["state"], "starting");
        assert_eq!(parsed[1]["state"], "healthy");
        assert_eq!(parsed[2]["state"], "stopping");
        assert_eq!(parsed[0]["healthy"], false);
        assert_eq!(parsed[1]["healthy"], true);
        assert_eq!(parsed[2]["healthy"], false);
        let combined = records.join("\n");
        for forbidden in [
            "account_fingerprint",
            "fencing_token",
            "database_host",
            "database_user",
            "api_secret",
            "provider_payload",
        ] {
            assert!(!combined.contains(forbidden));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_rejects_wall_clock_jump_against_monotonic_time() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let (_shutdown_sender, shutdown_receiver) = watch::channel(false);
        let task = tokio::spawn(run_heartbeat_loop(
            QueueClock::new([wall_time(0), wall_time(600)]),
            RecordingHeartbeatSink {
                records: Arc::clone(&records),
            },
            format!("sha256:{}", "a".repeat(64)),
            starting_health(),
            shutdown_receiver,
            StdDuration::from_secs(30),
        ));
        yield_to_runtime().await;
        tokio::time::advance(StdDuration::from_secs(30)).await;
        yield_to_runtime().await;

        assert_eq!(task.await.unwrap(), Err(ObserverRuntimeError::ClockInvalid));
        assert_eq!(
            records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            1,
            "future-dated heartbeat must not be emitted"
        );
    }

    #[test]
    fn observer_composition_root_has_no_execution_capability() {
        let source = include_str!("observer_runtime.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production observer source");
        for forbidden in [
            "BrokerPort",
            "Executor",
            "PgExecutionStore",
            "ActivationPermit",
            "AlpacaPaperAdapter",
            "LIVE_TRADING_API",
            "submit_committed_intent",
            "cancel_order",
            "replace_order",
        ] {
            assert!(
                !production.contains(forbidden),
                "observer runtime imported forbidden execution capability: {forbidden}"
            );
        }
    }

    #[test]
    fn runtime_inputs_reject_placeholders_and_noncanonical_values() {
        assert_eq!(
            decode_secret_hex("ab".repeat(MIN_FINGERPRINT_SALT_BYTES)).unwrap(),
            vec![0xab; MIN_FINGERPRINT_SALT_BYTES]
        );
        assert_eq!(
            decode_secret_hex("AB".repeat(MIN_FINGERPRINT_SALT_BYTES)),
            Err(ObserverRuntimeError::UnsafeConfiguration)
        );
        assert!(validate_image_digest(&format!("sha256:{}", "a".repeat(64))).is_ok());
        assert_eq!(
            validate_image_digest(&format!("sha256:{}", "0".repeat(64))),
            Err(ObserverRuntimeError::UnsafeConfiguration)
        );
        assert!(validate_metric_namespace("AlpacaAutotrader/paper").is_ok());
        assert_eq!(
            validate_metric_namespace("AlpacaAutotrader/live"),
            Err(ObserverRuntimeError::UnsafeConfiguration)
        );
    }
}
