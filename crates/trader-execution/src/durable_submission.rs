//! Durable paper-order submission orchestration.
//!
//! This module is deliberately separate from the legacy in-memory executor.
//! A broker POST is reachable only after the complete execution chain commits
//! and PostgreSQL grants the one-time first-dispatch claim. Once that claim
//! exists, every uncertain outcome is recovery-by-GET only.

use async_trait::async_trait;
use chrono::{DateTime, Timelike, Utc};
use serde_json::{json, Value};
use thiserror::Error;
use trader_core::{Environment, HashDigest, Money, OrderIntent, WholeQuantity};
use uuid::Uuid;

use crate::{
    port::{
        BrokerPort, ObservedBrokerOrder, RegularTradingSessionPermit, SubmissionNotDispatched,
        SubmissionOutcome,
    },
    store::{
        BrokerEventWrite, BrokerFill, BrokerWriteResult, ClaimedOutbox, CommitRecoveryKey,
        CommitResolution, DurableExecutionChain, ExecutionStore, FencedLease, OutboxClaimKind,
        PersistedExecutionChain, StoreError, UnresolvedOutbox,
    },
    ExecutionError,
};

const SUBMISSION_UNKNOWN_REASON: &str = "SUBMISSION_UNKNOWN";
const TERMINAL_COMPLETION_REASON: &str = "BROKER_TERMINAL_TRUTH";
const MAX_DETAIL_CHARACTERS: usize = 256;

/// Persistence errors preserve deterministic commit-recovery evidence without
/// exposing PostgreSQL transport internals to provider-free fault tests.
#[derive(Debug, Error)]
pub enum SubmissionStoreError {
    #[error("durable submission commit for {operation} is unknown")]
    CommitUnknown {
        operation: &'static str,
        recovery: Box<CommitRecoveryKey>,
    },
    #[error("durable submission store failed: {0}")]
    Store(String),
}

impl From<StoreError> for SubmissionStoreError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::CommitUnknown {
                operation,
                recovery,
                ..
            } => Self::CommitUnknown {
                operation,
                recovery,
            },
            error => Self::Store(error.to_string()),
        }
    }
}

/// Narrow durable boundary used by the coordinator. Every production
/// `ExecutionStore` implements it; fakes can inject exact commit boundaries.
#[async_trait]
pub trait SubmissionStorePort: Send {
    async fn persist_execution_chain(
        &mut self,
        chain: &DurableExecutionChain<'_>,
    ) -> Result<PersistedExecutionChain, SubmissionStoreError>;
    async fn claim_first_dispatch(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError>;
    async fn claim_recovery(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError>;
    async fn claim_terminal_completion(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError>;
    async fn discover_unresolved_outboxes(
        &mut self,
        lease: &FencedLease,
        limit: u16,
    ) -> Result<Vec<UnresolvedOutbox>, SubmissionStoreError>;
    async fn append_submission_unknown(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        reason_code: &str,
        detail: &Value,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, SubmissionStoreError>;
    async fn record_broker_event(
        &mut self,
        write: &BrokerEventWrite<'_>,
    ) -> Result<BrokerWriteResult, SubmissionStoreError>;
    async fn finalize_outbox(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        completion_reason: &str,
    ) -> Result<bool, SubmissionStoreError>;
    async fn resolve_commit(
        &mut self,
        key: &CommitRecoveryKey,
    ) -> Result<CommitResolution, SubmissionStoreError>;
}

#[async_trait]
impl<T: ExecutionStore + Send> SubmissionStorePort for T {
    async fn persist_execution_chain(
        &mut self,
        chain: &DurableExecutionChain<'_>,
    ) -> Result<PersistedExecutionChain, SubmissionStoreError> {
        ExecutionStore::persist_execution_chain(self, chain)
            .await
            .map_err(Into::into)
    }

    async fn claim_first_dispatch(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError> {
        ExecutionStore::claim_first_dispatch(self, outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn claim_recovery(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError> {
        ExecutionStore::claim_recovery(self, outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn claim_terminal_completion(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError> {
        ExecutionStore::claim_terminal_completion(self, outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn discover_unresolved_outboxes(
        &mut self,
        lease: &FencedLease,
        limit: u16,
    ) -> Result<Vec<UnresolvedOutbox>, SubmissionStoreError> {
        ExecutionStore::discover_unresolved_outboxes(self, lease, limit)
            .await
            .map_err(Into::into)
    }

    async fn append_submission_unknown(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        reason_code: &str,
        detail: &Value,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, SubmissionStoreError> {
        ExecutionStore::append_submission_unknown(
            self,
            outbox_id,
            lease,
            reason_code,
            detail,
            occurred_at,
        )
        .await
        .map_err(Into::into)
    }

    async fn record_broker_event(
        &mut self,
        write: &BrokerEventWrite<'_>,
    ) -> Result<BrokerWriteResult, SubmissionStoreError> {
        ExecutionStore::record_broker_event(self, write)
            .await
            .map_err(Into::into)
    }

    async fn finalize_outbox(
        &mut self,
        outbox_id: Uuid,
        lease: &FencedLease,
        completion_reason: &str,
    ) -> Result<bool, SubmissionStoreError> {
        ExecutionStore::finalize_outbox(self, outbox_id, lease, completion_reason)
            .await
            .map_err(Into::into)
    }

    async fn resolve_commit(
        &mut self,
        key: &CommitRecoveryKey,
    ) -> Result<CommitResolution, SubmissionStoreError> {
        ExecutionStore::resolve_commit(self, key)
            .await
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmissionProgress {
    BrokerStatePersisted {
        outbox_id: Uuid,
        broker_event_id: Uuid,
        client_order_id: String,
        provider_status: String,
    },
    SubmissionUnknown {
        outbox_id: Uuid,
        client_order_id: String,
        detail: String,
    },
    RecoveryRequired {
        outbox_id: Uuid,
        client_order_id: String,
    },
    LookupStillUnresolved {
        outbox_id: Uuid,
        client_order_id: String,
        detail: String,
    },
    TerminalFinalized {
        outbox_id: Uuid,
        client_order_id: String,
        provider_status: String,
    },
}

/// Paper-only durable coordinator. It cannot mint live authority and owns no
/// path that retries a POST after a first-dispatch claim.
pub struct DurableSubmissionCoordinator<B> {
    broker: B,
}

impl<B: BrokerPort> DurableSubmissionCoordinator<B> {
    pub fn new(broker: B) -> Self {
        Self { broker }
    }

    /// Persists the complete authority/evidence chain, obtains the one-time
    /// dispatch claim, then makes at most one broker POST.
    pub async fn persist_and_dispatch<S: SubmissionStorePort>(
        &self,
        store: &mut S,
        chain: &DurableExecutionChain<'_>,
        session_permit: &RegularTradingSessionPermit,
        now: DateTime<Utc>,
    ) -> Result<SubmissionProgress, ExecutionError> {
        validate_paper_lease(chain.lease, now)?;
        let not_after = session_permit.submission_deadline(chain.intent, now)?;
        if not_after > chain.lease.lease_until {
            return Err(ExecutionError::AuthorityDenied(
                "submission deadline extends beyond the execution lease".into(),
            ));
        }
        self.broker.validate_submission_window(not_after, now)?;

        let expected = ExpectedExecutionChain::new(chain)?;
        let persisted = persist_chain_resolving(store, chain, &expected).await?;
        if persisted != expected.persisted {
            return Err(ExecutionError::LedgerInvariant(
                "persisted execution-chain identifiers differ from deterministic evidence".into(),
            ));
        }

        // Recheck after persistence. The concrete transport performs its own
        // final arrival check immediately before writing bytes.
        let rechecked_deadline = session_permit.submission_deadline(chain.intent, now)?;
        if rechecked_deadline != not_after || rechecked_deadline > chain.lease.lease_until {
            return Err(ExecutionError::AuthorityDenied(
                "submission authority changed after durable persistence".into(),
            ));
        }
        self.broker
            .validate_submission_window(rechecked_deadline, now)?;

        let claim = store
            .claim_first_dispatch(persisted.outbox_id, chain.lease)
            .await
            .map_err(submission_store_error)?;
        let Some(claim) = claim else {
            return Ok(SubmissionProgress::RecoveryRequired {
                outbox_id: persisted.outbox_id,
                client_order_id: chain.intent.client_order_id.clone(),
            });
        };
        validate_claim(
            &claim,
            chain.lease,
            chain.intent,
            persisted.outbox_id,
            OutboxClaimKind::FirstDispatch,
            true,
        )?;

        match self
            .broker
            .submit_committed_intent(chain.intent, session_permit, rechecked_deadline)
            .await
        {
            Ok(SubmissionOutcome::Observed(observed)) => {
                self.persist_observed(
                    store,
                    chain.lease,
                    persisted.outbox_id,
                    chain.intent,
                    observed,
                )
                .await
            }
            Ok(SubmissionOutcome::NotDispatched(evidence)) => {
                validate_not_dispatched(&evidence, &chain.intent.client_order_id)?;
                let detail = bounded_detail(&format!(
                    "transport proved no dispatch, but no durable retry transition exists: {}",
                    evidence.detail
                ));
                let payload = json!({
                    "detail": detail,
                    "reason_code": evidence.reason_code,
                    "not_dispatched_evidence_hash": evidence.evidence_hash,
                });
                append_unknown_resolving(
                    store,
                    persisted.outbox_id,
                    chain.lease,
                    SUBMISSION_UNKNOWN_REASON,
                    &payload,
                    now,
                )
                .await?;
                Ok(SubmissionProgress::SubmissionUnknown {
                    outbox_id: persisted.outbox_id,
                    client_order_id: chain.intent.client_order_id.clone(),
                    detail,
                })
            }
            Ok(SubmissionOutcome::Unknown { detail }) | Err(ExecutionError::Broker(detail)) => {
                self.persist_unknown(
                    store,
                    chain.lease,
                    persisted.outbox_id,
                    &chain.intent.client_order_id,
                    &detail,
                    now,
                )
                .await
            }
            Err(error) => {
                self.persist_unknown(
                    store,
                    chain.lease,
                    persisted.outbox_id,
                    &chain.intent.client_order_id,
                    &error.to_string(),
                    now,
                )
                .await
            }
        }
    }

    /// Restarts incomplete work without ever POSTing. `eligible` work is
    /// surfaced for a fresh supervised decision path; every post-dispatch state
    /// obtains lookup-only authority and queries by the committed client ID.
    pub async fn recover_unresolved<S: SubmissionStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        limit: u16,
        now: DateTime<Utc>,
    ) -> Result<Vec<SubmissionProgress>, ExecutionError> {
        validate_paper_lease(lease, now)?;
        let unresolved = store
            .discover_unresolved_outboxes(lease, limit)
            .await
            .map_err(submission_store_error)?;
        let mut progress = Vec::with_capacity(unresolved.len());

        for item in unresolved {
            let intent = validate_unresolved(&item, lease)?;
            if item.available_at > now || item.current_state == "eligible" {
                progress.push(SubmissionProgress::RecoveryRequired {
                    outbox_id: item.outbox_id,
                    client_order_id: intent.client_order_id,
                });
                continue;
            }
            if item.current_state == "terminal" {
                progress.push(
                    finalize_terminal(store, lease, item.outbox_id, &intent, "persisted_terminal")
                        .await?,
                );
                continue;
            }
            if !matches!(
                item.current_state.as_str(),
                "dispatch_started"
                    | "submission_unknown"
                    | "acknowledged"
                    | "broker_confirmed"
                    | "blocked"
            ) {
                return Err(ExecutionError::Lifecycle(format!(
                    "unresolved outbox has unsupported state {}",
                    bounded_detail(&item.current_state)
                )));
            }

            let claim = store
                .claim_recovery(item.outbox_id, lease)
                .await
                .map_err(submission_store_error)?;
            let Some(claim) = claim else {
                progress.push(SubmissionProgress::RecoveryRequired {
                    outbox_id: item.outbox_id,
                    client_order_id: intent.client_order_id,
                });
                continue;
            };
            validate_claim(
                &claim,
                lease,
                &intent,
                item.outbox_id,
                OutboxClaimKind::RecoveryLookupOnly,
                false,
            )?;

            match self.broker.find_order_by_client_id(&intent).await {
                Ok(Some(observed)) => {
                    progress.push(
                        self.persist_observed(store, lease, item.outbox_id, &intent, observed)
                            .await?,
                    );
                }
                Ok(None) => progress.push(SubmissionProgress::LookupStillUnresolved {
                    outbox_id: item.outbox_id,
                    client_order_id: intent.client_order_id,
                    detail: "broker lookup returned no order".into(),
                }),
                Err(error) => progress.push(SubmissionProgress::LookupStillUnresolved {
                    outbox_id: item.outbox_id,
                    client_order_id: intent.client_order_id,
                    detail: bounded_detail(&error.to_string()),
                }),
            }
        }
        Ok(progress)
    }

    async fn persist_unknown<S: SubmissionStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        outbox_id: Uuid,
        client_order_id: &str,
        detail: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<SubmissionProgress, ExecutionError> {
        let detail = bounded_detail(detail);
        let payload = json!({"detail": detail});
        append_unknown_resolving(
            store,
            outbox_id,
            lease,
            SUBMISSION_UNKNOWN_REASON,
            &payload,
            occurred_at,
        )
        .await?;
        Ok(SubmissionProgress::SubmissionUnknown {
            outbox_id,
            client_order_id: client_order_id.to_owned(),
            detail,
        })
    }

    async fn persist_observed<S: SubmissionStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        outbox_id: Uuid,
        intent: &OrderIntent,
        observed: ObservedBrokerOrder,
    ) -> Result<SubmissionProgress, ExecutionError> {
        validate_observed(&observed, intent)?;
        let event = observed.event();
        let provider_order_id = event.provider_order_id.as_deref().ok_or_else(|| {
            ExecutionError::Lifecycle("observed broker event lacks provider order identity".into())
        })?;
        let observed_fills = if event.filled_quantity == WholeQuantity::ZERO {
            Vec::new()
        } else {
            self.broker
                .fills_for_order(intent, provider_order_id, event.filled_quantity)
                .await?
        };
        let fills = observed_fills
            .iter()
            .map(|fill| BrokerFill {
                fill_id: fill.fill_id.clone(),
                quantity: fill.quantity,
                price: fill.price,
                // Alpaca's FILL activity contract carries execution economics
                // but not a per-fill commission/fee. Separate account-activity
                // accounting must append any fee; inventing one here would be
                // worse than the explicit zero evidence.
                fee: Money::ZERO,
                executed_at: fill.executed_at,
                received_at: fill.received_at,
                raw_payload_hash: fill.raw_payload_hash,
                activity_evidence_hash: fill.activity_evidence_hash,
            })
            .collect::<Vec<_>>();

        let raw_payload: Value =
            serde_json::from_slice(observed.raw_response_json()).map_err(|_| {
                ExecutionError::Lifecycle("validated broker evidence could not be decoded".into())
            })?;
        let broker_event_id = record_broker_event_resolving(
            store,
            &BrokerEventWrite {
                intent_id: &intent.intent_id,
                event,
                raw_payload: &raw_payload,
                fills: &fills,
                lease,
            },
        )
        .await?;

        if is_terminal_status(&event.status) {
            return finalize_terminal(store, lease, outbox_id, intent, &event.status).await;
        }
        Ok(SubmissionProgress::BrokerStatePersisted {
            outbox_id,
            broker_event_id,
            client_order_id: intent.client_order_id.clone(),
            provider_status: event.status.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedExecutionChain {
    persisted: PersistedExecutionChain,
    recovery: CommitRecoveryKey,
}

impl ExpectedExecutionChain {
    fn new(chain: &DurableExecutionChain<'_>) -> Result<Self, ExecutionError> {
        let decision_id = parse_uuid(&chain.snapshot.decision_id, "decision_id")?;
        let order_plan_id = parse_uuid(&chain.plan.plan_id, "order plan_id")?;
        let intent_id = parse_uuid(&chain.intent.intent_id, "intent_id")?;
        let target_hash = HashDigest::of_json(chain.target)?;
        let risk_hash = HashDigest::of_json(chain.risk)?;
        let target_portfolio_id = stable_child_uuid(decision_id, &format!("target:{target_hash}"));
        let risk_decision_id = stable_child_uuid(target_portfolio_id, &format!("risk:{risk_hash}"));
        let outbox_id = stable_child_uuid(intent_id, "outbox:intent-committed");
        let decision_payload_hash = HashDigest::of_json(chain.snapshot)?;
        let risk_limits_hash = HashDigest::of_json(&chain.risk.limits)?;
        let outbox_payload_hash = HashDigest::of_json(chain.intent)?;
        Ok(Self {
            persisted: PersistedExecutionChain {
                target_portfolio_id,
                risk_decision_id,
                outbox_id,
            },
            recovery: CommitRecoveryKey::ExecutionChain {
                decision_id,
                target_portfolio_id,
                risk_decision_id,
                order_plan_id,
                intent_id,
                outbox_id,
                decision_payload_hash,
                target_payload_hash: target_hash,
                risk_limits_hash,
                outbox_payload_hash,
            },
        })
    }
}

async fn persist_chain_resolving<S: SubmissionStorePort>(
    store: &mut S,
    chain: &DurableExecutionChain<'_>,
    expected: &ExpectedExecutionChain,
) -> Result<PersistedExecutionChain, ExecutionError> {
    for attempt in 0..2 {
        match store.persist_execution_chain(chain).await {
            Ok(persisted) => return Ok(persisted),
            Err(SubmissionStoreError::CommitUnknown { recovery, .. }) => {
                if recovery.as_ref() != &expected.recovery {
                    return Err(ExecutionError::LedgerInvariant(
                        "execution-chain commit uncertainty returned the wrong recovery evidence"
                            .into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(submission_store_error)?
                {
                    CommitResolution::Committed => return Ok(expected.persisted),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "execution chain was not committed after one proven database retry"
                                .into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "execution-chain recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(submission_store_error(error)),
        }
    }
    unreachable!("bounded execution-chain persistence loop always returns")
}

async fn append_unknown_resolving<S: SubmissionStorePort>(
    store: &mut S,
    outbox_id: Uuid,
    lease: &FencedLease,
    reason_code: &str,
    detail: &Value,
    occurred_at: DateTime<Utc>,
) -> Result<(), ExecutionError> {
    let occurred_at = postgres_timestamp(occurred_at)?;
    let state_event_id = stable_child_uuid(outbox_id, "state:submission-unknown");
    let evidence_hash = submission_unknown_hash(reason_code, detail, lease, occurred_at)?;
    let expected = CommitRecoveryKey::SubmissionUnknown {
        outbox_id,
        state_event_id,
        evidence_hash,
    };
    for attempt in 0..2 {
        match store
            .append_submission_unknown(outbox_id, lease, reason_code, detail, occurred_at)
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(ExecutionError::LedgerInvariant(
                    "submission-unknown state was not durably appended".into(),
                ));
            }
            Err(SubmissionStoreError::CommitUnknown { recovery, .. }) => {
                if recovery.as_ref() != &expected {
                    return Err(ExecutionError::LedgerInvariant(
                        "submission-unknown commit returned the wrong recovery evidence".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(submission_store_error)?
                {
                    CommitResolution::Committed => return Ok(()),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "submission-unknown evidence did not commit after one proven retry"
                                .into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "submission-unknown recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(submission_store_error(error)),
        }
    }
    unreachable!("bounded submission-unknown persistence loop always returns")
}

async fn record_broker_event_resolving<S: SubmissionStorePort>(
    store: &mut S,
    write: &BrokerEventWrite<'_>,
) -> Result<Uuid, ExecutionError> {
    let provider_order_id =
        write.event.provider_order_id.as_deref().ok_or_else(|| {
            ExecutionError::Lifecycle("broker event lacks provider order ID".into())
        })?;
    let broker_event_id = stable_named_uuid(&format!(
        "broker-event:{provider_order_id}:{}",
        write.event.raw_payload_hash
    ));
    let expected = CommitRecoveryKey::BrokerEvent {
        broker_event_id,
        raw_payload_hash: write.event.raw_payload_hash,
        cumulative_filled_quantity: write.event.filled_quantity,
    };
    for attempt in 0..2 {
        match store.record_broker_event(write).await {
            Ok(result) if result.broker_event_id == broker_event_id => return Ok(broker_event_id),
            Ok(_) => {
                return Err(ExecutionError::LedgerInvariant(
                    "broker-event persistence returned a non-deterministic identifier".into(),
                ));
            }
            Err(SubmissionStoreError::CommitUnknown { recovery, .. }) => {
                if recovery.as_ref() != &expected {
                    return Err(ExecutionError::LedgerInvariant(
                        "broker-event commit returned the wrong recovery evidence".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(submission_store_error)?
                {
                    CommitResolution::Committed => return Ok(broker_event_id),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "broker event did not commit after one proven database retry".into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "broker-event recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(submission_store_error(error)),
        }
    }
    unreachable!("bounded broker-event persistence loop always returns")
}

async fn finalize_terminal<S: SubmissionStorePort>(
    store: &mut S,
    lease: &FencedLease,
    outbox_id: Uuid,
    intent: &OrderIntent,
    provider_status: &str,
) -> Result<SubmissionProgress, ExecutionError> {
    let claim = store
        .claim_terminal_completion(outbox_id, lease)
        .await
        .map_err(submission_store_error)?;
    let Some(claim) = claim else {
        return Ok(SubmissionProgress::RecoveryRequired {
            outbox_id,
            client_order_id: intent.client_order_id.clone(),
        });
    };
    validate_claim(
        &claim,
        lease,
        intent,
        outbox_id,
        OutboxClaimKind::TerminalCompletionOnly,
        false,
    )?;
    finalize_outbox_resolving(store, outbox_id, lease, TERMINAL_COMPLETION_REASON).await?;
    Ok(SubmissionProgress::TerminalFinalized {
        outbox_id,
        client_order_id: intent.client_order_id.clone(),
        provider_status: provider_status.to_owned(),
    })
}

async fn finalize_outbox_resolving<S: SubmissionStorePort>(
    store: &mut S,
    outbox_id: Uuid,
    lease: &FencedLease,
    completion_reason: &str,
) -> Result<(), ExecutionError> {
    let expected = CommitRecoveryKey::OutboxFinalization {
        outbox_id,
        completion_reason: completion_reason.to_owned(),
    };
    for attempt in 0..2 {
        match store
            .finalize_outbox(outbox_id, lease, completion_reason)
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(ExecutionError::LedgerInvariant(
                    "terminal broker truth did not finalize its order outbox".into(),
                ));
            }
            Err(SubmissionStoreError::CommitUnknown { recovery, .. }) => {
                if recovery.as_ref() != &expected {
                    return Err(ExecutionError::LedgerInvariant(
                        "outbox finalization returned the wrong recovery evidence".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(submission_store_error)?
                {
                    CommitResolution::Committed => return Ok(()),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "outbox finalization did not commit after one proven retry".into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "outbox-finalization recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(submission_store_error(error)),
        }
    }
    unreachable!("bounded outbox-finalization loop always returns")
}

fn validate_paper_lease(lease: &FencedLease, now: DateTime<Utc>) -> Result<(), ExecutionError> {
    if lease.environment != Environment::Paper {
        return Err(ExecutionError::AuthorityDenied(
            "durable submission coordinator is paper-only".into(),
        ));
    }
    if lease.fencing_token == 0 || now >= lease.lease_until {
        return Err(ExecutionError::AuthorityDenied(
            "durable submission requires a live positive execution fence".into(),
        ));
    }
    Ok(())
}

fn validate_claim(
    claim: &ClaimedOutbox,
    lease: &FencedLease,
    intent: &OrderIntent,
    outbox_id: Uuid,
    expected_kind: OutboxClaimKind,
    require_creation_fence_match: bool,
) -> Result<(), ExecutionError> {
    let intent_id = parse_uuid(&intent.intent_id, "claimed intent_id")?;
    let payload_intent: OrderIntent =
        serde_json::from_value(claim.payload.clone()).map_err(|_| {
            ExecutionError::LedgerInvariant("claimed outbox payload is not an OrderIntent".into())
        })?;
    if claim.kind != expected_kind
        || claim.outbox_id != outbox_id
        || claim.intent_id != intent_id
        || claim.environment != Environment::Paper
        || claim.environment != lease.environment
        || claim.account_fingerprint != lease.account_fingerprint
        || claim.claimed_by != lease.owner_id
        || claim.claim_fencing_token != lease.fencing_token
        || claim.created_fencing_token > lease.fencing_token
        || (require_creation_fence_match && claim.created_fencing_token != lease.fencing_token)
        || claim.claimed_at >= lease.lease_until
        || claim.available_at > claim.claimed_at
        || claim.attempt_count == 0
        || (expected_kind == OutboxClaimKind::FirstDispatch && claim.attempt_count != 1)
        || payload_intent != *intent
    {
        return Err(ExecutionError::AuthorityDenied(
            "outbox claim does not exactly match its paper account, intent, owner, fence, and authority class"
                .into(),
        ));
    }
    Ok(())
}

fn validate_unresolved(
    item: &UnresolvedOutbox,
    lease: &FencedLease,
) -> Result<OrderIntent, ExecutionError> {
    if item.created_fencing_token == 0 || item.created_fencing_token > lease.fencing_token {
        return Err(ExecutionError::AuthorityDenied(
            "unresolved outbox belongs to an invalid or future fence".into(),
        ));
    }
    let intent: OrderIntent = serde_json::from_value(item.payload.clone()).map_err(|_| {
        ExecutionError::LedgerInvariant("unresolved outbox payload is not an OrderIntent".into())
    })?;
    if parse_uuid(&intent.intent_id, "unresolved intent_id")? != item.intent_id
        || stable_child_uuid(item.intent_id, "outbox:intent-committed") != item.outbox_id
    {
        return Err(ExecutionError::LedgerInvariant(
            "unresolved outbox identity differs from its committed payload".into(),
        ));
    }
    Ok(intent)
}

fn validate_observed(
    observed: &ObservedBrokerOrder,
    intent: &OrderIntent,
) -> Result<(), ExecutionError> {
    let event = observed.event();
    if event.client_order_id != intent.client_order_id
        || event
            .provider_order_id
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || event.filled_quantity > intent.quantity
        || event.received_at < event.provider_timestamp
    {
        return Err(ExecutionError::Lifecycle(
            "observed broker event does not exactly match the committed order identity and quantity"
                .into(),
        ));
    }
    if event.filled_quantity != WholeQuantity::ZERO && event.fill_price.is_none() {
        return Err(ExecutionError::Lifecycle(
            "observed cumulative fill lacks its average fill price".into(),
        ));
    }
    Ok(())
}

fn validate_not_dispatched(
    evidence: &SubmissionNotDispatched,
    client_order_id: &str,
) -> Result<(), ExecutionError> {
    let expected_hash = HashDigest::of_json(&json!({
        "client_order_id": &evidence.client_order_id,
        "observed_at": evidence.observed_at,
        "reason_code": &evidence.reason_code,
        "detail": &evidence.detail,
    }))?;
    if evidence.client_order_id != client_order_id
        || evidence.reason_code.trim().is_empty()
        || evidence.detail.trim().is_empty()
        || evidence.evidence_hash != expected_hash
    {
        return Err(ExecutionError::Lifecycle(
            "pre-I/O submission evidence is invalid or belongs to another order".into(),
        ));
    }
    Ok(())
}

fn submission_unknown_hash(
    reason_code: &str,
    detail: &Value,
    lease: &FencedLease,
    occurred_at: DateTime<Utc>,
) -> Result<HashDigest, ExecutionError> {
    let fencing_token = i64::try_from(lease.fencing_token).map_err(|_| {
        ExecutionError::AuthorityDenied("execution fence exceeds database range".into())
    })?;
    Ok(HashDigest::of_json(&json!({
        "reason_code": reason_code,
        "detail": detail,
        "fencing_token": fencing_token,
        "occurred_at": occurred_at,
    }))?)
}

fn postgres_timestamp(value: DateTime<Utc>) -> Result<DateTime<Utc>, ExecutionError> {
    value
        .with_nanosecond((value.nanosecond() / 1_000) * 1_000)
        .ok_or_else(|| ExecutionError::LedgerInvariant("timestamp cannot be normalized".into()))
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, ExecutionError> {
    Uuid::parse_str(value)
        .map_err(|_| ExecutionError::LedgerInvariant(format!("{field} is not a UUID")))
}

fn stable_child_uuid(namespace: Uuid, label: &str) -> Uuid {
    Uuid::new_v5(&namespace, label.as_bytes())
}

fn stable_named_uuid(label: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, label.as_bytes())
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        "filled" | "canceled" | "expired" | "replaced" | "rejected"
    )
}

fn bounded_detail(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() || character == ' ' {
                character
            } else {
                '?'
            }
        })
        .take(MAX_DETAIL_CHARACTERS)
        .collect()
}

fn submission_store_error(error: SubmissionStoreError) -> ExecutionError {
    ExecutionError::LedgerInvariant(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex},
    };

    use trader_core::{
        AccountSnapshot, AccountStatus, ActivationPermit, BrokerEvent, DecisionSchedule,
        DecisionSnapshot, Fixed, MomentumTrendSpec, Money, OrderPlan, OrderSide, Price,
        RebalanceCadence, RiskDecision, RiskDisposition, RiskLimitSnapshot, StrategyRelease,
        StrategySpec, Symbol, TargetPortfolio, TimeInForce,
    };

    use super::*;
    use crate::port::{CancellationOutcome, ObservedBrokerFill, ObservedBrokerOrder};

    #[derive(Clone, Copy)]
    enum SubmitPlan {
        Observed(&'static str, u64),
        Unknown,
        NotDispatched,
    }

    #[derive(Clone, Copy)]
    enum LookupPlan {
        Observed(&'static str, u64),
        None,
        Error,
    }

    #[derive(Clone)]
    struct FakeBroker {
        log: Arc<Mutex<Vec<String>>>,
        submit_plans: Arc<Mutex<VecDeque<SubmitPlan>>>,
        lookup_plans: Arc<Mutex<VecDeque<LookupPlan>>>,
        posts: Arc<Mutex<u32>>,
        gets: Arc<Mutex<u32>>,
        now: DateTime<Utc>,
    }

    impl FakeBroker {
        fn new(
            log: Arc<Mutex<Vec<String>>>,
            now: DateTime<Utc>,
            submit_plans: impl IntoIterator<Item = SubmitPlan>,
            lookup_plans: impl IntoIterator<Item = LookupPlan>,
        ) -> Self {
            Self {
                log,
                submit_plans: Arc::new(Mutex::new(submit_plans.into_iter().collect())),
                lookup_plans: Arc::new(Mutex::new(lookup_plans.into_iter().collect())),
                posts: Arc::new(Mutex::new(0)),
                gets: Arc::new(Mutex::new(0)),
                now,
            }
        }

        fn observed(&self, intent: &OrderIntent, status: &str, filled: u64) -> ObservedBrokerOrder {
            let raw = format!(
                r#"{{"id":"provider-1","client_order_id":"{}","status":"{}","filled_qty":"{}"}}"#,
                intent.client_order_id, status, filled
            )
            .into_bytes();
            ObservedBrokerOrder::try_new(
                BrokerEvent {
                    provider_order_id: Some("provider-1".into()),
                    client_order_id: intent.client_order_id.clone(),
                    status: status.into(),
                    filled_quantity: WholeQuantity::new(filled),
                    fill_price: (filled > 0).then(|| "10".parse::<Price>().unwrap()),
                    provider_timestamp: self.now,
                    received_at: self.now,
                    raw_payload_hash: HashDigest::sha256(&raw),
                    request_id: Some("request-1".into()),
                },
                raw,
            )
            .unwrap()
        }
    }

    #[async_trait]
    impl BrokerPort for FakeBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        fn validate_submission_window(
            &self,
            broker_arrival_by: DateTime<Utc>,
            now: DateTime<Utc>,
        ) -> Result<(), ExecutionError> {
            self.log.lock().unwrap().push("validate".into());
            if now < broker_arrival_by {
                Ok(())
            } else {
                Err(ExecutionError::AuthorityDenied(
                    "test arrival deadline expired".into(),
                ))
            }
        }

        async fn find_order_by_client_id(
            &self,
            expected_intent: &OrderIntent,
        ) -> Result<Option<ObservedBrokerOrder>, ExecutionError> {
            *self.gets.lock().unwrap() += 1;
            self.log.lock().unwrap().push("get".into());
            match self.lookup_plans.lock().unwrap().pop_front().unwrap() {
                LookupPlan::Observed(status, filled) => {
                    Ok(Some(self.observed(expected_intent, status, filled)))
                }
                LookupPlan::None => Ok(None),
                LookupPlan::Error => Err(ExecutionError::Broker("lookup timeout".into())),
            }
        }

        async fn fills_for_order(
            &self,
            expected_intent: &OrderIntent,
            provider_order_id: &str,
            expected_cumulative_quantity: WholeQuantity,
        ) -> Result<Vec<ObservedBrokerFill>, ExecutionError> {
            if expected_cumulative_quantity == WholeQuantity::ZERO {
                return Ok(Vec::new());
            }
            let activities = (1..=expected_cumulative_quantity.get())
                .map(|cumulative| {
                    let leaves = expected_intent.quantity.get() - cumulative;
                    json!({
                        "id": format!("fill-activity-{cumulative}"),
                        "activity_type": "FILL",
                        "type": if leaves == 0 { "fill" } else { "partial_fill" },
                        "order_id": provider_order_id,
                        "symbol": expected_intent.symbol,
                        "side": expected_intent.side,
                        "qty": "1",
                        "cum_qty": cumulative.to_string(),
                        "leaves_qty": leaves.to_string(),
                        "price": "10",
                        "transaction_time": self.now,
                    })
                })
                .collect::<Vec<_>>();
            let raw: Arc<[u8]> = serde_json::to_vec(&activities).unwrap().into();
            let raw_hash = HashDigest::sha256(raw.as_ref());
            let received_at = self.now
                + chrono::Duration::seconds(
                    i64::try_from(expected_cumulative_quantity.get()).unwrap(),
                );
            activities
                .iter()
                .enumerate()
                .map(|(index, activity)| {
                    let cumulative = u64::try_from(index).unwrap() + 1;
                    let leaves = expected_intent.quantity.get() - cumulative;
                    ObservedBrokerFill::try_new(
                        activity["id"].as_str().unwrap().to_owned(),
                        if leaves == 0 {
                            "fill".into()
                        } else {
                            "partial_fill".into()
                        },
                        provider_order_id.to_owned(),
                        expected_intent.symbol.clone(),
                        expected_intent.side,
                        WholeQuantity::new(1),
                        WholeQuantity::new(cumulative),
                        WholeQuantity::new(leaves),
                        "10".parse().unwrap(),
                        self.now,
                        received_at,
                        Some(format!(
                            "fill-request-{}",
                            expected_cumulative_quantity.get()
                        )),
                        HashDigest::sha256(format!(
                            "fill-request-{}",
                            expected_cumulative_quantity.get()
                        )),
                        raw_hash,
                        Arc::clone(&raw),
                    )
                })
                .collect()
        }

        async fn find_order_by_provider_id(
            &self,
            _provider_order_id: &str,
            _expected_client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn submit_committed_intent(
            &self,
            intent: &OrderIntent,
            _session_permit: &RegularTradingSessionPermit,
            _not_after: DateTime<Utc>,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            *self.posts.lock().unwrap() += 1;
            self.log.lock().unwrap().push("post".into());
            match self.submit_plans.lock().unwrap().pop_front().unwrap() {
                SubmitPlan::Observed(status, filled) => Ok(SubmissionOutcome::Observed(
                    self.observed(intent, status, filled),
                )),
                SubmitPlan::Unknown => Ok(SubmissionOutcome::Unknown {
                    detail: "timeout after dispatch".into(),
                }),
                SubmitPlan::NotDispatched => {
                    let reason_code = "TRANSPORT_BEFORE_SEND".to_owned();
                    let detail = "local connector unavailable".to_owned();
                    let observed_at = self.now;
                    let evidence_hash = HashDigest::of_json(&json!({
                        "client_order_id": &intent.client_order_id,
                        "observed_at": observed_at,
                        "reason_code": &reason_code,
                        "detail": &detail,
                    }))
                    .unwrap();
                    Ok(SubmissionOutcome::NotDispatched(SubmissionNotDispatched {
                        client_order_id: intent.client_order_id.clone(),
                        observed_at,
                        reason_code,
                        detail,
                        evidence_hash,
                    }))
                }
            }
        }

        async fn cancel_order(
            &self,
            _provider_order_id: &str,
        ) -> Result<CancellationOutcome, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }
    }

    #[derive(Clone, Copy)]
    enum PersistPlan {
        Ok,
        CommitUnknown,
    }

    #[derive(Clone, Copy)]
    enum WritePlan {
        Ok,
        CommitUnknown,
    }

    struct FakeStore {
        log: Arc<Mutex<Vec<String>>>,
        persist_plans: VecDeque<PersistPlan>,
        resolve_plans: VecDeque<CommitResolution>,
        first_claims: VecDeque<Option<ClaimedOutbox>>,
        recovery_claims: VecDeque<Option<ClaimedOutbox>>,
        completion_claims: VecDeque<Option<ClaimedOutbox>>,
        unresolved: Vec<UnresolvedOutbox>,
        unknown_plans: VecDeque<WritePlan>,
        broker_event_plans: VecDeque<WritePlan>,
        finalization_plans: VecDeque<WritePlan>,
        unknown_writes: usize,
        broker_event_writes: Vec<(BrokerEvent, Value, Vec<BrokerFill>)>,
        durable_fills: BTreeMap<String, BrokerFill>,
        finalizations: usize,
    }

    impl FakeStore {
        fn new(log: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                log,
                persist_plans: VecDeque::from([PersistPlan::Ok]),
                resolve_plans: VecDeque::new(),
                first_claims: VecDeque::new(),
                recovery_claims: VecDeque::new(),
                completion_claims: VecDeque::new(),
                unresolved: Vec::new(),
                unknown_plans: VecDeque::new(),
                broker_event_plans: VecDeque::new(),
                finalization_plans: VecDeque::new(),
                unknown_writes: 0,
                broker_event_writes: Vec::new(),
                durable_fills: BTreeMap::new(),
                finalizations: 0,
            }
        }
    }

    #[async_trait]
    impl SubmissionStorePort for FakeStore {
        async fn persist_execution_chain(
            &mut self,
            chain: &DurableExecutionChain<'_>,
        ) -> Result<PersistedExecutionChain, SubmissionStoreError> {
            self.log.lock().unwrap().push("persist".into());
            let expected = ExpectedExecutionChain::new(chain).unwrap();
            match self.persist_plans.pop_front().unwrap_or(PersistPlan::Ok) {
                PersistPlan::Ok => Ok(expected.persisted),
                PersistPlan::CommitUnknown => Err(SubmissionStoreError::CommitUnknown {
                    operation: "commit_execution_chain",
                    recovery: Box::new(expected.recovery),
                }),
            }
        }

        async fn claim_first_dispatch(
            &mut self,
            _outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError> {
            self.log.lock().unwrap().push("claim_first".into());
            Ok(self.first_claims.pop_front().unwrap_or(None))
        }

        async fn claim_recovery(
            &mut self,
            _outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError> {
            self.log.lock().unwrap().push("claim_recovery".into());
            Ok(self.recovery_claims.pop_front().unwrap_or(None))
        }

        async fn claim_terminal_completion(
            &mut self,
            _outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedOutbox>, SubmissionStoreError> {
            self.log.lock().unwrap().push("claim_completion".into());
            Ok(self.completion_claims.pop_front().unwrap_or(None))
        }

        async fn discover_unresolved_outboxes(
            &mut self,
            _lease: &FencedLease,
            _limit: u16,
        ) -> Result<Vec<UnresolvedOutbox>, SubmissionStoreError> {
            self.log.lock().unwrap().push("discover".into());
            Ok(self.unresolved.clone())
        }

        async fn append_submission_unknown(
            &mut self,
            outbox_id: Uuid,
            lease: &FencedLease,
            reason_code: &str,
            detail: &Value,
            occurred_at: DateTime<Utc>,
        ) -> Result<bool, SubmissionStoreError> {
            self.log.lock().unwrap().push("append_unknown".into());
            self.unknown_writes += 1;
            match self.unknown_plans.pop_front().unwrap_or(WritePlan::Ok) {
                WritePlan::Ok => Ok(true),
                WritePlan::CommitUnknown => {
                    let occurred_at = postgres_timestamp(occurred_at).unwrap();
                    Err(SubmissionStoreError::CommitUnknown {
                        operation: "commit_append_submission_unknown",
                        recovery: Box::new(CommitRecoveryKey::SubmissionUnknown {
                            outbox_id,
                            state_event_id: stable_child_uuid(
                                outbox_id,
                                "state:submission-unknown",
                            ),
                            evidence_hash: submission_unknown_hash(
                                reason_code,
                                detail,
                                lease,
                                occurred_at,
                            )
                            .unwrap(),
                        }),
                    })
                }
            }
        }

        async fn record_broker_event(
            &mut self,
            write: &BrokerEventWrite<'_>,
        ) -> Result<BrokerWriteResult, SubmissionStoreError> {
            self.log.lock().unwrap().push("record_event".into());
            for fill in write.fills {
                if let Some(existing) = self.durable_fills.get(&fill.fill_id) {
                    if existing.quantity != fill.quantity
                        || existing.price != fill.price
                        || existing.fee != fill.fee
                        || existing.executed_at != fill.executed_at
                        || existing.activity_evidence_hash != fill.activity_evidence_hash
                    {
                        return Err(SubmissionStoreError::Store(
                            "stable fill identity changed economics".into(),
                        ));
                    }
                } else {
                    self.durable_fills
                        .insert(fill.fill_id.clone(), fill.clone());
                }
            }
            let durable_quantity = self
                .durable_fills
                .values()
                .try_fold(0u64, |total, fill| total.checked_add(fill.quantity.get()))
                .ok_or_else(|| SubmissionStoreError::Store("fill sum overflowed".into()))?;
            if durable_quantity != write.event.filled_quantity.get() {
                return Err(SubmissionStoreError::Store(
                    "durable fill sum differs from cumulative broker truth".into(),
                ));
            }
            self.broker_event_writes.push((
                write.event.clone(),
                write.raw_payload.clone(),
                write.fills.to_vec(),
            ));
            let provider_order_id = write.event.provider_order_id.as_deref().unwrap();
            let broker_event_id = stable_named_uuid(&format!(
                "broker-event:{provider_order_id}:{}",
                write.event.raw_payload_hash
            ));
            match self.broker_event_plans.pop_front().unwrap_or(WritePlan::Ok) {
                WritePlan::Ok => Ok(BrokerWriteResult {
                    broker_event_id,
                    duplicate: false,
                }),
                WritePlan::CommitUnknown => Err(SubmissionStoreError::CommitUnknown {
                    operation: "commit_broker_event",
                    recovery: Box::new(CommitRecoveryKey::BrokerEvent {
                        broker_event_id,
                        raw_payload_hash: write.event.raw_payload_hash,
                        cumulative_filled_quantity: write.event.filled_quantity,
                    }),
                }),
            }
        }

        async fn finalize_outbox(
            &mut self,
            outbox_id: Uuid,
            _lease: &FencedLease,
            completion_reason: &str,
        ) -> Result<bool, SubmissionStoreError> {
            self.log.lock().unwrap().push("finalize".into());
            self.finalizations += 1;
            match self.finalization_plans.pop_front().unwrap_or(WritePlan::Ok) {
                WritePlan::Ok => Ok(true),
                WritePlan::CommitUnknown => Err(SubmissionStoreError::CommitUnknown {
                    operation: "commit_finalize_order_outbox",
                    recovery: Box::new(CommitRecoveryKey::OutboxFinalization {
                        outbox_id,
                        completion_reason: completion_reason.to_owned(),
                    }),
                }),
            }
        }

        async fn resolve_commit(
            &mut self,
            _key: &CommitRecoveryKey,
        ) -> Result<CommitResolution, SubmissionStoreError> {
            self.log.lock().unwrap().push("resolve".into());
            Ok(self.resolve_plans.pop_front().unwrap())
        }
    }

    struct Fixture {
        release: StrategyRelease,
        permit: ActivationPermit,
        snapshot: DecisionSnapshot,
        target: TargetPortfolio,
        risk: RiskDecision,
        plan: OrderPlan,
        intent: OrderIntent,
        lease: FencedLease,
        session_permit: RegularTradingSessionPermit,
        now: DateTime<Utc>,
    }

    impl Fixture {
        fn new() -> Self {
            let now: DateTime<Utc> = "2026-07-20T14:00:00Z".parse().unwrap();
            let strategy = StrategySpec::MomentumTrend(MomentumTrendSpec {
                momentum_lookback_sessions: 63,
                trend_lookback_sessions: 126,
                cadence: RebalanceCadence::Weekly,
            });
            let release_id = Uuid::new_v4().to_string();
            let decision_id = Uuid::new_v4().to_string();
            let intent_id = Uuid::new_v4().to_string();
            let plan_id = Uuid::new_v4().to_string();
            let account_fingerprint = HashDigest::sha256("paper-account");
            let release = StrategyRelease {
                release_id: release_id.clone(),
                code_hash: HashDigest::sha256("code"),
                parameters_hash: HashDigest::of_json(&strategy).unwrap(),
                universe: ["SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "MDY"]
                    .into_iter()
                    .map(|value| Symbol::new(value).unwrap())
                    .collect(),
                data_hash: HashDigest::sha256("data"),
                cost_model_hash: HashDigest::sha256("cost"),
                statistical_certificate_hash: HashDigest::sha256("certificate"),
                strategy,
                valid_from: now - chrono::Duration::days(1),
                expires_at: now + chrono::Duration::days(1),
            };
            let account = AccountSnapshot {
                account_fingerprint,
                status: AccountStatus::Active,
                trading_blocked: false,
                cash: "1000".parse::<Money>().unwrap(),
                buying_power: "1000".parse::<Money>().unwrap(),
                equity: "1000".parse::<Money>().unwrap(),
                day_pnl: Money::ZERO,
                drawdown: Money::ZERO,
                positions: Vec::new(),
            };
            let snapshot = DecisionSnapshot {
                decision_id: decision_id.clone(),
                release_id: release_id.clone(),
                as_of: now - chrono::Duration::seconds(3),
                market_session: now.date_naive(),
                schedule: DecisionSchedule {
                    eligible_cadences: vec![RebalanceCadence::Weekly],
                    calendar_evidence_hash: HashDigest::sha256("calendar"),
                },
                account,
                account_snapshot_hash: HashDigest::sha256("account"),
                observations: Vec::new(),
                input_data_hash: HashDigest::sha256("input"),
            };
            let limits = RiskLimitSnapshot {
                max_gross_exposure: "1000".parse::<Money>().unwrap(),
                max_position_weight: "1".parse::<Fixed>().unwrap(),
                max_positions: 1,
                max_order_notional: "1000".parse::<Money>().unwrap(),
                max_planned_loss: "10".parse::<Money>().unwrap(),
                daily_loss_limit: "10".parse::<Money>().unwrap(),
                hard_drawdown_limit: "20".parse::<Money>().unwrap(),
                planned_stop_distance_bps: 100,
                marketable_limit_band_bps: 10,
                new_positions_enabled: true,
            };
            let target = TargetPortfolio {
                decision_id: decision_id.clone(),
                release_id: release_id.clone(),
                generated_at: snapshot.as_of,
                positions: Vec::new(),
                cash_target: true,
                reason_codes: vec!["TEST".into()],
            };
            let risk = RiskDecision {
                decision_id: decision_id.clone(),
                disposition: RiskDisposition::Approved,
                approved_positions: Vec::new(),
                limits: limits.clone(),
                reason_codes: vec!["TEST".into()],
            };
            let plan = OrderPlan {
                plan_id,
                release_id: release_id.clone(),
                decision_id: decision_id.clone(),
                symbol: Symbol::new("SPY").unwrap(),
                side: OrderSide::Buy,
                quantity: WholeQuantity::new(1),
                decision_reference_price: "10".parse::<Price>().unwrap(),
                decision_evidence_hash: HashDigest::sha256("decision"),
                created_at: now - chrono::Duration::seconds(2),
            };
            let intent = OrderIntent {
                intent_id,
                client_order_id: "wasp2-stable-client-1".into(),
                release_id: release_id.clone(),
                decision_id,
                symbol: Symbol::new("SPY").unwrap(),
                side: OrderSide::Buy,
                quantity: WholeQuantity::new(1),
                limit_price: "10".parse::<Price>().unwrap(),
                decision_at: now - chrono::Duration::seconds(3),
                arrival_quote: "10".parse::<Price>().unwrap(),
                quote_provider_at: now - chrono::Duration::seconds(1),
                quote_received_at: now - chrono::Duration::seconds(1),
                quote_valid_until: now + chrono::Duration::seconds(10),
                quote_payload_hash: HashDigest::sha256("quote"),
                time_in_force: TimeInForce::Day,
                decision_evidence_hash: HashDigest::sha256("decision"),
                materialization_evidence_hash: HashDigest::sha256("materialization"),
                created_at: now - chrono::Duration::seconds(1),
            };
            let permit = ActivationPermit {
                permit_id: Uuid::new_v4().to_string(),
                environment: Environment::Paper,
                account_fingerprint,
                strategy_release_id: release_id,
                strategy_release_hash: release.release_hash().unwrap(),
                max_gross_notional: "1000".parse::<Money>().unwrap(),
                max_position_notional: "1000".parse::<Money>().unwrap(),
                max_daily_loss: "10".parse::<Money>().unwrap(),
                max_drawdown: "20".parse::<Money>().unwrap(),
                risk_limits_hash: HashDigest::of_json(&limits).unwrap(),
                issued_at: now - chrono::Duration::minutes(1),
                expires_at: now + chrono::Duration::minutes(1),
                operator_subject: "operator".into(),
                approval_digest: HashDigest::sha256("approval"),
            };
            let lease = FencedLease {
                environment: Environment::Paper,
                account_fingerprint,
                owner_id: Uuid::new_v4(),
                fencing_token: 7,
                lease_until: now + chrono::Duration::seconds(30),
            };
            let session_permit = RegularTradingSessionPermit::verified(
                "NYSE".into(),
                now.date_naive(),
                now - chrono::Duration::hours(1),
                now + chrono::Duration::hours(5),
                now - chrono::Duration::seconds(1),
                now,
                HashDigest::sha256("clock"),
                HashDigest::sha256("calendar"),
                Some("clock-request".into()),
                Some("calendar-request".into()),
            )
            .unwrap();
            Self {
                release,
                permit,
                snapshot,
                target,
                risk,
                plan,
                intent,
                lease,
                session_permit,
                now,
            }
        }

        fn chain(&self) -> DurableExecutionChain<'_> {
            DurableExecutionChain {
                release: &self.release,
                activation_permit: &self.permit,
                snapshot: &self.snapshot,
                target: &self.target,
                risk: &self.risk,
                plan: &self.plan,
                intent: &self.intent,
                lease: &self.lease,
            }
        }

        fn claim(&self, kind: OutboxClaimKind, attempt_count: u32) -> ClaimedOutbox {
            let chain = self.chain();
            let expected = ExpectedExecutionChain::new(&chain).unwrap();
            ClaimedOutbox {
                kind,
                outbox_id: expected.persisted.outbox_id,
                intent_id: Uuid::parse_str(&self.intent.intent_id).unwrap(),
                environment: Environment::Paper,
                account_fingerprint: self.lease.account_fingerprint,
                created_fencing_token: self.lease.fencing_token,
                claim_fencing_token: self.lease.fencing_token,
                payload: serde_json::to_value(&self.intent).unwrap(),
                available_at: self.now - chrono::Duration::seconds(1),
                claimed_by: self.lease.owner_id,
                claimed_at: self.now,
                attempt_count,
            }
        }

        fn unresolved(&self, state: &str) -> UnresolvedOutbox {
            let intent_id = Uuid::parse_str(&self.intent.intent_id).unwrap();
            UnresolvedOutbox {
                outbox_id: stable_child_uuid(intent_id, "outbox:intent-committed"),
                intent_id,
                created_fencing_token: self.lease.fencing_token,
                payload: serde_json::to_value(&self.intent).unwrap(),
                available_at: self.now - chrono::Duration::seconds(1),
                current_state: state.into(),
            }
        }
    }

    #[tokio::test]
    async fn paper_and_lease_deadline_gate_before_persistence_or_post() {
        for invalidate in ["live", "short_lease"] {
            let mut fixture = Fixture::new();
            if invalidate == "live" {
                fixture.lease.environment = Environment::Live;
            } else {
                fixture.lease.lease_until = fixture.now + chrono::Duration::seconds(5);
            }
            let log = Arc::new(Mutex::new(Vec::new()));
            let broker = FakeBroker::new(log.clone(), fixture.now, [], []);
            let posts = broker.posts.clone();
            let coordinator = DurableSubmissionCoordinator::new(broker);
            let mut store = FakeStore::new(log);
            assert!(coordinator
                .persist_and_dispatch(
                    &mut store,
                    &fixture.chain(),
                    &fixture.session_permit,
                    fixture.now,
                )
                .await
                .is_err());
            assert_eq!(*posts.lock().unwrap(), 0);
            assert!(store.persist_plans.len() == 1);
        }
    }

    #[tokio::test]
    async fn commit_claim_post_and_raw_event_persistence_are_ordered() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [SubmitPlan::Observed("accepted", 0)],
            [],
        );
        let posts = broker.posts.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log.clone());
        store
            .first_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));

        let result = coordinator
            .persist_and_dispatch(
                &mut store,
                &fixture.chain(),
                &fixture.session_permit,
                fixture.now,
            )
            .await
            .unwrap();

        assert!(matches!(
            result,
            SubmissionProgress::BrokerStatePersisted { .. }
        ));
        assert_eq!(*posts.lock().unwrap(), 1);
        assert_eq!(store.broker_event_writes.len(), 1);
        assert_eq!(store.broker_event_writes[0].1["status"], "accepted");
        let log = log.lock().unwrap();
        let persist = log.iter().position(|entry| entry == "persist").unwrap();
        let claim = log.iter().position(|entry| entry == "claim_first").unwrap();
        let post = log.iter().position(|entry| entry == "post").unwrap();
        let record = log
            .iter()
            .position(|entry| entry == "record_event")
            .unwrap();
        assert!(persist < claim && claim < post && post < record);
    }

    #[tokio::test]
    async fn chain_commit_unknown_resolves_or_retries_database_without_second_post() {
        for resolution in [CommitResolution::Committed, CommitResolution::NotCommitted] {
            let fixture = Fixture::new();
            let log = Arc::new(Mutex::new(Vec::new()));
            let broker = FakeBroker::new(
                log.clone(),
                fixture.now,
                [SubmitPlan::Observed("accepted", 0)],
                [],
            );
            let posts = broker.posts.clone();
            let coordinator = DurableSubmissionCoordinator::new(broker);
            let mut store = FakeStore::new(log.clone());
            store.persist_plans = if resolution == CommitResolution::Committed {
                VecDeque::from([PersistPlan::CommitUnknown])
            } else {
                VecDeque::from([PersistPlan::CommitUnknown, PersistPlan::Ok])
            };
            store.resolve_plans.push_back(resolution);
            store
                .first_claims
                .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));

            coordinator
                .persist_and_dispatch(
                    &mut store,
                    &fixture.chain(),
                    &fixture.session_permit,
                    fixture.now,
                )
                .await
                .unwrap();
            assert_eq!(*posts.lock().unwrap(), 1);
            let persists = log
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry.as_str() == "persist")
                .count();
            assert_eq!(
                persists,
                if resolution == CommitResolution::Committed {
                    1
                } else {
                    2
                }
            );
        }
    }

    #[tokio::test]
    async fn missing_or_wrong_first_claim_never_posts() {
        for wrong_claim in [false, true] {
            let fixture = Fixture::new();
            let log = Arc::new(Mutex::new(Vec::new()));
            let broker = FakeBroker::new(log.clone(), fixture.now, [], []);
            let posts = broker.posts.clone();
            let coordinator = DurableSubmissionCoordinator::new(broker);
            let mut store = FakeStore::new(log);
            if wrong_claim {
                let mut claim = fixture.claim(OutboxClaimKind::FirstDispatch, 1);
                claim.claim_fencing_token += 1;
                store.first_claims.push_back(Some(claim));
            } else {
                store.first_claims.push_back(None);
            }
            let result = coordinator
                .persist_and_dispatch(
                    &mut store,
                    &fixture.chain(),
                    &fixture.session_permit,
                    fixture.now,
                )
                .await;
            if wrong_claim {
                assert!(result.is_err());
            } else {
                assert!(matches!(
                    result.unwrap(),
                    SubmissionProgress::RecoveryRequired { .. }
                ));
            }
            assert_eq!(*posts.lock().unwrap(), 0);
        }
    }

    #[tokio::test]
    async fn nonzero_cumulative_fill_persists_stable_activity_evidence() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [SubmitPlan::Observed("partially_filled", 1)],
            [],
        );
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store
            .first_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));

        let result = coordinator
            .persist_and_dispatch(
                &mut store,
                &fixture.chain(),
                &fixture.session_permit,
                fixture.now,
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            SubmissionProgress::BrokerStatePersisted { .. }
        ));
        assert_eq!(store.broker_event_writes.len(), 1);
        assert_eq!(store.broker_event_writes[0].2.len(), 1);
        assert_eq!(store.broker_event_writes[0].2[0].fill_id, "fill-activity-1");
        assert_eq!(store.finalizations, 0);
    }

    #[tokio::test]
    async fn partial_then_terminal_reobservation_is_idempotent_and_finalizes_once() {
        let mut fixture = Fixture::new();
        fixture.plan.quantity = WholeQuantity::new(2);
        fixture.intent.quantity = WholeQuantity::new(2);
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(log.clone(), fixture.now, [], []);
        let observed_broker = broker.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        let outbox_id = fixture
            .claim(OutboxClaimKind::RecoveryLookupOnly, 2)
            .outbox_id;
        store.completion_claims.push_back(Some(
            fixture.claim(OutboxClaimKind::TerminalCompletionOnly, 3),
        ));

        let partial = coordinator
            .persist_observed(
                &mut store,
                &fixture.lease,
                outbox_id,
                &fixture.intent,
                observed_broker.observed(&fixture.intent, "partially_filled", 1),
            )
            .await
            .unwrap();
        assert!(matches!(
            partial,
            SubmissionProgress::BrokerStatePersisted { .. }
        ));

        let terminal = coordinator
            .persist_observed(
                &mut store,
                &fixture.lease,
                outbox_id,
                &fixture.intent,
                observed_broker.observed(&fixture.intent, "filled", 2),
            )
            .await
            .unwrap();
        assert!(matches!(
            terminal,
            SubmissionProgress::TerminalFinalized { .. }
        ));

        assert_eq!(store.durable_fills.len(), 2);
        assert_eq!(store.broker_event_writes.len(), 2);
        let first_a = &store.broker_event_writes[0].2[0];
        let repeated_a = &store.broker_event_writes[1].2[0];
        assert_eq!(
            first_a.activity_evidence_hash,
            repeated_a.activity_evidence_hash
        );
        assert_ne!(first_a.raw_payload_hash, repeated_a.raw_payload_hash);
        assert_ne!(first_a.received_at, repeated_a.received_at);
        assert_eq!(
            store.durable_fills["fill-activity-1"].received_at,
            first_a.received_at
        );
        assert_eq!(store.finalizations, 1);
    }

    #[tokio::test]
    async fn ambiguous_and_before_send_outcomes_are_durable_and_never_reposted() {
        for plan in [SubmitPlan::Unknown, SubmitPlan::NotDispatched] {
            let fixture = Fixture::new();
            let log = Arc::new(Mutex::new(Vec::new()));
            let broker = FakeBroker::new(log.clone(), fixture.now, [plan], []);
            let posts = broker.posts.clone();
            let coordinator = DurableSubmissionCoordinator::new(broker);
            let mut store = FakeStore::new(log);
            store
                .first_claims
                .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));
            store.first_claims.push_back(None);

            assert!(matches!(
                coordinator
                    .persist_and_dispatch(
                        &mut store,
                        &fixture.chain(),
                        &fixture.session_permit,
                        fixture.now,
                    )
                    .await
                    .unwrap(),
                SubmissionProgress::SubmissionUnknown { .. }
            ));
            assert!(matches!(
                coordinator
                    .persist_and_dispatch(
                        &mut store,
                        &fixture.chain(),
                        &fixture.session_permit,
                        fixture.now,
                    )
                    .await
                    .unwrap(),
                SubmissionProgress::RecoveryRequired { .. }
            ));
            assert_eq!(*posts.lock().unwrap(), 1);
            assert_eq!(store.unknown_writes, 1);
        }
    }

    #[tokio::test]
    async fn post_dispatch_restart_is_get_only_and_persists_observed_state() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [],
            [LookupPlan::Observed("accepted", 0)],
        );
        let posts = broker.posts.clone();
        let gets = broker.gets.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store.unresolved = vec![fixture.unresolved("submission_unknown")];
        store
            .recovery_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::RecoveryLookupOnly, 2)));

        let results = coordinator
            .recover_unresolved(&mut store, &fixture.lease, 10, fixture.now)
            .await
            .unwrap();
        assert!(matches!(
            results.as_slice(),
            [SubmissionProgress::BrokerStatePersisted { .. }]
        ));
        assert_eq!(*posts.lock().unwrap(), 0);
        assert_eq!(*gets.lock().unwrap(), 1);
        assert_eq!(store.broker_event_writes.len(), 1);
    }

    #[tokio::test]
    async fn terminal_zero_fill_is_recorded_then_completed() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [SubmitPlan::Observed("rejected", 0)],
            [],
        );
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log.clone());
        store
            .first_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));
        store.completion_claims.push_back(Some(
            fixture.claim(OutboxClaimKind::TerminalCompletionOnly, 2),
        ));

        let result = coordinator
            .persist_and_dispatch(
                &mut store,
                &fixture.chain(),
                &fixture.session_permit,
                fixture.now,
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            SubmissionProgress::TerminalFinalized { .. }
        ));
        assert_eq!(store.broker_event_writes.len(), 1);
        assert_eq!(store.finalizations, 1);
        let log = log.lock().unwrap();
        assert!(
            log.iter().position(|entry| entry == "record_event")
                < log.iter().position(|entry| entry == "claim_completion")
        );
        assert!(
            log.iter().position(|entry| entry == "claim_completion")
                < log.iter().position(|entry| entry == "finalize")
        );
    }

    #[tokio::test]
    async fn lookup_none_and_error_remain_unresolved_without_post() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [],
            [LookupPlan::None, LookupPlan::Error],
        );
        let posts = broker.posts.clone();
        let gets = broker.gets.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store.unresolved = vec![
            fixture.unresolved("dispatch_started"),
            fixture.unresolved("submission_unknown"),
        ];
        store
            .recovery_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::RecoveryLookupOnly, 2)));
        store
            .recovery_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::RecoveryLookupOnly, 3)));

        let results = coordinator
            .recover_unresolved(&mut store, &fixture.lease, 10, fixture.now)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .all(|result| matches!(result, SubmissionProgress::LookupStillUnresolved { .. })));
        assert_eq!(*posts.lock().unwrap(), 0);
        assert_eq!(*gets.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn broker_event_and_finalization_commit_unknown_resolve_without_more_io() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [SubmitPlan::Observed("rejected", 0)],
            [],
        );
        let posts = broker.posts.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store
            .first_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));
        store.completion_claims.push_back(Some(
            fixture.claim(OutboxClaimKind::TerminalCompletionOnly, 2),
        ));
        store.broker_event_plans.push_back(WritePlan::CommitUnknown);
        store.finalization_plans.push_back(WritePlan::CommitUnknown);
        store.resolve_plans =
            VecDeque::from([CommitResolution::Committed, CommitResolution::Committed]);

        assert!(matches!(
            coordinator
                .persist_and_dispatch(
                    &mut store,
                    &fixture.chain(),
                    &fixture.session_permit,
                    fixture.now,
                )
                .await
                .unwrap(),
            SubmissionProgress::TerminalFinalized { .. }
        ));
        assert_eq!(*posts.lock().unwrap(), 1);
        assert_eq!(store.broker_event_writes.len(), 1);
        assert_eq!(store.finalizations, 1);
    }

    #[tokio::test]
    async fn conflicting_chain_commit_evidence_fails_before_claim_or_post() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(log.clone(), fixture.now, [], []);
        let posts = broker.posts.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store.persist_plans = VecDeque::from([PersistPlan::CommitUnknown]);
        store
            .resolve_plans
            .push_back(CommitResolution::ConflictingEvidence);

        assert!(coordinator
            .persist_and_dispatch(
                &mut store,
                &fixture.chain(),
                &fixture.session_permit,
                fixture.now,
            )
            .await
            .is_err());
        assert_eq!(*posts.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn unknown_state_commit_uncertainty_resolves_without_reposting() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(log.clone(), fixture.now, [SubmitPlan::Unknown], []);
        let posts = broker.posts.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store
            .first_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::FirstDispatch, 1)));
        store.unknown_plans.push_back(WritePlan::CommitUnknown);
        store.resolve_plans.push_back(CommitResolution::Committed);

        assert!(matches!(
            coordinator
                .persist_and_dispatch(
                    &mut store,
                    &fixture.chain(),
                    &fixture.session_permit,
                    fixture.now,
                )
                .await
                .unwrap(),
            SubmissionProgress::SubmissionUnknown { .. }
        ));
        assert_eq!(*posts.lock().unwrap(), 1);
        assert_eq!(store.unknown_writes, 1);
    }

    #[tokio::test]
    async fn eligible_and_already_terminal_recovery_perform_no_broker_io() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(log.clone(), fixture.now, [], []);
        let posts = broker.posts.clone();
        let gets = broker.gets.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store.unresolved = vec![
            fixture.unresolved("eligible"),
            fixture.unresolved("terminal"),
        ];
        store.completion_claims.push_back(Some(
            fixture.claim(OutboxClaimKind::TerminalCompletionOnly, 2),
        ));

        let results = coordinator
            .recover_unresolved(&mut store, &fixture.lease, 10, fixture.now)
            .await
            .unwrap();
        assert!(matches!(
            results.as_slice(),
            [
                SubmissionProgress::RecoveryRequired { .. },
                SubmissionProgress::TerminalFinalized { .. }
            ]
        ));
        assert_eq!(*posts.lock().unwrap(), 0);
        assert_eq!(*gets.lock().unwrap(), 0);
        assert_eq!(store.finalizations, 1);
    }

    #[tokio::test]
    async fn recovered_nonzero_fill_uses_rest_activity_and_never_reposts() {
        let fixture = Fixture::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let broker = FakeBroker::new(
            log.clone(),
            fixture.now,
            [],
            [LookupPlan::Observed("partially_filled", 1)],
        );
        let posts = broker.posts.clone();
        let coordinator = DurableSubmissionCoordinator::new(broker);
        let mut store = FakeStore::new(log);
        store.unresolved = vec![fixture.unresolved("submission_unknown")];
        store
            .recovery_claims
            .push_back(Some(fixture.claim(OutboxClaimKind::RecoveryLookupOnly, 2)));

        let results = coordinator
            .recover_unresolved(&mut store, &fixture.lease, 10, fixture.now)
            .await
            .unwrap();
        assert!(matches!(
            results.as_slice(),
            [SubmissionProgress::BrokerStatePersisted { .. }]
        ));
        assert_eq!(*posts.lock().unwrap(), 0);
        assert_eq!(store.broker_event_writes.len(), 1);
        assert_eq!(store.broker_event_writes[0].2.len(), 1);
        assert_eq!(store.finalizations, 0);
    }
}
