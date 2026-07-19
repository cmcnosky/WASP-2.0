use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trader_core::{BrokerEvent, OrderIntent};
use uuid::Uuid;

use crate::{
    authority::{AuthorityDecision, RecoveryAuthorityDecision},
    ledger::{ExecutionEvent, ExecutionLedger, OutboxAuthority},
    port::{
        BrokerPort, CancellationNotDispatched, CancellationOutcome, RegularTradingSessionPermit,
        SubmissionOutcome,
    },
    store::{
        CancelIntentWrite, ClaimedCancelOutbox, CommitRecoveryKey, CommitResolution,
        ExecutionStore, FencedLease, PersistedCancelIntent, StoreError, UnresolvedCancelOutbox,
    },
    ExecutionError,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DispatchResult {
    Observed {
        outbox_sequence: u64,
        client_order_id: String,
    },
    SubmissionUnknown {
        outbox_sequence: u64,
        client_order_id: String,
    },
    LookupStillUnresolved {
        outbox_sequence: u64,
        client_order_id: String,
    },
}

/// Cancellation-specific persistence errors preserve deterministic recovery
/// keys while hiding the database transport implementation from testable
/// orchestration.
#[derive(Debug, Error)]
pub enum CancellationStoreError {
    #[error("durable cancellation commit for {operation} is unknown")]
    CommitUnknown {
        operation: &'static str,
        recovery: Box<CommitRecoveryKey>,
    },
    #[error("durable cancellation store failed: {0}")]
    Store(String),
}

impl From<StoreError> for CancellationStoreError {
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

/// Narrow durable port used by the cancellation coordinator. Every production
/// `ExecutionStore` implements it, while fault tests can model exact crash and
/// commit-unknown boundaries without a database transport.
#[async_trait]
pub trait CancellationStorePort: Send {
    async fn persist_cancel_intent(
        &mut self,
        write: &CancelIntentWrite<'_>,
    ) -> Result<PersistedCancelIntent, CancellationStoreError>;
    async fn claim_cancel_dispatch(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError>;
    async fn claim_cancel_retry(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError>;
    async fn claim_cancel_recovery(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError>;
    async fn claim_cancel_terminal_completion(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError>;
    async fn record_cancel_request_accepted(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        accepted: &crate::port::CancellationRequestAccepted,
    ) -> Result<bool, CancellationStoreError>;
    async fn record_cancel_unknown(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        detail: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, CancellationStoreError>;
    async fn record_cancel_not_dispatched(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        expected_attempt_count: u32,
        evidence: &CancellationNotDispatched,
    ) -> Result<bool, CancellationStoreError>;
    async fn finalize_cancel_outbox(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        terminal_broker_event_id: Uuid,
        completion_reason: &str,
    ) -> Result<bool, CancellationStoreError>;
    async fn discover_unresolved_cancels(
        &mut self,
        lease: &FencedLease,
        limit: u16,
    ) -> Result<Vec<UnresolvedCancelOutbox>, CancellationStoreError>;
    async fn resolve_commit(
        &mut self,
        key: &CommitRecoveryKey,
    ) -> Result<CommitResolution, CancellationStoreError>;
}

#[async_trait]
impl<T: ExecutionStore + Send> CancellationStorePort for T {
    async fn persist_cancel_intent(
        &mut self,
        write: &CancelIntentWrite<'_>,
    ) -> Result<PersistedCancelIntent, CancellationStoreError> {
        ExecutionStore::persist_cancel_intent(self, write)
            .await
            .map_err(Into::into)
    }

    async fn claim_cancel_dispatch(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
        ExecutionStore::claim_cancel_dispatch(self, cancel_outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn claim_cancel_retry(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
        ExecutionStore::claim_cancel_retry(self, cancel_outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn claim_cancel_recovery(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
        ExecutionStore::claim_cancel_recovery(self, cancel_outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn claim_cancel_terminal_completion(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
    ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
        ExecutionStore::claim_cancel_terminal_completion(self, cancel_outbox_id, lease)
            .await
            .map_err(Into::into)
    }

    async fn record_cancel_request_accepted(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        accepted: &crate::port::CancellationRequestAccepted,
    ) -> Result<bool, CancellationStoreError> {
        ExecutionStore::record_cancel_request_accepted(self, cancel_outbox_id, lease, accepted)
            .await
            .map_err(Into::into)
    }

    async fn record_cancel_unknown(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        detail: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, CancellationStoreError> {
        ExecutionStore::record_cancel_unknown(self, cancel_outbox_id, lease, detail, occurred_at)
            .await
            .map_err(Into::into)
    }

    async fn record_cancel_not_dispatched(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        expected_attempt_count: u32,
        evidence: &CancellationNotDispatched,
    ) -> Result<bool, CancellationStoreError> {
        ExecutionStore::record_cancel_not_dispatched(
            self,
            cancel_outbox_id,
            lease,
            expected_attempt_count,
            evidence,
        )
        .await
        .map_err(Into::into)
    }

    async fn finalize_cancel_outbox(
        &mut self,
        cancel_outbox_id: Uuid,
        lease: &FencedLease,
        terminal_broker_event_id: Uuid,
        completion_reason: &str,
    ) -> Result<bool, CancellationStoreError> {
        ExecutionStore::finalize_cancel_outbox(
            self,
            cancel_outbox_id,
            lease,
            terminal_broker_event_id,
            completion_reason,
        )
        .await
        .map_err(Into::into)
    }

    async fn discover_unresolved_cancels(
        &mut self,
        lease: &FencedLease,
        limit: u16,
    ) -> Result<Vec<UnresolvedCancelOutbox>, CancellationStoreError> {
        ExecutionStore::discover_unresolved_cancels(self, lease, limit)
            .await
            .map_err(Into::into)
    }

    async fn resolve_commit(
        &mut self,
        key: &CommitRecoveryKey,
    ) -> Result<CommitResolution, CancellationStoreError> {
        ExecutionStore::resolve_commit(self, key)
            .await
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancellationProgress {
    RetryEligible {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        provider_order_id: String,
        completed_attempt_count: u32,
        evidence_hash: trader_core::HashDigest,
    },
    RequestAccepted {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        provider_order_id: String,
        request_id: String,
    },
    OutcomeUnknown {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        provider_order_id: String,
        detail: String,
    },
    RecoveryRequired {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        provider_order_id: String,
    },
    LookupStillUnresolved {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        provider_order_id: String,
        detail: String,
    },
    BrokerStateObserved {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        terminal: bool,
        event: BrokerEvent,
    },
    TerminalFinalized {
        cancel_intent_id: Uuid,
        cancel_outbox_id: Uuid,
        terminal_broker_event_id: Uuid,
        provider_status: String,
    },
}

/// Fenced executor orchestration. It contains no automatic retry path for an
/// ambiguous POST: recovery is exclusively a GET by stable client_order_id.
pub struct Executor<B> {
    broker: B,
}

impl<B: BrokerPort> Executor<B> {
    pub fn new(broker: B) -> Self {
        Self { broker }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_and_dispatch(
        &self,
        ledger: &mut ExecutionLedger,
        authority_decision: &AuthorityDecision,
        outbox_authority: &OutboxAuthority,
        owner_id: &str,
        claim_fencing_token: u64,
        intent: OrderIntent,
        session_permit: &RegularTradingSessionPermit,
        now: DateTime<Utc>,
    ) -> Result<DispatchResult, ExecutionError> {
        if outbox_authority.created_fencing_token != claim_fencing_token
            || !authority_decision.permits_submission(
                now,
                outbox_authority.environment,
                &outbox_authority.account_fingerprint,
                &intent.release_id,
                claim_fencing_token,
            )
        {
            return Err(ExecutionError::AuthorityDenied(
                "dispatch authority does not match environment/account/release/current fence"
                    .into(),
            ));
        }
        if now < intent.quote_received_at || now >= intent.quote_valid_until {
            return Err(ExecutionError::AuthorityDenied(
                "fresh execution quote expired or is not yet observable".into(),
            ));
        }
        let not_after = session_permit.submission_deadline(&intent, now)?;
        self.broker.validate_submission_window(not_after, now)?;
        let sequence = ledger.commit_intent(intent.clone(), outbox_authority, now)?;
        let projected = ledger.project_orders()?;
        let lifecycle = projected.get(&intent.client_order_id).ok_or_else(|| {
            ExecutionError::LedgerInvariant("committed intent projection is missing".into())
        })?;
        if !lifecycle.may_submit() {
            return Err(ExecutionError::SubmissionUnknown(
                "existing intent is not in IntentCommitted; query by client_order_id instead"
                    .into(),
            ));
        }
        ledger.claim_outbox(sequence, owner_id, claim_fencing_token, now)?;
        ledger.append(
            ExecutionEvent::SubmissionStarted {
                client_order_id: intent.client_order_id.clone(),
            },
            now,
        )?;

        match self
            .broker
            .submit_committed_intent(&intent, session_permit, not_after)
            .await
        {
            Ok(SubmissionOutcome::Observed(observed)) => {
                let (event, _raw_response_json) = observed.into_parts();
                if let Err(error) =
                    append_validated_broker_event(ledger, &intent.client_order_id, event, now)
                {
                    // The POST reached the broker but its response cannot prove the
                    // resulting order identity/state. Preserve lookup-only recovery
                    // instead of returning with the lifecycle stuck in-flight.
                    ledger.append(
                        ExecutionEvent::SubmissionUnknown {
                            client_order_id: intent.client_order_id.clone(),
                            detail: bounded_execution_detail(&format!(
                                "post-submit response validation failed: {error}"
                            )),
                        },
                        now,
                    )?;
                    return Ok(DispatchResult::SubmissionUnknown {
                        outbox_sequence: sequence,
                        client_order_id: intent.client_order_id,
                    });
                }
                ledger.mark_outbox_completed(sequence, owner_id, claim_fencing_token, now)?;
                Ok(DispatchResult::Observed {
                    outbox_sequence: sequence,
                    client_order_id: intent.client_order_id,
                })
            }
            Ok(SubmissionOutcome::NotDispatched(evidence)) => {
                // This legacy in-memory path has no durable state capable of
                // authorizing a safe retry. Preserve lookup-only recovery; the
                // PostgreSQL coordinator will record the typed evidence before
                // it may expose a retry transition.
                ledger.append(
                    ExecutionEvent::SubmissionUnknown {
                        client_order_id: intent.client_order_id.clone(),
                        detail: bounded_execution_detail(&format!(
                            "submission was not dispatched but retry is not durably authorized: {}",
                            evidence.detail
                        )),
                    },
                    now,
                )?;
                Ok(DispatchResult::SubmissionUnknown {
                    outbox_sequence: sequence,
                    client_order_id: intent.client_order_id,
                })
            }
            Ok(SubmissionOutcome::Unknown { detail }) | Err(ExecutionError::Broker(detail)) => {
                ledger.append(
                    ExecutionEvent::SubmissionUnknown {
                        client_order_id: intent.client_order_id.clone(),
                        detail: bounded_execution_detail(&detail),
                    },
                    now,
                )?;
                Ok(DispatchResult::SubmissionUnknown {
                    outbox_sequence: sequence,
                    client_order_id: intent.client_order_id,
                })
            }
            Err(error) => {
                // Any adapter error after dispatch started is conservatively ambiguous.
                ledger.append(
                    ExecutionEvent::SubmissionUnknown {
                        client_order_id: intent.client_order_id.clone(),
                        detail: bounded_execution_detail(&error.to_string()),
                    },
                    now,
                )?;
                Ok(DispatchResult::SubmissionUnknown {
                    outbox_sequence: sequence,
                    client_order_id: intent.client_order_id,
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn recover_submission_unknown(
        &self,
        ledger: &mut ExecutionLedger,
        recovery_authority: &RecoveryAuthorityDecision,
        outbox_sequence: u64,
        client_order_id: &str,
        owner_id: &str,
        claim_fencing_token: u64,
        now: DateTime<Utc>,
    ) -> Result<DispatchResult, ExecutionError> {
        let outbox = ledger
            .outbox_message(outbox_sequence)
            .ok_or_else(|| ExecutionError::LedgerInvariant("unknown outbox sequence".into()))?;
        if !recovery_authority.permits_recovery(
            now,
            outbox.environment,
            &outbox.account_fingerprint,
            claim_fencing_token,
        ) {
            return Err(ExecutionError::AuthorityDenied(
                "recovery authority does not match environment/account/current fence".into(),
            ));
        }
        let projected = ledger.project_orders()?;
        let lifecycle = projected.get(client_order_id).ok_or_else(|| {
            ExecutionError::LedgerInvariant("unknown recovery client_order_id".into())
        })?;
        if !lifecycle.requires_client_id_lookup() {
            return Err(ExecutionError::Lifecycle(
                "recovery is only allowed for SUBMISSION_UNKNOWN".into(),
            ));
        }
        ledger.claim_outbox(outbox_sequence, owner_id, claim_fencing_token, now)?;
        match self
            .broker
            .find_order_by_client_id(&lifecycle.intent)
            .await?
        {
            Some(observed) => {
                let (event, _raw_response_json) = observed.into_parts();
                append_validated_broker_event(ledger, client_order_id, event, now)?;
                ledger.mark_outbox_completed(
                    outbox_sequence,
                    owner_id,
                    claim_fencing_token,
                    now,
                )?;
                Ok(DispatchResult::Observed {
                    outbox_sequence,
                    client_order_id: client_order_id.into(),
                })
            }
            None => Ok(DispatchResult::LookupStillUnresolved {
                outbox_sequence,
                client_order_id: client_order_id.into(),
            }),
        }
    }
}

impl<B: BrokerPort> Executor<B> {
    /// Atomically persists cancellation authority, durably claims first
    /// dispatch, and only then permits one broker DELETE. A repeated call that
    /// cannot obtain the first-dispatch claim returns recovery-required and
    /// never resends the DELETE.
    #[allow(clippy::too_many_arguments)]
    pub async fn persist_and_dispatch_cancel<S: CancellationStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        client_order_id: &str,
        provider_order_id: &str,
        reason_code: &str,
        requested_at: DateTime<Utc>,
    ) -> Result<CancellationProgress, ExecutionError> {
        validate_active_cancel_lease(lease, requested_at)?;
        let write = CancelIntentWrite {
            client_order_id,
            provider_order_id,
            reason_code,
            requested_at,
            lease,
        };
        let persisted = persist_cancel_intent_resolving(store, &write).await?;
        let claim = store
            .claim_cancel_dispatch(persisted.cancel_outbox_id, lease)
            .await
            .map_err(cancellation_store_error)?;
        let Some(claim) = claim else {
            return Ok(CancellationProgress::RecoveryRequired {
                cancel_intent_id: persisted.cancel_intent_id,
                cancel_outbox_id: persisted.cancel_outbox_id,
                provider_order_id: provider_order_id.to_owned(),
            });
        };
        validate_cancel_claim(
            &claim,
            lease,
            persisted.cancel_intent_id,
            persisted.cancel_outbox_id,
            client_order_id,
            provider_order_id,
        )?;
        if claim.kind != crate::store::CancelOutboxClaimKind::FirstDispatch {
            return Err(ExecutionError::AuthorityDenied(
                "cancellation DELETE lacks a first-dispatch-only claim".into(),
            ));
        }
        self.dispatch_claimed_cancel(store, lease, claim).await
    }

    /// Restart path for every incomplete cancellation. An `eligible` command
    /// may receive its first DELETE. A `not_dispatched` command may receive one
    /// new attempt only through the store's fenced retry claim. Every state
    /// whose transport outcome may be ambiguous remains GET-only.
    pub async fn recover_unresolved_cancellations<S: CancellationStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        limit: u16,
        now: DateTime<Utc>,
    ) -> Result<Vec<CancellationProgress>, ExecutionError> {
        validate_active_cancel_lease(lease, now)?;
        let unresolved = store
            .discover_unresolved_cancels(lease, limit)
            .await
            .map_err(cancellation_store_error)?;
        let mut progress = Vec::with_capacity(unresolved.len());
        for item in unresolved {
            if item.available_at > now || item.created_fencing_token > lease.fencing_token {
                return Err(ExecutionError::AuthorityDenied(
                    "unresolved cancellation is unavailable or belongs to a future fence".into(),
                ));
            }
            if let Some(terminal) = item.terminal_broker_evidence.as_ref() {
                progress.push(
                    self.finalize_cancel_from_broker_truth(
                        store,
                        lease,
                        item.cancel_outbox_id,
                        terminal.broker_event_id,
                        &terminal.event,
                        now,
                    )
                    .await?,
                );
                continue;
            }
            if item.current_state == "eligible" {
                let claim = store
                    .claim_cancel_dispatch(item.cancel_outbox_id, lease)
                    .await
                    .map_err(cancellation_store_error)?;
                let Some(claim) = claim else {
                    progress.push(CancellationProgress::RecoveryRequired {
                        cancel_intent_id: item.cancel_intent_id,
                        cancel_outbox_id: item.cancel_outbox_id,
                        provider_order_id: item.provider_order_id,
                    });
                    continue;
                };
                validate_cancel_claim(
                    &claim,
                    lease,
                    item.cancel_intent_id,
                    item.cancel_outbox_id,
                    &item.client_order_id,
                    &item.provider_order_id,
                )?;
                if claim.kind != crate::store::CancelOutboxClaimKind::FirstDispatch {
                    return Err(ExecutionError::AuthorityDenied(
                        "eligible restart cancellation lacked first-dispatch authority".into(),
                    ));
                }
                progress.push(self.dispatch_claimed_cancel(store, lease, claim).await?);
                continue;
            }

            if item.current_state == "not_dispatched" {
                let completed_attempt_count =
                    item.current_dispatch_attempt_count.ok_or_else(|| {
                        ExecutionError::LedgerInvariant(
                            "not-dispatched cancellation lacks its durable attempt count".into(),
                        )
                    })?;
                let expected_retry_attempt =
                    completed_attempt_count.checked_add(1).ok_or_else(|| {
                        ExecutionError::LedgerInvariant(
                            "not-dispatched cancellation attempt count overflowed".into(),
                        )
                    })?;
                let evidence = item.not_dispatched_evidence.as_ref().ok_or_else(|| {
                    ExecutionError::LedgerInvariant(
                        "not-dispatched cancellation lacks exact pre-I/O evidence".into(),
                    )
                })?;
                validate_not_dispatched_evidence(
                    evidence,
                    &item.provider_order_id,
                    Some(item.state_occurred_at),
                )?;
                let claim = store
                    .claim_cancel_retry(item.cancel_outbox_id, lease)
                    .await
                    .map_err(cancellation_store_error)?;
                let Some(claim) = claim else {
                    progress.push(CancellationProgress::RecoveryRequired {
                        cancel_intent_id: item.cancel_intent_id,
                        cancel_outbox_id: item.cancel_outbox_id,
                        provider_order_id: item.provider_order_id,
                    });
                    continue;
                };
                validate_cancel_claim(
                    &claim,
                    lease,
                    item.cancel_intent_id,
                    item.cancel_outbox_id,
                    &item.client_order_id,
                    &item.provider_order_id,
                )?;
                if claim.kind != crate::store::CancelOutboxClaimKind::RetryDispatch
                    || claim.attempt_count != expected_retry_attempt
                    || claim.current_state != "dispatch_started"
                {
                    return Err(ExecutionError::AuthorityDenied(
                        "not-dispatched cancellation lacked the exact next retry authority".into(),
                    ));
                }
                progress.push(self.dispatch_claimed_cancel(store, lease, claim).await?);
                continue;
            }

            let claim = store
                .claim_cancel_recovery(item.cancel_outbox_id, lease)
                .await
                .map_err(cancellation_store_error)?;
            let Some(claim) = claim else {
                progress.push(CancellationProgress::RecoveryRequired {
                    cancel_intent_id: item.cancel_intent_id,
                    cancel_outbox_id: item.cancel_outbox_id,
                    provider_order_id: item.provider_order_id,
                });
                continue;
            };
            validate_cancel_claim(
                &claim,
                lease,
                item.cancel_intent_id,
                item.cancel_outbox_id,
                &item.client_order_id,
                &item.provider_order_id,
            )?;
            if claim.kind != crate::store::CancelOutboxClaimKind::RecoveryLookupOnly {
                return Err(ExecutionError::AuthorityDenied(
                    "dispatched cancellation recovery lacked lookup-only authority".into(),
                ));
            }
            match self
                .broker
                .find_order_by_provider_id(&item.provider_order_id, &item.client_order_id)
                .await
            {
                Ok(Some(event)) => {
                    validate_cancel_broker_event(
                        &event,
                        &item.provider_order_id,
                        &item.client_order_id,
                    )?;
                    progress.push(CancellationProgress::BrokerStateObserved {
                        cancel_intent_id: item.cancel_intent_id,
                        cancel_outbox_id: item.cancel_outbox_id,
                        terminal: is_terminal_broker_status(&event.status),
                        event,
                    });
                }
                Ok(None) => progress.push(CancellationProgress::LookupStillUnresolved {
                    cancel_intent_id: item.cancel_intent_id,
                    cancel_outbox_id: item.cancel_outbox_id,
                    provider_order_id: item.provider_order_id,
                    detail: "provider order lookup returned no order".into(),
                }),
                Err(error) => progress.push(CancellationProgress::LookupStillUnresolved {
                    cancel_intent_id: item.cancel_intent_id,
                    cancel_outbox_id: item.cancel_outbox_id,
                    provider_order_id: item.provider_order_id,
                    detail: bounded_execution_detail(&error.to_string()),
                }),
            }
        }
        Ok(progress)
    }

    /// Completes a cancel outbox only after the same terminal broker event has
    /// already been persisted. The PostgreSQL store independently enforces the
    /// broker-event foreign key, terminal status, order identity, and fence.
    pub async fn finalize_cancel_from_broker_truth<S: CancellationStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        cancel_outbox_id: Uuid,
        terminal_broker_event_id: Uuid,
        terminal_event: &BrokerEvent,
        now: DateTime<Utc>,
    ) -> Result<CancellationProgress, ExecutionError> {
        validate_active_cancel_lease(lease, now)?;
        if !is_terminal_broker_status(&terminal_event.status) {
            return Err(ExecutionError::Lifecycle(
                "cancellation cannot finalize from a nonterminal broker status".into(),
            ));
        }
        let claim = store
            .claim_cancel_terminal_completion(cancel_outbox_id, lease)
            .await
            .map_err(cancellation_store_error)?
            .ok_or_else(|| {
                ExecutionError::AuthorityDenied(
                    "terminal cancellation completion lacks persisted broker truth".into(),
                )
            })?;
        if claim.kind != crate::store::CancelOutboxClaimKind::TerminalCompletionOnly
            || claim.cancel_outbox_id != cancel_outbox_id
        {
            return Err(ExecutionError::AuthorityDenied(
                "terminal cancellation claim has the wrong authority class or identity".into(),
            ));
        }
        validate_cancel_claim(
            &claim,
            lease,
            claim.cancel_intent_id,
            cancel_outbox_id,
            &terminal_event.client_order_id,
            terminal_event
                .provider_order_id
                .as_deref()
                .unwrap_or_default(),
        )?;
        validate_cancel_broker_event(
            terminal_event,
            &claim.provider_order_id,
            &claim.client_order_id,
        )?;
        let completion_reason = format!(
            "BROKER_TERMINAL_{}",
            terminal_event.status.to_ascii_uppercase()
        );
        finalize_cancel_resolving(
            store,
            cancel_outbox_id,
            lease,
            terminal_broker_event_id,
            &completion_reason,
        )
        .await?;
        Ok(CancellationProgress::TerminalFinalized {
            cancel_intent_id: claim.cancel_intent_id,
            cancel_outbox_id,
            terminal_broker_event_id,
            provider_status: terminal_event.status.clone(),
        })
    }

    async fn dispatch_claimed_cancel<S: CancellationStorePort>(
        &self,
        store: &mut S,
        lease: &FencedLease,
        claim: ClaimedCancelOutbox,
    ) -> Result<CancellationProgress, ExecutionError> {
        let observed_at = || Utc::now();
        match self.broker.cancel_order(&claim.provider_order_id).await {
            Ok(CancellationOutcome::RequestAccepted(accepted))
                if accepted.provider_order_id == claim.provider_order_id =>
            {
                record_cancel_accepted_resolving(store, claim.cancel_outbox_id, lease, &accepted)
                    .await?;
                Ok(CancellationProgress::RequestAccepted {
                    cancel_intent_id: claim.cancel_intent_id,
                    cancel_outbox_id: claim.cancel_outbox_id,
                    provider_order_id: claim.provider_order_id,
                    request_id: accepted.request_id,
                })
            }
            Ok(CancellationOutcome::RequestAccepted(_)) => {
                let detail = "cancel acknowledgement provider_order_id mismatch";
                record_cancel_unknown_resolving(
                    store,
                    claim.cancel_outbox_id,
                    lease,
                    detail,
                    observed_at(),
                )
                .await?;
                Ok(CancellationProgress::OutcomeUnknown {
                    cancel_intent_id: claim.cancel_intent_id,
                    cancel_outbox_id: claim.cancel_outbox_id,
                    provider_order_id: claim.provider_order_id,
                    detail: detail.into(),
                })
            }
            Ok(CancellationOutcome::NotDispatched(proof)) => {
                if let Err(error) =
                    validate_not_dispatched_evidence(&proof, &claim.provider_order_id, None)
                {
                    let detail = bounded_execution_detail(&error.to_string());
                    record_cancel_unknown_resolving(
                        store,
                        claim.cancel_outbox_id,
                        lease,
                        &detail,
                        proof.observed_at,
                    )
                    .await?;
                    return Ok(CancellationProgress::OutcomeUnknown {
                        cancel_intent_id: claim.cancel_intent_id,
                        cancel_outbox_id: claim.cancel_outbox_id,
                        provider_order_id: claim.provider_order_id,
                        detail,
                    });
                }
                record_cancel_not_dispatched_resolving(
                    store,
                    claim.cancel_outbox_id,
                    lease,
                    claim.attempt_count,
                    &proof,
                )
                .await?;
                Ok(CancellationProgress::RetryEligible {
                    cancel_intent_id: claim.cancel_intent_id,
                    cancel_outbox_id: claim.cancel_outbox_id,
                    provider_order_id: claim.provider_order_id,
                    completed_attempt_count: claim.attempt_count,
                    evidence_hash: proof.evidence_hash,
                })
            }
            Ok(CancellationOutcome::Unknown { detail }) => {
                let detail = bounded_execution_detail(&detail);
                record_cancel_unknown_resolving(
                    store,
                    claim.cancel_outbox_id,
                    lease,
                    &detail,
                    observed_at(),
                )
                .await?;
                Ok(CancellationProgress::OutcomeUnknown {
                    cancel_intent_id: claim.cancel_intent_id,
                    cancel_outbox_id: claim.cancel_outbox_id,
                    provider_order_id: claim.provider_order_id,
                    detail,
                })
            }
            Err(error) => {
                let detail = bounded_execution_detail(&error.to_string());
                record_cancel_unknown_resolving(
                    store,
                    claim.cancel_outbox_id,
                    lease,
                    &detail,
                    observed_at(),
                )
                .await?;
                Ok(CancellationProgress::OutcomeUnknown {
                    cancel_intent_id: claim.cancel_intent_id,
                    cancel_outbox_id: claim.cancel_outbox_id,
                    provider_order_id: claim.provider_order_id,
                    detail,
                })
            }
        }
    }
}

fn validate_active_cancel_lease(
    lease: &FencedLease,
    now: DateTime<Utc>,
) -> Result<(), ExecutionError> {
    if lease.environment == trader_core::Environment::Shadow
        || lease.fencing_token == 0
        || now >= lease.lease_until
    {
        return Err(ExecutionError::AuthorityDenied(
            "cancellation requires an active fenced paper/live lease".into(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_cancel_claim(
    claim: &ClaimedCancelOutbox,
    lease: &FencedLease,
    expected_cancel_intent_id: Uuid,
    expected_cancel_outbox_id: Uuid,
    expected_client_order_id: &str,
    expected_provider_order_id: &str,
) -> Result<(), ExecutionError> {
    if claim.cancel_intent_id != expected_cancel_intent_id
        || claim.cancel_outbox_id != expected_cancel_outbox_id
        || claim.client_order_id != expected_client_order_id
        || claim.provider_order_id != expected_provider_order_id
        || claim.environment != lease.environment
        || claim.account_fingerprint != lease.account_fingerprint
        || claim.claim_fencing_token != lease.fencing_token
        || claim.claimed_by != lease.owner_id
        || claim.created_fencing_token > lease.fencing_token
    {
        return Err(ExecutionError::AuthorityDenied(
            "claimed cancellation identity or fence does not match durable authority".into(),
        ));
    }
    Ok(())
}

fn validate_cancel_broker_event(
    event: &BrokerEvent,
    expected_provider_order_id: &str,
    expected_client_order_id: &str,
) -> Result<(), ExecutionError> {
    if event.provider_order_id.as_deref() != Some(expected_provider_order_id)
        || event.client_order_id != expected_client_order_id
    {
        return Err(ExecutionError::Lifecycle(
            "cancellation recovery returned mismatched broker order identity".into(),
        ));
    }
    if !is_recognized_broker_status(&event.status) {
        return Err(ExecutionError::Lifecycle(
            "cancellation recovery returned an unknown broker status".into(),
        ));
    }
    Ok(())
}

fn validate_not_dispatched_evidence(
    evidence: &CancellationNotDispatched,
    expected_provider_order_id: &str,
    expected_observed_at: Option<DateTime<Utc>>,
) -> Result<(), ExecutionError> {
    let timestamp_is_exact =
        expected_observed_at.is_none_or(|expected| evidence.observed_at == expected);
    let text_is_bounded = !evidence.detail.is_empty()
        && evidence.detail.len() <= 512
        && !evidence.detail.chars().any(char::is_control);
    let expected_hash = trader_core::HashDigest::of_json(&serde_json::json!({
        "provider_order_id": &evidence.provider_order_id,
        "observed_at": evidence.observed_at,
        "reason_code": &evidence.reason_code,
        "detail": &evidence.detail,
    }))
    .map_err(|error| ExecutionError::Lifecycle(error.to_string()))?;
    if evidence.provider_order_id != expected_provider_order_id
        || evidence.reason_code != "TRANSPORT_BEFORE_SEND"
        || !text_is_bounded
        || !timestamp_is_exact
        || expected_hash != evidence.evidence_hash
    {
        return Err(ExecutionError::Lifecycle(
            "cancellation not-dispatched evidence is invalid or mismatched".into(),
        ));
    }
    Ok(())
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

async fn persist_cancel_intent_resolving<S: CancellationStorePort>(
    store: &mut S,
    write: &CancelIntentWrite<'_>,
) -> Result<PersistedCancelIntent, ExecutionError> {
    for attempt in 0..2 {
        match store.persist_cancel_intent(write).await {
            Ok(persisted) => return Ok(persisted),
            Err(CancellationStoreError::CommitUnknown { recovery, .. }) => {
                let persisted = match recovery.as_ref() {
                    CommitRecoveryKey::CancelIntent {
                        cancel_intent_id,
                        cancel_outbox_id,
                        ..
                    } => PersistedCancelIntent {
                        cancel_intent_id: *cancel_intent_id,
                        cancel_outbox_id: *cancel_outbox_id,
                    },
                    _ => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel intent commit returned the wrong recovery key".into(),
                        ));
                    }
                };
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(cancellation_store_error)?
                {
                    CommitResolution::Committed => return Ok(persisted),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel intent was proven not committed; DELETE remains forbidden"
                                .into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel intent commit recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(cancellation_store_error(error)),
        }
    }
    unreachable!("bounded cancel-intent commit recovery loop always returns")
}

async fn record_cancel_accepted_resolving<S: CancellationStorePort>(
    store: &mut S,
    cancel_outbox_id: Uuid,
    lease: &FencedLease,
    accepted: &crate::port::CancellationRequestAccepted,
) -> Result<(), ExecutionError> {
    for attempt in 0..2 {
        match store
            .record_cancel_request_accepted(cancel_outbox_id, lease, accepted)
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(ExecutionError::LedgerInvariant(
                    "cancel acknowledgement did not append to durable state".into(),
                ));
            }
            Err(CancellationStoreError::CommitUnknown { recovery, .. }) => {
                if !matches!(
                    recovery.as_ref(),
                    CommitRecoveryKey::CancelRequestAccepted {
                        cancel_outbox_id: recovered,
                        ..
                    } if *recovered == cancel_outbox_id
                ) {
                    return Err(ExecutionError::LedgerInvariant(
                        "cancel acknowledgement commit returned the wrong recovery key".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(cancellation_store_error)?
                {
                    CommitResolution::Committed => return Ok(()),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel acknowledgement remains uncommitted; reconciliation required"
                                .into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel acknowledgement recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(cancellation_store_error(error)),
        }
    }
    unreachable!("bounded cancel-acknowledgement recovery loop always returns")
}

async fn record_cancel_unknown_resolving<S: CancellationStorePort>(
    store: &mut S,
    cancel_outbox_id: Uuid,
    lease: &FencedLease,
    detail: &str,
    occurred_at: DateTime<Utc>,
) -> Result<(), ExecutionError> {
    for attempt in 0..2 {
        match store
            .record_cancel_unknown(cancel_outbox_id, lease, detail, occurred_at)
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(ExecutionError::LedgerInvariant(
                    "ambiguous cancel outcome did not append to durable state".into(),
                ));
            }
            Err(CancellationStoreError::CommitUnknown { recovery, .. }) => {
                if !matches!(
                    recovery.as_ref(),
                    CommitRecoveryKey::CancelUnknown {
                        cancel_outbox_id: recovered,
                        ..
                    } if *recovered == cancel_outbox_id
                ) {
                    return Err(ExecutionError::LedgerInvariant(
                        "ambiguous cancel commit returned the wrong recovery key".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(cancellation_store_error)?
                {
                    CommitResolution::Committed => return Ok(()),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "ambiguous cancel outcome remains uncommitted; reconciliation required"
                                .into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "ambiguous cancel recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(cancellation_store_error(error)),
        }
    }
    unreachable!("bounded ambiguous-cancel recovery loop always returns")
}

async fn record_cancel_not_dispatched_resolving<S: CancellationStorePort>(
    store: &mut S,
    cancel_outbox_id: Uuid,
    lease: &FencedLease,
    expected_attempt_count: u32,
    evidence: &CancellationNotDispatched,
) -> Result<(), ExecutionError> {
    for attempt in 0..2 {
        match store
            .record_cancel_not_dispatched(cancel_outbox_id, lease, expected_attempt_count, evidence)
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(ExecutionError::LedgerInvariant(
                    "pre-I/O cancel proof did not append to durable state".into(),
                ));
            }
            Err(CancellationStoreError::CommitUnknown { recovery, .. }) => {
                if !matches!(
                    recovery.as_ref(),
                    CommitRecoveryKey::CancelNotDispatched {
                        cancel_outbox_id: recovered,
                        attempt_count: recovered_attempt,
                        evidence_hash,
                        ..
                    } if *recovered == cancel_outbox_id
                        && *recovered_attempt == expected_attempt_count
                        && *evidence_hash == evidence.evidence_hash
                ) {
                    return Err(ExecutionError::LedgerInvariant(
                        "pre-I/O cancel proof commit returned the wrong recovery key".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(cancellation_store_error)?
                {
                    CommitResolution::Committed => return Ok(()),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "pre-I/O cancel proof remains uncommitted; retry is forbidden".into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "pre-I/O cancel proof recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(cancellation_store_error(error)),
        }
    }
    unreachable!("bounded pre-I/O cancel-proof recovery loop always returns")
}

async fn finalize_cancel_resolving<S: CancellationStorePort>(
    store: &mut S,
    cancel_outbox_id: Uuid,
    lease: &FencedLease,
    terminal_broker_event_id: Uuid,
    completion_reason: &str,
) -> Result<(), ExecutionError> {
    for attempt in 0..2 {
        match store
            .finalize_cancel_outbox(
                cancel_outbox_id,
                lease,
                terminal_broker_event_id,
                completion_reason,
            )
            .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                return Err(ExecutionError::LedgerInvariant(
                    "terminal broker truth did not finalize cancellation".into(),
                ));
            }
            Err(CancellationStoreError::CommitUnknown { recovery, .. }) => {
                if !matches!(
                    recovery.as_ref(),
                    CommitRecoveryKey::CancelFinalization {
                        cancel_outbox_id: recovered,
                        terminal_broker_event_id: recovered_event,
                        ..
                    } if *recovered == cancel_outbox_id
                        && *recovered_event == terminal_broker_event_id
                ) {
                    return Err(ExecutionError::LedgerInvariant(
                        "cancel finalization returned the wrong recovery key".into(),
                    ));
                }
                match store
                    .resolve_commit(&recovery)
                    .await
                    .map_err(cancellation_store_error)?
                {
                    CommitResolution::Committed => return Ok(()),
                    CommitResolution::NotCommitted if attempt == 0 => continue,
                    CommitResolution::NotCommitted => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel finalization remains uncommitted".into(),
                        ));
                    }
                    CommitResolution::ConflictingEvidence => {
                        return Err(ExecutionError::LedgerInvariant(
                            "cancel finalization recovery found conflicting evidence".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(cancellation_store_error(error)),
        }
    }
    unreachable!("bounded cancel-finalization recovery loop always returns")
}

fn cancellation_store_error(error: CancellationStoreError) -> ExecutionError {
    ExecutionError::LedgerInvariant(error.to_string())
}

fn bounded_execution_detail(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() || character == ' ' {
                character
            } else {
                '?'
            }
        })
        .take(256)
        .collect()
}

fn append_validated_broker_event(
    ledger: &mut ExecutionLedger,
    expected_client_order_id: &str,
    event: BrokerEvent,
    now: DateTime<Utc>,
) -> Result<(), ExecutionError> {
    if event.client_order_id != expected_client_order_id {
        return Err(ExecutionError::Lifecycle(
            "observed broker event does not match requested client_order_id".into(),
        ));
    }
    let projected = ledger.project_orders()?;
    let mut lifecycle = projected
        .get(expected_client_order_id)
        .cloned()
        .ok_or_else(|| ExecutionError::LedgerInvariant("observed event lacks intent".into()))?;
    lifecycle.apply_broker_event(&event)?;
    ledger.append(ExecutionEvent::BrokerEventObserved { event }, now)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use trader_core::{
        AccountSnapshot, Environment, HashDigest, OrderSide, Price, Symbol, TimeInForce,
        WholeQuantity,
    };

    use super::*;
    use crate::{
        port::{
            CancellationNotDispatched, CancellationOutcome, CancellationRequestAccepted,
            ObservedBrokerOrder,
        },
        store::{CancelOutboxClaimKind, PersistedTerminalBrokerEvent},
    };

    fn observed_test_order(mut event: BrokerEvent, label: &str) -> ObservedBrokerOrder {
        let raw_response_json = format!(r#"{{"test_evidence":"{label}"}}"#).into_bytes();
        event.raw_payload_hash = HashDigest::sha256(&raw_response_json);
        ObservedBrokerOrder::try_new(event, raw_response_json).unwrap()
    }

    #[derive(Clone, Default)]
    struct AmbiguousBroker {
        submissions: Arc<Mutex<u32>>,
        lookups: Arc<Mutex<u32>>,
        deadlines: Arc<Mutex<Vec<DateTime<Utc>>>>,
        reject_submission_window: bool,
    }

    #[async_trait]
    impl BrokerPort for AmbiguousBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        fn validate_submission_window(
            &self,
            broker_arrival_by: DateTime<Utc>,
            now: DateTime<Utc>,
        ) -> Result<(), ExecutionError> {
            if !self.reject_submission_window && now < broker_arrival_by {
                Ok(())
            } else {
                Err(ExecutionError::AuthorityDenied(
                    "test broker-arrival window expired".into(),
                ))
            }
        }

        async fn find_order_by_client_id(
            &self,
            _expected_intent: &OrderIntent,
        ) -> Result<Option<ObservedBrokerOrder>, ExecutionError> {
            *self.lookups.lock().unwrap() += 1;
            Ok(None)
        }

        async fn find_order_by_provider_id(
            &self,
            _provider_order_id: &str,
            _expected_client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            *self.lookups.lock().unwrap() += 1;
            Ok(None)
        }

        async fn submit_committed_intent(
            &self,
            _intent: &OrderIntent,
            _session_permit: &RegularTradingSessionPermit,
            not_after: DateTime<Utc>,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            *self.submissions.lock().unwrap() += 1;
            self.deadlines.lock().unwrap().push(not_after);
            Ok(SubmissionOutcome::Unknown {
                detail: "timeout".into(),
            })
        }

        async fn cancel_order(
            &self,
            _provider_order_id: &str,
        ) -> Result<CancellationOutcome, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }
    }

    struct WrongClientBroker;

    #[async_trait]
    impl BrokerPort for WrongClientBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        fn validate_submission_window(
            &self,
            broker_arrival_by: DateTime<Utc>,
            now: DateTime<Utc>,
        ) -> Result<(), ExecutionError> {
            if now < broker_arrival_by {
                Ok(())
            } else {
                Err(ExecutionError::AuthorityDenied(
                    "test broker-arrival window expired".into(),
                ))
            }
        }

        async fn find_order_by_client_id(
            &self,
            _expected_intent: &OrderIntent,
        ) -> Result<Option<ObservedBrokerOrder>, ExecutionError> {
            Ok(None)
        }

        async fn find_order_by_provider_id(
            &self,
            _provider_order_id: &str,
            _expected_client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            Ok(None)
        }

        async fn submit_committed_intent(
            &self,
            intent: &OrderIntent,
            _session_permit: &RegularTradingSessionPermit,
            _not_after: DateTime<Utc>,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            Ok(SubmissionOutcome::Observed(observed_test_order(
                BrokerEvent {
                    provider_order_id: Some("provider-1".into()),
                    client_order_id: "wrong-client-id".into(),
                    status: "accepted".into(),
                    filled_quantity: WholeQuantity::ZERO,
                    fill_price: None,
                    provider_timestamp: intent.created_at,
                    received_at: intent.created_at,
                    raw_payload_hash: HashDigest::sha256("wrong-event"),
                    request_id: Some("request-1".into()),
                },
                "wrong-client",
            )))
        }

        async fn cancel_order(
            &self,
            _provider_order_id: &str,
        ) -> Result<CancellationOutcome, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }
    }

    struct InvalidStatusBroker;

    #[async_trait]
    impl BrokerPort for InvalidStatusBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        fn validate_submission_window(
            &self,
            broker_arrival_by: DateTime<Utc>,
            now: DateTime<Utc>,
        ) -> Result<(), ExecutionError> {
            if now < broker_arrival_by {
                Ok(())
            } else {
                Err(ExecutionError::AuthorityDenied(
                    "test broker-arrival window expired".into(),
                ))
            }
        }

        async fn find_order_by_client_id(
            &self,
            _expected_intent: &OrderIntent,
        ) -> Result<Option<ObservedBrokerOrder>, ExecutionError> {
            Ok(None)
        }

        async fn find_order_by_provider_id(
            &self,
            _provider_order_id: &str,
            _expected_client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            Ok(None)
        }

        async fn submit_committed_intent(
            &self,
            intent: &OrderIntent,
            _session_permit: &RegularTradingSessionPermit,
            _not_after: DateTime<Utc>,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            Ok(SubmissionOutcome::Observed(observed_test_order(
                BrokerEvent {
                    provider_order_id: Some("provider-1".into()),
                    client_order_id: intent.client_order_id.clone(),
                    status: "future_provider_status".into(),
                    filled_quantity: WholeQuantity::ZERO,
                    fill_price: None,
                    provider_timestamp: intent.created_at,
                    received_at: intent.created_at,
                    raw_payload_hash: HashDigest::sha256("invalid-event"),
                    request_id: Some("request-invalid".into()),
                },
                "invalid-status",
            )))
        }

        async fn cancel_order(
            &self,
            _provider_order_id: &str,
        ) -> Result<CancellationOutcome, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FakeCancelState {
        Empty,
        Eligible,
        DispatchStarted,
        NotDispatched,
        Accepted,
        Unknown,
        Terminal,
    }

    #[derive(Clone)]
    struct CancellationBroker {
        log: Arc<Mutex<Vec<String>>>,
        cancel_calls: Arc<Mutex<u32>>,
        delete_calls: Arc<Mutex<u32>>,
        provider_lookups: Arc<Mutex<u32>>,
        cancellations: Arc<Mutex<std::collections::VecDeque<CancellationOutcome>>>,
        lookup: Option<BrokerEvent>,
    }

    impl CancellationBroker {
        fn new(
            log: Arc<Mutex<Vec<String>>>,
            cancellation: CancellationOutcome,
            lookup: Option<BrokerEvent>,
        ) -> Self {
            Self::with_cancellations(log, vec![cancellation], lookup)
        }

        fn with_cancellations(
            log: Arc<Mutex<Vec<String>>>,
            cancellations: Vec<CancellationOutcome>,
            lookup: Option<BrokerEvent>,
        ) -> Self {
            Self {
                log,
                cancel_calls: Arc::new(Mutex::new(0)),
                delete_calls: Arc::new(Mutex::new(0)),
                provider_lookups: Arc::new(Mutex::new(0)),
                cancellations: Arc::new(Mutex::new(cancellations.into())),
                lookup,
            }
        }
    }

    #[async_trait]
    impl BrokerPort for CancellationBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        fn validate_submission_window(
            &self,
            _broker_arrival_by: DateTime<Utc>,
            _now: DateTime<Utc>,
        ) -> Result<(), ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn find_order_by_client_id(
            &self,
            _expected_intent: &OrderIntent,
        ) -> Result<Option<ObservedBrokerOrder>, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn find_order_by_provider_id(
            &self,
            _provider_order_id: &str,
            _expected_client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            *self.provider_lookups.lock().unwrap() += 1;
            self.log.lock().unwrap().push("get".into());
            Ok(self.lookup.clone())
        }

        async fn submit_committed_intent(
            &self,
            _intent: &OrderIntent,
            _session_permit: &RegularTradingSessionPermit,
            _not_after: DateTime<Utc>,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn cancel_order(
            &self,
            _provider_order_id: &str,
        ) -> Result<CancellationOutcome, ExecutionError> {
            *self.cancel_calls.lock().unwrap() += 1;
            let outcome = self
                .cancellations
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| ExecutionError::Broker("unplanned cancellation call".into()))?;
            if matches!(outcome, CancellationOutcome::NotDispatched(_)) {
                self.log.lock().unwrap().push("before_send".into());
            } else {
                *self.delete_calls.lock().unwrap() += 1;
                self.log.lock().unwrap().push("delete".into());
            }
            Ok(outcome)
        }
    }

    struct FakeCancellationStore {
        log: Arc<Mutex<Vec<String>>>,
        lease: FencedLease,
        requested_at: DateTime<Utc>,
        state: FakeCancelState,
        fail_claim_before_transition: bool,
        fail_claim_after_transition: bool,
        fail_record_before_commit: bool,
        accepted_commit_unknown_once: bool,
        not_dispatched_commit_unknown_once: bool,
        wrong_not_dispatched_recovery_attempt: bool,
        claimed_attempt_count_override: Option<u32>,
        attempt_count: u32,
        not_dispatched_evidence: Option<CancellationNotDispatched>,
        terminal_broker_evidence: Option<PersistedTerminalBrokerEvent>,
        resolve_calls: u32,
        finalize_calls: u32,
    }

    impl FakeCancellationStore {
        const CANCEL_INTENT_ID: Uuid = Uuid::from_u128(11);
        const CANCEL_OUTBOX_ID: Uuid = Uuid::from_u128(12);
        const TERMINAL_EVENT_ID: Uuid = Uuid::from_u128(13);

        fn new(log: Arc<Mutex<Vec<String>>>, lease: FencedLease, now: DateTime<Utc>) -> Self {
            Self {
                log,
                lease,
                requested_at: now,
                state: FakeCancelState::Empty,
                fail_claim_before_transition: false,
                fail_claim_after_transition: false,
                fail_record_before_commit: false,
                accepted_commit_unknown_once: false,
                not_dispatched_commit_unknown_once: false,
                wrong_not_dispatched_recovery_attempt: false,
                claimed_attempt_count_override: None,
                attempt_count: 0,
                not_dispatched_evidence: None,
                terminal_broker_evidence: None,
                resolve_calls: 0,
                finalize_calls: 0,
            }
        }

        fn claimed(&self, kind: CancelOutboxClaimKind) -> ClaimedCancelOutbox {
            ClaimedCancelOutbox {
                kind,
                cancel_outbox_id: Self::CANCEL_OUTBOX_ID,
                cancel_intent_id: Self::CANCEL_INTENT_ID,
                client_order_id: "client-1".into(),
                provider_order_id: "provider-1".into(),
                reason_code: "RISK_EXIT".into(),
                requested_at: self.requested_at,
                environment: self.lease.environment,
                account_fingerprint: self.lease.account_fingerprint,
                created_fencing_token: self.lease.fencing_token,
                claim_fencing_token: self.lease.fencing_token,
                payload: serde_json::json!({"kind": "cancel"}),
                available_at: self.requested_at,
                claimed_by: self.lease.owner_id,
                claimed_at: self.requested_at,
                attempt_count: self
                    .claimed_attempt_count_override
                    .unwrap_or(self.attempt_count),
                current_state: match kind {
                    CancelOutboxClaimKind::FirstDispatch | CancelOutboxClaimKind::RetryDispatch => {
                        "dispatch_started"
                    }
                    CancelOutboxClaimKind::RecoveryLookupOnly => match self.state {
                        FakeCancelState::Accepted => "request_accepted",
                        FakeCancelState::Unknown => "cancel_unknown",
                        _ => "dispatch_started",
                    },
                    CancelOutboxClaimKind::TerminalCompletionOnly => match self.state {
                        FakeCancelState::Accepted => "request_accepted",
                        FakeCancelState::Unknown => "cancel_unknown",
                        FakeCancelState::Eligible => "eligible",
                        FakeCancelState::NotDispatched => "not_dispatched",
                        _ => "dispatch_started",
                    },
                }
                .into(),
            }
        }

        fn unresolved(&self) -> Option<UnresolvedCancelOutbox> {
            let current_state = match self.state {
                FakeCancelState::Empty | FakeCancelState::Terminal => return None,
                FakeCancelState::Eligible => "eligible",
                FakeCancelState::DispatchStarted => "dispatch_started",
                FakeCancelState::NotDispatched => "not_dispatched",
                FakeCancelState::Accepted => "request_accepted",
                FakeCancelState::Unknown => "cancel_unknown",
            };
            let state_occurred_at = self
                .not_dispatched_evidence
                .as_ref()
                .filter(|_| self.state == FakeCancelState::NotDispatched)
                .map_or(self.requested_at, |evidence| evidence.observed_at);
            Some(UnresolvedCancelOutbox {
                cancel_outbox_id: Self::CANCEL_OUTBOX_ID,
                cancel_intent_id: Self::CANCEL_INTENT_ID,
                client_order_id: "client-1".into(),
                provider_order_id: "provider-1".into(),
                reason_code: "RISK_EXIT".into(),
                requested_at: self.requested_at,
                created_fencing_token: self.lease.fencing_token,
                payload: serde_json::json!({"kind": "cancel"}),
                available_at: self.requested_at,
                current_state: current_state.into(),
                request_id: (self.state == FakeCancelState::Accepted)
                    .then(|| "request-cancel".into()),
                payload_hash: (self.state == FakeCancelState::Accepted)
                    .then(|| HashDigest::sha256("cancel-204")),
                detail: if self.state == FakeCancelState::Unknown {
                    "ambiguous".into()
                } else {
                    String::new()
                },
                broker_event_id: None,
                state_occurred_at,
                current_dispatch_attempt_count: matches!(
                    self.state,
                    FakeCancelState::DispatchStarted | FakeCancelState::NotDispatched
                )
                .then_some(self.attempt_count),
                not_dispatched_evidence: if self.state == FakeCancelState::NotDispatched {
                    self.not_dispatched_evidence.clone()
                } else {
                    None
                },
                terminal_broker_evidence: self.terminal_broker_evidence.clone(),
            })
        }
    }

    #[async_trait]
    impl CancellationStorePort for FakeCancellationStore {
        async fn persist_cancel_intent(
            &mut self,
            write: &CancelIntentWrite<'_>,
        ) -> Result<PersistedCancelIntent, CancellationStoreError> {
            if write.client_order_id != "client-1"
                || write.provider_order_id != "provider-1"
                || write.lease != &self.lease
            {
                return Err(CancellationStoreError::Store("identity mismatch".into()));
            }
            if self.state == FakeCancelState::Empty {
                self.state = FakeCancelState::Eligible;
                self.requested_at = write.requested_at;
                self.log.lock().unwrap().push("persist".into());
            }
            Ok(PersistedCancelIntent {
                cancel_intent_id: Self::CANCEL_INTENT_ID,
                cancel_outbox_id: Self::CANCEL_OUTBOX_ID,
            })
        }

        async fn claim_cancel_dispatch(
            &mut self,
            cancel_outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
            if cancel_outbox_id != Self::CANCEL_OUTBOX_ID || self.state != FakeCancelState::Eligible
            {
                return Ok(None);
            }
            if self.fail_claim_before_transition {
                return Err(CancellationStoreError::Store(
                    "process ended before durable claim".into(),
                ));
            }
            self.attempt_count = 1;
            self.state = FakeCancelState::DispatchStarted;
            self.log.lock().unwrap().push("claim".into());
            if self.fail_claim_after_transition {
                return Err(CancellationStoreError::Store(
                    "claim response lost after commit".into(),
                ));
            }
            Ok(Some(self.claimed(CancelOutboxClaimKind::FirstDispatch)))
        }

        async fn claim_cancel_retry(
            &mut self,
            cancel_outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
            if cancel_outbox_id != Self::CANCEL_OUTBOX_ID
                || self.state != FakeCancelState::NotDispatched
            {
                return Ok(None);
            }
            self.attempt_count = self
                .attempt_count
                .checked_add(1)
                .ok_or_else(|| CancellationStoreError::Store("attempt overflow".into()))?;
            self.state = FakeCancelState::DispatchStarted;
            self.log.lock().unwrap().push("retry_claim".into());
            Ok(Some(self.claimed(CancelOutboxClaimKind::RetryDispatch)))
        }

        async fn claim_cancel_recovery(
            &mut self,
            cancel_outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
            if cancel_outbox_id == Self::CANCEL_OUTBOX_ID
                && matches!(
                    self.state,
                    FakeCancelState::DispatchStarted
                        | FakeCancelState::Accepted
                        | FakeCancelState::Unknown
                )
            {
                self.log.lock().unwrap().push("recovery_claim".into());
                Ok(Some(
                    self.claimed(CancelOutboxClaimKind::RecoveryLookupOnly),
                ))
            } else {
                Ok(None)
            }
        }

        async fn claim_cancel_terminal_completion(
            &mut self,
            cancel_outbox_id: Uuid,
            _lease: &FencedLease,
        ) -> Result<Option<ClaimedCancelOutbox>, CancellationStoreError> {
            if cancel_outbox_id == Self::CANCEL_OUTBOX_ID && self.terminal_broker_evidence.is_some()
            {
                Ok(Some(
                    self.claimed(CancelOutboxClaimKind::TerminalCompletionOnly),
                ))
            } else {
                Ok(None)
            }
        }

        async fn record_cancel_request_accepted(
            &mut self,
            _cancel_outbox_id: Uuid,
            _lease: &FencedLease,
            _accepted: &CancellationRequestAccepted,
        ) -> Result<bool, CancellationStoreError> {
            if self.fail_record_before_commit {
                return Err(CancellationStoreError::Store(
                    "process ended before accepted evidence commit".into(),
                ));
            }
            self.state = FakeCancelState::Accepted;
            self.log.lock().unwrap().push("record_accepted".into());
            if self.accepted_commit_unknown_once {
                self.accepted_commit_unknown_once = false;
                return Err(CancellationStoreError::CommitUnknown {
                    operation: "record_cancel_request_accepted",
                    recovery: Box::new(CommitRecoveryKey::CancelRequestAccepted {
                        cancel_outbox_id: Self::CANCEL_OUTBOX_ID,
                        state_event_id: Uuid::from_u128(14),
                        evidence_hash: HashDigest::sha256("accepted-evidence"),
                    }),
                });
            }
            Ok(true)
        }

        async fn record_cancel_unknown(
            &mut self,
            _cancel_outbox_id: Uuid,
            _lease: &FencedLease,
            _detail: &str,
            _occurred_at: DateTime<Utc>,
        ) -> Result<bool, CancellationStoreError> {
            self.state = FakeCancelState::Unknown;
            self.log.lock().unwrap().push("record_unknown".into());
            Ok(true)
        }

        async fn record_cancel_not_dispatched(
            &mut self,
            cancel_outbox_id: Uuid,
            _lease: &FencedLease,
            expected_attempt_count: u32,
            evidence: &CancellationNotDispatched,
        ) -> Result<bool, CancellationStoreError> {
            if cancel_outbox_id != Self::CANCEL_OUTBOX_ID
                || self.state != FakeCancelState::DispatchStarted
                || expected_attempt_count != self.attempt_count
                || evidence.provider_order_id != "provider-1"
            {
                return Err(CancellationStoreError::Store(
                    "stale or mismatched not-dispatched attempt".into(),
                ));
            }
            validate_not_dispatched_evidence(evidence, "provider-1", None)
                .map_err(|error| CancellationStoreError::Store(error.to_string()))?;
            self.state = FakeCancelState::NotDispatched;
            self.not_dispatched_evidence = Some(evidence.clone());
            self.log
                .lock()
                .unwrap()
                .push("record_not_dispatched".into());
            if self.not_dispatched_commit_unknown_once {
                self.not_dispatched_commit_unknown_once = false;
                let recovery_attempt = if self.wrong_not_dispatched_recovery_attempt {
                    expected_attempt_count.saturating_add(1)
                } else {
                    expected_attempt_count
                };
                return Err(CancellationStoreError::CommitUnknown {
                    operation: "record_cancel_not_dispatched",
                    recovery: Box::new(CommitRecoveryKey::CancelNotDispatched {
                        cancel_outbox_id: Self::CANCEL_OUTBOX_ID,
                        state_event_id: Uuid::from_u128(15),
                        attempt_count: recovery_attempt,
                        evidence_hash: evidence.evidence_hash,
                    }),
                });
            }
            Ok(true)
        }

        async fn finalize_cancel_outbox(
            &mut self,
            _cancel_outbox_id: Uuid,
            _lease: &FencedLease,
            terminal_broker_event_id: Uuid,
            _completion_reason: &str,
        ) -> Result<bool, CancellationStoreError> {
            if self
                .terminal_broker_evidence
                .as_ref()
                .is_none_or(|evidence| evidence.broker_event_id != terminal_broker_event_id)
            {
                return Ok(false);
            }
            self.finalize_calls += 1;
            self.state = FakeCancelState::Terminal;
            self.log.lock().unwrap().push("finalize".into());
            Ok(true)
        }

        async fn discover_unresolved_cancels(
            &mut self,
            _lease: &FencedLease,
            _limit: u16,
        ) -> Result<Vec<UnresolvedCancelOutbox>, CancellationStoreError> {
            Ok(self.unresolved().into_iter().collect())
        }

        async fn resolve_commit(
            &mut self,
            key: &CommitRecoveryKey,
        ) -> Result<CommitResolution, CancellationStoreError> {
            self.resolve_calls += 1;
            Ok(match key {
                CommitRecoveryKey::CancelRequestAccepted { .. }
                    if self.state == FakeCancelState::Accepted =>
                {
                    CommitResolution::Committed
                }
                CommitRecoveryKey::CancelUnknown { .. }
                    if self.state == FakeCancelState::Unknown =>
                {
                    CommitResolution::Committed
                }
                CommitRecoveryKey::CancelNotDispatched {
                    attempt_count,
                    evidence_hash,
                    ..
                } if self.state == FakeCancelState::NotDispatched
                    && *attempt_count == self.attempt_count
                    && self
                        .not_dispatched_evidence
                        .as_ref()
                        .is_some_and(|evidence| evidence.evidence_hash == *evidence_hash) =>
                {
                    CommitResolution::Committed
                }
                CommitRecoveryKey::CancelFinalization { .. }
                    if self.state == FakeCancelState::Terminal =>
                {
                    CommitResolution::Committed
                }
                _ => CommitResolution::NotCommitted,
            })
        }
    }

    fn cancellation_lease(now: DateTime<Utc>) -> FencedLease {
        FencedLease {
            environment: Environment::Paper,
            account_fingerprint: HashDigest::sha256("paper-account"),
            owner_id: Uuid::from_u128(21),
            fencing_token: 7,
            lease_until: now + chrono::Duration::minutes(1),
        }
    }

    fn cancellation_acceptance(now: DateTime<Utc>) -> CancellationOutcome {
        CancellationOutcome::RequestAccepted(CancellationRequestAccepted {
            provider_order_id: "provider-1".into(),
            accepted_at: now,
            request_id: "request-cancel".into(),
            raw_payload_hash: HashDigest::sha256("cancel-204"),
        })
    }

    fn cancellation_not_dispatched(now: DateTime<Utc>) -> CancellationOutcome {
        let provider_order_id = "provider-1".to_owned();
        let reason_code = "TRANSPORT_BEFORE_SEND".to_owned();
        let detail = "HTTP request budget denied dispatch".to_owned();
        let evidence_hash = HashDigest::of_json(&serde_json::json!({
            "provider_order_id": &provider_order_id,
            "observed_at": now,
            "reason_code": &reason_code,
            "detail": &detail,
        }))
        .unwrap();
        CancellationOutcome::NotDispatched(CancellationNotDispatched {
            provider_order_id,
            observed_at: now,
            reason_code,
            detail,
            evidence_hash,
        })
    }

    fn cancellation_broker_event(now: DateTime<Utc>, status: &str) -> BrokerEvent {
        BrokerEvent {
            provider_order_id: Some("provider-1".into()),
            client_order_id: "client-1".into(),
            status: status.into(),
            filled_quantity: WholeQuantity::ZERO,
            fill_price: None,
            provider_timestamp: now,
            received_at: now,
            raw_payload_hash: HashDigest::sha256(status),
            request_id: Some("request-get".into()),
        }
    }

    fn intent(now: DateTime<Utc>) -> OrderIntent {
        OrderIntent {
            intent_id: "intent-1".into(),
            client_order_id: "stable-client-1".into(),
            release_id: "release-1".into(),
            decision_id: "decision-1".into(),
            symbol: Symbol::new("TEST").unwrap(),
            side: OrderSide::Buy,
            quantity: WholeQuantity::new(1),
            limit_price: "10".parse::<Price>().unwrap(),
            decision_at: now - chrono::Duration::seconds(2),
            arrival_quote: "10".parse::<Price>().unwrap(),
            quote_provider_at: now - chrono::Duration::seconds(1),
            quote_received_at: now - chrono::Duration::seconds(1),
            quote_valid_until: now + chrono::Duration::seconds(10),
            quote_payload_hash: HashDigest::sha256("quote"),
            time_in_force: TimeInForce::Day,
            decision_evidence_hash: HashDigest::sha256("decision"),
            materialization_evidence_hash: HashDigest::sha256("materialization"),
            created_at: now - chrono::Duration::seconds(1),
        }
    }

    fn session_permit(now: DateTime<Utc>) -> RegularTradingSessionPermit {
        session_permit_with(now, now, now + chrono::Duration::hours(5))
    }

    fn session_permit_with(
        now: DateTime<Utc>,
        verified_at: DateTime<Utc>,
        close: DateTime<Utc>,
    ) -> RegularTradingSessionPermit {
        RegularTradingSessionPermit::verified(
            "NYSE".into(),
            now.date_naive(),
            now - chrono::Duration::hours(1),
            close,
            verified_at - chrono::Duration::seconds(1),
            verified_at,
            HashDigest::sha256("clock"),
            HashDigest::sha256("calendar"),
            Some("clock-request".into()),
            Some("calendar-request".into()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn ambiguous_submit_is_never_blindly_resubmitted() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let broker = AmbiguousBroker::default();
        let counters = broker.clone();
        let executor = Executor::new(broker);
        let mut ledger = ExecutionLedger::default();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let result = executor
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &session_permit(now),
                now,
            )
            .await
            .unwrap();
        let DispatchResult::SubmissionUnknown {
            outbox_sequence, ..
        } = result
        else {
            panic!("expected ambiguous result")
        };
        assert!(executor
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &session_permit(now),
                now,
            )
            .await
            .is_err());
        assert_eq!(*counters.submissions.lock().unwrap(), 1);

        let recovery_authority = RecoveryAuthorityDecision::test_authorized(
            trader_core::Environment::Paper,
            account,
            2,
            now,
            now + chrono::Duration::minutes(1),
        );
        let recovered = executor
            .recover_submission_unknown(
                &mut ledger,
                &recovery_authority,
                outbox_sequence,
                "stable-client-1",
                "owner-2",
                2,
                now,
            )
            .await
            .unwrap();
        assert!(matches!(
            recovered,
            DispatchResult::LookupStillUnresolved { .. }
        ));
        assert_eq!(*counters.submissions.lock().unwrap(), 1);
        assert_eq!(*counters.lookups.lock().unwrap(), 1);
        assert_eq!(ledger.outbox()[0].created_fencing_token, 1);
        assert_eq!(ledger.outbox()[0].claim_fencing_token, Some(2));
    }

    #[tokio::test]
    async fn mismatched_observed_event_becomes_lookup_only_recovery() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(WrongClientBroker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &session_permit(now),
                now,
            )
            .await;
        assert!(matches!(
            result,
            Ok(DispatchResult::SubmissionUnknown { .. })
        ));
        assert_eq!(ledger.outbox()[0].completed_at, None);
        assert!(!ledger
            .records()
            .iter()
            .any(|record| matches!(&record.event, ExecutionEvent::BrokerEventObserved { .. })));
        assert!(ledger.project_orders().unwrap()["stable-client-1"].requires_client_id_lookup());
    }

    #[tokio::test]
    async fn invalid_matching_event_becomes_lookup_only_recovery() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(InvalidStatusBroker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &session_permit(now),
                now,
            )
            .await;

        assert!(matches!(
            result,
            Ok(DispatchResult::SubmissionUnknown { .. })
        ));
        assert_eq!(ledger.outbox()[0].completed_at, None);
        assert!(!ledger
            .records()
            .iter()
            .any(|record| matches!(&record.event, ExecutionEvent::BrokerEventObserved { .. })));
        assert!(ledger.project_orders().unwrap()["stable-client-1"].requires_client_id_lookup());
    }

    #[tokio::test]
    async fn authority_cannot_be_replayed_for_another_account() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let broker = AmbiguousBroker::default();
        let counters = broker.clone();
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            HashDigest::sha256("authorized-account"),
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let wrong_outbox = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: HashDigest::sha256("different-account"),
            created_fencing_token: 1,
        };
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(broker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &wrong_outbox,
                "owner-1",
                1,
                intent(now),
                &session_permit(now),
                now,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(*counters.submissions.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn authority_cannot_be_replayed_for_another_release() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let broker = AmbiguousBroker::default();
        let counters = broker.clone();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let mut wrong_release_intent = intent(now);
        wrong_release_intent.release_id = "release-2".into();
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(broker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                wrong_release_intent,
                &session_permit(now),
                now,
            )
            .await;

        assert!(result.is_err());
        assert!(ledger.records().is_empty());
        assert_eq!(*counters.submissions.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn dispatch_deadline_is_minimum_of_quote_expiry_and_exact_session_close() {
        let now: DateTime<Utc> = "2026-07-18T19:59:50Z".parse().unwrap();
        let broker = AmbiguousBroker::default();
        let counters = broker.clone();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let close = now + chrono::Duration::seconds(5);
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(broker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &session_permit_with(now, now, close),
                now,
            )
            .await
            .unwrap();

        assert!(matches!(result, DispatchResult::SubmissionUnknown { .. }));
        assert_eq!(counters.deadlines.lock().unwrap().as_slice(), &[close]);
    }

    #[tokio::test]
    async fn stale_or_pre_quote_session_permit_blocks_before_ledger_or_broker() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let broker = AmbiguousBroker::default();
        let counters = broker.clone();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let stale = session_permit_with(
            now,
            now - chrono::Duration::seconds(16),
            now + chrono::Duration::hours(5),
        );
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(broker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &stale,
                now,
            )
            .await;

        assert!(matches!(result, Err(ExecutionError::AuthorityDenied(_))));
        assert!(ledger.records().is_empty());
        assert_eq!(*counters.submissions.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn insufficient_broker_arrival_allowance_skips_before_intent_commit() {
        let now: DateTime<Utc> = "2026-07-18T14:00:00Z".parse().unwrap();
        let broker = AmbiguousBroker {
            reject_submission_window: true,
            ..AmbiguousBroker::default()
        };
        let counters = broker.clone();
        let account = HashDigest::sha256("paper-account");
        let authority = AuthorityDecision::test_enabled(
            trader_core::Environment::Paper,
            account,
            "release-1",
            1,
            now,
            now + chrono::Duration::minutes(1),
        );
        let outbox_authority = OutboxAuthority {
            environment: trader_core::Environment::Paper,
            account_fingerprint: account,
            created_fencing_token: 1,
        };
        let mut ledger = ExecutionLedger::default();
        let result = Executor::new(broker)
            .commit_and_dispatch(
                &mut ledger,
                &authority,
                &outbox_authority,
                "owner-1",
                1,
                intent(now),
                &session_permit(now),
                now,
            )
            .await;

        assert!(matches!(result, Err(ExecutionError::AuthorityDenied(_))));
        assert!(ledger.records().is_empty());
        assert!(ledger.outbox().is_empty());
        assert_eq!(*counters.submissions.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn cancellation_commits_and_claims_before_delete_and_recovers_accepted_commit() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        store.accepted_commit_unknown_once = true;
        let broker = CancellationBroker::new(log.clone(), cancellation_acceptance(now), None);
        let counters = broker.clone();

        let result = Executor::new(broker)
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .unwrap();

        assert!(matches!(
            result,
            CancellationProgress::RequestAccepted { .. }
        ));
        assert_eq!(store.state, FakeCancelState::Accepted);
        assert_eq!(store.resolve_calls, 1);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);
        assert_eq!(
            *log.lock().unwrap(),
            ["persist", "claim", "delete", "record_accepted"]
        );
        assert_eq!(store.finalize_calls, 0, "HTTP 204 is not terminal truth");
    }

    #[tokio::test]
    async fn before_send_is_durable_then_restart_retries_exactly_one_delete() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        store.not_dispatched_commit_unknown_once = true;
        let broker = CancellationBroker::with_cancellations(
            log.clone(),
            vec![
                cancellation_not_dispatched(now),
                cancellation_acceptance(now),
            ],
            None,
        );
        let counters = broker.clone();

        let first = Executor::new(broker.clone())
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .unwrap();
        assert!(matches!(
            first,
            CancellationProgress::RetryEligible {
                completed_attempt_count: 1,
                ..
            }
        ));
        assert_eq!(store.state, FakeCancelState::NotDispatched);
        assert_eq!(store.attempt_count, 1);
        assert_eq!(store.resolve_calls, 1);
        assert_eq!(*counters.cancel_calls.lock().unwrap(), 1);
        assert_eq!(
            *counters.delete_calls.lock().unwrap(),
            0,
            "BeforeSend is proof that no DELETE reached the network"
        );

        let recovered = Executor::new(broker.clone())
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [CancellationProgress::RequestAccepted { .. }]
        ));
        assert_eq!(store.state, FakeCancelState::Accepted);
        assert_eq!(store.attempt_count, 2);
        assert_eq!(*counters.cancel_calls.lock().unwrap(), 2);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);

        let unresolved = Executor::new(broker)
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            unresolved.as_slice(),
            [CancellationProgress::LookupStillUnresolved { .. }]
        ));
        assert_eq!(*counters.cancel_calls.lock().unwrap(), 2);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);
        assert_eq!(*counters.provider_lookups.lock().unwrap(), 1);
        assert_eq!(
            *log.lock().unwrap(),
            [
                "persist",
                "claim",
                "before_send",
                "record_not_dispatched",
                "retry_claim",
                "delete",
                "record_accepted",
                "recovery_claim",
                "get",
            ]
        );
    }

    #[tokio::test]
    async fn stale_not_dispatched_attempt_and_wrong_recovery_key_fail_closed() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let lease = cancellation_lease(now);

        let log = Arc::new(Mutex::new(Vec::new()));
        let mut stale_store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        stale_store.claimed_attempt_count_override = Some(2);
        let stale_broker = CancellationBroker::new(
            log,
            cancellation_not_dispatched(now),
            Some(cancellation_broker_event(now, "pending_cancel")),
        );
        let stale_counters = stale_broker.clone();
        assert!(Executor::new(stale_broker.clone())
            .persist_and_dispatch_cancel(
                &mut stale_store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .is_err());
        assert_eq!(stale_store.state, FakeCancelState::DispatchStarted);
        assert_eq!(*stale_counters.delete_calls.lock().unwrap(), 0);
        let recovery = Executor::new(stale_broker)
            .recover_unresolved_cancellations(&mut stale_store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            recovery.as_slice(),
            [CancellationProgress::BrokerStateObserved { .. }]
        ));
        assert_eq!(*stale_counters.delete_calls.lock().unwrap(), 0);
        assert_eq!(*stale_counters.provider_lookups.lock().unwrap(), 1);

        let log = Arc::new(Mutex::new(Vec::new()));
        let mut wrong_key_store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        wrong_key_store.not_dispatched_commit_unknown_once = true;
        wrong_key_store.wrong_not_dispatched_recovery_attempt = true;
        let wrong_key_broker = CancellationBroker::new(log, cancellation_not_dispatched(now), None);
        let wrong_key_counters = wrong_key_broker.clone();
        assert!(Executor::new(wrong_key_broker)
            .persist_and_dispatch_cancel(
                &mut wrong_key_store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .is_err());
        assert_eq!(wrong_key_store.state, FakeCancelState::NotDispatched);
        assert_eq!(wrong_key_store.resolve_calls, 0);
        assert_eq!(*wrong_key_counters.delete_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn restart_dispatches_eligible_cancel_once_after_pre_claim_failure() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        store.fail_claim_before_transition = true;
        let broker = CancellationBroker::new(log, cancellation_acceptance(now), None);
        let counters = broker.clone();
        let executor = Executor::new(broker);

        assert!(executor
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .is_err());
        assert_eq!(store.state, FakeCancelState::Eligible);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 0);

        store.fail_claim_before_transition = false;
        let recovered = executor
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [CancellationProgress::RequestAccepted { .. }]
        ));
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn restart_after_durable_dispatch_marker_is_get_only() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        store.fail_claim_after_transition = true;
        let broker = CancellationBroker::new(
            log,
            cancellation_acceptance(now),
            Some(cancellation_broker_event(now, "pending_cancel")),
        );
        let counters = broker.clone();
        let executor = Executor::new(broker);

        assert!(executor
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .is_err());
        assert_eq!(store.state, FakeCancelState::DispatchStarted);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 0);

        store.fail_claim_after_transition = false;
        let recovered = executor
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [CancellationProgress::BrokerStateObserved {
                terminal: false,
                ..
            }]
        ));
        assert_eq!(*counters.delete_calls.lock().unwrap(), 0);
        assert_eq!(*counters.provider_lookups.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn crash_after_delete_recovers_by_get_and_terminal_needs_persisted_broker_event() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        store.fail_record_before_commit = true;
        let terminal = cancellation_broker_event(now, "canceled");
        let broker =
            CancellationBroker::new(log, cancellation_acceptance(now), Some(terminal.clone()));
        let counters = broker.clone();
        let executor = Executor::new(broker);

        assert!(executor
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .is_err());
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);
        assert_eq!(store.state, FakeCancelState::DispatchStarted);

        store.fail_record_before_commit = false;
        let recovered = executor
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [CancellationProgress::BrokerStateObserved { terminal: true, .. }]
        ));
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);
        assert_eq!(*counters.provider_lookups.lock().unwrap(), 1);

        assert!(executor
            .finalize_cancel_from_broker_truth(
                &mut store,
                &lease,
                FakeCancellationStore::CANCEL_OUTBOX_ID,
                FakeCancellationStore::TERMINAL_EVENT_ID,
                &terminal,
                now,
            )
            .await
            .is_err());
        assert_eq!(store.finalize_calls, 0);

        store.terminal_broker_evidence = Some(PersistedTerminalBrokerEvent {
            broker_event_id: FakeCancellationStore::TERMINAL_EVENT_ID,
            event: terminal.clone(),
        });
        let finalized = executor
            .finalize_cancel_from_broker_truth(
                &mut store,
                &lease,
                FakeCancellationStore::CANCEL_OUTBOX_ID,
                FakeCancellationStore::TERMINAL_EVENT_ID,
                &terminal,
                now,
            )
            .await
            .unwrap();
        assert!(matches!(
            finalized,
            CancellationProgress::TerminalFinalized { .. }
        ));
        assert_eq!(store.state, FakeCancelState::Terminal);
        assert_eq!(store.finalize_calls, 1);
    }

    #[tokio::test]
    async fn restart_auto_finalizes_persisted_terminal_event_without_broker_call() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log, lease.clone(), now);
        store.state = FakeCancelState::Accepted;
        store.attempt_count = 1;
        store.terminal_broker_evidence = Some(PersistedTerminalBrokerEvent {
            broker_event_id: FakeCancellationStore::TERMINAL_EVENT_ID,
            event: cancellation_broker_event(now, "canceled"),
        });

        // This new process has no retained terminal event and no planned
        // cancellation outcome. Recovery must use only the store projection.
        let broker = CancellationBroker::with_cancellations(
            Arc::new(Mutex::new(Vec::new())),
            Vec::new(),
            None,
        );
        let counters = broker.clone();
        let recovered = Executor::new(broker)
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();

        assert!(matches!(
            recovered.as_slice(),
            [CancellationProgress::TerminalFinalized {
                terminal_broker_event_id,
                provider_status,
                ..
            }] if *terminal_broker_event_id == FakeCancellationStore::TERMINAL_EVENT_ID
                && provider_status == "canceled"
        ));
        assert_eq!(store.state, FakeCancelState::Terminal);
        assert_eq!(store.finalize_calls, 1);
        assert_eq!(*counters.cancel_calls.lock().unwrap(), 0);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 0);
        assert_eq!(*counters.provider_lookups.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn ambiguous_delete_stays_unresolved_and_restart_never_resends() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let lease = cancellation_lease(now);
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        let broker = CancellationBroker::new(
            log,
            CancellationOutcome::Unknown {
                detail: "timeout after bytes may have been written".into(),
            },
            Some(cancellation_broker_event(now, "pending_cancel")),
        );
        let counters = broker.clone();
        let executor = Executor::new(broker);

        let first = executor
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .unwrap();
        assert!(matches!(first, CancellationProgress::OutcomeUnknown { .. }));
        assert_eq!(store.state, FakeCancelState::Unknown);

        let recovered = executor
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [CancellationProgress::BrokerStateObserved { .. }]
        ));
        assert_eq!(*counters.delete_calls.lock().unwrap(), 1);
        assert_eq!(*counters.provider_lookups.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn expired_fence_and_mismatched_recovery_identity_fail_closed() {
        let now: DateTime<Utc> = "2026-07-19T14:00:00Z".parse().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut lease = cancellation_lease(now);
        lease.lease_until = now;
        let mut store = FakeCancellationStore::new(log.clone(), lease.clone(), now);
        let broker = CancellationBroker::new(log, cancellation_acceptance(now), None);
        let counters = broker.clone();
        assert!(Executor::new(broker)
            .persist_and_dispatch_cancel(
                &mut store,
                &lease,
                "client-1",
                "provider-1",
                "RISK_EXIT",
                now,
            )
            .await
            .is_err());
        assert_eq!(store.state, FakeCancelState::Empty);
        assert_eq!(*counters.delete_calls.lock().unwrap(), 0);

        let lease = cancellation_lease(now);
        let mut store =
            FakeCancellationStore::new(Arc::new(Mutex::new(Vec::new())), lease.clone(), now);
        store.state = FakeCancelState::DispatchStarted;
        let mut wrong = cancellation_broker_event(now, "pending_cancel");
        wrong.client_order_id = "other-client".into();
        let broker = CancellationBroker::new(
            Arc::new(Mutex::new(Vec::new())),
            cancellation_acceptance(now),
            Some(wrong),
        );
        assert!(Executor::new(broker)
            .recover_unresolved_cancellations(&mut store, &lease, 10, now)
            .await
            .is_err());
        assert_eq!(store.state, FakeCancelState::DispatchStarted);
    }
}
