use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use trader_core::{BrokerEvent, OrderIntent};

use crate::{
    authority::{AuthorityDecision, RecoveryAuthorityDecision},
    ledger::{ExecutionEvent, ExecutionLedger, OutboxAuthority},
    port::{BrokerPort, SubmissionOutcome},
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

        match self.broker.submit_committed_intent(&intent).await {
            Ok(SubmissionOutcome::Observed(event)) => {
                if let Err(error) =
                    append_validated_broker_event(ledger, &intent.client_order_id, event, now)
                {
                    // The POST reached the broker but its response cannot prove the
                    // resulting order identity/state. Preserve lookup-only recovery
                    // instead of returning with the lifecycle stuck in-flight.
                    ledger.append(
                        ExecutionEvent::SubmissionUnknown {
                            client_order_id: intent.client_order_id.clone(),
                            detail: format!("post-submit response validation failed: {error}"),
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
            Ok(SubmissionOutcome::Unknown { detail }) | Err(ExecutionError::Broker(detail)) => {
                ledger.append(
                    ExecutionEvent::SubmissionUnknown {
                        client_order_id: intent.client_order_id.clone(),
                        detail,
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
                        detail: error.to_string(),
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
        match self.broker.find_order_by_client_id(client_order_id).await? {
            Some(event) => {
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
        AccountSnapshot, HashDigest, OrderSide, Price, Symbol, TimeInForce, WholeQuantity,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct AmbiguousBroker {
        submissions: Arc<Mutex<u32>>,
        lookups: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl BrokerPort for AmbiguousBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn find_order_by_client_id(
            &self,
            _client_order_id: &str,
        ) -> Result<Option<trader_core::BrokerEvent>, ExecutionError> {
            *self.lookups.lock().unwrap() += 1;
            Ok(None)
        }

        async fn submit_committed_intent(
            &self,
            _intent: &OrderIntent,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            *self.submissions.lock().unwrap() += 1;
            Ok(SubmissionOutcome::Unknown {
                detail: "timeout".into(),
            })
        }

        async fn cancel_order(&self, _provider_order_id: &str) -> Result<(), ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }
    }

    struct WrongClientBroker;

    #[async_trait]
    impl BrokerPort for WrongClientBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn find_order_by_client_id(
            &self,
            _client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            Ok(None)
        }

        async fn submit_committed_intent(
            &self,
            intent: &OrderIntent,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            Ok(SubmissionOutcome::Observed(BrokerEvent {
                provider_order_id: Some("provider-1".into()),
                client_order_id: "wrong-client-id".into(),
                status: "accepted".into(),
                filled_quantity: WholeQuantity::ZERO,
                fill_price: None,
                provider_timestamp: intent.created_at,
                received_at: intent.created_at,
                raw_payload_hash: HashDigest::sha256("wrong-event"),
                request_id: Some("request-1".into()),
            }))
        }

        async fn cancel_order(&self, _provider_order_id: &str) -> Result<(), ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }
    }

    struct InvalidStatusBroker;

    #[async_trait]
    impl BrokerPort for InvalidStatusBroker {
        async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
        }

        async fn find_order_by_client_id(
            &self,
            _client_order_id: &str,
        ) -> Result<Option<BrokerEvent>, ExecutionError> {
            Ok(None)
        }

        async fn submit_committed_intent(
            &self,
            intent: &OrderIntent,
        ) -> Result<SubmissionOutcome, ExecutionError> {
            Ok(SubmissionOutcome::Observed(BrokerEvent {
                provider_order_id: Some("provider-1".into()),
                client_order_id: intent.client_order_id.clone(),
                status: "future_provider_status".into(),
                filled_quantity: WholeQuantity::ZERO,
                fill_price: None,
                provider_timestamp: intent.created_at,
                received_at: intent.created_at,
                raw_payload_hash: HashDigest::sha256("invalid-event"),
                request_id: Some("request-invalid".into()),
            }))
        }

        async fn cancel_order(&self, _provider_order_id: &str) -> Result<(), ExecutionError> {
            Err(ExecutionError::Broker("not used".into()))
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
                now,
            )
            .await;

        assert!(result.is_err());
        assert!(ledger.records().is_empty());
        assert_eq!(*counters.submissions.lock().unwrap(), 0);
    }
}
