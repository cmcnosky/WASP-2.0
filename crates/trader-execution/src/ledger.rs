use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use trader_core::{
    BrokerEvent, Environment, HashDigest, KillState, OrderIntent, ReconciliationReport,
};

use crate::{lifecycle::OrderLifecycle, ExecutionError};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionEvent {
    IntentCommitted {
        intent: OrderIntent,
    },
    SubmissionStarted {
        client_order_id: String,
    },
    SubmissionUnknown {
        client_order_id: String,
        detail: String,
    },
    BrokerEventObserved {
        event: BrokerEvent,
    },
    ReconciliationRecorded {
        report: ReconciliationReport,
    },
    KillStateChanged {
        state: KillState,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerRecord {
    pub sequence: u64,
    pub recorded_at: DateTime<Utc>,
    pub event: ExecutionEvent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxMessage {
    pub outbox_id: String,
    pub sequence: u64,
    pub intent_id: String,
    pub environment: Environment,
    pub account_fingerprint: HashDigest,
    /// Immutable authority token recorded atomically with the intent.
    pub created_fencing_token: u64,
    /// Mutable claim token: a later reconciled lease may safely recover unsent work.
    pub claim_fencing_token: Option<u64>,
    pub topic: String,
    pub payload_hash: HashDigest,
    pub available_at: DateTime<Utc>,
    pub claimed_by: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub attempt_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxAuthority {
    pub environment: Environment,
    pub account_fingerprint: HashDigest,
    pub created_fencing_token: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IntentIndexEntry {
    intent_id: String,
    payload_hash: HashDigest,
}

#[derive(Clone, Debug, Default)]
pub struct ExecutionLedger {
    records: Vec<LedgerRecord>,
    outbox: Vec<OutboxMessage>,
    client_intents: BTreeMap<String, IntentIndexEntry>,
}

impl ExecutionLedger {
    pub fn records(&self) -> &[LedgerRecord] {
        &self.records
    }

    pub fn outbox(&self) -> &[OutboxMessage] {
        &self.outbox
    }

    pub fn outbox_message(&self, sequence: u64) -> Option<&OutboxMessage> {
        self.outbox
            .iter()
            .find(|message| message.sequence == sequence)
    }

    /// Models one database transaction: append intent and its outbox notification
    /// together before any network submission is authorized.
    pub fn commit_intent(
        &mut self,
        intent: OrderIntent,
        authority: &OutboxAuthority,
        recorded_at: DateTime<Utc>,
    ) -> Result<u64, ExecutionError> {
        if !matches!(
            authority.environment,
            Environment::Paper | Environment::Live
        ) || authority.created_fencing_token == 0
        {
            return Err(ExecutionError::LedgerInvariant(
                "outbox requires paper/live environment and a positive creation fence".into(),
            ));
        }
        let payload_hash = HashDigest::of_json(&intent)?;
        if let Some(existing) = self.client_intents.get(&intent.client_order_id) {
            if existing.intent_id == intent.intent_id && existing.payload_hash == payload_hash {
                return self
                    .records
                    .iter()
                    .find_map(|record| match &record.event {
                        ExecutionEvent::IntentCommitted { intent: existing }
                            if existing.intent_id == intent.intent_id =>
                        {
                            Some(record.sequence)
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        ExecutionError::LedgerInvariant("intent index is corrupt".into())
                    });
            }
            return Err(ExecutionError::LedgerInvariant(
                "duplicate intent identity has non-identical payload bytes".into(),
            ));
        }
        let sequence = self.next_sequence()?;
        self.records.push(LedgerRecord {
            sequence,
            recorded_at,
            event: ExecutionEvent::IntentCommitted {
                intent: intent.clone(),
            },
        });
        self.outbox.push(OutboxMessage {
            outbox_id: format!("outbox-{}", intent.intent_id),
            sequence,
            intent_id: intent.intent_id.clone(),
            environment: authority.environment,
            account_fingerprint: authority.account_fingerprint,
            created_fencing_token: authority.created_fencing_token,
            claim_fencing_token: None,
            topic: "intent_committed".into(),
            payload_hash,
            available_at: recorded_at,
            claimed_by: None,
            claimed_at: None,
            completed_at: None,
            last_error: None,
            attempt_count: 0,
        });
        self.client_intents.insert(
            intent.client_order_id,
            IntentIndexEntry {
                intent_id: intent.intent_id,
                payload_hash,
            },
        );
        Ok(sequence)
    }

    pub fn append(
        &mut self,
        event: ExecutionEvent,
        recorded_at: DateTime<Utc>,
    ) -> Result<u64, ExecutionError> {
        let sequence = self.next_sequence()?;
        self.records.push(LedgerRecord {
            sequence,
            recorded_at,
            event,
        });
        Ok(sequence)
    }

    pub fn claim_outbox(
        &mut self,
        sequence: u64,
        owner_id: impl Into<String>,
        claim_fencing_token: u64,
        claimed_at: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        if claim_fencing_token == 0 {
            return Err(ExecutionError::LedgerInvariant(
                "claim fencing token must be positive".into(),
            ));
        }
        let message = self
            .outbox
            .iter_mut()
            .find(|message| message.sequence == sequence)
            .ok_or_else(|| ExecutionError::LedgerInvariant("unknown outbox sequence".into()))?;
        if message.completed_at.is_some() {
            return Err(ExecutionError::LedgerInvariant(
                "completed outbox item cannot be reclaimed".into(),
            ));
        }
        if claim_fencing_token < message.created_fencing_token {
            return Err(ExecutionError::LedgerInvariant(
                "claim fence predates immutable creation fence".into(),
            ));
        }
        let owner_id = owner_id.into();
        if let Some(existing_token) = message.claim_fencing_token {
            if existing_token == claim_fencing_token
                && message.claimed_by.as_deref() == Some(owner_id.as_str())
            {
                return Ok(());
            }
            if claim_fencing_token <= existing_token {
                return Err(ExecutionError::LedgerInvariant(
                    "outbox claim fence must advance monotonically".into(),
                ));
            }
        }
        message.claimed_by = Some(owner_id);
        message.claimed_at = Some(claimed_at);
        message.claim_fencing_token = Some(claim_fencing_token);
        message.attempt_count = message
            .attempt_count
            .checked_add(1)
            .ok_or_else(|| ExecutionError::LedgerInvariant("outbox attempt overflow".into()))?;
        message.last_error = None;
        Ok(())
    }

    pub fn mark_outbox_completed(
        &mut self,
        sequence: u64,
        owner_id: &str,
        claim_fencing_token: u64,
        completed_at: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        let message = self
            .outbox
            .iter_mut()
            .find(|message| message.sequence == sequence)
            .ok_or_else(|| ExecutionError::LedgerInvariant("unknown outbox sequence".into()))?;
        if message.claimed_by.as_deref() != Some(owner_id)
            || message.claim_fencing_token != Some(claim_fencing_token)
        {
            return Err(ExecutionError::LedgerInvariant(
                "outbox completion does not own the current claim fence".into(),
            ));
        }
        if message.completed_at.is_some() {
            return Err(ExecutionError::LedgerInvariant(
                "outbox completion is immutable and already recorded".into(),
            ));
        }
        message.completed_at = Some(completed_at);
        Ok(())
    }

    pub fn project_orders(&self) -> Result<BTreeMap<String, OrderLifecycle>, ExecutionError> {
        let mut orders = BTreeMap::new();
        for record in &self.records {
            match &record.event {
                ExecutionEvent::IntentCommitted { intent } => {
                    orders.insert(
                        intent.client_order_id.clone(),
                        OrderLifecycle::committed(intent.clone()),
                    );
                }
                ExecutionEvent::SubmissionStarted { client_order_id } => {
                    order_mut(&mut orders, client_order_id)?.begin_submission()?
                }
                ExecutionEvent::SubmissionUnknown {
                    client_order_id,
                    detail,
                } => order_mut(&mut orders, client_order_id)?
                    .mark_submission_unknown(detail.clone())?,
                ExecutionEvent::BrokerEventObserved { event } => {
                    order_mut(&mut orders, &event.client_order_id)?.apply_broker_event(event)?
                }
                ExecutionEvent::ReconciliationRecorded { .. }
                | ExecutionEvent::KillStateChanged { .. } => {}
            }
        }
        Ok(orders)
    }

    fn next_sequence(&self) -> Result<u64, ExecutionError> {
        u64::try_from(self.records.len())
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| ExecutionError::LedgerInvariant("ledger sequence overflow".into()))
    }
}

fn order_mut<'a>(
    orders: &'a mut BTreeMap<String, OrderLifecycle>,
    client_order_id: &str,
) -> Result<&'a mut OrderLifecycle, ExecutionError> {
    orders.get_mut(client_order_id).ok_or_else(|| {
        ExecutionError::LedgerInvariant(format!(
            "event references uncommitted order {client_order_id}"
        ))
    })
}
