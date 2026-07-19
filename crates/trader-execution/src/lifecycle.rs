use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use trader_core::{BrokerEvent, HashDigest, OrderIntent, Price, WholeQuantity};

use crate::ExecutionError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerOrderStatus {
    Accepted,
    New,
    PendingNew,
    AcceptedForBidding,
    PartiallyFilled,
    Filled,
    DoneForDay,
    Canceled,
    Expired,
    Replaced,
    PendingCancel,
    PendingReplace,
    Stopped,
    Rejected,
    Suspended,
    Calculated,
    Unknown(String),
}

impl BrokerOrderStatus {
    pub fn from_provider(value: &str) -> Self {
        match value {
            "accepted" => Self::Accepted,
            "new" => Self::New,
            "pending_new" => Self::PendingNew,
            "accepted_for_bidding" => Self::AcceptedForBidding,
            "partially_filled" => Self::PartiallyFilled,
            "filled" => Self::Filled,
            "done_for_day" => Self::DoneForDay,
            "canceled" => Self::Canceled,
            "expired" => Self::Expired,
            "replaced" => Self::Replaced,
            "pending_cancel" => Self::PendingCancel,
            "pending_replace" => Self::PendingReplace,
            "stopped" => Self::Stopped,
            "rejected" => Self::Rejected,
            "suspended" => Self::Suspended,
            "calculated" => Self::Calculated,
            unknown => Self::Unknown(bounded_provider_detail(unknown)),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrderPhase {
    IntentCommitted,
    SubmissionInFlight,
    SubmissionUnknown { detail: String },
    BrokerWorking { status: String },
    PartiallyFilled,
    Filled,
    DoneForDay,
    Canceled,
    Expired,
    Replaced,
    Stopped,
    Rejected,
    Suspended,
    Calculated,
    FailClosedUnknown { provider_status: String },
}

impl OrderPhase {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled
                | Self::Canceled
                | Self::Expired
                | Self::Replaced
                | Self::Rejected
                | Self::FailClosedUnknown { .. }
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrderLifecycle {
    pub intent: OrderIntent,
    pub phase: OrderPhase,
    pub provider_order_id: Option<String>,
    pub filled_quantity: WholeQuantity,
    pub average_fill_price: Option<Price>,
    pub last_event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_provider_at: Option<DateTime<Utc>>,
    pub request_ids: BTreeSet<String>,
    pub seen_payload_hashes: BTreeSet<HashDigest>,
    #[serde(default)]
    last_observation: Option<BrokerObservation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct BrokerObservation {
    provider_order_id: String,
    status: String,
    filled_quantity: WholeQuantity,
    fill_price: Option<Price>,
}

impl BrokerObservation {
    fn from_event(event: &BrokerEvent) -> Result<Self, ExecutionError> {
        let provider_order_id = event.provider_order_id.clone().ok_or_else(|| {
            ExecutionError::Lifecycle("broker event omitted provider order identity".into())
        })?;
        Ok(Self {
            provider_order_id,
            status: event.status.clone(),
            filled_quantity: event.filled_quantity,
            fill_price: event.fill_price,
        })
    }
}

impl OrderLifecycle {
    pub fn committed(intent: OrderIntent) -> Self {
        Self {
            intent,
            phase: OrderPhase::IntentCommitted,
            provider_order_id: None,
            filled_quantity: WholeQuantity::ZERO,
            average_fill_price: None,
            last_event_at: None,
            last_provider_at: None,
            request_ids: BTreeSet::new(),
            seen_payload_hashes: BTreeSet::new(),
            last_observation: None,
        }
    }

    pub fn begin_submission(&mut self) -> Result<(), ExecutionError> {
        if self.phase != OrderPhase::IntentCommitted {
            return Err(ExecutionError::Lifecycle(
                "only a committed intent may begin its first submission".into(),
            ));
        }
        self.phase = OrderPhase::SubmissionInFlight;
        Ok(())
    }

    pub fn mark_submission_unknown(
        &mut self,
        detail: impl Into<String>,
    ) -> Result<(), ExecutionError> {
        if self.phase != OrderPhase::SubmissionInFlight {
            return Err(ExecutionError::Lifecycle(
                "ambiguous submission can only follow an in-flight submission".into(),
            ));
        }
        self.phase = OrderPhase::SubmissionUnknown {
            detail: bounded_provider_detail(&detail.into()),
        };
        Ok(())
    }

    /// SubmissionUnknown is deliberately not resubmittable. The caller must query
    /// the broker using the same client_order_id and apply the observed event.
    pub fn may_submit(&self) -> bool {
        self.phase == OrderPhase::IntentCommitted
    }

    pub fn requires_client_id_lookup(&self) -> bool {
        matches!(self.phase, OrderPhase::SubmissionUnknown { .. })
    }

    pub fn apply_broker_event(&mut self, event: &BrokerEvent) -> Result<(), ExecutionError> {
        if event.client_order_id != self.intent.client_order_id {
            return Err(ExecutionError::Lifecycle(
                "broker event client_order_id does not match committed intent".into(),
            ));
        }
        if self.phase == OrderPhase::IntentCommitted {
            return self.fail_closed("broker event arrived before submission began");
        }
        let observation = match BrokerObservation::from_event(event) {
            Ok(observation) => observation,
            Err(error) => {
                self.phase = OrderPhase::FailClosedUnknown {
                    provider_status: "missing_provider_order_id".into(),
                };
                return Err(error);
            }
        };
        if self
            .provider_order_id
            .as_ref()
            .is_some_and(|expected| *expected != observation.provider_order_id)
        {
            return self.fail_closed("provider order identity changed within one intent");
        }
        if event.provider_timestamp > event.received_at {
            return self.fail_closed("broker event provider timestamp is future-dated");
        }

        if self.last_observation.as_ref() == Some(&observation) {
            self.record_observation_evidence(event, observation);
            return Ok(());
        }
        if self.seen_payload_hashes.contains(&event.raw_payload_hash) {
            return self.fail_closed("identical broker payload produced contradictory semantics");
        }
        if self
            .last_provider_at
            .is_some_and(|last_provider| event.provider_timestamp < last_provider)
        {
            return Err(ExecutionError::Lifecycle(
                "older non-duplicate provider event quarantined".into(),
            ));
        }
        if self
            .last_provider_at
            .is_some_and(|last_provider| event.provider_timestamp == last_provider)
        {
            return self
                .fail_closed("same provider timestamp produced contradictory order semantics");
        }
        if self
            .last_event_at
            .is_some_and(|last_received| event.received_at < last_received)
        {
            return self.fail_closed("newer provider event regressed receive order");
        }
        if event.filled_quantity.get() > self.intent.quantity.get()
            || event.filled_quantity.get() < self.filled_quantity.get()
        {
            self.phase = OrderPhase::FailClosedUnknown {
                provider_status: "invalid_filled_quantity".into(),
            };
            return Err(ExecutionError::Lifecycle(
                "provider fill quantity violates monotonic/order bounds".into(),
            ));
        }
        if self.phase.terminal() {
            return self.fail_closed("contradictory event arrived after terminal state");
        }
        let status = BrokerOrderStatus::from_provider(&event.status);
        if let BrokerOrderStatus::Unknown(provider_status) = &status {
            self.phase = OrderPhase::FailClosedUnknown {
                provider_status: provider_status.clone(),
            };
            return Err(ExecutionError::Lifecycle(format!(
                "unknown provider order status: {}",
                bounded_provider_detail(&event.status)
            )));
        }
        if !transition_allowed(&self.phase, &status) {
            return self.fail_closed("provider order state violated the explicit transition graph");
        }
        if (event.filled_quantity == WholeQuantity::ZERO && event.fill_price.is_some())
            || (event.filled_quantity != WholeQuantity::ZERO
                && event
                    .fill_price
                    .is_none_or(|price| !price.fixed().is_positive()))
        {
            return self
                .fail_closed("filled quantity and positive average fill price are inconsistent");
        }
        self.phase = match status {
            BrokerOrderStatus::Accepted
            | BrokerOrderStatus::New
            | BrokerOrderStatus::PendingNew
            | BrokerOrderStatus::AcceptedForBidding
            | BrokerOrderStatus::PendingCancel
            | BrokerOrderStatus::PendingReplace => {
                if event.filled_quantity == WholeQuantity::ZERO {
                    OrderPhase::BrokerWorking {
                        status: event.status.clone(),
                    }
                } else {
                    OrderPhase::PartiallyFilled
                }
            }
            BrokerOrderStatus::PartiallyFilled => {
                if event.filled_quantity == WholeQuantity::ZERO
                    || event.filled_quantity == self.intent.quantity
                    || event
                        .fill_price
                        .is_none_or(|price| !price.fixed().is_positive())
                {
                    return self.fail_closed(
                        "partially_filled requires bounded quantity and positive fill price",
                    );
                }
                OrderPhase::PartiallyFilled
            }
            BrokerOrderStatus::Filled => {
                if event.filled_quantity != self.intent.quantity
                    || event
                        .fill_price
                        .is_none_or(|price| !price.fixed().is_positive())
                {
                    return self.fail_closed(
                        "filled status requires full quantity and positive fill price",
                    );
                }
                OrderPhase::Filled
            }
            BrokerOrderStatus::DoneForDay => OrderPhase::DoneForDay,
            BrokerOrderStatus::Canceled => OrderPhase::Canceled,
            BrokerOrderStatus::Expired => OrderPhase::Expired,
            BrokerOrderStatus::Replaced => OrderPhase::Replaced,
            BrokerOrderStatus::Stopped => OrderPhase::Stopped,
            BrokerOrderStatus::Rejected => OrderPhase::Rejected,
            BrokerOrderStatus::Suspended => OrderPhase::Suspended,
            BrokerOrderStatus::Calculated => OrderPhase::Calculated,
            BrokerOrderStatus::Unknown(_) => unreachable!("unknown status rejected above"),
        };
        self.filled_quantity = event.filled_quantity;
        if event.fill_price.is_some() {
            self.average_fill_price = event.fill_price;
        }
        self.record_observation_evidence(event, observation);
        Ok(())
    }

    fn record_observation_evidence(&mut self, event: &BrokerEvent, observation: BrokerObservation) {
        self.provider_order_id = Some(observation.provider_order_id.clone());
        self.last_event_at = Some(
            self.last_event_at
                .map_or(event.received_at, |current| current.max(event.received_at)),
        );
        self.last_provider_at = Some(
            self.last_provider_at
                .map_or(event.provider_timestamp, |current| {
                    current.max(event.provider_timestamp)
                }),
        );
        if let Some(request_id) = &event.request_id {
            self.request_ids.insert(request_id.clone());
        }
        self.seen_payload_hashes.insert(event.raw_payload_hash);
        self.last_observation = Some(observation);
    }

    fn fail_closed<T>(&mut self, detail: &str) -> Result<T, ExecutionError> {
        self.phase = OrderPhase::FailClosedUnknown {
            provider_status: detail.into(),
        };
        Err(ExecutionError::Lifecycle(detail.into()))
    }
}

fn transition_allowed(from: &OrderPhase, to: &BrokerOrderStatus) -> bool {
    match from {
        OrderPhase::SubmissionInFlight | OrderPhase::SubmissionUnknown { .. } => true,
        OrderPhase::BrokerWorking { status } => {
            transition_from_working(&BrokerOrderStatus::from_provider(status), to)
        }
        OrderPhase::PartiallyFilled => matches!(
            to,
            BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::PendingReplace
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Replaced
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Suspended
                | BrokerOrderStatus::Calculated
        ),
        OrderPhase::DoneForDay => matches!(
            to,
            BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Calculated
        ),
        OrderPhase::Stopped => matches!(
            to,
            BrokerOrderStatus::Stopped
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::Rejected
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Calculated
        ),
        OrderPhase::Suspended => matches!(
            to,
            BrokerOrderStatus::Suspended
                | BrokerOrderStatus::New
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Rejected
        ),
        OrderPhase::Calculated => matches!(
            to,
            BrokerOrderStatus::Calculated
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
        ),
        OrderPhase::IntentCommitted
        | OrderPhase::Filled
        | OrderPhase::Canceled
        | OrderPhase::Expired
        | OrderPhase::Replaced
        | OrderPhase::Rejected
        | OrderPhase::FailClosedUnknown { .. } => false,
    }
}

fn transition_from_working(from: &BrokerOrderStatus, to: &BrokerOrderStatus) -> bool {
    match from {
        BrokerOrderStatus::Accepted => matches!(
            to,
            BrokerOrderStatus::Accepted
                | BrokerOrderStatus::PendingNew
                | BrokerOrderStatus::New
                | BrokerOrderStatus::AcceptedForBidding
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::PendingReplace
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Replaced
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Rejected
                | BrokerOrderStatus::Suspended
                | BrokerOrderStatus::Calculated
        ),
        BrokerOrderStatus::PendingNew => matches!(
            to,
            BrokerOrderStatus::PendingNew
                | BrokerOrderStatus::New
                | BrokerOrderStatus::AcceptedForBidding
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::PendingReplace
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Replaced
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Rejected
                | BrokerOrderStatus::Suspended
                | BrokerOrderStatus::Calculated
        ),
        BrokerOrderStatus::AcceptedForBidding => matches!(
            to,
            BrokerOrderStatus::AcceptedForBidding
                | BrokerOrderStatus::New
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::PendingReplace
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Replaced
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Rejected
                | BrokerOrderStatus::Suspended
                | BrokerOrderStatus::Calculated
        ),
        BrokerOrderStatus::New => matches!(
            to,
            BrokerOrderStatus::New
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::PendingReplace
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Replaced
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Rejected
                | BrokerOrderStatus::Suspended
                | BrokerOrderStatus::Calculated
        ),
        BrokerOrderStatus::PendingCancel => matches!(
            to,
            BrokerOrderStatus::PendingCancel
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Calculated
        ),
        BrokerOrderStatus::PendingReplace => matches!(
            to,
            BrokerOrderStatus::PendingReplace
                | BrokerOrderStatus::New
                | BrokerOrderStatus::PartiallyFilled
                | BrokerOrderStatus::Filled
                | BrokerOrderStatus::DoneForDay
                | BrokerOrderStatus::Canceled
                | BrokerOrderStatus::Expired
                | BrokerOrderStatus::Replaced
                | BrokerOrderStatus::Stopped
                | BrokerOrderStatus::Calculated
        ),
        _ => false,
    }
}

fn bounded_provider_detail(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() || character == ' ' {
                character
            } else {
                '?'
            }
        })
        .take(128)
        .collect()
}

#[cfg(test)]
mod tests {
    use trader_core::{OrderSide, Symbol, TimeInForce};

    use super::*;

    fn intent(now: DateTime<Utc>) -> OrderIntent {
        OrderIntent {
            intent_id: "intent-1".into(),
            client_order_id: "client-1".into(),
            release_id: "release-1".into(),
            decision_id: "decision-1".into(),
            symbol: Symbol::new("SPY").unwrap(),
            side: OrderSide::Buy,
            quantity: WholeQuantity::new(1),
            limit_price: "500".parse().unwrap(),
            decision_at: now - chrono::Duration::seconds(2),
            arrival_quote: "500".parse().unwrap(),
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

    fn event(
        status: &str,
        filled_quantity: u64,
        fill_price: Option<&str>,
        provider_timestamp: DateTime<Utc>,
    ) -> BrokerEvent {
        BrokerEvent {
            provider_order_id: Some("provider-1".into()),
            client_order_id: "client-1".into(),
            status: status.into(),
            filled_quantity: WholeQuantity::new(filled_quantity),
            fill_price: fill_price.map(|price| price.parse().unwrap()),
            provider_timestamp,
            received_at: provider_timestamp,
            raw_payload_hash: HashDigest::sha256(format!(
                "{status}-{filled_quantity}-{provider_timestamp}"
            )),
            request_id: Some(format!("request-{status}")),
        }
    }

    #[test]
    fn provider_controlled_status_and_ambiguity_detail_are_sanitized_and_bounded() {
        let now: DateTime<Utc> = "2026-07-20T14:00:00Z".parse().unwrap();
        let mut lifecycle = OrderLifecycle::committed(intent(now));
        lifecycle.begin_submission().unwrap();
        let unsafe_status = format!("future\n{}", "x".repeat(1_000));
        let error = lifecycle
            .apply_broker_event(&BrokerEvent {
                provider_order_id: Some("provider-1".into()),
                client_order_id: "client-1".into(),
                status: unsafe_status,
                filled_quantity: WholeQuantity::ZERO,
                fill_price: None,
                provider_timestamp: now,
                received_at: now,
                raw_payload_hash: HashDigest::sha256("event"),
                request_id: None,
            })
            .unwrap_err();
        let rendered = error.to_string();
        assert!(!rendered.contains('\n'));
        assert!(rendered.len() < 200);

        let mut ambiguous = OrderLifecycle::committed(intent(now));
        ambiguous.begin_submission().unwrap();
        ambiguous
            .mark_submission_unknown(format!("timeout\n{}", "y".repeat(1_000)))
            .unwrap();
        let OrderPhase::SubmissionUnknown { detail } = ambiguous.phase else {
            panic!("expected submission unknown")
        };
        assert!(!detail.contains('\n'));
        assert!(detail.len() <= 128);
    }

    #[test]
    fn semantically_identical_terminal_observations_merge_new_evidence() {
        let now: DateTime<Utc> = "2026-07-20T14:00:00Z".parse().unwrap();
        let mut lifecycle = OrderLifecycle::committed(intent(now));
        lifecycle.begin_submission().unwrap();
        let filled = event("filled", 1, Some("500"), now + chrono::Duration::seconds(1));
        lifecycle.apply_broker_event(&filled).unwrap();

        let mut repeated = filled.clone();
        repeated.provider_timestamp += chrono::Duration::seconds(1);
        repeated.received_at += chrono::Duration::seconds(2);
        repeated.raw_payload_hash = HashDigest::sha256("different-response-envelope");
        repeated.request_id = Some("request-second-observation".into());
        lifecycle.apply_broker_event(&repeated).unwrap();

        assert_eq!(lifecycle.phase, OrderPhase::Filled);
        assert_eq!(
            lifecycle.last_provider_at,
            Some(repeated.provider_timestamp)
        );
        assert!(lifecycle.request_ids.contains("request-second-observation"));
        assert!(lifecycle
            .seen_payload_hashes
            .contains(&repeated.raw_payload_hash));
    }

    #[test]
    fn older_nonduplicate_provider_event_is_quarantined_without_state_regression() {
        let now: DateTime<Utc> = "2026-07-20T14:00:00Z".parse().unwrap();
        let mut lifecycle = OrderLifecycle::committed(intent(now));
        lifecycle.begin_submission().unwrap();
        let current = event("new", 0, None, now + chrono::Duration::seconds(2));
        lifecycle.apply_broker_event(&current).unwrap();

        let mut stale = event("accepted", 0, None, now + chrono::Duration::seconds(1));
        stale.received_at = now + chrono::Duration::seconds(3);
        assert!(lifecycle.apply_broker_event(&stale).is_err());
        assert_eq!(
            lifecycle.phase,
            OrderPhase::BrokerWorking {
                status: "new".into()
            }
        );
        assert_eq!(lifecycle.last_provider_at, Some(current.provider_timestamp));
    }

    #[test]
    fn same_provider_time_contradiction_and_partial_fill_regression_fail_closed() {
        let now: DateTime<Utc> = "2026-07-20T14:00:00Z".parse().unwrap();
        let mut same_time = OrderLifecycle::committed(intent(now));
        same_time.begin_submission().unwrap();
        same_time
            .apply_broker_event(&event("accepted", 0, None, now))
            .unwrap();
        assert!(same_time
            .apply_broker_event(&event("new", 0, None, now))
            .is_err());
        assert!(matches!(
            same_time.phase,
            OrderPhase::FailClosedUnknown { .. }
        ));

        let mut partial_intent = intent(now);
        partial_intent.quantity = WholeQuantity::new(2);
        let mut regression = OrderLifecycle::committed(partial_intent);
        regression.begin_submission().unwrap();
        regression
            .apply_broker_event(&event(
                "partially_filled",
                1,
                Some("500"),
                now + chrono::Duration::seconds(1),
            ))
            .unwrap();
        assert!(regression
            .apply_broker_event(&event(
                "new",
                1,
                Some("500"),
                now + chrono::Duration::seconds(2),
            ))
            .is_err());
        assert!(matches!(
            regression.phase,
            OrderPhase::FailClosedUnknown { .. }
        ));
    }
}
