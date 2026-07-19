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
            unknown => Self::Unknown(unknown.into()),
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
    pub request_ids: BTreeSet<String>,
    pub seen_payload_hashes: BTreeSet<HashDigest>,
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
            request_ids: BTreeSet::new(),
            seen_payload_hashes: BTreeSet::new(),
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
            detail: detail.into(),
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
        if self.seen_payload_hashes.contains(&event.raw_payload_hash) {
            return Ok(());
        }
        if self.phase == OrderPhase::IntentCommitted {
            return self.fail_closed("broker event arrived before submission began");
        }
        if let (Some(expected), Some(observed)) =
            (&self.provider_order_id, &event.provider_order_id)
        {
            if observed != expected {
                return self.fail_closed("provider order identity changed within one intent");
            }
        }
        if event.provider_timestamp > event.received_at
            || self
                .last_event_at
                .is_some_and(|last_received| event.received_at < last_received)
        {
            return self.fail_closed("broker event timestamps are future or out of receive order");
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
            return Err(ExecutionError::Lifecycle(
                "new non-duplicate event arrived after terminal state".into(),
            ));
        }
        let status = BrokerOrderStatus::from_provider(&event.status);
        if self.phase == OrderPhase::PartiallyFilled
            && matches!(
                status,
                BrokerOrderStatus::Accepted
                    | BrokerOrderStatus::New
                    | BrokerOrderStatus::PendingNew
                    | BrokerOrderStatus::AcceptedForBidding
            )
        {
            return self.fail_closed("broker order state regressed after a partial fill");
        }
        self.phase = match status {
            BrokerOrderStatus::Accepted
            | BrokerOrderStatus::New
            | BrokerOrderStatus::PendingNew
            | BrokerOrderStatus::AcceptedForBidding
            | BrokerOrderStatus::PendingCancel
            | BrokerOrderStatus::PendingReplace => OrderPhase::BrokerWorking {
                status: event.status.clone(),
            },
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
            BrokerOrderStatus::Unknown(provider_status) => {
                OrderPhase::FailClosedUnknown { provider_status }
            }
        };
        self.provider_order_id = event
            .provider_order_id
            .clone()
            .or_else(|| self.provider_order_id.clone());
        self.filled_quantity = event.filled_quantity;
        if event.fill_price.is_some() {
            self.average_fill_price = event.fill_price;
        }
        self.last_event_at = Some(event.received_at);
        if let Some(request_id) = &event.request_id {
            self.request_ids.insert(request_id.clone());
        }
        self.seen_payload_hashes.insert(event.raw_payload_hash);
        if matches!(self.phase, OrderPhase::FailClosedUnknown { .. }) {
            return Err(ExecutionError::Lifecycle(format!(
                "unknown provider order status: {}",
                event.status
            )));
        }
        Ok(())
    }

    fn fail_closed<T>(&mut self, detail: &str) -> Result<T, ExecutionError> {
        self.phase = OrderPhase::FailClosedUnknown {
            provider_status: detail.into(),
        };
        Err(ExecutionError::Lifecycle(detail.into()))
    }
}
