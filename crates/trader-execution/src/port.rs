use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Serialize;
use trader_core::{
    AccountSnapshot, BrokerEvent, HashDigest, OrderIntent, OrderSide, Price, Symbol, WholeQuantity,
};

use crate::ExecutionError;

const MAX_SESSION_PERMIT_AGE_SECONDS: i64 = 15;
pub const MAX_SUBMISSION_RESPONSE_JSON_BYTES: usize = 64 * 1024;
pub const MAX_FILL_ACTIVITY_RESPONSE_JSON_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FILL_ACTIVITY_TRAVERSAL_JSON_BYTES: usize = 32 * 1024 * 1024;

/// Exact provider response evidence for a successfully observed submission.
///
/// The raw JSON bytes are retained verbatim rather than reconstructed from the
/// typed event. That allows the durable coordinator to persist the provider
/// payload whose hash is already carried by [`BrokerEvent`]. Construction
/// validates both a strict size ceiling and the payload hash so an
/// implementation cannot accidentally join an event to different evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct ObservedBrokerOrder {
    event: BrokerEvent,
    raw_response_json: Vec<u8>,
}

impl ObservedBrokerOrder {
    pub fn try_new(event: BrokerEvent, raw_response_json: Vec<u8>) -> Result<Self, ExecutionError> {
        if raw_response_json.is_empty()
            || raw_response_json.len() > MAX_SUBMISSION_RESPONSE_JSON_BYTES
        {
            return Err(ExecutionError::Broker(
                "observed broker-order response JSON is empty or exceeds its byte ceiling".into(),
            ));
        }
        let raw_value: serde_json::Value =
            serde_json::from_slice(&raw_response_json).map_err(|_| {
                ExecutionError::Broker("observed broker-order response is not JSON".into())
            })?;
        if !raw_value.is_object() {
            return Err(ExecutionError::Broker(
                "observed broker-order response JSON is not an object".into(),
            ));
        }
        if HashDigest::sha256(&raw_response_json) != event.raw_payload_hash {
            return Err(ExecutionError::Broker(
                "observed broker-order event does not match its raw response hash".into(),
            ));
        }
        Ok(Self {
            event,
            raw_response_json,
        })
    }

    pub fn event(&self) -> &BrokerEvent {
        &self.event
    }

    pub fn raw_response_json(&self) -> &[u8] {
        &self.raw_response_json
    }

    pub fn into_parts(self) -> (BrokerEvent, Vec<u8>) {
        (self.event, self.raw_response_json)
    }
}

/// One stable incremental FILL activity joined to the exact REST page that
/// carried it. The provider activity ID is the durable fill identity; the
/// order response's cumulative quantity is never split into invented fills.
#[derive(Clone, Eq, PartialEq)]
pub struct ObservedBrokerFill {
    pub fill_id: String,
    pub fill_type: String,
    pub provider_order_id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub quantity: WholeQuantity,
    pub cumulative_quantity: WholeQuantity,
    pub leaves_quantity: WholeQuantity,
    pub price: Price,
    pub executed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub request_id: Option<String>,
    pub request_parameters_hash: HashDigest,
    /// Stable hash of the normalized activity itself. This intentionally
    /// excludes page grouping, receive time, request ID, and pagination so the
    /// same provider fill remains idempotent when later traversals place it in
    /// a different response page.
    pub activity_evidence_hash: HashDigest,
    pub raw_payload_hash: HashDigest,
    raw_response_json: Arc<[u8]>,
}

impl ObservedBrokerFill {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        fill_id: String,
        fill_type: String,
        provider_order_id: String,
        symbol: Symbol,
        side: OrderSide,
        quantity: WholeQuantity,
        cumulative_quantity: WholeQuantity,
        leaves_quantity: WholeQuantity,
        price: Price,
        executed_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        request_id: Option<String>,
        request_parameters_hash: HashDigest,
        raw_payload_hash: HashDigest,
        raw_response_json: Arc<[u8]>,
    ) -> Result<Self, ExecutionError> {
        if fill_id.trim().is_empty()
            || !matches!(fill_type.as_str(), "fill" | "partial_fill")
            || (fill_type == "fill") != (leaves_quantity == WholeQuantity::ZERO)
            || provider_order_id.trim().is_empty()
            || quantity == WholeQuantity::ZERO
            || cumulative_quantity < quantity
            || !price.fixed().is_positive()
            || received_at < executed_at
            || raw_response_json.is_empty()
            || raw_response_json.len() > MAX_FILL_ACTIVITY_RESPONSE_JSON_BYTES
            || HashDigest::sha256(&raw_response_json) != raw_payload_hash
        {
            return Err(ExecutionError::Broker(
                "observed FILL activity evidence is incomplete or inconsistent".into(),
            ));
        }
        let raw: serde_json::Value = serde_json::from_slice(&raw_response_json).map_err(|_| {
            ExecutionError::Broker("observed FILL activity response is not JSON".into())
        })?;
        let occurrences = raw
            .as_array()
            .ok_or_else(|| {
                ExecutionError::Broker("observed FILL activity response is not an array".into())
            })?
            .iter()
            .filter(|item| item.get("id").and_then(serde_json::Value::as_str) == Some(&fill_id))
            .count();
        if occurrences != 1 {
            return Err(ExecutionError::Broker(
                "observed FILL activity ID is not uniquely present in its raw response".into(),
            ));
        }
        let activity_evidence_hash = HashDigest::of_json(&serde_json::json!({
            "schema": "wasp2/stable-rest-fill-activity/v1",
            "fill_id": &fill_id,
            "activity_type": "FILL",
            "fill_type": &fill_type,
            "provider_order_id": &provider_order_id,
            "symbol": &symbol,
            "side": side,
            "quantity": quantity,
            "cumulative_quantity": cumulative_quantity,
            "leaves_quantity": leaves_quantity,
            "price": price,
            "executed_at": executed_at,
        }))?;
        Ok(Self {
            fill_id,
            fill_type,
            provider_order_id,
            symbol,
            side,
            quantity,
            cumulative_quantity,
            leaves_quantity,
            price,
            executed_at,
            received_at,
            request_id,
            request_parameters_hash,
            activity_evidence_hash,
            raw_payload_hash,
            raw_response_json,
        })
    }

    pub fn raw_response_json(&self) -> &[u8] {
        &self.raw_response_json
    }
}

/// Local transport evidence proving that a submission never began broker I/O.
/// A future durable coordinator may authorize a retry only after recording
/// this exact attempt; this transient value alone never authorizes another
/// POST.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SubmissionNotDispatched {
    pub client_order_id: String,
    pub observed_at: DateTime<Utc>,
    pub reason_code: String,
    pub detail: String,
    pub evidence_hash: HashDigest,
}

#[derive(Clone, Eq, PartialEq)]
pub enum SubmissionOutcome {
    Observed(ObservedBrokerOrder),
    /// The transport proved that it rejected the request before any I/O. A
    /// retry requires a separate durable state transition and fresh authority.
    NotDispatched(SubmissionNotDispatched),
    /// The request may have reached the broker. Callers must persist
    /// SUBMISSION_UNKNOWN and query by the identical client_order_id.
    Unknown {
        detail: String,
    },
}

/// Evidence that Alpaca's current clock and authoritative NYSE calendar agreed
/// that the regular U.S. equity session was open when this permit was issued.
///
/// Fields are private and construction is crate-private so strategy/research
/// code cannot mint session authority. The permit is intentionally short-lived
/// and is not an order acknowledgement. Its two payload hashes make the exact
/// clock and calendar inputs independently auditable without retaining their
/// potentially sensitive response bodies in logs.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct RegularTradingSessionPermit {
    market: String,
    session_date: NaiveDate,
    session_open: DateTime<Utc>,
    session_close: DateTime<Utc>,
    clock_timestamp: DateTime<Utc>,
    verified_at: DateTime<Utc>,
    clock_payload_hash: HashDigest,
    calendar_payload_hash: HashDigest,
    clock_request_id: Option<String>,
    calendar_request_id: Option<String>,
}

impl RegularTradingSessionPermit {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn verified(
        market: String,
        session_date: NaiveDate,
        session_open: DateTime<Utc>,
        session_close: DateTime<Utc>,
        clock_timestamp: DateTime<Utc>,
        verified_at: DateTime<Utc>,
        clock_payload_hash: HashDigest,
        calendar_payload_hash: HashDigest,
        clock_request_id: Option<String>,
        calendar_request_id: Option<String>,
    ) -> Result<Self, ExecutionError> {
        if market != "NYSE"
            || session_open >= session_close
            || session_open.date_naive() != session_date
            || session_close.date_naive() != session_date
            || clock_timestamp < session_open
            || clock_timestamp >= session_close
            || verified_at < clock_timestamp
            || verified_at - clock_timestamp > Duration::seconds(MAX_SESSION_PERMIT_AGE_SECONDS)
        {
            return Err(ExecutionError::AuthorityDenied(
                "regular-session evidence is inconsistent or stale".into(),
            ));
        }
        Ok(Self {
            market,
            session_date,
            session_open,
            session_close,
            clock_timestamp,
            verified_at,
            clock_payload_hash,
            calendar_payload_hash,
            clock_request_id,
            calendar_request_id,
        })
    }

    /// Revalidates this ephemeral permit against a committed order and returns
    /// the final transport deadline. A permit must be obtained after the quote,
    /// making the last provider gate the session clock/calendar check.
    pub fn submission_deadline(
        &self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ExecutionError> {
        let not_after = intent.quote_valid_until.min(self.session_close);
        self.validate_submission_deadline(intent, not_after)?;
        if now < self.verified_at
            || now - self.verified_at > Duration::seconds(MAX_SESSION_PERMIT_AGE_SECONDS)
            || self.verified_at < intent.quote_received_at
            || now < self.session_open
            || now >= self.session_close
        {
            return Err(ExecutionError::AuthorityDenied(
                "regular-session permit is not current for this quote".into(),
            ));
        }
        if now >= not_after {
            return Err(ExecutionError::AuthorityDenied(
                "quote or regular trading session expired before dispatch".into(),
            ));
        }
        Ok(not_after)
    }

    pub(crate) fn validate_submission_deadline(
        &self,
        intent: &OrderIntent,
        not_after: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        let quote_validity = intent
            .quote_valid_until
            .signed_duration_since(intent.quote_received_at);
        if self.verified_at < intent.quote_received_at
            || self.verified_at >= self.session_close
            || quote_validity <= Duration::zero()
            || quote_validity > Duration::seconds(MAX_SESSION_PERMIT_AGE_SECONDS)
            || not_after != intent.quote_valid_until.min(self.session_close)
            || not_after <= self.verified_at
            || not_after - self.verified_at > Duration::seconds(MAX_SESSION_PERMIT_AGE_SECONDS)
        {
            return Err(ExecutionError::AuthorityDenied(
                "submission deadline is not bound to quote and session evidence".into(),
            ));
        }
        Ok(())
    }

    pub fn session_close(&self) -> DateTime<Utc> {
        self.session_close
    }

    pub fn verified_at(&self) -> DateTime<Utc> {
        self.verified_at
    }
}

/// A 204 response only acknowledges that Alpaca accepted the cancellation
/// request. It is never evidence that the order reached terminal `canceled`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CancellationRequestAccepted {
    pub provider_order_id: String,
    pub accepted_at: DateTime<Utc>,
    pub request_id: String,
    pub raw_payload_hash: HashDigest,
}

/// Local transport evidence proving that no broker I/O began. This outcome may
/// become retryable only after it is durably appended to the cancellation
/// state machine; an in-memory value alone never authorizes another DELETE.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CancellationNotDispatched {
    pub provider_order_id: String,
    pub observed_at: DateTime<Utc>,
    pub reason_code: String,
    pub detail: String,
    pub evidence_hash: HashDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancellationOutcome {
    RequestAccepted(CancellationRequestAccepted),
    /// The transport proved that it rejected the request before any I/O. The
    /// coordinator must durably record this exact attempt before retrying.
    NotDispatched(CancellationNotDispatched),
    /// Dispatch may have occurred but no trustworthy acknowledgement exists.
    /// Callers must reconcile by reading broker order state and must not infer
    /// a terminal lifecycle transition from this result.
    Unknown {
        detail: String,
    },
}

/// Narrow broker port owned exclusively by the fenced executor.
/// Implementations must not retry `submit_committed_intent` after an ambiguous result.
#[async_trait]
pub trait BrokerPort: Send + Sync {
    async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError>;
    /// Checks the concrete transport's certified provider-arrival allowance.
    /// This gate runs before intent/outbox commit and is rechecked by the
    /// transport immediately before bytes may be written.
    fn validate_submission_window(
        &self,
        broker_arrival_by: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), ExecutionError>;
    async fn find_order_by_client_id(
        &self,
        expected_intent: &OrderIntent,
    ) -> Result<Option<ObservedBrokerOrder>, ExecutionError>;
    /// Retrieves stable incremental FILL activities for one observed order.
    /// The default is deliberately unavailable so test doubles and future
    /// adapters cannot silently manufacture fills from cumulative order state.
    async fn fills_for_order(
        &self,
        _expected_intent: &OrderIntent,
        _provider_order_id: &str,
        _expected_cumulative_quantity: WholeQuantity,
    ) -> Result<Vec<ObservedBrokerFill>, ExecutionError> {
        Err(ExecutionError::Broker(
            "stable incremental FILL activity retrieval is unavailable".into(),
        ))
    }
    /// Reconciliation-only lookup for a cancellation whose DELETE may already
    /// have reached the broker. Implementations must validate both provider and
    /// client identities and must never turn this read into another DELETE.
    async fn find_order_by_provider_id(
        &self,
        provider_order_id: &str,
        expected_client_order_id: &str,
    ) -> Result<Option<BrokerEvent>, ExecutionError>;
    async fn submit_committed_intent(
        &self,
        intent: &OrderIntent,
        session_permit: &RegularTradingSessionPermit,
        not_after: DateTime<Utc>,
    ) -> Result<SubmissionOutcome, ExecutionError>;
    async fn cancel_order(
        &self,
        provider_order_id: &str,
    ) -> Result<CancellationOutcome, ExecutionError>;
}
