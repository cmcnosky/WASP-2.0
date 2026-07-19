use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Serialize;
use trader_core::{AccountSnapshot, BrokerEvent, HashDigest, OrderIntent};

use crate::ExecutionError;

const MAX_SESSION_PERMIT_AGE_SECONDS: i64 = 15;

#[derive(Clone, Eq, PartialEq)]
pub enum SubmissionOutcome {
    Observed(BrokerEvent),
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
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct CancellationRequestAccepted {
    pub provider_order_id: String,
    pub accepted_at: DateTime<Utc>,
    pub request_id: String,
    pub raw_payload_hash: HashDigest,
}

#[derive(Clone, Eq, PartialEq)]
pub enum CancellationOutcome {
    RequestAccepted(CancellationRequestAccepted),
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
