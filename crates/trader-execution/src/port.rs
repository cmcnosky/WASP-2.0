use async_trait::async_trait;
use trader_core::{AccountSnapshot, BrokerEvent, OrderIntent};

use crate::ExecutionError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmissionOutcome {
    Observed(BrokerEvent),
    /// The request may have reached the broker. Callers must persist
    /// SUBMISSION_UNKNOWN and query by the identical client_order_id.
    Unknown {
        detail: String,
    },
}

/// Narrow broker port owned exclusively by the fenced executor.
/// Implementations must not retry `submit_committed_intent` after an ambiguous result.
#[async_trait]
pub trait BrokerPort: Send + Sync {
    async fn account_snapshot(&self) -> Result<AccountSnapshot, ExecutionError>;
    async fn find_order_by_client_id(
        &self,
        client_order_id: &str,
    ) -> Result<Option<BrokerEvent>, ExecutionError>;
    async fn submit_committed_intent(
        &self,
        intent: &OrderIntent,
    ) -> Result<SubmissionOutcome, ExecutionError>;
    async fn cancel_order(&self, provider_order_id: &str) -> Result<(), ExecutionError>;
}
