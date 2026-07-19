//! Execution boundary. Broker-facing code may depend on `trader-core`; the
//! inverse dependency is intentionally impossible.

#![forbid(unsafe_code)]

pub mod alpaca;
pub mod authority;
pub mod config;
pub mod coordinator;
pub mod database;
pub mod durable_submission;
pub mod executor;
pub mod http_transport;
pub mod ledger;
pub mod lifecycle;
pub mod observer_database;
pub mod observer_runtime;
pub mod observer_store;
pub mod port;
pub mod rate_limit;
pub mod reconciliation;
pub mod store;

use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ExecutionError {
    #[error("unsafe configuration: {0}")]
    UnsafeConfiguration(String),
    #[error("execution authority denied: {0}")]
    AuthorityDenied(String),
    #[error("ledger invariant failed: {0}")]
    LedgerInvariant(String),
    #[error("order lifecycle failed closed: {0}")]
    Lifecycle(String),
    #[error("broker outcome is ambiguous: {0}")]
    SubmissionUnknown(String),
    #[error("broker adapter failed: {0}")]
    Broker(String),
    #[error("core domain failed: {0}")]
    Core(String),
}

impl From<trader_core::CoreError> for ExecutionError {
    fn from(value: trader_core::CoreError) -> Self {
        Self::Core(value.to_string())
    }
}
