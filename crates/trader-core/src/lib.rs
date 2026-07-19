//! Deterministic, broker-independent trading domain.
//!
//! This crate intentionally has no HTTP, database, cloud, or broker dependency.
//! The production executor lives in a downstream crate so strategy code cannot
//! submit an order by construction.

#![forbid(unsafe_code)]

pub mod accounting;
pub mod backtest;
pub mod domain;
pub mod error;
pub mod fixed;
pub mod market;
pub mod performance;
pub mod pipeline;
pub mod risk;
pub mod strategy;

pub use backtest::{
    run_backtest, run_decision_replay, BacktestResult, DecisionReplayRequest, DecisionReplayResult,
};
pub use domain::*;
pub use error::{CoreError, CoreResult};
pub use fixed::{Fixed, Money, Price};
pub use performance::{
    run_performance_backtest, DatasetManifest, PerformanceBacktestRequest,
    PerformanceBacktestResult, PerformanceCostModel, ResearchStage, TerminalValuation,
    MAX_PERFORMANCE_REQUEST_BYTES,
};
pub use pipeline::{evaluate_decision, materialize_order_intent, EvaluationResult};
