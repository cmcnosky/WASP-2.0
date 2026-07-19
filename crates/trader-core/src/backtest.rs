use serde::{Deserialize, Serialize};

use crate::{
    domain::{DecisionSnapshot, HashDigest, RiskLimitSnapshot, StrategyRelease},
    error::{CoreError, CoreResult},
    pipeline::{evaluate_decision, EvaluationResult},
};

/// Provider-free deterministic decision replay. This is intentionally not a
/// performance backtest: it has no post-decision quotes, fills, or P&L model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplayRequest {
    pub release: StrategyRelease,
    pub risk_limits: RiskLimitSnapshot,
    pub snapshots: Vec<DecisionSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplayStep {
    pub as_of: chrono::DateTime<chrono::Utc>,
    pub evaluation: EvaluationResult,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplayResult {
    pub release_id: String,
    pub steps: Vec<DecisionReplayStep>,
    pub result_hash: HashDigest,
}

/// Fail-closed response for callers that request a "backtest". Decision replay
/// is returned for parity diagnostics, but it cannot certify profitability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestResult {
    pub performance_evidence_available: bool,
    pub hold_reason: String,
    pub decision_replay: DecisionReplayResult,
}

pub fn run_decision_replay(request: &DecisionReplayRequest) -> CoreResult<DecisionReplayResult> {
    request.release.validate()?;
    request.risk_limits.validate()?;
    let mut previous = None;
    let mut steps = Vec::with_capacity(request.snapshots.len());
    for snapshot in &request.snapshots {
        if previous.is_some_and(|timestamp| snapshot.as_of <= timestamp) {
            return Err(CoreError::InvalidDomain(
                "decision replay snapshots are not strictly chronological".into(),
            ));
        }
        previous = Some(snapshot.as_of);
        steps.push(DecisionReplayStep {
            as_of: snapshot.as_of,
            evaluation: evaluate_decision(snapshot, &request.release, &request.risk_limits)?,
        });
    }
    let result_hash = HashDigest::of_json(&(&request.release.release_id, &steps))?;
    Ok(DecisionReplayResult {
        release_id: request.release.release_id.clone(),
        steps,
        result_hash,
    })
}

pub fn run_backtest(request: &DecisionReplayRequest) -> CoreResult<BacktestResult> {
    Ok(BacktestResult {
        performance_evidence_available: false,
        hold_reason:
            "HOLD: deterministic decision replay has no post-decision execution/P&L evidence".into(),
        decision_replay: run_decision_replay(request)?,
    })
}
