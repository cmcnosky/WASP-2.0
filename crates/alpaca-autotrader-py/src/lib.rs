use pyo3::{exceptions::PyValueError, prelude::*};
use trader_core::{
    backtest::DecisionReplayRequest, evaluate_decision as evaluate_core,
    materialize_order_intent as materialize_core, run_backtest, run_decision_replay,
    DecisionSnapshot, FreshExecutionQuote, OrderPlan, RiskDecision, RiskLimitSnapshot,
    StrategyRelease,
};

fn invalid(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
fn evaluate_decision(
    snapshot_json: &str,
    release_json: &str,
    risk_limits_json: &str,
) -> PyResult<String> {
    let snapshot: DecisionSnapshot = serde_json::from_str(snapshot_json).map_err(invalid)?;
    let release: StrategyRelease = serde_json::from_str(release_json).map_err(invalid)?;
    let limits: RiskLimitSnapshot = serde_json::from_str(risk_limits_json).map_err(invalid)?;
    let result = evaluate_core(&snapshot, &release, &limits).map_err(invalid)?;
    serde_json::to_string(&result).map_err(invalid)
}

#[pyfunction]
fn backtest(request_json: &str) -> PyResult<String> {
    let request: DecisionReplayRequest = serde_json::from_str(request_json).map_err(invalid)?;
    let result = run_backtest(&request).map_err(invalid)?;
    serde_json::to_string(&result).map_err(invalid)
}

#[pyfunction]
fn decision_replay(request_json: &str) -> PyResult<String> {
    let request: DecisionReplayRequest = serde_json::from_str(request_json).map_err(invalid)?;
    let result = run_decision_replay(&request).map_err(invalid)?;
    serde_json::to_string(&result).map_err(invalid)
}

#[pyfunction]
fn materialize_order_intent(
    snapshot_json: &str,
    release_json: &str,
    risk_decision_json: &str,
    plan_json: &str,
    quote_json: &str,
) -> PyResult<String> {
    let snapshot: DecisionSnapshot = serde_json::from_str(snapshot_json).map_err(invalid)?;
    let release: StrategyRelease = serde_json::from_str(release_json).map_err(invalid)?;
    let risk: RiskDecision = serde_json::from_str(risk_decision_json).map_err(invalid)?;
    let plan: OrderPlan = serde_json::from_str(plan_json).map_err(invalid)?;
    let quote: FreshExecutionQuote = serde_json::from_str(quote_json).map_err(invalid)?;
    let result = materialize_core(&snapshot, &release, &risk, &plan, &quote).map_err(invalid)?;
    serde_json::to_string(&result).map_err(invalid)
}

#[pymodule]
fn alpaca_autotrader_core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(evaluate_decision, module)?)?;
    module.add_function(wrap_pyfunction!(backtest, module)?)?;
    module.add_function(wrap_pyfunction!(decision_replay, module)?)?;
    module.add_function(wrap_pyfunction!(materialize_order_intent, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
